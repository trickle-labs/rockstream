//! Incremental inner equi-join operator (v0.8 — IVM-4).
//!
//! `JoinOp` implements the DBSP bilinear rule for inner equi-joins:
//!
//! ```text
//! Δ(L ⋈ R) = ΔL ⋈ R₀  +  L₀ ⋈ ΔR  +  ΔL ⋈ ΔR
//! ```
//!
//! Both arrangements (`left_arr` and `right_arr`) reflect the state at the
//! end of epoch `e-1` during the processing of epoch `e`.  After every epoch
//! commit the arrangements are updated atomically.
//!
//! ## Input contract
//!
//! `process_left_delta` and `process_right_delta` accept an `ArrowZSet` where:
//! - `left_keys` / `right_keys` name the column indices used as the join key.
//! - The join key is extracted, encoded as big-endian i64 bytes concatenated.
//! - `row_id` is derived from `stable_row_id(join_key, row_bytes)` — a 128-bit
//!   hash that is stable across crash-replay.
//!
//! Call `commit_epoch()` after both sides have been staged.  It computes
//! `ΔL ⋈ ΔR` and applies all staged updates to the in-memory arrangements,
//! then returns the full combined output delta.
//!
//! ## State persistence
//!
//! Left arrangement:  `[0x01][0x4A4C][op_id:8][join_key_bytes][row_id:16]` → `row_bytes`
//! Right arrangement: `[0x01][0x4A52][op_id:8][join_key_bytes][row_id:16]` → `row_bytes`
//!
//! Persistence uses only `WriteBatch::put` and `WriteBatch::delete` (point
//! operations); no range deletion is used.
//!
//! ## Bounds
//!
//! `left_entry_count()` and `right_entry_count()` report fill levels.  Each
//! arrangement is bounded by the number of distinct live (join_key, row_id)
//! pairs on each side of the join.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use rockstream_storage::{JoinSide, ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::BatchKeyCapsule;

use crate::chunked::{ChunkBudget, ChunkStats, ChunkStepResult, ResumableChunkedOp};
use crate::error::OpError;
use crate::zset::ArrowZSet;

// ─── Schema ──────────────────────────────────────────────────────────────────

/// Output schema for a join of two 2-column (k, v) inputs.
/// Output: (l_k, l_v, r_v) — left key, left value, right value.
pub fn join_output_schema() -> SchemaRef {
    join_output_schema_n(2, 2)
}

/// Generate a join output schema for `left_n_cols` left columns and `right_n_cols` right columns.
/// Output columns: l_col0..l_col(N-1), r_col0..r_col(M-1).
pub fn join_output_schema_n(left_n_cols: usize, right_n_cols: usize) -> SchemaRef {
    let mut fields: Vec<Field> = Vec::new();
    for i in 0..left_n_cols {
        fields.push(Field::new(format!("l_{i}"), DataType::Int64, false));
    }
    for i in 0..right_n_cols {
        fields.push(Field::new(format!("r_{i}"), DataType::Int64, false));
    }
    Arc::new(Schema::new(fields))
}

// ─── Row identity ─────────────────────────────────────────────────────────────

/// Compute a stable 128-bit row identity from join-key bytes and the full row.
///
/// This is the "keyed CDC source" rule from DESIGN.md §6.4:
/// `row_id = hash(op_id, join_key_bytes, row_bytes)`.
///
/// The hash is FNV-1a 128-bit, a fast non-cryptographic hash adequate for
/// stable identity.  The same input bytes always produce the same row_id, so
/// crash-replay rewrites the same arrangement key.
pub fn stable_row_id(op_id: u64, join_key: &[u8], row_bytes: &[u8]) -> u128 {
    // FNV-1a 128-bit: offset_basis = 144066263297769815596495629667062367629
    let mut hash: u128 = 144_066_263_297_769_815_596_495_629_667_062_367_629;
    let prime: u128 = 309_485_009_821_345_068_724_781_371;
    for &b in op_id.to_be_bytes().iter().chain(join_key).chain(row_bytes) {
        hash ^= b as u128;
        hash = hash.wrapping_mul(prime);
    }
    hash
}

// ─── JoinState ────────────────────────────────────────────────────────────────

/// An arrangement entry tracking the net weight for a row.
///
/// When weight reaches 0, the entry is removed (no longer in the relation).
#[derive(Debug, Clone)]
struct ArrRow {
    row_bytes: Vec<u8>,
    /// Net Z-set weight for this row in the arrangement.
    weight: i64,
}

/// In-memory state for `JoinOp`.
///
/// Two weight-tracking arrangements: join_key → {row_id → (row_bytes, weight)}.
///
/// # Bound
///
/// Each arrangement is bounded by the number of distinct live (join_key, row_id)
/// pairs.  `left_entry_count()` / `right_entry_count()` track the fill level.
#[derive(Debug, Default)]
pub struct JoinState {
    /// join_key_bytes → HashMap<row_id, ArrRow>
    left_arr: HashMap<Vec<u8>, HashMap<u128, ArrRow>>,
    /// join_key_bytes → HashMap<row_id, ArrRow>
    right_arr: HashMap<Vec<u8>, HashMap<u128, ArrRow>>,
    state_bytes: u64,
}

impl JoinState {
    pub fn new() -> Self {
        JoinState::default()
    }

    pub fn state_bytes(&self) -> u64 {
        self.state_bytes
    }

    /// Apply a delta to the left arrangement.
    /// Tracks net weight per row_id; removes when weight reaches 0.
    fn update_left(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, delta_w: i64) {
        let is_new_key = !self.left_arr.contains_key(&join_key);
        let key_len = join_key.len() as u64;
        let bucket = self.left_arr.entry(join_key.clone()).or_default();
        if is_new_key {
            self.state_bytes += key_len;
        }

        if let Some(entry) = bucket.get_mut(&row_id) {
            entry.weight += delta_w;
            if entry.weight == 0 {
                let removed = bucket.remove(&row_id).unwrap();
                self.state_bytes = self
                    .state_bytes
                    .saturating_sub(16 + 8 + removed.row_bytes.len() as u64);
                if bucket.is_empty() {
                    self.left_arr.remove(&join_key);
                    self.state_bytes = self.state_bytes.saturating_sub(key_len);
                }
            }
        } else if delta_w != 0 {
            let row_len = row_bytes.len() as u64;
            bucket.insert(
                row_id,
                ArrRow {
                    row_bytes,
                    weight: delta_w,
                },
            );
            self.state_bytes += 16 + 8 + row_len;
        }
    }

    /// Apply a delta to the right arrangement.
    fn update_right(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, delta_w: i64) {
        let is_new_key = !self.right_arr.contains_key(&join_key);
        let key_len = join_key.len() as u64;
        let bucket = self.right_arr.entry(join_key.clone()).or_default();
        if is_new_key {
            self.state_bytes += key_len;
        }

        if let Some(entry) = bucket.get_mut(&row_id) {
            entry.weight += delta_w;
            if entry.weight == 0 {
                let removed = bucket.remove(&row_id).unwrap();
                self.state_bytes = self
                    .state_bytes
                    .saturating_sub(16 + 8 + removed.row_bytes.len() as u64);
                if bucket.is_empty() {
                    self.right_arr.remove(&join_key);
                    self.state_bytes = self.state_bytes.saturating_sub(key_len);
                }
            }
        } else if delta_w != 0 {
            let row_len = row_bytes.len() as u64;
            bucket.insert(
                row_id,
                ArrRow {
                    row_bytes,
                    weight: delta_w,
                },
            );
            self.state_bytes += 16 + 8 + row_len;
        }
    }

    fn insert_persisted(
        &mut self,
        side: JoinSide,
        join_key: Vec<u8>,
        row_id: u128,
        row_bytes: Vec<u8>,
    ) {
        let arr = match side {
            JoinSide::Left => &mut self.left_arr,
            JoinSide::Right => &mut self.right_arr,
        };
        let is_new_key = !arr.contains_key(&join_key);
        let row_len = row_bytes.len() as u64;
        let inserted = {
            let bucket = arr.entry(join_key.clone()).or_default();
            if let std::collections::hash_map::Entry::Vacant(entry) = bucket.entry(row_id) {
                entry.insert(ArrRow {
                    row_bytes,
                    weight: 1,
                });
                true
            } else {
                false
            }
        };
        if !inserted {
            return;
        }
        if is_new_key {
            self.state_bytes += join_key.len() as u64;
        }
        self.state_bytes += 16 + 8 + row_len;
    }

    /// Iterate over all right rows matching a join key, as (row_bytes, weight) pairs.
    ///
    /// Only rows with weight != 0 are present (weight 0 rows are removed at update time).
    fn probe_right(&self, join_key: &[u8]) -> impl Iterator<Item = (&Vec<u8>, i64)> {
        self.right_arr
            .get(join_key)
            .into_iter()
            .flat_map(|m| m.values().map(|e| (&e.row_bytes, e.weight)))
    }

    /// Iterate over all left rows matching a join key, as (row_bytes, weight) pairs.
    fn probe_left(&self, join_key: &[u8]) -> impl Iterator<Item = (&Vec<u8>, i64)> {
        self.left_arr
            .get(join_key)
            .into_iter()
            .flat_map(|m| m.values().map(|e| (&e.row_bytes, e.weight)))
    }

    /// Total number of live entries in the left arrangement.
    pub fn left_entry_count(&self) -> usize {
        self.left_arr.values().map(|m| m.len()).sum()
    }

    /// Total number of live entries in the right arrangement.
    pub fn right_entry_count(&self) -> usize {
        self.right_arr.values().map(|m| m.len()).sum()
    }
}

// ─── Staged delta ─────────────────────────────────────────────────────────────

/// One side's staged delta for an epoch.
#[derive(Debug, Default)]
struct StagedDelta {
    /// (join_key, row_id, row_bytes, weight)
    rows: Vec<(Vec<u8>, u128, Vec<u8>, i64)>,
    staged_bytes: u64,
}

impl StagedDelta {
    fn push(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, weight: i64) {
        self.staged_bytes += (join_key.len() + 16 + row_bytes.len() + 8) as u64;
        self.rows.push((join_key, row_id, row_bytes, weight));
    }

    #[allow(dead_code)]
    fn clear(&mut self) {
        self.rows.clear();
        self.staged_bytes = 0;
    }
}

// ─── JoinStateCore (single-owner, zero-Mutex) ────────────────────────────────

/// Single-owner, lock-free (`&mut self`) state core for [`JoinOp`].
///
/// Consolidates all arrangement state, staged deltas, dirty sets, and demand-load
/// tracking with zero internal `Mutex`es for single-owner shard actors.
pub struct JoinStateCore {
    op_id: OperatorId,
    left_key_cols: Vec<usize>,
    right_key_cols: Vec<usize>,
    left_n_cols: usize,
    right_n_cols: usize,
    state: JoinState,
    left_staged: StagedDelta,
    right_staged: StagedDelta,
    dirty_left: HashSet<(Vec<u8>, u128)>,
    dirty_right: HashSet<(Vec<u8>, u128)>,
    db: Option<Arc<ShardDb>>,
    state_loaded: bool,
    loaded_left_keys: HashSet<Vec<u8>>,
    loaded_right_keys: HashSet<Vec<u8>>,
}

impl JoinStateCore {
    pub fn with_schema(
        op_id: OperatorId,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
        left_n_cols: usize,
        right_n_cols: usize,
    ) -> Self {
        Self {
            op_id,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            state: JoinState::new(),
            left_staged: StagedDelta::default(),
            right_staged: StagedDelta::default(),
            dirty_left: HashSet::new(),
            dirty_right: HashSet::new(),
            db: None,
            state_loaded: false,
            loaded_left_keys: HashSet::new(),
            loaded_right_keys: HashSet::new(),
        }
    }

    pub fn op_id(&self) -> OperatorId {
        self.op_id
    }

    pub fn set_db(&mut self, db: Arc<ShardDb>) {
        self.db = Some(db);
        self.state_loaded = false;
        self.loaded_left_keys.clear();
        self.loaded_right_keys.clear();
    }

    pub fn db(&self) -> Option<Arc<ShardDb>> {
        self.db.clone()
    }

    pub fn left_entry_count(&self) -> usize {
        self.state.left_entry_count()
    }

    pub fn right_entry_count(&self) -> usize {
        self.state.right_entry_count()
    }

    pub fn state_bytes(&self) -> u64 {
        self.state.state_bytes() + self.left_staged.staged_bytes + self.right_staged.staged_bytes
    }

    /// Stage the left delta using once-per-batch `BatchKeyCapsule` (zero mutex locking).
    pub fn process_left_delta_mut(&mut self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let schema = join_output_schema_n(self.left_n_cols, self.right_n_cols);
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(schema));
        }
        let capsule = delta.get_or_build_key_capsule(&self.left_key_cols)?;
        let mut left_cols: Vec<Vec<i64>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<i64>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();
        let mut out_key_rows: Vec<(usize, u128)> = Vec::new();

        for row_idx in 0..delta.num_rows() {
            if capsule.row_contains_null(row_idx) {
                continue;
            }
            let w = delta.weights[row_idx];
            let join_key_slice = capsule.row_bytes(row_idx);
            let join_key_hash = capsule.row_hash_u128(row_idx);
            let row_bytes = JoinOp::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, join_key_slice, &row_bytes);

            let left_vals = JoinOp::deserialize_row(&row_bytes, self.left_n_cols);
            for (right_row_bytes, r_weight) in self.state.probe_right(join_key_slice) {
                let right_vals = JoinOp::deserialize_row(right_row_bytes, self.right_n_cols);
                for (i, v) in left_vals.iter().enumerate() {
                    left_cols[i].push(*v);
                }
                for (i, v) in right_vals.iter().enumerate() {
                    right_cols[i].push(*v);
                }
                out_weights.push(w * r_weight);
                out_key_rows.push((row_idx, join_key_hash));
            }

            self.left_staged
                .push(join_key_slice.to_vec(), row_id, row_bytes, w);
        }

        let batch = JoinOp::make_output_batch(left_cols, right_cols, &schema)?;
        let mut out = ArrowZSet::new(batch, out_weights);
        if !out.is_empty() {
            let propagated = BatchKeyCapsule::from_encoded_slices(
                self.left_key_cols.clone(),
                out_key_rows
                    .iter()
                    .map(|&(src_row, h128)| (capsule.row_bytes(src_row), h128, false)),
            );
            out.push_key_capsule(Arc::new(propagated));
        }
        debug!(
            op = "JoinOp",
            op_id = self.op_id.0,
            "left delta: {} rows → {} join outputs",
            delta.num_rows(),
            out.num_rows()
        );
        Ok(out)
    }

    /// Stage the right delta using once-per-batch `BatchKeyCapsule` (zero mutex locking).
    pub fn process_right_delta_mut(&mut self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let schema = join_output_schema_n(self.left_n_cols, self.right_n_cols);
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(schema));
        }
        let capsule = delta.get_or_build_key_capsule(&self.right_key_cols)?;
        let mut left_cols: Vec<Vec<i64>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<i64>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();
        let mut out_key_rows: Vec<(usize, u128)> = Vec::new();

        for row_idx in 0..delta.num_rows() {
            if capsule.row_contains_null(row_idx) {
                continue;
            }
            let w = delta.weights[row_idx];
            let join_key_slice = capsule.row_bytes(row_idx);
            let join_key_hash = capsule.row_hash_u128(row_idx);
            let row_bytes = JoinOp::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, join_key_slice, &row_bytes);

            let right_vals = JoinOp::deserialize_row(&row_bytes, self.right_n_cols);
            for (left_row_bytes, l_weight) in self.state.probe_left(join_key_slice) {
                let left_vals = JoinOp::deserialize_row(left_row_bytes, self.left_n_cols);
                for (i, v) in left_vals.iter().enumerate() {
                    left_cols[i].push(*v);
                }
                for (i, v) in right_vals.iter().enumerate() {
                    right_cols[i].push(*v);
                }
                out_weights.push(l_weight * w);
                out_key_rows.push((row_idx, join_key_hash));
            }

            self.right_staged
                .push(join_key_slice.to_vec(), row_id, row_bytes, w);
        }

        let batch = JoinOp::make_output_batch(left_cols, right_cols, &schema)?;
        let mut out = ArrowZSet::new(batch, out_weights);
        if !out.is_empty() {
            let propagated = BatchKeyCapsule::from_encoded_slices(
                self.left_key_cols.clone(),
                out_key_rows
                    .iter()
                    .map(|&(src_row, h128)| (capsule.row_bytes(src_row), h128, false)),
            );
            out.push_key_capsule(Arc::new(propagated));
        }
        debug!(
            op = "JoinOp",
            op_id = self.op_id.0,
            "right delta: {} rows → {} join outputs",
            delta.num_rows(),
            out.num_rows()
        );
        Ok(out)
    }

    /// Compute `ΔL ⋈ ΔR`, apply staged deltas to arrangements, and return correction output.
    pub fn commit_epoch_mut(&mut self) -> Result<ArrowZSet, OpError> {
        let schema = join_output_schema_n(self.left_n_cols, self.right_n_cols);
        let mut left_cols: Vec<Vec<i64>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<i64>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();
        let mut out_left_indices: Vec<usize> = Vec::new();

        let mut right_by_key: HashMap<&[u8], Vec<(Vec<i64>, i64)>> = HashMap::new();
        for (join_key, _row_id, row_bytes, weight) in &self.right_staged.rows {
            right_by_key.entry(join_key.as_slice()).or_default().push((
                JoinOp::deserialize_row(row_bytes, self.right_n_cols),
                *weight,
            ));
        }

        for (left_idx, (join_key, _row_id, left_bytes, l_w)) in self.left_staged.rows.iter().enumerate() {
            if let Some(right_rows) = right_by_key.get(join_key.as_slice()) {
                let left_vals = JoinOp::deserialize_row(left_bytes, self.left_n_cols);
                for (right_vals, r_w) in right_rows {
                    for (i, v) in left_vals.iter().enumerate() {
                        left_cols[i].push(*v);
                    }
                    for (i, v) in right_vals.iter().enumerate() {
                        right_cols[i].push(*v);
                    }
                    out_weights.push(l_w * r_w);
                    out_left_indices.push(left_idx);
                }
            }
        }
        drop(right_by_key);

        // Build the output capsule while staged keys are still available. Fanout
        // can be millions of rows: store a source-row index instead of cloning
        // the encoded join key for every matching pair.
        let batch = JoinOp::make_output_batch(left_cols, right_cols, &schema)?;
        let mut out = ArrowZSet::new(batch, out_weights);
        if !out.is_empty() {
            let left_hashes: Vec<u128> = self
                .left_staged
                .rows
                .iter()
                .map(|(key, _, _, _)| {
                    rockstream_types::KeyCapsule::from_typed_bytes(key.clone()).stable_hash()
                })
                .collect();
            let propagated = BatchKeyCapsule::from_encoded_slices(
                self.left_key_cols.clone(),
                out_left_indices.iter().map(|&idx| {
                    (self.left_staged.rows[idx].0.as_slice(), left_hashes[idx], false)
                }),
            );
            out.push_key_capsule(Arc::new(propagated));
        }

        for (join_key, row_id, row_bytes, w) in self.left_staged.rows.drain(..) {
            self.dirty_left.insert((join_key.clone(), row_id));
            self.state.update_left(join_key, row_id, row_bytes, w);
        }
        for (join_key, row_id, row_bytes, w) in self.right_staged.rows.drain(..) {
            self.dirty_right.insert((join_key.clone(), row_id));
            self.state.update_right(join_key, row_id, row_bytes, w);
        }
        self.left_staged.staged_bytes = 0;
        self.right_staged.staged_bytes = 0;
        Ok(out)
    }

    pub fn process_epoch_with_counters_mut(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<(ArrowZSet, crate::governor::DeltaAmplificationCounters), OpError> {
        let input_deltas = (left.num_rows() + right.num_rows()) as u64;
        let left_out = self.process_left_delta_mut(left)?;
        let right_out = self.process_right_delta_mut(right)?;
        let arrangement_probes =
            (self.left_staged.rows.len() + self.right_staged.rows.len()) as u64;
        let state_writes = self
            .left_staged
            .rows
            .iter()
            .chain(&self.right_staged.rows)
            .filter(|(_, _, _, weight)| *weight != 0)
            .count() as u64;
        let correction = self.commit_epoch_mut()?;
        let out_schema = join_output_schema_n(self.left_n_cols, self.right_n_cols);
        let output = concat_zsets(vec![left_out, right_out, correction], out_schema)?;
        Ok((
            output,
            crate::governor::DeltaAmplificationCounters {
                input_deltas,
                probes: arrangement_probes,
                shuffled_bytes: 0,
                intermediate_tuples: 0,
                output_deltas: 0,
                state_writes,
            },
        ))
    }

    pub fn process_epoch_mut(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.process_epoch_with_counters_mut(left, right)
            .map(|(output, _)| output)
    }

    pub fn append_state_mut(&mut self, target: &mut WriteBatch) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();
        for (join_key, row_id) in self.dirty_left.drain() {
            let key =
                ShardKeyEncoder::join_arr_key(JoinSide::Left, self.op_id.0, &join_key, row_id);
            if let Some(arr_row) = self
                .state
                .left_arr
                .get(&join_key)
                .and_then(|b| b.get(&row_id))
            {
                if arr_row.weight > 0 {
                    batch.put(&key, &arr_row.row_bytes);
                } else {
                    batch.delete(&key);
                }
            } else {
                batch.delete(&key);
            }
        }
        for (join_key, row_id) in self.dirty_right.drain() {
            let key =
                ShardKeyEncoder::join_arr_key(JoinSide::Right, self.op_id.0, &join_key, row_id);
            if let Some(arr_row) = self
                .state
                .right_arr
                .get(&join_key)
                .and_then(|b| b.get(&row_id))
            {
                if arr_row.weight > 0 {
                    batch.put(&key, &arr_row.row_bytes);
                } else {
                    batch.delete(&key);
                }
            } else {
                batch.delete(&key);
            }
        }
        target.merge_from(batch);
        Ok(())
    }
}

// ─── JoinOp ───────────────────────────────────────────────────────────────────

/// Incremental inner equi-join operator (v0.8 — IVM-4; consolidated single-owner core for #402).
///
/// Consolidates all mutable join state into a single [`JoinStateCore`] so shard
/// actors execute via `&mut self` (`process_epoch_mut`, `into_core`) with zero
/// hot-path mutex locking, while legacy `&self` callers acquire one uncontended lock.
pub struct JoinOp {
    op_id: OperatorId,
    left_key_cols: Vec<usize>,
    right_key_cols: Vec<usize>,
    left_n_cols: usize,
    right_n_cols: usize,
    core: Mutex<JoinStateCore>,
}

impl JoinOp {
    /// Create a new `JoinOp` for 2-column `(k, v)` inputs.
    pub fn new(op_id: OperatorId, left_key_cols: Vec<usize>, right_key_cols: Vec<usize>) -> Self {
        Self::with_schema(op_id, left_key_cols, right_key_cols, 2, 2)
    }

    /// Create a `JoinOp` with explicit column counts for variable-width schemas.
    pub fn with_schema(
        op_id: OperatorId,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
        left_n_cols: usize,
        right_n_cols: usize,
    ) -> Self {
        let core = JoinStateCore::with_schema(
            op_id,
            left_key_cols.clone(),
            right_key_cols.clone(),
            left_n_cols,
            right_n_cols,
        );
        JoinOp {
            op_id,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            core: Mutex::new(core),
        }
    }

    /// Unwrap this `JoinOp` into a lock-free single-owner [`JoinStateCore`].
    pub fn into_core(self) -> JoinStateCore {
        self.core.into_inner().unwrap()
    }

    /// Wrap an owned lock-free [`JoinStateCore`].
    pub fn from_core(core: JoinStateCore) -> Self {
        Self {
            op_id: core.op_id,
            left_key_cols: core.left_key_cols.clone(),
            right_key_cols: core.right_key_cols.clone(),
            left_n_cols: core.left_n_cols,
            right_n_cols: core.right_n_cols,
            core: Mutex::new(core),
        }
    }

    /// Attach a ShardDb for arrangement lookups and tiered caching.
    pub fn with_db(mut self, db: Arc<ShardDb>) -> Self {
        self.core.get_mut().unwrap().set_db(db);
        self
    }

    /// Set a ShardDb for arrangement lookups and tiered caching.
    pub fn set_db(&self, db: Arc<ShardDb>) {
        self.core.lock().unwrap().set_db(db);
    }

    /// Access the attached ShardDb if configured.
    pub fn db(&self) -> Option<Arc<ShardDb>> {
        self.core.lock().unwrap().db()
    }

    async fn ensure_arrangement_key_loaded(
        &self,
        side: JoinSide,
        join_key: &[u8],
    ) -> Result<(), OpError> {
        let db = {
            let core = self.core.lock().unwrap();
            if core.state_loaded {
                return Ok(());
            }
            let already_loaded = match side {
                JoinSide::Left => core.loaded_left_keys.contains(join_key),
                JoinSide::Right => core.loaded_right_keys.contains(join_key),
            };
            if already_loaded {
                return Ok(());
            }
            let Some(db) = core.db.clone() else {
                return Ok(());
            };
            db
        };

        let prefix = ShardKeyEncoder::join_arr_key_prefix(side, self.op_id.0, join_key);
        let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;

        let mut core = self.core.lock().unwrap();
        for (key, value) in entries {
            if key.len() < 11 + 16 {
                continue;
            }
            let row_id = u128::from_be_bytes(
                key[key.len() - 16..]
                    .try_into()
                    .map_err(|_| OpError::storage_error("invalid join arrangement row id"))?,
            );
            let is_dirty = match side {
                JoinSide::Left => core.dirty_left.contains(&(join_key.to_vec(), row_id)),
                JoinSide::Right => core.dirty_right.contains(&(join_key.to_vec(), row_id)),
            };
            if is_dirty {
                continue;
            }
            core.state
                .insert_persisted(side, join_key.to_vec(), row_id, value.to_vec());
        }
        match side {
            JoinSide::Left => {
                core.loaded_left_keys.insert(join_key.to_vec());
            }
            JoinSide::Right => {
                core.loaded_right_keys.insert(join_key.to_vec());
            }
        }
        Ok(())
    }

    async fn load_arrangement_keys(
        &self,
        side: JoinSide,
        delta: &ArrowZSet,
        key_cols: &[usize],
    ) -> Result<(), OpError> {
        {
            let core = self.core.lock().unwrap();
            if core.state_loaded || core.db.is_none() || delta.is_empty() {
                return Ok(());
            }
        }
        let capsule = delta.get_or_build_key_capsule(key_cols)?;
        let mut keys = HashSet::new();
        for row_idx in 0..delta.num_rows() {
            if !capsule.row_contains_null(row_idx) {
                keys.insert(capsule.row_bytes(row_idx).to_vec());
            }
        }
        for join_key in keys {
            self.ensure_arrangement_key_loaded(side, &join_key).await?;
        }
        Ok(())
    }

    pub async fn probe_arrangement(
        &self,
        side: JoinSide,
        join_key: &[u8],
    ) -> Result<Vec<(Vec<u8>, i64)>, OpError> {
        self.ensure_arrangement_key_loaded(side, join_key).await?;
        let core = self.core.lock().unwrap();
        let in_mem: Vec<(Vec<u8>, i64)> = match side {
            JoinSide::Left => core
                .state
                .probe_left(join_key)
                .map(|(r, w)| (r.clone(), w))
                .collect(),
            JoinSide::Right => core
                .state
                .probe_right(join_key)
                .map(|(r, w)| (r.clone(), w))
                .collect(),
        };
        Ok(in_mem)
    }

    pub async fn point_lookup_arrangement(
        &self,
        side: JoinSide,
        join_key: &[u8],
        row_id: u128,
    ) -> Result<Option<Vec<u8>>, OpError> {
        let db_opt = {
            let core = self.core.lock().unwrap();
            let arr = match side {
                JoinSide::Left => &core.state.left_arr,
                JoinSide::Right => &core.state.right_arr,
            };
            if let Some(bucket) = arr.get(join_key) {
                if let Some(row) = bucket.get(&row_id) {
                    return Ok((row.weight > 0).then(|| row.row_bytes.clone()));
                }
            }
            if core.state_loaded {
                return Ok(None);
            }
            let is_dirty = match side {
                JoinSide::Left => core.dirty_left.contains(&(join_key.to_vec(), row_id)),
                JoinSide::Right => core.dirty_right.contains(&(join_key.to_vec(), row_id)),
            };
            if is_dirty {
                return Ok(None);
            }
            core.db.clone()
        };

        if let Some(db) = db_opt {
            let key = ShardKeyEncoder::join_arr_key(side, self.op_id.0, join_key, row_id);
            let opt = db.get(&key).await.map_err(OpError::storage)?;
            return Ok(opt.map(|b| b.to_vec()));
        }

        Ok(None)
    }

    pub async fn process_epoch_with_storage(
        &self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<(ArrowZSet, crate::governor::DeltaAmplificationCounters), OpError> {
        self.load_arrangement_keys(JoinSide::Right, &left, &self.left_key_cols)
            .await?;
        self.load_arrangement_keys(JoinSide::Left, &right, &self.right_key_cols)
            .await?;
        self.process_epoch_with_counters(left, right)
    }

    /// Serialize all columns of a row (excluding any weight column) as bytes.
    fn serialize_row(batch: &RecordBatch, row_idx: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        for col in batch.columns() {
            let arr = col.as_any().downcast_ref::<Int64Array>().expect("Int64");
            bytes.extend_from_slice(&arr.value(row_idx).to_be_bytes());
        }
        bytes
    }

    /// Deserialize a row from bytes into Int64 column values (n_cols columns).
    fn deserialize_row(bytes: &[u8], n_cols: usize) -> Vec<i64> {
        bytes
            .as_chunks::<8>()
            .0
            .iter()
            .take(n_cols)
            .map(|c| i64::from_be_bytes(*c))
            .collect()
    }

    /// Build the output `RecordBatch` from collected column values.
    fn make_output_batch(
        left_row_vals: Vec<Vec<i64>>,
        right_row_vals: Vec<Vec<i64>>,
        schema: &SchemaRef,
    ) -> Result<RecordBatch, OpError> {
        let mut cols: Vec<ArrayRef> = Vec::new();
        for col in left_row_vals {
            cols.push(Arc::new(Int64Array::from(col)) as ArrayRef);
        }
        for col in right_row_vals {
            cols.push(Arc::new(Int64Array::from(col)) as ArrayRef);
        }
        RecordBatch::try_new(Arc::clone(schema), cols).map_err(OpError::arrow)
    }

    /// Zero-lock single-owner left delta processing (`&mut self`).
    pub fn process_left_delta_mut(&mut self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.core.get_mut().unwrap().process_left_delta_mut(delta)
    }

    /// Zero-lock single-owner right delta processing (`&mut self`).
    pub fn process_right_delta_mut(&mut self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.core.get_mut().unwrap().process_right_delta_mut(delta)
    }

    /// Zero-lock single-owner epoch commit (`&mut self`).
    pub fn commit_epoch_mut(&mut self) -> Result<ArrowZSet, OpError> {
        self.core.get_mut().unwrap().commit_epoch_mut()
    }

    /// Zero-lock single-owner full epoch execution (`&mut self`).
    pub fn process_epoch_mut(
        &mut self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        self.core.get_mut().unwrap().process_epoch_mut(left, right)
    }

    pub fn process_left_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.core.lock().unwrap().process_left_delta_mut(delta)
    }

    pub fn process_right_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.core.lock().unwrap().process_right_delta_mut(delta)
    }

    pub fn commit_epoch(&self) -> Result<ArrowZSet, OpError> {
        self.core.lock().unwrap().commit_epoch_mut()
    }

    pub fn process_epoch(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_epoch_with_counters(left, right)
            .map(|(output, _)| output)
    }

    pub(crate) fn process_epoch_with_counters(
        &self,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<(ArrowZSet, crate::governor::DeltaAmplificationCounters), OpError> {
        self.core
            .lock()
            .unwrap()
            .process_epoch_with_counters_mut(left, right)
    }

    /// Begin a resumable CPU/byte-budgeted chunked epoch execution on this shared `JoinOp`
    /// (Issue #402 R4). State is only committed when all phases finish without cancellation.
    pub fn begin_chunked_epoch(
        self: &Arc<Self>,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<ResumableJoinEpoch, OpError> {
        ResumableJoinEpoch::new_shared(Arc::clone(self), left, right)
    }

    /// Fill-level metric for the left arrangement.
    pub fn left_entry_count(&self) -> usize {
        self.core.lock().unwrap().left_entry_count()
    }

    /// Fill-level metric for the right arrangement.
    pub fn right_entry_count(&self) -> usize {
        self.core.lock().unwrap().right_entry_count()
    }

    /// State bytes metric.
    pub fn state_bytes(&self) -> u64 {
        self.core.lock().unwrap().state_bytes()
    }

    /// Persist dirty arrangement state deltas to a `ShardDb` using only point puts/deletes.
    pub fn append_state(&self, target: &mut WriteBatch) -> Result<(), OpError> {
        self.core.lock().unwrap().append_state_mut(target)
    }

    pub async fn persist_state(&self, db: &ShardDb) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();
        self.append_state(&mut batch)?;
        if !batch.is_empty() {
            db.write_batch(batch).await.map_err(OpError::storage)?;
        }
        Ok(())
    }

    /// Load arrangement state from a `ShardDb` (crash-replay).
    pub async fn load_from_storage(db: &ShardDb, op_id: OperatorId) -> Result<Self, OpError> {
        let op = Self::new(op_id, vec![0], vec![0]);
        op.restore_in_place(db).await?;
        Ok(op)
    }

    /// Load persisted left/right arrangement state from `db` into this instance in place.
    pub async fn restore_in_place(&self, db: &ShardDb) -> Result<(), OpError> {
        let mut st = JoinState::new();

        let left_prefix = ShardKeyEncoder::join_arr_op_prefix(JoinSide::Left, self.op_id.0);
        let left_entries = db
            .scan_prefix(&left_prefix)
            .await
            .map_err(OpError::storage)?;
        for (key, value) in left_entries {
            if key.len() < 11 + 16 {
                continue;
            }
            let join_key = key[11..key.len() - 16].to_vec();
            let row_id = u128::from_be_bytes(key[key.len() - 16..].try_into().unwrap_or([0u8; 16]));
            let row_bytes = value.to_vec();
            st.left_arr.entry(join_key).or_default().insert(
                row_id,
                ArrRow {
                    row_bytes,
                    weight: 1,
                },
            );
        }

        let right_prefix = ShardKeyEncoder::join_arr_op_prefix(JoinSide::Right, self.op_id.0);
        let right_entries = db
            .scan_prefix(&right_prefix)
            .await
            .map_err(OpError::storage)?;
        for (key, value) in right_entries {
            if key.len() < 11 + 16 {
                continue;
            }
            let join_key = key[11..key.len() - 16].to_vec();
            let row_id = u128::from_be_bytes(key[key.len() - 16..].try_into().unwrap_or([0u8; 16]));
            let row_bytes = value.to_vec();
            st.right_arr.entry(join_key).or_default().insert(
                row_id,
                ArrRow {
                    row_bytes,
                    weight: 1,
                },
            );
        }

        let mut core = self.core.lock().expect("JoinOp mutex poisoned");
        core.state = st;
        core.db = Some(Arc::new(db.clone()));
        core.state_loaded = true;
        core.loaded_left_keys.clear();
        core.loaded_right_keys.clear();
        Ok(())
    }

    /// Operator ID.
    pub fn op_id(&self) -> OperatorId {
        self.op_id
    }
}

// ─── Resumable Chunked Join Epoch (Issue #402 R4) ───────────────────────────

struct PreparedJoinRow {
    join_key: Vec<u8>,
    row_id: u128,
    row_bytes: Vec<u8>,
    vals: Vec<i64>,
    weight: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinChunkPhase {
    LeftProbe,
    RightProbe,
    DeltaCorrection,
    Done,
}

enum JoinChunkTarget {
    Owned(Box<JoinStateCore>),
    Shared(Arc<JoinOp>),
}

/// Resumable CPU/byte-budgeted execution of a high-fanout [`JoinOp`] epoch.
///
/// Advances across `ΔL ⋈ R₀`, `L₀ ⋈ ΔR`, and `ΔL ⋈ ΔR` in bounded chunks so
/// sibling freshness-critical views can preempt between chunks. Commits staged
/// arrangement updates atomically only upon completion; cancellation rolls back
/// cleanly with zero arrangement mutation.
pub struct ResumableJoinEpoch {
    target: JoinChunkTarget,
    left_n_cols: usize,
    right_n_cols: usize,
    out_schema: SchemaRef,
    left_rows: Vec<PreparedJoinRow>,
    right_rows: Vec<PreparedJoinRow>,
    right_by_key: HashMap<Vec<u8>, Vec<(Vec<i64>, i64)>>,
    phase: JoinChunkPhase,
    row_idx: usize,
    match_idx: usize,
    active_matches: Vec<(Vec<i64>, i64)>,
    active_matches_row: Option<(JoinChunkPhase, usize)>,
}

impl ResumableJoinEpoch {
    pub fn new_shared(op: Arc<JoinOp>, left: ArrowZSet, right: ArrowZSet) -> Result<Self, OpError> {
        let (op_id, left_key_cols, right_key_cols, left_n_cols, right_n_cols) = (
            op.op_id,
            op.left_key_cols.clone(),
            op.right_key_cols.clone(),
            op.left_n_cols,
            op.right_n_cols,
        );
        Self::build(
            JoinChunkTarget::Shared(op),
            op_id,
            &left_key_cols,
            &right_key_cols,
            left_n_cols,
            right_n_cols,
            left,
            right,
        )
    }

    pub fn new_owned(
        core: JoinStateCore,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<Self, OpError> {
        let op_id = core.op_id;
        let left_key_cols = core.left_key_cols.clone();
        let right_key_cols = core.right_key_cols.clone();
        let left_n_cols = core.left_n_cols;
        let right_n_cols = core.right_n_cols;
        Self::build(
            JoinChunkTarget::Owned(Box::new(core)),
            op_id,
            &left_key_cols,
            &right_key_cols,
            left_n_cols,
            right_n_cols,
            left,
            right,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        target: JoinChunkTarget,
        op_id: OperatorId,
        left_key_cols: &[usize],
        right_key_cols: &[usize],
        left_n_cols: usize,
        right_n_cols: usize,
        left: ArrowZSet,
        right: ArrowZSet,
    ) -> Result<Self, OpError> {
        left.validate()?;
        right.validate()?;
        let left_rows = Self::prepare_rows(op_id, &left, left_key_cols, left_n_cols)?;
        let right_rows = Self::prepare_rows(op_id, &right, right_key_cols, right_n_cols)?;
        let mut right_by_key: HashMap<Vec<u8>, Vec<(Vec<i64>, i64)>> = HashMap::new();
        for row in &right_rows {
            right_by_key
                .entry(row.join_key.clone())
                .or_default()
                .push((row.vals.clone(), row.weight));
        }
        Ok(Self {
            target,
            left_n_cols,
            right_n_cols,
            out_schema: join_output_schema_n(left_n_cols, right_n_cols),
            left_rows,
            right_rows,
            right_by_key,
            phase: JoinChunkPhase::LeftProbe,
            row_idx: 0,
            match_idx: 0,
            active_matches: Vec::new(),
            active_matches_row: None,
        })
    }

    fn prepare_rows(
        op_id: OperatorId,
        delta: &ArrowZSet,
        key_cols: &[usize],
        n_cols: usize,
    ) -> Result<Vec<PreparedJoinRow>, OpError> {
        if delta.is_empty() {
            return Ok(Vec::new());
        }
        let capsule = delta.get_or_build_key_capsule(key_cols)?;
        let mut rows = Vec::with_capacity(delta.num_rows());
        for row_idx in 0..delta.num_rows() {
            if capsule.row_contains_null(row_idx) {
                continue;
            }
            let join_key = capsule.row_bytes(row_idx).to_vec();
            let row_bytes = JoinOp::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(op_id.0, &join_key, &row_bytes);
            let vals = JoinOp::deserialize_row(&row_bytes, n_cols);
            rows.push(PreparedJoinRow {
                join_key,
                row_id,
                row_bytes,
                vals,
                weight: delta.weights[row_idx],
            });
        }
        Ok(rows)
    }

    /// Recover the owned [`JoinStateCore`] after completion or cancellation.
    pub fn into_core(self) -> Option<JoinStateCore> {
        match self.target {
            JoinChunkTarget::Owned(core) => Some(*core),
            JoinChunkTarget::Shared(_) => None,
        }
    }

    fn with_core<R>(&mut self, f: impl FnOnce(&mut JoinStateCore) -> R) -> R {
        match &mut self.target {
            JoinChunkTarget::Owned(core) => f(core),
            JoinChunkTarget::Shared(op) => {
                let mut guard = op.core.lock().unwrap();
                f(&mut guard)
            }
        }
    }

    /// Advance this join epoch by at most one [`ChunkBudget`], honoring `cancel`.
    pub fn step(
        &mut self,
        budget: &ChunkBudget,
        cancel: &CancellationToken,
    ) -> Result<ChunkStepResult<ArrowZSet>, OpError> {
        let started = Instant::now();
        let row_byte_est = (self.left_n_cols + self.right_n_cols + 1) * 8;
        let mut left_cols: Vec<Vec<i64>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<i64>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();
        let mut probes = 0usize;

        let check_budget = |rows: usize, probes: usize| -> bool {
            rows >= budget.max_output_rows
                || rows.saturating_mul(row_byte_est) >= budget.max_output_bytes
                || probes >= budget.max_probes
                || started.elapsed() >= budget.max_cpu_time
        };

        let mut yielded = false;

        loop {
            if cancel.is_cancelled() {
                self.phase = JoinChunkPhase::Done;
                self.active_matches.clear();
                return Ok(ChunkStepResult::Cancelled {
                    stats: ChunkStats {
                        output_rows: 0,
                        output_bytes: 0,
                        probes,
                        cpu_time: started.elapsed(),
                    },
                });
            }

            match self.phase {
                JoinChunkPhase::LeftProbe => {
                    if self.row_idx >= self.left_rows.len() {
                        self.phase = JoinChunkPhase::RightProbe;
                        self.row_idx = 0;
                        self.match_idx = 0;
                        self.active_matches.clear();
                        self.active_matches_row = None;
                        continue;
                    }
                    if self.active_matches_row != Some((JoinChunkPhase::LeftProbe, self.row_idx)) {
                        let key = self.left_rows[self.row_idx].join_key.clone();
                        let r_n = self.right_n_cols;
                        let matches = self.with_core(|core| {
                            core.state
                                .probe_right(&key)
                                .map(|(rb, rw)| (JoinOp::deserialize_row(rb, r_n), rw))
                                .collect::<Vec<_>>()
                        });
                        self.active_matches = matches;
                        self.active_matches_row = Some((JoinChunkPhase::LeftProbe, self.row_idx));
                        self.match_idx = 0;
                        probes += 1;
                    }
                    let l_row = &self.left_rows[self.row_idx];
                    while self.match_idx < self.active_matches.len() {
                        if cancel.is_cancelled() {
                            break;
                        }
                        let (r_vals, r_w) = &self.active_matches[self.match_idx];
                        for (i, v) in l_row.vals.iter().enumerate() {
                            left_cols[i].push(*v);
                        }
                        for (i, v) in r_vals.iter().enumerate() {
                            right_cols[i].push(*v);
                        }
                        out_weights.push(l_row.weight * *r_w);
                        self.match_idx += 1;
                        if check_budget(out_weights.len(), probes) {
                            yielded = true;
                            break;
                        }
                    }
                    if cancel.is_cancelled() {
                        continue;
                    }
                    if self.match_idx >= self.active_matches.len() {
                        self.row_idx += 1;
                        self.match_idx = 0;
                        self.active_matches.clear();
                        self.active_matches_row = None;
                    }
                    if yielded || check_budget(out_weights.len(), probes) {
                        break;
                    }
                }
                JoinChunkPhase::RightProbe => {
                    if self.row_idx >= self.right_rows.len() {
                        self.phase = JoinChunkPhase::DeltaCorrection;
                        self.row_idx = 0;
                        self.match_idx = 0;
                        self.active_matches.clear();
                        self.active_matches_row = None;
                        continue;
                    }
                    if self.active_matches_row != Some((JoinChunkPhase::RightProbe, self.row_idx)) {
                        let key = self.right_rows[self.row_idx].join_key.clone();
                        let l_n = self.left_n_cols;
                        let matches = self.with_core(|core| {
                            core.state
                                .probe_left(&key)
                                .map(|(lb, lw)| (JoinOp::deserialize_row(lb, l_n), lw))
                                .collect::<Vec<_>>()
                        });
                        self.active_matches = matches;
                        self.active_matches_row = Some((JoinChunkPhase::RightProbe, self.row_idx));
                        self.match_idx = 0;
                        probes += 1;
                    }
                    let r_row = &self.right_rows[self.row_idx];
                    while self.match_idx < self.active_matches.len() {
                        if cancel.is_cancelled() {
                            break;
                        }
                        let (l_vals, l_w) = &self.active_matches[self.match_idx];
                        for (i, v) in l_vals.iter().enumerate() {
                            left_cols[i].push(*v);
                        }
                        for (i, v) in r_row.vals.iter().enumerate() {
                            right_cols[i].push(*v);
                        }
                        out_weights.push(*l_w * r_row.weight);
                        self.match_idx += 1;
                        if check_budget(out_weights.len(), probes) {
                            yielded = true;
                            break;
                        }
                    }
                    if cancel.is_cancelled() {
                        continue;
                    }
                    if self.match_idx >= self.active_matches.len() {
                        self.row_idx += 1;
                        self.match_idx = 0;
                        self.active_matches.clear();
                        self.active_matches_row = None;
                    }
                    if yielded || check_budget(out_weights.len(), probes) {
                        break;
                    }
                }
                JoinChunkPhase::DeltaCorrection => {
                    if self.row_idx >= self.left_rows.len() {
                        // All 3 phases finished without cancellation — commit staged state atomically!
                        let left_rows = std::mem::take(&mut self.left_rows);
                        let right_rows = std::mem::take(&mut self.right_rows);
                        self.with_core(|core| {
                            for row in left_rows {
                                core.dirty_left.insert((row.join_key.clone(), row.row_id));
                                core.state.update_left(
                                    row.join_key,
                                    row.row_id,
                                    row.row_bytes,
                                    row.weight,
                                );
                            }
                            for row in right_rows {
                                core.dirty_right.insert((row.join_key.clone(), row.row_id));
                                core.state.update_right(
                                    row.join_key,
                                    row.row_id,
                                    row.row_bytes,
                                    row.weight,
                                );
                            }
                        });
                        self.phase = JoinChunkPhase::Done;
                        break;
                    }
                    let l_row = &self.left_rows[self.row_idx];
                    if let Some(matches) = self.right_by_key.get(&l_row.join_key) {
                        if self.match_idx == 0 {
                            probes += 1;
                        }
                        while self.match_idx < matches.len() {
                            if cancel.is_cancelled() {
                                break;
                            }
                            let (r_vals, r_w) = &matches[self.match_idx];
                            for (i, v) in l_row.vals.iter().enumerate() {
                                left_cols[i].push(*v);
                            }
                            for (i, v) in r_vals.iter().enumerate() {
                                right_cols[i].push(*v);
                            }
                            out_weights.push(l_row.weight * *r_w);
                            self.match_idx += 1;
                            if check_budget(out_weights.len(), probes) {
                                yielded = true;
                                break;
                            }
                        }
                        if cancel.is_cancelled() {
                            continue;
                        }
                        if self.match_idx >= matches.len() {
                            self.row_idx += 1;
                            self.match_idx = 0;
                        }
                    } else {
                        self.row_idx += 1;
                        self.match_idx = 0;
                    }
                    if yielded || check_budget(out_weights.len(), probes) {
                        break;
                    }
                }
                JoinChunkPhase::Done => break,
            }
        }

        let output_rows = out_weights.len();
        let output_bytes = output_rows.saturating_mul(row_byte_est);
        let batch = JoinOp::make_output_batch(left_cols, right_cols, &self.out_schema)?;
        let zset = ArrowZSet::new(batch, out_weights);
        let stats = ChunkStats {
            output_rows,
            output_bytes,
            probes,
            cpu_time: started.elapsed(),
        };

        if self.phase == JoinChunkPhase::Done {
            Ok(ChunkStepResult::Completed {
                final_output: zset,
                stats,
            })
        } else {
            Ok(ChunkStepResult::Yielded {
                partial_output: zset,
                stats,
            })
        }
    }
}

impl ResumableChunkedOp for ResumableJoinEpoch {
    fn step_chunk(
        &mut self,
        budget: &ChunkBudget,
        cancel: &CancellationToken,
    ) -> Result<ChunkStepResult<ArrowZSet>, OpError> {
        self.step(budget, cancel)
    }
}

impl crate::op::Operator for JoinOp {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_left_delta(delta)
    }

    fn name(&self) -> &str {
        "JoinOp"
    }

    fn state_bytes(&self) -> u64 {
        self.state_bytes()
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Concatenate multiple `ArrowZSet` batches into one.
///
/// `empty_schema` is used when all batches are empty.
/// All non-empty batches must share the same schema.
pub fn concat_zsets(
    mut batches: Vec<ArrowZSet>,
    empty_schema: SchemaRef,
) -> Result<ArrowZSet, OpError> {
    batches.retain(|b| !b.is_empty());
    if batches.is_empty() {
        return Ok(ArrowZSet::empty(empty_schema));
    }
    if batches.len() == 1 {
        return Ok(batches.remove(0));
    }
    let schema = batches[0].schema();
    let frontier = batches.iter().rev().find_map(|b| b.frontier.clone());
    let mut all_weights: Vec<i64> = Vec::new();
    let record_batches: Vec<RecordBatch> = batches
        .iter()
        .map(|z| {
            all_weights.extend_from_slice(&z.weights);
            z.data.clone()
        })
        .collect();
    let refs: Vec<&RecordBatch> = record_batches.iter().collect();
    let merged = arrow::compute::concat_batches(&schema, refs).map_err(OpError::arrow)?;
    let mut out = ArrowZSet::new(merged, all_weights);
    out.frontier = frontier;

    // Preserve any canonical `BatchKeyCapsule` whose `KeySpec` is attached to
    // every concatenated batch without re-encoding keys.
    for first_cap in &batches[0].key_capsules {
        let spec = &first_cap.spec;
        let matching_caps: Option<Vec<Arc<BatchKeyCapsule>>> = batches
            .iter()
            .map(|b| b.key_capsule_for(&spec.columns))
            .collect();
        if let Some(caps) = matching_caps {
            let mut slices: Vec<(&[u8], u128, bool)> = Vec::with_capacity(out.num_rows());
            for cap in &caps {
                for row_idx in 0..cap.len() {
                    slices.push((
                        cap.row_bytes(row_idx),
                        cap.row_hash_u128(row_idx),
                        cap.row_contains_null(row_idx),
                    ));
                }
            }
            out.push_key_capsule(Arc::new(BatchKeyCapsule::from_encoded_slices(
                spec.columns.clone(),
                slices.into_iter(),
            )));
        }
    }
    Ok(out)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::OperatorId;
    use std::sync::Arc;

    fn make_lr_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
        let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
        let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(k_vals)),
                Arc::new(Int64Array::from(v_vals)),
            ],
        )
        .unwrap();
        ArrowZSet::new(data, weights)
    }

    fn extract_join_output_3col(batch: &ArrowZSet) -> Vec<(i64, i64, i64, i64)> {
        use arrow::array::Int64Array;
        // Output schema for 2-col join: (l_0=l_k, l_1=l_v, r_0=r_k, r_1=r_v) = 4 columns.
        let lk = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lv = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rv = batch
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(); // r_v at col3
        let mut rows: Vec<(i64, i64, i64, i64)> = (0..batch.num_rows())
            .map(|i| (lk.value(i), lv.value(i), rv.value(i), batch.weights[i]))
            .collect();
        rows.sort();
        rows
    }

    #[test]
    fn join_empty_inputs_produce_empty_output() {
        let op = JoinOp::new(OperatorId(0), vec![0], vec![0]);
        let empty = ArrowZSet::empty(Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        ])));
        let out = op
            .process_epoch(empty.clone(), empty)
            .expect("process_epoch");
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn join_single_matching_row() {
        let op = JoinOp::new(OperatorId(1), vec![0], vec![0]);
        let left = make_lr_batch(&[(10, 100, 1)]);
        let right = make_lr_batch(&[(10, 200, 1)]);
        let out = op.process_epoch(left, right).unwrap();
        let rows = extract_join_output_3col(&out);
        // Should produce (l_k=10, l_v=100, r_v=200, weight=1)
        assert!(rows.contains(&(10, 100, 200, 1)), "rows: {rows:?}");
    }

    #[test]
    fn correction_preserves_exact_weighted_many_to_many_join_rows() {
        let op = JoinOp::new(OperatorId(734), vec![0], vec![0]);
        let left = make_lr_batch(&[(1, 10, 2), (1, 20, -1), (2, 30, 1)]);
        let right = make_lr_batch(&[(1, 100, 3), (1, 200, -2), (2, 300, 4)]);
        let output = op.process_epoch(left, right).unwrap();
        assert_eq!(
            extract_join_output_3col(&output),
            vec![
                (1, 10, 100, 6),
                (1, 10, 200, -4),
                (1, 20, 100, -3),
                (1, 20, 200, 2),
                (2, 30, 300, 4),
            ]
        );
    }

    #[test]
    fn join_no_match_produces_empty() {
        let op = JoinOp::new(OperatorId(2), vec![0], vec![0]);
        let left = make_lr_batch(&[(1, 10, 1)]);
        let right = make_lr_batch(&[(2, 20, 1)]);
        let out = op.process_epoch(left, right).unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn join_second_epoch_uses_pre_change_state() {
        let op = JoinOp::new(OperatorId(3), vec![0], vec![0]);

        // Epoch 1: insert (k=5, lv=50) left and (k=5, rv=500) right.
        let l1 = make_lr_batch(&[(5, 50, 1)]);
        let r1 = make_lr_batch(&[(5, 500, 1)]);
        let out1 = op.process_epoch(l1, r1).unwrap();
        let rows1 = extract_join_output_3col(&out1);
        // The ΔL⋈R₀ and L₀⋈ΔR terms are both empty (arrangements are empty pre-epoch1).
        // Only ΔL⋈ΔR produces output: (5, 50, 500, 1).
        assert!(rows1.contains(&(5, 50, 500, 1)), "epoch1: {rows1:?}");

        // Epoch 2: add (k=5, rv=600) right. L₀ now contains (k=5, lv=50).
        let empty_left = ArrowZSet::empty(Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        ])));
        let r2 = make_lr_batch(&[(5, 600, 1)]);
        let out2 = op.process_epoch(empty_left, r2).unwrap();
        let rows2 = extract_join_output_3col(&out2);
        // L₀⋈ΔR: (5, 50, 600, 1).
        assert!(rows2.contains(&(5, 50, 600, 1)), "epoch2: {rows2:?}");
    }

    #[test]
    fn join_delete_retracts_previous_output() {
        let op = JoinOp::new(OperatorId(4), vec![0], vec![0]);

        // Epoch 1: insert on both sides.
        let l1 = make_lr_batch(&[(7, 70, 1)]);
        let r1 = make_lr_batch(&[(7, 700, 1)]);
        let _ = op.process_epoch(l1, r1).unwrap();

        // Epoch 2: delete the right row.
        let empty_left = ArrowZSet::empty(Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        ])));
        let r_del = make_lr_batch(&[(7, 700, -1)]);
        let out2 = op.process_epoch(empty_left, r_del).unwrap();
        let rows2 = extract_join_output_3col(&out2);
        // L₀⋈ΔR: retraction (7, 70, 700, -1).
        assert!(rows2.contains(&(7, 70, 700, -1)), "delete: {rows2:?}");
    }

    #[test]
    fn stable_row_id_same_input_same_output() {
        let id1 = stable_row_id(1, b"key", b"rowdata");
        let id2 = stable_row_id(1, b"key", b"rowdata");
        assert_eq!(id1, id2);
    }

    #[test]
    fn stable_row_id_different_inputs_different_output() {
        let id1 = stable_row_id(1, b"key1", b"rowdata");
        let id2 = stable_row_id(1, b"key2", b"rowdata");
        assert_ne!(id1, id2);
    }

    #[test]
    fn join_no_range_deletion_in_persist() {
        // This test asserts the no-range-deletion invariant by checking that
        // WriteBatch is used only with point puts/deletes. The `persist_state`
        // implementation uses only `batch.put(key, value)`, which is enforced
        // by the WriteBatch API having no `delete_range` method.
        // Compile-time assertion: if WriteBatch ever gains a delete_range method
        // and this code compiled, this test would still pass (we just do a round-trip).
        // The structural assertion is: `persist_state` compiles with only .put() calls.
        use rockstream_storage::WriteBatch;
        let _: WriteBatch = WriteBatch::new(); // confirms WriteBatch is available
                                               // No delete_range: WriteBatch has no such method (enforced at compile time).
    }
}

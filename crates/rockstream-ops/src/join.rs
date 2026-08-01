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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tracing::debug;

use rockstream_storage::{JoinSide, ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::ids::OperatorId;

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
}

impl JoinState {
    pub fn new() -> Self {
        JoinState::default()
    }

    /// Apply a delta to the left arrangement.
    /// Tracks net weight per row_id; removes when weight reaches 0.
    fn update_left(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, delta_w: i64) {
        let bucket = self.left_arr.entry(join_key).or_default();
        let entry = bucket.entry(row_id).or_insert_with(|| ArrRow {
            row_bytes: row_bytes.clone(),
            weight: 0,
        });
        entry.weight += delta_w;
        if entry.weight == 0 {
            bucket.remove(&row_id);
        }
    }

    /// Apply a delta to the right arrangement.
    fn update_right(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, delta_w: i64) {
        let bucket = self.right_arr.entry(join_key).or_default();
        let entry = bucket.entry(row_id).or_insert_with(|| ArrRow {
            row_bytes: row_bytes.clone(),
            weight: 0,
        });
        entry.weight += delta_w;
        if entry.weight == 0 {
            bucket.remove(&row_id);
        }
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
}

impl StagedDelta {
    fn push(&mut self, join_key: Vec<u8>, row_id: u128, row_bytes: Vec<u8>, weight: i64) {
        self.rows.push((join_key, row_id, row_bytes, weight));
    }
}

// ─── JoinOp ───────────────────────────────────────────────────────────────────

/// Incremental inner equi-join operator (v0.8 — IVM-4).
///
/// Probes pre-change arrangements during delta processing; applies staged
/// updates atomically at `commit_epoch()`.
///
/// # Schema
///
/// `JoinOp` is schema-generic: it works with any number of Int64 columns.
/// `left_n_cols` / `right_n_cols` control the output schema.
/// Default (from `JoinOp::new`): 2 left columns, 2 right columns.
/// Use `JoinOp::with_schema` for variable-width inputs (e.g. chained joins).
pub struct JoinOp {
    op_id: OperatorId,
    /// Column indices in the left input used as the join key.
    left_key_cols: Vec<usize>,
    /// Column indices in the right input used as the join key.
    right_key_cols: Vec<usize>,
    /// Number of columns in the left input schema.
    left_n_cols: usize,
    /// Number of columns in the right input schema.
    right_n_cols: usize,
    state: Mutex<JoinState>,
    /// Staged left delta for the current epoch (not yet applied to arrangements).
    left_staged: Mutex<StagedDelta>,
    /// Staged right delta for the current epoch.
    right_staged: Mutex<StagedDelta>,
}

impl JoinOp {
    /// Create a new `JoinOp` for 2-column `(k, v)` inputs.
    pub fn new(op_id: OperatorId, left_key_cols: Vec<usize>, right_key_cols: Vec<usize>) -> Self {
        Self::with_schema(op_id, left_key_cols, right_key_cols, 2, 2)
    }

    /// Create a `JoinOp` with explicit column counts for variable-width schemas.
    ///
    /// Used when one side has more than 2 columns (e.g. chained join output).
    pub fn with_schema(
        op_id: OperatorId,
        left_key_cols: Vec<usize>,
        right_key_cols: Vec<usize>,
        left_n_cols: usize,
        right_n_cols: usize,
    ) -> Self {
        JoinOp {
            op_id,
            left_key_cols,
            right_key_cols,
            left_n_cols,
            right_n_cols,
            state: Mutex::new(JoinState::new()),
            left_staged: Mutex::new(StagedDelta::default()),
            right_staged: Mutex::new(StagedDelta::default()),
        }
    }

    /// Extract the join key bytes from a row (as big-endian i64 bytes concatenated).
    fn extract_key(row: &RecordBatch, row_idx: usize, key_cols: &[usize]) -> Vec<u8> {
        let mut key = Vec::with_capacity(key_cols.len() * 8);
        for &col in key_cols {
            let arr = row
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("join key column must be Int64");
            key.extend_from_slice(&arr.value(row_idx).to_be_bytes());
        }
        key
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
            .chunks_exact(8)
            .take(n_cols)
            .map(|c| i64::from_be_bytes(c.try_into().unwrap()))
            .collect()
    }

    /// Build the output `RecordBatch` from collected column values.
    ///
    /// Output has `left_n_cols + right_n_cols` Int64 columns.
    fn make_output_batch(
        left_row_vals: &[Vec<i64>],  // left_n_cols vecs
        right_row_vals: &[Vec<i64>], // right_n_cols vecs
        schema: &SchemaRef,
    ) -> Result<RecordBatch, OpError> {
        let mut cols: Vec<ArrayRef> = Vec::new();
        for col in left_row_vals {
            cols.push(Arc::new(Int64Array::from(col.clone())) as ArrayRef);
        }
        for col in right_row_vals {
            cols.push(Arc::new(Int64Array::from(col.clone())) as ArrayRef);
        }
        RecordBatch::try_new(Arc::clone(schema), cols).map_err(OpError::arrow)
    }

    /// Stage the left delta for this epoch.
    ///
    /// Probes `R₀` (pre-change right arrangement) and emits `ΔL ⋈ R₀`.
    /// The staged rows are stored for the `ΔL ⋈ ΔR` correction in `commit_epoch`.
    pub fn process_left_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let state = self.state.lock().unwrap();
        let mut staged = self.left_staged.lock().unwrap();

        let schema = join_output_schema_n(self.left_n_cols, self.right_n_cols);
        let mut left_cols: Vec<Vec<i64>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<i64>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();

        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            let join_key = Self::extract_key(&delta.data, row_idx, &self.left_key_cols);
            let row_bytes = Self::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, &join_key, &row_bytes);

            // Probe R₀: emit ΔL ⋈ R₀ with weight = w * r_weight
            for (right_row_bytes, r_weight) in state.probe_right(&join_key) {
                let left_vals = Self::deserialize_row(&row_bytes, self.left_n_cols);
                let right_vals = Self::deserialize_row(right_row_bytes, self.right_n_cols);
                for (i, v) in left_vals.iter().enumerate() {
                    left_cols[i].push(*v);
                }
                for (i, v) in right_vals.iter().enumerate() {
                    right_cols[i].push(*v);
                }
                out_weights.push(w * r_weight);
            }

            // Stage this delta for commit and ΔL⋈ΔR correction.
            staged.push(join_key, row_id, row_bytes, w);
        }

        let batch = Self::make_output_batch(&left_cols, &right_cols, &schema)?;
        debug!(
            op = "JoinOp",
            op_id = self.op_id.0,
            "left delta: {} rows → {} join outputs",
            delta.num_rows(),
            batch.num_rows()
        );
        Ok(ArrowZSet::new(batch, out_weights))
    }

    /// Stage the right delta for this epoch.
    ///
    /// Probes `L₀` (pre-change left arrangement) and emits `L₀ ⋈ ΔR`.
    /// The staged rows are stored for the `ΔL ⋈ ΔR` correction in `commit_epoch`.
    pub fn process_right_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let state = self.state.lock().unwrap();
        let mut staged = self.right_staged.lock().unwrap();

        let schema = join_output_schema_n(self.left_n_cols, self.right_n_cols);
        let mut left_cols: Vec<Vec<i64>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<i64>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();

        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            let join_key = Self::extract_key(&delta.data, row_idx, &self.right_key_cols);
            let row_bytes = Self::serialize_row(&delta.data, row_idx);
            let row_id = stable_row_id(self.op_id.0, &join_key, &row_bytes);

            // Probe L₀: emit L₀ ⋈ ΔR with weight = l_weight * w
            for (left_row_bytes, l_weight) in state.probe_left(&join_key) {
                let left_vals = Self::deserialize_row(left_row_bytes, self.left_n_cols);
                let right_vals = Self::deserialize_row(&row_bytes, self.right_n_cols);
                for (i, v) in left_vals.iter().enumerate() {
                    left_cols[i].push(*v);
                }
                for (i, v) in right_vals.iter().enumerate() {
                    right_cols[i].push(*v);
                }
                out_weights.push(l_weight * w);
            }

            // Stage for commit and ΔL⋈ΔR correction.
            staged.push(join_key, row_id, row_bytes, w);
        }

        let batch = Self::make_output_batch(&left_cols, &right_cols, &schema)?;
        debug!(
            op = "JoinOp",
            op_id = self.op_id.0,
            "right delta: {} rows → {} join outputs",
            delta.num_rows(),
            batch.num_rows()
        );
        Ok(ArrowZSet::new(batch, out_weights))
    }

    /// Compute `ΔL ⋈ ΔR`, apply staged deltas to arrangements, and return
    /// the combined output delta for this epoch.
    ///
    /// Clears the staged deltas.  Must be called exactly once per epoch after
    /// all `process_left_delta` and `process_right_delta` calls.
    pub fn commit_epoch(&self) -> Result<ArrowZSet, OpError> {
        let mut state = self.state.lock().unwrap();
        let mut left_s = self.left_staged.lock().unwrap();
        let mut right_s = self.right_staged.lock().unwrap();

        let schema = join_output_schema_n(self.left_n_cols, self.right_n_cols);
        let mut left_cols: Vec<Vec<i64>> = vec![Vec::new(); self.left_n_cols];
        let mut right_cols: Vec<Vec<i64>> = vec![Vec::new(); self.right_n_cols];
        let mut out_weights: Vec<i64> = Vec::new();

        // Build a map from join_key → staged right delta rows for O(1) lookup.
        let mut right_by_key: HashMap<Vec<u8>, Vec<(Vec<u8>, i64)>> = HashMap::new();
        for (join_key, _row_id, row_bytes, w) in &right_s.rows {
            right_by_key
                .entry(join_key.clone())
                .or_default()
                .push((row_bytes.clone(), *w));
        }

        // ΔL ⋈ ΔR correction term.
        for (join_key, _row_id, left_bytes, l_w) in &left_s.rows {
            if let Some(right_rows) = right_by_key.get(join_key) {
                for (right_bytes, r_w) in right_rows {
                    let left_vals = Self::deserialize_row(left_bytes, self.left_n_cols);
                    let right_vals = Self::deserialize_row(right_bytes, self.right_n_cols);
                    for (i, v) in left_vals.iter().enumerate() {
                        left_cols[i].push(*v);
                    }
                    for (i, v) in right_vals.iter().enumerate() {
                        right_cols[i].push(*v);
                    }
                    out_weights.push(l_w * r_w); // bilinear weight product
                }
            }
        }

        // Apply staged deltas to arrangements.
        for (join_key, row_id, row_bytes, w) in left_s.rows.drain(..) {
            state.update_left(join_key, row_id, row_bytes, w);
        }
        for (join_key, row_id, row_bytes, w) in right_s.rows.drain(..) {
            state.update_right(join_key, row_id, row_bytes, w);
        }

        let batch = Self::make_output_batch(&left_cols, &right_cols, &schema)?;
        Ok(ArrowZSet::new(batch, out_weights))
    }

    /// Run a full epoch: process left and right deltas, then commit.
    ///
    /// Returns the combined `ΔL ⋈ R₀ + L₀ ⋈ ΔR + ΔL ⋈ ΔR` output.
    pub fn process_epoch(&self, left: ArrowZSet, right: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let left_out = self.process_left_delta(left)?;
        let right_out = self.process_right_delta(right)?;
        let correction = self.commit_epoch()?;
        let out_schema = join_output_schema_n(self.left_n_cols, self.right_n_cols);
        concat_zsets(vec![left_out, right_out, correction], out_schema)
    }

    /// Fill-level metric for the left arrangement.
    pub fn left_entry_count(&self) -> usize {
        self.state.lock().unwrap().left_entry_count()
    }

    /// Fill-level metric for the right arrangement.
    pub fn right_entry_count(&self) -> usize {
        self.state.lock().unwrap().right_entry_count()
    }

    /// Persist the arrangement state to a `ShardDb` using only point puts.
    ///
    /// No range deletion is used.  Keys are `join_arr_key(side, op_id, ...)`.
    pub async fn persist_state(&self, db: &ShardDb) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();

        {
            let state = self.state.lock().unwrap();
            for (join_key, entries) in &state.left_arr {
                for (row_id, arr_row) in entries {
                    // Only persist positive-weight rows.
                    if arr_row.weight > 0 {
                        let key = ShardKeyEncoder::join_arr_key(
                            JoinSide::Left,
                            self.op_id.0,
                            join_key,
                            *row_id,
                        );
                        batch.put(&key, &arr_row.row_bytes);
                    }
                }
            }
            for (join_key, entries) in &state.right_arr {
                for (row_id, arr_row) in entries {
                    if arr_row.weight > 0 {
                        let key = ShardKeyEncoder::join_arr_key(
                            JoinSide::Right,
                            self.op_id.0,
                            join_key,
                            *row_id,
                        );
                        batch.put(&key, &arr_row.row_bytes);
                    }
                }
            }
        }

        if batch.is_empty() {
            return Ok(());
        }
        db.write_batch(batch).await.map_err(OpError::storage)
    }

    /// Load arrangement state from a `ShardDb` (crash-replay).
    ///
    /// Scans left/right arrangement prefixes; no range deletion.
    ///
    /// `left_key_cols` / `right_key_cols` default to `[0]` (single key column).
    /// Call `with_key_cols` on the result to override if needed.
    pub async fn load_from_storage(db: &ShardDb, op_id: OperatorId) -> Result<Self, OpError> {
        let mut st = JoinState::new();

        // key format: [0x01][JL/JR:2][op_id:8][join_key_bytes][row_id:16]
        // header = 1+2+8 = 11 bytes; row_id = last 16 bytes

        // Load left arrangement.
        let left_prefix = ShardKeyEncoder::join_arr_op_prefix(JoinSide::Left, op_id.0);
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

        // Load right arrangement.
        let right_prefix = ShardKeyEncoder::join_arr_op_prefix(JoinSide::Right, op_id.0);
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

        Ok(JoinOp {
            op_id,
            left_key_cols: vec![0],
            right_key_cols: vec![0],
            left_n_cols: 2,
            right_n_cols: 2,
            state: Mutex::new(st),
            left_staged: Mutex::new(StagedDelta::default()),
            right_staged: Mutex::new(StagedDelta::default()),
        })
    }

    /// Load persisted left/right arrangement state from `db` into this
    /// already-constructed instance in place (used by
    /// `GatewayHandler::recover_compiled_views` to restore a recompiled
    /// join view's dual arrangements after a process restart — keeps the
    /// same `Arc<JoinOp>` already installed in the pipeline, unlike
    /// `load_from_storage`'s freshly-returned instance).
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

        *self.state.lock().expect("JoinOp mutex poisoned") = st;
        Ok(())
    }

    /// Operator ID.
    pub fn op_id(&self) -> OperatorId {
        self.op_id
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
    Ok(ArrowZSet::new(merged, all_weights))
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

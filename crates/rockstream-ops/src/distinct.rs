//! Distinct / Intersect / Except operators — weight-based, zero-crossing (v0.10 — IVM-6).
//!
//! ## DistinctOp
//!
//! Maintains a `row_key → i64 weight` arrangement. Emits `(row, +1)` when the
//! accumulated weight crosses from `≤0` to `>0`, and `(row, -1)` when it
//! crosses from `>0` to `≤0`.
//!
//! ## IntersectOp
//!
//! Maintains two distinct-style arrangements (one per input side). Computes
//! the output weight as `min(left_weight, right_weight)` (bag) or
//! `min(clamp01(l), clamp01(r))` (set).
//!
//! ## ExceptOp
//!
//! Emits `max(0, left_weight − right_weight)` (bag) or the clamped variant (set).
//!
//! ## Arrangement bound
//!
//! All three operators are bounded by the cardinality of the input relation(s).
//! Fill level = `entry_count()`; backpressure = epoch backpressure from scheduler.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use rockstream_storage::{keys::ShardKeyEncoder, MergeOperatorRegistry, ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::laws::weight_add::WeightAddV1;
use rockstream_types::merge_law::LawBundle;

use crate::error::OpError;
use crate::op::Operator;
use crate::zset::ArrowZSet;

// ─── Row-key helpers ─────────────────────────────────────────────────────────

/// Extract all i64 column values from a RecordBatch row.
///
/// Non-Int64 columns yield 0 (schema must be validated at a higher layer).
fn extract_row_vals(batch: &RecordBatch, row_idx: usize) -> Vec<i64> {
    batch
        .columns()
        .iter()
        .map(|col| {
            col.as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row_idx))
                .unwrap_or(0)
        })
        .collect()
}

/// Serialize row values to a byte key.
fn row_key(vals: &[i64]) -> Vec<u8> {
    let mut key = Vec::with_capacity(vals.len() * 8);
    for v in vals {
        key.extend_from_slice(&v.to_be_bytes());
    }
    key
}

// ─── Output builder ──────────────────────────────────────────────────────────

/// Build an ArrowZSet from a list of (row_values, emit_weight) pairs.
///
/// Aggregates multiple entries for the same row_key by summing their weights.
/// Only rows with a non-zero net weight are emitted.
fn build_output(schema: &SchemaRef, rows: Vec<(Vec<i64>, i64)>) -> Result<ArrowZSet, OpError> {
    if rows.is_empty() {
        return Ok(ArrowZSet::empty(schema.clone()));
    }

    // Aggregate by row key to cancel intra-epoch retractions.
    let mut agg: HashMap<Vec<u8>, (i64, Vec<i64>)> = HashMap::new();
    for (vals, w) in rows {
        let key = row_key(&vals);
        let entry = agg.entry(key).or_insert((0, vals));
        entry.0 += w;
    }

    // Collect non-zero entries into column builders.
    let num_cols = schema.fields().len();
    let mut col_builders: Vec<Vec<i64>> = vec![Vec::new(); num_cols];
    let mut weights: Vec<i64> = Vec::new();

    for (_, (net_w, vals)) in agg {
        if net_w == 0 {
            continue;
        }
        weights.push(net_w);
        for (col_idx, &val) in vals.iter().enumerate().take(num_cols) {
            col_builders[col_idx].push(val);
        }
    }

    if weights.is_empty() {
        return Ok(ArrowZSet::empty(schema.clone()));
    }

    let arrays: Vec<ArrayRef> = col_builders
        .into_iter()
        .map(|col| Arc::new(Int64Array::from(col)) as ArrayRef)
        .collect();

    let data = RecordBatch::try_new(schema.clone(), arrays).map_err(OpError::arrow)?;
    Ok(ArrowZSet::new(data, weights))
}

// ─── DistinctOp ──────────────────────────────────────────────────────────────

/// Persistence mode for [`DistinctOp`] (Issue #402 R2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistinctPersistenceMode {
    /// Legacy full-row `Put`/`Delete` persistence under `[0x01, 'D', 'S']`.
    #[default]
    PutDelete,
    /// ADR 0004 `WeightAdd/v1` law-native `WriteBatch::merge` persistence under
    /// `[0x01, 'D', 'W'][op_id:8][row_bytes] -> [0x04][weight_delta: i64 BE]`.
    WeightMerge,
}

/// Prefix for `WeightAdd/v1` merge-persisted distinct arrangements: `[0x01, 'D', 'W']`.
const DISTINCT_WEIGHT_MERGE_PREFIX: [u8; 3] = [0x01, 0x44, 0x57];

fn distinct_weight_merge_prefix(op_id: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(3 + 8);
    prefix.extend_from_slice(&DISTINCT_WEIGHT_MERGE_PREFIX);
    prefix.extend_from_slice(&op_id.to_be_bytes());
    prefix
}

fn distinct_weight_merge_key(op_id: u64, row_bytes: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(3 + 8 + row_bytes.len());
    key.extend_from_slice(&DISTINCT_WEIGHT_MERGE_PREFIX);
    key.extend_from_slice(&op_id.to_be_bytes());
    key.extend_from_slice(row_bytes);
    key
}

/// In-memory distinct arrangement: `row_key → (accumulated_weight, row_vals)`.
///
/// Only rows with non-zero weight are stored; the arrangement is removed from
/// the map when weight reaches 0.
#[derive(Debug, Default)]
pub struct DistinctState {
    entries: HashMap<Vec<u8>, (i64, Vec<i64>)>,
    dirty: HashMap<Vec<u8>, u64>,
    appended_dirty: HashMap<Vec<u8>, u64>,
    pending_deltas: HashMap<Vec<u8>, i128>,
    appended_deltas: HashMap<Vec<u8>, i128>,
    persisted_weights: HashMap<Vec<u8>, i64>,
    deleted: HashMap<Vec<u8>, u64>,
    appended_deletions: HashMap<Vec<u8>, u64>,
    deletion_generation: u64,
    dirty_generation: u64,
    deleted_bytes: u64,
    appended_deletion_bytes: u64,
    state_bytes: u64,
    mode: DistinctPersistenceMode,
}

impl DistinctState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of live distinct rows (fill level metric).
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// State bytes metric.
    pub fn state_bytes(&self) -> u64 {
        self.state_bytes + self.deleted_bytes + self.appended_deletion_bytes
    }

    fn apply_delta_inner(
        &mut self,
        schema: &SchemaRef,
        delta: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(schema.clone()));
        }

        let mut out_rows: Vec<(Vec<i64>, i64)> = Vec::new();

        for row_idx in 0..delta.num_rows() {
            let delta_w = delta.weights[row_idx];
            if delta_w == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&delta.data, row_idx);
            let key = row_key(&row_vals);

            let old_w = self.entries.get(&key).map(|(w, _)| *w).unwrap_or(0);
            let new_w = old_w
                .checked_add(delta_w)
                .ok_or_else(|| OpError::NumericOverflow {
                    detail: format!("distinct weight overflow ({old_w} + {delta_w})"),
                    code: rockstream_types::error_code::RS_1002,
                })?;
            let is_new = !self.entries.contains_key(&key);
            let entry_bytes = (key.len() + 8 + row_vals.len() * 8) as u64;

            *self.pending_deltas.entry(key.clone()).or_insert(0) += i128::from(delta_w);

            // Maintain arrangement (no range delete — point writes only).
            if new_w != 0 {
                if is_new {
                    self.state_bytes += entry_bytes;
                }
                if self.deleted.remove(&key).is_some() {
                    let bytes = (key.len() + std::mem::size_of::<u64>()) as u64;
                    self.deleted_bytes = self.deleted_bytes.saturating_sub(bytes);
                }
                self.dirty_generation = self.dirty_generation.wrapping_add(1);
                let dirty_gen = self.dirty_generation;
                self.dirty.insert(key.clone(), dirty_gen);
                self.entries.insert(key, (new_w, row_vals.clone()));
            } else if !is_new {
                self.entries.remove(&key);
                self.dirty.remove(&key);
                self.pending_deltas.remove(&key);
                self.deletion_generation = self.deletion_generation.wrapping_add(1);
                let generation = self.deletion_generation;
                let bytes = (key.len() + std::mem::size_of::<u64>()) as u64;
                if self.deleted.insert(key, generation).is_none() {
                    self.deleted_bytes += bytes;
                }
                self.state_bytes = self.state_bytes.saturating_sub(entry_bytes);
            }

            // Zero-crossing emission.
            if old_w <= 0 && new_w > 0 {
                out_rows.push((row_vals, 1));
            } else if old_w > 0 && new_w <= 0 {
                out_rows.push((row_vals, -1));
            }
        }

        build_output(schema, out_rows)
    }
}

/// Distinct deduplication operator (v0.10 — IVM-6).
///
/// # Bound
///
/// Bounded by the cardinality of the input relation.
/// Fill level = `fill_level()`. Backpressure = epoch backpressure from scheduler.
pub struct DistinctOp {
    state: Mutex<DistinctState>,
    schema: SchemaRef,
    fill_level: Arc<AtomicUsize>,
}

impl DistinctOp {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            state: Mutex::new(DistinctState::new()),
            schema,
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a [`DistinctOp`] configured for ADR 0004 `WeightAdd/v1` merge persistence.
    pub fn with_weight_merge(schema: SchemaRef) -> Self {
        Self::new(schema).with_persistence_mode(DistinctPersistenceMode::WeightMerge)
    }

    /// Configure persistence mode (`PutDelete` or `WeightMerge`).
    pub fn with_persistence_mode(self, mode: DistinctPersistenceMode) -> Self {
        self.state.lock().unwrap().mode = mode;
        self
    }

    /// Active persistence mode.
    pub fn persistence_mode(&self) -> DistinctPersistenceMode {
        self.state.lock().unwrap().mode
    }

    /// Current arrangement fill level (distinct row count).
    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    /// State bytes metric.
    pub fn state_bytes(&self) -> u64 {
        self.state.lock().unwrap().state_bytes()
    }

    /// Single-owner zero-lock delta processing (`&mut self`).
    pub fn process_delta_mut(&mut self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let state = self.state.get_mut().unwrap();
        let out = state.apply_delta_inner(&self.schema, delta)?;
        self.fill_level
            .store(state.entry_count(), Ordering::Relaxed);
        Ok(out)
    }

    pub fn ack_persisted_state(&self) {
        let mut state = self.state.lock().unwrap();
        for (key, generation) in std::mem::take(&mut state.appended_deletions) {
            if state.deleted.get(&key) == Some(&generation) {
                state.deleted.remove(&key);
                state.deleted_bytes = state
                    .deleted_bytes
                    .saturating_sub((key.len() + std::mem::size_of::<u64>()) as u64);
                state.persisted_weights.remove(&key);
            }
        }
        for (key, generation) in std::mem::take(&mut state.appended_dirty) {
            if state.dirty.get(&key) == Some(&generation) {
                state.dirty.remove(&key);
            }
            if let Some(w) = state.entries.get(&key).map(|(w, _)| *w) {
                state.persisted_weights.insert(key, w);
            }
        }
        for (key, ack_delta) in std::mem::take(&mut state.appended_deltas) {
            if let Some(pending) = state.pending_deltas.get_mut(&key) {
                *pending -= ack_delta;
                if *pending == 0 {
                    state.pending_deltas.remove(&key);
                }
            }
        }
        state.appended_deletion_bytes = 0;
    }

    pub async fn restore_in_place(&self, db: &ShardDb, op_id: OperatorId) -> Result<(), OpError> {
        let restored = load_distinct_state(db, self.schema.clone(), op_id).await?;
        let mut state = restored.state.into_inner().unwrap();
        let current_mode = self.state.lock().unwrap().mode;
        if state.entries.is_empty() {
            state.mode = current_mode;
        }
        self.fill_level
            .store(state.entry_count(), Ordering::Relaxed);
        *self.state.lock().unwrap() = state;
        Ok(())
    }
}

impl Operator for DistinctOp {
    fn name(&self) -> &str {
        "DistinctOp"
    }

    fn state_bytes(&self) -> u64 {
        self.state_bytes()
    }

    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let mut state = self.state.lock().unwrap();
        let out = state.apply_delta_inner(&self.schema, delta)?;
        self.fill_level
            .store(state.entry_count(), Ordering::Relaxed);
        Ok(out)
    }
}

// ─── IntersectOp ─────────────────────────────────────────────────────────────

/// Dual-arrangement state for Intersect / Except.
///
/// Each side maintains a `row_key → i64 weight` map.
#[derive(Debug, Default)]
pub struct DualArrangement {
    left: HashMap<Vec<u8>, (i64, Vec<i64>)>,
    right: HashMap<Vec<u8>, (i64, Vec<i64>)>,
}

impl DualArrangement {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn left_count(&self) -> usize {
        self.left.len()
    }

    pub fn right_count(&self) -> usize {
        self.right.len()
    }

    fn apply(&mut self, side: Side, key: Vec<u8>, vals: Vec<i64>, delta: i64) -> (i64, i64) {
        let map = match side {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        };
        let old_w = map.get(&key).map(|(w, _)| *w).unwrap_or(0);
        let new_w = old_w + delta;
        if new_w != 0 {
            map.insert(key, (new_w, vals));
        } else {
            map.remove(&key);
        }
        (old_w, new_w)
    }

    fn left_weight(&self, key: &[u8]) -> i64 {
        self.left.get(key).map(|(w, _)| *w).unwrap_or(0)
    }

    fn right_weight(&self, key: &[u8]) -> i64 {
        self.right.get(key).map(|(w, _)| *w).unwrap_or(0)
    }
}

#[derive(Clone, Copy)]
enum Side {
    Left,
    Right,
}

/// Intersect output weight for a row given left and right weights.
fn intersect_weight(lw: i64, rw: i64, all: bool) -> i64 {
    if all {
        lw.max(0).min(rw.max(0))
    } else {
        // Set: clamp both to {0,1}
        let lc = if lw > 0 { 1i64 } else { 0 };
        let rc = if rw > 0 { 1i64 } else { 0 };
        lc.min(rc)
    }
}

/// Except output weight for a row given left and right weights.
fn except_weight(lw: i64, rw: i64, all: bool) -> i64 {
    if all {
        (lw.max(0) - rw.max(0)).max(0)
    } else {
        let lc = if lw > 0 { 1i64 } else { 0 };
        let rc = if rw > 0 { 1i64 } else { 0 };
        (lc - rc).max(0)
    }
}

/// Intersect operator — set or bag semantics (v0.10 — IVM-6).
///
/// # Bound
///
/// Bounded by the cardinality of each input relation (two arrangements).
pub struct IntersectOp {
    state: Mutex<DualArrangement>,
    schema: SchemaRef,
    all: bool,
    fill_level: Arc<AtomicUsize>,
}

impl IntersectOp {
    pub fn new(schema: SchemaRef, all: bool) -> Self {
        Self {
            state: Mutex::new(DualArrangement::new()),
            schema,
            all,
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Current fill level (sum of left and right arrangement sizes).
    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    /// Process one epoch: (left_delta, right_delta) → output delta.
    pub fn process_epoch(
        &self,
        left_delta: ArrowZSet,
        right_delta: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        let mut state = self.state.lock().unwrap();
        let all = self.all;
        let mut out_rows: Vec<(Vec<i64>, i64)> = Vec::new();

        // Process left delta
        for row_idx in 0..left_delta.num_rows() {
            let delta_w = left_delta.weights[row_idx];
            if delta_w == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&left_delta.data, row_idx);
            let key = row_key(&row_vals);
            let rw = state.right_weight(&key);
            let old_out = intersect_weight(state.left_weight(&key), rw, all);
            state.apply(Side::Left, key.clone(), row_vals.clone(), delta_w);
            let new_out = intersect_weight(state.left_weight(&key), rw, all);
            let diff = new_out - old_out;
            if diff != 0 {
                out_rows.push((row_vals, diff));
            }
        }

        // Process right delta
        for row_idx in 0..right_delta.num_rows() {
            let delta_w = right_delta.weights[row_idx];
            if delta_w == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&right_delta.data, row_idx);
            let key = row_key(&row_vals);
            let lw = state.left_weight(&key);
            let old_out = intersect_weight(lw, state.right_weight(&key), all);
            state.apply(Side::Right, key.clone(), row_vals.clone(), delta_w);
            let new_out = intersect_weight(lw, state.right_weight(&key), all);
            let diff = new_out - old_out;
            if diff != 0 {
                out_rows.push((row_vals, diff));
            }
        }

        self.fill_level
            .store(state.left_count() + state.right_count(), Ordering::Relaxed);
        drop(state);

        build_output(&self.schema, out_rows)
    }
}

/// Except operator — set or bag semantics (v0.10 — IVM-6).
///
/// # Bound
///
/// Bounded by the cardinality of each input relation (two arrangements).
pub struct ExceptOp {
    state: Mutex<DualArrangement>,
    schema: SchemaRef,
    all: bool,
    fill_level: Arc<AtomicUsize>,
}

impl ExceptOp {
    pub fn new(schema: SchemaRef, all: bool) -> Self {
        Self {
            state: Mutex::new(DualArrangement::new()),
            schema,
            all,
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Current fill level (sum of left and right arrangement sizes).
    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    /// Process one epoch: (left_delta, right_delta) → output delta.
    pub fn process_epoch(
        &self,
        left_delta: ArrowZSet,
        right_delta: ArrowZSet,
    ) -> Result<ArrowZSet, OpError> {
        let mut state = self.state.lock().unwrap();
        let all = self.all;
        let mut out_rows: Vec<(Vec<i64>, i64)> = Vec::new();

        // Process left delta
        for row_idx in 0..left_delta.num_rows() {
            let delta_w = left_delta.weights[row_idx];
            if delta_w == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&left_delta.data, row_idx);
            let key = row_key(&row_vals);
            let rw = state.right_weight(&key);
            let old_out = except_weight(state.left_weight(&key), rw, all);
            state.apply(Side::Left, key.clone(), row_vals.clone(), delta_w);
            let new_out = except_weight(state.left_weight(&key), rw, all);
            let diff = new_out - old_out;
            if diff != 0 {
                out_rows.push((row_vals, diff));
            }
        }

        // Process right delta
        for row_idx in 0..right_delta.num_rows() {
            let delta_w = right_delta.weights[row_idx];
            if delta_w == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&right_delta.data, row_idx);
            let key = row_key(&row_vals);
            let lw = state.left_weight(&key);
            let old_out = except_weight(lw, state.right_weight(&key), all);
            state.apply(Side::Right, key.clone(), row_vals.clone(), delta_w);
            let new_out = except_weight(lw, state.right_weight(&key), all);
            let diff = new_out - old_out;
            if diff != 0 {
                out_rows.push((row_vals, diff));
            }
        }

        self.fill_level
            .store(state.left_count() + state.right_count(), Ordering::Relaxed);
        drop(state);

        build_output(&self.schema, out_rows)
    }
}

// ─── Storage persistence ─────────────────────────────────────────────────────

/// Compute a 128-bit stable hash of a row key (Vec<u8>).
///
/// Uses two independent FNV-1a 64-bit hashes with different offsets to
/// produce a collision-resistant 128-bit key for storage.
fn row_hash_u128(key_bytes: &[u8]) -> u128 {
    const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;
    const OFFSET_A: u64 = 0xcbf2_9ce4_8422_2325;
    const OFFSET_B: u64 = 0x6c62_272e_07bb_0142;
    let mut h0 = OFFSET_A;
    let mut h1 = OFFSET_B;
    for &b in key_bytes {
        h0 ^= b as u64;
        h0 = h0.wrapping_mul(FNV_PRIME);
        h1 ^= (b ^ 0x5A) as u64;
        h1 = h1.wrapping_mul(FNV_PRIME);
    }
    ((h0 as u128) << 64) | (h1 as u128)
}

/// Serialize row values to the value portion of a distinct storage entry.
///
/// Format: `[weight:8 BE][col0:8 BE][col1:8 BE]...`
fn encode_distinct_value(weight: i64, vals: &[i64]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + vals.len() * 8);
    v.extend_from_slice(&weight.to_be_bytes());
    for &val in vals {
        v.extend_from_slice(&val.to_be_bytes());
    }
    v
}

/// Decode a distinct storage value into (weight, row_vals).
fn decode_distinct_value(bytes: &[u8]) -> Option<(i64, Vec<i64>)> {
    if bytes.len() < 8 || !(bytes.len() - 8).is_multiple_of(8) {
        return None;
    }
    let weight = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let n_cols = (bytes.len() - 8) / 8;
    let mut vals = Vec::with_capacity(n_cols);
    for i in 0..n_cols {
        let start = 8 + i * 8;
        let v = i64::from_be_bytes(bytes[start..start + 8].try_into().ok()?);
        vals.push(v);
    }
    Some((weight, vals))
}

/// Persist the DistinctOp arrangement to a ShardDb.
///
/// Uses a WriteBatch with only point writes/merges/deletes (no range deletion)
/// proportional to changed/deleted keys rather than the full arrangement.
pub fn append_distinct_state(
    op: &DistinctOp,
    op_id: OperatorId,
    target: &mut WriteBatch,
) -> Result<(), OpError> {
    let batch = {
        let mut state = op.state.lock().unwrap();
        let mut batch = WriteBatch::new();

        state.appended_deletions = state.deleted.clone();
        state.appended_deletion_bytes = state.deleted_bytes;
        state.appended_dirty = state.dirty.clone();
        state.appended_deltas = state.pending_deltas.clone();

        match state.mode {
            DistinctPersistenceMode::PutDelete => {
                for row_bytes in state.deleted.keys() {
                    batch.delete(&ShardKeyEncoder::distinct_key(
                        op_id.0,
                        row_hash_u128(row_bytes),
                    ));
                }

                for row_bytes in state.dirty.keys() {
                    if let Some((weight, vals)) = state.entries.get(row_bytes) {
                        if *weight == 0 {
                            continue;
                        }
                        let hash = row_hash_u128(row_bytes);
                        let key = ShardKeyEncoder::distinct_key(op_id.0, hash);
                        let value = encode_distinct_value(*weight, vals);
                        batch.put(&key, &value);
                    }
                }
            }
            DistinctPersistenceMode::WeightMerge => {
                for row_bytes in state.deleted.keys() {
                    batch.delete(&distinct_weight_merge_key(op_id.0, row_bytes));
                }

                for row_bytes in state.dirty.keys() {
                    if let Some((weight, _)) = state.entries.get(row_bytes) {
                        if *weight == 0 {
                            continue;
                        }
                        let key = distinct_weight_merge_key(op_id.0, row_bytes);
                        let delta_i128 = state.pending_deltas.get(row_bytes).copied().unwrap_or(0);
                        let delta_i64 = i64::try_from(delta_i128).ok();
                        let use_merge = match (state.persisted_weights.get(row_bytes), delta_i64) {
                            (Some(&persisted_w), Some(d)) if d != 0 => {
                                WeightAddV1.admits_reassociation(&[persisted_w, d])
                            }
                            _ => false,
                        };
                        if use_merge {
                            let d = delta_i64.expect("checked Some");
                            batch.merge(&key, &MergeOperatorRegistry::encode_pn_counter(d));
                        } else {
                            batch.put(&key, &MergeOperatorRegistry::encode_pn_counter(*weight));
                        }
                    }
                }
            }
        }
        batch
    };

    target.merge_from(batch);
    Ok(())
}

pub async fn persist_distinct_state(
    db: &ShardDb,
    op: &DistinctOp,
    op_id: OperatorId,
) -> Result<(), OpError> {
    let mut batch = WriteBatch::new();
    append_distinct_state(op, op_id, &mut batch)?;
    if !batch.is_empty() {
        db.write_batch(batch).await.map_err(OpError::storage)?;
    }
    op.ack_persisted_state();
    Ok(())
}

/// Load DistinctOp arrangement from a ShardDb, returning a new DistinctOp.
///
/// Scans both legacy `PutDelete` and ADR 0004 `WeightMerge` prefixes for the
/// given operator ID and rebuilds the in-memory arrangement.
pub async fn load_distinct_state(
    db: &ShardDb,
    schema: arrow::datatypes::SchemaRef,
    op_id: OperatorId,
) -> Result<DistinctOp, OpError> {
    let prefix = ShardKeyEncoder::distinct_op_prefix(op_id.0);
    let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;

    let wm_prefix = distinct_weight_merge_prefix(op_id.0);
    let wm_entries = db.scan_prefix(&wm_prefix).await.map_err(OpError::storage)?;
    let header_len = wm_prefix.len();

    let op = DistinctOp::new(schema);
    let count = {
        let mut state = op.state.lock().unwrap();

        for (_, value_bytes) in entries {
            if let Some((weight, vals)) = decode_distinct_value(&value_bytes) {
                if weight != 0 {
                    let row_bytes = row_key(&vals);
                    state.state_bytes += (row_bytes.len() + 8 + vals.len() * 8) as u64;
                    state.persisted_weights.insert(row_bytes.clone(), weight);
                    state.entries.insert(row_bytes, (weight, vals));
                }
            }
        }

        for (key_bytes, value_bytes) in wm_entries {
            if key_bytes.len() <= header_len {
                continue;
            }
            let row_bytes = &key_bytes[header_len..];
            if !row_bytes.len().is_multiple_of(8) {
                continue;
            }
            let weight =
                MergeOperatorRegistry::decode_pn_counter(&value_bytes).ok_or_else(|| {
                    OpError::storage_error("RS-3009: corrupt distinct WeightAdd/v1 record")
                })?;
            state.mode = DistinctPersistenceMode::WeightMerge;
            if weight != 0 {
                let vals: Vec<i64> = row_bytes
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|chunk| i64::from_be_bytes(*chunk))
                    .collect();
                let row_vec = row_bytes.to_vec();
                state.state_bytes += (row_vec.len() + 8 + vals.len() * 8) as u64;
                state.persisted_weights.insert(row_vec.clone(), weight);
                state.entries.insert(row_vec, (weight, vals));
            }
        }

        state.entry_count()
    };
    op.fill_level.store(count, Ordering::Relaxed);
    Ok(op)
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn kv_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn make_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        let schema = kv_schema();
        let k: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
        let v: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
        let w: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(k)) as ArrayRef,
                Arc::new(Int64Array::from(v)) as ArrayRef,
            ],
        )
        .unwrap();
        ArrowZSet::new(data, w)
    }

    fn kv_pairs(zset: &ArrowZSet) -> Vec<((i64, i64), i64)> {
        let mut out = Vec::new();
        zset.validate()
            .expect("distinct output must have aligned row weights");
        if zset.is_empty() {
            return out;
        }
        let k_col = zset
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v_col = zset
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for (i, &weight) in zset.weights.iter().enumerate() {
            out.push(((k_col.value(i), v_col.value(i)), weight));
        }
        out.sort_by_key(|(k, _)| *k);
        out
    }

    // ── DistinctOp ────────────────────────────────────────────────────────

    #[test]
    fn distinct_first_insert_emits_plus1() {
        let op = DistinctOp::new(kv_schema());
        let delta = make_batch(&[(1, 10, 1)]);
        let out = op.process_delta(delta).unwrap();
        let pairs = kv_pairs(&out);
        assert_eq!(pairs, vec![((1, 10), 1)]);
        assert_eq!(op.fill_level(), 1);
    }

    #[test]
    fn distinct_duplicate_insert_no_output() {
        let op = DistinctOp::new(kv_schema());
        op.process_delta(make_batch(&[(1, 10, 1)])).unwrap();
        let out = op.process_delta(make_batch(&[(1, 10, 1)])).unwrap();
        assert!(out.is_empty(), "weight 1→2: no output");
    }

    #[test]
    fn distinct_partial_retract_no_output() {
        let op = DistinctOp::new(kv_schema());
        op.process_delta(make_batch(&[(1, 10, 2)])).unwrap();
        let out = op.process_delta(make_batch(&[(1, 10, -1)])).unwrap();
        assert!(out.is_empty(), "weight 2→1: no output");
    }

    #[test]
    fn distinct_full_retract_emits_minus1() {
        let op = DistinctOp::new(kv_schema());
        op.process_delta(make_batch(&[(1, 10, 1)])).unwrap();
        let out = op.process_delta(make_batch(&[(1, 10, -1)])).unwrap();
        let pairs = kv_pairs(&out);
        assert_eq!(pairs, vec![((1, 10), -1)]);
        assert_eq!(op.fill_level(), 0);
    }

    #[test]
    fn distinct_negative_weight_no_output() {
        let op = DistinctOp::new(kv_schema());
        // Insert with weight -1 (retract before insert) → weight 0 → -1: no output
        let out = op.process_delta(make_batch(&[(1, 10, -1)])).unwrap();
        assert!(out.is_empty(), "negative-only weight: no output");
    }

    #[test]
    fn distinct_fill_level_tracks_entries() {
        let op = DistinctOp::new(kv_schema());
        op.process_delta(make_batch(&[(1, 10, 1), (2, 20, 1)]))
            .unwrap();
        assert_eq!(op.fill_level(), 2);
        op.process_delta(make_batch(&[(1, 10, -1)])).unwrap();
        assert_eq!(op.fill_level(), 1);
    }

    // ── IntersectOp (SET) ─────────────────────────────────────────────────

    #[test]
    fn intersect_set_both_present_emits_plus1() {
        let op = IntersectOp::new(kv_schema(), false);
        let l = make_batch(&[(1, 10, 1)]);
        let r = make_batch(&[(1, 10, 1)]);
        let out = op.process_epoch(l, r).unwrap();
        let pairs = kv_pairs(&out);
        assert_eq!(pairs, vec![((1, 10), 1)]);
    }

    #[test]
    fn intersect_set_only_left_no_output() {
        let op = IntersectOp::new(kv_schema(), false);
        let empty = ArrowZSet::empty(kv_schema());
        op.process_epoch(make_batch(&[(1, 10, 1)]), empty).unwrap();
        assert_eq!(op.fill_level(), 1); // left side has 1 entry
    }

    #[test]
    fn intersect_set_retract_right_emits_minus1() {
        let op = IntersectOp::new(kv_schema(), false);
        let empty = ArrowZSet::empty(kv_schema());
        // Epoch 1: both sides present → output +1
        op.process_epoch(make_batch(&[(1, 10, 1)]), make_batch(&[(1, 10, 1)]))
            .unwrap();
        // Epoch 2: retract right → output -1
        let out = op.process_epoch(empty, make_batch(&[(1, 10, -1)])).unwrap();
        let pairs = kv_pairs(&out);
        assert_eq!(pairs, vec![((1, 10), -1)]);
    }

    // ── IntersectOp (BAG) ─────────────────────────────────────────────────

    #[test]
    fn intersect_bag_weight_is_min() {
        let op = IntersectOp::new(kv_schema(), true);
        let l = make_batch(&[(1, 10, 3)]);
        let r = make_batch(&[(1, 10, 2)]);
        let out = op.process_epoch(l, r).unwrap();
        // output weight = min(3, 2) = 2
        let pairs = kv_pairs(&out);
        assert_eq!(pairs, vec![((1, 10), 2)]);
    }

    // ── ExceptOp (SET) ────────────────────────────────────────────────────

    #[test]
    fn except_set_left_only_emits_plus1() {
        let op = ExceptOp::new(kv_schema(), false);
        let empty = ArrowZSet::empty(kv_schema());
        let out = op.process_epoch(make_batch(&[(1, 10, 1)]), empty).unwrap();
        let pairs = kv_pairs(&out);
        assert_eq!(pairs, vec![((1, 10), 1)]);
    }

    #[test]
    fn except_set_both_present_no_output() {
        let op = ExceptOp::new(kv_schema(), false);
        let out = op
            .process_epoch(make_batch(&[(1, 10, 1)]), make_batch(&[(1, 10, 1)]))
            .unwrap();
        assert!(out.is_empty(), "both present: no output for EXCEPT SET");
    }

    #[test]
    fn except_set_retract_right_restores_output() {
        let op = ExceptOp::new(kv_schema(), false);
        let empty = ArrowZSet::empty(kv_schema());
        // Both present: no output
        op.process_epoch(make_batch(&[(1, 10, 1)]), make_batch(&[(1, 10, 1)]))
            .unwrap();
        // Retract right: output +1
        let out = op
            .process_epoch(empty.clone(), make_batch(&[(1, 10, -1)]))
            .unwrap();
        let pairs = kv_pairs(&out);
        assert_eq!(pairs, vec![((1, 10), 1)]);
    }

    // ── ExceptOp (BAG) ────────────────────────────────────────────────────

    #[test]
    fn except_bag_weight_is_difference() {
        let op = ExceptOp::new(kv_schema(), true);
        let out = op
            .process_epoch(make_batch(&[(1, 10, 5)]), make_batch(&[(1, 10, 2)]))
            .unwrap();
        // output weight = max(0, 5 - 2) = 3
        let pairs = kv_pairs(&out);
        assert_eq!(pairs, vec![((1, 10), 3)]);
    }

    #[test]
    fn except_bag_clamps_at_zero() {
        let op = ExceptOp::new(kv_schema(), true);
        // right > left → 0
        let out = op
            .process_epoch(make_batch(&[(1, 10, 1)]), make_batch(&[(1, 10, 3)]))
            .unwrap();
        assert!(out.is_empty(), "max(0, 1-3)=0: no output");
    }

    // ── Fill level for dual operators ─────────────────────────────────────

    #[test]
    fn intersect_fill_level_sums_both_sides() {
        let op = IntersectOp::new(kv_schema(), false);
        let empty = ArrowZSet::empty(kv_schema());
        op.process_epoch(make_batch(&[(1, 10, 1), (2, 20, 1)]), empty.clone())
            .unwrap();
        op.process_epoch(empty, make_batch(&[(1, 10, 1)])).unwrap();
        assert_eq!(op.fill_level(), 3); // 2 left + 1 right
    }
}

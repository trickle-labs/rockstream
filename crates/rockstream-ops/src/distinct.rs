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

use rockstream_storage::{keys::ShardKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;

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

/// In-memory distinct arrangement: `row_key → (accumulated_weight, row_vals)`.
///
/// Only rows with non-zero weight are stored; the arrangement is removed from
/// the map when weight reaches 0.
#[derive(Debug, Default)]
pub struct DistinctState {
    entries: HashMap<Vec<u8>, (i64, Vec<i64>)>,
}

impl DistinctState {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Number of live distinct rows (fill level metric).
    pub fn entry_count(&self) -> usize {
        self.entries.len()
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

    /// Current arrangement fill level (distinct row count).
    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }
}

impl Operator for DistinctOp {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(self.schema.clone()));
        }

        let mut state = self.state.lock().unwrap();
        let mut out_rows: Vec<(Vec<i64>, i64)> = Vec::new();

        for row_idx in 0..delta.num_rows() {
            let delta_w = delta.weights[row_idx];
            if delta_w == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&delta.data, row_idx);
            let key = row_key(&row_vals);

            let old_w = state.entries.get(&key).map(|(w, _)| *w).unwrap_or(0);
            let new_w = old_w + delta_w;

            // Maintain arrangement (no range delete — point writes only).
            if new_w != 0 {
                state.entries.insert(key, (new_w, row_vals.clone()));
            } else {
                state.entries.remove(&key);
            }

            // Zero-crossing emission.
            if old_w <= 0 && new_w > 0 {
                out_rows.push((row_vals, 1));
            } else if old_w > 0 && new_w <= 0 {
                out_rows.push((row_vals, -1));
            }
        }

        self.fill_level
            .store(state.entry_count(), Ordering::Relaxed);
        drop(state);

        build_output(&self.schema, out_rows)
    }

    fn name(&self) -> &str {
        "DistinctOp"
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
/// Uses a WriteBatch with only point writes (no range deletion).
/// All non-zero entries are written; any previously written entries for
/// rows that now have zero weight must be explicitly deleted.
pub async fn persist_distinct_state(
    db: &ShardDb,
    op: &DistinctOp,
    op_id: OperatorId,
) -> Result<(), OpError> {
    let batch = {
        let state = op.state.lock().unwrap();
        let mut batch = WriteBatch::new();

        for (row_bytes, (weight, vals)) in &state.entries {
            if *weight == 0 {
                continue;
            }
            let hash = row_hash_u128(row_bytes);
            let key = ShardKeyEncoder::distinct_key(op_id.0, hash);
            let value = encode_distinct_value(*weight, vals);
            batch.put(&key, &value);
        }
        batch
    };

    if batch.is_empty() {
        return Ok(());
    }
    db.write_batch(batch).await.map_err(OpError::storage)?;
    Ok(())
}

/// Load DistinctOp arrangement from a ShardDb, returning a new DistinctOp.
///
/// Scans the `distinct_op_prefix` for the given operator ID and rebuilds
/// the in-memory arrangement from the stored entries.
pub async fn load_distinct_state(
    db: &ShardDb,
    schema: arrow::datatypes::SchemaRef,
    op_id: OperatorId,
) -> Result<DistinctOp, OpError> {
    let prefix = ShardKeyEncoder::distinct_op_prefix(op_id.0);
    let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;

    let op = DistinctOp::new(schema);
    let mut state = op.state.lock().unwrap();

    for (_, value_bytes) in entries {
        if let Some((weight, vals)) = decode_distinct_value(&value_bytes) {
            if weight != 0 {
                let row_bytes = row_key(&vals);
                state.entries.insert(row_bytes, (weight, vals));
            }
        }
    }

    let count = state.entry_count();
    drop(state);
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
        for i in 0..zset.num_rows() {
            out.push(((k_col.value(i), v_col.value(i)), zset.weights[i]));
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

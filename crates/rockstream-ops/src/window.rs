//! Window function operator (v0.11 — IVM-7).
//!
//! ## Cost Model
//!
//! Recomputation cost per epoch = Σ_{affected partitions P} O(|P| × log|P|)
//!
//! Only partitions with at least one row in the input delta are recomputed.
//! Unchanged partitions are not touched. This means:
//! - Sparse deltas (few partitions changed): near-constant cost.
//! - Dense deltas (many partitions changed): proportional to total relation size.
//!
//! Bound: WINDOW_PARTITION_THRESHOLD = 10_000 rows.
//! Metric: `fill_level()` = total rows in arrangement; `oversized_partition_keys()`
//! = partitions exceeding the threshold (RS-5023 NOTICE in EXPLAIN).

#![allow(clippy::needless_range_loop)]

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use crate::op::Operator;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use rockstream_plan::{WindowExpr, WindowFunc};
use rockstream_storage::{
    keys::{window_sort_key, ShardKeyEncoder},
    ShardDb, WriteBatch,
};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::zset::ArrowZSet;

// ─── Constants ───────────────────────────────────────────────────────────────

pub const WINDOW_PARTITION_THRESHOLD: usize = 10_000;

pub const RS_5023_NOTICE_FMT: &str =
    "RS-5023: Window partition too large: partition {key_hex} has {size} rows \
     (limit {threshold}). Large partitions increase recomputation cost; \
     consider partitioning differently.";

// ─── Row helpers ──────────────────────────────────────────────────────────────

fn extract_vals(batch: &RecordBatch, row: usize, n_cols: usize) -> Vec<i64> {
    batch
        .columns()
        .iter()
        .take(n_cols)
        .map(|col| {
            col.as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row))
                .unwrap_or(0)
        })
        .collect()
}

fn encode_cols(vals: &[i64]) -> Vec<u8> {
    let mut b = Vec::with_capacity(vals.len() * 8);
    for v in vals {
        b.extend_from_slice(&v.to_be_bytes());
    }
    b
}

fn encode_order_cols(vals: &[i64]) -> Vec<u8> {
    let mut b = Vec::with_capacity(vals.len() * 8);
    for v in vals {
        b.extend_from_slice(&window_sort_key(*v));
    }
    b
}

fn row_hash(vals: &[i64]) -> u128 {
    const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;
    const OFFSET_A: u64 = 0xcbf2_9ce4_8422_2325;
    const OFFSET_B: u64 = 0x6c62_272e_07bb_0142;
    let mut h0 = OFFSET_A;
    let mut h1 = OFFSET_B;
    for v in vals {
        for &b in &v.to_be_bytes() {
            h0 ^= b as u64;
            h0 = h0.wrapping_mul(FNV_PRIME);
            h1 ^= (b ^ 0x5A) as u64;
            h1 = h1.wrapping_mul(FNV_PRIME);
        }
    }
    ((h0 as u128) << 64) | (h1 as u128)
}

fn output_row_hash(output_vals: &[i64]) -> u128 {
    row_hash(output_vals)
}

// ─── Window function evaluation ───────────────────────────────────────────────

/// Evaluate all window expressions over a sorted partition.
///
/// `sorted_rows`: slice of (order_key_bytes, input_vals) in ascending order_key order.
/// Returns one output column per window expr, indexed by row position.
fn eval_window_batch(
    sorted_rows: &[(Vec<u8>, Vec<i64>)],
    window_exprs: &[WindowExpr],
    n_input_cols: usize,
) -> Result<Vec<Vec<i64>>, OpError> {
    let n = sorted_rows.len();
    let mut result = vec![vec![0i64; n]; window_exprs.len()];

    for (expr_idx, expr) in window_exprs.iter().enumerate() {
        let out = &mut result[expr_idx];

        // Extract order_key column values (decoded from the sort key bytes).
        // For ranking: order_key is from expr.order_by encoded as sort-preserving bytes.
        // For value-based funcs: use the first order_by col value from input_vals.
        let order_col_idx = expr.order_by.first().copied().unwrap_or(0);
        let order_vals: Vec<i64> = sorted_rows
            .iter()
            .map(|(_, vals)| {
                if order_col_idx < n_input_cols {
                    vals[order_col_idx]
                } else {
                    0
                }
            })
            .collect();

        match &expr.func {
            WindowFunc::RowNumber => {
                for i in 0..n {
                    out[i] = (i + 1) as i64;
                }
            }

            WindowFunc::Rank => {
                let mut rank = 1i64;
                for i in 0..n {
                    if i > 0 && order_vals[i] != order_vals[i - 1] {
                        rank = (i + 1) as i64;
                    }
                    out[i] = rank;
                }
            }

            WindowFunc::DenseRank => {
                let mut dense = 1i64;
                for i in 0..n {
                    if i > 0 && order_vals[i] != order_vals[i - 1] {
                        dense += 1;
                    }
                    out[i] = dense;
                }
            }

            WindowFunc::Lag { offset } => {
                let lag_col_idx = expr.order_by.first().copied().unwrap_or(0);
                for i in 0..n {
                    if i >= *offset {
                        let src_vals = &sorted_rows[i - offset].1;
                        out[i] = if lag_col_idx < n_input_cols {
                            src_vals[lag_col_idx]
                        } else {
                            0
                        };
                    } else {
                        out[i] = 0;
                    }
                }
            }

            WindowFunc::Lead { offset } => {
                let lead_col_idx = expr.order_by.first().copied().unwrap_or(0);
                for i in 0..n {
                    if i + offset < n {
                        let src_vals = &sorted_rows[i + offset].1;
                        out[i] = if lead_col_idx < n_input_cols {
                            src_vals[lead_col_idx]
                        } else {
                            0
                        };
                    } else {
                        out[i] = 0;
                    }
                }
            }

            WindowFunc::SlidingSum {
                frame_rows,
                value_col,
            } => {
                let val_col = *value_col;
                for i in 0..n {
                    let start = if i + 1 >= *frame_rows {
                        i + 1 - frame_rows
                    } else {
                        0
                    };
                    let mut sum = 0i64;
                    for j in start..=i {
                        let v = &sorted_rows[j].1;
                        sum += if val_col < n_input_cols {
                            v[val_col]
                        } else {
                            0
                        };
                    }
                    out[i] = sum;
                }
            }

            WindowFunc::SlidingAvg {
                frame_rows,
                value_col,
            } => {
                let val_col = *value_col;
                for i in 0..n {
                    let start = if i + 1 >= *frame_rows {
                        i + 1 - frame_rows
                    } else {
                        0
                    };
                    let count = (i - start + 1) as i64;
                    let mut sum = 0i64;
                    for j in start..=i {
                        let v = &sorted_rows[j].1;
                        sum += if val_col < n_input_cols {
                            v[val_col]
                        } else {
                            0
                        };
                    }
                    out[i] = if count > 0 { sum / count } else { 0 };
                }
            }

            WindowFunc::Ntile(_) => {
                return Err(OpError::Unimplemented {
                    feature: "NTILE window function is not supported".to_string(),
                    code: rockstream_types::error_code::RS_1016,
                });
            }
        }
    }

    Ok(result)
}

// ─── State ───────────────────────────────────────────────────────────────────

/// Per-partition arrangement entry: row_id → (order_key_bytes, input_vals, accumulated_weight).
///
/// Negative-weight entries represent over-retractions (rows retracted before insertion).
/// Only rows with weight > 0 participate in window computation.
type PartMap = HashMap<u128, (Vec<u8>, Vec<i64>, i64)>;

/// Output cache entry: output_row_hash → output_vals (input + window results).
type OutputCache = HashMap<u128, Vec<i64>>;

struct WindowState {
    /// Input arrangement: part_key_bytes → row entries
    arrangement: HashMap<Vec<u8>, PartMap>,
    /// Previous output cache: part_key_bytes → output entries
    prev_output: HashMap<Vec<u8>, OutputCache>,
}

impl WindowState {
    fn new() -> Self {
        Self {
            arrangement: HashMap::new(),
            prev_output: HashMap::new(),
        }
    }

    /// Count rows with positive weight (present in the relation).
    fn total_rows(&self) -> usize {
        self.arrangement
            .values()
            .flat_map(|m| m.values())
            .filter(|(_, _, w)| *w > 0)
            .count()
    }

    fn state_bytes(&self) -> u64 {
        let mut bytes = 0u64;
        for (pk, pmap) in &self.arrangement {
            bytes += pk.len() as u64;
            for (order_key, input_vals, _) in pmap.values() {
                bytes += (16 + order_key.len() + input_vals.len() * 8 + 8) as u64;
            }
        }
        for (pk, ocache) in &self.prev_output {
            bytes += pk.len() as u64;
            for output_vals in ocache.values() {
                bytes += (16 + output_vals.len() * 8) as u64;
            }
        }
        bytes
    }
}

// ─── WindowOp ────────────────────────────────────────────────────────────────

/// Window function operator (v0.11 — IVM-7).
///
/// Implements partition-based recomputation: when any row in a partition
/// changes, the entire partition is re-evaluated and the diff is emitted.
pub struct WindowOp {
    /// Output schema: input_schema + one Int64 column per window expr.
    schema: SchemaRef,
    /// Number of input columns (schema.fields().len() - window_exprs.len()).
    n_input_cols: usize,
    window_exprs: Vec<WindowExpr>,
    state: Mutex<WindowState>,
    fill_level: Arc<AtomicUsize>,
    oversized_partitions: Mutex<Vec<Vec<u8>>>,
}

impl WindowOp {
    /// Create an in-memory WindowOp (no LFS persistence).
    pub fn new(schema: SchemaRef, window_exprs: Vec<WindowExpr>) -> Self {
        let n_input_cols = schema.fields().len().saturating_sub(window_exprs.len());
        Self {
            schema,
            n_input_cols,
            window_exprs,
            state: Mutex::new(WindowState::new()),
            fill_level: Arc::new(AtomicUsize::new(0)),
            oversized_partitions: Mutex::new(Vec::new()),
        }
    }

    pub fn state_bytes(&self) -> u64 {
        self.state.lock().unwrap().state_bytes()
    }

    /// Create an LFS-backed WindowOp.
    pub fn new_with_db(
        schema: SchemaRef,
        window_exprs: Vec<WindowExpr>,
        _db: Arc<ShardDb>,
        _op_id: u64,
    ) -> Self {
        let n_input_cols = schema.fields().len().saturating_sub(window_exprs.len());
        Self {
            schema,
            n_input_cols,
            window_exprs,
            state: Mutex::new(WindowState::new()),
            fill_level: Arc::new(AtomicUsize::new(0)),
            oversized_partitions: Mutex::new(Vec::new()),
        }
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    pub fn oversized_partition_keys(&self) -> Vec<Vec<u8>> {
        self.oversized_partitions.lock().unwrap().clone()
    }

    /// Process one epoch: apply `delta` to the arrangement and return the output delta.
    pub fn process_epoch(&self, delta: ArrowZSet, _epoch: u64) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(self.schema.clone()));
        }

        let n_in = self.n_input_cols;
        let n_exprs = self.window_exprs.len();

        // Collect affected partition keys from the delta.
        let mut affected: HashMap<Vec<u8>, ()> = HashMap::new();
        for row_idx in 0..delta.num_rows() {
            let input_vals = extract_vals(&delta.data, row_idx, n_in);
            let part_key = encode_cols(&self.partition_key_vals(&input_vals));
            affected.insert(part_key, ());
        }

        let mut state = self.state.lock().unwrap();

        // Apply delta to arrangement (accumulate weights; track negatives for correctness).
        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            if w == 0 {
                continue;
            }
            let input_vals = extract_vals(&delta.data, row_idx, n_in);
            let part_key = encode_cols(&self.partition_key_vals(&input_vals));
            let order_key = encode_order_cols(&self.order_key_vals(&input_vals));
            let rid = row_hash(&input_vals);

            let part = state.arrangement.entry(part_key).or_default();
            let entry = part.entry(rid).or_insert((order_key, input_vals, 0i64));
            entry.2 += w;
        }

        // Recompute output for each affected partition.
        let mut all_output: Vec<(Vec<i64>, i64)> = Vec::new();
        let mut new_oversized: Vec<Vec<u8>> = Vec::new();

        for part_key in affected.keys() {
            let rows_in_part: Vec<(Vec<u8>, Vec<i64>)> = {
                let part = state.arrangement.get(part_key);
                // Only include rows with positive accumulated weight.
                let mut rows: Vec<(Vec<u8>, Vec<i64>)> = part
                    .map(|m| {
                        m.values()
                            .filter(|(_, _, w)| *w > 0)
                            .map(|(ok, v, _)| (ok.clone(), v.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                rows
            };

            let part_size = rows_in_part.len();
            if part_size > WINDOW_PARTITION_THRESHOLD {
                new_oversized.push(part_key.clone());
            }

            // Retract previous output for this partition.
            if let Some(prev) = state.prev_output.get(part_key) {
                for out_vals in prev.values() {
                    all_output.push((out_vals.clone(), -1));
                }
            }

            // Compute new output.
            if !rows_in_part.is_empty() {
                let window_results = eval_window_batch(&rows_in_part, &self.window_exprs, n_in)?;
                let mut new_prev: OutputCache = HashMap::with_capacity(rows_in_part.len());

                for i in 0..rows_in_part.len() {
                    let mut out_vals = rows_in_part[i].1.clone();
                    for expr_idx in 0..n_exprs {
                        out_vals.push(window_results[expr_idx][i]);
                    }
                    let oh = output_row_hash(&out_vals);
                    new_prev.insert(oh, out_vals.clone());
                    all_output.push((out_vals, 1));
                }

                state.prev_output.insert(part_key.clone(), new_prev);
            } else {
                state.prev_output.remove(part_key);
            }
        }

        let total = state.total_rows();
        drop(state);

        self.fill_level.store(total, Ordering::Relaxed);
        *self.oversized_partitions.lock().unwrap() = new_oversized;

        build_output(&self.schema, all_output)
    }

    fn partition_key_vals(&self, input_vals: &[i64]) -> Vec<i64> {
        let n_in = self.n_input_cols;
        // Use the first window expr's partition_by, or empty if no exprs.
        if let Some(expr) = self.window_exprs.first() {
            expr.partition_by
                .iter()
                .map(|&col| if col < n_in { input_vals[col] } else { 0 })
                .collect()
        } else {
            vec![]
        }
    }

    fn order_key_vals(&self, input_vals: &[i64]) -> Vec<i64> {
        let n_in = self.n_input_cols;
        if let Some(expr) = self.window_exprs.first() {
            expr.order_by
                .iter()
                .map(|&col| if col < n_in { input_vals[col] } else { 0 })
                .collect()
        } else {
            vec![]
        }
    }
}

impl Operator for WindowOp {
    fn name(&self) -> &str {
        "WindowOp"
    }

    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_epoch(delta, 0)
    }

    fn state_bytes(&self) -> u64 {
        self.state_bytes()
    }
}

// ─── Output builder ──────────────────────────────────────────────────────────

fn build_output(schema: &SchemaRef, rows: Vec<(Vec<i64>, i64)>) -> Result<ArrowZSet, OpError> {
    if rows.is_empty() {
        return Ok(ArrowZSet::empty(schema.clone()));
    }

    // Aggregate by output_row_hash to cancel intra-epoch retractions.
    let mut agg: HashMap<u128, (i64, Vec<i64>)> = HashMap::new();
    for (vals, w) in rows {
        let h = output_row_hash(&vals);
        let entry = agg.entry(h).or_insert((0, vals));
        entry.0 += w;
    }

    let num_cols = schema.fields().len();
    let mut col_builders: Vec<Vec<i64>> = vec![Vec::new(); num_cols];
    let mut weights: Vec<i64> = Vec::new();

    for (_, (net_w, vals)) in agg {
        if net_w == 0 {
            continue;
        }
        weights.push(net_w);
        for (ci, &val) in vals.iter().enumerate().take(num_cols) {
            col_builders[ci].push(val);
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

// ─── LFS persistence ─────────────────────────────────────────────────────────

/// Encode input row (order_key_bytes, input_vals, weight) as storage value bytes.
///
/// Format: `[weight:8 BE][order_key_len:8 BE][order_key_bytes][col0:8 BE]...[colN:8 BE]`
fn encode_arr_value(order_key: &[u8], input_vals: &[i64], weight: i64) -> Vec<u8> {
    let oklen = order_key.len() as u64;
    let mut v = Vec::with_capacity(8 + 8 + order_key.len() + input_vals.len() * 8);
    v.extend_from_slice(&weight.to_be_bytes());
    v.extend_from_slice(&oklen.to_be_bytes());
    v.extend_from_slice(order_key);
    for val in input_vals {
        v.extend_from_slice(&val.to_be_bytes());
    }
    v
}

fn decode_arr_value(bytes: &[u8], n_input_cols: usize) -> Option<(Vec<u8>, Vec<i64>, i64)> {
    if bytes.len() < 16 {
        return None;
    }
    let weight = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let oklen = u64::from_be_bytes(bytes[8..16].try_into().ok()?) as usize;
    if bytes.len() < 16 + oklen + n_input_cols * 8 {
        return None;
    }
    let order_key = bytes[16..16 + oklen].to_vec();
    let mut vals = Vec::with_capacity(n_input_cols);
    for i in 0..n_input_cols {
        let off = 16 + oklen + i * 8;
        let v = i64::from_be_bytes(bytes[off..off + 8].try_into().ok()?);
        vals.push(v);
    }
    Some((order_key, vals, weight))
}

/// Encode output row values as storage value bytes.
///
/// Format: `[col0:8 BE]...[colN:8 BE]`
fn encode_output_value(vals: &[i64]) -> Vec<u8> {
    let mut v = Vec::with_capacity(vals.len() * 8);
    for val in vals {
        v.extend_from_slice(&val.to_be_bytes());
    }
    v
}

fn decode_output_value(bytes: &[u8], n_cols: usize) -> Option<Vec<i64>> {
    if bytes.len() < n_cols * 8 {
        return None;
    }
    let mut vals = Vec::with_capacity(n_cols);
    for i in 0..n_cols {
        let v = i64::from_be_bytes(bytes[i * 8..(i + 1) * 8].try_into().ok()?);
        vals.push(v);
    }
    Some(vals)
}

/// Persist WindowOp arrangement and output cache to a ShardDb.
///
/// Uses only point Put/Delete operations — never DeleteRange.
pub fn append_window_state(
    op: &WindowOp,
    op_id: OperatorId,
    target: &mut WriteBatch,
) -> Result<(), OpError> {
    let batch = {
        let state = op.state.lock().unwrap();
        let mut batch = WriteBatch::new();
        let oid = op_id.0;

        // Write arrangement entries (all weights, including zero/negative, for crash-replay).
        for (part_key, part_map) in &state.arrangement {
            for (row_id, (order_key, input_vals, weight)) in part_map {
                let key = ShardKeyEncoder::window_arr_key(oid, part_key, order_key, *row_id);
                let value = encode_arr_value(order_key, input_vals, *weight);
                batch.put(&key, &value);
            }
        }

        // Write output cache entries.
        for (part_key, out_map) in &state.prev_output {
            for (out_hash, out_vals) in out_map {
                let key = ShardKeyEncoder::window_prev_output_key(oid, part_key, *out_hash);
                let value = encode_output_value(out_vals);
                batch.put(&key, &value);
            }
        }
        batch
    };

    target.merge_from(batch);
    Ok(())
}

pub async fn persist_window_state(
    db: &ShardDb,
    op: &WindowOp,
    op_id: OperatorId,
) -> Result<(), OpError> {
    let mut batch = WriteBatch::new();
    append_window_state(op, op_id, &mut batch)?;
    if !batch.is_empty() {
        db.write_batch(batch).await.map_err(OpError::storage)?;
    }
    Ok(())
}

/// Load WindowOp state from a ShardDb.
pub async fn load_window_state(
    db: &ShardDb,
    schema: SchemaRef,
    window_exprs: Vec<WindowExpr>,
    op_id: OperatorId,
) -> Result<WindowOp, OpError> {
    let n_input_cols = schema.fields().len().saturating_sub(window_exprs.len());
    let n_output_cols = schema.fields().len();
    let oid = op_id.0;

    // Load arrangement from OpState prefix before locking state.
    let arr_prefix = ShardKeyEncoder::window_arr_op_prefix(oid);
    let arr_entries = db
        .scan_prefix(&arr_prefix)
        .await
        .map_err(OpError::storage)?;

    // Load output cache from OpIndex prefix before locking state.
    let out_prefix_base = {
        let mut p = Vec::with_capacity(1 + 2 + 8);
        p.push(0x02u8); // OpIndex
        p.extend_from_slice(&[0x57, 0x4E]);
        p.extend_from_slice(&oid.to_be_bytes());
        p
    };
    let out_entries = db
        .scan_prefix(&out_prefix_base)
        .await
        .map_err(OpError::storage)?;

    let op = WindowOp::new(schema, window_exprs);
    let mut state = op.state.lock().unwrap();

    // We need to extract part_key from the stored key.
    // Key format: [0x01][WN][op_id:8][part_key][order_key][row_id:16]
    // We don't know part_key length from the key alone, but the value encodes order_key length.
    // So: decode value → get order_key_len → then extract row_id from last 16 bytes,
    // and part_key = key[11..key.len()-order_key_len-16].
    let prefix_len = arr_prefix.len(); // 1 + 2 + 8 = 11
    for (key, value) in arr_entries {
        if let Some((order_key, input_vals, weight)) = decode_arr_value(&value, n_input_cols) {
            // key = arr_prefix + part_key + order_key + row_id(16)
            if key.len() < prefix_len + order_key.len() + 16 {
                continue;
            }
            let part_key_end = key.len() - order_key.len() - 16;
            let part_key = key[prefix_len..part_key_end].to_vec();
            let row_id_bytes: [u8; 16] = key[key.len() - 16..].try_into().unwrap_or([0; 16]);
            let row_id = u128::from_be_bytes(row_id_bytes);
            state
                .arrangement
                .entry(part_key)
                .or_default()
                .insert(row_id, (order_key, input_vals, weight));
        }
    }

    for (key, value) in out_entries {
        if let Some(out_vals) = decode_output_value(&value, n_output_cols) {
            // key = [0x02][WN][op_id:8][part_key][row_hash:16]
            if key.len() < out_prefix_base.len() + 16 {
                continue;
            }
            let part_key_end = key.len() - 16;
            let part_key = key[out_prefix_base.len()..part_key_end].to_vec();
            let hash_bytes: [u8; 16] = key[key.len() - 16..].try_into().unwrap_or([0; 16]);
            let out_hash = u128::from_be_bytes(hash_bytes);
            state
                .prev_output
                .entry(part_key)
                .or_default()
                .insert(out_hash, out_vals);
        }
    }

    let total = state.total_rows();
    drop(state);
    op.fill_level.store(total, Ordering::Relaxed);
    Ok(op)
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema_kv_rn() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("rn", DataType::Int64, false),
        ]))
    }

    fn schema_kv_result() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("result", DataType::Int64, false),
        ]))
    }

    fn make_batch(schema: SchemaRef, rows: &[(i64, i64, i64)]) -> ArrowZSet {
        let k: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let v: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
        let data = RecordBatch::try_new(
            // Use only input cols (k, v) — weights are separate
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(k)) as ArrayRef,
                Arc::new(Int64Array::from(v)) as ArrayRef,
            ],
        )
        .unwrap();
        let _ = schema; // schema is for the op, not the input batch
        ArrowZSet::new(data, w)
    }

    /// Collect (k, v, window_result) rows from the positive-weight output.
    fn collect_output(zset: &ArrowZSet) -> Vec<(i64, i64, i64)> {
        if zset.is_empty() {
            return vec![];
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
        let r_col = zset
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut rows = vec![];
        for i in 0..zset.num_rows() {
            if zset.weights[i] > 0 {
                rows.push((k_col.value(i), v_col.value(i), r_col.value(i)));
            }
        }
        rows.sort();
        rows
    }

    fn input_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        make_batch(input_schema(), rows)
    }

    fn rn_expr() -> WindowExpr {
        WindowExpr {
            func: WindowFunc::RowNumber,
            partition_by: vec![],
            order_by: vec![1], // order by v
        }
    }

    fn rank_expr() -> WindowExpr {
        WindowExpr {
            func: WindowFunc::Rank,
            partition_by: vec![],
            order_by: vec![1],
        }
    }

    fn dense_rank_expr() -> WindowExpr {
        WindowExpr {
            func: WindowFunc::DenseRank,
            partition_by: vec![],
            order_by: vec![1],
        }
    }

    fn lag_expr(offset: usize) -> WindowExpr {
        WindowExpr {
            func: WindowFunc::Lag { offset },
            partition_by: vec![],
            order_by: vec![1],
        }
    }

    fn lead_expr(offset: usize) -> WindowExpr {
        WindowExpr {
            func: WindowFunc::Lead { offset },
            partition_by: vec![],
            order_by: vec![1],
        }
    }

    fn sliding_sum_expr(frame_rows: usize) -> WindowExpr {
        WindowExpr {
            func: WindowFunc::SlidingSum {
                frame_rows,
                value_col: 1,
            },
            partition_by: vec![],
            order_by: vec![1],
        }
    }

    fn sliding_avg_expr(frame_rows: usize) -> WindowExpr {
        WindowExpr {
            func: WindowFunc::SlidingAvg {
                frame_rows,
                value_col: 1,
            },
            partition_by: vec![],
            order_by: vec![1],
        }
    }

    #[test]
    fn window_row_number_basic() {
        // 5 rows in one partition, ordered by v = 10,20,30,40,50.
        let op = WindowOp::new(schema_kv_rn(), vec![rn_expr()]);
        let delta = make_input(&[(1, 30, 1), (1, 10, 1), (1, 50, 1), (1, 20, 1), (1, 40, 1)]);
        let out = op.process_epoch(delta, 1).unwrap();
        let rows = collect_output(&out);
        // Output should be (k, v, rn). Sort by v to check row numbers.
        let mut by_v: Vec<(i64, i64)> = rows.iter().map(|(_, v, rn)| (*v, *rn)).collect();
        by_v.sort();
        assert_eq!(
            by_v,
            vec![(10, 1), (20, 2), (30, 3), (40, 4), (50, 5)],
            "row numbers should be 1..5 in order of v"
        );
    }

    #[test]
    fn window_rank_with_ties() {
        // 6 rows: v = 10,10,20,20,30,30
        // Rank: 1,1,3,3,5,5 (gap after ties)
        let op = WindowOp::new(schema_kv_rn(), vec![rank_expr()]);
        let delta = make_input(&[
            (1, 10, 1),
            (2, 10, 1),
            (3, 20, 1),
            (4, 20, 1),
            (5, 30, 1),
            (6, 30, 1),
        ]);
        let out = op.process_epoch(delta, 1).unwrap();
        let rows = collect_output(&out);
        let mut by_v: Vec<(i64, i64)> = rows.iter().map(|(_, v, rn)| (*v, *rn)).collect();
        by_v.sort();
        // Rows with same v get same rank; next rank skips.
        assert_eq!(by_v[0].1, by_v[1].1, "tied rows same rank");
        assert_eq!(by_v[2].1, by_v[3].1, "tied rows same rank");
        assert_eq!(by_v[0].1, 1, "first rank=1");
        assert_eq!(by_v[2].1, 3, "second group rank=3");
        assert_eq!(by_v[4].1, 5, "third group rank=5");
    }

    #[test]
    fn window_dense_rank_with_ties() {
        // 6 rows: v = 10,10,20,20,30,30
        // DenseRank: 1,1,2,2,3,3 (no gaps)
        let op = WindowOp::new(schema_kv_rn(), vec![dense_rank_expr()]);
        let delta = make_input(&[
            (1, 10, 1),
            (2, 10, 1),
            (3, 20, 1),
            (4, 20, 1),
            (5, 30, 1),
            (6, 30, 1),
        ]);
        let out = op.process_epoch(delta, 1).unwrap();
        let rows = collect_output(&out);
        let mut by_v: Vec<(i64, i64)> = rows.iter().map(|(_, v, rn)| (*v, *rn)).collect();
        by_v.sort();
        assert_eq!(by_v[0].1, 1);
        assert_eq!(by_v[1].1, 1);
        assert_eq!(by_v[2].1, 2);
        assert_eq!(by_v[3].1, 2);
        assert_eq!(by_v[4].1, 3);
        assert_eq!(by_v[5].1, 3);
    }

    #[test]
    fn window_lag_basic() {
        // 4 rows ordered by v = 10,20,30,40
        // LAG(v, 1): 0,10,20,30 (0 for out-of-bounds)
        let op = WindowOp::new(schema_kv_result(), vec![lag_expr(1)]);
        let delta = make_input(&[(1, 10, 1), (1, 20, 1), (1, 30, 1), (1, 40, 1)]);
        let out = op.process_epoch(delta, 1).unwrap();
        let rows = collect_output(&out);
        let mut by_v: Vec<(i64, i64)> = rows.iter().map(|(_, v, r)| (*v, *r)).collect();
        by_v.sort();
        assert_eq!(by_v, vec![(10, 0), (20, 10), (30, 20), (40, 30)]);
    }

    #[test]
    fn window_lead_basic() {
        // 4 rows ordered by v = 10,20,30,40
        // LEAD(v, 1): 20,30,40,0 (0 for out-of-bounds)
        let op = WindowOp::new(schema_kv_result(), vec![lead_expr(1)]);
        let delta = make_input(&[(1, 10, 1), (1, 20, 1), (1, 30, 1), (1, 40, 1)]);
        let out = op.process_epoch(delta, 1).unwrap();
        let rows = collect_output(&out);
        let mut by_v: Vec<(i64, i64)> = rows.iter().map(|(_, v, r)| (*v, *r)).collect();
        by_v.sort();
        assert_eq!(by_v, vec![(10, 20), (20, 30), (30, 40), (40, 0)]);
    }

    #[test]
    fn window_sliding_sum_basic() {
        // 5 rows ordered by v = 1,2,3,4,5; frame=3
        // SlidingSum: 1, 1+2=3, 1+2+3=6, 2+3+4=9, 3+4+5=12
        let op = WindowOp::new(schema_kv_result(), vec![sliding_sum_expr(3)]);
        let delta = make_input(&[(1, 1, 1), (1, 2, 1), (1, 3, 1), (1, 4, 1), (1, 5, 1)]);
        let out = op.process_epoch(delta, 1).unwrap();
        let rows = collect_output(&out);
        let mut by_v: Vec<(i64, i64)> = rows.iter().map(|(_, v, r)| (*v, *r)).collect();
        by_v.sort();
        assert_eq!(by_v, vec![(1, 1), (2, 3), (3, 6), (4, 9), (5, 12)]);
    }

    #[test]
    fn window_sliding_avg_basic() {
        // 5 rows ordered by v = 10,20,30,40,50; frame=3
        // SlidingAvg: 10/1=10, (10+20)/2=15, (10+20+30)/3=20, (20+30+40)/3=30, (30+40+50)/3=40
        let op = WindowOp::new(schema_kv_result(), vec![sliding_avg_expr(3)]);
        let delta = make_input(&[(1, 10, 1), (1, 20, 1), (1, 30, 1), (1, 40, 1), (1, 50, 1)]);
        let out = op.process_epoch(delta, 1).unwrap();
        let rows = collect_output(&out);
        let mut by_v: Vec<(i64, i64)> = rows.iter().map(|(_, v, r)| (*v, *r)).collect();
        by_v.sort();
        assert_eq!(by_v, vec![(10, 10), (20, 15), (30, 20), (40, 30), (50, 40)]);
    }

    #[test]
    fn window_oversized_partition_sets_fill_level() {
        let op = WindowOp::new(schema_kv_rn(), vec![rn_expr()]);
        // Insert THRESHOLD+1 rows into one partition.
        let n = WINDOW_PARTITION_THRESHOLD + 1;
        let rows: Vec<(i64, i64, i64)> = (0..n as i64).map(|i| (1, i, 1)).collect();
        let k: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let v: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(k)) as ArrayRef,
                Arc::new(Int64Array::from(v)) as ArrayRef,
            ],
        )
        .unwrap();
        let delta = ArrowZSet::new(data, w);
        op.process_epoch(delta, 1).unwrap();
        assert_eq!(op.fill_level(), n, "fill level should equal rows inserted");
        assert_eq!(
            op.oversized_partition_keys().len(),
            1,
            "one oversized partition"
        );
    }

    #[test]
    fn window_partition_recompute_cost_bounded() {
        // Two partitions, only one is touched per epoch.
        // Verify that cost is proportional to the touched partition, not all rows.
        use std::time::Instant;

        let op = WindowOp::new(
            schema_kv_rn(),
            vec![WindowExpr {
                func: WindowFunc::RowNumber,
                partition_by: vec![0], // partition by k
                order_by: vec![1],
            }],
        );

        // Epoch 1: fill partition k=0 with 1000 rows, partition k=1 with 1000 rows.
        let rows_p0: Vec<(i64, i64, i64)> = (0..1000).map(|i| (0, i, 1)).collect();
        let rows_p1: Vec<(i64, i64, i64)> = (0..1000).map(|i| (1, i, 1)).collect();
        let mut all_rows = rows_p0.clone();
        all_rows.extend(rows_p1);
        let input_sc = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let k: Vec<i64> = all_rows.iter().map(|r| r.0).collect();
        let v: Vec<i64> = all_rows.iter().map(|r| r.1).collect();
        let w: Vec<i64> = all_rows.iter().map(|r| r.2).collect();
        let data = RecordBatch::try_new(
            input_sc.clone(),
            vec![
                Arc::new(Int64Array::from(k)) as ArrayRef,
                Arc::new(Int64Array::from(v)) as ArrayRef,
            ],
        )
        .unwrap();
        op.process_epoch(ArrowZSet::new(data, w), 1).unwrap();

        // Epoch 2: touch only partition k=0 with 1 new row.
        let t0 = Instant::now();
        let data2 = RecordBatch::try_new(
            input_sc,
            vec![
                Arc::new(Int64Array::from(vec![0i64])) as ArrayRef,
                Arc::new(Int64Array::from(vec![9999i64])) as ArrayRef,
            ],
        )
        .unwrap();
        op.process_epoch(ArrowZSet::new(data2, vec![1]), 2).unwrap();
        let elapsed = t0.elapsed();

        // Cost bounded: recomputing ~1001-row partition should finish quickly.
        // This is a soft assertion; hard failures only if it takes > 1 second.
        assert!(
            elapsed.as_secs() < 1,
            "partition recompute cost should be bounded: {:?}",
            elapsed
        );
    }
}

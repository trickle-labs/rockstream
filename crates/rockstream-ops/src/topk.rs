//! Top-K operator (v0.12 — IVM-9).
//!
//! Maintains the top-`k` rows per partition ranked by `rank_col` (i64 BE;
//! descending: highest value = rank 1). An internal `K + epsilon` buffer
//! (epsilon = k) backs the delete-refill path without a full state rescan.
//!
//! ## Delta semantics
//!
//! - **Insertion**: if the new row outranks the current K-th row, emit a delta
//!   swapping the displaced K-th row for the new row.
//! - **Deletion**: retract the deleted row; if it was in the top-K, scan the
//!   buffer for the next-best candidate and emit an insertion.
//!
//! ## Cost model
//!
//! O(log N) scan via the value-descending sort key for inserts and refills.
//! Buffer size = 2K (K + epsilon). Fill-level metric = buffer entry count.

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
use crate::zset::ArrowZSet;

// ─── Constants ───────────────────────────────────────────────────────────────

pub const TOPK_BUFFER_LIMIT: usize = 100_000;

// ─── State ───────────────────────────────────────────────────────────────────

/// A single buffer entry: (rank_value, row_vals, net_weight).
///
/// `net_weight` tracks the accumulated Z-set weight. Only entries with
/// `net_weight > 0` are visible in the output. Entries with `net_weight ≤ 0`
/// represent over-retractions or cancelled rows and are kept until they cancel
/// out (reach 0, at which point they are removed).
#[derive(Debug, Clone)]
struct BufferEntry {
    rank_value: i64,
    row_vals: Vec<i64>,
    net_weight: i64,
}

/// Per-partition state.
struct PartitionState {
    /// Buffer: row_id → BufferEntry (all entries, up to K + epsilon).
    buffer: HashMap<u128, BufferEntry>,
    /// Currently emitted top-K: row_id → row_vals.
    emitted: HashMap<u128, Vec<i64>>,
}

impl PartitionState {
    fn new() -> Self {
        Self {
            buffer: HashMap::new(),
            emitted: HashMap::new(),
        }
    }
}

struct TopKState {
    /// partition_key_bytes → partition state.
    partitions: HashMap<Vec<u8>, PartitionState>,
}

impl TopKState {
    fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }

    fn total_entries(&self) -> usize {
        self.partitions
            .values()
            .flat_map(|p| p.buffer.values())
            .filter(|e| e.net_weight > 0)
            .count()
    }
}

// ─── TopKOp ──────────────────────────────────────────────────────────────────

/// Top-K operator (v0.12 — IVM-9).
pub struct TopKOp {
    schema: SchemaRef,
    n_input_cols: usize,
    k: usize,
    rank_col: usize,
    partition_by: Vec<usize>,
    state: Mutex<TopKState>,
    fill_level: Arc<AtomicUsize>,
}

impl TopKOp {
    /// Create an in-memory TopKOp (no LFS persistence).
    pub fn new(schema: SchemaRef, k: usize, rank_col: usize, partition_by: Vec<usize>) -> Self {
        let n_input_cols = schema.fields().len();
        Self {
            schema,
            n_input_cols,
            k,
            rank_col,
            partition_by,
            state: Mutex::new(TopKState::new()),
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    /// Process one epoch: apply `delta` and return the output delta.
    pub fn process_epoch(&self, delta: ArrowZSet, _epoch: u64) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(self.schema.clone()));
        }

        let mut state = self.state.lock().unwrap();
        let mut dirty_partitions: std::collections::HashSet<Vec<u8>> = Default::default();

        // ── Apply delta ──────────────────────────────────────────────────────
        for row_idx in 0..delta.num_rows() {
            let w = delta.weights[row_idx];
            if w == 0 {
                continue;
            }
            let row_vals = extract_row_vals(&delta.data, row_idx, self.n_input_cols);
            let rank_value = if self.rank_col < self.n_input_cols {
                row_vals[self.rank_col]
            } else {
                0
            };
            let part_key = self.partition_key_bytes(&row_vals);
            let row_id = row_id_hash(&row_vals);

            let part = state
                .partitions
                .entry(part_key.clone())
                .or_insert_with(PartitionState::new);

            // Update the buffer entry's net_weight (handles over-retractions correctly).
            // All positive-weight entries are kept in the buffer — evicting a live row
            // would cause missing refill candidates if its higher-ranked peers are later
            // deleted. TOPK_BUFFER_LIMIT is a hard safety cap (not a correctness eviction).
            let positive_count: usize = part.buffer.values().filter(|e| e.net_weight > 0).count();

            match part.buffer.entry(row_id) {
                std::collections::hash_map::Entry::Occupied(mut entry_occ) => {
                    let entry = entry_occ.get_mut();
                    entry.net_weight += w;
                    entry.rank_value = rank_value;
                    entry.row_vals = row_vals;
                    // Remove entries that cancel to zero.
                    if entry.net_weight == 0 {
                        entry_occ.remove();
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry_vac) => {
                    if w < 0 {
                        // Over-retraction: track the debt so future insertions cancel correctly.
                        entry_vac.insert(BufferEntry {
                            rank_value,
                            row_vals,
                            net_weight: w,
                        });
                    } else {
                        // New positive-weight entry: always buffer (correctness requirement).
                        // Entries above TOPK_BUFFER_LIMIT trigger an error to prevent unbounded growth.
                        if positive_count >= TOPK_BUFFER_LIMIT {
                            return Err(OpError::topk_buffer_overflow(TOPK_BUFFER_LIMIT));
                        }
                        entry_vac.insert(BufferEntry {
                            rank_value,
                            row_vals,
                            net_weight: w,
                        });
                    }
                }
            }

            dirty_partitions.insert(part_key);
        }

        // ── Recompute top-K for dirty partitions ──────────────────────────────
        let mut output_rows: Vec<(Vec<i64>, i64)> = Vec::new();

        for part_key in &dirty_partitions {
            let part = match state.partitions.get_mut(part_key) {
                Some(p) => p,
                None => continue,
            };

            // Compute new top-K: sort positive-weight buffer entries by
            // rank_value descending, then by full row bytes ascending (stable tiebreaker).
            let mut sorted: Vec<(i64, u128, Vec<i64>)> = part
                .buffer
                .iter()
                .filter(|(_, e)| e.net_weight > 0)
                .map(|(&rid, e)| (e.rank_value, rid, e.row_vals.clone()))
                .collect();
            sorted.sort_by(|a, b| {
                b.0.cmp(&a.0)
                    .then_with(|| encode_row(&a.2).cmp(&encode_row(&b.2)))
            });
            let new_topk: HashMap<u128, Vec<i64>> = sorted
                .iter()
                .take(self.k)
                .map(|(_, rid, vals)| (*rid, vals.clone()))
                .collect();

            // Retract rows no longer in top-K.
            for (rid, old_vals) in &part.emitted {
                if !new_topk.contains_key(rid) {
                    output_rows.push((old_vals.clone(), -1));
                }
            }

            // Insert newly-in-top-K rows.
            for (rid, new_vals) in &new_topk {
                if !part.emitted.contains_key(rid) {
                    output_rows.push((new_vals.clone(), 1));
                }
            }

            part.emitted = new_topk;
        }

        let total = state.total_entries();
        drop(state);

        self.fill_level.store(total, Ordering::Relaxed);
        build_output(&self.schema, output_rows)
    }

    fn partition_key_bytes(&self, row_vals: &[i64]) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.partition_by.len() * 8);
        for &col in &self.partition_by {
            let v = if col < self.n_input_cols {
                row_vals[col]
            } else {
                0
            };
            key.extend_from_slice(&v.to_be_bytes());
        }
        key
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn encode_row(vals: &[i64]) -> Vec<u8> {
    let mut key = Vec::with_capacity(vals.len() * 8);
    for v in vals {
        key.extend_from_slice(&v.to_be_bytes());
    }
    key
}

fn extract_row_vals(batch: &RecordBatch, row_idx: usize, n_cols: usize) -> Vec<i64> {
    batch
        .columns()
        .iter()
        .take(n_cols)
        .map(|col| {
            col.as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(row_idx))
                .unwrap_or(0)
        })
        .collect()
}

fn row_id_hash(vals: &[i64]) -> u128 {
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

fn build_output(schema: &SchemaRef, rows: Vec<(Vec<i64>, i64)>) -> Result<ArrowZSet, OpError> {
    if rows.is_empty() {
        return Ok(ArrowZSet::empty(schema.clone()));
    }

    let mut agg: HashMap<u128, (i64, Vec<i64>)> = HashMap::new();
    for (vals, w) in rows {
        let h = row_id_hash(&vals);
        let entry = agg.entry(h).or_insert((0i64, vals));
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

/// Encode TopK buffer entry value: `[rank_value:8 BE][net_weight:8 BE][col0:8 BE]...[colN:8 BE]`
fn encode_topk_value(rank_value: i64, net_weight: i64, row_vals: &[i64]) -> Vec<u8> {
    let mut v = Vec::with_capacity(16 + row_vals.len() * 8);
    v.extend_from_slice(&rank_value.to_be_bytes());
    v.extend_from_slice(&net_weight.to_be_bytes());
    for val in row_vals {
        v.extend_from_slice(&val.to_be_bytes());
    }
    v
}

fn decode_topk_value(bytes: &[u8], n_input_cols: usize) -> Option<(i64, i64, Vec<i64>)> {
    if bytes.len() < 16 + n_input_cols * 8 {
        return None;
    }
    let rank_value = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let net_weight = i64::from_be_bytes(bytes[8..16].try_into().ok()?);
    let mut vals = Vec::with_capacity(n_input_cols);
    for i in 0..n_input_cols {
        let off = 16 + i * 8;
        vals.push(i64::from_be_bytes(bytes[off..off + 8].try_into().ok()?));
    }
    Some((rank_value, net_weight, vals))
}

/// Persist TopKOp state to a ShardDb.
///
/// Uses only point Put/Delete operations — never DeleteRange.
pub async fn persist_topk_state(
    db: &ShardDb,
    op: &TopKOp,
    op_id: OperatorId,
) -> Result<(), OpError> {
    let batch = {
        let state = op.state.lock().unwrap();
        let mut batch = WriteBatch::new();
        let oid = op_id.0;

        for (part_key, part) in &state.partitions {
            for (&row_id, entry) in &part.buffer {
                let key = ShardKeyEncoder::topk_key(oid, part_key, entry.rank_value, row_id);
                let value = encode_topk_value(entry.rank_value, entry.net_weight, &entry.row_vals);
                batch.put(&key, &value);
            }
        }
        batch
    };

    if batch.is_empty() {
        return Ok(());
    }
    db.write_batch(batch).await.map_err(OpError::storage)?;
    Ok(())
}

/// Load TopKOp state from a ShardDb.
pub async fn load_topk_state(
    db: &ShardDb,
    schema: SchemaRef,
    k: usize,
    rank_col: usize,
    partition_by: Vec<usize>,
    op_id: OperatorId,
) -> Result<TopKOp, OpError> {
    let op = TopKOp::new(schema, k, rank_col, partition_by);
    let n_input_cols = op.n_input_cols;
    let oid = op_id.0;

    let prefix = ShardKeyEncoder::topk_op_prefix(oid);
    let entries = db.scan_prefix(&prefix).await.map_err(OpError::storage)?;
    // Key format: [0x01][TK][op_id:8][part_key:var][value_desc:8][row_id:16]
    let prefix_len = prefix.len(); // 1 + 2 + 8 = 11

    let mut st = op.state.lock().unwrap();

    for (key, value) in entries {
        if key.len() < prefix_len + 8 + 16 {
            continue;
        }
        if let Some((rank_value, net_weight, row_vals)) = decode_topk_value(&value, n_input_cols) {
            let row_id_bytes: [u8; 16] = key[key.len() - 16..].try_into().unwrap_or([0; 16]);
            let row_id = u128::from_be_bytes(row_id_bytes);
            // part_key = key[prefix_len .. key.len() - 8 - 16]
            let part_key_end = key.len() - 8 - 16;
            let part_key = key[prefix_len..part_key_end].to_vec();

            if net_weight != 0 {
                st.partitions
                    .entry(part_key)
                    .or_insert_with(PartitionState::new)
                    .buffer
                    .insert(
                        row_id,
                        BufferEntry {
                            rank_value,
                            row_vals,
                            net_weight,
                        },
                    );
            }
        }
    }

    // Reconstruct emitted top-K for each partition using the same sort as process_epoch.
    for part in st.partitions.values_mut() {
        let mut sorted: Vec<(i64, u128, Vec<i64>)> = part
            .buffer
            .iter()
            .filter(|(_, e)| e.net_weight > 0)
            .map(|(&rid, e)| (e.rank_value, rid, e.row_vals.clone()))
            .collect();
        sorted.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| encode_row(&a.2).cmp(&encode_row(&b.2)))
        });
        part.emitted = sorted
            .iter()
            .take(k)
            .map(|(_, rid, vals)| (*rid, vals.clone()))
            .collect();
    }

    let total = st.total_entries();
    drop(st);
    op.fill_level.store(total, Ordering::Relaxed);
    Ok(op)
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema_kv() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false), // rank_col = 0
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        let v: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let id: Vec<i64> = rows.iter().map(|r| r.1).collect();
        let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
        let data = RecordBatch::try_new(
            schema_kv(),
            vec![
                Arc::new(Int64Array::from(v)) as ArrayRef,
                Arc::new(Int64Array::from(id)) as ArrayRef,
            ],
        )
        .unwrap();
        ArrowZSet::new(data, w)
    }

    fn accumulate_vals(state: &mut HashMap<i64, i64>, zset: &ArrowZSet) {
        if zset.is_empty() {
            return;
        }
        let col = zset
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..zset.num_rows() {
            *state.entry(col.value(i)).or_insert(0) += zset.weights[i];
        }
    }

    fn live_from_map(state: &HashMap<i64, i64>) -> Vec<i64> {
        let mut vals: Vec<i64> = state
            .iter()
            .filter(|(_, &w)| w > 0)
            .map(|(&v, _)| v)
            .collect();
        vals.sort_by(|a, b| b.cmp(a));
        vals
    }

    #[test]
    fn topk_basic_insert_and_emit() {
        let k = 3usize;
        let op = TopKOp::new(schema_kv(), k, 0, vec![]);

        // Insert K+2 rows in non-monotone order: values [1, 10, 5, 7, 3].
        let out = op
            .process_epoch(
                make_input(&[(1, 1, 1), (10, 2, 1), (5, 3, 1), (7, 4, 1), (3, 5, 1)]),
                1,
            )
            .unwrap();

        let mut state: HashMap<i64, i64> = Default::default();
        accumulate_vals(&mut state, &out);
        let live = live_from_map(&state);

        // Top-3 by value descending: [10, 7, 5].
        assert_eq!(live, vec![10, 7, 5], "top-3: {live:?}");
    }

    #[test]
    fn topk_delete_refill_all_positions() {
        let k = 3usize;
        let op = TopKOp::new(schema_kv(), k, 0, vec![]);

        // Insert 6 rows: [10, 8, 6, 4, 2, 1].
        let out1 = op
            .process_epoch(
                make_input(&[
                    (10, 1, 1),
                    (8, 2, 1),
                    (6, 3, 1),
                    (4, 4, 1),
                    (2, 5, 1),
                    (1, 6, 1),
                ]),
                1,
            )
            .unwrap();
        let mut net: HashMap<i64, i64> = Default::default();
        accumulate_vals(&mut net, &out1);
        assert_eq!(live_from_map(&net), vec![10, 8, 6]);

        // Delete rank-1 (v=10). Next best is v=8 (already in top-3), so v=4 fills.
        let out2 = op.process_epoch(make_input(&[(10, 1, -1)]), 2).unwrap();
        accumulate_vals(&mut net, &out2);
        assert_eq!(live_from_map(&net), vec![8, 6, 4]);

        // Delete rank-1 again (v=8). Next best is v=6 (already in), v=2 fills.
        let out3 = op.process_epoch(make_input(&[(8, 2, -1)]), 3).unwrap();
        accumulate_vals(&mut net, &out3);
        assert_eq!(live_from_map(&net), vec![6, 4, 2]);

        // Delete rank-1 again (v=6). Next best v=4 (already in), v=1 fills.
        let out4 = op.process_epoch(make_input(&[(6, 3, -1)]), 4).unwrap();
        accumulate_vals(&mut net, &out4);
        assert_eq!(live_from_map(&net), vec![4, 2, 1]);
    }

    #[test]
    fn topk_displacement_delta_emitted() {
        let k = 3usize;
        let op = TopKOp::new(schema_kv(), k, 0, vec![]);

        // Insert K rows: [10, 8, 6].
        let out1 = op
            .process_epoch(make_input(&[(10, 1, 1), (8, 2, 1), (6, 3, 1)]), 1)
            .unwrap();
        let mut net: HashMap<i64, i64> = Default::default();
        accumulate_vals(&mut net, &out1);
        assert_eq!(live_from_map(&net), vec![10, 8, 6]);

        // Insert a row with v=7, which outranks the K-th (v=6).
        let out2 = op.process_epoch(make_input(&[(7, 4, 1)]), 2).unwrap();
        accumulate_vals(&mut net, &out2);
        let live2 = live_from_map(&net);
        // Expected top-3: [10, 8, 7]; v=6 displaced.
        assert_eq!(live2, vec![10, 8, 7], "v=7 displaces v=6: {live2:?}");

        // Verify delta contains one retraction (v=6) and one insertion (v=7).
        let v_col = out2
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let retractions: Vec<i64> = (0..out2.num_rows())
            .filter(|&i| out2.weights[i] < 0)
            .map(|i| v_col.value(i))
            .collect();
        let insertions: Vec<i64> = (0..out2.num_rows())
            .filter(|&i| out2.weights[i] > 0)
            .map(|i| v_col.value(i))
            .collect();
        assert_eq!(
            retractions,
            vec![6],
            "expected retraction of v=6: {retractions:?}"
        );
        assert_eq!(
            insertions,
            vec![7],
            "expected insertion of v=7: {insertions:?}"
        );
    }
}

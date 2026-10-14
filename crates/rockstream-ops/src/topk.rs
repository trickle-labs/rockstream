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

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};

use crate::op::Operator;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use rockstream_storage::{keys::ShardKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::zset::ArrowZSet;

// ─── Constants ───────────────────────────────────────────────────────────────

pub const TOPK_BUFFER_LIMIT: usize = 100_000;
static NEXT_SPILL_ID: AtomicU64 = AtomicU64::new(1);

// ─── State ───────────────────────────────────────────────────────────────────

/// A single buffer entry: (rank_value, row_vals, net_weight).
///
/// `net_weight` tracks the accumulated Z-set weight. Only entries with
/// `net_weight > 0` are visible in the output. Entries with `net_weight ≤ 0`
/// represent over-retractions or cancelled rows and are kept until they cancel
/// out (reach 0, at which point they are removed).
#[derive(Debug, Clone, PartialEq, Eq)]
struct BufferEntry {
    rank_value: i64,
    row_vals: Vec<i64>,
    net_weight: i64,
}

/// Per-partition state.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionState {
    /// Buffer: row_id → BufferEntry (all entries, up to K + epsilon).
    buffer: HashMap<u128, BufferEntry>,
    positive_count: usize,
    spilled: HashSet<u128>,
    /// Currently emitted top-K: row_id → (row_vals, emitted multiplicity).
    emitted: HashMap<u128, (Vec<i64>, i64)>,
}

impl PartitionState {
    fn new() -> Self {
        Self {
            buffer: HashMap::new(),
            positive_count: 0,
            spilled: HashSet::new(),
            emitted: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

    fn state_bytes(&self) -> u64 {
        let mut bytes = 0u64;
        for (pk, part) in &self.partitions {
            bytes += pk.len() as u64;
            bytes += (part.spilled.len() * 16) as u64;
            for entry in part.buffer.values() {
                bytes += (32 + entry.row_vals.len() * 8) as u64;
            }
            for (row_vals, _) in part.emitted.values() {
                bytes += (24 + row_vals.len() * 8) as u64;
            }
        }
        bytes
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
    spill_id: u64,
    db: Mutex<Option<Arc<ShardDb>>>,
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
            spill_id: NEXT_SPILL_ID.fetch_add(1, Ordering::Relaxed),
            db: Mutex::new(None),
            state: Mutex::new(TopKState::new()),
            fill_level: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_db(self, db: Arc<ShardDb>) -> Self {
        *self.db.lock().unwrap() = Some(db);
        self
    }

    pub fn set_db(&self, db: Arc<ShardDb>) {
        *self.db.lock().unwrap() = Some(db);
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    pub fn state_bytes(&self) -> u64 {
        self.state.lock().unwrap().state_bytes()
    }

    /// Process one epoch: apply `delta` and return the output delta.
    pub fn process_epoch(&self, delta: ArrowZSet, _epoch: u64) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(self.schema.clone()));
        }

        let mut accepted = self.state.lock().unwrap();
        // Stage only touched partitions; failed reads/writes must not publish an epoch.
        let mut state = TopKState::new();
        let db_guard = self.db.lock().unwrap();
        let mut dirty_partitions: std::collections::HashSet<Vec<u8>> = Default::default();
        let mut spills = HashMap::new();
        let mut writes = WriteBatch::new();
        let mut spilled_bytes = 0;

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

            if !state.partitions.contains_key(&part_key) {
                let part = accepted
                    .partitions
                    .get(&part_key)
                    .cloned()
                    .unwrap_or_else(PartitionState::new);
                spills.insert(
                    part_key.clone(),
                    read_spilled(
                        &part,
                        db_guard.as_ref(),
                        &self.spill_partition_prefix(&part_key),
                        self.n_input_cols,
                    )?,
                );
                state.partitions.insert(part_key.clone(), part);
            }
            let spilled_entries = spills.get_mut(&part_key).unwrap();

            let part = state.partitions.get_mut(&part_key).unwrap();

            if part.spilled.contains(&row_id) {
                let prefix = self.spill_partition_prefix(&part_key);
                let db_key = [prefix.as_slice(), &row_id.to_be_bytes()].concat();
                let old_weight = spilled_entries[&row_id].net_weight;
                let net_weight = old_weight + w;
                if net_weight == 0 {
                    writes.delete(&db_key);
                    spilled_entries.remove(&row_id);
                    part.spilled.remove(&row_id);
                } else {
                    let value = encode_topk_value(rank_value, net_weight, &row_vals);
                    writes.put(&db_key, &value);
                    spilled_entries.insert(
                        row_id,
                        BufferEntry {
                            rank_value,
                            row_vals,
                            net_weight,
                        },
                    );
                }
                dirty_partitions.insert(part_key);
                continue;
            }

            // Update the buffer entry's net_weight (handles over-retractions correctly).
            // All positive-weight entries are kept in the buffer — evicting a live row
            // would cause missing refill candidates if its higher-ranked peers are later
            // deleted. TOPK_BUFFER_LIMIT is a hard safety cap (not a correctness eviction).
            match part.buffer.entry(row_id) {
                std::collections::hash_map::Entry::Occupied(mut entry_occ) => {
                    let entry = entry_occ.get_mut();
                    part.positive_count -= usize::from(entry.net_weight > 0);
                    entry.net_weight += w;
                    part.positive_count += usize::from(entry.net_weight > 0);
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
                        if part.positive_count >= TOPK_BUFFER_LIMIT {
                            if db_guard.is_some() {
                                let prefix = self.spill_partition_prefix(&part_key);
                                let db_key = [prefix.as_slice(), &row_id.to_be_bytes()].concat();
                                let v_bytes = encode_topk_value(rank_value, w, &row_vals);
                                writes.put(&db_key, &v_bytes);
                                spilled_entries.insert(
                                    row_id,
                                    BufferEntry {
                                        rank_value,
                                        row_vals,
                                        net_weight: w,
                                    },
                                );
                                spilled_bytes += (db_key.len() + v_bytes.len()) as u64;
                                part.spilled.insert(row_id);
                            } else {
                                return Err(OpError::topk_buffer_overflow(TOPK_BUFFER_LIMIT));
                            }
                        } else {
                            part.positive_count += 1;
                            entry_vac.insert(BufferEntry {
                                rank_value,
                                row_vals,
                                net_weight: w,
                            });
                        }
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
            // rank_value descending, then by full row bytes ascending (stable
            // tiebreaker). Consume each entry's full positive multiplicity so
            // duplicate input rows occupy duplicate Top-K positions.
            let new_topk = select_topk(part, self.k, &spills[part_key]);

            // Retract rows no longer in top-K.
            for (rid, (old_vals, old_count)) in &part.emitted {
                let new_count = new_topk.get(rid).map_or(0, |(_, count)| *count);
                if *old_count > new_count {
                    output_rows.push((old_vals.clone(), new_count - *old_count));
                }
            }

            // Insert newly-in-top-K rows.
            for (rid, (new_vals, new_count)) in &new_topk {
                let old_count = part.emitted.get(rid).map_or(0, |(_, count)| *count);
                if *new_count > old_count {
                    output_rows.push((new_vals.clone(), *new_count - old_count));
                }
            }

            part.emitted = new_topk;
        }

        let output = build_output(&self.schema, output_rows)?;
        if !writes.is_empty() {
            crate::spill::block_on_future(db_guard.as_ref().unwrap().write_batch(writes))
                .map_err(OpError::storage)?;
        }
        accepted.partitions.extend(state.partitions);
        let total = accepted.total_entries();

        rockstream_types::metrics::inc_spilled_bytes(spilled_bytes);
        self.fill_level.store(total, Ordering::Relaxed);
        Ok(output)
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

    fn spill_partition_prefix(&self, part_key: &[u8]) -> Vec<u8> {
        [
            b"topk_spill:".as_slice(),
            &self.spill_id.to_be_bytes(),
            part_key,
        ]
        .concat()
    }
}

impl Operator for TopKOp {
    fn name(&self) -> &str {
        "TopKOp"
    }

    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.process_epoch(delta, 0)
    }

    fn state_bytes(&self) -> u64 {
        self.state_bytes()
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

fn read_spilled(
    part: &PartitionState,
    db: Option<&Arc<ShardDb>>,
    prefix: &[u8],
    n_input_cols: usize,
) -> Result<HashMap<u128, BufferEntry>, OpError> {
    if part.spilled.is_empty() {
        return Ok(HashMap::new());
    }
    let db = db.ok_or_else(|| OpError::storage_error("topk spill database missing"))?;
    let pairs = crate::spill::block_on_future(db.scan_prefix(prefix)).map_err(OpError::storage)?;
    let mut entries = HashMap::new();
    for (key, value) in pairs {
        if key.len() != prefix.len() + 16 {
            return Err(topk_corruption());
        }
        let row_id = u128::from_be_bytes(key[prefix.len()..].try_into().unwrap());
        if part.spilled.contains(&row_id) {
            let (rank_value, net_weight, row_vals) =
                decode_topk_value(&value, n_input_cols).ok_or_else(topk_corruption)?;
            if row_id_hash(&row_vals) != row_id {
                return Err(topk_corruption());
            }
            rockstream_types::metrics::inc_spill_faults_total();
            entries.insert(
                row_id,
                BufferEntry {
                    rank_value,
                    net_weight,
                    row_vals,
                },
            );
        }
    }
    if entries.keys().copied().collect::<HashSet<_>>() != part.spilled {
        return Err(topk_corruption());
    }
    Ok(entries)
}

fn topk_corruption() -> OpError {
    OpError::storage_error("corrupt topk record")
}

fn select_topk(
    part: &PartitionState,
    k: usize,
    spilled: &HashMap<u128, BufferEntry>,
) -> HashMap<u128, (Vec<i64>, i64)> {
    let mut sorted: Vec<(i64, u128, Vec<i64>, i64)> = part
        .buffer
        .iter()
        .chain(spilled.iter())
        .filter(|(_, entry)| entry.net_weight > 0)
        .map(|(&row_id, entry)| {
            (
                entry.rank_value,
                row_id,
                entry.row_vals.clone(),
                entry.net_weight,
            )
        })
        .collect();
    sorted.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| encode_row(&a.2).cmp(&encode_row(&b.2)))
    });

    let mut remaining = k.min(i64::MAX as usize) as i64;
    let mut selected = HashMap::new();
    for (_, row_id, row_vals, net_weight) in sorted {
        if remaining == 0 {
            break;
        }
        let emitted_count = net_weight.min(remaining);
        selected.insert(row_id, (row_vals, emitted_count));
        remaining -= emitted_count;
    }
    selected
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
    if bytes.len() != 16 + n_input_cols * 8 {
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
pub fn append_topk_state(
    op: &TopKOp,
    op_id: OperatorId,
    target: &mut WriteBatch,
) -> Result<(), OpError> {
    let db = op.db.lock().unwrap().clone();
    append_topk_state_with_db(op, op_id, target, db.as_deref())
}

pub(crate) fn append_topk_state_with_db(
    op: &TopKOp,
    op_id: OperatorId,
    target: &mut WriteBatch,
    db: Option<&ShardDb>,
) -> Result<(), OpError> {
    let batch = {
        let state = op.state.lock().unwrap();
        let mut batch = WriteBatch::new();
        let oid = op_id.0;
        if let Some(db) = db {
            let prefix = ShardKeyEncoder::topk_op_prefix(oid);
            for (key, _) in
                crate::spill::block_on_future(db.scan_prefix(&prefix)).map_err(OpError::storage)?
            {
                batch.delete(&key);
            }
        }
        let spill_db = op.db.lock().unwrap();
        for (part_key, part) in &state.partitions {
            let spilled = read_spilled(
                part,
                spill_db.as_ref(),
                &op.spill_partition_prefix(part_key),
                op.n_input_cols,
            )?;
            for (&row_id, entry) in part.buffer.iter().chain(spilled.iter()) {
                let key = ShardKeyEncoder::topk_key(oid, part_key, entry.rank_value, row_id);
                let value = encode_topk_value(entry.rank_value, entry.net_weight, &entry.row_vals);
                batch.put(&key, &value);
            }
        }
        batch
    };

    target.merge_from(batch);
    Ok(())
}

pub async fn persist_topk_state(
    db: &ShardDb,
    op: &TopKOp,
    op_id: OperatorId,
) -> Result<(), OpError> {
    let mut batch = WriteBatch::new();
    append_topk_state_with_db(op, op_id, &mut batch, Some(db))?;
    if !batch.is_empty() {
        db.write_batch(batch).await.map_err(OpError::storage)?;
    }
    Ok(())
}

/// Load TopKOp state from a ShardDb, including checkpointed spill rows.
// shortcut: recovery materializes the checkpoint in memory; stream into spills for bounded recovery.
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

    let mut st = TopKState::new();
    for (key, value) in entries {
        if key.len() != prefix_len + op.partition_by.len() * 8 + 8 + 16 {
            return Err(topk_corruption());
        }
        let (rank_value, net_weight, row_vals) =
            decode_topk_value(&value, n_input_cols).ok_or_else(topk_corruption)?;
        let row_id = u128::from_be_bytes(key[key.len() - 16..].try_into().unwrap());
        let part_key = op.partition_key_bytes(&row_vals);
        if key.as_ref() != ShardKeyEncoder::topk_key(oid, &part_key, rank_value, row_id)
            || row_id_hash(&row_vals) != row_id
        {
            return Err(topk_corruption());
        }
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
    for part in st.partitions.values_mut() {
        part.positive_count = part.buffer.values().filter(|e| e.net_weight > 0).count();
        part.emitted = select_topk(part, k, &HashMap::new());
    }
    let total = st.total_entries();
    *op.state.lock().unwrap() = st;
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

    // #114 R4: scan/write/corruption failures preserve every accepted partition and spill.
    #[tokio::test(flavor = "multi_thread")]
    async fn topk_storage_failures_leave_full_state_unchanged() {
        let dir = tempfile::TempDir::new().unwrap();
        let store =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(
            ShardDb::builder("topk-failures", store)
                .build()
                .await
                .unwrap(),
        );
        let op = TopKOp::new(schema_kv(), 2, 0, vec![1]).with_db(db.clone());
        let mut rows = (1..=100001).map(|rank| (rank, 0, 1)).collect::<Vec<_>>();
        rows.push((7, 1, 1));
        op.process_epoch(make_input(&rows), 1).unwrap();
        let accepted = op.state.lock().unwrap().clone();
        let fill = op.fill_level();
        let bytes = op.state_bytes();
        let persisted = db.scan_prefix(b"topk_spill:").await.unwrap();
        assert_eq!(
            accepted.partitions[&0_i64.to_be_bytes().to_vec()].spilled,
            HashSet::from([row_id_hash(&[100001, 0])])
        );

        db.set_fail_scans(true);
        let result = op.process_epoch(make_input(&[(7, 1, -1), (100001, 0, 1)]), 2);
        assert!(matches!(result, Err(OpError::Storage {
            source: rockstream_storage::StorageError::Unsupported(ref msg), ..
        }) if msg == "injected storage scan failure"));
        assert_eq!(*op.state.lock().unwrap(), accepted);
        assert_eq!(op.fill_level(), fill);
        assert_eq!(op.state_bytes(), bytes);
        db.set_fail_scans(false);
        assert_eq!(db.scan_prefix(b"topk_spill:").await.unwrap(), persisted);

        db.set_fail_writes(true);
        let result = op.process_epoch(make_input(&[(7, 1, -1), (100001, 0, 1)]), 2);
        assert!(matches!(result, Err(OpError::Storage {
            source: rockstream_storage::StorageError::Unsupported(ref msg), ..
        }) if msg == "injected storage write failure"));
        assert_eq!(*op.state.lock().unwrap(), accepted);
        assert_eq!(op.fill_level(), fill);
        assert_eq!(op.state_bytes(), bytes);
        db.set_fail_writes(false);
        assert_eq!(db.scan_prefix(b"topk_spill:").await.unwrap(), persisted);

        let (key, value) = &persisted[0];
        db.put(key, &value[..value.len() - 1]).await.unwrap();
        let corrupted = db.scan_prefix(b"topk_spill:").await.unwrap();
        let result = op.process_epoch(make_input(&[(7, 1, -1), (100001, 0, 1)]), 2);
        assert!(matches!(result, Err(OpError::Storage {
            source: rockstream_storage::StorageError::KeyEncoding(ref msg), ..
        }) if msg == "corrupt topk record"));
        assert_eq!(*op.state.lock().unwrap(), accepted);
        assert_eq!(op.fill_level(), fill);
        assert_eq!(op.state_bytes(), bytes);
        assert_eq!(db.scan_prefix(b"topk_spill:").await.unwrap(), corrupted);

        db.put(key, value).await.unwrap();
        let out = op
            .process_epoch(make_input(&[(7, 1, -1), (100001, 0, 1)]), 2)
            .unwrap();
        assert_eq!(out.data.schema(), schema_kv());
        let mut expected = HashMap::new();
        for i in 0..out.num_rows() {
            let vals = extract_row_vals(&out.data, i, 2);
            expected.insert(vals, out.weights[i]);
        }
        assert_eq!(
            expected,
            HashMap::from([
                (vec![7, 1], -1),
                (vec![100000, 0], -1),
                (vec![100001, 0], 1),
            ])
        );
        drop(op);
        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }

    // #114 R5: valid records on both sides of a truncated record cannot yield a partial operator.
    #[tokio::test(flavor = "multi_thread")]
    async fn topk_truncated_checkpoint_returns_corruption() {
        let dir = tempfile::TempDir::new().unwrap();
        let store =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = ShardDb::builder("topk-corruption", store)
            .build()
            .await
            .unwrap();
        let op = TopKOp::new(schema_kv(), 2, 0, vec![]);
        op.process_epoch(make_input(&[(1, 1, 1), (2, 2, 1), (3, 3, 1)]), 1)
            .unwrap();
        persist_topk_state(&db, &op, OperatorId(114)).await.unwrap();
        let prefix = ShardKeyEncoder::topk_op_prefix(114);
        let records = db.scan_prefix(&prefix).await.unwrap();
        assert_eq!(
            records.iter().map(|(_, v)| v.to_vec()).collect::<Vec<_>>(),
            (1_i64..=3)
                .rev()
                .map(|v| encode_topk_value(v, 1, &[v, v]))
                .collect::<Vec<_>>()
        );
        let (key, value) = &records[1];
        db.put(key, &value[..value.len() - 1]).await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();
        let store =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = ShardDb::builder("topk-corruption", store)
            .build()
            .await
            .unwrap();
        let before = db.scan_prefix(&prefix).await.unwrap();
        let result = load_topk_state(&db, schema_kv(), 2, 0, vec![], OperatorId(114)).await;
        assert!(matches!(result, Err(OpError::Storage {
            source: rockstream_storage::StorageError::KeyEncoding(ref msg), ..
        }) if msg == "corrupt topk record"));
        assert_eq!(db.scan_prefix(&prefix).await.unwrap(), before);
        db.put(key, value).await.unwrap();
        let recovered = load_topk_state(&db, schema_kv(), 2, 0, vec![], OperatorId(114))
            .await
            .unwrap();
        assert_eq!(*recovered.state.lock().unwrap(), *op.state.lock().unwrap());
        db.close().await.unwrap();
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

    #[test]
    fn topk_duplicate_rows_consume_duplicate_positions() {
        let op = TopKOp::new(schema_kv(), 2, 0, vec![]);

        let out = op
            .process_epoch(
                make_input(&[(10, 1, 1), (10, 1, 1), (9, 2, 1), (8, 3, 1)]),
                1,
            )
            .unwrap();
        let mut net = HashMap::new();
        accumulate_vals(&mut net, &out);

        assert_eq!(net.get(&10), Some(&2));
        assert_eq!(net.get(&9), None);
        assert_eq!(net.get(&8), None);
    }
}

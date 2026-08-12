//! Incremental MIN/MAX aggregate operator (v0.6 — IVM-3).
//!
//! `MinMaxOp` maintains an **indexed multiset** per group and a **cached
//! extremum** so that every epoch produces the correct minimum or maximum
//! without a full table scan:
//!
//! ```text
//! For each input delta (k, v, weight):
//!   1. sort_key = encode(v) so the extremum sorts first in BTreeMap order.
//!   2. old_extremum = extremum_cache.get(k).
//!   3. multiset[k][sort_key] += weight; remove entry if weight == 0.
//!   4. new_extremum = first key in multiset[k] decoded back to i64.
//!   5. If old_extremum != new_extremum:
//!        emit (k, old_extremum, -1) ⊎ (k, new_extremum, +1).
//!   6. Update extremum_cache.
//! ```
//!
//! ## Input schema
//!
//! Two Int64 columns: `k` (group key) and `v` (value).
//!
//! ## Output schema
//!
//! Two Int64 columns: `k`, `extremum_v`.
//!
//! ## State persistence
//!
//! Multiset: `[0x01][0x4D][op_id:8][group_key:8][sort_key:8]` → `weight: i64 BE`
//! Extremum: `[0x02][0x4D][op_id:8][group_key:8]` → `value: i64 BE`
//!
//! Persistence uses scan-and-delete (no range deletion).
//!
//! ## Bounds
//!
//! `live_entries()` returns the total count of distinct (group_key, value)
//! pairs in the multiset.  The cache is bounded by the number of live groups.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tracing::debug;

use rockstream_storage::{
    minmax_sort_key, minmax_sort_key_decode, ShardDb, ShardKeyEncoder, WriteBatch,
};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::op::Operator;
use crate::zset::ArrowZSet;

// ─── Schema ──────────────────────────────────────────────────────────────────

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("extremum_v", DataType::Int64, false),
    ]))
}

// ─── MinMaxKind ───────────────────────────────────────────────────────────────

/// Whether the operator computes MIN or MAX.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinMaxKind {
    Min,
    Max,
}

impl MinMaxKind {
    fn invert(self) -> bool {
        self == MinMaxKind::Max
    }
}

// ─── MinMaxState ─────────────────────────────────────────────────────────────

/// In-memory state for the MIN/MAX operator.
///
/// # Bound
///
/// `live_entries()` counts all distinct (group, value) pairs in the multiset.
/// The extremum cache is bounded by the number of live groups.
#[derive(Debug)]
pub struct MinMaxState {
    /// group_key → BTreeMap<sort_key, weight>.
    ///
    /// The sort_key encodes the i64 value so the extremum is always the first
    /// key (BTreeMap is ascending; smallest sort_key = extremum).
    multiset: HashMap<i64, BTreeMap<[u8; 8], i64>>,
    /// group_key → current extremum i64 value.
    extremum_cache: HashMap<i64, i64>,
    kind: MinMaxKind,
}

impl MinMaxState {
    pub fn new(kind: MinMaxKind) -> Self {
        Self {
            multiset: HashMap::new(),
            extremum_cache: HashMap::new(),
            kind,
        }
    }

    /// Total live (group, value) entries across all groups (fill-level metric).
    pub fn live_entries(&self) -> usize {
        self.multiset.values().map(|m| m.len()).sum()
    }

    /// State bytes metric.
    pub fn state_bytes(&self) -> u64 {
        ((self.multiset.len() * 24) + (self.live_entries() * 16)) as u64
    }

    /// Number of live groups (groups with at least one value).
    pub fn live_groups(&self) -> usize {
        self.multiset.len()
    }

    /// Current cached extremum for a group, if any.
    pub fn cached_extremum(&self, group_key: i64) -> Option<i64> {
        self.extremum_cache.get(&group_key).copied()
    }

    /// True extremum from the multiset (recomputed, for oracle verification).
    pub fn true_extremum(&self, group_key: i64) -> Option<i64> {
        self.multiset
            .get(&group_key)?
            .keys()
            .next()
            .map(|sk| minmax_sort_key_decode(*sk, self.kind.invert()))
    }

    /// Apply one delta (k, v, weight) and return (old_extremum, new_extremum).
    ///
    /// Returns `Err(RS-1017)` if the retraction would make the entry weight negative.
    pub fn apply_delta(
        &mut self,
        k: i64,
        v: i64,
        w: i64,
    ) -> Result<(Option<i64>, Option<i64>), OpError> {
        let sk = minmax_sort_key(v, self.kind.invert());
        let old_extremum = self.extremum_cache.get(&k).copied();

        // Update multiset weight.
        let group = self.multiset.entry(k).or_default();
        let entry = group.entry(sk).or_insert(0);
        let new_weight = *entry + w;
        if new_weight < 0 {
            // Retraction underflow — RS-1017.
            group.remove(&sk);
            if group.is_empty() {
                self.multiset.remove(&k);
                self.extremum_cache.remove(&k);
            }
            return Err(OpError::minmax_retraction_underflow(k, v));
        } else if new_weight == 0 {
            group.remove(&sk);
            if group.is_empty() {
                self.multiset.remove(&k);
            }
        } else {
            *entry = new_weight;
        }

        // Recompute extremum.
        let new_extremum = self
            .multiset
            .get(&k)
            .and_then(|g| g.keys().next())
            .map(|sk| minmax_sort_key_decode(*sk, self.kind.invert()));

        // Update cache.
        match new_extremum {
            Some(e) => {
                self.extremum_cache.insert(k, e);
            }
            None => {
                self.extremum_cache.remove(&k);
            }
        }

        Ok((old_extremum, new_extremum))
    }

    /// Encode the full multiset state into a `WriteBatch` for `op_state`.
    pub fn encode_multiset_batch(&self, op_id: u64) -> WriteBatch {
        let mut wb = WriteBatch::new();
        for (&group_key, entries) in &self.multiset {
            for (sort_key, &weight) in entries {
                let key = ShardKeyEncoder::minmax_multiset_key(op_id, group_key, *sort_key);
                wb.put(&key, &weight.to_be_bytes());
            }
        }
        wb
    }

    /// Encode the extremum cache into a `WriteBatch` for `op_index`.
    pub fn encode_extremum_batch(&self, op_id: u64) -> WriteBatch {
        let mut wb = WriteBatch::new();
        for (&group_key, &extremum) in &self.extremum_cache {
            let key = ShardKeyEncoder::minmax_extremum_key(op_id, group_key);
            wb.put(&key, &extremum.to_be_bytes());
        }
        wb
    }
}

// ─── MinMaxOp ─────────────────────────────────────────────────────────────────

/// Stateful incremental MIN or MAX aggregate operator.
///
/// Input:  two Int64 columns `(k, v)`.
/// Output: two Int64 columns `(k, extremum_v)`.
pub struct MinMaxOp {
    state: Mutex<MinMaxState>,
    op_id: OperatorId,
    kind: MinMaxKind,
}

impl MinMaxOp {
    /// Create a new MIN operator with empty state.
    pub fn new_min(op_id: OperatorId) -> Self {
        Self::new(op_id, MinMaxKind::Min)
    }

    /// Create a new MAX operator with empty state.
    pub fn new_max(op_id: OperatorId) -> Self {
        Self::new(op_id, MinMaxKind::Max)
    }

    pub fn new(op_id: OperatorId, kind: MinMaxKind) -> Self {
        MinMaxOp {
            state: Mutex::new(MinMaxState::new(kind)),
            op_id,
            kind,
        }
    }

    /// Create from pre-loaded state (used after restoring from storage).
    pub fn with_state(op_id: OperatorId, state: MinMaxState) -> Self {
        let kind = state.kind;
        MinMaxOp {
            state: Mutex::new(state),
            op_id,
            kind,
        }
    }

    /// Total live multiset entries across all groups (fill-level metric).
    pub fn live_entries(&self) -> usize {
        self.state
            .lock()
            .expect("MinMaxOp mutex poisoned")
            .live_entries()
    }

    /// Number of live groups.
    pub fn live_groups(&self) -> usize {
        self.state
            .lock()
            .expect("MinMaxOp mutex poisoned")
            .live_groups()
    }

    /// Current cached extremum for a group (used in oracle tests).
    pub fn cached_extremum(&self, group_key: i64) -> Option<i64> {
        self.state
            .lock()
            .expect("MinMaxOp mutex poisoned")
            .cached_extremum(group_key)
    }

    /// True extremum from multiset (recomputed; used in oracle tests).
    pub fn true_extremum(&self, group_key: i64) -> Option<i64> {
        self.state
            .lock()
            .expect("MinMaxOp mutex poisoned")
            .true_extremum(group_key)
    }

    /// Restore MinMaxOp state from `db` into this instance in place.
    pub async fn restore_in_place(&self, db: &ShardDb) -> Result<(), OpError> {
        let loaded = Self::load_from_storage(db, self.op_id, self.kind).await?;
        let loaded_state = loaded.state.into_inner().expect("MinMaxOp mutex poisoned");
        *self.state.lock().expect("MinMaxOp mutex poisoned") = loaded_state;
        Ok(())
    }

    /// Encode multiset state as a `WriteBatch` for persistence.
    pub fn multiset_write_batch(&self) -> WriteBatch {
        self.state
            .lock()
            .expect("MinMaxOp mutex poisoned")
            .encode_multiset_batch(self.op_id.0)
    }

    /// Encode extremum cache as a `WriteBatch` for persistence.
    pub fn extremum_write_batch(&self) -> WriteBatch {
        self.state
            .lock()
            .expect("MinMaxOp mutex poisoned")
            .encode_extremum_batch(self.op_id.0)
    }

    /// Restore a `MinMaxOp` from a `ShardDb` (called at shard startup).
    pub async fn load_from_storage(
        db: &ShardDb,
        op_id: OperatorId,
        kind: MinMaxKind,
    ) -> Result<Self, OpError> {
        let mut state = MinMaxState::new(kind);

        // Load multiset entries.
        let multiset_prefix = ShardKeyEncoder::minmax_operator_prefix(op_id.0);
        let (entries, _truncated) = db
            .scan_prefix_bounded(&multiset_prefix, 64 * 1024 * 1024)
            .await
            .map_err(OpError::storage)?;

        for (key, value) in &entries {
            // Key: [0x01][0x4D][op_id:8][group_key:8][sort_key:8]
            let expected_prefix_len = 1 + 1 + 8 + 8 + 8;
            if key.len() < expected_prefix_len {
                continue;
            }
            let group_key_bytes: [u8; 8] = match key[1 + 1 + 8..1 + 1 + 8 + 8].try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };
            let sort_key: [u8; 8] = match key[1 + 1 + 8 + 8..1 + 1 + 8 + 8 + 8].try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };
            if value.len() < 8 {
                continue;
            }
            let weight = i64::from_be_bytes(value[..8].try_into().unwrap());
            if weight <= 0 {
                continue;
            }
            let group_key = i64::from_be_bytes(group_key_bytes);
            state
                .multiset
                .entry(group_key)
                .or_default()
                .insert(sort_key, weight);
        }

        // Load extremum cache.
        let extremum_prefix = ShardKeyEncoder::minmax_extremum_op_prefix(op_id.0);
        let (ext_entries, _) = db
            .scan_prefix_bounded(&extremum_prefix, 4 * 1024 * 1024)
            .await
            .map_err(OpError::storage)?;

        for (key, value) in &ext_entries {
            // Key: [0x02][0x4D][op_id:8][group_key:8]
            let expected_len = 1 + 1 + 8 + 8;
            if key.len() < expected_len || value.len() < 8 {
                continue;
            }
            let group_key_bytes: [u8; 8] = match key[1 + 1 + 8..1 + 1 + 8 + 8].try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };
            let group_key = i64::from_be_bytes(group_key_bytes);
            let extremum = i64::from_be_bytes(value[..8].try_into().unwrap());
            state.extremum_cache.insert(group_key, extremum);
        }

        Ok(Self::with_state(op_id, state))
    }

    /// State bytes metric.
    pub fn state_bytes(&self) -> u64 {
        self.state.lock().unwrap().state_bytes()
    }
}

impl Operator for MinMaxOp {
    fn name(&self) -> &str {
        match self.kind {
            MinMaxKind::Min => "MinOp",
            MinMaxKind::Max => "MaxOp",
        }
    }

    fn state_bytes(&self) -> u64 {
        self.state_bytes()
    }

    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(output_schema()));
        }

        if delta.data.num_columns() < 2 {
            return Err(OpError::column_out_of_bounds(1, delta.data.num_columns()));
        }

        let k_col = delta
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?;
        let v_raw = delta.data.column(1);
        let v_col_owned = if let Some(arr) = v_raw.as_any().downcast_ref::<Int64Array>() {
            arr.clone()
        } else if let Ok(cast_arr) = arrow::compute::cast(v_raw.as_ref(), &DataType::Int64) {
            cast_arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .cloned()
                .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?
        } else {
            return Err(OpError::column_type_mismatch(
                "Int64",
                format!("{:?}", v_raw.data_type()),
            ));
        };
        let v_col = &v_col_owned;

        let n = delta.num_rows();
        let mut out_k: Vec<i64> = Vec::with_capacity(n * 2);
        let mut out_extremum: Vec<i64> = Vec::with_capacity(n * 2);
        let mut out_weights: Vec<i64> = Vec::with_capacity(n * 2);

        let mut state = self.state.lock().expect("MinMaxOp mutex poisoned");

        for row in 0..n {
            let k = k_col.value(row);
            let v = v_col.value(row);
            let w = delta.weights[row];

            let (old_ext, new_ext) = state.apply_delta(k, v, w)?;

            if old_ext != new_ext {
                if let Some(old) = old_ext {
                    out_k.push(k);
                    out_extremum.push(old);
                    out_weights.push(-1);
                }
                if let Some(new) = new_ext {
                    out_k.push(k);
                    out_extremum.push(new);
                    out_weights.push(1);
                }
            }
        }

        drop(state);

        debug!(
            op_id = self.op_id.0,
            input_rows = n,
            output_rows = out_k.len(),
            "MinMaxOp: processed delta"
        );

        if out_k.is_empty() {
            return Ok(ArrowZSet::empty(output_schema()));
        }

        let schema = output_schema();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(out_k)),
            Arc::new(Int64Array::from(out_extremum)),
        ];
        let data = RecordBatch::try_new(schema, cols).map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(data, out_weights))
    }
}

// ─── Persistence helpers ──────────────────────────────────────────────────────

/// Persist the full MinMax state to `ShardDb`, cleaning up stale entries.
///
/// Uses scan-and-delete (no range deletion):
/// 1. Scan all existing multiset entries for `op_id`.
/// 2. Delete entries whose sort_key is no longer in live state.
/// 3. Write all live multiset entries and extremum cache entries.
pub async fn append_minmax_state(
    db: &ShardDb,
    op: &MinMaxOp,
    target: &mut WriteBatch,
) -> Result<(), OpError> {
    let multiset_prefix = ShardKeyEncoder::minmax_operator_prefix(op.op_id.0);
    let extremum_prefix = ShardKeyEncoder::minmax_extremum_op_prefix(op.op_id.0);

    // Scan existing entries (64 MB cap).
    let (existing_multiset, _) = db
        .scan_prefix_bounded(&multiset_prefix, 64 * 1024 * 1024)
        .await
        .map_err(OpError::storage)?;
    let (existing_extremum, _) = db
        .scan_prefix_bounded(&extremum_prefix, 4 * 1024 * 1024)
        .await
        .map_err(OpError::storage)?;

    // Build write batch while holding the state lock, drop lock before await.
    let wb = {
        let state = op.state.lock().expect("MinMaxOp mutex poisoned");
        let mut wb = WriteBatch::new();

        // Delete stale multiset entries.
        for (key, _) in &existing_multiset {
            let expected_len = 1 + 1 + 8 + 8 + 8;
            if key.len() < expected_len {
                continue;
            }
            let group_key = i64::from_be_bytes(key[1 + 1 + 8..1 + 1 + 8 + 8].try_into().unwrap());
            let sort_key: [u8; 8] = key[1 + 1 + 8 + 8..expected_len].try_into().unwrap();
            let still_live = state
                .multiset
                .get(&group_key)
                .map(|m| m.contains_key(&sort_key))
                .unwrap_or(false);
            if !still_live {
                wb.delete(key);
            }
        }

        // Delete stale extremum cache entries.
        for (key, _) in &existing_extremum {
            let expected_len = 1 + 1 + 8 + 8;
            if key.len() < expected_len {
                continue;
            }
            let group_key = i64::from_be_bytes(key[1 + 1 + 8..expected_len].try_into().unwrap());
            if !state.extremum_cache.contains_key(&group_key) {
                wb.delete(key);
            }
        }

        // Write all live entries.
        wb.merge_from(state.encode_multiset_batch(op.op_id.0));
        wb.merge_from(state.encode_extremum_batch(op.op_id.0));
        wb
        // Lock released here.
    };

    target.merge_from(wb);
    Ok(())
}

/// Persist MIN/MAX state as one standalone batch for legacy callers.
pub async fn persist_minmax_state(db: &ShardDb, op: &MinMaxOp) -> Result<(), OpError> {
    let mut batch = WriteBatch::new();
    append_minmax_state(db, op, &mut batch).await?;
    if !batch.is_empty() {
        db.write_batch(batch).await.map_err(OpError::storage)?;
    }
    Ok(())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::OperatorId;

    fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
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

    fn extract_rows(batch: &ArrowZSet) -> Vec<(i64, i64, i64)> {
        let k_col = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let e_col = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| (k_col.value(i), e_col.value(i), batch.weights[i]))
            .collect()
    }

    #[test]
    fn min_single_insert_creates_group() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let out = op.process_delta(make_kv_batch(&[(1, 10, 1)])).unwrap();
        let rows = extract_rows(&out);
        assert_eq!(rows, vec![(1, 10, 1)]);
        assert_eq!(op.live_groups(), 1);
    }

    #[test]
    fn max_single_insert_creates_group() {
        let op = MinMaxOp::new_max(OperatorId(0));
        let out = op.process_delta(make_kv_batch(&[(1, 10, 1)])).unwrap();
        let rows = extract_rows(&out);
        assert_eq!(rows, vec![(1, 10, 1)]);
    }

    #[test]
    fn min_smaller_value_replaces_extremum() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let _ = op.process_delta(make_kv_batch(&[(1, 10, 1)])).unwrap();
        let out = op.process_delta(make_kv_batch(&[(1, 5, 1)])).unwrap();
        let rows = extract_rows(&out);
        assert!(rows.contains(&(1, 10, -1)), "missing retraction: {rows:?}");
        assert!(rows.contains(&(1, 5, 1)), "missing insertion: {rows:?}");
        assert_eq!(op.cached_extremum(1), Some(5));
    }

    #[test]
    fn max_larger_value_replaces_extremum() {
        let op = MinMaxOp::new_max(OperatorId(0));
        let _ = op.process_delta(make_kv_batch(&[(1, 5, 1)])).unwrap();
        let out = op.process_delta(make_kv_batch(&[(1, 10, 1)])).unwrap();
        let rows = extract_rows(&out);
        assert!(rows.contains(&(1, 5, -1)), "missing retraction: {rows:?}");
        assert!(rows.contains(&(1, 10, 1)), "missing insertion: {rows:?}");
        assert_eq!(op.cached_extremum(1), Some(10));
    }

    #[test]
    fn min_non_extremum_insert_no_output() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let _ = op.process_delta(make_kv_batch(&[(1, 5, 1)])).unwrap();
        let out = op.process_delta(make_kv_batch(&[(1, 10, 1)])).unwrap();
        // min is still 5; inserting 10 doesn't change it.
        assert!(out.is_empty(), "expected empty output: {out:?}");
        assert_eq!(op.cached_extremum(1), Some(5));
    }

    #[test]
    fn min_delete_extremum_rescans_for_replacement() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let _ = op
            .process_delta(make_kv_batch(&[(1, 5, 1), (1, 10, 1)]))
            .unwrap();
        // Remove the minimum (5); new min should be 10.
        let out = op.process_delta(make_kv_batch(&[(1, 5, -1)])).unwrap();
        let rows = extract_rows(&out);
        assert!(rows.contains(&(1, 5, -1)), "missing retraction: {rows:?}");
        assert!(rows.contains(&(1, 10, 1)), "missing insertion: {rows:?}");
        assert_eq!(op.cached_extremum(1), Some(10));
    }

    #[test]
    fn max_delete_extremum_rescans_for_replacement() {
        let op = MinMaxOp::new_max(OperatorId(0));
        let _ = op
            .process_delta(make_kv_batch(&[(1, 5, 1), (1, 10, 1)]))
            .unwrap();
        // Remove the maximum (10); new max should be 5.
        let out = op.process_delta(make_kv_batch(&[(1, 10, -1)])).unwrap();
        let rows = extract_rows(&out);
        assert!(rows.contains(&(1, 10, -1)), "missing retraction: {rows:?}");
        assert!(rows.contains(&(1, 5, 1)), "missing insertion: {rows:?}");
        assert_eq!(op.cached_extremum(1), Some(5));
    }

    #[test]
    fn delete_last_value_removes_group() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let _ = op.process_delta(make_kv_batch(&[(1, 5, 1)])).unwrap();
        let out = op.process_delta(make_kv_batch(&[(1, 5, -1)])).unwrap();
        let rows = extract_rows(&out);
        assert_eq!(rows, vec![(1, 5, -1)]);
        assert_eq!(op.live_groups(), 0);
        assert_eq!(op.cached_extremum(1), None);
    }

    #[test]
    fn multiple_groups_independent() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let out = op
            .process_delta(make_kv_batch(&[(1, 10, 1), (2, 3, 1), (1, 5, 1)]))
            .unwrap();
        let rows = extract_rows(&out);
        assert!(rows.contains(&(2, 3, 1)), "k=2 insert missing: {rows:?}");
        // k=1: first insert (v=10) → emit (1,10,+1); then (v=5) replaces min
        assert!(rows.contains(&(1, 10, -1)), "k=1 retract missing: {rows:?}");
        assert!(
            rows.contains(&(1, 5, 1)),
            "k=1 final insert missing: {rows:?}"
        );
        assert_eq!(op.live_groups(), 2);
    }

    #[test]
    fn cached_extremum_matches_true_extremum_after_each_insert() {
        let op = MinMaxOp::new_min(OperatorId(0));
        for v in [10i64, 5, 8, 2, 15] {
            let _ = op.process_delta(make_kv_batch(&[(1, v, 1)])).unwrap();
            assert_eq!(
                op.cached_extremum(1),
                op.true_extremum(1),
                "cache/true diverge after inserting v={v}"
            );
        }
    }

    #[test]
    fn cached_extremum_matches_true_extremum_after_deletions() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let _ = op
            .process_delta(make_kv_batch(&[(1, 2, 1), (1, 5, 1), (1, 8, 1)]))
            .unwrap();
        // Remove min (2).
        let _ = op.process_delta(make_kv_batch(&[(1, 2, -1)])).unwrap();
        assert_eq!(op.cached_extremum(1), op.true_extremum(1));
        assert_eq!(op.cached_extremum(1), Some(5));
        // Remove new min (5).
        let _ = op.process_delta(make_kv_batch(&[(1, 5, -1)])).unwrap();
        assert_eq!(op.cached_extremum(1), op.true_extremum(1));
        assert_eq!(op.cached_extremum(1), Some(8));
    }

    #[test]
    fn negative_values_handled_correctly() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let _ = op
            .process_delta(make_kv_batch(&[(1, -10, 1), (1, -5, 1), (1, 0, 1)]))
            .unwrap();
        assert_eq!(op.cached_extremum(1), Some(-10));
    }

    #[test]
    fn i64_boundary_values() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let _ = op
            .process_delta(make_kv_batch(&[(1, i64::MAX, 1), (1, i64::MIN, 1)]))
            .unwrap();
        assert_eq!(op.cached_extremum(1), Some(i64::MIN));
        assert_eq!(op.true_extremum(1), Some(i64::MIN));
    }

    #[test]
    fn i64_boundary_max() {
        let op = MinMaxOp::new_max(OperatorId(0));
        let _ = op
            .process_delta(make_kv_batch(&[(1, i64::MAX, 1), (1, i64::MIN, 1)]))
            .unwrap();
        assert_eq!(op.cached_extremum(1), Some(i64::MAX));
        assert_eq!(op.true_extremum(1), Some(i64::MAX));
    }

    #[test]
    fn empty_delta_returns_empty() {
        let op = MinMaxOp::new_min(OperatorId(0));
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        ]));
        let empty = ArrowZSet::empty(schema);
        let out = op.process_delta(empty).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn duplicate_value_weight_accumulates() {
        let op = MinMaxOp::new_min(OperatorId(0));
        // Insert (k=1, v=5) twice — weight becomes 2.
        let _ = op.process_delta(make_kv_batch(&[(1, 5, 1)])).unwrap();
        let _ = op.process_delta(make_kv_batch(&[(1, 5, 1)])).unwrap();
        assert_eq!(op.live_entries(), 1); // one (group,value) pair, weight=2
                                          // Retract once — weight becomes 1, extremum unchanged.
        let out = op.process_delta(make_kv_batch(&[(1, 5, -1)])).unwrap();
        assert!(out.is_empty(), "extremum unchanged: {out:?}");
        assert_eq!(op.cached_extremum(1), Some(5));
        // Retract again — weight becomes 0, group removed.
        let out2 = op.process_delta(make_kv_batch(&[(1, 5, -1)])).unwrap();
        let rows = extract_rows(&out2);
        assert_eq!(rows, vec![(1, 5, -1)]);
        assert_eq!(op.live_groups(), 0);
    }

    #[test]
    fn sort_key_encode_decode_min() {
        use rockstream_storage::{minmax_sort_key, minmax_sort_key_decode};
        for v in [i64::MIN, -100, -1, 0, 1, 100, i64::MAX] {
            let sk = minmax_sort_key(v, false);
            let decoded = minmax_sort_key_decode(sk, false);
            assert_eq!(decoded, v, "roundtrip failed for v={v}");
        }
    }

    #[test]
    fn sort_key_encode_decode_max() {
        use rockstream_storage::{minmax_sort_key, minmax_sort_key_decode};
        for v in [i64::MIN, -100, -1, 0, 1, 100, i64::MAX] {
            let sk = minmax_sort_key(v, true);
            let decoded = minmax_sort_key_decode(sk, true);
            assert_eq!(decoded, v, "roundtrip failed for v={v}");
        }
    }

    #[test]
    fn sort_keys_min_order_ascending() {
        use rockstream_storage::minmax_sort_key;
        // MIN sort: smallest value → smallest sort key.
        let sk_min = minmax_sort_key(i64::MIN, false);
        let sk_neg = minmax_sort_key(-1, false);
        let sk_zero = minmax_sort_key(0, false);
        let sk_one = minmax_sort_key(1, false);
        let sk_max = minmax_sort_key(i64::MAX, false);
        assert!(sk_min < sk_neg);
        assert!(sk_neg < sk_zero);
        assert!(sk_zero < sk_one);
        assert!(sk_one < sk_max);
    }

    #[test]
    fn sort_keys_max_order_descending() {
        use rockstream_storage::minmax_sort_key;
        // MAX sort: largest value → smallest sort key.
        let sk_max = minmax_sort_key(i64::MAX, true);
        let sk_one = minmax_sort_key(1, true);
        let sk_zero = minmax_sort_key(0, true);
        let sk_neg = minmax_sort_key(-1, true);
        let sk_min = minmax_sort_key(i64::MIN, true);
        assert!(sk_max < sk_one);
        assert!(sk_one < sk_zero);
        assert!(sk_zero < sk_neg);
        assert!(sk_neg < sk_min);
    }
}

//! Incremental aggregate operator (v0.5 — IVM-2).
//!
//! `AggregateOp` implements the DBSP delta rule for GROUP BY aggregates:
//!
//! ```text
//! For each incoming row (k, v) with weight w:
//!   1. Look up old state (sum, count) for group key k.
//!   2. Compute new_sum = old_sum + v * w, new_count = old_count + w.
//!   3. If old_count != 0 → retract: emit (k, old_sum, old_count, avg) with weight -1.
//!   4. If new_count != 0 → insert:  emit (k, new_sum, new_count, avg) with weight +1.
//!   5. Update state: remove k if new_count == 0, else store (new_sum, new_count).
//! ```
//!
//! ## Input schema
//!
//! Two Int64 columns: `k` (group key) and `v` (value to aggregate).
//!
//! ## Output schema
//!
//! Four Int64 columns: `k`, `sum_v`, `count`, `avg_v`
//! where `avg_v = sum_v / count` (truncating integer division).
//!
//! ## State persistence
//!
//! `AggregateOp` optionally persists its arrangement to a `ShardDb` under the
//! `op_state` namespace so that state survives shard restart:
//!
//! - key:   `[0x01 (OpState)][op_id: 8 bytes BE][group_key: 8 bytes BE]`
//! - value: `[sum: 8 bytes BE][count: 8 bytes BE]`
//!
//! Call `persist_state(db)` after each epoch commit to write the full
//! arrangement.  Call `AggregateOp::load_from_storage(db, op_id)` on restart
//! to restore state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tracing::debug;

use rockstream_storage::{ShardDb, ShardKeyEncoder, ShardPrefix, WriteBatch};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::op::Operator;
use crate::zset::ArrowZSet;

// ─── Schema ──────────────────────────────────────────────────────────────────

/// Output schema for the aggregate operator.
fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("sum_v", DataType::Int64, false),
        Field::new("count", DataType::Int64, false),
        Field::new("avg_v", DataType::Int64, false),
    ]))
}

// ─── AggState ────────────────────────────────────────────────────────────────

/// In-memory aggregate arrangement: group_key → (sum, count).
///
/// Only entries with count > 0 are stored; entries are removed when count
/// reaches 0 (group deleted).
///
/// # Bound
///
/// The arrangement is bounded by the number of distinct group keys in the input
/// stream.  The fill level is tracked via `entry_count()`.
#[derive(Debug, Default)]
pub struct AggState {
    /// Group key → (sum_v, count).
    entries: HashMap<i64, (i64, i64)>,
}

impl AggState {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Number of live groups (fill level metric).
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Apply one delta `(key, value_delta * weight)` to the arrangement.
    ///
    /// Returns `(old_state, new_state)` where each is `Option<(sum, count)>`.
    /// `old_state` is `None` when the group did not previously exist.
    /// `new_state` is `None` when the group count drops to zero (group deleted).
    #[allow(clippy::type_complexity)]
    pub fn apply_delta(
        &mut self,
        k: i64,
        v: i64,
        w: i64,
    ) -> Result<(Option<(i64, i64)>, Option<(i64, i64)>), OpError> {
        let (old_sum, old_count) = self.entries.get(&k).copied().unwrap_or((0, 0));
        let old_state = if old_count != 0 {
            Some((old_sum, old_count))
        } else {
            None
        };

        // Checked arithmetic for sum to detect overflow.
        let new_sum = old_sum
            .checked_add(
                v.checked_mul(w)
                    .ok_or_else(|| OpError::aggregate_overflow(k))?,
            )
            .ok_or_else(|| OpError::aggregate_overflow(k))?;
        let new_count = old_count + w;

        let new_state = if new_count != 0 {
            self.entries.insert(k, (new_sum, new_count));
            Some((new_sum, new_count))
        } else {
            self.entries.remove(&k);
            None
        };

        Ok((old_state, new_state))
    }

    /// Encode this state as a `WriteBatch` for the `op_state` namespace.
    ///
    /// Call after each epoch commit to persist state to `ShardDb`.
    pub fn encode_as_write_batch(&self, op_id: OperatorId) -> WriteBatch {
        let mut wb = WriteBatch::new();
        for (&k, &(sum, count)) in &self.entries {
            let key = ShardKeyEncoder::encode(ShardPrefix::OpState, op_id.0, &k.to_be_bytes());
            let mut value = [0u8; 16];
            value[..8].copy_from_slice(&sum.to_be_bytes());
            value[8..].copy_from_slice(&count.to_be_bytes());
            wb.put(&key, &value);
        }
        wb
    }

    /// Decode from the raw entries stored by a previous `encode_as_write_batch`.
    ///
    /// `raw_entries` is the result of scanning the `op_state` namespace for
    /// the given `op_id`.
    pub fn decode_from_entries(
        raw_entries: &[(bytes::Bytes, bytes::Bytes)],
        op_id: OperatorId,
    ) -> Self {
        let op_prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op_id.0);
        let mut state = AggState::new();
        for (key, value) in raw_entries {
            // Strip the operator prefix to get the group key bytes.
            if key.len() < op_prefix.len() + 8 || !key.starts_with(&op_prefix) {
                continue;
            }
            let k_bytes: [u8; 8] = match key[op_prefix.len()..op_prefix.len() + 8].try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };
            if value.len() < 16 {
                continue;
            }
            let sum_bytes: [u8; 8] = match value[..8].try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };
            let count_bytes: [u8; 8] = match value[8..16].try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };
            let k = i64::from_be_bytes(k_bytes);
            let sum = i64::from_be_bytes(sum_bytes);
            let count = i64::from_be_bytes(count_bytes);
            if count != 0 {
                state.entries.insert(k, (sum, count));
            }
        }
        state
    }
}

// ─── AggregateOp ─────────────────────────────────────────────────────────────

/// Stateful incremental aggregate operator.
///
/// Input:  two Int64 columns `(k, v)`.
/// Output: four Int64 columns `(k, sum_v, count, avg_v)`.
///
/// Uses interior mutability (`Mutex`) so it satisfies `Operator: &self`.
pub struct AggregateOp {
    state: Mutex<AggState>,
    op_id: OperatorId,
}

impl AggregateOp {
    /// Create a new aggregate operator with empty state.
    pub fn new(op_id: OperatorId) -> Self {
        AggregateOp {
            state: Mutex::new(AggState::new()),
            op_id,
        }
    }

    /// Create from pre-loaded state (used after loading from storage).
    pub fn with_state(op_id: OperatorId, state: AggState) -> Self {
        AggregateOp {
            state: Mutex::new(state),
            op_id,
        }
    }

    /// Number of live groups (fill-level metric).
    pub fn live_groups(&self) -> usize {
        self.state
            .lock()
            .expect("AggregateOp mutex poisoned")
            .entry_count()
    }

    /// Encode current state as a `WriteBatch` for persistence.
    ///
    /// The caller (usually `ViewSinkOp` or group commit) merges this batch
    /// into the epoch's group-commit `WriteBatch`.
    pub fn state_write_batch(&self) -> WriteBatch {
        self.state
            .lock()
            .expect("AggregateOp mutex poisoned")
            .encode_as_write_batch(self.op_id)
    }

    /// Restore an `AggregateOp` from a `ShardDb` (called at shard startup).
    pub async fn load_from_storage(db: &ShardDb, op_id: OperatorId) -> Result<Self, OpError> {
        let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op_id.0);
        let (entries, _truncated) = db
            .scan_prefix_bounded(&prefix, 64 * 1024 * 1024) // 64 MB cap
            .await
            .map_err(OpError::storage)?;
        let state = AggState::decode_from_entries(&entries, op_id);
        Ok(Self::with_state(op_id, state))
    }
}

impl Operator for AggregateOp {
    fn name(&self) -> &str {
        "AggregateOp"
    }

    /// Apply one Z-set delta batch through the aggregate arrangement.
    ///
    /// For each row `(k, v, weight)`:
    /// - Compute the state transition.
    /// - Emit retraction of old aggregate row (if group existed).
    /// - Emit insertion of new aggregate row (if group still exists).
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        if delta.is_empty() {
            return Ok(ArrowZSet::empty(output_schema()));
        }

        // Validate input schema: need at least 2 Int64 columns (k, v).
        if delta.data.num_columns() < 2 {
            return Err(OpError::column_out_of_bounds(1, delta.data.num_columns()));
        }

        let k_col = delta
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?;
        let v_col = delta
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?;

        let n = delta.num_rows();

        // Output vecs — pre-allocate for 2 output rows per input row (retract + insert).
        let mut out_k: Vec<i64> = Vec::with_capacity(n * 2);
        let mut out_sum: Vec<i64> = Vec::with_capacity(n * 2);
        let mut out_count: Vec<i64> = Vec::with_capacity(n * 2);
        let mut out_avg: Vec<i64> = Vec::with_capacity(n * 2);
        let mut out_weights: Vec<i64> = Vec::with_capacity(n * 2);

        let mut state = self.state.lock().expect("AggregateOp mutex poisoned");

        for row in 0..n {
            let k = k_col.value(row);
            let v = v_col.value(row);
            let w = delta.weights[row];

            let (old_state, new_state) = state.apply_delta(k, v, w)?;

            // Retract old aggregate row.
            if let Some((old_sum, old_count)) = old_state {
                let old_avg = old_sum / old_count;
                out_k.push(k);
                out_sum.push(old_sum);
                out_count.push(old_count);
                out_avg.push(old_avg);
                out_weights.push(-1);
            }

            // Insert new aggregate row.
            if let Some((new_sum, new_count)) = new_state {
                let new_avg = new_sum / new_count;
                out_k.push(k);
                out_sum.push(new_sum);
                out_count.push(new_count);
                out_avg.push(new_avg);
                out_weights.push(1);
            }
        }

        drop(state);

        debug!(
            op_id = self.op_id.0,
            input_rows = n,
            output_rows = out_k.len(),
            "AggregateOp: processed delta"
        );

        if out_k.is_empty() {
            return Ok(ArrowZSet::empty(output_schema()));
        }

        let schema = output_schema();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(out_k)),
            Arc::new(Int64Array::from(out_sum)),
            Arc::new(Int64Array::from(out_count)),
            Arc::new(Int64Array::from(out_avg)),
        ];
        let data = RecordBatch::try_new(schema, cols).map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(data, out_weights))
    }
}

// ─── Frontier persistence ─────────────────────────────────────────────────────

/// Persist the shard frontier (current committed epoch) to `ShardDb`.
///
/// Key: `[0x06 (ShardMeta)][b"frontier"]` (defined by `ShardKeyEncoder::frontier_key()`).
/// Value: `epoch: u64` as 8 bytes big-endian.
pub async fn persist_frontier(db: &ShardDb, epoch: u64) -> Result<(), OpError> {
    let key = ShardKeyEncoder::frontier_key();
    let value = epoch.to_be_bytes();
    db.put(&key, &value).await.map_err(OpError::storage)
}

/// Load the persisted frontier from `ShardDb`.
///
/// Returns `None` if no frontier has been committed yet (fresh shard).
pub async fn load_frontier(db: &ShardDb) -> Result<Option<u64>, OpError> {
    let key = ShardKeyEncoder::frontier_key();
    let raw = db.get(&key).await.map_err(OpError::storage)?;
    match raw {
        None => Ok(None),
        Some(bytes) if bytes.len() == 8 => {
            let epoch = u64::from_be_bytes(bytes[..8].try_into().unwrap());
            Ok(Some(epoch))
        }
        Some(_) => Ok(None), // malformed — treat as absent
    }
}

/// Persist the full aggregate state to `ShardDb`, cleaning up stale entries.
///
/// This is the canonical state-persistence call for `AggregateOp`.  It:
/// 1. Scans all existing `op_state` entries for `op_id`.
/// 2. Deletes entries whose group keys are no longer in the live state.
/// 3. Writes all current live entries.
///
/// This scan-and-delete pattern satisfies the "no range deletion" constraint.
///
/// Call this after each epoch commit to ensure storage reflects current state.
pub async fn persist_agg_state(db: &ShardDb, op: &AggregateOp) -> Result<(), OpError> {
    let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op.op_id.0);

    // Scan all existing state entries (64 MB cap — a bound on the scan).
    let (existing, _truncated) = db
        .scan_prefix_bounded(&prefix, 64 * 1024 * 1024)
        .await
        .map_err(OpError::storage)?;

    // Build a write batch: delete all stale entries, write all live entries.
    // Drop the mutex guard BEFORE the .await call to avoid holding a lock
    // across an await point (clippy::await_holding_lock).
    let wb = {
        let state = op.state.lock().expect("AggregateOp mutex poisoned");
        let mut wb = WriteBatch::new();

        // Deletions: existing entries whose group key is not in current state.
        for (key, _) in &existing {
            if key.len() < prefix.len() + 8 {
                continue;
            }
            let k_bytes: [u8; 8] = match key[prefix.len()..prefix.len() + 8].try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };
            let k = i64::from_be_bytes(k_bytes);
            if !state.entries.contains_key(&k) {
                wb.delete(key);
            }
        }

        // Insertions: current live entries.
        let new_wb = state.encode_as_write_batch(op.op_id);
        wb.merge_from(new_wb);
        wb
        // `state` (MutexGuard) is dropped here — before any await
    };

    if !wb.is_empty() {
        db.write_batch(wb).await.map_err(OpError::storage)?;
    }
    Ok(())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::OperatorId;

    fn make_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
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

    fn extract_rows(batch: &ArrowZSet) -> Vec<(i64, i64, i64, i64, i64)> {
        // (k, sum_v, count, avg_v, weight)
        let k_col = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s_col = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c_col = batch
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let a_col = batch
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| {
                (
                    k_col.value(i),
                    s_col.value(i),
                    c_col.value(i),
                    a_col.value(i),
                    batch.weights[i],
                )
            })
            .collect()
    }

    #[test]
    fn single_insert_creates_group() {
        let op = AggregateOp::new(OperatorId(0));
        let delta = make_batch(&[(1, 10, 1)]);
        let out = op.process_delta(delta).unwrap();
        let rows = extract_rows(&out);
        // No retraction; one insertion of (k=1, sum=10, count=1, avg=10).
        assert_eq!(rows, vec![(1, 10, 1, 10, 1)]);
        assert_eq!(op.live_groups(), 1);
    }

    #[test]
    fn second_insert_into_same_group_retracts_and_inserts() {
        let op = AggregateOp::new(OperatorId(0));
        // Insert k=1, v=10.
        let _ = op.process_delta(make_batch(&[(1, 10, 1)])).unwrap();
        // Insert k=1, v=6.
        let out = op.process_delta(make_batch(&[(1, 6, 1)])).unwrap();
        let rows = extract_rows(&out);
        // Retract (k=1, sum=10, count=1) and insert (k=1, sum=16, count=2, avg=8).
        assert!(
            rows.contains(&(1, 10, 1, 10, -1)),
            "missing retraction: {rows:?}"
        );
        assert!(
            rows.contains(&(1, 16, 2, 8, 1)),
            "missing insertion: {rows:?}"
        );
        assert_eq!(op.live_groups(), 1);
    }

    #[test]
    fn delete_last_row_removes_group() {
        let op = AggregateOp::new(OperatorId(0));
        let _ = op.process_delta(make_batch(&[(1, 10, 1)])).unwrap();
        let out = op.process_delta(make_batch(&[(1, 10, -1)])).unwrap();
        let rows = extract_rows(&out);
        // Retraction of (k=1, sum=10, count=1); no new insertion.
        assert_eq!(rows, vec![(1, 10, 1, 10, -1)]);
        assert_eq!(op.live_groups(), 0);
    }

    #[test]
    fn multiple_groups_independent() {
        let op = AggregateOp::new(OperatorId(0));
        let delta = make_batch(&[(1, 5, 1), (2, 20, 1), (1, 3, 1)]);
        let out = op.process_delta(delta).unwrap();
        let rows = extract_rows(&out);
        // Process k=1,v=5: no old → insert (k=1, sum=5, count=1, avg=5).
        // Process k=2,v=20: no old → insert (k=2, sum=20, count=1, avg=20).
        // Process k=1,v=3: old=(5,1) → retract (k=1,5,1,5,-1), insert (k=1,8,2,4,+1).
        assert!(
            rows.contains(&(2, 20, 1, 20, 1)),
            "k=2 insert missing: {rows:?}"
        );
        assert!(
            rows.contains(&(1, 5, 1, 5, -1)),
            "k=1 first retract missing: {rows:?}"
        );
        assert!(
            rows.contains(&(1, 8, 2, 4, 1)),
            "k=1 final insert missing: {rows:?}"
        );
        assert_eq!(op.live_groups(), 2);
    }

    #[test]
    fn empty_delta_returns_empty_output() {
        let op = AggregateOp::new(OperatorId(0));
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        ]));
        let empty = ArrowZSet::empty(schema);
        let out = op.process_delta(empty).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn state_encode_decode_roundtrip() {
        let op = AggregateOp::new(OperatorId(7));
        let _ = op
            .process_delta(make_batch(&[(1, 10, 1), (2, 20, 1)]))
            .unwrap();
        let wb = op.state_write_batch();
        // The batch should have 2 entries (one per live group).
        assert_eq!(wb.len(), 2);
    }

    #[test]
    fn avg_truncates_toward_zero() {
        let op = AggregateOp::new(OperatorId(0));
        // Insert 3 rows for k=1 with v=-7,-7,-7 → sum=-21, count=3, avg=-7.
        let _ = op.process_delta(make_batch(&[(1, -7, 1)])).unwrap();
        let _ = op.process_delta(make_batch(&[(1, -7, 1)])).unwrap();
        let out = op.process_delta(make_batch(&[(1, -7, 1)])).unwrap();
        // After 3 inserts: sum=-21, count=3, avg=-7. Last output is (+1 new row).
        let rows = extract_rows(&out);
        let new_row = rows.iter().find(|r| r.4 == 1).expect("no +1 row");
        assert_eq!(new_row.1, -21); // sum
        assert_eq!(new_row.2, 3); // count
        assert_eq!(new_row.3, -7); // avg = -21/3
    }

    #[test]
    fn group_count_zero_after_matching_retractions() {
        let op = AggregateOp::new(OperatorId(0));
        let _ = op
            .process_delta(make_batch(&[(3, 5, 1), (3, 7, 1)]))
            .unwrap();
        // Retract both rows.
        op.process_delta(make_batch(&[(3, 5, -1), (3, 7, -1)]))
            .unwrap();
        assert_eq!(op.live_groups(), 0);
    }
}

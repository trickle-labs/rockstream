//! IndexArrangeOp: incremental secondary index maintenance (v0.32-S3).
//!
//! Maintains a `(index_key_bytes ++ pk_bytes) → row_bytes` point-write arrangement.
//! On delete (negative weight) performs a point-delete (NOT a range delete).
//!
//! ## Key format
//!
//! `[ShardPrefix::ViewOutput byte][op_id: 8 BE][index_col_bytes...][pk_col_bytes...]`
//!
//! ## Bound
//!
//! `MAX_INDEX_ARRANGE_ROWS` (configurable via `max_arrangement_rows` field).
//! Fill metric: `index_arrange_row_count` gauge (exposed via `row_count()`).
//! Backpressure: caller must check `is_over_limit()` before sending more rows.
//!
//! ## No range deletion
//!
//! This operator NEVER performs range deletion. Inserts are point-writes;
//! deletes are point-deletes keyed by `(index_key_bytes ++ pk_bytes)`.

use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

use arrow::array::Int64Array;

use rockstream_storage::{ShardDb, ShardPrefix, WriteBatch};
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::zset::ArrowZSet;

/// Default maximum rows in an index arrangement.
pub const MAX_INDEX_ARRANGE_ROWS: u64 = 10_000_000;

/// A source row for backfill operations (v0.32-S4).
#[derive(Debug, Clone)]
pub struct BackfillRow {
    /// The index key value (Int64).
    pub index_val: i64,
    /// The primary key value (Int64).
    pub pk_val: i64,
}

/// Index arrangement operator (v0.32-S3).
///
/// Maintains a `(index_key_bytes ++ pk_bytes) → row_bytes` point-write arrangement.
/// Bounded by `max_rows`; fill level exposed via `row_count()`.
///
/// Optional partial-index filter: when `filter_col` and `filter_val` are set,
/// only rows where `row[filter_col] == filter_val` are arranged (S7).
pub struct IndexArrangeOp {
    db: Arc<ShardDb>,
    op_id: OperatorId,
    index_cols: Vec<usize>,
    pk_cols: Vec<usize>,
    // Signed counter to handle spurious decrements gracefully.
    row_count: Arc<AtomicI64>,
    max_rows: u64,
    /// Partial-index filter: (column index, required value). Rows not matching
    /// are silently skipped.
    filter: Option<(usize, i64)>,
}

impl IndexArrangeOp {
    /// Create a new `IndexArrangeOp`.
    pub fn new(
        db: Arc<ShardDb>,
        op_id: OperatorId,
        index_cols: Vec<usize>,
        pk_cols: Vec<usize>,
        max_rows: u64,
    ) -> Self {
        Self {
            db,
            op_id,
            index_cols,
            pk_cols,
            row_count: Arc::new(AtomicI64::new(0)),
            max_rows,
            filter: None,
        }
    }

    /// Create a new partial-index `IndexArrangeOp` with an equality filter.
    ///
    /// Only rows where `row[filter_col] == filter_val` are arranged (S7).
    pub fn new_partial(
        db: Arc<ShardDb>,
        op_id: OperatorId,
        index_cols: Vec<usize>,
        pk_cols: Vec<usize>,
        max_rows: u64,
        filter_col: usize,
        filter_val: i64,
    ) -> Self {
        Self {
            db,
            op_id,
            index_cols,
            pk_cols,
            row_count: Arc::new(AtomicI64::new(0)),
            max_rows,
            filter: Some((filter_col, filter_val)),
        }
    }

    /// Current number of rows in the arrangement (fill metric).
    pub fn row_count(&self) -> u64 {
        self.row_count.load(Ordering::Relaxed).max(0) as u64
    }

    /// Returns `true` if the arrangement has reached its row limit (backpressure signal).
    pub fn is_over_limit(&self) -> bool {
        self.row_count() >= self.max_rows
    }

    /// Apply a Z-set delta to the arrangement.
    ///
    /// - Weight > 0: point-write the row.
    /// - Weight < 0: point-delete the row.
    /// - Weight == 0: no-op.
    ///
    /// NEVER performs range deletion.
    pub async fn apply_delta(&self, delta: &ArrowZSet) -> Result<(), OpError> {
        let mut batch = WriteBatch::new();
        let num_rows = delta.data.num_rows();

        for row in 0..num_rows {
            let weight = delta.weights[row];
            if weight == 0 {
                continue;
            }

            // Partial-index filter: skip rows that don't match (S7).
            if let Some((filter_col, filter_val)) = self.filter {
                let num_cols = delta.data.num_columns();
                if filter_col < num_cols {
                    let arr = delta
                        .data
                        .column(filter_col)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?;
                    if arr.value(row) != filter_val {
                        continue; // row does not satisfy partial-index predicate
                    }
                }
            }

            let key = self.encode_arrangement_key(delta, row)?;
            if weight > 0 {
                let value = self.encode_row_value(delta, row)?;
                batch.put(&key, &value);
                self.row_count.fetch_add(1, Ordering::Relaxed);
            } else {
                // weight < 0: point-delete — never range-delete
                batch.delete(&key);
                self.row_count.fetch_sub(1, Ordering::Relaxed);
            }
        }

        if !batch.is_empty() {
            self.db.write_batch(batch).await.map_err(OpError::storage)?;
        }
        Ok(())
    }

    /// Look up rows by exact index key value (first index column only for now).
    ///
    /// Returns the encoded row bytes for all matching rows.
    pub async fn point_lookup(&self, index_key_bytes: &[u8]) -> Result<Vec<Vec<u8>>, OpError> {
        let mut prefix = Vec::new();
        prefix.push(ShardPrefix::ViewOutput.as_byte());
        prefix.extend_from_slice(&self.op_id.0.to_be_bytes());
        prefix.extend_from_slice(index_key_bytes);

        let entries = self
            .db
            .scan_prefix(&prefix)
            .await
            .map_err(OpError::storage)?;
        Ok(entries.into_iter().map(|(_, v)| v.to_vec()).collect())
    }

    /// Encode the arrangement key for a given row.
    ///
    /// Format: `[ViewOutput byte][op_id: 8 BE][index_col_bytes...][pk_col_bytes...]`
    fn encode_arrangement_key(&self, delta: &ArrowZSet, row: usize) -> Result<Vec<u8>, OpError> {
        let num_cols = delta.data.num_columns();
        let mut key = Vec::with_capacity(1 + 8 + 8 * (self.index_cols.len() + self.pk_cols.len()));
        key.push(ShardPrefix::ViewOutput.as_byte());
        key.extend_from_slice(&self.op_id.0.to_be_bytes());

        for &col_idx in &self.index_cols {
            if col_idx >= num_cols {
                return Err(OpError::column_out_of_bounds(col_idx, num_cols));
            }
            let arr = delta
                .data
                .column(col_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?;
            key.extend_from_slice(&arr.value(row).to_be_bytes());
        }

        for &col_idx in &self.pk_cols {
            if col_idx >= num_cols {
                return Err(OpError::column_out_of_bounds(col_idx, num_cols));
            }
            let arr = delta
                .data
                .column(col_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?;
            key.extend_from_slice(&arr.value(row).to_be_bytes());
        }

        Ok(key)
    }

    /// Encode row bytes (all columns concatenated as 8-byte BE i64 values).
    fn encode_row_value(&self, delta: &ArrowZSet, row: usize) -> Result<Vec<u8>, OpError> {
        let num_cols = delta.data.num_columns();
        let mut val = Vec::with_capacity(8 * num_cols);
        for col_idx in 0..num_cols {
            let arr = delta
                .data
                .column(col_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| OpError::column_type_mismatch("Int64", "other"))?;
            val.extend_from_slice(&arr.value(row).to_be_bytes());
        }
        Ok(val)
    }

    // ── v0.32-S4: Backfill / frontier persistence ────────────────────────────

    /// Shard-meta key for the backfill frontier of a named index.
    fn backfill_frontier_key(index_name: &str) -> Vec<u8> {
        let mut key = Vec::new();
        key.push(ShardPrefix::ShardMeta.as_byte());
        key.extend_from_slice(b"index_backfill_frontier/");
        key.extend_from_slice(index_name.as_bytes());
        key
    }

    /// Persist the backfill frontier position durably so crash-restart can resume.
    async fn write_backfill_frontier(
        db: &Arc<ShardDb>,
        index_name: &str,
        position: u64,
    ) -> Result<(), OpError> {
        let key = Self::backfill_frontier_key(index_name);
        let mut batch = WriteBatch::new();
        batch.put(&key, &position.to_be_bytes());
        db.write_batch(batch).await.map_err(OpError::storage)
    }

    /// Read the persisted backfill frontier (returns 0 if not yet written).
    pub async fn read_backfill_frontier(
        db: Arc<ShardDb>,
        index_name: &str,
    ) -> Result<u64, OpError> {
        let key = Self::backfill_frontier_key(index_name);
        match db.get(&key).await.map_err(OpError::storage)? {
            Some(bytes) if bytes.len() >= 8 => {
                let arr: [u8; 8] = bytes[..8].try_into().unwrap();
                Ok(u64::from_be_bytes(arr))
            }
            _ => Ok(0),
        }
    }

    /// Backfill rows from a pre-collected slice of `BackfillRow`.
    ///
    /// Skips the first `resume_row` rows (already committed from a prior run).
    /// Writes the frontier durably after each row so crash-restart can resume
    /// without duplication.
    ///
    /// This is the low-level primitive used by the crash-restart test (S4).
    pub async fn run_backfill_rows(
        &self,
        rows: &[BackfillRow],
        index_name: &str,
        db: Arc<ShardDb>,
        resume_row: u64,
    ) -> Result<(), OpError> {
        for (i, row) in rows.iter().enumerate() {
            let position = i as u64;
            if position < resume_row {
                // Already committed in a prior run — skip.
                continue;
            }

            // Build a single-row ZSet for this backfill row.
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("index_col", arrow::datatypes::DataType::Int64, false),
                arrow::datatypes::Field::new("pk_col", arrow::datatypes::DataType::Int64, false),
            ]));
            let batch = arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![row.index_val])) as Arc<dyn arrow::array::Array>,
                    Arc::new(Int64Array::from(vec![row.pk_val])) as Arc<dyn arrow::array::Array>,
                ],
            )
            .map_err(OpError::arrow)?;
            let delta = ArrowZSet::new(batch, vec![1i64]);
            self.apply_delta(&delta).await?;

            // Persist frontier after each row (durable crash recovery).
            let new_frontier = position + 1;
            Self::write_backfill_frontier(&db, index_name, new_frontier).await?;
        }
        Ok(())
    }
}

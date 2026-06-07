//! ViewSink operator: writes Z-set deltas to shard storage.
//!
//! `ViewSinkOp` persists output delta batches to the `view_output` namespace
//! of a `ShardDb`.  The key format is:
//!
//! ```text
//! [ShardPrefix::ViewOutput (1 byte)] [operator_id (8 bytes BE)] [epoch (8 bytes BE)] [row_index (8 bytes BE)]
//! ```
//!
//! The value is the row serialised as a sequence of `i64` big-endian values
//! (one per column, in column order), followed by the `_weight` as the final
//! i64.
//!
//! This simple encoding is correct for v0.4's Int64-only schema and allows
//! round-trip read-back in the LFS integration test.

use std::sync::Arc;

use arrow::array::Int64Array;
use rockstream_storage::{ShardDb, ShardPrefix, WriteBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::timestamp::Epoch;
use tracing::debug;

use crate::error::OpError;
use crate::zset::ArrowZSet;

/// A sink that writes output Z-set deltas to a `ShardDb`.
pub struct ViewSinkOp {
    /// The shard database to write into.
    db: Arc<ShardDb>,
    /// Operator ID used as part of the storage key prefix.
    op_id: OperatorId,
    /// Current epoch counter (incremented per `write_epoch`).
    epoch: std::sync::atomic::AtomicU64,
}

impl ViewSinkOp {
    /// Create a new sink backed by `db` with the given operator ID.
    pub fn new(db: Arc<ShardDb>, op_id: OperatorId) -> Self {
        ViewSinkOp {
            db,
            op_id,
            epoch: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Encode a single row as raw bytes.
    ///
    /// Format: `[col_0: i64 BE][col_1: i64 BE]...[weight: i64 BE]`
    fn encode_row(batch: &ArrowZSet, row: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity((batch.data.num_columns() + 1) * 8);
        for col in batch.data.columns() {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ViewSink: column must be Int64");
            buf.extend_from_slice(&arr.value(row).to_be_bytes());
        }
        buf.extend_from_slice(&batch.weights[row].to_be_bytes());
        buf
    }

    /// Write one delta batch to storage.
    ///
    /// Each row becomes one key-value entry in the `view_output` namespace.
    pub async fn write_epoch(
        &self,
        batch: &ArrowZSet,
        epoch: Epoch,
    ) -> Result<(), OpError> {
        if batch.is_empty() {
            return Ok(());
        }
        let op_id_raw = self.op_id.0;
        let epoch_bytes = epoch.to_be_bytes();
        let mut wb = WriteBatch::new();

        for row in 0..batch.num_rows() {
            let mut key = Vec::with_capacity(1 + 8 + 8 + 8);
            key.push(ShardPrefix::ViewOutput.as_byte());
            key.extend_from_slice(&op_id_raw.to_be_bytes());
            key.extend_from_slice(&epoch_bytes);
            key.extend_from_slice(&(row as u64).to_be_bytes());
            let value = Self::encode_row(batch, row);
            wb.put(&key, &value);
        }

        debug!(
            op_id = op_id_raw,
            epoch,
            rows = batch.num_rows(),
            "ViewSink: writing epoch"
        );

        self.db
            .write_batch(wb)
            .await
            .map_err(OpError::storage)
    }

    /// Convenience: write batch at the next auto-incremented epoch.
    pub async fn write_next_epoch(&self, batch: &ArrowZSet) -> Result<Epoch, OpError> {
        let epoch = self
            .epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.write_epoch(batch, epoch).await?;
        Ok(epoch)
    }
}

/// Read back all rows written by `ViewSinkOp` for a given operator ID.
///
/// Returns a `Vec<(epoch, row_index, columns: Vec<i64>, weight: i64)>` sorted
/// by `(epoch, row_index)`.  Used in tests.
pub async fn read_view_output(
    db: &ShardDb,
    op_id: OperatorId,
    num_cols: usize,
) -> Result<Vec<(u64, u64, Vec<i64>, i64)>, OpError> {
    let prefix = {
        let mut p = Vec::with_capacity(9);
        p.push(ShardPrefix::ViewOutput.as_byte());
        p.extend_from_slice(&op_id.0.to_be_bytes());
        p
    };

    let entries = db
        .scan_prefix_bounded(&prefix, 10_000_000)
        .await
        .map_err(OpError::storage)?;

    let mut rows = Vec::new();
    for (key, value) in entries.0 {
        // key: [prefix:1][op_id:8][epoch:8][row:8] = 25 bytes
        if key.len() < 25 {
            continue;
        }
        let kb: &[u8] = &key;
        let vb: &[u8] = &value;
        let epoch = u64::from_be_bytes(kb[9..17].try_into().unwrap());
        let row_idx = u64::from_be_bytes(kb[17..25].try_into().unwrap());

        // value: [col_0:8]...[col_n:8][weight:8]
        let expected_len = (num_cols + 1) * 8;
        if vb.len() < expected_len {
            continue;
        }
        let mut cols = Vec::with_capacity(num_cols);
        for c in 0..num_cols {
            let v = i64::from_be_bytes(vb[c * 8..(c + 1) * 8].try_into().unwrap());
            cols.push(v);
        }
        let weight =
            i64::from_be_bytes(vb[num_cols * 8..(num_cols + 1) * 8].try_into().unwrap());
        rows.push((epoch, row_idx, cols, weight));
    }
    rows.sort();
    Ok(rows)
}

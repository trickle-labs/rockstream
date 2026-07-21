//! ViewSink operator: writes Z-set deltas to shard storage.
//!
//! `ViewSinkOp` persists output delta batches to the `view_output` namespace
//! of a `ShardDb`.  The key format is:
//!
//! ```text
//! [ShardPrefix::ViewOutput (1 byte)] [operator_id (8 bytes BE)] [epoch (8 bytes BE)] [row_index (8 bytes BE)]
//! ```
//!
//! The value is a self-describing, per-column, type-tagged encoding: each
//! column value is preceded by a one-byte type tag (`Int64`, `Utf8`,
//! `Boolean`, `Float64`) followed by that type's variable-width payload, in
//! column order, followed by the `_weight` as a final fixed 8-byte `i64`
//! (weight is always `Int64`, so it carries no tag). This lets a row mix
//! column types (e.g. `BIGINT` + `TEXT`) without needing a separately
//! persisted schema — decode reads `num_cols` tagged values, then the
//! trailing weight.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use rockstream_storage::{ShardDb, ShardPrefix, WriteBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::timestamp::Epoch;
use tracing::debug;

use crate::error::OpError;
use crate::zset::ArrowZSet;

/// Type tags used in `ViewSinkOp`'s self-describing row encoding.
const TAG_INT64: u8 = 0;
const TAG_UTF8: u8 = 1;
const TAG_BOOLEAN: u8 = 2;
const TAG_FLOAT64: u8 = 3;

/// A decoded column value read back from `view_output` storage.
///
/// Mirrors the `arrow::datatypes::DataType` variants that `ViewSinkOp`'s
/// encoding currently supports: `Int64`, `Utf8`, `Boolean`, `Float64`.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Int64(i64),
    Utf8(String),
    Boolean(bool),
    Float64(f64),
}

impl ColumnValue {
    /// Returns the inner `i64` if this is `ColumnValue::Int64`, else `None`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ColumnValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the inner `&str` if this is `ColumnValue::Utf8`, else `None`.
    pub fn as_utf8(&self) -> Option<&str> {
        match self {
            ColumnValue::Utf8(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the inner `bool` if this is `ColumnValue::Boolean`, else `None`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ColumnValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the inner `f64` if this is `ColumnValue::Float64`, else `None`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ColumnValue::Float64(v) => Some(*v),
            _ => None,
        }
    }
}

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
    /// Per-column, schema-driven, type-tagged encoding (see module docs),
    /// followed by the fixed 8-byte `_weight`.
    fn encode_row(batch: &ArrowZSet, row: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity((batch.data.num_columns() + 1) * 9);
        for col in batch.data.columns() {
            match col.data_type() {
                DataType::Int64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("ViewSink: column typed Int64 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&arr.value(row).to_be_bytes());
                }
                DataType::Utf8 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("ViewSink: column typed Utf8 but downcast failed");
                    let s = arr.value(row);
                    buf.push(TAG_UTF8);
                    buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                DataType::Boolean => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("ViewSink: column typed Boolean but downcast failed");
                    buf.push(TAG_BOOLEAN);
                    buf.push(if arr.value(row) { 1 } else { 0 });
                }
                DataType::Float64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("ViewSink: column typed Float64 but downcast failed");
                    buf.push(TAG_FLOAT64);
                    buf.extend_from_slice(&arr.value(row).to_bits().to_be_bytes());
                }
                other => panic!("ViewSink: unsupported column type {other:?}"),
            }
        }
        buf.extend_from_slice(&batch.weights[row].to_be_bytes());
        buf
    }

    /// Write one delta batch to storage.
    ///
    /// Each row becomes one key-value entry in the `view_output` namespace.
    pub async fn write_epoch(&self, batch: &ArrowZSet, epoch: Epoch) -> Result<(), OpError> {
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

        self.db.write_batch(wb).await.map_err(OpError::storage)
    }

    /// Convenience: write batch at the next auto-incremented epoch.
    pub async fn write_next_epoch(&self, batch: &ArrowZSet) -> Result<Epoch, OpError> {
        let epoch = self.epoch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.write_epoch(batch, epoch).await?;
        Ok(epoch)
    }
}

/// Decode one row's value bytes into `num_cols` type-tagged `ColumnValue`s
/// plus the trailing weight. Returns `None` on truncated/malformed input
/// (the row is skipped by the caller rather than causing a panic on
/// corrupt/partial data).
pub(crate) fn decode_row(vb: &[u8], num_cols: usize) -> Option<(Vec<ColumnValue>, i64)> {
    let mut pos = 0usize;
    let mut cols = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        let tag = *vb.get(pos)?;
        pos += 1;
        match tag {
            TAG_INT64 => {
                let end = pos.checked_add(8)?;
                let v = i64::from_be_bytes(vb.get(pos..end)?.try_into().ok()?);
                pos = end;
                cols.push(ColumnValue::Int64(v));
            }
            TAG_UTF8 => {
                let len_end = pos.checked_add(4)?;
                let len = u32::from_be_bytes(vb.get(pos..len_end)?.try_into().ok()?) as usize;
                pos = len_end;
                let str_end = pos.checked_add(len)?;
                let s = String::from_utf8(vb.get(pos..str_end)?.to_vec()).ok()?;
                pos = str_end;
                cols.push(ColumnValue::Utf8(s));
            }
            TAG_BOOLEAN => {
                let v = *vb.get(pos)?;
                pos += 1;
                cols.push(ColumnValue::Boolean(v != 0));
            }
            TAG_FLOAT64 => {
                let end = pos.checked_add(8)?;
                let bits = u64::from_be_bytes(vb.get(pos..end)?.try_into().ok()?);
                pos = end;
                cols.push(ColumnValue::Float64(f64::from_bits(bits)));
            }
            _ => return None,
        }
    }
    let end = pos.checked_add(8)?;
    let weight = i64::from_be_bytes(vb.get(pos..end)?.try_into().ok()?);
    Some((cols, weight))
}

/// Read back all rows written by `ViewSinkOp` for a given operator ID.
///
/// Returns a `Vec<(epoch, row_index, columns: Vec<ColumnValue>, weight:
/// i64)>` sorted by `(epoch, row_index)`. `num_cols` must match the number
/// of columns the batch was written with (the type of each column is
/// self-described in the stored bytes; see module docs). Used in tests.
pub async fn read_view_output(
    db: &ShardDb,
    op_id: OperatorId,
    num_cols: usize,
) -> Result<Vec<(u64, u64, Vec<ColumnValue>, i64)>, OpError> {
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

        let Some((cols, weight)) = decode_row(vb, num_cols) else {
            continue;
        };
        rows.push((epoch, row_idx, cols, weight));
    }
    rows.sort_by_key(|(epoch, row_idx, ..)| (*epoch, *row_idx));
    Ok(rows)
}

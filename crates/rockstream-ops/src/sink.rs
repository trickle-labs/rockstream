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

use arrow::array::{Array, Float64Array, Int64Array};
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
const TAG_NULL: u8 = 4;

/// A decoded column value read back from `view_output` storage.
///
/// Mirrors the `arrow::datatypes::DataType` variants that `ViewSinkOp`'s
/// encoding currently supports: `Int64`, `Utf8`, `Boolean`, `Float64`.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Null,
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
            if col.is_null(row) {
                buf.push(TAG_NULL);
                continue;
            }
            match col.data_type() {
                DataType::Int64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("ViewSink: column typed Int64 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&arr.value(row).to_be_bytes());
                }
                DataType::Int32 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .expect("ViewSink: column typed Int32 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&(arr.value(row) as i64).to_be_bytes());
                }
                DataType::Int16 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int16Array>()
                        .expect("ViewSink: column typed Int16 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&(arr.value(row) as i64).to_be_bytes());
                }
                DataType::Int8 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int8Array>()
                        .expect("ViewSink: column typed Int8 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&(arr.value(row) as i64).to_be_bytes());
                }
                DataType::UInt64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::UInt64Array>()
                        .expect("ViewSink: column typed UInt64 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&(arr.value(row) as i64).to_be_bytes());
                }
                DataType::UInt32 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::UInt32Array>()
                        .expect("ViewSink: column typed UInt32 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&(arr.value(row) as i64).to_be_bytes());
                }
                DataType::UInt16 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::UInt16Array>()
                        .expect("ViewSink: column typed UInt16 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&(arr.value(row) as i64).to_be_bytes());
                }
                DataType::UInt8 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::UInt8Array>()
                        .expect("ViewSink: column typed UInt8 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&(arr.value(row) as i64).to_be_bytes());
                }
                DataType::Date32 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Date32Array>()
                        .expect("ViewSink: column typed Date32 but downcast failed");
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&(arr.value(row) as i64).to_be_bytes());
                }
                DataType::Float64 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("ViewSink: column typed Float64 but downcast failed");
                    buf.push(TAG_FLOAT64);
                    buf.extend_from_slice(&arr.value(row).to_bits().to_be_bytes());
                }
                DataType::Float32 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()
                        .expect("ViewSink: column typed Float32 but downcast failed");
                    buf.push(TAG_FLOAT64);
                    buf.extend_from_slice(&(arr.value(row) as f64).to_bits().to_be_bytes());
                }
                DataType::Utf8 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("ViewSink: column typed Utf8 but downcast failed");
                    let s = arr.value(row);
                    buf.push(TAG_UTF8);
                    buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                DataType::Boolean => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .expect("ViewSink: column typed Boolean but downcast failed");
                    buf.push(TAG_BOOLEAN);
                    buf.push(if arr.value(row) { 1 } else { 0 });
                }
                other => panic!("ViewSink: unsupported column type {other:?}"),
            }
        }
        buf.extend_from_slice(&batch.weights[row].to_be_bytes());
        buf
    }

    /// Append one delta batch to an existing atomic write.
    ///
    /// Each row becomes one key-value entry in the `view_output` namespace.
    pub fn append_epoch(&self, wb: &mut WriteBatch, batch: &ArrowZSet, epoch: Epoch) {
        if batch.is_empty() {
            return;
        }
        let op_id_raw = self.op_id.0;
        let epoch_bytes = epoch.to_be_bytes();

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
    }

    /// Write one delta batch to storage.
    pub async fn write_epoch(&self, batch: &ArrowZSet, epoch: Epoch) -> Result<(), OpError> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut wb = WriteBatch::new();
        self.append_epoch(&mut wb, batch, epoch);
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
            TAG_NULL => cols.push(ColumnValue::Null),
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
    let prefix = view_output_prefix(op_id);
    let entries = db
        .scan_prefix_bounded(&prefix, 10_000_000)
        .await
        .map_err(OpError::storage)?;
    Ok(decode_view_output_entries(entries.0, num_cols))
}

/// Same as [`read_view_output`] but reads through a read-only `ShardReader`
/// (v0.51.4 Slice 8 — the multi-shard/cross-process publish-and-scatter-read
/// path: a standalone `--role gateway` process with no local `ShardDb` reads
/// a compiled view's output this way, via [`crate::view_directory`]'s
/// `view_name -> op_id` lookup rather than a shared in-memory catalog).
pub async fn read_view_output_via_reader(
    reader: &rockstream_storage::ShardReader,
    op_id: OperatorId,
    num_cols: usize,
) -> Result<Vec<(u64, u64, Vec<ColumnValue>, i64)>, OpError> {
    let prefix = view_output_prefix(op_id);
    let entries = reader
        .scan_prefix(&prefix)
        .await
        .map_err(OpError::storage)?;
    Ok(decode_view_output_entries(entries, num_cols))
}

fn view_output_prefix(op_id: OperatorId) -> Vec<u8> {
    let mut p = Vec::with_capacity(9);
    p.push(ShardPrefix::ViewOutput.as_byte());
    p.extend_from_slice(&op_id.0.to_be_bytes());
    p
}

fn decode_view_output_entries(
    entries: Vec<(bytes::Bytes, bytes::Bytes)>,
    num_cols: usize,
) -> Vec<(u64, u64, Vec<ColumnValue>, i64)> {
    let mut rows = Vec::new();
    for (key, value) in entries {
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
    rows
}

fn serialize_column_value(value: &ColumnValue) -> String {
    match value {
        ColumnValue::Null => "n".to_string(),
        ColumnValue::Int64(v) => format!("i:{v}"),
        ColumnValue::Utf8(v) => format!("s:{}:{v}", v.len()),
        ColumnValue::Boolean(v) => format!("b:{}", u8::from(*v)),
        ColumnValue::Float64(v) => format!("f:{:016x}", v.to_bits()),
    }
}

fn serialize_pk(row: &[ColumnValue], pk: &[usize]) -> String {
    if pk.is_empty() {
        return row
            .iter()
            .map(serialize_column_value)
            .collect::<Vec<_>>()
            .join("|");
    }
    pk.iter()
        .filter_map(|idx| row.get(*idx))
        .map(serialize_column_value)
        .collect::<Vec<_>>()
        .join("|")
}

/// Collapse a `ViewSinkOp`'s stored `(epoch, row_index, columns, weight)`
/// delta log into current materialized state, keyed by `pk` (or the whole
/// row, if `pk` is empty). A row's weight reaching `0` removes it.
pub type MaterializedViewState = std::collections::BTreeMap<String, (Vec<ColumnValue>, i64)>;

pub fn materialize_view_state(
    rows: Vec<(u64, u64, Vec<ColumnValue>, i64)>,
    pk: &[usize],
) -> MaterializedViewState {
    let mut state = std::collections::BTreeMap::new();
    for (_epoch, _row_idx, row, weight) in rows {
        let key = serialize_pk(&row, pk);
        let mut remove_key = false;
        match state.entry(key.clone()) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let (stored_row, count) = entry.get_mut();
                *stored_row = row;
                *count += weight;
                remove_key = *count == 0;
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                if weight != 0 {
                    entry.insert((row, weight));
                }
            }
        }
        if remove_key {
            state.remove(&key);
        }
    }
    state
}

/// Serialize one materialized-state row as a tab-separated byte string
/// (the wire format `ViewReader` implementations and pgwire's row encoder
/// both expect).
pub fn column_values_to_tsv_bytes(row: &[ColumnValue]) -> Vec<u8> {
    row.iter()
        .map(|value| match value {
            ColumnValue::Null => r"\N".to_string(),
            ColumnValue::Int64(v) => v.to_string(),
            ColumnValue::Utf8(v) => v.clone(),
            ColumnValue::Boolean(v) => v.to_string(),
            ColumnValue::Float64(v) => v.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\t")
        .into_bytes()
}

// ── View directory: `view_name -> (op_id, num_cols, pk)` ─────────────────────
//
// A compiled view's `ViewSinkOp` storage is keyed by `OperatorId`, an
// in-process-only value at `CREATE VIEW` time — a reader with no shared
// in-memory catalog (a standalone `--role gateway` process reading a
// published/remote shard via `ShardReader`, v0.51.4 Slice 8) has no other
// way to resolve `view_name -> OperatorId`/column count/primary key. This
// small directory entry, written once per view at compile time, is that
// resolution path — the multi-shard publish/read mechanism's only
// remaining dependency on a name-keyed record now that the legacy
// `view_materializer.rs` (which used to write a whole snapshot under
// `view_output/{view_name}/`) is gone.

const VIEW_DIRECTORY_TAG: &[u8] = b"vwdir:";

fn view_directory_key(view_name: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + VIEW_DIRECTORY_TAG.len() + view_name.len());
    k.push(ShardPrefix::ShardMeta.as_byte());
    k.extend_from_slice(VIEW_DIRECTORY_TAG);
    k.extend_from_slice(view_name.as_bytes());
    k
}

fn encode_view_directory_entry(op_id: OperatorId, num_cols: usize, pk: &[usize]) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + 4 + 1 + pk.len() * 2);
    v.extend_from_slice(&op_id.0.to_be_bytes());
    v.extend_from_slice(&(num_cols as u32).to_be_bytes());
    v.push(pk.len() as u8);
    for &idx in pk {
        v.extend_from_slice(&(idx as u16).to_be_bytes());
    }
    v
}

fn decode_view_directory_entry(bytes: &[u8]) -> Option<(OperatorId, usize, Vec<usize>)> {
    let op_id = OperatorId(u64::from_be_bytes(bytes.get(0..8)?.try_into().ok()?));
    let num_cols = u32::from_be_bytes(bytes.get(8..12)?.try_into().ok()?) as usize;
    let pk_len = *bytes.get(12)? as usize;
    let mut pk = Vec::with_capacity(pk_len);
    let mut pos = 13usize;
    for _ in 0..pk_len {
        let idx = u16::from_be_bytes(bytes.get(pos..pos + 2)?.try_into().ok()?) as usize;
        pk.push(idx);
        pos += 2;
    }
    Some((op_id, num_cols, pk))
}

/// Write `view_name`'s directory entry (called once, right after
/// `compile_plan` succeeds for it at `CREATE VIEW`/`CREATE MATERIALIZED
/// VIEW` time — the entry never changes afterward, since a compiled view's
/// column set/primary key is fixed at creation).
pub async fn write_view_directory_entry(
    db: &ShardDb,
    view_name: &str,
    op_id: OperatorId,
    num_cols: usize,
    pk: &[usize],
) -> Result<(), OpError> {
    let key = view_directory_key(view_name);
    let value = encode_view_directory_entry(op_id, num_cols, pk);
    let mut wb = WriteBatch::new();
    wb.put(&key, &value);
    db.write_batch(wb).await.map_err(OpError::storage)
}

/// Resolve `view_name`'s directory entry directly from a local `ShardDb`
/// (the same-process read path — see [`read_view_directory_entry_via_reader`]
/// for the cross-process/multi-shard equivalent).
pub async fn read_view_directory_entry(
    db: &ShardDb,
    view_name: &str,
) -> Result<Option<(OperatorId, usize, Vec<usize>)>, OpError> {
    let key = view_directory_key(view_name);
    let value = db.get(&key).await.map_err(OpError::storage)?;
    Ok(value.and_then(|bytes| decode_view_directory_entry(&bytes)))
}

/// Resolve `view_name`'s directory entry through a read-only `ShardReader`
/// (the cross-process/multi-shard read path).
pub async fn read_view_directory_entry_via_reader(
    reader: &rockstream_storage::ShardReader,
    view_name: &str,
) -> Result<Option<(OperatorId, usize, Vec<usize>)>, OpError> {
    let key = view_directory_key(view_name);
    let value = reader.get(&key).await.map_err(OpError::storage)?;
    Ok(value.and_then(|bytes| decode_view_directory_entry(&bytes)))
}

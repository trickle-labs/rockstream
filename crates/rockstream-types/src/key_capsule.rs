//! Canonical typed join keys shared by physical consumers.

use arrow::array::{
    Array, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Int64Array, StringArray,
    UInt64Array, UInt64Builder,
};
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const NULL_TAG: u8 = 0;
const INT64_TAG: u8 = 1;
const UTF8_TAG: u8 = 2;
const COMPOSITE_TAG: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum KeyValue {
    Null,
    Int64(i64),
    Utf8(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyCapsuleError {
    UnsupportedType(String),
    InvalidEncoding,
}

impl fmt::Display for KeyCapsuleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedType(data_type) => write!(f, "unsupported key type {data_type}"),
            Self::InvalidEncoding => write!(f, "invalid key capsule encoding"),
        }
    }
}

impl std::error::Error for KeyCapsuleError {}

/// One canonical representation for lookup, partitioning, shuffle, skew, and storage.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyCapsule {
    typed_bytes: Vec<u8>,
    stable_hash: u128,
}

impl KeyCapsule {
    pub fn from_values(values: &[KeyValue]) -> Result<Self, KeyCapsuleError> {
        if values.is_empty() {
            return Err(KeyCapsuleError::InvalidEncoding);
        }
        let typed_bytes = if values.len() == 1 {
            encode_value(&values[0])
        } else {
            let payload: Vec<u8> = values.iter().flat_map(encode_value).collect();
            frame(COMPOSITE_TAG, &payload)
        };
        Ok(Self::from_typed_bytes(typed_bytes))
    }

    pub fn from_array(array: &dyn Array, row: usize) -> Result<Self, KeyCapsuleError> {
        if array.is_null(row) {
            return Self::from_values(&[KeyValue::Null]);
        }
        let value = if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            KeyValue::Int64(values.value(row))
        } else if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            KeyValue::Utf8(values.value(row).to_owned())
        } else {
            return Err(KeyCapsuleError::UnsupportedType(
                array.data_type().to_string(),
            ));
        };
        Self::from_values(&[value])
    }

    pub fn from_arrays(arrays: &[&dyn Array], row: usize) -> Result<Self, KeyCapsuleError> {
        let values = arrays
            .iter()
            .map(|array| {
                if array.is_null(row) {
                    Ok(KeyValue::Null)
                } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
                    Ok(KeyValue::Int64(values.value(row)))
                } else if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
                    Ok(KeyValue::Utf8(values.value(row).to_owned()))
                } else {
                    Err(KeyCapsuleError::UnsupportedType(
                        array.data_type().to_string(),
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_values(&values)
    }

    pub fn from_typed_bytes(typed_bytes: Vec<u8>) -> Self {
        Self {
            stable_hash: stable_hash(&typed_bytes),
            typed_bytes,
        }
    }

    pub fn typed_bytes(&self) -> &[u8] {
        &self.typed_bytes
    }

    pub fn stable_hash(&self) -> u128 {
        self.stable_hash
    }

    pub fn is_null(&self) -> bool {
        self.typed_bytes.first() == Some(&NULL_TAG)
    }

    pub fn contains_null(&self) -> bool {
        if self.is_null() {
            return true;
        }
        if self.typed_bytes.first() != Some(&COMPOSITE_TAG) || self.typed_bytes.len() < 5 {
            return false;
        }
        let mut offset = 5;
        while offset + 5 <= self.typed_bytes.len() {
            let tag = self.typed_bytes[offset];
            let len = u32::from_be_bytes(
                self.typed_bytes[offset + 1..offset + 5]
                    .try_into()
                    .expect("checked capsule frame"),
            ) as usize;
            if tag == NULL_TAG {
                return true;
            }
            offset = offset.saturating_add(5 + len);
        }
        false
    }
}

fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + payload.len());
    bytes.push(tag);
    bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn encode_value(value: &KeyValue) -> Vec<u8> {
    match value {
        KeyValue::Null => frame(NULL_TAG, &[]),
        KeyValue::Int64(value) => frame(INT64_TAG, &value.to_be_bytes()),
        KeyValue::Utf8(value) => frame(UTF8_TAG, value.as_bytes()),
    }
}

fn stable_hash(bytes: &[u8]) -> u128 {
    let mut hash: u128 = 144_066_263_297_769_815_596_495_629_667_062_367_629;
    let prime: u128 = 309_485_009_821_345_068_724_781_371;
    for byte in bytes {
        hash ^= *byte as u128;
        hash = hash.wrapping_mul(prime);
    }
    hash
}

static BATCH_KEY_CAPSULE_ENCODINGS_GLOBAL: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static BATCH_KEY_CAPSULE_ENCODINGS_LOCAL: Cell<u64> = const { Cell::new(0) };
}

/// Global counter of `BatchKeyCapsule::from_record_batch` invocations.
pub fn batch_key_capsule_encodings_global() -> u64 {
    BATCH_KEY_CAPSULE_ENCODINGS_GLOBAL.load(Ordering::Relaxed)
}

/// Thread-local counter of `BatchKeyCapsule::from_record_batch` invocations.
pub fn batch_key_capsule_encodings_local() -> u64 {
    BATCH_KEY_CAPSULE_ENCODINGS_LOCAL.with(Cell::get)
}

/// Reset the thread-local batch key capsule encoding counter.
pub fn reset_batch_key_capsule_encodings_local() {
    BATCH_KEY_CAPSULE_ENCODINGS_LOCAL.with(|c| c.set(0));
}

/// Measure exact `BatchKeyCapsule` batch encodings executed within `f` on the current thread.
pub fn measure_batch_key_capsule_encodings<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let before = batch_key_capsule_encodings_local();
    let result = f();
    let after = batch_key_capsule_encodings_local();
    (result, after.saturating_sub(before))
}

/// Logical key specification identifying the column indices encoded in a `BatchKeyCapsule`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeySpec {
    pub columns: Vec<usize>,
}

impl KeySpec {
    pub fn new(columns: impl Into<Vec<usize>>) -> Self {
        Self {
            columns: columns.into(),
        }
    }
}

enum TypedKeyColumn<'a> {
    Int64(&'a Int64Array),
    Utf8(&'a StringArray),
}

impl<'a> TypedKeyColumn<'a> {
    fn from_array(array: &'a dyn Array) -> Result<Self, KeyCapsuleError> {
        if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            Ok(Self::Int64(arr))
        } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            Ok(Self::Utf8(arr))
        } else {
            Err(KeyCapsuleError::UnsupportedType(
                array.data_type().to_string(),
            ))
        }
    }

    #[inline]
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Int64(arr) => arr.is_null(row),
            Self::Utf8(arr) => arr.is_null(row),
        }
    }

    #[inline]
    fn append_encoded(&self, row: usize, out: &mut Vec<u8>) {
        if self.is_null(row) {
            out.push(NULL_TAG);
            out.extend_from_slice(&0u32.to_be_bytes());
            return;
        }
        match self {
            Self::Int64(arr) => {
                out.push(INT64_TAG);
                out.extend_from_slice(&8u32.to_be_bytes());
                out.extend_from_slice(&arr.value(row).to_be_bytes());
            }
            Self::Utf8(arr) => {
                let s = arr.value(row).as_bytes();
                out.push(UTF8_TAG);
                out.extend_from_slice(&(s.len() as u32).to_be_bytes());
                out.extend_from_slice(s);
            }
        }
    }
}

/// Once-per-batch canonical key encoding and hash capsule shared across
/// exchange routing, stateless fusion, join probes, aggregates, and storage keys.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchKeyCapsule {
    pub spec: KeySpec,
    pub encoded_rows: Arc<BinaryArray>,
    pub hashes_u64: Arc<UInt64Array>,
    pub hashes_u128: Arc<Vec<u128>>,
    pub contains_null: Arc<BooleanArray>,
}

impl BatchKeyCapsule {
    /// Encode all rows of `batch` for `key_cols` in a single vectorized pass.
    ///
    /// Byte-for-byte and hash-for-hash identical to calling `KeyCapsule::from_arrays`
    /// per row, while avoiding per-row downcasts and per-row Vec allocations.
    pub fn from_record_batch(
        batch: &RecordBatch,
        key_cols: &[usize],
    ) -> Result<Self, KeyCapsuleError> {
        if key_cols.is_empty() {
            return Err(KeyCapsuleError::InvalidEncoding);
        }
        let mut typed_cols = Vec::with_capacity(key_cols.len());
        for &col_idx in key_cols {
            if col_idx >= batch.num_columns() {
                return Err(KeyCapsuleError::InvalidEncoding);
            }
            typed_cols.push(TypedKeyColumn::from_array(batch.column(col_idx).as_ref())?);
        }

        BATCH_KEY_CAPSULE_ENCODINGS_GLOBAL.fetch_add(1, Ordering::Relaxed);
        BATCH_KEY_CAPSULE_ENCODINGS_LOCAL.with(|c| c.set(c.get().saturating_add(1)));

        let n = batch.num_rows();
        let mut binary_builder = BinaryBuilder::with_capacity(n, n * 16);
        let mut hashes_u64_builder = UInt64Builder::with_capacity(n);
        let mut hashes_u128 = Vec::with_capacity(n);
        let mut null_builder = BooleanBuilder::with_capacity(n);
        let mut scratch = Vec::with_capacity(32);
        let mut composite_payload = Vec::with_capacity(32);

        let is_single = typed_cols.len() == 1;
        for row in 0..n {
            scratch.clear();
            let mut row_has_null = false;
            if is_single {
                row_has_null = typed_cols[0].is_null(row);
                typed_cols[0].append_encoded(row, &mut scratch);
            } else {
                composite_payload.clear();
                for col in &typed_cols {
                    if col.is_null(row) {
                        row_has_null = true;
                    }
                    col.append_encoded(row, &mut composite_payload);
                }
                scratch.push(COMPOSITE_TAG);
                scratch.extend_from_slice(&(composite_payload.len() as u32).to_be_bytes());
                scratch.extend_from_slice(&composite_payload);
            }
            let h128 = stable_hash(&scratch);
            let h64 = (h128 as u64) ^ ((h128 >> 64) as u64);
            binary_builder.append_value(&scratch);
            hashes_u64_builder.append_value(h64);
            hashes_u128.push(h128);
            null_builder.append_value(row_has_null);
        }

        Ok(Self {
            spec: KeySpec::new(key_cols.to_vec()),
            encoded_rows: Arc::new(binary_builder.finish()),
            hashes_u64: Arc::new(hashes_u64_builder.finish()),
            hashes_u128: Arc::new(hashes_u128),
            contains_null: Arc::new(null_builder.finish()),
        })
    }

    /// Build a `BatchKeyCapsule` directly from pre-encoded row byte slices (e.g. propagating
    /// already-canonical join keys into join output without re-encoding from Arrow columns).
    pub fn from_encoded_slices<'a>(
        key_cols: Vec<usize>,
        rows: impl ExactSizeIterator<Item = (&'a [u8], u128, bool)>,
    ) -> Self {
        let n = rows.len();
        let mut binary_builder = BinaryBuilder::with_capacity(n, n * 16);
        let mut hashes_u64_builder = UInt64Builder::with_capacity(n);
        let mut hashes_u128 = Vec::with_capacity(n);
        let mut null_builder = BooleanBuilder::with_capacity(n);
        for (bytes, h128, has_null) in rows {
            let h64 = (h128 as u64) ^ ((h128 >> 64) as u64);
            binary_builder.append_value(bytes);
            hashes_u64_builder.append_value(h64);
            hashes_u128.push(h128);
            null_builder.append_value(has_null);
        }
        Self {
            spec: KeySpec::new(key_cols),
            encoded_rows: Arc::new(binary_builder.finish()),
            hashes_u64: Arc::new(hashes_u64_builder.finish()),
            hashes_u128: Arc::new(hashes_u128),
            contains_null: Arc::new(null_builder.finish()),
        }
    }

    pub fn len(&self) -> usize {
        self.encoded_rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.encoded_rows.is_empty()
    }

    #[inline]
    pub fn row_bytes(&self, row: usize) -> &[u8] {
        self.encoded_rows.value(row)
    }

    #[inline]
    pub fn row_hash_u64(&self, row: usize) -> u64 {
        self.hashes_u64.value(row)
    }

    #[inline]
    pub fn row_hash_u128(&self, row: usize) -> u128 {
        self.hashes_u128[row]
    }

    #[inline]
    pub fn row_contains_null(&self, row: usize) -> bool {
        self.contains_null.value(row)
    }

    /// Gather rows at `indices` without re-encoding or re-hashing any keys.
    pub fn select_rows(&self, indices: &[usize]) -> Self {
        let mut binary_builder = BinaryBuilder::with_capacity(indices.len(), indices.len() * 16);
        let mut hashes_u64_builder = UInt64Builder::with_capacity(indices.len());
        let mut hashes_u128 = Vec::with_capacity(indices.len());
        let mut null_builder = BooleanBuilder::with_capacity(indices.len());
        for &idx in indices {
            binary_builder.append_value(self.encoded_rows.value(idx));
            hashes_u64_builder.append_value(self.hashes_u64.value(idx));
            hashes_u128.push(self.hashes_u128[idx]);
            null_builder.append_value(self.contains_null.value(idx));
        }
        Self {
            spec: self.spec.clone(),
            encoded_rows: Arc::new(binary_builder.finish()),
            hashes_u64: Arc::new(hashes_u64_builder.finish()),
            hashes_u128: Arc::new(hashes_u128),
            contains_null: Arc::new(null_builder.finish()),
        }
    }

    /// Return a zero-copy clone of this capsule mapped to new output column indices.
    pub fn with_remapped_columns(&self, new_columns: Vec<usize>) -> Self {
        Self {
            spec: KeySpec::new(new_columns),
            encoded_rows: Arc::clone(&self.encoded_rows),
            hashes_u64: Arc::clone(&self.hashes_u64),
            hashes_u128: Arc::clone(&self.hashes_u128),
            contains_null: Arc::clone(&self.contains_null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn batch_key_capsule_matches_row_key_capsule_byte_for_byte() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Int64, true),
            Field::new("k2", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(10), None, Some(-7)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .unwrap();

        let (single, single_count) = measure_batch_key_capsule_encodings(|| {
            BatchKeyCapsule::from_record_batch(&batch, &[0]).unwrap()
        });
        assert_eq!(single_count, 1);
        for row in 0..batch.num_rows() {
            let expected = KeyCapsule::from_array(batch.column(0).as_ref(), row).unwrap();
            assert_eq!(single.row_bytes(row), expected.typed_bytes());
            assert_eq!(single.row_hash_u128(row), expected.stable_hash());
            assert_eq!(single.row_contains_null(row), expected.contains_null());
        }

        let (composite, composite_count) = measure_batch_key_capsule_encodings(|| {
            BatchKeyCapsule::from_record_batch(&batch, &[0, 1]).unwrap()
        });
        assert_eq!(composite_count, 1);
        let arrays: Vec<&dyn Array> = vec![batch.column(0).as_ref(), batch.column(1).as_ref()];
        for row in 0..batch.num_rows() {
            let expected = KeyCapsule::from_arrays(&arrays, row).unwrap();
            assert_eq!(composite.row_bytes(row), expected.typed_bytes());
            assert_eq!(composite.row_hash_u128(row), expected.stable_hash());
            assert_eq!(composite.row_contains_null(row), expected.contains_null());
        }

        let (selected, select_count) =
            measure_batch_key_capsule_encodings(|| composite.select_rows(&[2, 0]));
        assert_eq!(select_count, 0);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected.row_bytes(0), composite.row_bytes(2));
        assert_eq!(selected.row_bytes(1), composite.row_bytes(0));
    }
}

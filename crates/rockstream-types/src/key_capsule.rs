//! Canonical typed join keys shared by physical consumers.

use arrow::array::{Array, Int64Array, StringArray};
use serde::{Deserialize, Serialize};
use std::fmt;

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

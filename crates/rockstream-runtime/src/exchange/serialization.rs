use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::arrow_batch::{append_weight_column, split_weight_column};
use rockstream_types::error_code::RS_3017;

/// Serialize an ArrowZSet to a binary payload using Arrow IPC stream format.
/// The `_weight` column is appended as the last column of the batch.
pub fn serialize_zset(zset: &ArrowZSet) -> Result<Bytes, String> {
    if zset.is_empty() {
        // Return empty bytes for empty Z-sets
        return Ok(Bytes::new());
    }

    let weighted_batch = append_weight_column(zset.data.clone(), &zset.weights)
        .map_err(|e| format!("Failed to append weight column: {:?}", e))?;

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &weighted_batch.schema())
            .map_err(|e| format!("Failed to create IPC stream writer: {:?}", e))?;
        writer
            .write(&weighted_batch)
            .map_err(|e| format!("Failed to write IPC batch: {:?}", e))?;
        writer
            .finish()
            .map_err(|e| format!("Failed to finish IPC stream: {:?}", e))?;
    }

    Ok(Bytes::from(buffer))
}

/// Deserialize an ArrowZSet from a binary payload using Arrow IPC stream format.
/// Splitting the `_weight` column back into the Z-set representation.
pub fn deserialize_zset(
    payload: &[u8],
    schema: arrow::datatypes::SchemaRef,
) -> Result<ArrowZSet, String> {
    if payload.is_empty() {
        return Ok(ArrowZSet::empty(schema));
    }

    let cursor = Cursor::new(payload);
    let mut reader = StreamReader::try_new(cursor, None)
        .map_err(|e| format!("Failed to create IPC stream reader: {:?}", e))?;

    let weighted_batch = match reader.next() {
        Some(Ok(batch)) => batch,
        Some(Err(e)) => {
            return Err(format!(
                "[{RS_3017}] Failed to read IPC batch: {:?}. Next steps: inspect the Arrow IPC shuffle payload for truncation or a writer/reader version mismatch.",
                e
            ))
        }
        None => {
            return Err(format!(
                "[{RS_3017}] Empty Arrow IPC stream. Next steps: inspect the Arrow IPC shuffle payload for truncation or a writer/reader version mismatch."
            ))
        }
    };

    let (data, weights) = split_weight_column(&weighted_batch)
        .ok_or_else(|| "Failed to split weight column from deserialized batch".to_string())?;

    Ok(ArrowZSet::new(data, weights))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_roundtrip() {
        let original = ArrowZSet::from_ab_rows(&[(1, 100), (2, 200)], 1);
        let bytes = serialize_zset(&original).unwrap();

        let schema = original.schema();
        let recovered = deserialize_zset(&bytes, schema).unwrap();

        assert_eq!(recovered.num_rows(), 2);
        assert_eq!(recovered.weights, vec![1, 1]);
        assert_eq!(recovered.positive_ab_rows(), vec![(1, 100), (2, 200)]);
    }

    #[test]
    fn test_empty_serialization_roundtrip() {
        let original = ArrowZSet::from_ab_rows(&[], 1);
        let bytes = serialize_zset(&original).unwrap();
        assert!(bytes.is_empty());

        let schema = original.schema();
        let recovered = deserialize_zset(&bytes, schema).unwrap();
        assert!(recovered.is_empty());
    }
}

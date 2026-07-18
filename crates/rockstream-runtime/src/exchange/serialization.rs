use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::arrow_batch::{append_weight_column, split_weight_column};
use rockstream_types::error_code::{RS_3017, RS_3020};
use rockstream_types::exchange::ShuffleCompression;

const FRAME_MAGIC: &[u8; 4] = b"RSF1";
const FRAME_HEADER_LEN: usize = 4 + 1 + 1 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PayloadHeader {
    wire_version: u8,
    codec: ShuffleCompression,
    uncompressed_len: u64,
}

/// Serialize an ArrowZSet to a binary payload using Arrow IPC stream format.
/// The `_weight` column is appended as the last column of the batch.
pub fn serialize_zset(zset: &ArrowZSet) -> Result<Bytes, String> {
    serialize_zset_with_compression(zset, ShuffleCompression::None, false)
}

pub fn serialize_zset_with_compression(
    zset: &ArrowZSet,
    compression: ShuffleCompression,
    codec_capability_floor: bool,
) -> Result<Bytes, String> {
    if zset.is_empty() {
        return Ok(Bytes::new());
    }
    let raw = serialize_raw_ipc(zset)?;
    if !codec_capability_floor || matches!(compression, ShuffleCompression::None) {
        return Ok(raw);
    }
    encode_frame(&raw, compression)
}

pub fn frame_payload_bytes(
    payload: &[u8],
    compression: ShuffleCompression,
    codec_capability_floor: bool,
) -> Result<Bytes, String> {
    if !codec_capability_floor || matches!(compression, ShuffleCompression::None) {
        return Ok(Bytes::copy_from_slice(payload));
    }
    if framed_payload_codec(payload).is_some() {
        return Ok(Bytes::copy_from_slice(payload));
    }
    encode_frame(payload, compression)
}

pub fn framed_payload_codec(payload: &[u8]) -> Option<ShuffleCompression> {
    if payload.len() < FRAME_HEADER_LEN || &payload[..4] != FRAME_MAGIC {
        return None;
    }
    decode_codec(payload[5]).ok()
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

    let decoded = decode_payload(payload)?;
    deserialize_raw_ipc(decoded.as_ref(), schema)
}

fn serialize_raw_ipc(zset: &ArrowZSet) -> Result<Bytes, String> {
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

fn deserialize_raw_ipc(
    payload: &[u8],
    _schema: arrow::datatypes::SchemaRef,
) -> Result<ArrowZSet, String> {
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

fn encode_frame(payload: &[u8], compression: ShuffleCompression) -> Result<Bytes, String> {
    let compressed = match compression {
        ShuffleCompression::None => payload.to_vec(),
        ShuffleCompression::Lz4 => lz4_flex::compress_prepend_size(payload),
        ShuffleCompression::Zstd => zstd::bulk::compress(payload, 1)
            .map_err(|e| codec_error(format!("zstd compression failed: {e}")))?,
    };
    let mut framed = Vec::with_capacity(FRAME_HEADER_LEN + compressed.len());
    framed.extend_from_slice(FRAME_MAGIC);
    framed.push(1);
    framed.push(codec_byte(compression));
    framed.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    framed.extend_from_slice(&compressed);
    Ok(Bytes::from(framed))
}

fn decode_payload(payload: &[u8]) -> Result<Bytes, String> {
    if payload.len() < FRAME_HEADER_LEN || &payload[..4] != FRAME_MAGIC {
        return Ok(Bytes::copy_from_slice(payload));
    }
    let header = decode_header(payload)?;
    let encoded = &payload[FRAME_HEADER_LEN..];
    let decoded = match header.codec {
        ShuffleCompression::None => Bytes::copy_from_slice(encoded),
        ShuffleCompression::Lz4 => Bytes::from(
            lz4_flex::decompress_size_prepended(encoded)
                .map_err(|e| codec_error(format!("lz4 decompression failed: {e}")))?,
        ),
        ShuffleCompression::Zstd => Bytes::from(
            zstd::bulk::decompress(encoded, header.uncompressed_len as usize)
                .map_err(|e| codec_error(format!("zstd decompression failed: {e}")))?,
        ),
    };
    if decoded.len() as u64 != header.uncompressed_len {
        return Err(codec_error(format!(
            "decoded length {} did not match header {}",
            decoded.len(),
            header.uncompressed_len
        )));
    }
    Ok(decoded)
}

fn decode_header(payload: &[u8]) -> Result<PayloadHeader, String> {
    let wire_version = payload[4];
    if wire_version != 1 {
        return Err(codec_error(format!(
            "unsupported shuffle payload wire_version {wire_version}"
        )));
    }
    let codec = decode_codec(payload[5])?;
    let uncompressed_len = u64::from_be_bytes(
        payload[6..FRAME_HEADER_LEN]
            .try_into()
            .expect("frame header length is fixed"),
    );
    Ok(PayloadHeader {
        wire_version,
        codec,
        uncompressed_len,
    })
}

fn codec_byte(codec: ShuffleCompression) -> u8 {
    match codec {
        ShuffleCompression::None => 0,
        ShuffleCompression::Lz4 => 1,
        ShuffleCompression::Zstd => 2,
    }
}

fn decode_codec(codec: u8) -> Result<ShuffleCompression, String> {
    match codec {
        0 => Ok(ShuffleCompression::None),
        1 => Ok(ShuffleCompression::Lz4),
        2 => Ok(ShuffleCompression::Zstd),
        other => Err(codec_error(format!("unknown shuffle codec {other}"))),
    }
}

fn codec_error(detail: String) -> String {
    format!(
        "[{RS_3020}] {detail}. Next steps: verify both peers advertise shuffle_codec_v1, inspect the payload bytes for corruption, and retry after rolling the cluster to a compatible build."
    )
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

    #[test]
    fn legacy_raw_arrow_ipc_decodes_under_codec_v1() {
        let original = ArrowZSet::from_ab_rows(&[(3, 30), (4, 40)], 1);
        let raw = serialize_zset(&original).unwrap();
        let recovered = deserialize_zset(&raw, original.schema()).unwrap();
        assert_eq!(recovered.positive_ab_rows(), vec![(3, 30), (4, 40)]);
        assert_eq!(recovered.weights, vec![1, 1]);
    }

    #[test]
    fn lz4_shuffle_payload_roundtrip_is_exact() {
        let original = ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1);
        let framed =
            serialize_zset_with_compression(&original, ShuffleCompression::Lz4, true).unwrap();
        assert_eq!(&framed[..4], FRAME_MAGIC);
        assert_eq!(framed[5], codec_byte(ShuffleCompression::Lz4));
        let recovered = deserialize_zset(&framed, original.schema()).unwrap();
        assert_eq!(recovered.positive_ab_rows(), vec![(5, 50), (6, 60)]);
        assert_eq!(recovered.weights, vec![1, 1]);
    }

    #[test]
    fn zstd_shuffle_payload_roundtrip_is_exact() {
        let original = ArrowZSet::from_ab_rows(&[(7, 70), (8, 80)], 1);
        let framed =
            serialize_zset_with_compression(&original, ShuffleCompression::Zstd, true).unwrap();
        assert_eq!(&framed[..4], FRAME_MAGIC);
        assert_eq!(framed[5], codec_byte(ShuffleCompression::Zstd));
        let recovered = deserialize_zset(&framed, original.schema()).unwrap();
        assert_eq!(recovered.positive_ab_rows(), vec![(7, 70), (8, 80)]);
        assert_eq!(recovered.weights, vec![1, 1]);
    }

    #[test]
    fn unknown_shuffle_codec_returns_registered_error() {
        let mut payload = Vec::new();
        payload.extend_from_slice(FRAME_MAGIC);
        payload.push(1);
        payload.push(99);
        payload.extend_from_slice(&10u64.to_be_bytes());
        payload.extend_from_slice(b"garbage");
        let error = deserialize_zset(&payload, ArrowZSet::from_ab_rows(&[(1, 10)], 1).schema())
            .unwrap_err();
        assert!(error.contains("RS-3020"));
        assert!(error.contains("unknown shuffle codec 99"));
    }
}

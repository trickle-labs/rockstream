//! ExchangeFrame Contract & Negative Validation Matrix Tests (v0.67 Slice 2 / Phase 3a).
//!
//! Validates all 9 fields of ExchangeFrame, ensuring strict pre-allocation validation
//! and registered deterministic error codes (Table 4.1, Roadmap 13.2).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::exchange::proto::ExchangeFrame;
use rockstream_runtime::exchange::serialization::{
    build_exchange_frame, compute_frame_checksum, compute_schema_fingerprint,
    decode_exchange_frame, validate_exchange_frame, CURRENT_EXCHANGE_PROTOCOL_VERSION,
    DEFAULT_MAX_BATCH_BYTES,
};
use rockstream_types::error_code::{RS_3001, RS_3004, RS_3006, RS_3017, RS_3018, RS_3019, RS_3020};

fn sample_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, true),
    ]))
}

fn sample_different_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]))
}

#[test]
fn test_frame_round_trip_exact_success() {
    let schema = sample_schema();
    let original = ArrowZSet::from_ab_rows(&[(1, 100), (2, 200)], 1);
    let frame = build_exchange_frame(1, 2, 3, 10, 5, &schema, &original).unwrap();

    let fingerprint = compute_schema_fingerprint(&schema);
    let val_result = validate_exchange_frame(
        &frame,
        5, // active lease = 5 (matching)
        Some(&fingerprint),
        DEFAULT_MAX_BATCH_BYTES,
    );
    assert!(
        val_result.is_ok(),
        "Valid frame must pass validation: {:?}",
        val_result
    );

    let decoded = decode_exchange_frame(&frame, schema).unwrap();
    assert_eq!(decoded.num_rows(), 2);
    assert_eq!(decoded.weights, vec![1, 1]);
    assert_eq!(decoded.positive_ab_rows(), vec![(1, 100), (2, 200)]);
}

#[test]
fn test_frame_rejects_incompatible_protocol_version() {
    let schema = sample_schema();
    let original = ArrowZSet::from_ab_rows(&[(1, 100)], 1);
    let mut frame = build_exchange_frame(1, 2, 3, 10, 5, &schema, &original).unwrap();
    frame.protocol_version = 99; // Incompatible version

    let val_result = validate_exchange_frame(&frame, 5, None, DEFAULT_MAX_BATCH_BYTES);
    match val_result {
        Err(err) => {
            assert_eq!(err.error_code(), RS_3001);
            let display = format!("{err}");
            assert!(display.contains("RS-3001"));
            assert!(display.contains("unsupported exchange protocol version 99"));
        }
        Ok(_) => panic!("Expected failure on incompatible protocol version"),
    }
}

#[test]
fn test_frame_rejects_corrupted_checksum() {
    let schema = sample_schema();
    let original = ArrowZSet::from_ab_rows(&[(1, 100)], 1);
    let mut frame = build_exchange_frame(1, 2, 3, 10, 5, &schema, &original).unwrap();
    frame.checksum[0] ^= 0xff; // Corrupt checksum

    let val_result = validate_exchange_frame(&frame, 5, None, DEFAULT_MAX_BATCH_BYTES);
    match val_result {
        Err(err) => {
            assert_eq!(err.error_code(), RS_3019);
            let display = format!("{err}");
            assert!(display.contains("RS-3019"));
            assert!(display.contains("corrupt frame checksum"));
        }
        Ok(_) => panic!("Expected failure on corrupt checksum"),
    }
}

#[test]
fn test_frame_rejects_schema_fingerprint_mismatch() {
    let schema = sample_schema();
    let different_schema = sample_different_schema();
    let original = ArrowZSet::from_ab_rows(&[(1, 100)], 1);
    let frame = build_exchange_frame(1, 2, 3, 10, 5, &schema, &original).unwrap();

    let expected_fingerprint = compute_schema_fingerprint(&different_schema);
    let val_result = validate_exchange_frame(
        &frame,
        5,
        Some(&expected_fingerprint),
        DEFAULT_MAX_BATCH_BYTES,
    );
    match val_result {
        Err(err) => {
            assert_eq!(err.error_code(), RS_3018);
            let display = format!("{err}");
            assert!(display.contains("RS-3018"));
            assert!(display.contains("schema fingerprint mismatch"));
        }
        Ok(_) => panic!("Expected failure on schema fingerprint mismatch"),
    }
}

#[test]
fn test_frame_rejects_stale_lease_token() {
    let schema = sample_schema();
    let original = ArrowZSet::from_ab_rows(&[(1, 100)], 1);
    let frame = build_exchange_frame(1, 2, 3, 10, 4, &schema, &original).unwrap();

    // Active lease is 5, frame has lease 4 -> stale
    let val_result = validate_exchange_frame(&frame, 5, None, DEFAULT_MAX_BATCH_BYTES);
    match val_result {
        Err(err) => {
            assert_eq!(err.error_code(), RS_3004);
            let display = format!("{err}");
            assert!(display.contains("RS-3004"));
            assert!(display.contains("stale lease token 4 < active worker lease 5"));
        }
        Ok(_) => panic!("Expected failure on stale lease token"),
    }
}

#[test]
fn test_frame_rejects_oversized_payload() {
    let schema = sample_schema();
    let original = ArrowZSet::from_ab_rows(&[(1, 100)], 1);
    let frame = build_exchange_frame(1, 2, 3, 10, 5, &schema, &original).unwrap();

    // Limit to 10 bytes -> frame payload exceeds limit
    let val_result = validate_exchange_frame(&frame, 5, None, 10);
    match val_result {
        Err(err) => {
            assert_eq!(err.error_code(), RS_3006);
            let display = format!("{err}");
            assert!(display.contains("RS-3006"));
            assert!(display.contains("exceeds max_batch_bytes"));
        }
        Ok(_) => panic!("Expected failure on oversized payload"),
    }
}

#[test]
fn test_frame_rejects_truncated_payload() {
    let schema = sample_schema();
    let original = ArrowZSet::from_ab_rows(&[(1, 100)], 1);
    let mut frame = build_exchange_frame(1, 2, 3, 10, 5, &schema, &original).unwrap();

    // Truncate payload in half
    frame.payload.truncate(frame.payload.len() / 2);
    // Recompute valid checksum for this truncated payload so validation passes, but decode fails
    let checksum = compute_frame_checksum(
        frame.protocol_version,
        frame.workload_id,
        frame.shard_id,
        frame.operator_id,
        frame.epoch,
        frame.lease_token,
        &frame.schema_fingerprint,
        &frame.payload,
    );
    frame.checksum = checksum.to_vec();

    assert!(validate_exchange_frame(&frame, 5, None, DEFAULT_MAX_BATCH_BYTES).is_ok());

    let decode_result = decode_exchange_frame(&frame, schema);
    match decode_result {
        Err(err) => {
            assert_eq!(err.error_code(), RS_3017);
            let display = format!("{err}");
            assert!(display.contains("RS-3017"));
            assert!(display.contains("truncated frame payload"));
        }
        Ok(_) => panic!("Expected failure on truncated payload decode"),
    }
}

#[test]
fn test_frame_rejects_malformed_ipc_payload() {
    let schema = sample_schema();
    let fingerprint = compute_schema_fingerprint(&schema);
    let garbage_payload = vec![
        0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
        0x0c, 0x0d, 0x0e, 0x0f,
    ];
    let checksum = compute_frame_checksum(
        CURRENT_EXCHANGE_PROTOCOL_VERSION,
        1,
        2,
        3,
        10,
        5,
        &fingerprint,
        &garbage_payload,
    );

    let frame = ExchangeFrame {
        protocol_version: CURRENT_EXCHANGE_PROTOCOL_VERSION,
        workload_id: 1,
        shard_id: 2,
        operator_id: 3,
        epoch: 10,
        lease_token: 5,
        schema_fingerprint: fingerprint.to_vec(),
        payload: garbage_payload,
        checksum: checksum.to_vec(),
    };

    assert!(validate_exchange_frame(&frame, 5, None, DEFAULT_MAX_BATCH_BYTES).is_ok());

    let decode_result = decode_exchange_frame(&frame, schema);
    match decode_result {
        Err(err) => {
            assert_eq!(err.error_code(), RS_3020);
            let display = format!("{err}");
            assert!(display.contains("RS-3020"));
            assert!(display.contains("malformed Arrow IPC payload"));
        }
        Ok(_) => panic!("Expected failure on malformed IPC payload decode"),
    }
}

#[test]
fn test_exchange_frame_validation_and_rejection_matrix() {
    // Run all 8 matrix cases in sequence to fulfill the committed matrix assertion
    test_frame_round_trip_exact_success();
    test_frame_rejects_incompatible_protocol_version();
    test_frame_rejects_corrupted_checksum();
    test_frame_rejects_schema_fingerprint_mismatch();
    test_frame_rejects_stale_lease_token();
    test_frame_rejects_oversized_payload();
    test_frame_rejects_truncated_payload();
    test_frame_rejects_malformed_ipc_payload();
}

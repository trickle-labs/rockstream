//! v0.51.17 Untrusted-Input Panic Safety Tests — Connectors (Slice 3).
//!
//! Asserts that malformed, truncated, or adversarial byte sequences passed to
//! the Postgres CDC decoder boundaries (`pgoutput` / `wal2json`) never panic
//! the process and always return a structured `SourceError`.
//!
//! These tests satisfy the proof commitments from the coverage matrix in
//! `.claude/v0.51.17-plan.md § 3`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{CdcWireFormat, PostgresCdcSource};
use rockstream_types::ids::ConnectorId;

fn test_source(format: CdcWireFormat) -> PostgresCdcSource {
    PostgresCdcSource::new(
        ConnectorId(9999),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        format,
    )
}

/// Reachability: verify the `decode_and_enqueue` entry point is callable
/// without importing private modules. Truncated pgoutput header must return
/// `Err` (RS-4014 / InvalidPayload), not panic.
#[test]
fn cdc_pgoutput_truncated_tuple_returns_rs_error_no_panic() {
    let mut cdc = test_source(CdcWireFormat::PgOutput);
    // Truncated binary replication tuple: has header byte but is missing
    // all remaining required fields — triggers the field-count check.
    let truncated = b"B|0/16B3748|1001|I";
    let res = cdc.decode_and_enqueue(truncated);
    assert!(
        res.is_err(),
        "truncated pgoutput payload must return Err, not panic"
    );
    let err_msg = format!("{:?}", res.unwrap_err());
    assert!(
        err_msg.contains("pgoutput") || err_msg.contains("invalid"),
        "error must describe the invalid pgoutput payload, got: {err_msg}"
    );
}

/// Reachability: verify `decode_and_enqueue` with wal2json format is callable
/// and rejects malformed JSON at the entry point, returning Err (RS-4011).
#[test]
fn cdc_wal2json_malformed_json_returns_rs_error_no_panic() {
    let mut cdc = test_source(CdcWireFormat::Wal2Json);
    // Malformed JSON AST — missing closing bracket / invalid syntax.
    let malformed = b"{\"change\": [invalid json";
    let res = cdc.decode_and_enqueue(malformed);
    assert!(
        res.is_err(),
        "malformed wal2json payload must return Err, not panic"
    );
}

/// Negative test: empty payload must be rejected (RS-4014 / InvalidPayload).
#[test]
fn cdc_pgoutput_empty_payload_returns_rs_error_no_panic() {
    let mut cdc = test_source(CdcWireFormat::PgOutput);
    let res = cdc.decode_and_enqueue(b"");
    assert!(
        res.is_err(),
        "empty pgoutput payload must return Err, not panic"
    );
}

/// Negative test: wal2json with empty bytes must be rejected.
#[test]
fn cdc_wal2json_empty_payload_returns_rs_error_no_panic() {
    let mut cdc = test_source(CdcWireFormat::Wal2Json);
    let res = cdc.decode_and_enqueue(b"");
    assert!(
        res.is_err(),
        "empty wal2json payload must return Err, not panic"
    );
}

/// Negative test: random binary noise must not crash the pgoutput decoder.
#[test]
fn cdc_pgoutput_random_binary_noise_returns_rs_error_no_panic() {
    let mut cdc = test_source(CdcWireFormat::PgOutput);
    let noise = [0xDE, 0xAD, 0xBE, 0xEF, 0xFF, 0x00, 0x01, 0x02, 0x03];
    let res = cdc.decode_and_enqueue(&noise);
    assert!(
        res.is_err(),
        "binary noise must return Err from pgoutput decoder, not panic"
    );
}

/// Negative test: random binary noise must not crash the wal2json decoder.
#[test]
fn cdc_wal2json_random_binary_noise_returns_rs_error_no_panic() {
    let mut cdc = test_source(CdcWireFormat::Wal2Json);
    let noise = [0xDE, 0xAD, 0xBE, 0xEF, 0xFF, 0x00, 0x01, 0x02, 0x03];
    let res = cdc.decode_and_enqueue(&noise);
    assert!(
        res.is_err(),
        "binary noise must return Err from wal2json decoder, not panic"
    );
}

/// Connection-remains-healthy: after a decode failure the source is still
/// operable and can process subsequent valid messages (RS-4014 recovery).
#[test]
fn cdc_source_remains_operational_after_decode_failure() {
    let mut cdc = test_source(CdcWireFormat::Wal2Json);

    // First: send malformed payload — must return Err.
    let malformed = b"not json at all";
    assert!(cdc.decode_and_enqueue(malformed).is_err());

    // Then: send a valid synthetic wal2json payload — must succeed.
    let valid = br#"{"lsn":"0/16B374C","table_id":1001,"op":"i","key":"k1","old":null,"new":[42]}"#;
    assert!(
        cdc.decode_and_enqueue(valid).is_ok(),
        "source must remain operational after a prior decode failure"
    );
}

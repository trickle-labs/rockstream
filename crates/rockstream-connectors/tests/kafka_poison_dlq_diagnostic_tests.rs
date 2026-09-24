//! Bounded Poison-Record Handling & DLQ Diagnostics Tests (v0.70 Slice 6 / Table 4.6).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    redact_sensitive_payload, KafkaDlqDiagnostic, KafkaDlqPolicy, KafkaSource,
};
use rockstream_types::dlq::{get_global_dlq, DlqEntry, MAX_DLQ_CAPACITY};
use rockstream_types::ids::ConnectorId;
use sha2::{Digest, Sha256};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

fn clear_dlq() {
    get_global_dlq().lock().clear();
}

#[tokio::test]
async fn test_poison_default_strict_blocks_on_malformed_json() {
    clear_dlq();
    let source = KafkaSource::connect(
        ConnectorId(9001),
        schema(),
        "127.0.0.1:1",
        "topic_strict",
        "grp_strict",
    )
    .unwrap();

    assert_eq!(source.dlq_policy(), KafkaDlqPolicy::Strict);

    let malformed_payload = b"not-valid-json";
    let res = source.handle_poison_record(
        0,
        100,
        "RS-1003",
        "malformed JSON payload",
        malformed_payload,
    );

    assert!(res.is_err());
    let err = res.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("RS-1003"), "got: {msg}");
    assert!(msg.contains("Next steps:"), "got: {msg}");

    // DLQ remains empty under strict fail-closed policy
    assert_eq!(get_global_dlq().lock().len(), 0);
    clear_dlq();
}

#[tokio::test]
async fn test_poison_dlq_quarantines_malformed_json() {
    clear_dlq();
    let mut source = KafkaSource::connect(
        ConnectorId(9002),
        schema(),
        "127.0.0.1:1",
        "topic_dlq",
        "grp_dlq",
    )
    .unwrap();

    source.set_dlq_policy(KafkaDlqPolicy::Dlq);
    assert_eq!(source.dlq_policy(), KafkaDlqPolicy::Dlq);

    let malformed_payload = b"{corrupt json syntax";
    let res = source.handle_poison_record(
        2,
        250,
        "RS-1003",
        "Kafka record payload is not valid JSON shape",
        malformed_payload,
    );

    assert!(res.is_ok());
    let diag: KafkaDlqDiagnostic = res.unwrap();
    assert_eq!(diag.topic, "topic_dlq");
    assert_eq!(diag.partition, 2);
    assert_eq!(diag.offset, 250);
    assert!(diag.error.contains("RS-1003"));
    assert!(!diag.schema.is_empty());

    let expected_digest = format!("{:x}", Sha256::digest(malformed_payload));
    assert_eq!(diag.payload_digest, expected_digest);

    // Global DLQ receives +1 entry with exact diagnostics
    let dlq = get_global_dlq().lock();
    assert_eq!(dlq.len(), 1);
    let entry = &dlq[0];
    assert_eq!(entry.source_name, "topic_dlq");
    assert_eq!(entry.source_offset, "2:250");
    assert_eq!(entry.error_code, "RS-1003");
    drop(dlq);
    clear_dlq();
}

#[tokio::test]
async fn test_poison_dlq_quarantines_schema_mismatch() {
    clear_dlq();
    let mut source = KafkaSource::connect(
        ConnectorId(9003),
        schema(),
        "127.0.0.1:1",
        "topic_schema",
        "grp_schema",
    )
    .unwrap();

    source.set_dlq_policy(KafkaDlqPolicy::Dlq);

    let row_values = vec![
        serde_json::json!("string_instead_of_int64"),
        serde_json::json!("valid_name"),
    ];
    let validation = source.validate_record_schema(&row_values);
    assert!(validation.is_err());
    let err_msg = validation.unwrap_err();
    assert!(err_msg.contains("expected integer"));

    let payload = serde_json::to_vec(&serde_json::json!({
        "timestamp": 1,
        "values": row_values,
        "weight": 1
    }))
    .unwrap();

    let res = source.handle_poison_record(
        0,
        300,
        "RS-1003",
        &format!("schema type mismatch: {err_msg}"),
        &payload,
    );

    assert!(res.is_ok());
    let diag = res.unwrap();
    assert_eq!(diag.offset, 300);
    assert!(diag.error.contains("expected integer"));

    assert_eq!(get_global_dlq().lock().len(), 1);
    clear_dlq();
}

#[tokio::test]
async fn test_poison_oversized_payload_quarantined() {
    clear_dlq();
    let mut source = KafkaSource::connect(
        ConnectorId(9004),
        schema(),
        "127.0.0.1:1",
        "topic_oversized",
        "grp_oversized",
    )
    .unwrap();

    source.set_dlq_policy(KafkaDlqPolicy::Dlq);

    let large_payload = vec![b'x'; 17 * 1024 * 1024]; // 17 MiB > 16 MiB
    let res = source.handle_poison_record(
        1,
        400,
        "RS-4014",
        "Kafka record payload exceeds 16 MiB",
        &large_payload,
    );

    assert!(res.is_ok());
    let diag = res.unwrap();
    assert_eq!(diag.topic, "topic_oversized");
    assert_eq!(diag.partition, 1);
    assert_eq!(diag.offset, 400);
    assert!(diag.error.contains("RS-4014"));

    let dlq = get_global_dlq().lock();
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].error_code, "RS-4014");
    drop(dlq);
    clear_dlq();
}

#[tokio::test]
async fn test_poison_dlq_redacts_sensitive_payload() {
    clear_dlq();
    let mut source = KafkaSource::connect(
        ConnectorId(9005),
        schema(),
        "127.0.0.1:1",
        "topic_sensitive",
        "grp_sensitive",
    )
    .unwrap();

    source.set_dlq_policy(KafkaDlqPolicy::Dlq);

    let raw_sensitive = serde_json::json!({
        "timestamp": 12345,
        "values": [1, "alice"],
        "password": "super_secret_password_abc",
        "api_key": "secret_api_key_xyz",
        "auth_token": "bearer_jwt_token_123"
    });
    let payload = serde_json::to_vec(&raw_sensitive).unwrap();
    let raw_sha256 = format!("{:x}", Sha256::digest(&payload));

    let res = source.handle_poison_record(0, 500, "RS-1003", "invalid structure", &payload);

    assert!(res.is_ok());
    let diag = res.unwrap();
    assert_eq!(diag.payload_digest, raw_sha256);

    let redacted = diag
        .redacted_payload
        .expect("redacted payload must be present");
    assert!(!redacted.contains("super_secret_password_abc"));
    assert!(!redacted.contains("secret_api_key_xyz"));
    assert!(!redacted.contains("bearer_jwt_token_123"));
    assert!(redacted.contains("[REDACTED]"));

    // Check helper directly
    let direct_redacted = redact_sensitive_payload(&payload);
    assert!(!direct_redacted.contains("super_secret_password_abc"));
    assert!(direct_redacted.contains("[REDACTED]"));
    clear_dlq();
}

#[tokio::test]
async fn test_poison_dlq_capacity_exhaustion_fails_closed() {
    clear_dlq();
    let mut source = KafkaSource::connect(
        ConnectorId(9006),
        schema(),
        "127.0.0.1:1",
        "topic_cap",
        "grp_cap",
    )
    .unwrap();

    source.set_dlq_policy(KafkaDlqPolicy::Dlq);

    // Pre-populate global DLQ up to MAX_DLQ_CAPACITY (10,000)
    {
        let mut dlq = get_global_dlq().lock();
        for i in 0..MAX_DLQ_CAPACITY {
            dlq.push(DlqEntry {
                arrived_at: 1000 + i as u64,
                source_name: "topic_cap".to_string(),
                source_offset: format!("0:{i}"),
                error_code: "RS-1003".to_string(),
                error_message: "pre-filled entry".to_string(),
                raw_bytes_hex: "7b7d".to_string(),
                replay_attempt: 0,
            });
        }
        assert_eq!(dlq.len(), MAX_DLQ_CAPACITY);
    }

    // 10,001st bad record must fail closed with RS-4014
    let res =
        source.handle_poison_record(0, 10001, "RS-1003", "record after capacity reached", b"{}");

    assert!(res.is_err());
    let err = res.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("RS-4014"), "got: {msg}");
    assert!(msg.contains("DLQ capacity exhausted"), "got: {msg}");
    assert!(msg.contains("Next steps:"), "got: {msg}");

    clear_dlq();
}

#[test]
fn test_poison_record_dlq_diagnostics_and_safe_payload_redaction() {
    test_poison_default_strict_blocks_on_malformed_json();
    test_poison_dlq_quarantines_malformed_json();
    test_poison_dlq_quarantines_schema_mismatch();
    test_poison_oversized_payload_quarantined();
    test_poison_dlq_redacts_sensitive_payload();
    test_poison_dlq_capacity_exhaustion_fails_closed();
}

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use rockstream_types::diagnostic::{redact_secrets_with_count, DiagnosticOccurrence};
use rockstream_types::error_code::RS_2018;
use uuid::Uuid;

#[test]
fn redaction_mutations_never_leak_secret_values() {
    let occurrence = DiagnosticOccurrence::new(
        RS_2018,
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        [(
            "detail".to_string(),
            "password=one bearer two https://user:three@example.test/a https://a:four@example.test/b"
                .to_string(),
        )],
        None,
        None,
    )
    .unwrap();

    assert_eq!(
        serde_json::to_string(&occurrence).unwrap(),
        r#"{"code":"RS-2018","correlation_id":"11111111-1111-4111-8111-111111111111","message":"Published frontier exceeded the session max_staleness bound; query proceeded (detail=password=[REDACTED] bearer [REDACTED] https://user:[REDACTED]@example.test/a https://a:[REDACTED]@example.test/b)","context":{"detail":"password=[REDACTED] bearer [REDACTED] https://user:[REDACTED]@example.test/a https://a:[REDACTED]@example.test/b"},"retry_after":null}"#
    );

    let key = DiagnosticOccurrence::new(
        RS_2018,
        Uuid::nil(),
        [(
            "detail".to_string(),
            "-----BEGIN PRIVATE KEY-----".to_string(),
        )],
        None,
        None,
    )
    .unwrap();
    assert_eq!(key.context["detail"], "[REDACTED_PRIVATE_KEY_MATERIAL]");
}

#[test]
fn encoded_private_key_is_absent_from_diagnostic_output() {
    let encoded_key = BASE64_STANDARD
        .encode("-----BEGIN PRIVATE KEY-----\nfixture-key-material\n-----END PRIVATE KEY-----");
    assert_eq!(
        redact_secrets_with_count(&encoded_key),
        ("[REDACTED_PRIVATE_KEY_MATERIAL]".to_string(), 1),
    );
    let occurrence = DiagnosticOccurrence::new(
        RS_2018,
        Uuid::nil(),
        [("detail".to_string(), encoded_key)],
        None,
        None,
    )
    .unwrap();

    assert_eq!(
        serde_json::to_string(&occurrence).unwrap(),
        r#"{"code":"RS-2018","correlation_id":"00000000-0000-0000-0000-000000000000","message":"Published frontier exceeded the session max_staleness bound; query proceeded (detail=[REDACTED_PRIVATE_KEY_MATERIAL])","context":{"detail":"[REDACTED_PRIVATE_KEY_MATERIAL]"},"retry_after":null}"#,
    );
}

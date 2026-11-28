use base64::{engine::general_purpose::STANDARD, Engine as _};
use rockstream_control::audit::FileAuditLog;
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::{SecretListing, SecretStore};
use rockstream_types::audit::AuditEvent;
use rockstream_types::secret::SecretType;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn secret_literal_is_absent_from_audit_and_metadata_artifacts() {
    let dir = TempDir::new().unwrap();
    let audit_path = dir.path().join("audit.jsonl");
    let audit = Arc::new(FileAuditLog::open(&audit_path).unwrap());
    let store = SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("redaction-test-kek")),
    )
    .with_audit_log(audit.clone());
    let literal = "literal-secret-that-must-not-leak";
    let encoded_literal = STANDARD.encode(literal);
    let private_key =
        "-----BEGIN PRIVATE KEY-----\nsynthetic-private-key-material\n-----END PRIVATE KEY-----";
    let encoded_private_key = STANDARD.encode(private_key);
    store
        .create_secret(
            0,
            "redacted",
            SecretType::BearerToken,
            HashMap::from([
                (String::from("token"), literal.to_string()),
                (String::from("encoded"), encoded_literal.clone()),
                (String::from("private_key"), private_key.to_string()),
                (
                    String::from("encoded_private_key"),
                    encoded_private_key.clone(),
                ),
            ]),
            "admin",
        )
        .await
        .unwrap();
    let listing = store.list_secrets(0).await.unwrap();
    assert_eq!(listing.len(), 1);
    let created_at = listing[0].created_at;
    let updated_at = listing[0].updated_at;
    assert!(created_at > 0);
    assert_eq!(created_at, updated_at);
    assert_eq!(
        listing,
        vec![SecretListing {
            name: "redacted".to_string(),
            secret_type: SecretType::BearerToken,
            created_at,
            updated_at,
            version: 1,
        }]
    );

    let audit_records = audit.read_all().unwrap();
    assert_eq!(audit_records.len(), 1);
    let audit_record = &audit_records[0];
    assert!(audit_record.timestamp_ms > 0);
    let expected_audit = AuditEvent {
        timestamp_ms: audit_record.timestamp_ms,
        actor: "admin".to_string(),
        action: "secret.created".to_string(),
        resource: "redacted".to_string(),
        error_code: None,
        detail: None,
    };
    let mut expected_audit_bytes = serde_json::to_vec(&expected_audit).unwrap();
    expected_audit_bytes.push(b'\n');
    let audit_bytes = std::fs::read(&audit_path).unwrap();
    assert_eq!(audit_bytes, expected_audit_bytes);
    for forbidden in [
        literal.as_bytes(),
        encoded_literal.as_bytes(),
        private_key.as_bytes(),
        encoded_private_key.as_bytes(),
    ] {
        assert!(!audit_bytes
            .windows(forbidden.len())
            .any(|window| window == forbidden));
    }

    let metadata = serde_json::json!({
        "name": listing[0].name,
        "type": listing[0].secret_type.to_string(),
        "created_at": listing[0].created_at,
        "updated_at": listing[0].updated_at,
        "version": listing[0].version,
    });
    assert_eq!(
        metadata,
        serde_json::json!({
            "name": "redacted",
            "type": "bearer_token",
            "created_at": created_at,
            "updated_at": updated_at,
            "version": 1,
        })
    );
    let metadata_bytes = serde_json::to_vec(&metadata).unwrap();
    for forbidden in [
        literal.as_bytes(),
        encoded_literal.as_bytes(),
        private_key.as_bytes(),
        encoded_private_key.as_bytes(),
    ] {
        assert!(!metadata_bytes
            .windows(forbidden.len())
            .any(|window| window == forbidden));
    }
}

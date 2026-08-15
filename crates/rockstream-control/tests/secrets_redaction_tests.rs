use rockstream_control::audit::FileAuditLog;
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
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
    .with_audit_log(audit);
    let literal = "literal-secret-that-must-not-leak";
    store
        .create_secret(
            0,
            "redacted",
            SecretType::BearerToken,
            HashMap::from([(String::from("token"), literal.to_string())]),
            "admin",
        )
        .await
        .unwrap();
    let listing = store.list_secrets(0).await.unwrap();
    assert_eq!(listing[0].name, "redacted");
    assert_eq!(listing[0].secret_type, SecretType::BearerToken);
    let audit_bytes = std::fs::read(&audit_path).unwrap();
    assert!(!audit_bytes
        .windows(literal.len())
        .any(|window| window == literal.as_bytes()));
    let metadata = serde_json::to_vec(&serde_json::json!({
        "name": listing[0].name,
        "type": listing[0].secret_type.to_string(),
        "created_at": listing[0].created_at,
        "updated_at": listing[0].updated_at,
    }))
    .unwrap();
    assert!(!metadata
        .windows(literal.len())
        .any(|window| window == literal.as_bytes()));
}

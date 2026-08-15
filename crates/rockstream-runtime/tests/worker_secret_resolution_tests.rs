use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
use rockstream_runtime::WorkerSecretManager;
use rockstream_types::secret::SecretType;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn worker_resolves_exact_payload_in_memory_and_rejects_wrong_identity() {
    let store = SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("worker-test-kek")),
    );
    let mut payload = HashMap::new();
    payload.insert("username".to_string(), "alice".to_string());
    payload.insert("password".to_string(), "worker-only-secret".to_string());
    store
        .create_secret(0, "kafka", SecretType::SaslPlain, payload.clone(), "test")
        .await
        .unwrap();

    let token = store
        .issue_worker_token(0, "kafka", "worker-7", 300, "test")
        .await
        .unwrap();
    let manager = WorkerSecretManager::new("worker-7");
    let resolved = manager.resolve_token(&token, token.issued_at).unwrap();
    assert_eq!(resolved.payload, payload);
    assert_eq!(
        manager.get("kafka", token.issued_at).unwrap().payload,
        payload
    );
    assert_eq!(manager.fill_level(), 1);

    let wrong_worker = WorkerSecretManager::new("worker-8");
    assert!(wrong_worker.resolve_token(&token, token.issued_at).is_err());
    assert!(!TempDir::new()
        .unwrap()
        .path()
        .join("worker-only-secret")
        .exists());
}

#[tokio::test]
async fn expired_token_is_removed_from_memory() {
    let store = SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("worker-test-kek")),
    );
    store
        .create_secret(
            0,
            "short_lived",
            SecretType::BearerToken,
            HashMap::from([(String::from("token"), String::from("secret-value"))]),
            "test",
        )
        .await
        .unwrap();
    let token = store
        .issue_worker_token(0, "short_lived", "worker-7", 1, "test")
        .await
        .unwrap();
    let manager = WorkerSecretManager::new("worker-7");
    manager.resolve_token(&token, token.issued_at).unwrap();
    assert!(manager.get("short_lived", token.expires_at).is_none());
    assert_eq!(manager.fill_level(), 0);
}

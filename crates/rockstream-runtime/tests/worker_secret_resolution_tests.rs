use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
use rockstream_runtime::WorkerSecretManager;
use rockstream_types::ids::WorkloadId;
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
    let invalid_workload = store
        .issue_worker_token(0, WorkloadId(0), "kafka", "worker-7", 300, "test")
        .await
        .unwrap_err();
    assert_eq!(
        invalid_workload.to_string(),
        "[RS-2423] secret.token_invalid: worker token requires a valid workload identity"
    );

    let token = store
        .issue_worker_token(0, WorkloadId(17), "kafka", "worker-7", 300, "test")
        .await
        .unwrap();
    let manager = WorkerSecretManager::new("worker-7");
    manager.initialize_workload_epoch(WorkloadId(17), 5);
    let resolved = manager.resolve_token(&token, token.issued_at).unwrap();
    assert_eq!(resolved.payload, payload);
    assert_eq!(
        manager
            .get(WorkloadId(17), "kafka", token.issued_at)
            .unwrap()
            .payload,
        payload
    );
    let replacement = store
        .issue_worker_token(0, WorkloadId(17), "kafka", "worker-7", 300, "test")
        .await
        .unwrap();
    manager
        .resolve_token(&replacement, replacement.issued_at)
        .unwrap();
    assert_eq!(
        manager
            .get(WorkloadId(17), "kafka", replacement.issued_at)
            .unwrap()
            .token_id,
        token.token_id
    );
    assert_eq!(manager.advance_workload_epoch(WorkloadId(17), 5), 0);
    assert_eq!(manager.advance_workload_epoch(WorkloadId(17), 6), 1);
    assert_eq!(
        manager
            .get(WorkloadId(17), "kafka", replacement.issued_at)
            .unwrap()
            .token_id,
        replacement.token_id
    );
    assert!(manager
        .get(WorkloadId(18), "kafka", token.issued_at)
        .is_none());
    let mut workload_tamper = token.clone();
    workload_tamper.workload_id = WorkloadId(18);
    assert!(manager
        .resolve_token(&workload_tamper, token.issued_at)
        .is_err());
    let mut unsupported_version = token.clone();
    unsupported_version.format_version = 1;
    assert_eq!(
        manager
            .resolve_token(&unsupported_version, token.issued_at)
            .err()
            .unwrap()
            .to_string(),
        "[RS-2423] secret.token_invalid: token format version is unsupported. Next steps: request a fresh secret token using valid mTLS node credentials."
    );
    assert_eq!(manager.fill_level(), 1);
    manager.track_refresh(WorkloadId(17), "kafka").unwrap();
    let active = std::collections::HashSet::from([WorkloadId(17)]);
    assert_eq!(
        manager.refresh_targets(&active),
        vec![(WorkloadId(17), "kafka".to_owned())]
    );
    manager.reject_refresh(WorkloadId(17), "kafka", true);
    assert!(manager
        .get(WorkloadId(17), "kafka", token.issued_at)
        .is_none());
    assert_eq!(
        manager.refresh_targets(&active),
        vec![(WorkloadId(17), "kafka".to_owned())]
    );
    manager.reject_refresh(WorkloadId(17), "kafka", false);
    assert!(manager.refresh_targets(&active).is_empty());
    assert_eq!(manager.fill_level(), 0);

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
        .issue_worker_token(0, WorkloadId(17), "short_lived", "worker-7", 1, "test")
        .await
        .unwrap();
    let manager = WorkerSecretManager::new("worker-7");
    manager.resolve_token(&token, token.issued_at).unwrap();
    assert!(manager
        .get(WorkloadId(17), "short_lived", token.expires_at)
        .is_none());
    assert_eq!(manager.fill_level(), 0);
}

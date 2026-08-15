//! Lifecycle, SlateDB storage, point-delete, and audit tests for SecretStore (v0.55.1).

use rockstream_control::audit::FileAuditLog;
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::secret_store::SecretStore;
use rockstream_types::secret::SecretType;
use slatedb::Db;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

async fn open_test_slatedb(dir: &TempDir) -> Arc<Db> {
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Db::builder("catalog_db", object_store)
        .build()
        .await
        .unwrap();
    Arc::new(db)
}

#[tokio::test]
async fn test_secrets_lifecycle_in_memory_and_audit() {
    let dir = TempDir::new().unwrap();
    let audit_file = dir.path().join("audit.jsonl");
    let audit_log = Arc::new(FileAuditLog::open(&audit_file).unwrap());

    let kek_provider = Arc::new(EnvKekProvider::from_passphrase("kek-pass-1"));
    let store = SecretStore::new(None, kek_provider.clone()).with_audit_log(audit_log.clone());

    let ns = 100u128;
    let mut payload = HashMap::new();
    payload.insert("user".to_string(), "pg_replication".to_string());
    payload.insert("password".to_string(), "UltraSecret987!".to_string());

    // 1. Create secret
    let meta = store
        .create_secret(
            ns,
            "pg_cdc_creds",
            SecretType::PostgresPassword,
            payload.clone(),
            "admin_user",
        )
        .await
        .expect("create_secret must succeed");
    assert_eq!(meta.version, 1);

    // 2. Duplicate creation rejected with RS-2421
    let dup_err = store
        .create_secret(
            ns,
            "pg_cdc_creds",
            SecretType::PostgresPassword,
            payload.clone(),
            "admin_user",
        )
        .await;
    assert!(dup_err.is_err());
    let dup_err_str = dup_err.unwrap_err().to_string();
    assert!(
        dup_err_str.contains("RS-2421"),
        "must return RS-2421, got: {dup_err_str}"
    );

    // 3. Get decrypted secret
    let decrypted = store
        .get_secret(ns, "pg_cdc_creds")
        .await
        .expect("get_secret must succeed");
    assert_eq!(decrypted.payload.get("user").unwrap(), "pg_replication");
    assert_eq!(
        decrypted.payload.get("password").unwrap(),
        "UltraSecret987!"
    );

    // 4. Alter secret
    let mut new_payload = HashMap::new();
    new_payload.insert("user".to_string(), "pg_replication_v2".to_string());
    new_payload.insert("password".to_string(), "NewPassword999!".to_string());

    let altered_meta = store
        .alter_secret(ns, "pg_cdc_creds", new_payload, "admin_user")
        .await
        .expect("alter_secret must succeed");
    assert_eq!(altered_meta.version, 2);

    let decrypted_v2 = store.get_secret(ns, "pg_cdc_creds").await.unwrap();
    assert_eq!(
        decrypted_v2.payload.get("password").unwrap(),
        "NewPassword999!"
    );

    // 5. Alter non-existent fails with RS-2420
    let non_exist_alter = store
        .alter_secret(ns, "unknown_secret", payload.clone(), "admin_user")
        .await;
    assert!(non_exist_alter.is_err());
    assert!(non_exist_alter.unwrap_err().to_string().contains("RS-2420"));

    // 6. List secrets (metadata only)
    let listings = store.list_secrets(ns).await.unwrap();
    assert_eq!(listings.len(), 1);
    assert_eq!(listings[0].name, "pg_cdc_creds");
    assert_eq!(listings[0].secret_type, SecretType::PostgresPassword);
    assert_eq!(listings[0].version, 2);

    // 7. Dependency check: Add reference, try to drop -> fails with RS-2426
    store
        .add_reference(ns, "pg_cdc_creds", "source_pg_cdc_1")
        .unwrap();
    let in_use_drop = store.drop_secret(ns, "pg_cdc_creds", "admin_user").await;
    assert!(in_use_drop.is_err());
    let in_use_str = in_use_drop.unwrap_err().to_string();
    assert!(
        in_use_str.contains("RS-2426"),
        "must return RS-2426, got: {in_use_str}"
    );

    // Remove reference and drop succeeds
    store.remove_reference(ns, "pg_cdc_creds", "source_pg_cdc_1");
    store
        .drop_secret(ns, "pg_cdc_creds", "admin_user")
        .await
        .expect("drop after unreferencing must succeed");

    // Dropping again fails with RS-2420
    let drop_again = store.drop_secret(ns, "pg_cdc_creds", "admin_user").await;
    assert!(drop_again.is_err());
    assert!(drop_again.unwrap_err().to_string().contains("RS-2420"));

    // 8. Assert audit log entries contain no plaintext secrets
    let audit_events = audit_log.read_all().unwrap();
    let actions: Vec<_> = audit_events.iter().map(|e| e.action.as_str()).collect();
    assert!(actions.contains(&"secret.created"));
    assert!(actions.contains(&"secret.create_failed"));
    assert!(actions.contains(&"secret.altered"));
    assert!(actions.contains(&"secret.alter_failed"));
    assert!(actions.contains(&"secret.drop_failed"));
    assert!(actions.contains(&"secret.dropped"));

    // Read audit file directly as raw string and assert zero occurrences of raw passwords
    let audit_raw = std::fs::read_to_string(&audit_file).unwrap();
    assert!(!audit_raw.contains("UltraSecret987!"));
    assert!(!audit_raw.contains("NewPassword999!"));
}

#[tokio::test]
async fn test_secrets_lifecycle_slatedb_persistence() {
    let dir = TempDir::new().unwrap();
    let db = open_test_slatedb(&dir).await;

    let kek_provider = Arc::new(EnvKekProvider::from_passphrase("slatedb-kek-v1"));
    let store = SecretStore::new(Some(db.clone()), kek_provider.clone());

    let ns = 42u128;
    let mut payload1 = HashMap::new();
    payload1.insert("token".to_string(), "ghp_securetoken123456789".to_string());

    store
        .create_secret(
            ns,
            "github_webhook_token",
            SecretType::BearerToken,
            payload1,
            "root",
        )
        .await
        .unwrap();

    let mut payload2 = HashMap::new();
    payload2.insert("username".to_string(), "kafka_prod".to_string());
    payload2.insert("password".to_string(), "KafkaPass#321".to_string());

    store
        .create_secret(
            ns,
            "kafka_source_secret",
            SecretType::SaslPlain,
            payload2,
            "root",
        )
        .await
        .unwrap();

    // List secrets
    let list = store.list_secrets(ns).await.unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].name, "github_webhook_token");
    assert_eq!(list[1].name, "kafka_source_secret");

    // Rotate KEK
    let new_kek_provider = Arc::new(EnvKekProvider::from_passphrase("slatedb-kek-rotated-v2"));
    let rotated = store
        .rotate_kek(new_kek_provider.clone(), "root")
        .await
        .unwrap();
    assert_eq!(rotated, 2);

    // Verify secrets are still decryptable with new KEK
    let dec_gh = store.get_secret(ns, "github_webhook_token").await.unwrap();
    assert_eq!(
        dec_gh.payload.get("token").unwrap(),
        "ghp_securetoken123456789"
    );

    let dec_kf = store.get_secret(ns, "kafka_source_secret").await.unwrap();
    assert_eq!(dec_kf.payload.get("password").unwrap(), "KafkaPass#321");

    // Issue worker token
    let token = store
        .issue_worker_token(ns, "kafka_source_secret", "worker_node_1", 300, "root")
        .await
        .unwrap();
    assert_eq!(token.secret_name, "kafka_source_secret");
    assert_eq!(token.worker_identity, "worker_node_1");
    assert!(!token.is_expired(token.issued_at + 100));

    // Point-delete one secret
    store
        .drop_secret(ns, "github_webhook_token", "root")
        .await
        .unwrap();

    let remaining = store.list_secrets(ns).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].name, "kafka_source_secret");
}

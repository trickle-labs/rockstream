use rockstream_control::kek::EnvKekProvider;
use rockstream_control::secret_store::{secret_key, SecretStore, SecretStoreError};
use rockstream_storage::catalog::{CatalogStore, DurableCatalogStore};
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{minio_object_store, start_minio};
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, SecurityAuditClass, SecurityAuditOutcome,
    SecurityAuditRecord, SECURITY_AUDIT_FORMAT_VERSION,
};
use rockstream_types::secret::{EncryptedSecret, SecretType};
use slatedb::Db;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

const MINIO_BUCKET: &str = "rockstream-secrets-durability-test";

fn assert_rotation_audit(records: &[SecurityAuditRecord], rotated_count: usize) {
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert!(!record.event_id.is_empty());
    assert_ne!(record.timestamp_ms, 0);
    assert_eq!(
        records,
        &[SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: record.event_id.clone(),
            timestamp_ms: record.timestamp_ms,
            actor: AuditIdentity::Unverified("test".to_owned()),
            target: AuditField::Known("cluster_kek".to_owned()),
            scope: AuditField::Known("cluster".to_owned()),
            action: SecurityAuditClass::SecretRotation,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::NotApplicable,
            operation_id: AuditField::Unknown,
            policy_revision: AuditField::Unknown,
            before_state: AuditField::Unknown,
            after_state: AuditField::Known(format!("rewrapped_envelopes={rotated_count}")),
        }]
    );
}

#[tokio::test]
async fn secrets_survive_lfs_reopen_and_kek_rotation_uses_point_keys() {
    let dir = TempDir::new().unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        Db::builder("catalog_db", object_store.clone())
            .build()
            .await
            .unwrap(),
    );
    let audit_catalog = Arc::new(DurableCatalogStore::new(object_store.clone(), "security"));
    let store = SecretStore::new(
        Some(db.clone()),
        Arc::new(EnvKekProvider::from_passphrase("before")),
    )
    .with_security_audit_catalog(audit_catalog.clone());
    store
        .create_secret(
            0,
            "durable",
            SecretType::SaslPlain,
            HashMap::from([(String::from("password"), String::from("durable-secret"))]),
            "test",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .rotate_kek(Arc::new(EnvKekProvider::from_passphrase("after")), "test",)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .get_secret(0, "durable")
            .await
            .unwrap()
            .payload
            .get("password"),
        Some(&String::from("durable-secret"))
    );
    let records = audit_catalog.list_security_audit_records().await.unwrap();
    assert_rotation_audit(&records, 1);
    drop(store);
    db.close().await.unwrap();

    let audit_catalog = Arc::new(
        DurableCatalogStore::recover(object_store.clone(), "security")
            .await
            .unwrap(),
    );
    assert_eq!(
        audit_catalog.list_security_audit_records().await.unwrap(),
        records
    );
    let reopened_db = Arc::new(
        Db::builder("catalog_db", object_store)
            .build()
            .await
            .unwrap(),
    );
    let stored_bytes = reopened_db
        .get(&secret_key(0, "durable"))
        .await
        .unwrap()
        .unwrap();
    assert!(!stored_bytes
        .windows(b"durable-secret".len())
        .any(|window| window == b"durable-secret"));
    let envelope: EncryptedSecret = serde_json::from_slice(&stored_bytes).unwrap();
    assert_eq!(envelope.name, "durable");
    assert_eq!(envelope.secret_type, SecretType::SaslPlain);
    assert_eq!(envelope.kek_provider, "env");
    assert!(!envelope.wrapped_dek.is_empty());
    assert!(!envelope.ciphertext.is_empty());
    assert_eq!(envelope.nonce.len(), 12);
    let reopened = SecretStore::new(
        Some(reopened_db.clone()),
        Arc::new(EnvKekProvider::from_passphrase("after")),
    )
    .with_security_audit_catalog(audit_catalog);
    assert_eq!(
        reopened
            .get_secret(0, "durable")
            .await
            .unwrap()
            .payload
            .get("password"),
        Some(&String::from("durable-secret"))
    );
    drop(reopened);
    reopened_db.close().await.unwrap();
}

#[tokio::test]
async fn secrets_survive_minio_tc_reopen_and_kek_rotation() {
    if !docker_available() {
        eprintln!("SKIP secrets_survive_minio_tc_reopen_and_kek_rotation: Docker not available");
        return;
    }

    let (_container, port) = match start_minio(MINIO_BUCKET).await {
        Some(res) => res,
        None => return,
    };

    let object_store = minio_object_store(port, MINIO_BUCKET);
    let db = Arc::new(
        Db::builder("catalog_db", object_store.clone())
            .build()
            .await
            .unwrap(),
    );
    let audit_catalog = Arc::new(DurableCatalogStore::new(object_store.clone(), "security"));
    let store = SecretStore::new(
        Some(Arc::clone(&db)),
        Arc::new(EnvKekProvider::from_passphrase("before")),
    )
    .with_security_audit_catalog(audit_catalog.clone());
    store
        .create_secret(
            0,
            "durable",
            SecretType::SaslPlain,
            HashMap::from([(String::from("password"), String::from("durable-secret"))]),
            "test",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .rotate_kek(Arc::new(EnvKekProvider::from_passphrase("after")), "test")
            .await
            .unwrap(),
        1
    );
    let records = audit_catalog.list_security_audit_records().await.unwrap();
    assert_rotation_audit(&records, 1);
    drop(store);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();

    let audit_catalog = Arc::new(
        DurableCatalogStore::recover(object_store.clone(), "security")
            .await
            .unwrap(),
    );
    assert_eq!(
        audit_catalog.list_security_audit_records().await.unwrap(),
        records
    );
    let reloaded = SecretStore::new(
        Some(Arc::new(
            Db::builder("catalog_db", object_store)
                .build()
                .await
                .unwrap(),
        )),
        Arc::new(EnvKekProvider::from_passphrase("after")),
    )
    .with_security_audit_catalog(audit_catalog);
    assert_eq!(
        reloaded
            .get_secret(0, "durable")
            .await
            .unwrap()
            .payload
            .get("password"),
        Some(&String::from("durable-secret"))
    );
}

#[tokio::test]
async fn durable_kek_rotation_fails_closed_without_audit_catalog() {
    let dir = TempDir::new().unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        Db::builder("catalog_db", object_store)
            .build()
            .await
            .unwrap(),
    );
    let store = SecretStore::new(
        Some(db.clone()),
        Arc::new(EnvKekProvider::from_passphrase("before")),
    );
    store
        .create_secret(
            0,
            "durable",
            SecretType::SaslPlain,
            HashMap::from([(String::from("password"), String::from("durable-secret"))]),
            "test",
        )
        .await
        .unwrap();

    let result = store
        .rotate_kek(Arc::new(EnvKekProvider::from_passphrase("after")), "test")
        .await;
    assert!(matches!(result, Err(SecretStoreError::Storage { .. })));
    assert_eq!(
        store
            .get_secret(0, "durable")
            .await
            .unwrap()
            .payload
            .get("password"),
        Some(&String::from("durable-secret"))
    );
    drop(store);
    db.close().await.unwrap();
}

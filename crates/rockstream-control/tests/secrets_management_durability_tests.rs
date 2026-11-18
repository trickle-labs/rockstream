use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::secret_store::{
    secret_key, SecretRotationOperationContext, SecretStore, SecretStoreError,
};
use rockstream_control::{
    ManagementOperationStore, OperationKind, OperationStatus, OperationUpdate,
    SecretRotationOperationRequest,
};
use rockstream_storage::catalog::{CatalogStore, DurableCatalogStore};
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{minio_object_store, start_minio};
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, PolicyScope, SecurityAuditClass,
    SecurityAuditOutcome, SecurityAuditRecord, SECURITY_AUDIT_FORMAT_VERSION,
};
use rockstream_types::secret::{EncryptedSecret, SecretType};
use sha2::{Digest, Sha256};
use slatedb::Db;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

const MINIO_BUCKET: &str = "rockstream-secrets-durability-test";

fn independent_aes_gcm_open(key: &[u8; 32], nonce: &[u8], ciphertext: &[u8]) -> Vec<u8> {
    let unbound = UnboundKey::new(&AES_256_GCM, key).unwrap();
    let key = LessSafeKey::new(unbound);
    let nonce = Nonce::try_assume_unique_for_key(nonce).unwrap();
    let mut in_out = ciphertext.to_vec();
    key.open_in_place(nonce, Aad::empty(), &mut in_out)
        .unwrap()
        .to_vec()
}

fn independently_open_secret_envelope(
    envelope: &EncryptedSecret,
    passphrase: &str,
) -> HashMap<String, String> {
    let kek: [u8; 32] = Sha256::digest(passphrase.as_bytes()).into();
    assert_eq!(envelope.wrapped_dek.len(), NONCE_LEN + 32 + 16);
    let dek = independent_aes_gcm_open(
        &kek,
        &envelope.wrapped_dek[..NONCE_LEN],
        &envelope.wrapped_dek[NONCE_LEN..],
    );
    let dek: [u8; 32] = dek.try_into().unwrap();
    let plaintext = independent_aes_gcm_open(&dek, &envelope.nonce, &envelope.ciphertext);
    serde_json::from_slice(&plaintext).unwrap()
}

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
    let expected_metadata = store
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
    assert_eq!(envelope.nonce.len(), 12);
    assert_eq!(envelope.metadata, expected_metadata);
    assert_eq!(
        independently_open_secret_envelope(&envelope, "after"),
        HashMap::from([(String::from("password"), String::from("durable-secret"))])
    );
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
async fn secret_rotation_reconciles_a_committed_audit_after_restart() {
    let dir = TempDir::new().unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let catalog = Arc::new(DurableCatalogStore::new(object_store.clone(), "gateway"));
    let operations = ManagementOperationStore::new(object_store.clone());
    let operation_id = "00000000-0000-4000-8000-000000000305";
    let deployment_id = "rotation-test-deployment";
    let accepted_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let scope = PolicyScope::Secret {
        namespace: "public".to_owned(),
        name: "rotation".to_owned(),
        consumer: None,
    };
    let payload = HashMap::from([("password".to_owned(), "rotated-secret".to_owned())]);
    let canonical_payload = payload.iter().collect::<BTreeMap<_, _>>();
    let request = SecretRotationOperationRequest {
        deployment_id: deployment_id.to_owned(),
        actor_id: Some(7),
        scope: scope.clone(),
        payload_digest: ManagementOperationStore::canonical_request_digest(1, &canonical_payload)
            .unwrap(),
    };
    operations
        .accept_idempotent(
            operation_id,
            1,
            &request,
            operation_id,
            OperationKind::SecretRotation,
            accepted_at_ms,
        )
        .await
        .unwrap();
    operations
        .transition(
            operation_id,
            OperationUpdate {
                status: OperationStatus::Running,
                updated_at_ms: accepted_at_ms + 1,
                progress: None,
                phase: Some("rotating_secret".to_owned()),
                error_code: None,
                next_steps: Vec::new(),
            },
        )
        .await
        .unwrap();

    let make_audit = || SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: format!("gateway-secret-rotation-{operation_id}"),
        timestamp_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        actor: AuditIdentity::Verified("principal:7".to_owned()),
        target: AuditField::Known("secret:public/rotation".to_owned()),
        scope: AuditField::Known(serde_json::to_string(&scope).unwrap()),
        action: SecurityAuditClass::SecretRotation,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Observed(operation_id.to_owned()),
        operation_id: AuditField::Known(operation_id.to_owned()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::Unknown,
        after_state: AuditField::Unknown,
    };
    let secret_store = SecretStore::open_durable(
        "gateway-secrets",
        object_store.clone(),
        Arc::new(EnvKekProvider::from_passphrase("rotation-test-key")),
    )
    .await
    .unwrap()
    .with_security_audit_catalog(catalog.clone());
    secret_store
        .create_secret(
            0,
            "rotation",
            SecretType::SaslPlain,
            HashMap::from([("password".to_owned(), "initial-secret".to_owned())]),
            "test",
        )
        .await
        .unwrap();

    // Model a crash after the atomic secret/audit commit but before the operation result commit.
    secret_store
        .alter_secret_with_security_audit(0, "rotation", payload.clone(), "test", make_audit())
        .await
        .unwrap();
    drop(secret_store);

    let secret_store = SecretStore::open_durable(
        "gateway-secrets",
        object_store.clone(),
        Arc::new(EnvKekProvider::from_passphrase("rotation-test-key")),
    )
    .await
    .unwrap()
    .with_security_audit_catalog(catalog.clone())
    .with_management_operation_store(operations.clone(), deployment_id.to_owned());
    secret_store
        .flush_pending_security_audit_outbox()
        .await
        .unwrap();
    let reconciled = secret_store
        .alter_secret_with_operation(
            0,
            "rotation",
            payload.clone(),
            "test",
            SecretRotationOperationContext {
                actor_id: Some(7),
                scope: scope.clone(),
                operation_id: operation_id.to_owned(),
                audit: make_audit(),
            },
        )
        .await
        .unwrap();
    assert_eq!(reconciled.status(), OperationStatus::Succeeded);
    assert_eq!(
        reconciled.result(),
        Some(&serde_json::json!({"command_tag": "ALTER SECRET", "rows": 0}))
    );
    assert_eq!(
        secret_store
            .get_secret(0, "rotation")
            .await
            .unwrap()
            .metadata
            .version,
        2
    );
    let audits = catalog.list_security_audit_records().await.unwrap();
    assert_eq!(audits.len(), 1);
    assert_eq!(
        audits[0].operation_id,
        AuditField::Known(operation_id.to_owned())
    );
    assert!(!serde_json::to_string(reconciled.request().unwrap())
        .unwrap()
        .contains("rotated-secret"));

    let conflict = secret_store
        .alter_secret_with_operation(
            0,
            "rotation",
            HashMap::from([("password".to_owned(), "different-secret".to_owned())]),
            "test",
            SecretRotationOperationContext {
                actor_id: Some(7),
                scope: scope.clone(),
                operation_id: operation_id.to_owned(),
                audit: make_audit(),
            },
        )
        .await;
    assert!(matches!(
        conflict,
        Err(rockstream_control::SecretRotationOperationError::Operation(
            rockstream_control::OperationStoreError::IdempotencyConflict { operation_id: id }
        )) if id == operation_id
    ));
    assert_eq!(
        secret_store
            .get_secret(0, "rotation")
            .await
            .unwrap()
            .metadata
            .version,
        2
    );
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

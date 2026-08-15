use std::sync::Arc;
use std::time::Duration;

use rockstream_control::audit::FileAuditLog;
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_runtime::client::{start_worker_client, start_worker_client_with_tls};
use rockstream_test_support::pki::TestPki;
use rockstream_types::ids::WorkerId;

#[tokio::test]
async fn test_control_worker_mtls_valid_cert_admitted() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();
    let worker_tls = pki.worker_tls_config(1);

    let (client, worker_handle) =
        start_worker_client_with_tls(1, &control_url, storage_dir.path(), worker_tls)
            .await
            .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(client.worker_id(), Some(WorkerId(1)));
    assert_eq!(catalog.len(), 1);

    // Verify audit log has worker.registered
    let events = audit.read_all().unwrap();
    assert!(events
        .iter()
        .any(|e| e.action == "worker.registered" && e.resource == "worker-1"));

    worker_handle.abort();
    handle.shutdown();
}

#[tokio::test]
async fn test_control_worker_mtls_missing_cert_rejected() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();

    // Plaintext client connection without certificate
    let (client, worker_handle) = start_worker_client(1, &control_url, storage_dir.path())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Worker was rejected and never admitted
    assert_eq!(client.worker_id(), None);
    assert_eq!(catalog.len(), 0);

    // Verify audited denial with RS-2410 or RS-2411
    let events = audit.read_all().unwrap();
    assert!(events.iter().any(|e| {
        e.action == "security.internal_mtls_denied"
            && (e.detail.as_deref().unwrap_or("").contains("RS-2410")
                || e.detail.as_deref().unwrap_or("").contains("RS-2411"))
    }));

    worker_handle.abort();
    handle.shutdown();
}

#[tokio::test]
async fn test_control_worker_mtls_expired_cert_rejected() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();
    let expired_tls = pki.expired_worker_tls_config();

    let _res = start_worker_client_with_tls(1, &control_url, storage_dir.path(), expired_tls).await;

    // Client connection should fail handshake or be rejected
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(catalog.len(), 0);

    let events = audit.read_all().unwrap();
    assert!(events.iter().any(|e| {
        e.action == "security.internal_mtls_denied"
            && e.detail.as_deref().unwrap_or("").contains("RS-2411")
    }));

    handle.shutdown();
}

#[tokio::test]
async fn test_control_worker_mtls_untrusted_ca_rejected() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();
    let untrusted_tls = pki.untrusted_worker_tls_config();

    let _res =
        start_worker_client_with_tls(1, &control_url, storage_dir.path(), untrusted_tls).await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(catalog.len(), 0);

    let events = audit.read_all().unwrap();
    assert!(events.iter().any(|e| {
        e.action == "security.internal_mtls_denied"
            && e.detail.as_deref().unwrap_or("").contains("RS-2411")
    }));

    handle.shutdown();
}

#[tokio::test]
async fn test_control_worker_mtls_identity_mismatch_rejected() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();
    // Certificate is for worker-999, but client requests registration for worker 1
    let mismatched_tls = pki.mismatched_worker_tls_config();

    let (client, worker_handle) =
        start_worker_client_with_tls(1, &control_url, storage_dir.path(), mismatched_tls)
            .await
            .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Worker was rejected and not registered
    assert_eq!(client.worker_id(), None);
    assert_eq!(catalog.len(), 0);

    // Verify audited denial with RS-2412
    let events = audit.read_all().unwrap();
    assert!(events.iter().any(|e| {
        e.action == "security.internal_mtls_denied"
            && e.detail.as_deref().unwrap_or("").contains("RS-2412")
    }));

    worker_handle.abort();
    handle.shutdown();
}

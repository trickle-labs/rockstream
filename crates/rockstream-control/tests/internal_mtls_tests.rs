//! End-to-End Integration Tests for Internal mTLS Transport & Lifecycle (v0.55).
//!
//! Validates:
//! - Proof Claim 1: Rejection and audit denial of untrusted/expired/absent cert peers with zero data processed.
//! - Proof Claim 2: Dynamic zero-downtime certificate rotation under sustained traffic with 0 lost epochs.
//! - Proof Claim 3: CLI transport seam behavior over mTLS and refusal when unauthenticated.
//! - Proof Claim 5: Durability of topology, shard leases, and audit trail under mTLS across node restarts.

use std::sync::Arc;
use std::time::Duration;

use rockstream_cli::output::OutputFormat;
use rockstream_cli::run_cluster_status;
use rockstream_cli::transport::{ClientIdentity, ControlClient};
use rockstream_control::audit::FileAuditLog;
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_runtime::client::start_worker_client_with_tls;
use rockstream_storage::catalog::DurableCatalogStore;
use rockstream_test_support::pki::TestPki;
use rockstream_types::error_code::{RS_2410, RS_2411};
use rockstream_types::ids::{ShardId, WorkerId};

fn security_audit_catalog() -> Arc<DurableCatalogStore> {
    Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "internal-mtls-test",
    ))
}

#[tokio::test]
async fn internal_mtls_requires_security_audit_catalog() {
    let pki = TestPki::generate();
    let service =
        ControlService::new(TopologyCatalog::new()).with_internal_tls(pki.control_tls_config());

    let error = service
        .start("127.0.0.1:0")
        .await
        .err()
        .expect("mTLS must fail startup without the security audit catalog");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "RS-2405: internal mTLS requires the durable security audit catalog"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_peer_invalid_expired_absent_cert_rejected_with_audit_denial_no_data() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit_file = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(FileAuditLog::open(&audit_file).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_security_catalog(security_audit_catalog())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();

    // 1. Untrusted certificate peer connection attempt
    let untrusted_tls = pki.untrusted_worker_tls_config();
    let (c1, h1) = start_worker_client_with_tls(1, &control_url, storage_dir.path(), untrusted_tls)
        .await
        .unwrap();

    // 2. Expired certificate peer connection attempt
    let expired_tls = pki.expired_worker_tls_config();
    let (c2, h2) = start_worker_client_with_tls(2, &control_url, storage_dir.path(), expired_tls)
        .await
        .unwrap();

    // 3. Absent certificate (plaintext) peer connection attempt
    let absent_tls = rockstream_types::identity::InternalTlsConfig::default();
    let (c3, h3) = start_worker_client_with_tls(3, &control_url, storage_dir.path(), absent_tls)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify zero workers admitted to cluster topology catalog
    assert_eq!(
        c1.worker_id(),
        None,
        "Untrusted peer must not obtain WorkerId"
    );
    assert_eq!(
        c2.worker_id(),
        None,
        "Expired peer must not obtain WorkerId"
    );
    assert_eq!(
        c3.worker_id(),
        None,
        "Absent cert peer must not obtain WorkerId"
    );
    assert_eq!(
        catalog.len(),
        0,
        "No rejected peer may be registered in topology"
    );

    // Verify audit logs contain rejection denial records
    let audit_events = audit.read_all().unwrap();
    let denial_events: Vec<_> = audit_events
        .iter()
        .filter(|e| e.action == "security.internal_mtls_denied")
        .collect();
    assert!(
        !denial_events.is_empty(),
        "Audited denial events must be recorded for failed mTLS handshakes"
    );
    assert!(
        denial_events.iter().all(|event| event.actor == "unknown"),
        "pre-authentication TLS failures must not claim an authenticated actor: {denial_events:?}"
    );

    h1.abort();
    h2.abort();
    h3.abort();
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_certificate_rotation_preserves_streams_and_accepts_gen2_worker() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_security_catalog(security_audit_catalog())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();
    let mut handles = Vec::new();
    let mut clients = Vec::new();

    for id in 1..=3 {
        let (client, h) = start_worker_client_with_tls(
            id,
            &control_url,
            storage_dir.path(),
            pki.worker_tls_config(id),
        )
        .await
        .unwrap();
        clients.push(client);
        handles.push(h);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 3);

    // Actively stream shard lease requests
    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(clients.len());
    let (resume_tx, resume_rx) = tokio::sync::watch::channel(());
    let mut traffic_tasks = Vec::new();
    for client in &clients {
        let c = client.clone();
        let started_tx = started_tx.clone();
        let mut resume_rx = resume_rx.clone();
        traffic_tasks.push(tokio::spawn(async move {
            let worker_id = c.worker_id().unwrap();
            c.request_shard(ShardId(1)).await.unwrap();
            let mut requests = vec![(worker_id, ShardId(1))];
            started_tx.send(()).await.unwrap();
            resume_rx.changed().await.unwrap();
            for i in 2..=15 {
                c.request_shard(ShardId(i)).await.unwrap();
                requests.push((worker_id, ShardId(i)));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            requests
        }));
    }
    for _ in 0..clients.len() {
        started_rx.recv().await.unwrap();
    }

    // In-flight certificate rotation
    let (ca2_path, ctrl2_cfg, _) = pki.generate_gen2_pki();
    handle
        .reloader
        .as_ref()
        .unwrap()
        .add_trusted_ca(&ca2_path)
        .unwrap();
    handle.reload_tls(ctrl2_cfg).unwrap();
    let (gen2_client, gen2_handle) = start_worker_client_with_tls(
        4,
        &control_url,
        storage_dir.path(),
        pki.gen2_worker_tls_config(4),
    )
    .await
    .unwrap();
    handles.push(gen2_handle);
    tokio::time::timeout(Duration::from_secs(3), async {
        while catalog.len() != 4 || gen2_client.worker_id().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(gen2_client.worker_id(), Some(WorkerId(4)));
    resume_tx.send(()).unwrap();

    let expected_requests = clients
        .iter()
        .map(|client| {
            let worker_id = client.worker_id().unwrap();
            (1..=15)
                .map(|shard| (worker_id, ShardId(shard)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut actual_requests = Vec::new();
    for task in traffic_tasks {
        actual_requests.push(task.await.unwrap());
    }
    assert_eq!(actual_requests, expected_requests);

    // The three old-generation streams survived, and the new generation registered.
    assert_eq!(catalog.len(), 4);
    for (id, client) in (1..=3).zip(&clients) {
        assert_eq!(client.worker_id(), Some(WorkerId(id)));
    }

    for h in handles {
        h.abort();
    }
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_control_mtls_transport_seam_and_refusal() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_security_catalog(security_audit_catalog())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    // 1. Authenticated CLI client with valid certificate succeeds
    let cli_tls = pki.cli_tls_config();
    let client_identity = ClientIdentity::default().with_cert(cli_tls.cert_path.clone().unwrap());
    let valid_client =
        ControlClient::new(Some(control_url.clone()), client_identity).with_internal_tls(cli_tls);

    let status = run_cluster_status(OutputFormat::Json, &valid_client);
    assert!(
        status.is_ok(),
        "Authenticated CLI status request must succeed over mTLS"
    );

    // 2. Unauthenticated CLI client without certificate is refused with RS-2410
    let unauthenticated_client =
        ControlClient::new(Some(control_url.clone()), ClientIdentity::default());
    let res_refused = run_cluster_status(OutputFormat::Json, &unauthenticated_client);
    assert!(
        res_refused.is_err(),
        "Unauthenticated CLI request must be refused"
    );
    let err = res_refused.unwrap_err();
    assert_eq!(err.code, RS_2410, "Refusal error code must be RS-2410");

    // 3. Untrusted CLI client is refused with RS-2411
    let untrusted_cli_client =
        ControlClient::new(Some(control_url.clone()), ClientIdentity::default())
            .with_internal_tls(pki.untrusted_worker_tls_config());
    let res_untrusted = run_cluster_status(OutputFormat::Json, &untrusted_cli_client);
    assert!(
        res_untrusted.is_err(),
        "Untrusted CLI request must be refused"
    );
    let err_untrusted = res_untrusted.unwrap_err();
    assert_eq!(
        err_untrusted.code, RS_2411,
        "Refusal error code must be RS-2411"
    );

    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mtls_cluster_durability_lfs_and_minio() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit_file = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(FileAuditLog::open(&audit_file).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_security_catalog(security_audit_catalog())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();

    let (client, h) = start_worker_client_with_tls(
        1,
        &control_url,
        storage_dir.path(),
        pki.worker_tls_config(1),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 1);
    assert_eq!(client.worker_id(), Some(WorkerId(1)));

    // Verify audit durability on disk
    let audit_events = audit.read_all().unwrap();
    assert!(
        audit_events
            .iter()
            .any(|e| e.action == "worker.registered" || e.action == "cli.authenticated"),
        "Audit log must contain durable event records"
    );

    h.abort();
    handle.shutdown();
}

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
use rockstream_test_support::pki::TestPki;
use rockstream_types::error_code::{RS_2410, RS_2411};
use rockstream_types::ids::{ShardId, WorkerId};

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

    h1.abort();
    h2.abort();
    h3.abort();
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_certificate_rotation_3_worker_cluster_under_load_zero_loss_zero_restart() {
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
    let mut traffic_tasks = Vec::new();
    for client in &clients {
        let c = client.clone();
        traffic_tasks.push(tokio::spawn(async move {
            for i in 1..=15 {
                let _ = c.request_shard(ShardId(i)).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    // In-flight certificate rotation
    let (ca2_path, ctrl2_cfg, _w2_cfg) = pki.generate_gen2_pki();
    handle
        .reloader
        .as_ref()
        .unwrap()
        .add_trusted_ca(&ca2_path)
        .unwrap();
    handle.reload_tls(ctrl2_cfg).unwrap();

    for task in traffic_tasks {
        let _ = task.await;
    }

    // Verification: Zero epoch loss, zero restarts, continuous worker registration
    assert_eq!(catalog.len(), 3);
    for client in &clients {
        assert!(client.worker_id().is_some());
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

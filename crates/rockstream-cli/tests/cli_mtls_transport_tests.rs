use std::sync::Arc;
use std::time::Duration;

use rockstream_cli::output::OutputFormat;
use rockstream_cli::run_cluster_status;
use rockstream_cli::transport::{ClientIdentity, ControlClient};
use rockstream_control::audit::FileAuditLog;
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_test_support::pki::TestPki;
use rockstream_types::error_code::{RS_2410, RS_2411};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cli_control_mtls_valid_cert_executes() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(FileAuditLog::open(&audit_path).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog)
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let cli_tls = pki.cli_tls_config();
    let client_identity = ClientIdentity::default().with_cert(cli_tls.cert_path.clone().unwrap());
    let control_client =
        ControlClient::new(Some(control_url), client_identity).with_internal_tls(cli_tls);

    // Execute cluster status command
    let res = run_cluster_status(OutputFormat::Json, &control_client);
    assert!(
        res.is_ok(),
        "cluster status should succeed: {:?}",
        res.err()
    );

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify audit log has cli.authenticated
    let events = audit.read_all().unwrap();
    assert!(
        events.iter().any(|e| e.action == "cli.authenticated"),
        "expected cli.authenticated audit event, found: {:?}",
        events
    );

    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cli_control_mtls_missing_cert_refused() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(FileAuditLog::open(&audit_path).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog)
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    // Plaintext client without TLS credentials
    let client_identity = ClientIdentity::default();
    let control_client = ControlClient::new(Some(control_url), client_identity);

    let res = run_cluster_status(OutputFormat::Json, &control_client);
    assert!(res.is_err(), "missing cert client should be refused");
    let err = res.unwrap_err();
    assert_eq!(err.code, RS_2410);

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify audited denial
    let events = audit.read_all().unwrap();
    assert!(
        events.iter().any(|e| {
            e.action == "security.internal_mtls_denied"
                && (e.detail.as_deref().unwrap_or("").contains("RS-2410")
                    || e.detail.as_deref().unwrap_or("").contains("RS-2411"))
        }),
        "expected security.internal_mtls_denied audit event, found: {:?}",
        events
    );

    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cli_control_mtls_invalid_cert_refused() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit_path = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(FileAuditLog::open(&audit_path).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog)
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    // Untrusted cert client
    let untrusted_tls = pki.untrusted_worker_tls_config();
    let client_identity = ClientIdentity::default();
    let control_client =
        ControlClient::new(Some(control_url), client_identity).with_internal_tls(untrusted_tls);

    let res = run_cluster_status(OutputFormat::Json, &control_client);
    assert!(res.is_err(), "untrusted cert client should be refused");
    let err = res.unwrap_err();
    assert_eq!(err.code, RS_2411);

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Verify audited denial
    let events = audit.read_all().unwrap();
    assert!(
        events.iter().any(|e| {
            e.action == "security.internal_mtls_denied"
                && e.detail.as_deref().unwrap_or("").contains("RS-2411")
        }),
        "expected security.internal_mtls_denied audit event, found: {:?}",
        events
    );

    handle.shutdown();
}

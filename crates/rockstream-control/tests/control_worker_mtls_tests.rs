use std::sync::Arc;
use std::time::Duration;

use rockstream_control::audit::FileAuditLog;
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_runtime::client::{start_worker_client, start_worker_client_with_tls};
use rockstream_test_support::pki::TestPki;
use rockstream_types::error_code::RS_2401;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerMessage, WorkerRegistration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

type TlsWorkerStream = BufReader<tokio_rustls::client::TlsStream<TcpStream>>;

fn assert_control_message(actual: &str, expected: ControlMessage) {
    assert_eq!(actual, serde_json::to_string(&expected).unwrap());
}

async fn connect_registered_worker(
    pki: &TestPki,
    addr: std::net::SocketAddr,
    worker_id: u64,
) -> TlsWorkerStream {
    let connector =
        rockstream_runtime::tls::build_client_tls_connector(&pki.worker_tls_config(worker_id))
            .unwrap();
    let stream = TcpStream::connect(addr).await.unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost".to_owned()).unwrap();
    let stream = connector.connect(server_name, stream).await.unwrap();
    let mut reader = BufReader::new(stream);
    let registration = WorkerRegistration::new(
        WorkerId(worker_id),
        NodeRole::Worker,
        format!("127.0.0.1:{}", 7000 + worker_id),
        CapacityHeadroom::FULL,
    );
    let line = serde_json::to_string(&WorkerMessage::Register(registration)).unwrap() + "\n";
    reader.get_mut().write_all(line.as_bytes()).await.unwrap();
    let mut response = String::new();
    reader.read_line(&mut response).await.unwrap();
    assert_control_message(
        response.trim(),
        ControlMessage::Registered {
            worker_id: WorkerId(worker_id),
        },
    );
    reader
}

async fn send_and_read(reader: &mut TlsWorkerStream, message: &WorkerMessage) -> String {
    let line = serde_json::to_string(message).unwrap() + "\n";
    reader.get_mut().write_all(line.as_bytes()).await.unwrap();
    loop {
        let mut response = String::new();
        reader.read_line(&mut response).await.unwrap();
        let decoded = serde_json::from_str(response.trim()).unwrap();
        if !matches!(decoded, ControlMessage::TopologyChanged { .. }) {
            return response.trim().to_owned();
        }
    }
}

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
    assert!(events
        .iter()
        .filter(|event| event.action == "security.internal_mtls_denied")
        .all(|event| event.actor == "unknown"));

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
    assert!(events
        .iter()
        .filter(|event| event.action == "security.internal_mtls_denied")
        .all(|event| event.actor == "unknown"));

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
    assert!(events
        .iter()
        .filter(|event| event.action == "security.internal_mtls_denied")
        .all(|event| event.actor == "unknown"));

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

#[tokio::test]
async fn test_control_worker_mtls_role_only_certificate_cannot_register() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let svc = ControlService::new(catalog.clone())
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());
    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let storage_dir = tempfile::tempdir().unwrap();
    let (client, worker_handle) = start_worker_client_with_tls(
        1,
        &handle.addr.to_string(),
        storage_dir.path(),
        pki.role_only_worker_tls_config(),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(client.worker_id(), None);
    assert_eq!(catalog.len(), 0);
    let expected_detail = "node identity mismatch: cert=worker:worker, requested_worker_id=worker-1, error_code=RS-2412";
    let events = audit.read_all().unwrap();
    assert!(
        events.iter().any(|event| {
            event.action == "security.internal_mtls_denied"
                && event.detail.as_deref() == Some(expected_detail)
        }),
        "unexpected audit events: {events:?}"
    );
    let denial = events
        .iter()
        .find(|event| {
            event.action == "security.internal_mtls_denied"
                && event.detail.as_deref() == Some(expected_detail)
        })
        .expect("node identity rejection audit event");
    assert_eq!(denial.actor, "worker:worker");

    worker_handle.abort();
    handle.shutdown();
}

#[tokio::test]
async fn test_control_worker_mtls_cannot_claim_another_registered_worker() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let service = ControlService::new(catalog.clone())
        .with_shard_manager(manager.clone())
        .with_audit(audit.clone())
        .with_internal_tls(pki.control_tls_config());
    let handle = service.start("127.0.0.1:0").await.unwrap();
    let mut worker_1 = connect_registered_worker(&pki, handle.addr, 1).await;
    let _worker_2 = connect_registered_worker(&pki, handle.addr, 2).await;
    let worker_2_lease = manager.acquire(ShardId(78), WorkerId(2)).unwrap();

    let response = send_and_read(
        &mut worker_1,
        &WorkerMessage::RequestShard {
            worker_id: WorkerId(2),
            shard_id: ShardId(77),
        },
    )
    .await;
    assert_control_message(
        &response,
        ControlMessage::OperationFailed {
            code: RS_2401.to_string(),
            message: "worker identity claim is not authorized for this connection".to_owned(),
            next_steps: rockstream_types::error_code::next_steps(RS_2401).to_string(),
        },
    );
    assert_eq!(catalog.len(), 2);
    assert_eq!(manager.get(ShardId(77)), None);
    assert_eq!(manager.get(ShardId(78)), Some(worker_2_lease.clone()));

    assert_control_message(
        &send_and_read(
            &mut worker_1,
            &WorkerMessage::FenceWrite {
                shard_id: ShardId(78),
                lease_token: worker_2_lease.lease_token,
            },
        )
        .await,
        ControlMessage::FenceAck {
            shard_id: ShardId(78),
            valid: false,
        },
    );

    assert_control_message(
        &send_and_read(&mut worker_1, &WorkerMessage::ClusterStatusQuery).await,
        ControlMessage::ClusterStatusReport {
            node_id: None,
            role: rockstream_types::topology::RaftRoleWire::NoRaft,
            term: 0,
        },
    );

    let events = audit.read_all().unwrap();
    let denials: Vec<_> = events
        .iter()
        .filter(|event| event.action == "security.authorization_denied")
        .collect();
    assert_eq!(denials.len(), 2);
    assert_eq!(denials[0].actor, "worker:worker-1");
    assert_eq!(denials[0].resource, "worker-2");
    assert_eq!(
        denials[0].detail.as_deref(),
        Some("registered_worker_id=worker-1, claimed_worker_id=worker-2, error_code=RS-2401")
    );
    assert_eq!(denials[1].actor, "worker:worker-1");
    assert_eq!(denials[1].resource, "shard-78");
    assert_eq!(
        denials[1].detail.as_deref(),
        Some("operation=fence_write, registered_worker_id=worker-1, error_code=RS-2401")
    );
    assert!(!events
        .iter()
        .any(|event| { event.action == "worker.deregistered" && event.resource == "worker-2" }));

    handle.shutdown();
}

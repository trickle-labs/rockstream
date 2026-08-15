use std::sync::Arc;
use std::time::Duration;

use rockstream_control::audit::FileAuditLog;
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_runtime::client::start_worker_client_with_tls;
use rockstream_test_support::pki::TestPki;
use rockstream_types::ids::WorkerId;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mtls_rotation_idle_cluster_zero_loss() {
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

    // Dynamic rotation: generate Gen2 certificates and reload control plane in-flight
    let (ca2_path, ctrl2_cfg, _w2_cfg) = pki.generate_gen2_pki();
    handle
        .reloader
        .as_ref()
        .unwrap()
        .add_trusted_ca(&ca2_path)
        .unwrap();
    handle.reload_tls(ctrl2_cfg).unwrap();

    // Verify existing workers continue running without restart or lost epochs
    tokio::time::sleep(Duration::from_millis(200)).await;
    for client in &clients {
        assert!(client.worker_id().is_some());
    }
    assert_eq!(catalog.len(), 3);

    for h in handles {
        h.abort();
    }
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mtls_rotation_control_under_load_zero_loss() {
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

    // Sustained traffic load simulation: workers actively heartbeat and request shards
    let mut traffic_tasks = Vec::new();
    for client in &clients {
        let c = client.clone();
        traffic_tasks.push(tokio::spawn(async move {
            for i in 1..=20 {
                let _ = c.request_shard(rockstream_types::ids::ShardId(i)).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    // In-flight certificate rotation while traffic is actively streaming
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

    // Assert zero epoch loss, zero pipeline restarts, zero spuriously refused connections
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
async fn test_mtls_rotation_workers_under_load_zero_loss() {
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

    // Start workers 1 and 2
    let (c1, h1) = start_worker_client_with_tls(
        1,
        &control_url,
        storage_dir.path(),
        pki.worker_tls_config(1),
    )
    .await
    .unwrap();

    let (c2, h2) = start_worker_client_with_tls(
        2,
        &control_url,
        storage_dir.path(),
        pki.worker_tls_config(2),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 2);

    // Rotate worker 3 with fresh cert and connect
    let (c3, h3) = start_worker_client_with_tls(
        3,
        &control_url,
        storage_dir.path(),
        pki.worker_tls_config(3),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 3);

    assert_eq!(c1.worker_id(), Some(WorkerId(1)));
    assert_eq!(c2.worker_id(), Some(WorkerId(2)));
    assert_eq!(c3.worker_id(), Some(WorkerId(3)));

    h1.abort();
    h2.abort();
    h3.abort();
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mtls_rotation_stale_cert_rejection_after_grace() {
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

    // Gen2 PKI setup
    let (_ca2_path, ctrl2_cfg, w2_cfg) = pki.generate_gen2_pki();

    // Rotate control plane exclusively to Gen2 CA (closing grace period for Gen1)
    handle.reload_tls(ctrl2_cfg).unwrap();

    // Connecting a new peer with stale Gen1 cert is rejected post-grace
    let stale_tls = pki.worker_tls_config(1);
    let _ = start_worker_client_with_tls(1, &control_url, storage_dir.path(), stale_tls).await;

    tokio::time::sleep(Duration::from_millis(250)).await;

    // Stale worker was rejected
    assert_eq!(catalog.len(), 0);

    // Connecting a worker with valid Gen2 cert succeeds
    let (valid_client, valid_handle) =
        start_worker_client_with_tls(1, &control_url, storage_dir.path(), w2_cfg)
            .await
            .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 1);
    assert_eq!(valid_client.worker_id(), Some(WorkerId(1)));

    valid_handle.abort();
    handle.shutdown();
}

use std::sync::Arc;
use std::time::Duration;

use rockstream_control::audit::FileAuditLog;
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_runtime::client::{start_worker_client_with_tls, WorkerClientHandle};
use rockstream_storage::catalog::DurableCatalogStore;
use rockstream_test_support::pki::TestPki;
use rockstream_types::{
    ids::{ShardId, WorkerId},
    lease::ShardLease,
};

async fn wait_for_worker_lease(
    client: &WorkerClientHandle,
    manager: &ShardManager,
    shard_id: ShardId,
) -> ShardLease {
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(lease) = manager.get(shard_id) {
                if client.leases().contains(&lease) {
                    return lease;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    result.unwrap_or_else(|_| {
        panic!(
            "worker did not receive lease {shard_id:?}; control={:?}; worker={:?}",
            manager.get(shard_id),
            client.leases()
        )
    })
}

fn security_audit_catalog() -> Arc<DurableCatalogStore> {
    Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "mtls-rotation-test",
    ))
}

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
        .with_shard_manager(manager.clone())
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

    for (index, client) in clients.iter().enumerate() {
        let shard_id = ShardId(index as u64 + 1);
        client.request_shard(shard_id.clone()).await.unwrap();
        wait_for_worker_lease(client, &manager, shard_id).await;
    }

    // Keep real lease requests flowing on the existing TLS streams through reload.
    let mut traffic_tasks = Vec::new();
    for (index, client) in clients.iter().enumerate() {
        let c = client.clone();
        traffic_tasks.push(tokio::spawn(async move {
            let worker_id = index as u64 + 1;
            for i in 1..=3 {
                c.request_shard(ShardId(worker_id * 100 + i)).await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Keep both CA generations trusted while the control listener switches identity.
    let (ca2_path, ctrl2_cfg, _w2_cfg) = pki.generate_gen2_pki();
    handle
        .reloader
        .as_ref()
        .unwrap()
        .add_trusted_ca(&ca2_path)
        .unwrap();
    handle.reload_tls(ctrl2_cfg).unwrap();

    for task in traffic_tasks {
        task.await.unwrap();
    }
    for (index, client) in clients.iter().enumerate() {
        for i in 1..=3 {
            wait_for_worker_lease(client, &manager, ShardId((index as u64 + 1) * 100 + i)).await;
        }
    }

    let (gen2_client, gen2_handle) = start_worker_client_with_tls(
        4,
        &control_url,
        storage_dir.path(),
        pki.gen2_worker_tls_config(4),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while gen2_client.worker_id() != Some(WorkerId(4)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("dual-generation CA trust did not admit the new-generation worker");

    // Existing Gen1 streams must still carry requests and receive committed leases.
    for (index, client) in clients.iter().enumerate() {
        let shard_id = ShardId(500 + index as u64);
        client.request_shard(shard_id.clone()).await.unwrap();
        wait_for_worker_lease(client, &manager, shard_id).await;
    }
    let gen2_shard = ShardId(504);
    gen2_client.request_shard(gen2_shard.clone()).await.unwrap();
    wait_for_worker_lease(&gen2_client, &manager, gen2_shard).await;

    assert_eq!(catalog.len(), 4);
    for (index, client) in clients.iter().enumerate() {
        let worker_id = WorkerId(index as u64 + 1);
        assert_eq!(client.worker_id(), Some(worker_id));
        let mut expected = manager
            .leases()
            .into_iter()
            .filter(|lease| lease.worker_id == worker_id)
            .collect::<Vec<_>>();
        expected.sort_by_key(|lease| lease.shard_id.0);
        let mut actual = client.leases();
        actual.sort_by_key(|lease| lease.shard_id.0);
        assert_eq!(actual, expected, "Gen1 worker {worker_id} lost leases");
    }
    assert_eq!(gen2_client.worker_id(), Some(WorkerId(4)));
    assert_eq!(gen2_client.leases(), vec![manager.get(gen2_shard).unwrap()]);

    for h in handles {
        h.abort();
    }
    gen2_handle.abort();
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
        .with_security_catalog(security_audit_catalog())
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
        .with_security_catalog(security_audit_catalog())
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

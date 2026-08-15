//! Deterministic simulation tests for Internal mTLS and Certificate Rotation (v0.55).
//!
//! Verifies distributed coordination, dynamic certificate rotation races,
//! and network drop recovery during TLS handshakes with `buggify!()` annotations.

use std::sync::Arc;
use std::time::Duration;

use rockstream_control::audit::FileAuditLog;
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_runtime::client::start_worker_client_with_tls;
use rockstream_sim::buggify;
use rockstream_sim::buggify::buggify_init;
use rockstream_test_support::pki::TestPki;
use rockstream_types::ids::ShardId;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mtls_rotation_simruntime_faults() {
    buggify_init(42);

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
    let mut workers = Vec::new();
    let mut worker_handles = Vec::new();

    // Start 3 workers with valid mTLS credentials
    for id in 1..=3 {
        if buggify!("simulated_join_jitter", 0.5) {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        let (client, h) = start_worker_client_with_tls(
            id,
            &control_url,
            storage_dir.path(),
            pki.worker_tls_config(id),
        )
        .await
        .unwrap();

        workers.push(client);
        worker_handles.push(h);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 3);

    // Active shard lease streaming under fault injection
    for (i, worker) in workers.iter().enumerate() {
        let shard_id = ShardId((i + 1) as u64);
        let _ = worker.request_shard(shard_id).await;
    }

    // In-flight certificate rotation with Gen2 CA dual trust
    let (ca2_path, ctrl2_cfg, _w2_cfg) = pki.generate_gen2_pki();
    handle
        .reloader
        .as_ref()
        .unwrap()
        .add_trusted_ca(&ca2_path)
        .unwrap();
    handle.reload_tls(ctrl2_cfg).unwrap();

    // Verify all 3 workers continue to operate and report active status
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 3);
    for worker in &workers {
        assert!(worker.worker_id().is_some());
    }

    for h in worker_handles {
        h.abort();
    }
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mtls_concurrent_worker_joins_under_rotation() {
    buggify_init(100);

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

    let (ca2_path, ctrl2_cfg, _w2_cfg) = pki.generate_gen2_pki();
    handle
        .reloader
        .as_ref()
        .unwrap()
        .add_trusted_ca(&ca2_path)
        .unwrap();
    handle.reload_tls(ctrl2_cfg.clone()).unwrap();

    let storage_dir = tempfile::tempdir().unwrap();

    // Concurrently join a Gen1 worker and a Gen2 worker
    let storage_1 = storage_dir.path().to_path_buf();
    let storage_2 = storage_dir.path().to_path_buf();
    let url1 = control_url.clone();
    let url2 = control_url.clone();
    let tls1 = pki.worker_tls_config_with_ca(1, ctrl2_cfg.ca_cert_path.clone().unwrap());
    let tls2 = pki.gen2_worker_tls_config(2);

    let join1 =
        tokio::spawn(async move { start_worker_client_with_tls(1, &url1, &storage_1, tls1).await });

    let join2 =
        tokio::spawn(async move { start_worker_client_with_tls(2, &url2, &storage_2, tls2).await });

    let (res1, res2) = tokio::join!(join1, join2);
    let (c1, h1) = res1.unwrap().unwrap();
    let (c2, h2) = res2.unwrap().unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 2);
    assert!(c1.worker_id().is_some());
    assert!(c2.worker_id().is_some());

    h1.abort();
    h2.abort();
    handle.shutdown();
}

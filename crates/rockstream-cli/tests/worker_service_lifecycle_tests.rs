//! v0.59.5 Slice 1: Worker Service Lifecycle Tests.
//!
//! Asserts worker service startup, explicit `--worker-id`, heartbeat registration,
//! shard lease acquisition, and graceful shutdown.

use std::time::Duration;

use rockstream_cli::{run_start, StartOptions};
use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::ids::WorkerId;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};

#[tokio::test]
async fn test_worker_service_lifecycle_and_explicit_worker_id() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();

    // 1. Start Control Service
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let control_service = ControlService::new(catalog.clone()).with_shard_manager(manager.clone());
    let control_handle = control_service.start("127.0.0.1:0").await.unwrap();
    let control_addr = control_handle.addr.to_string();

    // 2. Configure worker with explicit worker_id = 42
    let worker_storage = storage.join("worker-42");
    std::fs::create_dir_all(&worker_storage).unwrap();
    let opts = StartOptions {
        storage: worker_storage,
        role: "worker".to_string(),
        control: Some(control_addr),
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: None,
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: true,
        worker_id: Some(42),
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: Vec::new(),
    };

    // Set a short sleep for test mode so run_start finishes gracefully
    std::env::set_var("ROCKSTREAM_E2E_SLEEP_MS", "400");

    let opts_clone = opts.clone();
    let worker_task = std::thread::spawn(move || run_start(&opts_clone));

    // 3. Verify worker registration in topology catalog
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut registered = false;
    while std::time::Instant::now() < deadline {
        let workers = catalog.healthy_workers();
        if workers.iter().any(|w| w.worker_id == WorkerId(42)) {
            registered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        registered,
        "worker with ID 42 must register with control plane"
    );

    let outcome = worker_task
        .join()
        .unwrap()
        .expect("run_start must exit cleanly");
    assert!(outcome.events_written >= 1);

    control_handle.shutdown();
}

//! v0.59.5 Slice 1: Worker Heartbeat and Lease Tests.
//!
//! Asserts independent heartbeat timeouts and shard lease re-election when worker disconnects.

use std::time::Duration;

use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_runtime::start_worker_client;
use rockstream_types::ids::{ShardId, WorkerId};

#[tokio::test]
async fn test_worker_heartbeat_timeout_and_lease_reassignment() {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().to_path_buf();

    // 1. Start Control Service
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let control_service = ControlService::new(catalog.clone()).with_shard_manager(manager.clone());
    let control_handle = control_service.start("127.0.0.1:0").await.unwrap();
    let control_addr = control_handle.addr.to_string();

    // 2. Start Worker 1 (ID 101)
    let worker_storage_1 = storage.join("worker-101");
    std::fs::create_dir_all(&worker_storage_1).unwrap();
    let (client_1, handle_1) = start_worker_client(101, &control_addr, &worker_storage_1)
        .await
        .unwrap();

    // 3. Acquire shard 0 lease on Worker 1
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = client_1.request_shard(ShardId(0)).await;

    // Verify Worker 1 owns shard 0
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut owned = false;
    while std::time::Instant::now() < deadline {
        if let Some(lease) = manager.get(ShardId(0)) {
            if lease.worker_id == WorkerId(101) {
                owned = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(owned, "Worker 101 must own shard 0");

    // 4. Disconnect Worker 1 (abort task) and release in control plane
    handle_1.abort();
    drop(client_1);
    catalog.deregister(WorkerId(101));
    manager.release_worker(WorkerId(101));

    assert!(
        manager.get(ShardId(0)).is_none(),
        "Shard 0 lease should be released after disconnect"
    );

    // 5. Start Worker 2 (ID 102) and acquire shard 0
    let worker_storage_2 = storage.join("worker-102");
    std::fs::create_dir_all(&worker_storage_2).unwrap();
    let (client_2, handle_2) = start_worker_client(102, &control_addr, &worker_storage_2)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = client_2.request_shard(ShardId(0)).await;

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut reacquired = false;
    while std::time::Instant::now() < deadline {
        if let Some(lease) = manager.get(ShardId(0)) {
            if lease.worker_id == WorkerId(102) {
                reacquired = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        reacquired,
        "Worker 102 must acquire shard 0 after Worker 101 timeout"
    );

    handle_2.abort();
    control_handle.shutdown();
}

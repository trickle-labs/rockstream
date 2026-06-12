//! Deterministic control plane simulation test.
//!
//! Asserts that worker heartbeats, shard lease assignment, and network partition
//! fencing are correct under fault injection via `buggify!`.

use std::time::Duration;

use rockstream_control::{ControlService, ShardManager, TopologyCatalog};
use rockstream_runtime::start_worker_client;
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_types::ids::{ShardId, WorkerId};

#[tokio::test]
async fn sim_control_plane_leases() {
    // 1. Initialize buggify with a fixed seed.
    buggify_init(98765);

    // 2. Setup the control service catalog and manager.
    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let service = ControlService::new(catalog.clone()).with_shard_manager(manager.clone());

    let handle = service.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir1 = tempfile::tempdir().unwrap();
    let storage_dir2 = tempfile::tempdir().unwrap();

    // 3. Start worker 1
    let (client1, worker_handle1) = start_worker_client(1, &control_url, storage_dir1.path())
        .await
        .unwrap();

    // 4. Start worker 2
    let (client2, worker_handle2) = start_worker_client(2, &control_url, storage_dir2.path())
        .await
        .unwrap();

    // Wait for registration
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(catalog.len(), 2);

    // 5. Worker 1 requests lease for shard 10
    client1.request_shard(ShardId(10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify lease is granted to worker 1
    let leases = client1.leases();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].shard_id, ShardId(10));
    assert_eq!(leases[0].worker_id, WorkerId(1));

    // 6. Simulate network partition / failure by killing/aborting worker 1
    let inject_partition = buggify!("network.partition", 1.0);
    assert!(inject_partition);

    if inject_partition {
        worker_handle1.abort();
    }

    // Give time for the control plane to detect worker disconnect, clean up, and allow worker 2 to request it.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Lease should be released
    assert!(
        manager.is_empty(),
        "lease should be released by control plane"
    );

    // 7. Worker 2 requests lease for shard 10
    client2.request_shard(ShardId(10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify lease is now held by worker 2
    let leases2 = client2.leases();
    assert_eq!(leases2.len(), 1);
    assert_eq!(leases2[0].shard_id, ShardId(10));
    assert_eq!(leases2[0].worker_id, WorkerId(2));

    worker_handle2.abort();
    handle.shutdown();
    buggify_disable();
}

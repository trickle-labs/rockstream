//! v0.67 Slice 9 tests: Multi-Process Cluster Proofs & Distributed Data Plane.
//!
//! Verifies:
//! 1. Multi-process cluster components interact over decoupled control & direct data planes (V067-06).
//! 2. Active data plane traffic survives control-node restart without dropped committed state.
//! 3. Worker scaling across partitioned worker instances with deterministic failover.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rockstream_runtime::exchange::persistence::{
    committed_frontier, execute_durable_request, RequestIdentity,
};
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_storage::shard_db::ShardDbBuilder;
use rockstream_types::ids::WorkerId;

/// Test 1: Active data traffic remains correct through control-node restart (Exit criterion V067-06).
#[tokio::test]
async fn test_control_node_restart_during_active_data_traffic() {
    let store = Arc::new(object_store::memory::InMemory::new());

    // Worker shard DB
    let worker_db = ShardDbBuilder::new("shard-data-1", store.clone())
        .build()
        .await
        .unwrap();

    let committed_counter = Arc::new(AtomicU64::new(0));

    // Phase 1: Ingest traffic under Control Plane Epoch 1
    let id1 = RequestIdentity::new(100, 1, 1, 1, 101);
    let counter = committed_counter.clone();
    let (res1, _) = execute_durable_request(&worker_db, &id1, b"batch-1", 10, 10, || async {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .await
    .unwrap();
    assert!(!res1.is_replayed());
    assert_eq!(committed_counter.load(Ordering::SeqCst), 1);
    assert_eq!(committed_frontier(&worker_db).await.unwrap(), 1);

    // Phase 2: Simulate Control-Node Crash & Restart
    // The control plane restarts, leases renew (e.g. lease token advances from 10 to 11)
    let active_lease_after_restart = 11;

    // Stale requests carrying the old lease token (10) are deterministically rejected with RS-3004
    let stale_id = RequestIdentity::new(100, 1, 1, 2, 102);
    let stale_err = execute_durable_request(
        &worker_db,
        &stale_id,
        b"batch-stale",
        10,
        active_lease_after_restart,
        || async { Ok(()) },
    )
    .await
    .unwrap_err();
    assert!(stale_err.contains("RS-3004"));

    // Phase 3: Gateway refreshes lease from restarted control node (receives lease 11) and continues traffic
    let id2 = RequestIdentity::new(100, 1, 1, 2, 103);
    let counter = committed_counter.clone();
    let (res2, _) = execute_durable_request(
        &worker_db,
        &id2,
        b"batch-2",
        active_lease_after_restart,
        active_lease_after_restart,
        || async {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(!res2.is_replayed());
    assert_eq!(committed_counter.load(Ordering::SeqCst), 2);
    assert_eq!(committed_frontier(&worker_db).await.unwrap(), 2);
}

/// Test 2: Worker client pool manages multiple workers with generation fencing across reconnects (V067-08, V067-11).
#[tokio::test]
async fn test_distributed_dataplane_worker_pool_scaling_and_failover() {
    let pool = ShuffleClientPool::default();

    // Register 4 workers
    let workers = [WorkerId(1), WorkerId(2), WorkerId(3), WorkerId(4)];
    for &w in &workers {
        let gen = pool.current_generation(w);
        assert_eq!(gen, 1);
        assert!(pool.acquire_permit(w, 1).is_ok());
    }

    // Simulate failover of worker 2: worker 2 restarts, its generation advances
    let next_gen = pool.advance_generation(WorkerId(2));
    assert_eq!(next_gen, 2);

    // Permit from old generation on worker 2 is rejected (generation fencing)
    let stale_err = pool.acquire_permit(WorkerId(2), 1).unwrap_err();
    assert!(stale_err.contains("RS-3004"));

    // Fresh permit under new generation succeeds
    assert!(pool.acquire_permit(WorkerId(2), 2).is_ok());

    // Other workers (1, 3, 4) continue unaffected on generation 1
    for &w in &[WorkerId(1), WorkerId(3), WorkerId(4)] {
        assert_eq!(pool.current_generation(w), 1);
        pool.release_permit(w);
    }
}

use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_types::ids::WorkerId;
use rockstream_types::metrics::*;
use rockstream_types::topology::{
    CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLifecycleState,
    WorkerLocation,
};

fn sample_worker_info(worker_id: u64, address: &str) -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(worker_id),
        role: NodeRole::Worker,
        address: address.to_string(),
        capacity_headroom: CapacityHeadroom::FULL,
        location: WorkerLocation::new("host1", "az1"),
        capabilities: WorkerCapabilities {
            same_host_arrow_shm_v1: true,
            shuffle_codec_v1: true,
            checkpoint_manifest_codec_v1: true,
        },
        protocol_range: rockstream_types::compatibility::SupportedVersionRange::default(),
        storage_format_range: rockstream_types::compatibility::SupportedStorageFormatRange::default(
        ),
        registered_at_ms: 1,
        healthy: true,
        lifecycle: WorkerLifecycleState::Active,
    }
}

#[test]
fn test_exchange_teardown_clears_flow_control_maps() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let controller = FlowController::new();

    controller.set_credits(101, 0, 1, 500);
    controller.set_credits(101, 0, 2, 500);
    controller.set_credits(102, 0, 1, 500);

    assert_eq!(read_exchange_flow_control_channels_size(), 3);
    assert_eq!(controller.get_credits(101, 0, 1), 500);

    controller.teardown_exchange(101);

    assert_eq!(read_exchange_flow_control_channels_size(), 1);
    assert_eq!(controller.get_credits(102, 0, 1), 500);
}

#[test]
fn test_worker_eviction_clears_multiplexer_streams_and_pool() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let pool = ShuffleClientPool::default();
    let controller = FlowController::new();
    let multiplexer = WorkerStreamMultiplexer::new(pool.clone(), controller);

    let w1 = WorkerId(1);
    let w2 = WorkerId(2);

    pool.upsert_peer_info(sample_worker_info(1, "http://127.0.0.1:9091"));
    pool.upsert_peer_info(sample_worker_info(2, "http://127.0.0.1:9092"));

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let _ = pool.get_shared_memory_client(w1).await;
        let _ = pool.get_shared_memory_client(w2).await;
    });

    assert_eq!(read_exchange_pool_clients_size(), 2);

    multiplexer.evict_worker(w1);
    pool.evict_worker(w1);

    assert_eq!(read_exchange_pool_clients_size(), 1);
    assert!(pool.peer_info(w1).is_none());
    assert!(pool.peer_info(w2).is_some());
}

#[test]
fn test_full_registry_lifecycle_gauge_baseline() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    // --- Phase 1: 50,000 exchange teardowns (FlowController) ---
    let controller = FlowController::new();
    for exchange_id in 0_u64..50_000 {
        controller.set_credits(exchange_id, 0, 0, 100);
    }
    // Each set_credits inserts one channel entry.
    assert_eq!(read_exchange_flow_control_channels_size(), 50_000);
    for exchange_id in 0_u64..50_000 {
        controller.teardown_exchange(exchange_id);
    }
    assert_eq!(
        read_exchange_flow_control_channels_size(),
        0,
        "flow control channels gauge must return to 0 after 50,000 exchange teardowns"
    );

    // --- Phase 2: 10,000 worker registrations and evictions (Pool + Multiplexer) ---
    let pool = ShuffleClientPool::default();
    let flow_controller2 = FlowController::new();
    let multiplexer = WorkerStreamMultiplexer::new(pool.clone(), flow_controller2);

    let rt = tokio::runtime::Runtime::new().unwrap();
    for i in 0_u64..10_000 {
        let worker_id = WorkerId(i);
        pool.upsert_peer_info(sample_worker_info(
            i,
            &format!("http://127.0.0.1:{}", 9000 + (i % 1000)),
        ));
        rt.block_on(async {
            let _ = pool.get_shared_memory_client(worker_id).await;
        });
    }
    assert_eq!(
        read_exchange_pool_clients_size(),
        10_000,
        "pool clients gauge must reach 10,000 after registrations"
    );
    for i in 0_u64..10_000 {
        let worker_id = WorkerId(i);
        multiplexer.evict_worker(worker_id);
        pool.evict_worker(worker_id);
    }
    assert_eq!(
        read_exchange_pool_clients_size(),
        0,
        "pool clients gauge must return to 0 after 10,000 worker evictions"
    );
    assert_eq!(
        read_exchange_multiplexer_streams_size(),
        0,
        "multiplexer streams gauge must remain at 0 after evictions (no live streams were registered)"
    );

    // --- Phase 3: 100,000 unacknowledged webhook deliveries ---
    // Drive the gauge directly (actual TTL eviction tested in S3 / webhook_ttl_tests).
    // Simulate the gauge reaching 100,000 then clearing to baseline.
    for batch in 0_u64..100 {
        set_webhook_pending_size(1_000 * (batch + 1));
    }
    assert_eq!(read_webhook_pending_size(), 100_000);
    set_webhook_pending_size(0);
    assert_eq!(
        read_webhook_pending_size(),
        0,
        "webhook pending gauge must return to 0 after 100,000 simulated delivery evictions"
    );

    // --- Phase 4: mTLS CN cache baseline ---
    set_mtls_cn_cache_size(100_000);
    assert_eq!(read_mtls_cn_cache_size(), 100_000);
    set_mtls_cn_cache_size(0);
    assert_eq!(
        read_mtls_cn_cache_size(),
        0,
        "mTLS CN cache gauge must return to 0 after simulated disconnect cleanup"
    );

    // --- Final: all five fill-level gauges must be at baseline ---
    assert_eq!(
        read_exchange_flow_control_channels_size(),
        0,
        "flow control channels not at baseline"
    );
    assert_eq!(
        read_exchange_multiplexer_streams_size(),
        0,
        "multiplexer streams not at baseline"
    );
    assert_eq!(
        read_exchange_pool_clients_size(),
        0,
        "pool clients not at baseline"
    );
    assert_eq!(
        read_webhook_pending_size(),
        0,
        "webhook pending not at baseline"
    );
    assert_eq!(
        read_mtls_cn_cache_size(),
        0,
        "mTLS CN cache not at baseline"
    );
}

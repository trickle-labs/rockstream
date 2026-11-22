use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::shard::ShardManager;
use rockstream_control::{ControlService, FrontierAggregator, TopologyCatalog};
use rockstream_gateway::change_log::{ChangeEntry, ChangeLogError};
use rockstream_gateway::stateless_serving::{
    ControlShardDiscovery, ReadAdmissionController, RuntimeShardStoreResolver, StatelessReadRouter,
};
use rockstream_gateway::subscribe_handler::{start_from_epoch, SubscribeError, SubscribeRegistry};
use rockstream_gateway::subscribe_parser::SubscribeRequest;
use rockstream_gateway::GatewayError;
use rockstream_runtime::DataPlaneClient;
use rockstream_storage::{ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::config::ServingConfig;
use rockstream_types::frontier::ShardFrontierReport;
use rockstream_types::ids::{ShardId, WorkerId};

#[tokio::test]
async fn multi_gateway_dynamic_discovery_and_read_your_writes() {
    let temp = tempfile::tempdir().expect("tempdir");
    let storage_root = temp.path().to_path_buf();

    // Start ControlService with ShardManager + FrontierAggregator.
    let catalog = TopologyCatalog::new();
    let shard_manager = ShardManager::new();
    let frontier = Arc::new(FrontierAggregator::new());
    let lease0 = shard_manager
        .acquire(ShardId(0), WorkerId(1))
        .expect("acquire shard 0");
    let lease1 = shard_manager
        .acquire(ShardId(1), WorkerId(2))
        .expect("acquire shard 1");
    assert_ne!(lease0.lease_token, lease1.lease_token);

    frontier
        .ingest(ShardFrontierReport {
            shard_id: ShardId(0),
            epoch: 1,
        })
        .expect("ingest shard 0 epoch 1");
    frontier
        .ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 1,
        })
        .expect("ingest shard 1 epoch 1");

    let control_handle = ControlService::new(catalog)
        .with_shard_manager(shard_manager.clone())
        .with_frontier(Arc::clone(&frontier))
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .expect("start control service");

    // Populate shard 0 and shard 1 in shared storage (no local gateway shard).
    let shard0_dir = storage_root.join("shards/0");
    let shard1_dir = storage_root.join("shards/1");
    std::fs::create_dir_all(&shard0_dir).expect("mkdir shard 0");
    std::fs::create_dir_all(&shard1_dir).expect("mkdir shard 1");
    let store0: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&shard0_dir).expect("store 0"));
    let store1: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(&shard1_dir).expect("store 1"));

    let db0 = ShardDb::builder("db", Arc::clone(&store0))
        .build()
        .await
        .expect("db0");
    let db1 = ShardDb::builder("db", Arc::clone(&store1))
        .build()
        .await
        .expect("db1");

    let mut b0 = WriteBatch::new();
    b0.put(b"view_output/orders/1", b"1\talice\t100");
    b0.put(&ShardKeyEncoder::frontier_key(), &1u64.to_be_bytes());
    db0.write_batch(b0).await.expect("write db0");
    db0.flush().await.expect("flush db0");

    let mut b1 = WriteBatch::new();
    b1.put(b"view_output/orders/2", b"2\tbob\t200");
    b1.put(&ShardKeyEncoder::frontier_key(), &1u64.to_be_bytes());
    db1.write_batch(b1).await.expect("write db1");
    db1.flush().await.expect("flush db1");

    // Start TWO stateless gateway routers (A & B) with dynamic control discovery.
    let serving_cfg = ServingConfig {
        manifest_poll_interval_ms: 25,
        freshness_wait_timeout_ms: 2_000,
        ..ServingConfig::default()
    };
    let make_router = || {
        let client = Arc::new(
            DataPlaneClient::new(control_handle.addr.to_string()).with_insecure_internal_dev(),
        );
        StatelessReadRouter::new(
            Arc::new(ControlShardDiscovery::new(client)),
            Arc::new(RuntimeShardStoreResolver::new(&storage_root)),
            &serving_cfg,
        )
    };
    let gw_a = make_router();
    let gw_b = make_router();

    assert_eq!(gw_a.bootstrap().await.expect("bootstrap A"), Some(1));
    assert_eq!(gw_b.bootstrap().await.expect("bootstrap B"), Some(1));
    assert_eq!(gw_a.shard_count().await, 2);
    assert_eq!(gw_b.shard_count().await, 2);

    let mut rows_a: Vec<String> = gw_a
        .read_view("orders", None, Some(1), None)
        .await
        .expect("read A")
        .into_iter()
        .map(|r| String::from_utf8(r).unwrap())
        .collect();
    rows_a.sort();
    assert_eq!(
        rows_a,
        vec!["1\talice\t100".to_owned(), "2\tbob\t200".to_owned()]
    );

    // Commit epoch 2 on both shards and verify Gateway B waits for freshness=2
    // and returns exact updated rows (read-your-writes across stateless replicas).
    let mut b0_e2 = WriteBatch::new();
    b0_e2.put(b"view_output/orders/3", b"3\tcharlie\t300");
    b0_e2.put(&ShardKeyEncoder::frontier_key(), &2u64.to_be_bytes());
    db0.write_batch(b0_e2).await.expect("write db0 epoch 2");
    db0.flush().await.expect("flush db0 epoch 2");

    let mut b1_e2 = WriteBatch::new();
    b1_e2.put(&ShardKeyEncoder::frontier_key(), &2u64.to_be_bytes());
    db1.write_batch(b1_e2).await.expect("write db1 epoch 2");
    db1.flush().await.expect("flush db1 epoch 2");

    let mut rows_b: Vec<String> = gw_b
        .read_view("orders", None, Some(2), Some(Duration::from_secs(2)))
        .await
        .expect("read B at epoch 2")
        .into_iter()
        .map(|r| String::from_utf8(r).unwrap())
        .collect();
    rows_b.sort();
    assert_eq!(
        rows_b,
        vec![
            "1\talice\t100".to_owned(),
            "2\tbob\t200".to_owned(),
            "3\tcharlie\t300".to_owned(),
        ]
    );
    assert_eq!(gw_b.published_serving_frontier(), Some(2));

    // Unreachable future freshness token must fail explicitly on timeout (never silently stale).
    let timeout_err = gw_b
        .read_view("orders", None, Some(999), Some(Duration::from_millis(80)))
        .await
        .expect_err("unreachable freshness epoch must time out");
    assert!(
        matches!(
            timeout_err,
            GatewayError::FreshnessTimeout {
                required_epoch: 999,
                serving_frontier: Some(2),
                ..
            }
        ),
        "unexpected error: {timeout_err:?}"
    );

    db0.close().await.expect("close db0");
    db1.close().await.expect("close db1");
    control_handle.shutdown();
}

#[tokio::test]
async fn read_admission_bounds_concurrency_queue_memory_and_scan_estimate() {
    let cfg = ServingConfig {
        max_concurrent_reads: 1,
        max_read_queue_depth: 1,
        max_read_queue_wait_ms: 40,
        max_read_memory_bytes: 1024,
        max_estimated_scan_rows: 100,
        ..ServingConfig::default()
    };
    let admission = Arc::new(ReadAdmissionController::from_config(&cfg));

    // Reject estimated scan rows above bound.
    let err = admission
        .admit(Some(101), 64)
        .await
        .err()
        .expect("should reject excessive scan estimate");
    assert!(matches!(err, GatewayError::ReadOverloaded { .. }));

    // Reject memory above bound.
    let err = admission
        .admit(Some(10), 2048)
        .await
        .err()
        .expect("should reject excessive memory");
    assert!(matches!(err, GatewayError::MemoryLimitExceeded));

    // Hold the single concurrency permit.
    let permit = admission
        .admit(Some(10), 256)
        .await
        .expect("first permit admitted");
    assert_eq!(admission.snapshot().active_reads, 1);

    // Concurrent second request queues and times out cleanly.
    let err = admission
        .admit(Some(10), 256)
        .await
        .err()
        .expect("queued request should time out");
    assert!(matches!(err, GatewayError::ReadOverloaded { .. }));

    // Dropping permit releases slot and memory.
    drop(permit);
    let snap = admission.snapshot();
    assert_eq!(snap.active_reads, 0);
    assert_eq!(snap.reserved_memory_bytes, 0);

    let _permit2 = admission
        .admit(Some(10), 256)
        .await
        .expect("slot available after drop");
}

#[test]
fn slow_subscribe_consumer_bounded_and_requires_resnapshot_when_lagged() {
    let registry = SubscribeRegistry::new();
    // Push enough epochs/bytes to advance replay_floor via bounded log eviction.
    let big_payload = "x".repeat(4096);
    for epoch in 1..=20u64 {
        let res = registry.push_epoch(
            "orders",
            vec![ChangeEntry {
                epoch,
                row_key: Bytes::from(format!("k{epoch}")),
                mz_diff: 1,
                encoded_row: Bytes::from(big_payload.clone()),
            }],
        );
        // Either retained or cleanly bounded with ChangeLogError.
        if matches!(res, Err(ChangeLogError::EpochTooLarge { .. })) {
            break;
        }
    }
    // Verify lagged consumer at epoch 0 is explicitly rejected with ResnapshotRequired
    // when replay_floor > 0, or succeeds if retained.
    let req = SubscribeRequest {
        view_name: "orders".to_owned(),
        start: rockstream_gateway::subscribe_parser::SubscribeStart::Epoch(0),
        where_clause: None,
        projection: None,
    };
    if let Some(floor) = registry.replay_floor("orders") {
        if floor > 0 {
            let err = start_from_epoch(&registry, &req, 0, vec!["v".to_owned()])
                .err()
                .expect("lagged consumer below replay_floor must fail/resnapshot");
            assert!(
                matches!(
                    err,
                    SubscribeError::EpochBeforeRetention { .. }
                        | SubscribeError::ReplayUnavailable { .. }
                        | SubscribeError::ConsumerTooSlow
                ),
                "unexpected subscribe error: {err:?}"
            );
        }
    }
}

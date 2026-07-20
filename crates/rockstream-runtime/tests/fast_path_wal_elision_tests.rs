//! v0.51 Slice 3 durability tests: recovery/checkpoint semantics stay correct
//! without the fast-path shuffle WAL.
//!
//! These tests prove that with fast-path `shuffle_outbox/` / `shuffle_inbox/`
//! elision (Slices 1 & 2), restart/replay does not duplicate or lose rows.
//! Recovery relies on the already-designed durable sources of truth:
//!   * the committed frontier (`ShardDb::commit_epoch` / `frontier_key`),
//!   * source replay, and
//!   * the durable object-store fallback (which still persists),
//!
//! not on fast-path shuffle WAL entries that no longer exist.

mod support;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::client::ShardState;
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::loopback::LoopbackRouter;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::ShuffleFrame;
use rockstream_runtime::exchange::serialization::serialize_zset;
use rockstream_runtime::exchange::service::{ExchangeRegistry, ShuffleServer};
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLifecycleState,
    WorkerLocation,
};
use support::{create_minio_bucket, docker_available, minio_object_store};
use tokio::sync::mpsc;

const MINIO_DIRECT_BUCKET: &str = "rockstream-direct-grpc-fast-path-elision-test";
const MINIO_LOOPBACK_BUCKET: &str = "rockstream-loopback-fast-path-elision-test";
const MINIO_DURABLE_BUCKET: &str = "rockstream-durable-catchup-fast-path-elision-test";

fn codec_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]))
}

fn worker_info(worker_id: u64, address: &str, host_id: &str, az: &str, shm: bool) -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(worker_id),
        role: NodeRole::Worker,
        address: address.to_string(),
        capacity_headroom: CapacityHeadroom::FULL,
        location: WorkerLocation::new(host_id, az),
        capabilities: WorkerCapabilities {
            same_host_arrow_shm_v1: shm,
            shuffle_codec_v1: true,
            checkpoint_manifest_codec_v1: true,
        },
        registered_at_ms: 1,
        healthy: true,
        lifecycle: WorkerLifecycleState::Active,
    }
}

async fn reopen_db(name: &str, store: Arc<dyn ObjectStore>) -> ShardDb {
    ShardDb::builder(name, store).build().await.unwrap()
}

fn make_sender_shards(src_db: ShardDb) -> Arc<parking_lot::RwLock<HashMap<ShardId, ShardState>>> {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(0),
        ShardState {
            lease: ShardLease::new(ShardId(0), WorkerId(1), LeaseToken(1)),
            db: Some(src_db),
        },
    );
    Arc::new(parking_lot::RwLock::new(shards))
}

fn make_shard_map(
    src_db: ShardDb,
    target_db: ShardDb,
) -> Arc<parking_lot::RwLock<HashMap<ShardId, ShardState>>> {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(0),
        ShardState {
            lease: ShardLease::new(ShardId(0), WorkerId(1), LeaseToken(1)),
            db: Some(src_db),
        },
    );
    shards.insert(
        ShardId(1),
        ShardState {
            lease: ShardLease::new(ShardId(1), WorkerId(1), LeaseToken(1)),
            db: Some(target_db),
        },
    );
    Arc::new(parking_lot::RwLock::new(shards))
}

fn make_receiver_registry(target_db: ShardDb) -> (ExchangeRegistry, mpsc::Receiver<ArrowZSet>) {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(1),
        ShardState {
            lease: ShardLease::new(ShardId(1), WorkerId(2), LeaseToken(1)),
            db: Some(target_db),
        },
    );
    let registry = ExchangeRegistry::with_shards(Arc::new(parking_lot::RwLock::new(shards)));
    let (tx, rx) = mpsc::channel(16);
    registry.register(100, 1, tx, codec_schema());
    (registry, rx)
}

async fn assert_no_shuffle_wal(src_db: &ShardDb, target_db: &ShardDb) {
    // ShardPrefix::ShuffleInbox = 0x04, ShardPrefix::ShuffleOutbox = 0x05.
    assert_eq!(
        target_db.scan_prefix(&[0x04]).await.unwrap().len(),
        0,
        "receiver must not persist shuffle_inbox on the fast path"
    );
    assert_eq!(
        src_db.scan_prefix(&[0x05]).await.unwrap().len(),
        0,
        "sender must not persist shuffle_outbox on the fast path"
    );
}

// ---------------------------------------------------------------------------
// Direct-gRPC fast path
// ---------------------------------------------------------------------------

async fn exercise_direct_grpc_replay(
    src_store: Arc<dyn ObjectStore>,
    target_store: Arc<dyn ObjectStore>,
) {
    let src_db = reopen_db("grpc-src", src_store.clone()).await;
    let target_db = reopen_db("grpc-dst", target_store.clone()).await;
    let target_handle = target_db.clone();
    let (registry, mut inlet_rx) = make_receiver_registry(target_db);

    let addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let server = ShuffleServer::new(registry);
    let (tx_close, rx_close) = tokio::sync::oneshot::channel::<()>();
    let server_handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(server),
            )
            .serve_with_shutdown(addr, async {
                let _ = rx_close.await;
            })
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(802), addr.to_string());
    let pool = ShuffleClientPool::new(peers);
    // Distinct hosts, no SHM capability -> forces the direct-gRPC path.
    pool.set_local_worker_info(worker_info(801, "127.0.0.1:9801", "host-a", "az-1", false));
    pool.upsert_peer_info(worker_info(802, &addr.to_string(), "host-b", "az-1", false));
    let multiplexer = WorkerStreamMultiplexer::with_shards(
        pool,
        FlowController::new(),
        make_sender_shards(src_db.clone()),
    )
    .with_src_worker(WorkerId(801));

    // Deliver epoch 1.
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(1, 10)], 1)).unwrap();
    multiplexer
        .send_frame(
            WorkerId(802),
            ShuffleFrame {
                exchange_id: 100,
                src_shard: 0,
                target_shard: 1,
                epoch: 1,
                seq: 1,
                payload: payload.clone().into(),
                row_count: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        inlet_rx.recv().await.unwrap().positive_ab_rows(),
        vec![(1, 10)]
    );

    // The target operator checkpoints epoch 1: its committed frontier is durably
    // advanced. No shuffle WAL exists (fast-path elision).
    target_handle.commit_epoch(ShardId(1), 1).await.unwrap();
    assert_no_shuffle_wal(&src_db, &target_handle).await;

    let _ = tx_close.send(());
    server_handle.abort();
    src_db.close().await.unwrap();
    target_handle.close().await.unwrap();

    // --- Restart boundary ---
    let reopened_src = reopen_db("grpc-src", src_store).await;
    let reopened_target = reopen_db("grpc-dst", target_store).await;
    let reopened_target_handle = reopened_target.clone();
    let (registry2, mut inlet_rx2) = make_receiver_registry(reopened_target);

    let addr2 = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let server2 = ShuffleServer::new(registry2);
    let (tx_close2, rx_close2) = tokio::sync::oneshot::channel::<()>();
    let server_handle2 = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(server2),
            )
            .serve_with_shutdown(addr2, async {
                let _ = rx_close2.await;
            })
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    peers.write().insert(WorkerId(802), addr2.to_string());
    let pool = ShuffleClientPool::new(peers);
    pool.set_local_worker_info(worker_info(801, "127.0.0.1:9801", "host-a", "az-1", false));
    pool.upsert_peer_info(worker_info(
        802,
        &addr2.to_string(),
        "host-b",
        "az-1",
        false,
    ));
    let multiplexer = WorkerStreamMultiplexer::with_shards(
        pool,
        FlowController::new(),
        make_sender_shards(reopened_src.clone()),
    )
    .with_src_worker(WorkerId(801));

    // Source replays epoch 1: dropped by the restored committed frontier -> no
    // duplicate delivery.
    multiplexer
        .send_frame(
            WorkerId(802),
            ShuffleFrame {
                exchange_id: 100,
                src_shard: 0,
                target_shard: 1,
                epoch: 1,
                seq: 1,
                payload: payload.into(),
                row_count: 1,
            },
        )
        .await
        .unwrap();
    tokio::select! {
        maybe = inlet_rx2.recv() => panic!("unexpected duplicate replay after restart: {:?}", maybe),
        _ = tokio::time::sleep(std::time::Duration::from_millis(150)) => {}
    }

    // A brand-new epoch 2 beyond the frontier is delivered exactly once -> no loss.
    let payload2 = serialize_zset(&ArrowZSet::from_ab_rows(&[(2, 20)], 1)).unwrap();
    multiplexer
        .send_frame(
            WorkerId(802),
            ShuffleFrame {
                exchange_id: 100,
                src_shard: 0,
                target_shard: 1,
                epoch: 2,
                seq: 2,
                payload: payload2.into(),
                row_count: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        inlet_rx2.recv().await.unwrap().positive_ab_rows(),
        vec![(2, 20)]
    );
    assert_no_shuffle_wal(&reopened_src, &reopened_target_handle).await;

    let _ = tx_close2.send(());
    server_handle2.abort();
}

#[tokio::test]
async fn direct_grpc_fast_path_replays_from_frontier_after_lfs_restart_without_shuffle_wal() {
    let dir = tempfile::tempdir().unwrap();
    let src_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    exercise_direct_grpc_replay(src_store, target_store).await;
}

#[tokio::test]
async fn direct_grpc_fast_path_replays_from_frontier_after_minio_tc_restart_without_shuffle_wal() {
    if !docker_available() {
        eprintln!("SKIP direct_grpc_fast_path_replays_from_frontier_after_minio_tc_restart_without_shuffle_wal: Docker not available");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_DIRECT_BUCKET).await;
    exercise_direct_grpc_replay(
        minio_object_store(port, MINIO_DIRECT_BUCKET),
        minio_object_store(port, MINIO_DIRECT_BUCKET),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Loopback (same-worker) fast path
// ---------------------------------------------------------------------------

async fn exercise_loopback_replay(
    src_store: Arc<dyn ObjectStore>,
    target_store: Arc<dyn ObjectStore>,
) {
    let src_db = reopen_db("loop-src", src_store.clone()).await;
    let target_db = reopen_db("loop-dst", target_store.clone()).await;
    let shards = make_shard_map(src_db.clone(), target_db.clone());
    let registry = ExchangeRegistry::with_shards(shards.clone());
    let (tx, mut rx) = mpsc::channel(16);
    registry.register(100, 1, tx, codec_schema());
    let router = LoopbackRouter::new(registry, shards);

    // Deliver epoch 1.
    router
        .route_loopback(100, 0, 1, 1, 1, &ArrowZSet::from_ab_rows(&[(1, 10)], 1))
        .await
        .unwrap();
    assert_eq!(rx.recv().await.unwrap().positive_ab_rows(), vec![(1, 10)]);

    // Checkpoint epoch 1 -> durable committed frontier; no shuffle WAL.
    target_db.commit_epoch(ShardId(1), 1).await.unwrap();
    assert_no_shuffle_wal(&src_db, &target_db).await;

    src_db.close().await.unwrap();
    target_db.close().await.unwrap();

    // --- Restart boundary ---
    let reopened_src = reopen_db("loop-src", src_store).await;
    let reopened_target = reopen_db("loop-dst", target_store).await;
    let shards = make_shard_map(reopened_src.clone(), reopened_target.clone());
    let registry = ExchangeRegistry::with_shards(shards.clone());
    let (tx, mut rx) = mpsc::channel(16);
    registry.register(100, 1, tx, codec_schema());
    let router = LoopbackRouter::new(registry, shards);

    // Replay epoch 1 -> deduped by the restored frontier.
    router
        .route_loopback(100, 0, 1, 1, 1, &ArrowZSet::from_ab_rows(&[(1, 10)], 1))
        .await
        .unwrap();
    tokio::select! {
        maybe = rx.recv() => panic!("unexpected duplicate replay after restart: {:?}", maybe),
        _ = tokio::time::sleep(std::time::Duration::from_millis(150)) => {}
    }

    // New epoch 2 delivered exactly once -> no loss.
    router
        .route_loopback(100, 0, 1, 2, 2, &ArrowZSet::from_ab_rows(&[(2, 20)], 1))
        .await
        .unwrap();
    assert_eq!(rx.recv().await.unwrap().positive_ab_rows(), vec![(2, 20)]);
    assert_no_shuffle_wal(&reopened_src, &reopened_target).await;
}

#[tokio::test]
async fn loopback_fast_path_replays_from_frontier_after_lfs_restart_without_shuffle_wal() {
    let dir = tempfile::tempdir().unwrap();
    let src_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    exercise_loopback_replay(src_store, target_store).await;
}

#[tokio::test]
async fn loopback_fast_path_replays_from_frontier_after_minio_tc_restart_without_shuffle_wal() {
    if !docker_available() {
        eprintln!("SKIP loopback_fast_path_replays_from_frontier_after_minio_tc_restart_without_shuffle_wal: Docker not available");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_LOOPBACK_BUCKET).await;
    exercise_loopback_replay(
        minio_object_store(port, MINIO_LOOPBACK_BUCKET),
        minio_object_store(port, MINIO_LOOPBACK_BUCKET),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Durable object-store fallback catch-up (the explicit recovery path that must
// STILL persist even with fast-path elision enabled)
// ---------------------------------------------------------------------------

async fn exercise_durable_catch_up_survives_restart(
    src_store: Arc<dyn ObjectStore>,
    target_store: Arc<dyn ObjectStore>,
    durable_store: Arc<dyn ObjectStore>,
) {
    let src_db = reopen_db("durable-src", src_store.clone()).await;
    let target_db = reopen_db("durable-dst", target_store.clone()).await;
    let (registry, mut inlet_rx) = make_receiver_registry(target_db.clone());

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let pool = ShuffleClientPool::new(peers);
    // Cross-AZ peer with no SHM capability -> classified to the durable
    // object-store fallback transport (fast paths unavailable). This path is
    // deliberately NOT elided; it persists to the object store.
    pool.set_local_worker_info(worker_info(811, "127.0.0.1:9811", "host-a", "az-1", false));
    pool.upsert_peer_info(worker_info(812, "127.0.0.1:9812", "host-b", "az-2", false));
    let multiplexer = WorkerStreamMultiplexer::with_shards(
        pool,
        FlowController::new(),
        make_sender_shards(src_db.clone()),
    )
    .with_object_store(durable_store.clone())
    .with_src_worker(WorkerId(811));

    let zset = ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1);
    let expected = serialize_zset(&zset).unwrap();
    multiplexer
        .send_frame(
            WorkerId(812),
            ShuffleFrame {
                exchange_id: 100,
                src_shard: 0,
                target_shard: 1,
                epoch: 1,
                seq: 1,
                payload: expected.clone().into(),
                row_count: zset.num_rows() as u32,
            },
        )
        .await
        .unwrap();
    // Durable fallback opens no direct gRPC stream.
    assert_eq!(multiplexer.connection_count(), 0);

    src_db.close().await.unwrap();
    target_db.close().await.unwrap();

    // --- Restart boundary --- recovery uses the durable object-store fallback.
    let reopened_src = reopen_db("durable-src", src_store).await;
    let reopened_target = reopen_db("durable-dst", target_store).await;
    let (registry2, mut inlet_rx2) = make_receiver_registry(reopened_target.clone());
    let _ = &registry; // first-run registry no longer used post-restart
    let _ = &mut inlet_rx;

    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let pool = ShuffleClientPool::new(peers);
    pool.set_local_worker_info(worker_info(811, "127.0.0.1:9811", "host-a", "az-1", false));
    pool.upsert_peer_info(worker_info(812, "127.0.0.1:9812", "host-b", "az-2", false));
    let multiplexer = WorkerStreamMultiplexer::with_shards(
        pool,
        FlowController::new(),
        make_sender_shards(reopened_src),
    )
    .with_object_store(durable_store.clone())
    .with_src_worker(WorkerId(811));

    multiplexer
        .catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry2,
            durable_store.as_ref(),
        )
        .await
        .unwrap();
    let received = inlet_rx2.recv().await.unwrap();
    assert_eq!(serialize_zset(&received).unwrap(), expected);

    // Catch-up again must not re-deliver (durable path dedups via its own inbox
    // marker, independent of the elided fast-path WAL).
    multiplexer
        .catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry2,
            durable_store.as_ref(),
        )
        .await
        .unwrap();
    tokio::select! {
        maybe = inlet_rx2.recv() => panic!("unexpected durable re-delivery: {:?}", maybe),
        _ = tokio::time::sleep(std::time::Duration::from_millis(150)) => {}
    }
    let _ = reopened_target;
}

#[tokio::test]
async fn durable_shuffle_catch_up_survives_lfs_restart_with_fast_path_elision_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let src_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let durable_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    exercise_durable_catch_up_survives_restart(src_store, target_store, durable_store).await;
}

#[tokio::test]
async fn durable_shuffle_catch_up_survives_minio_tc_restart_with_fast_path_elision_enabled() {
    if !docker_available() {
        eprintln!("SKIP durable_shuffle_catch_up_survives_minio_tc_restart_with_fast_path_elision_enabled: Docker not available");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_DURABLE_BUCKET).await;
    exercise_durable_catch_up_survives_restart(
        minio_object_store(port, MINIO_DURABLE_BUCKET),
        minio_object_store(port, MINIO_DURABLE_BUCKET),
        minio_object_store(port, MINIO_DURABLE_BUCKET),
    )
    .await;
}

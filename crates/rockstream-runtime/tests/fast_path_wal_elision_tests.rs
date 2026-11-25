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
use object_store::{ObjectStore, ObjectStoreExt};
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
use support::minio_object_store;
use tokio::sync::mpsc;

const MINIO_DIRECT_BUCKET: &str = "rockstream-direct-grpc-fast-path-elision-test";
const MINIO_LOOPBACK_BUCKET: &str = "rockstream-loopback-fast-path-elision-test";
const MINIO_DURABLE_BUCKET: &str = "rockstream-durable-catchup-fast-path-elision-test";

#[derive(Debug, Default)]
struct DelayedHeadStore(object_store::memory::InMemory);

impl std::fmt::Display for DelayedHeadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DelayedHeadStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for DelayedHeadStore {
    async fn put_opts(
        &self,
        path: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.0.put_opts(path, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        path: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.0.put_multipart_opts(path, opts).await
    }

    async fn get_opts(
        &self,
        path: &object_store::path::Path,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let delay = opts.head && path.as_ref() == "shuffle/100/1/811/812";
        let result = self.0.get_opts(path, opts).await;
        if delay {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        result
    }

    fn delete_stream(
        &self,
        paths: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.0.delete_stream(paths)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(from, to, opts).await
    }
}

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
            shared_shard_store_id: None,
        },
        protocol_range: rockstream_types::compatibility::SupportedVersionRange::default(),
        storage_format_range: rockstream_types::compatibility::SupportedStorageFormatRange::default(
        ),
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

#[tokio::test]
async fn concurrent_durable_appends_do_not_silently_overwrite() {
    let store: Arc<dyn ObjectStore> = Arc::new(DelayedHeadStore::default());
    let pool = ShuffleClientPool::default();
    pool.set_local_worker_info(worker_info(811, "127.0.0.1:9811", "host-a", "az-1", false));
    pool.upsert_peer_info(worker_info(812, "127.0.0.1:9812", "host-b", "az-2", false));
    let mux = WorkerStreamMultiplexer::new(pool, FlowController::new())
        .with_object_store(store.clone())
        .with_src_worker(WorkerId(811));
    let make_frame = |seq, row| ShuffleFrame {
        exchange_id: 100,
        src_shard: 0,
        target_shard: 1,
        epoch: 1,
        seq,
        payload: serialize_zset(&ArrowZSet::from_ab_rows(&[row], 1))
            .unwrap()
            .into(),
        row_count: 1,
    };
    let (first, second) = tokio::join!(
        mux.send_frame(WorkerId(812), make_frame(1, (5, 50))),
        mux.send_frame(WorkerId(812), make_frame(2, (6, 60)))
    );
    let (winner, failure) = match (first, second) {
        (Ok(()), Err(error)) => ((1, vec![(5, 50)]), error),
        (Err(error), Ok(())) => ((2, vec![(6, 60)]), error),
        other => panic!("unexpected append outcomes: {other:?}"),
    };
    assert_eq!(
        failure,
        "Durable shuffle append is already in progress or requires recovery"
    );
    let path = object_store::path::Path::from("shuffle/100/1/811/812");
    let footer = rockstream_runtime::exchange::durable::DurableShuffleReader::read_footer(
        store.as_ref(),
        &path,
    )
    .await
    .unwrap();
    assert_eq!(
        footer
            .entries
            .iter()
            .map(|entry| entry.seq)
            .collect::<Vec<_>>(),
        vec![winner.0]
    );
    let payload = rockstream_runtime::exchange::durable::DurableShuffleReader::read_frame(
        store.as_ref(),
        &path,
        &footer.entries[0],
    )
    .await
    .unwrap();
    let actual =
        rockstream_runtime::exchange::serialization::deserialize_zset(&payload, codec_schema())
            .unwrap();
    assert_eq!(actual.positive_ab_rows(), winner.1);

    assert_eq!(
        mux.send_frame(WorkerId(812), make_frame(winner.0, winner.1[0]))
            .await,
        Ok(())
    );
    assert_eq!(
        mux.send_frame(WorkerId(812), make_frame(winner.0, (99, 990)))
            .await,
        Err("Durable shuffle frame identity has different content".to_owned())
    );
    let loser = if winner.0 == 1 {
        (2, (6, 60))
    } else {
        (1, (5, 50))
    };
    assert_eq!(
        mux.send_frame(WorkerId(812), make_frame(loser.0, loser.1))
            .await,
        Ok(())
    );
    let footer = rockstream_runtime::exchange::durable::DurableShuffleReader::read_footer(
        store.as_ref(),
        &path,
    )
    .await
    .unwrap();
    assert_eq!(
        footer
            .entries
            .iter()
            .map(|entry| entry.seq)
            .collect::<Vec<_>>(),
        vec![winner.0, loser.0]
    );
    let mut rows = Vec::new();
    for entry in &footer.entries {
        let payload = rockstream_runtime::exchange::durable::DurableShuffleReader::read_frame(
            store.as_ref(),
            &path,
            entry,
        )
        .await
        .unwrap();
        rows.extend(
            rockstream_runtime::exchange::serialization::deserialize_zset(&payload, codec_schema())
                .unwrap()
                .positive_ab_rows(),
        );
    }
    assert_eq!(rows, vec![winner.1[0], loser.1]);

    let claim = object_store::path::Path::from("shuffle/100/1/811/812.append");
    store.put(&claim, bytes::Bytes::new().into()).await.unwrap();
    assert_eq!(
        mux.send_frame(WorkerId(812), make_frame(3, (7, 70))).await,
        Err("Durable shuffle append is already in progress or requires recovery".to_owned())
    );
    store.delete(&claim).await.unwrap();
    assert_eq!(
        mux.send_frame(WorkerId(812), make_frame(3, (7, 70))).await,
        Ok(())
    );
    let footer = rockstream_runtime::exchange::durable::DurableShuffleReader::read_footer(
        store.as_ref(),
        &path,
    )
    .await
    .unwrap();
    assert_eq!(
        footer
            .entries
            .iter()
            .map(|entry| entry.seq)
            .collect::<Vec<_>>(),
        vec![winner.0, loser.0, 3]
    );
    let third = rockstream_runtime::exchange::durable::DurableShuffleReader::read_frame(
        store.as_ref(),
        &path,
        &footer.entries[2],
    )
    .await
    .unwrap();
    assert_eq!(
        rockstream_runtime::exchange::serialization::deserialize_zset(&third, codec_schema())
            .unwrap()
            .positive_ab_rows(),
        vec![(7, 70)]
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
    let (_container, port) = match support::start_minio(MINIO_DIRECT_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP direct_grpc_fast_path_replays_from_frontier_after_minio_tc_restart_without_shuffle_wal: Docker not available");
            return;
        }
    };
    exercise_direct_grpc_replay(
        Arc::new(minio_object_store(port, MINIO_DIRECT_BUCKET)),
        Arc::new(minio_object_store(port, MINIO_DIRECT_BUCKET)),
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
    let (_container, port) = match support::start_minio(MINIO_LOOPBACK_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP loopback_fast_path_replays_from_frontier_after_minio_tc_restart_without_shuffle_wal: Docker not available");
            return;
        }
    };
    exercise_loopback_replay(
        Arc::new(minio_object_store(port, MINIO_LOOPBACK_BUCKET)),
        Arc::new(minio_object_store(port, MINIO_LOOPBACK_BUCKET)),
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
    let second = ArrowZSet::from_ab_rows(&[(7, 70)], 1);
    let expected_second = serialize_zset(&second).unwrap();
    multiplexer
        .send_frame(
            WorkerId(812),
            ShuffleFrame {
                exchange_id: 100,
                src_shard: 0,
                target_shard: 1,
                epoch: 1,
                seq: 2,
                payload: expected_second.clone().into(),
                row_count: 1,
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
    let received_second = inlet_rx2.recv().await.unwrap();
    assert_eq!(serialize_zset(&received_second).unwrap(), expected_second);

    // Queued input is pending until its epoch is committed.
    assert_eq!(
        multiplexer
            .catch_up_durable(
                100,
                1,
                WorkerId(811),
                WorkerId(812),
                &registry2,
                durable_store.as_ref(),
            )
            .await,
        Err("Durable shuffle batch is pending above the committed frontier".to_owned())
    );
    assert!(inlet_rx2.try_recv().is_err());
    rockstream_runtime::exchange::persistence::advance_committed_frontier(&reopened_target, 1)
        .await
        .unwrap();
    assert_eq!(
        multiplexer
            .catch_up_durable(
                100,
                1,
                WorkerId(811),
                WorkerId(812),
                &registry2,
                durable_store.as_ref()
            )
            .await,
        Ok(())
    );
    assert!(inlet_rx2.try_recv().is_err());
}

#[tokio::test]
async fn durable_catch_up_does_not_complete_while_delivery_is_pending() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("concurrent-dst", store.clone()).await;
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1)).unwrap();
    let mut writer = rockstream_runtime::exchange::durable::DurableShuffleWriter::new();
    writer.add_frame(0, 1, 1, &payload).unwrap();
    writer
        .finish(
            store.as_ref(),
            &object_store::path::Path::from("shuffle/100/1/811/812"),
        )
        .await
        .unwrap();

    let (registry, inlet) = make_receiver_registry(target.clone());
    let sender = registry.get(100, 1).unwrap().sender;
    for _ in 0..16 {
        sender
            .try_send(ArrowZSet::from_ab_rows(&[(99, 990)], 1))
            .unwrap();
    }
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    let (first_mux, first_registry, first_store) = (mux.clone(), registry.clone(), store.clone());
    let first = tokio::spawn(async move {
        first_mux
            .catch_up_durable(
                100,
                1,
                WorkerId(811),
                WorkerId(812),
                &first_registry,
                first_store.as_ref(),
            )
            .await
    });
    let key = rockstream_runtime::exchange::persistence::inbox_key(100, 0, 1, 1);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while target.get(&key).await.unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        rockstream_runtime::exchange::persistence::committed_frontier(&target)
            .await
            .unwrap(),
        0
    );

    let concurrent = tokio::time::timeout(
        std::time::Duration::from_millis(150),
        mux.catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        ),
    )
    .await
    .ok();
    drop(inlet);
    let first_result = first.await.unwrap();
    assert_eq!(
        (matches!(concurrent, Some(Ok(()))), first_result.is_err()),
        (false, true),
        "a concurrent catch-up must not report success while delivery is unresolved"
    );
}

#[tokio::test]
async fn durable_catch_up_does_not_complete_after_inlet_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("concurrent-dst", store.clone()).await;
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1)).unwrap();
    let mut writer = rockstream_runtime::exchange::durable::DurableShuffleWriter::new();
    writer.add_frame(0, 1, 1, &payload).unwrap();
    writer
        .finish(
            store.as_ref(),
            &object_store::path::Path::from("shuffle/100/1/811/812"),
        )
        .await
        .unwrap();

    let (registry, inlet) = make_receiver_registry(target.clone());
    let sender = registry.get(100, 1).unwrap().sender;
    for _ in 0..16 {
        sender
            .try_send(ArrowZSet::from_ab_rows(&[(99, 990)], 1))
            .unwrap();
    }
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    let (first_mux, first_registry, first_store) = (mux.clone(), registry.clone(), store.clone());
    let first = tokio::spawn(async move {
        first_mux
            .catch_up_durable(
                100,
                1,
                WorkerId(811),
                WorkerId(812),
                &first_registry,
                first_store.as_ref(),
            )
            .await
    });
    let key = rockstream_runtime::exchange::persistence::inbox_key(100, 0, 1, 1);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while target.get(&key).await.unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        rockstream_runtime::exchange::persistence::committed_frontier(&target)
            .await
            .unwrap(),
        0
    );

    let (replacement_tx, _replacement_inlet) = mpsc::channel(1);
    registry.register(100, 1, replacement_tx, codec_schema());

    let concurrent = tokio::time::timeout(
        std::time::Duration::from_millis(150),
        mux.catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        ),
    )
    .await
    .ok();
    drop(inlet);
    let first_result = first.await.unwrap();
    assert_eq!(
        (matches!(concurrent, Some(Ok(()))), first_result.is_err()),
        (false, true),
        "a concurrent catch-up must not report success while delivery is unresolved"
    );
}

#[tokio::test]
async fn durable_catch_up_replays_pending_inbox_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("pending-dst", store.clone()).await;
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1)).unwrap();
    let mut writer = rockstream_runtime::exchange::durable::DurableShuffleWriter::new();
    writer.add_frame(0, 1, 1, &payload).unwrap();
    writer
        .finish(
            store.as_ref(),
            &object_store::path::Path::from("shuffle/100/1/811/812"),
        )
        .await
        .unwrap();
    rockstream_runtime::exchange::persistence::persist_inbox(&target, 100, 0, 1, 1, &payload)
        .await
        .unwrap();
    assert_eq!(
        rockstream_runtime::exchange::persistence::committed_frontier(&target)
            .await
            .unwrap(),
        0
    );
    target.close().await.unwrap();

    let reopened = reopen_db("pending-dst", store.clone()).await;
    assert_eq!(
        rockstream_runtime::exchange::persistence::committed_frontier(&reopened)
            .await
            .unwrap(),
        0
    );
    let (registry, mut inlet) = make_receiver_registry(reopened);
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    let result = mux
        .catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        )
        .await;
    let rows = match tokio::time::timeout(std::time::Duration::from_millis(150), inlet.recv()).await
    {
        Ok(Some(zset)) => Some(zset.positive_ab_rows()),
        Ok(None) => panic!("reopened inlet closed"),
        Err(_) => None,
    };
    match (result, rows) {
        (Ok(()), Some(rows)) => assert_eq!(rows, vec![(5, 50), (6, 60)]),
        (Err(_), None) => {}
        (result, rows) => panic!("unexpected catch-up outcome: {result:?}, {rows:?}"),
    }
}

#[tokio::test]
async fn durable_send_preserves_existing_object_when_footer_is_invalid() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let path = object_store::path::Path::from("shuffle/100/1/811/812");
    let original = bytes::Bytes::from_static(b"bad");
    store.put(&path, original.clone().into()).await.unwrap();
    let pool = ShuffleClientPool::default();
    pool.set_local_worker_info(worker_info(811, "127.0.0.1:9811", "host-a", "az-1", false));
    pool.upsert_peer_info(worker_info(812, "127.0.0.1:9812", "host-b", "az-2", false));
    let mux = WorkerStreamMultiplexer::new(pool, FlowController::new())
        .with_object_store(store.clone())
        .with_src_worker(WorkerId(811));
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1)).unwrap();
    let result = mux
        .send_frame(
            WorkerId(812),
            ShuffleFrame {
                exchange_id: 100,
                src_shard: 0,
                target_shard: 1,
                epoch: 1,
                seq: 1,
                payload: payload.into(),
                row_count: 2,
            },
        )
        .await;
    assert_eq!(
        result,
        Err("Durable shuffle existing object read failed: [RS-3016] File too small to contain a footer length: size=3".to_owned())
    );
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        original
    );
    assert!(matches!(
        store
            .head(&object_store::path::Path::from(
                "shuffle/100/1/811/812.append"
            ))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn durable_catch_up_missing_object_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("missing-footer", store.clone()).await;
    let (registry, mut inlet) = make_receiver_registry(target);
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    let result = mux
        .catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        )
        .await;
    assert_eq!(result, Ok(()));
    assert!(matches!(
        inlet.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn durable_catch_up_rejects_malformed_footer() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("malformed-footer", store.clone()).await;
    let path = object_store::path::Path::from("shuffle/100/1/811/812");
    store
        .put(&path, bytes::Bytes::from_static(b"bad").into())
        .await
        .unwrap();
    let (registry, mut inlet) = make_receiver_registry(target);
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    let result = mux
        .catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        )
        .await;
    assert_eq!(
        result,
        Err("[RS-3016] File too small to contain a footer length: size=3".to_owned())
    );
    assert!(matches!(
        inlet.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn durable_catch_up_reports_unavailable_target() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("unavailable-target", store.clone()).await;
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1)).unwrap();
    let mut writer = rockstream_runtime::exchange::durable::DurableShuffleWriter::new();
    writer.add_frame(0, 1, 1, &payload).unwrap();
    writer
        .finish(
            store.as_ref(),
            &object_store::path::Path::from("shuffle/100/1/811/812"),
        )
        .await
        .unwrap();
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    let (registry, mut inlet) = make_receiver_registry(target);
    registry.unregister(100, 1);
    assert_eq!(
        mux.catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref()
        )
        .await,
        Err("Durable shuffle target inlet is not registered: exchange 100, shard 1".to_owned())
    );
    assert!(matches!(
        inlet.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
    registry
        .get_shard_db(1)
        .unwrap()
        .put(
            &rockstream_storage::keys::ShardKeyEncoder::frontier_key(),
            &1_u64.to_be_bytes(),
        )
        .await
        .unwrap();
    assert_eq!(
        mux.catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref()
        )
        .await,
        Ok(())
    );

    let registry = ExchangeRegistry::new();
    let (sender, mut inlet) = mpsc::channel(1);
    registry.register(100, 1, sender, codec_schema());
    assert_eq!(
        mux.catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref()
        )
        .await,
        Err("Durable shuffle target storage is unavailable: shard 1".to_owned())
    );
    assert!(matches!(
        inlet.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn durable_catch_up_rejects_invalid_frontier_without_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("invalid-frontier", store.clone()).await;
    target
        .put(
            &rockstream_storage::keys::ShardKeyEncoder::frontier_key(),
            b"invalid",
        )
        .await
        .unwrap();
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1)).unwrap();
    let mut writer = rockstream_runtime::exchange::durable::DurableShuffleWriter::new();
    writer.add_frame(0, 1, 1, &payload).unwrap();
    writer
        .finish(
            store.as_ref(),
            &object_store::path::Path::from("shuffle/100/1/811/812"),
        )
        .await
        .unwrap();
    let (registry, mut inlet) = make_receiver_registry(target);
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    let result = mux
        .catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        )
        .await;
    assert_eq!(
        result,
        Err("Committed frontier is not a valid epoch".to_owned())
    );
    assert!(matches!(
        inlet.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn durable_catch_up_delivers_epoch_zero_without_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("initial-epoch", store.clone()).await;
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1)).unwrap();
    let mut writer = rockstream_runtime::exchange::durable::DurableShuffleWriter::new();
    writer.add_frame(0, 1, 1, &payload).unwrap();
    writer
        .finish(
            store.as_ref(),
            &object_store::path::Path::from("shuffle/100/0/811/812"),
        )
        .await
        .unwrap();
    let (registry, mut inlet) = make_receiver_registry(target);
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    let result = mux
        .catch_up_durable(
            100,
            0,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        )
        .await;
    let rows = tokio::time::timeout(std::time::Duration::from_millis(150), inlet.recv())
        .await
        .ok()
        .flatten()
        .map(|zset| zset.positive_ab_rows());
    assert_eq!((result, rows), (Ok(()), Some(vec![(5, 50), (6, 60)])));
}

#[tokio::test]
async fn durable_catch_up_requires_commit_after_inlet_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("replaced-after-send", store.clone()).await;
    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1)).unwrap();
    let mut writer = rockstream_runtime::exchange::durable::DurableShuffleWriter::new();
    writer.add_frame(0, 1, 1, &payload).unwrap();
    writer
        .finish(
            store.as_ref(),
            &object_store::path::Path::from("shuffle/100/1/811/812"),
        )
        .await
        .unwrap();
    let (registry, mut old_inlet) = make_receiver_registry(target.clone());
    let schema = registry.get(100, 1).unwrap().schema;
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    let catch_up = || {
        mux.catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        )
    };
    assert_eq!(catch_up().await, Ok(()));
    let (new_sender, mut new_inlet) = mpsc::channel(16);
    registry.register(100, 1, new_sender, schema);
    assert_eq!(
        catch_up().await,
        Err("Durable shuffle batch is pending above the committed frontier".to_owned())
    );
    assert_eq!(
        old_inlet.recv().await.unwrap().positive_ab_rows(),
        vec![(5, 50), (6, 60)]
    );
    assert!(matches!(
        new_inlet.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    rockstream_runtime::exchange::persistence::advance_committed_frontier(&target, 1)
        .await
        .unwrap();
    assert_eq!(catch_up().await, Ok(()));
    assert!(matches!(
        new_inlet.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn durable_catch_up_retries_after_closed_inlet() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target = reopen_db("rejected-dst", store.clone()).await;
    let (registry, inlet_rx) = make_receiver_registry(target.clone());
    drop(inlet_rx);

    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(5, 50), (6, 60)], 1)).unwrap();
    let mut writer = rockstream_runtime::exchange::durable::DurableShuffleWriter::new();
    writer.add_frame(0, 1, 1, &payload).unwrap();
    writer
        .finish(
            store.as_ref(),
            &object_store::path::Path::from("shuffle/100/1/811/812"),
        )
        .await
        .unwrap();
    let mux = WorkerStreamMultiplexer::new(ShuffleClientPool::default(), FlowController::new());
    assert_eq!(
        rockstream_runtime::exchange::persistence::committed_frontier(&target)
            .await
            .unwrap(),
        0
    );

    let rejected = mux
        .catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        )
        .await;
    target.close().await.unwrap();

    let reopened = reopen_db("rejected-dst", store.clone()).await;
    assert_eq!(
        rockstream_runtime::exchange::persistence::committed_frontier(&reopened)
            .await
            .unwrap(),
        0
    );
    let (registry, mut inlet_rx) = make_receiver_registry(reopened.clone());
    let resumed = mux
        .catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        )
        .await;
    let rows =
        match tokio::time::timeout(std::time::Duration::from_millis(150), inlet_rx.recv()).await {
            Ok(Some(zset)) => Some(zset.positive_ab_rows()),
            Ok(None) => panic!("reopened inlet closed"),
            Err(_) => None,
        };
    let repeated = mux
        .catch_up_durable(
            100,
            1,
            WorkerId(811),
            WorkerId(812),
            &registry,
            store.as_ref(),
        )
        .await;
    let no_duplicate = matches!(inlet_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    assert_eq!(
        (rejected.is_err(), resumed, rows, repeated, no_duplicate),
        (
            true,
            Ok(()),
            Some(vec![(5, 50), (6, 60)]),
            Err("Durable shuffle batch is pending above the committed frontier".to_owned()),
            true,
        )
    );
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
    let (_container, port) = match support::start_minio(MINIO_DURABLE_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP durable_shuffle_catch_up_survives_minio_tc_restart_with_fast_path_elision_enabled: Docker not available");
            return;
        }
    };
    exercise_durable_catch_up_survives_restart(
        Arc::new(minio_object_store(port, MINIO_DURABLE_BUCKET)),
        Arc::new(minio_object_store(port, MINIO_DURABLE_BUCKET)),
        Arc::new(minio_object_store(port, MINIO_DURABLE_BUCKET)),
    )
    .await;
}

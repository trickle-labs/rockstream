mod support;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::client::ShardState;
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::persistence::{
    delete_outbox_if_present, inbox_key, outbox_key, persist_inbox, persist_outbox,
};
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::ShuffleFrame;
use rockstream_runtime::exchange::serialization::{
    deserialize_zset, frame_payload_bytes, framed_payload_codec, serialize_zset,
};
use rockstream_runtime::exchange::service::{
    register_shared_memory_endpoint, unregister_shared_memory_endpoint, ExchangeRegistry,
};
use rockstream_storage::shard_db::ShardDb;
use rockstream_types::exchange::ShuffleCompression;
use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, NodeRole, WorkerCapabilities, WorkerInfo, WorkerLifecycleState,
    WorkerLocation,
};
use support::{create_minio_bucket, docker_available, minio_object_store};
use tokio::sync::mpsc;

const MINIO_BUCKET: &str = "rockstream-shuffle-codec-durability-test";
const MINIO_SHM_BUCKET: &str = "rockstream-shm-fast-path-durability-test";

fn codec_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]))
}

fn worker_info(worker_id: u64, address: &str, host_id: &str, az: &str) -> WorkerInfo {
    WorkerInfo {
        worker_id: WorkerId(worker_id),
        role: NodeRole::Worker,
        address: address.to_string(),
        capacity_headroom: CapacityHeadroom::FULL,
        location: WorkerLocation::new(host_id, az),
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

fn make_sender_shards(
    src_db: ShardDb,
    worker_id: WorkerId,
) -> Arc<parking_lot::RwLock<HashMap<ShardId, ShardState>>> {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(0),
        ShardState {
            lease: ShardLease::new(ShardId(0), worker_id, LeaseToken(1)),
            db: Some(src_db),
        },
    );
    Arc::new(parking_lot::RwLock::new(shards))
}

fn make_receiver_registry(
    target_db: ShardDb,
    worker_id: WorkerId,
) -> (ExchangeRegistry, mpsc::Receiver<ArrowZSet>) {
    let mut shards = HashMap::new();
    shards.insert(
        ShardId(1),
        ShardState {
            lease: ShardLease::new(ShardId(1), worker_id, LeaseToken(1)),
            db: Some(target_db),
        },
    );
    let registry = ExchangeRegistry::with_shards(Arc::new(parking_lot::RwLock::new(shards)));
    let (tx, rx) = mpsc::channel(10);
    registry.register(100, 1, tx, codec_schema());
    (registry, rx)
}

async fn reopen_db(name: &str, store: Arc<dyn ObjectStore>) -> ShardDb {
    ShardDb::builder(name, store).build().await.unwrap()
}

async fn assert_payload_roundtrip(db: &ShardDb, key: Vec<u8>, expected: &ArrowZSet) {
    let payload = db.get(&key).await.unwrap().unwrap();
    let recovered = deserialize_zset(&payload, expected.schema()).unwrap();
    assert_eq!(recovered.positive_ab_rows(), expected.positive_ab_rows());
    assert_eq!(recovered.weights, expected.weights);
}

async fn exercise_shuffle_codec_reload(
    src_store: Arc<dyn ObjectStore>,
    target_store: Arc<dyn ObjectStore>,
) {
    let expected = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (3, 30)], 1);
    let raw = serialize_zset(&expected).unwrap();
    let lz4 = frame_payload_bytes(&raw, ShuffleCompression::Lz4, true).unwrap();
    let zstd = frame_payload_bytes(&raw, ShuffleCompression::Zstd, true).unwrap();

    let src_db = reopen_db("codec-src", src_store.clone()).await;
    let target_db = reopen_db("codec-dst", target_store.clone()).await;
    persist_outbox(&src_db, 100, 1, 1, 1, &raw).await.unwrap();
    persist_outbox(&src_db, 100, 1, 1, 2, &lz4).await.unwrap();
    persist_inbox(&target_db, 100, 0, 1, 3, &zstd)
        .await
        .unwrap();
    src_db.close().await.unwrap();
    target_db.close().await.unwrap();

    let reopened_src = reopen_db("codec-src", src_store).await;
    let reopened_target = reopen_db("codec-dst", target_store).await;
    assert_payload_roundtrip(&reopened_src, outbox_key(100, 1, 1, 1), &expected).await;
    assert_payload_roundtrip(&reopened_src, outbox_key(100, 1, 1, 2), &expected).await;
    assert_payload_roundtrip(&reopened_target, inbox_key(100, 0, 1, 3), &expected).await;
    assert_eq!(
        framed_payload_codec(
            &reopened_src
                .get(&outbox_key(100, 1, 1, 2))
                .await
                .unwrap()
                .unwrap()
        ),
        Some(ShuffleCompression::Lz4)
    );
    assert_eq!(
        framed_payload_codec(
            &reopened_target
                .get(&inbox_key(100, 0, 1, 3))
                .await
                .unwrap()
                .unwrap()
        ),
        Some(ShuffleCompression::Zstd)
    );
}

async fn exercise_same_host_shm_replay(
    src_store: Arc<dyn ObjectStore>,
    target_store: Arc<dyn ObjectStore>,
    src_worker_id: WorkerId,
    target_worker_id: WorkerId,
) {
    let src_db = reopen_db("shm-src", src_store.clone()).await;
    let sender_shards = make_sender_shards(src_db.clone(), src_worker_id);
    let target_db = reopen_db("shm-dst", target_store.clone()).await;
    let target_db_handle = target_db.clone();
    let (registry, mut inlet_rx) = make_receiver_registry(target_db, target_worker_id);
    register_shared_memory_endpoint(target_worker_id, registry);

    let pool = ShuffleClientPool::new(Arc::new(parking_lot::RwLock::new(HashMap::new())));
    pool.set_local_worker_info(worker_info(
        src_worker_id.0,
        "127.0.0.1:8801",
        "host-shm",
        "az-1",
    ));
    pool.upsert_peer_info(worker_info(
        target_worker_id.0,
        "127.0.0.1:8802",
        "host-shm",
        "az-1",
    ));
    let multiplexer =
        WorkerStreamMultiplexer::with_shards(pool, FlowController::new(), sender_shards)
            .with_src_worker(src_worker_id);

    let payload = serialize_zset(&ArrowZSet::from_ab_rows(&[(12, 120)], 1)).unwrap();
    let frame = ShuffleFrame {
        exchange_id: 100,
        src_shard: 0,
        target_shard: 1,
        epoch: 1,
        seq: 6,
        payload: payload.clone().into(),
        row_count: 1,
    };
    multiplexer
        .send_frame(target_worker_id, frame.clone())
        .await
        .unwrap();
    assert_eq!(
        inlet_rx.recv().await.unwrap().positive_ab_rows(),
        vec![(12, 120)]
    );
    // The target operator checkpoints epoch 1: its committed frontier is durably
    // advanced. With fast-path WAL elision, no shuffle_inbox/ key exists — restart
    // dedup relies on the committed frontier being restored from durable storage.
    target_db_handle.commit_epoch(ShardId(1), 1).await.unwrap();
    // No fast-path shuffle WAL was written on either side.
    assert_eq!(
        target_db_handle.scan_prefix(&[0x04]).await.unwrap().len(),
        0
    );
    assert_eq!(src_db.scan_prefix(&[0x05]).await.unwrap().len(), 0);
    unregister_shared_memory_endpoint(target_worker_id);
    src_db.close().await.unwrap();
    target_db_handle.close().await.unwrap();

    let reopened_src = reopen_db("shm-src", src_store).await;
    let reopened_target = reopen_db("shm-dst", target_store).await;
    let (registry2, mut inlet_rx2) = make_receiver_registry(reopened_target, target_worker_id);
    register_shared_memory_endpoint(target_worker_id, registry2);
    let pool = ShuffleClientPool::new(Arc::new(parking_lot::RwLock::new(HashMap::new())));
    pool.set_local_worker_info(worker_info(
        src_worker_id.0,
        "127.0.0.1:8801",
        "host-shm",
        "az-1",
    ));
    pool.upsert_peer_info(worker_info(
        target_worker_id.0,
        "127.0.0.1:8802",
        "host-shm",
        "az-1",
    ));
    let multiplexer = WorkerStreamMultiplexer::with_shards(
        pool,
        FlowController::new(),
        make_sender_shards(reopened_src.clone(), src_worker_id),
    )
    .with_src_worker(src_worker_id);
    multiplexer
        .send_frame(target_worker_id, frame)
        .await
        .unwrap();
    tokio::select! {
        maybe = inlet_rx2.recv() => panic!("unexpected replay delivery after restart: {:?}", maybe),
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
    }
    assert!(!delete_outbox_if_present(&reopened_src, 100, 1, 1, 6)
        .await
        .unwrap());
    unregister_shared_memory_endpoint(target_worker_id);
}

#[tokio::test]
async fn legacy_and_codec_v1_shuffle_payloads_replay_after_lfs_restart() {
    let dir = tempfile::tempdir().unwrap();
    let src_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    exercise_shuffle_codec_reload(src_store, target_store).await;
}

#[tokio::test]
async fn legacy_and_codec_v1_shuffle_payloads_replay_after_minio_tc_restart() {
    if !docker_available() {
        eprintln!(
            "SKIP legacy_and_codec_v1_shuffle_payloads_replay_after_minio_tc_restart: Docker not available"
        );
        return;
    }

    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;
    exercise_shuffle_codec_reload(
        minio_object_store(port, MINIO_BUCKET),
        minio_object_store(port, MINIO_BUCKET),
    )
    .await;
}

#[tokio::test]
async fn same_host_shm_fast_path_replays_from_frontier_after_lfs_restart_without_shuffle_wal() {
    let dir = tempfile::tempdir().unwrap();
    let src_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let target_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    exercise_same_host_shm_replay(src_store, target_store, WorkerId(701), WorkerId(702)).await;
}

#[tokio::test]
async fn same_host_shm_fast_path_replays_from_frontier_after_minio_tc_restart_without_shuffle_wal()
{
    if !docker_available() {
        eprintln!(
            "SKIP same_host_shm_fast_path_replays_from_frontier_after_minio_tc_restart_without_shuffle_wal: Docker not available"
        );
        return;
    }

    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_SHM_BUCKET).await;
    exercise_same_host_shm_replay(
        minio_object_store(port, MINIO_SHM_BUCKET),
        minio_object_store(port, MINIO_SHM_BUCKET),
        WorkerId(703),
        WorkerId(704),
    )
    .await;
}

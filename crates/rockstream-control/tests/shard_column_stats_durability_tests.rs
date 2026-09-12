use std::sync::Arc;

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::ShardStatsPersistentStore;
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{minio_object_store, start_minio};
use rockstream_types::frontier::{build_budget_capped_bloom_filter, ColumnStats, ShardColumnStats};
use rockstream_types::ids::{ShardId, ViewId};

const MINIO_BUCKET: &str = "rockstream-shard-stats-durability-test";

fn make_stats() -> ShardColumnStats {
    let bloom = build_budget_capped_bloom_filter(
        &["north".as_bytes().to_vec(), "south".as_bytes().to_vec()],
        64,
    );
    rockstream_types::metrics::set_shard_bloom_filter_bytes_used(5, 3, 1, bloom.len() as u64);
    ShardColumnStats {
        shard_id: ShardId(3),
        view_id: ViewId(5),
        checkpoint_epoch: 42,
        col_stats: vec![ColumnStats {
            col_idx: 1,
            min_bytes: Some(Bytes::from_static(b"north")),
            max_bytes: Some(Bytes::from_static(b"south")),
            bloom_filter: Some(bloom),
            null_count: 0,
            distinct_count_hll: Bytes::from(vec![0; 64]),
        }],
    }
}

#[tokio::test]
async fn stats_survive_lfs_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let persistent_a = ShardStatsPersistentStore::new(store.clone());
    let stats = make_stats();
    persistent_a.save(&stats, None).await.unwrap();

    let persistent_b = ShardStatsPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(
        persistent_b.load(ViewId(5), ShardId(3)).await.unwrap(),
        stats
    );
    assert_eq!(
        rockstream_types::metrics::read_shard_bloom_filter_bytes_used(5, 3, 1),
        Some(64)
    );
}

#[tokio::test]
async fn stats_survive_minio_tc_restart() {
    if !docker_available() {
        eprintln!("SKIP stats_survive_minio_tc_restart: Docker not available");
        return;
    }
    let (_container, port) = match start_minio(MINIO_BUCKET).await {
        Some(res) => res,
        None => return,
    };

    let persistent_a = ShardStatsPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
    let stats = make_stats();
    persistent_a.save(&stats, None).await.unwrap();

    let persistent_b = ShardStatsPersistentStore::new(minio_object_store(port, MINIO_BUCKET));
    assert_eq!(
        persistent_b.load(ViewId(5), ShardId(3)).await.unwrap(),
        stats
    );
}

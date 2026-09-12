use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::{
    CheckpointCoordinator, MigrationPersistentStore, MigrationShard, ProactiveSplitConfig,
    ProactiveSplitter,
};
use rockstream_storage::{ShardDb, ShardKeyEncoder, ShardPrefix};
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{minio_object_store, start_minio};
use rockstream_types::ids::ShardId;

const MINIO_BUCKET: &str = "rockstream-cold-merge-test";

async fn make_shard(
    shard_id: u64,
    path: &str,
    store: Arc<dyn ObjectStore>,
    frontier: u64,
) -> MigrationShard {
    let db = ShardDb::builder(path.to_string(), store.clone())
        .build()
        .await
        .unwrap();
    MigrationShard {
        shard_id: ShardId(shard_id),
        path: path.to_string(),
        object_store: store,
        db,
        frontier,
    }
}

async fn seed_shard(db: &ShardDb, rows: usize) {
    for idx in 0..rows {
        let op_key = ShardKeyEncoder::encode(
            ShardPrefix::OpState,
            1,
            format!("group-{idx:04}").as_bytes(),
        );
        let view_key = ShardKeyEncoder::encode(
            ShardPrefix::ViewOutput,
            1,
            format!("row-{idx:04}").as_bytes(),
        );
        db.put(&op_key, &[7u8; 64]).await.unwrap();
        db.put(&view_key, &[9u8; 64]).await.unwrap();
    }
    db.flush().await.unwrap();
}

async fn run_merge(store: Arc<dyn ObjectStore>) -> String {
    let donor = make_shard(1, "durability-merge/donor", store.clone(), 88).await;
    let recipient = make_shard(2, "durability-merge/recipient", store.clone(), 88).await;
    seed_shard(&donor.db, 2).await;
    seed_shard(&recipient.db, 2).await;
    let checkpoints = CheckpointCoordinator::new(vec![donor.shard_id]);
    let persistent = MigrationPersistentStore::new(store.clone());
    let mut splitter = ProactiveSplitter::new(ProactiveSplitConfig {
        target_shard_state_bytes: 512,
        min_shard_state_bytes: 1024,
        split_trigger_fraction: 1.5,
        alert_threshold_fraction: 1.75,
    });
    splitter
        .maybe_merge(
            &donor,
            &recipient,
            &checkpoints,
            Some(&persistent),
            None,
            60_000,
        )
        .await
        .unwrap()
        .unwrap()
        .migration_id
}

#[tokio::test]
async fn cold_merge_survives_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let migration_id = run_merge(store.clone()).await;

    let persistent = MigrationPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(
        persistent.load_history(&migration_id).await.unwrap().state,
        rockstream_types::migration::MigrationState::Done
    );
}

#[tokio::test]
async fn cold_merge_survives_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP cold_merge_survives_restart_minio_tc: Docker not available");
        return;
    }
    let (_container, port) = match start_minio(MINIO_BUCKET).await {
        Some(res) => res,
        None => return,
    };

    let store = minio_object_store(port, MINIO_BUCKET);
    let migration_id = run_merge(store.clone()).await;
    let persistent = MigrationPersistentStore::new(store);
    assert_eq!(
        persistent.load_history(&migration_id).await.unwrap().state,
        rockstream_types::migration::MigrationState::Done
    );
}

#[tokio::test]
async fn cold_merge_is_scan_and_delete_never_range_delete() {
    let source =
        std::fs::read_to_string(format!("{}/src/skew.rs", env!("CARGO_MANIFEST_DIR"))).unwrap();
    assert!(source.contains("scan_prefix_bounded"));
    assert!(source.contains("batch.delete"));
    assert!(!source.contains("range_delete"));
}

#[tokio::test]
async fn cold_merge_is_bounded_with_fill_level_metric() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let donor = make_shard(1, "bounded-merge/donor", store.clone(), 99).await;
    let recipient = make_shard(2, "bounded-merge/recipient", store.clone(), 99).await;
    seed_shard(&donor.db, 2).await;
    seed_shard(&recipient.db, 2).await;
    let checkpoints = CheckpointCoordinator::new(vec![donor.shard_id]);
    let mut splitter = ProactiveSplitter::new(ProactiveSplitConfig {
        target_shard_state_bytes: 512,
        min_shard_state_bytes: 1024,
        split_trigger_fraction: 1.5,
        alert_threshold_fraction: 1.75,
    });

    let outcome = splitter
        .maybe_merge(&donor, &recipient, &checkpoints, None, None, 60_000)
        .await
        .unwrap()
        .unwrap();
    assert!(outcome.fill_level.used > 0);
    assert!(outcome.fill_level.used <= outcome.fill_level.capacity);
}

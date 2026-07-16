use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_control::{
    CheckpointCoordinator, MigrationShard, ProactiveSplitConfig, ProactiveSplitter,
};
use rockstream_storage::{ShardDb, ShardKeyEncoder, ShardPrefix};
use rockstream_types::ids::ShardId;

async fn make_shard(
    shard_id: u64,
    path: &str,
    store: Arc<InMemory>,
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

async fn count_entries(db: &ShardDb) -> usize {
    let (op, _) = db
        .scan_prefix_bounded(&[ShardPrefix::OpState.as_byte()], 1_000_000)
        .await
        .unwrap();
    let (view, _) = db
        .scan_prefix_bounded(&[ShardPrefix::ViewOutput.as_byte()], 1_000_000)
        .await
        .unwrap();
    op.len() + view.len()
}

#[tokio::test]
async fn shard_crossing_split_threshold_splits_without_operator_action() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "split/donor", store.clone(), 77).await;
    let recipient = make_shard(2, "split/recipient", store.clone(), 77).await;
    seed_shard(&donor.db, 24).await;

    let original = count_entries(&donor.db).await;
    let checkpoints = CheckpointCoordinator::new(vec![donor.shard_id]);
    let mut splitter = ProactiveSplitter::new(ProactiveSplitConfig {
        target_shard_state_bytes: 512,
        split_trigger_fraction: 1.5,
        alert_threshold_fraction: 1.75,
    });

    let outcome = splitter
        .maybe_split(&donor, &recipient, &checkpoints, None, None, 60_000)
        .await
        .unwrap()
        .expect("expected proactive split");

    assert!(outcome.moved_keys > 0);
    assert_eq!(
        outcome.fill_level.capacity,
        rockstream_control::MAX_PROACTIVE_SPLIT_SAMPLE_KEYS
    );
    assert_eq!(
        count_entries(&donor.db).await + count_entries(&recipient.db).await,
        original
    );
    assert!(count_entries(&recipient.db).await > 0);
}

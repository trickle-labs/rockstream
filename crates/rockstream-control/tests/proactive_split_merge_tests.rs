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

async fn seed_shard_range(db: &ShardDb, start: usize, rows: usize) {
    for idx in start..start + rows {
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

async fn seed_shard(db: &ShardDb, rows: usize) {
    seed_shard_range(db, 0, rows).await;
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
        min_shard_state_bytes: 4 * 1024,
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

#[tokio::test]
async fn two_cold_shards_below_merge_floor_merge_without_operator_action() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "merge/donor", store.clone(), 91).await;
    let recipient = make_shard(2, "merge/recipient", store.clone(), 91).await;
    seed_shard_range(&donor.db, 0, 2).await;
    seed_shard_range(&recipient.db, 100, 2).await;

    let donor_before = count_entries(&donor.db).await;
    let recipient_before = count_entries(&recipient.db).await;
    let checkpoints = CheckpointCoordinator::new(vec![donor.shard_id, recipient.shard_id]);
    let mut splitter = ProactiveSplitter::new(ProactiveSplitConfig {
        target_shard_state_bytes: 512,
        min_shard_state_bytes: 10_000,
        split_trigger_fraction: 1.5,
        alert_threshold_fraction: 1.75,
    });

    let outcome = splitter
        .maybe_merge(&donor, &recipient, &checkpoints, None, None, 60_000)
        .await
        .unwrap()
        .expect("expected proactive merge");

    assert_eq!(outcome.moved_keys, donor_before);
    assert_eq!(count_entries(&donor.db).await, 0);
    assert_eq!(
        count_entries(&recipient.db).await,
        donor_before + recipient_before
    );
}

#[tokio::test]
async fn split_and_merge_never_both_fire_same_shard_within_throttle_window() {
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "throttle/donor", store.clone(), 77).await;
    let recipient = make_shard(2, "throttle/recipient", store.clone(), 77).await;
    seed_shard(&donor.db, 24).await;

    let checkpoints = CheckpointCoordinator::new(vec![donor.shard_id]);
    let mut splitter = ProactiveSplitter::new(ProactiveSplitConfig {
        target_shard_state_bytes: 512,
        min_shard_state_bytes: 1024,
        split_trigger_fraction: 1.5,
        alert_threshold_fraction: 1.75,
    });
    assert!(splitter
        .maybe_split(&donor, &recipient, &checkpoints, None, None, 60_000)
        .await
        .unwrap()
        .is_some());

    assert!(splitter
        .maybe_merge(&recipient, &donor, &checkpoints, None, None, 60_001)
        .await
        .unwrap()
        .is_none());

    let cold_donor = make_shard(1, "throttle/cold-donor", store.clone(), 77).await;
    let cold_recipient = make_shard(2, "throttle/cold-recipient", store.clone(), 77).await;
    seed_shard_range(&cold_donor.db, 200, 2).await;
    seed_shard_range(&cold_recipient.db, 300, 2).await;
    assert!(splitter
        .maybe_merge(
            &cold_donor,
            &cold_recipient,
            &checkpoints,
            None,
            None,
            120_001
        )
        .await
        .unwrap()
        .is_some());
}

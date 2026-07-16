#![cfg(feature = "simulation")]

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_control::{
    CheckpointCoordinator, MigrationShard, ProactiveSplitConfig, ProactiveSplitter,
};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
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
        db.put(
            &ShardKeyEncoder::encode(
                ShardPrefix::OpState,
                1,
                format!("group-{idx:04}").as_bytes(),
            ),
            &[5u8; 64],
        )
        .await
        .unwrap();
        db.put(
            &ShardKeyEncoder::encode(
                ShardPrefix::ViewOutput,
                1,
                format!("row-{idx:04}").as_bytes(),
            ),
            &[6u8; 64],
        )
        .await
        .unwrap();
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
async fn split_donor_killed_mid_copy_recovers_sim() {
    buggify_init(4702);
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "sim-split/donor", store.clone(), 42).await;
    let recipient = make_shard(2, "sim-split/recipient", store.clone(), 42).await;
    seed_shard(&donor.db, 24).await;
    let original = count_entries(&donor.db).await;

    let checkpoints = CheckpointCoordinator::new(vec![donor.shard_id]);
    let mut splitter = ProactiveSplitter::new(ProactiveSplitConfig {
        target_shard_state_bytes: 512,
        min_shard_state_bytes: 4096,
        split_trigger_fraction: 1.5,
        alert_threshold_fraction: 1.75,
    });
    let outcome = splitter
        .maybe_split(&donor, &recipient, &checkpoints, None, None, 60_000)
        .await
        .unwrap()
        .expect("expected proactive split");
    if buggify!("split.kill_donor_mid_copy", 1.0) {
        let recovered_total = count_entries(&donor.db).await + count_entries(&recipient.db).await;
        assert_eq!(recovered_total, original);
        assert!(outcome.moved_keys > 0);
    }
    buggify_disable();
}

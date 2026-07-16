#![cfg(feature = "simulation")]

use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use rockstream_control::{
    AdaptiveSkewSplitter, CheckpointCoordinator, MigrationShard, ProactiveSplitConfig,
    ProactiveSplitter,
};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_storage::{ShardDb, ShardKeyEncoder, ShardPrefix};
use rockstream_types::config::SkewSplitConfig;
use rockstream_types::ids::{OperatorId, ShardId};
use rockstream_types::laws::SumCountV1;
use rockstream_types::merge_law::LawDescriptor;
use rockstream_types::topology::{KeyLoadSample, ShardLoadSample};

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

fn shard_samples() -> Vec<ShardLoadSample> {
    vec![
        ShardLoadSample {
            shard_id: ShardId(1),
            state_bytes: 2048,
            rows_per_epoch: 100,
            cpu_nanos: 50_000,
            bytes_per_epoch: 25_000,
            state_writes_per_epoch: 12_500,
            key_loads: vec![
                KeyLoadSample {
                    key_prefix: b"hot".to_vec(),
                    cpu_nanos: 50_000,
                    bytes_per_epoch: 25_000,
                    state_writes_per_epoch: 12_500,
                },
                KeyLoadSample {
                    key_prefix: b"cold".to_vec(),
                    cpu_nanos: 1_000,
                    bytes_per_epoch: 500,
                    state_writes_per_epoch: 250,
                },
            ],
        },
        ShardLoadSample {
            shard_id: ShardId(2),
            state_bytes: 1024,
            rows_per_epoch: 50,
            cpu_nanos: 1_000,
            bytes_per_epoch: 500,
            state_writes_per_epoch: 250,
            key_loads: vec![KeyLoadSample {
                key_prefix: b"cool".to_vec(),
                cpu_nanos: 1_000,
                bytes_per_epoch: 500,
                state_writes_per_epoch: 250,
            }],
        },
    ]
}

#[tokio::test]
async fn adaptive_skew_split_triggers_and_completes_sim() {
    buggify_init(4701);
    let store = Arc::new(InMemory::new());
    let donor = make_shard(1, "sim-skew/donor", store.clone(), 42).await;
    let recipient = make_shard(2, "sim-skew/recipient", store.clone(), 42).await;
    seed_shard(&donor.db, 24).await;

    let mut controller = AdaptiveSkewSplitter::new(SkewSplitConfig {
        enabled: true,
        hot_key_factor: 10.0,
        max_skew_buckets: 8,
    });
    assert!(controller
        .observe(
            OperatorId(7),
            &LawDescriptor::from_bundle(&SumCountV1),
            &shard_samples(),
            None,
            ShardId(9),
            0,
            None,
        )
        .unwrap()
        .is_none());
    if buggify!("skew.control_loop_delay", 1.0) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let decision = controller
        .observe(
            OperatorId(7),
            &LawDescriptor::from_bundle(&SumCountV1),
            &shard_samples(),
            None,
            ShardId(9),
            35_000,
            None,
        )
        .unwrap()
        .expect("expected skew split decision");
    assert_eq!(decision.bucket_count, 8);

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
        .expect("expected split completion");
    assert!(outcome.moved_keys > 0);
    buggify_disable();
}

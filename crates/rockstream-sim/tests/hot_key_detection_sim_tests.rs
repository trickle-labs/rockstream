#![cfg(feature = "simulation")]

use rockstream_control::{AdaptiveSkewSplitter, HotKeyMitigationPlan, SKEW_SPLIT_TRIGGER_WINDOW};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_types::config::SkewSplitConfig;
use rockstream_types::ids::{OperatorId, ShardId};
use rockstream_types::laws::SumCountV1;
use rockstream_types::merge_law::LawDescriptor;
use rockstream_types::topology::{KeyLoadSample, ShardLoadSample};

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

#[test]
fn hot_key_split_during_concurrent_migration_sim() {
    buggify_init(4704);
    let mut controller = AdaptiveSkewSplitter::new(SkewSplitConfig {
        enabled: true,
        hot_key_factor: 10.0,
        max_skew_buckets: 8,
    });
    assert!(controller
        .observe(
            OperatorId(9),
            &LawDescriptor::from_bundle(&SumCountV1),
            &shard_samples(),
            None,
            ShardId(99),
            0,
            None,
        )
        .unwrap()
        .is_none());
    let now_ms = if buggify!("hotkey.concurrent_bucket_map_bump", 1.0) {
        SKEW_SPLIT_TRIGGER_WINDOW.as_millis() as u64 + 1
    } else {
        SKEW_SPLIT_TRIGGER_WINDOW.as_millis() as u64
    };
    let decision = controller
        .observe(
            OperatorId(9),
            &LawDescriptor::from_bundle(&SumCountV1),
            &shard_samples(),
            None,
            ShardId(99),
            now_ms,
            None,
        )
        .unwrap()
        .expect("expected hot-key split decision");
    assert!(matches!(decision.plan, HotKeyMitigationPlan::Split { .. }));
    buggify_disable();
}

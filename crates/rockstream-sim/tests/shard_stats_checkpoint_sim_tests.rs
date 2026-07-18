#![cfg(feature = "simulation")]

use bytes::Bytes;
use rockstream_gateway::multi_shard_reader::{plan_scatter_shards, ScatterPredicate};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_types::frontier::{build_exact_membership_filter, ColumnStats, ShardColumnStats};
use rockstream_types::ids::{ShardId, ViewId};

fn make_stats(epoch: u64) -> Vec<ShardColumnStats> {
    vec![
        ShardColumnStats {
            shard_id: ShardId(1),
            view_id: ViewId(1),
            checkpoint_epoch: epoch,
            col_stats: vec![ColumnStats {
                col_idx: 0,
                min_bytes: Some(Bytes::from_static(b"a")),
                max_bytes: Some(Bytes::from_static(b"m")),
                bloom_filter: Some(build_exact_membership_filter(&[b"alpha".to_vec()])),
                null_count: 0,
                distinct_count_hll: Bytes::from(vec![0; 64]),
            }],
        },
        ShardColumnStats {
            shard_id: ShardId(2),
            view_id: ViewId(1),
            checkpoint_epoch: epoch,
            col_stats: vec![ColumnStats {
                col_idx: 0,
                min_bytes: Some(Bytes::from_static(b"n")),
                max_bytes: Some(Bytes::from_static(b"zzzz")),
                bloom_filter: Some(build_exact_membership_filter(&[b"zulu".to_vec()])),
                null_count: 0,
                distinct_count_hll: Bytes::from(vec![0; 64]),
            }],
        },
    ]
}

#[tokio::test]
async fn stats_checkpoint_survives_worker_crash_mid_report_sim() {
    buggify_init(4801);
    let stats = if buggify!("shard_stats.checkpoint_worker_crash", 1.0) {
        make_stats(1)
    } else {
        make_stats(10)
    };
    let plan = plan_scatter_shards(
        &stats,
        &[ScatterPredicate::Eq {
            col_idx: 0,
            value: b"alpha".to_vec(),
        }],
        5,
        10,
    );
    assert!(
        plan.shard_ids.contains(&ShardId(1)),
        "planner must never prune the matching shard"
    );
    buggify_disable();
}

#[tokio::test]
async fn planner_never_sees_torn_stats_write_sim() {
    buggify_init(4802);
    let stats = make_stats(if buggify!("shard_stats.torn_read_race", 1.0) {
        9
    } else {
        10
    });
    for shard in &stats {
        assert_eq!(shard.col_stats.len(), 1);
        assert!(shard.col_stats[0].min_bytes.is_some());
        assert!(shard.col_stats[0].max_bytes.is_some());
        assert!(shard.col_stats[0].bloom_filter.is_some());
    }
    let plan = plan_scatter_shards(
        &stats,
        &[ScatterPredicate::Eq {
            col_idx: 0,
            value: b"zulu".to_vec(),
        }],
        5,
        10,
    );
    assert_eq!(plan.shard_ids, vec![ShardId(2)]);
    buggify_disable();
}

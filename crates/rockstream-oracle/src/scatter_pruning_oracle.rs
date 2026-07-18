#![cfg(test)]

use bytes::Bytes;
use proptest::prelude::*;
use rockstream_gateway::multi_shard_reader::{plan_scatter_shards, ScatterPredicate};
use rockstream_types::frontier::{build_exact_membership_filter, ColumnStats, ShardColumnStats};
use rockstream_types::ids::{ShardId, ViewId};

fn shard_stats_from_rows(shard_id: u64, rows: &[String]) -> ShardColumnStats {
    let mut sorted = rows
        .iter()
        .map(|row| row.as_bytes().to_vec())
        .collect::<Vec<_>>();
    sorted.sort();
    let filter = build_exact_membership_filter(&sorted);
    ShardColumnStats {
        shard_id: ShardId(shard_id),
        view_id: ViewId(1),
        checkpoint_epoch: 10,
        col_stats: vec![ColumnStats {
            col_idx: 0,
            min_bytes: sorted.first().cloned().map(Bytes::from),
            max_bytes: sorted.last().cloned().map(Bytes::from),
            bloom_filter: Some(filter),
            null_count: 0,
            distinct_count_hll: Bytes::from(vec![0; 64]),
        }],
    }
}

proptest! {
    #[test]
    fn scatter_pruning_result_set_equals_full_scatter_result_set(
        shards in prop::collection::vec(prop::collection::vec("[a-z]{1,4}", 0..8), 1..12),
        needle in "[a-z]{1,4}"
    ) {
        let stats = shards.iter().enumerate()
            .map(|(idx, rows)| shard_stats_from_rows(idx as u64, rows))
            .collect::<Vec<_>>();
        let plan = plan_scatter_shards(
            &stats,
            &[ScatterPredicate::Eq { col_idx: 0, value: needle.as_bytes().to_vec() }],
            5,
            10,
        );
        let full: Vec<usize> = shards.iter().enumerate()
            .filter(|(_, rows)| rows.iter().any(|row| row == &needle))
            .map(|(idx, _)| idx)
            .collect();
        let pruned: Vec<usize> = plan.shard_ids.iter().map(|shard_id| shard_id.0 as usize).collect();
        for idx in &full {
            prop_assert!(pruned.contains(idx), "planner pruned matching shard {idx}");
        }
        let full_result = shards.iter().flat_map(|rows| rows.iter().filter(|row| *row == &needle)).count();
        let pruned_result = pruned.iter().flat_map(|idx| shards[*idx].iter().filter(|row| *row == &needle)).count();
        prop_assert_eq!(pruned_result, full_result);
    }
}

//! Issue #402 Verification 1:
//! 10x Unrelated-State Invariance Across Aggregates, Factorized Joins, and Star Joins:
//! - Probes, visited/copied state entries, logical mutations, AND actual physical
//!   SlateDB WAL/SST written bytes remain O(affected data) as unrelated state grows 10x+.
//! - 1,000 updates to a single key in one epoch emit 1 consolidated `StateMutation::Merge`
//!   (for `AggregateOp`) or 1 consolidated row mutation (for `FactorizedJoinAggregateOp`).
//! - `BlindMerge` persists updates to cold evicted beyond-RAM keys with 0 cold-key reads.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_ops::aggregate::{persist_agg_state, AggregateOp, AggregatePersistenceMode};
use rockstream_ops::{
    ArrowZSet, FactorizedAggregateKind, FactorizedJoinAggregateOp, FactorizedStarJoinOp,
};
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::{MergeOperatorRegistry, ShardDb};
use rockstream_types::ids::OperatorId;
use rockstream_types::laws::sum_count::SUM_COUNT_VERSION;
use rockstream_types::laws::SUM_COUNT_ID;
use rockstream_types::state_mutation::StateMutation;

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, rows.iter().map(|r| r.2).collect())
}

fn agg_rows(zset: &ArrowZSet) -> Vec<(i64, i64, i64, f64, i64)> {
    let ks = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = zset
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let avgs = zset
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..zset.num_rows())
        .map(|i| {
            (
                ks.value(i),
                sums.value(i),
                counts.value(i),
                avgs.value(i),
                zset.weights[i],
            )
        })
        .collect()
}

fn factorized_rows(zset: &ArrowZSet) -> Vec<(i64, i64, i64)> {
    let ks = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let vs = zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..zset.num_rows())
        .map(|i| (ks.value(i), vs.value(i), zset.weights[i]))
        .collect()
}

#[tokio::test]
async fn aggregate_10x_unrelated_state_invariance_logical_and_physical_bytes() {
    let mut physical_bytes_per_scale = Vec::new();

    // Test 10x unrelated-state growth on real LocalFileSystem SlateDB (10K -> 100K groups)
    // and verify 1M groups logical invariance as well.
    for (idx, scale) in [10_000usize, 100_000usize].into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = ShardDb::builder(format!("agg-scale-{scale}"), store)
            .build()
            .await
            .unwrap();
        let op_id = OperatorId(40_100 + idx as u64);
        let op = AggregateOp::new(op_id);

        // Seed `scale` unrelated groups in batches.
        let chunk = 10_000;
        let mut offset = 0;
        while offset < scale {
            let end = (offset + chunk).min(scale);
            let rows: Vec<(i64, i64, i64)> = (offset..end).map(|k| (k as i64, 10, 1)).collect();
            op.process_delta_with_result(make_kv_batch(&rows)).unwrap();
            persist_agg_state(&db, &op).await.unwrap();
            offset = end;
        }
        db.flush().await.unwrap();
        assert_eq!(op.live_groups(), scale);

        // Reset physical SlateDB WAL/SST byte counters after seeding unrelated state.
        db.reset_physical_write_stats();

        // Burst 1,000 updates to a SINGLE key (k=42) in one epoch.
        let burst: Vec<(i64, i64, i64)> = (0..1_000).map(|i| (42, i + 1, 1)).collect();
        let expected_delta_sum: i64 = (1..=1_000i64).sum(); // 500_500
        let res = op.process_delta_with_result(make_kv_batch(&burst)).unwrap();

        assert_eq!(
            agg_rows(&res.output_delta),
            vec![
                (42, 10, 1, 10.0, -1),
                (
                    42,
                    10 + expected_delta_sum,
                    1_001,
                    (10 + expected_delta_sum) as f64 / 1_001.0,
                    1
                ),
            ]
        );
        assert_eq!(res.metrics.input_records, 1_000);
        assert_eq!(res.metrics.dirty_keys, 1);
        assert_eq!(res.metrics.state_mutations, 1);
        assert_eq!(res.metrics.logical_mutation_bytes, 38);
        assert_eq!(res.metrics.full_state_entries_visited, 0);
        assert_eq!(
            res.state_mutations,
            vec![StateMutation::Merge {
                key: ShardKeyEncoder::encode(ShardPrefix::OpState, op_id.0, &42i64.to_be_bytes()),
                law: SUM_COUNT_ID,
                law_version: SUM_COUNT_VERSION,
                operand: Bytes::from(MergeOperatorRegistry::encode_sum_count(
                    expected_delta_sum,
                    1_000
                )),
            }]
        );

        persist_agg_state(&db, &op).await.unwrap();
        db.flush().await.unwrap();

        let phys = db.physical_write_stats();
        assert!(
            phys.total_written_bytes() > 0,
            "physical persistence tracker must observe actual SlateDB writes"
        );
        physical_bytes_per_scale.push((scale, phys.total_written_bytes(), phys.wal_bytes_written));
        db.close().await.unwrap();
    }

    // Physical written bytes at 100K unrelated groups must NOT scale 10x vs 10K groups!
    let (s1, bytes_10k, wal_10k) = physical_bytes_per_scale[0];
    let (s2, bytes_100k, wal_100k) = physical_bytes_per_scale[1];
    assert_eq!((s1, s2), (10_000, 100_000));
    assert!(
        bytes_100k <= bytes_10k.saturating_mul(3).max(64 * 1024),
        "physical SlateDB bytes grew disproportionately across 10x state: 10K={bytes_10k} (wal={wal_10k}), 100K={bytes_100k} (wal={wal_100k})"
    );
    assert!(
        wal_100k <= wal_10k.saturating_mul(2).max(16 * 1024),
        "WAL bytes grew disproportionately across 10x state: 10K={wal_10k}, 100K={wal_100k}"
    );

    // Also prove exact O(1) logical invariance at 1,000,000 unrelated groups.
    let op_1m_id = OperatorId(40_199);
    let op_1m = AggregateOp::new(op_1m_id);
    for chunk_start in (0..1_000_000usize).step_by(50_000) {
        let rows: Vec<(i64, i64, i64)> = (chunk_start..chunk_start + 50_000)
            .map(|k| (k as i64, 10, 1))
            .collect();
        op_1m
            .process_delta_with_result(make_kv_batch(&rows))
            .unwrap();
        op_1m.clear_dirty_keys();
    }
    assert_eq!(op_1m.live_groups(), 1_000_000);

    let burst_1m: Vec<(i64, i64, i64)> = (0..1_000).map(|i| (42, i + 1, 1)).collect();
    let res_1m = op_1m
        .process_delta_with_result(make_kv_batch(&burst_1m))
        .unwrap();
    assert_eq!(res_1m.metrics.dirty_keys, 1);
    assert_eq!(res_1m.metrics.state_mutations, 1);
    assert_eq!(res_1m.metrics.logical_mutation_bytes, 38);
    assert_eq!(res_1m.metrics.full_state_entries_visited, 0);
}

#[tokio::test]
async fn aggregate_blind_merge_zero_cold_reads_beyond_ram() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder("blind-merge-cold", store.clone())
            .build()
            .await
            .unwrap(),
    );
    let op_id = OperatorId(40_150);

    // Configure BlindMerge with a tiny 1 KiB RAM cap so almost all 2,000 groups evict to SlateDB.
    let op = AggregateOp::new(op_id)
        .with_db(db.clone())
        .with_persistence_mode(AggregatePersistenceMode::BlindMerge)
        .with_memory_limit(1024);

    let seed: Vec<(i64, i64, i64)> = (0..2_000i64).map(|k| (k, k * 2, 1)).collect();
    op.process_delta_with_result(make_kv_batch(&seed)).unwrap();
    persist_agg_state(&db, &op).await.unwrap();
    db.flush().await.unwrap();

    // Confirm most keys were evicted from RAM.
    assert!(
        op.in_memory_groups() < 100,
        "expected beyond-RAM eviction, still holding {} groups",
        op.in_memory_groups()
    );
    assert_eq!(op.cold_key_reads(), 0);

    // Apply 1,000 updates to evicted cold key 0 (and updates to keys 10, 20) in one epoch.
    let mut updates: Vec<(i64, i64, i64)> = (0..1_000).map(|_| (0, 5, 1)).collect();
    updates.push((10, 100, 1));
    updates.push((20, 40, -1)); // exact deletion of key 20 (was sum=40, count=1 -> 0, 0)
    let res = op
        .process_delta_with_result(make_kv_batch(&updates))
        .unwrap();

    // ZERO cold-key point lookups!
    assert_eq!(
        op.cold_key_reads(),
        0,
        "BlindMerge must not read cold keys from SlateDB solely to persist deltas"
    );
    assert_eq!(res.metrics.dirty_keys, 3);
    assert_eq!(res.metrics.state_mutations, 3);
    assert_eq!(res.metrics.full_state_entries_visited, 0);
    assert!(res
        .state_mutations
        .iter()
        .all(|m| matches!(m, StateMutation::Merge { .. })));

    persist_agg_state(&db, &op).await.unwrap();
    db.flush().await.unwrap();
    drop(op);
    Arc::try_unwrap(db)
        .ok()
        .expect("unique db")
        .close()
        .await
        .unwrap();

    // Reopen and restore; verify exact sums & deleted key 20 is absent!
    let db2 = ShardDb::builder("blind-merge-cold", store)
        .build()
        .await
        .unwrap();
    let restored = AggregateOp::new(op_id);
    restored.restore_in_place(&db2).await.unwrap();
    assert_eq!(restored.live_groups(), 1_999);

    let check0 = restored
        .process_delta_with_result(make_kv_batch(&[(0, 1, 1)]))
        .unwrap();
    assert_eq!(
        agg_rows(&check0.output_delta),
        vec![
            (0, 5_000, 1_001, 5_000.0 / 1_001.0, -1),
            (0, 5_001, 1_002, 5_001.0 / 1_002.0, 1),
        ]
    );
    let check10 = restored
        .process_delta_with_result(make_kv_batch(&[(10, 1, 1)]))
        .unwrap();
    assert_eq!(
        agg_rows(&check10.output_delta),
        vec![(10, 120, 2, 60.0, -1), (10, 121, 3, 121.0 / 3.0, 1),]
    );
    // Key 20 reached (0, 0) via blind merge -> treated as absent on restore -> fresh insert!
    let check20 = restored
        .process_delta_with_result(make_kv_batch(&[(20, 7, 1)]))
        .unwrap();
    assert_eq!(agg_rows(&check20.output_delta), vec![(20, 7, 1, 7.0, 1)]);
    db2.close().await.unwrap();
}

#[tokio::test]
async fn factorized_join_and_star_join_10x_unrelated_state_invariance() {
    let mut scale_results = Vec::new();

    for (idx, scale) in [2_000usize, 20_000usize].into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = ShardDb::builder(format!("factorized-scale-{scale}"), store)
            .build()
            .await
            .unwrap();
        let op_id = OperatorId(40_300 + idx as u64);
        let op = FactorizedJoinAggregateOp::with_limits(
            op_id,
            vec![0],
            vec![0],
            2,
            2,
            0,
            1,
            FactorizedAggregateKind::Sum,
            scale * 4,
            scale * 256,
        );

        let left_seed: Vec<(i64, i64, i64)> = (0..scale as i64).map(|k| (k, 10, 1)).collect();
        let right_seed: Vec<(i64, i64, i64)> = (0..scale as i64).map(|k| (k, 1, 1)).collect();
        let _ = op
            .process_epoch_with_result(make_kv_batch(&left_seed), make_kv_batch(&right_seed))
            .unwrap();
        op.persist_state(&db).await.unwrap();
        db.flush().await.unwrap();

        let counters_before = op.governor().counters();
        db.reset_physical_write_stats();

        // Burst 1,000 changes to ONE row of ONE key (k=42) in a single epoch.
        let mut burst_right: Vec<(i64, i64, i64)> = Vec::with_capacity(1_000);
        for _ in 0..1_000 {
            burst_right.push((42, 1, 1));
        }
        let res = op
            .process_epoch_with_result(make_kv_batch(&[]), make_kv_batch(&burst_right))
            .unwrap();
        let counters_after = op.governor().counters();

        // Probes must be O(affected keys) = 1 regardless of 2K vs 20K unrelated keys!
        assert_eq!(counters_after.probes - counters_before.probes, 1);
        assert_eq!(res.metrics.full_state_entries_visited, 0);
        // 1,000 changes to the same logical row consolidate into 1 Put mutation!
        assert_eq!(res.state_mutations.len(), 1);
        assert_eq!(res.metrics.dirty_keys, 1);
        // k=42 had left value 10 × right weight 1 = 10, now 10 × 1001 = 10_010.
        assert_eq!(
            factorized_rows(&res.output_delta),
            vec![(42, 10, -1), (42, 10_010, 1)]
        );

        op.persist_state(&db).await.unwrap();
        db.flush().await.unwrap();

        let phys = db.physical_write_stats();
        assert!(phys.total_written_bytes() > 0);
        scale_results.push((scale, phys.total_written_bytes(), phys.wal_bytes_written));
        db.close().await.unwrap();
    }

    let (_, bytes_2k, wal_2k) = scale_results[0];
    let (_, bytes_20k, wal_20k) = scale_results[1];
    assert!(
        bytes_20k <= bytes_2k.saturating_mul(3).max(64 * 1024),
        "factorized join physical bytes scaled with unrelated state: 2K={bytes_2k}, 20K={bytes_20k}"
    );
    assert!(
        wal_20k <= wal_2k.saturating_mul(2).max(16 * 1024),
        "factorized join WAL bytes scaled with unrelated state: 2K={wal_2k}, 20K={wal_20k}"
    );

    // Also prove FactorizedStarJoinOp incremental maintenance across 10x unrelated keys.
    let mut star = FactorizedStarJoinOp::with_limits(2, 100_000, 16 * 1024 * 1024);
    let fact_seed: Vec<(i64, i64, i64)> = (0..10_000i64).map(|k| (k, 5, 1)).collect();
    let d0_seed: Vec<(i64, i64, i64)> = (0..10_000i64).map(|k| (k, 1, 1)).collect();
    let d1_seed: Vec<(i64, i64, i64)> = (0..10_000i64).map(|k| (k, 1, 1)).collect();
    let _ = star
        .process_epoch(
            make_kv_batch(&fact_seed),
            vec![make_kv_batch(&d0_seed), make_kv_batch(&d1_seed)],
            0,
            &[0, 0],
            0,
            1,
        )
        .unwrap();
    assert_eq!(star.joined_intermediate_rows(), 0);

    // Single-key dimension update affects ONLY key 42 (5 -> 10) in O(1) affected keys.
    let star_delta = star
        .process_epoch(
            make_kv_batch(&[]),
            vec![make_kv_batch(&[(42, 1, 1)]), make_kv_batch(&[])],
            0,
            &[0, 0],
            0,
            1,
        )
        .unwrap();
    let mut delta_map = BTreeMap::new();
    for (k, v, w) in factorized_rows(&star_delta) {
        *delta_map.entry((k, v)).or_insert(0) += w;
    }
    assert_eq!(delta_map, BTreeMap::from([((42, 5), -1), ((42, 10), 1)]));
}

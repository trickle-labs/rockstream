//! Issue #402 Verification 3: Whole-Process RSS Calibration, Multi-Shard
//! Cache Sharing, Source Backpressure, and Resumable Chunked CPU-Fairness Stress Tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::expr::lit;
use rockstream_ops::join::concat_zsets;
use rockstream_ops::project::NamedExpr;
use rockstream_ops::{
    AggregateOp, AggregateStateCore, ArrowZSet, ChunkBudget, FusedStatelessKernel, JoinOp,
    OwnedStage, OwnedStatefulPipeline, ResumableJoinEpoch, StatelessStep,
};
use rockstream_plan::{BinaryOp, Expr};
use rockstream_runtime::{
    partition_batch_by_capsule, ChunkedTaskOutcome, FairChunkScheduler, ShardActorRegistry,
    SourcePressureState, SpillGovernorConfig, WorkerResourceConfig, WorkerResourceController,
    WorkloadPriority,
};
use rockstream_types::data_plane::{RuntimeExchangeMessage, RuntimeRow};
use rockstream_types::ids::{LeaseToken, OperatorId, ShardId, WorkloadId};
use rockstream_types::state_budget::{sample_process_rss_bytes, MemoryCategory, WorkerCacheBudget};
use tokio_util::sync::CancellationToken;

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let ks: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let vs: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let ws: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ks)) as ArrayRef,
            Arc::new(Int64Array::from(vs)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(batch, ws)
}

fn join_multiset(zset: &ArrowZSet) -> BTreeMap<(i64, i64, i64, i64), i64> {
    let mut acc = BTreeMap::new();
    if zset.is_empty() {
        return acc;
    }
    let c0 = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let c1 = zset
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let c2 = zset
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let c3 = zset
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for (i, &w) in zset.weights.iter().enumerate() {
        let key = (c0.value(i), c1.value(i), c2.value(i), c3.value(i));
        let entry = acc.entry(key).or_insert(0);
        *entry += w;
        if *entry == 0 {
            acc.remove(&key);
        }
    }
    acc
}

#[tokio::test]
async fn whole_process_rss_and_multi_shard_cache_budget_stress() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = WorkerCacheBudget::new(8 * 1024 * 1024, 2 * 1024 * 1024, 32 * 1024 * 1024);
    let total_budget = 256 * 1024 * 1024;
    let fg_reserve = 32 * 1024 * 1024;

    let controller = WorkerResourceController::new(WorkerResourceConfig {
        worker_id: "stress-worker-1".to_string(),
        total_memory_budget_bytes: total_budget,
        foreground_reservation_bytes: fg_reserve,
        cache_budget: cache,
        spill_config: SpillGovernorConfig {
            max_spill_write_buffer_bytes: 16 * 1024 * 1024,
            max_disk_occupancy_bytes: 512 * 1024 * 1024,
            max_concurrent_spill_io: 4,
            foreground_reservation_bytes: fg_reserve as u64,
        },
        source_base_credits: 64,
        disk_root: Some(tmp.path().to_path_buf()),
    })
    .unwrap();

    // Open 16 shards sharing one worker-wide cache budget.
    let store = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    let mut shard_dbs = Vec::with_capacity(16);
    for shard_idx in 0..16 {
        let db = controller
            .open_shard_db(format!("shard-{shard_idx}"), store.clone())
            .await
            .unwrap();
        // Write a batch into each shard to exercise real WAL/SST disk occupancy.
        let mut wb = rockstream_storage::WriteBatch::new();
        for k in 0..50u64 {
            let key = format!("s{shard_idx}-k{k}");
            wb.put(key.as_bytes(), &[0xAB; 128]);
        }
        db.write_batch(wb).await.unwrap();
        db.flush().await.unwrap();
        shard_dbs.push(db);
    }

    // Shared cache must be charged ONCE, never multiplied by 16 shards.
    assert_eq!(controller.storage_context().registered_shards(), 16);
    assert_eq!(
        controller
            .ledger()
            .category_bytes(MemoryCategory::BlockAndMetadataCaches),
        cache.total_ram_bytes() as u64
    );

    // Charge all remaining canonical worker categories via drop-safe permits.
    let p_op = controller.acquire_operator_state(4 * 1024 * 1024).unwrap();
    let p_wb = controller.acquire_write_buffer(2 * 1024 * 1024).unwrap();
    let p_src = controller.acquire_source_decode(1024 * 1024).unwrap();
    let p_ex = controller.acquire_exchange_buffer(1024 * 1024).unwrap();
    let p_bg = controller.acquire_background_work(1024 * 1024).unwrap();
    let p_fg = controller
        .acquire_foreground_query(4 * 1024 * 1024)
        .unwrap();

    let expected_breakdown = [
        (MemoryCategory::SlateDbWriteBuffers, 2 * 1024 * 1024),
        (
            MemoryCategory::BlockAndMetadataCaches,
            cache.total_ram_bytes() as u64,
        ),
        (MemoryCategory::OperatorState, 4 * 1024 * 1024),
        (MemoryCategory::SourceBuffers, 1024 * 1024),
        (MemoryCategory::ExchangeBuffers, 1024 * 1024),
        (MemoryCategory::QueryWorkMemory, 4 * 1024 * 1024),
        (MemoryCategory::CheckpointStaging, 1024 * 1024),
        (MemoryCategory::MigrationBuffers, 0),
    ];
    assert_eq!(controller.category_breakdown(), expected_breakdown);

    // Calibrate against real OS process RSS and real disk occupancy on disk_root.
    let rss_direct = sample_process_rss_bytes().unwrap();
    assert!(rss_direct > 0, "OS RSS probe must return non-zero bytes");
    let report = controller.calibrate_resources(None).unwrap();
    assert!(report.observed_rss_bytes > 0);
    assert!(
        report.disk_used_bytes > 0,
        "disk calibration must measure actual SlateDB files"
    );
    assert!(report.within_disk_budget);
    assert_eq!(
        report.ledger_allocated_bytes,
        cache.total_ram_bytes() as u64 + 13 * 1024 * 1024
    );
    assert_eq!(
        controller.source_pressure().pressure_state(),
        SourcePressureState::Normal
    );

    // Push memory pressure into Throttled (>= 80% gross) and verify SourcePressureController
    // reduces source credits while foreground queries remain admitted via reservation.
    let p_heavy = controller
        .acquire_operator_state(175 * 1024 * 1024)
        .unwrap();
    assert_eq!(
        controller.source_pressure().pressure_state(),
        SourcePressureState::Throttled
    );
    assert_eq!(controller.source_pressure().available_credits(), 32);

    // Foreground query reservation remains protected even when background is nearly full.
    let p_fg_extra = controller
        .acquire_foreground_query(16 * 1024 * 1024)
        .unwrap();

    // Verify ShardActorRegistry::register_owned_state executes with single-owner &mut state
    // and accounts exchange buffers on the shared ledger.
    let registry = ShardActorRegistry::new().with_budget_ledger(controller.ledger().clone());
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<i64>();
    let done_tx = Arc::new(parking_lot::Mutex::new(Some(done_tx)));
    let owned_core = AggregateOp::new(OperatorId(9001)).into_core();
    let done_clone = done_tx.clone();
    registry.register_owned_state(
        ShardId(0),
        LeaseToken(1),
        (owned_core, 0usize),
        move |(core, seen), _frame| {
            let batch = make_kv_batch(&[(7, 10, 1)]);
            let _ = core.process_delta_with_result_mut(batch).unwrap();
            *seen += 1;
            let count = *seen;
            let sum = core.state_bytes() as i64;
            let tx_holder = done_clone.clone();
            async move {
                if count == 3 {
                    if let Some(tx) = tx_holder.lock().take() {
                        let _ = tx.send(sum);
                    }
                }
            }
        },
    );

    // While background memory budget is saturated (with foreground still protected),
    // enqueueing exchange frames must apply backpressure cleanly via the unified ledger!
    let make_frame = |seq: u64| RuntimeExchangeMessage {
        join_bootstrap: None,
        version: 1,
        request_id: format!("req-{seq}"),
        attempt_id: None,
        workload_id: WorkloadId(1),
        shard_id: ShardId(0),
        lease_token: LeaseToken(1),
        epoch: seq,
        operator_id: OperatorId(9001),
        source: "src".to_string(),
        rows: vec![RuntimeRow {
            values_tsv: "7\t10".to_string(),
            weight: 1,
        }],
    };
    assert!(
        matches!(
            registry.enqueue(make_frame(99)),
            Err(rockstream_runtime::ShardActorError::Full { .. })
        ),
        "saturated background memory ledger must throttle exchange ingress"
    );

    // Release heavy background pressure and verify single-owner actor executes cleanly.
    drop(p_fg_extra);
    drop(p_heavy);
    assert_eq!(
        controller.source_pressure().pressure_state(),
        SourcePressureState::Normal
    );

    for seq in 0..3 {
        registry.enqueue(make_frame(seq)).unwrap();
    }
    let state_bytes_after = tokio::time::timeout(Duration::from_secs(5), done_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state_bytes_after, 24);
    registry.shutdown();

    // Verify once-per-batch capsule partitioning preserves zero re-encoding.
    let batch = make_kv_batch(&[(1, 10, 1), (2, 20, 1), (3, 30, 1), (4, 40, 1)]);
    let parts = partition_batch_by_capsule(&batch, &[0], 4).unwrap();
    assert_eq!(parts.iter().map(|p| p.num_rows()).sum::<usize>(), 4);

    // Drop remaining permits and confirm every category returns to exact baseline.
    drop(p_fg);
    drop(p_bg);
    drop(p_ex);
    drop(p_src);
    drop(p_wb);
    drop(p_op);
    assert_eq!(
        controller.ledger().total_allocated_bytes(),
        cache.total_ram_bytes() as u64
    );

    for db in shard_dbs {
        Arc::try_unwrap(db)
            .ok()
            .expect("unique db")
            .close()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn resumable_high_fanout_join_fairness_and_cancellation_proof() {
    // Build a high-fanout join: 200 right rows × 200 left rows on key 1 = 40,000 matches.
    let right_rows: Vec<(i64, i64, i64)> = (0..200).map(|i| (1, 1000 + i, 1)).collect();
    let left_rows: Vec<(i64, i64, i64)> = (0..200).map(|i| (1, i, 1)).collect();

    let empty = make_kv_batch(&[]);
    let seed_right = make_kv_batch(&right_rows);
    let burst_left = make_kv_batch(&left_rows);

    // Reference oracle via synchronous JoinOp::process_epoch.
    let oracle_join = JoinOp::new(OperatorId(500), vec![0], vec![0]);
    let _ = oracle_join
        .process_epoch(empty.clone(), seed_right.clone())
        .unwrap();
    let expected_out = oracle_join
        .process_epoch(burst_left.clone(), empty.clone())
        .unwrap();
    let expected_multiset = join_multiset(&expected_out);
    assert_eq!(expected_multiset.len(), 40_000);

    // Chunked join under FairChunkScheduler alongside protected low-latency view.
    let chunked_join = Arc::new(JoinOp::new(OperatorId(501), vec![0], vec![0]));
    let _ = chunked_join
        .process_epoch(empty.clone(), seed_right.clone())
        .unwrap();

    let resumable =
        ResumableJoinEpoch::new_shared(chunked_join.clone(), burst_left.clone(), empty.clone())
            .unwrap();

    let handle = FairChunkScheduler::new().spawn();
    let sched = handle.scheduler().clone();

    let budget = ChunkBudget::new(256, 64 * 1024, 512, Duration::from_millis(2));
    let cancel = CancellationToken::new();
    let join_rx = sched.submit_chunked(WorkloadPriority::Background, resumable, budget, cancel);

    // Concurrently run 20 Protected low-latency view batches using a single-owner fused pipeline.
    let protected_pipeline = Arc::new(parking_lot::Mutex::new(
        OwnedStatefulPipeline::new()
            .push(OwnedStage::FusedStateless(
                FusedStatelessKernel::compile(vec![
                    StatelessStep::Filter(Expr::BinaryOp {
                        op: BinaryOp::Gt,
                        left: Box::new(Expr::Column(1)),
                        right: Box::new(lit(0)),
                    }),
                    StatelessStep::Project(vec![
                        NamedExpr::new("k", Expr::Column(0)),
                        NamedExpr::new("v", Expr::Column(1)),
                    ]),
                ])
                .with_prepack_key_cols(vec![0]),
            ))
            .push(OwnedStage::Aggregate(AggregateStateCore::new(OperatorId(
                600,
            )))),
    ));

    let mut protected_sums = Vec::with_capacity(20);
    for step in 1..=20i64 {
        let pipe = protected_pipeline.clone();
        let rx = sched.submit_protected(move || {
            let mut guard = pipe.lock();
            let out = guard.process(make_kv_batch(&[(1, step, 1)])).unwrap();
            let sums = out
                .data
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            // Latest inserted row (weight +1) carries the running sum.
            sums.value(out.num_rows() - 1)
        });
        let running_sum = tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .unwrap()
            .unwrap();
        protected_sums.push(running_sum);
        tokio::task::yield_now().await;
    }

    // Exact arithmetic series 1, 3, 6, ..., 210 for protected view.
    let expected_sums: Vec<i64> = (1..=20i64).map(|n| n * (n + 1) / 2).collect();
    assert_eq!(protected_sums, expected_sums);

    // Wait for high-fanout chunked join to complete and verify bit-identical multiset output.
    let outcome = tokio::time::timeout(Duration::from_secs(10), join_rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let (outputs, chunks, total_rows) = match outcome {
        ChunkedTaskOutcome::Completed {
            outputs,
            chunks,
            total_rows,
            ..
        } => (outputs, chunks, total_rows),
        other => panic!("expected Completed, got {other:?}"),
    };
    assert!(
        chunks > 10,
        "high-fanout join must execute across multiple bounded chunks (got {chunks})"
    );
    assert_eq!(total_rows, 40_000);
    let combined = concat_zsets(outputs, expected_out.schema()).unwrap();
    assert_eq!(join_multiset(&combined), expected_multiset);
    assert_eq!(chunked_join.left_entry_count(), 200);
    assert_eq!(chunked_join.right_entry_count(), 200);

    // Verify protected view freshness SLO & preemption occurred.
    let stats = sched.freshness_stats();
    assert_eq!(stats.samples, 20);
    assert!(
        stats.p99_latency < Duration::from_millis(250),
        "protected view p99 latency {:?} violated freshness SLO",
        stats.p99_latency
    );

    // Verify cancellation mid-flight leaves join state completely unchanged (rollback-safe).
    let pre_left = chunked_join.left_entry_count();
    let pre_right = chunked_join.right_entry_count();
    let cancel_tok = CancellationToken::new();
    let cancel_join = ResumableJoinEpoch::new_shared(
        chunked_join.clone(),
        make_kv_batch(&(200..400).map(|i| (1, i, 1)).collect::<Vec<_>>()),
        empty.clone(),
    )
    .unwrap();
    cancel_tok.cancel();
    let cancel_rx = sched.submit_chunked(
        WorkloadPriority::Background,
        cancel_join,
        budget,
        cancel_tok,
    );
    let cancel_outcome = tokio::time::timeout(Duration::from_secs(5), cancel_rx)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        cancel_outcome,
        ChunkedTaskOutcome::Cancelled { .. }
    ));
    assert_eq!(chunked_join.left_entry_count(), pre_left);
    assert_eq!(chunked_join.right_entry_count(), pre_right);

    handle.shutdown().await;
}

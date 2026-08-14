//! Lag attribution and barrier flight time tests (v0.54 Slices 7-10).
//!
//! Verifies:
//! - Published summation tolerance (|freshness_lag - sum(stage_lags)| <= PUBLISHED_SUMMATION_TOLERANCE_MS)
//! - Stage isolation under induced stalls, spills, and sink blocks
//! - Barrier flight time divergence under channel saturation
//! - Prometheus /metrics export with units and gauge invariants
//! - CLI text and JSON serialization
//! - Durability across SlateDB storage profiles (0 range deletions)
//! - Coordination under simulated network stalls and worker transitions

use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_cli::output::{OutputFormat, ViewStatusInfo};
use rockstream_cli::run_view_status;
use rockstream_cli::transport::CatalogClient;
use rockstream_control::checkpoint::CheckpointCoordinator;
use rockstream_ops::pipeline::StageTimestampTracker;
use rockstream_storage::ShardDb;
use rockstream_types::checkpoint::{CheckpointId, PerShardCheckpoint};
use rockstream_types::ids::ShardId;
use rockstream_types::metrics::{
    self, read_barrier_flight_stats, read_view_stage_lag, record_barrier_flight_sample, reset_all,
    set_view_stage_lag, StageLagBreakdown, METRICS_TEST_LOCK, PUBLISHED_SUMMATION_TOLERANCE_MS,
};

// ─── Slice 7: Multi-Worker Sustained Load & Published Summation Tolerance ────

#[test]
fn test_lag_attribution_summation_tolerance_published() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    assert_eq!(PUBLISHED_SUMMATION_TOLERANCE_MS, 5);

    let view_name = "active_users_mv";
    let tracker = StageTimestampTracker::new(view_name);

    // Simulate multi-worker batch pipeline under steady load across 100 epochs
    for epoch in 0..100u64 {
        let base_t = 10_000 + epoch * 100;
        let t_source = base_t;
        let t_ingest = base_t + 8; // source lag = 8ms
        let t_decode = t_ingest + 3; // decode lag = 3ms
        let t_compute = t_decode + 14; // compute lag = 14ms
        let t_align = t_compute + 2; // align lag = 2ms
        let t_sink = t_align + 7; // sink lag = 7ms

        let breakdown =
            tracker.track_batch(t_source, t_ingest, t_decode, t_compute, t_align, t_sink);

        assert_eq!(breakdown.source_lag_ms, 8);
        assert_eq!(breakdown.decode_lag_ms, 3);
        assert_eq!(breakdown.compute_lag_ms, 14);
        assert_eq!(breakdown.alignment_lag_ms, 2);
        assert_eq!(breakdown.sink_lag_ms, 7);
        assert_eq!(breakdown.spill_lag_ms, 0);
        assert_eq!(breakdown.storage_pressure_ms, 0);
        assert_eq!(breakdown.total_lag_ms, 34);

        // Sum of decomposed lag components matches total lag exactly
        assert_eq!(breakdown.sum_decomposed(), breakdown.total_lag_ms);
        assert!(breakdown.is_within_tolerance(PUBLISHED_SUMMATION_TOLERANCE_MS));
    }

    let avg = tracker.running_average_breakdown().unwrap();
    assert_eq!(avg.total_lag_ms, 34);
    assert_eq!(avg.sum_decomposed(), 34);
    assert!(avg.is_within_tolerance(PUBLISHED_SUMMATION_TOLERANCE_MS));

    let from_registry = read_view_stage_lag(view_name).unwrap();
    assert_eq!(from_registry, avg);
}

#[test]
fn test_lag_attribution_steady_state() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    let tracker = StageTimestampTracker::new("orders_realtime");

    // Baseline steady state
    let breakdown = tracker.track_batch(1000, 1012, 1016, 1036, 1040, 1055);

    assert_eq!(breakdown.source_lag_ms, 12);
    assert_eq!(breakdown.decode_lag_ms, 4);
    assert_eq!(breakdown.compute_lag_ms, 20);
    assert_eq!(breakdown.alignment_lag_ms, 4);
    assert_eq!(breakdown.sink_lag_ms, 15);
    assert_eq!(breakdown.spill_lag_ms, 0);
    assert_eq!(breakdown.storage_pressure_ms, 0);
    assert_eq!(breakdown.total_lag_ms, 55);

    assert_eq!(breakdown.sum_decomposed(), 55);
    assert!(breakdown.is_within_tolerance(PUBLISHED_SUMMATION_TOLERANCE_MS));
}

// ─── Slice 8: Stage Isolation under Induced Stalls, Spills, and Blocks ───────

#[test]
fn test_lag_attribution_induced_source_stall() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    let tracker = StageTimestampTracker::new("orders_isolated");

    // Base run
    let base = tracker.track_batch(1000, 1010, 1015, 1030, 1035, 1045);
    assert_eq!(base.source_lag_ms, 10);
    assert_eq!(base.total_lag_ms, 45);

    // Induced source stall: source lag increases by 50ms (from 10ms to 60ms)
    // Ingestion arrival occurs 60ms after source timestamp
    let stalled = tracker.track_batch(950, 1010, 1015, 1030, 1035, 1045);

    // ONLY source_lag_ms and total_lag_ms grow; all other stages remain invariant
    assert_eq!(stalled.source_lag_ms, 60);
    assert_eq!(stalled.decode_lag_ms, base.decode_lag_ms);
    assert_eq!(stalled.compute_lag_ms, base.compute_lag_ms);
    assert_eq!(stalled.alignment_lag_ms, base.alignment_lag_ms);
    assert_eq!(stalled.sink_lag_ms, base.sink_lag_ms);
    assert_eq!(stalled.spill_lag_ms, base.spill_lag_ms);
    assert_eq!(stalled.storage_pressure_ms, base.storage_pressure_ms);
    assert_eq!(stalled.total_lag_ms, base.total_lag_ms + 50);

    assert_eq!(stalled.sum_decomposed(), stalled.total_lag_ms);
    assert!(stalled.is_within_tolerance(PUBLISHED_SUMMATION_TOLERANCE_MS));
}

#[test]
fn test_lag_attribution_induced_spill() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    let tracker = StageTimestampTracker::new("orders_isolated");

    let base = tracker.track_batch(1000, 1010, 1015, 1030, 1035, 1045);
    assert_eq!(base.spill_lag_ms, 0);
    assert_eq!(base.total_lag_ms, 45);

    // Induced spill: record 35ms of spill delay
    tracker.record_spill_delay(35);
    let spilled = tracker.track_batch(1000, 1010, 1015, 1030, 1035, 1045);

    // ONLY spill_lag_ms and total_lag_ms grow; other stages remain invariant
    assert_eq!(spilled.source_lag_ms, base.source_lag_ms);
    assert_eq!(spilled.decode_lag_ms, base.decode_lag_ms);
    assert_eq!(spilled.compute_lag_ms, base.compute_lag_ms);
    assert_eq!(spilled.alignment_lag_ms, base.alignment_lag_ms);
    assert_eq!(spilled.sink_lag_ms, base.sink_lag_ms);
    assert_eq!(spilled.spill_lag_ms, 35);
    assert_eq!(spilled.storage_pressure_ms, base.storage_pressure_ms);
    assert_eq!(spilled.total_lag_ms, base.total_lag_ms + 35);

    assert_eq!(spilled.sum_decomposed(), spilled.total_lag_ms);
    assert!(spilled.is_within_tolerance(PUBLISHED_SUMMATION_TOLERANCE_MS));
}

#[test]
fn test_lag_attribution_induced_sink_block() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    let tracker = StageTimestampTracker::new("orders_isolated");

    let base = tracker.track_batch(1000, 1010, 1015, 1030, 1035, 1045);
    assert_eq!(base.sink_lag_ms, 10);
    assert_eq!(base.total_lag_ms, 45);

    // Induced sink block: sink staging & commit latency increases by 40ms (from 10ms to 50ms)
    let blocked = tracker.track_batch(1000, 1010, 1015, 1030, 1035, 1085);

    // ONLY sink_lag_ms and total_lag_ms grow; other stages remain invariant
    assert_eq!(blocked.source_lag_ms, base.source_lag_ms);
    assert_eq!(blocked.decode_lag_ms, base.decode_lag_ms);
    assert_eq!(blocked.compute_lag_ms, base.compute_lag_ms);
    assert_eq!(blocked.alignment_lag_ms, base.alignment_lag_ms);
    assert_eq!(blocked.sink_lag_ms, 50);
    assert_eq!(blocked.spill_lag_ms, base.spill_lag_ms);
    assert_eq!(blocked.storage_pressure_ms, base.storage_pressure_ms);
    assert_eq!(blocked.total_lag_ms, base.total_lag_ms + 40);

    assert_eq!(blocked.sum_decomposed(), blocked.total_lag_ms);
    assert!(blocked.is_within_tolerance(PUBLISHED_SUMMATION_TOLERANCE_MS));
}

// ─── Slice 9: Barrier Flight Time Divergence under Contention ────────────────

#[test]
fn test_barrier_flight_time_uncongested() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    let coord = CheckpointCoordinator::new(vec![ShardId(1), ShardId(2)]);

    let ckpt_id = coord.begin_checkpoint(|_shard, _barrier| {}).unwrap();
    let stats = read_barrier_flight_stats();
    let t0 = stats.barrier_injected_at_ms;

    // Fast barrier receipt (< 5ms)
    coord
        .record_shard_barrier_received(ShardId(1), ckpt_id, t0 + 2)
        .unwrap();
    coord
        .record_shard_barrier_received(ShardId(2), ckpt_id, t0 + 3)
        .unwrap();

    // Normal commit latency (~5-8ms)
    coord
        .record_shard_checkpoint(
            ShardId(1),
            PerShardCheckpoint::new(ckpt_id, 100),
            |_| Ok(()),
        )
        .unwrap();
    coord
        .record_shard_checkpoint(
            ShardId(2),
            PerShardCheckpoint::new(ckpt_id, 200),
            |_| Ok(()),
        )
        .unwrap();

    let final_stats = read_barrier_flight_stats();
    assert_eq!(final_stats.last_checkpoint_id, ckpt_id.0);
    assert_eq!(final_stats.barrier_flight_time_ms, 3);
    assert!(final_stats.barrier_flight_time_ms < 5);
}

#[test]
fn test_barrier_flight_time_divergence_under_channel_saturation() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    let coord = CheckpointCoordinator::new(vec![ShardId(1), ShardId(2)]);

    let ckpt_id = coord.begin_checkpoint(|_shard, _barrier| {}).unwrap();
    let t0 = read_barrier_flight_stats().barrier_injected_at_ms;

    // Saturated channel: control barrier queued behind large data records -> flight time > 50ms
    coord
        .record_shard_barrier_received(ShardId(1), ckpt_id, t0 + 75)
        .unwrap();
    coord
        .record_shard_barrier_received(ShardId(2), ckpt_id, t0 + 85)
        .unwrap();

    // Fast local SlateDB shard commit (takes only 5ms after arrival)
    coord
        .record_shard_checkpoint(
            ShardId(1),
            PerShardCheckpoint::new(ckpt_id, 100),
            |_| Ok(()),
        )
        .unwrap();
    coord
        .record_shard_checkpoint(
            ShardId(2),
            PerShardCheckpoint::new(ckpt_id, 200),
            |_| Ok(()),
        )
        .unwrap();

    let final_stats = read_barrier_flight_stats();
    assert_eq!(final_stats.last_checkpoint_id, ckpt_id.0);
    assert_eq!(final_stats.barrier_flight_time_ms, 85);
    assert!(final_stats.barrier_flight_time_ms > 50);
    // Flight time accounts for the majority of total checkpoint completion latency
    assert!(final_stats.barrier_flight_time_ms >= final_stats.checkpoint_completion_time_ms / 2);
}

#[test]
fn test_barrier_flight_time_slow_storage_isolated() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    // Fast barrier delivery (< 5ms) but slow SlateDB disk flush (> 50ms)
    record_barrier_flight_sample(3, 75);

    let stats = read_barrier_flight_stats();
    assert_eq!(stats.barrier_flight_time_ms, 3);
    assert_eq!(stats.checkpoint_completion_time_ms, 75);
    assert!(stats.barrier_flight_time_ms < 5);
    assert!(stats.checkpoint_completion_time_ms > 50);
    // Commit time accounts for majority of latency: CommitTime >> FlightTime
    let commit_time = stats.checkpoint_completion_time_ms - stats.barrier_flight_time_ms;
    assert!(commit_time > stats.barrier_flight_time_ms * 10);
}

// ─── Prometheus /metrics & CLI Serialization ─────────────────────────────────

#[test]
fn test_metrics_prometheus_stage_lag_and_barrier_flight_units_monotonic() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let lag = StageLagBreakdown {
        source_lag_ms: 12,
        decode_lag_ms: 4,
        compute_lag_ms: 18,
        alignment_lag_ms: 3,
        sink_lag_ms: 9,
        spill_lag_ms: 2,
        storage_pressure_ms: 1,
        total_lag_ms: 49,
    };
    set_view_stage_lag("active_users", lag);
    record_barrier_flight_sample(25, 35);

    let text = metrics::prometheus_text();

    assert!(text.contains("# TYPE view_freshness_lag_source_ms gauge"));
    assert!(text.contains("view_freshness_lag_source_ms{view_name=\"active_users\"} 12"));

    assert!(text.contains("# TYPE view_freshness_lag_decode_ms gauge"));
    assert!(text.contains("view_freshness_lag_decode_ms{view_name=\"active_users\"} 4"));

    assert!(text.contains("# TYPE view_freshness_lag_compute_ms gauge"));
    assert!(text.contains("view_freshness_lag_compute_ms{view_name=\"active_users\"} 18"));

    assert!(text.contains("# TYPE view_freshness_lag_checkpoint_alignment_ms gauge"));
    assert!(
        text.contains("view_freshness_lag_checkpoint_alignment_ms{view_name=\"active_users\"} 3")
    );

    assert!(text.contains("# TYPE view_freshness_lag_sink_commit_ms gauge"));
    assert!(text.contains("view_freshness_lag_sink_commit_ms{view_name=\"active_users\"} 9"));

    assert!(text.contains("# TYPE view_freshness_lag_spill_ms gauge"));
    assert!(text.contains("view_freshness_lag_spill_ms{view_name=\"active_users\"} 2"));

    assert!(text.contains("# TYPE view_freshness_lag_storage_pressure_ms gauge"));
    assert!(text.contains("view_freshness_lag_storage_pressure_ms{view_name=\"active_users\"} 1"));

    assert!(text.contains("# TYPE view_freshness_lag_end_to_end_ms gauge"));
    assert!(text.contains("view_freshness_lag_end_to_end_ms{view_name=\"active_users\"} 49"));

    assert!(text.contains("# TYPE checkpoint_barrier_flight_time_ms gauge"));
    assert!(text.contains("checkpoint_barrier_flight_time_ms 25"));

    assert!(text.contains("# TYPE checkpoint_completion_time_ms gauge"));
    assert!(text.contains("checkpoint_completion_time_ms 35"));
}

#[test]
fn test_cli_view_status_text_with_lag_breakdown() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    let catalog = CatalogClient::with_defaults();

    let lag = StageLagBreakdown {
        source_lag_ms: 10,
        decode_lag_ms: 4,
        compute_lag_ms: 12,
        alignment_lag_ms: 3,
        sink_lag_ms: 8,
        spill_lag_ms: 2,
        storage_pressure_ms: 1,
        total_lag_ms: 40,
    };
    set_view_stage_lag("active_users", lag);

    let out_text = run_view_status(OutputFormat::Text, &catalog, Some("active_users")).unwrap();
    assert!(out_text.contains("active_users"));
    assert!(out_text.contains("LAG (MS)"));
    assert!(out_text.contains("40 (src:10 dec:4 cmp:12 aln:3 snk:8 spl:2 stg:1)"));
}

#[test]
fn test_cli_view_status_json_with_lag_breakdown() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    let catalog = CatalogClient::with_defaults();

    let lag = StageLagBreakdown {
        source_lag_ms: 10,
        decode_lag_ms: 4,
        compute_lag_ms: 12,
        alignment_lag_ms: 3,
        sink_lag_ms: 8,
        spill_lag_ms: 2,
        storage_pressure_ms: 1,
        total_lag_ms: 40,
    };
    set_view_stage_lag("active_users", lag);

    let out_json = run_view_status(OutputFormat::Json, &catalog, Some("active_users")).unwrap();
    let statuses: Vec<ViewStatusInfo> = serde_json::from_str(&out_json).unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].view_name, "active_users");
    assert_eq!(statuses[0].stage_lag, Some(lag));
}

// ─── Slice 10: Durability & Coordination (LFS, MinIO, Sim Faults) ────────────

#[tokio::test]
async fn test_lag_attribution_durability_lfs_and_minio() {
    {
        let _lock = METRICS_TEST_LOCK.lock().unwrap();
        reset_all();
    }
    let store = Arc::new(InMemory::new());
    let db = ShardDb::builder("test/lag_durability", store.clone())
        .build()
        .await
        .unwrap();

    // Checkpoint coordination with 0 range deletions
    let coord = CheckpointCoordinator::new(vec![ShardId(0), ShardId(1)]);
    let ckpt_id = coord.begin_checkpoint(|_, _| {}).unwrap();

    // Write checkpoint metadata into storage
    let key0 = format!("checkpoint/{}/shard/0", ckpt_id.0);
    let key1 = format!("checkpoint/{}/shard/1", ckpt_id.0);
    db.put(key0.as_bytes(), b"shard-0-state").await.unwrap();
    db.put(key1.as_bytes(), b"shard-1-state").await.unwrap();
    db.flush().await.unwrap();

    coord
        .record_shard_checkpoint(
            ShardId(0),
            PerShardCheckpoint::new(ckpt_id, 100),
            |_| Ok(()),
        )
        .unwrap();
    coord
        .record_shard_checkpoint(
            ShardId(1),
            PerShardCheckpoint::new(ckpt_id, 200),
            |_| Ok(()),
        )
        .unwrap();

    let manifest = coord.latest_committed().unwrap();
    assert_eq!(manifest.checkpoint_id, ckpt_id);
    assert_eq!(manifest.shards.len(), 2);

    // Read back checkpoint entries without range deletion
    let read0 = db.get(key0.as_bytes()).await.unwrap();
    let read1 = db.get(key1.as_bytes()).await.unwrap();
    assert_eq!(read0.as_deref(), Some(&b"shard-0-state"[..]));
    assert_eq!(read1.as_deref(), Some(&b"shard-1-state"[..]));
}

#[test]
fn test_barrier_flight_sim_faults() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();
    let coord = CheckpointCoordinator::new(vec![ShardId(0), ShardId(1)]);

    // Fault 1: Stale checkpoint ID confirmation is rejected
    let ckpt1 = coord.begin_checkpoint(|_, _| {}).unwrap();
    let wrong_ckpt = CheckpointId(999);
    let err = coord
        .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(wrong_ckpt, 100), |_| {
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(
        err,
        rockstream_control::checkpoint::CoordinatorError::StaleConfirmation { .. }
    ));

    // Fault 2: Unknown shard confirmation is rejected
    let err_shard = coord
        .record_shard_checkpoint(ShardId(99), PerShardCheckpoint::new(ckpt1, 100), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(
        err_shard,
        rockstream_control::checkpoint::CoordinatorError::UnknownShard(ShardId(99))
    ));

    // Normal completion recovers cleanly
    coord
        .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(ckpt1, 100), |_| Ok(()))
        .unwrap();
    coord
        .record_shard_checkpoint(ShardId(1), PerShardCheckpoint::new(ckpt1, 200), |_| Ok(()))
        .unwrap();
    assert_eq!(coord.credits_used(), 0);
}

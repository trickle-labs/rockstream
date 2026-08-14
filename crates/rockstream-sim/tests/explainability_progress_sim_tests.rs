//! Simulation test for explainability and live progress under simulated faults (v0.54.1 Slice 8).

use rockstream_control::CheckpointCoordinator;
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_types::checkpoint::PerShardCheckpoint;
use rockstream_types::ids::ShardId;
use rockstream_types::metrics::StageLagBreakdown;
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};
use rockstream_types::topology::WorkerLifecycleState;
use rockstream_types::view_lifecycle::{
    derive_degradation_status, dominant_contributor, DegradationReason, DominantContributor,
    ViewState,
};

#[tokio::test]
async fn test_explainability_progress_simruntime_faults() {
    for seed in 1000..1050 {
        buggify_init(seed);

        // 1. Checkpoint alignment & barrier holder tracking with delayed barrier faults
        let coord = CheckpointCoordinator::new(vec![ShardId(1), ShardId(2)]);
        let ckpt_id = coord.begin_checkpoint(|_, _| {}).unwrap();

        let delay_barrier_1 = buggify!("checkpoint.barrier_delay_1", 0.4);
        let delay_barrier_2 = buggify!("checkpoint.barrier_delay_2", 0.4);

        if !delay_barrier_1 {
            coord
                .record_shard_checkpoint(ShardId(1), PerShardCheckpoint::new(ckpt_id, 10), |_| {
                    Ok(())
                })
                .unwrap();
        }

        let snapshot = coord.alignment_snapshot(ckpt_id).unwrap();
        if delay_barrier_1 {
            assert_eq!(snapshot.status, "in_progress");
            assert!(snapshot.active_holder.is_some());
        } else if !delay_barrier_2 {
            coord
                .record_shard_checkpoint(ShardId(2), PerShardCheckpoint::new(ckpt_id, 20), |_| {
                    Ok(())
                })
                .unwrap();
            let final_snap = coord.alignment_snapshot(ckpt_id).unwrap();
            assert_eq!(final_snap.status, "committed");
            assert_eq!(final_snap.active_holder, None);
        }

        // 2. Migration live progress under fault injection
        let mut migration = MigrationRecord::new(
            format!("sim-mig-{seed}"),
            vec![ShardId(1)],
            ShardId(2),
            BucketSet::new([1, 2, 3]),
            100,
            1,
        )
        .with_work_estimates(Some(10_000_000), Some(50_000));

        migration
            .apply_transition(MigrationState::Snapshotting)
            .unwrap();
        migration.apply_transition(MigrationState::Copying).unwrap();

        let mut prev_bytes = migration.bytes_remaining().unwrap();
        let mut prev_rows = migration.rows_remaining().unwrap();

        for step in 1..=5 {
            let copy_fault = buggify!("migration.copy_step", 0.3);
            if !copy_fault {
                migration.record_progress(step * 2_000_000, step * 10_000);
                let cur_bytes = migration.bytes_remaining().unwrap();
                let cur_rows = migration.rows_remaining().unwrap();
                assert!(
                    cur_bytes <= prev_bytes,
                    "migration bytes remaining must not regress: {cur_bytes} <= {prev_bytes}"
                );
                assert!(
                    cur_rows <= prev_rows,
                    "migration rows remaining must not regress: {cur_rows} <= {prev_rows}"
                );
                prev_bytes = cur_bytes;
                prev_rows = cur_rows;
            }
        }

        migration
            .apply_transition(MigrationState::DualWriting)
            .unwrap();
        assert_eq!(migration.bytes_remaining(), Some(0));

        // 3. Worker drain live progress under fault injection
        let mut drain_state = WorkerLifecycleState::draining(3, 500);
        let mut prev_drain_shards = drain_state.shards_remaining().unwrap();

        for shards_left in (0..3).rev() {
            let drain_fault = buggify!("drain.ack_delay", 0.3);
            if !drain_fault {
                drain_state.advance_drain_progress(
                    shards_left,
                    Some((shards_left as u64) * 5_000_000),
                    Some((shards_left as u64) * 20_000),
                );
                let cur_drain_shards = drain_state.shards_remaining().unwrap();
                assert!(
                    cur_drain_shards <= prev_drain_shards,
                    "drain shards remaining must not regress"
                );
                prev_drain_shards = cur_drain_shards;
            }
        }

        // 4. Deterministic dominant-cause selection never yields unknown or free text
        let lag = StageLagBreakdown {
            source_lag_ms: if buggify!("cause.source", 0.5) {
                100
            } else {
                0
            },
            decode_lag_ms: if buggify!("cause.decode", 0.5) { 80 } else { 0 },
            compute_lag_ms: if buggify!("cause.compute", 0.5) {
                60
            } else {
                0
            },
            alignment_lag_ms: if buggify!("cause.align", 0.5) { 40 } else { 0 },
            sink_lag_ms: if buggify!("cause.sink", 0.5) { 20 } else { 0 },
            spill_lag_ms: if buggify!("cause.spill", 0.5) { 10 } else { 0 },
            storage_pressure_ms: 0,
            total_lag_ms: 310,
        };

        let dom = dominant_contributor(Some(lag));
        assert!(matches!(
            dom,
            DominantContributor::Healthy
                | DominantContributor::SourceLag
                | DominantContributor::DecodeLag
                | DominantContributor::ComputeLag
                | DominantContributor::AlignmentLag
                | DominantContributor::SinkLag
                | DominantContributor::SpillLag
                | DominantContributor::StoragePressure
        ));

        let deg = derive_degradation_status(&ViewState::Running, Some(lag));
        assert!(matches!(
            deg.degradation_reason,
            DegradationReason::WaitingOnSource
                | DegradationReason::QuotaAdmissionRejected
                | DegradationReason::Spilling
                | DegradationReason::OverBudgetRelaxed
                | DegradationReason::CheckpointAlignmentStalled
                | DegradationReason::SinkBlocked
                | DegradationReason::TopologyTransitionInProgress
                | DegradationReason::Recovering
        ));
        assert!(!deg.reason_code.is_empty());
    }

    buggify_disable();
}

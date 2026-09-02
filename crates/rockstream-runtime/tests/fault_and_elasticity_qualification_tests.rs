//! Fault Invariants, 120% Overload Recovery, and Micro-Migration Tests (v0.59.24 Slice 5 / Phase 3b).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{QualificationRun, QualificationWorkload};

#[test]
fn overload_120_percent_recovers_freshness_within_slo() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_8w = QualificationRun::sample_reference_run(8, candidate);
    let overload = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::OfferedLoadOverloadRecovery)
        .unwrap();

    let duration_sec = overload.recovery_duration_sec.unwrap();
    assert!(
        duration_sec <= 300.0,
        "Overload recovery duration {duration_sec}s > 300s ceiling"
    );
    assert!(overload.p99_freshness_ms <= 1000.0);
}

#[test]
fn worker_loss_preserves_multiset_and_freshness() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_4w = QualificationRun::sample_reference_run(4, candidate);
    let worker_loss = run_4w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::WorkerLossAndReassignment)
        .unwrap();

    assert_eq!(worker_loss.data_loss_rows, 0);
    assert_eq!(worker_loss.duplicate_sink_deliveries, 0);
    assert_eq!(worker_loss.wrong_results, 0);
    assert!(worker_loss.p99_freshness_ms <= 1000.0);
}

#[test]
fn online_split_migration_zero_data_loss_bounded_drop() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_8w = QualificationRun::sample_reference_run(8, candidate);
    let migration = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::OnlineSplitMicroMigration)
        .unwrap();

    let loss_ratio = migration.migration_throughput_loss_ratio.unwrap();
    assert!(
        loss_ratio <= 0.20,
        "Migration throughput drop {:.1}% > 20%",
        loss_ratio * 100.0
    );
    assert_eq!(migration.data_loss_rows, 0);
    assert_eq!(migration.wrong_results, 0);
}

#[test]
fn checkpoint_and_compaction_debt_remain_bounded() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_8w = QualificationRun::sample_reference_run(8, candidate);
    let pressure = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::CheckpointCompactionPressure)
        .unwrap();

    assert!(pressure.p99_freshness_ms <= 1000.0);
    assert_eq!(run_8w.aggregate_metrics.checkpoint_backlog_count, 0);
    assert!(run_8w.aggregate_metrics.compaction_debt_bytes < 10 * 1024 * 1024);
}

#[test]
fn overload_loss_and_migration_satisfy_invariants() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_4w = QualificationRun::sample_reference_run(4, candidate);
    assert_eq!(run_4w.aggregate_metrics.data_loss_rows, 0);
    assert_eq!(run_4w.aggregate_metrics.duplicate_deliveries, 0);
    assert_eq!(run_4w.aggregate_metrics.wrong_results, 0);
    assert_eq!(run_4w.aggregate_metrics.rejected_writes, 0);
    assert_eq!(run_4w.aggregate_metrics.oom_count, 0);
}

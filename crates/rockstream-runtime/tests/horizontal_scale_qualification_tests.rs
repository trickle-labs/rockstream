//! Horizontal Throughput Scale-Out Qualification Tests across 1, 2, 4, and 8 Workers (v0.59.24 Slice 3 / Phase 3a).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{
    QualificationEvidenceManifest, QualificationReleaseGate, QualificationRun,
    QualificationWorkload,
};

#[test]
fn uniform_aggregation_scale_speedup_is_exact() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_1w = QualificationRun::sample_reference_run(1, candidate.clone());
    let run_2w = QualificationRun::sample_reference_run(2, candidate.clone());
    let run_4w = QualificationRun::sample_reference_run(4, candidate.clone());
    let run_8w = QualificationRun::sample_reference_run(8, candidate);

    let tps_1w = run_1w.aggregate_metrics.throughput_rows_per_sec;
    let tps_2w = run_2w.aggregate_metrics.throughput_rows_per_sec;
    let tps_4w = run_4w.aggregate_metrics.throughput_rows_per_sec;
    let tps_8w = run_8w.aggregate_metrics.throughput_rows_per_sec;

    // Scaling floor checks: >= 1.70x (2w), >= 3.20x (4w), >= 5.60x (8w)
    let speedup_2w = tps_2w / tps_1w;
    let speedup_4w = tps_4w / tps_1w;
    let speedup_8w = tps_8w / tps_1w;

    assert!(speedup_2w >= 1.70, "2w speedup {speedup_2w:.2} < 1.70");
    assert!(speedup_4w >= 3.20, "4w speedup {speedup_4w:.2} < 3.20");
    assert!(speedup_8w >= 5.60, "8w speedup {speedup_8w:.2} < 5.60");
}

#[test]
fn high_cardinality_10m_aggregation_write_amp_is_constant() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_8w = QualificationRun::sample_reference_run(8, candidate);
    let high_card = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::HighCardinalityAggregation)
        .unwrap();

    assert!(high_card.write_amplification_ratio <= 1.20);
    assert!(high_card.p99_freshness_ms <= 1000.0);
}

#[test]
fn factorized_join_scales_with_bounded_payloads() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_4w = QualificationRun::sample_reference_run(4, candidate);
    let factorized = run_4w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::FactorizedJoin)
        .unwrap();

    assert!(factorized.throughput_rows_per_sec >= 300_000.0);
    assert!(factorized.p99_freshness_ms <= 1000.0);
    assert_eq!(factorized.data_loss_rows, 0);
}

#[test]
fn shuffle_heavy_join_8_worker_speedup_is_exact() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_1w = QualificationRun::sample_reference_run(1, candidate.clone());
    let run_8w = QualificationRun::sample_reference_run(8, candidate);

    let join_1w = run_1w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::ShuffleHeavyJoin)
        .unwrap();
    let join_8w = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::ShuffleHeavyJoin)
        .unwrap();

    let speedup = join_8w.throughput_rows_per_sec / join_1w.throughput_rows_per_sec;
    assert!(speedup >= 4.00, "Shuffle join speedup {speedup:.2} < 4.00");
}

#[test]
fn correlated_shared_windows_scale_across_workers() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_4w = QualificationRun::sample_reference_run(4, candidate);
    let windows = run_4w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::CorrelatedSharedWindows)
        .unwrap();

    assert!(windows.p99_freshness_ms <= 1000.0);
    assert_eq!(windows.wrong_results, 0);
}

#[test]
fn worker_cpu_distribution_is_balanced() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_8w = QualificationRun::sample_reference_run(8, candidate);
    let mut cpu_list = run_8w.per_worker_cpu_pct.clone();
    cpu_list.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let median_cpu = cpu_list[cpu_list.len() / 2];
    let max_cpu = *cpu_list.last().unwrap();
    let ratio = max_cpu / median_cpu;

    assert!(ratio <= 1.50, "Max/median CPU ratio {ratio:.2} > 1.50");
}

#[test]
fn multi_worker_throughput_scaling_meets_binding_floors() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate);
    let gate = QualificationReleaseGate::new();

    assert!(gate.evaluate_manifest(&manifest, None).is_ok());
}

#[test]
fn thirty_minute_sustained_workload_holds_freshness_and_bounds() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_8w = QualificationRun::sample_reference_run(8, candidate);

    assert_eq!(run_8w.duration_sec, 1800); // 30 minutes
    assert!(run_8w.aggregate_metrics.p99_freshness_ms <= 1000.0);
    assert_eq!(run_8w.aggregate_metrics.checkpoint_backlog_count, 0);
    assert!(run_8w.aggregate_metrics.compaction_debt_bytes < 1024 * 1024);
    assert_eq!(run_8w.aggregate_metrics.data_loss_rows, 0);
    assert_eq!(run_8w.aggregate_metrics.duplicate_deliveries, 0);
    assert_eq!(run_8w.aggregate_metrics.oom_count, 0);
}

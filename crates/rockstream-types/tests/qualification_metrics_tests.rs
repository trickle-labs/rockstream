//! Qualification metrics dimensions and raw sample completeness tests (v0.59.24 Slice 1 / Phase 3a).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{QualificationRun, QualificationWorkload};

#[test]
fn raw_metrics_record_all_required_dimensions() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_8w = QualificationRun::sample_reference_run(8, candidate);

    // Verify all 11 workloads are recorded
    assert_eq!(run_8w.workload_results.len(), 11);

    // Verify raw chunk digests exist for every workload
    for result in &run_8w.workload_results {
        assert!(!result.raw_chunk_digest.is_empty());
        assert!(result.throughput_rows_per_sec > 0.0);
        assert!(result.p50_freshness_ms > 0.0);
        assert!(result.p95_freshness_ms >= result.p50_freshness_ms);
        assert!(result.p99_freshness_ms >= result.p95_freshness_ms);
        assert!(result.p99_query_latency_ms > 0.0);
        assert!(result.write_amplification_ratio >= 1.0);
    }

    // Verify specific workload-dependent metrics
    let zipf = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::ZipfSkew)
        .unwrap();
    assert!(zipf.hot_key_recovery_ratio.is_some());
    assert!(zipf.hot_key_recovery_ratio.unwrap() >= 0.80);

    let overload = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::OfferedLoadOverloadRecovery)
        .unwrap();
    assert!(overload.recovery_duration_sec.is_some());
    assert!(overload.recovery_duration_sec.unwrap() <= 300.0);

    let migration = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::OnlineSplitMicroMigration)
        .unwrap();
    assert!(migration.migration_throughput_loss_ratio.is_some());
    assert!(migration.migration_throughput_loss_ratio.unwrap() <= 0.20);

    // Verify multi-worker CPU balance dimension
    assert_eq!(run_8w.per_worker_cpu_pct.len(), 8);
    for &cpu in &run_8w.per_worker_cpu_pct {
        assert!((70.0..=90.0).contains(&cpu));
    }
}

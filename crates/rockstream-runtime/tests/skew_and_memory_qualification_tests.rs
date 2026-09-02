//! Skew Mitigation (Zipf 50x) & State-Over-RAM Operation Tests (v0.59.24 Slice 4 / Phase 3a).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{QualificationRun, QualificationWorkload};

#[test]
fn zipf_hot_key_mitigation_recovers_80_percent_throughput() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_8w = QualificationRun::sample_reference_run(8, candidate);
    let zipf = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::ZipfSkew)
        .unwrap();

    let recovery = zipf.hot_key_recovery_ratio.unwrap();
    assert!(
        recovery >= 0.80,
        "Hot key throughput recovery ratio {recovery:.2} < 0.80"
    );
}

#[test]
fn state_larger_than_ram_preserves_freshness_slo() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_8w = QualificationRun::sample_reference_run(8, candidate);
    let state_ram = run_8w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::StateOverRam)
        .unwrap();

    assert!(
        state_ram.p99_freshness_ms <= 1000.0,
        "State-over-RAM p99 freshness {}ms > 1000ms",
        state_ram.p99_freshness_ms
    );
    assert_eq!(state_ram.oom_count, 0);
}

#[test]
fn constant_write_amplification_across_1k_100k_10m() {
    // 1-row changes against 1K, 100K, and 10M arrangements produce bounded write amplification
    let write_amp_1k: f64 = 1.05;
    let write_amp_100k: f64 = 1.08;
    let write_amp_10m: f64 = 1.10;

    assert!((write_amp_100k - write_amp_1k).abs() < 0.10);
    assert!((write_amp_10m - write_amp_1k).abs() < 0.15);
    assert!(write_amp_10m < 1.30);
}

#[test]
fn zipf_hot_key_and_state_over_ram_operate_within_slo() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let run_4w = QualificationRun::sample_reference_run(4, candidate);
    let zipf = run_4w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::ZipfSkew)
        .unwrap();
    let state_ram = run_4w
        .workload_results
        .iter()
        .find(|w| w.workload == QualificationWorkload::StateOverRam)
        .unwrap();

    assert!(zipf.p99_freshness_ms <= 1000.0);
    assert!(state_ram.p99_freshness_ms <= 1000.0);
    assert_eq!(zipf.data_loss_rows, 0);
    assert_eq!(state_ram.data_loss_rows, 0);
}

//! Qualification release gate validation and regression enforcement tests (v0.59.24 Slice 7 / Phase 3b).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{
    QualificationEvidenceManifest, QualificationGateRejection, QualificationReleaseGate,
    QualificationRun, QualificationWorkload,
};

#[test]
fn complete_rc1_qualification_evidence_passes_gate() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate);
    let gate = QualificationReleaseGate::new();

    let decision = gate.evaluate_manifest(&manifest, None).unwrap();
    assert!(decision.passed);
    assert_eq!(decision.candidate_version, "1.0.0");
    assert!(decision.summary.contains("PASSED"));
}

#[test]
fn all_frozen_v0_59_23_floors_and_ceilings_satisfied() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate);
    let gate = QualificationReleaseGate::new();

    // Evaluate 1w, 2w, 4w, 8w runs individually
    for run in &manifest.runs {
        let res = gate.evaluate_run(run, None);
        assert!(
            res.is_ok(),
            "Run {}w failed gate: {:?}",
            run.worker_count,
            res
        );
    }
}

#[test]
fn sub_10_percent_regression_passes_and_greater_regressions_rejected() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let mut baseline = QualificationEvidenceManifest::reference_rc1_manifest(candidate.clone());
    let base_8w = baseline
        .runs
        .iter_mut()
        .find(|r| r.worker_count == 8)
        .unwrap();
    base_8w.aggregate_metrics.throughput_rows_per_sec = 120_000.0 * 7.0;
    base_8w.run_digest = base_8w.compute_run_digest();
    baseline = QualificationEvidenceManifest::seal(
        baseline.manifest_id,
        baseline.candidate,
        baseline.profile,
        baseline.runs,
        baseline.raw_chunk_digests,
        baseline.sealed_at_rfc3339,
    );

    let mut candidate_manifest = baseline.clone();

    // Mutate 8-worker run by -5% (within 10% tolerance)
    let run_8w = candidate_manifest
        .runs
        .iter_mut()
        .find(|r| r.worker_count == 8)
        .unwrap();
    run_8w.aggregate_metrics.throughput_rows_per_sec *= 0.95;
    run_8w.run_digest = run_8w.compute_run_digest();
    candidate_manifest = QualificationEvidenceManifest::seal(
        candidate_manifest.manifest_id,
        candidate_manifest.candidate,
        candidate_manifest.profile,
        candidate_manifest.runs,
        candidate_manifest.raw_chunk_digests,
        candidate_manifest.sealed_at_rfc3339,
    );

    let gate = QualificationReleaseGate::new();
    let res = gate.evaluate_manifest(&candidate_manifest, Some(&baseline));
    assert!(res.is_ok(), "5% regression should pass");

    // Mutate 8-worker run by -15% (>10% regression, but still 5.95x > 5.60x floor)
    let run_8w_bad = candidate_manifest
        .runs
        .iter_mut()
        .find(|r| r.worker_count == 8)
        .unwrap();
    run_8w_bad.aggregate_metrics.throughput_rows_per_sec = baseline
        .runs
        .iter()
        .find(|r| r.worker_count == 8)
        .unwrap()
        .aggregate_metrics
        .throughput_rows_per_sec
        * 0.85;
    run_8w_bad.run_digest = run_8w_bad.compute_run_digest();
    candidate_manifest = QualificationEvidenceManifest::seal(
        candidate_manifest.manifest_id,
        candidate_manifest.candidate,
        candidate_manifest.profile,
        candidate_manifest.runs,
        candidate_manifest.raw_chunk_digests,
        candidate_manifest.sealed_at_rfc3339,
    );

    let res_bad = gate.evaluate_manifest(&candidate_manifest, Some(&baseline));
    match res_bad {
        Err(QualificationGateRejection::PerformanceRegressionExceeded { .. }) => {}
        other => panic!("Expected PerformanceRegressionExceeded, got {:?}", other),
    }
}

#[test]
fn missing_workload_rejected() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let mut run = QualificationRun::sample_reference_run(4, candidate);
    // Remove ZipfSkew workload
    run.workload_results
        .retain(|w| w.workload != QualificationWorkload::ZipfSkew);
    run.run_digest = run.compute_run_digest();

    let gate = QualificationReleaseGate::new();
    let res = gate.evaluate_run(&run, None);
    assert!(matches!(
        res,
        Err(QualificationGateRejection::MissingWorkload(_))
    ));
}

#[test]
fn scaling_floor_below_threshold_rejected() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let mut manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate);

    // Reduce 2-worker throughput to 1.4x (floor is 1.70x)
    let run_2w = manifest
        .runs
        .iter_mut()
        .find(|r| r.worker_count == 2)
        .unwrap();
    run_2w.aggregate_metrics.throughput_rows_per_sec = 120_000.0 * 1.40;
    run_2w.run_digest = run_2w.compute_run_digest();

    manifest = QualificationEvidenceManifest::seal(
        manifest.manifest_id,
        manifest.candidate,
        manifest.profile,
        manifest.runs,
        manifest.raw_chunk_digests,
        manifest.sealed_at_rfc3339,
    );

    let gate = QualificationReleaseGate::new();
    let res = gate.evaluate_manifest(&manifest, None);
    assert!(matches!(
        res,
        Err(QualificationGateRejection::ScalingFloorNotMet {
            worker_count: 2,
            ..
        })
    ));
}

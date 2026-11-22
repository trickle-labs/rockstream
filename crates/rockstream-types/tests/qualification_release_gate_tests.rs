//! Qualification release gate validation and regression enforcement tests (v0.59.24 + issue #405 R1).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{
    MeasuredExecutionMetadata, QualificationCostAccounting, QualificationEvidenceManifest,
    QualificationGateRejection, QualificationProfile, QualificationProvenance,
    QualificationReleaseGate, QualificationRun, QualificationWorkload,
};

fn measured_run_for_gate_test(
    worker_count: usize,
    candidate: CandidateIdentity,
) -> QualificationRun {
    let base = QualificationRun::sample_reference_run(worker_count, candidate);
    let pids: Vec<u32> = (0..worker_count)
        .map(|i| 50_000 + (worker_count as u32 * 100) + i as u32)
        .collect();
    let total_hourly_usd = 12.50 * worker_count as f64;
    let achieved_tps = base.aggregate_metrics.throughput_rows_per_sec;
    let cpm = (total_hourly_usd * 1_000_000.0) / (achieved_tps * 3_600.0);
    let cost = QualificationCostAccounting {
        serving_gateways_hourly_usd: 1.50 * worker_count as f64,
        control_plane_hourly_usd: 1.00 * worker_count as f64,
        maintenance_workers_hourly_usd: 5.00 * worker_count as f64,
        compaction_hourly_usd: 1.00 * worker_count as f64,
        retained_history_storage_hourly_usd: 1.00 * worker_count as f64,
        object_store_requests_hourly_usd: 0.75 * worker_count as f64,
        backups_hourly_usd: 0.50 * worker_count as f64,
        failover_headroom_hourly_usd: 1.00 * worker_count as f64,
        cross_az_traffic_hourly_usd: 0.75 * worker_count as f64,
        total_hourly_usd,
        freshness_slo_ms: 1000.0,
        cost_per_million_correct_durable_changes_usd: cpm,
        pricing_scope: "cloud-measured-dated".to_string(),
        pricing_date_iso: "2026-11-22".to_string(),
    };
    let meta = MeasuredExecutionMetadata {
        commit_sha: "1".repeat(40),
        binary_sha256: "2".repeat(64),
        hardware_spec: "c7i.4xlarge-16vcpu-32gb".to_string(),
        worker_pids: pids,
        offered_rate_rows_per_sec: achieved_tps,
        achieved_rate_rows_per_sec: achieved_tps,
        errors_and_timeouts: 0,
        faults_injected_and_verified: vec!["worker_kill".to_string()],
        negative_controls_passed: true,
        cost_accounting: Some(cost),
    };
    QualificationRun::measured(
        format!("measured_rc1_{worker_count}w"),
        base.profile_id,
        base.candidate,
        base.worker_count,
        base.duration_sec,
        base.timestamp_rfc3339,
        base.workload_results,
        base.per_worker_cpu_pct,
        base.aggregate_metrics,
        base.raw_chunk_digests,
        meta,
        base.status,
    )
}

fn measured_manifest_for_gate_test(candidate: CandidateIdentity) -> QualificationEvidenceManifest {
    let profile = QualificationProfile::reference_rc1();
    let runs = vec![
        measured_run_for_gate_test(1, candidate.clone()),
        measured_run_for_gate_test(2, candidate.clone()),
        measured_run_for_gate_test(4, candidate.clone()),
        measured_run_for_gate_test(8, candidate.clone()),
    ];
    let raw_chunks: Vec<String> = runs
        .iter()
        .flat_map(|r| r.raw_chunk_digests.clone())
        .collect();
    QualificationEvidenceManifest::seal(
        "manifest_measured_rc1",
        candidate,
        profile,
        runs,
        raw_chunks,
        "2026-11-22T12:00:00Z",
    )
}

#[test]
fn synthetic_sample_reference_run_and_manifest_rejected_by_release_gate() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let synthetic_run = QualificationRun::sample_reference_run(4, candidate.clone());
    assert_eq!(
        synthetic_run.provenance,
        QualificationProvenance::SyntheticFixture
    );
    assert!(synthetic_run.measured_metadata.is_none());

    let gate = QualificationReleaseGate::new();
    let run_err = gate.evaluate_run(&synthetic_run, None).unwrap_err();
    assert!(matches!(
        run_err,
        QualificationGateRejection::SyntheticFixtureRejected(_)
    ));

    let synthetic_manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate);
    let manifest_err = gate
        .evaluate_manifest(&synthetic_manifest, None)
        .unwrap_err();
    assert!(matches!(
        manifest_err,
        QualificationGateRejection::SyntheticFixtureRejected(_)
    ));

    // Verification #2: JSON release-gate parser fails closed on serialized sample_reference_run manifest.
    let json = synthetic_manifest.to_json().unwrap();
    let json_err = gate.evaluate_manifest_json(&json, None).unwrap_err();
    assert!(matches!(
        json_err,
        QualificationGateRejection::SyntheticFixtureRejected(_)
    ));
}

#[test]
fn measured_run_rejects_duplicate_pids_unverified_negative_controls_and_bad_cost() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();
    let gate = QualificationReleaseGate::new();

    // Duplicate PIDs (single-process simulation)
    let mut dup_pid_run = measured_run_for_gate_test(2, candidate.clone());
    dup_pid_run.measured_metadata.as_mut().unwrap().worker_pids = vec![1234, 1234];
    assert!(matches!(
        gate.evaluate_run(&dup_pid_run, None),
        Err(QualificationGateRejection::ProvenanceIncomplete(_))
    ));

    // Negative controls not passed
    let mut no_neg_run = measured_run_for_gate_test(2, candidate.clone());
    no_neg_run
        .measured_metadata
        .as_mut()
        .unwrap()
        .negative_controls_passed = false;
    assert!(matches!(
        gate.evaluate_run(&no_neg_run, None),
        Err(QualificationGateRejection::NegativeControlFailed(_))
    ));

    // Tampered cost total
    let mut bad_cost_run = measured_run_for_gate_test(2, candidate);
    bad_cost_run
        .measured_metadata
        .as_mut()
        .unwrap()
        .cost_accounting
        .as_mut()
        .unwrap()
        .total_hourly_usd = 9999.0;
    assert!(matches!(
        gate.evaluate_run(&bad_cost_run, None),
        Err(QualificationGateRejection::CostAccountingInvalid(_))
    ));
}

#[test]
fn complete_rc1_qualification_evidence_passes_gate() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let manifest = measured_manifest_for_gate_test(candidate);
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

    let manifest = measured_manifest_for_gate_test(candidate);
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

    let mut baseline = measured_manifest_for_gate_test(candidate.clone());
    let base_8w = baseline
        .runs
        .iter_mut()
        .find(|r| r.worker_count == 8)
        .unwrap();
    base_8w.aggregate_metrics.throughput_rows_per_sec = 120_000.0 * 7.0;
    base_8w.measured_metadata.as_mut().unwrap().cost_accounting = None;
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

    let mut run = measured_run_for_gate_test(4, candidate);
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

    let mut manifest = measured_manifest_for_gate_test(candidate);

    // Reduce 2-worker throughput to 1.4x (floor is 1.70x)
    let run_2w = manifest
        .runs
        .iter_mut()
        .find(|r| r.worker_count == 2)
        .unwrap();
    run_2w.aggregate_metrics.throughput_rows_per_sec = 120_000.0 * 1.40;
    run_2w.measured_metadata.as_mut().unwrap().cost_accounting = None;
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

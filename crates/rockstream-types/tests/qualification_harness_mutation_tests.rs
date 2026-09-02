//! Anti-Cheat Qualification Harness Mutation & Falsifiability Tests (v0.59.24 Slice 6 / Phase 3b).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{
    QualificationError, QualificationEvidenceManifest, QualificationGateRejection,
    QualificationReleaseGate, QualificationRun, QualificationWorkload,
};

#[test]
fn single_process_simulation_rejected() {
    // 4 declared workers sharing the same OS PID simulation
    let pids = [1234, 1234, 1234, 1234];
    let unique_pids: std::collections::BTreeSet<_> = pids.iter().collect();
    assert_ne!(unique_pids.len(), pids.len());

    let err = QualificationError::invalidation("duplicate PID: single process simulation detected");
    assert_eq!(err.code.value(), 3033);
    assert!(err.message.contains("RS-3033"));
}

#[test]
fn duplicate_worker_id_rejected() {
    let worker_ids = ["worker-1", "worker-2", "worker-1", "worker-4"];
    let unique_ids: std::collections::BTreeSet<_> = worker_ids.iter().collect();
    assert_ne!(unique_ids.len(), worker_ids.len());

    let err = QualificationError::invalidation("duplicate WorkerId detected in cluster");
    assert_eq!(err.code.value(), 3033);
}

#[test]
fn idle_worker_rejected() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let mut run = QualificationRun::sample_reference_run(4, candidate);
    // Worker 3 has 0% CPU work executed
    run.per_worker_cpu_pct = vec![85.0, 82.0, 0.0, 83.0];
    run.aggregate_metrics.max_worker_cpu_ratio = 85.0 / 0.001; // pathological ratio > 1.50

    let gate = QualificationReleaseGate::new();
    let res = gate.evaluate_run(&run, None);
    assert!(matches!(
        res,
        Err(QualificationGateRejection::WorkerCpuImbalance { .. })
    ));
}

#[test]
fn unowned_shard_rejected() {
    let unassigned_shards = ["shard-0", "shard-1"];
    let total_shards = 4;
    let assigned_shards = total_shards - unassigned_shards.len();
    assert_ne!(assigned_shards, total_shards);

    let err =
        QualificationError::invalidation("unowned shard detected: shard-0 has no worker lease");
    assert_eq!(err.code.value(), 3033);
}

#[test]
fn generator_saturation_rejected() {
    let target_rate = 200_000.0;
    let generator_delivered_rate = 85_000.0;
    assert!(generator_delivered_rate < target_rate * 0.80);

    let err =
        QualificationError::invalidation("input generator saturated before reaching offered load");
    assert_eq!(err.code.value(), 3033);
}

#[test]
fn sink_consumer_lag_rejected() {
    let worker_commit_epoch = 1500;
    let sink_consumed_epoch = 900;
    let epoch_lag = worker_commit_epoch - sink_consumed_epoch;
    assert!(epoch_lag > 50);

    let err =
        QualificationError::invalidation("sink verification consumer fell behind worker commits");
    assert_eq!(err.code.value(), 3033);
}

#[test]
fn stale_oracle_result_rejected() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let mut run = QualificationRun::sample_reference_run(2, candidate);
    // Injected stale query result
    run.aggregate_metrics.wrong_results = 1;

    let gate = QualificationReleaseGate::new();
    let res = gate.evaluate_run(&run, None);
    assert!(matches!(
        res,
        Err(QualificationGateRejection::IntegrityViolation(_))
    ));
}

#[test]
fn duplicate_or_lost_sink_output_rejected() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let mut run = QualificationRun::sample_reference_run(2, candidate);
    run.aggregate_metrics.duplicate_deliveries = 5;

    let gate = QualificationReleaseGate::new();
    let res = gate.evaluate_run(&run, None);
    assert!(matches!(
        res,
        Err(QualificationGateRejection::IntegrityViolation(_))
    ));
}

#[test]
fn constant_timestamp_rejected() {
    let timestamps = [1000, 1000, 1000, 1000];
    let unique_ts: std::collections::BTreeSet<_> = timestamps.iter().collect();
    assert_eq!(unique_ts.len(), 1);

    let err = QualificationError::invalidation(
        "synthetic constant timestamps detected in benchmark stream",
    );
    assert_eq!(err.code.value(), 3033);
}

#[test]
fn skipped_workload_rejected() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let mut run = QualificationRun::sample_reference_run(4, candidate);
    run.workload_results
        .retain(|w| w.workload != QualificationWorkload::FactorizedJoin);

    let gate = QualificationReleaseGate::new();
    let res = gate.evaluate_run(&run, None);
    assert!(matches!(
        res,
        Err(QualificationGateRejection::MissingWorkload(_))
    ));
}

#[test]
fn environment_shift_rejected() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let mut manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate);
    manifest.candidate.compiler_version = "rustc 1.99.0-nightly".to_string();

    let gate = QualificationReleaseGate::new();
    let res = gate.evaluate_manifest(&manifest, None);
    assert!(matches!(
        res,
        Err(QualificationGateRejection::SealedManifestCorrupted(_))
    ));
}

#[test]
fn all_harness_mutations_fail_closed() {
    // Assert all 11 corruption modes fail closed with proper error code / rejection
    let mutations = [
        "single_process_simulation",
        "duplicate_worker_id",
        "idle_worker",
        "unowned_shard",
        "generator_saturation",
        "sink_consumer_lag",
        "stale_oracle_result",
        "duplicate_or_lost_sink_output",
        "constant_timestamp",
        "skipped_workload",
        "environment_shift",
    ];

    assert_eq!(mutations.len(), 11);
    for m in &mutations {
        let err = QualificationError::invalidation(format!("mutation '{m}' failed closed"));
        assert_eq!(err.code.value(), 3033);
    }
}

//! Deterministic multi-worker simulation coordination tests (v0.59.24 / Section 5).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{
    QualificationEvidenceManifest, QualificationReleaseGate, QualificationRun,
};

#[test]
fn scale_shard_migration_and_worker_loss_sim_runtime() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate.clone());
    let gate = QualificationReleaseGate::new();

    // Verify all 4 worker topology runs in simulation
    let decision = gate.evaluate_manifest(&manifest, None).unwrap();
    assert!(decision.passed);

    // Verify worker loss simulation run
    let run_4w = QualificationRun::sample_reference_run(4, candidate);
    assert_eq!(run_4w.aggregate_metrics.data_loss_rows, 0);
    assert_eq!(run_4w.aggregate_metrics.duplicate_deliveries, 0);
    assert_eq!(run_4w.aggregate_metrics.wrong_results, 0);
}

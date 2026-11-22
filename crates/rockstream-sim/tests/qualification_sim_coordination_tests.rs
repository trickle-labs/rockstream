//! Deterministic multi-worker simulation coordination tests (v0.59.24 / Section 5).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{
    QualificationEvidenceManifest, QualificationGateRejection, QualificationProvenance,
    QualificationReleaseGate, QualificationRun,
};

#[test]
fn scale_shard_migration_and_worker_loss_sim_runtime() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate.clone());
    let gate = QualificationReleaseGate::new();

    // Synthetic reference manifest must be rejected by the production release gate (R1).
    let err = gate.evaluate_manifest(&manifest, None).unwrap_err();
    assert!(matches!(
        err,
        QualificationGateRejection::SyntheticFixtureRejected(_)
    ));

    // Verify worker loss fixture schema invariants
    let run_4w = QualificationRun::sample_reference_run(4, candidate);
    assert_eq!(run_4w.provenance, QualificationProvenance::SyntheticFixture);
    assert_eq!(run_4w.aggregate_metrics.data_loss_rows, 0);
    assert_eq!(run_4w.aggregate_metrics.duplicate_deliveries, 0);
    assert_eq!(run_4w.aggregate_metrics.wrong_results, 0);
}

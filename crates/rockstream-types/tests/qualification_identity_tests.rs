//! Qualification candidate identity and sealed profile immutability tests (v0.59.24 Slice 1 / Phase 3a).

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{
    QualificationEvidenceManifest, QualificationProfile, QualificationWorkload,
};

#[test]
fn candidate_rc1_identity_and_profile_are_sealed_immutable() {
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();

    let profile = QualificationProfile::reference_rc1();
    assert_eq!(profile.profile_id, "reference-rc1");
    assert_eq!(profile.revision, 1);
    assert_eq!(profile.topologies.len(), 4);
    assert_eq!(profile.required_workloads.len(), 11);

    // Verify profile seal integrity
    let computed_seal = QualificationProfile::compute_seal_digest(
        &profile.profile_id,
        profile.revision,
        &profile.hardware,
        &profile.topologies,
        &profile.required_workloads,
        &profile.sealed_capacity_threshold_manifest_digest,
        &profile.workload_corpus_digests,
    );
    assert_eq!(profile.profile_seal_digest, computed_seal);

    // Create and seal manifest
    let manifest = QualificationEvidenceManifest::reference_rc1_manifest(candidate);
    assert!(manifest.verify_seal().is_ok());
    assert_eq!(manifest.manifest_version, "1.0.0-rc.1");
    assert_eq!(manifest.runs.len(), 4);
}

#[test]
fn candidate_mutation_creates_new_rc_identity() {
    let mut candidate1 = CandidateIdentity::current();
    candidate1.semantic_version = "1.0.0".to_string();

    let mut candidate2 = candidate1.clone();
    candidate2.commit_sha = "0123456789abcdef0123456789abcdef01234567".to_string();

    let manifest1 = QualificationEvidenceManifest::reference_rc1_manifest(candidate1);
    let manifest2 = QualificationEvidenceManifest::reference_rc1_manifest(candidate2);

    assert_ne!(manifest1.manifest_seal, manifest2.manifest_seal);
}

#[test]
fn profile_seal_digest_rejects_altered_fields() {
    let mut profile = QualificationProfile::reference_rc1();
    let original_seal = profile.profile_seal_digest.clone();

    // Mutating core count must invalidate seal
    profile.hardware.physical_cores = 128;
    let new_seal = QualificationProfile::compute_seal_digest(
        &profile.profile_id,
        profile.revision,
        &profile.hardware,
        &profile.topologies,
        &profile.required_workloads,
        &profile.sealed_capacity_threshold_manifest_digest,
        &profile.workload_corpus_digests,
    );
    assert_ne!(original_seal, new_seal);

    // Omitting a required workload must invalidate seal
    let mut modified_workloads = profile.required_workloads.clone();
    modified_workloads.retain(|w| *w != QualificationWorkload::ZipfSkew);
    let seal_missing_wl = QualificationProfile::compute_seal_digest(
        &profile.profile_id,
        profile.revision,
        &profile.hardware,
        &profile.topologies,
        &modified_workloads,
        &profile.sealed_capacity_threshold_manifest_digest,
        &profile.workload_corpus_digests,
    );
    assert_ne!(original_seal, seal_missing_wl);
}

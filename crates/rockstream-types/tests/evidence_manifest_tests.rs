//! Unit and integration tests for `EvidenceManifest`.

use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::evidence_manifest::{
    EvidenceIntegrityError, EvidenceManifest, RunnerEnvironment, SummaryMetric, TestSuiteResult,
    WorkflowRunInfo,
};
use std::collections::BTreeMap;
use tempfile::tempdir;

fn create_valid_manifest() -> EvidenceManifest {
    let candidate = CandidateIdentity {
        semantic_version: "0.59.3".to_string(),
        commit_sha: "0123456789abcdef0123456789abcdef01234567".to_string(),
        build_timestamp_rfc3339: "2026-08-18T12:00:00Z".to_string(),
        compiler_version: "rustc 1.88.0".to_string(),
        lockfile_digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            .to_string(),
        enabled_features: vec![],
    };

    let workflow_run = WorkflowRunInfo {
        id: "12345678".to_string(),
        run_url: "https://github.com/trickle-labs/rockstream/actions/runs/12345678".to_string(),
        trigger_event: "push".to_string(),
        runner_environment: RunnerEnvironment {
            os: "linux".to_string(),
            arch: "x86_64".to_string(),
            cpu_cores: 16,
            memory_gb: 64.0,
        },
    };

    let mut artifacts = BTreeMap::new();
    artifacts.insert("rockstream-x86_64".to_string(), "a".repeat(64));

    let mut workloads = BTreeMap::new();
    workloads.insert("workload_a".to_string(), "b".repeat(64));

    let mut test_results = BTreeMap::new();
    test_results.insert(
        "unit_tests".to_string(),
        TestSuiteResult {
            total: 100,
            passed: 100,
            failed: 0,
            skipped: 0,
            mandatory_skipped: 0,
        },
    );

    let mut raw_metrics = BTreeMap::new();
    let samples = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0];
    raw_metrics.insert("recovery_latency_ms".to_string(), samples.clone());

    let mut summary_metrics = BTreeMap::new();
    let summary = SummaryMetric::calculate_from_raw(&samples, Some(5000.0)).unwrap();
    summary_metrics.insert("recovery_latency_ms".to_string(), summary);

    let mut targets = BTreeMap::new();
    targets.insert("recovery_latency_ms".to_string(), 5000.0);

    EvidenceManifest {
        candidate,
        workflow_run,
        artifacts,
        workloads,
        test_results,
        raw_metrics,
        summary_metrics,
        targets,
    }
}

#[test]
fn test_valid_evidence_manifest_passes_validation() {
    let manifest = create_valid_manifest();
    assert!(manifest.validate().is_ok());
}

#[test]
fn test_candidate_identity_mutation_fails_validation() {
    let mut manifest = create_valid_manifest();
    manifest.candidate.commit_sha = "".to_string();
    let res = manifest.validate();
    assert!(matches!(
        res,
        Err(EvidenceIntegrityError::InvalidCandidateIdentity(_))
    ));

    let mut manifest2 = create_valid_manifest();
    manifest2.candidate.lockfile_digest = "short".to_string();
    let res2 = manifest2.validate();
    assert!(matches!(
        res2,
        Err(EvidenceIntegrityError::InvalidCandidateIdentity(_))
    ));
}

#[test]
fn test_skipped_mandatory_tests_fails_validation() {
    let mut manifest = create_valid_manifest();
    manifest
        .test_results
        .get_mut("unit_tests")
        .unwrap()
        .mandatory_skipped = 1;
    let res = manifest.validate();
    assert!(matches!(
        res,
        Err(EvidenceIntegrityError::MandatoryTestsSkipped { .. })
    ));
}

#[test]
fn test_failed_tests_fails_validation() {
    let mut manifest = create_valid_manifest();
    manifest.test_results.get_mut("unit_tests").unwrap().failed = 2;
    let res = manifest.validate();
    assert!(matches!(
        res,
        Err(EvidenceIntegrityError::TestFailed { .. })
    ));
}

#[test]
fn test_invalid_artifact_digest_fails_validation() {
    let mut manifest = create_valid_manifest();
    manifest.artifacts.insert(
        "bad_artifact".to_string(),
        "not-a-valid-hex-sha256".to_string(),
    );
    let res = manifest.validate();
    assert!(matches!(
        res,
        Err(EvidenceIntegrityError::InvalidArtifactDigest { .. })
    ));
}

#[test]
fn test_missing_raw_data_fails_validation() {
    let mut manifest = create_valid_manifest();
    manifest.raw_metrics.clear();
    let res = manifest.validate();
    assert!(matches!(
        res,
        Err(EvidenceIntegrityError::MissingRawData { .. })
    ));
}

#[test]
fn test_summary_regeneration_mismatch_fails_validation() {
    let mut manifest = create_valid_manifest();
    // Tamper with p99
    manifest
        .summary_metrics
        .get_mut("recovery_latency_ms")
        .unwrap()
        .p99 += 5.0;
    let res = manifest.validate();
    assert!(matches!(
        res,
        Err(EvidenceIntegrityError::SummaryRegenerationMismatch { .. })
    ));

    let mut manifest2 = create_valid_manifest();
    // Tamper with mean
    manifest2
        .summary_metrics
        .get_mut("recovery_latency_ms")
        .unwrap()
        .mean = 1.0;
    let res2 = manifest2.validate();
    assert!(matches!(
        res2,
        Err(EvidenceIntegrityError::SummaryRegenerationMismatch { .. })
    ));
}

#[test]
fn test_target_cannot_satisfy_measured_fails_validation() {
    let mut manifest = create_valid_manifest();
    // Fabricate raw samples to contain only 1 sample matching target threshold
    let target_val = 5000.0;
    manifest
        .targets
        .insert("fake_metric".to_string(), target_val);
    manifest
        .raw_metrics
        .insert("fake_metric".to_string(), vec![target_val]);
    let fake_summary = SummaryMetric::calculate_from_raw(&[target_val], None).unwrap();
    manifest
        .summary_metrics
        .insert("fake_metric".to_string(), fake_summary);

    let res = manifest.validate();
    assert!(matches!(
        res,
        Err(EvidenceIntegrityError::TargetCannotSatisfyMeasured { .. })
    ));
}

#[test]
fn test_verify_files_on_disk() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_artifact.bin");
    let content = b"release candidate artifact bytes";
    std::fs::write(&file_path, content).unwrap();

    let digest = CandidateIdentity::compute_sha256_hex(content);

    let mut manifest = create_valid_manifest();
    manifest.artifacts.clear();
    manifest
        .artifacts
        .insert("test_artifact.bin".to_string(), digest.clone());

    assert!(manifest.verify_files_on_disk(dir.path()).is_ok());

    // Mutate file on disk
    std::fs::write(&file_path, b"tampered bytes").unwrap();
    let res = manifest.verify_files_on_disk(dir.path());
    assert!(matches!(
        res,
        Err(EvidenceIntegrityError::ArtifactDigestMismatch { .. })
    ));

    // Missing file on disk
    manifest
        .artifacts
        .insert("nonexistent.bin".to_string(), digest);
    let res_missing = manifest.verify_files_on_disk(dir.path());
    assert!(matches!(
        res_missing,
        Err(EvidenceIntegrityError::ArtifactFileNotFound(_))
    ));
}

#[test]
fn test_json_roundtrip() {
    let manifest = create_valid_manifest();
    let json_str = manifest.to_json().unwrap();
    let decoded = EvidenceManifest::from_json(&json_str).unwrap();
    assert_eq!(manifest, decoded);
}

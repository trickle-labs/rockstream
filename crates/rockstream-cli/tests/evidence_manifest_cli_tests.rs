//! CLI integration tests for evidence manifest validation.

use rockstream_cli::output::OutputFormat;
use rockstream_cli::run_manifest_validate;
use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::evidence_manifest::{
    EvidenceManifest, RunnerEnvironment, SummaryMetric, TestSuiteResult, WorkflowRunInfo,
};
use std::collections::BTreeMap;
use tempfile::tempdir;

fn make_manifest(artifact_file: &str, artifact_digest: &str) -> EvidenceManifest {
    let candidate = CandidateIdentity {
        semantic_version: "0.59.1".to_string(),
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
            cpu_cores: 8,
            memory_gb: 32.0,
        },
    };

    let mut artifacts = BTreeMap::new();
    artifacts.insert(artifact_file.to_string(), artifact_digest.to_string());

    let mut workloads = BTreeMap::new();
    workloads.insert("chaos_recovery".to_string(), "c".repeat(64));

    let mut test_results = BTreeMap::new();
    test_results.insert(
        "chaos_suite".to_string(),
        TestSuiteResult {
            total: 50,
            passed: 50,
            failed: 0,
            skipped: 0,
            mandatory_skipped: 0,
        },
    );

    let mut raw_metrics = BTreeMap::new();
    let samples = vec![100.0, 200.0, 300.0, 400.0, 500.0];
    raw_metrics.insert("failure_detection_ms".to_string(), samples.clone());

    let mut summary_metrics = BTreeMap::new();
    let summary = SummaryMetric::calculate_from_raw(&samples, Some(1000.0)).unwrap();
    summary_metrics.insert("failure_detection_ms".to_string(), summary);

    let mut targets = BTreeMap::new();
    targets.insert("failure_detection_ms".to_string(), 5000.0);

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
fn test_cli_manifest_validate_success() {
    let dir = tempdir().unwrap();
    let artifact_rel = "target/release/rockstream";
    let artifact_path = dir.path().join(artifact_rel);
    std::fs::create_dir_all(artifact_path.parent().unwrap()).unwrap();
    let artifact_bytes = b"rockstream binary artifact bytes";
    std::fs::write(&artifact_path, artifact_bytes).unwrap();

    let digest = CandidateIdentity::compute_sha256_hex(artifact_bytes);
    let manifest = make_manifest(artifact_rel, &digest);

    let manifest_path = dir.path().join("evidence-manifest.json");
    std::fs::write(&manifest_path, manifest.to_json().unwrap()).unwrap();

    // Text format
    let text_res =
        run_manifest_validate(OutputFormat::Text, &manifest_path, Some(dir.path())).unwrap();
    assert!(text_res.contains("OK: Evidence manifest is valid"));
    assert!(text_res.contains("0.59.1"));

    // JSON format
    let json_res =
        run_manifest_validate(OutputFormat::Json, &manifest_path, Some(dir.path())).unwrap();
    assert!(json_res.contains("\"status\": \"VALID\""));
    assert!(json_res.contains("\"candidate_version\": \"0.59.1\""));
}

#[test]
fn test_cli_manifest_validate_missing_file() {
    let dir = tempdir().unwrap();
    let missing_path = dir.path().join("nonexistent-manifest.json");
    let err = run_manifest_validate(OutputFormat::Text, &missing_path, None).unwrap_err();
    assert_eq!(err.code.to_string(), "RS-0003");
    assert!(err.message.contains("not found"));
}

#[test]
fn test_cli_manifest_validate_invalid_json() {
    let dir = tempdir().unwrap();
    let manifest_path = dir.path().join("bad-manifest.json");
    std::fs::write(&manifest_path, "{ not valid json }").unwrap();

    let err = run_manifest_validate(OutputFormat::Text, &manifest_path, None).unwrap_err();
    assert_eq!(err.code.to_string(), "RS-0002");
    assert!(err.message.contains("Invalid evidence manifest JSON"));
}

#[test]
fn test_cli_manifest_validate_integrity_failure() {
    let dir = tempdir().unwrap();
    let mut manifest = make_manifest("rockstream", &"a".repeat(64));
    manifest
        .test_results
        .get_mut("chaos_suite")
        .unwrap()
        .mandatory_skipped = 2;

    let manifest_path = dir.path().join("evidence-manifest.json");
    std::fs::write(&manifest_path, manifest.to_json().unwrap()).unwrap();

    let err = run_manifest_validate(OutputFormat::Text, &manifest_path, None).unwrap_err();
    assert_eq!(err.code.to_string(), "RS-0001");
    assert!(err.message.contains("skipped tests"));
}

#[test]
fn test_cli_manifest_validate_artifact_digest_mismatch() {
    let dir = tempdir().unwrap();
    let artifact_path = dir.path().join("rockstream");
    std::fs::write(&artifact_path, b"actual file bytes").unwrap();

    let manifest = make_manifest("rockstream", &"0".repeat(64)); // wrong digest
    let manifest_path = dir.path().join("evidence-manifest.json");
    std::fs::write(&manifest_path, manifest.to_json().unwrap()).unwrap();

    let err =
        run_manifest_validate(OutputFormat::Text, &manifest_path, Some(dir.path())).unwrap_err();
    assert_eq!(err.code.to_string(), "RS-0001");
    assert!(err.message.contains("digest mismatch"));
}

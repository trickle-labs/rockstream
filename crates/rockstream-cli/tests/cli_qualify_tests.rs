//! CLI `rockstream qualify` command integration and parity tests (v0.59.24 + issue #405 R1).

use rockstream_cli::output::OutputFormat;
use rockstream_cli::run_qualify;
use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::qualification::{
    MeasuredExecutionMetadata, QualificationEvidenceManifest, QualificationProfile,
    QualificationRun,
};

fn build_measured_manifest(candidate: CandidateIdentity) -> QualificationEvidenceManifest {
    let profile = QualificationProfile::reference_rc1();
    let runs: Vec<QualificationRun> = [1usize, 2, 4, 8]
        .into_iter()
        .map(|w| {
            let base = QualificationRun::sample_reference_run(w, candidate.clone());
            let pids: Vec<u32> = (0..w)
                .map(|i| 41000 + (w as u32 * 100) + i as u32)
                .collect();
            let meta = MeasuredExecutionMetadata {
                commit_sha: "a".repeat(40),
                binary_sha256: "b".repeat(64),
                hardware_spec: "c7i.4xlarge-16vcpu-32gb".to_string(),
                worker_pids: pids,
                offered_rate_rows_per_sec: base.aggregate_metrics.throughput_rows_per_sec,
                achieved_rate_rows_per_sec: base.aggregate_metrics.throughput_rows_per_sec,
                errors_and_timeouts: 0,
                faults_injected_and_verified: vec!["worker_kill".to_string()],
                negative_controls_passed: true,
                cost_accounting: None,
            };
            QualificationRun::measured(
                format!("measured_run_{w}w"),
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
        })
        .collect();
    let raw_chunks: Vec<String> = runs
        .iter()
        .flat_map(|r| r.raw_chunk_digests.clone())
        .collect();
    QualificationEvidenceManifest::seal(
        "manifest_measured_v072",
        candidate,
        profile,
        runs,
        raw_chunks,
        "2026-11-22T12:00:00Z",
    )
}

#[test]
fn cli_qualify_rejects_synthetic_reference_fixture_and_missing_manifest() {
    let err_none = run_qualify(OutputFormat::Json, false, None, None).unwrap_err();
    assert_eq!(err_none.code.value(), 3033);
    assert!(err_none.message.contains("synthetic reference fixtures"));

    let tmp = tempfile::tempdir().unwrap();
    let synthetic_path = tmp.path().join("synthetic-manifest.json");
    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();
    let synthetic = QualificationEvidenceManifest::reference_rc1_manifest(candidate);
    std::fs::write(&synthetic_path, synthetic.to_json().unwrap()).unwrap();

    let err_synth = run_qualify(
        OutputFormat::Json,
        false,
        Some(synthetic_path.to_str().unwrap()),
        None,
    )
    .unwrap_err();
    assert_eq!(err_synth.code.value(), 3033);
    assert!(err_synth.message.contains("Synthetic fixture rejected"));
}

#[test]
fn cli_qualify_runs_and_emits_structured_output_for_measured_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let input_file = tmp.path().join("measured-manifest.json");
    let out_file = tmp.path().join("qualification-manifest.json");

    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();
    let manifest = build_measured_manifest(candidate);
    std::fs::write(&input_file, manifest.to_json().unwrap()).unwrap();
    let input_str = input_file.to_str().unwrap();

    let text_output =
        run_qualify(OutputFormat::Text, false, Some(input_str), Some(&out_file)).unwrap();

    assert!(text_output.contains(&format!("OK: Qualification suite `{input_str}` passed")));
    assert!(text_output.contains("Manifest Seal:"));
    assert!(out_file.exists());

    let json_output = run_qualify(OutputFormat::Json, false, Some(input_str), None).unwrap();
    let json_val: serde_json::Value = serde_json::from_str(&json_output).unwrap();
    assert_eq!(json_val["status"], "PASSED");
    assert_eq!(json_val["candidate_version"], "1.0.0");
    assert_eq!(json_val["scenarios_passed"], 4);
}

#[test]
fn cli_qualify_verify_manifest_parity() {
    let tmp = tempfile::tempdir().unwrap();
    let input_file = tmp.path().join("in.json");
    let out_file = tmp.path().join("out.json");

    let mut candidate = CandidateIdentity::current();
    candidate.semantic_version = "1.0.0".to_string();
    let manifest = build_measured_manifest(candidate);
    std::fs::write(&input_file, manifest.to_json().unwrap()).unwrap();

    let _ = run_qualify(
        OutputFormat::Json,
        false,
        Some(input_file.to_str().unwrap()),
        Some(&out_file),
    )
    .unwrap();
    let file_bytes = std::fs::read_to_string(&out_file).unwrap();
    assert!(file_bytes.contains("manifest_seal"));
    assert!(file_bytes.contains("measured_execution"));
}

//! CLI `rockstream qualify` command integration and parity tests (v0.59.24 Slice 7 / Phase 3b).

use rockstream_cli::output::OutputFormat;
use rockstream_cli::run_qualify;

#[test]
fn cli_qualify_runs_and_emits_structured_output() {
    let tmp = tempfile::tempdir().unwrap();
    let out_file = tmp.path().join("qualification-manifest.json");

    let text_output = run_qualify(
        OutputFormat::Text,
        false,
        Some("reference-rc1"),
        Some(&out_file),
    )
    .unwrap();

    assert!(text_output.contains("OK: Qualification suite `reference-rc1` passed"));
    assert!(text_output.contains("Manifest Seal:"));
    assert!(out_file.exists());

    let json_output = run_qualify(OutputFormat::Json, false, None, None).unwrap();
    let json_val: serde_json::Value = serde_json::from_str(&json_output).unwrap();
    assert_eq!(json_val["status"], "PASSED");
    assert_eq!(json_val["candidate_version"], "1.0.0");
    assert_eq!(json_val["scenarios_passed"], 4);
}

#[test]
fn cli_qualify_verify_manifest_parity() {
    let tmp = tempfile::tempdir().unwrap();
    let out_file = tmp.path().join("out.json");

    let _ = run_qualify(OutputFormat::Json, false, None, Some(&out_file)).unwrap();
    let file_bytes = std::fs::read_to_string(&out_file).unwrap();
    assert!(file_bytes.contains("manifest_seal"));
    assert!(file_bytes.contains("1.0.0-rc.1"));
}

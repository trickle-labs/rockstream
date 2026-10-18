//! Validator regressions only. Fixtures are not capacity measurements.
use r1_local_harness::negative::qualify_evidence;
use sha2::{Digest, Sha256};
use std::fs;

#[test]
fn v072_frozen_profile_matches_issued_revision_byte_for_byte() {
    let bytes = include_bytes!("../../../benchmarks/r1-local/profile-v072.toml");
    assert_eq!(
        hex::encode(Sha256::digest(bytes)),
        "d5cf97b3d0a181cddd78dbf811e22224c4767bd1948143b45beb38390464719a"
    );
    let profile: toml::Value = toml::from_str(std::str::from_utf8(bytes).unwrap()).unwrap();
    assert_eq!(profile["profile_id"].as_str(), Some("v072-m5pro-48gb-v1"));
    assert_eq!(profile["revision"].as_integer(), Some(1));
    assert_eq!(profile["state"].as_str(), Some("frozen"));
    assert_eq!(profile["contract"].as_str(), Some("issue-132-v2"));
}

#[test]
fn v072_empty_evidence_directory_is_not_qualified() {
    let directory = tempfile::tempdir().unwrap();
    assert_eq!(
        qualify_evidence(directory.path()).unwrap_err().to_string(),
        "missing required evidence file: matrix-report.json"
    );
}

#[test]
fn v072_matrix_only_bundle_is_not_qualified() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(directory.path().join("matrix-report.json"), "{}").unwrap();
    assert_eq!(
        qualify_evidence(directory.path()).unwrap_err().to_string(),
        "missing required evidence file: raw-samples.jsonl"
    );
}

#[test]
fn v072_samples_only_bundle_is_not_qualified() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(directory.path().join("raw-samples.jsonl"), "").unwrap();
    assert_eq!(
        qualify_evidence(directory.path()).unwrap_err().to_string(),
        "missing required evidence file: matrix-report.json"
    );
}

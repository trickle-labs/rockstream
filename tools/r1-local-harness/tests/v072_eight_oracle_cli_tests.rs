//! Structural oracle CLI fixtures; these do not qualify any release workload.
use r1_local_harness::corpus::{Change, Corpus, SourceRow};
use r1_local_harness::oracle::complete_output;
use r1_local_harness::v072_oracles::{WorkloadKind, FROZEN_PROFILE_SHA256};
use serde_json::json;
use std::fs;
use std::path::Path;
use std::process::Command;

fn fixture(root: &Path) -> std::path::PathBuf {
    let row = |id, group_id, dimension_id, value, active| SourceRow {
        id,
        group_id,
        dimension_id,
        value,
        active,
    };
    let before = row(0, 1, 0, 10, true);
    let corpus = Corpus {
        source: vec![
            before.clone(),
            row(1, 1, 1, 20, true),
            row(2, 2, 0, 5, false),
        ],
        dimension: vec![(0, 7), (1, 8)],
        changes: vec![
            Change::Update {
                before,
                after: row(0, 1, 0, 15, true),
            },
            Change::Delete {
                before: row(1, 1, 1, 20, true),
            },
            Change::Insert {
                after: row(3, 2, 1, 4, true),
            },
        ],
    };
    fs::write(
        root.join("input.json"),
        serde_json::to_vec(&corpus).unwrap(),
    )
    .unwrap();
    let mut runs = Vec::new();
    for (i, workload) in WorkloadKind::ALL.iter().enumerate() {
        let result = complete_output(&corpus, workload.sqlite_query()).unwrap();
        let filename = format!("result-{i}.json");
        fs::write(root.join(&filename), serde_json::to_vec(&result).unwrap()).unwrap();
        runs.push(json!({
            "workload": serde_json::to_value(workload).unwrap(),
            "input_path": "input.json",
            "observed_rows_path": filename,
            "reported_transport": serde_json::to_value(workload.expected_transport()).unwrap(),
            "committed_epoch": 4,
            "observed_epoch": 4
        }));
    }
    let manifest = root.join("manifest.json");
    fs::write(
        &manifest,
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "profile_sha256": FROZEN_PROFILE_SHA256,
            "runs": runs
        }))
        .unwrap(),
    )
    .unwrap();
    manifest
}

fn cmd(manifest: &Path, output: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_verify-v072-oracles"))
        .arg("--profile")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../benchmarks/r1-local/profile-v072.toml"
        ))
        .arg("--manifest")
        .arg(manifest)
        .arg("--output")
        .arg(output)
        .output()
        .unwrap()
}

#[test]
fn complete_eight_cli_emits_explicitly_unqualified_exact_oracles() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = fixture(dir.path());
    let output = dir.path().join("oracle-summary.json");
    let result = cmd(&manifest, &output);
    assert!(
        result.status.success(),
        "{:?}",
        String::from_utf8_lossy(&result.stderr)
    );
    let body: serde_json::Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
    assert_eq!(
        body["status"],
        "independent_multiset_oracle_only_not_capacity_qualified"
    );
    assert_eq!(body["complete_results"].as_object().unwrap().len(), 8);
    assert!(String::from_utf8_lossy(&result.stdout).contains("not_capacity_qualified"));
}

#[test]
fn corrupt_observed_multiset_has_nonzero_exit_and_no_success_artifact() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = fixture(dir.path());
    fs::write(dir.path().join("result-4.json"), b"[]").unwrap();
    let output = dir.path().join("oracle-summary.json");
    let result = cmd(&manifest, &output);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("complete-result oracle mismatch"));
    assert!(!output.exists());
}

#[test]
fn existing_report_and_evidence_files_cannot_be_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = fixture(dir.path());
    let first_report = dir.path().join("oracle-summary.json");
    assert!(cmd(&manifest, &first_report).status.success());
    let original = fs::read(&first_report).unwrap();
    let second = cmd(&manifest, &first_report);
    assert!(!second.status.success());
    assert!(String::from_utf8_lossy(&second.stderr).contains("report output already exists"));
    assert_eq!(fs::read(first_report).unwrap(), original);

    let manifest_before = fs::read(&manifest).unwrap();
    assert!(!cmd(&manifest, &manifest).status.success());
    assert_eq!(fs::read(manifest).unwrap(), manifest_before);
}

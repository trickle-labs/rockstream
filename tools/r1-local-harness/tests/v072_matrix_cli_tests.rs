//! Real command execution with inventory fixtures, not benchmark measurements.
use r1_local_harness::matrix::default_matrix_cells;
use serde_json::json;
use std::fs;
use std::process::Command;

#[test]
fn v072_matrix_command_never_fabricates_a_measured_report() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_r1-local-harness"))
        .arg("matrix")
        .arg("--output")
        .arg(directory.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stderr, b"");
    assert_eq!(
        output.stdout,
        b"Matrix plan written; no benchmark measurements were executed.\n"
    );
    let plan: serde_json::Value =
        serde_json::from_slice(&fs::read(directory.path().join("matrix-plan.json")).unwrap())
            .unwrap();
    assert_eq!(
        plan,
        json!({
            "schema_version": 1,
            "kind": "matrix_plan",
            "measurements_executed": false,
            "cells": default_matrix_cells(),
        })
    );
    let mut files = fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    files.sort();
    assert_eq!(files, vec![std::ffi::OsString::from("matrix-plan.json")]);
}

#[test]
fn v072_matrix_plan_does_not_overwrite_or_reuse_existing_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let report = directory.path().join("matrix-report.json");
    fs::write(&report, "retained evidence\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_r1-local-harness"))
        .arg("matrix")
        .arg("--output")
        .arg(directory.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(output.stdout, b"");
    assert_eq!(
        output.stderr,
        b"VIOLATION: refusing to mix an inventory plan with an existing matrix-report.json\n"
    );
    assert_eq!(fs::read_to_string(report).unwrap(), "retained evidence\n");
    assert!(!directory.path().join("matrix-plan.json").exists());
}

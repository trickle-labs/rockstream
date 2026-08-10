//! CI Operator State Accounting Invariant Gate Integration Test (v0.51.23 - Slice 4).
//!
//! Validates `scripts/check-operator-state-bytes.sh` against real tree and proves
//! that injecting an un-accounted arrangement field fails the gate.

use std::path::PathBuf;
use std::process::Command;

fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn test_operator_state_accounting_gate_passes_on_repo() {
    let root = workspace_root();
    let script = root.join("scripts/check-operator-state-bytes.sh");
    let output = Command::new("bash")
        .arg(&script)
        .arg(&root)
        .current_dir(&root)
        .output()
        .expect("Failed to execute check-operator-state-bytes.sh");

    assert!(
        output.status.success(),
        "check-operator-state-bytes.sh must succeed on repo tree: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_operator_state_accounting_gate_self_test() {
    let root = workspace_root();
    let script = root.join("scripts/check-operator-state-bytes.test.sh");
    let output = Command::new("bash")
        .arg(&script)
        .current_dir(&root)
        .output()
        .expect("Failed to execute check-operator-state-bytes.test.sh");

    assert!(
        output.status.success(),
        "check-operator-state-bytes.test.sh must succeed: stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

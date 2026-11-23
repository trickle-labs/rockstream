#[test]
fn runbook_marks_current_checkpoint_recovery_unavailable() {
    let runbook = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/disaster-recovery.md"),
    )
    .unwrap();
    assert!(runbook
        .contains("The v0.73 CLI does not currently expose authenticated checkpoint inspection"));
    assert!(runbook.contains("The binary now fails closed with"));
    assert!(runbook.contains(
        "An authenticated management operation is required before this runbook can be used for v0.73."
    ));
    assert!(!runbook.contains("--identity-role admin checkpoint"));
    assert!(runbook.contains("Measured RPO: 0 committed checkpoints."));
    assert!(runbook.contains(
        "Measured RTO: 0.42 seconds from restore invocation to published bootstrap pointer."
    ));
}

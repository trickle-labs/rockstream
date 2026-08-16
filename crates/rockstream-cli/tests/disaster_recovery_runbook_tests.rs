use std::process::Command;

use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint};

#[test]
fn runbook_only_drill_records_measured_rpo_and_rto() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let export = root.path().join("export");
    let target = root.path().join("target");
    std::fs::create_dir_all(source.join("control/checkpoints")).unwrap();
    let checkpoint = serde_json::to_vec(&ClusterCheckpoint::new(CheckpointId(56))).unwrap();
    std::fs::write(source.join("control/checkpoints/56"), &checkpoint).unwrap();
    let checkpoint_bytes = checkpoint.len();
    let runbook = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/disaster-recovery.md"),
    )
    .unwrap();
    assert!(runbook.contains(
        "rockstream --storage-dir \"$ROCKSTREAM_STORAGE\" --identity-role admin checkpoint export --destination \"$DR_EXPORT_URL\""
    ));
    assert!(runbook.contains(
        "rockstream --storage-dir \"$ROCKSTREAM_AUDIT_DIR\" --identity-role admin checkpoint restore --source \"$DR_EXPORT_URL\" --storage \"$FRESH_STORAGE_URL\" --yes"
    ));
    assert!(runbook.contains("Measured RPO: 0 committed checkpoints."));
    assert!(runbook.contains(
        "Measured RTO: 0.42 seconds from restore invocation to published bootstrap pointer."
    ));

    let binary = env!("CARGO_BIN_EXE_rockstream");
    let export_output = Command::new(binary)
        .args([
            "--storage-dir",
            source.to_str().unwrap(),
            "--identity-role",
            "admin",
            "checkpoint",
            "export",
            "--destination",
            export.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    let restore_output = Command::new(binary)
        .args([
            "--storage-dir",
            source.to_str().unwrap(),
            "--identity-role",
            "admin",
            "checkpoint",
            "restore",
            "--source",
            export.to_str().unwrap(),
            "--storage",
            target.to_str().unwrap(),
            "--yes",
        ])
        .output()
        .unwrap();
    assert!(export_output.status.success());
    assert!(restore_output.status.success());
    assert_eq!(
        (
            String::from_utf8(export_output.stdout).unwrap(),
            String::from_utf8(restore_output.stdout).unwrap(),
            target.join("control/bootstrap/active-generation").is_file(),
        ),
        (
            format!(
                "Checkpoint 56: exported from {} to {} (objects: 1, bytes: {checkpoint_bytes}, status: SUCCESS)\n",
                source.display(),
                export.display()
            ),
            format!(
                "Checkpoint 56: restored from {} to {} (objects: 1, bytes: {checkpoint_bytes}, shards: 0, status: SUCCESS)\n",
                export.display(),
                target.display()
            ),
            true
        )
    );
}

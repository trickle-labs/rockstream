use std::process::Command;

use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint};

fn normalize_correlation_id(stderr: Vec<u8>) -> String {
    let stderr = String::from_utf8(stderr).unwrap();
    let marker = "(correlation_id=";
    let start = stderr.find(marker).unwrap() + marker.len();
    let end = start + stderr[start..].find(' ').unwrap();
    uuid::Uuid::parse_str(&stderr[start..end]).unwrap();
    stderr.replacen(&stderr[start..end], "<correlation_id>", 1)
}

fn seed_checkpoint(path: &std::path::Path, checkpoint_id: u64) -> u64 {
    let bytes = serde_json::to_vec(&ClusterCheckpoint::new(CheckpointId(checkpoint_id))).unwrap();
    let dir = path.join("control/checkpoints");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(checkpoint_id.to_string()), &bytes).unwrap();
    bytes.len() as u64
}

#[test]
fn checkpoint_export_restore_cli_outputs_and_audits_exactly() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let export = root.path().join("export");
    let target = root.path().join("target");
    std::fs::create_dir_all(&source).unwrap();
    let bytes = seed_checkpoint(&source, 56);
    let binary = env!("CARGO_BIN_EXE_rockstream");

    let export_result = Command::new(binary)
        .args([
            "--json",
            "--storage-dir",
            source.to_str().unwrap(),
            "--identity-user",
            "dr-admin",
            "--identity-role",
            "admin",
            "checkpoint",
            "export",
            "--destination",
            export.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(export_result.status.success());
    assert_eq!(
        String::from_utf8(export_result.stdout).unwrap(),
        format!(
            "{{\n  \"checkpoint_id\": 56,\n  \"source\": \"{}\",\n  \"destination\": \"{}\",\n  \"object_count\": 1,\n  \"byte_count\": {bytes},\n  \"status\": \"SUCCESS\"\n}}\n",
            source.display(),
            export.display()
        )
    );
    assert_eq!(String::from_utf8(export_result.stderr).unwrap(), "");

    let restore_result = Command::new(binary)
        .args([
            "--storage-dir",
            source.to_str().unwrap(),
            "--identity-user",
            "dr-admin",
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
    assert!(restore_result.status.success());
    assert_eq!(
        String::from_utf8(restore_result.stdout).unwrap(),
        format!(
            "Checkpoint 56: restored from {} to {} (objects: 1, bytes: {bytes}, shards: 0, status: SUCCESS)\n",
            export.display(),
            target.display()
        )
    );
    assert_eq!(String::from_utf8(restore_result.stderr).unwrap(), "");

    let denied = Command::new(binary)
        .args([
            "--storage-dir",
            source.to_str().unwrap(),
            "--identity-user",
            "dr-viewer",
            "--identity-role",
            "viewer",
            "checkpoint",
            "export",
            "--destination",
            export.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!denied.status.success());
    assert_eq!(String::from_utf8(denied.stdout).unwrap(), "");
    assert_eq!(
        normalize_correlation_id(denied.stderr),
        "[RS-2401] auth.permission_denied: Permission denied: authenticated principal lacks required RBAC role (detail=permission denied: principal 'dr-viewer' lacks required role Admin) (correlation_id=<correlation_id> context=detail=permission denied: principal 'dr-viewer' lacks required role Admin) next_steps: Request elevated RBAC role from an admin or contact the namespace owner\n  next steps: Request elevated RBAC role (Admin) or run under an authorized principal.\n"
    );

    let audit_lines: Vec<rockstream_types::audit::AuditEvent> =
        std::fs::read_to_string(source.join("audit.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    assert_eq!(audit_lines.len(), 3);
    assert_eq!(
        (
            audit_lines[0].actor.as_str(),
            audit_lines[0].action.as_str(),
            audit_lines[0].resource.as_str(),
            audit_lines[0].detail.as_deref(),
            audit_lines[0].error_code.as_deref(),
        ),
        (
            "dr-admin",
            "checkpoint.export",
            "56",
            Some(
                format!(
                    "source={} destination={} objects=1 bytes={bytes} status=SUCCESS",
                    source.display(),
                    export.display()
                )
                .as_str()
            ),
            None
        )
    );
    assert_eq!(audit_lines[1].actor, "dr-admin");
    assert_eq!(audit_lines[1].action, "checkpoint.restore");
    assert_eq!(audit_lines[1].resource, "56");
    assert_eq!(
        audit_lines[1].detail.as_deref(),
        Some(
            format!(
                "source={} target={} objects=1 bytes={bytes} status=SUCCESS",
                export.display(),
                target.display()
            )
            .as_str()
        )
    );
    assert_eq!(audit_lines[1].error_code, None);
    assert_eq!(audit_lines[2].actor, "dr-viewer");
    assert_eq!(audit_lines[2].action, "checkpoint.export");
    assert_eq!(audit_lines[2].resource, export.to_string_lossy());
    assert_eq!(audit_lines[2].detail.as_deref(), Some("unauthorized role"));
    assert_eq!(audit_lines[2].error_code.as_deref(), Some("RS-2401"));
}

#[test]
fn checkpoint_restore_rejects_missing_export_with_rs5035() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("missing-export");
    let target = root.path().join("target");
    let result = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .args([
            "--storage-dir",
            root.path().to_str().unwrap(),
            "--identity-role",
            "admin",
            "checkpoint",
            "restore",
            "--source",
            source.to_str().unwrap(),
            "--storage",
            target.to_str().unwrap(),
            "--yes",
        ])
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert_eq!(String::from_utf8(result.stdout).unwrap(), "");
    assert_eq!(
        normalize_correlation_id(result.stderr),
        "[RS-5035] skew.slo_cannot_be_met: Skew-bound SLO cannot be met without composable partial-state splitting (detail=checkpoint export integrity validation failed: no committed export generation exists; next_steps: discard the incomplete generation and retry from the same committed checkpoint) (correlation_id=<correlation_id> context=detail=checkpoint export integrity validation failed: no committed export generation exists; next_steps: discard the incomplete generation and retry from the same committed checkpoint) next_steps: Add composable partial-state semantics for this operator, reduce the hot key's skew at the source, or route the workload to a spill-shard plan that can tolerate the SLO miss.\n  next steps: Verify the committed export, object-store access, and target freshness, then retry.\n"
    );
}

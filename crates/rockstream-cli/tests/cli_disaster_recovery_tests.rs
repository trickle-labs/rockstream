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
fn checkpoint_cli_does_not_treat_identity_role_as_authorization() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let export = root.path().join("export");
    let target = root.path().join("target");
    std::fs::create_dir_all(&source).unwrap();
    seed_checkpoint(&source, 56);
    let binary = env!("CARGO_BIN_EXE_rockstream");

    let cases = [
        ("listing", vec!["checkpoint", "list"]),
        ("inspection", vec!["checkpoint", "show", "56"]),
        (
            "export",
            vec![
                "checkpoint",
                "export",
                "--destination",
                export.to_str().unwrap(),
            ],
        ),
        (
            "restore",
            vec![
                "checkpoint",
                "restore",
                "--source",
                export.to_str().unwrap(),
                "--storage",
                target.to_str().unwrap(),
                "--yes",
            ],
        ),
        ("listing", vec!["admin", "checkpoint", "list"]),
    ];
    for (operation, tail) in cases {
        let output = Command::new(binary)
            .args([
                "--storage-dir",
                source.to_str().unwrap(),
                "--identity-user",
                "dr-admin",
                "--identity-role",
                "admin",
            ])
            .args(tail)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert_eq!(String::from_utf8(output.stdout).unwrap(), "");
        assert_eq!(
            normalize_correlation_id(output.stderr),
            format!(
                "[RS-2400] auth.unauthenticated: Unauthenticated: request missing or carrying invalid credentials (detail=authenticated checkpoint {operation} is unavailable from the management API) (correlation_id=<correlation_id> context=detail=authenticated checkpoint {operation} is unavailable from the management API) next_steps: Provide valid credentials (Bearer token or mTLS certificate)\n  next steps: Do not use --identity-role as authorization; use an authenticated checkpoint endpoint when one is available.\n"
            )
        );
    }
    assert!(source.join("control/checkpoints/56").is_file());
    assert!(!source.join("audit.jsonl").exists());
    assert!(!export.exists());
    assert!(!target.exists());
}

#[test]
fn local_privileged_cli_routes_fail_closed_without_authenticated_endpoints() {
    let root = tempfile::tempdir().unwrap();
    let storage = root.path().join("storage");
    std::fs::create_dir_all(&storage).unwrap();
    let backup = root.path().join("backup");
    let restore_source = root.path().join("restore-source");
    let restore_target = root.path().join("restore-target");
    let bundle = root.path().join("support.json");
    let diagnosis = root.path().join("diagnosis.json");
    let cases = [
        (
            "local backup creation",
            "Configure an authenticated --management endpoint; do not use --identity-role as authorization.",
            vec!["admin", "backup", "create", backup.to_str().unwrap()],
        ),
        (
            "backup restore",
            "The CLI has no authenticated backup-restore endpoint; do not use --identity-role as authorization.",
            vec![
                "admin",
                "restore",
                restore_source.to_str().unwrap(),
                "--target",
                restore_target.to_str().unwrap(),
                "--yes",
            ],
        ),
        (
            "local support bundle and diagnosis",
            "Use support artifact create/download through an authenticated management connection; do not use --identity-role as authorization.",
            vec!["support", "bundle", "--out", bundle.to_str().unwrap()],
        ),
        (
            "local support bundle and diagnosis",
            "Use support artifact create/download through an authenticated management connection; do not use --identity-role as authorization.",
            vec![
                "support",
                "diagnose",
                "--code",
                "RS-0001",
                "--out",
                diagnosis.to_str().unwrap(),
            ],
        ),
        (
            "local audit history",
            "Use an authenticated pgwire client with SHOW SECURITY AUDIT; local audit output is not scoped to a durable principal.",
            vec!["audit", "tail"],
        ),
        (
            "local audit history",
            "Use an authenticated pgwire client with SHOW SECURITY AUDIT; local audit output is not scoped to a durable principal.",
            vec!["audit", "query", "--filter", "admin"],
        ),
    ];
    let binary = env!("CARGO_BIN_EXE_rockstream");
    for (operation, next_steps, tail) in cases {
        let detail = format!("{operation} is unavailable without an authenticated endpoint");
        let output = Command::new(binary)
            .args([
                "--storage-dir",
                storage.to_str().unwrap(),
                "--identity-user",
                "dr-admin",
                "--identity-role",
                "admin",
            ])
            .args(tail)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert_eq!(String::from_utf8(output.stdout).unwrap(), "");
        assert_eq!(
            normalize_correlation_id(output.stderr),
            format!(
                "[RS-2400] auth.unauthenticated: Unauthenticated: request missing or carrying invalid credentials (detail={detail}) (correlation_id=<correlation_id> context=detail={detail}) next_steps: Provide valid credentials (Bearer token or mTLS certificate)\n  next steps: {next_steps}\n"
            )
        );
    }
    assert!(!backup.exists());
    assert!(!restore_target.exists());
    assert!(!bundle.exists());
    assert!(!diagnosis.exists());
    assert!(!storage.join("audit.jsonl").exists());
}

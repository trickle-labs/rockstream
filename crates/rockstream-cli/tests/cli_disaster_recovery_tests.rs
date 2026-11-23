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

use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use tempfile::tempdir;

use rockstream_cli::transport::StorageClient;

fn normalize_correlation_id(stderr: Vec<u8>) -> String {
    let stderr = String::from_utf8(stderr).unwrap();
    let marker = "(correlation_id=";
    let start = stderr.find(marker).unwrap() + marker.len();
    let end = start + stderr[start..].find(' ').unwrap();
    uuid::Uuid::parse_str(&stderr[start..end]).unwrap();
    stderr.replacen(&stderr[start..end], "<correlation_id>", 1)
}

fn create_backup_fixture(storage_dir: &Path, backup_dir: &Path) {
    StorageClient::new()
        .create_backup(storage_dir, backup_dir.to_str().unwrap())
        .expect("create backup fixture through the storage adapter");
}

fn run_restore(source: &Path, target: &Path, confirm_overwrite: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockstream"));
    command.args([
        "--identity-user",
        "admin-user",
        "--identity-role",
        "admin",
        "admin",
        "restore",
        source.to_str().unwrap(),
        "--target",
        target.to_str().unwrap(),
    ]);
    if confirm_overwrite {
        command.arg("--yes");
    }
    command.output().unwrap()
}

fn assert_restore_unavailable(output: Output) {
    assert!(!output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "");
    assert_eq!(
        normalize_correlation_id(output.stderr),
        "[RS-2400] auth.unauthenticated: Unauthenticated: request missing or carrying invalid credentials (detail=backup restore is unavailable without an authenticated endpoint) (correlation_id=<correlation_id> context=detail=backup restore is unavailable without an authenticated endpoint) next_steps: Provide valid credentials (Bearer token or mTLS certificate)\n  next steps: The CLI has no authenticated backup-restore endpoint; do not use --identity-role as authorization.\n"
    );
}

#[test]
fn test_restore_empty_destination_fails_closed() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let backup_dir = root.path().join("backup");
    let target_dir = root.path().join("target");

    fs::create_dir_all(storage_dir.join("data")).unwrap();
    fs::write(storage_dir.join("data/records.bin"), b"persisted-records").unwrap();

    create_backup_fixture(&storage_dir, &backup_dir);

    assert_restore_unavailable(run_restore(&backup_dir, &target_dir, false));
    assert!(!target_dir.exists());
}

#[test]
fn test_restore_refusal_unconfirmed_destination() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let backup_dir = root.path().join("backup");
    let target_dir = root.path().join("target");

    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("sample.txt"), b"sample").unwrap();

    create_backup_fixture(&storage_dir, &backup_dir);

    // Create occupied target directory
    fs::create_dir_all(&target_dir).unwrap();
    fs::write(target_dir.join("precious_file.txt"), b"do-not-overwrite").unwrap();

    // Confirmation does not authorize this local restore route.
    assert_restore_unavailable(run_restore(&backup_dir, &target_dir, false));

    // Ensure precious file is intact
    assert_eq!(
        fs::read(target_dir.join("precious_file.txt")).unwrap(),
        b"do-not-overwrite"
    );
}

#[test]
fn test_restore_confirmed_overwrite_fails_closed() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let backup_dir = root.path().join("backup");
    let target_dir = root.path().join("target");

    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("restored.txt"), b"restored-content").unwrap();

    create_backup_fixture(&storage_dir, &backup_dir);

    // Target directory exists with prior content
    fs::create_dir_all(&target_dir).unwrap();
    fs::write(target_dir.join("old_stale.txt"), b"stale").unwrap();

    assert_restore_unavailable(run_restore(&backup_dir, &target_dir, true));
    assert!(!target_dir.join("restored.txt").exists());
    assert_eq!(
        fs::read(target_dir.join("old_stale.txt")).unwrap(),
        b"stale"
    );
}

#[test]
fn test_restore_invalid_source_leaves_target_intact() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let backup_dir = root.path().join("backup");
    let target_dir = root.path().join("target");

    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("valid.bin"), b"valid-bytes").unwrap();

    create_backup_fixture(&storage_dir, &backup_dir);

    // Occupy target
    fs::create_dir_all(&target_dir).unwrap();
    fs::write(target_dir.join("sacred_data.txt"), b"sacred").unwrap();

    // Tamper source backup payload; the CLI must reject before reading it.
    fs::write(backup_dir.join("valid.bin"), b"corrupted-tampered").unwrap();

    // Restore authorization fails before validation and leaves the target intact.
    assert_restore_unavailable(run_restore(&backup_dir, &target_dir, true));

    // Target must be completely unchanged
    assert_eq!(
        fs::read(target_dir.join("sacred_data.txt")).unwrap(),
        b"sacred"
    );
    assert!(!target_dir.join("valid.bin").exists());
}

#[test]
fn test_admin_restore_destination_guards_and_rollback() {
    test_restore_refusal_unconfirmed_destination();
    test_restore_invalid_source_leaves_target_intact();
}

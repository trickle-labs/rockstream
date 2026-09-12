use std::fs;
use std::process::Command;
use tempfile::tempdir;

#[test]
fn test_restore_empty_destination_success() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let backup_dir = root.path().join("backup");
    let target_dir = root.path().join("target");

    fs::create_dir_all(storage_dir.join("data")).unwrap();
    fs::write(storage_dir.join("data/records.bin"), b"persisted-records").unwrap();

    let binary = env!("CARGO_BIN_EXE_rockstream");
    let create_out = Command::new(binary)
        .args([
            "--storage-dir",
            storage_dir.to_str().unwrap(),
            "--identity-user",
            "admin-user",
            "--identity-role",
            "admin",
            "admin",
            "backup",
            "create",
            backup_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(create_out.status.success());

    // Restore into empty target directory (no --yes needed for empty target)
    let restore_out = Command::new(binary)
        .args([
            "--identity-user",
            "admin-user",
            "--identity-role",
            "admin",
            "admin",
            "restore",
            backup_dir.to_str().unwrap(),
            "--target",
            target_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(
        restore_out.status.success(),
        "restore failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&restore_out.stdout),
        String::from_utf8_lossy(&restore_out.stderr)
    );

    let stdout = String::from_utf8_lossy(&restore_out.stdout);
    assert!(stdout.contains("status: SUCCESS"));
    assert!(target_dir.join("data/records.bin").exists());
    assert_eq!(
        fs::read(target_dir.join("data/records.bin")).unwrap(),
        b"persisted-records"
    );
}

#[test]
fn test_restore_refusal_unconfirmed_destination() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let backup_dir = root.path().join("backup");
    let target_dir = root.path().join("target");

    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("sample.txt"), b"sample").unwrap();

    let binary = env!("CARGO_BIN_EXE_rockstream");
    let create_out = Command::new(binary)
        .args([
            "--storage-dir",
            storage_dir.to_str().unwrap(),
            "--identity-user",
            "admin-user",
            "--identity-role",
            "admin",
            "admin",
            "backup",
            "create",
            backup_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(create_out.status.success());

    // Create occupied target directory
    fs::create_dir_all(&target_dir).unwrap();
    fs::write(target_dir.join("precious_file.txt"), b"do-not-overwrite").unwrap();

    // Restore without --yes should fail with RS-0005
    let restore_out = Command::new(binary)
        .args([
            "--identity-user",
            "admin-user",
            "--identity-role",
            "admin",
            "admin",
            "restore",
            backup_dir.to_str().unwrap(),
            "--target",
            target_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!restore_out.status.success());
    let stderr = String::from_utf8_lossy(&restore_out.stderr);
    assert!(stderr.contains("RS-0005"));

    // Ensure precious file is intact
    assert_eq!(
        fs::read(target_dir.join("precious_file.txt")).unwrap(),
        b"do-not-overwrite"
    );
}

#[test]
fn test_restore_confirmed_overwrite_success() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let backup_dir = root.path().join("backup");
    let target_dir = root.path().join("target");

    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("restored.txt"), b"restored-content").unwrap();

    let binary = env!("CARGO_BIN_EXE_rockstream");
    let create_out = Command::new(binary)
        .args([
            "--storage-dir",
            storage_dir.to_str().unwrap(),
            "--identity-user",
            "admin-user",
            "--identity-role",
            "admin",
            "admin",
            "backup",
            "create",
            backup_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(create_out.status.success());

    // Target directory exists with prior content
    fs::create_dir_all(&target_dir).unwrap();
    fs::write(target_dir.join("old_stale.txt"), b"stale").unwrap();

    // Restore WITH --yes should succeed
    let restore_out = Command::new(binary)
        .args([
            "--identity-user",
            "admin-user",
            "--identity-role",
            "admin",
            "admin",
            "restore",
            backup_dir.to_str().unwrap(),
            "--target",
            target_dir.to_str().unwrap(),
            "--yes",
        ])
        .output()
        .unwrap();

    assert!(restore_out.status.success());
    assert!(target_dir.join("restored.txt").exists());
    assert_eq!(
        fs::read(target_dir.join("restored.txt")).unwrap(),
        b"restored-content"
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

    let binary = env!("CARGO_BIN_EXE_rockstream");
    let create_out = Command::new(binary)
        .args([
            "--storage-dir",
            storage_dir.to_str().unwrap(),
            "--identity-user",
            "admin-user",
            "--identity-role",
            "admin",
            "admin",
            "backup",
            "create",
            backup_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(create_out.status.success());

    // Occupy target
    fs::create_dir_all(&target_dir).unwrap();
    fs::write(target_dir.join("sacred_data.txt"), b"sacred").unwrap();

    // Tamper source backup payload
    fs::write(backup_dir.join("valid.bin"), b"corrupted-tampered").unwrap();

    // Restore should fail pre-validation and refuse to touch target
    let restore_out = Command::new(binary)
        .args([
            "--identity-user",
            "admin-user",
            "--identity-role",
            "admin",
            "admin",
            "restore",
            backup_dir.to_str().unwrap(),
            "--target",
            target_dir.to_str().unwrap(),
            "--yes",
        ])
        .output()
        .unwrap();

    assert!(!restore_out.status.success());
    let stderr = String::from_utf8_lossy(&restore_out.stderr);
    assert!(stderr.contains("RS-3616"));

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

use std::fs;
use std::process::Command;
use tempfile::tempdir;

use rockstream_control::{BackupManifest, BACKUP_MANIFEST_FILENAME};

#[test]
fn test_backup_create_empty_destination() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let dest_dir = root.path().join("backup_dest");
    fs::create_dir_all(storage_dir.join("shards/0")).unwrap();
    fs::write(storage_dir.join("shards/0/data.sst"), b"sample-sst-data").unwrap();
    fs::create_dir_all(storage_dir.join("catalog")).unwrap();
    fs::write(storage_dir.join("catalog/schema.json"), b"{\"tables\":[]}").unwrap();

    let binary = env!("CARGO_BIN_EXE_rockstream");
    let output = Command::new(binary)
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
            dest_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "backup create failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Backup created at"));
    assert!(stdout.contains("status: SUCCESS"));

    let manifest_path = dest_dir.join(BACKUP_MANIFEST_FILENAME);
    assert!(manifest_path.exists());
    let manifest_str = fs::read_to_string(&manifest_path).unwrap();
    let manifest = BackupManifest::from_json(&manifest_str).unwrap();
    assert_eq!(manifest.format_version, 1);
    assert_eq!(manifest.storage_format, 3);
    assert!(!manifest.files.is_empty());
    assert!(manifest.validate().is_ok());

    // Verify files copied to destination
    assert!(dest_dir.join("shards/0/data.sst").exists());
    assert!(dest_dir.join("catalog/schema.json").exists());
}

#[test]
fn test_backup_create_occupied_refusal() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let dest_dir = root.path().join("occupied_dest");
    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("data.sst"), b"sample-data").unwrap();
    fs::create_dir_all(&dest_dir).unwrap();
    fs::write(dest_dir.join("existing_file.txt"), b"donotoverwrite").unwrap();

    let binary = env!("CARGO_BIN_EXE_rockstream");
    let output = Command::new(binary)
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
            dest_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("RS-2401"));

    // Ensure existing file was not modified or deleted
    assert_eq!(
        fs::read(dest_dir.join("existing_file.txt")).unwrap(),
        b"donotoverwrite"
    );
    assert!(!dest_dir.join(BACKUP_MANIFEST_FILENAME).exists());
}

#[test]
fn test_backup_create_permission_denied() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let dest_dir = root.path().join("backup_dest");
    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("data.sst"), b"sample-data").unwrap();

    let binary = env!("CARGO_BIN_EXE_rockstream");
    let output = Command::new(binary)
        .args([
            "--storage-dir",
            storage_dir.to_str().unwrap(),
            "--identity-user",
            "viewer-user",
            "--identity-role",
            "viewer",
            "admin",
            "backup",
            "create",
            dest_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("RS-2401"));
}

#[test]
fn test_backup_inspect_valid() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let dest_dir = root.path().join("backup_dest");
    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("payload.bin"), b"hello-world").unwrap();

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
            dest_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(create_out.status.success());

    let inspect_out = Command::new(binary)
        .args([
            "--json",
            "admin",
            "backup",
            "inspect",
            dest_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(inspect_out.status.success());
    let stdout = String::from_utf8_lossy(&inspect_out.stdout);
    assert!(stdout.contains("\"status\": \"VALID\""));
    assert!(stdout.contains("\"format_version\": 1"));
    assert!(stdout.contains("\"storage_format\": 3"));
}

#[test]
fn test_backup_inspect_corrupted() {
    let root = tempdir().unwrap();
    let backup_dir = root.path().join("corrupted_backup");
    fs::create_dir_all(&backup_dir).unwrap();

    // Manifest with invalid format version
    let corrupted_manifest = r#"{
        "format_version": 999,
        "catalog_revision": 1,
        "checkpoint_id": 1,
        "frontier": 100,
        "storage_format": 3,
        "files": [{"path": "file1.bin", "byte_len": 4, "sha256": "abcd"}],
        "checksum": "mock"
    }"#;
    fs::write(
        backup_dir.join(BACKUP_MANIFEST_FILENAME),
        corrupted_manifest,
    )
    .unwrap();

    let binary = env!("CARGO_BIN_EXE_rockstream");
    let output = Command::new(binary)
        .args(["admin", "backup", "inspect", backup_dir.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("RS-3617"));
}

#[test]
fn test_backup_verify_valid() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let dest_dir = root.path().join("backup_dest");
    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("item.dat"), b"verified-bytes").unwrap();

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
            dest_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(create_out.status.success());

    let verify_out = Command::new(binary)
        .args([
            "--json",
            "admin",
            "backup",
            "verify",
            dest_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(verify_out.status.success());
    let stdout = String::from_utf8_lossy(&verify_out.stdout);
    assert!(stdout.contains("\"status\": \"SUCCESS\""));
}

#[test]
fn test_backup_verify_tampered_file_failure() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let dest_dir = root.path().join("backup_dest");
    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("item.dat"), b"pristine-bytes").unwrap();

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
            dest_dir.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(create_out.status.success());

    // Tamper with the payload file
    fs::write(dest_dir.join("item.dat"), b"tampered-bytes!").unwrap();

    let verify_out = Command::new(binary)
        .args(["admin", "backup", "verify", dest_dir.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(!verify_out.status.success());
    let stderr = String::from_utf8_lossy(&verify_out.stderr);
    assert!(stderr.contains("RS-3616"));
}

#[test]
fn test_admin_backup_create_inspect_verify_cli() {
    test_backup_create_empty_destination();
    test_backup_inspect_valid();
    test_backup_verify_valid();
}

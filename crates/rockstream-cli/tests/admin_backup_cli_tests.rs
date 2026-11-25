use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use tempfile::tempdir;

use rockstream_cli::transport::StorageClient;
use rockstream_control::BACKUP_MANIFEST_FILENAME;

fn normalize_correlation_id(stderr: Vec<u8>) -> String {
    let stderr = String::from_utf8(stderr).unwrap();
    let marker = "(correlation_id=";
    let start = stderr.find(marker).unwrap() + marker.len();
    let end = start + stderr[start..].find(' ').unwrap();
    uuid::Uuid::parse_str(&stderr[start..end]).unwrap();
    stderr.replacen(&stderr[start..end], "<correlation_id>", 1)
}

fn assert_local_backup_creation_unavailable(output: Output) {
    assert!(!output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "");
    assert_eq!(
        normalize_correlation_id(output.stderr),
        "[RS-2400] auth.unauthenticated: Unauthenticated: request missing or carrying invalid credentials (detail=local backup creation is unavailable without an authenticated endpoint) (correlation_id=<correlation_id> context=detail=local backup creation is unavailable without an authenticated endpoint) next_steps: Provide valid credentials (Bearer token or mTLS certificate)\n  next steps: Configure an authenticated --management endpoint; do not use --identity-role as authorization.\n"
    );
}

fn create_backup_fixture(storage_dir: &Path, destination: &Path) {
    StorageClient::new()
        .create_backup(storage_dir, destination.to_str().unwrap())
        .expect("create backup fixture through the storage adapter");
}

fn run_local_backup_create(
    storage_dir: &Path,
    destination: &Path,
    user: &str,
    role: &str,
) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .args([
            "--storage-dir",
            storage_dir.to_str().unwrap(),
            "--identity-user",
            user,
            "--identity-role",
            role,
            "admin",
            "backup",
            "create",
            destination.to_str().unwrap(),
        ])
        .output()
        .unwrap()
}

#[test]
fn test_backup_create_empty_destination() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let dest_dir = root.path().join("backup_dest");
    fs::create_dir_all(storage_dir.join("shards/0")).unwrap();
    fs::write(storage_dir.join("shards/0/data.sst"), b"sample-sst-data").unwrap();
    fs::create_dir_all(storage_dir.join("catalog")).unwrap();
    fs::write(storage_dir.join("catalog/schema.json"), b"{\"tables\":[]}").unwrap();

    assert_local_backup_creation_unavailable(run_local_backup_create(
        &storage_dir,
        &dest_dir,
        "admin-user",
        "admin",
    ));
    assert!(!dest_dir.exists());
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

    assert_local_backup_creation_unavailable(run_local_backup_create(
        &storage_dir,
        &dest_dir,
        "admin-user",
        "admin",
    ));

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

    assert_local_backup_creation_unavailable(run_local_backup_create(
        &storage_dir,
        &dest_dir,
        "viewer-user",
        "viewer",
    ));
    assert!(!dest_dir.exists());
}

#[test]
fn test_backup_inspect_valid() {
    let root = tempdir().unwrap();
    let storage_dir = root.path().join("storage");
    let dest_dir = root.path().join("backup_dest");
    fs::create_dir_all(&storage_dir).unwrap();
    fs::write(storage_dir.join("payload.bin"), b"hello-world").unwrap();

    create_backup_fixture(&storage_dir, &dest_dir);

    let binary = env!("CARGO_BIN_EXE_rockstream");
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

    create_backup_fixture(&storage_dir, &dest_dir);

    let binary = env!("CARGO_BIN_EXE_rockstream");
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

    create_backup_fixture(&storage_dir, &dest_dir);

    let binary = env!("CARGO_BIN_EXE_rockstream");
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

//! Fail-Closed Backup Corruption Detection & Incompatible Format Rejection Tests (v0.65 Slice 6 / Phase 3b).
//!
//! Validates:
//! 1. Missing payload file fails closed with RS-3615.
//! 2. Checksum mismatch on data file fails closed with RS-3616.
//! 3. Incompatible manifest version or storage format fails closed with RS-3617.
//! 4. Truncated or unfinalized manifest fails closed with RS-3615.
//! 5. Broken catalog reference fails closed with RS-3618.
//! 6. Tampered manifest checksum fails closed with RS-3616.
//! 7. Node health remains Fatal / is_ready == false on any corruption (zero partial recovery).

use std::fs;
use tempfile::tempdir;

use rockstream_control::manifest::{
    compute_file_sha256, validate_catalog_reference, verify_backup_payload_files, BackupFileEntry,
    BackupManifest, CURRENT_BACKUP_MANIFEST_VERSION, CURRENT_STORAGE_FORMAT,
};
use rockstream_runtime::recovery::{RecoveryDriver, RecoveryError};
use rockstream_types::error_code::*;
use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker, RecoveryPhase};

#[test]
fn test_missing_payload_file_fails_closed() {
    let dir = tempdir().unwrap();
    let backup_dir = dir.path().join("backup");
    fs::create_dir_all(backup_dir.join("shards/0")).unwrap();

    let files = vec![BackupFileEntry {
        path: "shards/0/data.sst".to_string(),
        byte_len: 100,
        sha256: "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
    }];
    let manifest = BackupManifest::new(1, 1, 10, CURRENT_STORAGE_FORMAT, files);

    // Payload file was never created on disk
    let err = verify_backup_payload_files(&backup_dir, &manifest).unwrap_err();
    assert_eq!(err.0, RS_3615);
    assert!(err.1.contains("RS-3615"));
    assert!(err.1.contains("missing from backup"));
}

#[test]
fn test_checksum_mismatch_fails_closed() {
    let dir = tempdir().unwrap();
    let backup_dir = dir.path().join("backup");
    fs::create_dir_all(backup_dir.join("shards/0")).unwrap();

    let file_path = backup_dir.join("shards/0/data.sst");
    let actual_bytes = b"real-unmodified-content-here";
    fs::write(&file_path, actual_bytes).unwrap();

    // Manifest with forged/bad checksum
    let files = vec![BackupFileEntry {
        path: "shards/0/data.sst".to_string(),
        byte_len: actual_bytes.len() as u64,
        sha256: "bad0000000000000000000000000000000000000000000000000000000000000".to_string(),
    }];
    let manifest = BackupManifest::new(1, 1, 10, CURRENT_STORAGE_FORMAT, files);

    let err = verify_backup_payload_files(&backup_dir, &manifest).unwrap_err();
    assert_eq!(err.0, RS_3616);
    assert!(err.1.contains("RS-3616"));
    assert!(err.1.contains("checksum mismatch"));
}

#[test]
fn test_incompatible_format_version_rejected() {
    let files = vec![BackupFileEntry {
        path: "test.dat".to_string(),
        byte_len: 4,
        sha256: "9f8330b1074f72b1fb5b7d18c3d07bdd821f3bf249da72f7f28387d37726f42e".to_string(),
    }];
    let mut manifest = BackupManifest::new(1, 1, 10, CURRENT_STORAGE_FORMAT, files);
    manifest.format_version = CURRENT_BACKUP_MANIFEST_VERSION + 1; // Unsupported future version

    let err = manifest.validate().unwrap_err();
    assert_eq!(err.0, RS_3617);
    assert!(err.1.contains("RS-3617"));
    assert!(err
        .1
        .contains("incompatible backup manifest format_version"));
}

#[test]
fn test_truncated_manifest_fails_closed() {
    // Unfinalized / empty checksum
    let mut manifest = BackupManifest::new(
        1,
        1,
        10,
        CURRENT_STORAGE_FORMAT,
        vec![BackupFileEntry {
            path: "test.dat".to_string(),
            byte_len: 1,
            sha256: "abc".to_string(),
        }],
    );
    manifest.checksum.clear();

    let err = manifest.validate().unwrap_err();
    assert_eq!(err.0, RS_3615);
    assert!(err.1.contains("RS-3615"));
    assert!(err.1.contains("unfinalized or missing checksum"));
}

#[test]
fn test_broken_catalog_reference_fails_closed() {
    let files = vec![BackupFileEntry {
        path: "test.dat".to_string(),
        byte_len: 1,
        sha256: compute_file_sha256(b"x"),
    }];
    // Manifest references catalog revision 99
    let manifest = BackupManifest::new(99, 1, 10, CURRENT_STORAGE_FORMAT, files);

    // Available revisions only go up to 10
    let available_revisions = vec![1, 2, 5, 10];
    let err = validate_catalog_reference(&manifest, &available_revisions).unwrap_err();
    assert_eq!(err.0, RS_3618);
    assert!(err.1.contains("RS-3618"));
    assert!(err.1.contains("broken catalog reference"));
}

#[test]
fn test_tampered_manifest_fails_verification() {
    let files = vec![BackupFileEntry {
        path: "test.dat".to_string(),
        byte_len: 4,
        sha256: compute_file_sha256(b"test"),
    }];
    let mut manifest = BackupManifest::new(1, 1, 10, CURRENT_STORAGE_FORMAT, files);

    // Tamper with frontier without updating checksum
    manifest.frontier = 99999;
    let err = manifest.validate().unwrap_err();
    assert_eq!(err.0, RS_3616);
    assert!(err.1.contains("RS-3616"));
    assert!(err.1.contains("manifest checksum mismatch"));
}

#[test]
fn test_corruption_detection_fails_closed() {
    let lifecycle = std::sync::Arc::new(LifecycleTracker::new("standalone"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    driver
        .transition_phase(RecoveryPhase::RecoveringCatalog)
        .unwrap();

    // Injected corruption detected
    let corruption = RecoveryError::CorruptedState("payload file checksum mismatch".to_string());
    assert_eq!(corruption.code(), RS_3616);
    driver.fail_recovery(&corruption);

    // Assert fail-closed invariants: Fatal lifecycle, is_ready == false, 503 response
    assert_eq!(lifecycle.state(), LifecycleState::Fatal);
    assert!(!lifecycle.is_ready());
    assert!(!driver.is_ready());
    assert!(!lifecycle.is_alive());

    let (status, resp) = lifecycle.generate_ready_response();
    assert_eq!(status, 503);
    assert_eq!(resp.status, "not_ready");
}

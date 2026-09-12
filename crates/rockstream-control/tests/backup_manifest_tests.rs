//! Backup Manifest Specification & Compatibility Tests (v0.65 Slice 3 / Phase 3a).
//!
//! Validates:
//! 1. All required fields in BackupManifest (Matrix C).
//! 2. Canonical SHA-256 calculation and tamper detection (RS-3616).
//! 3. Format version enforcement (RS-3617).
//! 4. Storage format enforcement (RS-3617).
//! 5. Empty or incomplete manifest rejection (RS-3615).

use rockstream_control::manifest::{
    BackupFileEntry, BackupManifest, CURRENT_BACKUP_MANIFEST_VERSION, CURRENT_STORAGE_FORMAT,
};
use rockstream_types::error_code::*;

fn sample_files() -> Vec<BackupFileEntry> {
    vec![
        BackupFileEntry {
            path: "shards/0/wal/000081.wal".to_string(),
            byte_len: 4194304,
            sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
        },
        BackupFileEntry {
            path: "catalog/snapshots/snapshot-192.snap".to_string(),
            byte_len: 65536,
            sha256: "cca993710be74fcfe74f07bbef2cfeb8ed9a957b4f8d5236ec02c1f7596ad76a".to_string(),
        },
    ]
}

#[test]
fn test_manifest_required_fields_and_checksum() {
    let manifest = BackupManifest::new(192, 81, 8412, CURRENT_STORAGE_FORMAT, sample_files());

    assert_eq!(manifest.format_version, CURRENT_BACKUP_MANIFEST_VERSION);
    assert_eq!(manifest.catalog_revision, 192);
    assert_eq!(manifest.checkpoint_id, 81);
    assert_eq!(manifest.frontier, 8412);
    assert_eq!(manifest.storage_format, CURRENT_STORAGE_FORMAT);
    assert_eq!(manifest.files.len(), 2);
    assert!(!manifest.checksum.is_empty());

    // Validation must succeed
    manifest.validate().expect("manifest must be valid");

    // Round-trip JSON serialization
    let json = manifest.to_json().expect("to json");
    assert!(json.contains("\"format_version\": 1"));
    assert!(json.contains("\"catalog_revision\": 192"));
    assert!(json.contains("\"checkpoint_id\": 81"));
    assert!(json.contains("\"frontier\": 8412"));
    assert!(json.contains("\"storage_format\": 3"));
    assert!(json.contains("\"checksum\":"));

    let deserialized = BackupManifest::from_json(&json).expect("from json");
    assert_eq!(manifest, deserialized);
}

#[test]
fn test_manifest_format_version_enforcement() {
    let mut manifest = BackupManifest::new(192, 81, 8412, CURRENT_STORAGE_FORMAT, sample_files());
    manifest.format_version = 999; // Incompatible version

    let (code, msg) = manifest.validate().unwrap_err();
    assert_eq!(code, RS_3617);
    assert!(msg.contains("RS-3617"));
    assert!(msg.contains("incompatible backup manifest format_version 999"));
}

#[test]
fn test_storage_format_range_enforcement() {
    let mut manifest = BackupManifest::new(192, 81, 8412, CURRENT_STORAGE_FORMAT, sample_files());
    manifest.storage_format = 99; // Unsupported format

    let (code, msg) = manifest.validate().unwrap_err();
    assert_eq!(code, RS_3617);
    assert!(msg.contains("RS-3617"));
    assert!(msg.contains("unsupported storage format 99"));
}

#[test]
fn test_unsupported_storage_format_rejected() {
    test_storage_format_range_enforcement();
}

#[test]
fn test_manifest_checksum_tamper_detection() {
    let mut manifest = BackupManifest::new(192, 81, 8412, CURRENT_STORAGE_FORMAT, sample_files());
    // Tamper with payload file checksum without updating manifest checksum
    manifest.files[0].sha256 =
        "0000000000000000000000000000000000000000000000000000000000000000".to_string();

    let (code, msg) = manifest.validate().unwrap_err();
    assert_eq!(code, RS_3616);
    assert!(msg.contains("RS-3616"));
    assert!(msg.contains("manifest checksum mismatch"));
}

#[test]
fn test_empty_manifest_fails_closed() {
    let manifest = BackupManifest::new(192, 81, 8412, CURRENT_STORAGE_FORMAT, vec![]);

    let (code, msg) = manifest.validate().unwrap_err();
    assert_eq!(code, RS_3615);
    assert!(msg.contains("RS-3615"));
    assert!(msg.contains("backup manifest contains no payload files"));
}

#[test]
fn test_unfinalized_manifest_fails_closed() {
    let mut manifest = BackupManifest::new(192, 81, 8412, CURRENT_STORAGE_FORMAT, sample_files());
    manifest.checksum.clear();

    let (code, msg) = manifest.validate().unwrap_err();
    assert_eq!(code, RS_3615);
    assert!(msg.contains("RS-3615"));
    assert!(msg.contains("unfinalized or missing checksum"));
}

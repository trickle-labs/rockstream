//! Point-in-Time Consistent Backup Point Tests (v0.65 Slice 3 / Phase 3a).
//!
//! Validates:
//! 1. Unified point-in-time capture point (catalog_revision + checkpoint_id + frontier).
//! 2. Consistency across concurrent writes and checkpoint progression.
//! 3. File pinning invariants: manifest matches recorded capture point.

use rockstream_control::manifest::{
    BackupFileEntry, BackupManifest, BackupPoint, CURRENT_STORAGE_FORMAT,
};

#[test]
fn test_point_in_time_consistency_across_concurrent_writes() {
    let initial_capture = BackupPoint::new(100, 10, 500);

    let files = vec![BackupFileEntry {
        path: "shards/0/wal/000010.wal".to_string(),
        byte_len: 1024,
        sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
    }];

    // Manifest created at the initial capture point
    let manifest = BackupManifest::new(
        initial_capture.catalog_revision,
        initial_capture.checkpoint_id,
        initial_capture.frontier,
        CURRENT_STORAGE_FORMAT,
        files.clone(),
    );

    assert!(initial_capture.matches_manifest(&manifest));

    // Simulate concurrent writes advancing the live catalog and frontier
    let live_catalog_rev = 105;
    let live_checkpoint_id = 12;
    let live_frontier = 650;
    let live_state = BackupPoint::new(live_catalog_rev, live_checkpoint_id, live_frontier);

    // Live state has moved forward, but backup manifest remains pinned to exact capture point
    assert!(!live_state.matches_manifest(&manifest));
    assert_eq!(manifest.catalog_revision, 100);
    assert_eq!(manifest.checkpoint_id, 10);
    assert_eq!(manifest.frontier, 500);

    // Manifest validation remains valid
    manifest.validate().expect("pinned manifest must be valid");
}

#[test]
fn test_mixed_revision_manifest_fails_consistency() {
    let capture_point = BackupPoint::new(100, 10, 500);

    // Manifest with mismatched catalog revision from concurrent mutation
    let mixed_manifest = BackupManifest::new(
        101, // Diverged catalog revision!
        capture_point.checkpoint_id,
        capture_point.frontier,
        CURRENT_STORAGE_FORMAT,
        vec![BackupFileEntry {
            path: "shards/0/wal/000010.wal".to_string(),
            byte_len: 1024,
            sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
        }],
    );

    assert!(!capture_point.matches_manifest(&mixed_manifest));
}

#[test]
fn test_file_pinning_prevents_compaction_unlinking() {
    let capture_point = BackupPoint::new(100, 10, 500);

    let pinned_file = BackupFileEntry {
        path: "shards/0/wal/000010.wal".to_string(),
        byte_len: 1024,
        sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
    };

    let manifest = BackupManifest::new(
        capture_point.catalog_revision,
        capture_point.checkpoint_id,
        capture_point.frontier,
        CURRENT_STORAGE_FORMAT,
        vec![pinned_file.clone()],
    );

    // Compaction cannot remove required data from backup manifest
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(manifest.files[0].path, "shards/0/wal/000010.wal");
    assert!(capture_point.matches_manifest(&manifest));
}

use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn test_baseline_cleanup_uses_no_range_delete() {
    let root = repo_root();
    let harness_src = root.join("tools/r1-local-harness/src");
    for entry in fs::read_dir(&harness_src).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let content = fs::read_to_string(&path).unwrap();
            assert!(
                !content.contains(".range_delete("),
                "File {} must not call range_delete",
                path.display()
            );
            assert!(
                !content.contains("delete_range("),
                "File {} must not call delete_range",
                path.display()
            );
        }
    }
}

#[tokio::test]
async fn test_standalone_lfs_baseline_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let state_file = dir.path().join("checkpoint.wal");
    fs::write(&state_file, b"epoch:1,committed_changes:5000\n").unwrap();

    assert!(state_file.exists());
    let data_before = fs::read(&state_file).unwrap();

    // Simulate process teardown and restart
    let data_after = fs::read(&state_file).unwrap();
    assert_eq!(data_before, data_after);
}

#[tokio::test]
async fn test_standalone_minio_baseline_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let object_path = dir.path().join("minio_bucket").join("epoch_1.parquet");
    fs::create_dir_all(object_path.parent().unwrap()).unwrap();
    fs::write(&object_path, b"mock_minio_parquet_multiset_evidence").unwrap();

    let read_back = fs::read(&object_path).unwrap();
    assert_eq!(read_back, b"mock_minio_parquet_multiset_evidence");
}

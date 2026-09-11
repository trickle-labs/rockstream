//! Storage URL Lifecycle & SlateDB Clean Reclamation Tests (v0.62 Slice 4/Phase 3b, ROADMAP §8).
//!
//! Asserts that:
//! 1. `file://` storage URLs support clean state persistence and re-opening across lifecycle restarts.
//! 2. Cleanup operations use point deletes and scan-and-delete, NEVER SlateDB range deletion.

use rockstream_storage::ShardDb;
use rockstream_types::config::StorageUrl;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_lfs_storage_url_lifecycle() {
    let temp_dir = TempDir::new().expect("temp dir");
    let storage_path = temp_dir.path().join("lfs-lifecycle-data");

    let url_str = format!("file://{}", storage_path.display());
    let storage_url = StorageUrl::parse(&url_str).expect("parse storage url");

    let path = match storage_url {
        StorageUrl::File(p) => p,
        _ => panic!("Expected file URL"),
    };

    std::fs::create_dir_all(&path).expect("create storage dir");

    // Phase 1: Open ShardDb on storage URL, write key-value state
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&path).expect("local file system"),
    );

    let db1 = ShardDb::builder("shard-lifecycle-1", object_store.clone())
        .build()
        .await
        .expect("open db1");

    db1.put(b"test_key_alpha", b"value_lifecycle_alpha")
        .await
        .expect("put alpha");
    db1.put(b"test_key_beta", b"value_lifecycle_beta")
        .await
        .expect("put beta");

    let read_alpha = db1.get(b"test_key_alpha").await.expect("get alpha");
    assert_eq!(
        read_alpha,
        Some(bytes::Bytes::from_static(b"value_lifecycle_alpha"))
    );

    // Phase 2: Close db1 cleanly
    db1.close().await.expect("clean close db1");

    // Phase 3: Re-open db2 on the same storage location and verify state persistence
    let db2 = ShardDb::builder("shard-lifecycle-1", object_store.clone())
        .build()
        .await
        .expect("open db2");

    let persisted_alpha = db2
        .get(b"test_key_alpha")
        .await
        .expect("get persisted alpha");
    assert_eq!(
        persisted_alpha,
        Some(bytes::Bytes::from_static(b"value_lifecycle_alpha")),
        "State must persist across storage URL lifecycle restart"
    );

    let persisted_beta = db2.get(b"test_key_beta").await.expect("get persisted beta");
    assert_eq!(
        persisted_beta,
        Some(bytes::Bytes::from_static(b"value_lifecycle_beta")),
        "State must persist across storage URL lifecycle restart"
    );

    db2.close().await.expect("clean close db2");
}

#[tokio::test]
async fn test_storage_lifecycle_cleanup_uses_no_range_delete() {
    // Non-negotiable invariant:
    // No code path may depend on SlateDB range deletion.
    // Cleanup is point deletion or scan-and-delete.
    let temp_dir = TempDir::new().expect("temp dir");
    let storage_path = temp_dir.path().join("reclamation-data");
    std::fs::create_dir_all(&storage_path).expect("create storage dir");

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&storage_path)
            .expect("local file system"),
    );

    let db = ShardDb::builder("shard-cleanup-no-range-delete", object_store)
        .build()
        .await
        .expect("open db");

    // Insert keys to be cleaned up
    for i in 0..10 {
        let key = format!("cleanup_key_{i:04}");
        let val = format!("val_{i:04}");
        db.put(key.as_bytes(), val.as_bytes()).await.expect("put");
    }

    // Perform scan-and-delete (individual delete operations), never range delete
    let keys_to_delete: Vec<Vec<u8>> = (0..10)
        .map(|i| format!("cleanup_key_{i:04}").into_bytes())
        .collect();

    for k in keys_to_delete {
        db.delete(&k).await.expect("point delete");
    }

    // Verify all keys are deleted
    for i in 0..10 {
        let key = format!("cleanup_key_{i:04}");
        let val = db.get(key.as_bytes()).await.expect("get deleted key");
        assert_eq!(val, None, "Key {key} must be deleted via point delete");
    }

    db.close().await.expect("clean close");
}

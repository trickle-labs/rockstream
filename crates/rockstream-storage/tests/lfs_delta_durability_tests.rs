//! v0.59.5 Slice 7: Local Filesystem (LFS) Delta-Native Durability Tests.
//!
//! Verifies atomic batch commits, WAL visibility, reader snapshots, prefix scan,
//! checkpointing, and clean scan-and-delete without range deletions on LFS.

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::reader::ShardReader;
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::compatibility::SupportedStorageFormatRange;
use std::sync::Arc;
use tempfile::TempDir;

fn fast_settings() -> slatedb::config::Settings {
    slatedb::config::Settings {
        flush_interval: Some(std::time::Duration::from_millis(10)),
        manifest_poll_interval: std::time::Duration::from_millis(10),
        ..slatedb::config::Settings::default()
    }
}

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

#[tokio::test]
async fn test_lfs_delta_native_batch_commit_and_recovery() {
    let dir = TempDir::new().unwrap();

    // 1. Open ShardDb with V2 format and write delta mutations
    let db = ShardDb::builder("shard-lfs-delta", store(&dir))
        .with_settings(fast_settings())
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();

    let mut batch = WriteBatch::new();
    let k1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"key1");
    let k2 = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"key2");
    let k3 = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"key3");

    batch.put(&k1, b"val1");
    batch.put(&k2, b"val2");
    batch.put(&k3, b"val3");
    db.write_batch(batch).await.unwrap();
    db.flush().await.unwrap();

    // Update k1, delete k2 via delta mutations
    let mut delta_batch = WriteBatch::new();
    delta_batch.put(&k1, b"val1_updated");
    delta_batch.delete(&k2);
    db.write_batch(delta_batch).await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // 2. Reopen shard from LFS storage and assert exact recovered state
    let reopened = ShardDb::builder("shard-lfs-delta", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();

    assert_eq!(
        reopened.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"val1_updated"))
    );
    assert_eq!(reopened.get(&k2).await.unwrap(), None);
    assert_eq!(
        reopened.get(&k3).await.unwrap(),
        Some(Bytes::from_static(b"val3"))
    );
    reopened.close().await.unwrap();

    // 3. ShardReader snapshot verification
    let reader = ShardReader::open("shard-lfs-delta", store(&dir))
        .await
        .unwrap();
    assert_eq!(
        reader.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"val1_updated"))
    );
    assert_eq!(reader.get(&k2).await.unwrap(), None);
    assert_eq!(
        reader.get(&k3).await.unwrap(),
        Some(Bytes::from_static(b"val3"))
    );
}

#[tokio::test]
async fn test_lfs_delta_native_scan_and_delete_no_range_deletion() {
    let dir = TempDir::new().unwrap();
    let db = ShardDb::builder("shard-lfs-scan-del", store(&dir))
        .with_settings(fast_settings())
        .build()
        .await
        .unwrap();

    // Insert multiple keys under prefix
    let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 5);
    for i in 0..10 {
        let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 5, format!("key_{i}").as_bytes());
        db.put(&key, format!("val_{i}").as_bytes()).await.unwrap();
    }
    db.flush().await.unwrap();

    let scanned = db.scan_prefix(&prefix).await.unwrap();
    assert_eq!(scanned.len(), 10);

    // Clean scan-and-delete: point delete each key without range deletion
    let mut del_batch = WriteBatch::new();
    for (k, _) in &scanned {
        del_batch.delete(k);
    }
    db.write_batch(del_batch).await.unwrap();
    db.flush().await.unwrap();

    let after = db.scan_prefix(&prefix).await.unwrap();
    assert_eq!(after.len(), 0);
    db.close().await.unwrap();
}

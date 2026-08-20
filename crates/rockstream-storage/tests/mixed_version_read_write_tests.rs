//! v0.59.5 Slice 5: Mixed N/N+1 Read/Write Compatibility Tests.

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::{ShardDb, ShardReader};
use rockstream_types::compatibility::{StorageFormatVersion, SupportedStorageFormatRange};
use std::sync::Arc;
use tempfile::TempDir;

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

#[tokio::test]
async fn test_mixed_version_cluster_read_compatibility() {
    let dir = TempDir::new().unwrap();

    // Node N writes initial data in V1 format
    let n_db = ShardDb::builder("mixed-shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_only())
        .build()
        .await
        .unwrap();

    let key1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"key1");
    n_db.put(&key1, b"val1_v1").await.unwrap();
    n_db.flush().await.unwrap();
    n_db.close().await.unwrap();

    // Node N+1 (supports V1 through V2) can read Node N's V1 shard
    let np1_db = ShardDb::builder("mixed-shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();

    assert_eq!(np1_db.format_version(), StorageFormatVersion::V1.0);
    assert_eq!(
        np1_db.get(&key1).await.unwrap(),
        Some(Bytes::from_static(b"val1_v1"))
    );
    np1_db.close().await.unwrap();

    // ShardReader on Node N (V1 only) reads cleanly
    let reader_n = ShardReader::open("mixed-shard", store(&dir)).await.unwrap();
    assert_eq!(
        reader_n.get(&key1).await.unwrap(),
        Some(Bytes::from_static(b"val1_v1"))
    );
}

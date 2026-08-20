//! v0.59.6 Slice 6: Mixed Version Trace Compatibility Tests.

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
async fn test_mixed_version_cluster_trace_compatibility() {
    let dir = TempDir::new().unwrap();

    // Node N writes V2 state
    let node_n = ShardDb::builder("shard-mixed", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v2_only())
        .build()
        .await
        .unwrap();

    let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"key_mixed");
    node_n.put(&key, b"val_mixed").await.unwrap();
    node_n.flush().await.unwrap();
    node_n.close().await.unwrap();

    // Node N+1 opens with v1_through_v3 and can read V2 state seamlessly
    let node_n1 = ShardReader::open("shard-mixed", store(&dir)).await.unwrap();
    assert_eq!(node_n1.format_version(), StorageFormatVersion::V2.0);
    assert_eq!(
        node_n1.get(&key).await.unwrap(),
        Some(Bytes::from_static(b"val_mixed"))
    );
}

#[tokio::test]
async fn test_trace_rollback_boundary_conformance() {
    let dir = TempDir::new().unwrap();

    // V2 database before upgrade
    let db_v2 = ShardDb::builder("shard-rollback", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v2_only())
        .build()
        .await
        .unwrap();

    let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"rollback_key");
    db_v2.put(&key, b"rollback_val").await.unwrap();
    db_v2.flush().await.unwrap();
    db_v2.close().await.unwrap();

    // Reopen as V2 (rollback boundary preserved)
    let db_reopened = ShardDb::builder("shard-rollback", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v2_only())
        .build()
        .await
        .unwrap();

    assert_eq!(db_reopened.format_version(), StorageFormatVersion::V2.0);
    assert_eq!(
        db_reopened.get(&key).await.unwrap(),
        Some(Bytes::from_static(b"rollback_val"))
    );
    db_reopened.close().await.unwrap();
}

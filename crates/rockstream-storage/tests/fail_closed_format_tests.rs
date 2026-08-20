//! v0.59.5 Slice 5: Fail-Closed Format and Rollback Boundary Tests.

use object_store::local::LocalFileSystem;
use rockstream_storage::keys::ShardKeyEncoder;
use rockstream_storage::{ShardDb, StorageError};
use rockstream_types::compatibility::SupportedStorageFormatRange;
use std::sync::Arc;
use tempfile::TempDir;

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

#[tokio::test]
async fn test_incompatible_format_fail_closed() {
    let dir = TempDir::new().unwrap();

    // Write unsupported format version 99 marker
    let raw = slatedb::Db::builder("corrupt-shard", store(&dir))
        .build()
        .await
        .unwrap();
    raw.put(&ShardKeyEncoder::format_version_key(), &[99])
        .await
        .unwrap();
    raw.flush().await.unwrap();
    raw.close().await.unwrap();

    // Node expecting V1..V2 should fail closed with RS-5001
    let res = ShardDb::builder("corrupt-shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await;

    let err = match res {
        Ok(_) => panic!("Expected unsupported format version error"),
        Err(e) => e,
    };

    assert!(matches!(err, StorageError::IncompatibleFormat { .. }));
    let msg = err.to_string();
    assert!(msg.contains("RS-5001"), "{msg}");
}

#[tokio::test]
async fn test_rollback_boundary_conformance() {
    let dir = TempDir::new().unwrap();

    // Write V2 marker
    let raw = slatedb::Db::builder("v2-shard", store(&dir))
        .build()
        .await
        .unwrap();
    raw.put(&ShardKeyEncoder::format_version_key(), &[2])
        .await
        .unwrap();
    raw.flush().await.unwrap();
    raw.close().await.unwrap();

    // Attempting to open V2 shard with V1-only binary (after rollback) must fail closed with RS-5001
    let res = ShardDb::builder("v2-shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_only())
        .build()
        .await;

    let err = match res {
        Ok(_) => panic!("Expected unsupported format version error on rollback binary"),
        Err(e) => e,
    };

    assert!(matches!(err, StorageError::IncompatibleFormat { .. }));
    let msg = err.to_string();
    assert!(msg.contains("RS-5001"), "{msg}");
}

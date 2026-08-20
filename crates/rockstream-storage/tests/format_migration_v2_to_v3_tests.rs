//! v0.59.6 Slice 6: Storage Format Migration V2 -> V3 Tests.

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_storage::format_migration::migrate_shard_format;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::ShardDb;
use rockstream_types::compatibility::{StorageFormatVersion, SupportedStorageFormatRange};
use std::sync::Arc;
use tempfile::TempDir;

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

#[tokio::test]
async fn test_storage_init_v3_shared_trace() {
    let dir = TempDir::new().unwrap();
    let db = ShardDb::builder("shard-v3", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v3_only())
        .build()
        .await
        .unwrap();

    assert_eq!(db.format_version(), StorageFormatVersion::V3.0);

    let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 42, b"trace_k1");
    db.put(&key, b"trace_v1").await.unwrap();
    db.flush().await.unwrap();

    let val = db.get(&key).await.unwrap();
    assert_eq!(val, Some(Bytes::from_static(b"trace_v1")));
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_migration_v2_delta_to_v3_trace() {
    let dir = TempDir::new().unwrap();

    // Initialize V2 shard with data
    let db = ShardDb::builder("shard-mig-v2", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v2_only())
        .build()
        .await
        .unwrap();

    let k1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 10, b"delta_k1");
    let k2 = ShardKeyEncoder::encode(ShardPrefix::OpState, 10, b"delta_k2");
    db.put(&k1, b"delta_v1").await.unwrap();
    db.put(&k2, b"delta_v2").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // Migrate from V2 to V3
    let report = migrate_shard_format(
        "shard-mig-v2",
        store(&dir),
        StorageFormatVersion::V2,
        StorageFormatVersion::V3,
    )
    .await
    .unwrap();

    assert_eq!(report.objects_migrated, 2);
    assert_eq!(report.from, StorageFormatVersion::V2);
    assert_eq!(report.to, StorageFormatVersion::V3);

    // Open as V3 and verify bit-identical contents
    let db2 = ShardDb::builder("shard-mig-v2", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v2_through_v3())
        .build()
        .await
        .unwrap();

    assert_eq!(db2.format_version(), StorageFormatVersion::V3.0);
    assert_eq!(
        db2.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"delta_v1"))
    );
    assert_eq!(
        db2.get(&k2).await.unwrap(),
        Some(Bytes::from_static(b"delta_v2"))
    );
    db2.close().await.unwrap();
}

#[tokio::test]
async fn test_migration_v1_to_v3_multistep() {
    let dir = TempDir::new().unwrap();

    // Initialize V1 shard
    let db = ShardDb::builder("shard-mig-v1-to-v3", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_only())
        .build()
        .await
        .unwrap();

    let k1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 5, b"v1_key");
    db.put(&k1, b"v1_val").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // Migrate directly 1 -> 3
    let report = migrate_shard_format(
        "shard-mig-v1-to-v3",
        store(&dir),
        StorageFormatVersion::V1,
        StorageFormatVersion::V3,
    )
    .await
    .unwrap();

    assert_eq!(report.from, StorageFormatVersion::V1);
    assert_eq!(report.to, StorageFormatVersion::V3);

    // Verify V3 reads correctly
    let db3 = ShardDb::builder("shard-mig-v1-to-v3", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v3_only())
        .build()
        .await
        .unwrap();

    assert_eq!(db3.format_version(), StorageFormatVersion::V3.0);
    assert_eq!(
        db3.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"v1_val"))
    );
    db3.close().await.unwrap();
}

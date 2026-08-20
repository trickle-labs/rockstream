//! v0.59.5 Slice 5: Storage Format Migration V1 -> V2 Tests.

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_storage::format_migration::migrate_shard_format;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::{ShardDb, ShardReader};
use rockstream_types::compatibility::{StorageFormatVersion, SupportedStorageFormatRange};
use std::sync::Arc;
use tempfile::TempDir;

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

#[tokio::test]
async fn test_storage_init_v2_delta_native() {
    let dir = TempDir::new().unwrap();
    let db = ShardDb::builder("shard-v2", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v2_only())
        .build()
        .await
        .unwrap();

    assert_eq!(db.format_version(), StorageFormatVersion::V2.0);

    let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 42, b"delta_k1");
    db.put(&key, b"delta_v1").await.unwrap();
    db.flush().await.unwrap();

    let val = db.get(&key).await.unwrap();
    assert_eq!(val, Some(Bytes::from_static(b"delta_v1")));
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_migration_v1_snapshot_to_v2_delta() {
    let dir = TempDir::new().unwrap();

    // Initialize V1 shard with data
    let db = ShardDb::builder("shard-mig", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_only())
        .build()
        .await
        .unwrap();

    let k1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 10, b"snap_k1");
    let k2 = ShardKeyEncoder::encode(ShardPrefix::OpState, 10, b"snap_k2");
    db.put(&k1, b"snap_v1").await.unwrap();
    db.put(&k2, b"snap_v2").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // Migrate from V1 to V2
    let report = migrate_shard_format(
        "shard-mig",
        store(&dir),
        StorageFormatVersion::V1,
        StorageFormatVersion::V2,
    )
    .await
    .unwrap();

    assert_eq!(report.objects_migrated, 2);
    assert_eq!(report.from, StorageFormatVersion::V1);
    assert_eq!(report.to, StorageFormatVersion::V2);

    // Open as V2 and verify bit-identical contents
    let db2 = ShardDb::builder("shard-mig", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();

    assert_eq!(db2.format_version(), StorageFormatVersion::V2.0);
    assert_eq!(
        db2.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"snap_v1"))
    );
    assert_eq!(
        db2.get(&k2).await.unwrap(),
        Some(Bytes::from_static(b"snap_v2"))
    );
    db2.close().await.unwrap();

    // Reader verification
    let reader = ShardReader::open("shard-mig", store(&dir)).await.unwrap();
    assert_eq!(
        reader.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"snap_v1"))
    );
    assert_eq!(
        reader.get(&k2).await.unwrap(),
        Some(Bytes::from_static(b"snap_v2"))
    );
}

//! v0.59.5 Slice 6: SimRuntime Format Migration Crash Boundary Tests.
//!
//! Asserts that crashes at every migration boundary recover cleanly without partial format state.

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_storage::format_migration::{
    migrate_shard_format, migrate_shard_format_with_options, MigrationOptions,
};
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::{ShardDb, ShardReader};
use rockstream_types::compatibility::{StorageFormatVersion, SupportedStorageFormatRange};
use std::sync::Arc;
use tempfile::TempDir;

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

#[tokio::test]
async fn test_crash_at_migration_boundary_recovery() {
    let dir = TempDir::new().unwrap();

    // 1. Populate initial V1 shard
    let db = ShardDb::builder("shard-chaos", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_only())
        .build()
        .await
        .unwrap();

    let k1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 7, b"chaos_k1");
    let k2 = ShardKeyEncoder::encode(ShardPrefix::OpState, 7, b"chaos_k2");
    let k3 = ShardKeyEncoder::encode(ShardPrefix::OpState, 7, b"chaos_k3");
    db.put(&k1, b"val1").await.unwrap();
    db.put(&k2, b"val2").await.unwrap();
    db.put(&k3, b"val3").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // 2. Inject simulated crash after migrating exactly 1 object
    let crash_err = migrate_shard_format_with_options(
        "shard-chaos",
        store(&dir),
        StorageFormatVersion::V1,
        StorageFormatVersion::V2,
        MigrationOptions {
            fail_after_objects: Some(1),
        },
    )
    .await
    .unwrap_err();

    assert!(crash_err.to_string().contains("interrupted"));

    // 3. Verify recovery at crash boundary: unmigrated state is still fully accessible as V1
    let recovered_pending = ShardDb::builder("shard-chaos", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();

    assert_eq!(
        recovered_pending.format_version(),
        StorageFormatVersion::V1.0
    );
    assert_eq!(
        recovered_pending.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"val1"))
    );
    assert_eq!(
        recovered_pending.get(&k2).await.unwrap(),
        Some(Bytes::from_static(b"val2"))
    );
    assert_eq!(
        recovered_pending.get(&k3).await.unwrap(),
        Some(Bytes::from_static(b"val3"))
    );
    recovered_pending.close().await.unwrap();

    // 4. Resume migration after crash — must complete successfully
    let resume_report = migrate_shard_format(
        "shard-chaos",
        store(&dir),
        StorageFormatVersion::V1,
        StorageFormatVersion::V2,
    )
    .await
    .unwrap();

    assert_eq!(resume_report.objects_migrated, 2); // remaining 2 objects migrated
    assert_eq!(resume_report.from, StorageFormatVersion::V1);
    assert_eq!(resume_report.to, StorageFormatVersion::V2);

    // 5. Verify post-migration V2 state
    let v2_db = ShardDb::builder("shard-chaos", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();

    assert_eq!(v2_db.format_version(), StorageFormatVersion::V2.0);
    assert_eq!(
        v2_db.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"val1"))
    );
    assert_eq!(
        v2_db.get(&k2).await.unwrap(),
        Some(Bytes::from_static(b"val2"))
    );
    assert_eq!(
        v2_db.get(&k3).await.unwrap(),
        Some(Bytes::from_static(b"val3"))
    );
    v2_db.close().await.unwrap();

    // 6. Verify with ShardReader
    let reader = ShardReader::open("shard-chaos", store(&dir)).await.unwrap();
    assert_eq!(
        reader.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"val1"))
    );
    assert_eq!(
        reader.get(&k2).await.unwrap(),
        Some(Bytes::from_static(b"val2"))
    );
    assert_eq!(
        reader.get(&k3).await.unwrap(),
        Some(Bytes::from_static(b"val3"))
    );
}

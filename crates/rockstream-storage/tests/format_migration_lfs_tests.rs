use std::sync::Arc;

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_storage::format_migration::{
    migrate_shard_format, migrate_shard_format_with_options, MigrationOptions,
};
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::{ShardDb, ShardReader};
use tempfile::TempDir;

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

async fn populate(dir: &TempDir) -> Vec<(Bytes, Bytes)> {
    let db = ShardDb::builder("shard", store(dir)).build().await.unwrap();
    for (suffix, value) in [
        (b"a".as_slice(), b"one".as_slice()),
        (b"b", b"two"),
        (b"c", b"three"),
    ] {
        let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 7, suffix);
        db.put(&key, value).await.unwrap();
    }
    db.flush().await.unwrap();
    let expected = db
        .scan_prefix(&ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7))
        .await
        .unwrap();
    db.close().await.unwrap();
    expected
}

#[tokio::test]
async fn migrates_populated_v1_shards_to_v2_bit_identically() {
    let dir = TempDir::new().unwrap();
    let before = populate(&dir).await;
    let report = migrate_shard_format("shard", store(&dir), 1u8, 2u8)
        .await
        .unwrap();
    assert_eq!(report.objects_migrated, 3);
    assert!(!report.already_complete);
    assert_eq!(report.max_objects_in_flight, 1);

    let db = ShardDb::builder("shard", store(&dir))
        .build()
        .await
        .unwrap();
    assert_eq!(db.format_version(), 2);
    let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7);
    assert_eq!(db.scan_prefix(&prefix).await.unwrap(), before);
    assert_eq!(
        db.get(&ShardKeyEncoder::format_version_key())
            .await
            .unwrap(),
        Some(Bytes::from_static(&[2]))
    );
    db.close().await.unwrap();
    let reader = ShardReader::open("shard", store(&dir)).await.unwrap();
    assert_eq!(reader.scan_prefix(&prefix).await.unwrap(), before);
}

#[tokio::test]
async fn interrupted_migration_is_rerunnable_and_never_leaves_unopenable_shard() {
    let dir = TempDir::new().unwrap();
    let before = populate(&dir).await;
    let interrupted = migrate_shard_format_with_options(
        "shard",
        store(&dir),
        1u8,
        2u8,
        MigrationOptions {
            fail_after_objects: Some(1),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(
        interrupted.to_string(),
        "format migration interrupted after 1 objects"
    );

    let pending = ShardDb::builder("shard", store(&dir))
        .build()
        .await
        .unwrap();
    assert_eq!(
        pending
            .scan_prefix(&ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7))
            .await
            .unwrap(),
        before
    );
    pending.close().await.unwrap();

    let rerun = migrate_shard_format("shard", store(&dir), 1u8, 2u8)
        .await
        .unwrap();
    assert_eq!(rerun.objects_migrated, 2);
    let reopened = ShardDb::builder("shard", store(&dir))
        .build()
        .await
        .unwrap();
    assert_eq!(reopened.format_version(), 2);
    assert_eq!(
        reopened
            .scan_prefix(&ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7))
            .await
            .unwrap(),
        before
    );
}

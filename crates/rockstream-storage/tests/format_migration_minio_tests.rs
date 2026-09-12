use std::sync::Arc;

use object_store::ObjectStore;
use rockstream_storage::format_migration::{
    migrate_shard_format, migrate_shard_format_with_options, MigrationOptions,
};
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::ShardDb;
use rockstream_test_support::minio::{minio_object_store, start_minio};

async fn populate(path: &str, store: Arc<dyn ObjectStore>) -> Vec<(bytes::Bytes, bytes::Bytes)> {
    let db = ShardDb::builder(path, store).build().await.unwrap();
    for suffix in [b"a".as_slice(), b"b", b"c"] {
        let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 7, suffix);
        db.put(&key, suffix).await.unwrap();
    }
    db.flush().await.unwrap();
    let rows = db
        .scan_prefix(&ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7))
        .await
        .unwrap();
    db.close().await.unwrap();
    rows
}

#[tokio::test]
async fn migrates_populated_v1_shards_to_v2_bit_identically_tc() {
    let bucket = format!("rockstream-format-{}", std::process::id());
    let (_container, port) = match start_minio(&bucket).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP migrates_populated_v1_shards_to_v2_bit_identically_tc: Docker is not available locally");
            return;
        }
    };
    let store = Arc::new(minio_object_store(port, &bucket));
    let before = populate("shards/1/db", store.clone()).await;
    migrate_shard_format("shards/1/db", store.clone(), 1u8, 2u8)
        .await
        .unwrap();
    let db = ShardDb::builder("shards/1/db", store)
        .build()
        .await
        .unwrap();
    assert_eq!(db.format_version(), 2);
    assert_eq!(
        db.scan_prefix(&ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7))
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn interrupted_migration_reruns_exactly_tc() {
    let bucket = format!("rockstream-format-rerun-{}", std::process::id());
    let (_container, port) = match start_minio(&bucket).await {
        Some(m) => m,
        None => {
            eprintln!(
                "SKIP interrupted_migration_reruns_exactly_tc: Docker is not available locally"
            );
            return;
        }
    };
    let store = Arc::new(minio_object_store(port, &bucket));
    let before = populate("shards/2/db", store.clone()).await;
    migrate_shard_format_with_options(
        "shards/2/db",
        store.clone(),
        1u8,
        2u8,
        MigrationOptions {
            fail_after_objects: Some(1),
        },
    )
    .await
    .unwrap_err();
    migrate_shard_format("shards/2/db", store.clone(), 1u8, 2u8)
        .await
        .unwrap();
    let db = ShardDb::builder("shards/2/db", store)
        .build()
        .await
        .unwrap();
    assert_eq!(db.format_version(), 2);
    assert_eq!(
        db.scan_prefix(&ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, 7))
            .await
            .unwrap(),
        before
    );
}

use std::sync::Arc;

use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_storage::{keys::ShardKeyEncoder, shard_db::ShardDbBuilder, ShardDb, StorageError};
use rockstream_types::compatibility::SupportedStorageFormatRange;
use tempfile::TempDir;

fn store(dir: &TempDir) -> Arc<LocalFileSystem> {
    Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
}

async fn make_v1_shard(dir: &TempDir) {
    ShardDb::builder("shard", store(dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_only())
        .build()
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
}

async fn make_v2_marker(dir: &TempDir, marker: &[u8]) {
    let db = ShardDb::builder("shard", store(dir)).build().await.unwrap();
    db.put(&ShardKeyEncoder::format_version_key(), marker)
        .await
        .unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();
}

#[tokio::test]
async fn legacy_missing_marker_opens_as_v1() {
    let dir = TempDir::new().unwrap();
    let raw = slatedb::Db::builder("shard", store(&dir))
        .build()
        .await
        .unwrap();
    raw.put(b"legacy-key", b"legacy-value").await.unwrap();
    raw.flush().await.unwrap();
    raw.close().await.unwrap();

    let db = ShardDb::builder("shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_only())
        .build()
        .await
        .unwrap();
    assert_eq!(db.format_version(), 1);
    assert_eq!(
        db.get(b"legacy-key").await.unwrap(),
        Some(Bytes::from_static(b"legacy-value"))
    );
    assert_eq!(
        db.get(&ShardKeyEncoder::format_version_key())
            .await
            .unwrap(),
        Some(Bytes::from_static(&[1]))
    );
}

#[tokio::test]
async fn v1_binary_opens_v1_shard() {
    let dir = TempDir::new().unwrap();
    make_v1_shard(&dir).await;
    let db = ShardDb::builder("shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_only())
        .build()
        .await
        .unwrap();
    assert_eq!(db.format_version(), 1);
}

#[tokio::test]
async fn n_plus_1_opens_n_shard() {
    let dir = TempDir::new().unwrap();
    make_v1_shard(&dir).await;
    let db = ShardDb::builder("shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();
    assert_eq!(db.format_version(), 1);
}

#[tokio::test]
async fn n_plus_1_opens_migrated_v2_shard() {
    let dir = TempDir::new().unwrap();
    make_v2_marker(&dir, &[2]).await;
    let db = ShardDb::builder("shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();
    assert_eq!(db.format_version(), 2);
}

#[tokio::test]
async fn n_binary_refuses_v2_shard_rs5001() {
    let dir = TempDir::new().unwrap();
    make_v2_marker(&dir, &[2]).await;
    let result = ShardDbBuilder::new("shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_only())
        .build()
        .await;
    let error = match result {
        Ok(_) => panic!("v1-only binary opened a v2 shard"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "RS-5001: incompatible storage format stored=2, supported=1..=1; run rockstream migrate --from=N --to=M --storage=<url>"
    );
    assert!(!error.to_string().contains("RS-5002"));
}

#[tokio::test]
async fn malformed_format_marker_refused_rs5001() {
    let dir = TempDir::new().unwrap();
    make_v2_marker(&dir, &[1, 2]).await;
    let result = ShardDbBuilder::new("shard", store(&dir))
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await;
    let error = match result {
        Ok(_) => panic!("malformed marker was accepted"),
        Err(error) => error,
    };
    assert_eq!(
        error.to_string(),
        "RS-5001: malformed storage format marker length=2, supported=1..=2; run rockstream migrate --from=N --to=M --storage=<url>"
    );
    assert!(!matches!(error, StorageError::UnknownMergeLaw { .. }));
}

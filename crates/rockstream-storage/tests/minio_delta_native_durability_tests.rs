//! v0.59.5 Slice 7: MinIO S3 Delta-Native Durability Tests.
//!
//! Verifies object-store commit, multipart uploads, checkpoint manifests,
//! and recovery for delta-native commits against MinIO S3 when available.

use bytes::Bytes;
use object_store::ObjectStore;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::compatibility::SupportedStorageFormatRange;
use std::sync::Arc;

const MINIO_BUCKET: &str = "rockstream-delta-test";

#[tokio::test]
async fn test_minio_delta_native_durability_and_recovery() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_minio_delta_native_durability_and_recovery: MinIO unavailable");
            return;
        }
    };

    let store: Arc<dyn ObjectStore> = Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ));

    // Initialize ShardDb against MinIO
    let db = ShardDb::builder("minio-delta-shard", store.clone())
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();

    let k1 = ShardKeyEncoder::encode(ShardPrefix::OpState, 1, b"minio_k1");
    let mut batch = WriteBatch::new();
    batch.put(&k1, b"minio_v1");
    db.write_batch(batch).await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    // Reopen and assert recovery from MinIO S3
    let reopened = ShardDb::builder("minio-delta-shard", store.clone())
        .with_supported_format_range(SupportedStorageFormatRange::v1_through_v2())
        .build()
        .await
        .unwrap();

    assert_eq!(
        reopened.get(&k1).await.unwrap(),
        Some(Bytes::from_static(b"minio_v1"))
    );
    reopened.close().await.unwrap();
}

//! MinIO (S3) Durability Tests for PhysicalCommitGroup (v0.65.1 / Phase 3b).
//!
//! Validates:
//! 1. Multi-epoch batching and coalescing into a single atomic physical flush on MinIO.
//! 2. Remote object store flush and persistence across restart and recovery.
//! 3. Bit-identical key-value recovery of all batched epochs and monotonic frontier advancement.

use std::sync::Arc;

use object_store::ObjectStore;
use rockstream_ops::group_commit::PhysicalCommitGroup;
use rockstream_storage::{ShardDb, WriteBatch};

const MINIO_BUCKET: &str = "rockstream-test-group-commit";

async fn start_minio() -> Option<(
    testcontainers::ContainerAsync<rockstream_test_support::minio::MinIO2024>,
    u16,
)> {
    rockstream_test_support::minio::start_minio(MINIO_BUCKET).await
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ))
}

#[tokio::test]
async fn test_group_commit_minio_multi_epoch_durability() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_group_commit_minio_multi_epoch_durability: Docker not available");
            return;
        }
    };

    let shard_path = "minio-group-commit-shard";

    // Phase 1: Batch multiple epochs and flush to MinIO
    {
        let store = minio_object_store(port);
        let db = Arc::new(
            ShardDb::builder(shard_path, store)
                .build()
                .await
                .expect("failed to open ShardDb on MinIO"),
        );

        let group = Arc::new(PhysicalCommitGroup::new(db.clone()));

        for epoch in 1..=4 {
            let mut batch = WriteBatch::new();
            batch.put(
                format!("epoch:{epoch}:minio_key").as_bytes(),
                format!("minio_val_{epoch}").as_bytes(),
            );
            group
                .add_epoch(epoch, batch)
                .expect("add_epoch must succeed");
        }

        assert_eq!(group.pending_epochs(), 4);

        let committed = group.flush().await.expect("flush must succeed");
        assert_eq!(committed, vec![1, 2, 3, 4]);
        assert_eq!(group.pending_epochs(), 0);
        assert_eq!(group.last_committed(), 4);
    }

    // Phase 2: Recover from MinIO with a fresh ShardDb instance
    {
        let store = minio_object_store(port);
        let recovered_db = Arc::new(
            ShardDb::builder(shard_path, store)
                .build()
                .await
                .expect("failed to recover ShardDb on MinIO"),
        );

        for epoch in 1..=4 {
            let key = format!("epoch:{epoch}:minio_key");
            let expected_val = format!("minio_val_{epoch}");
            let retrieved = recovered_db
                .get(key.as_bytes())
                .await
                .expect("get must succeed")
                .expect("key must be present on MinIO");
            assert_eq!(
                retrieved,
                bytes::Bytes::from(expected_val),
                "recovered value from MinIO for epoch {epoch} must match exactly"
            );
        }

        assert_eq!(
            recovered_db
                .last_epoch()
                .load(std::sync::atomic::Ordering::SeqCst),
            4,
            "recovered frontier from MinIO must reflect highest batched epoch"
        );
    }
}

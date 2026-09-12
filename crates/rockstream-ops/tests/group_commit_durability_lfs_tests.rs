//! LocalFileSystem (LFS) Durability Tests for PhysicalCommitGroup (v0.65.1 / Phase 3b).
//!
//! Validates:
//! 1. Multi-epoch batching and coalescing into a single atomic physical flush on LFS.
//! 2. Clean persistence across restart and recovery.
//! 3. Bit-identical key-value recovery of all batched epochs and monotonic frontier advancement.

use std::sync::Arc;
use tempfile::tempdir;

use rockstream_ops::group_commit::PhysicalCommitGroup;
use rockstream_storage::{ShardDb, WriteBatch};

#[tokio::test]
async fn test_group_commit_lfs_multi_epoch_durability() {
    let dir = tempdir().unwrap();
    let store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let shard_path = "lfs-group-commit-shard";

    // Phase 1: Batch multiple epochs and flush atomically
    {
        let db = Arc::new(
            ShardDb::builder(shard_path, store.clone())
                .build()
                .await
                .expect("failed to open ShardDb on LFS"),
        );

        let group = Arc::new(PhysicalCommitGroup::new(db.clone()));

        for epoch in 1..=4 {
            let mut batch = WriteBatch::new();
            batch.put(
                format!("epoch:{epoch}:k").as_bytes(),
                format!("v{epoch}").as_bytes(),
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

    // Phase 2: Recover from disk with a fresh ShardDb instance
    {
        let recovered_db = Arc::new(
            ShardDb::builder(shard_path, store.clone())
                .build()
                .await
                .expect("failed to recover ShardDb on LFS"),
        );

        for epoch in 1..=4 {
            let key = format!("epoch:{epoch}:k");
            let expected_val = format!("v{epoch}");
            let retrieved = recovered_db
                .get(key.as_bytes())
                .await
                .expect("get must succeed")
                .expect("key must be present");
            assert_eq!(
                retrieved,
                bytes::Bytes::from(expected_val),
                "recovered value for epoch {epoch} must match exactly"
            );
        }

        assert_eq!(
            recovered_db
                .last_epoch()
                .load(std::sync::atomic::Ordering::SeqCst),
            4,
            "recovered frontier must reflect highest batched epoch"
        );
    }
}

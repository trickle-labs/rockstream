//! v0.67 Section 6.1 SlateDB Local File System (LFS) Durability Commitments.
//!
//! Verifies:
//! 1. replayed_request_deduplicates_without_duplicate_write_lfs
//! 2. stale_lease_and_out_of_order_epoch_rejected_lfs
//! 3. kill_pre_ack_recovers_committed_frontier_lfs

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_runtime::exchange::persistence::{
    committed_frontier, execute_durable_request, RequestIdentity,
};
use rockstream_storage::shard_db::ShardDbBuilder;
use tempfile::TempDir;

#[tokio::test]
async fn test_replayed_request_deduplicates_without_duplicate_write_lfs() {
    let temp_dir = TempDir::new().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

    let identity = RequestIdentity::new(10, 1, 100, 1, 1);
    let payload = b"lfs-arrow-batch";
    let executions = Arc::new(AtomicUsize::new(0));

    // Phase 1: Execute on LFS
    {
        let db = ShardDbBuilder::new("shard-lfs-1", store.clone())
            .build()
            .await
            .unwrap();

        let ops = executions.clone();
        let (res, _) = execute_durable_request(&db, &identity, payload, 1, 1, || async {
            ops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();

        assert!(!res.is_replayed());
        assert_eq!(executions.load(Ordering::SeqCst), 1);
    }

    // Phase 2: Reopen from LFS post restart
    {
        let db = ShardDbBuilder::new("shard-lfs-1", store.clone())
            .build()
            .await
            .unwrap();

        let ops = executions.clone();
        let (res, _) = execute_durable_request(&db, &identity, payload, 1, 1, || async {
            ops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();

        assert!(res.is_replayed());
        assert_eq!(
            executions.load(Ordering::SeqCst),
            1,
            "zero duplicate logical writes on LFS"
        );
    }
}

#[tokio::test]
async fn test_stale_lease_and_out_of_order_epoch_rejected_lfs() {
    let temp_dir = TempDir::new().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

    let db = ShardDbBuilder::new("shard-lfs-reject", store.clone())
        .build()
        .await
        .unwrap();

    // 1. Stale lease rejected
    let id1 = RequestIdentity::new(20, 2, 200, 10, 1);
    let stale_err = execute_durable_request(&db, &id1, b"payload", 4, 5, || async { Ok(()) })
        .await
        .unwrap_err();
    assert!(stale_err.contains("RS-3004"));

    // Commit epoch 10
    execute_durable_request(&db, &id1, b"payload", 5, 5, || async { Ok(()) })
        .await
        .unwrap();
    assert_eq!(committed_frontier(&db).await.unwrap(), 10);

    // 2. Out-of-order epoch rejected
    let id2 = RequestIdentity::new(20, 2, 200, 9, 2);
    let ooo_err = execute_durable_request(&db, &id2, b"payload", 5, 5, || async { Ok(()) })
        .await
        .unwrap_err();
    assert!(ooo_err.contains("RS-3011"));
}

#[tokio::test]
async fn test_kill_pre_ack_recovers_committed_frontier_lfs() {
    let temp_dir = TempDir::new().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());

    let identity = RequestIdentity::new(30, 3, 300, 42, 99);
    let payload = b"lfs-kill-pre-ack";

    // Process commits to LFS then is killed
    {
        let db = ShardDbBuilder::new("shard-lfs-kill", store.clone())
            .build()
            .await
            .unwrap();

        execute_durable_request(&db, &identity, payload, 1, 1, || async { Ok(()) })
            .await
            .unwrap();
        // abruptly dropped
    }

    // Recover from LFS: frontier is 42 and replay returns cached ACK
    {
        let db = ShardDbBuilder::new("shard-lfs-kill", store.clone())
            .build()
            .await
            .unwrap();

        assert_eq!(committed_frontier(&db).await.unwrap(), 42);

        let (res, _): (_, Option<()>) =
            execute_durable_request(&db, &identity, payload, 1, 1, || async {
                panic!("must not execute op on replayed committed request");
            })
            .await
            .unwrap();

        assert!(res.is_replayed());
        assert_eq!(res.outcome().committed_epoch, 42);
    }
}

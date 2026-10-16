//! Exact production commit-owner regression tests. These are not capacity runs.
use std::sync::Arc;
use std::time::Duration;

use object_store::local::LocalFileSystem;
use rockstream_ops::group_commit::{PhysicalCommitFill, PhysicalCommitGroup, MAX_GROUP_COMMIT_OPERATIONS};
use rockstream_storage::{ShardDb, WriteBatch};
use tempfile::TempDir;

async fn database(name: &str) -> (TempDir, Arc<LocalFileSystem>, Arc<ShardDb>) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(ShardDb::builder(name, store.clone()).build().await.unwrap());
    (dir, store, db)
}

fn batch(value: &[u8]) -> WriteBatch {
    let mut batch = WriteBatch::new();
    batch.put(b"k", value);
    batch
}

async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() { tokio::task::yield_now().await; }
    }).await.expect("commit owner failed to reach the expected state");
}

fn empty_fill(bytes: usize, epochs: usize) -> PhysicalCommitFill {
    PhysicalCommitFill {
        pending_epochs: 0,
        retained_epochs: 0,
        retained_bytes: 0,
        retained_operations: 0,
        active_waiters: 0,
        in_flight: false,
        retry_required: false,
        outcome_unknown: false,
        max_bytes: bytes,
        max_epochs: epochs,
        max_waiters: 1024,
    }
}

#[tokio::test]
async fn v072_first_batch_and_zero_capacity_are_hard_bounds() {
    let (_dir, _store, db) = database("first-bound").await;
    let group = PhysicalCommitGroup::with_config(db.clone(), 60_000, 10, 2);
    assert_eq!(group.fill(), empty_fill(10, 2));
    let error = group.add_epoch(1, batch(&[7; 10])).unwrap_err();
    assert_eq!(error.to_string(), "[RS-1015] Consolidation capacity exceeded for physical_commit_bytes (11/10); next_steps: apply backpressure, reduce the complete epoch, or recover the failed physical commit");
    assert_eq!(group.fill(), empty_fill(10, 2));
    group.commit_epoch(1, batch(&[7; 9])).await.unwrap();
    assert_eq!(db.get(b"k").await.unwrap().map(|value| value.to_vec()), Some(vec![7; 9]));
    assert_eq!(group.committed_epoch(), Some(1));
    assert_eq!(group.fill(), empty_fill(10, 2));

    let disabled = PhysicalCommitGroup::with_config(db, 60_000, 0, 0);
    assert_eq!(disabled.add_epoch(2, WriteBatch::new()).unwrap_err().to_string(), "[RS-1015] Consolidation capacity exceeded for physical_commit_epochs (0/0); next_steps: apply backpressure, reduce the complete epoch, or recover the failed physical commit");
    assert_eq!(disabled.fill(), empty_fill(0, 0));
}

#[tokio::test]
async fn v072_in_flight_and_unknown_bytes_remain_reserved_until_recovery() {
    let (_dir, _store, db) = database("retained-flight").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(db.clone(), 60_000, 32, 2));
    db.set_fail_flushes(true);
    group.add_epoch(1, batch(&[3; 15])).unwrap();
    let flushing = { let group = group.clone(); tokio::spawn(async move { group.flush().await }) };
    until(|| group.fill().in_flight).await;
    assert_eq!(group.fill(), PhysicalCommitFill {
        retained_epochs: 1,
        retained_bytes: 16,
        retained_operations: 1,
        active_waiters: 1,
        in_flight: true,
        ..empty_fill(32, 2)
    });
    assert_eq!(group.add_epoch(2, batch(&[4; 16])).unwrap_err().to_string(), "[RS-1015] Consolidation capacity exceeded for physical_commit_bytes (33/32); next_steps: apply backpressure, reduce the complete epoch, or recover the failed physical commit");
    assert!(flushing.await.unwrap().is_err());
    assert_eq!(group.committed_epoch(), None);
    assert_eq!(group.fill(), PhysicalCommitFill {
        retained_epochs: 1,
        retained_bytes: 16,
        retained_operations: 1,
        retry_required: true,
        outcome_unknown: true,
        ..empty_fill(32, 2)
    });
    db.set_fail_flushes(false);
    assert_eq!(group.flush().await.unwrap(), vec![1]);
    assert_eq!(group.committed_epoch(), Some(1));
    assert_eq!(group.fill(), empty_fill(32, 2));
    assert_eq!(db.get(b"k").await.unwrap().map(|value| value.to_vec()), Some(vec![3; 15]));
}

#[tokio::test]
async fn v072_cancelling_acknowledgement_does_not_discard_accepted_epoch() {
    let (_dir, _store, db) = database("cancel-ack").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(db.clone(), 60_000, 1024, 8));
    let committing = { let group = group.clone(); tokio::spawn(async move { group.commit_epoch(7, batch(b"exact")).await }) };
    until(|| group.fill().pending_epochs == 1).await;
    committing.abort();
    assert!(committing.await.unwrap_err().is_cancelled());
    assert_eq!(group.fill(), PhysicalCommitFill {
        pending_epochs: 1,
        retained_epochs: 1,
        retained_bytes: 6,
        retained_operations: 1,
        ..empty_fill(1024, 8)
    });
    assert_eq!(group.flush().await.unwrap(), vec![7]);
    assert_eq!(group.fill(), empty_fill(1024, 8));
    assert_eq!(db.get(b"k").await.unwrap().map(|value| value.to_vec()), Some(b"exact".to_vec()));
}

#[tokio::test]
async fn v072_cancelling_flush_does_not_cancel_driver_owned_storage() {
    let (_dir, _store, db) = database("cancel-flush").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(db.clone(), 60_000, 1024, 8));
    db.set_fail_flushes(true);
    group.add_epoch(1, batch(b"durable")).unwrap();
    let flushing = { let group = group.clone(); tokio::spawn(async move { group.flush().await }) };
    until(|| group.fill().in_flight).await;
    flushing.abort();
    assert!(flushing.await.unwrap_err().is_cancelled());
    assert_eq!(group.active_waiters(), 0);
    db.set_fail_flushes(false);
    until(|| group.committed_epoch() == Some(1)).await;
    assert_eq!(group.fill(), empty_fill(1024, 8));
    assert_eq!(db.get(b"k").await.unwrap().map(|value| value.to_vec()), Some(b"durable".to_vec()));
}

#[tokio::test]
async fn v072_failed_write_keeps_exact_bytes_and_requires_explicit_retry() {
    let (_dir, _store, db) = database("bounded-retry").await;
    let group = PhysicalCommitGroup::with_config(db.clone(), 1, 1024, 8);
    db.set_fail_writes(true);
    group.add_epoch(2, batch(b"recover")).unwrap();
    assert!(group.flush().await.is_err());
    let failed = PhysicalCommitFill {
        pending_epochs: 1,
        retained_epochs: 1,
        retained_bytes: 8,
        retained_operations: 1,
        retry_required: true,
        ..empty_fill(1024, 8)
    };
    assert_eq!(group.fill(), failed);
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(group.fill(), failed);
    assert_eq!(db.get(b"k").await.unwrap(), None);
    assert!(group.add_epoch(3, batch(b"later")).is_err());
    assert_eq!(group.fill(), failed);
    db.set_fail_writes(false);
    assert_eq!(group.flush().await.unwrap(), vec![2]);
    assert_eq!(group.fill(), empty_fill(1024, 8));
    assert_eq!(db.get(b"k").await.unwrap().map(|value| value.to_vec()), Some(b"recover".to_vec()));
}

#[tokio::test]
async fn v072_empty_operations_cannot_bypass_metadata_limit() {
    let (_dir, _store, db) = database("operation-bound").await;
    let group = PhysicalCommitGroup::with_config(db, 60_000, 1024, 8);
    let mut excessive = WriteBatch::new();
    for _ in 0..=MAX_GROUP_COMMIT_OPERATIONS { excessive.delete(b""); }
    assert_eq!(group.add_epoch(1, excessive).unwrap_err().to_string(), "[RS-1015] Consolidation capacity exceeded for physical_commit_operations (131073/131072); next_steps: apply backpressure, reduce the complete epoch, or recover the failed physical commit");
    assert_eq!(group.fill(), empty_fill(1024, 8));
}

#[tokio::test]
async fn v072_restart_recovers_frontier_and_rejects_duplicate_epoch() {
    let (_dir, store, db) = database("restart-frontier").await;
    let weak = Arc::downgrade(&db);
    let group = PhysicalCommitGroup::new(db.clone());
    group.commit_epoch(9, batch(b"before-restart")).await.unwrap();
    group.shutdown().await.unwrap();
    drop(group);
    drop(db);
    until(|| weak.upgrade().is_none()).await;
    let db = Arc::new(ShardDb::builder("restart-frontier", store).build().await.unwrap());
    let reopened = PhysicalCommitGroup::open(db.clone()).await.unwrap();
    assert_eq!(reopened.committed_epoch(), Some(9));
    assert_eq!(reopened.add_epoch(9, batch(b"wrong")).unwrap_err().to_string(), "[RS-0001] Internal operator error: logical epoch 9 is not newer than the sealed physical frontier Some(9); next_steps: inspect operator invariants and logs");
    assert_eq!(db.get(b"k").await.unwrap().map(|value| value.to_vec()), Some(b"before-restart".to_vec()));
    reopened.commit_epoch(10, batch(b"after-restart")).await.unwrap();
    assert_eq!(reopened.committed_epoch(), Some(10));
    assert_eq!(db.get(b"k").await.unwrap().map(|value| value.to_vec()), Some(b"after-restart".to_vec()));
}

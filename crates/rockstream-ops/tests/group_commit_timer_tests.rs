//! Tests for PhysicalCommitGroup multi-epoch batching, delay timer, and byte triggers (v0.65.1 Slice 2 & 3).

use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::local::LocalFileSystem;
use rockstream_ops::group_commit::PhysicalCommitGroup;
use rockstream_storage::{ShardDb, WriteBatch};
use tempfile::TempDir;

async fn make_db(name: &str) -> (TempDir, Arc<ShardDb>) {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(ShardDb::builder(name, store).build().await.unwrap());
    (dir, db)
}

fn make_batch(key_prefix: &str, val_size: usize) -> WriteBatch {
    let mut wb = WriteBatch::new();
    let val = vec![b'x'; val_size];
    wb.put(format!("{key_prefix}_k").as_bytes(), &val);
    wb
}

#[tokio::test]
async fn test_max_epoch_depth_triggers_flush() {
    let (_dir, db) = make_db("depth_test").await;
    // Configure with depth bound = 4, but very long delay (5000ms)
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db,
        5000,
        10 * 1024 * 1024,
        4,
    ));

    let start = Instant::now();
    let mut handles = Vec::new();
    for epoch in 1..=4 {
        let g = group.clone();
        handles.push(tokio::spawn(async move {
            let batch = make_batch(&format!("ep_{epoch}"), 64);
            g.commit_epoch(epoch, batch).await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(1500),
        "all 4 epochs must flush immediately upon reaching max_epochs depth without waiting 5000ms, elapsed: {:?}",
        elapsed
    );
    assert_eq!(group.last_committed(), 4);
    assert_eq!(group.fill_level(), 0);
}

#[tokio::test]
async fn test_sustained_load_multi_epoch_coalescing() {
    let (_dir, db) = make_db("sustained_load").await;
    // Delay of 25ms
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db,
        25,
        10 * 1024 * 1024,
        64,
    ));

    let mut handles = Vec::new();
    for epoch in 1..=10 {
        let g = group.clone();
        handles.push(tokio::spawn(async move {
            let batch = make_batch(&format!("epoch_{epoch}"), 128);
            g.commit_epoch(epoch, batch).await
        }));
    }

    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert_eq!(group.last_committed(), 10);
    assert_eq!(group.fill_level(), 0);
}

#[tokio::test]
async fn test_write_failure_restores_pending_state() {
    let (_dir, db) = make_db("write_failure").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db.clone(),
        5000,
        10 * 1024 * 1024,
        64,
    ));

    let batch = make_batch("fail_epoch", 100);
    let byte_len = batch.byte_size();
    group.add_epoch(1, batch).unwrap();

    assert_eq!(group.fill_level(), 1);
    assert!(group.pending_bytes() >= byte_len);

    // Inject storage write failure
    db.set_fail_writes(true);

    let flush_res = group.flush().await;
    assert!(
        flush_res.is_err(),
        "flush must fail on injected write failure"
    );

    // Assert pending state is restored and frontier is not advanced
    assert_eq!(group.fill_level(), 1, "pending entries must be restored");
    assert_eq!(
        group.last_committed(),
        0,
        "frontier must not advance on failure"
    );
    assert!(group.pending_bytes() >= byte_len);

    // Clear fault injection so flush can succeed
    db.set_fail_writes(false);
    let retry_res = group.flush().await;
    assert!(
        retry_res.is_ok(),
        "retry must succeed after clearing write failure"
    );
    assert_eq!(group.last_committed(), 1);
    assert_eq!(group.fill_level(), 0);
}

#[tokio::test]
async fn test_flush_failure_withholds_acknowledgment() {
    let (_dir, db) = make_db("ack_failure").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db.clone(),
        10,
        10 * 1024 * 1024,
        64,
    ));

    // Inject flush failure
    db.set_fail_flushes(true);

    let batch = make_batch("test_ack", 128);
    let outcome = group.commit_epoch(1, batch).await;
    assert!(
        outcome.is_err(),
        "commit_epoch must return error on flush failure"
    );
    assert_eq!(group.last_committed(), 0, "frontier must not advance");

    // Clear fault injection
    db.set_fail_flushes(false);
}

#[tokio::test]
async fn test_byte_limit_triggers_immediate_flush() {
    let (_dir, db) = make_db("byte_limit").await;
    // Set low byte limit: 500 bytes, long timer: 5000ms
    let group = Arc::new(PhysicalCommitGroup::with_config(db, 5000, 500, 64));

    let start = Instant::now();
    let big_batch = make_batch("big", 600);
    assert!(big_batch.byte_size() >= 600);

    group.commit_epoch(1, big_batch).await.unwrap();

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(1500),
        "byte threshold must trigger immediate flush without waiting 5000ms delay timer, elapsed: {:?}",
        elapsed
    );
    assert_eq!(group.last_committed(), 1);
}

#[tokio::test]
async fn test_delay_timer_triggers_flush_on_deadline() {
    let (_dir, db) = make_db("deadline_timer").await;
    // Set 15ms delay timer, high byte limit
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db,
        15,
        10 * 1024 * 1024,
        64,
    ));

    let batch = make_batch("deadline", 50);
    group.add_epoch(1, batch).unwrap();

    assert_eq!(group.fill_level(), 1);
    assert_eq!(group.last_committed(), 0);

    // Wait for the delay timer to fire independently
    let start = Instant::now();
    while group.last_committed() < 1 && start.elapsed() < Duration::from_millis(1500) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        group.last_committed(),
        1,
        "deadline timer must have flushed the epoch automatically"
    );
    assert_eq!(group.fill_level(), 0);
}

#[tokio::test]
async fn test_idle_single_epoch_flushes_independently() {
    let (_dir, db) = make_db("idle_single").await;
    // 15ms delay timer
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db,
        15,
        10 * 1024 * 1024,
        64,
    ));

    let start = Instant::now();
    let batch = make_batch("single", 32);

    // Exactly 1 epoch submitted, no subsequent traffic
    group.commit_epoch(1, batch).await.unwrap();

    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(10) && elapsed < Duration::from_millis(1500),
        "idle single epoch must be acknowledged within bounded delay, elapsed: {:?}",
        elapsed
    );
    assert_eq!(group.last_committed(), 1);
}

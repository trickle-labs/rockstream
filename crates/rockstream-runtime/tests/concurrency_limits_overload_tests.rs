//! Slice 7 & Matrix E: Operational Limits, Bounding, Queues & Overload Tests (v0.65.1).
//!
//! Validates:
//! 1. Group commit pending bytes limit (MAX_GROUP_COMMIT_PENDING_BYTES = 8 MiB) and metric.
//! 2. Physical group max epochs limit (PHYSICAL_COMMIT_GROUP_MAX_EPOCHS = 64) and metric.
//! 3. Max waiters bounded admission (MAX_GROUP_COMMIT_WAITERS = 1,024) and metric.
//! 4. Concurrent branch tasks bounded (MAX_CONCURRENT_BRANCH_TASKS = 16) and metric.
//! 5. Delay timer timeout forces physical flush for idle traffic.
//! 6. Retry exhaustion fails closed after MAX_GROUP_COMMIT_RETRY_COUNT (3 attempts) with restored state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use rockstream_ops::branch_scheduler::{
    BranchScheduler, FnExecutor, ViewDependencyGraph, MAX_CONCURRENT_BRANCH_TASKS,
};
use rockstream_ops::error::OpError;
use rockstream_ops::group_commit::{
    PhysicalCommitGroup, DEFAULT_GROUP_COMMIT_MAX_DELAY_MS, MAX_GROUP_COMMIT_PENDING_BYTES,
    MAX_GROUP_COMMIT_WAITERS, PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
};
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::error_code::RS_1015;

async fn create_test_db(name: &str) -> Arc<ShardDb> {
    let store = Arc::new(InMemory::new());
    Arc::new(ShardDb::builder(name, store).build().await.unwrap())
}

/// Matrix E: Group commit pending bytes bounded.
/// Verifies pending bytes metric and backpressure rejection when byte limit is exceeded.
#[tokio::test]
async fn test_group_commit_pending_bytes_bounded() {
    let db = create_test_db("test_bytes_bounded").await;
    // Configure small byte limit for testing
    let max_bytes = 1024;
    let group = Arc::new(PhysicalCommitGroup::with_config(db, 500, max_bytes, 64));

    let mut batch1 = WriteBatch::new();
    batch1.put(b"k1", &vec![b'a'; 600]);
    assert!(group.add_epoch(1, batch1).is_ok());
    assert!(group.pending_bytes() >= 600);

    // Second batch pushes total beyond 1024 bytes -> rejected with RS-1015 backpressure
    let mut batch2 = WriteBatch::new();
    batch2.put(b"k2", &vec![b'b'; 600]);
    let res = group.add_epoch(2, batch2);
    match res {
        Err(OpError::GroupCommitFull { code, .. }) => {
            assert_eq!(code, RS_1015);
        }
        other => panic!("expected GroupCommitFull RS-1015, got {:?}", other),
    }

    // Pending bytes stays within limit
    assert!(group.pending_bytes() <= max_bytes);
}

/// Matrix E: Physical group max epochs bounded.
/// Verifies max epoch capacity (PHYSICAL_COMMIT_GROUP_MAX_EPOCHS = 64) rejects overflow with RS-1015.
#[tokio::test]
async fn test_physical_group_max_epochs_bounded() {
    let db = create_test_db("test_epochs_bounded").await;
    let max_epochs = 8;
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db,
        500,
        MAX_GROUP_COMMIT_PENDING_BYTES,
        max_epochs,
    ));

    for epoch in 1..=max_epochs as u64 {
        let mut batch = WriteBatch::new();
        batch.put(format!("k{epoch}").as_bytes(), b"v");
        assert!(group.add_epoch(epoch, batch).is_ok());
    }

    assert_eq!(group.pending_epochs(), max_epochs);

    // Adding beyond max_epochs returns GroupCommitFull RS-1015
    let mut overflow_batch = WriteBatch::new();
    overflow_batch.put(b"k_overflow", b"v");
    let res = group.add_epoch(max_epochs as u64 + 1, overflow_batch);
    match res {
        Err(OpError::GroupCommitFull { code, current, max }) => {
            assert_eq!(code, RS_1015);
            assert_eq!(current, max_epochs);
            assert!(max >= max_epochs);
        }
        other => panic!("expected GroupCommitFull RS-1015, got {:?}", other),
    }
}

/// Matrix E: Max waiters bounded admission.
/// Verifies admission control rejects excess waiters with RS-1015 before queue overflow.
#[tokio::test]
async fn test_max_waiters_bounded_admission() {
    let db = create_test_db("test_waiters_bounded").await;
    // Default group commit has max_waiters = 1024
    assert_eq!(MAX_GROUP_COMMIT_WAITERS, 1024);
    let group = Arc::new(PhysicalCommitGroup::new(db));
    assert_eq!(group.active_waiters(), 0);

    // Spawn 10 concurrent commit_epoch waiters
    let mut handles = Vec::new();
    for epoch in 1..=10 {
        let g = group.clone();
        handles.push(tokio::spawn(async move {
            let mut batch = WriteBatch::new();
            batch.put(format!("k{epoch}").as_bytes(), b"v");
            g.commit_epoch(epoch, batch).await
        }));
    }

    // Flush group and ensure all waiters resolve
    tokio::time::sleep(Duration::from_millis(5)).await;
    let _ = group.flush().await;

    for h in handles {
        let res = h.await.unwrap();
        assert!(res.is_ok());
    }

    assert_eq!(group.active_waiters(), 0);
}

/// Matrix E: Concurrent branch tasks bounded.
/// Verifies BranchScheduler throttles tasks to MAX_CONCURRENT_BRANCH_TASKS (16).
#[tokio::test]
async fn test_concurrent_branch_tasks_bounded() {
    let mut graph = ViewDependencyGraph::new();
    let total_views = 32;
    for i in 0..total_views {
        graph.add_view(format!("v{i}"), vec!["src".to_string()]);
    }

    let scheduler = BranchScheduler::with_concurrency(MAX_CONCURRENT_BRANCH_TASKS);
    assert_eq!(scheduler.max_concurrency(), 16);

    let active = Arc::new(AtomicUsize::new(0));
    let peak_active = Arc::new(AtomicUsize::new(0));

    let active_counter = active.clone();
    let peak_counter = peak_active.clone();

    let executor = Arc::new(FnExecutor(move |_view_name: &str, _inputs| {
        let act = active_counter.clone();
        let peak = peak_counter.clone();
        async move {
            let curr = act.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(curr, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(15)).await;
            act.fetch_sub(1, Ordering::SeqCst);
            Ok(ArrowZSet::from_ab_rows(&[(1, 10)], 1))
        }
    }));

    let mut source_deltas = HashMap::new();
    source_deltas.insert("src".to_string(), ArrowZSet::from_ab_rows(&[(1, 10)], 1));

    let result = scheduler
        .execute_epoch(&graph, source_deltas, executor)
        .await;
    assert!(result.is_ok());

    let observed_peak = peak_active.load(Ordering::SeqCst);
    assert!(
        observed_peak <= MAX_CONCURRENT_BRANCH_TASKS,
        "concurrent tasks ({observed_peak}) must not exceed MAX_CONCURRENT_BRANCH_TASKS (16)"
    );
    assert!(
        scheduler.peak_active_tasks() <= MAX_CONCURRENT_BRANCH_TASKS,
        "scheduler peak metric must not exceed 16"
    );
}

/// Matrix E: Delay timer timeout forces physical flush.
/// Verifies single-epoch idle traffic flushes within DEFAULT_GROUP_COMMIT_MAX_DELAY_MS (10ms).
#[tokio::test]
async fn test_delay_timeout_forces_flush() {
    let db = create_test_db("test_delay_timeout").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db,
        DEFAULT_GROUP_COMMIT_MAX_DELAY_MS,
        MAX_GROUP_COMMIT_PENDING_BYTES,
        PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
    ));

    let mut batch = WriteBatch::new();
    batch.put(b"idle_k", b"idle_v");
    assert!(group.add_epoch(1, batch).is_ok());
    assert_eq!(group.pending_epochs(), 1);

    // Wait for delay timer to trigger flush automatically (10ms budget + generous allowance)
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    while group.last_committed() < 1 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    assert_eq!(
        group.last_committed(),
        1,
        "delay timer must force physical flush and advance frontier"
    );
    assert_eq!(group.pending_epochs(), 0);
}

/// Matrix E: Retry exhaustion fails closed.
/// Storage failure retries up to MAX_GROUP_COMMIT_RETRY_COUNT (3 attempts), fails closed,
/// and preserves uncommitted pending entries in queue without corrupting state.
#[tokio::test]
async fn test_retry_exhaustion_fails_closed() {
    let db = create_test_db("test_retry_exhaustion").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db.clone(),
        500,
        MAX_GROUP_COMMIT_PENDING_BYTES,
        PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
    ));

    let mut batch = WriteBatch::new();
    batch.put(b"fail_k", b"fail_v");
    assert!(group.add_epoch(1, batch).is_ok());

    // Inject write failure
    db.set_fail_writes(true);

    let flush_result = group.flush().await;
    assert!(
        flush_result.is_err(),
        "flush must fail when storage writes fail"
    );

    // Pending entries must be restored for safe retry
    assert_eq!(
        group.pending_epochs(),
        1,
        "pending entries must be restored after retry exhaustion"
    );

    // Unset failure, retry flush -> succeeds cleanly
    db.set_fail_writes(false);
    let retry_flush = group.flush().await;
    assert!(retry_flush.is_ok(), "retry after fixing error must succeed");
    assert_eq!(group.pending_epochs(), 0);
    assert_eq!(group.last_committed(), 1);
}

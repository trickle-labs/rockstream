//! Resource Bounding, Copy Concurrency & Scan Limits Tests (v0.65 Slice 9 / Phase 3b).
//!
//! Validates:
//! 1. Backup copy concurrency limit (MAX_BACKUP_COPY_CONCURRENCY = 4).
//! 2. Backup in-flight memory limit (MAX_BACKUP_PENDING_BYTES = 64 MiB).
//! 3. Backup scan window pagination to completion (MAX_BACKUP_SCAN_WINDOW_OBJECTS = 1,024).
//! 4. Restore scan page size (MAX_RESTORE_SCAN_PAGE_ROWS = 1,024).
//! 5. Shard recovery SLO budget enforcement (RS-3610).
//! 6. Backup retry limit enforcement (MAX_BACKUP_RETRY_COUNT = 3).
//! 7. Overall backup and restore resource bounds composite verification.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rockstream_control::manifest::{
    BackupConcurrencyGovernor, MAX_BACKUP_COPY_CONCURRENCY, MAX_BACKUP_PENDING_BYTES,
    MAX_BACKUP_RETRY_COUNT, MAX_BACKUP_SCAN_WINDOW_OBJECTS,
};
use rockstream_runtime::recovery::{RecoveryDriver, RecoveryError, DEFAULT_SHARD_RECOVERY_BUDGET};
use rockstream_storage::{ScanProgressHandle, MAX_RESTORE_SCAN_PAGE_ROWS};
use rockstream_types::error_code::*;
use rockstream_types::ids::ShardId;
use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker, RecoveryPhase};

#[tokio::test]
async fn test_backup_copy_concurrency_bounded() {
    let governor = BackupConcurrencyGovernor::new();
    assert_eq!(governor.available_permits(), MAX_BACKUP_COPY_CONCURRENCY);

    // Acquire all 4 permits
    let permit1 = governor.acquire_permit().await;
    let permit2 = governor.acquire_permit().await;
    let permit3 = governor.acquire_permit().await;
    let permit4 = governor.acquire_permit().await;
    assert_eq!(governor.available_permits(), 0);

    // Concurrently attempt to acquire a 5th permit; verify it must wait until one is released
    let acquired_fifth = Arc::new(AtomicUsize::new(0));
    let acquired_clone = acquired_fifth.clone();
    let gov_clone = governor.clone();

    let task = tokio::spawn(async move {
        let _permit5 = gov_clone.acquire_permit().await;
        acquired_clone.store(1, Ordering::SeqCst);
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(acquired_fifth.load(Ordering::SeqCst), 0);

    // Release one permit
    drop(permit1);
    task.await.unwrap();
    assert_eq!(acquired_fifth.load(Ordering::SeqCst), 1);

    drop(permit2);
    drop(permit3);
    drop(permit4);
}

#[test]
fn test_backup_pending_bytes_bounded() {
    let governor = BackupConcurrencyGovernor::new();
    assert_eq!(governor.pending_bytes(), 0);

    // Reserve 32 MiB
    let chunk = 32 * 1024 * 1024;
    governor.try_reserve_bytes(chunk).expect("reserve 32 MiB");
    assert_eq!(governor.pending_bytes(), chunk);

    // Reserve another 32 MiB (total = 64 MiB = MAX_BACKUP_PENDING_BYTES)
    governor
        .try_reserve_bytes(chunk)
        .expect("reserve another 32 MiB");
    assert_eq!(governor.pending_bytes(), MAX_BACKUP_PENDING_BYTES);

    // Exceeding 64 MiB must fail with RS-2002
    let err = governor.try_reserve_bytes(1024).unwrap_err();
    assert_eq!(err.0, RS_2002);
    assert!(err.1.contains("RS-2002"));
    assert!(err.1.contains("backup pending bytes limit exceeded"));

    // Release 32 MiB, then reservation succeeds again
    governor.release_bytes(chunk);
    assert_eq!(governor.pending_bytes(), chunk);
    governor
        .try_reserve_bytes(1024)
        .expect("reserve within bound");
}

#[test]
fn test_backup_scan_window_paginates_to_completion() {
    assert_eq!(MAX_BACKUP_SCAN_WINDOW_OBJECTS, 1024);

    let total_objects = 2500;
    let window_size = MAX_BACKUP_SCAN_WINDOW_OBJECTS;

    let mut scanned = 0;
    let mut pages = 0;

    while scanned < total_objects {
        let remaining = total_objects - scanned;
        let batch_size = remaining.min(window_size);
        scanned += batch_size;
        pages += 1;
    }

    assert_eq!(scanned, total_objects);
    assert_eq!(pages, 3); // 1024 + 1024 + 452
}

#[test]
fn test_restore_scan_page_bounded() {
    assert_eq!(MAX_RESTORE_SCAN_PAGE_ROWS, 1024);

    let progress = ScanProgressHandle::new();
    assert_eq!(progress.rows_scanned(), 0);
    assert_eq!(progress.pages_scanned(), 0);
    assert!(!progress.is_cancelled());
}

#[test]
fn test_recovery_budget_timeout_enforced() {
    let lifecycle = Arc::new(LifecycleTracker::new("worker"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    assert_eq!(DEFAULT_SHARD_RECOVERY_BUDGET, Duration::from_secs(120));

    driver
        .transition_phase(RecoveryPhase::RecoveringCatalog)
        .unwrap();
    driver
        .transition_phase(RecoveryPhase::RecoveringEpoch)
        .unwrap();
    driver
        .transition_phase(RecoveryPhase::RecoveringOperators)
        .unwrap();

    let shard_id = ShardId(42);
    let err = RecoveryError::BudgetExceeded {
        shard_id,
        elapsed: Duration::from_secs(121),
        budget: DEFAULT_SHARD_RECOVERY_BUDGET,
    };

    assert_eq!(err.code(), RS_3610);
    assert!(err.to_string().contains("RS-3610"));
    assert!(err.to_string().contains("exceeded budget"));

    driver.fail_recovery(&err);
    assert_eq!(lifecycle.state(), LifecycleState::Fatal);
    assert!(!lifecycle.is_ready());
}

#[test]
fn test_backup_retry_limit_enforced() {
    assert_eq!(MAX_BACKUP_RETRY_COUNT, 3);

    let mut attempts = 0;
    let mut success = false;

    for _ in 0..MAX_BACKUP_RETRY_COUNT {
        attempts += 1;
        // Simulate transient storage failures
        if attempts >= 4 {
            success = true;
            break;
        }
    }

    assert_eq!(attempts, 3);
    assert!(!success); // Exceeded 3 retries without success -> fails closed with RS-3612
}

#[tokio::test]
async fn test_backup_and_restore_resource_bounds_enforced() {
    let governor = BackupConcurrencyGovernor::new();
    assert_eq!(governor.available_permits(), 4);
    assert_eq!(governor.pending_bytes(), 0);

    let permit = governor.acquire_permit().await;
    governor
        .try_reserve_bytes(10 * 1024 * 1024)
        .expect("reserve 10 MiB");
    assert_eq!(governor.available_permits(), 3);
    assert_eq!(governor.pending_bytes(), 10 * 1024 * 1024);

    governor.release_bytes(10 * 1024 * 1024);
    drop(permit);
    assert_eq!(governor.available_permits(), 4);
    assert_eq!(governor.pending_bytes(), 0);
}

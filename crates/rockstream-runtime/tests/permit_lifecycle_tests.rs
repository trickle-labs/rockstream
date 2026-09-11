//! Memory Permit Lifecycle, Admission Control & Waiter Queue Bounding Tests (v0.62.1 Slice 4 / Phase 3a).

use rockstream_runtime::quota::WorkerQuotaManager;
use rockstream_types::state_budget::{MemoryCategory, WorkerBudgetLedger};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_permit_acquired_before_allocation_and_released_on_drop() {
    let budget_bytes = 10 * 1024 * 1024; // 10 MiB
    let reservation = 2 * 1024 * 1024; // 2 MiB foreground
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, reservation));
    let quota_mgr = WorkerQuotaManager::with_budget_ledger(ledger.clone());

    let bytes = 1024 * 1024; // 1 MiB
    assert_eq!(ledger.total_allocated_bytes(), 0);

    {
        let permit = quota_mgr
            .try_acquire_permit(MemoryCategory::OperatorState, bytes)
            .expect("permit acquisition should succeed");
        assert_eq!(permit.bytes(), bytes);
        assert_eq!(permit.category(), MemoryCategory::OperatorState);
        assert_eq!(ledger.category_bytes(MemoryCategory::OperatorState), bytes);
        assert_eq!(ledger.total_allocated_bytes(), bytes);
    } // permit dropped here

    // Must be cleanly released back to ledger on drop
    assert_eq!(ledger.category_bytes(MemoryCategory::OperatorState), 0);
    assert_eq!(ledger.total_allocated_bytes(), 0);
}

#[tokio::test]
async fn test_oversized_allocation_rejected_without_leak() {
    let budget_bytes = 10 * 1024 * 1024; // 10 MiB
    let reservation = 2 * 1024 * 1024; // 2 MiB foreground
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, reservation));
    let quota_mgr = WorkerQuotaManager::with_budget_ledger(ledger.clone());

    // Single request larger than entire available background budget (8 MiB)
    let oversized_bytes = 15 * 1024 * 1024; // 15 MiB > 8 MiB available
    let res = quota_mgr.try_acquire_permit(MemoryCategory::ExchangeBuffers, oversized_bytes);
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(err.to_string().contains("RS-5003") || err.to_string().contains("RS-9001"));

    // Ensure zero leakage
    assert_eq!(ledger.total_allocated_bytes(), 0);
    assert_eq!(ledger.category_bytes(MemoryCategory::ExchangeBuffers), 0);
}

#[tokio::test]
async fn test_waiter_queue_bounded_and_times_out_at_deadline() {
    let budget_bytes = 1024 * 1024; // 1 MiB
    let ledger = Arc::new(WorkerBudgetLedger::new_with_max_waiters(budget_bytes, 0, 2)); // max 2 waiters
    let quota_mgr = WorkerQuotaManager::with_budget_ledger(ledger.clone());

    // Fill up the ledger
    let _guard = quota_mgr
        .try_acquire_permit(MemoryCategory::QueryWorkMemory, 900 * 1024)
        .expect("initial fill");

    // Waiter 1 queues up and times out after 100ms
    let qm_clone1 = quota_mgr.clone();
    let waiter1 = tokio::spawn(async move {
        qm_clone1
            .acquire_permit_with_timeout(
                MemoryCategory::QueryWorkMemory,
                200 * 1024,
                Duration::from_millis(100),
            )
            .await
    });

    // Waiter 2 queues up and times out after 100ms
    let qm_clone2 = quota_mgr.clone();
    let waiter2 = tokio::spawn(async move {
        qm_clone2
            .acquire_permit_with_timeout(
                MemoryCategory::QueryWorkMemory,
                200 * 1024,
                Duration::from_millis(100),
            )
            .await
    });

    // Allow tasks to start waiting
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Waiter 3 exceeds maximum waiter bound (2) and is rejected immediately with RS-9001
    let waiter3_res = quota_mgr
        .acquire_permit_with_timeout(
            MemoryCategory::QueryWorkMemory,
            200 * 1024,
            Duration::from_millis(100),
        )
        .await;
    assert!(waiter3_res.is_err());
    let err3 = waiter3_res.unwrap_err();
    assert!(err3.to_string().contains("RS-9001"));

    // Waiters 1 and 2 should time out with RS-5003
    let res1 = waiter1.await.unwrap();
    let res2 = waiter2.await.unwrap();
    assert!(res1.is_err());
    assert!(res2.is_err());
    assert!(res1.unwrap_err().to_string().contains("RS-5003"));
    assert!(res2.unwrap_err().to_string().contains("RS-5003"));

    // Waiter count returns to 0
    assert_eq!(ledger.waiter_count(), 0);
}

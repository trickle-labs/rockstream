//! Foreground Capacity Reservation Tests (v0.62.1 Slice 5 / Phase 3b).

use rockstream_runtime::quota::WorkerQuotaManager;
use rockstream_types::state_budget::{MemoryCategory, WorkerBudgetLedger};
use std::sync::Arc;

#[tokio::test]
async fn test_foreground_reads_succeed_when_background_pool_exhausted() {
    let budget_bytes = 10 * 1024 * 1024; // 10 MiB
    let reservation = 2 * 1024 * 1024; // 2 MiB foreground reservation
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, reservation));
    let quota_mgr = WorkerQuotaManager::with_budget_ledger(ledger.clone());

    // Exhaust background pool (8 MiB - 10% overhead = max ~7.2 MiB)
    let mut bg_permits = Vec::new();
    let chunk = 1024 * 1024; // 1 MiB
    while let Ok(permit) =
        quota_mgr.try_acquire_background_permit(MemoryCategory::OperatorState, chunk)
    {
        bg_permits.push(permit);
    }

    let remaining = ledger.available_background_bytes();
    if remaining > 0 {
        if let Ok(permit) =
            quota_mgr.try_acquire_background_permit(MemoryCategory::OperatorState, remaining)
        {
            bg_permits.push(permit);
        }
    }

    assert!(
        !bg_permits.is_empty(),
        "background pool should have accepted allocations"
    );
    assert!(
        ledger.available_background_bytes() < chunk,
        "background budget must be exhausted for chunk size"
    );

    // Any further background allocation of chunk size MUST fail
    let bg_fail = quota_mgr.try_acquire_background_permit(MemoryCategory::OperatorState, chunk);
    assert!(bg_fail.is_err());
    let err_str = bg_fail.unwrap_err().to_string();
    assert!(
        err_str.contains("RS-5003") || err_str.contains("worker-budget-operator_state"),
        "error should indicate worker budget exhaustion: {err_str}"
    );

    // Foreground read allocation MUST SUCCEED from the reserved capacity
    let fg_permit = quota_mgr
        .try_acquire_foreground_permit(MemoryCategory::QueryWorkMemory, 1024 * 1024)
        .expect("foreground read query must succeed using reserved capacity");

    assert_eq!(fg_permit.category(), MemoryCategory::QueryWorkMemory);
    assert_eq!(fg_permit.bytes(), 1024 * 1024);

    // Dropping foreground read returns memory
    drop(fg_permit);
    assert_eq!(ledger.category_bytes(MemoryCategory::QueryWorkMemory), 0);
}

#[tokio::test]
async fn test_foreground_maintenance_commits_during_storage_overload() {
    let budget_bytes = 5 * 1024 * 1024; // 5 MiB
    let reservation = 1024 * 1024; // 1 MiB reserved for foreground/maintenance
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, reservation));
    let quota_mgr = WorkerQuotaManager::with_budget_ledger(ledger.clone());

    // Fill background allocations (write buffers, operator state) up to capacity
    let bg_permit = quota_mgr
        .try_acquire_background_permit(MemoryCategory::SlateDbWriteBuffers, 3 * 1024 * 1024)
        .expect("background write buffers allocation");

    // Background allocation exceeding general pool fails
    let bg_overload =
        quota_mgr.try_acquire_background_permit(MemoryCategory::ExchangeBuffers, 2 * 1024 * 1024);
    assert!(bg_overload.is_err());

    // Foreground maintenance (epoch commit / checkpoint staging) succeeds
    let maintenance_permit = quota_mgr
        .try_acquire_foreground_permit(MemoryCategory::CheckpointStaging, 512 * 1024)
        .expect("foreground checkpoint maintenance commit must succeed");

    assert_eq!(
        ledger.category_bytes(MemoryCategory::CheckpointStaging),
        512 * 1024
    );
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbWriteBuffers),
        3 * 1024 * 1024
    );

    // Release maintenance permit on completion
    drop(maintenance_permit);
    assert_eq!(ledger.category_bytes(MemoryCategory::CheckpointStaging), 0);

    drop(bg_permit);
    assert_eq!(ledger.total_allocated_bytes(), 0);
}

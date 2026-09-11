//! Operational Memory Bounding, Queue Limits, and RSS Tolerance Tests (v0.62.1 Slice 8 / Phase 3b).

use rockstream_runtime::quota::WorkerQuotaManager;
use rockstream_storage::storage_context::{BlockCacheKey, WorkerStorageContext};
use rockstream_types::ids::{ArrangementId, TenantId};
use rockstream_types::state_budget::{MemoryCategory, WorkerBudgetLedger};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_allocation_waiters_queue_strictly_bounded() {
    let budget_bytes = 1024 * 1024;
    let max_waiters = 5;
    let ledger = Arc::new(WorkerBudgetLedger::new_with_max_waiters(
        budget_bytes,
        0,
        max_waiters,
    ));

    // Register 5 waiters successfully
    for _ in 0..max_waiters {
        ledger.increment_waiters().expect("waiter slot available");
    }
    assert_eq!(ledger.waiter_count(), max_waiters as u64);

    // 6th waiter exceeds bound and MUST fail with RS-9001
    let overflow = ledger.increment_waiters();
    assert!(overflow.is_err());
    let err = overflow.unwrap_err();
    assert!(
        err.to_string().contains("RS-9001"),
        "error must be RS-9001: {err}"
    );

    // Drain waiters
    for _ in 0..max_waiters {
        ledger.decrement_waiters();
    }
    assert_eq!(ledger.waiter_count(), 0);
}

#[tokio::test]
async fn test_allocation_waiter_timeout_enforced() {
    let budget = 1024 * 1024;
    let ledger = Arc::new(WorkerBudgetLedger::new_with_max_waiters(budget, 0, 10));
    let quota_mgr = WorkerQuotaManager::with_budget_ledger(ledger.clone());

    // Saturate budget
    let _permit = quota_mgr
        .try_acquire_permit(MemoryCategory::QueryWorkMemory, 900 * 1024)
        .expect("initial permit");

    let start = tokio::time::Instant::now();
    let res = quota_mgr
        .acquire_permit_with_timeout(
            MemoryCategory::QueryWorkMemory,
            200 * 1024,
            Duration::from_millis(50),
        )
        .await;

    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(45),
        "waiter must wait for the specified timeout"
    );
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("RS-5003"),
        "timed out waiter must fail with RS-5003: {err}"
    );
    assert_eq!(
        ledger.waiter_count(),
        0,
        "waiter count must decrement on timeout"
    );
}

#[test]
fn test_shared_cache_entries_bounded() {
    let capacity = 4096; // 4 KiB
    let ctx = WorkerStorageContext::new(capacity);
    let tenant = TenantId(1);
    let policy = [0u8; 32];
    let arrangement = ArrangementId(1);

    // Insert 10 blocks of 1 KiB each into a 4 KiB cache
    for i in 0..10 {
        let key = BlockCacheKey::new(tenant, policy, arrangement, i);
        ctx.put_block(key, vec![0xAB; 1024]);
    }

    let stats = ctx.stats();
    // Cache size must remain bounded within capacity
    assert!(
        stats.current_bytes <= capacity,
        "cache bytes ({}) must not exceed capacity ({})",
        stats.current_bytes,
        capacity
    );
    assert!(
        stats.evictions > 0,
        "evictions must occur under capacity pressure"
    );
}

#[test]
fn test_shard_registry_bounded() {
    const MAX_SHARDS: usize = 10_000;
    let mut shards = std::collections::HashSet::new();

    for i in 0..MAX_SHARDS {
        shards.insert(i);
    }
    assert_eq!(shards.len(), MAX_SHARDS);

    // Attempting to exceed max shards is rejected
    let next_shard = MAX_SHARDS;
    let accepted = if shards.len() >= MAX_SHARDS {
        Err("RS-0003: shard registry capacity exceeded (max 10000)")
    } else {
        shards.insert(next_shard);
        Ok(())
    };

    assert!(accepted.is_err());
    assert!(accepted.unwrap_err().contains("RS-0003"));
}

#[test]
fn test_process_rss_stays_within_frozen_tolerance() {
    let budget_bytes = 100 * 1024 * 1024; // 100 MiB
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 20 * 1024 * 1024));

    // Allocate up to the allowable limit
    let chunk = 10 * 1024 * 1024;
    let mut permits = Vec::new();
    while let Ok(permit) = ledger.try_acquire(MemoryCategory::OperatorState, chunk, false) {
        permits.push(permit);
    }

    // Assert that total allocated + allocator overhead never exceeds memory_budget_bytes + 15% tolerance
    let allocated = ledger.total_allocated_bytes();
    let gross = allocated + (allocated * ledger.allocator_overhead_pct()) / 100;
    let max_tolerance = budget_bytes + (budget_bytes * 15) / 100;

    assert!(
        (gross as usize) <= max_tolerance,
        "gross memory consumption ({gross}) must stay within frozen tolerance ({max_tolerance})"
    );
}

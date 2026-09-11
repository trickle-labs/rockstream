//! Worker Memory Budget Ledger & Category Accounting Tests (v0.62.1 Slice 3 / Phase 3a).

use rockstream_types::state_budget::{MemoryCategory, WorkerBudgetLedger};
use std::sync::Arc;

#[test]
fn test_all_eight_categories_charged_accurately() {
    let budget_bytes = 100 * 1024 * 1024; // 100 MiB
    let foreground_reservation = 20 * 1024 * 1024; // 20 MiB
    let ledger = Arc::new(WorkerBudgetLedger::new(
        budget_bytes,
        foreground_reservation,
    ));

    let categories = MemoryCategory::all();
    assert_eq!(categories.len(), 8);

    let charge_per_cat = 1024 * 1024; // 1 MiB each

    for &cat in categories {
        let permit = ledger
            .try_acquire(cat, charge_per_cat, false)
            .expect("acquire should succeed");
        assert_eq!(ledger.category_bytes(cat), charge_per_cat);
        // Retain permit in scope
        std::mem::forget(permit);
    }

    assert_eq!(ledger.total_allocated_bytes(), 8 * charge_per_cat);

    // Verify each category has exact 1 MiB allocated
    for &cat in categories {
        assert_eq!(ledger.category_bytes(cat), charge_per_cat);
    }
}

#[test]
fn test_shared_cache_charged_exactly_once() {
    let budget_bytes = 50 * 1024 * 1024; // 50 MiB
    let foreground_reservation = 10 * 1024 * 1024;
    let ledger = Arc::new(WorkerBudgetLedger::new(
        budget_bytes,
        foreground_reservation,
    ));

    let cache_size: u64 = 5 * 1024 * 1024; // 5 MiB

    // First charge succeeds
    let first_charge = ledger
        .charge_shared_cache_once(cache_size)
        .expect("charge shared cache");
    assert!(first_charge);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::BlockAndMetadataCaches),
        cache_size
    );

    // Second shard charging the exact same shared cache is a no-op (charged once)
    let second_charge = ledger
        .charge_shared_cache_once(cache_size)
        .expect("second charge should succeed");
    assert!(!second_charge);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::BlockAndMetadataCaches),
        cache_size
    );

    // Third shard also a no-op
    let third_charge = ledger
        .charge_shared_cache_once(cache_size)
        .expect("third charge should succeed");
    assert!(!third_charge);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::BlockAndMetadataCaches),
        cache_size
    );
}

#[test]
fn test_allocator_overhead_headroom_enforced() {
    // 100 MiB total budget, 0 reservation for simplicity
    let budget_bytes = 100 * 1024 * 1024;
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 0));

    // Ledger enforces 10% allocator overhead headroom
    assert_eq!(ledger.allocator_overhead_pct(), 10);

    // If we allocate 90 MiB: 90 MiB + 10% overhead (9 MiB) = 99 MiB <= 100 MiB (succeeds)
    let p1 = ledger.try_acquire(MemoryCategory::OperatorState, 90 * 1024 * 1024, false);
    assert!(p1.is_ok());

    // Next 2 MiB request: 90 + 2 = 92 MiB + 10% overhead (9.2 MiB) = 101.2 MiB > 100 MiB (rejected!)
    let p2 = ledger.try_acquire(MemoryCategory::OperatorState, 2 * 1024 * 1024, false);
    assert!(p2.is_err());
    let err = p2.unwrap_err();
    assert!(err.to_string().contains("RS-5003") || err.to_string().contains("RS-9001"));
}

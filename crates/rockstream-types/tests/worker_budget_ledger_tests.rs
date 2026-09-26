//! Worker Memory Budget Ledger & Category Accounting Tests (v0.62.1 Slice 3 / Phase 3a).

use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::{
    MemoryCategory, MemoryOwner, MemoryOwnerUsage, StateBudgetError, WorkerBudgetLedger,
};
use std::sync::Arc;

#[test]
fn test_all_ten_categories_charged_accurately() {
    let budget_bytes = 100 * 1024 * 1024; // 100 MiB
    let foreground_reservation = 20 * 1024 * 1024; // 20 MiB
    let ledger = Arc::new(WorkerBudgetLedger::new(
        budget_bytes,
        foreground_reservation,
    ));

    let categories = MemoryCategory::all();
    assert_eq!(
        categories,
        &[
            MemoryCategory::SlateDbWriteBuffers,
            MemoryCategory::SlateDbBlockCache,
            MemoryCategory::SlateDbMetadataCache,
            MemoryCategory::OperatorState,
            MemoryCategory::SourceBuffers,
            MemoryCategory::ExchangeBuffers,
            MemoryCategory::QueryWorkMemory,
            MemoryCategory::CatalogCaches,
            MemoryCategory::CheckpointStaging,
            MemoryCategory::MigrationBuffers,
        ]
    );
    assert_eq!(
        categories
            .iter()
            .map(|category| category.name())
            .collect::<Vec<_>>(),
        vec![
            "slatedb_write_buffers",
            "slatedb_block_cache",
            "slatedb_metadata_cache",
            "operator_state",
            "source_buffers",
            "exchange_buffers",
            "query_work_memory",
            "catalog_caches",
            "checkpoint_staging",
            "migration_buffers",
        ]
    );

    let charge_per_cat = 1024 * 1024; // 1 MiB each

    for &cat in categories {
        let permit = ledger
            .try_acquire(cat, charge_per_cat, false)
            .expect("acquire should succeed");
        assert_eq!(ledger.category_bytes(cat), charge_per_cat);
        // Retain permit in scope
        std::mem::forget(permit);
    }

    assert_eq!(ledger.total_allocated_bytes(), 10 * charge_per_cat);

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
    let allocation_id =
        rockstream_types::state_budget::MemoryAllocationId::new("worker-7/block-cache");

    // First charge succeeds
    let first_charge = ledger
        .charge_shared_allocation_once(
            allocation_id.clone(),
            MemoryCategory::SlateDbBlockCache,
            MemoryOwner::worker("worker-storage-context"),
            cache_size,
        )
        .expect("charge shared cache");
    assert!(first_charge);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbBlockCache),
        cache_size
    );
    assert_eq!(
        ledger.status().owners,
        vec![MemoryOwnerUsage {
            category: MemoryCategory::SlateDbBlockCache,
            owner: MemoryOwner::worker("worker-storage-context"),
            bytes: cache_size,
        }]
    );

    // Second shard charging the exact same shared cache is a no-op (charged once)
    let second_charge = ledger
        .charge_shared_allocation_once(
            allocation_id.clone(),
            MemoryCategory::SlateDbBlockCache,
            MemoryOwner::worker("worker-storage-context"),
            cache_size,
        )
        .expect("second charge should succeed");
    assert!(!second_charge);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbBlockCache),
        cache_size
    );

    // Third shard also a no-op
    let third_charge = ledger
        .charge_shared_allocation_once(
            allocation_id,
            MemoryCategory::SlateDbBlockCache,
            MemoryOwner::worker("worker-storage-context"),
            cache_size,
        )
        .expect("third charge should succeed");
    assert!(!third_charge);
    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbBlockCache),
        cache_size
    );
}

#[test]
fn distinct_shared_allocations_with_the_same_owner_and_label_are_both_charged() {
    use rockstream_types::state_budget::MemoryAllocationId;

    let ledger = Arc::new(WorkerBudgetLedger::new(1024, 0));
    let owner = MemoryOwner::worker("worker-7");
    for _ in 0..2 {
        assert!(ledger
            .charge_shared_allocation_once(
                MemoryAllocationId::new("cache"),
                MemoryCategory::SlateDbBlockCache,
                owner.clone(),
                128,
            )
            .unwrap());
    }

    assert_eq!(
        ledger.category_bytes(MemoryCategory::SlateDbBlockCache),
        256
    );
    assert_eq!(ledger.allocated_bytes_for_owner(&owner), 256);
}

#[test]
fn public_budget_status_reports_exact_category_and_workload_owner_bytes() {
    let ledger = Arc::new(WorkerBudgetLedger::new(1024 * 1024, 0));
    let workload = MemoryOwner::workload(WorkloadId(42));
    let _state = ledger
        .try_acquire_for_owner(MemoryCategory::OperatorState, workload.clone(), 2048, false)
        .expect("reserve operator state");
    let _catalog = ledger
        .try_acquire_for_owner(MemoryCategory::CatalogCaches, workload.clone(), 1024, false)
        .expect("reserve catalog cache");
    let _blocks = ledger
        .charge_shared_allocation_once(
            rockstream_types::state_budget::MemoryAllocationId::new("worker-7/block-cache"),
            MemoryCategory::SlateDbBlockCache,
            MemoryOwner::worker("worker-storage-context"),
            512,
        )
        .expect("charge shared block cache");
    let _metadata = ledger
        .charge_shared_allocation_once(
            rockstream_types::state_budget::MemoryAllocationId::new("worker-7/metadata-cache"),
            MemoryCategory::SlateDbMetadataCache,
            MemoryOwner::worker("worker-storage-context"),
            256,
        )
        .expect("charge shared metadata cache");

    let status = ledger.status();
    assert_eq!(status.total_budget_bytes, 1024 * 1024);
    assert_eq!(status.foreground_reservation_bytes, 0);
    assert_eq!(status.allocator_overhead_pct, 10);
    assert_eq!(status.allocated_bytes, 3840);
    assert_eq!(status.allocation_waiter_fill, 0);
    assert_eq!(status.allocation_waiter_capacity, 1024);
    assert_eq!(
        status.categories,
        vec![
            (MemoryCategory::SlateDbWriteBuffers, 0),
            (MemoryCategory::SlateDbBlockCache, 512),
            (MemoryCategory::SlateDbMetadataCache, 256),
            (MemoryCategory::OperatorState, 2048),
            (MemoryCategory::SourceBuffers, 0),
            (MemoryCategory::ExchangeBuffers, 0),
            (MemoryCategory::QueryWorkMemory, 0),
            (MemoryCategory::CatalogCaches, 1024),
            (MemoryCategory::CheckpointStaging, 0),
            (MemoryCategory::MigrationBuffers, 0),
        ]
        .into_iter()
        .map(|(category, bytes)| {
            rockstream_types::state_budget::MemoryCategoryUsage { category, bytes }
        })
        .collect::<Vec<_>>()
    );
    assert_eq!(
        status.owners,
        vec![
            MemoryOwnerUsage {
                category: MemoryCategory::SlateDbBlockCache,
                owner: MemoryOwner::worker("worker-storage-context"),
                bytes: 512,
            },
            MemoryOwnerUsage {
                category: MemoryCategory::SlateDbMetadataCache,
                owner: MemoryOwner::worker("worker-storage-context"),
                bytes: 256,
            },
            MemoryOwnerUsage {
                category: MemoryCategory::OperatorState,
                owner: workload.clone(),
                bytes: 2048,
            },
            MemoryOwnerUsage {
                category: MemoryCategory::CatalogCaches,
                owner: workload,
                bytes: 1024,
            },
        ]
    );

    drop((_state, _catalog));
    let released = ledger.status();
    assert_eq!(released.allocated_bytes, 768);
    assert_eq!(
        released.owners,
        vec![
            MemoryOwnerUsage {
                category: MemoryCategory::SlateDbBlockCache,
                owner: MemoryOwner::worker("worker-storage-context"),
                bytes: 512,
            },
            MemoryOwnerUsage {
                category: MemoryCategory::SlateDbMetadataCache,
                owner: MemoryOwner::worker("worker-storage-context"),
                bytes: 256,
            },
        ]
    );
}

#[test]
fn public_budget_status_reports_exact_allocation_waiter_fill_and_capacity() {
    let ledger = Arc::new(WorkerBudgetLedger::new_with_max_waiters(1024, 0, 2));
    ledger.increment_waiters().unwrap();

    let status = ledger.status();
    assert_eq!(status.allocation_waiter_fill, 1);
    assert_eq!(status.allocation_waiter_capacity, 2);
    ledger.decrement_waiters();
    assert_eq!(ledger.status().allocation_waiter_fill, 0);
}

#[test]
fn workload_hard_limit_admits_exact_bytes_and_rolls_back_rejection() {
    let limit = WorkerBudgetLedger::WORKLOAD_MEMORY_HARD_LIMIT_BYTES;
    let ledger = Arc::new(WorkerBudgetLedger::new(2_147_483_648, 429_496_729));
    let owner = MemoryOwner::workload(WorkloadId(133));
    let permit = ledger
        .try_acquire_for_owner(MemoryCategory::SourceBuffers, owner.clone(), limit, false)
        .expect("the exact workload hard limit must be admitted");
    assert_eq!(
        ledger.status().owners,
        vec![MemoryOwnerUsage {
            category: MemoryCategory::SourceBuffers,
            owner: owner.clone(),
            bytes: limit,
        }]
    );

    let error = ledger
        .try_acquire_for_owner(MemoryCategory::ExchangeBuffers, owner.clone(), 1, false)
        .unwrap_err();
    assert_eq!(
        error,
        StateBudgetError {
            operator_name: "workload-hard-budget-133".to_string(),
            max_bytes: limit,
            current_bytes: limit,
            requested_bytes: 1,
        }
    );
    assert_eq!(ledger.allocated_bytes_for_owner(&owner), limit);
    assert_eq!(ledger.category_bytes(MemoryCategory::ExchangeBuffers), 0);
    drop(permit);
    assert_eq!(ledger.allocated_bytes_for_owner(&owner), 0);
    assert_eq!(ledger.status().allocated_bytes, 0);
}

#[test]
fn concurrent_workload_reservations_share_one_hard_limit() {
    use std::sync::Barrier;
    use std::thread;

    let ledger = Arc::new(WorkerBudgetLedger::new(2_147_483_648, 429_496_729));
    let owner = MemoryOwner::workload(WorkloadId(133));
    let requested = 300_000_000;
    let acquired = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let (tx, rx) = std::sync::mpsc::channel();
    let workers = (0..2)
        .map(|_| {
            let ledger = ledger.clone();
            let owner = owner.clone();
            let acquired = acquired.clone();
            let release = release.clone();
            let tx = tx.clone();
            thread::spawn(move || {
                acquired.wait();
                let result = ledger.try_acquire_for_owner(
                    MemoryCategory::OperatorState,
                    owner,
                    requested,
                    false,
                );
                let observed = result.as_ref().map(|_| ()).map_err(Clone::clone);
                tx.send(observed).unwrap();
                acquired.wait();
                release.wait();
                drop(result);
            })
        })
        .collect::<Vec<_>>();
    drop(tx);
    acquired.wait();
    acquired.wait();
    let results = vec![rx.recv().unwrap(), rx.recv().unwrap()];
    let accepted = results.iter().filter(|result| result.is_ok()).count();
    assert_eq!(accepted, 1);
    let error = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .unwrap();
    assert_eq!(
        error,
        &StateBudgetError {
            operator_name: "workload-hard-budget-133".to_string(),
            max_bytes: WorkerBudgetLedger::WORKLOAD_MEMORY_HARD_LIMIT_BYTES,
            current_bytes: requested,
            requested_bytes: requested,
        }
    );
    assert_eq!(ledger.allocated_bytes_for_owner(&owner), requested);
    release.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(ledger.allocated_bytes_for_owner(&owner), 0);
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

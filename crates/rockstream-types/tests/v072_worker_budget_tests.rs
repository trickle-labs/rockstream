//! Production-ledger regressions for #133; not release-process RSS evidence.
use rockstream_types::state_budget::{MemoryCategory, StateBudgetError, WorkerBudgetLedger};
use std::sync::{Arc, Barrier};

fn snapshot(ledger: &WorkerBudgetLedger) -> (u64, Vec<u64>) {
    (
        ledger.total_allocated_bytes(),
        MemoryCategory::all()
            .iter()
            .map(|category| ledger.category_bytes(*category))
            .collect(),
    )
}

#[test]
fn v072_all_categories_reconcile_after_acquisition_and_drop() {
    let ledger = Arc::new(WorkerBudgetLedger::new(1100, 0));
    let mut permits = Vec::new();
    let mut expected = vec![0; 8];
    for (index, category) in MemoryCategory::all().iter().enumerate() {
        let bytes = index as u64 + 1;
        permits.push(ledger.try_acquire(*category, bytes, false).unwrap());
        expected[index] = bytes;
        assert_eq!(snapshot(&ledger), (expected.iter().sum(), expected.clone()));
    }
    for index in (0..8).rev() {
        drop(permits.pop().unwrap());
        expected[index] = 0;
        assert_eq!(snapshot(&ledger), (expected.iter().sum(), expected.clone()));
    }
    assert_eq!(snapshot(&ledger), (0, vec![0; 8]));
}

#[test]
fn v072_cross_category_admission_cannot_oversubscribe_worker() {
    let ledger = Arc::new(WorkerBudgetLedger::new(1100, 0));
    let barrier = Arc::new(Barrier::new(9));
    let mut observations = Vec::new();
    std::thread::scope(|scope| {
        for category in MemoryCategory::all() {
            let ledger = ledger.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                for _ in 0..128 {
                    barrier.wait();
                    // Hold every successful permit until all attempts are observed.
                    let permit = ledger.try_acquire(*category, 600, false).ok();
                    barrier.wait();
                    barrier.wait();
                    drop(permit);
                    barrier.wait();
                }
            });
        }
        for _ in 0..128 {
            barrier.wait();
            barrier.wait();
            let (total, mut categories) = snapshot(&ledger);
            categories.sort_unstable();
            let available = ledger.available_background_bytes();
            barrier.wait();
            barrier.wait();
            observations.push((total, categories, available, snapshot(&ledger)));
        }
    });
    // Assert after threads finish so a failed assertion cannot strand a barrier.
    assert_eq!(
        observations,
        vec![(600, vec![0, 0, 0, 0, 0, 0, 0, 600], 440, (0, vec![0; 8])); 128]
    );
}

#[test]
fn v072_zero_and_fully_reserved_background_capacity_are_hard_bounds() {
    for (budget, reserved) in [(0, 0), (1000, 1000), (1000, 2000)] {
        let ledger = Arc::new(WorkerBudgetLedger::new(budget, reserved));
        let error = ledger
            .try_acquire(MemoryCategory::MigrationBuffers, 1, false)
            .unwrap_err();
        assert_eq!(
            error,
            StateBudgetError {
                operator_name: "oversized-migration_buffers".to_owned(),
                max_bytes: 0,
                current_bytes: 0,
                requested_bytes: 1,
            }
        );
        let empty = ledger
            .try_acquire(MemoryCategory::MigrationBuffers, 0, false)
            .unwrap();
        drop(empty);
        assert_eq!(snapshot(&ledger), (0, vec![0; 8]));
        if budget != 0 {
            let foreground = ledger
                .try_acquire(MemoryCategory::QueryWorkMemory, 900, true)
                .unwrap();
            assert_eq!(ledger.available_background_bytes(), 0);
            assert_eq!(ledger.available_foreground_bytes(), 10);
            drop(foreground);
            assert_eq!(snapshot(&ledger), (0, vec![0; 8]));
        }
    }
}

#[test]
fn v072_background_pressure_preserves_foreground_reservation() {
    let ledger = Arc::new(WorkerBudgetLedger::new(1100, 220));
    let background = ledger
        .try_acquire(MemoryCategory::MigrationBuffers, 800, false)
        .unwrap();
    assert_eq!(ledger.available_background_bytes(), 0);
    assert_eq!(ledger.available_foreground_bytes(), 220);
    assert_eq!(
        ledger
            .try_acquire(MemoryCategory::SourceBuffers, 1, false)
            .unwrap_err(),
        StateBudgetError {
            operator_name: "worker-budget-source_buffers".to_owned(),
            max_bytes: 880,
            current_bytes: 800,
            requested_bytes: 1,
        }
    );
    let foreground = ledger
        .try_acquire(MemoryCategory::QueryWorkMemory, 200, true)
        .unwrap();
    assert_eq!(snapshot(&ledger), (1000, vec![0, 0, 0, 0, 0, 200, 0, 800]));
    assert_eq!(ledger.available_foreground_bytes(), 0);
    drop(background);
    assert_eq!(snapshot(&ledger), (200, vec![0, 0, 0, 0, 0, 200, 0, 0]));
    assert_eq!(ledger.available_background_bytes(), 660);
    drop(foreground);
    assert_eq!(snapshot(&ledger), (0, vec![0; 8]));
}

#[test]
fn v072_failed_shared_cache_reservation_remains_retryable() {
    let ledger = Arc::new(WorkerBudgetLedger::new(1100, 0));
    let occupied = ledger
        .try_acquire(MemoryCategory::SourceBuffers, 1000, false)
        .unwrap();
    let expected_error = StateBudgetError {
        operator_name: "worker-budget-block_and_metadata_caches".to_owned(),
        max_bytes: 1100,
        current_bytes: 1000,
        requested_bytes: 100,
    };
    for _ in 0..2 {
        assert_eq!(
            ledger.charge_shared_cache_once(100),
            Err(expected_error.clone())
        );
        assert_eq!(snapshot(&ledger), (1000, vec![0, 0, 0, 1000, 0, 0, 0, 0]));
    }
    drop(occupied);
    assert_eq!(ledger.charge_shared_cache_once(100), Ok(true));
    assert_eq!(ledger.charge_shared_cache_once(100), Ok(false));
    assert_eq!(snapshot(&ledger), (100, vec![0, 100, 0, 0, 0, 0, 0, 0]));
}

#[test]
fn v072_concurrent_cache_initializers_never_observe_speculative_success() {
    let ledger = Arc::new(WorkerBudgetLedger::new(1100, 0));
    let occupied = ledger
        .try_acquire(MemoryCategory::SourceBuffers, 1000, false)
        .unwrap();
    let initialize = || {
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16)
                .map(|_| scope.spawn(|| ledger.charge_shared_cache_once(100)))
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(
        initialize(),
        vec![
            Err(StateBudgetError {
                operator_name: "worker-budget-block_and_metadata_caches".to_owned(),
                max_bytes: 1100,
                current_bytes: 1000,
                requested_bytes: 100,
            });
            16
        ]
    );
    drop(occupied);
    let mut outcomes = initialize()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    outcomes.sort_unstable();
    let mut expected = vec![false; 15];
    expected.push(true);
    assert_eq!(outcomes, expected);
    assert_eq!(snapshot(&ledger), (100, vec![0, 100, 0, 0, 0, 0, 0, 0]));
}

#[test]
#[cfg(target_pointer_width = "64")]
fn v072_large_reservations_do_not_overflow_overhead_or_wrap_total() {
    let ledger = Arc::new(WorkerBudgetLedger::new(usize::MAX, 0));
    let bytes = u64::MAX / 2;
    let permit = ledger
        .try_acquire(MemoryCategory::OperatorState, bytes, false)
        .unwrap();
    let gross = u128::from(bytes) + u128::from(bytes) * 10 / 100;
    let expected_available = (u128::from(u64::MAX) - gross) as u64;
    assert_eq!(ledger.available_foreground_bytes(), expected_available);
    assert_eq!(ledger.available_background_bytes(), expected_available);
    assert_eq!(
        ledger
            .try_acquire(MemoryCategory::ExchangeBuffers, u64::MAX, false)
            .unwrap_err(),
        StateBudgetError {
            operator_name: "quota-overflow-worker-memory".to_owned(),
            max_bytes: u64::MAX,
            current_bytes: bytes,
            requested_bytes: u64::MAX,
        }
    );
    assert_eq!(snapshot(&ledger), (bytes, vec![0, 0, bytes, 0, 0, 0, 0, 0]));
    drop(permit);
    assert_eq!(snapshot(&ledger), (0, vec![0; 8]));
}

#[test]
fn v072_empty_category_release_does_not_wrap_or_release_another_owner() {
    let ledger = Arc::new(WorkerBudgetLedger::new(1100, 0));
    let occupied = ledger
        .try_acquire(MemoryCategory::SourceBuffers, 100, false)
        .unwrap();
    ledger.release(MemoryCategory::OperatorState, u64::MAX);
    assert_eq!(snapshot(&ledger), (100, vec![0, 0, 0, 100, 0, 0, 0, 0]));
    drop(occupied);
    ledger.release(MemoryCategory::SourceBuffers, u64::MAX);
    assert_eq!(snapshot(&ledger), (0, vec![0; 8]));
}

#[test]
fn v072_duplicate_waiter_release_never_wraps_or_blocks_admission() {
    let ledger = WorkerBudgetLedger::new_with_max_waiters(1100, 0, 2);

    // A release with no live waiter must not wrap the quota counter.
    ledger.decrement_waiters();
    assert_eq!(ledger.waiter_count(), 0);
    assert_eq!(ledger.increment_waiters(), Ok(()));
    assert_eq!(ledger.increment_waiters(), Ok(()));
    assert_eq!(
        ledger.increment_waiters(),
        Err(StateBudgetError {
            operator_name: "allocation-waiters-queue".to_owned(),
            max_bytes: 2,
            current_bytes: 2,
            requested_bytes: 1,
        })
    );

    ledger.decrement_waiters();
    assert_eq!(ledger.waiter_count(), 1);
    ledger.decrement_waiters();
    ledger.decrement_waiters();
    assert_eq!(ledger.waiter_count(), 0);

    // Capacity remains available after the duplicate release.
    assert_eq!(ledger.increment_waiters(), Ok(()));
    assert_eq!(ledger.waiter_count(), 1);
    ledger.decrement_waiters();
    assert_eq!(ledger.waiter_count(), 0);
}

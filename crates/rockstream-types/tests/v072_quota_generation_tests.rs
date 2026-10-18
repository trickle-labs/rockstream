//! Public-ledger lifecycle regressions for issue #133. Accounting bytes only.
use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::{
    DistributedQuotaLedger, StateBudget, StateBudgetError, WorkloadBudget, WorkloadQuotaLedgerEntry,
};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};

fn usage(entry: &WorkloadQuotaLedgerEntry) -> (u64, u64) {
    (
        entry.current_memory_bytes.load(Ordering::Acquire),
        entry.current_parallelism.load(Ordering::Acquire),
    )
}

#[test]
fn old_reservation_releases_original_entry_after_workload_id_reuse() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let id = WorkloadId(72);
    ledger.register_workload(id, 1000, 4).unwrap();
    let original = ledger.get_entry(id).unwrap();
    let mut old = ledger.try_acquire_batch(id, 120, 2).unwrap();
    ledger.unregister_workload(id);
    ledger.register_workload(id, 1000, 4).unwrap();
    let replacement = ledger.get_entry(id).unwrap();
    let current = ledger.try_acquire_batch(id, 80, 1).unwrap();
    assert_eq!((usage(&original), usage(&replacement)), ((120, 2), (80, 1)));
    old.release();
    old.release();
    drop(old);
    assert_eq!((usage(&original), usage(&replacement)), ((0, 0), (80, 1)));
    drop(current);
    assert_eq!((usage(&original), usage(&replacement)), ((0, 0), (0, 0)));
}

#[test]
fn unregistered_reservation_cannot_release_a_subsequently_registered_workload() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let id = WorkloadId(73);
    let untracked = ledger.try_acquire_batch(id, 100, 4).unwrap();
    ledger.register_workload(id, 1000, 4).unwrap();
    let entry = ledger.get_entry(id).unwrap();
    let current = ledger.try_acquire_batch(id, 80, 1).unwrap();
    drop(untracked);
    assert_eq!(usage(&entry), (80, 1));
    drop(current);
    assert_eq!(usage(&entry), (0, 0));
}

#[test]
fn repeated_generation_churn_releases_every_retired_owner_exactly_once() {
    let ledger = Arc::new(DistributedQuotaLedger::with_capacity(1));
    let id = WorkloadId(74);
    let mut guards = Vec::new();
    let mut entries = Vec::new();
    for bytes in 1..=32 {
        ledger.register_workload(id, 1000, 4).unwrap();
        guards.push(ledger.try_acquire_batch(id, bytes, 1).unwrap());
        entries.push(ledger.get_entry(id).unwrap());
        ledger.unregister_workload(id);
    }
    assert_eq!(
        entries.iter().map(|entry| usage(entry)).collect::<Vec<_>>(),
        (1..=32).map(|bytes| (bytes, 1)).collect::<Vec<_>>()
    );
    drop(guards);
    assert_eq!(
        entries.iter().map(|entry| usage(entry)).collect::<Vec<_>>(),
        vec![(0, 0); 32]
    );
}

#[test]
fn concurrent_registration_preserves_the_single_workload_capacity() {
    for _ in 0..16 {
        let ledger = Arc::new(DistributedQuotaLedger::with_capacity(1));
        let start = Arc::new(Barrier::new(16));
        let outcomes = std::thread::scope(|scope| {
            let tasks = (0..16)
                .map(|id| {
                    let ledger = ledger.clone();
                    let start = start.clone();
                    scope.spawn(move || {
                        start.wait();
                        (
                            WorkloadId(id),
                            ledger.register_workload(WorkloadId(id), 100, 1),
                        )
                    })
                })
                .collect::<Vec<_>>();
            tasks
                .into_iter()
                .map(|task| task.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut normalized = Vec::new();
        for (id, result) in outcomes {
            match result {
                Ok(()) => {
                    assert_eq!(usage(&ledger.get_entry(id).unwrap()), (0, 0));
                    normalized.push(true);
                }
                Err(error) => {
                    assert_eq!(
                        error,
                        StateBudgetError {
                            operator_name: format!("ledger-capacity-{}", id.0),
                            max_bytes: 1,
                            current_bytes: 1,
                            requested_bytes: 1,
                        }
                    );
                    assert!(ledger.get_entry(id).is_none());
                    normalized.push(false);
                }
            }
        }
        normalized.sort_unstable();
        let mut expected = vec![false; 15];
        expected.push(true);
        assert_eq!(normalized, expected);
    }
}

#[test]
fn saturated_operator_release_does_not_remove_sibling_workload_charges() {
    let workload = Arc::new(WorkloadBudget::new(WorkloadId(75), 100));
    let first = StateBudget::new_with_workload("first", 50, Some(workload.clone()));
    let second = StateBudget::new_with_workload("second", 50, Some(workload.clone()));
    first.try_acquire(20).unwrap();
    second.try_acquire(30).unwrap();
    first.release(u64::MAX);
    first.release(u64::MAX);
    assert_eq!(
        (
            first.current_bytes(),
            second.current_bytes(),
            workload.current_bytes()
        ),
        (0, 30, 30)
    );
    second.release(30);
    assert_eq!(
        (
            first.current_bytes(),
            second.current_bytes(),
            workload.current_bytes()
        ),
        (0, 0, 0)
    );
}

#[test]
fn workload_and_operator_counter_overflow_rejects_without_saturating() {
    let workload = WorkloadBudget::new(WorkloadId(76), u64::MAX);
    workload.try_acquire(u64::MAX).unwrap();
    assert_eq!(
        workload.try_acquire(1),
        Err(StateBudgetError {
            operator_name: "quota-overflow-workload-memory".to_owned(),
            max_bytes: u64::MAX,
            current_bytes: u64::MAX,
            requested_bytes: 1,
        })
    );
    assert_eq!(workload.current_bytes(), u64::MAX);
    workload.release(u64::MAX);
    assert_eq!(workload.current_bytes(), 0);
    let operator = StateBudget::new("operator", u64::MAX);
    operator.try_acquire(u64::MAX).unwrap();
    assert_eq!(
        operator.try_acquire(1),
        Err(StateBudgetError {
            operator_name: "quota-overflow-operator-memory".to_owned(),
            max_bytes: u64::MAX,
            current_bytes: u64::MAX,
            requested_bytes: 1,
        })
    );
    assert_eq!(operator.current_bytes(), u64::MAX);
    operator.release(u64::MAX);
    assert_eq!(operator.current_bytes(), 0);
}

#[test]
fn reset_and_drop_release_only_the_original_operator_charge() {
    let workload = Arc::new(WorkloadBudget::new(WorkloadId(77), 1000));
    let first = StateBudget::new_with_workload("first", 800, Some(workload.clone()));
    let second = StateBudget::new_with_workload("second", 800, Some(workload.clone()));

    first.try_acquire(300).unwrap();
    second.try_acquire(400).unwrap();
    assert_eq!(
        (first.current_bytes(), second.current_bytes(), workload.current_bytes()),
        (300, 400, 700)
    );
    first.release(u64::MAX);
    assert_eq!(
        (first.current_bytes(), second.current_bytes(), workload.current_bytes()),
        (0, 400, 400)
    );
    first.try_acquire(100).unwrap();
    first.reset();
    assert_eq!(
        (first.current_bytes(), second.current_bytes(), workload.current_bytes()),
        (0, 400, 400)
    );
    first.try_acquire(100).unwrap();
    drop(first);
    assert_eq!((second.current_bytes(), workload.current_bytes()), (400, 400));
    drop(second);
    assert_eq!(workload.current_bytes(), 0);
}

//! Allocation owners survive workload removal and identifiers may be reused safely.
use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::{
    DistributedQuotaLedger, StateBudgetError, WorkloadQuotaLedgerEntry,
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
fn removed_guard_releases_original_generation_only() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let id = WorkloadId(17);
    ledger.register_workload(id, 100, 4).unwrap();
    let original = ledger.get_entry(id).unwrap();
    let old = ledger.try_acquire_batch(id, 70, 2).unwrap();
    ledger.unregister_workload(id);
    ledger.register_workload(id, 100, 4).unwrap();
    let replacement = ledger.get_entry(id).unwrap();
    let new = ledger.try_acquire_batch(id, 50, 1).unwrap();
    assert!(!Arc::ptr_eq(&original, &replacement));
    assert_eq!((usage(&original), usage(&replacement)), ((70, 2), (50, 1)));
    drop(old);
    assert_eq!((usage(&original), usage(&replacement)), ((0, 0), (50, 1)));
    drop(new);
    assert_eq!((usage(&original), usage(&replacement)), ((0, 0), (0, 0)));
}

#[test]
fn untracked_guard_cannot_release_a_later_registration() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let id = WorkloadId(18);
    let untracked = ledger.try_acquire_batch(id, 99, 3).unwrap();
    ledger.register_workload(id, 100, 4).unwrap();
    let entry = ledger.get_entry(id).unwrap();
    let live = ledger.try_acquire_batch(id, 40, 2).unwrap();
    drop(untracked);
    assert_eq!(usage(&entry), (40, 2));
    drop(live);
    assert_eq!(usage(&entry), (0, 0));
}

#[test]
fn explicit_guard_release_after_removal_is_idempotent() {
    let ledger = Arc::new(DistributedQuotaLedger::new());
    let id = WorkloadId(19);
    ledger.register_workload(id, 100, 4).unwrap();
    let entry = ledger.get_entry(id).unwrap();
    let mut first = ledger.try_acquire_batch(id, 60, 2).unwrap();
    let second = ledger.try_acquire_batch(id, 30, 1).unwrap();
    ledger.unregister_workload(id);
    first.release();
    assert_eq!(usage(&entry), (30, 1));
    first.release();
    drop(first);
    assert_eq!(usage(&entry), (30, 1));
    drop(second);
    assert_eq!(usage(&entry), (0, 0));
    assert!(ledger.get_entry(id).is_none());
}

#[test]
fn concurrent_registration_obeys_capacity_across_map_shards() {
    for _ in 0..32 {
        let ledger = Arc::new(DistributedQuotaLedger::with_capacity(1));
        let start = Arc::new(Barrier::new(9));
        let tasks: Vec<_> = (1..=8)
            .map(|n| {
                let ledger = ledger.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait();
                    (
                        WorkloadId(n),
                        ledger.register_workload(WorkloadId(n), 100, 2),
                    )
                })
            })
            .collect();
        start.wait();
        let outcomes: Vec<_> = tasks.into_iter().map(|t| t.join().unwrap()).collect();
        let winner = outcomes
            .iter()
            .find(|(_, outcome)| outcome.is_ok())
            .unwrap()
            .0;
        for (id, outcome) in outcomes {
            if id == winner {
                assert_eq!(outcome, Ok(()));
                let e = ledger.get_entry(id).unwrap();
                assert_eq!(
                    (
                        e.workload_id,
                        e.memory_limit_bytes.load(Ordering::Acquire),
                        e.max_parallelism.load(Ordering::Acquire),
                        usage(&e)
                    ),
                    (winner, 100, 2, (0, 0))
                );
            } else {
                assert_eq!(
                    outcome,
                    Err(StateBudgetError {
                        operator_name: format!("ledger-capacity-{}", id.0),
                        max_bytes: 1,
                        current_bytes: 1,
                        requested_bytes: 1,
                    })
                );
                assert!(ledger.get_entry(id).is_none());
            }
        }
    }
}

#[test]
fn registry_capacity_is_reclaimed_and_existing_workloads_can_be_updated() {
    let ledger = Arc::new(DistributedQuotaLedger::with_capacity(1));
    let id = WorkloadId(20);
    ledger.register_workload(id, 100, 2).unwrap();
    let entry = ledger.get_entry(id).unwrap();
    ledger.register_workload(id, 200, 3).unwrap();
    assert!(Arc::ptr_eq(&entry, &ledger.get_entry(id).unwrap()));
    assert_eq!(
        (
            entry.memory_limit_bytes.load(Ordering::Acquire),
            entry.max_parallelism.load(Ordering::Acquire),
            usage(&entry)
        ),
        (200, 3, (0, 0))
    );
    ledger.unregister_workload(id);
    ledger.register_workload(WorkloadId(21), 300, 4).unwrap();
    let new = ledger.get_entry(WorkloadId(21)).unwrap();
    assert_eq!(
        (
            new.workload_id,
            new.memory_limit_bytes.load(Ordering::Acquire),
            new.max_parallelism.load(Ordering::Acquire),
            usage(&new)
        ),
        (WorkloadId(21), 300, 4, (0, 0))
    );
    assert!(ledger.get_entry(id).is_none());
}

#[test]
fn operator_release_reset_and_drop_preserve_sibling_charges() {
    use rockstream_types::state_budget::{StateBudget, WorkloadBudget};
    let workload = Arc::new(WorkloadBudget::new(WorkloadId(22), 1000));
    let first = StateBudget::new_with_workload("first", 800, Some(workload.clone()));
    let second = StateBudget::new_with_workload("second", 800, Some(workload.clone()));
    first.try_acquire(300).unwrap();
    second.try_acquire(400).unwrap();
    let snapshot = || {
        (
            first.current_bytes(),
            second.current_bytes(),
            workload.current_bytes(),
        )
    };
    assert_eq!(snapshot(), (300, 400, 700));
    first.release(500);
    assert_eq!(snapshot(), (0, 400, 400));
    first.try_acquire(100).unwrap();
    first.reset();
    assert_eq!(snapshot(), (0, 400, 400));
    first.try_acquire(100).unwrap();
    drop(first);
    assert_eq!(
        (second.current_bytes(), workload.current_bytes()),
        (400, 400)
    );
    drop(second);
    assert_eq!(workload.current_bytes(), 0);
}

#[test]
fn workload_and_operator_overflow_cannot_succeed_via_saturation() {
    use rockstream_types::state_budget::{StateBudget, WorkloadBudget};
    let workload = WorkloadBudget::new(WorkloadId(23), 0);
    workload.try_acquire(u64::MAX).unwrap();
    assert_eq!(
        workload.try_acquire(1),
        Err(StateBudgetError {
            operator_name: "quota-overflow-workload-memory".into(),
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
            operator_name: "quota-overflow-operator-memory".into(),
            max_bytes: u64::MAX,
            current_bytes: u64::MAX,
            requested_bytes: 1,
        })
    );
    assert_eq!(operator.current_bytes(), u64::MAX);
    operator.release(u64::MAX);
    assert_eq!(operator.current_bytes(), 0);
}

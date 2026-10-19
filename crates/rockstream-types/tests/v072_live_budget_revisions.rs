//! Focused production quota-owner safety during effective workload revisions.
use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::{StateBudget, StateBudgetError, WorkloadBudget};
use std::sync::Arc;

#[test]
fn v072_cap_revision_keeps_sibling_reservations_and_fences_new_work() {
    let owner = Arc::new(WorkloadBudget::new(WorkloadId(137), 100));
    let first = StateBudget::new_with_workload("first", 200, Some(owner.clone()));
    let second = StateBudget::new_with_workload("second", 200, Some(owner.clone()));
    first.try_acquire(70).unwrap();

    assert_eq!(
        owner.reconfigure_max_bytes(60),
        Err(StateBudgetError {
            operator_name: "workload-137".to_string(),
            max_bytes: 60,
            current_bytes: 70,
            requested_bytes: 0,
        })
    );
    assert_eq!((owner.max_bytes(), owner.current_bytes()), (100, 70));

    first.release(20);
    owner.reconfigure_max_bytes(60).unwrap();
    assert_eq!(owner.max_bytes(), 60);
    assert_eq!(
        second.try_acquire(20),
        Err(StateBudgetError {
            operator_name: "workload-137".to_string(),
            max_bytes: 60,
            current_bytes: 50,
            requested_bytes: 20,
        })
    );
    assert_eq!(owner.current_bytes(), 50);
    second.try_acquire(10).unwrap();
    assert_eq!(owner.current_bytes(), 60);

    owner.reconfigure_max_bytes(u64::MAX).unwrap();
    second.try_acquire(100).unwrap();
    assert_eq!(owner.current_bytes(), 160);

    first.release(50);
    second.release(110);
    assert_eq!(owner.current_bytes(), 0);
}

#[test]
fn v072_zero_limit_remains_a_real_limit_after_live_reconfiguration() {
    let owner = Arc::new(WorkloadBudget::new(WorkloadId(138), 100));
    let existing = StateBudget::new_with_workload("existing", 200, Some(owner.clone()));
    owner.reconfigure_max_bytes(0).unwrap();
    assert_eq!(owner.max_bytes(), 0);
    assert!(existing.try_acquire(1).is_err());
    assert_eq!(owner.current_bytes(), 0);
    owner.reconfigure_max_bytes(1).unwrap();
    existing.try_acquire(1).unwrap();
    assert_eq!(owner.current_bytes(), 1);
}

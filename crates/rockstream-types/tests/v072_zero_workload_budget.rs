//! A zero-memory workload policy must never become an unlimited state budget.
//! Exact producer-facing ledger regression for parent #132 v2:R2–R3.

use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::{StateBudget, StateBudgetError, WorkloadBudget};
use std::sync::Arc;

#[test]
fn explicit_zero_workload_limit_rejects_positive_reservations() {
    let budget = Arc::new(WorkloadBudget::new(WorkloadId(733), 0));
    let err = budget.try_acquire(1).unwrap_err();
    assert_eq!(
        err,
        StateBudgetError {
            operator_name: "workload-733".to_owned(),
            max_bytes: 0,
            current_bytes: 0,
            requested_bytes: 1,
        }
    );
    assert_eq!(budget.current_bytes(), 0);
    assert!(budget.try_acquire(0).is_ok());
    assert_eq!(budget.current_bytes(), 0);
}

#[test]
fn zero_workload_limit_propagates_through_operator_and_keeps_ledgers_empty() {
    let workload = Arc::new(WorkloadBudget::new(WorkloadId(734), 0));
    let operator =
        StateBudget::new_with_workload("zero-worker", 1_024, Some(Arc::clone(&workload)));
    let err = operator.try_acquire(1).unwrap_err();
    assert_eq!(
        err,
        StateBudgetError {
            operator_name: "workload-734".to_owned(),
            max_bytes: 0,
            current_bytes: 0,
            requested_bytes: 1,
        }
    );
    assert_eq!((operator.current_bytes(), workload.current_bytes()), (0, 0));
    assert!(operator.try_acquire(0).is_ok());
    assert_eq!((operator.current_bytes(), workload.current_bytes()), (0, 0));
}

#[test]
fn nonzero_workload_boundary_preserved_without_overflow() {
    let workload = Arc::new(WorkloadBudget::new(WorkloadId(735), 10));
    let a = StateBudget::new_with_workload("a", 10, Some(Arc::clone(&workload)));
    let b = StateBudget::new_with_workload("b", 10, Some(Arc::clone(&workload)));
    a.try_acquire(7).unwrap();
    b.try_acquire(3).unwrap();
    assert_eq!(
        (
            a.current_bytes(),
            b.current_bytes(),
            workload.current_bytes()
        ),
        (7, 3, 10)
    );
    let err = b.try_acquire(1).unwrap_err();
    assert_eq!(
        err,
        StateBudgetError {
            operator_name: "workload-735".to_owned(),
            max_bytes: 10,
            current_bytes: 10,
            requested_bytes: 1,
        }
    );
    b.release(3);
    a.release(7);
    assert_eq!(
        (
            a.current_bytes(),
            b.current_bytes(),
            workload.current_bytes()
        ),
        (0, 0, 0)
    );
}

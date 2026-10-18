//! v0.72 producer source-pressure accounting and configured boundaries (#133).
use rockstream_runtime::source_pressure::{SourcePressureController, SourcePressureState};
use rockstream_types::state_budget::{MemoryCategory, StateBudgetError, WorkerBudgetLedger};
use std::sync::Arc;

#[test]
fn v072_explicit_soft_hard_pressure_throttles_pauses_and_recovers() {
    let ledger = Arc::new(WorkerBudgetLedger::new(1100, 0));
    let controller = SourcePressureController::new(ledger.clone(), 8)
        .with_limits(600, 950)
        .unwrap();
    assert_eq!(controller.pressure_limits(), (600, 950));
    assert_eq!(controller.pressure_state(), SourcePressureState::Normal);
    assert_eq!(controller.available_credits(), 8);

    let soft = ledger
        .try_acquire(MemoryCategory::SourceBuffers, 550, false)
        .unwrap();
    assert_eq!(ledger.total_allocated_bytes(), 550);
    assert_eq!(controller.pressure_state(), SourcePressureState::Throttled);
    assert_eq!(controller.available_credits(), 4);
    assert_eq!(controller.can_ingest(), Ok(()));

    let hard = ledger
        .try_acquire(MemoryCategory::ExchangeBuffers, 350, false)
        .unwrap();
    assert_eq!(ledger.total_allocated_bytes(), 900);
    assert_eq!(controller.pressure_state(), SourcePressureState::Paused);
    assert_eq!(controller.available_credits(), 0);
    assert_eq!(
        controller.can_ingest(),
        Err(StateBudgetError {
            operator_name: "source_pressure_paused".to_string(),
            max_bytes: 1100,
            current_bytes: 900,
            requested_bytes: 0,
        })
    );
    drop(hard);
    assert_eq!(controller.pressure_state(), SourcePressureState::Throttled);
    assert_eq!(controller.available_credits(), 4);
    drop(soft);
    assert_eq!(controller.pressure_state(), SourcePressureState::Normal);
    assert_eq!(controller.available_credits(), 8);
    assert_eq!(ledger.total_allocated_bytes(), 0);
}

#[test]
fn v072_misconfigured_pressure_thresholds_are_rejected_without_state_mutation() {
    let ledger = Arc::new(WorkerBudgetLedger::new(1100, 0));
    for (soft, hard) in [(0, 950), (900, 900), (950, 900), (600, 1200)] {
        assert_eq!(
            SourcePressureController::new(ledger.clone(), 2)
                .with_limits(soft, hard)
                .unwrap_err(),
            StateBudgetError {
                operator_name: "source-pressure-bounds".to_string(),
                max_bytes: 1100,
                current_bytes: soft,
                requested_bytes: hard,
            }
        );
        assert_eq!(ledger.total_allocated_bytes(), 0);
    }
}

#[test]
fn v072_zero_worker_budget_never_advertises_ingestion_capacity() {
    let ledger = Arc::new(WorkerBudgetLedger::new(0, 0));
    let controller = SourcePressureController::new(ledger, 8);
    assert_eq!(controller.pressure_limits(), (0, 0));
    assert_eq!(controller.utilization_ratio(), f64::INFINITY);
    assert_eq!(controller.pressure_state(), SourcePressureState::Paused);
    assert_eq!(controller.available_credits(), 0);
    assert_eq!(
        controller.can_ingest(),
        Err(StateBudgetError {
            operator_name: "source_pressure_paused".to_string(),
            max_bytes: 0,
            current_bytes: 0,
            requested_bytes: 0,
        })
    );
}

#[test]
#[cfg(target_pointer_width = "64")]
fn v072_near_u64_pressure_accounting_avoids_multiplication_overflow() {
    let ledger = Arc::new(WorkerBudgetLedger::new(usize::MAX, 0));
    let permit = ledger
        .try_acquire(MemoryCategory::SourceBuffers, u64::MAX / 2, false)
        .unwrap();
    let controller = SourcePressureController::new(ledger.clone(), 4);
    assert_eq!(controller.pressure_state(), SourcePressureState::Normal);
    assert!(controller.utilization_ratio() > 0.5);
    assert!(controller.utilization_ratio() < 0.6);
    drop(permit);
    assert_eq!(ledger.total_allocated_bytes(), 0);
    assert_eq!(controller.pressure_state(), SourcePressureState::Normal);
}

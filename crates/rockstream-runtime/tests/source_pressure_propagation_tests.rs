//! Source Pressure Propagation and Credit Reduction Tests (v0.62.1 Slice 7 / Phase 3b).

use rockstream_runtime::source_pressure::{SourcePressureController, SourcePressureState};
use rockstream_types::state_budget::{MemoryCategory, WorkerBudgetLedger};
use std::sync::Arc;

#[test]
fn test_source_credits_reduced_at_eighty_percent() {
    let budget_bytes = 10 * 1024 * 1024; // 10 MiB
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 0));
    let controller = SourcePressureController::new(ledger.clone(), 1000);

    assert_eq!(controller.pressure_state(), SourcePressureState::Normal);
    assert_eq!(controller.available_credits(), 1000);
    assert!(controller.can_ingest().is_ok());

    // Allocate net bytes so gross (allocated + 10% overhead) reaches ~82% (>80% and <95%)
    let net_80 = ((budget_bytes * 82) / 110) as u64;
    let _permit = ledger
        .try_acquire(MemoryCategory::SourceBuffers, net_80, false)
        .expect("allocate to reach 80%");

    assert!(controller.utilization_ratio() >= 0.80);
    assert!(controller.utilization_ratio() < 0.95);
    assert_eq!(controller.pressure_state(), SourcePressureState::Throttled);

    // At 80% pressure, credits must be reduced to 50%
    assert_eq!(controller.available_credits(), 500);
    // Ingestion is throttled but still permitted
    assert!(controller.can_ingest().is_ok());
}

#[test]
fn test_source_paused_at_ninety_five_percent() {
    let budget_bytes = 10 * 1024 * 1024; // 10 MiB
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 0));
    let controller = SourcePressureController::new(ledger.clone(), 1000);

    // Allocate net bytes so gross (allocated + 10% overhead) reaches ~96% (>=95% and <=100%)
    let net_95 = ((budget_bytes * 96) / 110) as u64;
    let _permit = ledger
        .try_acquire(MemoryCategory::SourceBuffers, net_95, false)
        .expect("allocate to reach 95%");

    assert!(controller.utilization_ratio() >= 0.95);
    assert_eq!(controller.pressure_state(), SourcePressureState::Paused);

    // Credits drop to 0
    assert_eq!(controller.available_credits(), 0);

    // can_ingest must return Err with RS-5003 error
    let ingest_res = controller.can_ingest();
    assert!(ingest_res.is_err());
    let err = ingest_res.unwrap_err();
    assert!(err.to_string().contains("RS-5003"));
}

#[test]
fn test_source_resumes_when_pressure_clears() {
    let budget_bytes = 10 * 1024 * 1024; // 10 MiB
    let ledger = Arc::new(WorkerBudgetLedger::new(budget_bytes, 0));
    let controller = SourcePressureController::new(ledger.clone(), 1000);

    // Phase 1: Induce 95% pressure
    let net_95 = ((budget_bytes * 96) / 110) as u64;
    let permit = ledger
        .try_acquire(MemoryCategory::SourceBuffers, net_95, false)
        .expect("reach 95%");
    assert_eq!(controller.pressure_state(), SourcePressureState::Paused);
    assert_eq!(controller.available_credits(), 0);

    // Phase 2: Release memory back to 0% (well below 75% hysteresis threshold)
    drop(permit);
    assert_eq!(ledger.total_allocated_bytes(), 0);
    assert!(controller.utilization_ratio() < 0.75);

    // Pressure clears: state transitions back to Normal, full credits restored
    assert_eq!(controller.pressure_state(), SourcePressureState::Normal);
    assert_eq!(controller.available_credits(), 1000);
    assert!(controller.can_ingest().is_ok());
}

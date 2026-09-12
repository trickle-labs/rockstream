//! Recovery State Machine & Multi-Category Ownership Tests (v0.65 Slice 1 / Phase 3a).
//!
//! Validates traversal through the 6 recovery phases:
//! OpeningStorage -> RecoveringCatalog -> RecoveringEpoch -> RecoveringOperators -> ValidatingState -> Ready
//! and confirms that readiness is false until Ready, and any failure transitions to Fatal (fails closed).

use std::sync::Arc;

use rockstream_runtime::recovery::{RecoveryDriver, RecoveryError};
use rockstream_types::error_code::*;
use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker, RecoveryPhase};

#[test]
fn test_recovery_state_machine_traversal_and_readiness_gate() {
    let lifecycle = Arc::new(LifecycleTracker::new("worker"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    // 1. Initial state must be OpeningStorage, is_ready must be false.
    assert_eq!(driver.phase(), RecoveryPhase::OpeningStorage);
    assert_eq!(
        lifecycle.recovery_phase(),
        Some(RecoveryPhase::OpeningStorage)
    );
    assert_eq!(lifecycle.state(), LifecycleState::Recovering);
    assert!(!driver.is_ready());
    assert!(!lifecycle.is_ready());

    // Illegal skip directly to Ready must fail with RS-0001.
    let err = driver.transition_phase(RecoveryPhase::Ready).unwrap_err();
    assert_eq!(err.code(), RS_0001);
    assert!(err.to_string().contains("RS-0001"));
    assert!(!driver.is_ready());

    // 2. Transition to RecoveringCatalog.
    driver
        .transition_phase(RecoveryPhase::RecoveringCatalog)
        .expect("valid transition");
    assert_eq!(driver.phase(), RecoveryPhase::RecoveringCatalog);
    assert_eq!(
        lifecycle.recovery_phase(),
        Some(RecoveryPhase::RecoveringCatalog)
    );
    assert!(!driver.is_ready());
    assert!(!lifecycle.is_ready());

    // 3. Transition to RecoveringEpoch.
    driver
        .transition_phase(RecoveryPhase::RecoveringEpoch)
        .expect("valid transition");
    assert_eq!(driver.phase(), RecoveryPhase::RecoveringEpoch);
    assert!(!driver.is_ready());

    // 4. Transition to RecoveringOperators.
    driver
        .transition_phase(RecoveryPhase::RecoveringOperators)
        .expect("valid transition");
    assert_eq!(driver.phase(), RecoveryPhase::RecoveringOperators);
    assert!(!driver.is_ready());

    // 5. Transition to ValidatingState.
    driver
        .transition_phase(RecoveryPhase::ValidatingState)
        .expect("valid transition");
    assert_eq!(driver.phase(), RecoveryPhase::ValidatingState);
    assert!(!driver.is_ready());

    // 6. Transition to Ready.
    driver
        .transition_phase(RecoveryPhase::Ready)
        .expect("valid transition");
    assert_eq!(driver.phase(), RecoveryPhase::Ready);
    assert_eq!(lifecycle.state(), LifecycleState::Ready);
    assert!(driver.is_ready());
    assert!(lifecycle.is_ready());
}

#[test]
fn test_opening_storage_failure_fails_fast() {
    let lifecycle = Arc::new(LifecycleTracker::new("worker"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    assert_eq!(driver.phase(), RecoveryPhase::OpeningStorage);
    let err = RecoveryError::StorageError("object store bucket not accessible".to_string());
    assert_eq!(err.code(), RS_3612);
    driver.fail_recovery(&err);

    assert_eq!(lifecycle.state(), LifecycleState::Fatal);
    assert!(!lifecycle.is_ready());
    assert!(!lifecycle.is_alive());
    let (code, ready_resp) = lifecycle.generate_ready_response();
    assert_eq!(code, 503);
    assert_eq!(ready_resp.status, "not_ready");
}

#[test]
fn test_recovering_catalog_failure_blocks_readiness() {
    let lifecycle = Arc::new(LifecycleTracker::new("worker"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    driver
        .transition_phase(RecoveryPhase::RecoveringCatalog)
        .unwrap();
    let err = RecoveryError::CatalogRecoveryFailed("missing catalog snapshot 192".to_string());
    assert_eq!(err.code(), RS_1002);
    driver.fail_recovery(&err);

    assert_eq!(lifecycle.state(), LifecycleState::Fatal);
    assert!(!driver.is_ready());
    assert!(!lifecycle.is_ready());
    let (code, _) = lifecycle.generate_ready_response();
    assert_eq!(code, 503);
}

#[test]
fn test_recovering_epoch_failure_blocks_readiness() {
    let lifecycle = Arc::new(LifecycleTracker::new("worker"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    driver
        .transition_phase(RecoveryPhase::RecoveringCatalog)
        .unwrap();
    driver
        .transition_phase(RecoveryPhase::RecoveringEpoch)
        .unwrap();
    let err = RecoveryError::EpochRecoveryFailed("frontier sequence gap detected".to_string());
    assert_eq!(err.code(), RS_3605);
    driver.fail_recovery(&err);

    assert_eq!(lifecycle.state(), LifecycleState::Fatal);
    assert!(!driver.is_ready());
    assert!(!lifecycle.is_ready());
}

#[test]
fn test_recovering_operators_failure_blocks_readiness() {
    let lifecycle = Arc::new(LifecycleTracker::new("worker"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    driver
        .transition_phase(RecoveryPhase::RecoveringCatalog)
        .unwrap();
    driver
        .transition_phase(RecoveryPhase::RecoveringEpoch)
        .unwrap();
    driver
        .transition_phase(RecoveryPhase::RecoveringOperators)
        .unwrap();
    let err = RecoveryError::OperatorRecoveryFailed("corrupted arrangement key".to_string());
    assert_eq!(err.code(), RS_3610);
    driver.fail_recovery(&err);

    assert_eq!(lifecycle.state(), LifecycleState::Fatal);
    assert!(!driver.is_ready());
}

#[test]
fn test_validating_state_failure_blocks_readiness() {
    let lifecycle = Arc::new(LifecycleTracker::new("worker"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    driver
        .transition_phase(RecoveryPhase::RecoveringCatalog)
        .unwrap();
    driver
        .transition_phase(RecoveryPhase::RecoveringEpoch)
        .unwrap();
    driver
        .transition_phase(RecoveryPhase::RecoveringOperators)
        .unwrap();
    driver
        .transition_phase(RecoveryPhase::ValidatingState)
        .unwrap();
    let err =
        RecoveryError::StateValidationFailed("checksum mismatch across categories".to_string());
    assert_eq!(err.code(), RS_3615);
    driver.fail_recovery(&err);

    assert_eq!(lifecycle.state(), LifecycleState::Fatal);
    assert!(!driver.is_ready());
}

#[test]
fn test_recovery_state_machine_full_transition_to_ready() {
    let lifecycle = Arc::new(LifecycleTracker::new("standalone"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    let phases = [
        RecoveryPhase::OpeningStorage,
        RecoveryPhase::RecoveringCatalog,
        RecoveryPhase::RecoveringEpoch,
        RecoveryPhase::RecoveringOperators,
        RecoveryPhase::ValidatingState,
        RecoveryPhase::Ready,
    ];

    for (idx, &phase) in phases.iter().enumerate() {
        if idx > 0 {
            driver
                .transition_phase(phase)
                .expect("transition must succeed");
        }
        assert_eq!(driver.phase(), phase);
        if phase == RecoveryPhase::Ready {
            assert!(driver.is_ready());
            assert!(lifecycle.is_ready());
            let (code, ready_resp) = lifecycle.generate_ready_response();
            assert_eq!(code, 200);
            assert_eq!(ready_resp.status, "ready");
        } else {
            assert!(!driver.is_ready());
            assert!(!lifecycle.is_ready());
            let (code, ready_resp) = lifecycle.generate_ready_response();
            assert_eq!(code, 503);
            assert_eq!(ready_resp.status, "not_ready");
        }
    }
}

#[test]
fn test_mandatory_recovery_error_fails_closed() {
    let lifecycle = Arc::new(LifecycleTracker::new("worker"));
    let driver = RecoveryDriver::with_lifecycle(lifecycle.clone());

    driver
        .transition_phase(RecoveryPhase::RecoveringCatalog)
        .unwrap();
    // Simulate missing mandatory dependency / corrupted state:
    let err = RecoveryError::CorruptedState("mandatory state file missing".to_string());
    assert_eq!(err.code(), RS_3616);
    driver.fail_recovery(&err);

    // Node must be Fatal, never fallback to empty state or report ready
    assert_eq!(lifecycle.state(), LifecycleState::Fatal);
    assert!(!lifecycle.is_alive());
    assert!(!lifecycle.is_ready());
    assert!(!driver.is_ready());
}

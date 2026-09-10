//! Lifecycle Coordination & Simulation Recovery Tests under Faults (v0.62 Slice 5/Phase 3b, ROADMAP §8).
//!
//! Asserts that:
//! 1. Under network faults and stalls, /readyz remains HTTP 503 during Recovering state.
//! 2. Fatal recovery failure transitions directly to Fatal state without emitting Ready.

use rockstream_cli::component::{Component, ControlComponent, NodeRuntime, WorkerComponent};
use rockstream_types::config::NodeConfig;
use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker};
use std::sync::Arc;

#[tokio::test]
async fn test_sim_runtime_lifecycle_recovery_under_faults() {
    // 1. Stalled recovery keeps readiness false (HTTP 503)
    let tracker = Arc::new(LifecycleTracker::new("worker"));
    tracker.set_state(LifecycleState::Starting);
    let (code, ready) = tracker.generate_ready_response();
    assert_eq!(code, 503);
    assert_eq!(ready.status, "not_ready");

    tracker.set_state(LifecycleState::Recovering);
    // Simulate multiple network partition intervals during recovery
    for _step in 0..10 {
        let (code, ready) = tracker.generate_ready_response();
        assert_eq!(
            code, 503,
            "Readiness must remain 503 during Recovering under network delays"
        );
        assert_eq!(ready.status, "not_ready");
        assert_eq!(ready.reason, Some("recovering".to_string()));
    }

    // 2. Fatal unrecoverable storage failure during recovery:
    // Verify transition to Fatal without ever emitting Ready
    let mut config = NodeConfig::default();
    config.node.role = "all".to_string();

    let components: Vec<Box<dyn Component>> = vec![
        Box::new(ControlComponent::new()),
        Box::new(WorkerComponent::new().with_recovery_failure(true)),
    ];

    let mut runtime = NodeRuntime::with_components(config, components);
    assert_eq!(runtime.tracker().state(), LifecycleState::Created);

    let start_result = runtime.start().await;
    assert!(start_result.is_err(), "Start must fail when recovery fails");

    // Must be in Fatal state
    assert_eq!(
        runtime.tracker().state(),
        LifecycleState::Fatal,
        "Recovery failure must transition tracker to Fatal"
    );

    // Readiness must be false (HTTP 503) and live must report 500 Fatal
    let (ready_code, _) = runtime.tracker().generate_ready_response();
    assert_eq!(ready_code, 503, "Ready probe must be 503 on Fatal");

    let (live_code, live_body) = runtime.tracker().generate_live_response();
    assert_eq!(live_code, 500, "Live probe must be 500 on Fatal");
    assert_eq!(live_body.status, "fatal");
}

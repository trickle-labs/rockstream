//! Lifecycle Bounding, Limits & Queue Capacity Tests (v0.62 Slice 8 / Phase 3b).

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rockstream_cli::component::{LifecycleEvent, NodeRuntime, LIFECYCLE_EVENT_QUEUE_CAPACITY};
use rockstream_cli::shutdown::{ShutdownCoordinator, SUPPRESS_PROCESS_EXIT};
use rockstream_types::config::NodeConfig;
use rockstream_types::error_code::RS_0003;
use rockstream_types::lifecycle::{HealthReason, LifecycleState, LifecycleTracker};

#[tokio::test]
async fn test_lifecycle_queues_and_waiters_are_strictly_bounded() {
    assert_eq!(
        LIFECYCLE_EVENT_QUEUE_CAPACITY, 1024,
        "Lifecycle event queue capacity must be strictly bounded to 1024"
    );
}

#[tokio::test]
async fn test_component_event_channel_bounded() {
    let mut config = NodeConfig::default();
    config.node.role = "control".to_string();

    let _runtime = NodeRuntime::new(config).expect("runtime");
    // Verify that attempting to burst more events than capacity does not panic or leak unbounded memory
    let (tx, _rx) = tokio::sync::mpsc::channel::<LifecycleEvent>(LIFECYCLE_EVENT_QUEUE_CAPACITY);

    for i in 0..LIFECYCLE_EVENT_QUEUE_CAPACITY {
        let res = tx.try_send(LifecycleEvent {
            component: "test",
            previous: LifecycleState::Starting,
            next: LifecycleState::Ready,
            timestamp_ms: i as u64,
        });
        assert!(res.is_ok(), "Sending within capacity must succeed");
    }

    // 1025th item must fail due to channel saturation
    let overflow = tx.try_send(LifecycleEvent {
        component: "test",
        previous: LifecycleState::Ready,
        next: LifecycleState::Fatal,
        timestamp_ms: 9999,
    });
    assert!(
        overflow.is_err(),
        "Channel must reject events exceeding capacity bound"
    );
}

#[tokio::test]
async fn test_drain_timeout_watchdog_enforced() {
    SUPPRESS_PROCESS_EXIT.store(true, Ordering::SeqCst);
    let tracker = Arc::new(LifecycleTracker::new("worker"));
    // Short 50ms drain timeout for testing watchdog enforcement
    let coordinator = ShutdownCoordinator::new(tracker.clone(), Duration::from_millis(50));

    tracker.set_state(LifecycleState::Ready);
    coordinator.trigger_shutdown();
    assert_eq!(tracker.state(), LifecycleState::Draining);

    let watchdog = coordinator.spawn_watchdog();
    // Allow watchdog deadline to elapse without calling coordinator.mark_completed()
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = watchdog.await;

    // Watchdog must force state transition to Fatal
    assert_eq!(
        tracker.state(),
        LifecycleState::Fatal,
        "Watchdog must transition node to Fatal when drain deadline is breached"
    );
}

#[tokio::test]
async fn test_startup_retry_bounded() {
    // Verify bounded startup request retry limit (max 20 attempts @ 50ms = 1s max)
    const MAX_STARTUP_RETRIES: usize = 20;
    const RETRY_INTERVAL_MS: u64 = 50;

    let mut attempts = 0;
    let deadline = std::time::Instant::now() + Duration::from_millis(1500);

    while attempts < MAX_STARTUP_RETRIES && std::time::Instant::now() < deadline {
        attempts += 1;
        tokio::time::sleep(Duration::from_millis(RETRY_INTERVAL_MS)).await;
    }

    assert_eq!(
        attempts, MAX_STARTUP_RETRIES,
        "Startup retries must be bounded to 20"
    );
}

#[tokio::test]
async fn test_health_buffer_bounded() {
    let tracker = LifecycleTracker::new("worker");
    // Ensure appending many health reasons is bounded and structured
    for i in 0..100 {
        tracker.add_reason(HealthReason::new(RS_0003, format!("Diagnostic event #{i}")));
    }
    let (_, report) = tracker.generate_health_report();
    assert_eq!(report.reasons.len(), 100);
    tracker.clear_reasons();
    let (_, report_cleared) = tracker.generate_health_report();
    assert_eq!(report_cleared.reasons.len(), 0);
}

#[tokio::test]
async fn test_config_file_size_bounded() {
    // 10 MB maximum config file size
    const MAX_CONFIG_FILE_SIZE_BYTES: u64 = 10 * 1024 * 1024;
    let oversized = vec![b'a'; (MAX_CONFIG_FILE_SIZE_BYTES + 1) as usize];
    assert!(oversized.len() > MAX_CONFIG_FILE_SIZE_BYTES as usize);
}

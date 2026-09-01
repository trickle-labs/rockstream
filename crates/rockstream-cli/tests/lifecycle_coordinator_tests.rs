//! Lifecycle Coordinator & Watchdog Tests (v0.59.21 Slice 1 / Phase 3a).

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use rockstream_cli::{ShutdownCoordinator, SUPPRESS_PROCESS_EXIT};
use rockstream_types::lifecycle::{LifecycleState, LifecycleTracker};

#[tokio::test]
async fn test_shutdown_coordinator_signal_and_deadline_watchdog() {
    SUPPRESS_PROCESS_EXIT.store(true, Ordering::SeqCst);
    let tracker = Arc::new(LifecycleTracker::new("worker"));
    let coordinator = ShutdownCoordinator::new(tracker.clone(), Duration::from_millis(100));

    // 1. Initial state is Starting
    assert_eq!(tracker.state(), LifecycleState::Starting);
    assert!(!coordinator.is_shutting_down());

    // 2. Transition to Ready
    tracker.set_state(LifecycleState::Ready);
    assert_eq!(tracker.state(), LifecycleState::Ready);
    assert!(tracker.is_ready());

    // 3. Trigger graceful shutdown
    let mut rx = coordinator.subscribe();
    coordinator.trigger_shutdown();
    assert!(coordinator.is_shutting_down());
    assert_eq!(tracker.state(), LifecycleState::Draining);
    assert!(!tracker.is_ready());
    assert!(rx.recv().await.is_ok());

    // 4. Test watchdog timeout triggers self-fencing and Fatal state
    let watchdog = coordinator.spawn_watchdog();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = watchdog.await;
    assert_eq!(tracker.state(), LifecycleState::Fatal);

    // 5. Test clean completion before deadline
    let tracker2 = Arc::new(LifecycleTracker::new("gateway"));
    let coordinator2 = ShutdownCoordinator::new(tracker2.clone(), Duration::from_millis(200));
    coordinator2.trigger_shutdown();
    let watchdog2 = coordinator2.spawn_watchdog();
    tokio::time::sleep(Duration::from_millis(50)).await;
    coordinator2.mark_completed();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _ = watchdog2.await;
    assert_eq!(tracker2.state(), LifecycleState::Terminated);
}

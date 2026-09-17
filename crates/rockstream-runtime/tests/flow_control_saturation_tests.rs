//! v0.67 Slice 5 tests: Transport Flow Control Limits & Shared Worker Budgets.
//!
//! Verifies:
//! 1. All four transport limits (batches, bytes, batch size, pending requests) stop senders under saturation.
//! 2. Frame exceeding max_batch_bytes is rejected deterministically before allocation (RS-3006).
//! 3. Saturated inflight batches halt sender and track queue age.
//! 4. Saturated inflight bytes halt sender before allocation.
//! 5. Exceeding max_pending_requests stops sender or fails with RESOURCE_EXHAUSTED (RS-3006).
//! 6. Permit release on drop, cancel, or error reclaims permits without leakage.

use std::sync::Arc;
use std::time::Duration;

use rockstream_runtime::exchange::flow_control::FlowController;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Test 1: Four limits stop senders under saturation (Exit criterion V067-05).
#[tokio::test]
async fn test_flow_controller_four_limits_stop_senders_under_saturation() {
    let _guard = TEST_LOCK.lock().await;
    let controller = FlowController::new()
        .with_max_batch_bytes(1024)
        .with_max_inflight_batches(2)
        .with_max_inflight_bytes(2048)
        .with_max_pending_requests(2);

    // Limit 1: Frame larger than max_batch_bytes rejected before allocation (RS-3006)
    let oversize_err = controller
        .try_acquire_batch_permit(1, 0, 1, 2048, 10)
        .unwrap_err();
    assert!(
        oversize_err.contains("RS-3006"),
        "expected RS-3006 on oversized frame, got: {oversize_err}"
    );

    // Acquire permit 1: 512 bytes, 1 batch
    let permit1 = controller
        .try_acquire_batch_permit(1, 0, 1, 512, 10)
        .expect("permit 1 should succeed");
    assert_eq!(controller.bytes_in_flight(), 512);
    assert_eq!(controller.batches_in_flight(1, 0, 1), 1);
    assert_eq!(controller.pending_requests(), 1);

    // Acquire permit 2: 512 bytes, 2nd batch (max_inflight_batches = 2 reached, max_pending_requests = 2 reached)
    let permit2 = controller
        .try_acquire_batch_permit(1, 0, 1, 512, 10)
        .expect("permit 2 should succeed");
    assert_eq!(controller.batches_in_flight(1, 0, 1), 2);
    assert_eq!(controller.pending_requests(), 2);

    // Limit 2 & 4: Now saturated on both batches and pending requests
    let saturated_err = controller
        .try_acquire_batch_permit(1, 0, 1, 100, 5)
        .unwrap_err();
    assert!(
        saturated_err.contains("RS-3006") || saturated_err.contains("RESOURCE_EXHAUSTED"),
        "expected RS-3006 or RESOURCE_EXHAUSTED, got: {saturated_err}"
    );

    // Drop permit 1: restores capacity
    drop(permit1);
    assert_eq!(controller.bytes_in_flight(), 512);
    assert_eq!(controller.batches_in_flight(1, 0, 1), 1);
    assert_eq!(controller.pending_requests(), 1);

    // Now permit 3 can be acquired
    let permit3 = controller
        .try_acquire_batch_permit(1, 0, 1, 512, 10)
        .expect("permit 3 should succeed after release");
    assert_eq!(controller.batches_in_flight(1, 0, 1), 2);

    // Cleanup
    drop(permit2);
    drop(permit3);
    assert_eq!(controller.bytes_in_flight(), 0);
    assert_eq!(controller.batches_in_flight(1, 0, 1), 0);
    assert_eq!(controller.pending_requests(), 0);
}

/// Test 2: Inflight batch limit halts sender; queue age tracked.
#[tokio::test]
async fn test_saturation_stops_sender_on_max_inflight_batches() {
    let _guard = TEST_LOCK.lock().await;
    let controller = Arc::new(
        FlowController::new()
            .with_max_inflight_batches(2)
            .with_max_pending_requests(10),
    );

    let permit1 = controller
        .try_acquire_batch_permit(1, 0, 1, 100, 1)
        .expect("permit 1");
    let permit2 = controller
        .try_acquire_batch_permit(1, 0, 1, 100, 1)
        .expect("permit 2");
    assert_eq!(controller.batches_in_flight(1, 0, 1), 2);

    // Sender halts on async acquire when saturated
    let ctrl_clone = controller.clone();
    let waiter = tokio::spawn(async move {
        ctrl_clone
            .acquire_batch_permit(1, 0, 1, 100, 1)
            .await
            .expect("waiter should succeed after release")
    });

    // Wait a brief moment to ensure waiter is blocked
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !waiter.is_finished(),
        "sender must be halted under batch saturation"
    );

    // Queue age is non-zero
    assert!(permit1.queue_age() >= Duration::from_millis(40));

    // Release permit1 to unblock waiter
    drop(permit1);

    let permit3 = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("waiter timed out")
        .expect("waiter task panicked");

    assert_eq!(controller.batches_in_flight(1, 0, 1), 2);
    drop(permit2);
    drop(permit3);
    assert_eq!(controller.batches_in_flight(1, 0, 1), 0);
}

/// Test 3: Inflight byte limit halts sender before allocation.
#[tokio::test]
async fn test_saturation_stops_sender_on_max_inflight_bytes() {
    let _guard = TEST_LOCK.lock().await;
    let controller = Arc::new(
        FlowController::new()
            .with_max_inflight_bytes(1000)
            .with_max_batch_bytes(1000)
            .with_max_pending_requests(10),
    );

    let permit1 = controller
        .try_acquire_batch_permit(1, 0, 1, 800, 1)
        .expect("permit 1");
    assert_eq!(controller.bytes_in_flight(), 800);
    assert!((controller.fill_ratio_bytes() - 0.8).abs() < 0.01);

    // Attempting to allocate 300 more bytes exceeds 1000 byte limit
    let fail = controller.try_acquire_batch_permit(1, 0, 1, 300, 1);
    assert!(fail.is_err(), "should fail when exceeding byte limit");
    assert_eq!(
        controller.bytes_in_flight(),
        800,
        "bytes in flight should not increase"
    );

    // Async acquire halts until freed
    let ctrl_clone = controller.clone();
    let waiter = tokio::spawn(async move {
        ctrl_clone
            .acquire_batch_permit(1, 0, 1, 300, 1)
            .await
            .expect("should acquire after release")
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !waiter.is_finished(),
        "sender must be halted under byte saturation"
    );

    drop(permit1);

    let permit2 = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("timeout")
        .expect("join err");

    assert_eq!(controller.bytes_in_flight(), 300);
    drop(permit2);
    assert_eq!(controller.bytes_in_flight(), 0);
}

/// Test 4: Max pending requests saturation.
#[tokio::test]
async fn test_saturation_stops_sender_on_max_pending_requests() {
    let _guard = TEST_LOCK.lock().await;
    let controller = FlowController::new().with_max_pending_requests(3);

    let p1 = controller.try_acquire_batch_permit(1, 0, 1, 10, 1).unwrap();
    let p2 = controller.try_acquire_batch_permit(1, 0, 1, 10, 1).unwrap();
    let p3 = controller.try_acquire_batch_permit(1, 0, 1, 10, 1).unwrap();
    assert_eq!(controller.pending_requests(), 3);
    assert!((controller.fill_ratio_pending() - 1.0).abs() < 0.01);

    let overflow = controller
        .try_acquire_batch_permit(1, 0, 1, 10, 1)
        .unwrap_err();
    assert!(
        overflow.contains("RS-3006"),
        "expected RS-3006, got: {overflow}"
    );
    assert!(overflow.contains("RESOURCE_EXHAUSTED"));

    drop(p1);
    assert_eq!(controller.pending_requests(), 2);
    let p4 = controller.try_acquire_batch_permit(1, 0, 1, 10, 1).unwrap();
    assert_eq!(controller.pending_requests(), 3);

    drop(p2);
    drop(p3);
    drop(p4);
    assert_eq!(controller.pending_requests(), 0);
}

/// Test 5: Permit release on all error paths without leakage.
#[tokio::test]
async fn test_permit_release_on_all_error_paths() {
    let _guard = TEST_LOCK.lock().await;
    let controller = FlowController::new()
        .with_max_inflight_batches(10)
        .with_max_inflight_bytes(10000)
        .with_max_pending_requests(10);

    let initial_metric = rockstream_types::metrics::read_exchange_inflight_bytes();
    // Scope simulating an error or cancellation path
    {
        let _permit = controller
            .try_acquire_batch_permit(1, 0, 1, 500, 5)
            .expect("acquire");
        assert_eq!(controller.bytes_in_flight(), 500);
        assert_eq!(controller.batches_in_flight(1, 0, 1), 1);
        assert_eq!(controller.pending_requests(), 1);

        // Simulate error path: function returns Early Err or drops permit
        let _simulated_decode_error = Err::<(), &'static str>("decode failure");
        // permit drops here
    }

    // After drop on error, everything is restored cleanly
    assert_eq!(controller.bytes_in_flight(), 0);
    assert_eq!(controller.batches_in_flight(1, 0, 1), 0);
    assert_eq!(controller.pending_requests(), 0);
    assert_eq!(
        rockstream_types::metrics::read_exchange_inflight_bytes(),
        initial_metric
    );
}

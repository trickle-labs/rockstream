//! Bounded and cancellation-safe queued exchange-credit waiters (#133, #132 R6).
use rockstream_runtime::exchange::flow_control::FlowController;
use std::time::Duration;

async fn wait_for_one_waiter(controller: &FlowController) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if controller.queued_credit_waiters() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn fast_row_credit_admission_never_reserves_waiter_capacity() {
    let controller = FlowController::with_row_budget(1).with_max_pending_requests(1);
    controller.acquire_credit(7, 1, 2, 1).await.unwrap();
    assert_eq!(controller.queued_credit_waiters(), 0);
    assert_eq!(controller.get_credits(7, 1, 2), 0);
    assert_eq!(controller.rows_in_flight(7, 1, 2), 1);
    controller.release_credit(7, 1, 2, 1);
    assert_eq!(controller.get_credits(7, 1, 2), 1);
    assert_eq!(controller.rows_in_flight(7, 1, 2), 0);
}

#[tokio::test]
async fn legacy_row_waiter_is_bounded_and_abort_releases_slot() {
    let controller = FlowController::with_row_budget(1).with_max_pending_requests(1);
    controller.acquire_credit(7, 1, 2, 1).await.unwrap();
    let waiting = controller.clone();
    let task = tokio::spawn(async move { waiting.acquire_credit(7, 1, 2, 1).await });
    wait_for_one_waiter(&controller).await;
    assert_eq!(controller.queued_credit_waiters(), 1);
    let error = controller.acquire_credit(7, 1, 2, 1).await.unwrap_err();
    assert!(error.contains("RESOURCE_EXHAUSTED: queued exchange credit waiters"));
    assert_eq!(controller.queued_credit_waiters(), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(controller.queued_credit_waiters(), 0);
    controller.release_credit(7, 1, 2, 1);
    controller.acquire_credit(7, 1, 2, 1).await.unwrap();
    assert_eq!(controller.queued_credit_waiters(), 0);
    controller.release_credit(7, 1, 2, 1);
}

#[tokio::test]
async fn woken_legacy_row_waiter_releases_own_slot() {
    let controller = FlowController::with_row_budget(1).with_max_pending_requests(1);
    controller.acquire_credit(7, 1, 2, 1).await.unwrap();
    let waiting = controller.clone();
    let task = tokio::spawn(async move { waiting.acquire_credit(7, 1, 2, 1).await });
    wait_for_one_waiter(&controller).await;
    controller.release_credit(7, 1, 2, 1);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(controller.queued_credit_waiters(), 0);
    assert_eq!(
        (controller.get_credits(7, 1, 2), controller.rows_in_flight(7, 1, 2)),
        (0, 1)
    );
    controller.release_credit(7, 1, 2, 1);
}

#[tokio::test]
async fn batch_credit_waiters_share_bounded_raii_cleanup() {
    let controller = FlowController::with_row_budget(1).with_max_pending_requests(1);
    let first = controller.try_acquire_batch_permit(7, 1, 2, 64, 1).unwrap();
    assert_eq!(controller.pending_requests(), 1);
    assert_eq!(controller.queued_credit_waiters(), 0);
    let waiting = controller.clone();
    let task = tokio::spawn(async move {
        waiting.acquire_batch_permit(7, 1, 2, 64, 1).await
    });
    wait_for_one_waiter(&controller).await;
    let error = controller.acquire_batch_permit(7, 1, 2, 64, 1).await.unwrap_err();
    assert!(error.contains("RESOURCE_EXHAUSTED: queued exchange credit waiters"));
    assert_eq!(controller.queued_credit_waiters(), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(controller.queued_credit_waiters(), 0);
    drop(first);
    assert_eq!(controller.pending_requests(), 0);
    let next = controller.acquire_batch_permit(7, 1, 2, 64, 1).await.unwrap();
    assert_eq!(controller.queued_credit_waiters(), 0);
    drop(next);
    assert_eq!(controller.pending_requests(), 0);
}

#[tokio::test]
async fn rejecting_unsatisfiable_credit_request_does_not_own_waiter_slot() {
    let controller = FlowController::with_row_budget(1).with_max_pending_requests(1);
    let err = controller.acquire_credit(7, 1, 2, 2).await.unwrap_err();
    assert!(err.contains("RS-3024"));
    let err = controller.acquire_batch_permit(7, 1, 2, 64, 2).await.unwrap_err();
    assert!(err.contains("RS-3024"));
    assert_eq!(controller.queued_credit_waiters(), 0);
    assert_eq!(controller.pending_requests(), 0);
}

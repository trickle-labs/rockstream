//! Exact ledger checks for the production allocation-wait future (issue #133).
use rockstream_runtime::WorkerQuotaManager;
use rockstream_types::state_budget::{MemoryCategory, MemoryPermit, StateBudgetError, WorkerBudgetLedger};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

fn manager() -> WorkerQuotaManager {
    WorkerQuotaManager::with_budget_ledger(Arc::new(WorkerBudgetLedger::new_with_max_waiters(
        110, 0, 1,
    )))
}

fn assert_ledger(manager: &WorkerQuotaManager, waiters: u64, source: u64, query: u64) {
    let ledger = manager.budget_ledger();
    let categories: Vec<_> = MemoryCategory::all()
        .iter()
        .map(|category| (*category, ledger.category_bytes(*category)))
        .collect();
    assert_eq!(
        (ledger.waiter_count(), ledger.total_allocated_bytes(), categories),
        (
            waiters,
            source + query,
            vec![
                (MemoryCategory::SlateDbWriteBuffers, 0),
                (MemoryCategory::BlockAndMetadataCaches, 0),
                (MemoryCategory::OperatorState, 0),
                (MemoryCategory::SourceBuffers, source),
                (MemoryCategory::ExchangeBuffers, 0),
                (MemoryCategory::QueryWorkMemory, query),
                (MemoryCategory::CheckpointStaging, 0),
                (MemoryCategory::MigrationBuffers, 0),
            ],
        )
    );
}

fn assert_pending(future: Pin<&mut impl Future<Output = Result<MemoryPermit, StateBudgetError>>>) {
    assert!(matches!(
        future.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}

#[tokio::test]
async fn v072_unpolled_and_immediate_success_own_no_waiter() {
    let manager = manager();
    let future = manager.acquire_permit_with_timeout(
        MemoryCategory::QueryWorkMemory,
        1,
        Duration::from_secs(1),
    );
    assert_ledger(&manager, 0, 0, 0);
    drop(future);
    assert_ledger(&manager, 0, 0, 0);
    let permit = manager
        .acquire_permit_with_timeout(MemoryCategory::QueryWorkMemory, 1, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!((permit.category(), permit.bytes()), (MemoryCategory::QueryWorkMemory, 1));
    assert_ledger(&manager, 0, 0, 1);
    drop(permit);
    assert_ledger(&manager, 0, 0, 0);
}

#[tokio::test]
async fn v072_cancellation_releases_waiter_without_releasing_live_bytes() {
    let manager = manager();
    let occupied = manager.try_acquire_permit(MemoryCategory::SourceBuffers, 100).unwrap();
    for _ in 0..16 {
        let mut future = Box::pin(manager.acquire_permit_with_timeout(
            MemoryCategory::QueryWorkMemory,
            1,
            Duration::from_secs(60),
        ));
        assert_pending(future.as_mut());
        assert_ledger(&manager, 1, 100, 0);
        drop(future);
        assert_ledger(&manager, 0, 100, 0);
    }
    drop(occupied);
    assert_ledger(&manager, 0, 0, 0);
}

#[tokio::test]
async fn v072_waiter_overflow_does_not_release_another_future_slot() {
    let manager = manager();
    let occupied = manager.try_acquire_permit(MemoryCategory::SourceBuffers, 100).unwrap();
    let mut first = Box::pin(manager.acquire_permit_with_timeout(
        MemoryCategory::QueryWorkMemory,
        1,
        Duration::from_secs(60),
    ));
    assert_pending(first.as_mut());
    let error = manager
        .acquire_permit_with_timeout(MemoryCategory::QueryWorkMemory, 1, Duration::ZERO)
        .await
        .unwrap_err();
    assert_eq!(error, StateBudgetError {
        operator_name: "allocation-waiters-queue".to_owned(),
        max_bytes: 1,
        current_bytes: 1,
        requested_bytes: 1,
    });
    assert_ledger(&manager, 1, 100, 0);
    drop(first);
    assert_ledger(&manager, 0, 100, 0);
    drop(occupied);
    assert_ledger(&manager, 0, 0, 0);
}

#[tokio::test]
async fn v072_timeout_releases_its_slot_and_preserves_full_error() {
    let manager = manager();
    let occupied = manager.try_acquire_permit(MemoryCategory::SourceBuffers, 100).unwrap();
    let error = manager
        .acquire_permit_with_timeout(MemoryCategory::QueryWorkMemory, 1, Duration::ZERO)
        .await
        .unwrap_err();
    assert_eq!(error, StateBudgetError {
        operator_name: "waiter-timeout-query_work_memory".to_owned(),
        max_bytes: 110,
        current_bytes: 100,
        requested_bytes: 1,
    });
    assert_ledger(&manager, 0, 100, 0);
    drop(occupied);
    assert_ledger(&manager, 0, 0, 0);
}

#[tokio::test]
async fn v072_delayed_success_releases_waiter_before_returning_permit() {
    let manager = manager();
    let occupied = manager.try_acquire_permit(MemoryCategory::SourceBuffers, 100).unwrap();
    let mut future = Box::pin(manager.acquire_permit_with_timeout(
        MemoryCategory::QueryWorkMemory,
        1,
        Duration::from_secs(60),
    ));
    assert_pending(future.as_mut());
    assert_ledger(&manager, 1, 100, 0);
    drop(occupied);
    let permit = future.await.unwrap();
    assert_eq!((permit.category(), permit.bytes()), (MemoryCategory::QueryWorkMemory, 1));
    assert_ledger(&manager, 0, 0, 1);
    drop(permit);
    assert_ledger(&manager, 0, 0, 0);
}

#[tokio::test]
async fn v072_task_abort_cleans_up_before_join_completes() {
    let manager = manager();
    let occupied = manager.try_acquire_permit(MemoryCategory::SourceBuffers, 100).unwrap();
    let worker = manager.clone();
    let (started, observed) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut started = Some(started);
        let mut future = Box::pin(worker.acquire_permit_with_timeout(
            MemoryCategory::QueryWorkMemory,
            1,
            Duration::from_secs(60),
        ));
        std::future::poll_fn(|context| {
            let result = future.as_mut().poll(context);
            if result.is_pending() {
                if let Some(started) = started.take() {
                    started.send(()).unwrap();
                }
            }
            result
        }).await
    });
    observed.await.unwrap();
    assert_ledger(&manager, 1, 100, 0);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_ledger(&manager, 0, 100, 0);
    drop(occupied);
    assert_ledger(&manager, 0, 0, 0);
}

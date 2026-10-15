use super::*;
use crate::exchange::flow_control::{FlowController, FlowPermit};
use std::future::Future;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
enum Path {
    Send,
    Batch,
}

fn frame(seq: u64) -> ShuffleFrame {
    ShuffleFrame {
        exchange_id: 7,
        src_shard: 1,
        target_shard: 2,
        epoch: 1,
        seq,
        payload: vec![42],
        row_count: 1,
    }
}

fn waiter(
    controller: &FlowController,
) -> (
    WorkerStreamMultiplexer,
    mpsc::Receiver<ShuffleFrame>,
    Arc<(Barrier, Barrier)>,
) {
    let barriers = Arc::new((Barrier::new(2), Barrier::new(2)));
    let mut controller = controller.clone();
    controller.wait_barriers = Some(barriers.clone());
    let pool = ShuffleClientPool::new(Arc::new(RwLock::new(HashMap::new())));
    let mux = WorkerStreamMultiplexer::new(pool, controller);
    let (tx, rx) = mpsc::channel(2);
    mux.streams.lock().insert(WorkerId(2), tx);
    (mux, rx, barriers)
}

async fn acquire(path: Path, mux: &WorkerStreamMultiplexer, seq: u64) -> Option<FlowPermit> {
    match path {
        Path::Send => {
            mux.send_frame(WorkerId(2), frame(seq)).await.unwrap();
            None
        }
        Path::Batch => Some(
            mux.flow_controller
                .acquire_batch_permit(7, 1, 2, 1, 1)
                .await
                .unwrap(),
        ),
    }
}

fn release(path: Path, controller: &FlowController, permit: Option<FlowPermit>) {
    match path {
        Path::Send => controller.release_credit(7, 1, 2, 1),
        Path::Batch => drop(permit.unwrap()),
    }
}

fn accounting(controller: &FlowController) -> (u32, u32, usize, usize, usize) {
    (
        controller.rows_in_flight(7, 1, 2),
        controller.get_credits(7, 1, 2),
        controller.batches_in_flight(7, 1, 2),
        controller.bytes_in_flight(),
        controller.pending_requests(),
    )
}

fn busy(path: Path) -> (u32, u32, usize, usize, usize) {
    match path {
        Path::Send => (1, 0, 0, 0, 0),
        Path::Batch => (1, 0, 1, 1, 1),
    }
}

async fn pause_after_failed_check<F: Future>(
    mut future: Pin<&mut F>,
    barriers: &(Barrier, Barrier),
) {
    assert!(futures::poll!(future.as_mut()).is_pending());
    barriers.0.wait().await;
    assert!(futures::poll!(future.as_mut()).is_pending());
}

fn assert_frames(path: Path, rx: &mut mpsc::Receiver<ShuffleFrame>, seq: u64) {
    let frames: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert_eq!(
        frames,
        match path {
            Path::Send => vec![frame(seq)],
            Path::Batch => vec![],
        }
    );
}

async fn single_waiter(path: Path, initial_path: Path) {
    let controller = FlowController::with_row_budget(1);
    let (initial_mux, mut initial_rx, _) = waiter(&controller);
    let initial = acquire(initial_path, &initial_mux, 0).await;
    assert_frames(initial_path, &mut initial_rx, 0);
    let mut transcript = vec![("held", false, accounting(&controller))];
    let (mux, mut rx, barriers) = waiter(&controller);
    let pending = acquire(path, &mux, 1);
    tokio::pin!(pending);
    pause_after_failed_check(pending.as_mut(), &barriers).await;
    transcript.push(("blocked", false, accounting(&controller)));

    // Release after the failed check, before the notification future is polled.
    release(initial_path, &controller, initial);
    transcript.push(("released", false, accounting(&controller)));
    barriers.1.wait().await;
    let acquired = timeout(DEADLINE, pending)
        .await
        .expect("lost credit notification");
    transcript.push(("acquired", true, accounting(&controller)));
    assert_frames(path, &mut rx, 1);
    release(path, &controller, acquired);
    transcript.push(("idle", true, accounting(&controller)));

    assert_eq!(
        transcript,
        vec![
            ("held", false, busy(initial_path)),
            ("blocked", false, busy(initial_path)),
            ("released", false, (0, 1, 0, 0, 0)),
            ("acquired", true, busy(path)),
            ("idle", true, (0, 1, 0, 0, 0)),
        ]
    );
}

async fn two_waiters(path: Path) {
    let controller = FlowController::with_row_budget(1);
    let (initial_mux, mut initial_rx, _) = waiter(&controller);
    let initial = acquire(path, &initial_mux, 0).await;
    assert_frames(path, &mut initial_rx, 0);
    let (mux1, mut rx1, barriers1) = waiter(&controller);
    let (mux2, mut rx2, barriers2) = waiter(&controller);
    let first = acquire(path, &mux1, 1);
    let second = acquire(path, &mux2, 2);
    tokio::pin!(first, second);
    pause_after_failed_check(first.as_mut(), &barriers1).await;
    pause_after_failed_check(second.as_mut(), &barriers2).await;
    let mut transcript = vec![("both blocked", [false, false], accounting(&controller))];

    release(path, &controller, initial);
    transcript.push(("released", [false, false], accounting(&controller)));
    barriers1.1.wait().await;
    let acquired1 = timeout(DEADLINE, first)
        .await
        .expect("first waiter stranded");
    transcript.push(("first acquired", [true, false], accounting(&controller)));
    barriers2.1.wait().await;
    pause_after_failed_check(second.as_mut(), &barriers2).await;
    barriers2.1.wait().await;
    assert!(futures::poll!(second.as_mut()).is_pending());
    transcript.push(("second waiting", [true, false], accounting(&controller)));
    assert_frames(path, &mut rx1, 1);
    assert_eq!(rx2.try_recv(), Err(mpsc::error::TryRecvError::Empty));

    release(path, &controller, acquired1);
    transcript.push(("first released", [true, false], accounting(&controller)));
    let acquired2 = timeout(DEADLINE, second)
        .await
        .expect("second waiter stranded");
    transcript.push(("second acquired", [true, true], accounting(&controller)));
    assert_frames(path, &mut rx2, 2);
    release(path, &controller, acquired2);
    transcript.push(("idle", [true, true], accounting(&controller)));

    assert_eq!(
        transcript,
        vec![
            ("both blocked", [false, false], busy(path)),
            ("released", [false, false], (0, 1, 0, 0, 0)),
            ("first acquired", [true, false], busy(path)),
            ("second waiting", [true, false], busy(path)),
            ("first released", [true, false], (0, 1, 0, 0, 0)),
            ("second acquired", [true, true], busy(path)),
            ("idle", [true, true], (0, 1, 0, 0, 0)),
        ]
    );
}

#[tokio::test]
async fn send_frame_release_before_waiter_registration() {
    timeout(DEADLINE * 3, single_waiter(Path::Send, Path::Send))
        .await
        .unwrap();
}

#[tokio::test]
async fn batch_permit_release_before_waiter_registration() {
    timeout(DEADLINE * 3, single_waiter(Path::Batch, Path::Batch))
        .await
        .unwrap();
}

#[tokio::test]
async fn batch_permit_waiter_wakes_on_send_credit_release() {
    timeout(DEADLINE * 3, single_waiter(Path::Batch, Path::Send))
        .await
        .unwrap();
}

#[tokio::test]
async fn send_frame_two_waiters_exact_transcript() {
    timeout(DEADLINE * 3, two_waiters(Path::Send))
        .await
        .unwrap();
}

#[tokio::test]
async fn batch_permit_two_waiters_exact_transcript() {
    timeout(DEADLINE * 3, two_waiters(Path::Batch))
        .await
        .unwrap();
}

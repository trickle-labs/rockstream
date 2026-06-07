//! `OperatorTask` — async Tokio task wrapping one operator instance.
//!
//! Each operator in the pipeline runs as a separate Tokio task.  Data flows
//! through bounded `mpsc` channels, which provide **credit-based backpressure**:
//! the channel capacity is the credit window.  When the channel is full, the
//! upstream task blocks until the downstream consumes a batch, restoring a
//! credit.
//!
//! The task loop:
//! 1. `recv()` from the input channel (blocks when empty).
//! 2. Call `op.process_delta(batch)`.
//! 3. If the output is non-empty, `send()` to the output channel (blocks when
//!    full = credit exhausted = backpressure applied).
//! 4. Repeat until the input channel is closed.
//!
//! A dropped `OperatorTask` does not panic — the channels simply close and the
//! upstream task will observe a closed channel on its next `send`.

use std::sync::Arc;

use crate::op::Operator;
use crate::zset::ArrowZSet;
use tokio::sync::mpsc;
use tracing::error;

/// Maximum number of in-flight batches between two adjacent operators.
///
/// This is the credit window. When the downstream channel is full (N batches
/// queued), the upstream task suspends until at least one is consumed.
/// Named bound (DESIGN.md constraint: every buffer must have a name).
pub const OPERATOR_CHANNEL_CAPACITY: usize = 16;

/// An async Tokio task wrapping one operator instance.
///
/// Each `OperatorTask` owns:
/// - A shared reference to the operator (stateless operators can be `Arc`).
/// - An input channel (receives batches from upstream or the scheduler).
/// - An output channel (sends results downstream or to the view sink).
///
/// The task is consumed by calling `run()`, which drives it to completion.
pub struct OperatorTask {
    /// The operator to invoke for each incoming batch.
    pub op: Arc<dyn Operator>,
    /// Receive channel (bounded — provides backpressure).
    pub input_rx: mpsc::Receiver<ArrowZSet>,
    /// Send channel (bounded — applies backpressure to this task).
    pub output_tx: mpsc::Sender<ArrowZSet>,
    /// Counter: number of gRPC calls this task has issued (must stay 0 in
    /// the embedded profile). Shared with the embedded runtime monitor.
    pub grpc_call_count: Arc<std::sync::atomic::AtomicU64>,
    /// Counter: number of shuffle objects written (must stay 0 in embedded).
    pub shuffle_write_count: Arc<std::sync::atomic::AtomicU64>,
}

impl OperatorTask {
    /// Drive the operator task to completion.
    ///
    /// Returns when the input channel is closed and drained.
    pub async fn run(mut self) {
        while let Some(batch) = self.input_rx.recv().await {
            match self.op.process_delta(batch) {
                Ok(output) => {
                    if !output.is_empty() && self.output_tx.send(output).await.is_err() {
                        // Downstream closed; stop processing.
                        break;
                    }
                }
                Err(e) => {
                    error!(
                        operator = self.op.name(),
                        error = %e,
                        "RS-0001: operator processing error"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use crate::filter::FilterOp;
    use crate::zset::ArrowZSet;
    use rockstream_plan::{BinaryOp, Expr};
    use std::sync::atomic::AtomicU64;

    fn b_gt_5() -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::Column(1)),
            right: Box::new(lit(5)),
        }
    }

    #[tokio::test]
    async fn task_passes_matching_rows() {
        let (in_tx, in_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let task = OperatorTask {
            op: Arc::new(FilterOp::new(b_gt_5())),
            input_rx: in_rx,
            output_tx: out_tx,
            grpc_call_count: Arc::new(AtomicU64::new(0)),
            shuffle_write_count: Arc::new(AtomicU64::new(0)),
        };
        let handle = tokio::spawn(task.run());

        // Send one batch: (a=1, b=3) should be filtered out; (a=2, b=7) passes.
        in_tx
            .send(ArrowZSet::from_ab_rows(&[(1, 3), (2, 7)], 1))
            .await
            .unwrap();
        drop(in_tx); // close input

        handle.await.unwrap();

        let out = out_rx.recv().await.unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.positive_ab_rows(), vec![(2, 7)]);
        assert!(out_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn task_filters_all_out_sends_nothing() {
        let (in_tx, in_rx) = mpsc::channel(16);
        let (out_tx, mut out_rx) = mpsc::channel(16);
        let task = OperatorTask {
            op: Arc::new(FilterOp::new(b_gt_5())),
            input_rx: in_rx,
            output_tx: out_tx,
            grpc_call_count: Arc::new(AtomicU64::new(0)),
            shuffle_write_count: Arc::new(AtomicU64::new(0)),
        };
        let handle = tokio::spawn(task.run());

        in_tx
            .send(ArrowZSet::from_ab_rows(&[(1, 1), (2, 2)], 1))
            .await
            .unwrap();
        drop(in_tx);
        handle.await.unwrap();

        assert!(out_rx.recv().await.is_none());
    }
}

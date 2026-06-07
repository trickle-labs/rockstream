//! Credit-based scheduler for the operator pipeline.
//!
//! The `CreditScheduler` wires a chain of operators together using bounded
//! Tokio `mpsc` channels.  Each channel's capacity is the credit window
//! (see `OPERATOR_CHANNEL_CAPACITY`).  When a downstream channel is full,
//! the upstream task suspends — this is credit-based backpressure.
//!
//! In v0.4 the scheduler handles linear chains only (Source → Op* → Sink).
//! Multi-input operators and Exchange routing arrive in later versions.
//!
//! # Named bounds
//! Every inter-operator channel has capacity `OPERATOR_CHANNEL_CAPACITY`
//! (= 16 batches).  This is the "fill-level upper bound" required by the
//! design's "unbounded accumulation is never acceptable" rule.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use tokio::sync::mpsc::{self, Sender};

use crate::task::{OperatorTask, OPERATOR_CHANNEL_CAPACITY};
use crate::op::Operator;
use crate::zset::ArrowZSet;

/// A linear-chain scheduler.
///
/// Call `push_op` for each operator in pipeline order, then call `build` to
/// obtain the input sender (to inject source batches) and the output receiver
/// (to collect sink results).
pub struct CreditScheduler {
    /// Operators queued in pipeline order.
    ops: Vec<Arc<dyn Operator>>,
    /// Shared gRPC call counter (must stay 0 in embedded mode).
    grpc_call_count: Arc<AtomicU64>,
    /// Shared shuffle-write counter (must stay 0 in embedded mode).
    shuffle_write_count: Arc<AtomicU64>,
}

impl CreditScheduler {
    /// Create a new empty scheduler.
    pub fn new(
        grpc_call_count: Arc<AtomicU64>,
        shuffle_write_count: Arc<AtomicU64>,
    ) -> Self {
        CreditScheduler {
            ops: Vec::new(),
            grpc_call_count,
            shuffle_write_count,
        }
    }

    /// Append an operator at the end of the pipeline chain.
    pub fn push_op(&mut self, op: Arc<dyn Operator>) {
        self.ops.push(op);
    }

    /// Build the pipeline: spawn all operator tasks and return the
    /// (source sender, sink receiver) pair.
    ///
    /// Must be called inside a Tokio runtime.
    pub fn build(self) -> (Sender<ArrowZSet>, mpsc::Receiver<ArrowZSet>) {
        let n = self.ops.len();
        assert!(n > 0, "CreditScheduler: at least one operator required");

        // Create one channel between each pair of adjacent operators plus
        // the source inlet and the sink outlet.
        let (source_tx, mut prev_rx) = mpsc::channel::<ArrowZSet>(OPERATOR_CHANNEL_CAPACITY);
        let (final_tx, final_rx) = mpsc::channel::<ArrowZSet>(OPERATOR_CHANNEL_CAPACITY);

        for (i, op) in self.ops.into_iter().enumerate() {
            let (next_tx, next_rx) = if i + 1 < n {
                mpsc::channel::<ArrowZSet>(OPERATOR_CHANNEL_CAPACITY)
            } else {
                (final_tx.clone(), {
                    // dummy — not used; we'll use final_rx below
                    mpsc::channel::<ArrowZSet>(1).1
                })
            };

            // For the last operator, use the final_tx.
            let out_tx = if i + 1 < n { next_tx } else { final_tx.clone() };
            let in_rx = prev_rx;

            let task = OperatorTask {
                op,
                input_rx: in_rx,
                output_tx: out_tx,
                grpc_call_count: self.grpc_call_count.clone(),
                shuffle_write_count: self.shuffle_write_count.clone(),
            };
            tokio::spawn(task.run());

            prev_rx = if i + 1 < n { next_rx } else {
                // unused for last op; create a dummy
                mpsc::channel::<ArrowZSet>(1).1
            };
        }

        (source_tx, final_rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::FilterOp;
    use crate::project::{NamedExpr, ProjectOp};
    use crate::expr::lit;
    use crate::zset::ArrowZSet;
    use rockstream_plan::{BinaryOp, Expr};
    use std::sync::atomic::AtomicU64;

    /// Build: Filter(b*2 > 10) → Project(a, b*2 AS c)
    fn make_pipeline() -> CreditScheduler {
        let grpc = Arc::new(AtomicU64::new(0));
        let shuf = Arc::new(AtomicU64::new(0));
        let mut sched = CreditScheduler::new(grpc, shuf);

        let predicate = Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(2)),
            }),
            right: Box::new(lit(10)),
        };
        sched.push_op(Arc::new(FilterOp::new(predicate)));

        let project = ProjectOp::new(vec![
            NamedExpr::new("a", Expr::Column(0)),
            NamedExpr::new("c", Expr::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(2)),
            }),
        ]);
        sched.push_op(Arc::new(project));
        sched
    }

    #[tokio::test]
    async fn scheduler_end_to_end() {
        let sched = make_pipeline();
        let (source_tx, mut sink_rx) = sched.build();

        // b=3 → b*2=6 ≤ 10 → filtered out
        // b=6 → b*2=12 > 10 → passes, c=12
        source_tx
            .send(ArrowZSet::from_ab_rows(&[(1, 3), (2, 6)], 1))
            .await
            .unwrap();
        drop(source_tx);

        let out = sink_rx.recv().await.unwrap();
        assert_eq!(out.num_rows(), 1);
        let a_col = out.data.column(0).as_any()
            .downcast_ref::<arrow::array::Int64Array>().unwrap();
        let c_col = out.data.column(1).as_any()
            .downcast_ref::<arrow::array::Int64Array>().unwrap();
        assert_eq!(a_col.value(0), 2);
        assert_eq!(c_col.value(0), 12);
    }
}

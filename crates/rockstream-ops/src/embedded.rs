//! Embedded single-process runtime profile (v0.4).
//!
//! `EmbeddedRuntime` wires control, worker, and gateway in-process.
//! In this profile the hot path issues **zero gRPC calls** and creates
//! **zero shuffle objects**.  Both properties are enforced by atomic counters
//! that the proof test reads after pipeline execution.
//!
//! The embedded profile is activated by `rockstream start --role=all` (or the
//! equivalent `--role=embedded`).  It is the only execution model in v0.4;
//! distributed execution over gRPC arrives in v0.16.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Counters for the embedded runtime invariants.
///
/// After running a pipeline in embedded mode:
/// - `grpc_call_count()` must return 0.
/// - `shuffle_write_count()` must return 0.
#[derive(Clone, Default)]
pub struct EmbeddedCounters {
    grpc_calls: Arc<AtomicU64>,
    shuffle_writes: Arc<AtomicU64>,
}

impl EmbeddedCounters {
    pub fn new() -> Self {
        EmbeddedCounters {
            grpc_calls: Arc::new(AtomicU64::new(0)),
            shuffle_writes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Total gRPC calls issued by the embedded runtime (must be 0).
    pub fn grpc_call_count(&self) -> u64 {
        self.grpc_calls.load(Ordering::Relaxed)
    }

    /// Total shuffle objects written by the embedded runtime (must be 0).
    pub fn shuffle_write_count(&self) -> u64 {
        self.shuffle_writes.load(Ordering::Relaxed)
    }

    /// Arc to the gRPC counter (passed to `OperatorTask`).
    pub fn grpc_arc(&self) -> Arc<AtomicU64> {
        self.grpc_calls.clone()
    }

    /// Arc to the shuffle counter (passed to `OperatorTask`).
    pub fn shuffle_arc(&self) -> Arc<AtomicU64> {
        self.shuffle_writes.clone()
    }
}

/// The embedded single-process runtime.
///
/// Provides a `CreditScheduler` pre-wired with the shared counters so that
/// any operator that accidentally issues a gRPC call or writes a shuffle
/// object will be caught immediately.
pub struct EmbeddedRuntime {
    pub counters: EmbeddedCounters,
}

impl EmbeddedRuntime {
    /// Create a new embedded runtime.
    pub fn new() -> Self {
        EmbeddedRuntime {
            counters: EmbeddedCounters::new(),
        }
    }

    /// Build a `CreditScheduler` pre-wired to this runtime's counters.
    pub fn make_scheduler(&self) -> crate::scheduler::CreditScheduler {
        crate::scheduler::CreditScheduler::new(
            self.counters.grpc_arc(),
            self.counters.shuffle_arc(),
        )
    }
}

impl Default for EmbeddedRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use crate::filter::FilterOp;
    use crate::project::{NamedExpr, ProjectOp};
    use crate::source::VecDeltaSource;
    use crate::zset::ArrowZSet;
    use rockstream_plan::{BinaryOp, Expr};
    use std::sync::Arc;

    /// Proof: embedded hot path issues zero gRPC calls and creates zero shuffle
    /// objects.
    #[tokio::test]
    async fn embedded_hot_path_zero_grpc_zero_shuffle() {
        let rt = EmbeddedRuntime::new();
        let mut sched = rt.make_scheduler();

        // Filter(b*2 > 10) → Project(a, b*2 AS c)
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
        sched.push_op(Arc::new(ProjectOp::new(vec![
            NamedExpr::new("a", Expr::Column(0)),
            NamedExpr::new(
                "c",
                Expr::BinaryOp {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Column(1)),
                    right: Box::new(lit(2)),
                },
            ),
        ])));

        let (source_tx, mut sink_rx) = sched.build();

        // Run a source through the pipeline.
        let batches = vec![
            ArrowZSet::from_ab_rows(&[(1, 3), (2, 6)], 1),
            ArrowZSet::from_ab_rows(&[(3, 8), (4, 1)], 1),
        ];
        VecDeltaSource::new(batches).run(source_tx).await;

        // Drain output.
        while sink_rx.recv().await.is_some() {}

        // ── Proof assertions ───────────────────────────────────────────────
        assert_eq!(
            rt.counters.grpc_call_count(),
            0,
            "embedded hot path must issue zero gRPC calls"
        );
        assert_eq!(
            rt.counters.shuffle_write_count(),
            0,
            "embedded hot path must create zero shuffle objects"
        );
    }

    #[test]
    fn counters_start_at_zero() {
        let rt = EmbeddedRuntime::new();
        assert_eq!(rt.counters.grpc_call_count(), 0);
        assert_eq!(rt.counters.shuffle_write_count(), 0);
    }
}

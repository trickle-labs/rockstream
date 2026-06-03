//! Operator trait definition.
//!
//! All IVM operators implement this trait. The trait works with `ZSetBatch`
//! (the delta unit) and carries a merge-law annotation for `EXPLAIN`.

use std::sync::Arc;
use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch, ZSetBatch};
use rockstream_types::merge_law::MergeLawId;
use rockstream_types::timestamp::Epoch;
use rockstream_types::ids::WorkloadId;
use rockstream_types::state_budget::StateBudgetMeter;

/// Context provided to each operator instance containing workload and budgeting info.
#[derive(Clone)]
pub struct OperatorContext {
    pub workload_id: WorkloadId,
    pub state_budget: Option<Arc<StateBudgetMeter>>,
}

impl OperatorContext {
    pub fn new(workload_id: WorkloadId, state_budget: Option<Arc<StateBudgetMeter>>) -> Self {
        Self {
            workload_id,
            state_budget,
        }
    }
}

/// Metrics collected from a single operator instance.
#[derive(Debug, Clone, Default)]
pub struct OperatorMetrics {
    pub rows_processed: u64,
    pub state_read_count: u64,
    pub rmw_avoided: bool,
    pub p99_latency_ms: f64,
}

/// Tuning hints sent to an operator to reconfigure its behavior.
#[derive(Debug, Clone, Default)]
pub struct OperatorHints {
    // Placeholders for future auto-tuner integration
}

/// Outcome of a reconfiguration attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconfigOutcome {
    Success,
    NoOp,
    Unsupported,
}

/// Trait that all operators must implement.
#[async_trait]
pub trait Operator: Send {
    /// Initialize or update the operator's context.
    fn set_context(&mut self, _ctx: OperatorContext) {}

    /// Process an input batch and produce an output batch.
    async fn process(&mut self, input: &SourceBatch) -> SinkBatch;

    /// Process a Z-set delta and produce an output delta.
    ///
    /// This is the primary IVM interface. Operators receive incremental
    /// changes and produce incremental changes.
    async fn process_delta(&mut self, input: &ZSetBatch) -> ZSetBatch {
        // Default: pass through (override in real operators)
        input.clone()
    }

    /// Called when an epoch is complete.
    async fn epoch_complete(&mut self, epoch: Epoch);

    /// Name of this operator for diagnostics.
    fn name(&self) -> &str;

    /// The merge law this operator uses (if any).
    /// Used for `EXPLAIN INCREMENTAL` annotations.
    fn merge_law(&self) -> Option<MergeLawId> {
        None
    }

    /// Snapshot the current execution metrics for this operator.
    fn snapshot_metrics(&self) -> OperatorMetrics {
        OperatorMetrics::default()
    }

    /// Reconfigure the operator's tuning parameters dynamically.
    fn reconfigure(&mut self, _hints: OperatorHints) -> ReconfigOutcome {
        ReconfigOutcome::NoOp
    }

    /// Return the active state size in bytes currently managed by this operator.
    fn state_bytes(&self) -> u64 {
        0
    }
}

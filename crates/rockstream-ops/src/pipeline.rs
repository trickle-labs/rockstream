//! Pipeline: compose a source → operators → sink for synchronous execution.
//!
//! `LinearPipeline` is a simple composable pipeline that processes delta
//! batches synchronously (no Tokio tasks).  It is used by the oracle harness
//! and unit tests to run a sequence of operators against a list of input
//! batches.
//!
//! For the async, credit-based execution path, see `CreditScheduler` and
//! `OperatorTask`.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::OpError;
use crate::op::Operator;
use crate::zset::ArrowZSet;

/// A synchronous linear pipeline: `[op_0, op_1, ..., op_n]`.
///
/// The pipeline applies each operator in order to every input batch.
/// It returns the accumulated output Z-set as a `BTreeMap<(i64,i64), i64>`.
pub struct LinearPipeline {
    stages: Vec<Arc<dyn Operator>>,
}

impl LinearPipeline {
    /// Create a new empty pipeline.
    pub fn new() -> Self {
        LinearPipeline { stages: Vec::new() }
    }

    /// Append an operator at the end of the pipeline.
    pub fn push(mut self, op: Arc<dyn Operator>) -> Self {
        self.stages.push(op);
        self
    }

    /// Process a single batch through all stages and return the result.
    pub fn process(&self, batch: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let mut current = batch;
        for stage in &self.stages {
            current = stage.process_delta(current)?;
        }
        Ok(current)
    }

    /// Accumulate all batches from `input_epochs` and return the output
    /// accumulated Z-set as a `BTreeMap<(a,b), weight>`.
    ///
    /// Only works with `{a: Int64, b: Int64}` output schemas (used in tests).
    pub fn accumulate_ab(
        &self,
        input_epochs: &[ArrowZSet],
    ) -> Result<BTreeMap<(i64, i64), i64>, OpError> {
        let mut acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        for input_batch in input_epochs {
            let output = self.process(input_batch.clone())?;
            output.accumulate_ab(&mut acc);
        }
        Ok(acc)
    }

    /// Return total state bytes across all stages in this pipeline.
    pub fn state_bytes(&self) -> u64 {
        self.stages.iter().map(|s| s.state_bytes()).sum()
    }
}

impl Default for LinearPipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Tracks timestamps and decomposed latency across pipeline execution stages.
#[derive(Debug, Clone)]
pub struct StageTimestampTracker {
    view_name: String,
    history: Arc<std::sync::Mutex<Vec<rockstream_types::metrics::StageTimestamps>>>,
    max_history: usize,
    spill_delay_accum_ms: Arc<std::sync::atomic::AtomicU64>,
    storage_pressure_accum_ms: Arc<std::sync::atomic::AtomicU64>,
}

impl StageTimestampTracker {
    /// Create a new stage timestamp tracker for a view.
    pub fn new(view_name: impl Into<String>) -> Self {
        Self {
            view_name: view_name.into(),
            history: Arc::new(std::sync::Mutex::new(Vec::new())),
            max_history: rockstream_types::metrics::MAX_STAGE_TIMESTAMPS_PER_VIEW,
            spill_delay_accum_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            storage_pressure_accum_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn view_name(&self) -> &str {
        &self.view_name
    }

    pub fn record_spill_delay(&self, delay_ms: u64) {
        self.spill_delay_accum_ms
            .fetch_add(delay_ms, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_storage_pressure_delay(&self, delay_ms: u64) {
        self.storage_pressure_accum_ms
            .fetch_add(delay_ms, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn track_batch(
        &self,
        t_source_ms: u64,
        t_ingest_ms: u64,
        t_decode_ms: u64,
        t_compute_ms: u64,
        t_align_ms: u64,
        t_sink_ms: u64,
    ) -> rockstream_types::metrics::StageLagBreakdown {
        let spill_delay = self
            .spill_delay_accum_ms
            .swap(0, std::sync::atomic::Ordering::Relaxed);
        let storage_delay = self
            .storage_pressure_accum_ms
            .swap(0, std::sync::atomic::Ordering::Relaxed);

        let timestamps = rockstream_types::metrics::StageTimestamps {
            t_source_ms,
            t_ingest_ms,
            t_decode_ms,
            t_compute_ms,
            t_align_ms,
            t_sink_ms,
            spill_delay_ms: spill_delay,
            storage_delay_ms: storage_delay,
        };

        let breakdown = timestamps.compute_breakdown();

        let mut history = self.history.lock().unwrap();
        if history.len() >= self.max_history {
            history.remove(0);
        }
        history.push(timestamps);

        rockstream_types::metrics::record_stage_event(&self.view_name, timestamps);
        breakdown
    }

    pub fn current_breakdown(&self) -> Option<rockstream_types::metrics::StageLagBreakdown> {
        let history = self.history.lock().unwrap();
        history.last().map(|ts| ts.compute_breakdown())
    }

    pub fn running_average_breakdown(
        &self,
    ) -> Option<rockstream_types::metrics::StageLagBreakdown> {
        let history = self.history.lock().unwrap();
        if history.is_empty() {
            return None;
        }
        let count = history.len() as u64;
        let mut sum_src = 0u64;
        let mut sum_dec = 0u64;
        let mut sum_comp = 0u64;
        let mut sum_align = 0u64;
        let mut sum_sink = 0u64;
        let mut sum_spill = 0u64;
        let mut sum_storage = 0u64;
        let mut sum_total = 0u64;

        for ts in history.iter() {
            let b = ts.compute_breakdown();
            sum_src += b.source_lag_ms;
            sum_dec += b.decode_lag_ms;
            sum_comp += b.compute_lag_ms;
            sum_align += b.alignment_lag_ms;
            sum_sink += b.sink_lag_ms;
            sum_spill += b.spill_lag_ms;
            sum_storage += b.storage_pressure_ms;
            sum_total += b.total_lag_ms;
        }

        Some(rockstream_types::metrics::StageLagBreakdown {
            source_lag_ms: sum_src / count,
            decode_lag_ms: sum_dec / count,
            compute_lag_ms: sum_comp / count,
            alignment_lag_ms: sum_align / count,
            sink_lag_ms: sum_sink / count,
            spill_lag_ms: sum_spill / count,
            storage_pressure_ms: sum_storage / count,
            total_lag_ms: sum_total / count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use crate::filter::FilterOp;
    use crate::project::{NamedExpr, ProjectOp};
    use rockstream_plan::{BinaryOp, Expr};

    fn make_filter_project() -> LinearPipeline {
        let predicate = Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(2)),
            }),
            right: Box::new(lit(10)),
        };
        let project = ProjectOp::new(vec![
            NamedExpr::new("a", Expr::Column(0)),
            NamedExpr::new(
                "c",
                Expr::BinaryOp {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Column(1)),
                    right: Box::new(lit(2)),
                },
            ),
        ]);
        LinearPipeline::new()
            .push(Arc::new(FilterOp::new(predicate)))
            .push(Arc::new(project))
    }

    #[test]
    fn pipeline_end_to_end() {
        let pipeline = make_filter_project();
        // b=3 → b*2=6 ≤ 10 → filtered; b=7 → b*2=14 > 10 → passes with c=14
        let input = ArrowZSet::from_ab_rows(&[(1, 3), (2, 7)], 1);
        let out = pipeline.process(input).unwrap();
        assert_eq!(out.num_rows(), 1);
        let acc = {
            let mut m = BTreeMap::new();
            out.accumulate_ab(&mut m);
            m
        };
        assert!(acc.contains_key(&(2, 14)));
    }

    #[test]
    fn pipeline_accumulate_two_epochs() {
        let pipeline = make_filter_project();
        let epoch1 = ArrowZSet::from_ab_rows(&[(1, 6)], 1); // b*2=12 > 10 ✓
        let epoch2 = ArrowZSet::from_ab_rows(&[(2, 3)], 1); // b*2=6 ≤ 10 ✗
        let acc = pipeline.accumulate_ab(&[epoch1, epoch2]).unwrap();
        assert_eq!(acc.len(), 1);
        assert_eq!(acc[&(1, 12)], 1); // projected: a=1, c=6*2=12
    }

    #[test]
    fn test_stage_timestamp_propagation_through_pipeline() {
        rockstream_types::metrics::reset_all();
        let tracker = StageTimestampTracker::new("mv_orders");

        tracker.record_spill_delay(3);
        tracker.record_storage_pressure_delay(1);

        let breakdown = tracker.track_batch(1000, 1010, 1015, 1030, 1035, 1050);

        assert_eq!(breakdown.source_lag_ms, 10);
        assert_eq!(breakdown.decode_lag_ms, 5);
        assert_eq!(breakdown.compute_lag_ms, 15);
        assert_eq!(breakdown.alignment_lag_ms, 5);
        assert_eq!(breakdown.sink_lag_ms, 15);
        assert_eq!(breakdown.spill_lag_ms, 3);
        assert_eq!(breakdown.storage_pressure_ms, 1);
        assert_eq!(breakdown.total_lag_ms, 54);

        let current = tracker.current_breakdown().unwrap();
        assert_eq!(current, breakdown);

        let avg = tracker.running_average_breakdown().unwrap();
        assert_eq!(avg, breakdown);

        let from_registry = rockstream_types::metrics::read_view_stage_lag("mv_orders").unwrap();
        assert_eq!(from_registry, breakdown);
    }
}

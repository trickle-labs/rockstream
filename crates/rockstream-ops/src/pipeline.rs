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
}

//! Filter operator: stateless Z-set filter.
//!
//! `FilterOp` applies a boolean predicate to each row of an `ArrowZSet`.
//! Rows where the predicate is `false` are dropped; all others pass through
//! with their original weights unchanged.
//!
//! DBSP linear-operator rule: for a filter function `F`,
//! `ΔF(Δx) = F(Δx)`. The filter is already incremental — just apply it to
//! each incoming delta batch.

use rockstream_plan::Expr;

use crate::error::OpError;
use crate::expr::eval_bool;
use crate::op::Operator;
use crate::zset::ArrowZSet;

/// A stateless filter operator.
pub struct FilterOp {
    /// The boolean predicate to evaluate against each row.
    predicate: Expr,
}

impl FilterOp {
    /// Create a new filter operator with the given predicate.
    pub fn new(predicate: Expr) -> Self {
        FilterOp { predicate }
    }

    /// Apply the filter to a single delta batch.
    pub fn apply(&self, input: ArrowZSet) -> Result<ArrowZSet, OpError> {
        if input.is_empty() {
            return Ok(input);
        }
        let mask = eval_bool(&self.predicate, &input.data)?;
        let indices: Vec<usize> = mask
            .iter()
            .enumerate()
            .filter(|(_, b)| **b)
            .map(|(i, _)| i)
            .collect();
        if indices.len() == input.num_rows() {
            // All rows pass — return unchanged.
            return Ok(input);
        }
        input.select_rows(&indices)
    }
}

impl Operator for FilterOp {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.apply(delta)
    }

    fn name(&self) -> &str {
        "FilterOp"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use rockstream_plan::{BinaryOp, Expr};

    /// `b * 2 > 10` predicate.
    fn b_times_2_gt_10() -> Expr {
        Expr::BinaryOp {
            op: BinaryOp::Gt,
            left: Box::new(Expr::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(2)),
            }),
            right: Box::new(lit(10)),
        }
    }

    #[test]
    fn filter_keeps_passing_rows() {
        // b*2 > 10 means b > 5
        let input = ArrowZSet::from_ab_rows(&[(1, 3), (2, 6), (3, 8)], 1);
        let op = FilterOp::new(b_times_2_gt_10());
        let out = op.apply(input).unwrap();
        assert_eq!(out.num_rows(), 2);
        let rows = out.positive_ab_rows();
        assert!(rows.contains(&(2, 6)));
        assert!(rows.contains(&(3, 8)));
    }

    #[test]
    fn filter_drops_all_rows() {
        let input = ArrowZSet::from_ab_rows(&[(1, 1), (2, 2)], 1);
        let op = FilterOp::new(b_times_2_gt_10());
        let out = op.apply(input).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn filter_passes_all_rows() {
        let input = ArrowZSet::from_ab_rows(&[(1, 6), (2, 7)], 1);
        let op = FilterOp::new(b_times_2_gt_10());
        let out = op.apply(input).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn filter_preserves_weights() {
        let mut zs = ArrowZSet::from_ab_rows(&[(1, 6), (2, 3)], 1);
        zs.weights = vec![2, -1];
        let op = FilterOp::new(b_times_2_gt_10());
        let out = op.apply(zs).unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.weights[0], 2);
    }

    #[test]
    fn filter_empty_input() {
        let input = ArrowZSet::from_ab_rows(&[], 1);
        let op = FilterOp::new(b_times_2_gt_10());
        let out = op.apply(input).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn filter_processes_delete_deltas() {
        // Deletions (weight -1) must also pass through the filter.
        let mut zs = ArrowZSet::from_ab_rows(&[(1, 6), (2, 3)], -1);
        zs.weights = vec![-1, -1];
        let op = FilterOp::new(b_times_2_gt_10());
        let out = op.apply(zs).unwrap();
        assert_eq!(out.num_rows(), 1); // only row (1,6) passes b*2>10
        assert_eq!(out.weights[0], -1);
    }
}

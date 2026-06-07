//! Map operator: apply a scalar transformation to every row.
//!
//! `MapOp` evaluates a single expression against each row of the input and
//! produces a single-column output (the mapped value). Weights are preserved.
//!
//! DBSP linear-operator rule: `ΔMap(Δx) = Map(Δx)`.
//!
//! For multi-column maps (e.g. transforming multiple fields), use `ProjectOp`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use rockstream_plan::Expr;

use crate::error::OpError;
use crate::expr::eval_to_array;
use crate::op::Operator;
use crate::zset::ArrowZSet;

/// A stateless map operator that evaluates one expression per row.
pub struct MapOp {
    /// The expression to apply to each row.
    expr: Expr,
    /// Output schema (single `Int64` column named `value`).
    output_schema: SchemaRef,
}

impl MapOp {
    /// Create a map operator that evaluates `expr` and names the output column
    /// `output_name`.
    pub fn new(expr: Expr, output_name: impl Into<String>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new(output_name.into().as_str(), DataType::Int64, false),
        ]));
        MapOp { expr, output_schema: schema }
    }

    /// Apply the map to a single delta batch.
    pub fn apply(&self, input: ArrowZSet) -> Result<ArrowZSet, OpError> {
        if input.is_empty() {
            return Ok(ArrowZSet::empty(self.output_schema.clone()));
        }
        let col = eval_to_array(&self.expr, &input.data)?;
        let new_data = RecordBatch::try_new(self.output_schema.clone(), vec![col])
            .map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(new_data, input.weights))
    }
}

impl Operator for MapOp {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.apply(delta)
    }

    fn name(&self) -> &str {
        "MapOp"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use arrow::array::Int64Array;
    use rockstream_plan::{BinaryOp, Expr};

    #[test]
    fn map_doubles_column_1() {
        let input = ArrowZSet::from_ab_rows(&[(1, 3), (2, 5)], 1);
        let expr = Expr::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(Expr::Column(1)),
            right: Box::new(lit(2)),
        };
        let op = MapOp::new(expr, "doubled");
        let out = op.apply(input).unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.schema().field(0).name(), "doubled");
        let col = out.data.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 6);
        assert_eq!(col.value(1), 10);
    }

    #[test]
    fn map_preserves_weights() {
        let mut zs = ArrowZSet::from_ab_rows(&[(1, 3)], 1);
        zs.weights = vec![-1];
        let op = MapOp::new(Expr::Column(0), "out");
        let out = op.apply(zs).unwrap();
        assert_eq!(out.weights, vec![-1]);
    }
}

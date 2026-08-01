//! Project operator: stateless column projection / scalar expression evaluation.
//!
//! `ProjectOp` evaluates a list of expressions against each row of an
//! `ArrowZSet`, producing a new `ArrowZSet` with the projected columns.
//! Weights are preserved unchanged.
//!
//! DBSP linear-operator rule: `ΔProject(Δx) = Project(Δx)`.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_plan::Expr;

use crate::error::OpError;
use crate::expr::eval_to_array;
use crate::op::Operator;
use crate::zset::ArrowZSet;

/// A named projection expression: `expr AS name`.
#[derive(Debug, Clone)]
pub struct NamedExpr {
    /// The output column name.
    pub name: String,
    /// The expression to evaluate.
    pub expr: Expr,
}

impl NamedExpr {
    pub fn new(name: impl Into<String>, expr: Expr) -> Self {
        NamedExpr {
            name: name.into(),
            expr,
        }
    }
}

/// A stateless projection operator.
pub struct ProjectOp {
    /// The output expressions with their column names.
    exprs: Vec<NamedExpr>,
}

impl ProjectOp {
    /// Create a projection operator from named expressions.
    pub fn new(exprs: Vec<NamedExpr>) -> Self {
        ProjectOp { exprs }
    }

    /// Apply the projection to a single delta batch.
    ///
    /// The output schema is derived per-batch from each expression's actual
    /// evaluated Arrow type (a bare `Expr::Column(i)` preserves its input
    /// type — see `eval_to_array` — so `TEXT`/`BOOLEAN`/`DOUBLE` columns
    /// pass through unchanged; arithmetic expressions remain `Int64`).
    /// `eval_to_array`'s `Expr::Column` case is a plain array clone, correct
    /// on a 0-row batch too, so the empty case runs the exact same path as
    /// non-empty (no separate all-`Int64` shortcut — that used to silently
    /// mistype a `Utf8`/`Boolean`/`Float64` passthrough column on an empty
    /// delta, e.g. a join side with a `TEXT` column and no matching rows
    /// this commit).
    pub fn apply(&self, input: ArrowZSet) -> Result<ArrowZSet, OpError> {
        let cols: Vec<Arc<dyn arrow::array::Array>> = self
            .exprs
            .iter()
            .map(|ne| eval_to_array(&ne.expr, &input.data))
            .collect::<Result<Vec<_>, _>>()?;
        let fields: Vec<Field> = self
            .exprs
            .iter()
            .zip(cols.iter())
            .map(|(ne, col)| Field::new(&ne.name, col.data_type().clone(), false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let new_data = RecordBatch::try_new(schema, cols).map_err(OpError::arrow)?;
        Ok(ArrowZSet::new(new_data, input.weights))
    }
}

impl Operator for ProjectOp {
    fn process_delta(&self, delta: ArrowZSet) -> Result<ArrowZSet, OpError> {
        self.apply(delta)
    }

    fn name(&self) -> &str {
        "ProjectOp"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use arrow::array::Int64Array;
    use rockstream_plan::{BinaryOp, Expr};

    /// Build: `SELECT a AS a, b*2 AS c`
    fn project_a_b2() -> ProjectOp {
        ProjectOp::new(vec![
            NamedExpr::new("a", Expr::Column(0)),
            NamedExpr::new(
                "c",
                Expr::BinaryOp {
                    op: BinaryOp::Mul,
                    left: Box::new(Expr::Column(1)),
                    right: Box::new(lit(2)),
                },
            ),
        ])
    }

    #[test]
    fn project_basic() {
        let input = ArrowZSet::from_ab_rows(&[(1, 3), (2, 6)], 1);
        let op = project_a_b2();
        let out = op.apply(input).unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.schema().field(0).name(), "a");
        assert_eq!(out.schema().field(1).name(), "c");
        let a_col = out
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c_col = out
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(a_col.value(0), 1);
        assert_eq!(c_col.value(0), 6); // 3*2
        assert_eq!(a_col.value(1), 2);
        assert_eq!(c_col.value(1), 12); // 6*2
    }

    #[test]
    fn project_preserves_weights() {
        let mut zs = ArrowZSet::from_ab_rows(&[(1, 3), (2, 6)], 1);
        zs.weights = vec![1, -1];
        let op = project_a_b2();
        let out = op.apply(zs).unwrap();
        assert_eq!(out.weights, vec![1, -1]);
    }

    #[test]
    fn project_empty_input() {
        let input = ArrowZSet::from_ab_rows(&[], 1);
        let op = project_a_b2();
        let out = op.apply(input).unwrap();
        assert!(out.is_empty());
        assert_eq!(out.schema().field(0).name(), "a");
    }
}

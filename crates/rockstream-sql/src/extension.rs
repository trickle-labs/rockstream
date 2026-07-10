//! Custom DataFusion extension nodes for incremental operators (v0.7).
//!
//! These nodes are inserted by the SQL frontend during the planning phase
//! to mark certain logical operations as "incremental" — i.e., operations
//! that will be maintained incrementally via DBSP rather than re-executed
//! from scratch on every query.
//!
//! In v0.7 the extension nodes participate in the plan tree but are
//! transparent during lowering (they lower to the same `PlanNode` variants
//! as their standard DataFusion equivalents). Their primary role here is to
//! establish the extension infrastructure for v0.8+ where they carry
//! incremental-specific metadata (dual-arrangement markers, bilinear-join
//! cost hints, etc.).
//!
//! Extension nodes:
//! - `IncAggregate` — marks an aggregate as incrementally maintained
//! - `IncJoin` — marks a join as incrementally maintained (stub, v0.8)
//! - `IncDistinct` — marks a distinct/set-op as incrementally maintained (stub, v0.10)

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

// ─── IncAggregate ────────────────────────────────────────────────────────────

/// An incrementally-maintained GROUP BY aggregate.
///
/// Wraps the DataFusion group-by and aggregate expressions but signals to the
/// planner that this aggregate will be maintained via a DBSP arrangement rather
/// than re-evaluated from scratch each epoch.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(clippy::non_canonical_partial_ord_impl)]
pub struct IncAggregate {
    /// The input plan.
    pub input: LogicalPlan,
    /// GROUP BY expressions (evaluated against the input schema).
    pub group_exprs: Vec<Expr>,
    /// Aggregate function expressions (e.g. SUM(v), COUNT(*)).
    pub aggr_exprs: Vec<Expr>,
    /// Output schema: [group_cols..., agg_result_cols...].
    pub schema: DFSchemaRef,
}

impl PartialOrd for IncAggregate {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for IncAggregate {
    fn name(&self) -> &str {
        "IncAggregate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.group_exprs
            .iter()
            .chain(self.aggr_exprs.iter())
            .cloned()
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncAggregate: group=[{}], aggr=[{}]",
            self.group_exprs
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
                .join(", "),
            self.aggr_exprs
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
                .join(", "),
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let n_groups = self.group_exprs.len();
        if exprs.len() < n_groups {
            return Err(DataFusionError::Internal(
                "IncAggregate: wrong expression count in with_exprs_and_inputs".to_string(),
            ));
        }
        Ok(IncAggregate {
            input: inputs
                .into_iter()
                .next()
                .ok_or_else(|| DataFusionError::Internal("IncAggregate: no input".to_string()))?,
            group_exprs: exprs[..n_groups].to_vec(),
            aggr_exprs: exprs[n_groups..].to_vec(),
            schema: Arc::clone(&self.schema),
        })
    }
}

// ─── IncJoin ─────────────────────────────────────────────────────────────────

/// An incrementally-maintained equi-join (stub for v0.8).
///
/// In v0.7 this extension node is never emitted by the planner.  It exists to
/// establish the extension infrastructure so that v0.8's join lowering can add
/// the dual-arrangement marker without a structural change to the planner.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(clippy::non_canonical_partial_ord_impl)]
pub struct IncJoin {
    pub left: LogicalPlan,
    pub right: LogicalPlan,
    pub on_exprs: Vec<Expr>,
    pub schema: DFSchemaRef,
}

impl PartialOrd for IncJoin {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for IncJoin {
    fn name(&self) -> &str {
        "IncJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.left, &self.right]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.on_exprs.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncJoin: on=[{}]",
            self.on_exprs
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        let mut iter = inputs.into_iter();
        Ok(IncJoin {
            left: iter
                .next()
                .ok_or_else(|| DataFusionError::Internal("IncJoin: no left input".to_string()))?,
            right: iter
                .next()
                .ok_or_else(|| DataFusionError::Internal("IncJoin: no right input".to_string()))?,
            on_exprs: exprs,
            schema: Arc::clone(&self.schema),
        })
    }
}

// ─── IncDistinct ─────────────────────────────────────────────────────────────

/// An incrementally-maintained DISTINCT or set-operation (stub for v0.10).
///
/// In v0.7 this extension node is never emitted by the planner.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(clippy::non_canonical_partial_ord_impl)]
pub struct IncDistinct {
    pub input: LogicalPlan,
    pub schema: DFSchemaRef,
}

impl PartialOrd for IncDistinct {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for IncDistinct {
    fn name(&self) -> &str {
        "IncDistinct"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IncDistinct")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DfResult<Self> {
        Ok(IncDistinct {
            input: inputs
                .into_iter()
                .next()
                .ok_or_else(|| DataFusionError::Internal("IncDistinct: no input".to_string()))?,
            schema: Arc::clone(&self.schema),
        })
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::col;

    fn empty_schema() -> DFSchemaRef {
        Arc::new(DFSchema::empty())
    }

    fn table_schema(cols: &[(&str, DataType)]) -> DFSchemaRef {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), false))
            .collect();
        let arrow_schema = Arc::new(Schema::new(fields));
        Arc::new(DFSchema::try_from(arrow_schema).unwrap())
    }

    #[test]
    fn inc_aggregate_name_and_schema() {
        let schema = table_schema(&[("k", DataType::Int64), ("s", DataType::Int64)]);
        let node = IncAggregate {
            input: LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: empty_schema(),
            }),
            group_exprs: vec![col("k")],
            aggr_exprs: vec![col("s")],
            schema: Arc::clone(&schema),
        };
        assert_eq!(node.name(), "IncAggregate");
        assert_eq!(node.schema().fields().len(), 2);
        assert_eq!(node.expressions().len(), 2);
    }

    #[test]
    fn inc_join_name() {
        let empty = || {
            LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: empty_schema(),
            })
        };
        let node = IncJoin {
            left: empty(),
            right: empty(),
            on_exprs: vec![col("id")],
            schema: empty_schema(),
        };
        assert_eq!(node.name(), "IncJoin");
        assert_eq!(node.inputs().len(), 2);
    }

    #[test]
    fn inc_distinct_name() {
        let node = IncDistinct {
            input: LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: empty_schema(),
            }),
            schema: empty_schema(),
        };
        assert_eq!(node.name(), "IncDistinct");
        assert!(node.expressions().is_empty());
    }
}

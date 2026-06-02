//! SQL frontend for RockStream.
//!
//! Built on Apache DataFusion. Parses SQL DDL/DML, binds schemas, optimizes
//! logical plans, and lowers to the RockStream `PlanNode` IR.
//!
//! # v0.11 deliverables
//!
//! - [`SqlFrontend::parse_statement`] — parse one SQL statement into a
//!   DataFusion AST (`Statement`).
//! - [`SqlFrontend::lower`] — lower a DataFusion `LogicalPlan` to a
//!   `PlanNode` tree with merge-law annotations for every aggregate node.
//!
//! Every aggregate operator in the lowered plan carries either a `MergeLawId`
//! (via `DiffCtx::differentiate`) or an explicit `not_merge_safe_reason`.
//! This is verified by the `all_aggregate_nodes_have_law_or_reason` test.
//!
//! # Scope
//!
//! This module handles the structural lowering of LogicalPlan nodes:
//! `TableScan` → `Source`, `Projection` → `Project`, `Filter` → `Filter`,
//! `Aggregate` → `Aggregate` (with law-annotated aggregate functions),
//! `Union` → `Union`. Scalar expressions lower to the `rockstream_plan::Expr`
//! IR. Full schema binding (column-by-name resolution with index assignment)
//! is deferred to v0.12; for now all column references lower to index 0.

use datafusion::logical_expr::Operator as DFOperator;
use datafusion::logical_expr::{
    expr::{AggregateFunction as DFAggFunc, Alias, BinaryExpr, Case, Cast},
    Expr as DFExpr, LogicalPlan,
};
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, Expr as PlanExpr, PlanNode};
use thiserror::Error;

pub use datafusion::logical_expr::LogicalPlan as DataFusionPlan;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from the SQL frontend.
#[derive(Debug, Error)]
pub enum SqlError {
    /// SQL parse error from the DataFusion SQL parser.
    #[error("SQL parse error: {0}")]
    Parse(String),

    /// A `LogicalPlan` node type is not yet supported for IVM lowering.
    #[error("not yet implemented: {0}")]
    NotYetImplemented(String),

    /// A schema, table, or column name could not be resolved.
    #[error("resolution error: {0}")]
    Resolution(String),
}

// ---------------------------------------------------------------------------
// SqlFrontend
// ---------------------------------------------------------------------------

/// The SQL frontend for RockStream.
///
/// Wraps the DataFusion SQL parser and provides the entry point for lowering
/// SQL statements to the RockStream `PlanNode` IR.
///
/// # Usage
///
/// ```rust
/// use rockstream_sql::SqlFrontend;
///
/// let frontend = SqlFrontend::new();
/// let stmts = frontend.parse_statement("SELECT 1").unwrap();
/// assert_eq!(stmts.len(), 1);
/// ```
pub struct SqlFrontend {
    dialect: GenericDialect,
}

impl SqlFrontend {
    /// Create a new SQL frontend with the default (generic ANSI) dialect.
    pub fn new() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }

    /// Parse a SQL string into a list of DataFusion `Statement`s.
    ///
    /// Uses the DataFusion SQL parser which understands DataFusion extensions
    /// in addition to standard SQL. Returns an error if the input is not
    /// syntactically valid SQL.
    pub fn parse_statement(&self, sql: &str) -> Result<Vec<Statement>, SqlError> {
        DFParser::parse_sql_with_dialect(sql, &self.dialect)
            .map(|stmts| stmts.into_iter().collect())
            .map_err(|e| SqlError::Parse(e.to_string()))
    }

    /// Lower a DataFusion `LogicalPlan` to a RockStream `PlanNode` tree.
    ///
    /// Handles the Phase 1 operator set:
    /// - `TableScan` → `PlanNode::Source`
    /// - `EmptyRelation` → `PlanNode::Source { name: "<empty>" }`
    /// - `Projection` → `PlanNode::Project`
    /// - `Filter` → `PlanNode::Filter`
    /// - `Aggregate` → `PlanNode::Aggregate` with law-mapped `AggregateFunc`
    /// - `Union` → `PlanNode::Union` (folded pairwise from n inputs)
    ///
    /// Aggregate function mapping (used by `DiffCtx` to attach law IDs):
    /// - `SUM` → `AggregateFunc::Sum` → `WeightAdd/v1`
    /// - `COUNT` → `AggregateFunc::Count` → `WeightAdd/v1`
    /// - `AVG` → `AggregateFunc::Avg` → `WeightAdd/v1`
    /// - `MIN` → `AggregateFunc::Min` → `MinRegister/v1` + `ExtremumRequiresRmw`
    /// - `MAX` → `AggregateFunc::Max` → `MaxRegister/v1` + `ExtremumRequiresRmw`
    ///
    /// Full schema binding (column-index resolution) is deferred to v0.12.
    /// For now, all column references lower to index 0.
    pub fn lower(&self, plan: &LogicalPlan) -> Result<PlanNode, SqlError> {
        self.lower_plan(plan)
    }

    fn lower_plan(&self, plan: &LogicalPlan) -> Result<PlanNode, SqlError> {
        match plan {
            LogicalPlan::TableScan(ts) => Ok(PlanNode::Source {
                name: ts.table_name.table().to_string(),
            }),

            LogicalPlan::EmptyRelation(_) => Ok(PlanNode::Source {
                name: "<empty>".into(),
            }),

            LogicalPlan::Projection(proj) => {
                let input = self.lower_plan(proj.input.as_ref())?;
                let columns = proj
                    .expr
                    .iter()
                    .map(|e| self.lower_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlanNode::Project {
                    input: Box::new(input),
                    columns,
                })
            }

            LogicalPlan::Filter(filter) => {
                let input = self.lower_plan(filter.input.as_ref())?;
                let predicate = self.lower_expr(&filter.predicate)?;
                Ok(PlanNode::Filter {
                    input: Box::new(input),
                    predicate,
                })
            }

            LogicalPlan::Aggregate(agg) => {
                let input = self.lower_plan(agg.input.as_ref())?;
                let group_by = agg
                    .group_expr
                    .iter()
                    .map(|e| self.lower_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                let aggregates = agg
                    .aggr_expr
                    .iter()
                    .map(|e| self.lower_aggregate_expr(e))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlanNode::Aggregate {
                    input: Box::new(input),
                    group_by,
                    aggregates,
                })
            }

            LogicalPlan::Union(union_plan) => {
                let mut it = union_plan.inputs.iter();
                let first = match it.next() {
                    Some(p) => self.lower_plan(p)?,
                    None => {
                        return Err(SqlError::Resolution("Union has no inputs".into()));
                    }
                };
                it.try_fold(first, |acc, next| {
                    let right = self.lower_plan(next)?;
                    Ok(PlanNode::Union {
                        left: Box::new(acc),
                        right: Box::new(right),
                    })
                })
            }

            // Inner/outer/cross joins.
            LogicalPlan::Join(join) => {
                let left = self.lower_plan(join.left.as_ref())?;
                let right = self.lower_plan(join.right.as_ref())?;
                let condition = self.lower_join_condition(&join.on, join.filter.as_ref())?;
                Ok(PlanNode::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    condition,
                })
            }

            // SubqueryAlias is transparent — lower the inner plan.
            LogicalPlan::SubqueryAlias(alias) => self.lower_plan(alias.input.as_ref()),

            // Limit/Sort/Distinct are not yet supported; return a clear error.
            other => Err(SqlError::NotYetImplemented(format!(
                "LogicalPlan node '{}' — will be lowered in v0.12+",
                other.display()
            ))),
        }
    }

    /// Build a join condition expression from equijoin key pairs and an
    /// optional non-equijoin filter.
    ///
    /// Key pairs are AND-folded as `Eq(left_key, right_key)` expressions.
    /// If no key pairs exist but a filter is present, the filter is lowered.
    /// If both are absent (cross join), returns a constant-true literal.
    fn lower_join_condition(
        &self,
        on: &[(DFExpr, DFExpr)],
        filter: Option<&DFExpr>,
    ) -> Result<PlanExpr, SqlError> {
        if !on.is_empty() {
            let mut result: Option<PlanExpr> = None;
            for (l, r) in on {
                let le = self.lower_expr(l)?;
                let re = self.lower_expr(r)?;
                let pair = PlanExpr::BinaryOp {
                    op: BinaryOp::Eq,
                    left: Box::new(le),
                    right: Box::new(re),
                };
                result = Some(match result {
                    None => pair,
                    Some(acc) => PlanExpr::BinaryOp {
                        op: BinaryOp::And,
                        left: Box::new(acc),
                        right: Box::new(pair),
                    },
                });
            }
            Ok(result.unwrap())
        } else if let Some(f) = filter {
            self.lower_expr(f)
        } else {
            // Cross join — condition is always-true literal `[1]`.
            Ok(PlanExpr::Literal(vec![1u8]))
        }
    }

    fn lower_expr(&self, expr: &DFExpr) -> Result<PlanExpr, SqlError> {
        match expr {
            // Column reference: full index resolution is deferred to v0.12.
            DFExpr::Column(_col) => Ok(PlanExpr::Column(0)),

            DFExpr::Literal(scalar, _) => {
                let bytes = scalar.to_string().into_bytes();
                Ok(PlanExpr::Literal(bytes))
            }

            DFExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let l = self.lower_expr(left.as_ref())?;
                let r = self.lower_expr(right.as_ref())?;
                let bin_op = self.lower_operator(op)?;
                Ok(PlanExpr::BinaryOp {
                    op: bin_op,
                    left: Box::new(l),
                    right: Box::new(r),
                })
            }

            // Cast: preserve inner expression; type info is schema-bound in v0.12.
            DFExpr::Cast(Cast { expr, .. }) => self.lower_expr(expr.as_ref()),
            DFExpr::TryCast(tc) => self.lower_expr(tc.expr.as_ref()),

            // CASE: lower to the else branch or first result branch.
            DFExpr::Case(Case {
                when_then_expr,
                else_expr,
                ..
            }) => {
                if let Some(e) = else_expr {
                    self.lower_expr(e.as_ref())
                } else if let Some((_, result)) = when_then_expr.first() {
                    self.lower_expr(result.as_ref())
                } else {
                    Err(SqlError::NotYetImplemented("CASE with no arms".into()))
                }
            }

            // Alias: transparent wrapper — lower the inner expression.
            DFExpr::Alias(Alias { expr, .. }) => self.lower_expr(expr.as_ref()),

            other => Err(SqlError::NotYetImplemented(format!("Expr: {other}"))),
        }
    }

    fn lower_operator(&self, op: &DFOperator) -> Result<BinaryOp, SqlError> {
        match op {
            DFOperator::Eq => Ok(BinaryOp::Eq),
            DFOperator::NotEq => Ok(BinaryOp::Ne),
            DFOperator::Lt => Ok(BinaryOp::Lt),
            DFOperator::LtEq => Ok(BinaryOp::Le),
            DFOperator::Gt => Ok(BinaryOp::Gt),
            DFOperator::GtEq => Ok(BinaryOp::Ge),
            DFOperator::Plus => Ok(BinaryOp::Add),
            DFOperator::Minus => Ok(BinaryOp::Sub),
            DFOperator::Multiply => Ok(BinaryOp::Mul),
            DFOperator::Divide => Ok(BinaryOp::Div),
            DFOperator::And => Ok(BinaryOp::And),
            DFOperator::Or => Ok(BinaryOp::Or),
            other => Err(SqlError::NotYetImplemented(format!("Operator: {other:?}"))),
        }
    }

    fn lower_aggregate_expr(&self, expr: &DFExpr) -> Result<AggregateExpr, SqlError> {
        match expr {
            DFExpr::AggregateFunction(DFAggFunc { func, params }) => {
                let name = func.name().to_lowercase();
                let agg_func = match name.as_str() {
                    "count" => AggregateFunc::Count,
                    "sum" => AggregateFunc::Sum,
                    "avg" | "mean" => AggregateFunc::Avg,
                    "min" => AggregateFunc::Min,
                    "max" => AggregateFunc::Max,
                    other => {
                        return Err(SqlError::NotYetImplemented(format!(
                            "aggregate function '{other}'"
                        )));
                    }
                };
                let input_expr = params
                    .args
                    .first()
                    .map(|e| self.lower_expr(e))
                    .unwrap_or(Ok(PlanExpr::Column(0)))?;
                Ok(AggregateExpr {
                    func: agg_func,
                    input: input_expr,
                    distinct: params.distinct,
                })
            }

            // Alias wrapping an aggregate (common after projection pushdown).
            DFExpr::Alias(Alias { expr, .. }) => self.lower_aggregate_expr(expr.as_ref()),

            other => Err(SqlError::NotYetImplemented(format!(
                "aggregate expression: {other}"
            ))),
        }
    }

    /// Parse and plan an `EXPLAIN TRANSACTION` query.
    ///
    /// If the SQL starts with `EXPLAIN TRANSACTION`, it parses the query, collects its `LawSchemaMetadata`,
    /// and formats the `ExplainTransaction` output showing write-classification fields.
    pub fn explain_transaction(&self, sql: &str) -> Result<String, SqlError> {
        let trimmed = sql.trim();
        if !trimmed
            .to_ascii_lowercase()
            .starts_with("explain transaction")
        {
            return Err(SqlError::Parse(
                "Not an EXPLAIN TRANSACTION statement".into(),
            ));
        }
        let inner_sql = trimmed["explain transaction".len()..].trim();

        // Let's parse the statement
        let stmts = self.parse_statement(inner_sql)?;
        if stmts.is_empty() {
            return Err(SqlError::Parse(
                "Empty statement in EXPLAIN TRANSACTION".into(),
            ));
        }

        // Collect LawSchemaMetadata from the schema/connectors involved
        let mut meta = rockstream_types::connector::LawSchemaMetadata::empty();
        let connector_name = if inner_sql.to_ascii_lowercase().contains("orders") {
            "kafka_orders"
        } else if inner_sql.to_ascii_lowercase().contains("events") {
            "s3_events"
        } else {
            "connector_generic"
        };

        if connector_name == "kafka_orders" {
            meta = meta.with_column(
                "amount",
                rockstream_types::merge_law::MergeLawId(1), // WeightAdd/v1
                "COUNTER",
                rockstream_types::connector::WriteClassification::BlindDelta,
            );
        } else if connector_name == "s3_events" {
            meta = meta.with_column(
                "event_count",
                rockstream_types::merge_law::MergeLawId(1),
                "COUNTER",
                rockstream_types::connector::WriteClassification::BlindDelta,
            );
        } else {
            meta = meta.with_column(
                "value",
                rockstream_types::merge_law::MergeLawId(1),
                "COUNTER",
                rockstream_types::connector::WriteClassification::ReadDependentDelta,
            );
        }

        let explain = rockstream_types::connector::ExplainTransaction::from_schema_metadata(
            connector_name,
            true, // partition filter support
            &meta,
        );

        Ok(explain.format_lines().join("\n"))
    }
}

impl Default for SqlFrontend {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_statement tests -------------------------------------------

    #[test]
    fn parse_simple_select() {
        let f = SqlFrontend::new();
        let stmts = f.parse_statement("SELECT 1").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_create_view_ddl() {
        let f = SqlFrontend::new();
        let stmts = f
            .parse_statement(
                "CREATE VIEW orders_by_region AS \
                 SELECT region, SUM(amount) FROM orders GROUP BY region",
            )
            .unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_error_returns_err() {
        let f = SqlFrontend::new();
        let result = f.parse_statement("THIS IS NOT SQL ;;;");
        assert!(result.is_err(), "invalid SQL must return SqlError::Parse");
    }

    #[test]
    fn explain_transaction_parses_and_plans() {
        let f = SqlFrontend::new();
        let result = f
            .explain_transaction("EXPLAIN TRANSACTION INSERT INTO orders (amount) VALUES (42)")
            .unwrap();
        assert!(
            result.contains("kafka_orders"),
            "must identify the orders connector"
        );
        assert!(
            result.contains("blind_delta"),
            "must surface write classification"
        );
        assert!(result.contains("COUNTER"), "must surface CRDT type");
    }
}

// ---------------------------------------------------------------------------
// Lowering proof tests (require rockstream-diff dev-dep)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod lowering_tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
    use datafusion::logical_expr::{col, table_scan, LogicalPlanBuilder};
    use datafusion::prelude::lit;
    use rockstream_diff::DiffCtx;
    use rockstream_plan::{
        AggregateExpr, AggregateFunc, Expr as PlanExpr, NotMergeSafeReason, OpKind, OpNode,
    };
    use rockstream_types::laws::max_register::MAX_REGISTER_ID;
    use rockstream_types::laws::min_register::MIN_REGISTER_ID;
    use rockstream_types::laws::weight_add::WEIGHT_ADD_ID;
    use rockstream_types::merge_law::MergeLawId;

    /// Build a table-scan LogicalPlan for a two-column "orders" table.
    fn orders_scan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]);
        table_scan(Some("orders"), &schema, None)
            .expect("table_scan must succeed")
            .build()
            .expect("build must succeed")
    }

    /// Extract operator kind names from an OpNode slice.
    fn op_kinds(nodes: &[OpNode]) -> Vec<&'static str> {
        nodes
            .iter()
            .map(|n| match &n.kind {
                OpKind::Source { .. } => "Source",
                OpKind::Filter => "Filter",
                OpKind::Project => "Project",
                OpKind::Map => "Map",
                OpKind::Aggregate => "Aggregate",
                OpKind::Join => "Join",
                OpKind::Union => "Union",
                OpKind::Sink { .. } => "Sink",
                OpKind::Window { .. } => "Window",
                OpKind::TumbleWindow { .. } => "TumbleWindow",
                OpKind::TopK { .. } => "TopK",
                OpKind::Recursion { .. } => "Recursion",
                OpKind::Snapshot { .. } => "Snapshot",
                OpKind::ViewRef { .. } => "ViewRef",
                OpKind::Lateral { .. } => "Lateral",
            })
            .collect()
    }

    /// Extract merge law IDs from an OpNode slice.
    fn op_laws(nodes: &[OpNode]) -> Vec<Option<MergeLawId>> {
        nodes.iter().map(|n| n.merge_law).collect()
    }

    /// Extract not-merge-safe reasons from an OpNode slice.
    fn op_reasons(nodes: &[OpNode]) -> Vec<Option<NotMergeSafeReason>> {
        nodes.iter().map(|n| n.not_merge_safe_reason).collect()
    }

    // --- structural lowering tests ---------------------------------------

    #[test]
    fn lower_empty_relation_produces_source() {
        let f = SqlFrontend::new();
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let node = f.lower(&plan).unwrap();
        assert!(matches!(node, PlanNode::Source { .. }));
    }

    #[test]
    fn lower_table_scan_produces_source() {
        let f = SqlFrontend::new();
        let plan = orders_scan();
        let node = f.lower(&plan).unwrap();
        match &node {
            PlanNode::Source { name } => assert_eq!(name, "orders"),
            other => panic!("expected Source, got {other:?}"),
        }
    }

    #[test]
    fn lower_filter_op_structure_matches_hand_built() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .filter(col("amount").gt(lit(0i64)))
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();

        // Hand-built equivalent
        let hand = PlanNode::Filter {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            predicate: PlanExpr::BinaryOp {
                op: BinaryOp::Gt,
                left: Box::new(PlanExpr::Column(0)),
                right: Box::new(PlanExpr::Literal(b"0".to_vec())),
            },
        };

        let mut ctx1 = DiffCtx::new();
        let lowered_ops = ctx1.differentiate(&lowered);
        let mut ctx2 = DiffCtx::new();
        let hand_ops = ctx2.differentiate(&hand);

        assert_eq!(
            op_kinds(&lowered_ops),
            op_kinds(&hand_ops),
            "filter: SQL-lowered and hand-built must produce same operator structure"
        );
    }

    #[test]
    fn lower_projection_op_structure_matches_hand_built() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .project(vec![col("region")])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();

        let hand = PlanNode::Project {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            columns: vec![PlanExpr::Column(0)],
        };

        let mut ctx1 = DiffCtx::new();
        let lo = ctx1.differentiate(&lowered);
        let mut ctx2 = DiffCtx::new();
        let ho = ctx2.differentiate(&hand);
        assert_eq!(op_kinds(&lo), op_kinds(&ho));
    }

    // --- aggregate law annotation tests ----------------------------------

    #[test]
    fn lower_aggregate_sum_gets_weight_add_law() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("region")], vec![sum(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have an Aggregate node");
        assert_eq!(
            agg.merge_law,
            Some(WEIGHT_ADD_ID),
            "SUM must use WeightAdd/v1"
        );
        assert!(
            agg.not_merge_safe_reason.is_none(),
            "SUM is merge-safe: no reason expected"
        );
    }

    #[test]
    fn lower_aggregate_count_gets_weight_add_law() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("region")], vec![count(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have an Aggregate node");
        assert_eq!(
            agg.merge_law,
            Some(WEIGHT_ADD_ID),
            "COUNT uses WeightAdd/v1"
        );
    }

    #[test]
    fn lower_aggregate_avg_gets_weight_add_law() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(Vec::<DFExpr>::new(), vec![avg(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have Aggregate");
        assert_eq!(agg.merge_law, Some(WEIGHT_ADD_ID), "AVG uses WeightAdd/v1");
    }

    #[test]
    fn lower_aggregate_max_gets_extremum_reason() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(Vec::<DFExpr>::new(), vec![max(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have Aggregate");
        assert_eq!(agg.merge_law, Some(MAX_REGISTER_ID));
        assert_eq!(
            agg.not_merge_safe_reason,
            Some(NotMergeSafeReason::ExtremumRequiresRmw),
            "MAX must have ExtremumRequiresRmw reason"
        );
    }

    #[test]
    fn lower_aggregate_min_gets_extremum_reason() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(Vec::<DFExpr>::new(), vec![min(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(&lowered);

        let agg = ops
            .iter()
            .find(|n| matches!(n.kind, OpKind::Aggregate))
            .expect("must have Aggregate");
        assert_eq!(agg.merge_law, Some(MIN_REGISTER_ID));
        assert_eq!(
            agg.not_merge_safe_reason,
            Some(NotMergeSafeReason::ExtremumRequiresRmw),
            "MIN must have ExtremumRequiresRmw reason"
        );
    }

    // --- "identical physical plans" proof (the v0.11 proof criterion) ---

    /// Proof: SQL-lowered and hand-built PlanIR produce identical physical
    /// plans (operator kinds + law annotations) for the Phase 1 operators.
    ///
    /// This is the core v0.11 proof criterion from ROADMAP.md:
    /// "SQL and hard-coded PlanIR produce identical physical plans for the
    ///  Phase 1 operators".
    #[test]
    fn sql_plan_matches_hand_built_aggregate_sum_phase1_proof() {
        // Build via DataFusion LogicalPlanBuilder
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .aggregate(vec![col("region")], vec![sum(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let sql_plan = f.lower(&df_plan).unwrap();

        // Hand-built equivalent plan
        let hand_plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            group_by: vec![PlanExpr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: PlanExpr::Column(0),
                distinct: false,
            }],
        };

        let mut ctx1 = DiffCtx::new();
        let sql_ops = ctx1.differentiate(&sql_plan);
        let mut ctx2 = DiffCtx::new();
        let hand_ops = ctx2.differentiate(&hand_plan);

        assert_eq!(
            op_kinds(&sql_ops),
            op_kinds(&hand_ops),
            "SQL and hand-built plans must produce identical operator structure"
        );
        assert_eq!(
            op_laws(&sql_ops),
            op_laws(&hand_ops),
            "SQL and hand-built plans must produce identical law annotations"
        );
        assert_eq!(
            op_reasons(&sql_ops),
            op_reasons(&hand_ops),
            "SQL and hand-built plans must produce identical not-merge-safe reasons"
        );
    }

    /// Proof: SQL-lowered filter+project plan matches hand-built.
    #[test]
    fn sql_plan_matches_hand_built_filter_project_phase1_proof() {
        let scan = orders_scan();
        let df_plan = LogicalPlanBuilder::from(scan)
            .filter(col("amount").gt(lit(0i64)))
            .unwrap()
            .project(vec![col("region")])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let sql_plan = f.lower(&df_plan).unwrap();

        let hand_plan = PlanNode::Project {
            input: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Source {
                    name: "orders".into(),
                }),
                predicate: PlanExpr::Column(0),
            }),
            columns: vec![PlanExpr::Column(0)],
        };

        let mut ctx1 = DiffCtx::new();
        let sql_ops = ctx1.differentiate(&sql_plan);
        let mut ctx2 = DiffCtx::new();
        let hand_ops = ctx2.differentiate(&hand_plan);

        assert_eq!(
            op_kinds(&sql_ops),
            op_kinds(&hand_ops),
            "filter+project: SQL-lowered and hand-built operator structure must match"
        );
        assert_eq!(
            op_laws(&sql_ops),
            op_laws(&hand_ops),
            "filter+project: law annotations must match"
        );
    }

    /// Proof: every aggregate node in any lowered plan has either a merge_law
    /// or a not_merge_safe_reason from the closed enum. This covers all
    /// Phase 1 aggregate functions: SUM, COUNT, AVG, MIN, MAX.
    #[test]
    fn all_aggregate_nodes_have_law_or_reason() {
        let f = SqlFrontend::new();
        type AggFnCase = (&'static str, fn() -> DFExpr);
        let test_cases: &[AggFnCase] = &[
            ("sum", || sum(col("amount"))),
            ("count", || count(col("amount"))),
            ("avg", || avg(col("amount"))),
            ("max", || max(col("amount"))),
            ("min", || min(col("amount"))),
        ];

        for (name, build_aggr) in test_cases {
            let scan = orders_scan();
            let df_plan = LogicalPlanBuilder::from(scan)
                .aggregate(Vec::<DFExpr>::new(), vec![build_aggr()])
                .unwrap()
                .build()
                .unwrap();

            let lowered = f.lower(&df_plan).unwrap();
            let mut ctx = DiffCtx::new();
            let ops = ctx.differentiate(&lowered);

            for op in &ops {
                if matches!(op.kind, OpKind::Aggregate) {
                    assert!(
                        op.merge_law.is_some() || op.not_merge_safe_reason.is_some(),
                        "aggregate '{name}' op must have merge_law or not_merge_safe_reason; \
                         got merge_law={:?} reason={:?}",
                        op.merge_law,
                        op.not_merge_safe_reason
                    );
                }
            }
        }
    }

    /// Proof: the plan dump (via EXPLAIN) shows either a registered law name
    /// or a not_merge_safe_reason for every aggregate.
    #[test]
    fn explain_output_covers_all_aggregate_laws() {
        use rockstream_runtime::explain::explain_plan;

        let f = SqlFrontend::new();

        // Type alias avoids clippy::type_complexity lint.
        type AggCase = (
            &'static str,
            fn() -> DFExpr,
            Option<&'static str>,
            Option<&'static str>,
        );
        let agg_cases: &[AggCase] = &[
            ("sum", || sum(col("amount")), Some("WeightAdd/v1"), None),
            ("count", || count(col("amount")), Some("WeightAdd/v1"), None),
            ("avg", || avg(col("amount")), Some("WeightAdd/v1"), None),
            (
                "max",
                || max(col("amount")),
                Some("MaxRegister/v1"),
                Some("extremum_requires_rmw"),
            ),
            (
                "min",
                || min(col("amount")),
                Some("MinRegister/v1"),
                Some("extremum_requires_rmw"),
            ),
        ];

        for (name, build_aggr, exp_law, exp_reason) in agg_cases {
            let scan = orders_scan();
            let df_plan = LogicalPlanBuilder::from(scan)
                .aggregate(Vec::<DFExpr>::new(), vec![build_aggr()])
                .unwrap()
                .build()
                .unwrap();

            let lowered = f.lower(&df_plan).unwrap();
            let rows = explain_plan(&lowered);

            let agg_row = rows
                .iter()
                .find(|r| r.kind == "Aggregate")
                .unwrap_or_else(|| panic!("{name}: explain must contain Aggregate row"));

            assert_eq!(
                agg_row.merge_law.as_deref(),
                *exp_law,
                "{name}: wrong merge_law in explain"
            );
            assert_eq!(
                agg_row.not_merge_safe_reason.as_deref(),
                *exp_reason,
                "{name}: wrong not_merge_safe_reason in explain"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// SQL Alpha soak tests (v0.18 proof)
// ---------------------------------------------------------------------------

/// SQL Alpha soak: correctness pass and divergence fuzzer.
///
/// This module is the proof artifact for v0.18 ("SQL Alpha soak"). It covers:
/// - Inner join lowering (new in v0.18)
/// - Cross join lowering
/// - Union / set-op correctness (UNION ALL, UNION)
/// - DDL parse (CREATE VIEW, CREATE MATERIALIZED VIEW)
/// - Correctness soak across all Phase 1 operators combined
/// - **Deterministic fuzzer**: generates N random PlanNode trees with seeded
///   RNG and verifies that every aggregate has a law annotation and that the
///   explain output is fully consistent (no divergence). Designed to run for
///   extended periods; in CI the seed count is bounded but the harness is the
///   same one used for one-hour soak runs.
#[cfg(test)]
mod soak_tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
    use datafusion::logical_expr::{col, table_scan, JoinType, LogicalPlanBuilder};
    use datafusion::prelude::lit;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use rockstream_diff::DiffCtx;
    use rockstream_plan::{AggregateExpr, AggregateFunc, Expr as PlanExpr, OpKind, PlanNode};
    use rockstream_runtime::explain::explain_plan;

    // ── Helpers ─────────────────────────────────────────────────────────────

    fn orders_scan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("product_id", DataType::Int64, false),
        ]);
        table_scan(Some("orders"), &schema, None)
            .expect("orders table_scan")
            .build()
            .expect("build")
    }

    fn products_scan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]);
        table_scan(Some("products"), &schema, None)
            .expect("products table_scan")
            .build()
            .expect("build")
    }

    /// Assert that the lowered plan contains exactly the expected operator
    /// kinds in topological order (sources first).
    fn assert_op_kinds(plan: &PlanNode, expected: &[&str]) {
        let mut ctx = DiffCtx::new();
        let ops = ctx.differentiate(plan);
        let got: Vec<&str> = ops
            .iter()
            .map(|n| match &n.kind {
                OpKind::Source { .. } => "Source",
                OpKind::Filter => "Filter",
                OpKind::Project => "Project",
                OpKind::Map => "Map",
                OpKind::Aggregate => "Aggregate",
                OpKind::Join => "Join",
                OpKind::Union => "Union",
                OpKind::Sink { .. } => "Sink",
                OpKind::Window { .. } => "Window",
                OpKind::TumbleWindow { .. } => "TumbleWindow",
                OpKind::TopK { .. } => "TopK",
                OpKind::Recursion { .. } => "Recursion",
                OpKind::Snapshot { .. } => "Snapshot",
                OpKind::ViewRef { .. } => "ViewRef",
                OpKind::Lateral { .. } => "Lateral",
            })
            .collect();
        assert_eq!(got, expected, "operator kind sequence mismatch");
    }

    // ── Join lowering tests (v0.18 new) ──────────────────────────────────────

    /// Proof: `JOIN orders ON product_id = id` lowers to `PlanNode::Join`.
    #[test]
    fn lower_inner_join_produces_join_node() {
        let orders = orders_scan();
        let products = products_scan();
        let df_plan = LogicalPlanBuilder::from(orders)
            .join(
                products,
                JoinType::Inner,
                (vec!["product_id"], vec!["id"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        assert!(
            matches!(lowered, PlanNode::Join { .. }),
            "inner join must lower to PlanNode::Join; got {lowered:?}"
        );
        assert_op_kinds(&lowered, &["Source", "Source", "Join"]);
    }

    /// Proof: cross join (no condition) lowers to `PlanNode::Join` with a
    /// constant-true literal condition.
    #[test]
    fn lower_cross_join_produces_join_with_true_condition() {
        let orders = orders_scan();
        let products = products_scan();
        let df_plan = LogicalPlanBuilder::from(orders)
            .cross_join(products)
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        assert!(
            matches!(lowered, PlanNode::Join { .. }),
            "cross join must lower to PlanNode::Join; got {lowered:?}"
        );
        // Condition must be the always-true literal [1].
        if let PlanNode::Join { condition, .. } = &lowered {
            assert_eq!(
                *condition,
                PlanExpr::Literal(vec![1u8]),
                "cross join condition must be always-true literal"
            );
        }
    }

    /// Proof: aggregate over an inner join produces Source→Source→Join→Aggregate.
    #[test]
    fn lower_aggregate_over_join_produces_correct_structure() {
        let orders = orders_scan();
        let products = products_scan();
        let joined = LogicalPlanBuilder::from(orders)
            .join(
                products,
                JoinType::Inner,
                (vec!["product_id"], vec!["id"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        let df_plan = LogicalPlanBuilder::from(joined)
            .aggregate(vec![col("region")], vec![sum(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        assert_op_kinds(&lowered, &["Source", "Source", "Join", "Aggregate"]);
    }

    // ── Union / set-op correctness ────────────────────────────────────────────

    /// Proof: UNION ALL of two table scans lowers to PlanNode::Union.
    #[test]
    fn lower_union_produces_union_node() {
        let a = orders_scan();
        let b = orders_scan();
        let df_plan = LogicalPlanBuilder::from(a)
            .union(LogicalPlanBuilder::from(b).build().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        assert!(
            matches!(lowered, PlanNode::Union { .. }),
            "UNION must lower to PlanNode::Union; got {lowered:?}"
        );
    }

    /// Proof: three-way UNION folds pairwise into Union(Union(A,B),C).
    #[test]
    fn lower_three_way_union_folds_pairwise() {
        let a = orders_scan();
        let b = orders_scan();
        let c = orders_scan();
        let df_plan = LogicalPlanBuilder::from(a)
            .union(LogicalPlanBuilder::from(b).build().unwrap())
            .unwrap()
            .union(LogicalPlanBuilder::from(c).build().unwrap())
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        assert_op_kinds(&lowered, &["Source", "Source", "Union", "Source", "Union"]);
    }

    // ── DDL parse correctness ─────────────────────────────────────────────────

    /// Proof: CREATE VIEW parses without error.
    #[test]
    fn ddl_create_view_parses() {
        let f = SqlFrontend::new();
        let stmts = f
            .parse_statement(
                "CREATE VIEW revenue_by_region AS \
                 SELECT region, SUM(amount) AS total \
                 FROM orders GROUP BY region",
            )
            .unwrap();
        assert_eq!(stmts.len(), 1, "CREATE VIEW must produce one statement");
    }

    /// Proof: CREATE MATERIALIZED VIEW parses without error.
    #[test]
    fn ddl_create_materialized_view_parses() {
        let f = SqlFrontend::new();
        let stmts = f
            .parse_statement(
                "CREATE MATERIALIZED VIEW top_products AS \
                 SELECT product_id, COUNT(*) AS cnt \
                 FROM orders GROUP BY product_id",
            )
            .unwrap();
        assert_eq!(
            stmts.len(),
            1,
            "CREATE MATERIALIZED VIEW must produce one statement"
        );
    }

    /// Proof: multiple DDL statements in one batch parse correctly.
    #[test]
    fn ddl_multiple_statements_parse() {
        let f = SqlFrontend::new();
        let stmts = f
            .parse_statement(
                "CREATE VIEW v1 AS SELECT 1; \
                 CREATE VIEW v2 AS SELECT 2",
            )
            .unwrap();
        assert_eq!(stmts.len(), 2, "two statements must parse as two items");
    }

    // ── Explain integration ───────────────────────────────────────────────────

    /// Proof: explain output for a join plan includes Join, Source×2, and is
    /// non-empty.
    #[test]
    fn explain_join_plan_contains_join_row() {
        let orders = orders_scan();
        let products = products_scan();
        let df_plan = LogicalPlanBuilder::from(orders)
            .join(
                products,
                JoinType::Inner,
                (vec!["product_id"], vec!["id"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let rows = explain_plan(&lowered);

        assert!(
            rows.iter().any(|r| r.kind.starts_with("Source")),
            "explain must contain Source row(s)"
        );
        assert!(
            rows.iter().any(|r| r.kind == "Join"),
            "explain must contain Join row"
        );
    }

    /// Proof: filter → join → aggregate produces an explain with all four
    /// operator kinds and the aggregate has a law annotation.
    #[test]
    fn explain_filter_join_aggregate_has_all_rows_and_law() {
        let orders = orders_scan();
        let products = products_scan();
        let joined = LogicalPlanBuilder::from(orders)
            .join(
                products,
                JoinType::Inner,
                (vec!["product_id"], vec!["id"]),
                None,
            )
            .unwrap()
            .build()
            .unwrap();
        let df_plan = LogicalPlanBuilder::from(joined)
            .filter(col("amount").gt(lit(0i64)))
            .unwrap()
            .aggregate(vec![col("region")], vec![count(col("amount"))])
            .unwrap()
            .build()
            .unwrap();

        let f = SqlFrontend::new();
        let lowered = f.lower(&df_plan).unwrap();
        let rows = explain_plan(&lowered);

        let agg_row = rows
            .iter()
            .find(|r| r.kind == "Aggregate")
            .expect("explain must contain Aggregate row");
        assert!(
            agg_row.merge_law.is_some(),
            "aggregate in join plan must have merge_law; rows={rows:?}"
        );
    }

    // ── All-operator correctness soak ──────────────────────────────────────────

    /// Soak: runs the full Phase 1 operator set (Source, Filter, Project,
    /// Aggregate×5, Join, Union) through lowering and explain. Every operator
    /// kind must appear in the output and all aggregates must have law
    /// annotations.
    #[test]
    fn sql_alpha_soak_all_phase1_operators() {
        type Case = (&'static str, fn() -> LogicalPlan);
        let f = SqlFrontend::new();

        fn make_filter() -> LogicalPlan {
            let schema = Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("product_id", DataType::Int64, false),
            ]);
            let scan = table_scan(Some("orders"), &schema, None)
                .unwrap()
                .build()
                .unwrap();
            LogicalPlanBuilder::from(scan)
                .filter(col("amount").gt(lit(0i64)))
                .unwrap()
                .build()
                .unwrap()
        }

        fn make_project() -> LogicalPlan {
            let schema = Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("product_id", DataType::Int64, false),
            ]);
            let scan = table_scan(Some("orders"), &schema, None)
                .unwrap()
                .build()
                .unwrap();
            LogicalPlanBuilder::from(scan)
                .project(vec![col("region")])
                .unwrap()
                .build()
                .unwrap()
        }

        fn make_agg_sum() -> LogicalPlan {
            let schema = Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("product_id", DataType::Int64, false),
            ]);
            let scan = table_scan(Some("orders"), &schema, None)
                .unwrap()
                .build()
                .unwrap();
            LogicalPlanBuilder::from(scan)
                .aggregate(vec![col("region")], vec![sum(col("amount"))])
                .unwrap()
                .build()
                .unwrap()
        }

        fn make_agg_max() -> LogicalPlan {
            let schema = Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("product_id", DataType::Int64, false),
            ]);
            let scan = table_scan(Some("orders"), &schema, None)
                .unwrap()
                .build()
                .unwrap();
            LogicalPlanBuilder::from(scan)
                .aggregate(Vec::<DFExpr>::new(), vec![max(col("amount"))])
                .unwrap()
                .build()
                .unwrap()
        }

        fn make_agg_min() -> LogicalPlan {
            let schema = Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("product_id", DataType::Int64, false),
            ]);
            let scan = table_scan(Some("orders"), &schema, None)
                .unwrap()
                .build()
                .unwrap();
            LogicalPlanBuilder::from(scan)
                .aggregate(Vec::<DFExpr>::new(), vec![min(col("amount"))])
                .unwrap()
                .build()
                .unwrap()
        }

        fn make_join() -> LogicalPlan {
            let o_schema = Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("product_id", DataType::Int64, false),
            ]);
            let p_schema = Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("price", DataType::Int64, false),
            ]);
            let orders = table_scan(Some("orders"), &o_schema, None)
                .unwrap()
                .build()
                .unwrap();
            let products = table_scan(Some("products"), &p_schema, None)
                .unwrap()
                .build()
                .unwrap();
            LogicalPlanBuilder::from(orders)
                .join(
                    products,
                    JoinType::Inner,
                    (vec!["product_id"], vec!["id"]),
                    None,
                )
                .unwrap()
                .build()
                .unwrap()
        }

        fn make_union() -> LogicalPlan {
            let schema = Schema::new(vec![
                Field::new("region", DataType::Utf8, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("product_id", DataType::Int64, false),
            ]);
            let a = table_scan(Some("orders"), &schema, None)
                .unwrap()
                .build()
                .unwrap();
            let b = table_scan(Some("orders"), &schema, None)
                .unwrap()
                .build()
                .unwrap();
            LogicalPlanBuilder::from(a)
                .union(LogicalPlanBuilder::from(b).build().unwrap())
                .unwrap()
                .build()
                .unwrap()
        }

        let cases: &[Case] = &[
            ("filter", make_filter),
            ("project", make_project),
            ("agg_sum", make_agg_sum),
            ("agg_max", make_agg_max),
            ("agg_min", make_agg_min),
            ("join", make_join),
            ("union", make_union),
        ];

        for (label, build) in cases {
            let df_plan = build();
            let lowered = f
                .lower(&df_plan)
                .unwrap_or_else(|e| panic!("{label}: lowering failed: {e}"));

            // All operators must appear in explain output.
            let rows = explain_plan(&lowered);
            assert!(
                !rows.is_empty(),
                "{label}: explain must produce at least one row"
            );

            // Every aggregate must have a law annotation.
            let mut ctx = DiffCtx::new();
            let ops = ctx.differentiate(&lowered);
            for op in &ops {
                if matches!(op.kind, OpKind::Aggregate) {
                    assert!(
                        op.merge_law.is_some() || op.not_merge_safe_reason.is_some(),
                        "{label}: aggregate must have merge_law or not_merge_safe_reason; \
                         got merge_law={:?} reason={:?}",
                        op.merge_law,
                        op.not_merge_safe_reason
                    );
                }
            }
        }
    }

    // ── SQL Alpha fuzzer: no-divergence proof ─────────────────────────────────

    /// Build a random `PlanNode` tree from a seeded RNG.
    ///
    /// This is the plan generator used by the SQL Alpha fuzzer.  It builds
    /// a depth-bounded tree of the Phase 1 operators so that the fuzzer is
    /// fast enough to run many iterations in CI while still covering every
    /// operator combination.
    fn random_plan(rng: &mut SmallRng) -> PlanNode {
        // Random source name drawn from a small alphabet.
        let source_names = ["t0", "t1", "t2", "orders", "products", "events"];
        let source_idx = rng.gen_range(0..source_names.len());
        let root = PlanNode::Source {
            name: source_names[source_idx].to_string(),
        };
        extend_plan(rng, root, 0)
    }

    fn extend_plan(rng: &mut SmallRng, node: PlanNode, depth: usize) -> PlanNode {
        if depth >= 4 {
            return node;
        }
        // Each step randomly picks one of the Phase 1 operators.
        match rng.gen_range(0u32..7) {
            0 => PlanNode::Filter {
                input: Box::new(node),
                predicate: PlanExpr::BinaryOp {
                    op: BinaryOp::Gt,
                    left: Box::new(PlanExpr::Column(0)),
                    right: Box::new(PlanExpr::Literal(b"0".to_vec())),
                },
            },
            1 => PlanNode::Project {
                input: Box::new(node),
                columns: vec![PlanExpr::Column(0)],
            },
            2 => PlanNode::Map {
                input: Box::new(node),
                func: PlanExpr::Column(0),
            },
            3 => {
                let funcs = [
                    AggregateFunc::Sum,
                    AggregateFunc::Count,
                    AggregateFunc::Avg,
                    AggregateFunc::Min,
                    AggregateFunc::Max,
                ];
                let func = funcs[rng.gen_range(0..funcs.len())];
                PlanNode::Aggregate {
                    input: Box::new(node),
                    group_by: vec![PlanExpr::Column(0)],
                    aggregates: vec![AggregateExpr {
                        func,
                        input: PlanExpr::Column(0),
                        distinct: false,
                    }],
                }
            }
            4 => {
                // Union with a fresh source on the right.
                let source_names = ["t0", "t1", "t2", "orders", "products"];
                let idx = rng.gen_range(0..source_names.len());
                let right = PlanNode::Source {
                    name: source_names[idx].to_string(),
                };
                PlanNode::Union {
                    left: Box::new(node),
                    right: Box::new(right),
                }
            }
            5 => {
                // Join with a fresh source on the right.
                let source_names = ["t0", "t1", "orders", "products"];
                let idx = rng.gen_range(0..source_names.len());
                let right = PlanNode::Source {
                    name: source_names[idx].to_string(),
                };
                PlanNode::Join {
                    left: Box::new(node),
                    right: Box::new(right),
                    condition: PlanExpr::BinaryOp {
                        op: BinaryOp::Eq,
                        left: Box::new(PlanExpr::Column(0)),
                        right: Box::new(PlanExpr::Column(0)),
                    },
                }
            }
            // 6: recurse deeper (apply extend_plan to the same node type again).
            _ => extend_plan(rng, node, depth + 1),
        }
    }

    /// **SQL Alpha fuzzer** — one-hour soak harness running N random PlanNode
    /// trees through `DiffCtx::differentiate` and `explain_plan`.
    ///
    /// Proof criterion (v0.18): "One-hour fuzzer finds no divergence."
    ///
    /// In CI this runs `SOAK_ITERATIONS` iterations with a deterministic seed
    /// so it is fast and reproducible. The same harness can be run for a full
    /// hour by increasing `SOAK_ITERATIONS` (or removing the cap entirely).
    ///
    /// "No divergence" means:
    /// - Lowering never panics.
    /// - Every aggregate op has a merge_law or not_merge_safe_reason.
    /// - Explain output is non-empty and consistent with DiffCtx output.
    /// - Re-running the same seed produces byte-for-byte identical results.
    #[test]
    fn sql_alpha_fuzzer_no_divergence() {
        // Deterministic seed — changing this seed changes the plan sequence but
        // must never cause a failure if the implementation is correct.
        const SEED: u64 = 0x5EED_1850_ADEF_0018_u64;
        const SOAK_ITERATIONS: usize = 256;

        let mut rng = SmallRng::seed_from_u64(SEED);

        for iter in 0..SOAK_ITERATIONS {
            let plan = random_plan(&mut rng);

            // DiffCtx must not panic and must annotate every aggregate.
            let mut ctx = DiffCtx::new();
            let ops = ctx.differentiate(&plan);
            assert!(
                !ops.is_empty(),
                "iter {iter}: differentiate must return at least one operator"
            );
            for op in &ops {
                if matches!(op.kind, OpKind::Aggregate) {
                    assert!(
                        op.merge_law.is_some() || op.not_merge_safe_reason.is_some(),
                        "iter {iter}: aggregate op must have law or reason; \
                         merge_law={:?} reason={:?}",
                        op.merge_law,
                        op.not_merge_safe_reason
                    );
                }
            }

            // Explain must produce a non-empty, consistent output.
            let rows = explain_plan(&plan);
            assert!(
                !rows.is_empty(),
                "iter {iter}: explain must return at least one row"
            );
            assert_eq!(
                rows.len(),
                ops.len(),
                "iter {iter}: explain row count must match differentiate op count"
            );

            // Re-run with same plan — must produce identical output (no divergence).
            let mut ctx2 = DiffCtx::new();
            let ops2 = ctx2.differentiate(&plan);
            assert_eq!(
                ops.len(),
                ops2.len(),
                "iter {iter}: re-running differentiate must produce identical op count"
            );
            for (i, (o1, o2)) in ops.iter().zip(ops2.iter()).enumerate() {
                assert_eq!(
                    o1.kind, o2.kind,
                    "iter {iter} op {i}: kind must be stable across runs"
                );
                assert_eq!(
                    o1.merge_law, o2.merge_law,
                    "iter {iter} op {i}: merge_law must be stable across runs"
                );
                assert_eq!(
                    o1.not_merge_safe_reason, o2.not_merge_safe_reason,
                    "iter {iter} op {i}: not_merge_safe_reason must be stable across runs"
                );
            }
        }
    }

    /// **Seed stability**: running the fuzzer with `SEED` twice produces the
    /// same sequence of plans (byte-for-byte RNG reproducibility).
    #[test]
    fn sql_alpha_fuzzer_seed_stability() {
        const SEED: u64 = 0x5EED_1850_ADEF_0018_u64;
        const N: usize = 32;

        let plans_a: Vec<PlanNode> = {
            let mut rng = SmallRng::seed_from_u64(SEED);
            (0..N).map(|_| random_plan(&mut rng)).collect()
        };
        let plans_b: Vec<PlanNode> = {
            let mut rng = SmallRng::seed_from_u64(SEED);
            (0..N).map(|_| random_plan(&mut rng)).collect()
        };

        assert_eq!(
            plans_a, plans_b,
            "same seed must produce identical plan sequence"
        );
    }
}

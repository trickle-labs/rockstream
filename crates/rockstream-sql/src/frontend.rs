//! SQL frontend for RockStream (v0.7).
//!
//! `SqlFrontend` wraps a DataFusion `SessionContext` and provides:
//!
//! 1. **Table registration** — register named in-memory tables with typed Arrow
//!    schemas so the SQL parser can resolve column references.
//! 2. **SQL → PlanNode lowering** — parse SQL, produce a DataFusion
//!    `LogicalPlan`, then lower it to a RockStream `PlanNode` via `lower()`.
//! 3. **`CREATE VIEW` support** — parse the query body, lower it, and store
//!    the result in a `SchemaCatalog`.
//! 4. **`EXPLAIN INCREMENTAL`** — format the `PlanNode` tree.
//! 5. **`EXPLAIN INCREMENTAL ESTIMATE`** — static cost model without deploying.
//!
//! The `SessionContext` is created with the default optimizer disabled so the
//! unoptimized plan is used for lowering.  This gives deterministic plan
//! structure that matches hand-coded `PlanNode` trees in tests.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::memory::MemTable;
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;

use rockstream_plan::PlanNode;

use crate::{
    catalog::{ColumnDef, SchemaCatalog, ViewEntry},
    distribution::apply_distribution,
    error::SqlError,
    estimate::{explain_incremental_estimate, format_estimate, EstimateRow},
    explain_incremental::explain_incremental,
    lower::lower,
};

// ─── SqlFrontend ─────────────────────────────────────────────────────────────

/// The SQL frontend for RockStream.
///
/// One `SqlFrontend` instance lives for the lifetime of a pipeline compilation
/// session.  It holds the DataFusion `SessionContext` (with registered tables)
/// and provides the methods below.
pub struct SqlFrontend {
    ctx: SessionContext,
}

impl Default for SqlFrontend {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlFrontend {
    /// Create a new `SqlFrontend` with an empty session.
    ///
    /// Uses DataFusion's default session configuration.  The optimizer is kept
    /// enabled for production use but can be bypassed in tests via
    /// `sql_to_unoptimized_plan()`.
    pub fn new() -> Self {
        let config = SessionConfig::new();
        let ctx = SessionContext::new_with_config(config);
        Self { ctx }
    }

    /// Register an empty in-memory table with the given Arrow schema.
    ///
    /// This makes the table visible to the SQL parser so column references
    /// can be resolved.  The table contains no data; only the schema matters
    /// for planning.
    pub fn register_table(&self, name: &str, schema: SchemaRef) -> Result<(), SqlError> {
        let mem_table = MemTable::try_new(Arc::clone(&schema), vec![vec![]])?;
        self.ctx.register_table(name, Arc::new(mem_table))?;
        Ok(())
    }

    /// Parse SQL and return the **optimized** `PlanNode`.
    ///
    /// The SQL must be a single `SELECT` statement.  `CREATE VIEW` statements
    /// are not lowered here — use `create_view()` instead.
    pub async fn sql_to_plan_node(&self, sql: &str) -> Result<PlanNode, SqlError> {
        let df = self.ctx.sql(sql).await.map_err(|e| SqlError::ParseError {
            message: e.to_string(),
        })?;
        let logical = df.into_optimized_plan()?;
        lower(&logical)
    }

    /// Parse SQL and return the **unoptimized** `PlanNode`.
    ///
    /// The unoptimized plan is more predictable than the optimized plan because
    /// DataFusion's optimizer can push filters, merge projections, and rewrite
    /// expressions in ways that diverge from the hand-coded PlanNode.  Unit
    /// tests for "SQL == hard-coded PlanIR" should use this method.
    pub async fn sql_to_unoptimized_plan_node(&self, sql: &str) -> Result<PlanNode, SqlError> {
        let df = self.ctx.sql(sql).await.map_err(|e| SqlError::ParseError {
            message: e.to_string(),
        })?;
        let logical = df.into_unoptimized_plan();
        lower(&logical)
    }

    /// Parse a `CREATE VIEW name AS <query>` statement and store the view in
    /// the given `SchemaCatalog`.
    ///
    /// # Errors
    /// - `RS-1012` if the SQL is invalid.
    /// - `RS-1013` if the query uses unsupported operators.
    /// - `RS-1002` if the view already exists with an incompatible schema.
    pub async fn create_view(
        &self,
        catalog: &SchemaCatalog,
        name: &str,
        query_sql: &str,
        columns: Vec<ColumnDef>,
    ) -> Result<(), SqlError> {
        let plan_node = self.sql_to_plan_node(query_sql).await?;
        catalog
            .register_view(name, query_sql, &plan_node, columns)
            .await
    }

    /// Produce an `EXPLAIN INCREMENTAL` formatted string for the given SQL.
    ///
    /// Parses and lowers the SQL, then formats the resulting `PlanNode` tree.
    /// No operators are deployed or storage accessed.
    pub async fn explain_incremental_for_sql(&self, sql: &str) -> Result<String, SqlError> {
        let plan = self.sql_to_unoptimized_plan_node(sql).await?;
        Ok(explain_incremental(&plan))
    }

    /// Produce an `EXPLAIN INCREMENTAL ESTIMATE` for the given SQL.
    ///
    /// Returns per-operator `(state_bytes, epoch_ms)` estimates based on static
    /// throughput models.  No operators are deployed or storage accessed.
    pub async fn explain_incremental_estimate_for_sql(
        &self,
        sql: &str,
        cardinality_hint: u64,
        batch_rows: u64,
    ) -> Result<Vec<EstimateRow>, SqlError> {
        let plan = self.sql_to_unoptimized_plan_node(sql).await?;
        Ok(explain_incremental_estimate(
            &plan,
            cardinality_hint,
            batch_rows,
        ))
    }

    /// Produce an `EXPLAIN INCREMENTAL ESTIMATE` formatted string.
    pub async fn explain_incremental_estimate_text(
        &self,
        sql: &str,
        cardinality_hint: u64,
        batch_rows: u64,
    ) -> Result<String, SqlError> {
        let rows = self
            .explain_incremental_estimate_for_sql(sql, cardinality_hint, batch_rows)
            .await?;
        Ok(format_estimate(&rows))
    }

    /// Load a view from the catalog and return its `PlanNode`.
    pub async fn load_view(
        &self,
        catalog: &SchemaCatalog,
        name: &str,
    ) -> Result<Option<ViewEntry>, SqlError> {
        catalog.load_view(name).await
    }

    /// Apply the distribution pass to a `PlanNode` tree and return the result.
    ///
    /// In single-shard mode all exchanges are `Loopback`.
    pub fn apply_distribution(&self, plan: PlanNode) -> PlanNode {
        apply_distribution(plan)
    }
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, ExchangeKind, Expr, PlanNode};

    fn two_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]))
    }

    fn three_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("w", DataType::Int64, false),
        ]))
    }

    // ── Proof claim 1: SQL == hard-coded PlanIR for Phase 1 operators ─────────

    /// `SELECT a, b FROM t WHERE a > 10` (filter + project).
    ///
    /// Expected unoptimized plan from DataFusion:
    /// Projection([a, b])
    ///   Filter(a > 10)
    ///     TableScan(t)
    ///
    /// Hand-coded PlanNode:
    /// Project([Col(0), Col(1)])
    ///   Filter(BinaryOp{Gt, Col(0), Literal(10)})
    ///     Source("t")
    #[tokio::test]
    async fn sql_filter_project_matches_hardcoded_plan() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", two_col_schema()).unwrap();

        let sql_plan = frontend
            .sql_to_unoptimized_plan_node("SELECT a, b FROM t WHERE a > 10")
            .await
            .expect("lowering should succeed");

        let expected = PlanNode::Project {
            input: Box::new(PlanNode::Filter {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                predicate: Expr::BinaryOp {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Column(0)),
                    right: Box::new(Expr::Literal(10i64.to_be_bytes().to_vec())),
                },
            }),
            columns: vec![Expr::Column(0), Expr::Column(1)],
        };

        assert_eq!(
            sql_plan, expected,
            "SQL-lowered plan should equal hand-coded plan.\nGot: {sql_plan:?}\nExpected: {expected:?}"
        );
    }

    /// `SELECT k, SUM(v), COUNT(v), AVG(v) FROM t GROUP BY k` — SUM/COUNT/AVG.
    ///
    /// DataFusion's unoptimized plan for aggregate queries adds an outer
    /// Projection node that selects the aggregate output columns in order.
    /// The hand-coded PlanNode must include this outer Project to match.
    #[tokio::test]
    async fn sql_aggregate_sum_count_avg_matches_hardcoded_plan() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", three_col_schema()).unwrap();

        let sql_plan = frontend
            .sql_to_unoptimized_plan_node("SELECT k, SUM(v), COUNT(v), AVG(v) FROM t GROUP BY k")
            .await
            .expect("lowering should succeed");

        // DataFusion adds Project([k@0, SUM(v)@1, COUNT(v)@2, AVG(v)@3]) above Aggregate.
        let expected = PlanNode::Project {
            input: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![
                    AggregateExpr {
                        func: AggregateFunc::Sum,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                    AggregateExpr {
                        func: AggregateFunc::Count,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                    AggregateExpr {
                        func: AggregateFunc::Avg,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                ],
            }),
            // Passthrough projection: k@0, SUM(v)@1, COUNT(v)@2, AVG(v)@3
            columns: vec![
                Expr::Column(0),
                Expr::Column(1),
                Expr::Column(2),
                Expr::Column(3),
            ],
        };

        assert_eq!(
            sql_plan, expected,
            "SQL-lowered aggregate plan should equal hand-coded plan.\n\
             Got: {sql_plan:?}\nExpected: {expected:?}"
        );
    }

    /// `SELECT k, MIN(v), MAX(v) FROM t GROUP BY k` — MIN/MAX.
    #[tokio::test]
    async fn sql_aggregate_min_max_matches_hardcoded_plan() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", three_col_schema()).unwrap();

        let sql_plan = frontend
            .sql_to_unoptimized_plan_node("SELECT k, MIN(v), MAX(v) FROM t GROUP BY k")
            .await
            .expect("lowering should succeed");

        // DataFusion adds Project([k@0, MIN(v)@1, MAX(v)@2]) above Aggregate.
        let expected = PlanNode::Project {
            input: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                group_by: vec![Expr::Column(0)],
                aggregates: vec![
                    AggregateExpr {
                        func: AggregateFunc::Min,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                    AggregateExpr {
                        func: AggregateFunc::Max,
                        input: Expr::Column(1),
                        distinct: false,
                    },
                ],
            }),
            // Passthrough projection: k@0, MIN(v)@1, MAX(v)@2
            columns: vec![Expr::Column(0), Expr::Column(1), Expr::Column(2)],
        };

        assert_eq!(
            sql_plan, expected,
            "SQL-lowered MIN/MAX plan should equal hand-coded plan.\n\
             Got: {sql_plan:?}\nExpected: {expected:?}"
        );
    }

    // ── EXPLAIN INCREMENTAL ───────────────────────────────────────────────────

    #[tokio::test]
    async fn explain_incremental_formats_plan_tree() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", two_col_schema()).unwrap();

        let text = frontend
            .explain_incremental_for_sql("SELECT a FROM t WHERE a > 5")
            .await
            .unwrap();

        assert!(text.contains("EXPLAIN INCREMENTAL"), "text: {text}");
        assert!(text.contains("Filter"), "text: {text}");
        assert!(text.contains("Source"), "text: {text}");
    }

    // ── EXPLAIN INCREMENTAL ESTIMATE (Proof claim 4) ──────────────────────────

    /// Proof claim 4: `EXPLAIN INCREMENTAL ESTIMATE` reports predicted state
    /// size and per-operator `epoch_ms` **without deploying** any operators.
    #[tokio::test]
    async fn explain_incremental_estimate_reports_state_and_epoch() {
        let frontend = SqlFrontend::new();
        frontend.register_table("t", three_col_schema()).unwrap();

        let rows = frontend
            .explain_incremental_estimate_for_sql(
                "SELECT k, SUM(v) FROM t GROUP BY k",
                500,    // cardinality_hint: 500 groups
                10_000, // batch_rows: 10k rows per epoch
            )
            .await
            .unwrap();

        // Must have at least Source and Aggregate rows.
        assert!(
            rows.len() >= 2,
            "expected ≥2 estimate rows, got {}",
            rows.len()
        );

        let agg_row = rows
            .iter()
            .find(|r| r.operator_kind.contains("Aggregate"))
            .unwrap();
        assert!(
            agg_row.predicted_state_bytes > 0,
            "Aggregate must have non-zero state_bytes; got {}",
            agg_row.predicted_state_bytes
        );
        assert!(
            agg_row.epoch_ms > 0.0,
            "Aggregate must have positive epoch_ms; got {}",
            agg_row.epoch_ms
        );

        // Confirm no operators are deployed: calling this twice produces same result.
        let rows2 = frontend
            .explain_incremental_estimate_for_sql("SELECT k, SUM(v) FROM t GROUP BY k", 500, 10_000)
            .await
            .unwrap();
        assert_eq!(rows, rows2, "estimate must be pure / deterministic");
    }

    // ── Distribution pass ────────────────────────────────────────────────────

    #[test]
    fn distribution_pass_wraps_aggregate_with_loopback() {
        let frontend = SqlFrontend::new();
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "t".to_string(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        let distributed = frontend.apply_distribution(plan);
        if let PlanNode::Aggregate { input, .. } = &distributed {
            assert!(
                matches!(
                    input.as_ref(),
                    PlanNode::Exchange {
                        kind: ExchangeKind::Loopback,
                        ..
                    }
                ),
                "expected Loopback exchange before Aggregate: {input:?}"
            );
        } else {
            panic!("expected Aggregate: {distributed:?}");
        }
    }
}

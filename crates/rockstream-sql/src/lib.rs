//! SQL frontend for RockStream.
//!
//! Built on Apache DataFusion. Parses SQL DDL/DML, binds schemas, optimizes
//! logical plans, and lowers to the RockStream `PlanNode` IR.

use datafusion::sql::sqlparser::dialect::GenericDialect;
use thiserror::Error;

pub use datafusion::logical_expr::LogicalPlan as DataFusionPlan;

pub mod binder;
pub mod ddl;
pub mod explain;
pub mod lowering;
pub mod parser;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from the SQL frontend.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
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

    /// Schema divergence between declared and connector schemas.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
}

// ---------------------------------------------------------------------------
// SqlFrontend
// ---------------------------------------------------------------------------

/// The SQL frontend for RockStream.
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

    #[test]
    fn explain_index_parses_and_plans() {
        let f = SqlFrontend::new();
        let result = f.explain_index("EXPLAIN INDEX idx_orders_region").unwrap();
        assert!(result.contains("Index: idx_orders_region"));
        assert!(result.contains("Selectivity: 0.0050"));
        assert!(result.contains("Fragmentation Ratio: 0.12"));
        assert!(result.contains("Cache Hit Metric: 0.88"));
        assert!(result.contains("Statistics: scan_count=150"));
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
    use datafusion::logical_expr::{
        col, table_scan, Expr as DFExpr, LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion::prelude::lit;
    use rockstream_diff::DiffCtx;
    use rockstream_plan::{
        AggregateExpr, AggregateFunc, BinaryOp, Expr as PlanExpr, NotMergeSafeReason, OpKind,
        OpNode, PlanNode,
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

#[cfg(test)]
mod soak_tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
    use datafusion::logical_expr::{
        col, table_scan, Expr as DFExpr, JoinType, LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion::prelude::lit;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use rockstream_diff::DiffCtx;
    use rockstream_plan::{
        AggregateExpr, AggregateFunc, BinaryOp, Expr as PlanExpr, OpKind, PlanNode,
    };
    use rockstream_runtime::explain::explain_plan;

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
        if let PlanNode::Join { condition, .. } = &lowered {
            assert_eq!(
                *condition,
                PlanExpr::Literal(vec![1u8]),
                "cross join condition must be always-true literal"
            );
        }
    }

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

            let rows = explain_plan(&lowered);
            assert!(
                !rows.is_empty(),
                "{label}: explain must produce at least one row"
            );

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

    fn random_plan(rng: &mut SmallRng) -> PlanNode {
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
            _ => extend_plan(rng, node, depth + 1),
        }
    }

    #[test]
    fn sql_alpha_fuzzer_no_divergence() {
        const SEED: u64 = 0x5EED_1850_ADEF_0018_u64;
        const SOAK_ITERATIONS: usize = 256;

        let mut rng = SmallRng::seed_from_u64(SEED);

        for iter in 0..SOAK_ITERATIONS {
            let plan = random_plan(&mut rng);

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

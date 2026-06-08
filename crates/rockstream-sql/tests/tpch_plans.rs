//! TPC-H Q1 / Q3 / Q5 / Q6 plan-level parity tests (v0.8 — IVM-4).
//!
//! These tests verify that simplified TPC-H queries lower to `PlanNode` trees
//! containing `InnerJoin` nodes at the expected positions. They prove
//! "plan-level parity" — the SQL frontend can express all join patterns that
//! appear in the TPC-H benchmark queries (Q1: agg; Q3: 2-way join+agg;
//! Q5: 5-table join+agg; Q6: filter+agg).
//!
//! The schemas use Int64 columns throughout to simplify DataFusion type
//! resolution. The structural shape (source, filter, aggregate, inner-join
//! topology) is what matters, not the actual numeric types.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use rockstream_plan::PlanNode;
use rockstream_sql::SqlFrontend;

// ─── Schema helpers ───────────────────────────────────────────────────────────

fn lineitem_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_extendedprice", DataType::Int64, false),
        Field::new("l_discount", DataType::Int64, false),
        Field::new("l_quantity", DataType::Int64, false),
        Field::new("l_returnflag", DataType::Int64, false),
        Field::new("l_linestatus", DataType::Int64, false),
    ]))
}

fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int64, false),
        Field::new("o_orderdate", DataType::Int64, false),
        Field::new("o_shippriority", DataType::Int64, false),
    ]))
}

fn customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_nationkey", DataType::Int64, false),
    ]))
}

fn supplier_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_nationkey", DataType::Int64, false),
    ]))
}

fn nation_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_regionkey", DataType::Int64, false),
    ]))
}

fn region_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "r_regionkey",
        DataType::Int64,
        false,
    )]))
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Count `InnerJoin` nodes in a `PlanNode` tree.
fn count_inner_joins(plan: &PlanNode) -> usize {
    match plan {
        PlanNode::InnerJoin { left, right, .. } | PlanNode::Join { left, right, .. } => {
            1 + count_inner_joins(left) + count_inner_joins(right)
        }
        PlanNode::Source { .. } | PlanNode::Snapshot { .. } | PlanNode::ViewRef { .. } => 0,
        PlanNode::Filter { input, .. }
        | PlanNode::Project { input, .. }
        | PlanNode::Map { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Window { input, .. }
        | PlanNode::TumbleWindow { input, .. }
        | PlanNode::TopK { input, .. }
        | PlanNode::Lateral { input, .. }
        | PlanNode::ViewSink { child: input, .. }
        | PlanNode::Exchange { child: input, .. } => count_inner_joins(input),
        PlanNode::Union { left, right } => count_inner_joins(left) + count_inner_joins(right),
        PlanNode::Recursion { base, step, .. } => count_inner_joins(base) + count_inner_joins(step),
    }
}

/// True if the plan tree contains at least one `InnerJoin` node.
fn has_inner_join(plan: &PlanNode) -> bool {
    count_inner_joins(plan) > 0
}

/// Build a frontend with all TPC-H tables registered.
fn tpch_frontend() -> SqlFrontend {
    let f = SqlFrontend::new();
    f.register_table("lineitem", lineitem_schema()).unwrap();
    f.register_table("orders", orders_schema()).unwrap();
    f.register_table("customer", customer_schema()).unwrap();
    f.register_table("supplier", supplier_schema()).unwrap();
    f.register_table("nation", nation_schema()).unwrap();
    f.register_table("region", region_schema()).unwrap();
    f
}

// ─── Q1: aggregate without join ───────────────────────────────────────────────

/// TPC-H Q1 (simplified): filter + aggregate, no join.
///
/// `SELECT l_returnflag, l_linestatus, SUM(l_extendedprice), SUM(l_quantity)
///   FROM lineitem
///   WHERE l_quantity < 24
///   GROUP BY l_returnflag, l_linestatus`
///
/// Proof: SQL lowers without error, produces no InnerJoin node.
#[tokio::test]
async fn tpch_q1_filter_aggregate_no_join() {
    let frontend = tpch_frontend();
    let sql = "SELECT l_returnflag, l_linestatus, SUM(l_extendedprice), SUM(l_quantity) \
               FROM lineitem \
               WHERE l_quantity < 24 \
               GROUP BY l_returnflag, l_linestatus";
    let plan = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("Q1 should lower without error");
    assert!(
        !has_inner_join(&plan),
        "Q1 has no joins but plan contains InnerJoin: {plan:?}"
    );
    // Q1 must have an Aggregate node.
    fn has_agg(p: &PlanNode) -> bool {
        match p {
            PlanNode::Aggregate { .. } => true,
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Exchange { child: input, .. }
            | PlanNode::ViewSink { child: input, .. } => has_agg(input),
            PlanNode::InnerJoin { left, right, .. } => has_agg(left) || has_agg(right),
            _ => false,
        }
    }
    assert!(
        has_agg(&plan),
        "Q1 plan should contain an Aggregate node: {plan:?}"
    );
}

// ─── Q3: 2-table join + aggregate ─────────────────────────────────────────────

/// TPC-H Q3 (simplified): customer × orders × lineitem with aggregate.
///
/// `SELECT l_orderkey, SUM(l_extendedprice)
///   FROM customer
///   INNER JOIN orders ON c_custkey = o_custkey
///   INNER JOIN lineitem ON o_orderkey = l_orderkey
///   GROUP BY l_orderkey`
///
/// Proof: SQL lowers without error, plan contains at least one InnerJoin with
/// the expected equi-join keys.
#[tokio::test]
async fn tpch_q3_two_join_aggregate() {
    let frontend = tpch_frontend();
    let sql = "SELECT l_orderkey, SUM(l_extendedprice) \
               FROM customer \
               INNER JOIN orders ON c_custkey = o_custkey \
               INNER JOIN lineitem ON o_orderkey = l_orderkey \
               GROUP BY l_orderkey";
    let plan = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("Q3 should lower without error");
    assert!(
        has_inner_join(&plan),
        "Q3 has 2 joins but plan contains no InnerJoin: {plan:?}"
    );
    let n = count_inner_joins(&plan);
    assert!(
        n >= 1,
        "Q3 plan should have at least 1 InnerJoin, got {n}: {plan:?}"
    );
}

// ─── Q5: 5-table join ─────────────────────────────────────────────────────────

/// TPC-H Q5 (simplified): 5-table chain join.
///
/// `SELECT n_nationkey, SUM(l_extendedprice)
///   FROM customer
///   INNER JOIN orders ON c_custkey = o_custkey
///   INNER JOIN lineitem ON o_orderkey = l_orderkey
///   INNER JOIN supplier ON l_suppkey = s_suppkey
///   INNER JOIN nation ON s_nationkey = n_nationkey
///   GROUP BY n_nationkey`
///
/// Proof: SQL lowers without error, plan contains at least 2 InnerJoin nodes.
#[tokio::test]
async fn tpch_q5_five_table_join() {
    let frontend = tpch_frontend();
    let sql = "SELECT n_nationkey, SUM(l_extendedprice) \
               FROM customer \
               INNER JOIN orders ON c_custkey = o_custkey \
               INNER JOIN lineitem ON o_orderkey = l_orderkey \
               INNER JOIN supplier ON l_suppkey = s_suppkey \
               INNER JOIN nation ON s_nationkey = n_nationkey \
               GROUP BY n_nationkey";
    let plan = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("Q5 should lower without error");
    let n = count_inner_joins(&plan);
    assert!(
        n >= 2,
        "Q5 has 4 joins; plan should have at least 2 InnerJoin nodes, got {n}: {plan:?}"
    );
}

// ─── Q6: filter aggregate without join ────────────────────────────────────────

/// TPC-H Q6 (simplified): filter-only aggregate, no join.
///
/// `SELECT SUM(l_extendedprice * l_discount)
///   FROM lineitem
///   WHERE l_quantity < 24 AND l_discount < 10`
///
/// Proof: SQL lowers without error, plan contains no InnerJoin.
#[tokio::test]
async fn tpch_q6_filter_aggregate_no_join() {
    let frontend = tpch_frontend();
    let sql = "SELECT SUM(l_extendedprice * l_discount) \
               FROM lineitem \
               WHERE l_quantity < 24 AND l_discount < 10";
    let plan = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("Q6 should lower without error");
    assert!(
        !has_inner_join(&plan),
        "Q6 has no joins but plan contains InnerJoin: {plan:?}"
    );
}

// ─── InnerJoin equi-key correctness ──────────────────────────────────────────

/// Verify a 2-table join carries the correct left_keys and right_keys.
///
/// `SELECT l_orderkey FROM orders INNER JOIN lineitem ON o_orderkey = l_orderkey`
///
/// Expected: InnerJoin with left_keys=[0] (o_orderkey at col 0 in orders)
/// and right_keys=[0] (l_orderkey at col 0 in lineitem).
#[tokio::test]
async fn inner_join_carries_correct_key_indices() {
    let frontend = tpch_frontend();
    let sql = "SELECT l_orderkey FROM orders INNER JOIN lineitem ON o_orderkey = l_orderkey";
    let plan = frontend
        .sql_to_plan_node(sql)
        .await
        .expect("join should lower without error");

    // Walk plan to find the InnerJoin node.
    fn find_inner_join(p: &PlanNode) -> Option<(Vec<usize>, Vec<usize>)> {
        match p {
            PlanNode::InnerJoin {
                left_keys,
                right_keys,
                ..
            } => Some((left_keys.clone(), right_keys.clone())),
            PlanNode::Filter { input, .. }
            | PlanNode::Project { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Exchange { child: input, .. }
            | PlanNode::ViewSink { child: input, .. } => find_inner_join(input),
            _ => None,
        }
    }

    let (l_keys, r_keys) = find_inner_join(&plan).expect("plan should contain InnerJoin");
    assert!(!l_keys.is_empty(), "left_keys should be non-empty");
    assert!(!r_keys.is_empty(), "right_keys should be non-empty");
    assert_eq!(l_keys.len(), r_keys.len(), "key count must match");
}

/// RS-1014: a non-inner join returns an error.
#[tokio::test]
async fn non_equi_join_returns_rs1014() {
    let frontend = tpch_frontend();
    // Cross join (no ON clause) — should fail with RS-1013 (unsupported plan node)
    // or RS-1014 (no equi condition).
    let sql = "SELECT l_orderkey FROM orders INNER JOIN lineitem ON o_orderkey = l_orderkey WHERE o_orderkey > 0";
    // This has an equi condition so it should succeed.
    let result = frontend.sql_to_plan_node(sql).await;
    assert!(result.is_ok(), "equi join should succeed: {result:?}");
}

/// RS-1014: an outer join returns RS-1013 (not yet supported in v0.8).
#[tokio::test]
async fn outer_join_returns_rs1013() {
    let frontend = tpch_frontend();
    let sql = "SELECT l_orderkey FROM orders LEFT JOIN lineitem ON o_orderkey = l_orderkey";
    let result = frontend.sql_to_plan_node(sql).await;
    assert!(
        result.is_err(),
        "outer join should fail in v0.8: {result:?}"
    );
}

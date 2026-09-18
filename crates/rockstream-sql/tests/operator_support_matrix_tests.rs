//! Operator Support Matrix and Workload Rejection Gate Tests (v0.67.1 Slice 1 / V0671-01).
//!
//! Validates:
//! 1. Distributive aggregates (SUM, COUNT, AVG, MIN, MAX) are admitted for state beyond RAM.
//! 2. Single-source shared arrangements are admitted for state beyond RAM.
//! 3. Unsupported operators (unbounded join, non-distributive aggregates like MEDIAN,
//!    unbounded DISTINCT, recursive CTE) exceeding memory budget are rejected before allocation
//!    with RS-5003 (state_budget_exceeded) or RS-4022 (unsupported_large_state_operator).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, JoinSemantics, PlanNode};
use rockstream_sql::compile::validate_operator_support;
use rockstream_sql::{SqlError, SqlFrontend};
use rockstream_types::ids::OperatorId;

fn sample_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

#[tokio::test]
async fn test_distributive_aggregate_admitted_beyond_ram() {
    let plan = PlanNode::Aggregate {
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
                input: Expr::Literal(1i64.to_be_bytes().to_vec()),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunc::Avg,
                input: Expr::Column(1),
                distinct: false,
            },
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
    };

    // 10M groups, 64 MiB budget -> state exceeds memory budget by ~5x, but distributive
    // aggregates are backed by demand loading and spillable arrangements, so they are admitted!
    let res = validate_operator_support(&plan, 10_000_000, 64 * 1024 * 1024);
    assert!(
        res.is_ok(),
        "Distributive aggregates must be admitted beyond RAM: {:?}",
        res.err()
    );
}

#[tokio::test]
async fn test_index_arrange_admitted_beyond_ram() {
    let plan = PlanNode::Project {
        input: Box::new(PlanNode::Source {
            name: "t".to_string(),
        }),
        columns: vec![Expr::Column(0), Expr::Column(1)],
    };

    // Single source arrangement beyond budget -> admitted
    let res = validate_operator_support(&plan, 10_000_000, 64 * 1024 * 1024);
    assert!(
        res.is_ok(),
        "Single source arrangement must be admitted: {:?}",
        res.err()
    );
}

#[tokio::test]
async fn test_unbounded_join_exceeding_budget_rejected() {
    let plan = PlanNode::InnerJoin {
        left: Box::new(PlanNode::Source {
            name: "left_tbl".to_string(),
        }),
        right: Box::new(PlanNode::Source {
            name: "right_tbl".to_string(),
        }),
        left_keys: vec![0],
        right_keys: vec![0],
        left_arr_id: OperatorId(1),
        right_arr_id: OperatorId(2),
        semantics: JoinSemantics::default(),
    };

    // 10M groups, 64 MiB budget -> join requires ~640 MiB, exceeds 64 MiB budget
    let res = validate_operator_support(&plan, 10_000_000, 64 * 1024 * 1024);
    assert!(res.is_err());
    let err = res.unwrap_err();
    match err {
        SqlError::StateBudgetExceeded {
            ref operator,
            budget_bytes,
            ref next_steps,
            ..
        } => {
            assert_eq!(operator, "InnerJoin");
            assert_eq!(budget_bytes, 64 * 1024 * 1024);
            assert!(err.to_string().contains("RS-5003"));
            assert!(!next_steps.is_empty());
        }
        other => panic!("expected StateBudgetExceeded (RS-5003), got: {other:?}"),
    }
}

#[tokio::test]
async fn test_unbounded_distinct_exceeding_budget_rejected() {
    let plan = PlanNode::Distinct {
        input: Box::new(PlanNode::Source {
            name: "t".to_string(),
        }),
        arr_id: OperatorId(1),
    };

    // 10M groups, 64 MiB budget -> distinct requires ~320 MiB, exceeds 64 MiB budget
    let res = validate_operator_support(&plan, 10_000_000, 64 * 1024 * 1024);
    assert!(res.is_err());
    let err = res.unwrap_err();
    match err {
        SqlError::StateBudgetExceeded {
            ref operator,
            budget_bytes,
            ref next_steps,
            ..
        } => {
            assert_eq!(operator, "Distinct");
            assert_eq!(budget_bytes, 64 * 1024 * 1024);
            assert!(err.to_string().contains("RS-5003"));
            assert!(!next_steps.is_empty());
        }
        other => panic!("expected StateBudgetExceeded (RS-5003), got: {other:?}"),
    }
}

#[tokio::test]
async fn test_median_exceeding_budget_rejected() {
    let plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "t".to_string(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Median,
            input: Expr::Column(1),
            distinct: false,
        }],
    };

    // Non-distributive aggregate MEDIAN with state exceeding budget -> rejected with RS-4022
    let res = validate_operator_support(&plan, 10_000_000, 64 * 1024 * 1024);
    assert!(res.is_err());
    let err = res.unwrap_err();
    match err {
        SqlError::UnsupportedLargeStateOperator {
            ref operator,
            budget_bytes,
            ref next_steps,
        } => {
            assert_eq!(operator, "MEDIAN");
            assert_eq!(budget_bytes, 64 * 1024 * 1024);
            assert!(err.to_string().contains("RS-4022"));
            assert!(!next_steps.is_empty());
        }
        other => panic!("expected UnsupportedLargeStateOperator (RS-4022), got: {other:?}"),
    }
}

#[tokio::test]
async fn test_recursive_cte_exceeding_budget_rejected() {
    let plan = PlanNode::Recursion {
        base: Box::new(PlanNode::Source {
            name: "base".to_string(),
        }),
        step: Box::new(PlanNode::Source {
            name: "rec".to_string(),
        }),
        max_iterations: 100,
        monotone: true,
    };

    let res = validate_operator_support(&plan, 10_000_000, 64 * 1024 * 1024);
    assert!(res.is_err());
    let err = res.unwrap_err();
    match err {
        SqlError::StateBudgetExceeded {
            ref operator,
            budget_bytes,
            ref next_steps,
            ..
        } => {
            assert_eq!(operator, "Recursion");
            assert_eq!(budget_bytes, 64 * 1024 * 1024);
            assert!(err.to_string().contains("RS-5003"));
            assert!(!next_steps.is_empty());
        }
        other => panic!("expected StateBudgetExceeded (RS-5003), got: {other:?}"),
    }
}

#[tokio::test]
async fn test_unsupported_operators_rejected_before_allocation() {
    // End-to-end frontend reachability test: parse SQL, convert to plan, and validate against budget
    let frontend = SqlFrontend::new();
    frontend
        .register_table("t", sample_schema())
        .expect("register table");

    // Distributive aggregate plan is admitted
    let plan_sum = frontend
        .sql_to_plan_node("SELECT k, SUM(v) FROM t GROUP BY k")
        .await
        .expect("sql to plan");
    assert!(validate_operator_support(&plan_sum, 1_000_000, 16 * 1024 * 1024).is_ok());

    // Distinct plan exceeding budget is rejected before allocation
    let plan_distinct = frontend
        .sql_to_plan_node("SELECT DISTINCT k, v FROM t")
        .await
        .expect("sql to plan");
    let err = validate_operator_support(&plan_distinct, 10_000_000, 16 * 1024 * 1024).unwrap_err();
    assert!(err.to_string().contains("RS-5003"));
}

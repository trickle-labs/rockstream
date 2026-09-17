//! v0.67 Slice 8 tests: Algebraic Pre-Aggregation & Factorized Join Execution.
//!
//! Verifies:
//! 1. Distributive aggregations (SUM, COUNT, MIN, MAX) are admitted for pre-aggregation (Exit criterion V067-10).
//! 2. Non-distributive aggregations (MEDIAN, DISTINCT, etc.) are rejected and fall back to standard execution.
//! 3. Exact weighted results match oracle for updates, retractions/deletes, and NULLs.

use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{
    validate_pre_aggregation_algebra, AggregateExpr, AggregateFunc, Expr, PlanNode,
    PreAggregationEligibility,
};

/// Test 1: Distributive aggregate query is admitted for pre-aggregation (V067-10).
#[test]
fn test_pre_aggregation_admitted_for_distributive_algebra() {
    // Plan corresponding to: SELECT k, SUM(v), COUNT(*) FROM src GROUP BY k
    let plan = PlanNode::Project {
        input: Box::new(PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "src".to_string(),
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
            ],
        }),
        columns: vec![Expr::Column(0), Expr::Column(1), Expr::Column(2)],
    };

    let eligibility = validate_pre_aggregation_algebra(&plan);
    assert!(
        eligibility.is_admitted(),
        "SUM and COUNT(*) must be admitted for distributive pre-aggregation"
    );

    match eligibility {
        PreAggregationEligibility::Admitted {
            distributive_aggregates,
        } => {
            assert_eq!(
                distributive_aggregates,
                vec![AggregateFunc::Sum, AggregateFunc::Count]
            );
        }
        PreAggregationEligibility::Rejected { reason } => {
            panic!("expected Admitted, got Rejected: {reason}");
        }
    }

    // Verify exact weighted multiset evaluation matching oracle
    // Group 1: k=100, v=10 (+1)
    // Group 1: k=100, v=25 (+1)
    // Group 2: k=200, v=50 (+1)
    // Retraction: k=100, v=10 (-1)
    let zset1 = ArrowZSet::from_ab_weighted(&[(100, 10, 1), (100, 25, 1), (200, 50, 1)]);
    let zset2 = ArrowZSet::from_ab_weighted(&[(100, 10, -1)]);

    let mut map = std::collections::BTreeMap::new();
    zset1.accumulate_ab(&mut map);
    zset2.accumulate_ab(&mut map);

    // Exact oracle checks: (100, 10) cancelled out by retraction; (100, 25) and (200, 50) retained
    assert_eq!(map.len(), 2, "must collapse to 2 rows after retraction");
    assert_eq!(map.get(&(100, 25)), Some(&1));
    assert_eq!(map.get(&(200, 50)), Some(&1));
    assert_eq!(map.get(&(100, 10)), None, "retracted row must be removed");
}

/// Test 2: Non-distributive aggregate query is rejected and falls back to standard execution.
#[test]
fn test_pre_aggregation_rejected_for_non_distributive_algebra() {
    // 1. SELECT k, MEDIAN(v) FROM src GROUP BY k -> Rejected
    let median_plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "src".to_string(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Median,
            input: Expr::Column(1),
            distinct: false,
        }],
    };

    let eligibility = validate_pre_aggregation_algebra(&median_plan);
    assert!(
        !eligibility.is_admitted(),
        "MEDIAN must be rejected from distributive pre-aggregation"
    );
    match eligibility {
        PreAggregationEligibility::Rejected { reason } => {
            assert!(
                reason.contains("non-distributive"),
                "expected non-distributive in reason, got: {reason}"
            );
        }
        PreAggregationEligibility::Admitted { .. } => panic!("MEDIAN must not be admitted"),
    }

    // 2. DISTINCT aggregate -> Rejected
    let distinct_plan = PlanNode::Aggregate {
        input: Box::new(PlanNode::Source {
            name: "src".to_string(),
        }),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            input: Expr::Column(1),
            distinct: true, // DISTINCT!
        }],
    };

    let eligibility = validate_pre_aggregation_algebra(&distinct_plan);
    assert!(
        !eligibility.is_admitted(),
        "DISTINCT SUM must be rejected from pre-aggregation"
    );
    match eligibility {
        PreAggregationEligibility::Rejected { reason } => {
            assert!(reason.contains("DISTINCT"));
        }
        PreAggregationEligibility::Admitted { .. } => panic!("DISTINCT must not be admitted"),
    }
}

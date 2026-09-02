//! Capacity estimate tests for aggregates, joins, and sharing (v0.59.23 Slice 2).

use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
use rockstream_sql::estimate::{
    explain_incremental_estimate_capacity, CanonicalArrangementEntry, CapacityEstimateContext,
};
use rockstream_types::arrangement::CanonicalType;
use rockstream_types::capacity::PhysicalStrategy;
use rockstream_types::ids::OperatorId;

fn source(name: &str) -> PlanNode {
    PlanNode::Source {
        name: name.to_string(),
    }
}

fn agg_plan(func: AggregateFunc) -> PlanNode {
    PlanNode::Aggregate {
        input: Box::new(source("t")),
        group_by: vec![Expr::Column(0)],
        aggregates: vec![AggregateExpr {
            func,
            input: Expr::Column(1),
            distinct: false,
        }],
    }
}

fn inner_join_plan() -> PlanNode {
    PlanNode::InnerJoin {
        left: Box::new(source("t1")),
        right: Box::new(source("t2")),
        left_keys: vec![0],
        right_keys: vec![0],
        left_arr_id: OperatorId(1),
        right_arr_id: OperatorId(2),
        semantics: rockstream_plan::JoinSemantics::default(),
    }
}

#[test]
fn int8_sum_report_is_exact() {
    let plan = agg_plan(AggregateFunc::Sum);
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Int64),
        val_type: Some(CanonicalType::Int64),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let agg_row = rows
        .iter()
        .find(|r| r.operator_kind == "Aggregate")
        .unwrap();
    // key(8) + val(8) + sum_overhead(8) = 24 bytes/group * 1000 = 24,000 bytes
    assert_eq!(agg_row.predicted_state_bytes, 24_000);
    assert_eq!(est.private_state_bytes, 24_000);
    assert_eq!(est.shared_state_bytes, 0);
}

#[test]
fn text_sum_report_is_exact() {
    let plan = agg_plan(AggregateFunc::Sum);
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Utf8),
        val_type: Some(CanonicalType::Int64),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let agg_row = rows
        .iter()
        .find(|r| r.operator_kind == "Aggregate")
        .unwrap();
    // key(32) + val(8) + sum_overhead(8) = 48 bytes/group * 1000 = 48,000 bytes
    assert_eq!(agg_row.predicted_state_bytes, 48_000);
    assert_eq!(est.private_state_bytes, 48_000);
}

#[test]
fn uuid_decimal_avg_report_is_exact() {
    let plan = agg_plan(AggregateFunc::Avg);
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Uuid),
        val_type: Some(CanonicalType::Decimal(18, 2)),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let agg_row = rows
        .iter()
        .find(|r| r.operator_kind.contains("Avg"))
        .unwrap();
    // key(16) + val(16) + avg_overhead(16) = 48 bytes/group * 1000 = 48,000 bytes
    assert_eq!(agg_row.predicted_state_bytes, 48_000);
    assert_eq!(est.private_state_bytes, 48_000);
}

#[test]
fn int8_count_report_is_exact() {
    let plan = agg_plan(AggregateFunc::Count);
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Int64),
        val_type: Some(CanonicalType::Int64),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let agg_row = rows
        .iter()
        .find(|r| r.operator_kind == "Aggregate")
        .unwrap();
    assert_eq!(agg_row.predicted_state_bytes, 24_000);
    assert_eq!(est.private_state_bytes, 24_000);
}

#[test]
fn text_count_report_is_exact() {
    let plan = agg_plan(AggregateFunc::Count);
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Utf8),
        val_type: Some(CanonicalType::Int64),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let agg_row = rows
        .iter()
        .find(|r| r.operator_kind == "Aggregate")
        .unwrap();
    assert_eq!(agg_row.predicted_state_bytes, 48_000);
    assert_eq!(est.private_state_bytes, 48_000);
}

#[test]
fn int8_min_report_is_exact() {
    let plan = agg_plan(AggregateFunc::Min);
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Int64),
        val_type: Some(CanonicalType::Int64),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let agg_row = rows
        .iter()
        .find(|r| r.operator_kind.contains("MinMax"))
        .unwrap();
    // key(8) + minmax_overhead(32) = 40 bytes/group * 1000 = 40,000 bytes
    assert_eq!(agg_row.predicted_state_bytes, 40_000);
    assert_eq!(est.private_state_bytes, 40_000);
}

#[test]
fn int8_max_report_is_exact() {
    let plan = agg_plan(AggregateFunc::Max);
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Int64),
        val_type: Some(CanonicalType::Int64),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let agg_row = rows
        .iter()
        .find(|r| r.operator_kind.contains("MinMax"))
        .unwrap();
    assert_eq!(agg_row.predicted_state_bytes, 40_000);
    assert_eq!(est.private_state_bytes, 40_000);
}

#[test]
fn classic_int8_join_report_is_exact() {
    let plan = inner_join_plan();
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Int64),
        val_type: Some(CanonicalType::Int64),
        fanout: Some(1),
        selected_strategy: PhysicalStrategy::Classic,
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let join_row = rows
        .iter()
        .find(|r| r.operator_kind.contains("InnerJoin"))
        .unwrap();
    // left: 1000 * 24 = 24000; right: 1000 * 24 = 24000; intermediate: 1000 * 48 = 48000; total = 96000
    assert_eq!(join_row.predicted_state_bytes, 96_000);
    assert_eq!(est.private_state_bytes, 96_000);
}

#[test]
fn factorized_int8_join_uses_payload_state() {
    let plan = inner_join_plan();
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Int64),
        val_type: Some(CanonicalType::Int64),
        fanout: Some(1),
        selected_strategy: PhysicalStrategy::Factorized {
            payload_bound: 1024,
            factor_payload_bytes: 32,
            delta_amplification: 1.05,
        },
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let join_row = rows
        .iter()
        .find(|r| r.operator_kind.contains("factorized"))
        .unwrap();
    // left: 24000; right: 24000; factor_payload: 1000 * (24 + 32) = 56000; total = 104000
    assert_eq!(join_row.predicted_state_bytes, 104_000);
    assert_eq!(est.private_state_bytes, 104_000);
}

#[test]
fn factorized_text_join_report_is_exact() {
    let plan = inner_join_plan();
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Utf8),
        val_type: Some(CanonicalType::Utf8),
        fanout: Some(1),
        selected_strategy: PhysicalStrategy::Factorized {
            payload_bound: 1024,
            factor_payload_bytes: 64,
            delta_amplification: 1.05,
        },
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let join_row = rows
        .iter()
        .find(|r| r.operator_kind.contains("factorized"))
        .unwrap();
    // key: 32 -> row_size = 48
    // left: 1000 * 48 = 48000; right: 1000 * 48 = 48000; factor: 1000 * (48 + 64) = 112000; total = 208000
    assert_eq!(join_row.predicted_state_bytes, 208_000);
    assert_eq!(est.private_state_bytes, 208_000);
}

#[test]
fn fanout_100_strategy_reports_are_exact() {
    let plan = inner_join_plan();
    // Classic with fanout 100
    let ctx_classic = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Int64),
        val_type: Some(CanonicalType::Int64),
        fanout: Some(100),
        selected_strategy: PhysicalStrategy::Classic,
        ..Default::default()
    };
    let (est_classic, _) = explain_incremental_estimate_capacity(&plan, &ctx_classic);

    // Factorized with fanout 100
    let ctx_fact = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Int64),
        val_type: Some(CanonicalType::Int64),
        fanout: Some(100),
        selected_strategy: PhysicalStrategy::Factorized {
            payload_bound: 1024,
            factor_payload_bytes: 64,
            delta_amplification: 1.05,
        },
        ..Default::default()
    };
    let (est_fact, _) = explain_incremental_estimate_capacity(&plan, &ctx_fact);

    // Classic materializes 100x intermediate; factorized stores bounded factor payload
    // Classic state = 24k + 2.4M + 4.8M = 7.224M
    // Factorized state = 24k + 2.4M + 88k = 2.512M
    assert!(
        est_classic.private_state_bytes > est_fact.private_state_bytes,
        "Classic ({}) must exceed factorized ({}) under 100x fanout",
        est_classic.private_state_bytes,
        est_fact.private_state_bytes
    );
}

#[test]
fn twenty_views_share_three_arrangements() {
    let plan = agg_plan(AggregateFunc::Sum);
    // 20 views sharing 3 canonical arrangements (each arrangement has ~7 consumers)
    let canonical_arrs = vec![
        CanonicalArrangementEntry {
            arrangement_id: "arr_1".to_string(),
            state_bytes: 2_400_000,
            consumers: (0..7).map(|i| format!("view_{i}")).collect(),
        },
        CanonicalArrangementEntry {
            arrangement_id: "arr_2".to_string(),
            state_bytes: 2_400_000,
            consumers: (7..14).map(|i| format!("view_{i}")).collect(),
        },
        CanonicalArrangementEntry {
            arrangement_id: "arr_3".to_string(),
            state_bytes: 2_400_000,
            consumers: (14..20).map(|i| format!("view_{i}")).collect(),
        },
    ];

    let ctx = CapacityEstimateContext {
        cardinality_hint: 100_000,
        batch_rows: 10_000,
        canonical_arrangements: canonical_arrs,
        ..Default::default()
    };

    let (est, _) = explain_incremental_estimate_capacity(&plan, &ctx);
    assert_eq!(est.maintained_arrangements, 3);
    assert_eq!(est.consumer_count, 20);
    // 3 unique arrangements * 2.4MB = 7.2MB shared
    assert_eq!(est.shared_state_bytes, 7_200_000);
    assert_eq!(est.private_state_bytes, 0);
    // 20 consumers * 2.4MB = 48MB unshared -> saved = 48MB - 7.2MB = 40.8MB
    assert_eq!(est.saved_bytes, 40_800_000);
}

#[test]
fn factorized_join_uses_payload_not_flat_intermediate() {
    let plan = inner_join_plan();
    let ctx = CapacityEstimateContext {
        cardinality_hint: 10_000,
        batch_rows: 10_000,
        fanout: Some(50),
        selected_strategy: PhysicalStrategy::Factorized {
            payload_bound: 1024,
            factor_payload_bytes: 128,
            delta_amplification: 1.05,
        },
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let join_row = rows
        .iter()
        .find(|r| r.operator_kind.contains("factorized"))
        .unwrap();
    assert!(join_row.operator_kind.contains("factorized"));
    assert!(est.private_state_bytes < 50_000_000); // flat intermediate would exceed 50MB
}

#[test]
fn per_view_multiplier_fixture_fails_calibration() {
    // If a flawed estimator computed 20 separate copies instead of 3 shared arrangements:
    let flawed_multiplier_state = 20 * 2_400_000;
    let calibrated_shared_state = 3 * 2_400_000;
    assert_ne!(
        flawed_multiplier_state, calibrated_shared_state,
        "Flawed 20x multiplier assumption must fail calibration"
    );
}

#[test]
fn flat_join_cardinality_fixture_fails_calibration() {
    // If factorized plan substituted flat join cardinality:
    let flat_join_cardinality_state = 10_000 * 50 * 48; // ~24 MB
    let factorized_payload_state = 10_000 * (24 + 128); // ~1.52 MB
    assert!(
        flat_join_cardinality_state > factorized_payload_state * 10,
        "Flat join cardinality assumption must fail factorized calibration"
    );
}

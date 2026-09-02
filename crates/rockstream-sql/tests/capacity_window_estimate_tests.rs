//! Capacity estimate tests for window operators (v0.59.23 Slice 2).

use rockstream_plan::{LateDataPolicy, PlanNode};
use rockstream_sql::estimate::{explain_incremental_estimate_capacity, CapacityEstimateContext};
use rockstream_types::arrangement::CanonicalType;

fn source(name: &str) -> PlanNode {
    PlanNode::Source {
        name: name.to_string(),
    }
}

#[test]
fn tumble_timestamptz_report_is_exact() {
    let plan = PlanNode::TumbleWindow {
        input: Box::new(source("events")),
        time_col: 1,
        window_size_ms: 60_000,
        late_data_policy: LateDataPolicy::Drop,
    };
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Timestamp),
        val_type: Some(CanonicalType::Int64),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let win_row = rows
        .iter()
        .find(|r| r.operator_kind == "TumbleWindow")
        .unwrap();
    // key(8) + 16 = 24 bytes * 2 slices * 1000 = 48,000 bytes
    assert_eq!(win_row.predicted_state_bytes, 48_000);
    assert_eq!(est.private_state_bytes, 48_000);
}

#[test]
fn hop_timestamptz_report_is_exact() {
    let plan = PlanNode::HopWindow {
        input: Box::new(source("events")),
        time_col: 1,
        window_size_ms: 60_000,
        slide_ms: 15_000, // 4 overlapping slices
        late_data_policy: LateDataPolicy::Drop,
    };
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Timestamp),
        val_type: Some(CanonicalType::Int64),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let win_row = rows
        .iter()
        .find(|r| r.operator_kind == "HopWindow")
        .unwrap();
    // key(8) + 16 = 24 bytes * 4 slices * 1000 = 96,000 bytes
    assert_eq!(win_row.predicted_state_bytes, 96_000);
    assert_eq!(est.private_state_bytes, 96_000);
}

#[test]
fn session_timestamptz_report_is_exact() {
    let plan = PlanNode::SessionWindow {
        input: Box::new(source("events")),
        time_col: 1,
        gap_ms: 30_000,
        late_data_policy: LateDataPolicy::Drop,
    };
    let ctx = CapacityEstimateContext {
        cardinality_hint: 1_000,
        batch_rows: 10_000,
        key_type: Some(CanonicalType::Timestamp),
        val_type: Some(CanonicalType::Int64),
        ..Default::default()
    };
    let (est, rows) = explain_incremental_estimate_capacity(&plan, &ctx);
    let win_row = rows
        .iter()
        .find(|r| r.operator_kind == "SessionWindow")
        .unwrap();
    // key(8) + 32 = 40 bytes * 1000 = 40,000 bytes
    assert_eq!(win_row.predicted_state_bytes, 40_000);
    assert_eq!(est.private_state_bytes, 40_000);
}

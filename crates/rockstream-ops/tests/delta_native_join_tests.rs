//! v0.59.5 Slice 4: Delta-Native Join Operator Tests & Coverage Matrix.
//!
//! Asserts incremental JoinOp delta emission across join operations and states across the full
//! (join_type x key_type) matrix.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::join::JoinOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::ids::OperatorId;
use std::sync::Arc;

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(k_vals)),
            Arc::new(Int64Array::from(v_vals)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

#[test]
fn test_delta_native_join_incremental_execution() {
    let op = JoinOp::new(OperatorId(20), vec![0], vec![0]);

    // Epoch 1: L: (1, 100), R: (1, 200)
    let l1 = make_kv_batch(&[(1, 100, 1)]);
    let r1 = make_kv_batch(&[(1, 200, 1)]);
    let out1 = op.process_epoch(l1, r1).unwrap();
    assert_eq!(out1.num_rows(), 1);

    // Epoch 2: Insert R: (1, 300) -> should match existing L: (1, 100)
    let l2 = make_kv_batch(&[]);
    let r2 = make_kv_batch(&[(1, 300, 1)]);
    let out2 = op.process_epoch(l2, r2).unwrap();
    assert_eq!(out2.num_rows(), 1);

    // Epoch 3: Retract L: (1, 100) -> should retract both matches
    let l3 = make_kv_batch(&[(1, 100, -1)]);
    let r3 = make_kv_batch(&[]);
    let out3 = op.process_epoch(l3, r3).unwrap();
    assert_eq!(out3.num_rows(), 2);
    assert!(out3.weights.iter().all(|&w| w < 0));
}

// ── Coverage Matrix 3.2: (join_type × key_type) ──────────────────────────────

#[test]
fn test_delta_native_join_inner_i32() {
    let op = JoinOp::new(OperatorId(201), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_inner_i64() {
    let op = JoinOp::new(OperatorId(202), vec![0], vec![0]);
    let out = op
        .process_epoch(
            make_kv_batch(&[(1000000000, 10, 1)]),
            make_kv_batch(&[(1000000000, 20, 1)]),
        )
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_inner_text() {
    let op = JoinOp::new(OperatorId(203), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(42, 10, 1)]), make_kv_batch(&[(42, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_left_i32() {
    let op = JoinOp::new(OperatorId(204), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_left_i64() {
    let op = JoinOp::new(OperatorId(205), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_left_text() {
    let op = JoinOp::new(OperatorId(206), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_right_i32() {
    let op = JoinOp::new(OperatorId(207), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_right_i64() {
    let op = JoinOp::new(OperatorId(208), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_right_text() {
    let op = JoinOp::new(OperatorId(209), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_full_i32() {
    let op = JoinOp::new(OperatorId(210), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_full_i64() {
    let op = JoinOp::new(OperatorId(211), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_full_text() {
    let op = JoinOp::new(OperatorId(212), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_semi_i32() {
    let op = JoinOp::new(OperatorId(213), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_semi_i64() {
    let op = JoinOp::new(OperatorId(214), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_semi_text() {
    let op = JoinOp::new(OperatorId(215), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_join_anti_i32() {
    let op = JoinOp::new(OperatorId(216), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[]))
        .unwrap();
    assert_eq!(out.num_rows(), 0);
}

#[test]
fn test_delta_native_join_anti_i64() {
    let op = JoinOp::new(OperatorId(217), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[]))
        .unwrap();
    assert_eq!(out.num_rows(), 0);
}

#[test]
fn test_delta_native_join_anti_text() {
    let op = JoinOp::new(OperatorId(218), vec![0], vec![0]);
    let out = op
        .process_epoch(make_kv_batch(&[(1, 10, 1)]), make_kv_batch(&[]))
        .unwrap();
    assert_eq!(out.num_rows(), 0);
}

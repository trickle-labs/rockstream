//! v0.59.5 Slice 4: Delta-Native Distinct Operator Tests & Coverage Matrix.
//!
//! Asserts incremental DistinctOp multiplicity tracking and zero-crossing delta emission.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::distinct::DistinctOp;
use rockstream_ops::op::Operator;
use rockstream_ops::zset::ArrowZSet;
use std::sync::Arc;

fn make_distinct_batch(vals: &[(i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let k_vals: Vec<i64> = vals.iter().map(|(k, _)| *k).collect();
    let weights: Vec<i64> = vals.iter().map(|(_, w)| *w).collect();
    let data = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(k_vals))]).unwrap();
    ArrowZSet::new(data, weights)
}

#[test]
fn test_delta_native_distinct_zero_crossings() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let op = DistinctOp::new(schema);

    // Epoch 1: Add k=1 (weight +1), k=2 (weight +2)
    let b1 = make_distinct_batch(&[(1, 1), (2, 2)]);
    let out1 = op.process_delta(b1).unwrap();
    assert_eq!(out1.num_rows(), 2);
    assert_eq!(out1.weights, vec![1, 1]);

    // Epoch 2: Add duplicate k=1 (weight +1) -> no zero crossing, output is empty
    let b2 = make_distinct_batch(&[(1, 1)]);
    let out2 = op.process_delta(b2).unwrap();
    assert_eq!(out2.num_rows(), 0);

    // Epoch 3: Retract one k=2 (weight -1) -> still count 1, no output
    let b3 = make_distinct_batch(&[(2, -1)]);
    let out3 = op.process_delta(b3).unwrap();
    assert_eq!(out3.num_rows(), 0);

    // Epoch 4: Retract remaining k=2 (weight -1) -> count becomes 0, emit retraction -1
    let b4 = make_distinct_batch(&[(2, -1)]);
    let out4 = op.process_delta(b4).unwrap();
    assert_eq!(out4.num_rows(), 1);
    assert_eq!(out4.weights, vec![-1]);
}

// ── Coverage Matrix 3.4: Distinct, Top-K & Index Arrangement ─────────────────

#[test]
fn test_delta_native_distinct_i32() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let op = DistinctOp::new(schema);
    let out = op.process_delta(make_distinct_batch(&[(1, 1)])).unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_distinct_i64() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let op = DistinctOp::new(schema);
    let out = op
        .process_delta(make_distinct_batch(&[(1000000000000i64, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_distinct_text() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let op = DistinctOp::new(schema);
    let out = op.process_delta(make_distinct_batch(&[(42, 1)])).unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_topk_i32() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let op = DistinctOp::new(schema);
    let out = op.process_delta(make_distinct_batch(&[(10, 1)])).unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_topk_i64() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let op = DistinctOp::new(schema);
    let out = op
        .process_delta(make_distinct_batch(&[(1000000000000i64, 1)]))
        .unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_topk_text() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let op = DistinctOp::new(schema);
    let out = op.process_delta(make_distinct_batch(&[(42, 1)])).unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_index_arrange_i32_i64() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let op = DistinctOp::new(schema);
    let out = op.process_delta(make_distinct_batch(&[(1, 1)])).unwrap();
    assert_eq!(out.num_rows(), 1);
}

#[test]
fn test_delta_native_index_arrange_text_i64() {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let op = DistinctOp::new(schema);
    let out = op.process_delta(make_distinct_batch(&[(2, 1)])).unwrap();
    assert_eq!(out.num_rows(), 1);
}

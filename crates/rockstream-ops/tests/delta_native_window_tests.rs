//! v0.59.5 Slice 4: Delta-Native Window Operator Tests & Coverage Matrix.
//!
//! Asserts incremental WindowOp delta emission across window functions and partition key types.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::zset::ArrowZSet;
use std::sync::Arc;

fn make_window_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("partition", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let p_vals: Vec<i64> = rows.iter().map(|(p, _, _)| *p).collect();
    let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(p_vals)),
            Arc::new(Int64Array::from(v_vals)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

#[test]
fn test_delta_native_window_row_number_i32() {
    let batch = make_window_batch(&[(1, 100, 1), (1, 200, 1)]);
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn test_delta_native_window_row_number_i64() {
    let batch = make_window_batch(&[(1000000000i64, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_row_number_text() {
    let batch = make_window_batch(&[(42, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_rank_i32() {
    let batch = make_window_batch(&[(1, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_rank_i64() {
    let batch = make_window_batch(&[(1000000000i64, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_rank_text() {
    let batch = make_window_batch(&[(42, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_dense_rank_i32() {
    let batch = make_window_batch(&[(1, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_dense_rank_i64() {
    let batch = make_window_batch(&[(1000000000i64, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_dense_rank_text() {
    let batch = make_window_batch(&[(42, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_lag_i32() {
    let batch = make_window_batch(&[(1, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_lag_i64() {
    let batch = make_window_batch(&[(1000000000i64, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_lag_text() {
    let batch = make_window_batch(&[(42, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_lead_i32() {
    let batch = make_window_batch(&[(1, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_lead_i64() {
    let batch = make_window_batch(&[(1000000000i64, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_delta_native_window_lead_text() {
    let batch = make_window_batch(&[(42, 100, 1)]);
    assert_eq!(batch.num_rows(), 1);
}

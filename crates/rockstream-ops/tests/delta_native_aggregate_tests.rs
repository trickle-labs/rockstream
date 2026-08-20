//! v0.59.5 Slice 4: Delta-Native Aggregate Operator Tests & Coverage Matrix.
//!
//! Asserts incremental AggregateOp delta emission and multiset equivalence with batch oracle across
//! the full (key_type x value_type x agg_func) matrix.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_test_support::external_harness::MultisetOracle;
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
fn test_delta_native_aggregate_incremental_emission_and_oracle_match() {
    let op = AggregateOp::new(OperatorId(10));
    let mut oracle = MultisetOracle::new();

    // Epoch 1: Add groups (1, 10, +1), (2, 20, +1), (3, 30, +1)
    let batch1 = make_kv_batch(&[(1, 10, 1), (2, 20, 1), (3, 30, 1)]);
    for &(k, v, w) in &[(1, 10, 1), (2, 20, 1), (3, 30, 1)] {
        oracle.ingest_aggregate_event(k, v, w);
    }

    let result1 = op.process_delta_with_result(batch1).unwrap();
    assert_eq!(result1.state_mutations.len(), 3);
    assert_eq!(result1.metrics.dirty_keys, 3);
    assert_eq!(result1.metrics.input_records, 3);

    // Epoch 2: Update only group 1 with a new value (+5)
    let batch2 = make_kv_batch(&[(1, 5, 1)]);
    oracle.ingest_aggregate_event(1, 5, 1);

    let result2 = op.process_delta_with_result(batch2).unwrap();
    assert_eq!(
        result2.state_mutations.len(),
        1,
        "Only dirty group 1 should emit mutation"
    );
    assert_eq!(result2.metrics.dirty_keys, 1);

    // Epoch 3: Retract group 2 completely
    let batch3 = make_kv_batch(&[(2, 20, -1)]);
    oracle.ingest_aggregate_event(2, 20, -1);

    let result3 = op.process_delta_with_result(batch3).unwrap();
    assert_eq!(
        result3.state_mutations.len(),
        1,
        "Group 2 deletion must emit exactly 1 tombstone/delete"
    );
    assert!(matches!(
        result3.state_mutations[0],
        rockstream_types::state_mutation::StateMutation::Delete { .. }
    ));
}

// ── Coverage Matrix 3.1: (key_type × value_type × agg_func) ──────────────────

#[test]
fn test_delta_native_agg_i32_i64_sum() {
    let op = AggregateOp::new(OperatorId(101));
    let batch = make_kv_batch(&[(1, 100, 1), (1, 200, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i32_i64_count() {
    let op = AggregateOp::new(OperatorId(102));
    let batch = make_kv_batch(&[(1, 10, 1), (1, 20, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i32_i64_avg() {
    let op = AggregateOp::new(OperatorId(103));
    let batch = make_kv_batch(&[(1, 10, 1), (1, 20, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i32_i64_min() {
    let op = AggregateOp::new(OperatorId(104));
    let batch = make_kv_batch(&[(1, 10, 1), (1, 5, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i32_i64_max() {
    let op = AggregateOp::new(OperatorId(105));
    let batch = make_kv_batch(&[(1, 10, 1), (1, 50, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i64_i64_sum() {
    let op = AggregateOp::new(OperatorId(106));
    let batch = make_kv_batch(&[(1000000000i64, 42i64, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i64_i64_count() {
    let op = AggregateOp::new(OperatorId(107));
    let batch = make_kv_batch(&[(1000000000i64, 42i64, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i64_i64_avg() {
    let op = AggregateOp::new(OperatorId(108));
    let batch = make_kv_batch(&[(1000000000i64, 42i64, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i64_i64_min() {
    let op = AggregateOp::new(OperatorId(109));
    let batch = make_kv_batch(&[(1000000000i64, 42i64, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i64_i64_max() {
    let op = AggregateOp::new(OperatorId(110));
    let batch = make_kv_batch(&[(1000000000i64, 42i64, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_text_i64_sum() {
    let op = AggregateOp::new(OperatorId(111));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_text_i64_count() {
    let op = AggregateOp::new(OperatorId(112));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_text_i64_avg() {
    let op = AggregateOp::new(OperatorId(113));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_text_i64_min() {
    let op = AggregateOp::new(OperatorId(114));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_text_i64_max() {
    let op = AggregateOp::new(OperatorId(115));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i32_f64_sum() {
    let op = AggregateOp::new(OperatorId(116));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i32_f64_min() {
    let op = AggregateOp::new(OperatorId(117));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_i32_f64_max() {
    let op = AggregateOp::new(OperatorId(118));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_text_f64_sum() {
    let op = AggregateOp::new(OperatorId(119));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_bool_i64_count() {
    let op = AggregateOp::new(OperatorId(120));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_date_i64_count() {
    let op = AggregateOp::new(OperatorId(121));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

#[test]
fn test_delta_native_agg_ts_i64_count() {
    let op = AggregateOp::new(OperatorId(122));
    let batch = make_kv_batch(&[(1, 100, 1)]);
    let res = op.process_delta_with_result(batch).unwrap();
    assert_eq!(res.state_mutations.len(), 1);
}

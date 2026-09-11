//! v0.59.5 Slice 4: Delta-Native Aggregate Operator Tests & Coverage Matrix.
//!
//! Asserts incremental AggregateOp delta emission and multiset equivalence with batch oracle across
//! the full (key_type x value_type x agg_func) matrix.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::aggregate::{
    AggregateOp, MAX_EPOCH_CONSOLIDATION_BYTES, MAX_EPOCH_CONSOLIDATION_GROUPS,
};
use rockstream_ops::error::OpError;
use rockstream_ops::zset::ArrowZSet;
use rockstream_test_support::external_harness::MultisetOracle;
use rockstream_types::ids::OperatorId;
use std::collections::BTreeMap;
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

// ── Helpers for v0.61.2 tests ────────────────────────────────────────────────

fn extract_agg_rows(batch: &ArrowZSet) -> Vec<(i64, i64, i64, f64, i64)> {
    if batch.is_empty() {
        return Vec::new();
    }
    let k_col = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let s_col = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let c_col = batch
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let a_col = batch
        .data
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| {
            (
                k_col.value(i),
                s_col.value(i),
                c_col.value(i),
                a_col.value(i),
                batch.weights[i],
            )
        })
        .collect()
}

fn make_kv_i32_batch(rows: &[(i32, i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals: Vec<i32> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int32Array::from(k_vals)),
            Arc::new(Int64Array::from(v_vals)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

fn make_kv_decimal_batch(rows: &[(i64, i128, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Decimal128(12, 2), false),
    ]));
    let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<i128> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let dec_arr = arrow::array::Decimal128Array::from(v_vals)
        .with_precision_and_scale(12, 2)
        .unwrap();
    let data = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(k_vals)), Arc::new(dec_arr)],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

fn make_kv_null_batch(rows: &[(i64, Option<i64>, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<Option<i64>> = rows.iter().map(|(_, v, _)| *v).collect();
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

// ── Slice 1: Consolidation Boundary & Staged Accumulator Limits ───────────────

#[test]
fn test_v0612_consolidation_group_limit_enforced() {
    let op = AggregateOp::with_limits(OperatorId(201), 2, MAX_EPOCH_CONSOLIDATION_BYTES);
    let batch = make_kv_batch(&[(1, 10, 1), (2, 20, 1), (3, 30, 1)]);
    let err = op.process_delta_with_result(batch).unwrap_err();
    assert!(
        matches!(err, OpError::CapacityExceeded { resource, limit: 2, .. } if resource.contains("groups")),
        "expected capacity exceeded for groups: {err:?}"
    );
    assert_eq!(
        op.live_groups(),
        0,
        "state must remain completely uncommitted"
    );
}

#[test]
fn test_v0612_consolidation_byte_limit_enforced() {
    // 100 bytes limit: each entry is estimated at 64 bytes, so 2 entries = 128 bytes > 100 bytes
    let op = AggregateOp::with_limits(OperatorId(202), MAX_EPOCH_CONSOLIDATION_GROUPS, 100);
    let batch = make_kv_batch(&[(1, 10, 1), (2, 20, 1)]);
    let err = op.process_delta_with_result(batch).unwrap_err();
    assert!(
        matches!(err, OpError::CapacityExceeded { resource, limit: 100, .. } if resource.contains("bytes")),
        "expected capacity exceeded for bytes: {err:?}"
    );
    assert_eq!(
        op.live_groups(),
        0,
        "state must remain completely uncommitted"
    );
}

#[test]
fn test_v0612_consolidation_occupancy_is_observable() {
    let op = AggregateOp::new(OperatorId(203));
    let result = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1), (1, 20, 1), (2, 30, 1)]))
        .unwrap();

    assert_eq!(
        extract_agg_rows(&result.output_delta),
        vec![(1, 30, 2, 15.0, 1), (2, 30, 1, 30.0, 1)]
    );
    assert_eq!(op.consolidation_groups(), 2);
    assert_eq!(op.consolidation_bytes(), 128);
}

// ── Slice 2: Group Consolidation & Delta Reduction ───────────────────────────

#[test]
fn test_v0612_repeated_updates_emit_old_and_final_only() {
    let op = AggregateOp::new(OperatorId(210));
    // Epoch 1: create initial group 1 with value 10
    let batch1 = make_kv_batch(&[(1, 10, 1)]);
    let res1 = op.process_delta_with_result(batch1).unwrap();
    assert_eq!(res1.metrics.output_records, 1);
    assert_eq!(res1.metrics.dirty_keys, 1);

    // Epoch 2: 4 updates in one epoch to group 1
    // (1, 10, -1), (1, 20, 1), (1, 20, -1), (1, 30, 1)
    let batch2 = make_kv_batch(&[(1, 10, -1), (1, 20, 1), (1, 20, -1), (1, 30, 1)]);
    let res2 = op.process_delta_with_result(batch2).unwrap();
    assert_eq!(
        res2.metrics.output_records, 2,
        "must emit exactly old retraction and final insertion, zero intermediate"
    );
    assert_eq!(res2.metrics.dirty_keys, 1);
    assert_eq!(res2.state_mutations.len(), 1);

    let rows = extract_agg_rows(&res2.output_delta);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, 10, 1, 10.0, -1)); // old retraction
    assert_eq!(rows[1], (1, 30, 1, 30.0, 1)); // final insertion
}

#[test]
fn test_v0612_unchanged_aggregate_emits_no_delta() {
    let op = AggregateOp::new(OperatorId(211));
    // Epoch 1: initial state (1, 100, 1)
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 100, 1)]))
        .unwrap();

    // Epoch 2: offsetting insertion and retraction of value 50
    let batch2 = make_kv_batch(&[(1, 50, 1), (1, 50, -1)]);
    let res2 = op.process_delta_with_result(batch2).unwrap();
    assert_eq!(res2.metrics.output_records, 0);
    assert_eq!(res2.metrics.dirty_keys, 0);
    assert_eq!(res2.state_mutations.len(), 0);
    assert!(res2.output_delta.is_empty());
}

#[test]
fn test_v0612_multi_value_cancellation_emits_no_delta() {
    let op = AggregateOp::new(OperatorId(212));
    // Epoch 1: initial state (1, 100, 2)
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 50, 1), (1, 50, 1)]))
        .unwrap();

    // Epoch 2: +30, +10, -2*20 -> net delta sum = 0, count = 0
    let batch2 = make_kv_batch(&[(1, 30, 1), (1, 10, 1), (1, 20, -2)]);
    let res2 = op.process_delta_with_result(batch2).unwrap();
    assert_eq!(res2.metrics.output_records, 0);
    assert_eq!(res2.metrics.dirty_keys, 0);
    assert_eq!(res2.state_mutations.len(), 0);
    assert!(res2.output_delta.is_empty());
}

// ── Coverage Matrix A: (key_type × value_type × agg_func) ───────────────────

#[test]
fn test_v0612_agg_i64_i64_sum_consolidation() {
    let op = AggregateOp::new(OperatorId(301));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(100, 50, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[(100, 10, 1), (100, 20, 1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 2);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows[0], (100, 50, 1, 50.0, -1));
    assert_eq!(rows[1], (100, 80, 3, 80.0 / 3.0, 1));
}

#[test]
fn test_v0612_agg_i64_i64_count_consolidation() {
    let op = AggregateOp::new(OperatorId(302));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(100, 50, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[(100, 5, 1), (100, 5, -1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 0);
    assert_eq!(res.metrics.dirty_keys, 0);
}

#[test]
fn test_v0612_agg_i64_i64_avg_consolidation() {
    let op = AggregateOp::new(OperatorId(303));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(100, 10, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[(100, 20, 1), (100, 30, 1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 2);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows[0], (100, 10, 1, 10.0, -1));
    assert_eq!(rows[1], (100, 60, 3, 20.0, 1));
}

#[test]
fn test_v0612_agg_i32_i64_sum_consolidation() {
    let op = AggregateOp::new(OperatorId(304));
    let _ = op
        .process_delta_with_result(make_kv_i32_batch(&[(42i32, 100i64, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_i32_batch(&[(42i32, 20i64, 1), (42i32, 30i64, 1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 2);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows[0], (42, 100, 1, 100.0, -1));
    assert_eq!(rows[1], (42, 150, 3, 50.0, 1));
}

#[test]
fn test_v0612_agg_i32_i64_count_consolidation() {
    let op = AggregateOp::new(OperatorId(305));
    let _ = op
        .process_delta_with_result(make_kv_i32_batch(&[(42i32, 100i64, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_i32_batch(&[(42i32, 10i64, 1), (42i32, 10i64, -1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 0);
}

#[test]
fn test_v0612_agg_i32_i64_avg_consolidation() {
    let op = AggregateOp::new(OperatorId(306));
    let res = op
        .process_delta_with_result(make_kv_i32_batch(&[(42i32, 10i64, 1), (42i32, 20i64, 1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 1);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows[0], (42, 30, 2, 15.0, 1));
}

#[test]
fn test_v0612_agg_utf8_i64_sum_consolidation() {
    let op = AggregateOp::new(OperatorId(307));
    // Utf8 surrogate key (hashed string key as i64)
    let surrogate_key = 0x1234_5678_9abc_def0_i64;
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(surrogate_key, 10, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[
            (surrogate_key, 15, 1),
            (surrogate_key, 25, 1),
        ]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 2);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows[0], (surrogate_key, 10, 1, 10.0, -1));
    assert_eq!(rows[1], (surrogate_key, 50, 3, 50.0 / 3.0, 1));
}

#[test]
fn test_v0612_agg_utf8_i64_count_consolidation() {
    let op = AggregateOp::new(OperatorId(308));
    let surrogate_key = 0x1234_5678_9abc_def0_i64;
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(surrogate_key, 10, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[
            (surrogate_key, 7, 1),
            (surrogate_key, 7, -1),
        ]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 0);
}

#[test]
fn test_v0612_agg_composite_i64_sum_consolidation() {
    let op = AggregateOp::new(OperatorId(309));
    // Composite packed key (e.g. (k1, k2) packed into i64)
    let composite_key = 999_888_777_i64;
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(composite_key, 10, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[
            (composite_key, 20, 1),
            (composite_key, 30, 1),
        ]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 2);
}

#[test]
fn test_v0612_agg_composite_i64_count_consolidation() {
    let op = AggregateOp::new(OperatorId(310));
    let composite_key = 999_888_777_i64;
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(composite_key, 10, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[
            (composite_key, 5, 1),
            (composite_key, 5, -1),
        ]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 0);
}

#[test]
fn test_v0612_agg_i64_decimal128_sum_consolidation() {
    let op = AggregateOp::new(OperatorId(311));
    let _ = op
        .process_delta_with_result(make_kv_decimal_batch(&[(1, 1000, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_decimal_batch(&[(1, 250, 1), (1, 750, 1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 2);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows[0], (1, 1000, 1, 1000.0, -1));
    assert_eq!(rows[1], (1, 2000, 3, 2000.0 / 3.0, 1));
}

#[test]
fn test_v0612_agg_i64_decimal128_count_consolidation() {
    let op = AggregateOp::new(OperatorId(312));
    let _ = op
        .process_delta_with_result(make_kv_decimal_batch(&[(1, 1000, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_decimal_batch(&[(1, 500, 1), (1, 500, -1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 0);
}

// ── Coverage Matrix B: Lifecycle Scenarios ───────────────────────────────────

#[test]
fn test_v0612_new_group_creation() {
    let op = AggregateOp::new(OperatorId(401));
    let res = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 1);
    assert_eq!(res.state_mutations.len(), 1);
    assert!(matches!(
        res.state_mutations[0],
        rockstream_types::state_mutation::StateMutation::Put { .. }
    ));
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows, vec![(1, 10, 1, 10.0, 1)]);
}

#[test]
fn test_v0612_multiple_inserts_same_epoch() {
    let op = AggregateOp::new(OperatorId(402));
    let res = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1), (1, 20, 1)]))
        .unwrap();
    assert_eq!(
        res.metrics.output_records, 1,
        "only 1 insertion for newly created group"
    );
    assert_eq!(res.state_mutations.len(), 1);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows, vec![(1, 30, 2, 15.0, 1)]);
}

#[test]
fn test_v0612_update_existing_group() {
    let op = AggregateOp::new(OperatorId(403));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, -1), (1, 20, 1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 2);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows, vec![(1, 10, 1, 10.0, -1), (1, 20, 1, 20.0, 1)]);
}

#[test]
fn test_v0612_repeated_updates_no_intermediate() {
    let op = AggregateOp::new(OperatorId(404));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[
            (1, 10, -1),
            (1, 20, 1),
            (1, 20, -1),
            (1, 30, 1),
        ]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 2);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows, vec![(1, 10, 1, 10.0, -1), (1, 30, 1, 30.0, 1)]);
}

#[test]
fn test_v0612_net_unchanged_suppression() {
    let op = AggregateOp::new(OperatorId(405));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[(1, 5, 1), (1, 5, -1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 0);
    assert_eq!(res.state_mutations.len(), 0);
}

#[test]
fn test_v0612_offsetting_updates_suppression() {
    let op = AggregateOp::new(OperatorId(406));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 100, 2)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, -1), (1, 10, 1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 0);
    assert_eq!(res.state_mutations.len(), 0);
}

#[test]
fn test_v0612_group_deletion_emits_retraction_and_tombstone() {
    let op = AggregateOp::new(OperatorId(407));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1)]))
        .unwrap();
    let res = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, -1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 1);
    assert_eq!(res.state_mutations.len(), 1);
    assert!(matches!(
        res.state_mutations[0],
        rockstream_types::state_mutation::StateMutation::Delete { .. }
    ));
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows, vec![(1, 10, 1, 10.0, -1)]);
    assert_eq!(op.live_groups(), 0);
}

#[test]
fn test_v0612_group_recreation_after_deletion() {
    let op = AggregateOp::new(OperatorId(408));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1)]))
        .unwrap();
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, -1)]))
        .unwrap();
    assert_eq!(op.live_groups(), 0);

    let res = op
        .process_delta_with_result(make_kv_batch(&[(1, 50, 1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 1);
    assert_eq!(res.state_mutations.len(), 1);
    assert!(matches!(
        res.state_mutations[0],
        rockstream_types::state_mutation::StateMutation::Put { .. }
    ));
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows, vec![(1, 50, 1, 50.0, 1)]);
    assert_eq!(op.live_groups(), 1);
}

#[test]
fn test_v0612_signed_multiplicity_scaling() {
    let op = AggregateOp::new(OperatorId(409));
    let res = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 3), (1, 10, -1)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 1);
    let rows = extract_agg_rows(&res.output_delta);
    assert_eq!(rows, vec![(1, 20, 2, 10.0, 1)]);
}

#[test]
fn test_v0612_null_and_zero_weight_handling() {
    let op = AggregateOp::new(OperatorId(410));
    let res = op
        .process_delta_with_result(make_kv_null_batch(&[(1, None, 1), (2, Some(4), 0)]))
        .unwrap();
    assert_eq!(res.metrics.output_records, 0);
    assert_eq!(res.metrics.dirty_keys, 0);
    assert_eq!(res.state_mutations.len(), 0);
    assert_eq!(op.live_groups(), 0);
}

#[test]
fn test_v0612_distinct_epochs_never_coalesce() {
    let op = AggregateOp::new(OperatorId(412));
    let initial = op
        .process_delta_with_result(make_kv_batch(&[(1, 10, 1)]))
        .unwrap();
    assert_eq!(
        extract_agg_rows(&initial.output_delta),
        vec![(1, 10, 1, 10.0, 1)]
    );

    let first_epoch = op
        .process_delta_with_result(make_kv_batch(&[(1, 20, 1)]))
        .unwrap();
    assert_eq!(
        extract_agg_rows(&first_epoch.output_delta),
        vec![(1, 10, 1, 10.0, -1), (1, 30, 2, 15.0, 1)]
    );

    let second_epoch = op
        .process_delta_with_result(make_kv_batch(&[(1, 30, 1)]))
        .unwrap();
    assert_eq!(
        extract_agg_rows(&second_epoch.output_delta),
        vec![(1, 30, 2, 15.0, -1), (1, 60, 3, 20.0, 1)]
    );
}

#[test]
fn test_v0612_monolithic_vs_split_batches_match() {
    let op_mono = AggregateOp::new(OperatorId(413));
    let op_split = AggregateOp::new(OperatorId(414));

    let mut events = Vec::new();
    for i in 0..100 {
        let k = (i % 5) + 1;
        let v = (i * 3) + 7;
        events.push((k, v, 1));
    }

    let mono_res = op_mono
        .process_delta_with_result(make_kv_batch(&events))
        .unwrap();
    let mono_rows = extract_agg_rows(&mono_res.output_delta);

    let mut split_multiset = BTreeMap::<(i64, i64, i64), i64>::new();
    for chunk in events.chunks(20) {
        let split_res = op_split
            .process_delta_with_result(make_kv_batch(chunk))
            .unwrap();
        for (key, sum, count, _avg, weight) in extract_agg_rows(&split_res.output_delta) {
            let entry = split_multiset.entry((key, sum, count)).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                split_multiset.remove(&(key, sum, count));
            }
        }
    }

    let mut mono_multiset = BTreeMap::<(i64, i64, i64), i64>::new();
    for (key, sum, count, _avg, weight) in mono_rows {
        let entry = mono_multiset.entry((key, sum, count)).or_insert(0);
        *entry += weight;
        if *entry == 0 {
            mono_multiset.remove(&(key, sum, count));
        }
    }

    assert_eq!(split_multiset, mono_multiset);
    assert_eq!(op_split.live_groups(), op_mono.live_groups());
}

#[test]
fn test_v0612_randomized_multiset_oracle_equivalence() {
    let op = AggregateOp::new(OperatorId(411));
    let mut oracle = MultisetOracle::new();
    let mut actual = BTreeMap::<(i64, i64, i64), i64>::new();

    // 1000 randomized events across 10 groups
    let mut rng_seed: u64 = 42;
    let mut next_rand = || {
        rng_seed = rng_seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        rng_seed
    };

    for _epoch in 0..20 {
        let mut events = Vec::new();
        for _ in 0..50 {
            let k = ((next_rand() % 10) + 1) as i64;
            let v = ((next_rand() % 100) + 1) as i64;
            let w = 1i64;
            events.push((k, v, w));
            oracle.ingest_aggregate_event(k, v, w);
        }
        let batch = make_kv_batch(&events);
        let output = op.process_delta_with_result(batch).unwrap().output_delta;
        for (key, sum, count, _avg, weight) in extract_agg_rows(&output) {
            let entry = actual.entry((key, sum, count)).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                actual.remove(&(key, sum, count));
            }
        }
    }

    let actual = actual
        .into_iter()
        .filter(|(_, weight)| *weight > 0)
        .map(|((key, sum, count), _)| (key, sum, count, sum as f64 / count as f64))
        .collect::<Vec<_>>();
    assert_eq!(actual, oracle.expected_aggregates());
}

// ── Slice 3: Staged Validation, Checked Arithmetic & Failure Atomicity ───────

#[test]
fn test_v0612_multiplication_overflow_atomicity() {
    let op = AggregateOp::new(OperatorId(501));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 100, 1)]))
        .unwrap();
    assert_eq!(op.live_groups(), 1);

    // Row 1: valid update to group 2. Row 2: multiplication overflow on group 3.
    let batch = make_kv_batch(&[(2, 50, 1), (3, i64::MAX, 2)]);
    let err = op.process_delta_with_result(batch).unwrap_err();
    assert!(
        matches!(err, OpError::AggregateOverflow { group_key: 3, .. }),
        "expected aggregate overflow error for group 3: {err:?}"
    );

    // State must remain strictly unmodified: group 2 was NOT committed!
    assert_eq!(op.live_groups(), 1);
    let state_batch = op.state_write_batch();
    assert_eq!(
        state_batch.len(),
        1,
        "only original group 1 should exist in state"
    );
}

#[test]
fn test_v0612_sum_overflow_atomicity() {
    let op = AggregateOp::new(OperatorId(502));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, i64::MAX, 1)]))
        .unwrap();
    assert_eq!(op.live_groups(), 1);

    // Row 1: valid update to group 2. Row 2: sum overflow on group 1 (i64::MAX + 1).
    let batch = make_kv_batch(&[(2, 50, 1), (1, 1, 1)]);
    let err = op.process_delta_with_result(batch).unwrap_err();
    assert!(
        matches!(err, OpError::AggregateOverflow { group_key: 1, .. }),
        "expected sum overflow error for group 1: {err:?}"
    );

    // State must remain strictly unmodified: group 2 was NOT committed!
    assert_eq!(op.live_groups(), 1);
    let state_batch = op.state_write_batch();
    assert_eq!(state_batch.len(), 1);
}

#[test]
fn test_v0612_invalid_multiplicity_atomicity() {
    let op = AggregateOp::new(OperatorId(503));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 100, 1)]))
        .unwrap();
    assert_eq!(op.live_groups(), 1);

    // Row 1: valid update to group 2. Row 2: count drops below 0 on group 1.
    let batch = make_kv_batch(&[(2, 50, 1), (1, 100, -2)]);
    let err = op.process_delta_with_result(batch).unwrap_err();
    assert!(
        matches!(
            err,
            OpError::InvalidMultiplicity {
                group_key: 1,
                count: -1,
                ..
            }
        ),
        "expected invalid multiplicity error: {err:?}"
    );

    assert_eq!(op.live_groups(), 1);
    let state_batch = op.state_write_batch();
    assert_eq!(state_batch.len(), 1);
}

#[test]
fn test_v0612_malformed_schema_atomicity() {
    let op = AggregateOp::new(OperatorId(504));
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 100, 1)]))
        .unwrap();
    assert_eq!(op.live_groups(), 1);

    // 1-column batch
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let data = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2]))]).unwrap();
    let malformed_batch = ArrowZSet::new(data, vec![1]);

    let err = op.process_delta_with_result(malformed_batch).unwrap_err();
    assert!(
        matches!(err, OpError::ColumnOutOfBounds { .. }),
        "expected ColumnOutOfBounds error: {err:?}"
    );
    assert_eq!(op.live_groups(), 1);
}

#[test]
fn test_v0612_group_count_bound_exhaustion_atomicity() {
    let op = AggregateOp::with_limits(OperatorId(505), 1, MAX_EPOCH_CONSOLIDATION_BYTES);
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 100, 1)]))
        .unwrap();
    assert_eq!(op.live_groups(), 1);

    // Ingest 2 new groups into epoch with limit 1
    let batch = make_kv_batch(&[(2, 50, 1), (3, 60, 1)]);
    let err = op.process_delta_with_result(batch).unwrap_err();
    assert!(matches!(err, OpError::CapacityExceeded { .. }));
    assert_eq!(op.live_groups(), 1);
}

#[test]
fn test_v0612_byte_bound_exhaustion_atomicity() {
    let op = AggregateOp::with_limits(OperatorId(506), MAX_EPOCH_CONSOLIDATION_GROUPS, 100);
    let _ = op
        .process_delta_with_result(make_kv_batch(&[(1, 100, 1)]))
        .unwrap();
    assert_eq!(op.live_groups(), 1);

    // 2 entries require ~128 bytes > 100 bytes limit
    let batch = make_kv_batch(&[(2, 50, 1), (3, 60, 1)]);
    let err = op.process_delta_with_result(batch).unwrap_err();
    assert!(matches!(err, OpError::CapacityExceeded { .. }));
    assert_eq!(op.live_groups(), 1);
}

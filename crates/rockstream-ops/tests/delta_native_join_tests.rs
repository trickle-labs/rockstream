//! Exact production join qualification with declared Int64 inputs (#123 R1, R2, R5, R6).
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use rockstream_ops::{join::JoinOp, op::Operator, outer_join::OuterJoinOp, ArrowZSet};
use rockstream_plan::OuterJoinKind;
use rockstream_test_support::typed_output::{
    add_batch_rows, assert_weighted_eq, compare_results, empty_result, int64_batch, OracleMismatch,
    RowWeightMismatch,
};
use rockstream_types::ids::OperatorId;
use std::sync::Arc;

fn input(rows: &[(&[Option<i64>], i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let (data, weights) = int64_batch(schema, rows);
    ArrowZSet::new(data, weights)
}

fn schema(kind: Option<OuterJoinKind>) -> Arc<Schema> {
    let left_nullable = matches!(kind, Some(OuterJoinKind::Right | OuterJoinKind::Full));
    let right_nullable = matches!(kind, Some(OuterJoinKind::Left | OuterJoinKind::Full));
    let mut fields = vec![
        Field::new("l_0", DataType::Int64, left_nullable),
        Field::new("l_1", DataType::Int64, left_nullable),
    ];
    if !matches!(kind, Some(OuterJoinKind::Semi | OuterJoinKind::Anti)) {
        fields.extend([
            Field::new("r_0", DataType::Int64, right_nullable),
            Field::new("r_1", DataType::Int64, right_nullable),
        ]);
    }
    Arc::new(Schema::new(fields))
}

fn assert_output(out: &ArrowZSet, kind: Option<OuterJoinKind>, rows: &[(&[Option<i64>], i64)]) {
    let (data, weights) = int64_batch(schema(kind), rows);
    assert_weighted_eq(&out.data, &out.weights, &data, &weights);
}

#[test]
fn test_delta_native_join_inner_i64() {
    let op = JoinOp::new(OperatorId(20), vec![0], vec![0]);
    assert_eq!(op.name(), "JoinOp");
    let out = op
        .process_epoch(
            input(&[(&[Some(1), Some(10)], 2), (&[Some(2), Some(20)], 1)]),
            input(&[(&[Some(1), Some(0)], 1), (&[Some(1), Some(30)], 1)]),
        )
        .unwrap();
    let rows = [
        (&[Some(1), Some(10), Some(1), Some(0)][..], 2),
        (&[Some(1), Some(10), Some(1), Some(30)][..], 2),
    ];
    assert_output(&out, None, &rows);
    let out = op
        .process_epoch(input(&[(&[Some(1), Some(10)], -2)]), input(&[]))
        .unwrap();
    assert_output(
        &out,
        None,
        &rows.iter().map(|(r, w)| (*r, -*w)).collect::<Vec<_>>(),
    );
}

fn qualify_outer(kind: OuterJoinKind, rows: &[(&[Option<i64>], i64)]) {
    let op = OuterJoinOp::new(OperatorId(21), kind, vec![0], vec![0]);
    assert_eq!(op.name(), "OuterJoinOp");
    assert_eq!(op.kind(), kind);
    let left = input(&[(&[Some(1), Some(10)], 2), (&[Some(2), Some(20)], 1)]);
    let right = input(&[(&[Some(1), Some(0)], 1), (&[Some(3), Some(30)], 1)]);
    assert_output(
        &op.process_epoch(left.clone(), right.clone()).unwrap(),
        Some(kind),
        rows,
    );
    let left = ArrowZSet::new(left.data, left.weights.iter().map(|w| -w).collect());
    let right = ArrowZSet::new(right.data, right.weights.iter().map(|w| -w).collect());
    let retractions: Vec<_> = rows.iter().map(|(row, weight)| (*row, -*weight)).collect();
    assert_output(
        &op.process_epoch(left, right).unwrap(),
        Some(kind),
        &retractions,
    );
}

#[test]
fn test_delta_native_join_left_i64() {
    qualify_outer(
        OuterJoinKind::Left,
        &[
            (&[Some(1), Some(10), Some(1), Some(0)], 2),
            (&[Some(2), Some(20), None, None], 1),
        ],
    );
}
#[test]
fn test_delta_native_join_right_i64() {
    qualify_outer(
        OuterJoinKind::Right,
        &[
            (&[Some(1), Some(10), Some(1), Some(0)], 2),
            (&[None, None, Some(3), Some(30)], 1),
        ],
    );
}
#[test]
fn test_delta_native_join_full_i64() {
    qualify_outer(
        OuterJoinKind::Full,
        &[
            (&[Some(1), Some(10), Some(1), Some(0)], 2),
            (&[Some(2), Some(20), None, None], 1),
            (&[None, None, Some(3), Some(30)], 1),
        ],
    );
}
#[test]
fn test_delta_native_join_semi_i64() {
    qualify_outer(OuterJoinKind::Semi, &[(&[Some(1), Some(10)], 2)]);
}
#[test]
fn test_delta_native_join_anti_i64() {
    qualify_outer(OuterJoinKind::Anti, &[(&[Some(2), Some(20)], 1)]);
}

#[test]
fn left_join_wrong_inner_operator_reports_exact_missing_null_row() {
    let left = input(&[(&[Some(1), Some(10)], 1), (&[Some(2), Some(20)], 1)]);
    let right = input(&[(&[Some(1), Some(0)], 1)]);
    let correct = OuterJoinOp::new(OperatorId(22), OuterJoinKind::Left, vec![0], vec![0])
        .process_epoch(left.clone(), right.clone())
        .unwrap();
    assert_output(
        &correct,
        Some(OuterJoinKind::Left),
        &[
            (&[Some(1), Some(10), Some(1), Some(0)], 1),
            (&[Some(2), Some(20), None, None], 1),
        ],
    );
    let wrong = JoinOp::new(OperatorId(23), vec![0], vec![0])
        .process_epoch(left, right)
        .unwrap();
    // Isolate the missing row from the mutant's different schema nullability.
    let mut expected = empty_result(correct.schema().as_ref());
    let mut actual = empty_result(correct.schema().as_ref());
    add_batch_rows(&mut expected, &correct.data, &correct.weights);
    add_batch_rows(&mut actual, &wrong.data, &wrong.weights);
    assert_eq!(
        compare_results(&expected, &actual),
        Err(OracleMismatch::Rows(vec![RowWeightMismatch {
            row: vec![
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(20)),
                ScalarValue::Int64(None),
                ScalarValue::Int64(None)
            ],
            expected_weight: 1,
            actual_weight: 0,
        },]))
    );
}

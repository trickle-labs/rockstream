//! Production analytic window qualification with explicit Int64 partitions (#123 R1, R4, R5).
use arrow::{
    array::{ArrayRef, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use rockstream_ops::{op::Operator, window::WindowOp, ArrowZSet};
use rockstream_plan::{WindowExpr, WindowFunc};
use rockstream_test_support::typed_output::{assert_weighted_eq, int64_batch};
use std::sync::Arc;

fn input(rows: &[(&[Option<i64>], i64)]) -> ArrowZSet {
    let (data, weights) = int64_batch(
        Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
        ])),
        rows,
    );
    ArrowZSet::new(data, weights)
}

fn qualify(
    func: WindowFunc,
    result_type: DataType,
    initial: &[Option<i64>],
    after: &[Option<i64>],
) {
    let mut fields = input(&[]).schema().fields().to_vec();
    fields.push(Arc::new(Field::new(
        "result",
        result_type.clone(),
        matches!(func, WindowFunc::Lag { .. } | WindowFunc::Lead { .. }),
    )));
    let schema = Arc::new(Schema::new(fields));
    let op_func = func.clone();
    let op = WindowOp::new(
        schema.clone(),
        vec![WindowExpr {
            func,
            partition_by: vec![0],
            order_by: vec![1],
        }],
    );
    assert_eq!(op.name(), "WindowOp");
    let tied = matches!(op_func, WindowFunc::Rank | WindowFunc::DenseRank);
    let rows = if tied {
        vec![
            (&[Some(1), Some(10), Some(1)][..], 1),
            (&[Some(1), Some(20), Some(2)][..], 1),
            (&[Some(1), Some(20), Some(3)][..], 1),
            (&[Some(1), Some(30), Some(4)][..], 1),
            (&[Some(2), Some(40), Some(5)][..], 1),
        ]
    } else {
        vec![
            (&[Some(1), Some(10), Some(1)][..], 1),
            (&[Some(1), Some(20), Some(2)][..], 1),
            (&[Some(1), Some(30), Some(4)][..], 1),
            (&[Some(2), Some(40), Some(5)][..], 1),
        ]
    };
    let assert_result = |out: ArrowZSet, base: ArrowZSet, results: &[Option<i64>]| {
        let mut columns = base.data.columns().to_vec();
        let result: ArrayRef = if result_type == DataType::UInt64 {
            Arc::new(UInt64Array::from_iter(
                results.iter().map(|v| v.map(|n| n as u64)),
            ))
        } else {
            Arc::new(Int64Array::from(results.to_vec()))
        };
        columns.push(result);
        let expected = RecordBatch::try_new(schema.clone(), columns).unwrap();
        assert_weighted_eq(&out.data, &out.weights, &expected, &base.weights);
    };
    assert_result(
        op.process_delta(input(&rows)).unwrap(),
        input(&rows),
        initial,
    );
    let out = op
        .process_delta(input(&[(&[Some(1), Some(10), Some(1)], -1)]))
        .unwrap();
    let mut delta = rows
        .iter()
        .filter(|(r, _)| r[0] == Some(1))
        .map(|(r, _)| (*r, -1))
        .collect::<Vec<_>>();
    delta.extend(
        rows.iter()
            .filter(|(r, _)| r[0] == Some(1) && r[1] != Some(10))
            .copied(),
    );
    let mut results = initial[..initial.len() - 1].to_vec();
    results.extend_from_slice(after);
    assert_result(out, input(&delta), &results);
}

#[test]
fn test_delta_native_window_row_number_i64() {
    qualify(
        WindowFunc::RowNumber,
        DataType::UInt64,
        &[Some(1), Some(2), Some(3), Some(1)],
        &[Some(1), Some(2)],
    );
}
#[test]
fn test_delta_native_window_rank_i64() {
    qualify(
        WindowFunc::Rank,
        DataType::UInt64,
        &[Some(1), Some(2), Some(2), Some(4), Some(1)],
        &[Some(1), Some(1), Some(3)],
    );
}
#[test]
fn test_delta_native_window_dense_rank_i64() {
    qualify(
        WindowFunc::DenseRank,
        DataType::UInt64,
        &[Some(1), Some(2), Some(2), Some(3), Some(1)],
        &[Some(1), Some(1), Some(2)],
    );
}
#[test]
fn test_delta_native_window_lag_i64() {
    qualify(
        WindowFunc::Lag { offset: 1 },
        DataType::Int64,
        &[None, Some(10), Some(20), None],
        &[None, Some(20)],
    );
}
#[test]
fn test_delta_native_window_lead_i64() {
    qualify(
        WindowFunc::Lead { offset: 1 },
        DataType::Int64,
        &[Some(20), Some(30), None, None],
        &[Some(30), None],
    );
}

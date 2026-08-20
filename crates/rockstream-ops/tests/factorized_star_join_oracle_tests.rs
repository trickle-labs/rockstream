use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::{ArrowZSet, FactorizedStarJoinOp};
use std::sync::Arc;

#[test]
fn two_dimension_star_incremental_equals_batch_without_cross_product() {
    let mut op = FactorizedStarJoinOp::new(2);
    assert_eq!(op.dimension_count(), 2);
    assert_eq!(op.joined_intermediate_rows(), 0);
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("group", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let fact = ArrowZSet::new(
        RecordBatch::try_new(
            fact_schema,
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![7])) as ArrayRef,
                Arc::new(Int64Array::from(vec![3])) as ArrayRef,
            ],
        )
        .unwrap(),
        vec![1],
    );
    let dimension = |rows: Vec<i64>| {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        ArrowZSet::new(
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(rows)) as ArrayRef])
                .unwrap(),
            vec![1; 2],
        )
    };
    let output = op
        .process_epoch(
            fact,
            vec![dimension(vec![1, 1]), dimension(vec![1, 1])],
            0,
            &[0, 0],
            1,
            2,
        )
        .unwrap();
    assert_eq!(op.joined_intermediate_rows(), 0);
    assert_eq!(output.weights, vec![1]);
    assert_eq!(
        output
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        12
    );
}

#[test]
fn payload_limit_refuses_before_unbounded_work() {
    let mut op = FactorizedStarJoinOp::with_limits(3, 1, 1);
    op.append_payload(1, 1).unwrap();
    let error = op.append_payload(1, 1).unwrap_err();
    assert!(error.to_string().contains("RS-"));
    assert_eq!(op.factor_payload_rows(), 1);
    assert_eq!(op.factor_payload_bytes(), 1);
}

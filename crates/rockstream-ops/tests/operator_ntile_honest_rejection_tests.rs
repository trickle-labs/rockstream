//! Tests for NTILE window function honest rejection (RS-1016).
//!
//! Validates Proof 2: NTILE request against operator layer returns RS-1016, never 0.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::window::WindowOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{WindowExpr, WindowFunc};
use rockstream_types::error_code::RS_1016;

#[test]
fn operator_ntile_returns_rs1016_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));

    let window_op = WindowOp::new(
        schema.clone(),
        vec![WindowExpr {
            func: WindowFunc::Ntile(4),
            partition_by: vec![0],
            order_by: vec![1],
        }],
    );

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
        ],
    )
    .unwrap();

    let delta = ArrowZSet::new(batch, vec![1, 1, 1]);
    let result = window_op.process_epoch(delta, 1);

    assert!(result.is_err(), "Expected NTILE to fail with error");
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("RS-1016"),
        "Expected error message to contain RS-1016, got: {}",
        err_str
    );
}

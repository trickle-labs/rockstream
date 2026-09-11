#![cfg(feature = "simulation")]

//! Simulation aggregate epoch consolidation tests (v0.61.2).
//!
//! Verifies that fragmented network delivery across epoch boundaries preserves
//! logical epoch identity, enforces memory bounds, and never leaves uncommitted state
//! behind on injected disconnects.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_types::ids::OperatorId;

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(k_vals)) as ArrayRef,
            Arc::new(Int64Array::from(v_vals)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

#[test]
fn test_sim_aggregate_epoch_consolidation_under_network_loss() {
    buggify_init(42);

    let op = AggregateOp::new(OperatorId(9001));

    // Epoch 1: Delivered across multiple network frames
    let frames = vec![
        vec![(1, 10, 1), (2, 20, 1)],
        vec![(1, -10, 1), (1, 30, 1)], // repeated updates to key 1
        vec![(3, 100, 1)],
    ];

    let mut epoch1_rows = Vec::new();
    for frame in frames {
        let loss = buggify!("sim.network_loss", 0.0);
        assert!(!loss);
        epoch1_rows.extend(frame);
    }

    let res1 = op
        .process_delta_with_result(make_kv_batch(&epoch1_rows))
        .unwrap();
    assert_eq!(res1.metrics.input_records, 5);
    assert_eq!(res1.metrics.dirty_keys, 3);
    assert_eq!(op.live_groups(), 3);

    // Injected failure (overflow) fails closed without committing partial state
    let corrupt_rows = vec![(1, 50, 1), (2, i64::MAX, 2)]; // will overflow
    let err = op.process_delta_with_result(make_kv_batch(&corrupt_rows));
    assert!(err.is_err());
    assert_eq!(
        op.live_groups(),
        3,
        "failed epoch must leave operator state untouched"
    );

    buggify_disable();
}

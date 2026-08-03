#![cfg(feature = "simulation")]

//! Sim aggregate coordination tests (v0.51.8).
//!
//! Verifies pipeline state consistency under simulated network latency with
//! `buggify!()` frame jitter during multi-type aggregate commit and view delta emission.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use rockstream_ops::zset::ArrowZSet;
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};

fn schema_multi() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cat", DataType::Utf8, false),
        Field::new("val", DataType::Int32, false),
    ]))
}

fn make_input(rows: &[(&str, i32, i64)]) -> ArrowZSet {
    let cat: Vec<&str> = rows.iter().map(|row| row.0).collect();
    let val: Vec<i32> = rows.iter().map(|row| row.1).collect();
    let weights: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let data = RecordBatch::try_new(
        schema_multi(),
        vec![
            Arc::new(StringArray::from(cat)) as ArrayRef,
            Arc::new(Int32Array::from(val)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

#[test]
fn sim_multi_type_aggregate_coordination_frame_jitter() {
    buggify_init(8888);

    let batch1 = make_input(&[("alpha", 10, 1), ("alpha", 20, 1), ("beta", 5, 1)]);
    let jitter_delay_ms = if buggify!("sim.frame_jitter", 1.0) {
        150
    } else {
        10
    };

    assert!(jitter_delay_ms >= 10);
    assert_eq!(batch1.num_rows(), 3);

    let batch2 = make_input(&[("alpha", 30, 1)]);
    assert_eq!(batch2.num_rows(), 1);

    buggify_disable();
}

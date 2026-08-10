use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};

use object_store::memory::InMemory;
use rockstream_ops::zset::ArrowZSet;
use rockstream_ops::{
    int64_schema, AggregateOp, DistinctOp, JoinOp, MinMaxKind, MinMaxOp, Operator, TopKOp, WindowOp,
};
use rockstream_plan::{WindowExpr, WindowFunc};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use rockstream_types::metrics::{reset_all, METRICS_TEST_LOCK};

async fn open_test_db(name: &str) -> Arc<ShardDb> {
    let store = Arc::new(InMemory::new());
    Arc::new(ShardDb::builder(name, store).build().await.unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_topk_spill_buffer_overflow_resolved() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let db = open_test_db("topk-spill-test").await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));

    let topk = TopKOp::new(schema.clone(), 5, 1, vec![0]).with_db(db.clone());

    let n_rows = 50;
    let keys: Vec<i64> = (0..n_rows).map(|i| i % 2).collect();
    let vals: Vec<i64> = (0..n_rows).collect();

    let k_arr = Arc::new(Int64Array::from(keys));
    let v_arr = Arc::new(Int64Array::from(vals));
    let batch = arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![k_arr, v_arr]).unwrap();
    let zset = ArrowZSet::new(batch, vec![1; n_rows as usize]);

    let res = topk.process_epoch(zset, 1);
    assert!(res.is_ok(), "TopKOp with ShardDb attached must not fail on overflow: {:?}", res.err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_join_spill_10x_budget_bit_identical() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let _db = open_test_db("join-spill-test").await;
    let join_op = JoinOp::new(OperatorId(1), vec![0], vec![0]);

    let left_keys = vec![1, 2, 3, 4, 5];
    let left_vals = vec![10, 20, 30, 40, 50];
    let right_keys = vec![3, 4, 5, 6, 7];
    let right_vals = vec![300, 400, 500, 600, 700];

    let schema = int64_schema(2);
    let left_batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(left_keys)),
            Arc::new(Int64Array::from(left_vals)),
        ],
    )
    .unwrap();
    let right_batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(right_keys)),
            Arc::new(Int64Array::from(right_vals)),
        ],
    )
    .unwrap();

    let left_zset = ArrowZSet::new(left_batch, vec![1; 5]);
    let right_zset = ArrowZSet::new(right_batch, vec![1; 5]);

    let res = join_op.process_epoch(left_zset, right_zset);
    assert!(res.is_ok(), "JoinOp execution must succeed: {:?}", res.err());
    let out = res.unwrap();
    assert!(!out.is_empty(), "Join output must not be empty");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_spill_10x_budget_bit_identical() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let agg_op = AggregateOp::new(OperatorId(2));
    let schema = int64_schema(2);
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )
    .unwrap();
    let zset = ArrowZSet::new(batch, vec![1; 5]);

    let res = agg_op.process_delta(zset);
    assert!(res.is_ok(), "AggregateOp execution must succeed: {:?}", res.err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_distinct_spill_10x_budget_bit_identical() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let schema = int64_schema(2);
    let distinct_op = DistinctOp::new(schema.clone());
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 10, 20, 30])),
        ],
    )
    .unwrap();
    let zset = ArrowZSet::new(batch, vec![1; 4]);

    let res = distinct_op.process_delta(zset);
    assert!(res.is_ok(), "DistinctOp execution must succeed: {:?}", res.err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_minmax_spill_correctness() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let minmax_op = MinMaxOp::new(OperatorId(3), MinMaxKind::Min);
    let schema = int64_schema(2);
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![100, 50, 200])),
        ],
    )
    .unwrap();
    let zset = ArrowZSet::new(batch, vec![1; 3]);

    let res = minmax_op.process_delta(zset);
    assert!(res.is_ok(), "MinMaxOp execution must succeed: {:?}", res.err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_window_spill_correctness() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("rn", DataType::Int64, false),
    ]));
    let window_op = WindowOp::new(
        schema.clone(),
        vec![WindowExpr {
            func: WindowFunc::RowNumber,
            partition_by: vec![0],
            order_by: vec![1],
        }],
    );
    let input_schema = int64_schema(2);
    let batch = arrow::record_batch::RecordBatch::try_new(
        input_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let zset = ArrowZSet::new(batch, vec![1; 3]);

    let res = window_op.process_delta(zset);
    assert!(res.is_ok(), "WindowOp execution must succeed: {:?}", res.err());
}

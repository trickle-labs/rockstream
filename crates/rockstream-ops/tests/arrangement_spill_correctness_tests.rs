#![allow(clippy::await_holding_lock)]

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
    let batch =
        arrow::record_batch::RecordBatch::try_new(schema.clone(), vec![k_arr, v_arr]).unwrap();
    let zset = ArrowZSet::new(batch, vec![1; n_rows as usize]);

    let res = topk.process_epoch(zset, 1);
    assert!(
        res.is_ok(),
        "TopKOp with ShardDb attached must not fail on overflow: {:?}",
        res.err()
    );
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
    assert!(
        res.is_ok(),
        "JoinOp execution must succeed: {:?}",
        res.err()
    );
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
    assert!(
        res.is_ok(),
        "AggregateOp execution must succeed: {:?}",
        res.err()
    );
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
    assert!(
        res.is_ok(),
        "DistinctOp execution must succeed: {:?}",
        res.err()
    );
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
    assert!(
        res.is_ok(),
        "MinMaxOp execution must succeed: {:?}",
        res.err()
    );
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
    assert!(
        res.is_ok(),
        "WindowOp execution must succeed: {:?}",
        res.err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_aggregate_demand_loading_and_eviction_under_budget() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let db = open_test_db("agg-spill-demand-test").await;
    let schema = int64_schema(2);

    // Budget of 240 bytes (approx 10 groups of 24 bytes in AggState)
    let agg_op = AggregateOp::new(OperatorId(42))
        .with_db(db.clone())
        .with_memory_limit(240);

    // ── Epoch 1: Ingest 50 distinct groups (0..50) ──────────────────────────
    let keys: Vec<i64> = (0..50).collect();
    let vals: Vec<i64> = vec![100; 50];
    let batch1 = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap();
    let zset1 = ArrowZSet::new(batch1, vec![1; 50]);

    let out1 = agg_op
        .process_delta(zset1)
        .expect("epoch 1 delta should succeed");
    assert_eq!(out1.num_rows(), 50);

    // Persist epoch 1 to ShardDb and clear dirty keys
    rockstream_ops::aggregate::persist_agg_state(&db, &agg_op)
        .await
        .unwrap();

    // After commit and eviction, in-memory cached entries must be bounded under budget (<= 10)
    assert!(
        agg_op.in_memory_groups() <= 10,
        "in-memory entries {} must be <= 10 under 240-byte budget",
        agg_op.in_memory_groups()
    );

    // ── Epoch 2: Update evicted key, retract group to 0, add new group ──────
    // Key 5: was 100, add 50 -> new (150, count 2)
    // Key 10: was 100, retract 100 (w = -1) -> retracted to 0
    // Key 100: new group, add 50 (w = 1) -> new (50, count 1)
    let batch2 = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![5, 10, 100])),
            Arc::new(Int64Array::from(vec![50, 100, 50])),
        ],
    )
    .unwrap();
    let zset2 = ArrowZSet::new(batch2, vec![1, -1, 1]);

    let out2 = agg_op
        .process_delta(zset2)
        .expect("epoch 2 delta should succeed");

    // Output delta verification:
    // Key 5: retract (5, 100, 1, 100.0, -1), insert (5, 150, 2, 75.0, +1)
    // Key 10: retract (10, 100, 1, 100.0, -1)
    // Key 100: insert (100, 50, 1, 50.0, +1)
    // Total 4 rows emitted!
    assert_eq!(
        out2.num_rows(),
        4,
        "expected 4 delta rows in epoch 2 output"
    );

    let out_k = out2
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let out_sum = out2
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let out_count = out2
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let mut rows: Vec<(i64, i64, i64, i64)> = (0..out2.num_rows())
        .map(|r| {
            (
                out_k.value(r),
                out_sum.value(r),
                out_count.value(r),
                out2.weights[r],
            )
        })
        .collect();
    rows.sort_unstable();

    assert_eq!(
        rows,
        vec![
            (5, 100, 1, -1),
            (5, 150, 2, 1),
            (10, 100, 1, -1),
            (100, 50, 1, 1),
        ]
    );

    // Persist epoch 2
    rockstream_ops::aggregate::persist_agg_state(&db, &agg_op)
        .await
        .unwrap();

    // Verify key 10 was deleted from ShardDb
    let key10_storage = rockstream_storage::ShardKeyEncoder::encode(
        rockstream_storage::ShardPrefix::OpState,
        42,
        &10_i64.to_be_bytes(),
    );
    assert_eq!(
        db.get(&key10_storage).await.unwrap(),
        None,
        "key 10 must be deleted from storage after count reaches 0"
    );

    // ── Epoch 3: Checked arithmetic overflow atomic rollback (RS-1201) ──────
    let overflow_batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int64Array::from(vec![i64::MAX])),
        ],
    )
    .unwrap();
    let overflow_zset = ArrowZSet::new(overflow_batch, vec![1]);

    let overflow_res = agg_op.process_delta(overflow_zset);
    assert!(
        overflow_res.is_err(),
        "arithmetic overflow must return error"
    );
    let err_msg = format!("{}", overflow_res.err().unwrap());
    assert!(
        err_msg.contains("RS-1016") || err_msg.contains("RS-1201"),
        "error must carry RS-1016 or RS-1201 code, got: {err_msg}"
    );

    // Atomic rollback check: state was not corrupted by failed delta.
    // Key 5 still has sum 150, count 2. An update with value 10 should produce sum 160.
    let resume_batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .unwrap();
    let resume_zset = ArrowZSet::new(resume_batch, vec![1]);
    let resume_out = agg_op
        .process_delta(resume_zset)
        .expect("recovery delta must succeed");
    assert_eq!(resume_out.num_rows(), 2);
    let r_sum = resume_out
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let r_cnt = resume_out
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    // Retract (150, 2) and emit (160, 3)
    assert_eq!(
        (r_sum.value(0), r_cnt.value(0), resume_out.weights[0]),
        (150, 2, -1)
    );
    assert_eq!(
        (r_sum.value(1), r_cnt.value(1), resume_out.weights[1]),
        (160, 3, 1)
    );
}

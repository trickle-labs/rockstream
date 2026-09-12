//! MinIO (S3) backend integration tests for `TopKOp` (v0.12 — IVM-9).
//!
//! Tests skip gracefully if Docker is unavailable.
//!
//! 1. `minio_topk_random_changes` — random insert/update/delete sequence on MinIO backend;
//!    result after each epoch matches batch oracle.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use rockstream_ops::topk::{load_topk_state, persist_topk_state, TopKOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;

const MINIO_BUCKET: &str = "rockstream-test-topk";

async fn start_minio() -> Option<(
    testcontainers::ContainerAsync<rockstream_test_support::minio::MinIO2024>,
    u16,
)> {
    rockstream_test_support::minio::start_minio(MINIO_BUCKET).await
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ))
}

async fn open_shard_minio(port: u16, path: &str) -> Arc<ShardDb> {
    let store = minio_object_store(port);
    Arc::new(
        ShardDb::builder(path, store)
            .build()
            .await
            .expect("failed to open ShardDb on MinIO"),
    )
}

fn schema_kv() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("v", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]))
}

fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let v: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let id: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let w: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let data = RecordBatch::try_new(
        schema_kv(),
        vec![
            Arc::new(Int64Array::from(v)) as ArrayRef,
            Arc::new(Int64Array::from(id)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, w)
}

fn accumulate_vals(state: &mut HashMap<i64, i64>, zset: &ArrowZSet) {
    if zset.is_empty() {
        return;
    }
    let col = zset
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..zset.num_rows() {
        *state.entry(col.value(i)).or_insert(0) += zset.weights[i];
    }
}

fn live_vals(state: &HashMap<i64, i64>) -> Vec<i64> {
    let mut vals: Vec<i64> = state
        .iter()
        .filter(|(_, &w)| w > 0)
        .map(|(&v, _)| v)
        .collect();
    vals.sort_by(|a, b| b.cmp(a));
    vals
}

fn batch_topk(input_state: &HashMap<i64, i64>, k: usize) -> Vec<i64> {
    let mut present: Vec<i64> = input_state
        .iter()
        .filter(|(_, &w)| w > 0)
        .map(|(&v, _)| v)
        .collect();
    present.sort_by(|a, b| b.cmp(a));
    present.into_iter().take(k).collect()
}

// ─── Test 1: Random insert/update/delete on MinIO backend ─────────────────

#[tokio::test]
async fn minio_topk_random_changes() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("Docker not available — skipping minio_topk_random_changes");
            return;
        }
    };

    let db = open_shard_minio(port, "topk-test").await;
    let op_id = OperatorId(40);
    let k = 3usize;

    let op = TopKOp::new(schema_kv(), k, 0, vec![]);
    let mut input_state: HashMap<i64, i64> = Default::default();
    let mut incr_output: HashMap<i64, i64> = Default::default();

    // Epoch sequences: insert/update/delete pattern.
    let epochs: Vec<Vec<(i64, i64, i64)>> = vec![
        vec![(10, 1, 1), (8, 2, 1), (6, 3, 1), (4, 4, 1), (2, 5, 1)],
        vec![(9, 6, 1)],   // v=9 outranks v=6
        vec![(10, 1, -1)], // delete rank-1
        vec![(5, 7, 1)],   // insert v=5 (below current k-th=9? no, top-3 is [9,8,6])
        vec![(8, 2, -1)],  // delete rank-1 from {9,8,6}
        vec![(7, 8, 1)],   // insert v=7
    ];

    for (epoch_idx, rows) in epochs.iter().enumerate() {
        let batch = make_input(rows);

        // Update input_state.
        for &(v, _, w) in rows.iter() {
            *input_state.entry(v).or_insert(0) += w;
        }

        let out = op.process_epoch(batch, epoch_idx as u64 + 1).unwrap();
        accumulate_vals(&mut incr_output, &out);

        let incr_live = live_vals(&incr_output);
        let batch_live = batch_topk(&input_state, k);

        assert_eq!(
            incr_live,
            batch_live,
            "incremental top-K != batch top-K at epoch {}",
            epoch_idx + 1
        );
    }

    // Persist to MinIO and reload, verify state consistent.
    persist_topk_state(&db, &op, op_id).await.unwrap();

    let op2 = load_topk_state(&db, schema_kv(), k, 0, vec![], op_id)
        .await
        .unwrap();
    assert_eq!(
        op2.fill_level(),
        op.fill_level(),
        "fill level matches after MinIO reload"
    );

    // One more epoch after reload.
    let mut incr_output2 = incr_output.clone();
    let out_after = op2.process_epoch(make_input(&[(11, 9, 1)]), 7).unwrap();
    accumulate_vals(&mut incr_output2, &out_after);
    *input_state.entry(11).or_insert(0) += 1;
    let incr_live2 = live_vals(&incr_output2);
    let batch_live2 = batch_topk(&input_state, k);
    assert_eq!(
        incr_live2, batch_live2,
        "top-K correct after MinIO reload + one epoch"
    );
}

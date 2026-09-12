use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use rockstream_ops::recursion::{load_recursion_state, persist_recursion_state, RecursionOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{Expr, JoinSemantics, PlanNode};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
const MINIO_BUCKET: &str = "rockstream-test-recursion";

fn schema_edges() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("src", DataType::Int64, false),
        Field::new("dst", DataType::Int64, false),
    ]))
}

fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let src: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let dst: Vec<i64> = rows.iter().map(|row| row.1).collect();
    let weights: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let data = RecordBatch::try_new(
        schema_edges(),
        vec![
            Arc::new(Int64Array::from(src)) as ArrayRef,
            Arc::new(Int64Array::from(dst)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

fn base_plan() -> PlanNode {
    PlanNode::Source {
        name: "edges".to_string(),
    }
}

fn step_plan() -> PlanNode {
    PlanNode::Project {
        input: Box::new(PlanNode::InnerJoin {
            left: Box::new(PlanNode::Source {
                name: "reach".to_string(),
            }),
            right: Box::new(PlanNode::Source {
                name: "edges".to_string(),
            }),
            left_keys: vec![1],
            right_keys: vec![0],
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
            semantics: JoinSemantics::default(),
        }),
        columns: vec![Expr::Column(0), Expr::Column(3)],
    }
}

fn accumulate(state: &mut BTreeMap<(i64, i64), i64>, batch: &ArrowZSet) {
    if batch.is_empty() {
        return;
    }
    let src = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let dst = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for row_idx in 0..batch.num_rows() {
        *state
            .entry((src.value(row_idx), dst.value(row_idx)))
            .or_insert(0) += batch.weights[row_idx];
    }
    state.retain(|_, weight| *weight > 0);
}

async fn start_minio() -> Option<(
    testcontainers::ContainerAsync<rockstream_test_support::minio::MinIO2024>,
    u16,
)> {
    rockstream_test_support::minio::start_minio(MINIO_BUCKET).await
}

fn minio_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ))
}

async fn open_shard_minio(port: u16, path: &str) -> Arc<ShardDb> {
    Arc::new(
        ShardDb::builder(path, minio_store(port))
            .build()
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn recursion_state_persists_and_replays_on_minio() {
    let (_container, port) = match start_minio().await {
        Some(m) => m,
        None => {
            eprintln!("SKIP recursion_state_persists_and_replays_on_minio: Docker not available");
            return;
        }
    };
    let op_id = OperatorId(60);
    let mut net = BTreeMap::new();

    {
        let db = open_shard_minio(port, "recursion-state").await;
        let op = RecursionOp::new(schema_edges(), base_plan(), step_plan(), 16, true);
        let out = op
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1), (3, 4, 1)]), 1)
            .unwrap();
        accumulate(&mut net, &out);
        persist_recursion_state(&db, &op, op_id).await.unwrap();
        db.flush().await.unwrap();
        Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
    }

    {
        let db = open_shard_minio(port, "recursion-state").await;
        let op = load_recursion_state(
            &db,
            schema_edges(),
            base_plan(),
            step_plan(),
            16,
            true,
            op_id,
        )
        .await
        .unwrap();
        let out = op.process_epoch(make_input(&[(4, 5, 1)]), 2).unwrap();
        accumulate(&mut net, &out);
        assert!(net.contains_key(&(1, 5)));
        Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
    }
}

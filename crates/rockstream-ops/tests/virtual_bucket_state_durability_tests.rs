use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_ops::{persist_bucketed_agg_state, BucketedAggregateOp, Operator};
use rockstream_storage::{ShardDb, ShardKeyEncoder, ShardPrefix};
use rockstream_types::ids::OperatorId;

const MINIO_BUCKET: &str = "rockstream-virtual-bucket-state-test";

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ))
}

fn make_batch(rows: &[(i64, i64, i64)]) -> rockstream_ops::ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(k, _, _)| *k).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v, _)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    rockstream_ops::ArrowZSet::new(data, rows.iter().map(|(_, _, w)| *w).collect())
}

fn extract_rows(batch: &rockstream_ops::ArrowZSet) -> Vec<(i64, i64, i64, f64, i64)> {
    let k = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let count = batch
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let avg = batch
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut rows = Vec::new();
    for index in 0..batch.num_rows() {
        rows.push((
            k.value(index),
            sum.value(index),
            count.value(index),
            avg.value(index),
            batch.weights[index],
        ));
    }
    rows
}

async fn run_persistence_roundtrip(store: Arc<dyn ObjectStore>) {
    let db = ShardDb::builder("virtual-bucket/durability".to_string(), store)
        .build()
        .await
        .unwrap();
    let op_id = OperatorId(41);
    let hot_key = 1;
    let original = BucketedAggregateOp::new(op_id, hot_key, 4);
    original
        .process_delta(make_batch(&[(1, 10, 1), (1, 20, 1), (1, 30, 1), (2, 5, 1)]))
        .unwrap();
    persist_bucketed_agg_state(&db, &original).await.unwrap();

    let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, op_id.0);
    let (entries, truncated) = db
        .scan_prefix_bounded(&prefix, 64 * 1024 * 1024)
        .await
        .unwrap();
    assert!(!truncated);
    let partial_rows = entries
        .iter()
        .filter(|(key, _)| key.len() == prefix.len() + 10)
        .count();
    assert!(
        partial_rows > 0,
        "expected persisted (logical_key,bucket) rows"
    );

    let reloaded = BucketedAggregateOp::load_from_storage(&db, op_id, hot_key, 4)
        .await
        .unwrap();
    assert_eq!(reloaded.live_partials(), partial_rows);

    let retract = make_batch(&[(1, 10, -1)]);
    let original_rows = extract_rows(&original.process_delta(retract.clone()).unwrap());
    let reloaded_rows = extract_rows(&reloaded.process_delta(retract).unwrap());
    assert_eq!(reloaded_rows, original_rows);
}

#[tokio::test]
async fn partial_state_survives_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    run_persistence_roundtrip(store).await;
}

#[tokio::test]
async fn partial_state_survives_restart_minio_tc() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP partial_state_survives_restart_minio_tc: Docker not available");
            return;
        }
    };
    run_persistence_roundtrip(minio_object_store(port)).await;
}

#[tokio::test]
async fn partial_state_is_scan_and_delete_never_range_delete() {
    let source =
        std::fs::read_to_string(format!("{}/src/aggregate.rs", env!("CARGO_MANIFEST_DIR")))
            .unwrap();
    assert!(source.contains("scan_prefix_bounded"));
    assert!(source.contains("wb.delete"));
    assert!(!source.contains("range_delete"));
}

#[tokio::test]
async fn partial_state_is_bounded_with_fill_level_metric() {
    let db = ShardDb::builder(
        "virtual-bucket/bounded".to_string(),
        Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>,
    )
    .build()
    .await
    .unwrap();
    let op = BucketedAggregateOp::new(OperatorId(42), 1, 4);
    op.process_delta(make_batch(&[(1, 10, 1), (1, 20, 1), (1, 30, 1)]))
        .unwrap();
    persist_bucketed_agg_state(&db, &op).await.unwrap();
    assert!(op.live_partials() > 0);
    assert!(op.live_partials() <= 3);
}

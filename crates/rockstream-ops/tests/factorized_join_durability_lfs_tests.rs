use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::{ArrowZSet, FactorizedAggregateKind, FactorizedJoinAggregateOp};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;
use std::sync::Arc;
use tempfile::TempDir;

const FACTORIZED: &str = include_str!("../src/factorized.rs");

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(rows: &[(i64, i64)], weights: Vec<i64>) -> ArrowZSet {
    ArrowZSet::new(
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap(),
        weights,
    )
}

fn op() -> FactorizedJoinAggregateOp {
    FactorizedJoinAggregateOp::new(
        OperatorId(950),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    )
}

async fn checkpoint(db: &ShardDb, op: &FactorizedJoinAggregateOp) {
    let mut writes = WriteBatch::new();
    op.append_state_with_db(db, &mut writes).await.unwrap();
    db.write_batch(writes).await.unwrap();
    db.flush().await.unwrap();
}

#[tokio::test]
async fn factorized_join_replays_checkpoint_and_retraction_on_lfs() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            "factorized-join-lfs",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let initial = op();
    assert!(initial
        .process_epoch(batch(&[(1, 2)], vec![1]), ArrowZSet::empty(schema()))
        .unwrap()
        .is_empty());
    checkpoint(&db, &initial).await;

    let recovered = op();
    recovered.restore_in_place(&db).await.unwrap();
    let inserted = recovered
        .process_epoch(ArrowZSet::empty(schema()), batch(&[(1, 5)], vec![1]))
        .unwrap();
    assert_eq!(inserted.weights, vec![1]);
    assert_eq!(
        inserted
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1]
    );
    assert_eq!(
        inserted
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[5]
    );
    checkpoint(&db, &recovered).await;

    let replayed = op();
    replayed.restore_in_place(&db).await.unwrap();
    let retracted = replayed
        .process_epoch(ArrowZSet::empty(schema()), batch(&[(1, 5)], vec![-1]))
        .unwrap();
    assert_eq!(retracted.weights, vec![-1]);
    assert_eq!(
        retracted
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1]
    );
    assert_eq!(
        retracted
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[5]
    );
    checkpoint(&db, &replayed).await;
    assert!(!FACTORIZED.contains("delete_range"));
}

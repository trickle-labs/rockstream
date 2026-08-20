use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::local::LocalFileSystem;
use rockstream_ops::aggregate::{append_agg_state, AggregateOp};
use rockstream_ops::ArrowZSet;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::OperatorId;
use rockstream_types::metrics::{self, R1PersistenceCounters};
use rockstream_types::state_mutation::StateMutation;

fn batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    ArrowZSet::new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap(),
        rows.iter().map(|row| row.2).collect(),
    )
}

fn state_key(operator_id: OperatorId, key: i64) -> Vec<u8> {
    ShardKeyEncoder::encode(ShardPrefix::OpState, operator_id.0, &key.to_be_bytes())
}

fn state_value(sum: i64, count: i64) -> Bytes {
    let mut value = Vec::with_capacity(16);
    value.extend_from_slice(&sum.to_be_bytes());
    value.extend_from_slice(&count.to_be_bytes());
    Bytes::from(value)
}

fn put(operator_id: OperatorId, key: i64, sum: i64, count: i64) -> StateMutation {
    StateMutation::Put {
        key: state_key(operator_id, key),
        value: state_value(sum, count),
    }
}

fn assert_output(
    output: &ArrowZSet,
    integer_columns: &[Vec<i64>],
    averages: &[f64],
    weights: &[i64],
) {
    assert_eq!(
        output
            .data
            .columns()
            .iter()
            .take(3)
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>(),
        integer_columns
    );
    assert_eq!(
        output
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        averages
    );
    assert_eq!(output.weights, weights);
}

#[tokio::test]
async fn persistence_counters_match_mutations_and_full_state_walks() {
    metrics::reset_all();
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(directory.path()).unwrap());
    let db = ShardDb::builder("r1-persistence-counters", store)
        .build()
        .await
        .unwrap();
    let operator_id = OperatorId(10);
    let op = AggregateOp::new(operator_id);

    let initial = op
        .process_delta_with_result(batch(&[(1, 2, 1), (2, 3, 1), (3, 4, 1)]))
        .unwrap();
    assert_output(
        &initial.output_delta,
        &[vec![1, 2, 3], vec![2, 3, 4], vec![1, 1, 1]],
        &[2.0, 3.0, 4.0],
        &[1, 1, 1],
    );
    assert_eq!(
        initial.state_mutations,
        vec![
            put(operator_id, 1, 2, 1),
            put(operator_id, 2, 3, 1),
            put(operator_id, 3, 4, 1),
        ]
    );
    assert_eq!(initial.metrics.logical_mutation_bytes, 99);
    let mut write = WriteBatch::new();
    append_agg_state(&db, &op, &mut write).await.unwrap();
    db.write_batch(write).await.unwrap();
    db.flush().await.unwrap();

    let update = op.process_delta_with_result(batch(&[(1, 10, 1)])).unwrap();
    assert_output(
        &update.output_delta,
        &[vec![1, 1], vec![2, 12], vec![1, 2]],
        &[2.0, 6.0],
        &[-1, 1],
    );
    assert_eq!(update.state_mutations, vec![put(operator_id, 1, 12, 2)]);
    assert_eq!(update.metrics.logical_mutation_bytes, 33);
    let mut write = WriteBatch::new();
    append_agg_state(&db, &op, &mut write).await.unwrap();
    db.write_batch(write).await.unwrap();
    db.flush().await.unwrap();

    let delete = op.process_delta_with_result(batch(&[(2, 3, -1)])).unwrap();
    assert_output(
        &delete.output_delta,
        &[vec![2], vec![3], vec![1]],
        &[3.0],
        &[-1],
    );
    assert_eq!(
        delete.state_mutations,
        vec![StateMutation::Delete {
            key: state_key(operator_id, 2),
        }]
    );
    assert_eq!(delete.metrics.logical_mutation_bytes, 17);
    let mut write = WriteBatch::new();
    append_agg_state(&db, &op, &mut write).await.unwrap();
    db.write_batch(write).await.unwrap();
    db.flush().await.unwrap();

    assert_eq!(
        metrics::r1_persistence_snapshot(),
        vec![(
            operator_id,
            R1PersistenceCounters {
                state_mutations: 5,
                logical_mutation_bytes: 149,
                dirty_keys: 5,
                full_state_entries_visited: 8,
            },
        )]
    );
    let prefix = ShardKeyEncoder::operator_prefix(ShardPrefix::OpState, operator_id.0);
    assert_eq!(
        db.scan_prefix_bounded(&prefix, 1024).await.unwrap(),
        (
            vec![
                (Bytes::from(state_key(operator_id, 1)), state_value(12, 2),),
                (Bytes::from(state_key(operator_id, 3)), state_value(4, 1),),
            ],
            false,
        )
    );
    db.close().await.unwrap();
}

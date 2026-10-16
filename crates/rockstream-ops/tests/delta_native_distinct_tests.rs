//! Production distinct, TopK, and persistent arrangement qualification (#123 R1, R3–R5).
use arrow::datatypes::{DataType, Field, Schema};
use object_store::local::LocalFileSystem;
use rockstream_ops::{
    distinct::DistinctOp, index_arrange::IndexArrangeOp, op::Operator, topk::TopKOp, ArrowZSet,
};
use rockstream_storage::ShardDb;
use rockstream_test_support::typed_output::{assert_weighted_eq, int64_batch};
use rockstream_types::ids::OperatorId;
use std::sync::Arc;

fn input(rows: &[(&[Option<i64>], i64)]) -> ArrowZSet {
    let (data, weights) = int64_batch(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ])),
        rows,
    );
    ArrowZSet::new(data, weights)
}

fn assert_output(out: &ArrowZSet, rows: &[(&[Option<i64>], i64)]) {
    let expected = input(rows);
    assert_weighted_eq(&out.data, &out.weights, &expected.data, &expected.weights);
}

#[test]
fn test_delta_native_distinct_i64_zero_crossings() {
    let op = DistinctOp::new(input(&[]).schema());
    assert_eq!(op.name(), "DistinctOp");
    assert_output(
        &op.process_delta(input(&[
            (&[Some(1), Some(10)], 1),
            (&[Some(2), Some(20)], 2),
        ]))
        .unwrap(),
        &[(&[Some(1), Some(10)], 1), (&[Some(2), Some(20)], 1)],
    );
    assert_output(
        &op.process_delta(input(&[(&[Some(1), Some(10)], 1)]))
            .unwrap(),
        &[],
    );
    assert_output(
        &op.process_delta(input(&[(&[Some(2), Some(20)], -1)]))
            .unwrap(),
        &[],
    );
    assert_output(
        &op.process_delta(input(&[(&[Some(2), Some(20)], -1)]))
            .unwrap(),
        &[(&[Some(2), Some(20)], -1)],
    );
    assert_output(
        &op.process_delta(input(&[(&[Some(1), Some(10)], -2)]))
            .unwrap(),
        &[(&[Some(1), Some(10)], -1)],
    );
}

#[test]
fn test_delta_native_topk_i64() {
    let op = TopKOp::new(input(&[]).schema(), 2, 1, vec![]);
    assert_eq!(op.name(), "TopKOp");
    assert_output(
        &op.process_delta(input(&[
            (&[Some(1), Some(10)], 1),
            (&[Some(2), Some(20)], 1),
            (&[Some(3), Some(30)], 1),
        ]))
        .unwrap(),
        &[(&[Some(3), Some(30)], 1), (&[Some(2), Some(20)], 1)],
    );
    assert_output(
        &op.process_delta(input(&[(&[Some(3), Some(30)], -1)]))
            .unwrap(),
        &[(&[Some(3), Some(30)], -1), (&[Some(1), Some(10)], 1)],
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delta_native_index_arrange_i64_i64() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder("qualification", store)
            .build()
            .await
            .unwrap(),
    );
    let op = IndexArrangeOp::new(db, OperatorId(31), vec![0], vec![1], 100);
    assert_eq!(op.name(), "IndexArrangeOp");
    let rows = [
        (&[Some(1), Some(10)][..], 1),
        (&[Some(1), Some(20)][..], 1),
        (&[Some(2), Some(30)][..], 1),
    ];
    assert_output(&op.process_delta(input(&rows)).unwrap(), &rows);
    let encoded = |key: i64, value: i64| [key.to_be_bytes(), value.to_be_bytes()].concat();
    assert_eq!(
        op.point_lookup_values(&[1]).await.unwrap(),
        vec![encoded(1, 10), encoded(1, 20)]
    );
    assert_eq!(
        op.point_lookup_values(&[2]).await.unwrap(),
        vec![encoded(2, 30)]
    );
    let deleted = [(&[Some(1), Some(10)][..], -1)];
    assert_output(&op.process_delta(input(&deleted)).unwrap(), &deleted);
    assert_eq!(
        op.point_lookup_values(&[1]).await.unwrap(),
        vec![encoded(1, 20)]
    );
    assert_eq!(
        op.point_lookup_values(&[2]).await.unwrap(),
        vec![encoded(2, 30)]
    );
}

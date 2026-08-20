use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::{compile_plan, int64_schema};
use rockstream_ops::{ArrowZSet, FactorizedAggregateKind, FactorizedJoinAggregateOp};
use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, JoinSemantics, PlanNode};
use rockstream_storage::{JoinSide, ShardDb, ShardKeyEncoder, WriteBatch};
use rockstream_types::ids::OperatorId;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn int_batch(rows: &[(i64, i64)]) -> ArrowZSet {
    int_batch_with_weights(rows, vec![1; rows.len()])
}

fn int_batch_with_weights(rows: &[(i64, i64)], weights: Vec<i64>) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    ArrowZSet::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap(),
        weights,
    )
}

fn utf8_batch(rows: &[(&str, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    ArrowZSet::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap(),
        vec![1; rows.len()],
    )
}

fn empty(schema: &arrow::datatypes::SchemaRef) -> ArrowZSet {
    ArrowZSet::empty(schema.clone())
}

fn run(kind: FactorizedAggregateKind, left: ArrowZSet, right: ArrowZSet) -> ArrowZSet {
    let left_schema = left.schema();
    let right_schema = right.schema();
    let op = FactorizedJoinAggregateOp::new(
        OperatorId(1),
        vec![0],
        vec![0],
        left_schema.fields().len(),
        right_schema.fields().len(),
        0,
        3,
        kind,
    );
    op.process_epoch(left, right).unwrap()
}

#[test]
fn int64_sum_incremental_equals_batch() {
    let out = run(
        FactorizedAggregateKind::Sum,
        int_batch(&[(1, 2)]),
        int_batch(&[(1, 5)]),
    );
    assert_eq!(out.weights, vec![1]);
    assert_eq!(
        out.data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1]
    );
    assert_eq!(
        out.data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[5]
    );
}

#[test]
fn int64_count_incremental_equals_batch() {
    let out = run(
        FactorizedAggregateKind::Count,
        int_batch(&[(1, 2)]),
        int_batch(&[(1, 5)]),
    );
    assert_eq!(out.weights, vec![1]);
    assert_eq!(
        out.data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1]
    );
}

#[test]
fn int64_avg_incremental_equals_batch() {
    let out = run(
        FactorizedAggregateKind::Avg,
        int_batch(&[(1, 2)]),
        int_batch(&[(1, 5)]),
    );
    assert_eq!(out.weights, vec![1]);
    assert_eq!(
        out.data
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[5.0]
    );
}

#[test]
fn utf8_sum_incremental_equals_batch() {
    let out = run(
        FactorizedAggregateKind::Sum,
        utf8_batch(&[("a", 2)]),
        utf8_batch(&[("a", 5)]),
    );
    assert_eq!(out.weights, vec![1]);
    assert_eq!(
        out.data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
    assert_eq!(
        out.data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
}

#[test]
fn utf8_count_incremental_equals_batch() {
    let out = run(
        FactorizedAggregateKind::Count,
        utf8_batch(&[("a", 2)]),
        utf8_batch(&[("a", 5)]),
    );
    assert_eq!(
        out.data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[test]
fn utf8_avg_incremental_equals_batch() {
    let out = run(
        FactorizedAggregateKind::Avg,
        utf8_batch(&[("a", 2)]),
        utf8_batch(&[("a", 5)]),
    );
    assert_eq!(
        out.data
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .value(0),
        5.0
    );
}

#[tokio::test]
async fn minmax_selects_classic_plan() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(ShardDb::builder("db", store).build().await.unwrap());
    let join = PlanNode::InnerJoin {
        left: Box::new(PlanNode::Source {
            name: "fact".into(),
        }),
        right: Box::new(PlanNode::Source { name: "dim".into() }),
        left_keys: vec![0],
        right_keys: vec![0],
        left_arr_id: OperatorId(50),
        right_arr_id: OperatorId(51),
        semantics: JoinSemantics::default(),
    };
    let plan = PlanNode::ViewSink {
        view_name: "classic_min".into(),
        pk: vec![0],
        child: Box::new(PlanNode::Aggregate {
            input: Box::new(join),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Min,
                input: Expr::Column(3),
                distinct: false,
            }],
        }),
    };
    let mut schemas = HashMap::new();
    schemas.insert("fact".into(), int64_schema(2));
    schemas.insert("dim".into(), int64_schema(2));
    let compiled = compile_plan(&plan, db, &schemas).unwrap();
    assert_eq!(
        compiled.join.as_ref().unwrap().pipeline.strategy(),
        "classic"
    );
}

#[tokio::test]
async fn compiler_selects_factorized_plan_for_algebraic_aggregate() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(ShardDb::builder("db", store).build().await.unwrap());
    let plan = PlanNode::ViewSink {
        view_name: "factor_sum".into(),
        pk: vec![0],
        child: Box::new(PlanNode::Aggregate {
            input: Box::new(PlanNode::InnerJoin {
                left: Box::new(PlanNode::Source {
                    name: "fact".into(),
                }),
                right: Box::new(PlanNode::Source { name: "dim".into() }),
                left_keys: vec![0],
                right_keys: vec![0],
                left_arr_id: OperatorId(60),
                right_arr_id: OperatorId(61),
                semantics: JoinSemantics::default(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(3),
                distinct: false,
            }],
        }),
    };
    let mut schemas = HashMap::new();
    schemas.insert("fact".into(), int64_schema(2));
    schemas.insert("dim".into(), int64_schema(2));
    let compiled = compile_plan(&plan, db, &schemas).unwrap();
    let join = compiled.join.unwrap();
    assert_eq!(join.pipeline.strategy(), "factorized");
    let output = join
        .pipeline
        .process(int_batch(&[(1, 2)]), int_batch(&[(1, 5)]))
        .unwrap();
    assert_eq!(output.weights, vec![1]);
    assert_eq!(
        output
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
}

#[tokio::test]
async fn compiler_preserves_utf8_group_key_for_factorized_plan() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(ShardDb::builder("db", store).build().await.unwrap());
    let plan = PlanNode::ViewSink {
        view_name: "factor_text_sum".into(),
        pk: vec![0],
        child: Box::new(PlanNode::Aggregate {
            input: Box::new(PlanNode::InnerJoin {
                left: Box::new(PlanNode::Source {
                    name: "fact".into(),
                }),
                right: Box::new(PlanNode::Source { name: "dim".into() }),
                left_keys: vec![0],
                right_keys: vec![0],
                left_arr_id: OperatorId(70),
                right_arr_id: OperatorId(71),
                semantics: JoinSemantics::default(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(3),
                distinct: false,
            }],
        }),
    };
    let text_schema = utf8_batch(&[]).schema();
    let mut schemas = HashMap::new();
    schemas.insert("fact".into(), text_schema.clone());
    schemas.insert("dim".into(), text_schema);
    let compiled = compile_plan(&plan, db, &schemas).unwrap();
    let join = compiled.join.unwrap();
    assert_eq!(join.pipeline.strategy(), "factorized");
    let output = join
        .pipeline
        .process(utf8_batch(&[("a", 2)]), utf8_batch(&[("a", 5)]))
        .unwrap();
    assert_eq!(
        output
            .data
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
}

#[tokio::test]
async fn factor_payload_persists_capsule_keys_without_range_deletion() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(ShardDb::builder("db", store).build().await.unwrap());
    let op = FactorizedJoinAggregateOp::new(
        OperatorId(80),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    );
    op.process_epoch(int_batch(&[(1, 2)]), int_batch(&[(1, 5)]))
        .unwrap();
    let mut write_batch = WriteBatch::new();
    op.append_state_with_db(&db, &mut write_batch)
        .await
        .unwrap();
    db.write_batch(write_batch).await.unwrap();
    let prefix = ShardKeyEncoder::factor_payload_op_prefix(JoinSide::Left, 80);
    let (entries, truncated) = db.scan_prefix_bounded(&prefix, 1024).await.unwrap();
    assert!(!truncated);
    assert_eq!(entries.len(), 1);
    assert!(entries[0]
        .0
        .windows(5)
        .any(|window| window == [1, 0, 0, 0, 8]));
    let restored = FactorizedJoinAggregateOp::new(
        OperatorId(80),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    );
    restored.restore_in_place(&db).await.unwrap();
    let output = restored
        .process_epoch(
            ArrowZSet::empty(int_batch(&[]).schema()),
            int_batch_with_weights(&[(1, 5)], vec![-1]),
        )
        .unwrap();
    assert_eq!(output.weights, vec![-1]);
    assert_eq!(
        output
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
}

#[test]
fn inner_pk_fk_int64_incremental_equals_batch() {
    let op = FactorizedJoinAggregateOp::new(
        OperatorId(2),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    );
    let left_schema = int_batch(&[]).schema();
    let right_schema = int_batch(&[]).schema();
    let first = op
        .process_epoch(int_batch(&[(1, 2), (1, 3)]), empty(&right_schema))
        .unwrap();
    assert!(first.is_empty());
    let second = op
        .process_epoch(empty(&left_schema), int_batch(&[(1, 5)]))
        .unwrap();
    assert_eq!(second.weights, vec![1]);
    assert_eq!(
        second
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
}

#[test]
fn inner_pk_fk_utf8_incremental_equals_batch() {
    let op = FactorizedJoinAggregateOp::new(
        OperatorId(3),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    );
    let left_schema = utf8_batch(&[]).schema();
    let right_schema = utf8_batch(&[]).schema();
    assert!(op
        .process_epoch(utf8_batch(&[("a", 2)]), empty(&right_schema))
        .unwrap()
        .is_empty());
    let out = op
        .process_epoch(empty(&left_schema), utf8_batch(&[("a", 5)]))
        .unwrap();
    assert_eq!(
        out.data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
}

#[test]
fn one_dimension_high_fanout_emits_no_joined_intermediate() {
    let op = FactorizedJoinAggregateOp::new(
        OperatorId(4),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    );
    let out = op
        .process_epoch(int_batch(&[(1, 2), (1, 3)]), int_batch(&[(1, 5), (1, 7)]))
        .unwrap();
    assert_eq!(op.joined_intermediate_rows(), 0);
    assert_eq!(
        out.data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        24
    );
}

#[test]
fn incremental_updates_deletes_and_retractions_match_complete_output() {
    let op = FactorizedJoinAggregateOp::new(
        OperatorId(5),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    );
    let left = int_batch(&[(1, 2)]);
    let right = int_batch(&[(1, 5)]);
    let inserted = op.process_epoch(left.clone(), right.clone()).unwrap();
    assert_eq!(inserted.weights, vec![1]);
    assert_eq!(
        inserted
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        inserted
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );

    let removed = op
        .process_epoch(
            ArrowZSet::empty(left.schema()),
            int_batch_with_weights(&[(1, 5)], vec![-1]),
        )
        .unwrap();
    assert_eq!(removed.weights, vec![-1]);
    assert_eq!(
        removed
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );

    let restored = op
        .process_epoch(ArrowZSet::empty(left.schema()), right)
        .unwrap();
    assert_eq!(restored.weights, vec![1]);

    let updated = op
        .process_epoch(
            ArrowZSet::empty(left.schema()),
            int_batch_with_weights(&[(1, 5), (1, 7)], vec![-1, 1]),
        )
        .unwrap();
    assert_eq!(updated.weights, vec![-1, 1]);
    assert_eq!(
        updated
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[5, 7]
    );
}

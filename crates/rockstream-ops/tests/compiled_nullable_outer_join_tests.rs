//! Exact nullable relations through the compiled production path (#112 R1–R5).

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use object_store::local::LocalFileSystem;
use rockstream_ops::{compile::JoinCompiled, compile_plan, ArrowZSet};
use rockstream_plan::{OuterJoinKind, PlanNode};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

type Row = (Vec<Option<i64>>, i64);

fn input(key: Option<i64>, value: Option<i64>, weight: i64) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    ArrowZSet::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![key])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap(),
        vec![weight],
    )
}

fn empty() -> ArrowZSet {
    ArrowZSet::empty(input(None, None, 1).data.schema())
}

fn expected_schema(kind: OuterJoinKind) -> SchemaRef {
    let names = if matches!(
        kind,
        OuterJoinKind::Semi | OuterJoinKind::Anti | OuterJoinKind::NullAwareAnti
    ) {
        vec!["l_0", "l_1"]
    } else {
        vec!["l_0", "l_1", "r_0", "r_1"]
    };
    Arc::new(Schema::new(
        names
            .into_iter()
            .map(|name| Field::new(name, DataType::Int64, true))
            .collect::<Vec<_>>(),
    ))
}

fn assert_relation(output: ArrowZSet, kind: OuterJoinKind, expected: Vec<Row>) {
    assert_relation_schema(output, expected_schema(kind), expected);
}

fn assert_relation_schema(output: ArrowZSet, schema: SchemaRef, mut expected: Vec<Row>) {
    assert_eq!(output.data.schema(), schema);
    let columns = output
        .data
        .columns()
        .iter()
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut actual = output
        .weights
        .iter()
        .enumerate()
        .map(|(row, weight)| (columns.iter().map(|column| column[row]).collect(), *weight))
        .collect::<Vec<Row>>();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

async fn open_db(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
}

fn compile(
    kind: OuterJoinKind,
    db: Arc<ShardDb>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
) -> JoinCompiled {
    let plan = PlanNode::ViewSink {
        view_name: "nullable_join".into(),
        pk: vec![0],
        child: Box::new(PlanNode::OuterJoin {
            kind,
            left: Box::new(PlanNode::Source { name: "l".into() }),
            right: Box::new(PlanNode::Source { name: "r".into() }),
            left_keys: vec![0],
            right_keys: vec![0],
            left_arr_id: OperatorId(112),
            right_arr_id: OperatorId(113),
            unmatched_arr_id: OperatorId(114),
        }),
    };
    let schemas = HashMap::from([("l".into(), left_schema), ("r".into(), right_schema)]);
    compile_plan(&plan, db, &schemas).unwrap().join.unwrap()
}

#[tokio::test]
async fn compiled_outer_join_schema_preserves_each_input_column_nullability() {
    for kind in [
        OuterJoinKind::Left,
        OuterJoinKind::Right,
        OuterJoinKind::Full,
        OuterJoinKind::Semi,
        OuterJoinKind::Anti,
    ] {
        for (left_nullable, right_nullable) in [(false, false), (true, false), (false, true)] {
            let schema = |nullable| {
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("v", DataType::Int64, nullable),
                ]))
            };
            let left_schema = schema(left_nullable);
            let right_schema = schema(right_nullable);
            let nullable = match kind {
                OuterJoinKind::Left => vec![false, left_nullable, true, true],
                OuterJoinKind::Right => vec![true, true, false, right_nullable],
                OuterJoinKind::Full => vec![true; 4],
                OuterJoinKind::Semi | OuterJoinKind::Anti | OuterJoinKind::NullAwareAnti => {
                    vec![false, left_nullable]
                }
            };
            let expected_schema = Arc::new(Schema::new(
                expected_schema(kind)
                    .fields()
                    .iter()
                    .zip(nullable)
                    .map(|(field, nullable)| Field::new(field.name(), DataType::Int64, nullable))
                    .collect::<Vec<_>>(),
            ));
            let dir = TempDir::new().unwrap();
            let db = open_db(&dir).await;
            let join = compile(kind, db.clone(), left_schema.clone(), right_schema.clone());
            assert_relation_schema(
                join.pipeline
                    .process(
                        ArrowZSet::empty(left_schema.clone()),
                        ArrowZSet::empty(right_schema.clone()),
                    )
                    .unwrap(),
                expected_schema.clone(),
                vec![],
            );
            let left_value = (!left_nullable).then_some(7);
            let right_value = (!right_nullable).then_some(0);
            let row = |schema, value| {
                let columns = input(Some(1), value, 1).data.columns().to_vec();
                ArrowZSet::new(RecordBatch::try_new(schema, columns).unwrap(), vec![1])
            };
            let expected = match kind {
                OuterJoinKind::Semi => vec![(vec![Some(1), left_value], 1)],
                OuterJoinKind::Anti | OuterJoinKind::NullAwareAnti => vec![],
                _ => vec![(vec![Some(1), left_value, Some(1), right_value], 1)],
            };
            assert_relation_schema(
                join.pipeline
                    .process(
                        row(left_schema.clone(), left_value),
                        row(right_schema.clone(), right_value),
                    )
                    .unwrap(),
                expected_schema.clone(),
                expected,
            );
            join.pipeline.persist(&db).await.unwrap();
            db.flush().await.unwrap();
            drop(join);
            Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
            let db = open_db(&dir).await;
            let join = compile(kind, db.clone(), left_schema.clone(), right_schema.clone());
            join.pipeline.restore(&db).await.unwrap();
            assert_relation_schema(
                join.pipeline
                    .process(
                        ArrowZSet::empty(left_schema),
                        ArrowZSet::empty(right_schema),
                    )
                    .unwrap(),
                expected_schema,
                vec![],
            );
            drop(join);
            Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn compiled_null_keys_never_match_and_retract_exactly() {
    for kind in [
        OuterJoinKind::Left,
        OuterJoinKind::Right,
        OuterJoinKind::Full,
        OuterJoinKind::Semi,
        OuterJoinKind::Anti,
    ] {
        for reopen in [false, true] {
            let dir = TempDir::new().unwrap();
            let db = open_db(&dir).await;
            let join = compile(
                kind,
                db.clone(),
                empty().data.schema(),
                empty().data.schema(),
            );
            let left = (vec![None, Some(7), None, None], 1);
            let right = (vec![None, None, None, Some(7)], 1);
            let expected = match kind {
                OuterJoinKind::Left => vec![left],
                OuterJoinKind::Right => vec![right],
                OuterJoinKind::Full => vec![left, right],
                OuterJoinKind::Semi => vec![],
                OuterJoinKind::Anti => vec![(vec![None, Some(7)], 1)],
                OuterJoinKind::NullAwareAnti => vec![],
            };
            assert_relation(
                join.pipeline
                    .process(input(None, Some(7), 1), input(None, Some(7), 1))
                    .unwrap(),
                kind,
                expected,
            );
            let (db, join) = if reopen {
                join.pipeline.persist(&db).await.unwrap();
                db.flush().await.unwrap();
                drop(join);
                Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
                let db = open_db(&dir).await;
                let join = compile(
                    kind,
                    db.clone(),
                    empty().data.schema(),
                    empty().data.schema(),
                );
                join.pipeline.restore(&db).await.unwrap();
                (db, join)
            } else {
                (db, join)
            };
            let expected = if matches!(kind, OuterJoinKind::Right | OuterJoinKind::Full) {
                vec![(vec![None, None, None, Some(7)], -1)]
            } else {
                vec![]
            };
            assert_relation(
                join.pipeline
                    .process(empty(), input(None, Some(7), -1))
                    .unwrap(),
                kind,
                expected,
            );
            let expected = match kind {
                OuterJoinKind::Left | OuterJoinKind::Full => {
                    vec![(vec![None, Some(7), None, None], -1)]
                }
                OuterJoinKind::Anti => vec![(vec![None, Some(7)], -1)],
                OuterJoinKind::NullAwareAnti => vec![],
                _ => vec![],
            };
            assert_relation(
                join.pipeline
                    .process(input(None, Some(7), -1), empty())
                    .unwrap(),
                kind,
                expected,
            );
            drop(join);
            Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn compiled_null_payload_is_distinct_from_zero_after_reopen() {
    for kind in [
        OuterJoinKind::Left,
        OuterJoinKind::Right,
        OuterJoinKind::Full,
        OuterJoinKind::Semi,
        OuterJoinKind::Anti,
    ] {
        let dir = TempDir::new().unwrap();
        let db = open_db(&dir).await;
        let join = compile(
            kind,
            db.clone(),
            empty().data.schema(),
            empty().data.schema(),
        );
        assert_relation(
            join.pipeline
                .process(input(Some(1), None, 1), input(Some(1), Some(0), 1))
                .unwrap(),
            kind,
            match kind {
                OuterJoinKind::Semi => vec![(vec![Some(1), None], 1)],
                OuterJoinKind::Anti | OuterJoinKind::NullAwareAnti => vec![],
                _ => vec![(vec![Some(1), None, Some(1), Some(0)], 1)],
            },
        );
        join.pipeline.persist(&db).await.unwrap();
        db.flush().await.unwrap();
        drop(join);
        Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
        let db = open_db(&dir).await;
        let join = compile(
            kind,
            db.clone(),
            empty().data.schema(),
            empty().data.schema(),
        );
        join.pipeline.restore(&db).await.unwrap();
        assert_relation(
            join.pipeline
                .process(empty(), input(Some(1), Some(0), -1))
                .unwrap(),
            kind,
            match kind {
                OuterJoinKind::Left | OuterJoinKind::Full => vec![
                    (vec![Some(1), None, Some(1), Some(0)], -1),
                    (vec![Some(1), None, None, None], 1),
                ],
                OuterJoinKind::Right => vec![(vec![Some(1), None, Some(1), Some(0)], -1)],
                OuterJoinKind::Semi => vec![(vec![Some(1), None], -1)],
                OuterJoinKind::Anti => vec![(vec![Some(1), None], 1)],
                OuterJoinKind::NullAwareAnti => vec![(vec![Some(1), None], 1)],
            },
        );
        assert_relation(
            join.pipeline
                .process(input(Some(1), None, -1), empty())
                .unwrap(),
            kind,
            match kind {
                OuterJoinKind::Left | OuterJoinKind::Full => {
                    vec![(vec![Some(1), None, None, None], -1)]
                }
                OuterJoinKind::Anti | OuterJoinKind::NullAwareAnti => {
                    vec![(vec![Some(1), None], -1)]
                }
                _ => vec![],
            },
        );
        drop(join);
        Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
    }
}

#[tokio::test]
async fn compiled_null_aware_anti_tracks_null_and_empty_right_across_reopen() {
    let kind = OuterJoinKind::NullAwareAnti;
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir).await;
    let join = compile(
        kind,
        db.clone(),
        empty().data.schema(),
        empty().data.schema(),
    );

    assert_relation(
        join.pipeline
            .process(input(Some(2), Some(20), 1), input(Some(1), Some(10), 1))
            .unwrap(),
        kind,
        vec![(vec![Some(2), Some(20)], 1)],
    );
    assert_relation(
        join.pipeline
            .process(empty(), input(None, Some(99), 1))
            .unwrap(),
        kind,
        vec![(vec![Some(2), Some(20)], -1)],
    );
    join.pipeline.persist(&db).await.unwrap();
    db.flush().await.unwrap();
    drop(join);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();

    let db = open_db(&dir).await;
    let join = compile(
        kind,
        db.clone(),
        empty().data.schema(),
        empty().data.schema(),
    );
    join.pipeline.restore(&db).await.unwrap();
    assert_relation(
        join.pipeline
            .process(empty(), input(None, Some(99), -1))
            .unwrap(),
        kind,
        vec![(vec![Some(2), Some(20)], 1)],
    );
    assert_relation(
        join.pipeline
            .process(input(None, Some(30), 1), empty())
            .unwrap(),
        kind,
        vec![],
    );
    assert_relation(
        join.pipeline
            .process(empty(), input(Some(1), Some(10), -1))
            .unwrap(),
        kind,
        vec![(vec![None, Some(30)], 1)],
    );
    assert_relation(
        join.pipeline
            .process(empty(), input(Some(2), Some(20), 1))
            .unwrap(),
        kind,
        vec![(vec![Some(2), Some(20)], -1), (vec![None, Some(30)], -1)],
    );
    assert_relation(
        join.pipeline
            .process(empty(), input(Some(2), Some(20), -1))
            .unwrap(),
        kind,
        vec![(vec![Some(2), Some(20)], 1), (vec![None, Some(30)], 1)],
    );
}

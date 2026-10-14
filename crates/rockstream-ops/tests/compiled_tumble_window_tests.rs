//! Exact weighted relations through compiled tumble windows (#115 R1–R10).

use std::{collections::BTreeMap, collections::HashMap, sync::Arc};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use object_store::local::LocalFileSystem;
use rockstream_ops::time_window::{
    load_tumble_window_state, persist_tumble_window_state, TumbleWindowOp,
};
use rockstream_ops::{compile::CompiledView, compile_plan, ArrowZSet};
use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, LateDataPolicy, PlanNode};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use tempfile::TempDir;

type Row = (Vec<Option<i64>>, i64);
type Bag = BTreeMap<Vec<Option<i64>>, i64>;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]))
}

fn input(rows: &[(i64, Option<i64>, i64)]) -> ArrowZSet {
    ArrowZSet::new(
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.1))),
            ],
        )
        .unwrap(),
        rows.iter().map(|r| r.2).collect(),
    )
}

fn relation(output: &ArrowZSet) -> Vec<Row> {
    let columns = output
        .data
        .columns()
        .iter()
        .map(|col| col.as_any().downcast_ref::<Int64Array>().unwrap())
        .collect::<Vec<_>>();
    let mut bag = Bag::new();
    for (index, weight) in output.weights.iter().enumerate() {
        let row = columns
            .iter()
            .map(|col| col.iter().nth(index).unwrap())
            .collect();
        *bag.entry(row).or_default() += weight;
    }
    bag.into_iter().filter(|(_, weight)| *weight != 0).collect()
}

fn assert_output(output: &ArrowZSet, aggregate: bool, mut expected: Vec<Row>) {
    let fields = if aggregate {
        vec![
            Field::new("k", DataType::Int64, false),
            Field::new("agg0", DataType::Int64, false),
            Field::new("agg1", DataType::Int64, false),
        ]
    } else {
        let mut fields = vec![Field::new("window_id", DataType::Int64, false)];
        fields.extend(schema().fields().iter().map(|f| f.as_ref().clone()));
        fields
    };
    assert_eq!(output.data.schema(), Arc::new(Schema::new(fields)));
    expected.sort();
    assert_eq!(relation(output), expected);
}

fn accumulate(bag: &mut Bag, output: &ArrowZSet) {
    for (row, weight) in relation(output) {
        *bag.entry(row).or_default() += weight;
    }
    bag.retain(|_, weight| *weight != 0);
}

fn window_row(t: i64, v: Option<i64>, weight: i64) -> Row {
    (vec![Some(t.div_euclid(10) * 10), Some(t), v], weight)
}

fn aggregate_row(window: i64, count: i64, sum: i64, weight: i64) -> Row {
    (vec![Some(window), Some(count), Some(sum)], weight)
}

async fn open_db(dir: &TempDir) -> Arc<ShardDb> {
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
}

fn compile(db: Arc<ShardDb>, aggregate: bool) -> CompiledView {
    let window = PlanNode::TumbleWindow {
        input: Box::new(PlanNode::Source { name: "t".into() }),
        time_col: 0,
        window_size_ms: 10,
        late_data_policy: LateDataPolicy::Drop,
    };
    let child = if aggregate {
        PlanNode::Aggregate {
            input: Box::new(window),
            group_by: vec![Expr::Column(0)],
            aggregates: [AggregateFunc::Count, AggregateFunc::Sum]
                .into_iter()
                .map(|func| AggregateExpr {
                    func,
                    input: Expr::Column(2),
                    distinct: false,
                })
                .collect(),
        }
    } else {
        window
    };
    let plan = PlanNode::ViewSink {
        view_name: format!("tumble_115_{aggregate}"),
        pk: vec![0],
        child: Box::new(child),
    };
    compile_plan(&plan, db, &HashMap::from([("t".into(), schema())])).unwrap()
}

async fn reopen(
    dir: &TempDir,
    db: Arc<ShardDb>,
    view: CompiledView,
    aggregate: bool,
) -> (Arc<ShardDb>, CompiledView) {
    view.pipeline.persist(&db).await.unwrap();
    db.flush().await.unwrap();
    drop(view);
    Arc::try_unwrap(db)
        .ok()
        .expect("single DB owner")
        .close()
        .await
        .unwrap();
    let db = open_db(dir).await;
    let view = compile(db.clone(), aggregate);
    view.pipeline.restore(&db).await.unwrap();
    (db, view)
}

#[tokio::test]
async fn compiled_tumble_preserves_multiplicity_and_aggregates_across_reopen() {
    for aggregate in [false, true] {
        for recover in [false, true] {
            let dir = TempDir::new().unwrap();
            let mut db = open_db(&dir).await;
            let mut view = compile(db.clone(), aggregate);
            let mut bag = Bag::new();
            for (epoch, weight) in [2, -1, -1].into_iter().enumerate() {
                let out = view
                    .pipeline
                    .process(input(&[(1, Some(7), weight)]))
                    .unwrap();
                let expected = if aggregate {
                    match epoch {
                        0 => vec![aggregate_row(0, 2, 14, 1)],
                        1 => vec![aggregate_row(0, 2, 14, -1), aggregate_row(0, 1, 7, 1)],
                        _ => vec![aggregate_row(0, 1, 7, -1)],
                    }
                } else {
                    vec![window_row(1, Some(7), weight)]
                };
                assert_output(&out, aggregate, expected);
                accumulate(&mut bag, &out);
                let expected_bag = if epoch == 2 {
                    Bag::new()
                } else if aggregate {
                    Bag::from([aggregate_row(0, 2 - epoch as i64, 14 - 7 * epoch as i64, 1)])
                } else {
                    Bag::from([window_row(1, Some(7), 2 - epoch as i64)])
                };
                assert_eq!(bag, expected_bag);
                if recover {
                    (db, view) = reopen(&dir, db, view, aggregate).await;
                }
            }
            assert_output(
                &view.pipeline.process(input(&[])).unwrap(),
                aggregate,
                vec![],
            );
            drop(view);
            Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn compiled_tumble_finalized_correction_accepts_both_orders_after_reopen() {
    for recover in [false, true] {
        for insert_first in [false, true] {
            let dir = TempDir::new().unwrap();
            let mut db = open_db(&dir).await;
            let mut view = compile(db.clone(), false);
            let initial = view
                .pipeline
                .process(input(&[(1, Some(10), 1), (100, Some(99), 1)]))
                .unwrap();
            assert_output(
                &initial,
                false,
                vec![window_row(1, Some(10), 1), window_row(100, Some(99), 1)],
            );
            let mut bag = Bag::new();
            accumulate(&mut bag, &initial);
            if recover {
                (db, view) = reopen(&dir, db, view, false).await;
            }
            // Window 0 is closed: an unpaired late insert is still dropped.
            assert_output(
                &view.pipeline.process(input(&[(3, Some(30), 1)])).unwrap(),
                false,
                vec![],
            );
            let mut correction = vec![(1, Some(10), -1), (2, Some(20), 1)];
            if insert_first {
                correction.reverse();
            }
            let out = view.pipeline.process(input(&correction)).unwrap();
            assert_output(
                &out,
                false,
                vec![window_row(1, Some(10), -1), window_row(2, Some(20), 1)],
            );
            accumulate(&mut bag, &out);
            assert_eq!(
                bag,
                Bag::from([window_row(2, Some(20), 1), window_row(100, Some(99), 1)])
            );
            drop(view);
            Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn compiled_tumble_distinguishes_null_payload_from_zero_after_reopen() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir).await;
    let view = compile(db.clone(), false);
    let out = view
        .pipeline
        .process(input(&[(1, None, 2), (1, Some(0), 1)]))
        .unwrap();
    assert_output(
        &out,
        false,
        vec![window_row(1, None, 2), window_row(1, Some(0), 1)],
    );
    let (db, view) = reopen(&dir, db, view, false).await;
    let out = view
        .pipeline
        .process(input(&[(1, None, -1), (1, Some(0), -1)]))
        .unwrap();
    assert_output(
        &out,
        false,
        vec![window_row(1, None, -1), window_row(1, Some(0), -1)],
    );
    drop(view);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

#[tokio::test]
async fn tumble_public_loader_restores_emitted_weights() {
    let dir = TempDir::new().unwrap();
    let op_id = OperatorId(115);
    let mut op = TumbleWindowOp::new(schema(), 0, 10, LateDataPolicy::Drop);
    for weight in [2, -1, -1] {
        let out = op.process_epoch(input(&[(1, Some(7), weight)]), 0).unwrap();
        assert_output(&out, false, vec![window_row(1, Some(7), weight)]);
        let db = open_db(&dir).await;
        persist_tumble_window_state(&db, &op, op_id).await.unwrap();
        db.flush().await.unwrap();
        Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
        let db = open_db(&dir).await;
        op = load_tumble_window_state(&db, schema(), 0, 10, LateDataPolicy::Drop, op_id)
            .await
            .unwrap();
        Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
    }
    assert_output(&op.process_epoch(input(&[]), 0).unwrap(), false, vec![]);
}

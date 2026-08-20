use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::{compile_plan, ArrowZSet};
use rockstream_plan::{AggregateExpr, AggregateFunc, BinaryOp, Expr, PlanNode};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn batch(rows: &[(i64, i64)]) -> ArrowZSet {
    ArrowZSet::new(
        RecordBatch::try_new(
            schema(),
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
        vec![1; rows.len()],
    )
}

fn greater_than(column: usize, value: i64) -> Expr {
    Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column(column)),
        right: Box::new(Expr::Literal(value.to_be_bytes().to_vec())),
    }
}

async fn compile_filtered_sum(predicate: Expr) -> rockstream_ops::CompiledView {
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
        left_arr_id: OperatorId(201),
        right_arr_id: OperatorId(202),
        semantics: Default::default(),
    };
    let plan = PlanNode::ViewSink {
        view_name: "filtered_sum".into(),
        pk: vec![0],
        child: Box::new(PlanNode::Aggregate {
            input: Box::new(PlanNode::Filter {
                input: Box::new(join),
                predicate,
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
    schemas.insert("fact".into(), schema());
    schemas.insert("dim".into(), schema());
    compile_plan(&plan, db, &schemas).unwrap()
}

#[tokio::test]
async fn inner_predicate_transfers_to_left_and_preserves_complete_output() {
    let compiled = compile_filtered_sum(greater_than(1, 5)).await;
    let join = compiled.join.unwrap();
    assert_eq!(join.pipeline.strategy(), "factorized");
    let output = join
        .pipeline
        .process(batch(&[(1, 10), (1, 2)]), batch(&[(1, 7)]))
        .unwrap();
    assert_eq!(output.weights, vec![1]);
    assert_eq!(
        output
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        output
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
}

#[tokio::test]
async fn right_predicate_transfers_without_false_matches() {
    let compiled = compile_filtered_sum(greater_than(3, 6)).await;
    let join = compiled.join.unwrap();
    let output = join
        .pipeline
        .process(batch(&[(1, 10)]), batch(&[(1, 7), (1, 2)]))
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
        7
    );
}

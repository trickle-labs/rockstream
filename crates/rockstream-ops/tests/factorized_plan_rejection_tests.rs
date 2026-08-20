use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use object_store::local::LocalFileSystem;
use rockstream_ops::{compile_plan, ArrowZSet};
use rockstream_plan::{PlanNode, WindowExpr, WindowFunc};
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
                    rows.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap(),
        vec![1; rows.len()],
    )
}

#[tokio::test]
async fn windowed_join_selects_classic_and_matches_existing_row_number_output() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            "factorized-plan-rejection",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let join = PlanNode::InnerJoin {
        left: Box::new(PlanNode::Source {
            name: "left".into(),
        }),
        right: Box::new(PlanNode::Source {
            name: "right".into(),
        }),
        left_keys: vec![0],
        right_keys: vec![0],
        left_arr_id: OperatorId(901),
        right_arr_id: OperatorId(902),
        semantics: Default::default(),
    };
    let plan = PlanNode::ViewSink {
        view_name: "windowed_join".into(),
        pk: vec![0],
        child: Box::new(PlanNode::Window {
            input: Box::new(join),
            window_exprs: vec![WindowExpr {
                func: WindowFunc::RowNumber,
                partition_by: vec![0],
                order_by: vec![1],
            }],
        }),
    };
    let schemas = HashMap::from([("left".into(), schema()), ("right".into(), schema())]);
    let compiled = compile_plan(&plan, db, &schemas).unwrap();
    let join = compiled.join.unwrap();

    assert_eq!(join.pipeline.strategy(), "classic");
    let output = join
        .pipeline
        .process(batch(&[(1, 20), (1, 10)]), batch(&[(1, 7)]))
        .unwrap();
    assert_eq!(output.weights, vec![1, 1]);
    let values = output
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let ranks = output
        .data
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut actual = (0..output.num_rows())
        .map(|row| (values.value(row), ranks.value(row)))
        .collect::<Vec<_>>();
    actual.sort_unstable();
    assert_eq!(actual, vec![(10, 1), (20, 2)]);
}

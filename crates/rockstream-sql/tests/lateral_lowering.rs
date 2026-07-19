use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_plan::{Expr, PlanNode};
use rockstream_sql::frontend::SqlFrontend;

fn make_frontend_with_docs(tags_type: DataType) -> SqlFrontend {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tags", tags_type, true),
    ]));
    let frontend = SqlFrontend::new();
    frontend.register_table("docs", schema).unwrap();
    frontend
}

#[tokio::test]
async fn lower_unnest_json_array_to_lateral_plan_node() {
    let frontend = make_frontend_with_docs(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Int64,
        true,
    ))));
    let plan = frontend
        .sql_to_unoptimized_plan_node("SELECT id, UNNEST(tags) AS tag FROM docs")
        .await
        .expect("UNNEST should lower");
    match plan {
        PlanNode::Project { input, columns } => {
            assert_eq!(columns, vec![Expr::Column(0), Expr::Column(1)]);
            match *input {
                PlanNode::Lateral { input, func } => {
                    assert_eq!(func, rockstream_plan::LateralFunc::Unnest { col: 1 });
                    match *input {
                        PlanNode::Project { input, columns } => {
                            assert_eq!(columns, vec![Expr::Column(0), Expr::Column(1)]);
                            assert!(matches!(*input, PlanNode::Source { ref name } if name == "docs"));
                        }
                        other => panic!("expected inner Project under Lateral, got {other:?}"),
                    }
                }
                other => panic!("expected Lateral under outer Project, got {other:?}"),
            }
        }
        other => panic!("expected Project over Lateral for UNNEST, got {other:?}"),
    }
}

#[tokio::test]
async fn lower_lateral_unnest_nested_array_column() {
    let nested_tags = DataType::List(Arc::new(Field::new(
        "item",
        DataType::List(Arc::new(Field::new("nested", DataType::Int64, true))),
        true,
    )));
    let frontend = make_frontend_with_docs(nested_tags);
    let plan = frontend
        .sql_to_unoptimized_plan_node("SELECT id, UNNEST(tags) AS tag FROM docs")
        .await
        .expect("nested UNNEST should lower");
    match plan {
        PlanNode::Project { input, .. } => match *input {
            PlanNode::Lateral { func, .. } => {
                assert_eq!(func, rockstream_plan::LateralFunc::Unnest { col: 1 });
            }
            other => panic!("expected Lateral for nested UNNEST, got {other:?}"),
        },
        other => panic!("expected Project over Lateral for nested UNNEST, got {other:?}"),
    }
}

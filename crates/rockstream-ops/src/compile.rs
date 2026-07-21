//! Compile a `PlanNode` tree directly into an executable operator chain.
//!
//! Part of v0.51.3 Slice 3 ("Gateway↔Runtime Unification: One Data Plane").
//! `compile_plan` walks a `PlanNode::ViewSink` root and builds:
//!
//! - a `LinearPipeline` of stateless operators (`FilterOp`, `ProjectOp`,
//!   `MapOp`) covering everything between the source and the sink, and
//! - a `ViewSinkOp` that persists the pipeline's output to `db`.
//!
//! This is the fast path used by `CREATE VIEW` for simple materialized
//! views (`Source → [Filter] → [Project|Map] → ViewSink`). Richer plan
//! shapes (joins, aggregates, windows, recursion, ...) are not supported
//! here — those go through the `rockstream-diff`/`OpNode` physical-plan
//! path instead. `compile_plan` returns `OpError::UnsupportedPlanNode` for
//! any node it does not recognize, mirroring `DiffCtx::next_op_id`'s
//! `OperatorId` assignment convention for the ids it does hand out.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rockstream_plan::PlanNode;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;

use crate::error::OpError;
use crate::filter::FilterOp;
use crate::map::MapOp;
use crate::op::Operator;
use crate::pipeline::LinearPipeline;
use crate::project::{NamedExpr, ProjectOp};
use crate::sink::ViewSinkOp;

static NEXT_VIEW_SINK_OP_ID: AtomicU64 = AtomicU64::new(1);

/// The result of compiling a `PlanNode::ViewSink` tree: an executable
/// stateless operator chain plus the sink that writes its output to
/// storage.
pub struct CompiledView {
    /// The stateless operator chain (everything under `ViewSink`).
    pub pipeline: LinearPipeline,
    /// The sink that persists the pipeline's output to `view_output` storage.
    pub sink: ViewSinkOp,
    /// The `OperatorId` assigned to `sink` (used as the storage key prefix).
    pub sink_op_id: OperatorId,
    /// The view name from the source plan.
    pub view_name: String,
    /// Primary-key column indices for the view.
    pub pk: Vec<usize>,
}

/// Compile `plan` (which must be rooted at `PlanNode::ViewSink`) into a
/// `CompiledView` backed by `db`.
///
/// Supports `Source`, `Filter`, `Project`, and `Map` nodes beneath the
/// `ViewSink` root. Any other node shape (joins, aggregates, windows,
/// recursion, exchange, lateral, etc.) returns
/// `OpError::UnsupportedPlanNode`.
pub fn compile_plan(plan: &PlanNode, db: Arc<ShardDb>) -> Result<CompiledView, OpError> {
    let PlanNode::ViewSink {
        view_name,
        pk,
        child,
    } = plan
    else {
        return Err(OpError::unsupported_plan_node(format!(
            "expected PlanNode::ViewSink at the root, found {}",
            plan_node_kind(plan)
        )));
    };

    let ops = compile_stateless_chain(child)?;
    let mut pipeline = LinearPipeline::new();
    for op in ops {
        pipeline = pipeline.push(op);
    }

    // Stateless operators (Filter/Project/Map) carry no persisted identity;
    // the sink is the only node whose `OperatorId` addresses persisted
    // `view_output` storage, so it must be unique across compiled views.
    let sink_op_id = OperatorId(NEXT_VIEW_SINK_OP_ID.fetch_add(1, Ordering::Relaxed));
    let sink = ViewSinkOp::new(db, sink_op_id);

    Ok(CompiledView {
        pipeline,
        sink,
        sink_op_id,
        view_name: view_name.clone(),
        pk: pk.clone(),
    })
}

/// Recursively compile the stateless prefix of a plan (everything under
/// `ViewSink`) into a source-to-sink ordered list of operators.
fn compile_stateless_chain(node: &PlanNode) -> Result<Vec<Arc<dyn Operator>>, OpError> {
    match node {
        PlanNode::Source { .. } => {
            // Source rows arrive as deltas from the table's commit path; no
            // operator is needed to represent them in the pipeline.
            Ok(Vec::new())
        }
        PlanNode::Filter { input, predicate } => {
            let mut ops = compile_stateless_chain(input)?;
            ops.push(Arc::new(FilterOp::new(predicate.clone())));
            Ok(ops)
        }
        PlanNode::Project { input, columns } => {
            let mut ops = compile_stateless_chain(input)?;
            let named: Vec<NamedExpr> = columns
                .iter()
                .enumerate()
                .map(|(i, expr)| NamedExpr::new(format!("col{i}"), expr.clone()))
                .collect();
            ops.push(Arc::new(ProjectOp::new(named)));
            Ok(ops)
        }
        PlanNode::Map { input, func } => {
            let mut ops = compile_stateless_chain(input)?;
            ops.push(Arc::new(MapOp::new(func.clone(), "value")));
            Ok(ops)
        }
        other => Err(OpError::unsupported_plan_node(plan_node_kind(other))),
    }
}

/// Human-readable name of a `PlanNode` variant, used in error messages.
fn plan_node_kind(node: &PlanNode) -> String {
    match node {
        PlanNode::Source { .. } => "Source",
        PlanNode::Filter { .. } => "Filter",
        PlanNode::Project { .. } => "Project",
        PlanNode::Map { .. } => "Map",
        PlanNode::Aggregate { .. } => "Aggregate",
        PlanNode::Join { .. } => "Join",
        PlanNode::InnerJoin { .. } => "InnerJoin",
        PlanNode::ViewSink { .. } => "ViewSink",
        PlanNode::Exchange { .. } => "Exchange",
        _ => "Unsupported",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::lit;
    use crate::zset::ArrowZSet;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use object_store::local::LocalFileSystem;
    use rockstream_plan::{BinaryOp, Expr};
    use tempfile::TempDir;

    async fn make_db() -> (TempDir, Arc<ShardDb>) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(ShardDb::builder("db", store).build().await.unwrap());
        (dir, db)
    }

    fn make_row_batch(ids: &[i64], names: &[&str]) -> ArrowZSet {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let data = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap();
        ArrowZSet::new(data, vec![1; ids.len()])
    }

    #[tokio::test]
    async fn rejects_non_view_sink_root() {
        let plan = PlanNode::Source {
            name: "t".to_string(),
        };
        let (_dir, db) = make_db().await;
        let result = compile_plan(&plan, db);
        let err = match result {
            Ok(_) => panic!("expected UnsupportedPlanNode error"),
            Err(e) => e,
        };
        assert!(matches!(err, OpError::UnsupportedPlanNode { .. }));
        assert!(err.to_string().contains("RS-1013"));
    }

    #[tokio::test]
    async fn compiles_source_filter_project_view_sink() {
        let (_dir, db) = make_db().await;
        let plan = PlanNode::ViewSink {
            view_name: "v".to_string(),
            pk: vec![0],
            child: Box::new(PlanNode::Project {
                input: Box::new(PlanNode::Filter {
                    input: Box::new(PlanNode::Source {
                        name: "t".to_string(),
                    }),
                    predicate: Expr::BinaryOp {
                        op: BinaryOp::Gt,
                        left: Box::new(Expr::Column(0)),
                        right: Box::new(lit(1)),
                    },
                }),
                columns: vec![Expr::Column(0), Expr::Column(1)],
            }),
        };

        let compiled = compile_plan(&plan, db.clone()).unwrap();
        assert_eq!(compiled.view_name, "v");
        assert_eq!(compiled.pk, vec![0]);

        let batch = make_row_batch(&[1, 2, 3], &["a", "b", "c"]);
        let out = compiled.pipeline.process(batch).unwrap();
        // id=1 filtered out (1 > 1 is false); rows 2,3 survive.
        assert_eq!(out.num_rows(), 2);

        let epoch = compiled.sink.write_next_epoch(&out).await.unwrap();
        let rows = crate::sink::read_view_output(&db, compiled.sink_op_id, 2)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|(e, _, _, _)| *e == epoch));
    }

    #[tokio::test]
    async fn unsupported_child_node_reports_kind() {
        let (_dir, db) = make_db().await;
        let plan = PlanNode::ViewSink {
            view_name: "v".to_string(),
            pk: vec![0],
            child: Box::new(PlanNode::Aggregate {
                input: Box::new(PlanNode::Source {
                    name: "t".to_string(),
                }),
                group_by: vec![],
                aggregates: vec![],
            }),
        };
        let result = compile_plan(&plan, db);
        let err = match result {
            Ok(_) => panic!("expected UnsupportedPlanNode error"),
            Err(e) => e,
        };
        match err {
            OpError::UnsupportedPlanNode { kind, .. } => assert_eq!(kind, "Aggregate"),
            other => panic!("expected UnsupportedPlanNode, got {other:?}"),
        }
    }
}

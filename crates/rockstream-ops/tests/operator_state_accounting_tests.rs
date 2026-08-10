//! Operator state bytes accounting tests (v0.51.23 - Slice 1).
//! Verifies incremental O(1) state_bytes() tracking across all 11 stateful operators
//! and pipeline reporting via set_pipeline_state_bytes.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::distinct::DistinctOp;
use rockstream_ops::index_arrange::IndexArrangeOp;
use rockstream_ops::join::JoinOp;
use rockstream_ops::lateral::LateralOp;
use rockstream_ops::live_exec::{Stage, StatefulPipeline};
use rockstream_ops::minmax::{MinMaxKind, MinMaxOp};
use rockstream_ops::op::Operator;
use rockstream_ops::outer_join::OuterJoinOp;
use rockstream_ops::recursion::RecursionOp;
use rockstream_ops::time_window::{HopWindowOp, SessionWindowOp, TumbleWindowOp};
use rockstream_ops::topk::TopKOp;
use rockstream_ops::window::WindowOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{
    LateDataPolicy, LateralFunc, OuterJoinKind, PlanNode, WindowExpr, WindowFunc,
};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use rockstream_types::metrics::{get_pipeline_state_bytes, set_pipeline_state_bytes};

async fn make_test_db() -> Arc<ShardDb> {
    let store = Arc::new(object_store::memory::InMemory::new());
    Arc::new(ShardDb::builder("shard", store).build().await.unwrap())
}

#[tokio::test]
async fn test_join_state_bytes() {
    let op = JoinOp::new(OperatorId(1), vec![0], vec![0]);
    assert_eq!(op.state_bytes(), 0);

    let left_input = ArrowZSet::from_ab_rows(&[(10, 100), (20, 200)], 1);
    op.process_left_delta(left_input).unwrap();
    let staged_bytes = op.state_bytes();
    assert!(
        staged_bytes > 0,
        "Staged left delta must contribute to state_bytes"
    );

    op.commit_epoch().unwrap();
    let committed_bytes = op.state_bytes();
    assert!(
        committed_bytes > 0,
        "Committed arrangement must contribute to state_bytes"
    );

    // Retract left rows
    let retract_input = ArrowZSet::from_ab_rows(&[(10, 100), (20, 200)], -1);
    op.process_left_delta(retract_input).unwrap();
    op.commit_epoch().unwrap();
    assert_eq!(
        op.state_bytes(),
        0,
        "State bytes must return to 0 after total retraction"
    );
}

#[tokio::test]
async fn test_outer_join_state_bytes() {
    let op = OuterJoinOp::new(OperatorId(2), OuterJoinKind::Left, vec![0], vec![0]);
    assert_eq!(op.state_bytes(), 0);

    let left_input = ArrowZSet::from_ab_rows(&[(1, 10)], 1);
    let empty_right = ArrowZSet::empty(left_input.data.schema());
    op.process_epoch(left_input, empty_right.clone()).unwrap();
    let state_b = op.state_bytes();
    assert!(state_b > 0);

    let retract = ArrowZSet::from_ab_rows(&[(1, 10)], -1);
    op.process_epoch(retract, empty_right).unwrap();
    assert_eq!(op.state_bytes(), 0);
}

#[test]
fn test_aggregate_state_bytes() {
    let op = AggregateOp::new(OperatorId(3));
    assert_eq!(op.state_bytes(), 0);

    let input = ArrowZSet::from_ab_rows(&[(1, 100), (2, 200)], 1);
    op.process_delta(input).unwrap();
    assert_eq!(op.state_bytes(), 2 * 24);

    let retract = ArrowZSet::from_ab_rows(&[(1, 100), (2, 200)], -1);
    op.process_delta(retract).unwrap();
    assert_eq!(op.state_bytes(), 0);
}

#[test]
fn test_distinct_state_bytes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let op = DistinctOp::new(schema);
    assert_eq!(op.state_bytes(), 0);

    let input = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], 1);
    op.process_delta(input).unwrap();
    assert!(op.state_bytes() > 0);

    let retract = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], -1);
    op.process_delta(retract).unwrap();
    assert_eq!(op.state_bytes(), 0);
}

#[test]
fn test_topk_state_bytes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let op = TopKOp::new(schema, 5, 1, vec![0]);
    assert_eq!(op.state_bytes(), 0);

    let input = ArrowZSet::from_ab_rows(&[(1, 50), (1, 100)], 1);
    op.process_epoch(input, 0).unwrap();
    assert!(op.state_bytes() > 0);
}

#[test]
fn test_minmax_state_bytes() {
    let op = MinMaxOp::new(OperatorId(4), MinMaxKind::Min);
    assert_eq!(op.state_bytes(), 0);

    let input = ArrowZSet::from_ab_rows(&[(1, 10), (1, 20)], 1);
    op.process_delta(input).unwrap();
    assert!(op.state_bytes() > 0);

    let retract = ArrowZSet::from_ab_rows(&[(1, 10), (1, 20)], -1);
    op.process_delta(retract).unwrap();
    assert_eq!(op.state_bytes(), 0);
}

#[test]
fn test_window_state_bytes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("w", DataType::Int64, false),
    ]));
    let expr = WindowExpr {
        func: WindowFunc::RowNumber,
        partition_by: vec![0],
        order_by: vec![1],
    };
    let op = WindowOp::new(schema, vec![expr]);
    assert_eq!(op.state_bytes(), 0);

    let input = ArrowZSet::from_ab_rows(&[(1, 100), (1, 200)], 1);
    op.process_epoch(input, 0).unwrap();
    assert!(op.state_bytes() > 0);
}

#[test]
fn test_time_window_state_bytes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]));
    let tumble = TumbleWindowOp::new(schema.clone(), 0, 1000, LateDataPolicy::Drop);
    assert_eq!(tumble.state_bytes(), 0);

    let input = ArrowZSet::from_ab_rows(&[(500, 42)], 1);
    tumble.process_epoch(input, 0).unwrap();
    assert!(tumble.state_bytes() > 0);

    let hop = HopWindowOp::new(schema.clone(), 0, 1000, 500, LateDataPolicy::Drop);
    assert_eq!(hop.state_bytes(), 0);
    let input_hop = ArrowZSet::from_ab_rows(&[(500, 42)], 1);
    hop.process_epoch(input_hop, 0).unwrap();
    assert!(hop.state_bytes() > 0);

    let session = SessionWindowOp::new(schema, 0, 500, LateDataPolicy::Drop);
    assert_eq!(session.state_bytes(), 0);
    let input_session = ArrowZSet::from_ab_rows(&[(100, 42)], 1);
    session.process_epoch(input_session, 0).unwrap();
    assert!(session.state_bytes() > 0);
}

#[tokio::test]
async fn test_index_arrange_state_bytes() {
    let db = make_test_db().await;
    let op = IndexArrangeOp::new(db, OperatorId(5), vec![0], vec![1], 1000);
    assert_eq!(op.state_bytes(), 0);

    let input = ArrowZSet::from_ab_rows(&[(1, 100), (2, 200)], 1);
    op.apply_delta(&input).await.unwrap();
    assert!(op.state_bytes() > 0);

    let retract = ArrowZSet::from_ab_rows(&[(1, 100), (2, 200)], -1);
    op.apply_delta(&retract).await.unwrap();
    assert_eq!(op.state_bytes(), 0);
}

#[test]
fn test_lateral_state_bytes() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let func = LateralFunc::GenerateSeries {
        start: 1,
        stop: 10,
        step: 1,
    };
    let op = LateralOp::new(schema, func).unwrap();
    assert_eq!(op.state_bytes(), 0);
}

#[test]
fn test_recursion_state_bytes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let base = PlanNode::Source {
        name: "input".to_string(),
    };
    let step = PlanNode::Source {
        name: "rec".to_string(),
    };
    let op = RecursionOp::new(schema, base, step, 5, true);
    assert_eq!(op.state_bytes(), 0);

    let input = ArrowZSet::from_ab_rows(&[(1, 2)], 1);
    op.process_epoch(input, 0).unwrap();
    assert!(op.state_bytes() > 0);
}

#[test]
fn test_pipeline_metrics_reporting() {
    let agg_op = Arc::new(AggregateOp::new(OperatorId(10)));
    let pipeline = StatefulPipeline::new().push(Stage::Aggregate(agg_op.clone()));

    assert_eq!(pipeline.state_bytes(), 0);

    let input = ArrowZSet::from_ab_rows(&[(1, 100)], 1);
    pipeline.process(input).unwrap();

    let total_bytes = pipeline.state_bytes();
    assert_eq!(total_bytes, 24);

    set_pipeline_state_bytes("test_view", total_bytes);
    assert_eq!(get_pipeline_state_bytes("test_view"), total_bytes);
}

#[tokio::test]
async fn test_operator_state_accounting_durability_lfs() {
    let db = make_test_db().await;
    let agg_op = Arc::new(AggregateOp::new(OperatorId(20)));
    let input = ArrowZSet::from_ab_rows(&[(1, 100), (2, 200)], 1);
    agg_op.process_delta(input).unwrap();

    let bytes_before = agg_op.state_bytes();
    assert_eq!(bytes_before, 48);

    rockstream_ops::aggregate::persist_agg_state(&db, &agg_op)
        .await
        .unwrap();

    let restored_agg = AggregateOp::new(OperatorId(20));
    restored_agg.restore_in_place(&db).await.unwrap();

    assert_eq!(restored_agg.state_bytes(), bytes_before);
}

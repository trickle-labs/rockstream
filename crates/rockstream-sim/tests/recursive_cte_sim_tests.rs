#![cfg(feature = "simulation")]

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use rockstream_control::FrontierAggregator;
use rockstream_ops::recursion::{DistributedShardStatus, RecursionOp, RecursionStrategy};
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{Expr, JoinSemantics, PlanNode};
use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_types::frontier::ShardFrontierReport;
use rockstream_types::ids::{OperatorId, ShardId};

fn schema_edges() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("src", DataType::Int64, false),
        Field::new("dst", DataType::Int64, false),
    ]))
}

fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let src: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let dst: Vec<i64> = rows.iter().map(|row| row.1).collect();
    let weights: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let data = RecordBatch::try_new(
        schema_edges(),
        vec![
            Arc::new(Int64Array::from(src)) as ArrayRef,
            Arc::new(Int64Array::from(dst)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

fn base_plan() -> PlanNode {
    PlanNode::Source {
        name: "edges".to_string(),
    }
}

fn step_plan() -> PlanNode {
    PlanNode::Project {
        input: Box::new(PlanNode::InnerJoin {
            left: Box::new(PlanNode::Exchange {
                kind: rockstream_plan::ExchangeKind::Loopback,
                child: Box::new(PlanNode::Source {
                    name: "reach".to_string(),
                }),
            }),
            right: Box::new(PlanNode::Source {
                name: "edges".to_string(),
            }),
            left_keys: vec![1],
            right_keys: vec![0],
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
            semantics: JoinSemantics::default(),
        }),
        columns: vec![Expr::Column(0), Expr::Column(3)],
    }
}

fn accumulate(state: &mut BTreeMap<(i64, i64), i64>, batch: &ArrowZSet) {
    if batch.is_empty() {
        return;
    }
    let src = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let dst = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for row_idx in 0..batch.num_rows() {
        *state
            .entry((src.value(row_idx), dst.value(row_idx)))
            .or_insert(0) += batch.weights[row_idx];
    }
    state.retain(|_, weight| *weight > 0);
}

#[test]
fn distributed_recursion_sim_faults_surface_registered_codes() {
    buggify_init(4242);
    let op = RecursionOp::new(schema_edges(), base_plan(), step_plan(), 16, true);
    let frontier = FrontierAggregator::new();
    frontier
        .ingest(ShardFrontierReport {
            shard_id: ShardId(1),
            epoch: 3,
        })
        .unwrap();
    frontier
        .ingest(ShardFrontierReport {
            shard_id: ShardId(2),
            epoch: if buggify!("recursion.inner_frontier_stall", 1.0) {
                1
            } else {
                3
            },
        })
        .unwrap();
    assert!(frontier.cluster_frontier().epoch.is_some());

    let err = op
        .process_distributed_epoch(
            &[
                (1, make_input(&[(1, 2, 1)])),
                (2, ArrowZSet::empty(schema_edges())),
            ],
            &[
                DistributedShardStatus {
                    shard_id: 1,
                    frontier_iteration: 3,
                    delta_is_empty: false,
                    iteration_cost: 10,
                },
                DistributedShardStatus {
                    shard_id: 2,
                    frontier_iteration: 1,
                    delta_is_empty: true,
                    iteration_cost: 10,
                },
            ],
            1,
        )
        .expect_err("stalled shard must fail");
    assert!(err.to_string().contains("RS-1512"));

    let _ = op
        .process_distributed_epoch(
            &[(1, make_input(&[(2, 3, 1)])), (2, make_input(&[(3, 4, 1)]))],
            &[
                DistributedShardStatus {
                    shard_id: 1,
                    frontier_iteration: 4,
                    delta_is_empty: true,
                    iteration_cost: 10,
                },
                DistributedShardStatus {
                    shard_id: 2,
                    frontier_iteration: 4,
                    delta_is_empty: true,
                    iteration_cost: if buggify!("recursion.per_shard_cost_spike", 1.0) {
                        100
                    } else {
                        10
                    },
                },
            ],
            2,
        )
        .unwrap();
    assert_eq!(op.strategy_for_shard(2), Some(RecursionStrategy::Recompute));
    buggify_disable();
}

#[test]
fn exchange_repartition_during_recursion_preserves_convergence_sim() {
    buggify_init(7373);
    let op = RecursionOp::new(schema_edges(), base_plan(), step_plan(), 16, true);
    let left = if buggify!("recursion.exchange_delta_reorder", 1.0) {
        make_input(&[(3, 4, 1)])
    } else {
        make_input(&[(1, 2, 1), (2, 3, 1)])
    };
    let right = if left.num_rows() == 1 {
        make_input(&[(1, 2, 1), (2, 3, 1)])
    } else {
        make_input(&[(3, 4, 1)])
    };
    let out = op
        .process_distributed_epoch(
            &[(1, left), (2, right)],
            &[
                DistributedShardStatus {
                    shard_id: 1,
                    frontier_iteration: 4,
                    delta_is_empty: true,
                    iteration_cost: 10,
                },
                DistributedShardStatus {
                    shard_id: 2,
                    frontier_iteration: 4,
                    delta_is_empty: true,
                    iteration_cost: 10,
                },
            ],
            1,
        )
        .unwrap();
    let mut rows = BTreeMap::new();
    accumulate(&mut rows, &out);
    assert!(rows.contains_key(&(1, 4)));
    buggify_disable();
}

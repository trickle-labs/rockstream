use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::{
    AggregateOp, ArrowZSet, FactorizedAggregateKind, FactorizedJoinAggregateOp, JoinKind, JoinOp,
    JoinPipeline, Stage,
};
use rockstream_types::ids::{OperatorId, ShardId, WorkerId, WorkloadId};
use rockstream_types::metrics::{
    self, R1ExecutionContext, R1ExecutionCounters, R1ExecutionKey, R1ExecutionStrategy,
    R1WorkerActivity,
};
use std::sync::{LazyLock, Mutex};

static METRICS_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn batch(rows: &[(i64, i64)]) -> ArrowZSet {
    weighted_batch(
        &rows
            .iter()
            .map(|&(key, value)| (key, value, 1))
            .collect::<Vec<_>>(),
    )
}

fn weighted_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    ArrowZSet::new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ])),
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
        rows.iter().map(|row| row.2).collect(),
    )
}

#[test]
fn classic_and_factorized_work_counters_are_exact() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    metrics::reset_all();
    let context = R1ExecutionContext {
        worker_id: WorkerId(1),
        workload_id: WorkloadId(7),
        shard_id: ShardId(9),
    };
    let classic = JoinPipeline::new(
        Vec::new(),
        Vec::new(),
        JoinKind::Inner(Arc::new(JoinOp::new(OperatorId(1), vec![0], vec![0]))),
        vec![Stage::Aggregate(Arc::new(AggregateOp::new(OperatorId(2))))],
    );
    let classic_output = metrics::with_r1_execution_context(context, || {
        classic.process(batch(&[(1, 2)]), batch(&[(1, 5), (1, 6)]))
    })
    .unwrap();
    assert_eq!(classic_output.weights, vec![1]);
    assert_eq!(
        classic_output
            .data
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|array| array.values().to_vec())
            })
            .collect::<Vec<_>>(),
        vec![Some(vec![1]), Some(vec![4]), Some(vec![2]), None]
    );
    assert_eq!(
        classic_output
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values()
            .to_vec(),
        vec![2.0]
    );

    let factorized_op = Arc::new(FactorizedJoinAggregateOp::new(
        OperatorId(3),
        vec![0],
        vec![0],
        2,
        2,
        0,
        1,
        FactorizedAggregateKind::Sum,
    ));
    let factorized = JoinPipeline::new(
        Vec::new(),
        Vec::new(),
        JoinKind::Factorized(factorized_op.clone()),
        Vec::new(),
    );
    let factorized_output = metrics::with_r1_execution_context(context, || {
        factorized.process(batch(&[(1, 2)]), batch(&[(1, 5), (1, 6)]))
    })
    .unwrap();
    assert_eq!(factorized_output.weights, vec![1]);
    assert_eq!(
        factorized_output
            .data
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>(),
        vec![vec![1], vec![4]]
    );
    assert_eq!(factorized_op.factor_payload_rows(), 3);
    assert_eq!(factorized_op.factor_payload_bytes(), 117);

    let snapshot = metrics::r1_execution_snapshot();
    assert_eq!(
        snapshot,
        vec![
            (
                R1ExecutionKey {
                    worker_id: WorkerId(1),
                    workload_id: WorkloadId(7),
                    shard_id: ShardId(9),
                    operator_id: OperatorId(1),
                    strategy: R1ExecutionStrategy::Classic,
                },
                R1ExecutionCounters {
                    input_deltas: 3,
                    arrangement_probes: 3,
                    flattened_intermediate_tuples: 2,
                    output_deltas: 1,
                    changed_state_writes: 3,
                    ..Default::default()
                },
            ),
            (
                R1ExecutionKey {
                    worker_id: WorkerId(1),
                    workload_id: WorkloadId(7),
                    shard_id: ShardId(9),
                    operator_id: OperatorId(2),
                    strategy: R1ExecutionStrategy::Classic,
                },
                R1ExecutionCounters {
                    input_deltas: 2,
                    arrangement_probes: 1,
                    output_deltas: 1,
                    changed_state_writes: 1,
                    ..Default::default()
                },
            ),
            (
                R1ExecutionKey {
                    worker_id: WorkerId(1),
                    workload_id: WorkloadId(7),
                    shard_id: ShardId(9),
                    operator_id: OperatorId(3),
                    strategy: R1ExecutionStrategy::Factorized,
                },
                R1ExecutionCounters {
                    input_deltas: 3,
                    arrangement_probes: 2,
                    output_deltas: 1,
                    changed_state_writes: 3,
                    factor_payload_rows: 3,
                    factor_payload_bytes: 117,
                    ..Default::default()
                },
            ),
        ]
    );
    assert_eq!(
        metrics::r1_worker_snapshot(),
        vec![(
            WorkerId(1),
            R1WorkerActivity {
                state_writes: 7,
                ..Default::default()
            },
        )]
    );
}

#[test]
fn test_v0612_execution_counters_reflect_reduced_deltas() {
    let _guard = METRICS_TEST_LOCK.lock().unwrap();
    metrics::reset_all();
    let operator_id = OperatorId(61_202);
    let op = AggregateOp::new(operator_id);
    let context = R1ExecutionContext {
        worker_id: WorkerId(1),
        workload_id: WorkloadId(7),
        shard_id: ShardId(9),
    };

    let (initial, update) = metrics::with_r1_execution_context(context, || {
        let initial = op
            .process_delta_with_result(weighted_batch(&[(1, 10, 1)]))
            .unwrap();
        let update = op
            .process_delta_with_result(weighted_batch(&[
                (1, 10, -1),
                (1, 20, 1),
                (1, 20, -1),
                (1, 40, 1),
            ]))
            .unwrap();
        (initial, update)
    });

    let rows = |result: &rockstream_ops::op::OperatorEpochResult| {
        let keys = result
            .output_delta
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = result
            .output_delta
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = result
            .output_delta
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let avgs = result
            .output_delta
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..result.output_delta.num_rows())
            .map(|row| {
                (
                    keys.value(row),
                    sums.value(row),
                    counts.value(row),
                    avgs.value(row),
                    result.output_delta.weights[row],
                )
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(rows(&initial), vec![(1, 10, 1, 10.0, 1)]);
    assert_eq!(
        rows(&update),
        vec![(1, 10, 1, 10.0, -1), (1, 40, 1, 40.0, 1)]
    );
    assert_eq!(
        metrics::r1_execution_snapshot(),
        vec![(
            R1ExecutionKey {
                worker_id: WorkerId(1),
                workload_id: WorkloadId(7),
                shard_id: ShardId(9),
                operator_id,
                strategy: R1ExecutionStrategy::Classic,
            },
            R1ExecutionCounters {
                input_deltas: 5,
                arrangement_probes: 2,
                output_deltas: 3,
                changed_state_writes: 2,
                ..Default::default()
            },
        )]
    );
}

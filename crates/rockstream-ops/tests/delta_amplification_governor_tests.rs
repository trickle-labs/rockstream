use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::{
    ArrowZSet, DeltaAmplificationBudget, DeltaAmplificationCounters, DeltaAmplificationGovernor,
    FactorizedAggregateKind, FactorizedJoinAggregateOp, PlanStrategy,
};
use rockstream_types::ids::OperatorId;
use std::sync::Arc;

fn batch(rows: &[(i64, i64)]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    ArrowZSet::new(
        RecordBatch::try_new(
            schema,
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

fn budget_with(max_probes: u64) -> DeltaAmplificationBudget {
    DeltaAmplificationBudget {
        max_probes,
        ..DeltaAmplificationBudget::default()
    }
}

#[test]
fn every_counter_is_recorded_exactly_once_per_epoch() {
    let op = FactorizedJoinAggregateOp::new(
        OperatorId(101),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    );
    let output = op
        .process_epoch(batch(&[(1, 2)]), batch(&[(1, 5)]))
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
    assert_eq!(
        op.governor().counters(),
        DeltaAmplificationCounters {
            input_deltas: 2,
            probes: 1,
            shuffled_bytes: 0,
            intermediate_tuples: 0,
            output_deltas: 1,
            state_writes: 2,
        }
    );
}

#[test]
fn budget_refusal_is_coded_and_atomic() {
    let op = FactorizedJoinAggregateOp::new(
        OperatorId(102),
        vec![0],
        vec![0],
        2,
        2,
        0,
        3,
        FactorizedAggregateKind::Sum,
    )
    .with_governor(DeltaAmplificationGovernor::new(budget_with(0)));
    let error = op
        .process_epoch(batch(&[(1, 2)]), batch(&[(1, 5)]))
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "[RS-2030] Delta amplification budget exceeded for probes (1/0); next_steps: use the classic plan, reduce the input delta, or raise the reviewed operator budget"
    );
    assert_eq!(op.factor_payload_rows(), 0);
    assert_eq!(op.factor_payload_bytes(), 0);
    assert_eq!(
        op.governor().counters(),
        DeltaAmplificationCounters::default()
    );
}

#[test]
fn compile_selection_falls_back_before_execution_when_estimate_exceeds_budget() {
    let estimate = DeltaAmplificationCounters {
        probes: 11,
        ..DeltaAmplificationCounters::default()
    };
    assert_eq!(
        DeltaAmplificationGovernor::select(estimate, budget_with(10)),
        PlanStrategy::Classic
    );
    assert_eq!(
        DeltaAmplificationGovernor::select(estimate, budget_with(11)),
        PlanStrategy::Factorized
    );
}

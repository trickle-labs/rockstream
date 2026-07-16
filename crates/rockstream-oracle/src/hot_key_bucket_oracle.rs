#![cfg(test)]

use std::collections::BTreeMap;
use std::sync::Arc;

use proptest::prelude::*;
use rockstream_ops::aggregate::{AggregateOp, BucketedAggregateOp};
use rockstream_ops::op::Operator;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::ids::OperatorId;

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals: Vec<i64> = rows.iter().map(|(k, _, _)| *k).collect();
    let v_vals: Vec<i64> = rows.iter().map(|(_, v, _)| *v).collect();
    let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(k_vals)),
            Arc::new(Int64Array::from(v_vals)),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

fn apply_output(state: &mut BTreeMap<i64, (i64, i64, i64)>, output: &ArrowZSet) {
    use arrow::array::Int64Array;

    if output.is_empty() {
        return;
    }

    let k_col = output
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let s_col = output
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let c_col = output
        .data
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let a_col = output
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..output.num_rows() {
        let row = (s_col.value(i), c_col.value(i), a_col.value(i));
        if output.weights[i] > 0 {
            state.insert(k_col.value(i), row);
        } else if output.weights[i] < 0 {
            state.remove(&k_col.value(i));
        }
    }
}

fn run_final_state<O: Operator>(
    operator: &O,
    epochs: &[Vec<(i64, i64, i64)>],
) -> Vec<(i64, i64, i64, i64)> {
    let mut state = BTreeMap::new();
    for epoch in epochs {
        let output = operator.process_delta(make_kv_batch(epoch)).unwrap();
        apply_output(&mut state, &output);
    }
    state
        .into_iter()
        .map(|(k, (sum, count, avg))| (k, sum, count, avg))
        .collect()
}

fn assert_bucketed_matches_unsplit(epochs: &[Vec<(i64, i64, i64)>], bucket_count: u16) {
    let unsplit = AggregateOp::new(OperatorId(10));
    let bucketed = BucketedAggregateOp::new(OperatorId(11), 1, bucket_count);
    assert_eq!(
        run_final_state(&unsplit, epochs),
        run_final_state(&bucketed, epochs)
    );
}

fn arb_delta_row() -> impl Strategy<Value = (i64, i64, i64)> {
    (0i64..=4, 0i64..=16, prop_oneof![Just(1i64), Just(-1i64)])
}

fn arb_epochs() -> impl Strategy<Value = Vec<Vec<(i64, i64, i64)>>> {
    prop::collection::vec(prop::collection::vec(arb_delta_row(), 0..=6), 1..=8)
}

mod tests {
    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1_024))]

        #[test]
        fn incremental_equals_batch_for_bucketed_hot_key_aggregate(epochs in arb_epochs()) {
            for bucket_count in [2u16, 4, 8, 16] {
                assert_bucketed_matches_unsplit(&epochs, bucket_count);
            }
        }
    }
}

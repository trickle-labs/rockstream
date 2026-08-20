//! v0.59.5 Slice 4: Constant Write Amplification Scale Tests (O(1) Hot Path).
//!
//! Asserts that a 1-key insert/update/delete against arrangements containing
//! 1,000, 100,000, and 10,000,000 groups produces approximately constant
//! state mutations and logical write bytes (O(1) write amplification).

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::op::OperatorEpochResult;
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_types::ids::OperatorId;
use rockstream_types::metrics::{self, R1PersistenceCounters};
use rockstream_types::state_mutation::{OperatorEpochMetrics, StateMutation};
use std::sync::Arc;

fn make_kv_batch(rows: &[(i64, i64, i64)]) -> ArrowZSet {
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

fn state_key(operator_id: OperatorId, key: i64) -> Vec<u8> {
    ShardKeyEncoder::encode(ShardPrefix::OpState, operator_id.0, &key.to_be_bytes())
}

fn put(operator_id: OperatorId, key: i64, sum: i64, count: i64) -> StateMutation {
    let mut value = Vec::with_capacity(16);
    value.extend_from_slice(&sum.to_be_bytes());
    value.extend_from_slice(&count.to_be_bytes());
    StateMutation::Put {
        key: state_key(operator_id, key),
        value: Bytes::from(value),
    }
}

fn assert_result(
    result: &OperatorEpochResult,
    integer_columns: &[Vec<i64>],
    averages: &[f64],
    weights: &[i64],
    mutations: Vec<StateMutation>,
    metrics: OperatorEpochMetrics,
) {
    assert_eq!(
        result
            .output_delta
            .data
            .columns()
            .iter()
            .take(3)
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>(),
        integer_columns
    );
    assert_eq!(
        result
            .output_delta
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        averages
    );
    assert_eq!(result.output_delta.weights, weights);
    assert_eq!(result.state_mutations, mutations);
    assert_eq!(result.metrics, metrics);
}

#[test]
fn test_constant_write_amplification_across_scales() {
    for scale in [1_000, 100_000, 10_000_000] {
        let operator_id = OperatorId(scale as u64);
        let op = AggregateOp::new(operator_id);
        let chunk_size = 50_000;
        let mut populated = 0;
        while populated < scale {
            let this_chunk = (scale - populated).min(chunk_size);
            let entries: Vec<(i64, i64, i64)> = (populated..populated + this_chunk)
                .map(|i| (i as i64, i as i64, 1))
                .collect();
            let batch = make_kv_batch(&entries);
            op.process_delta_with_result(batch).unwrap();
            populated += this_chunk;
        }
        assert_eq!(op.live_groups(), scale);
        metrics::reset_all();

        let inserted = op
            .process_delta_with_result(make_kv_batch(&[(scale as i64, 7, 1)]))
            .unwrap();
        assert_result(
            &inserted,
            &[vec![scale as i64], vec![7], vec![1]],
            &[7.0],
            &[1],
            vec![put(operator_id, scale as i64, 7, 1)],
            OperatorEpochMetrics {
                input_records: 1,
                output_records: 1,
                dirty_keys: 1,
                state_mutations: 1,
                logical_mutation_bytes: 33,
                full_state_entries_visited: 0,
                state_bytes: (scale + 1) * 24,
            },
        );

        let updated = op
            .process_delta_with_result(make_kv_batch(&[(42, 42, -1), (42, 99, 1)]))
            .unwrap();
        assert_result(
            &updated,
            &[vec![42, 42], vec![42, 99], vec![1, 1]],
            &[42.0, 99.0],
            &[-1, 1],
            vec![put(operator_id, 42, 99, 1)],
            OperatorEpochMetrics {
                input_records: 2,
                output_records: 2,
                dirty_keys: 1,
                state_mutations: 1,
                logical_mutation_bytes: 33,
                full_state_entries_visited: 0,
                state_bytes: (scale + 1) * 24,
            },
        );

        let deleted = op
            .process_delta_with_result(make_kv_batch(&[(43, 43, -1)]))
            .unwrap();
        assert_result(
            &deleted,
            &[vec![43], vec![43], vec![1]],
            &[43.0],
            &[-1],
            vec![StateMutation::Delete {
                key: state_key(operator_id, 43),
            }],
            OperatorEpochMetrics {
                input_records: 1,
                output_records: 1,
                dirty_keys: 1,
                state_mutations: 1,
                logical_mutation_bytes: 17,
                full_state_entries_visited: 0,
                state_bytes: scale * 24,
            },
        );

        assert_eq!(op.live_groups(), scale);
        assert_eq!(op.state_bytes(), (scale * 24) as u64);
        assert_eq!(
            metrics::r1_persistence_snapshot(),
            vec![(
                operator_id,
                R1PersistenceCounters {
                    state_mutations: 3,
                    logical_mutation_bytes: 83,
                    dirty_keys: 3,
                    full_state_entries_visited: 0,
                },
            )]
        );
    }
}

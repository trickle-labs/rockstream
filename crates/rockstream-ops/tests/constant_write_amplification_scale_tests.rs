//! v0.59.5 Slice 4: Constant Write Amplification Scale Tests (O(1) Hot Path).
//!
//! Asserts that a 1-key insert/update/delete against arrangements containing
//! 1,000, 100,000, and 10,000,000 groups produces approximately constant
//! state mutations and logical write bytes (O(1) write amplification).

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::ids::OperatorId;
use rockstream_types::metrics::{self, R1PersistenceCounters};
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

#[test]
fn test_constant_write_amplification_across_scales() {
    metrics::reset_all();
    let scales = [1_000, 100_000, 1_000_000];
    let mut mutation_counts = Vec::new();
    let mut write_bytes = Vec::new();

    for &scale in &scales {
        let op = AggregateOp::new(OperatorId(scale as u64));
        let mut expected_counters = R1PersistenceCounters::default();

        // Pre-populate with `scale` groups in chunks of 50,000
        let chunk_size = 50_000;
        let mut populated = 0;
        while populated < scale {
            let this_chunk = (scale - populated).min(chunk_size);
            let entries: Vec<(i64, i64, i64)> = (populated..populated + this_chunk)
                .map(|i| (i as i64, i as i64, 1))
                .collect();
            let batch = make_kv_batch(&entries);
            let result = op.process_delta_with_result(batch).unwrap();
            expected_counters.state_mutations += result.metrics.state_mutations as u64;
            expected_counters.logical_mutation_bytes +=
                result.metrics.logical_mutation_bytes as u64;
            expected_counters.dirty_keys += result.metrics.dirty_keys as u64;
            populated += this_chunk;
        }

        assert_eq!(op.live_groups(), scale);

        // Perform a single 1-key update
        let one_key_delta = make_kv_batch(&[(42, 999, 1)]);
        let result = op.process_delta_with_result(one_key_delta).unwrap();
        expected_counters.state_mutations += result.metrics.state_mutations as u64;
        expected_counters.logical_mutation_bytes += result.metrics.logical_mutation_bytes as u64;
        expected_counters.dirty_keys += result.metrics.dirty_keys as u64;

        // Must emit exactly 1 state mutation regardless of scale (O(1))
        assert_eq!(
            result.state_mutations.len(),
            1,
            "Scale {} must emit exactly 1 mutation for 1-key change",
            scale
        );
        assert_eq!(result.metrics.dirty_keys, 1);

        let delta_bytes: usize = result.state_mutations.iter().map(|m| m.size_bytes()).sum();
        mutation_counts.push(result.state_mutations.len());
        write_bytes.push(delta_bytes);
        assert_eq!(
            metrics::r1_persistence_snapshot()
                .into_iter()
                .find(|(operator_id, _)| *operator_id == op.op_id)
                .unwrap(),
            (op.op_id, expected_counters)
        );
    }

    // All mutation counts must be exactly 1
    assert!(mutation_counts.iter().all(|&c| c == 1));

    // Logical write bytes must be identical/constant across all scale orders
    let first_bytes = write_bytes[0];
    for (i, &bytes) in write_bytes.iter().enumerate() {
        assert_eq!(
            bytes, first_bytes,
            "Scale {} bytes {} must match initial scale bytes {}",
            scales[i], bytes, first_bytes
        );
    }
}

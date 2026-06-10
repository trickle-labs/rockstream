//! Oracle property tests for TopK (v0.12 — IVM-9).
//!
//! ## Oracle property
//!
//! `incremental(TopK(k, Δ)) == batch(TopK(k, accumulated))` for every random
//! sequence of insert/update/delete operations.
//!
//! ## TopKOracle
//!
//! Batches all inserts/deletes and computes the reference output as:
//! `sort_desc(rows).take(K)`, compared against the cumulative incremental output.

#[cfg(test)]
mod proptest_oracle {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use proptest::prelude::*;
    use rockstream_ops::topk::TopKOp;
    use rockstream_ops::zset::ArrowZSet;

    // ─── Type aliases ─────────────────────────────────────────────────────────

    /// One row in a delta epoch: (rank_value, id, partition_key, weight).
    type DeltaRow = (i64, i64, i64, i64);

    // ─── Helpers ──────────────────────────────────────────────────────────────

    fn schema_vpk() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),   // rank_col = 0
            Field::new("id", DataType::Int64, false),
            Field::new("pk", DataType::Int64, false),  // partition_col = 2
        ]))
    }

    fn make_batch(rows: &[DeltaRow]) -> ArrowZSet {
        let v: Vec<i64> = rows.iter().map(|(v, _, _, _)| *v).collect();
        let id: Vec<i64> = rows.iter().map(|(_, id, _, _)| *id).collect();
        let pk: Vec<i64> = rows.iter().map(|(_, _, pk, _)| *pk).collect();
        let w: Vec<i64> = rows.iter().map(|(_, _, _, w)| *w).collect();
        let data = RecordBatch::try_new(
            schema_vpk(),
            vec![
                Arc::new(Int64Array::from(v)) as ArrayRef,
                Arc::new(Int64Array::from(id)) as ArrayRef,
                Arc::new(Int64Array::from(pk)) as ArrayRef,
            ],
        )
        .unwrap();
        ArrowZSet::new(data, w)
    }

    // ─── Batch reference oracle ───────────────────────────────────────────────

    /// Accumulate (pk, v, id) → net_weight into the given map.
    fn accumulate_state(acc: &mut BTreeMap<(i64, i64, i64), i64>, batch: &ArrowZSet) {
        if batch.is_empty() { return; }
        let v_col = batch.data.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let id_col = batch.data.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let pk_col = batch.data.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            *acc.entry((pk_col.value(i), v_col.value(i), id_col.value(i))).or_insert(0) +=
                batch.weights[i];
        }
    }

    /// Encode row values to bytes for consistent tie-breaking.
    fn encode_row_key(v: i64, id: i64, pk: i64) -> Vec<u8> {
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(&v.to_be_bytes());
        key.extend_from_slice(&id.to_be_bytes());
        key.extend_from_slice(&pk.to_be_bytes());
        key
    }

    /// Compute batch top-K reference from accumulated state.
    ///
    /// Per partition: sort by v descending, then full row bytes ascending
    /// (same tiebreaker as TopKOp::process_epoch), take K.
    fn batch_topk(state: &BTreeMap<(i64, i64, i64), i64>, k: usize) -> BTreeMap<(i64, i64, i64), i64> {
        // Group by partition key.
        let mut by_pk: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
        for (&(pk, v, id), &w) in state {
            if w <= 0 { continue; }
            by_pk.entry(pk).or_default().push((v, id));
        }

        let mut result: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
        for (pk, mut rows) in by_pk {
            // Sort by v descending, then full row bytes ascending (matches TopKOp sort).
            rows.sort_by(|a, b| {
                b.0.cmp(&a.0).then_with(|| {
                    encode_row_key(a.0, a.1, pk).cmp(&encode_row_key(b.0, b.1, pk))
                })
            });
            for (v, id) in rows.into_iter().take(k) {
                result.insert((pk, v, id), 1);
            }
        }
        result
    }

    /// Accumulate incremental output into a running top-K state map.
    fn accumulate_output(out: &mut BTreeMap<(i64, i64, i64), i64>, batch: &ArrowZSet) {
        if batch.is_empty() { return; }
        let v_col = batch.data.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let id_col = batch.data.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let pk_col = batch.data.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            *out.entry((pk_col.value(i), v_col.value(i), id_col.value(i))).or_insert(0) +=
                batch.weights[i];
        }
    }

    fn positive_entries(map: &BTreeMap<(i64, i64, i64), i64>) -> BTreeMap<(i64, i64, i64), i64> {
        map.iter().filter(|(_, &w)| w > 0).map(|(&k, &w)| (k, w)).collect()
    }

    // ─── Oracle strategy ──────────────────────────────────────────────────────

    /// Generate a sequence of delta epochs for the oracle test.
    fn delta_strategy() -> impl Strategy<Value = Vec<Vec<DeltaRow>>> {
        prop::collection::vec(
            prop::collection::vec(
                (
                    1i64..=20i64,  // rank_value
                    1i64..=30i64,  // id (unique per row in a epoch)
                    1i64..=3i64,   // partition_key
                    prop_oneof![Just(1i64), Just(-1i64)],
                )
                    .prop_map(|(v, id, pk, w)| (v, id, pk, w)),
                1..=8,
            ),
            1..=10,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 1000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn oracle_topk_incremental_matches_batch(
            epochs in delta_strategy(),
            k in prop_oneof![Just(1usize), Just(3), Just(5), Just(10)],
        ) {
            let op = TopKOp::new(schema_vpk(), k, 0, vec![2]); // rank_col=0, partition_by=[2]
            let mut input_state: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
            let mut incr_output: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

            for (epoch_idx, epoch) in epochs.iter().enumerate() {
                let batch = make_batch(epoch);
                accumulate_state(&mut input_state, &batch);

                let out = op.process_epoch(batch, epoch_idx as u64 + 1).unwrap();
                accumulate_output(&mut incr_output, &out);

                let incr_live = positive_entries(&incr_output);
                let batch_live = batch_topk(&input_state, k);

                prop_assert_eq!(
                    &incr_live,
                    &batch_live,
                    "incremental top-K != batch top-K at epoch {} with k={}",
                    epoch_idx + 1,
                    k
                );
            }
        }
    }
}

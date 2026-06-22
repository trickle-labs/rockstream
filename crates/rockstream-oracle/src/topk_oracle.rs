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
            Field::new("v", DataType::Int64, false), // rank_col = 0
            Field::new("id", DataType::Int64, false),
            Field::new("pk", DataType::Int64, false), // partition_col = 2
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
        if batch.is_empty() {
            return;
        }
        let v_col = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let id_col = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let pk_col = batch
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            *acc.entry((pk_col.value(i), v_col.value(i), id_col.value(i)))
                .or_insert(0) += batch.weights[i];
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
    fn batch_topk(
        state: &BTreeMap<(i64, i64, i64), i64>,
        k: usize,
    ) -> BTreeMap<(i64, i64, i64), i64> {
        // Group by partition key.
        let mut by_pk: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
        for (&(pk, v, id), &w) in state {
            if w <= 0 {
                continue;
            }
            by_pk.entry(pk).or_default().push((v, id));
        }

        let mut result: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
        for (pk, mut rows) in by_pk {
            // Sort by v descending, then full row bytes ascending (matches TopKOp sort).
            rows.sort_by(|a, b| {
                b.0.cmp(&a.0)
                    .then_with(|| encode_row_key(a.0, a.1, pk).cmp(&encode_row_key(b.0, b.1, pk)))
            });
            for (v, id) in rows.into_iter().take(k) {
                result.insert((pk, v, id), 1);
            }
        }
        result
    }

    /// Accumulate incremental output into a running top-K state map.
    fn accumulate_output(out: &mut BTreeMap<(i64, i64, i64), i64>, batch: &ArrowZSet) {
        if batch.is_empty() {
            return;
        }
        let v_col = batch
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let id_col = batch
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let pk_col = batch
            .data
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            *out.entry((pk_col.value(i), v_col.value(i), id_col.value(i)))
                .or_insert(0) += batch.weights[i];
        }
    }

    fn positive_entries(map: &BTreeMap<(i64, i64, i64), i64>) -> BTreeMap<(i64, i64, i64), i64> {
        map.iter()
            .filter(|(_, &w)| w > 0)
            .map(|(&k, &w)| (k, w))
            .collect()
    }

    // ─── Oracle strategy ──────────────────────────────────────────────────────

    /// Generate a sequence of delta epochs for the oracle test.
    fn delta_strategy() -> impl Strategy<Value = Vec<Vec<DeltaRow>>> {
        prop::collection::vec(
            prop::collection::vec(
                (
                    1i64..=20i64, // rank_value
                    1i64..=30i64, // id (unique per row in a epoch)
                    1i64..=3i64,  // partition_key
                    prop_oneof![Just(1i64), Just(-1i64)],
                )
                    .prop_map(|(v, id, pk, w)| (v, id, pk, w)),
                1..=8,
            ),
            1..=10,
        )
    }

    // ─── Deterministic tests with exact expected output ───────────────────────

    /// K=1 must select the single highest-value row across 3 inserted rows.
    #[test]
    fn topk_k1_exact_selects_highest_value() {
        // Insert (v=5,id=3,pk=1), (v=10,id=1,pk=1), (v=7,id=2,pk=1) — top-1 must be v=10.
        let op = TopKOp::new(schema_vpk(), 1, 0, vec![2]);
        let mut input_state: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
        let mut incr_output: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        let epoch = vec![(5i64, 3, 1, 1), (10, 1, 1, 1), (7, 2, 1, 1)];
        let b = make_batch(&epoch);
        accumulate_state(&mut input_state, &b);
        let out = op.process_epoch(b, 1).unwrap();
        accumulate_output(&mut incr_output, &out);

        let incr_live = positive_entries(&incr_output);
        let batch_live = batch_topk(&input_state, 1);

        // Expected: (pk=1, v=10, id=1)
        let mut expected = BTreeMap::new();
        expected.insert((1i64, 10i64, 1i64), 1i64);
        assert_eq!(incr_live, expected, "K=1 must select the highest value v=10");
        assert_eq!(incr_live, batch_live, "incremental must match batch reference");
    }

    /// Deleting the current top-1 element must promote the next-best row.
    ///
    /// This exercises the retraction-and-replacement path, which is the
    /// most important correctness property of TopK in an IVM context.
    #[test]
    fn topk_k1_deleted_row_is_replaced_by_next() {
        let op = TopKOp::new(schema_vpk(), 1, 0, vec![2]);
        let mut input_state: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
        let mut incr_output: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        // epoch 1: insert three rows — top-1 = (pk=1, v=10, id=1)
        let epoch1: Vec<(i64, i64, i64, i64)> = vec![(10, 1, 1, 1), (8, 2, 1, 1), (5, 3, 1, 1)];
        let b1 = make_batch(&epoch1);
        accumulate_state(&mut input_state, &b1);
        let out1 = op.process_epoch(b1, 1).unwrap();
        accumulate_output(&mut incr_output, &out1);

        let after_e1 = positive_entries(&incr_output);
        let mut exp_e1 = BTreeMap::new();
        exp_e1.insert((1i64, 10i64, 1i64), 1i64);
        assert_eq!(after_e1, exp_e1, "after epoch 1: top-1 must be (pk=1,v=10,id=1)");

        // epoch 2: retract (v=10, id=1, pk=1) — top-1 must fall back to v=8
        let epoch2: Vec<(i64, i64, i64, i64)> = vec![(10, 1, 1, -1)];
        let b2 = make_batch(&epoch2);
        accumulate_state(&mut input_state, &b2);
        let out2 = op.process_epoch(b2, 2).unwrap();
        accumulate_output(&mut incr_output, &out2);

        let after_e2 = positive_entries(&incr_output);
        let batch_e2 = batch_topk(&input_state, 1);

        let mut exp_e2 = BTreeMap::new();
        exp_e2.insert((1i64, 8i64, 2i64), 1i64); // (pk=1, v=8, id=2)
        assert_eq!(after_e2, exp_e2, "after retraction of top element: v=8 must become top-1");
        assert_eq!(after_e2, batch_e2, "incremental must equal batch after retraction");
    }

    /// K=1 applied to two partitions must independently select the top row per partition.
    #[test]
    fn topk_two_partitions_k1_exact() {
        // pk=1: (v=10,id=1), (v=5,id=2) → top-1 = (pk=1, v=10, id=1)
        // pk=2: (v=3,id=3),  (v=7,id=4) → top-1 = (pk=2, v=7,  id=4)
        let op = TopKOp::new(schema_vpk(), 1, 0, vec![2]);
        let mut input_state: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
        let mut incr_output: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        let epoch: Vec<(i64, i64, i64, i64)> = vec![(10, 1, 1, 1), (5, 2, 1, 1), (3, 3, 2, 1), (7, 4, 2, 1)];
        let b = make_batch(&epoch);
        accumulate_state(&mut input_state, &b);
        let out = op.process_epoch(b, 1).unwrap();
        accumulate_output(&mut incr_output, &out);

        let incr_live = positive_entries(&incr_output);
        let batch_live = batch_topk(&input_state, 1);

        let mut expected = BTreeMap::new();
        expected.insert((1i64, 10i64, 1i64), 1i64);
        expected.insert((2i64, 7i64, 4i64), 1i64);
        assert_eq!(incr_live, expected, "K=1 per partition: wrong winners selected");
        assert_eq!(incr_live, batch_live, "incremental must match batch reference");
    }

    /// K=2 must return the two highest-value rows and not the third.
    #[test]
    fn topk_k2_exact_excludes_third_row() {
        // 4 rows in pk=1: v ∈ {10, 8, 5, 3}; K=2 must return v=10 and v=8 only.
        let op = TopKOp::new(schema_vpk(), 2, 0, vec![2]);
        let mut input_state: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
        let mut incr_output: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        let epoch: Vec<(i64, i64, i64, i64)> = vec![(3, 4, 1, 1), (10, 1, 1, 1), (8, 2, 1, 1), (5, 3, 1, 1)];
        let b = make_batch(&epoch);
        accumulate_state(&mut input_state, &b);
        let out = op.process_epoch(b, 1).unwrap();
        accumulate_output(&mut incr_output, &out);

        let incr_live = positive_entries(&incr_output);
        let batch_live = batch_topk(&input_state, 2);

        let mut expected = BTreeMap::new();
        expected.insert((1i64, 10i64, 1i64), 1i64);
        expected.insert((1i64, 8i64, 2i64), 1i64);
        assert_eq!(incr_live, expected, "K=2 must return only the two highest-value rows");
        assert_eq!(incr_live, batch_live, "incremental must match batch reference");
    }

    /// Inserting a new row with a higher value than the current top-K must displace
    /// the previous bottom element and include the newcomer.
    #[test]
    fn topk_k2_new_insert_displaces_bottom() {
        let op = TopKOp::new(schema_vpk(), 2, 0, vec![2]);
        let mut input_state: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
        let mut incr_output: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

        // epoch 1: v=5 and v=3 → top-2 = {v=5, v=3}
        let epoch1: Vec<(i64, i64, i64, i64)> = vec![(5, 1, 1, 1), (3, 2, 1, 1)];
        let b1 = make_batch(&epoch1);
        accumulate_state(&mut input_state, &b1);
        let out1 = op.process_epoch(b1, 1).unwrap();
        accumulate_output(&mut incr_output, &out1);

        // epoch 2: insert v=9 → top-2 = {v=9, v=5}, v=3 is displaced
        let epoch2: Vec<(i64, i64, i64, i64)> = vec![(9, 3, 1, 1)];
        let b2 = make_batch(&epoch2);
        accumulate_state(&mut input_state, &b2);
        let out2 = op.process_epoch(b2, 2).unwrap();
        accumulate_output(&mut incr_output, &out2);

        let incr_live = positive_entries(&incr_output);
        let batch_live = batch_topk(&input_state, 2);

        let mut expected = BTreeMap::new();
        expected.insert((1i64, 9i64, 3i64), 1i64);
        expected.insert((1i64, 5i64, 1i64), 1i64);
        assert_eq!(incr_live, expected, "new insert v=9 must displace old bottom v=3 from top-2");
        assert_eq!(incr_live, batch_live, "incremental must match batch reference");
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 20_000,
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

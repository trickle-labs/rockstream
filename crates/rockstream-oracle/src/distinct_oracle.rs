//! Oracle property tests for Distinct / Intersect / Except (v0.10 — IVM-6).
//!
//! ## Oracle property
//!
//! `incremental(q, Δ) == batch(q, accumulated)` for every sequence of random
//! insert/delete deltas.
//!
//! ## Distinct semantics
//!
//! A row appears in the output iff its accumulated weight is positive.
//! Output weight is always +1 or -1 (never > 1).
//!
//! ## Intersect semantics
//!
//! Set (INTERSECT): row appears if present on both sides (weight ≥ 1 each).
//! Bag (INTERSECT ALL): row appears with weight = min(left_weight, right_weight).
//!
//! ## Except semantics
//!
//! Set (EXCEPT): row appears if present on left but not right.
//! Bag (EXCEPT ALL): row appears with weight = max(0, left_weight − right_weight).

#[cfg(test)]
mod proptest_oracle {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use proptest::prelude::*;
    use rockstream_ops::distinct::{DistinctOp, ExceptOp, IntersectOp};
    use rockstream_ops::op::Operator;
    use rockstream_ops::zset::ArrowZSet;

    // ─── Type aliases ─────────────────────────────────────────────────────────

    /// One row in a delta epoch: (key, value, weight).
    type DeltaRow = (i64, i64, i64);

    // ─── Helpers ──────────────────────────────────────────────────────────────

    fn schema_kv() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn make_kv_batch(rows: &[DeltaRow]) -> ArrowZSet {
        let schema = schema_kv();
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

    fn empty_kv() -> ArrowZSet {
        ArrowZSet::empty(schema_kv())
    }

    // ─── Accumulator helpers ──────────────────────────────────────────────────

    fn accumulate(acc: &mut BTreeMap<(i64, i64), i64>, batch: &ArrowZSet) {
        if batch.is_empty() {
            return;
        }
        let k_col = batch.data.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let v_col = batch.data.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..batch.num_rows() {
            let key = (k_col.value(i), v_col.value(i));
            let w = batch.weights[i];
            let entry = acc.entry(key).or_insert(0);
            *entry += w;
            if *entry == 0 {
                acc.remove(&key);
            }
        }
    }

    // ─── Batch reference implementations ──────────────────────────────────────

    fn batch_distinct(acc: &BTreeMap<(i64, i64), i64>) -> BTreeMap<(i64, i64), i64> {
        acc.iter()
            .filter(|(_, &w)| w > 0)
            .map(|(&k, _)| (k, 1i64))
            .collect()
    }

    fn batch_intersect_set(
        left: &BTreeMap<(i64, i64), i64>,
        right: &BTreeMap<(i64, i64), i64>,
    ) -> BTreeMap<(i64, i64), i64> {
        let mut out = BTreeMap::new();
        for (k, &lw) in left {
            if lw <= 0 {
                continue;
            }
            if let Some(&rw) = right.get(k) {
                if rw > 0 {
                    out.insert(*k, 1i64);
                }
            }
        }
        out
    }

    fn batch_intersect_bag(
        left: &BTreeMap<(i64, i64), i64>,
        right: &BTreeMap<(i64, i64), i64>,
    ) -> BTreeMap<(i64, i64), i64> {
        let mut out = BTreeMap::new();
        for (k, &lw) in left {
            if lw <= 0 {
                continue;
            }
            if let Some(&rw) = right.get(k) {
                let w = lw.min(rw);
                if w > 0 {
                    out.insert(*k, w);
                }
            }
        }
        out
    }

    fn batch_except_set(
        left: &BTreeMap<(i64, i64), i64>,
        right: &BTreeMap<(i64, i64), i64>,
    ) -> BTreeMap<(i64, i64), i64> {
        let mut out = BTreeMap::new();
        for (k, &lw) in left {
            if lw <= 0 {
                continue;
            }
            let rw = right.get(k).copied().unwrap_or(0).max(0);
            if rw == 0 {
                out.insert(*k, 1i64);
            }
        }
        out
    }

    fn batch_except_bag(
        left: &BTreeMap<(i64, i64), i64>,
        right: &BTreeMap<(i64, i64), i64>,
    ) -> BTreeMap<(i64, i64), i64> {
        let mut out = BTreeMap::new();
        for (k, &lw) in left {
            if lw <= 0 {
                continue;
            }
            let rw = right.get(k).copied().unwrap_or(0).max(0);
            let w = (lw - rw).max(0);
            if w > 0 {
                out.insert(*k, w);
            }
        }
        out
    }

    // ─── Strategies ──────────────────────────────────────────────────────────

    fn arb_delta_row() -> impl Strategy<Value = DeltaRow> {
        (1i64..=5, 1i64..=5, prop_oneof![Just(1i64), Just(-1i64)])
            .prop_map(|(k, v, w)| (k, v, w))
    }

    fn arb_epoch(max_rows: usize) -> impl Strategy<Value = Vec<DeltaRow>> {
        prop::collection::vec(arb_delta_row(), 0..=max_rows)
    }

    fn arb_epochs(
        n: usize,
        max_rows_per_epoch: usize,
    ) -> impl Strategy<Value = Vec<Vec<DeltaRow>>> {
        prop::collection::vec(arb_epoch(max_rows_per_epoch), 1..=n)
    }

    // ─── Property tests ───────────────────────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50_000))]

        /// Proves incremental Distinct == batch DISTINCT at every epoch boundary.
        #[test]
        fn oracle_distinct_50k(epochs in arb_epochs(8, 8)) {
            let schema = schema_kv();
            let op = DistinctOp::new(schema.clone());

            let mut input_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut output_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();

            for epoch_rows in &epochs {
                let delta = make_kv_batch(epoch_rows);
                accumulate(&mut input_acc, &delta);

                let out = op.process_delta(delta).unwrap();
                accumulate(&mut output_acc, &out);

                let expected = batch_distinct(&input_acc);
                prop_assert_eq!(
                    &output_acc, &expected,
                    "Distinct oracle mismatch: incremental={:?} batch={:?}", output_acc, expected
                );
            }
        }

        /// Proves incremental INTERSECT SET == batch INTERSECT at every epoch boundary.
        #[test]
        fn oracle_intersect_set_50k(
            left_epochs in arb_epochs(8, 6),
            right_epochs in arb_epochs(8, 6),
        ) {
            let schema = schema_kv();
            let op = IntersectOp::new(schema.clone(), false);

            let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut output_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();

            let n = left_epochs.len().max(right_epochs.len());
            for i in 0..n {
                let left_delta = left_epochs.get(i).map(|r| make_kv_batch(r)).unwrap_or_else(empty_kv);
                let right_delta = right_epochs.get(i).map(|r| make_kv_batch(r)).unwrap_or_else(empty_kv);

                accumulate(&mut left_acc, &left_delta);
                accumulate(&mut right_acc, &right_delta);

                let out = op.process_epoch(left_delta, right_delta).unwrap();
                accumulate(&mut output_acc, &out);

                let expected = batch_intersect_set(&left_acc, &right_acc);
                prop_assert_eq!(
                    &output_acc, &expected,
                    "INTERSECT SET mismatch epoch {}: incremental={:?} batch={:?}", i, output_acc, expected
                );
            }
        }

        /// Proves incremental INTERSECT ALL == batch INTERSECT ALL at every epoch boundary.
        #[test]
        fn oracle_intersect_bag_50k(
            left_epochs in arb_epochs(6, 6),
            right_epochs in arb_epochs(6, 6),
        ) {
            let schema = schema_kv();
            let op = IntersectOp::new(schema.clone(), true);

            let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut output_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();

            let n = left_epochs.len().max(right_epochs.len());
            for i in 0..n {
                let left_delta = left_epochs.get(i).map(|r| make_kv_batch(r)).unwrap_or_else(empty_kv);
                let right_delta = right_epochs.get(i).map(|r| make_kv_batch(r)).unwrap_or_else(empty_kv);

                accumulate(&mut left_acc, &left_delta);
                accumulate(&mut right_acc, &right_delta);

                let out = op.process_epoch(left_delta, right_delta).unwrap();
                accumulate(&mut output_acc, &out);

                let expected = batch_intersect_bag(&left_acc, &right_acc);
                prop_assert_eq!(
                    &output_acc, &expected,
                    "INTERSECT ALL mismatch epoch {}: incremental={:?} batch={:?}", i, output_acc, expected
                );
            }
        }

        /// Proves incremental EXCEPT SET == batch EXCEPT at every epoch boundary.
        #[test]
        fn oracle_except_set_50k(
            left_epochs in arb_epochs(8, 6),
            right_epochs in arb_epochs(8, 6),
        ) {
            let schema = schema_kv();
            let op = ExceptOp::new(schema.clone(), false);

            let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut output_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();

            let n = left_epochs.len().max(right_epochs.len());
            for i in 0..n {
                let left_delta = left_epochs.get(i).map(|r| make_kv_batch(r)).unwrap_or_else(empty_kv);
                let right_delta = right_epochs.get(i).map(|r| make_kv_batch(r)).unwrap_or_else(empty_kv);

                accumulate(&mut left_acc, &left_delta);
                accumulate(&mut right_acc, &right_delta);

                let out = op.process_epoch(left_delta, right_delta).unwrap();
                accumulate(&mut output_acc, &out);

                let expected = batch_except_set(&left_acc, &right_acc);
                prop_assert_eq!(
                    &output_acc, &expected,
                    "EXCEPT SET mismatch epoch {}: incremental={:?} batch={:?}", i, output_acc, expected
                );
            }
        }

        /// Proves incremental EXCEPT ALL == batch EXCEPT ALL at every epoch boundary.
        #[test]
        fn oracle_except_bag_50k(
            left_epochs in arb_epochs(6, 6),
            right_epochs in arb_epochs(6, 6),
        ) {
            let schema = schema_kv();
            let op = ExceptOp::new(schema.clone(), true);

            let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut output_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();

            let n = left_epochs.len().max(right_epochs.len());
            for i in 0..n {
                let left_delta = left_epochs.get(i).map(|r| make_kv_batch(r)).unwrap_or_else(empty_kv);
                let right_delta = right_epochs.get(i).map(|r| make_kv_batch(r)).unwrap_or_else(empty_kv);

                accumulate(&mut left_acc, &left_delta);
                accumulate(&mut right_acc, &right_delta);

                let out = op.process_epoch(left_delta, right_delta).unwrap();
                accumulate(&mut output_acc, &out);

                let expected = batch_except_bag(&left_acc, &right_acc);
                prop_assert_eq!(
                    &output_acc, &expected,
                    "EXCEPT ALL mismatch epoch {}: incremental={:?} batch={:?}", i, output_acc, expected
                );
            }
        }
    }
}

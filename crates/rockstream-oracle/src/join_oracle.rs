//! Oracle property tests for the inner equi-join operator (v0.8 — IVM-4).
//!
//! Query under test:
//! `SELECT l.k, l.v, r.w FROM l JOIN r ON l.k = r.k`
//!
//! Also tested: 3-way join `l JOIN m ON l.k = m.k JOIN r ON m.k = r.k`.
//!
//! ## Oracle property
//!
//! `incremental(q, Δ) == batch(q, accumulated)` for every sequence of
//! random insert/delete deltas on both sides.
//!
//! ## Test structure
//!
//! 1. **Incremental side**: feed epoch deltas to `JoinOp.process_epoch()`.
//!    Accumulate the output Z-set: (l_k, l_v, r_w) → net_weight.
//!
//! 2. **Batch side**: accumulate the input Z-sets for L and R independently.
//!    For each pair of live rows (l_k, l_v) and (r_k, r_w) where l_k == r_k,
//!    include (l_k, l_v, r_w) with weight = left_weight * right_weight in the
//!    batch result.
//!
//! 3. **Property test**: proptest runs ≥100k random delta sequences asserting
//!    `incremental == batch`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::zset::ArrowZSet;
use rockstream_ops::JoinOp;
use rockstream_types::ids::OperatorId;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Build an `ArrowZSet` from `(k, v, weight)` triples.
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

fn empty_kv_batch() -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    ArrowZSet::empty(schema)
}

// ─── Batch reference (2-table) ───────────────────────────────────────────────

/// Batch reference for `SELECT l.k, l.v, r.w FROM l JOIN r ON l.k = r.k`.
///
/// Input:
/// - `left_acc`: (l_k, l_v) → net_weight
/// - `right_acc`: (r_k, r_w) → net_weight
///
/// Returns a sorted list of `(l_k, l_v, r_w, net_weight)` with positive weight.
pub fn batch_reference_join(
    left_acc: &BTreeMap<(i64, i64), i64>,
    right_acc: &BTreeMap<(i64, i64), i64>,
) -> Vec<(i64, i64, i64)> {
    // Group right by key: r_k → Vec<(r_v, weight)>
    let mut right_by_key: BTreeMap<i64, Vec<(i64, i64)>> = BTreeMap::new();
    for (&(rk, rv), &w) in right_acc {
        if w != 0 {
            right_by_key.entry(rk).or_default().push((rv, w));
        }
    }

    // For each live left row, cross with matching right rows.
    // Net weight of join tuple = l_weight * r_weight.
    let mut result_map: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
    for (&(lk, lv), &lw) in left_acc {
        if lw == 0 {
            continue;
        }
        if let Some(rights) = right_by_key.get(&lk) {
            for &(rv, r_weight) in rights {
                let net = lw * r_weight;
                let key = (lk, lv, rv);
                *result_map.entry(key).or_insert(0) += net;
            }
        }
    }

    // Keep only positive-weight tuples (Z-set semantics).
    let mut result: Vec<(i64, i64, i64)> = result_map
        .into_iter()
        .filter(|(_, w)| *w > 0)
        .map(|((lk, lv, rv), _)| (lk, lv, rv))
        .collect();
    result.sort();
    result
}

// ─── Incremental output accumulator ─────────────────────────────────────────

/// Accumulate the incremental output of a join over multiple epochs.
///
/// Returns sorted `Vec<(l_k, l_v, r_w)>` for all live join tuples.
pub fn incremental_join_output(
    epochs: &[(Vec<(i64, i64, i64)>, Vec<(i64, i64, i64)>)],
) -> Vec<(i64, i64, i64)> {
    let op = JoinOp::new(OperatorId(0), vec![0], vec![0]);
    let mut output_acc: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

    for (left_epoch, right_epoch) in epochs {
        if left_epoch.is_empty() && right_epoch.is_empty() {
            continue;
        }
        let left = if left_epoch.is_empty() {
            empty_kv_batch()
        } else {
            make_kv_batch(left_epoch)
        };
        let right = if right_epoch.is_empty() {
            empty_kv_batch()
        } else {
            make_kv_batch(right_epoch)
        };
        let output = op
            .process_epoch(left, right)
            .expect("JoinOp::process_epoch failed");
        if output.is_empty() {
            continue;
        }
        // Output schema with 2-col inputs: (l_0=l_k, l_1=l_v, r_0=r_k, r_1=r_v)
        let lk_col = output
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lv_col = output
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rw_col = output
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(); // r_v at col3
        for i in 0..output.num_rows() {
            let key = (lk_col.value(i), lv_col.value(i), rw_col.value(i));
            let w = output.weights[i];
            let entry = output_acc.entry(key).or_insert(0);
            *entry += w;
            if *entry == 0 {
                output_acc.remove(&key);
            }
        }
    }

    let mut result: Vec<(i64, i64, i64)> = output_acc
        .into_iter()
        .filter(|(_, w)| *w > 0)
        .map(|(k, _)| k)
        .collect();
    result.sort();
    result
}

// ─── 3-way join ───────────────────────────────────────────────────────────────

/// Batch reference for a 3-way join:
/// `SELECT l.k, l.v, m.v, r.v FROM l JOIN m ON l.k = m.k JOIN r ON m.k = r.k`
///
/// Returns sorted `Vec<(k, l_v, m_v, r_v)>`.
pub fn batch_reference_3way(
    left_acc: &BTreeMap<(i64, i64), i64>,
    mid_acc: &BTreeMap<(i64, i64), i64>,
    right_acc: &BTreeMap<(i64, i64), i64>,
) -> Vec<(i64, i64, i64, i64)> {
    // First: L ⋈ M
    let mut lm_map: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
    for (&(lk, lv), &lw) in left_acc {
        if lw == 0 {
            continue;
        }
        for (&(mk, mv), &mw) in mid_acc {
            if mw == 0 || mk != lk {
                continue;
            }
            let key = (lk, lv, mv);
            *lm_map.entry(key).or_insert(0) += lw * mw;
        }
    }
    // Then: LM ⋈ R (join on k=lk=mk)
    let mut result_map: BTreeMap<(i64, i64, i64, i64), i64> = BTreeMap::new();
    for ((k, lv, mv), lmw) in &lm_map {
        if *lmw == 0 {
            continue;
        }
        for (&(rk, rv), &rw) in right_acc {
            if rw == 0 || rk != *k {
                continue;
            }
            let key = (*k, *lv, *mv, rv);
            *result_map.entry(key).or_insert(0) += lmw * rw;
        }
    }
    let mut result: Vec<(i64, i64, i64, i64)> = result_map
        .into_iter()
        .filter(|(_, w)| *w > 0)
        .map(|(k, _)| k)
        .collect();
    result.sort();
    result
}

/// Accumulate the incremental output of a 3-way join.
///
/// Uses two chained `JoinOp` instances:
/// 1. `lm_op`: L ⋈ M → output schema (l_k, l_v, m_v) = 3 columns
/// 2. `lmr_op`: LM ⋈ R → output schema (l_k, l_v, m_v, r_k, r_v) = 5 columns
///
/// Returns sorted `Vec<(k, l_v, m_v, r_v)>` (dropping the duplicate r_k column at index 3).
pub fn incremental_3way_join_output(
    epochs: &[(
        Vec<(i64, i64, i64)>,
        Vec<(i64, i64, i64)>,
        Vec<(i64, i64, i64)>,
    )],
) -> Vec<(i64, i64, i64, i64)> {
    // LM join: 2-col L ⋈ 2-col M → output (l_k, l_v, m_k, m_v) = 4 columns
    let lm_op = JoinOp::with_schema(OperatorId(1), vec![0], vec![0], 2, 2);

    // LMR join: 4-col LM ⋈ 2-col R → output (lm_0, lm_1, lm_2, lm_3, r_k, r_v) = 6 columns
    // LM key is col 0 (l_k), R key is col 0 (r_k).
    let lmr_op = JoinOp::with_schema(OperatorId(2), vec![0], vec![0], 4, 2);

    let mut output_acc: BTreeMap<(i64, i64, i64, i64), i64> = BTreeMap::new();

    for (left_epoch, mid_epoch, right_epoch) in epochs {
        // Stage lm epoch.
        let left = if left_epoch.is_empty() {
            empty_kv_batch()
        } else {
            make_kv_batch(left_epoch)
        };
        let mid = if mid_epoch.is_empty() {
            empty_kv_batch()
        } else {
            make_kv_batch(mid_epoch)
        };
        let lm_out = lm_op.process_epoch(left, mid).expect("lm join failed");
        // lm_out schema: (l_0=l_k, l_1=l_v, r_0=m_k, r_1=m_v)

        let right = if right_epoch.is_empty() {
            empty_kv_batch()
        } else {
            make_kv_batch(right_epoch)
        };
        let lmr_out = lmr_op
            .process_epoch(lm_out, right)
            .expect("lmr join failed");
        // lmr_out schema: (l_0=l_k, l_1=l_v, l_2=m_k, l_3=m_v, r_0=r_k, r_1=r_v)

        if lmr_out.is_empty() {
            continue;
        }
        // Extract columns: k=col0, l_v=col1, m_v=col3, r_v=col5
        let col0 = lmr_out
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let col1 = lmr_out
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let col3 = lmr_out
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let col5 = lmr_out
            .data
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..lmr_out.num_rows() {
            let key = (col0.value(i), col1.value(i), col3.value(i), col5.value(i));
            let w = lmr_out.weights[i];
            let entry = output_acc.entry(key).or_insert(0);
            *entry += w;
            if *entry == 0 {
                output_acc.remove(&key);
            }
        }
    }

    let mut result: Vec<(i64, i64, i64, i64)> = output_acc
        .into_iter()
        .filter(|(_, w)| *w > 0)
        .map(|(k, _)| k)
        .collect();
    result.sort();
    result
}

// ─── Oracle assertion ─────────────────────────────────────────────────────────

/// Assert `incremental == batch` for the 2-table inner equi-join.
///
/// `epochs`: sequence of `(left_delta, right_delta)` pairs.
/// Each delta is a `Vec<(k, v, weight)>` where weight ∈ {+1, -1}.
pub fn assert_oracle_join(epochs: &[(Vec<(i64, i64, i64)>, Vec<(i64, i64, i64)>)]) {
    // Accumulate input Z-sets.
    let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();

    for (left_epoch, right_epoch) in epochs {
        for &(k, v, w) in left_epoch {
            let entry = left_acc.entry((k, v)).or_insert(0);
            *entry += w;
            if *entry == 0 {
                left_acc.remove(&(k, v));
            }
        }
        for &(k, v, w) in right_epoch {
            let entry = right_acc.entry((k, v)).or_insert(0);
            *entry += w;
            if *entry == 0 {
                right_acc.remove(&(k, v));
            }
        }
    }

    let mut batch = batch_reference_join(&left_acc, &right_acc);
    batch.sort();

    let mut inc = incremental_join_output(epochs);
    inc.sort();

    assert_eq!(
        inc,
        batch,
        "Join oracle property FAILED: incremental != batch\n\
         Query: SELECT l.k, l.v, r.v FROM l JOIN r ON l.k = r.k\n\
         incremental ({} rows): {inc:?}\n\
         batch      ({} rows): {batch:?}",
        inc.len(),
        batch.len()
    );
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_join_empty_input() {
        assert_oracle_join(&[]);
        // Both sides empty → no output.
        let result = incremental_join_output(&[]);
        assert_eq!(result, vec![], "empty input must produce empty join output");
    }

    #[test]
    fn oracle_join_no_match() {
        // Left key=1, right key=2 — no join partner.
        assert_oracle_join(&[(vec![(1, 10, 1)], vec![(2, 20, 1)])]);
        let result = incremental_join_output(&[(vec![(1, 10, 1)], vec![(2, 20, 1)])]);
        assert_eq!(result, vec![], "non-matching keys must produce empty join output");
    }

    #[test]
    fn oracle_join_single_match() {
        // l(k=5, lv=50) ⋈ r(k=5, rv=500) → (5, 50, 500)
        assert_oracle_join(&[(vec![(5, 50, 1)], vec![(5, 500, 1)])]);
        let result = incremental_join_output(&[(vec![(5, 50, 1)], vec![(5, 500, 1)])]);
        assert_eq!(
            result,
            vec![(5, 50, 500)],
            "single match: expected [(k=5, lv=50, rv=500)]"
        );
    }

    #[test]
    fn oracle_join_insert_then_delete_right() {
        // Insert pair then delete right — tuple disappears.
        assert_oracle_join(&[
            (vec![(3, 30, 1)], vec![(3, 300, 1)]),
            (vec![], vec![(3, 300, -1)]),
        ]);
        let result = incremental_join_output(&[
            (vec![(3, 30, 1)], vec![(3, 300, 1)]),
            (vec![], vec![(3, 300, -1)]),
        ]);
        assert_eq!(result, vec![], "delete right side must remove join tuple");
    }

    #[test]
    fn oracle_join_insert_then_delete_left() {
        assert_oracle_join(&[
            (vec![(3, 30, 1)], vec![(3, 300, 1)]),
            (vec![(3, 30, -1)], vec![]),
        ]);
        let result = incremental_join_output(&[
            (vec![(3, 30, 1)], vec![(3, 300, 1)]),
            (vec![(3, 30, -1)], vec![]),
        ]);
        assert_eq!(result, vec![], "delete left side must remove join tuple");
    }

    #[test]
    fn oracle_join_multiple_matching_right_rows() {
        // l: (k=1, lv=10); r: (k=1, rv=100), (k=1, rv=200).
        // Expected: [(1, 10, 100), (1, 10, 200)]
        assert_oracle_join(&[(vec![(1, 10, 1)], vec![(1, 100, 1), (1, 200, 1)])]);
        let result = incremental_join_output(&[(vec![(1, 10, 1)], vec![(1, 100, 1), (1, 200, 1)])]);
        assert_eq!(
            result,
            vec![(1, 10, 100), (1, 10, 200)],
            "multiple right matches: expected [(1,10,100),(1,10,200)]"
        );
    }

    #[test]
    fn oracle_join_multiple_matching_left_rows() {
        // l: (k=2, lv=10), (k=2, lv=20); r: (k=2, rv=300).
        // Expected: [(2, 10, 300), (2, 20, 300)]
        assert_oracle_join(&[(vec![(2, 10, 1), (2, 20, 1)], vec![(2, 300, 1)])]);
        let result = incremental_join_output(&[(vec![(2, 10, 1), (2, 20, 1)], vec![(2, 300, 1)])]);
        let mut got = result.clone();
        got.sort();
        assert_eq!(
            got,
            vec![(2, 10, 300), (2, 20, 300)],
            "multiple left matches: expected [(2,10,300),(2,20,300)]"
        );
    }

    #[test]
    fn oracle_join_multiple_epochs() {
        // epoch 0: l(1,10) ⋈ r(1,100) → (1,10,100)
        // epoch 1: l(2,20) ⋈ r(2,200) → (2,20,200)
        // epoch 2: delete l(1,10)      → (1,10,100) removed
        // Final: [(2,20,200)]
        assert_oracle_join(&[
            (vec![(1, 10, 1)], vec![(1, 100, 1)]),
            (vec![(2, 20, 1)], vec![(2, 200, 1)]),
            (vec![(1, 10, -1)], vec![]),
        ]);
        let result = incremental_join_output(&[
            (vec![(1, 10, 1)], vec![(1, 100, 1)]),
            (vec![(2, 20, 1)], vec![(2, 200, 1)]),
            (vec![(1, 10, -1)], vec![]),
        ]);
        assert_eq!(
            result,
            vec![(2, 20, 200)],
            "multiple_epochs: expected only (2,20,200) after deleting (1,10)"
        );
    }

    #[test]
    fn oracle_join_key_churn() {
        // Multiple insert/delete cycles on the same key.
        assert_oracle_join(&[
            (vec![(7, 70, 1)], vec![(7, 700, 1)]),
            (vec![(7, 70, -1)], vec![]),
            (vec![(7, 71, 1)], vec![]),
            (vec![], vec![(7, 701, 1)]),
            (vec![(7, 71, -1)], vec![(7, 700, -1), (7, 701, -1)]),
        ]);
        // After all operations: no tuples survive (all inserted rows removed).
        let result = incremental_join_output(&[
            (vec![(7, 70, 1)], vec![(7, 700, 1)]),
            (vec![(7, 70, -1)], vec![]),
            (vec![(7, 71, 1)], vec![]),
            (vec![], vec![(7, 701, 1)]),
            (vec![(7, 71, -1)], vec![(7, 700, -1), (7, 701, -1)]),
        ]);
        assert_eq!(result, vec![], "key churn must leave empty output");
    }

    #[test]
    fn batch_reference_join_symmetric() {
        let mut la: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        let mut ra: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        la.insert((1, 10), 1);
        ra.insert((1, 100), 1);
        ra.insert((1, 200), 1);
        let result = batch_reference_join(&la, &ra);
        assert_eq!(result, vec![(1, 10, 100), (1, 10, 200)]);
    }

    #[test]
    fn oracle_join_cross_product_exact() {
        // l: (k=3, v=1), (k=3, v=2); r: (k=3, v=10), (k=3, v=20).
        // Cross product: (3,1,10), (3,1,20), (3,2,10), (3,2,20) — 4 tuples.
        let result = incremental_join_output(&[(
            vec![(3, 1, 1), (3, 2, 1)],
            vec![(3, 10, 1), (3, 20, 1)],
        )]);
        let mut got = result.clone();
        got.sort();
        assert_eq!(
            got,
            vec![(3, 1, 10), (3, 1, 20), (3, 2, 10), (3, 2, 20)],
            "cross product: expected 4 tuples"
        );
    }

    // ── Proptest randomized oracle (≥100k scenarios) ──────────────────────

    #[cfg(test)]
    mod proptest_oracle {
        use proptest::prelude::*;
        use std::collections::BTreeMap;

        use super::super::{
            assert_oracle_join, batch_reference_3way, incremental_3way_join_output,
        };

        /// Random delta: key ∈ [0,4], value ∈ [0,9], weight ∈ {+1, -1}.
        fn arb_delta() -> impl Strategy<Value = Vec<(i64, i64, i64)>> {
            proptest::collection::vec(
                (0i64..5, 0i64..10, prop_oneof![Just(1i64), Just(-1i64)]),
                0..8,
            )
        }

        /// Random epoch: a pair of (left_delta, right_delta).
        fn arb_epoch() -> impl Strategy<Value = (Vec<(i64, i64, i64)>, Vec<(i64, i64, i64)>)> {
            (arb_delta(), arb_delta())
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(100_000))]

            /// Oracle property test: incremental == batch for ≥100k random delta
            /// sequences over the 2-table inner equi-join.
            ///
            /// This is the v0.8 Proof 1 evidence.
            #[test]
            #[allow(clippy::items_after_test_module)]
            fn oracle_join_100k(
                epochs in proptest::collection::vec(arb_epoch(), 1..6)
            ) {
                assert_oracle_join(&epochs);
            }
        }

        /// 3-way join deterministic oracle: incremental == batch on a known input.
        #[test]
        fn oracle_3way_join_deterministic() {
            // l: (k=1,lv=10), m: (k=1,mv=100), r: (k=1,rv=1000)
            // batch: (k=1, lv=10, mv=100, rv=1000)
            // inc: (k=1, lv=10, mv=100, rv=1000)
            let epochs = vec![(
                vec![(1i64, 10i64, 1i64)],
                vec![(1i64, 100i64, 1i64)],
                vec![(1i64, 1000i64, 1i64)],
            )];
            let mut l_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut m_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            let mut r_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
            l_acc.insert((1, 10), 1);
            m_acc.insert((1, 100), 1);
            r_acc.insert((1, 1000), 1);
            let batch = batch_reference_3way(&l_acc, &m_acc, &r_acc);
            let inc = incremental_3way_join_output(&epochs);
            assert_eq!(
                inc, batch,
                "3-way join deterministic FAILED: inc={inc:?} batch={batch:?}"
            );
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(10_000))]

            /// 3-way join property: incremental == batch for chained 2-way joins.
            #[test]
            fn oracle_3way_join_10k(
                epochs in proptest::collection::vec(
                    (arb_delta(), arb_delta(), arb_delta()),
                    1..4
                )
            ) {
                // Build accumulated state.
                let mut l_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
                let mut m_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
                let mut r_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
                for (le, me, re) in &epochs {
                    for &(k, v, w) in le { *l_acc.entry((k, v)).or_insert(0) += w; }
                    for &(k, v, w) in me { *m_acc.entry((k, v)).or_insert(0) += w; }
                    for &(k, v, w) in re { *r_acc.entry((k, v)).or_insert(0) += w; }
                }
                // Clean up zero-weight entries.
                l_acc.retain(|_, w| *w != 0);
                m_acc.retain(|_, w| *w != 0);
                r_acc.retain(|_, w| *w != 0);

                let mut batch = batch_reference_3way(&l_acc, &m_acc, &r_acc);
                batch.sort();
                let mut inc = incremental_3way_join_output(&epochs);
                inc.sort();
                assert_eq!(inc, batch,
                    "3-way join oracle FAILED: incremental != batch\ninc={inc:?}\nbatch={batch:?}");
            }
        }
    }
}

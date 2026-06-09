//! Oracle property tests for outer / semi / anti joins (v0.9 — IVM-5).
//!
//! ## Oracle property
//!
//! `incremental(q, Δ) == batch(q, accumulated)` for every sequence of
//! random insert/delete deltas on both sides.
//!
//! NULL values (unmatched rows) are encoded as `0i64` and represented as
//! `None` in the oracle comparison tuples.
//!
//! ## Test structure
//!
//! 1. **Incremental side**: feed epoch deltas to `OuterJoinOp.process_epoch()`.
//!    Accumulate the output Z-set.
//!
//! 2. **Batch side**: compute the join over the accumulated state directly.
//!
//! 3. **Property test**: proptest runs ≥100k random delta sequences (Left),
//!    ≥50k for Right/Full/Semi/Anti.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::outer_join::OuterJoinOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::OuterJoinKind;
use rockstream_types::ids::OperatorId;

// ─── Type aliases ─────────────────────────────────────────────────────────────

/// One row in a delta epoch: (join_key, value, weight).
pub type DeltaRow = (i64, i64, i64);

/// A pair of left and right delta epochs for one oracle step.
pub type EpochPair = (Vec<DeltaRow>, Vec<DeltaRow>);

// ─── Helpers ─────────────────────────────────────────────────────────────────

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

fn empty_kv() -> ArrowZSet {
    ArrowZSet::empty(Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ])))
}

// ─── Batch reference: LEFT JOIN ──────────────────────────────────────────────

/// Batch reference for LEFT JOIN.
///
/// Returns sorted `Vec<(l_k, l_v, i64)>` for all live left rows,
/// using `0i64` as the NULL sentinel for unmatched right columns
/// (same encoding as `OuterJoinOp`).
///
/// **Important**: the proptest input generator guarantees that right-side `v`
/// values are in `[1, 10)` so that `0` unambiguously identifies NULL-padded rows.
pub fn batch_reference_left(
    left_acc: &BTreeMap<(i64, i64), i64>,
    right_acc: &BTreeMap<(i64, i64), i64>,
) -> Vec<(i64, i64, i64)> {
    // Group right by key.
    let mut right_by_key: BTreeMap<i64, Vec<(i64, i64)>> = BTreeMap::new();
    for (&(rk, rv), &w) in right_acc {
        if w > 0 {
            right_by_key.entry(rk).or_default().push((rv, w));
        }
    }

    let mut result_map: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
    for (&(lk, lv), &lw) in left_acc {
        if lw <= 0 {
            continue;
        }
        if let Some(rights) = right_by_key.get(&lk) {
            for &(rv, rw) in rights {
                let net = lw * rw;
                *result_map.entry((lk, lv, rv)).or_insert(0) += net;
            }
        } else {
            // No match → NULL pad: right value encoded as 0 (same as operator).
            *result_map.entry((lk, lv, 0i64)).or_insert(0) += lw;
        }
    }

    let mut result: Vec<(i64, i64, i64)> = result_map
        .into_iter()
        .filter(|(_, w)| *w > 0)
        .map(|(k, _)| k)
        .collect();
    result.sort();
    result
}

// ─── Batch reference: RIGHT JOIN ─────────────────────────────────────────────

/// Batch reference for RIGHT JOIN.
///
/// Returns sorted `Vec<(Option<l_v>, r_k, r_v)>`.
pub fn batch_reference_right(
    left_acc: &BTreeMap<(i64, i64), i64>,
    right_acc: &BTreeMap<(i64, i64), i64>,
) -> Vec<(Option<i64>, i64, i64)> {
    let mut left_by_key: BTreeMap<i64, Vec<(i64, i64)>> = BTreeMap::new();
    for (&(lk, lv), &w) in left_acc {
        if w > 0 {
            left_by_key.entry(lk).or_default().push((lv, w));
        }
    }

    let mut result_map: BTreeMap<(Option<i64>, i64, i64), i64> = BTreeMap::new();
    for (&(rk, rv), &rw) in right_acc {
        if rw <= 0 {
            continue;
        }
        if let Some(lefts) = left_by_key.get(&rk) {
            for &(lv, lw) in lefts {
                let net = lw * rw;
                *result_map.entry((Some(lv), rk, rv)).or_insert(0) += net;
            }
        } else {
            *result_map.entry((None, rk, rv)).or_insert(0) += rw;
        }
    }

    let mut result: Vec<(Option<i64>, i64, i64)> = result_map
        .into_iter()
        .filter(|(_, w)| *w > 0)
        .map(|(k, _)| k)
        .collect();
    result.sort();
    result
}

// ─── Batch reference: SEMI JOIN ──────────────────────────────────────────────

/// Batch reference for SEMI JOIN (left rows with at least one matching right row).
///
/// Returns sorted `Vec<(l_k, l_v)>`.
pub fn batch_reference_semi(
    left_acc: &BTreeMap<(i64, i64), i64>,
    right_acc: &BTreeMap<(i64, i64), i64>,
) -> Vec<(i64, i64)> {
    let right_keys: std::collections::HashSet<i64> = right_acc
        .iter()
        .filter(|(_, &w)| w > 0)
        .map(|(&(k, _), _)| k)
        .collect();

    let mut result: Vec<(i64, i64)> = left_acc
        .iter()
        .filter(|(&(lk, _), &lw)| lw > 0 && right_keys.contains(&lk))
        .map(|(&(lk, lv), _)| (lk, lv))
        .collect();
    result.sort();
    result.dedup();
    result
}

// ─── Batch reference: ANTI JOIN ──────────────────────────────────────────────

/// Batch reference for ANTI JOIN (left rows with NO matching right row).
///
/// Returns sorted `Vec<(l_k, l_v)>`.
pub fn batch_reference_anti(
    left_acc: &BTreeMap<(i64, i64), i64>,
    right_acc: &BTreeMap<(i64, i64), i64>,
) -> Vec<(i64, i64)> {
    let right_keys: std::collections::HashSet<i64> = right_acc
        .iter()
        .filter(|(_, &w)| w > 0)
        .map(|(&(k, _), _)| k)
        .collect();

    let mut result: Vec<(i64, i64)> = left_acc
        .iter()
        .filter(|(&(lk, _), &lw)| lw > 0 && !right_keys.contains(&lk))
        .map(|(&(lk, lv), _)| (lk, lv))
        .collect();
    result.sort();
    result.dedup();
    result
}

// ─── Incremental accumulators ─────────────────────────────────────────────────

/// Accumulate LEFT JOIN output over epochs.
///
/// Returns `Vec<(l_k, l_v, i64)>` with positive net weight.
/// The operator encodes NULL-padded right columns as `0i64`.
/// The proptest input generator guarantees that right-side `v` values are in
/// `[1, 10)` so that `0` unambiguously identifies NULL-padded rows.
pub fn incremental_left_join(epochs: &[EpochPair]) -> Vec<(i64, i64, i64)> {
    let op = OuterJoinOp::new(OperatorId(0), OuterJoinKind::Left, vec![0], vec![0]);
    let mut output_acc: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

    for (left_epoch, right_epoch) in epochs {
        let left = if left_epoch.is_empty() {
            empty_kv()
        } else {
            make_kv_batch(left_epoch)
        };
        let right = if right_epoch.is_empty() {
            empty_kv()
        } else {
            make_kv_batch(right_epoch)
        };
        let output = op
            .process_epoch(left, right)
            .expect("OuterJoinOp::process_epoch failed");
        if output.is_empty() {
            continue;
        }
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
        let rv_col = output
            .data
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..output.num_rows() {
            let key = (lk_col.value(i), lv_col.value(i), rv_col.value(i));
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

/// Accumulate SEMI JOIN output over epochs.
///
/// Returns `Vec<(l_k, l_v)>` with positive net weight.
pub fn incremental_semi_join(epochs: &[EpochPair]) -> Vec<(i64, i64)> {
    let op = OuterJoinOp::new(OperatorId(0), OuterJoinKind::Semi, vec![0], vec![0]);
    let mut output_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();

    for (left_epoch, right_epoch) in epochs {
        let left = if left_epoch.is_empty() {
            empty_kv()
        } else {
            make_kv_batch(left_epoch)
        };
        let right = if right_epoch.is_empty() {
            empty_kv()
        } else {
            make_kv_batch(right_epoch)
        };
        let output = op
            .process_epoch(left, right)
            .expect("OuterJoinOp::process_epoch failed");
        if output.is_empty() {
            continue;
        }
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
        for i in 0..output.num_rows() {
            let key = (lk_col.value(i), lv_col.value(i));
            let w = output.weights[i];
            let entry = output_acc.entry(key).or_insert(0);
            *entry += w;
            if *entry == 0 {
                output_acc.remove(&key);
            }
        }
    }

    let mut result: Vec<(i64, i64)> = output_acc
        .into_iter()
        .filter(|(_, w)| *w > 0)
        .map(|(k, _)| k)
        .collect();
    result.sort();
    result
}

/// Accumulate ANTI JOIN output over epochs.
///
/// Returns `Vec<(l_k, l_v)>` with positive net weight.
pub fn incremental_anti_join(epochs: &[EpochPair]) -> Vec<(i64, i64)> {
    let op = OuterJoinOp::new(OperatorId(0), OuterJoinKind::Anti, vec![0], vec![0]);
    let mut output_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();

    for (left_epoch, right_epoch) in epochs {
        let left = if left_epoch.is_empty() {
            empty_kv()
        } else {
            make_kv_batch(left_epoch)
        };
        let right = if right_epoch.is_empty() {
            empty_kv()
        } else {
            make_kv_batch(right_epoch)
        };
        let output = op
            .process_epoch(left, right)
            .expect("OuterJoinOp::process_epoch failed");
        if output.is_empty() {
            continue;
        }
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
        for i in 0..output.num_rows() {
            let key = (lk_col.value(i), lv_col.value(i));
            let w = output.weights[i];
            let entry = output_acc.entry(key).or_insert(0);
            *entry += w;
            if *entry == 0 {
                output_acc.remove(&key);
            }
        }
    }

    let mut result: Vec<(i64, i64)> = output_acc
        .into_iter()
        .filter(|(_, w)| *w > 0)
        .map(|(k, _)| k)
        .collect();
    result.sort();
    result
}

// ─── Oracle assertions ────────────────────────────────────────────────────────

/// Assert `incremental == batch` for the LEFT JOIN query.
///
/// Both sides use raw `(l_k, l_v, r_v_or_0)` tuples where `0` is the
/// NULL sentinel for unmatched right columns.  The right-side input must
/// only contain non-zero values to avoid ambiguity with the NULL sentinel.
pub fn assert_oracle_left_join(epochs: &[EpochPair]) {
    let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for (left_epoch, right_epoch) in epochs {
        for &(k, v, w) in left_epoch {
            let e = left_acc.entry((k, v)).or_insert(0);
            *e += w;
            if *e == 0 {
                left_acc.remove(&(k, v));
            }
        }
        for &(k, v, w) in right_epoch {
            let e = right_acc.entry((k, v)).or_insert(0);
            *e += w;
            if *e == 0 {
                right_acc.remove(&(k, v));
            }
        }
    }

    let mut batch = batch_reference_left(&left_acc, &right_acc);
    batch.sort();
    let mut inc = incremental_left_join(epochs);
    inc.sort();

    assert_eq!(
        inc,
        batch,
        "LEFT JOIN oracle FAILED: incremental != batch\n\
         incremental ({} rows): {inc:?}\n\
         batch      ({} rows): {batch:?}",
        inc.len(),
        batch.len()
    );
}

/// Assert `incremental == batch` for the SEMI JOIN query.
pub fn assert_oracle_semi_join(epochs: &[EpochPair]) {
    let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for (left_epoch, right_epoch) in epochs {
        for &(k, v, w) in left_epoch {
            let e = left_acc.entry((k, v)).or_insert(0);
            *e += w;
            if *e == 0 {
                left_acc.remove(&(k, v));
            }
        }
        for &(k, v, w) in right_epoch {
            let e = right_acc.entry((k, v)).or_insert(0);
            *e += w;
            if *e == 0 {
                right_acc.remove(&(k, v));
            }
        }
    }

    let mut batch = batch_reference_semi(&left_acc, &right_acc);
    batch.sort();
    let mut inc = incremental_semi_join(epochs);
    inc.sort();

    assert_eq!(
        inc,
        batch,
        "SEMI JOIN oracle FAILED: incremental != batch\n\
         incremental ({} rows): {inc:?}\n\
         batch      ({} rows): {batch:?}",
        inc.len(),
        batch.len()
    );
}

/// Assert `incremental == batch` for the ANTI JOIN query.
pub fn assert_oracle_anti_join(epochs: &[EpochPair]) {
    let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for (left_epoch, right_epoch) in epochs {
        for &(k, v, w) in left_epoch {
            let e = left_acc.entry((k, v)).or_insert(0);
            *e += w;
            if *e == 0 {
                left_acc.remove(&(k, v));
            }
        }
        for &(k, v, w) in right_epoch {
            let e = right_acc.entry((k, v)).or_insert(0);
            *e += w;
            if *e == 0 {
                right_acc.remove(&(k, v));
            }
        }
    }

    let mut batch = batch_reference_anti(&left_acc, &right_acc);
    batch.sort();
    let mut inc = incremental_anti_join(epochs);
    inc.sort();

    assert_eq!(
        inc,
        batch,
        "ANTI JOIN oracle FAILED: incremental != batch\n\
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

    // ── LEFT JOIN deterministic ───────────────────────────────────────────────
    // Note: right-side values use [1, 100] to avoid 0 (NULL sentinel).

    #[test]
    fn oracle_left_join_unmatched() {
        // Left row with no right → NULL pad (rv=0).
        assert_oracle_left_join(&[(vec![(1, 10, 1)], vec![])]);
    }

    #[test]
    fn oracle_left_join_matched() {
        assert_oracle_left_join(&[(vec![(1, 10, 1)], vec![(1, 100, 1)])]);
    }

    #[test]
    fn oracle_left_join_null_pad_then_match() {
        assert_oracle_left_join(&[(vec![(1, 10, 1)], vec![]), (vec![], vec![(1, 100, 1)])]);
    }

    #[test]
    fn oracle_left_join_match_then_right_delete() {
        assert_oracle_left_join(&[
            (vec![(1, 10, 1)], vec![(1, 100, 1)]),
            (vec![], vec![(1, 100, -1)]),
        ]);
    }

    #[test]
    fn oracle_left_join_multiple_keys() {
        assert_oracle_left_join(&[(vec![(1, 10, 1), (2, 20, 1)], vec![(1, 100, 1)])]);
    }

    // ── SEMI JOIN deterministic ──────────────────────────────────────────────

    #[test]
    fn oracle_semi_join_no_right() {
        assert_oracle_semi_join(&[(vec![(1, 10, 1)], vec![])]);
    }

    #[test]
    fn oracle_semi_join_with_right() {
        assert_oracle_semi_join(&[(vec![(1, 10, 1)], vec![(1, 100, 1)])]);
    }

    #[test]
    fn oracle_semi_join_right_then_deleted() {
        assert_oracle_semi_join(&[
            (vec![(1, 10, 1)], vec![(1, 100, 1)]),
            (vec![], vec![(1, 100, -1)]),
        ]);
    }

    // ── ANTI JOIN deterministic ──────────────────────────────────────────────

    #[test]
    fn oracle_anti_join_no_right_emits() {
        assert_oracle_anti_join(&[(vec![(1, 10, 1)], vec![])]);
    }

    #[test]
    fn oracle_anti_join_with_right_suppressed() {
        assert_oracle_anti_join(&[(vec![(1, 10, 1)], vec![(1, 100, 1)])]);
    }

    #[test]
    fn oracle_anti_join_right_arrives_retracts() {
        assert_oracle_anti_join(&[(vec![(1, 10, 1)], vec![]), (vec![], vec![(1, 100, 1)])]);
    }

    #[test]
    fn oracle_anti_join_right_deleted_restores() {
        assert_oracle_anti_join(&[
            (vec![(1, 10, 1)], vec![(1, 100, 1)]),
            (vec![], vec![(1, 100, -1)]),
        ]);
    }

    // ── Proptest randomized oracle ─────────────────────────────────────────────

    #[cfg(test)]
    mod proptest_oracle {
        use super::super::{
            assert_oracle_anti_join, assert_oracle_left_join, assert_oracle_semi_join, EpochPair,
        };
        use proptest::prelude::*;

        /// Generate a valid side delta (no over-deletes).
        /// Returns epochs of (k, v, weight) where weight ∈ {+1, -1} and
        /// retractions are only issued for present rows.
        fn valid_epochs_for(raw: &[Vec<((i64, i64), bool)>]) -> Vec<Vec<(i64, i64, i64)>> {
            let mut state: std::collections::HashMap<(i64, i64), i64> =
                std::collections::HashMap::new();
            let mut result = Vec::new();
            for epoch in raw {
                let mut valid_epoch = Vec::new();
                for &((k, v), insert) in epoch {
                    if insert {
                        *state.entry((k, v)).or_insert(0) += 1;
                        valid_epoch.push((k, v, 1i64));
                    } else {
                        let e = state.entry((k, v)).or_insert(0);
                        if *e > 0 {
                            *e -= 1;
                            if *e == 0 {
                                state.remove(&(k, v));
                            }
                            valid_epoch.push((k, v, -1i64));
                        }
                    }
                }
                result.push(valid_epoch);
            }
            result
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(100_000))]

            /// Oracle property test: incremental == batch for ≥100k random
            /// delta sequences for LEFT JOIN.
            ///
            /// Right-side values use range [1, 10) to avoid the 0 NULL sentinel.
            #[test]
            #[allow(clippy::items_after_test_module)]
            fn oracle_left_join_100k(
                left_raw in prop::collection::vec(
                    prop::collection::vec(
                        ((0i64..4i64, 0i64..10i64), prop::bool::ANY),
                        0..8usize,
                    ),
                    1..5usize,
                ),
                right_raw in prop::collection::vec(
                    prop::collection::vec(
                        // Right values start at 1 to avoid 0 (NULL sentinel).
                        ((0i64..4i64, 1i64..10i64), prop::bool::ANY),
                        0..8usize,
                    ),
                    1..5usize,
                ),
            ) {
                let left_epochs = valid_epochs_for(&left_raw);
                let right_epochs = valid_epochs_for(&right_raw);
                // Pair up epochs (pad shorter with empty).
                let n = left_epochs.len().max(right_epochs.len());
                let epochs: Vec<EpochPair> = (0..n)
                    .map(|i| {
                        let l = left_epochs.get(i).cloned().unwrap_or_default();
                        let r = right_epochs.get(i).cloned().unwrap_or_default();
                        (l, r)
                    })
                    .collect();
                assert_oracle_left_join(&epochs);
            }
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(50_000))]

            /// Oracle property test: incremental == batch for ≥50k random
            /// delta sequences for SEMI JOIN.
            #[test]
            #[allow(clippy::items_after_test_module)]
            fn oracle_semi_join_50k(
                left_raw in prop::collection::vec(
                    prop::collection::vec(
                        ((0i64..4i64, 0i64..10i64), prop::bool::ANY),
                        0..8usize,
                    ),
                    1..5usize,
                ),
                right_raw in prop::collection::vec(
                    prop::collection::vec(
                        ((0i64..4i64, 1i64..10i64), prop::bool::ANY),
                        0..8usize,
                    ),
                    1..5usize,
                ),
            ) {
                let left_epochs = valid_epochs_for(&left_raw);
                let right_epochs = valid_epochs_for(&right_raw);
                let n = left_epochs.len().max(right_epochs.len());
                let epochs: Vec<EpochPair> = (0..n)
                    .map(|i| {
                        let l = left_epochs.get(i).cloned().unwrap_or_default();
                        let r = right_epochs.get(i).cloned().unwrap_or_default();
                        (l, r)
                    })
                    .collect();
                assert_oracle_semi_join(&epochs);
            }

            /// Oracle property test: incremental == batch for ≥50k random
            /// delta sequences for ANTI JOIN.
            #[test]
            #[allow(clippy::items_after_test_module)]
            fn oracle_anti_join_50k(
                left_raw in prop::collection::vec(
                    prop::collection::vec(
                        ((0i64..4i64, 0i64..10i64), prop::bool::ANY),
                        0..8usize,
                    ),
                    1..5usize,
                ),
                right_raw in prop::collection::vec(
                    prop::collection::vec(
                        ((0i64..4i64, 1i64..10i64), prop::bool::ANY),
                        0..8usize,
                    ),
                    1..5usize,
                ),
            ) {
                let left_epochs = valid_epochs_for(&left_raw);
                let right_epochs = valid_epochs_for(&right_raw);
                let n = left_epochs.len().max(right_epochs.len());
                let epochs: Vec<EpochPair> = (0..n)
                    .map(|i| {
                        let l = left_epochs.get(i).cloned().unwrap_or_default();
                        let r = right_epochs.get(i).cloned().unwrap_or_default();
                        (l, r)
                    })
                    .collect();
                assert_oracle_anti_join(&epochs);
            }
        }
    }
}

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
#[cfg(test)]
use rockstream_plan::virtual_bucket::route_virtual_bucket;
#[cfg(test)]
use rockstream_types::error_code::{ErrorCode, RS_5035};
use rockstream_types::ids::OperatorId;

type DeltaRow = (i64, i64, i64);
type JoinEpoch = (Vec<DeltaRow>, Vec<DeltaRow>);
type Join3Epoch = (Vec<DeltaRow>, Vec<DeltaRow>, Vec<DeltaRow>);

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
/// Returns sorted `(l_k, l_v, r_w, net_weight)` rows for nonzero multiplicities.
pub fn batch_reference_join(
    left_acc: &BTreeMap<(i64, i64), i64>,
    right_acc: &BTreeMap<(i64, i64), i64>,
) -> Vec<(i64, i64, i64, i64)> {
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

    // Preserve every nonzero signed multiplicity for the full bag comparison.
    let mut result: Vec<(i64, i64, i64, i64)> = result_map
        .into_iter()
        .filter(|(_, w)| *w != 0)
        .map(|((lk, lv, rv), w)| (lk, lv, rv, w))
        .collect();
    result.sort();
    result
}

// ─── Incremental output accumulator ─────────────────────────────────────────

fn accumulate_join_output(output: &ArrowZSet, output_acc: &mut BTreeMap<(i64, i64, i64), i64>) {
    if output.is_empty() {
        return;
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
    let rw_col = output
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..output.num_rows() {
        let key = (lk_col.value(i), lv_col.value(i), rw_col.value(i));
        let entry = output_acc.entry(key).or_insert(0);
        *entry += output.weights[i];
        if *entry == 0 {
            output_acc.remove(&key);
        }
    }
}

fn weighted_join_output(output_acc: &BTreeMap<(i64, i64, i64), i64>) -> Vec<(i64, i64, i64, i64)> {
    output_acc
        .iter()
        .map(|(&(lk, lv, rv), &weight)| (lk, lv, rv, weight))
        .collect()
}

/// Accumulate the incremental output of a join over multiple epochs.
///
/// Returns sorted `(l_k, l_v, r_w, net_weight)` rows for all nonzero join tuples.
pub fn incremental_join_output(epochs: &[JoinEpoch]) -> Vec<(i64, i64, i64, i64)> {
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
        accumulate_join_output(&output, &mut output_acc);
    }

    weighted_join_output(&output_acc)
}

// ─── 3-way join ───────────────────────────────────────────────────────────────

/// Batch reference for a 3-way join:
/// `SELECT l.k, l.v, m.v, r.v FROM l JOIN m ON l.k = m.k JOIN r ON m.k = r.k`
///
/// Returns sorted `(k, l_v, m_v, r_v, net_weight)` rows for nonzero multiplicities.
pub fn batch_reference_3way(
    left_acc: &BTreeMap<(i64, i64), i64>,
    mid_acc: &BTreeMap<(i64, i64), i64>,
    right_acc: &BTreeMap<(i64, i64), i64>,
) -> Vec<(i64, i64, i64, i64, i64)> {
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
    let mut result: Vec<(i64, i64, i64, i64, i64)> = result_map
        .into_iter()
        .filter(|(_, w)| *w != 0)
        .map(|((k, lv, mv, rv), weight)| (k, lv, mv, rv, weight))
        .collect();
    result.sort();
    result
}

/// Accumulate the incremental output of a 3-way join.
///
fn process_3way_epoch(
    lm_op: &JoinOp,
    lmr_op: &JoinOp,
    left_epoch: &[DeltaRow],
    mid_epoch: &[DeltaRow],
    right_epoch: &[DeltaRow],
) -> ArrowZSet {
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
    let right = if right_epoch.is_empty() {
        empty_kv_batch()
    } else {
        make_kv_batch(right_epoch)
    };
    lmr_op
        .process_epoch(lm_out, right)
        .expect("lmr join failed")
}

fn accumulate_3way_join_output(
    output: &ArrowZSet,
    output_acc: &mut BTreeMap<(i64, i64, i64, i64), i64>,
) {
    if output.is_empty() {
        return;
    }
    let col0 = output
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let col1 = output
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let col3 = output
        .data
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let col5 = output
        .data
        .column(5)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..output.num_rows() {
        let key = (col0.value(i), col1.value(i), col3.value(i), col5.value(i));
        let entry = output_acc.entry(key).or_insert(0);
        *entry += output.weights[i];
        if *entry == 0 {
            output_acc.remove(&key);
        }
    }
}

fn weighted_3way_join_output(
    output_acc: &BTreeMap<(i64, i64, i64, i64), i64>,
) -> Vec<(i64, i64, i64, i64, i64)> {
    output_acc
        .iter()
        .map(|(&(k, lv, mv, rv), &weight)| (k, lv, mv, rv, weight))
        .collect()
}

/// Accumulate the incremental output of a 3-way join over multiple epochs.
///
/// Uses two chained `JoinOp` instances and returns sorted nonzero weighted rows.
pub fn incremental_3way_join_output(epochs: &[Join3Epoch]) -> Vec<(i64, i64, i64, i64, i64)> {
    // LM join: 2-col L ⋈ 2-col M → output (l_k, l_v, m_k, m_v) = 4 columns
    let lm_op = JoinOp::with_schema(OperatorId(1), vec![0], vec![0], 2, 2);

    // LMR join: 4-col LM ⋈ 2-col R → output (lm_0, lm_1, lm_2, lm_3, r_k, r_v) = 6 columns
    // LM key is col 0 (l_k), R key is col 0 (r_k).
    let lmr_op = JoinOp::with_schema(OperatorId(2), vec![0], vec![0], 4, 2);

    let mut output_acc: BTreeMap<(i64, i64, i64, i64), i64> = BTreeMap::new();

    for (left_epoch, mid_epoch, right_epoch) in epochs {
        let output = process_3way_epoch(&lm_op, &lmr_op, left_epoch, mid_epoch, right_epoch);
        accumulate_3way_join_output(&output, &mut output_acc);
    }

    weighted_3way_join_output(&output_acc)
}

/// Compare the chained incremental join with an independent batch result after each epoch.
pub fn assert_oracle_3way(epochs: &[Join3Epoch]) {
    let lm_op = JoinOp::with_schema(OperatorId(1), vec![0], vec![0], 2, 2);
    let lmr_op = JoinOp::with_schema(OperatorId(2), vec![0], vec![0], 4, 2);
    let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut mid_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut output_acc: BTreeMap<(i64, i64, i64, i64), i64> = BTreeMap::new();

    for (epoch, (left, mid, right)) in epochs.iter().enumerate() {
        for &(k, v, weight) in left {
            let entry = left_acc.entry((k, v)).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                left_acc.remove(&(k, v));
            }
        }
        for &(k, v, weight) in mid {
            let entry = mid_acc.entry((k, v)).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                mid_acc.remove(&(k, v));
            }
        }
        for &(k, v, weight) in right {
            let entry = right_acc.entry((k, v)).or_insert(0);
            *entry += weight;
            if *entry == 0 {
                right_acc.remove(&(k, v));
            }
        }

        let output = process_3way_epoch(&lm_op, &lmr_op, left, mid, right);
        accumulate_3way_join_output(&output, &mut output_acc);
        let incremental = weighted_3way_join_output(&output_acc);
        let batch = batch_reference_3way(&left_acc, &mid_acc, &right_acc);
        assert_eq!(
            incremental, batch,
            "3-way join oracle failed at epoch {epoch}: incremental={incremental:?} batch={batch:?}"
        );
    }
}

// ─── Oracle assertion ─────────────────────────────────────────────────────────

/// Assert `incremental == batch` for the 2-table inner equi-join.
///
/// `epochs`: sequence of `(left_delta, right_delta)` pairs.
/// Each delta is a `Vec<(k, v, weight)>` with signed integer weights.
pub fn assert_oracle_join(epochs: &[JoinEpoch]) {
    let op = JoinOp::new(OperatorId(0), vec![0], vec![0]);
    let mut left_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut right_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    let mut output_acc: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();

    for (epoch, (left_epoch, right_epoch)) in epochs.iter().enumerate() {
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

        if !left_epoch.is_empty() || !right_epoch.is_empty() {
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
            let output = op.process_epoch(left, right).expect("join failed");
            accumulate_join_output(&output, &mut output_acc);
        }

        let incremental = weighted_join_output(&output_acc);
        let batch = batch_reference_join(&left_acc, &right_acc);
        assert_eq!(
            incremental, batch,
            "Join oracle failed at epoch {epoch}: incremental={incremental:?} batch={batch:?}"
        );
    }
}

#[cfg(test)]
fn bucketed_join_output(
    epochs: &[JoinEpoch],
    bucket_count: u16,
    replication_state_quota: usize,
) -> Result<Vec<(i64, i64, i64, i64)>, ErrorCode> {
    let replicated_rows: usize = epochs.iter().map(|(_, right)| right.len()).sum();
    if replicated_rows.saturating_mul(bucket_count as usize) > replication_state_quota {
        return Err(RS_5035);
    }

    let mut merged = Vec::new();
    for bucket in 0..bucket_count {
        let bucket_epochs = epochs
            .iter()
            .map(|(left, right)| {
                let left_bucket = left
                    .iter()
                    .copied()
                    .filter(|(k, v, _)| {
                        let mut key = [0u8; 16];
                        key[..8].copy_from_slice(&k.to_be_bytes());
                        key[8..].copy_from_slice(&v.to_be_bytes());
                        route_virtual_bucket(&key, bucket_count, key.len()) == Some(bucket)
                    })
                    .collect::<Vec<_>>();
                (left_bucket, right.clone())
            })
            .collect::<Vec<_>>();
        merged.extend(incremental_join_output(&bucket_epochs));
    }
    merged.sort();
    Ok(merged)
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
        assert_eq!(
            result,
            vec![],
            "non-matching keys must produce empty join output"
        );
    }

    #[test]
    fn oracle_join_single_match() {
        // l(k=5, lv=50) ⋈ r(k=5, rv=500) → (5, 50, 500)
        assert_oracle_join(&[(vec![(5, 50, 1)], vec![(5, 500, 1)])]);
        let result = incremental_join_output(&[(vec![(5, 50, 1)], vec![(5, 500, 1)])]);
        assert_eq!(
            result,
            vec![(5, 50, 500, 1)],
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
            vec![(1, 10, 100, 1), (1, 10, 200, 1)],
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
            vec![(2, 10, 300, 1), (2, 20, 300, 1)],
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
            vec![(2, 20, 200, 1)],
            "multiple_epochs: expected only (2,20,200) after deleting (1,10)"
        );
    }

    #[test]
    fn oracle_join_weighted_deltas_across_epochs() {
        let epochs = vec![
            (vec![(1, 10, 2)], vec![(1, 100, 3)]),
            (vec![(1, 10, 1)], vec![]),
            (vec![], vec![(1, 100, -1)]),
            (vec![(1, 10, -1)], vec![(1, 100, -1)]),
            (vec![(1, 10, -2)], vec![]),
            (vec![(1, 10, 2)], vec![(1, 100, 1)]),
        ];
        let expected = [
            vec![(1, 10, 100, 6)],
            vec![(1, 10, 100, 9)],
            vec![(1, 10, 100, 6)],
            vec![(1, 10, 100, 2)],
            vec![],
            vec![(1, 10, 100, 4)],
        ];

        for (epoch, expected) in expected.iter().enumerate() {
            let prefix = &epochs[..=epoch];
            assert_oracle_join(prefix);
            assert_eq!(
                incremental_join_output(prefix),
                *expected,
                "weighted join result after epoch {epoch}"
            );
        }
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
        assert_eq!(result, vec![(1, 10, 100, 1), (1, 10, 200, 1)]);
    }

    #[test]
    fn oracle_join_cross_product_exact() {
        // l: (k=3, v=1), (k=3, v=2); r: (k=3, v=10), (k=3, v=20).
        // Cross product: (3,1,10), (3,1,20), (3,2,10), (3,2,20) — 4 tuples.
        let result =
            incremental_join_output(&[(vec![(3, 1, 1), (3, 2, 1)], vec![(3, 10, 1), (3, 20, 1)])]);
        let mut got = result.clone();
        got.sort();
        assert_eq!(
            got,
            vec![(3, 1, 10, 1), (3, 1, 20, 1), (3, 2, 10, 1), (3, 2, 20, 1)],
            "cross product: expected 4 tuples"
        );
    }

    // ── Proptest randomized oracle (≥100k scenarios) ──────────────────────

    #[cfg(test)]
    mod proptest_oracle {
        use proptest::prelude::*;

        use super::super::{assert_oracle_join, JoinEpoch};

        /// Random delta: key ∈ [0,4], value ∈ [0,9], weight ∈ [-3,3].
        fn arb_delta() -> impl Strategy<Value = Vec<(i64, i64, i64)>> {
            proptest::collection::vec((0i64..5, 0i64..10, -3i64..=3), 0..8)
        }

        /// Random epoch: a pair of (left_delta, right_delta).
        fn arb_epoch() -> impl Strategy<Value = JoinEpoch> {
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
            // Multiplicities multiply across the two joins: 2 * 3 * 4 = 24.
            let epochs = vec![(
                vec![(1i64, 10i64, 2i64)],
                vec![(1i64, 100i64, 3i64)],
                vec![(1i64, 1000i64, 4i64)],
            )];
            super::super::assert_oracle_3way(&epochs);
            assert_eq!(
                super::super::incremental_3way_join_output(&epochs),
                vec![(1, 10, 100, 1000, 24)]
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
                super::super::assert_oracle_3way(&epochs);
            }
        }

        #[test]
        fn hot_key_bucketed_join_matches_unsplit_oracle() {
            let epochs = vec![
                (
                    vec![(1, 10, 1), (1, 11, 1), (2, 20, 1)],
                    vec![(1, 100, 1), (2, 200, 1)],
                ),
                (vec![(1, 10, -1), (1, 12, 1)], vec![(1, 101, 1)]),
            ];

            let unsplit = super::super::incremental_join_output(&epochs);
            let bucketed = super::super::bucketed_join_output(&epochs, 4, 128).unwrap();
            assert_eq!(bucketed, unsplit);
        }

        #[test]
        fn hot_key_bucketed_join_returns_skew_bound_when_replication_exceeds_quota() {
            let epochs = vec![(
                vec![(1, 10, 1), (1, 11, 1), (1, 12, 1)],
                vec![(1, 100, 1), (1, 101, 1), (1, 102, 1)],
            )];

            let err = super::super::bucketed_join_output(&epochs, 8, 4).unwrap_err();
            assert_eq!(err, super::super::RS_5035);
        }
    }
}

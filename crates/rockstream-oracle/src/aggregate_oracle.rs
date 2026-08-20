//! Oracle property tests for the Aggregate operator (v0.5 — IVM-2).
//!
//! Query under test:
//! `SELECT k, SUM(v), COUNT(*), SUM(v)/COUNT(*) AS avg_v FROM t GROUP BY k`
//!
//! ## Oracle property
//!
//! `incremental(q, Δ) == batch(q, accumulated)` for every sequence of
//! insert/update/delete deltas.
//!
//! ## Test structure
//!
//! 1. **Incremental side**: for each epoch of `(k: i64, v: i64, weight: ±1)`
//!    deltas, run through `AggregateOp` and accumulate the output Z-set.
//!    The accumulated output maps each live group k to `(sum_v, count, avg_v)`.
//!
//! 2. **Batch side**: accumulate the input Z-set (key = `(k, v)`, weight =
//!    net weight).  For each group k with positive net count, compute
//!    `SUM(v) = Σ v*weight(k,v)` and `COUNT(*) = Σ weight(k,v)` directly,
//!    then `avg_v = sum_v as f64 / count as f64` (correct floating-point
//!    division — v0.51.6 Slice 4, matching `sum_count.rs`'s
//!    `avg_from_sum_count` semantics).
//!
//! 3. **DataFusion validation**: the batch reference is validated against a
//!    real SQL engine using `SELECT k, SUM(v), COUNT(*), CAST(SUM(v) AS
//!    DOUBLE) / COUNT(*) AS avg_v FROM t GROUP BY k ORDER BY k` on the
//!    expanded input rows.
//!
//! 4. **Property test**: `proptest` runs ≥100k random delta sequences asserting
//!    `incremental == batch` (avg compared within `f64::EPSILON`).

use std::collections::BTreeMap;
use std::sync::Arc;

use rockstream_ops::aggregate::AggregateOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::ids::OperatorId;
use rockstream_types::laws::sum_count::avg_from_sum_count;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Build an `ArrowZSet` from `(k, v, weight)` triples.
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

// ─── Batch reference ─────────────────────────────────────────────────────────

/// Apply `SELECT k, SUM(v), COUNT(*), SUM(v)/COUNT(*) AS avg_v FROM t GROUP BY k`
/// to an accumulated input Z-set (pure Rust).
///
/// The input map is `(k, v) → net_weight`.  For each group k, the aggregate
/// values are:
///   - `count  = Σ_{v} weight(k, v)` (must be > 0 for the group to appear)
///   - `sum_v  = Σ_{v} v * weight(k, v)`
///   - `avg_v  = sum_v as f64 / count as f64` (correct floating-point division)
///
/// Returns a sorted `Vec<(k, sum_v, count, avg_v)>`.
fn batch_reference(input_acc: &BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64, i64, f64)> {
    // Group by k.
    let mut groups: BTreeMap<i64, (i64, i64)> = BTreeMap::new(); // k → (sum, count)
    for (&(k, v), &w) in input_acc {
        let entry = groups.entry(k).or_insert((0, 0));
        entry.0 += v * w; // sum += v * weight
        entry.1 += w; // count += weight
    }
    // Keep only groups with positive net count.
    let mut result: Vec<(i64, i64, i64, f64)> = groups
        .into_iter()
        .filter(|(_, (_, count))| *count > 0)
        .map(|(k, (sum, count))| {
            (
                k,
                sum,
                count,
                avg_from_sum_count(sum, count).expect("count > 0 guaranteed by filter above"),
            )
        })
        .collect();
    result.sort_by_key(|(k, _, _, _)| *k);
    result
}

// ─── Incremental side ────────────────────────────────────────────────────────

/// Run epochs through `AggregateOp` and return the accumulated output state.
///
/// Returns the sorted set of `(k, sum_v, count, avg_v)` live groups.
fn incremental_output(epochs: &[Vec<(i64, i64, i64)>]) -> Vec<(i64, i64, i64, f64)> {
    use arrow::array::{Float64Array, Int64Array};

    let op = AggregateOp::new(OperatorId(0));

    // Accumulate the output Z-set: k → (sum_v, count, avg_v) with net weight.
    // Since the output is a Z-set of aggregate rows, we need to track by
    // (k, sum_v, count, avg_v) → weight and then extract positive-weight entries.
    // But for our schema, k is the unique key and the operator always retracts
    // the old row before inserting the new one — so the net weight for each k
    // is always 0 or 1 in well-formed output.
    //
    // Strategy: maintain a map k → (sum_v, count, avg_v) by applying the
    // output Z-set deltas.
    let mut state: BTreeMap<i64, (i64, i64, f64)> = BTreeMap::new();

    for epoch in epochs {
        if epoch.is_empty() {
            continue;
        }
        let batch = make_kv_batch(epoch);
        let output = op.process_delta(batch).expect("AggregateOp failed");

        // Apply output deltas to state.
        if output.is_empty() {
            continue;
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
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..output.num_rows() {
            let k = k_col.value(i);
            let sum = s_col.value(i);
            let count = c_col.value(i);
            let avg = a_col.value(i);
            let w = output.weights[i];
            if w > 0 {
                state.insert(k, (sum, count, avg));
            } else if w < 0 {
                state.remove(&k);
            }
        }
    }

    let mut result: Vec<(i64, i64, i64, f64)> = state
        .into_iter()
        .map(|(k, (sum, count, avg))| (k, sum, count, avg))
        .collect();
    result.sort_by_key(|(k, _, _, _)| *k);
    result
}

// ─── Oracle assertion ─────────────────────────────────────────────────────────

/// Assert `incremental == batch` for the aggregate query.
///
/// `epochs`: sequence of delta epochs, each a `Vec<(k, v, weight)>`.
///
/// `weight` must be +1 or -1 only.  The function accumulates the input Z-set
/// across all epochs before computing the batch reference.
pub fn assert_oracle_aggregate(epochs: &[Vec<(i64, i64, i64)>]) {
    // Build accumulated input map: (k, v) → net_weight.
    let mut input_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for epoch in epochs {
        for &(k, v, w) in epoch {
            let entry = input_acc.entry((k, v)).or_insert(0);
            *entry += w;
            if *entry == 0 {
                input_acc.remove(&(k, v));
            }
        }
    }

    let mut batch = batch_reference(&input_acc);
    batch.sort_by_key(|(k, _, _, _)| *k);

    let mut inc = incremental_output(epochs);
    inc.sort_by_key(|(k, _, _, _)| *k);

    assert_eq!(
        inc.len(),
        batch.len(),
        "Aggregate oracle property FAILED: incremental != batch (different group counts)\n\
         Query: SELECT k, SUM(v), COUNT(*), SUM(v)/COUNT(*) AS avg_v FROM t GROUP BY k\n\
         incremental ({} groups): {inc:?}\n\
         batch      ({} groups): {batch:?}",
        inc.len(),
        batch.len()
    );
    for ((ik, isum, icount, iavg), (bk, bsum, bcount, bavg)) in inc.iter().zip(batch.iter()) {
        assert_eq!(
            (ik, isum, icount),
            (bk, bsum, bcount),
            "Aggregate oracle property FAILED: (k, sum, count) mismatch\n\
             incremental: {inc:?}\nbatch: {batch:?}"
        );
        assert!(
            (iavg - bavg).abs() < f64::EPSILON,
            "Aggregate oracle property FAILED: avg mismatch for k={ik}: \
             incremental avg={iavg}, batch avg={bavg}\n\
             incremental: {inc:?}\nbatch: {batch:?}"
        );
    }
}

// ─── DataFusion validation ────────────────────────────────────────────────────

/// Run `SELECT k, SUM(v), COUNT(*), CAST(SUM(v) AS DOUBLE) / COUNT(*) FROM t
/// GROUP BY k ORDER BY k` in DataFusion on the given rows.
///
/// `rows` is a list of `(k, v)` pairs where each pair represents one row in
/// the table (weight = 1 implicitly — DataFusion doesn't know about Z-sets).
pub async fn run_datafusion_aggregate(
    rows: &[(i64, i64)],
) -> datafusion::error::Result<Vec<(i64, i64, i64, f64)>> {
    use arrow::array::{Float64Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    if rows.is_empty() {
        return Ok(vec![]);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let k_vals: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let v_vals: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(k_vals)),
            Arc::new(Int64Array::from(v_vals)),
        ],
    )?;
    let ctx = SessionContext::new();
    let mem_table = datafusion::datasource::memory::MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(mem_table))?;
    // Correct floating-point division: cast the numerator to DOUBLE before
    // dividing so SUM(v)/COUNT(*) doesn't truncate (v0.51.6 Slice 4).
    let df = ctx
        .sql(
            "SELECT k, SUM(v), COUNT(*), CAST(SUM(v) AS DOUBLE) / COUNT(*) AS avg_v \
             FROM t GROUP BY k ORDER BY k",
        )
        .await?;
    let batches = df.collect().await?;
    let mut result = Vec::new();
    for b in &batches {
        let k_col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let s_col = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let c_col = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let a_col = b.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            result.push((
                k_col.value(i),
                s_col.value(i),
                c_col.value(i),
                a_col.value(i),
            ));
        }
    }
    Ok(result)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Deterministic oracle tests — with exact expected values ──────────

    #[test]
    fn oracle_aggregate_empty_input() {
        assert_oracle_aggregate(&[]);
        // Empty input → no groups.
        let result = incremental_output(&[]);
        assert_eq!(result, vec![], "empty input must produce empty output");
    }

    #[test]
    fn oracle_aggregate_single_insert() {
        // One row: k=1, v=10.
        // Expected: [(k=1, sum=10, count=1, avg=10.0)]
        assert_oracle_aggregate(&[vec![(1, 10, 1)]]);
        let result = incremental_output(&[vec![(1, 10, 1)]]);
        assert_eq!(
            result,
            vec![(1, 10, 1, 10.0)],
            "single insert k=1 v=10: expected (k=1, sum=10, count=1, avg=10.0)"
        );
    }

    #[test]
    fn oracle_aggregate_two_groups() {
        // k=1: v=10 + v=5 → sum=15, count=2, avg=7.5 (correct fractional division)
        // k=2: v=20        → sum=20, count=1, avg=20.0
        assert_oracle_aggregate(&[vec![(1, 10, 1), (2, 20, 1), (1, 5, 1)]]);
        let result = incremental_output(&[vec![(1, 10, 1), (2, 20, 1), (1, 5, 1)]]);
        assert_eq!(
            result,
            vec![(1, 15, 2, 7.5), (2, 20, 1, 20.0)],
            "two_groups: expected [(1,15,2,7.5),(2,20,1,20.0)]"
        );
    }

    #[test]
    fn oracle_aggregate_insert_then_delete() {
        // Insert then delete the same row → group disappears entirely.
        assert_oracle_aggregate(&[vec![(1, 10, 1)], vec![(1, 10, -1)]]);
        let result = incremental_output(&[vec![(1, 10, 1)], vec![(1, 10, -1)]]);
        assert_eq!(result, vec![], "insert-then-delete must leave no groups");
    }

    #[test]
    fn oracle_aggregate_group_churn() {
        // Group k=1: insert v=5, insert v=7, delete v=5, delete v=7 → disappears.
        assert_oracle_aggregate(&[
            vec![(1, 5, 1), (1, 7, 1)],
            vec![(1, 5, -1)],
            vec![(1, 7, -1)],
        ]);
        let result = incremental_output(&[
            vec![(1, 5, 1), (1, 7, 1)],
            vec![(1, 5, -1)],
            vec![(1, 7, -1)],
        ]);
        assert_eq!(result, vec![], "group churn must leave no groups");
    }

    #[test]
    fn oracle_aggregate_update_via_delete_insert() {
        // "Update" v=5 to v=9 for k=1.
        // Final state: k=1 sum=9, count=1, avg=9.0.
        assert_oracle_aggregate(&[vec![(1, 5, 1)], vec![(1, 5, -1), (1, 9, 1)]]);
        let result = incremental_output(&[vec![(1, 5, 1)], vec![(1, 5, -1), (1, 9, 1)]]);
        assert_eq!(
            result,
            vec![(1, 9, 1, 9.0)],
            "update (delete+insert): expected k=1 sum=9 count=1 avg=9.0"
        );
    }

    #[test]
    fn oracle_aggregate_negative_values() {
        // k=1: v=-3, v=-5 → sum=-8, count=2, avg=-4.0
        // k=2: v=4         → sum=4,  count=1, avg=4.0
        assert_oracle_aggregate(&[vec![(1, -3, 1), (1, -5, 1), (2, 4, 1)]]);
        let result = incremental_output(&[vec![(1, -3, 1), (1, -5, 1), (2, 4, 1)]]);
        assert_eq!(
            result,
            vec![(1, -8, 2, -4.0), (2, 4, 1, 4.0)],
            "negative values: expected [(1,-8,2,-4.0),(2,4,1,4.0)]"
        );
    }

    #[test]
    fn oracle_aggregate_multiple_epochs() {
        // epoch 0: k=1 v=10         → k=1 sum=10 count=1
        // epoch 1: k=1 v=20, k=2 v=5 → k=1 sum=30 count=2, k=2 sum=5 count=1
        // epoch 2: k=2 v=5 deleted   → k=2 disappears
        // epoch 3: k=1 v=10 deleted  → k=1 sum=20 count=1
        // Final: [(k=1, sum=20, count=1, avg=20.0)]
        assert_oracle_aggregate(&[
            vec![(1, 10, 1)],
            vec![(1, 20, 1), (2, 5, 1)],
            vec![(2, 5, -1)],
            vec![(1, 10, -1)],
        ]);
        let result = incremental_output(&[
            vec![(1, 10, 1)],
            vec![(1, 20, 1), (2, 5, 1)],
            vec![(2, 5, -1)],
            vec![(1, 10, -1)],
        ]);
        assert_eq!(
            result,
            vec![(1, 20, 1, 20.0)],
            "multiple_epochs: expected [(1,20,1,20.0)]"
        );
    }

    #[test]
    fn oracle_aggregate_group_deletion_and_resurrection() {
        // k=1 v=10 inserted, then deleted, then re-inserted with v=20.
        // Final: k=1 sum=20, count=1, avg=20.0.
        assert_oracle_aggregate(&[vec![(1, 10, 1)], vec![(1, 10, -1)], vec![(1, 20, 1)]]);
        let result = incremental_output(&[vec![(1, 10, 1)], vec![(1, 10, -1)], vec![(1, 20, 1)]]);
        assert_eq!(
            result,
            vec![(1, 20, 1, 20.0)],
            "group deletion+resurrection: expected k=1 sum=20"
        );
    }

    #[test]
    fn oracle_aggregate_multi_group_partial_deletion() {
        // k=1: 3 copies of v=5 → insert 3, delete 1 → 2 copies left.
        // Final: k=1 sum=10, count=2, avg=5.0.
        assert_oracle_aggregate(&[vec![(1, 5, 1), (1, 5, 1), (1, 5, 1)], vec![(1, 5, -1)]]);
        let result = incremental_output(&[vec![(1, 5, 1), (1, 5, 1), (1, 5, 1)], vec![(1, 5, -1)]]);
        assert_eq!(
            result,
            vec![(1, 10, 2, 5.0)],
            "partial deletion: expected k=1 sum=10 count=2 avg=5.0"
        );
    }

    #[test]
    fn oracle_aggregate_genuinely_fractional_average() {
        // k=1: v=1, v=2, v=4 → sum=7, count=3, avg=7/3 (not truncated to 2).
        assert_oracle_aggregate(&[vec![(1, 1, 1), (1, 2, 1), (1, 4, 1)]]);
        let result = incremental_output(&[vec![(1, 1, 1), (1, 2, 1), (1, 4, 1)]]);
        assert_eq!(result.len(), 1);
        let (k, sum, count, avg) = result[0];
        assert_eq!((k, sum, count), (1, 7, 3));
        assert!(
            (avg - (7.0 / 3.0)).abs() < f64::EPSILON,
            "expected avg ~= 7/3, got {avg}"
        );
        assert_ne!(avg, 2.0, "avg must not be truncated to an integer");
    }

    // ── DataFusion validation test ─────────────────────────────────────────

    /// Validate the batch reference against DataFusion.
    #[tokio::test]
    async fn oracle_datafusion_validates_batch_reference() {
        // Input rows: (k=1, v=5), (k=1, v=7), (k=2, v=10).
        let rows = vec![(1i64, 5i64), (1, 7), (2, 10)];
        let df_result = run_datafusion_aggregate(&rows).await.unwrap();

        // Build input_acc (weight = 1 for each row, since rows is the materialized state).
        let mut input_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
        for &(k, v) in &rows {
            *input_acc.entry((k, v)).or_insert(0) += 1;
        }
        let batch = batch_reference(&input_acc);

        assert_eq!(
            df_result, batch,
            "DataFusion and batch reference disagree\ndf={df_result:?}\nbatch={batch:?}"
        );
        // k=1: sum=12, count=2, avg=6.0; k=2: sum=10, count=1, avg=10.0.
        assert_eq!(df_result, vec![(1, 12, 2, 6.0), (2, 10, 1, 10.0)]);
    }

    #[tokio::test]
    async fn oracle_datafusion_validates_negative_avg() {
        // k=1: v=-7 (3 copies) → sum=-21, count=3, avg=-7.0.
        let rows = vec![(1i64, -7i64), (1, -7), (1, -7)];
        let df_result = run_datafusion_aggregate(&rows).await.unwrap();
        assert_eq!(df_result, vec![(1, -21, 3, -7.0)]);
    }

    #[tokio::test]
    async fn oracle_datafusion_validates_genuinely_fractional_avg() {
        // k=1: v=1, v=2, v=4 → sum=7, count=3, avg=7/3 (not an exact integer).
        let rows = vec![(1i64, 1i64), (1, 2), (1, 4)];
        let df_result = run_datafusion_aggregate(&rows).await.unwrap();
        assert_eq!(df_result.len(), 1);
        let (k, sum, count, avg) = df_result[0];
        assert_eq!((k, sum, count), (1, 7, 3));
        assert!(
            (avg - (7.0 / 3.0)).abs() < f64::EPSILON,
            "expected DataFusion avg ~= 7/3, got {avg}"
        );
    }

    // ── Proptest randomized oracle (100k scenarios) ────────────────────────

    #[cfg(test)]
    mod proptest_oracle {
        use proptest::prelude::*;

        use super::super::assert_oracle_aggregate;

        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(100_000))]

            /// Oracle property test: incremental == batch for ≥100k random
            /// delta sequences over the aggregate query.
            ///
            /// Key space expanded to k ∈ [0..20], v ∈ [-200..200], up to
            /// 10 epochs, up to 20 rows each — wider than the original
            /// [0..5]/[-50..50] to exercise more group-key diversity and
            /// deeper churn.
            #[test]
            #[allow(clippy::items_after_test_module)]
            fn oracle_aggregate_100k(
                epochs in prop::collection::vec(
                    prop::collection::vec(
                        // (k ∈ 0..20, v ∈ -200..200, weight ∈ {+1, -1})
                        (0i64..20i64, -200i64..200i64, prop::bool::ANY)
                            .prop_map(|(k, v, insert)| (k, v, if insert { 1i64 } else { -1i64 })),
                        0..20usize,
                    ),
                    1..10usize,
                )
            ) {
                // Filter epochs to only include valid deltas (no over-delete).
                // Track current input state per (k,v) to skip invalid retractions.
                let mut kv_state: std::collections::HashMap<(i64, i64), i64> =
                    std::collections::HashMap::new();
                let mut valid_epochs: Vec<Vec<(i64, i64, i64)>> = Vec::new();
                for epoch in &epochs {
                    let mut valid_epoch = Vec::new();
                    for &(k, v, w) in epoch {
                        if w > 0 {
                            let e = kv_state.entry((k, v)).or_insert(0);
                            *e += 1;
                            valid_epoch.push((k, v, 1));
                        } else {
                            // Only retract if the row is present.
                            let e = kv_state.entry((k, v)).or_insert(0);
                            if *e > 0 {
                                *e -= 1;
                                if *e == 0 { kv_state.remove(&(k, v)); }
                                valid_epoch.push((k, v, -1));
                            }
                        }
                    }
                    if !valid_epoch.is_empty() {
                        valid_epochs.push(valid_epoch);
                    }
                }
                assert_oracle_aggregate(&valid_epochs);
            }
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(20_000))]

            /// High-churn retraction proptest: groups are inserted and then
            /// fully retracted in alternating epochs, exercising the
            /// "group dies and is resurrected" code path.
            ///
            /// Every odd epoch inserts rows; every even epoch retracts all of them.
            /// At each even epoch the aggregate must be empty; at each odd epoch
            /// it must match the fresh batch reference.
            #[test]
            fn oracle_aggregate_high_churn(
                inserts in prop::collection::vec(
                    prop::collection::vec(
                        (0i64..10i64, 1i64..100i64)
                            .prop_map(|(k, v)| (k, v)),
                        1..15usize,
                    ),
                    1..8usize,
                )
            ) {
                // Build alternating insert / full-retract epoch pairs.
                let mut epochs: Vec<Vec<(i64, i64, i64)>> = Vec::new();
                for batch in &inserts {
                    // Insert epoch.
                    let ins: Vec<(i64, i64, i64)> = batch.iter().map(|&(k,v)| (k, v, 1)).collect();
                    // Retract epoch: delete everything just inserted.
                    let ret: Vec<(i64, i64, i64)> = batch.iter().map(|&(k,v)| (k, v, -1)).collect();
                    if !ins.is_empty() {
                        epochs.push(ins);
                        epochs.push(ret);
                    }
                }
                if !epochs.is_empty() {
                    assert_oracle_aggregate(&epochs);
                }
            }
        }
    }
}

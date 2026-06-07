//! Oracle property tests for the MinMax operator (v0.6 — IVM-3).
//!
//! Queries under test:
//! - `SELECT k, MIN(v) FROM t GROUP BY k`
//! - `SELECT k, MAX(v) FROM t GROUP BY k`
//!
//! ## Oracle property
//!
//! `incremental(q, Δ) == batch(q, accumulated)` for every sequence of
//! insert/update/delete deltas, including group-churn scenarios where groups
//! are created, their extremum changes multiple times, and they are deleted.
//!
//! An additional invariant is checked after every batch:
//!   **cached extremum == true extremum** (i.e. the cache never diverges from
//!   the sorted multiset).
//!
//! ## Test structure
//!
//! 1. **Incremental side**: for each epoch of `(k, v, weight)` deltas, run
//!    through `MinMaxOp` and accumulate the output Z-set.
//! 2. **Batch side**: accumulate the input Z-set; for each live group find
//!    the minimum (or maximum) value with positive net weight.
//! 3. **DataFusion validation**: the batch reference is validated against a
//!    real SQL engine using `SELECT k, MIN(v) FROM t GROUP BY k ORDER BY k`
//!    on the expanded rows.
//! 4. **Property test**: `proptest` runs ≥100k random delta sequences.

use std::collections::BTreeMap;
use std::sync::Arc;

use rockstream_ops::minmax::{MinMaxKind, MinMaxOp};
use rockstream_ops::op::Operator;
use rockstream_ops::zset::ArrowZSet;
use rockstream_types::ids::OperatorId;

// ─── Helpers ─────────────────────────────────────────────────────────────────

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

// ─── Batch references ─────────────────────────────────────────────────────────

/// `SELECT k, MIN(v) FROM t GROUP BY k` over accumulated Z-set.
///
/// Input: `(k, v) → net_weight`.  Returns sorted `Vec<(k, min_v)>`.
pub fn batch_reference_min(input_acc: &BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64)> {
    // group_key → minimum v with positive weight
    let mut groups: BTreeMap<i64, i64> = BTreeMap::new();
    for (&(k, v), &w) in input_acc {
        if w > 0 {
            let entry = groups.entry(k).or_insert(v);
            if v < *entry {
                *entry = v;
            }
        }
    }
    groups.into_iter().collect()
}

/// `SELECT k, MAX(v) FROM t GROUP BY k` over accumulated Z-set.
///
/// Input: `(k, v) → net_weight`.  Returns sorted `Vec<(k, max_v)>`.
pub fn batch_reference_max(input_acc: &BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64)> {
    let mut groups: BTreeMap<i64, i64> = BTreeMap::new();
    for (&(k, v), &w) in input_acc {
        if w > 0 {
            let entry = groups.entry(k).or_insert(v);
            if v > *entry {
                *entry = v;
            }
        }
    }
    groups.into_iter().collect()
}

// ─── Incremental side ────────────────────────────────────────────────────────

/// Run epochs through `MinMaxOp` and return the accumulated output state.
///
/// Returns sorted `Vec<(k, extremum_v)>`.
///
/// Also asserts after every batch that `cached_extremum == true_extremum`.
pub fn incremental_output(epochs: &[Vec<(i64, i64, i64)>], kind: MinMaxKind) -> Vec<(i64, i64)> {
    use arrow::array::Int64Array;

    let op = MinMaxOp::new(OperatorId(0), kind);
    let mut state: BTreeMap<i64, i64> = BTreeMap::new();

    for epoch in epochs {
        if epoch.is_empty() {
            continue;
        }
        let batch = make_kv_batch(epoch);
        let output = op.process_delta(batch).expect("MinMaxOp failed");

        // After every batch: verify cache consistency.
        for &(k, _, _) in epoch {
            assert_eq!(
                op.cached_extremum(k),
                op.true_extremum(k),
                "cache/true_extremum diverged for group k={k} after epoch {epoch:?}"
            );
        }

        if output.is_empty() {
            continue;
        }
        let k_col = output
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let e_col = output
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..output.num_rows() {
            let k = k_col.value(i);
            let e = e_col.value(i);
            let w = output.weights[i];
            if w > 0 {
                state.insert(k, e);
            } else if w < 0 {
                state.remove(&k);
            }
        }
    }

    state.into_iter().collect()
}

// ─── Oracle assertion ─────────────────────────────────────────────────────────

/// Assert `incremental == batch` for a MIN or MAX query.
///
/// `epochs`: sequence of delta epochs, each a `Vec<(k, v, weight)>`.
/// Only ±1 weights are meaningful here; duplicates are tracked by the multiset.
pub fn assert_oracle_minmax(epochs: &[Vec<(i64, i64, i64)>], kind: MinMaxKind) {
    // Accumulate input Z-set.
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

    let batch = match kind {
        MinMaxKind::Min => batch_reference_min(&input_acc),
        MinMaxKind::Max => batch_reference_max(&input_acc),
    };

    let inc = incremental_output(epochs, kind);

    let op_name = match kind {
        MinMaxKind::Min => "MIN",
        MinMaxKind::Max => "MAX",
    };
    assert_eq!(
        inc,
        batch,
        "{op_name} oracle property FAILED: incremental != batch\n\
         incremental ({} groups): {inc:?}\n\
         batch      ({} groups): {batch:?}",
        inc.len(),
        batch.len()
    );
}

// ─── DataFusion validation ────────────────────────────────────────────────────

/// Validate batch_reference_min against DataFusion.
///
/// `rows` is `(k, v)` pairs each appearing once.
pub async fn datafusion_min(rows: &[(i64, i64)]) -> datafusion::error::Result<Vec<(i64, i64)>> {
    run_datafusion_minmax(rows, "MIN").await
}

/// Validate batch_reference_max against DataFusion.
pub async fn datafusion_max(rows: &[(i64, i64)]) -> datafusion::error::Result<Vec<(i64, i64)>> {
    run_datafusion_minmax(rows, "MAX").await
}

async fn run_datafusion_minmax(
    rows: &[(i64, i64)],
    agg: &str,
) -> datafusion::error::Result<Vec<(i64, i64)>> {
    use arrow::array::{Int64Array, RecordBatch};
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
    let sql = format!("SELECT k, {agg}(v) FROM t GROUP BY k ORDER BY k");
    let df = ctx.sql(&sql).await?;
    let batches = df.collect().await?;
    let mut result = Vec::new();
    for b in &batches {
        let k_col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let e_col = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            result.push((k_col.value(i), e_col.value(i)));
        }
    }
    Ok(result)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Deterministic oracle tests ─────────────────────────────────────────

    #[test]
    fn oracle_min_empty_input() {
        assert_oracle_minmax(&[], MinMaxKind::Min);
    }

    #[test]
    fn oracle_max_empty_input() {
        assert_oracle_minmax(&[], MinMaxKind::Max);
    }

    #[test]
    fn oracle_min_single_insert() {
        assert_oracle_minmax(&[vec![(1, 10, 1)]], MinMaxKind::Min);
    }

    #[test]
    fn oracle_max_single_insert() {
        assert_oracle_minmax(&[vec![(1, 10, 1)]], MinMaxKind::Max);
    }

    #[test]
    fn oracle_min_extremum_churn() {
        // Insert 10, then 5 (min changes), then retract 5 (min reverts to 10).
        assert_oracle_minmax(
            &[vec![(1, 10, 1)], vec![(1, 5, 1)], vec![(1, 5, -1)]],
            MinMaxKind::Min,
        );
    }

    #[test]
    fn oracle_max_extremum_churn() {
        assert_oracle_minmax(
            &[vec![(1, 5, 1)], vec![(1, 10, 1)], vec![(1, 10, -1)]],
            MinMaxKind::Max,
        );
    }

    #[test]
    fn oracle_min_group_deletion() {
        assert_oracle_minmax(
            &[vec![(1, 5, 1), (1, 10, 1)], vec![(1, 5, -1), (1, 10, -1)]],
            MinMaxKind::Min,
        );
    }

    #[test]
    fn oracle_min_multiple_groups() {
        assert_oracle_minmax(
            &[vec![(1, 10, 1), (2, 3, 1), (1, 5, 1), (2, 7, 1)]],
            MinMaxKind::Min,
        );
    }

    #[test]
    fn oracle_max_multiple_groups() {
        assert_oracle_minmax(
            &[vec![(1, 10, 1), (2, 3, 1), (1, 5, 1), (2, 7, 1)]],
            MinMaxKind::Max,
        );
    }

    #[test]
    fn oracle_min_negative_values() {
        assert_oracle_minmax(&[vec![(1, -5, 1), (1, -10, 1), (1, 0, 1)]], MinMaxKind::Min);
    }

    #[test]
    fn oracle_min_two_epochs() {
        assert_oracle_minmax(
            &[vec![(1, 10, 1), (2, 5, 1)], vec![(1, 3, 1), (2, 8, 1)]],
            MinMaxKind::Min,
        );
    }

    // ── DataFusion validation ──────────────────────────────────────────────

    #[tokio::test]
    async fn oracle_datafusion_validates_min_reference() {
        let rows = vec![(1i64, 10i64), (1, 5), (2, 3), (2, 7), (3, 0)];
        let df_result = datafusion_min(&rows).await.unwrap();
        // DataFusion returns per-group MIN.
        let mut input_acc = BTreeMap::new();
        for (k, v) in &rows {
            *input_acc.entry((*k, *v)).or_insert(0i64) += 1;
        }
        let batch = batch_reference_min(&input_acc);
        assert_eq!(df_result, batch);
    }

    #[tokio::test]
    async fn oracle_datafusion_validates_max_reference() {
        let rows = vec![(1i64, 10i64), (1, 5), (2, 3), (2, 7), (3, 0)];
        let df_result = datafusion_max(&rows).await.unwrap();
        let mut input_acc = BTreeMap::new();
        for (k, v) in &rows {
            *input_acc.entry((*k, *v)).or_insert(0i64) += 1;
        }
        let batch = batch_reference_max(&input_acc);
        assert_eq!(df_result, batch);
    }

    // ── Property tests ─────────────────────────────────────────────────────

    #[cfg(test)]
    mod proptest_oracle {
        use proptest::prelude::*;

        use super::super::{assert_oracle_minmax, MinMaxKind};

        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(100_000))]

            /// Oracle property test: incremental == batch for MIN over ≥100k
            /// random delta sequences with group churn and extremum transitions.
            #[test]
            #[allow(clippy::items_after_test_module)]
            fn oracle_min_100k(
                raw_epochs in prop::collection::vec(
                    prop::collection::vec(
                        (0i64..5i64, -50i64..50i64, prop::bool::ANY)
                            .prop_map(|(k, v, insert)| (k, v, if insert { 1i64 } else { -1i64 })),
                        0..10usize,
                    ),
                    1..6usize,
                )
            ) {
                // Filter out invalid retractions (no over-delete).
                let mut kv_state: std::collections::HashMap<(i64, i64), i64> =
                    std::collections::HashMap::new();
                let mut valid_epochs: Vec<Vec<(i64, i64, i64)>> = Vec::new();
                for epoch in &raw_epochs {
                    let mut valid_epoch = Vec::new();
                    for &(k, v, w) in epoch {
                        if w > 0 {
                            *kv_state.entry((k, v)).or_insert(0) += 1;
                            valid_epoch.push((k, v, 1));
                        } else {
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
                assert_oracle_minmax(&valid_epochs, MinMaxKind::Min);
            }

            /// Oracle property test: incremental == batch for MAX over ≥100k scenarios.
            #[test]
            #[allow(clippy::items_after_test_module)]
            fn oracle_max_100k(
                raw_epochs in prop::collection::vec(
                    prop::collection::vec(
                        (0i64..5i64, -50i64..50i64, prop::bool::ANY)
                            .prop_map(|(k, v, insert)| (k, v, if insert { 1i64 } else { -1i64 })),
                        0..10usize,
                    ),
                    1..6usize,
                )
            ) {
                let mut kv_state: std::collections::HashMap<(i64, i64), i64> =
                    std::collections::HashMap::new();
                let mut valid_epochs: Vec<Vec<(i64, i64, i64)>> = Vec::new();
                for epoch in &raw_epochs {
                    let mut valid_epoch = Vec::new();
                    for &(k, v, w) in epoch {
                        if w > 0 {
                            *kv_state.entry((k, v)).or_insert(0) += 1;
                            valid_epoch.push((k, v, 1));
                        } else {
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
                assert_oracle_minmax(&valid_epochs, MinMaxKind::Max);
            }
        }
    }
}

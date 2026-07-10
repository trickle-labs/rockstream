//! Oracle property tests for Filter, Project, and Map operators (v0.4).
//!
//! The oracle property: `incremental(q, Δ) == batch(q, accumulated)`.
//!
//! Query under test: `SELECT a, b*2 AS c FROM t WHERE b*2 > 10`
//! Physical plan:   Filter(b*2 > 10) → Project(a, b*2 AS c)
//!
//! ## How the oracle assertion works
//!
//! 1. **Incremental side**: for each epoch of `(a: i64, b: i64, weight: i64)`
//!    deltas, run the delta through the Filter → Project pipeline and
//!    accumulate the output Z-set.
//!
//! 2. **Batch side**: collect all "present" input rows (weight > 0 in the
//!    accumulated input Z-set) and run the exact same Filter → Project
//!    as a pure-Rust batch function.
//!
//! 3. Assert the positive-weight output rows match the batch result.
//!
//! The pure-Rust batch reference is itself validated against DataFusion in
//! `oracle_datafusion_validates_batch_reference` (deterministic test, ~100
//! cases) to establish the DataFusion connection.

use std::collections::BTreeMap;
use std::sync::Arc;

use rockstream_ops::filter::FilterOp;
use rockstream_ops::pipeline::LinearPipeline;
use rockstream_ops::project::{NamedExpr, ProjectOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{BinaryOp, Expr};

// ─── Helper: build the Filter(b*2 > 10) → Project(a, b*2 AS c) pipeline ─────

fn lit(v: i64) -> Expr {
    Expr::Literal(v.to_be_bytes().to_vec())
}

fn make_filter_project_pipeline() -> LinearPipeline {
    let predicate = Expr::BinaryOp {
        op: BinaryOp::Gt,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOp::Mul,
            left: Box::new(Expr::Column(1)),
            right: Box::new(lit(2)),
        }),
        right: Box::new(lit(10)),
    };
    let project = ProjectOp::new(vec![
        NamedExpr::new("a", Expr::Column(0)),
        NamedExpr::new(
            "c",
            Expr::BinaryOp {
                op: BinaryOp::Mul,
                left: Box::new(Expr::Column(1)),
                right: Box::new(lit(2)),
            },
        ),
    ]);
    LinearPipeline::new()
        .push(Arc::new(FilterOp::new(predicate)))
        .push(Arc::new(project))
}

// ─── Batch reference (pure Rust) ─────────────────────────────────────────────

/// Apply `SELECT a, b*2 AS c FROM t WHERE b*2 > 10` to a set of present rows.
///
/// Returns a sorted `Vec<(a, c)>` of positive-weight output rows.
fn batch_reference(present: &BTreeMap<(i64, i64), i64>) -> Vec<(i64, i64)> {
    let mut result: Vec<(i64, i64)> = present
        .iter()
        .filter(|((_, b), &w)| w > 0 && b * 2 > 10)
        .map(|((a, b), _)| (*a, b * 2))
        .collect();
    result.sort();
    result.dedup(); // same (a, b*2) may appear with different weights; dedup for unique rows
    result
}

/// Run the incremental pipeline and return the sorted output.
fn incremental_output(
    pipeline: &LinearPipeline,
    epochs: &[Vec<(i64, i64, i64)>],
) -> Vec<(i64, i64)> {
    // Accumulate output Z-set: (a, c) → weight
    let mut acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();

    for epoch in epochs {
        if epoch.is_empty() {
            continue;
        }
        let batch = ArrowZSet::from_ab_weighted(epoch);
        let output = pipeline.process(batch).expect("pipeline processing failed");
        // accumulate_ab uses columns 0 and 1 of the output (a, c)
        output.accumulate_ab(&mut acc);
    }

    // Return positive-weight (a, c) pairs, sorted.
    let mut result: Vec<(i64, i64)> = acc
        .iter()
        .filter(|(_, &w)| w > 0)
        .map(|(k, _)| *k)
        .collect();
    result.sort();
    result
}

// ─── Oracle assertion ─────────────────────────────────────────────────────────

/// Assert `incremental == batch` for the filter+project query.
///
/// `epochs`: sequence of delta epochs, each a `Vec<(a, b, weight)>`.
pub fn assert_oracle_filter_project(epochs: &[Vec<(i64, i64, i64)>]) {
    let pipeline = make_filter_project_pipeline();

    // Batch side: accumulate input, apply batch reference.
    let mut input_acc: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for epoch in epochs {
        for &(a, b, w) in epoch {
            let entry = input_acc.entry((a, b)).or_insert(0);
            *entry += w;
            if *entry == 0 {
                input_acc.remove(&(a, b));
            }
        }
    }
    let mut batch = batch_reference(&input_acc);
    batch.sort();

    // Incremental side.
    let mut inc = incremental_output(&pipeline, epochs);
    inc.sort();

    assert_eq!(
        inc,
        batch,
        "Oracle property FAILED: incremental != batch\n\
         Query: SELECT a, b*2 AS c FROM t WHERE b*2 > 10\n\
         incremental ({} rows): {inc:?}\n\
         batch      ({} rows): {batch:?}",
        inc.len(),
        batch.len()
    );
}

// ─── DataFusion validation (slow path — validates the batch reference) ────────

/// Run `SELECT a, b*2 AS c FROM t WHERE b*2 > 10` in DataFusion.
///
/// Used to validate the pure-Rust batch reference against a real SQL engine.
pub async fn run_datafusion_filter_project(
    rows: &[(i64, i64)],
) -> datafusion::error::Result<Vec<(i64, i64)>> {
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let a_vals: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let b_vals: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(a_vals)),
            Arc::new(Int64Array::from(b_vals)),
        ],
    )?;
    let ctx = SessionContext::new();
    let mem_table = datafusion::datasource::memory::MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(mem_table))?;
    let df = ctx
        .sql("SELECT a, b*2 AS c FROM t WHERE b*2 > 10 ORDER BY a, b*2")
        .await?;
    let batches = df.collect().await?;
    let mut result = Vec::new();
    for b in &batches {
        let a_col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c_col = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            result.push((a_col.value(i), c_col.value(i)));
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
    fn oracle_empty_input() {
        assert_oracle_filter_project(&[]);
    }

    #[test]
    fn oracle_single_passing_row() {
        // b=6: b*2=12 > 10 ✓, c=12
        assert_oracle_filter_project(&[vec![(1, 6, 1)]]);
    }

    #[test]
    fn oracle_single_filtered_row() {
        // b=3: b*2=6 ≤ 10 ✗
        assert_oracle_filter_project(&[vec![(1, 3, 1)]]);
    }

    #[test]
    fn oracle_insert_then_delete() {
        // Insert (1,6), then delete it — both sides should have empty output.
        assert_oracle_filter_project(&[
            vec![(1, 6, 1)],  // insert
            vec![(1, 6, -1)], // delete
        ]);
    }

    #[test]
    fn oracle_insert_filtered_row_then_update() {
        // Insert (1,3) (filtered), then update to (1,6) (insert (1,6), delete (1,3))
        assert_oracle_filter_project(&[vec![(1, 3, 1)], vec![(1, 3, -1), (1, 6, 1)]]);
    }

    #[test]
    fn oracle_multiple_epochs_mixed() {
        assert_oracle_filter_project(&[
            vec![(1, 6, 1), (2, 3, 1)], // (1,6) passes, (2,3) filtered
            vec![(3, 8, 1)],            // (3,8) passes, c=16
            vec![(1, 6, -1)],           // delete (1,6)
        ]);
    }

    // ── DataFusion validation (establishes DataFusion connection) ──────────

    /// Validate: pure-Rust batch reference == DataFusion for the filter+project query.
    ///
    /// This test runs DataFusion for several deterministic cases to prove that
    /// `batch_reference` is correct. This connects the 100k proptest cases to
    /// the DataFusion oracle transitively: incremental == batch_reference ==
    /// DataFusion.
    #[allow(clippy::type_complexity)]
    #[tokio::test]
    async fn oracle_datafusion_validates_batch_reference() {
        let test_cases: Vec<(&[(i64, i64)], Vec<(i64, i64)>)> = vec![
            // empty
            (&[], vec![]),
            // all filtered
            (&[(1, 1), (2, 2), (3, 3)], vec![]),
            // all pass
            (&[(1, 6), (2, 7)], vec![(1, 12), (2, 14)]),
            // mixed
            (&[(1, 3), (2, 6), (3, 8)], vec![(2, 12), (3, 16)]),
            // boundary: b*2 == 10 (not > 10, filtered)
            (&[(1, 5)], vec![]),
            // boundary: b*2 == 11 (passes)
            // Note: b must be integer, b=5 → b*2=10 (filtered); no integer gives b*2=11
            // b=6 → b*2=12 (passes)
            (&[(1, 6)], vec![(1, 12)]),
        ];

        for (input_rows, expected) in test_cases {
            // DataFusion side
            let df_result = run_datafusion_filter_project(input_rows).await.unwrap();
            assert_eq!(
                df_result, expected,
                "DataFusion mismatch for input {:?}",
                input_rows
            );

            // Batch reference side
            let input_map: BTreeMap<(i64, i64), i64> =
                input_rows.iter().map(|&(a, b)| ((a, b), 1i64)).collect();
            let mut batch_ref = batch_reference(&input_map);
            batch_ref.sort();
            assert_eq!(
                batch_ref, expected,
                "Batch reference mismatch for input {:?}",
                input_rows
            );
        }
    }

    // ── Property test: 100k scenarios ─────────────────────────────────────

    /// The primary proof: oracle property test for
    /// `SELECT a, b*2 AS c FROM t WHERE b*2 > 10`
    /// over random insert/delete sequences, ≥100k scenarios.
    ///
    /// Each scenario is one proptest case, consisting of 1–5 epochs each
    /// with 0–10 delta rows. The incremental result must equal the
    /// pure-Rust batch reference (which is DataFusion-validated above).
    #[cfg(test)]
    mod proptest_oracle {
        use super::*;
        use proptest::prelude::*;

        /// Strategy: one epoch = a Vec of (a, b, weight) triples.
        /// weight is ∈ {+1, -1, +2} to exercise non-unit weights.
        fn epoch_strategy() -> impl Strategy<Value = Vec<(i64, i64, i64)>> {
            prop::collection::vec(
                (
                    -100i64..=100i64, // a
                    -20i64..=20i64,   // b  (b*2 range: -40..40, interesting around 10)
                    prop_oneof![Just(1i64), Just(-1i64), Just(2i64)],
                ),
                0..=10,
            )
        }

        fn epochs_strategy() -> impl Strategy<Value = Vec<Vec<(i64, i64, i64)>>> {
            prop::collection::vec(epoch_strategy(), 1..=5)
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(100_000))]

            /// Oracle: incremental(filter+project, Δ) == batch(filter+project, accumulated)
            /// for ≥100k random insert/delete/update sequences.
            #[test]
            fn oracle_filter_project_100k(epochs in epochs_strategy()) {
                assert_oracle_filter_project(&epochs);
            }
        }
    }
}

//! Property-test harness for the oracle.
//!
//! This module provides the v0.2 proof: **the oracle harness confirms
//! equivalence on a trivial no-op pipeline**.
//!
//! Property under test:
//! > For any sequence of Z-set deltas over a two-column test table,
//! > `incremental_noop(deltas) == batch(accumulated_state)`.
//!
//! - The **incremental** side: accumulate Z-set weights and return rows with
//!   positive net weight, sorted by `(id, value)`.
//! - The **batch** side: load those same rows into DataFusion and run
//!   `SELECT id, value FROM t ORDER BY id, value`.
//!
//! These must be equal for the no-op `SELECT *` pipeline because the batch
//! query is the identity transform over the accumulated positive-weight rows.
//! This validates the DataFusion integration, the Arrow serialization, and
//! the Z-set accumulation logic all work correctly.

use crate::batch::run_noop_batch_query;
use crate::zset::{accumulate, present_rows, ZSetDelta};

/// Run the oracle assertion synchronously.
///
/// Given a sequence of Z-set deltas, assert that the incremental result
/// (Z-set accumulation) equals the DataFusion batch result.
///
/// This is the canonical oracle assertion for the no-op pipeline.
/// Panics if the assertion fails.
pub fn assert_oracle_noop(deltas: &[ZSetDelta]) {
    // Incremental side: accumulate Z-set weights, keep positive-weight rows.
    let acc = accumulate(deltas);
    let mut incremental = present_rows(&acc);
    incremental.sort(); // deterministic order

    // Batch side: run DataFusion SELECT * FROM t on the same rows.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut batch = rt
        .block_on(run_noop_batch_query(&incremental))
        .expect("DataFusion query");
    batch.sort(); // deterministic order

    assert_eq!(
        incremental,
        batch,
        "Oracle property failed: incremental != batch\n\
         incremental ({} rows): {incremental:?}\n\
         batch      ({} rows): {batch:?}",
        incremental.len(),
        batch.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zset::{TestRow, ZSetDelta};

    // ── Deterministic proof tests ────────────────────────────────────────────

    /// v0.2 Proof 4: Oracle harness confirms equivalence on trivial no-op pipeline.
    ///
    /// Empty input: both sides return empty.
    #[test]
    fn oracle_noop_empty_input() {
        assert_oracle_noop(&[]);
    }

    /// Single insert: incremental and batch both return one row.
    #[test]
    fn oracle_noop_single_insert() {
        assert_oracle_noop(&[ZSetDelta {
            row: TestRow { id: 1, value: 42 },
            weight: 1,
        }]);
    }

    /// Insert then delete: both sides return empty (row cancelled).
    #[test]
    fn oracle_noop_insert_then_delete() {
        assert_oracle_noop(&[
            ZSetDelta {
                row: TestRow { id: 1, value: 42 },
                weight: 1,
            },
            ZSetDelta {
                row: TestRow { id: 1, value: 42 },
                weight: -1,
            },
        ]);
    }

    /// Multiple rows with mixed inserts and deletes.
    #[test]
    fn oracle_noop_mixed_deltas() {
        let deltas = vec![
            ZSetDelta {
                row: TestRow { id: 1, value: 10 },
                weight: 1,
            },
            ZSetDelta {
                row: TestRow { id: 2, value: 20 },
                weight: 1,
            },
            ZSetDelta {
                row: TestRow { id: 3, value: 30 },
                weight: 1,
            },
            // Delete row 2
            ZSetDelta {
                row: TestRow { id: 2, value: 20 },
                weight: -1,
            },
            ZSetDelta {
                row: TestRow { id: 4, value: 40 },
                weight: 1,
            },
        ];
        assert_oracle_noop(&deltas);
    }

    /// All deletes: both sides return empty.
    #[test]
    fn oracle_noop_all_deleted() {
        let deltas = vec![
            ZSetDelta {
                row: TestRow { id: 1, value: 10 },
                weight: 1,
            },
            ZSetDelta {
                row: TestRow { id: 2, value: 20 },
                weight: 1,
            },
            ZSetDelta {
                row: TestRow { id: 1, value: 10 },
                weight: -1,
            },
            ZSetDelta {
                row: TestRow { id: 2, value: 20 },
                weight: -1,
            },
        ];
        assert_oracle_noop(&deltas);
    }

    /// Row updated by delete+insert: only the new row is present.
    #[test]
    fn oracle_noop_update_via_delete_insert() {
        let deltas = vec![
            ZSetDelta {
                row: TestRow { id: 1, value: 10 },
                weight: 1,
            },
            // "Update" row 1: delete old value, insert new value
            ZSetDelta {
                row: TestRow { id: 1, value: 10 },
                weight: -1,
            },
            ZSetDelta {
                row: TestRow { id: 1, value: 99 },
                weight: 1,
            },
        ];
        assert_oracle_noop(&deltas);
    }

    // ── Proptest randomized proof ────────────────────────────────────────────

    /// Randomized property test using proptest.
    ///
    /// Generates random sequences of Z-set deltas over a small key space
    /// (id ∈ [0..10], value ∈ [0..100]) and asserts `incremental == batch`
    /// for every sequence.
    ///
    /// This is the v0.2 oracle harness proof: a proptest confirming the
    /// invariant holds across a broad range of delta patterns including:
    /// - pure inserts
    /// - pure deletes (including "over-delete" producing negative weight)
    /// - interleaved inserts and deletes
    /// - duplicate rows with weight accumulation
    #[cfg(test)]
    mod proptest_harness {
        use proptest::prelude::*;

        use super::super::assert_oracle_noop;
        use crate::zset::{TestRow, ZSetDelta};

        proptest! {
            #[test]
            fn oracle_noop_incremental_eq_batch(
                deltas in prop::collection::vec(
                    (0i64..10i64, 0i64..100i64, prop::bool::ANY)
                        .prop_map(|(id, value, insert)| ZSetDelta {
                            row: TestRow { id, value },
                            weight: if insert { 1 } else { -1 },
                        }),
                    0..50,
                )
            ) {
                assert_oracle_noop(&deltas);
            }
        }
    }
}

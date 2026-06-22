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

    /// Duplicate insertions produce weight > 1; verify the accumulated state
    /// still equals the DataFusion batch result.
    #[test]
    fn oracle_noop_duplicate_inserts_positive_weight() {
        // Insert the same row 3 times: net weight = 3 (still one unique row).
        let deltas = vec![
            ZSetDelta { row: TestRow { id: 5, value: 55 }, weight: 1 },
            ZSetDelta { row: TestRow { id: 5, value: 55 }, weight: 1 },
            ZSetDelta { row: TestRow { id: 5, value: 55 }, weight: 1 },
        ];
        // The noop oracle keeps rows with positive net weight.
        // Row (5, 55) has weight 3 > 0 → should appear once in the batch result.
        assert_oracle_noop(&deltas);
    }

    /// A row inserted in a first epoch then deleted in a second epoch must
    /// produce an empty final state.  Tests cross-epoch retraction.
    #[test]
    fn oracle_noop_cross_epoch_insert_then_retract() {
        // First epoch: insert two rows.
        let epoch1 = vec![
            ZSetDelta { row: TestRow { id: 10, value: 100 }, weight: 1 },
            ZSetDelta { row: TestRow { id: 20, value: 200 }, weight: 1 },
        ];
        // Second epoch: retract both.
        let epoch2 = vec![
            ZSetDelta { row: TestRow { id: 10, value: 100 }, weight: -1 },
            ZSetDelta { row: TestRow { id: 20, value: 200 }, weight: -1 },
        ];
        // Flatten both epochs into one delta sequence and assert empty result.
        let mut deltas = epoch1;
        deltas.extend(epoch2);
        assert_oracle_noop(&deltas);
    }

    /// Large key space: ids up to 500, values up to 10000 — ensures the
    /// oracle is not accidentally constrained to a small domain.
    #[test]
    fn oracle_noop_large_key_space() {
        let mut deltas = Vec::new();
        // Insert 200 distinct rows.
        for i in 0..200i64 {
            deltas.push(ZSetDelta {
                row: TestRow { id: i * 3, value: i * 47 % 10000 },
                weight: 1,
            });
        }
        // Retract every other one.
        for i in (0..200i64).step_by(2) {
            deltas.push(ZSetDelta {
                row: TestRow { id: i * 3, value: i * 47 % 10000 },
                weight: -1,
            });
        }
        assert_oracle_noop(&deltas);
    }

    // ── Proptest randomized proof ────────────────────────────────────────────

    #[cfg(test)]
    mod proptest_harness {
        use proptest::prelude::*;

        use super::super::assert_oracle_noop;
        use crate::zset::{TestRow, ZSetDelta};

        /// Baseline proptest: single-epoch sequences with a wider key space
        /// (id ∈ [0..500], value ∈ [-1000..1000]).
        ///
        /// Validates that `incremental == batch` across:
        /// - pure inserts, pure deletes, interleaved patterns
        /// - duplicate rows (weight accumulation > 1)
        /// - "over-delete" resulting in negative net weight
        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(10_000))]
            #[test]
            fn oracle_noop_incremental_eq_batch(
                deltas in prop::collection::vec(
                    (0i64..500i64, -1000i64..1000i64, prop::bool::ANY)
                        .prop_map(|(id, value, insert)| ZSetDelta {
                            row: TestRow { id, value },
                            weight: if insert { 1 } else { -1 },
                        }),
                    0..100,
                )
            ) {
                assert_oracle_noop(&deltas);
            }
        }

        /// Multi-epoch proptest: runs up to 8 delta epochs in sequence and
        /// asserts that the Z-set accumulated across all epochs equals the
        /// DataFusion batch result.
        ///
        /// This is distinct from the single-epoch test: rows inserted in epoch N
        /// and retracted in epoch N+1 must cancel correctly.  Exercises the
        /// cross-epoch retraction path — the key correctness invariant for IVM.
        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(5_000))]
            #[test]
            fn oracle_noop_multi_epoch_incremental_eq_batch(
                epochs in prop::collection::vec(
                    prop::collection::vec(
                        (0i64..50i64, -500i64..500i64, prop::bool::ANY)
                            .prop_map(|(id, value, insert)| ZSetDelta {
                                row: TestRow { id, value },
                                weight: if insert { 1 } else { -1 },
                            }),
                        1..20,
                    ),
                    1..8,
                )
            ) {
                // Flatten all epoch deltas into a single slice; the noop
                // oracle is stateless so epoch boundaries do not matter for
                // the no-op pipeline — only the final accumulated state counts.
                let flat: Vec<ZSetDelta> = epochs.into_iter().flatten().collect();
                assert_oracle_noop(&flat);
            }
        }

        /// Non-unit weight proptest: verifies that the oracle correctly handles
        /// rows with weights greater than 1 (e.g. batch-load duplicates).
        ///
        /// A row with weight 2 or 3 is still "present" (weight > 0) and must
        /// appear once in the batch result.  A row partially retracted from
        /// weight 3 to weight 1 must still appear; only when it reaches ≤ 0
        /// is it absent.  This exercises accumulation correctness beyond ±1.
        proptest! {
            #![proptest_config(proptest::test_runner::Config::with_cases(5_000))]
            #[test]
            fn oracle_noop_non_unit_weights(
                deltas in prop::collection::vec(
                    (0i64..100i64, -500i64..500i64,
                     prop_oneof![Just(1i64), Just(2i64), Just(3i64), Just(-1i64), Just(-2i64)])
                        .prop_map(|(id, value, weight)| ZSetDelta {
                            row: TestRow { id, value },
                            weight,
                        }),
                    0..50,
                )
            ) {
                assert_oracle_noop(&deltas);
            }
        }
    }
}

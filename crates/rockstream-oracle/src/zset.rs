//! Z-set accumulation for the oracle harness.
//!
//! A Z-set is the fundamental data structure of IVM: a finite map from rows
//! to integer weights. Positive weight means the row is "in" the relation;
//! negative weight represents a deletion; zero means the row is absent.
//!
//! This module provides the accumulation logic used by the oracle harness to
//! track the "current state" of a base table across a sequence of deltas.

use std::collections::BTreeMap;

/// A simple two-column test row: `(id: i64, value: i64)`.
///
/// Used as the canonical row type for the v0.2 oracle harness. Later versions
/// will generalize to Arrow `RecordBatch` rows.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TestRow {
    /// Unique row identifier.
    pub id: i64,
    /// Row payload value.
    pub value: i64,
}

/// A Z-set delta: a row paired with a weight.
///
/// - `weight = +1` — insert (row appears in the relation)
/// - `weight = -1` — delete (row is removed from the relation)
/// - Other weights are supported by the accumulator (DBSP allows arbitrary integer weights).
#[derive(Debug, Clone)]
pub struct ZSetDelta {
    /// The row being inserted or deleted.
    pub row: TestRow,
    /// The delta weight (+1 = insert, -1 = delete).
    pub weight: i64,
}

/// Accumulate a sequence of Z-set deltas into a weight map.
///
/// Returns a `BTreeMap<TestRow, i64>` containing only entries whose net weight
/// is non-zero. Entries that reach zero weight are removed.
///
/// # Example
/// ```
/// use rockstream_oracle::zset::{TestRow, ZSetDelta, accumulate};
/// let deltas = vec![
///     ZSetDelta { row: TestRow { id: 1, value: 10 }, weight: 1 },
///     ZSetDelta { row: TestRow { id: 1, value: 10 }, weight: -1 }, // cancels
///     ZSetDelta { row: TestRow { id: 2, value: 20 }, weight: 1 },
/// ];
/// let acc = accumulate(&deltas);
/// assert!(!acc.contains_key(&TestRow { id: 1, value: 10 })); // cancelled
/// assert_eq!(acc[&TestRow { id: 2, value: 20 }], 1);
/// ```
pub fn accumulate(deltas: &[ZSetDelta]) -> BTreeMap<TestRow, i64> {
    let mut acc: BTreeMap<TestRow, i64> = BTreeMap::new();
    for delta in deltas {
        let entry = acc.entry(delta.row.clone()).or_insert(0);
        *entry += delta.weight;
        if *entry == 0 {
            acc.remove(&delta.row);
        }
    }
    acc
}

/// Return the "present" rows: those whose net weight is strictly positive.
///
/// In IVM semantics, a row is "in the relation" when its net weight > 0.
/// Rows with negative or zero weight are considered absent.
///
/// The result is sorted by `(id, value)` for deterministic comparison.
pub fn present_rows(acc: &BTreeMap<TestRow, i64>) -> Vec<TestRow> {
    let mut rows: Vec<TestRow> = acc
        .iter()
        .filter(|(_, &w)| w > 0)
        .map(|(r, _)| r.clone())
        .collect();
    rows.sort();
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulate_empty() {
        let acc = accumulate(&[]);
        assert!(acc.is_empty());
    }

    #[test]
    fn accumulate_single_insert() {
        let deltas = vec![ZSetDelta {
            row: TestRow { id: 1, value: 42 },
            weight: 1,
        }];
        let acc = accumulate(&deltas);
        assert_eq!(acc[&TestRow { id: 1, value: 42 }], 1);
    }

    #[test]
    fn accumulate_insert_then_delete_cancels() {
        let deltas = vec![
            ZSetDelta {
                row: TestRow { id: 1, value: 10 },
                weight: 1,
            },
            ZSetDelta {
                row: TestRow { id: 1, value: 10 },
                weight: -1,
            },
        ];
        let acc = accumulate(&deltas);
        assert!(!acc.contains_key(&TestRow { id: 1, value: 10 }));
    }

    #[test]
    fn accumulate_multiple_inserts_accumulate_weight() {
        let deltas = vec![
            ZSetDelta {
                row: TestRow { id: 1, value: 10 },
                weight: 1,
            },
            ZSetDelta {
                row: TestRow { id: 1, value: 10 },
                weight: 1,
            },
        ];
        let acc = accumulate(&deltas);
        assert_eq!(acc[&TestRow { id: 1, value: 10 }], 2);
    }

    #[test]
    fn present_rows_excludes_negative_weights() {
        let mut acc = BTreeMap::new();
        acc.insert(TestRow { id: 1, value: 10 }, 1_i64);
        acc.insert(TestRow { id: 2, value: 20 }, -1_i64);
        acc.insert(TestRow { id: 3, value: 30 }, 2_i64);
        let rows = present_rows(&acc);
        assert_eq!(
            rows,
            vec![TestRow { id: 1, value: 10 }, TestRow { id: 3, value: 30 }]
        );
    }

    #[test]
    fn present_rows_sorted() {
        let mut acc = BTreeMap::new();
        acc.insert(TestRow { id: 5, value: 50 }, 1_i64);
        acc.insert(TestRow { id: 1, value: 10 }, 1_i64);
        acc.insert(TestRow { id: 3, value: 30 }, 1_i64);
        let rows = present_rows(&acc);
        assert_eq!(
            rows,
            vec![
                TestRow { id: 1, value: 10 },
                TestRow { id: 3, value: 30 },
                TestRow { id: 5, value: 50 },
            ]
        );
    }
}

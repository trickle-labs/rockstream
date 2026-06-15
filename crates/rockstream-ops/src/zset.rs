//! Arrow-based Z-set batch type for IVM operators.
//!
//! An `ArrowZSet` is the runtime data type between operators: an Arrow
//! `RecordBatch` containing the row data (user schema, without `_weight`)
//! paired with a `Vec<i64>` of per-row delta weights.
//!
//! - Positive weight: row is being inserted.
//! - Negative weight: row is being retracted (deleted).
//! - Zero weight: no-op (produced by cancellation; operators may compact these).
//!
//! The `_weight` column convention from `rockstream_types::arrow_batch` is
//! used for serialisation and I/O, but during in-process computation the
//! weights are kept separate for performance.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::error::OpError;

/// An Arrow-based Z-set delta batch.
///
/// The `data` batch holds the user-visible columns; `weights` holds the
/// per-row IVM weights. Both have the same number of rows.
#[derive(Debug, Clone)]
pub struct ArrowZSet {
    /// Row data (user schema, no `_weight` column).
    pub data: RecordBatch,
    /// Per-row delta weights. `weights[i]` corresponds to `data.column(j)[i]`.
    pub weights: Vec<i64>,
    /// Optional progress frontier associated with this Z-set.
    pub frontier: Option<rockstream_types::frontier::FreshnessToken>,
}

impl ArrowZSet {
    /// Create an `ArrowZSet` from a data batch and weight vector.
    ///
    /// # Panics
    /// Panics if `data.num_rows() != weights.len()`.
    pub fn new(data: RecordBatch, weights: Vec<i64>) -> Self {
        assert_eq!(
            data.num_rows(),
            weights.len(),
            "ArrowZSet: data rows ({}) != weights len ({})",
            data.num_rows(),
            weights.len()
        );
        ArrowZSet {
            data,
            weights,
            frontier: None,
        }
    }

    pub fn with_frontier(mut self, frontier: rockstream_types::frontier::FreshnessToken) -> Self {
        self.frontier = Some(frontier);
        self
    }

    /// Number of rows in this batch.
    pub fn num_rows(&self) -> usize {
        self.data.num_rows()
    }

    /// True if this batch contains no rows.
    pub fn is_empty(&self) -> bool {
        self.data.num_rows() == 0
    }

    /// Return the schema of the user data (without `_weight`).
    pub fn schema(&self) -> SchemaRef {
        self.data.schema()
    }

    /// Create an empty `ArrowZSet` with the given schema.
    pub fn empty(schema: SchemaRef) -> Self {
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
                _ => Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            })
            .collect();
        let data = RecordBatch::try_new(schema, columns).expect("empty batch");
        ArrowZSet {
            data,
            weights: Vec::new(),
            frontier: None,
        }
    }

    /// Build an `ArrowZSet` from a list of `(a: i64, b: i64)` rows with a
    /// uniform weight. Convenience constructor for tests.
    pub fn from_ab_rows(rows: &[(i64, i64)], weight: i64) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let a_vals: Vec<i64> = rows.iter().map(|(a, _)| *a).collect();
        let b_vals: Vec<i64> = rows.iter().map(|(_, b)| *b).collect();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(a_vals)),
            Arc::new(Int64Array::from(b_vals)),
        ];
        let data = RecordBatch::try_new(schema, cols).expect("from_ab_rows");
        let weights = vec![weight; rows.len()];
        ArrowZSet {
            data,
            weights,
            frontier: None,
        }
    }

    /// Build an `ArrowZSet` from `(a: i64, b: i64, weight: i64)` triples.
    ///
    /// Used by the oracle harness to process epoch deltas that carry
    /// per-row weights.
    pub fn from_ab_weighted(rows: &[(i64, i64, i64)]) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let a_vals: Vec<i64> = rows.iter().map(|(a, _, _)| *a).collect();
        let b_vals: Vec<i64> = rows.iter().map(|(_, b, _)| *b).collect();
        let weights: Vec<i64> = rows.iter().map(|(_, _, w)| *w).collect();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(a_vals)),
            Arc::new(Int64Array::from(b_vals)),
        ];
        let data = RecordBatch::try_new(schema, cols).expect("from_ab_weighted");
        ArrowZSet {
            data,
            weights,
            frontier: None,
        }
    }

    /// Compact the Z-set: remove rows whose weight is zero.
    pub fn compact(self) -> Self {
        let mask: Vec<bool> = self.weights.iter().map(|&w| w != 0).collect();
        if mask.iter().all(|&b| b) {
            return self; // nothing to remove
        }
        let bool_array = arrow::array::BooleanArray::from(mask.clone());
        let filtered_cols: Vec<ArrayRef> = self
            .data
            .columns()
            .iter()
            .map(|col| arrow::compute::filter(col.as_ref(), &bool_array).expect("compact filter"))
            .collect();
        let new_data =
            RecordBatch::try_new(self.data.schema(), filtered_cols).expect("compact batch");
        let new_weights: Vec<i64> = mask
            .iter()
            .zip(&self.weights)
            .filter(|(b, _)| **b)
            .map(|(_, w)| *w)
            .collect();
        ArrowZSet {
            data: new_data,
            weights: new_weights,
            frontier: self.frontier,
        }
    }

    /// Return the positive-weight rows as `(a: i64, b: i64)` pairs.
    /// Only works if schema is `{a: Int64, b: Int64}`. Used in tests.
    pub fn positive_ab_rows(&self) -> Vec<(i64, i64)> {
        if self.is_empty() {
            return Vec::new();
        }
        let a_col = self
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 0 must be Int64");
        let b_col = self
            .data
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column 1 must be Int64");
        (0..self.num_rows())
            .filter(|&i| self.weights[i] > 0)
            .map(|i| (a_col.value(i), b_col.value(i)))
            .collect()
    }

    /// Accumulate this batch into a weight map.
    ///
    /// Returns a `Vec<(row_bytes, weight)>` using a stable key representation.
    /// For the oracle test, use `accumulate_ab` instead.
    pub fn accumulate_ab(&self, acc: &mut std::collections::BTreeMap<(i64, i64), i64>) {
        if self.is_empty() {
            return;
        }
        let a_col = self.data.column(0).as_any().downcast_ref::<Int64Array>();
        let b_col = self.data.column(1).as_any().downcast_ref::<Int64Array>();
        if let (Some(a), Some(b)) = (a_col, b_col) {
            for i in 0..self.num_rows() {
                let key = (a.value(i), b.value(i));
                let entry = acc.entry(key).or_insert(0);
                *entry += self.weights[i];
                if *entry == 0 {
                    acc.remove(&key);
                }
            }
        }
    }

    /// Filter by indices: return only the rows at the given positions.
    pub fn select_rows(&self, indices: &[usize]) -> Result<ArrowZSet, OpError> {
        if indices.is_empty() {
            return Ok(ArrowZSet::empty(self.data.schema()));
        }
        let n = self.num_rows();
        let mut mask = vec![false; n];
        for &i in indices {
            if i < n {
                mask[i] = true;
            }
        }
        let bool_array = arrow::array::BooleanArray::from(mask.clone());
        let filtered_cols: Vec<ArrayRef> = self
            .data
            .columns()
            .iter()
            .map(|col| arrow::compute::filter(col.as_ref(), &bool_array).map_err(OpError::arrow))
            .collect::<Result<_, _>>()?;
        let new_data =
            RecordBatch::try_new(self.data.schema(), filtered_cols).map_err(OpError::arrow)?;
        let new_weights: Vec<i64> = mask
            .iter()
            .zip(&self.weights)
            .filter(|(b, _)| **b)
            .map(|(_, w)| *w)
            .collect();
        Ok(ArrowZSet {
            data: new_data,
            weights: new_weights,
            frontier: self.frontier.clone(),
        })
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_ab_rows_schema() {
        let zs = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20)], 1);
        assert_eq!(zs.num_rows(), 2);
        assert_eq!(zs.weights, vec![1, 1]);
        assert_eq!(zs.schema().field(0).name(), "a");
        assert_eq!(zs.schema().field(1).name(), "b");
    }

    #[test]
    fn compact_removes_zero_weights() {
        let zs = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (3, 30)], 1);
        // Manually set middle weight to 0
        let weights = vec![1, 0, 1];
        let zs2 = ArrowZSet {
            data: zs.data,
            weights,
            frontier: None,
        };

        let compacted = zs2.compact();
        assert_eq!(compacted.num_rows(), 2);
        assert_eq!(compacted.weights, vec![1, 1]);
    }

    #[test]
    fn select_rows_subset() {
        let zs = ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (3, 30)], 1);
        let sub = zs.select_rows(&[0, 2]).unwrap();
        assert_eq!(sub.num_rows(), 2);
        let rows = sub.positive_ab_rows();
        assert_eq!(rows, vec![(1, 10), (3, 30)]);
    }
}

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

use arrow::array::{ArrayRef, Int64Array, UInt64Array};
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
    /// Fallible constructor for adapter boundaries.
    pub fn try_new(data: RecordBatch, weights: Vec<i64>) -> Result<Self, OpError> {
        if !rockstream_verified::zset::validate_aligned_lengths(data.num_rows(), weights.len()) {
            return Err(OpError::invalid_literal(format!(
                "Z-set row/weight length mismatch: {} rows, {} weights",
                data.num_rows(),
                weights.len()
            )));
        }
        Ok(Self {
            data,
            weights,
            frontier: None,
        })
    }

    /// Create an `ArrowZSet` from a data batch and weight vector.
    ///
    /// # Panics
    /// Panics if `data.num_rows() != weights.len()`.
    pub fn new(data: RecordBatch, weights: Vec<i64>) -> Self {
        Self::try_new(data, weights).expect("ArrowZSet: data rows != weights len")
    }

    /// Validate the executable Z-set boundary, including public-field edits.
    pub fn validate(&self) -> Result<(), OpError> {
        if rockstream_verified::zset::validate_aligned_lengths(
            self.data.num_rows(),
            self.weights.len(),
        ) {
            Ok(())
        } else {
            Err(OpError::invalid_literal(format!(
                "Z-set row/weight length mismatch: {} rows, {} weights",
                self.data.num_rows(),
                self.weights.len()
            )))
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
        use arrow::array::{BooleanArray, Float64Array, StringArray};
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Utf8 => Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
                DataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())) as ArrayRef,
                DataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef,
                _ => Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            })
            .collect();
        let data = RecordBatch::try_new(schema, columns).expect("empty batch");
        Self::try_new(data, Vec::new()).expect("empty ArrowZSet")
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
    ///
    /// Equal rows remain separate; duplicate consolidation is a separate operation.
    pub fn compact(self) -> Self {
        self.validate()
            .expect("ArrowZSet: invalid row/weight alignment");
        let mask: Vec<bool> = self
            .weights
            .iter()
            .map(|&w| !rockstream_verified::zset::weight_cancels(w))
            .collect();
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
    pub fn try_accumulate_ab(
        &self,
        acc: &mut std::collections::BTreeMap<(i64, i64), i64>,
    ) -> Result<(), OpError> {
        self.validate()?;
        if self.is_empty() {
            return Ok(());
        }
        let a_col = self.data.column(0).as_any().downcast_ref::<Int64Array>();
        let b_col = self.data.column(1).as_any().downcast_ref::<Int64Array>();
        if let (Some(a), Some(b)) = (a_col, b_col) {
            for i in 0..self.num_rows() {
                let key = (a.value(i), b.value(i));
                let current = acc.get(&key).copied().unwrap_or(0);
                let next = rockstream_verified::zset::consolidate_weight(current, self.weights[i])
                    .ok_or_else(|| OpError::numeric_overflow("Z-set weight consolidation"))?;
                if rockstream_verified::zset::weight_cancels(next) {
                    acc.remove(&key);
                } else {
                    acc.insert(key, next);
                }
            }
        }
        Ok(())
    }

    /// Accumulate this test/oracle batch, preserving its historical panic-on-invalid API.
    pub fn accumulate_ab(&self, acc: &mut std::collections::BTreeMap<(i64, i64), i64>) {
        self.try_accumulate_ab(acc)
            .expect("Z-set weight consolidation failed")
    }

    /// Gather rows in the requested order, preserving repeated indices.
    pub fn select_rows(&self, indices: &[usize]) -> Result<ArrowZSet, OpError> {
        self.validate()?;
        if indices.is_empty() {
            let mut empty = ArrowZSet::empty(self.data.schema());
            empty.frontier = self.frontier.clone();
            return Ok(empty);
        }
        let n = self.num_rows();
        let mut take_indices = Vec::with_capacity(indices.len());
        for &i in indices {
            if !rockstream_verified::zset::validate_index(i, n) {
                return Err(OpError::invalid_literal(format!(
                    "Z-set row index {i} out of bounds for {n} rows"
                )));
            }
            take_indices.push(
                u64::try_from(i)
                    .map_err(|_| OpError::invalid_literal("Z-set row index exceeds u64"))?,
            );
        }
        let indices_array = UInt64Array::from(take_indices);
        let filtered_cols: Vec<ArrayRef> = self
            .data
            .columns()
            .iter()
            .map(|col| {
                arrow::compute::take(col.as_ref(), &indices_array, None).map_err(OpError::arrow)
            })
            .collect::<Result<_, _>>()?;
        let new_data =
            RecordBatch::try_new(self.data.schema(), filtered_cols).map_err(OpError::arrow)?;
        let new_weights = indices.iter().map(|&i| self.weights[i]).collect();
        let mut result = Self::try_new(new_data, new_weights)?;
        result.frontier = self.frontier.clone();
        Ok(result)
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
    fn try_new_rejects_misaligned_weights() {
        let zs = ArrowZSet::from_ab_rows(&[(1, 10)], 1);
        assert_eq!(
            ArrowZSet::try_new(zs.data, Vec::new())
                .unwrap_err()
                .to_string(),
            "[RS-1013] Invalid literal: Z-set row/weight length mismatch: 1 rows, 0 weights; next_steps: Z-set row/weight length mismatch: 1 rows, 0 weights"
        );
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
        let sub = zs.select_rows(&[2, 2, 0]).unwrap();
        assert_eq!(sub.num_rows(), 3);
        let rows = sub.positive_ab_rows();
        assert_eq!(rows, vec![(3, 30), (3, 30), (1, 10)]);
    }

    #[test]
    fn select_rows_rejects_out_of_range_and_preserves_empty_frontier() {
        let frontier = rockstream_types::frontier::FreshnessToken::new(Default::default(), 11);
        let zs = ArrowZSet::from_ab_rows(&[(1, 10)], 1).with_frontier(frontier.clone());
        assert_eq!(
            zs.select_rows(&[1]).unwrap_err().to_string(),
            "[RS-1013] Invalid literal: Z-set row index 1 out of bounds for 1 rows; next_steps: Z-set row index 1 out of bounds for 1 rows"
        );
        assert_eq!(zs.select_rows(&[]).unwrap().frontier, Some(frontier));
    }
}

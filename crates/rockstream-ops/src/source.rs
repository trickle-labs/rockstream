//! Built-in delta sources for the IVM engine.
//!
//! v0.4 provides two sources:
//!
//! - `VecDeltaSource` — pushes a pre-built sequence of `ArrowZSet` batches.
//!   Used in property tests, integration tests, and the oracle harness.
//!
//! - `GenerateRowsSource` — generates synthetic `(a: i64, b: i64)` rows at a
//!   configurable rate.  Used as the built-in `GENERATE ROWS` source.
//!   All rows have weight `+1`; the source repeats `total_epochs` times.

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::sync::mpsc::Sender;
use tracing::debug;

use crate::zset::ArrowZSet;

/// A source that drains a pre-built sequence of `ArrowZSet` batches.
///
/// The batches are sent one per epoch.  After the last batch is sent the
/// source completes and the pipeline can be shut down.
pub struct VecDeltaSource {
    batches: Vec<ArrowZSet>,
}

impl VecDeltaSource {
    /// Create a source from a vector of pre-built batches.
    pub fn new(batches: Vec<ArrowZSet>) -> Self {
        VecDeltaSource { batches }
    }

    /// Run the source: send all batches to `tx`, then drop `tx` to signal
    /// end-of-stream.
    pub async fn run(self, tx: Sender<ArrowZSet>) {
        for batch in self.batches {
            debug!(rows = batch.num_rows(), "VecDeltaSource: sending batch");
            if tx.send(batch).await.is_err() {
                // Downstream closed early.
                break;
            }
        }
        // `tx` dropped here → channel closed → downstream tasks terminate.
    }
}

/// A built-in row-generator source (`GENERATE ROWS`).
///
/// Generates `rows_per_epoch` rows of `(a: i64, b: i64)` where `a` is a
/// sequential counter and `b = a * 3`, all with weight `+1`.
///
/// Sends `total_epochs` batches, then closes.
pub struct GenerateRowsSource {
    /// Number of rows to produce per epoch.
    pub rows_per_epoch: usize,
    /// Total number of epochs to produce.
    pub total_epochs: u64,
}

impl GenerateRowsSource {
    pub fn new(rows_per_epoch: usize, total_epochs: u64) -> Self {
        GenerateRowsSource {
            rows_per_epoch,
            total_epochs,
        }
    }

    pub async fn run(self, tx: Sender<ArrowZSet>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let mut counter = 0i64;
        for epoch in 0..self.total_epochs {
            let a_vals: Vec<i64> = (counter..counter + self.rows_per_epoch as i64).collect();
            let b_vals: Vec<i64> = a_vals.iter().map(|&a| a * 3).collect();
            counter += self.rows_per_epoch as i64;
            let cols: Vec<Arc<dyn arrow::array::Array>> = vec![
                Arc::new(Int64Array::from(a_vals.clone())),
                Arc::new(Int64Array::from(b_vals)),
            ];
            let data = RecordBatch::try_new(schema.clone(), cols).expect("gen rows");
            let weights = vec![1i64; a_vals.len()];
            let zset = ArrowZSet::new(data, weights);
            debug!(
                epoch,
                rows = self.rows_per_epoch,
                "GenerateRowsSource: sending batch"
            );
            if tx.send(zset).await.is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn vec_source_sends_all_batches() {
        let batches = vec![
            ArrowZSet::from_ab_rows(&[(1, 10)], 1),
            ArrowZSet::from_ab_rows(&[(2, 20)], 1),
        ];
        let (tx, mut rx) = mpsc::channel(16);
        VecDeltaSource::new(batches).run(tx).await;
        let b1 = rx.recv().await.unwrap();
        let b2 = rx.recv().await.unwrap();
        assert_eq!(b1.num_rows(), 1);
        assert_eq!(b2.num_rows(), 1);
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn generate_rows_source_produces_correct_count() {
        let (tx, mut rx) = mpsc::channel(16);
        GenerateRowsSource::new(5, 3).run(tx).await;
        let mut total_rows = 0;
        while let Some(batch) = rx.recv().await {
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 15); // 5 rows × 3 epochs
    }

    #[tokio::test]
    async fn generate_rows_source_weight_is_1() {
        let (tx, mut rx) = mpsc::channel(16);
        GenerateRowsSource::new(3, 1).run(tx).await;
        let batch = rx.recv().await.unwrap();
        assert!(batch.weights.iter().all(|&w| w == 1));
    }
}

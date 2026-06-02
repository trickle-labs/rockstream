//! Built-in row-generator source (DESIGN.md §13.5.0).
//!
//! Produces synthetic rows for zero-friction first-run and local development.
//! Uses a seeded RNG for reproducible output across runs.
//!
//! ```sql
//! CREATE SOURCE demo.orders FROM GENERATE ROWS AS (
//!   order_id   BIGINT GENERATED,
//!   product_id INT    UNIFORM(1, 1000),
//!   quantity   INT    UNIFORM(1, 20)
//! ) RATE = 100 PER SECOND;
//! ```

use async_trait::async_trait;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rockstream_types::batch::{SourceBatch, ZSet, ZSetBatch};
use rockstream_types::timestamp::Epoch;

use crate::source::Source;

/// Configuration for a `GenerateRowsSource`.
#[derive(Debug, Clone)]
pub struct GenerateRowsConfig {
    /// Logical name of this source (schema.table or just table).
    pub name: String,
    /// Target rate of rows per epoch batch (not per second — the pipeline
    /// controls epoch cadence).
    pub rows_per_epoch: u64,
    /// Seed for the RNG — fixed seed gives reproducible output.
    pub seed: u64,
    /// Number of distinct product IDs to generate (UNIFORM range).
    pub product_id_range: u64,
    /// Maximum quantity per row (UNIFORM(1, quantity_max)).
    pub quantity_max: u64,
}

impl Default for GenerateRowsConfig {
    fn default() -> Self {
        Self {
            name: "demo.orders".to_string(),
            rows_per_epoch: 100,
            seed: 0,
            product_id_range: 1000,
            quantity_max: 20,
        }
    }
}

/// A synthetic data source that produces rows using a seeded RNG.
///
/// Each epoch emits `rows_per_epoch` rows with the schema:
/// - `order_id: u64` — monotonically increasing global counter
/// - `product_id: u64` — UNIFORM(1, product_id_range)
/// - `quantity: u64` — UNIFORM(1, quantity_max)
///
/// The source is never exhausted — it runs until the pipeline stops.
pub struct GenerateRowsSource {
    config: GenerateRowsConfig,
    rng: SmallRng,
    /// Monotonically increasing order counter.
    next_order_id: u64,
    /// Total rows emitted across all epochs.
    rows_emitted: u64,
}

impl GenerateRowsSource {
    /// Create a new generator source from the given config.
    pub fn new(config: GenerateRowsConfig) -> Self {
        let rng = SmallRng::seed_from_u64(config.seed);
        Self {
            config,
            rng,
            next_order_id: 1,
            rows_emitted: 0,
        }
    }

    /// Create with default config (100 rows/epoch, seed=0, 1000 products).
    pub fn with_defaults() -> Self {
        Self::new(GenerateRowsConfig::default())
    }

    /// Total rows emitted so far.
    pub fn rows_emitted(&self) -> u64 {
        self.rows_emitted
    }

    /// Generate a single epoch's Z-set delta (insert-only, weight = +1).
    pub fn generate_epoch(&mut self, epoch: Epoch) -> ZSetBatch {
        let mut zset = ZSet::new();
        for _ in 0..self.config.rows_per_epoch {
            let order_id = self.next_order_id;
            self.next_order_id += 1;

            let product_id = self.rng.gen_range(1..=self.config.product_id_range);
            let quantity = self.rng.gen_range(1..=self.config.quantity_max);

            // Encode row as simple key/value bytes:
            // key   = order_id (8 BE bytes)
            // value = product_id (8 BE) || quantity (8 BE)
            let key = order_id.to_be_bytes().to_vec();
            let mut value = Vec::with_capacity(16);
            value.extend_from_slice(&product_id.to_be_bytes());
            value.extend_from_slice(&quantity.to_be_bytes());

            zset.insert(key, value, 1);
        }
        self.rows_emitted += self.config.rows_per_epoch;
        ZSetBatch { zset, epoch }
    }
}

#[async_trait]
impl Source for GenerateRowsSource {
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch> {
        // Always produces data — never exhausted.
        let rows = self.config.rows_per_epoch as usize;
        // Advance the RNG state (side-effect of generating the epoch).
        self.generate_epoch(epoch);
        Some(SourceBatch {
            record_count: rows,
            epoch,
            offset: None,
            watermark: None,
        })
    }

    fn name(&self) -> &str {
        &self.config.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_produces_correct_rows_per_epoch() {
        let mut src = GenerateRowsSource::with_defaults();
        let batch = src.generate_epoch(0);
        assert_eq!(batch.zset.len(), 100);
    }

    #[test]
    fn seeded_output_is_reproducible() {
        let config = GenerateRowsConfig {
            seed: 42,
            rows_per_epoch: 10,
            ..Default::default()
        };
        let mut src1 = GenerateRowsSource::new(config.clone());
        let mut src2 = GenerateRowsSource::new(config);

        let b1 = src1.generate_epoch(0);
        let b2 = src2.generate_epoch(0);

        // Same seed → identical output.
        let rows1: Vec<_> = b1.zset.iter().collect();
        let rows2: Vec<_> = b2.zset.iter().collect();
        assert_eq!(rows1, rows2, "seeded output must be reproducible");
    }

    #[test]
    fn different_seeds_produce_different_output() {
        let mut src1 = GenerateRowsSource::new(GenerateRowsConfig {
            seed: 1,
            rows_per_epoch: 20,
            ..Default::default()
        });
        let mut src2 = GenerateRowsSource::new(GenerateRowsConfig {
            seed: 2,
            rows_per_epoch: 20,
            ..Default::default()
        });

        let b1 = src1.generate_epoch(0);
        let b2 = src2.generate_epoch(0);

        // Different seeds → different rows (overwhelmingly likely for 20 rows).
        let rows1: Vec<_> = b1.zset.iter().collect();
        let rows2: Vec<_> = b2.zset.iter().collect();
        assert_ne!(
            rows1, rows2,
            "different seeds must produce different output"
        );
    }

    #[test]
    fn order_ids_are_monotonically_increasing() {
        let mut src = GenerateRowsSource::new(GenerateRowsConfig {
            rows_per_epoch: 5,
            ..Default::default()
        });

        let b0 = src.generate_epoch(0);
        let b1 = src.generate_epoch(1);

        // Collect all keys (order IDs) from epoch 0 and epoch 1.
        let keys0: Vec<u64> = b0
            .zset
            .iter()
            .map(|r| u64::from_be_bytes(r.key.try_into().unwrap()))
            .collect();
        let keys1: Vec<u64> = b1
            .zset
            .iter()
            .map(|r| u64::from_be_bytes(r.key.try_into().unwrap()))
            .collect();

        let max0 = keys0.iter().copied().max().unwrap();
        let min1 = keys1.iter().copied().min().unwrap();
        assert!(
            max0 < min1,
            "epoch 0 order IDs must be strictly less than epoch 1"
        );
    }

    #[test]
    fn rows_emitted_counter_accumulates() {
        let mut src = GenerateRowsSource::new(GenerateRowsConfig {
            rows_per_epoch: 7,
            ..Default::default()
        });
        src.generate_epoch(0);
        src.generate_epoch(1);
        src.generate_epoch(2);
        assert_eq!(src.rows_emitted(), 21);
    }

    #[tokio::test]
    async fn poll_batch_never_exhausted() {
        let mut src = GenerateRowsSource::with_defaults();
        for epoch in 0..10 {
            let batch = src.poll_batch(epoch).await;
            assert!(batch.is_some(), "GenerateRowsSource must never return None");
        }
    }
}

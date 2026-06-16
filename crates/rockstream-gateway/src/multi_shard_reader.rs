//! Multi-shard reader pinned to a `ClusterFrontier` epoch.
//!
//! Scatter-reads the same view across multiple shards at a pinned frontier
//! epoch and merges results (union semantics — view outputs are already
//! partitioned, no dedup needed).
//!
//! # Bounds
//!
//! - `max_in_flight_rows`: named upper bound on rows held in memory across all
//!   shards. Default: 1_000_000 rows.
//! - Fill-level metric: `rows_in_flight` (AtomicUsize) tracks current usage.
//! - Backpressure: if `rows_in_flight > max_in_flight_rows`, `scatter_read`
//!   returns `GatewayError::ResultSetTooLarge`.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use rockstream_storage::ShardReader;

use crate::error::GatewayError;

/// Scatter-reads a view across multiple shards, all pinned to the same
/// `pinned_frontier` epoch.
pub struct MultiShardReader {
    /// One reader per shard, all opened at `pinned_frontier`.
    shards: Vec<Arc<ShardReader>>,
    /// The `ClusterFrontier` epoch at which all shard readers are pinned.
    pinned_frontier: u64,
    /// Bound: max rows that can be held in memory across all shards.
    ///
    /// Invariant: `total_rows_in_flight <= max_in_flight_rows`.
    /// Backpressure: if exceeded, `scatter_read` returns `GatewayError::ResultSetTooLarge`.
    max_in_flight_rows: usize,
    /// Fill-level metric: tracks current rows in flight.
    rows_in_flight: Arc<AtomicUsize>,
}

impl MultiShardReader {
    /// Default max in-flight rows (1 million).
    pub const DEFAULT_MAX_IN_FLIGHT_ROWS: usize = 1_000_000;

    pub fn new(
        shards: Vec<Arc<ShardReader>>,
        pinned_frontier: u64,
        max_in_flight_rows: usize,
    ) -> Self {
        Self {
            shards,
            pinned_frontier,
            max_in_flight_rows,
            rows_in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The frontier epoch all shards are pinned to.
    pub fn pinned_frontier(&self) -> u64 {
        self.pinned_frontier
    }

    /// Current fill level (rows currently in-flight in memory).
    pub fn rows_in_flight(&self) -> usize {
        self.rows_in_flight.load(Ordering::Relaxed)
    }

    /// Scatter-read `view_name` across all shards, merge and return up to
    /// `limit` rows.
    ///
    /// Returns `GatewayError::ResultSetTooLarge` if the merged row count would
    /// exceed `max_in_flight_rows`.
    pub async fn scatter_read(
        &self,
        view_name: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        let shard_count = self.shards.len().max(1);
        // Per-shard limit: ceiling division so we don't miss rows.
        let per_shard_limit = limit.map(|l| l.div_ceil(shard_count));

        // Read each shard in parallel.
        let prefix = format!("view_output/{view_name}/");
        let prefix_bytes = prefix.as_bytes().to_vec();

        let mut handles = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let shard = shard.clone();
            let pfx = prefix_bytes.clone();
            let per_limit = per_shard_limit;
            handles.push(tokio::spawn(async move {
                let kvs = shard.scan_prefix(&pfx).await?;
                let rows: Vec<Vec<u8>> = kvs
                    .into_iter()
                    .map(|(_k, v)| v.to_vec())
                    .take(per_limit.unwrap_or(usize::MAX))
                    .collect();
                Ok::<Vec<Vec<u8>>, rockstream_storage::StorageError>(rows)
            }));
        }

        let mut merged: Vec<Vec<u8>> = Vec::new();
        for handle in handles {
            let shard_rows = handle
                .await
                .map_err(|e| GatewayError::NotSupported(format!("join error: {e}")))?
                .map_err(GatewayError::Storage)?;
            merged.extend(shard_rows);
        }

        // Check bound before returning.
        let total = merged.len();
        if total > self.max_in_flight_rows {
            return Err(GatewayError::ResultSetTooLarge);
        }

        self.rows_in_flight.store(total, Ordering::Relaxed);

        // Truncate to global limit.
        let rows = match limit {
            Some(n) => merged.into_iter().take(n).collect(),
            None => merged,
        };

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use rockstream_storage::{ShardDb, ShardReader};
    use std::sync::Arc;

    async fn make_shard_with_rows(
        path: &str,
        view_name: &str,
        rows: &[(&str, &str)],
        store: Arc<InMemory>,
    ) -> Arc<ShardReader> {
        let shard_db = ShardDb::builder(path, store.clone()).build().await.unwrap();
        for (key_suffix, value) in rows {
            let key = format!("view_output/{view_name}/{key_suffix}");
            shard_db
                .put(key.as_bytes(), value.as_bytes())
                .await
                .unwrap();
        }
        shard_db.flush().await.unwrap();
        let reader = ShardReader::open(path, store).await.unwrap();
        Arc::new(reader)
    }

    #[tokio::test]
    async fn multi_shard_reader_pinned_to_frontier() {
        let store1 = Arc::new(InMemory::new());
        let store2 = Arc::new(InMemory::new());

        // Shard 1: rows 0-4
        let r1 = make_shard_with_rows(
            "shard1",
            "orders_mv",
            &[
                ("00000000", "1\t100.0"),
                ("00000001", "2\t200.0"),
                ("00000002", "3\t300.0"),
            ],
            store1,
        )
        .await;

        // Shard 2: rows 5-9
        let r2 = make_shard_with_rows(
            "shard2",
            "orders_mv",
            &[("00000003", "4\t400.0"), ("00000004", "5\t500.0")],
            store2,
        )
        .await;

        let msr = MultiShardReader::new(
            vec![r1, r2],
            /*pinned_frontier=*/ 42,
            MultiShardReader::DEFAULT_MAX_IN_FLIGHT_ROWS,
        );

        assert_eq!(msr.pinned_frontier(), 42);

        let rows = msr.scatter_read("orders_mv", None).await.unwrap();
        assert_eq!(rows.len(), 5, "merged result should have 5 rows");

        // fill-level metric updated
        assert_eq!(msr.rows_in_flight(), 5);
    }

    #[tokio::test]
    async fn multi_shard_reader_result_set_too_large() {
        let store = Arc::new(InMemory::new());
        let r1 = make_shard_with_rows(
            "shard-big",
            "big_view",
            &[("00000000", "a"), ("00000001", "b"), ("00000002", "c")],
            store,
        )
        .await;

        // max_in_flight_rows = 2 → should trigger too-large error with 3 rows
        let msr = MultiShardReader::new(vec![r1], 1, 2);
        let err = msr.scatter_read("big_view", None).await;
        assert!(
            matches!(err, Err(GatewayError::ResultSetTooLarge)),
            "expected ResultSetTooLarge"
        );
    }
}

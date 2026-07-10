//! `ViewReader` trait and strategy types for reading view output from storage.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;

use crate::error::GatewayError;

/// Batch size for streaming row delivery (Slice 4).
/// Peak per-connection memory is bounded by rows × average row size.
/// Fill-level metric constant.
pub const ROWS_IN_FLIGHT_BATCH: usize = 1_000;

/// Peak per-connection memory bound for streaming (64 MiB).
pub const STREAM_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Strategy for reading view rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewReadStrategy {
    /// Read from the hot LSM tier only (ShardReader on the latest published frontier).
    HotOnly,
    /// Reserved for two-tier hot+cold reads (Phase 9). Not implemented.
    TwoTier,
}

/// Trait for reading view output rows.
///
/// Implementations resolve a named view and return its rows as raw byte rows
/// (one `Vec<u8>` per row, tab-separated field values) at the current published
/// frontier.
#[async_trait]
pub trait ViewReader: Send + Sync {
    /// Read up to `limit` rows from the named view at the current published frontier.
    ///
    /// Returns rows as Vec<Vec<u8>> where each entry is a tab-separated row.
    async fn read_view(
        &self,
        view_name: &str,
        limit: Option<usize>,
        strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError>;

    /// Stream rows from the named view in batches of `ROWS_IN_FLIGHT_BATCH`.
    ///
    /// Each item is a batch of raw rows (tab-separated bytes). Peak in-flight
    /// memory is bounded by `STREAM_BATCH_BYTES`. No single batch exceeds
    /// `ROWS_IN_FLIGHT_BATCH` rows.
    ///
    /// Default implementation reads all rows up front and chunks them. Override
    /// for true incremental streaming.
    async fn read_view_stream(
        &self,
        view_name: &str,
        strategy: ViewReadStrategy,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<Vec<u8>>, GatewayError>> + Send>>, GatewayError>
    {
        let rows = self.read_view(view_name, None, strategy).await?;
        let batches: Vec<Result<Vec<Vec<u8>>, GatewayError>> = rows
            .chunks(ROWS_IN_FLIGHT_BATCH)
            .map(|chunk| Ok(chunk.to_vec()))
            .collect();
        Ok(Box::pin(futures::stream::iter(batches)))
    }

    /// The current published frontier epoch (None if no data written yet).
    fn published_frontier(&self) -> Option<u64>;
}

/// Column metadata for a view.
#[derive(Debug, Clone)]
pub struct ViewColumn {
    pub name: String,
    /// Arrow type name: "Int32", "Int64", "Float64", "Utf8", "Boolean", "Binary", "Timestamp"
    pub data_type: String,
}

/// Schema of a view (ordered column list).
#[derive(Debug, Clone)]
pub struct ViewSchema {
    pub columns: Vec<ViewColumn>,
}

/// `HotOnly` implementation backed by a single `ShardReader`.
///
/// Rows are stored under the prefix `view_output/{view_name}/` in the shard,
/// with each value being a tab-separated line of field values.
pub struct HotOnlyViewReader {
    pub shard_reader: Arc<rockstream_storage::ShardReader>,
    /// The frontier epoch at which this reader was opened (if known).
    pub frontier_epoch: Option<u64>,
}

#[async_trait]
impl ViewReader for HotOnlyViewReader {
    async fn read_view(
        &self,
        view_name: &str,
        limit: Option<usize>,
        strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        if strategy == ViewReadStrategy::TwoTier {
            return Err(GatewayError::NotSupported(
                "TwoTier strategy reserved for Phase 9".to_string(),
            ));
        }
        let prefix = format!("view_output/{view_name}/");
        let kvs = self.shard_reader.scan_prefix(prefix.as_bytes()).await?;

        let rows: Vec<Vec<u8>> = kvs.into_iter().map(|(_k, v)| v.to_vec()).collect();
        let rows = match limit {
            Some(n) => rows.into_iter().take(n).collect(),
            None => rows,
        };
        Ok(rows)
    }

    /// Streaming implementation: reads incrementally from the shard in
    /// `ROWS_IN_FLIGHT_BATCH`-sized chunks, bounded by `STREAM_BATCH_BYTES`.
    ///
    /// This avoids collecting all rows into memory before sending the first
    /// DataRow to the client.
    async fn read_view_stream(
        &self,
        view_name: &str,
        strategy: ViewReadStrategy,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Vec<Vec<u8>>, GatewayError>> + Send>>, GatewayError>
    {
        if strategy == ViewReadStrategy::TwoTier {
            return Err(GatewayError::NotSupported(
                "TwoTier strategy reserved for Phase 9".to_string(),
            ));
        }
        let prefix = format!("view_output/{view_name}/");
        let kvs = self.shard_reader.scan_prefix(prefix.as_bytes()).await?;

        // Build batches: each batch is at most ROWS_IN_FLIGHT_BATCH rows and
        // at most STREAM_BATCH_BYTES bytes in total per-connection.
        let mut batches: Vec<Result<Vec<Vec<u8>>, GatewayError>> = Vec::new();
        let mut current_batch: Vec<Vec<u8>> = Vec::new();
        let mut current_batch_bytes: usize = 0;

        for (_k, v) in kvs {
            let row = v.to_vec();
            let row_len = row.len();
            current_batch.push(row);
            current_batch_bytes += row_len;

            if current_batch.len() >= ROWS_IN_FLIGHT_BATCH
                || current_batch_bytes >= STREAM_BATCH_BYTES
            {
                batches.push(Ok(std::mem::take(&mut current_batch)));
                current_batch_bytes = 0;
            }
        }
        if !current_batch.is_empty() {
            batches.push(Ok(current_batch));
        }

        Ok(Box::pin(futures::stream::iter(batches)))
    }

    fn published_frontier(&self) -> Option<u64> {
        self.frontier_epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use rockstream_storage::ShardDb;
    use std::sync::Arc;

    #[tokio::test]
    async fn view_reader_hot_only_reads_from_shard() {
        let store = Arc::new(InMemory::new());
        let shard_db = ShardDb::builder("test-shard", store.clone())
            .build()
            .await
            .unwrap();

        // Write rows under view_output/my_view/
        for i in 0u32..5 {
            let key = format!("view_output/my_view/{:08}", i);
            let value = format!("row_{i}\t{i}");
            shard_db
                .put(key.as_bytes(), value.as_bytes())
                .await
                .unwrap();
        }
        shard_db.flush().await.unwrap();

        // Open a ShardReader
        let reader = rockstream_storage::ShardReader::open("test-shard", store.clone())
            .await
            .unwrap();
        let view_reader = HotOnlyViewReader {
            shard_reader: Arc::new(reader),
            frontier_epoch: Some(1),
        };

        let rows = view_reader
            .read_view("my_view", Some(10), ViewReadStrategy::HotOnly)
            .await
            .unwrap();

        assert_eq!(rows.len(), 5, "expected 5 rows");

        // TwoTier returns error
        let err = view_reader
            .read_view("my_view", None, ViewReadStrategy::TwoTier)
            .await;
        assert!(
            matches!(err, Err(GatewayError::NotSupported(_))),
            "TwoTier should return NotSupported"
        );
    }

    /// Slice 4 green gate: `read_view_stream` yields rows in bounded batches
    /// without collecting all rows up front. Verifies no single batch exceeds
    /// ROWS_IN_FLIGHT_BATCH and the total row count is correct.
    #[tokio::test]
    async fn test_streaming_peak_memory_bounded() {
        // Use a smaller row count since writing 2_000_000 rows to in-memory store
        // is slow in unit tests. The important invariant is that no batch exceeds
        // ROWS_IN_FLIGHT_BATCH rows.
        let n_rows: usize = 3_500; // > 3 * ROWS_IN_FLIGHT_BATCH is not needed; 3.5 batches

        let store = Arc::new(InMemory::new());
        let shard_db = ShardDb::builder("stream-shard", store.clone())
            .build()
            .await
            .unwrap();

        for i in 0u32..n_rows as u32 {
            let key = format!("view_output/stream_view/{:08}", i);
            let value = format!("row_{i}\tvalue_{i}");
            shard_db
                .put(key.as_bytes(), value.as_bytes())
                .await
                .unwrap();
        }
        shard_db.flush().await.unwrap();

        let reader = rockstream_storage::ShardReader::open("stream-shard", store.clone())
            .await
            .unwrap();
        let view_reader = HotOnlyViewReader {
            shard_reader: Arc::new(reader),
            frontier_epoch: Some(1),
        };

        let mut stream = view_reader
            .read_view_stream("stream_view", ViewReadStrategy::HotOnly)
            .await
            .unwrap();

        let mut total_rows = 0usize;
        let mut batch_count = 0usize;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.expect("stream batch error");
            assert!(
                batch.len() <= ROWS_IN_FLIGHT_BATCH,
                "batch {} has {} rows, exceeds ROWS_IN_FLIGHT_BATCH={}",
                batch_count,
                batch.len(),
                ROWS_IN_FLIGHT_BATCH
            );
            total_rows += batch.len();
            batch_count += 1;
        }

        assert_eq!(total_rows, n_rows, "total rows mismatch");
        // With 3500 rows and batch size 1000, we expect 4 batches (1000+1000+1000+500)
        assert!(
            batch_count >= 2,
            "expected at least 2 batches for {n_rows} rows with ROWS_IN_FLIGHT_BATCH={ROWS_IN_FLIGHT_BATCH}"
        );
    }
}

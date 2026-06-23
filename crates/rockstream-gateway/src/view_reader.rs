//! `ViewReader` trait and strategy types for reading view output from storage.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::GatewayError;

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

    fn published_frontier(&self) -> Option<u64> {
        self.frontier_epoch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}

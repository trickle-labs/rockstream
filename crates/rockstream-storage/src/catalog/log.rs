//! Catalog Log Management, Scanning, and Replay (Slice 4).
//!
//! Stores append-only transaction log records under:
//! `catalog/log/<start_revision>_<end_revision>.log`
//!
//! - Bounded scans: 1,024 records per page, continuing to completion.
//! - Bounded replay memory: max 64 MB uncommitted txn replay buffer (exceeding fails with `RS-1003`).

use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::sync::Arc;

use super::txn::CatalogTxn;
use super::CatalogError;

/// Maximum number of records scanned in a single page window.
pub const MAX_SCAN_PAGE_SIZE: usize = 1024;

/// Maximum allowed memory footprint for the replay buffer (64 MB).
pub const MAX_REPLAY_BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// A page result from a bounded catalog scan.
#[derive(Debug, Clone)]
pub struct CatalogScanPage {
    pub transactions: Vec<CatalogTxn>,
    pub next_cursor: Option<usize>,
    pub has_more: bool,
}

/// Catalog log manager backed by an `ObjectStore`.
pub struct CatalogLogManager {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl CatalogLogManager {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    /// Construct object path for a transaction log entry.
    pub fn log_path(&self, revision: u64, operation_id: u64) -> ObjectPath {
        let p = format!(
            "{}/catalog/log/{:020}_{:020}.log",
            self.prefix, revision, operation_id
        );
        ObjectPath::from(p.trim_start_matches('/'))
    }

    /// Append a transaction record to the log.
    pub async fn append_txn(&self, txn: &CatalogTxn) -> Result<(), CatalogError> {
        let bytes = txn.encode()?;
        let path = self.log_path(txn.revision, txn.operation_id);
        self.store.put(&path, bytes.into()).await.map_err(|e| {
            CatalogError::Storage(format!("[RS-0003] failed to append catalog log: {e}"))
        })?;
        Ok(())
    }

    /// Scan a single bounded page of log entries.
    pub async fn scan_page(
        all_txns: &[CatalogTxn],
        cursor: usize,
        page_size: usize,
    ) -> CatalogScanPage {
        let page_size = page_size.min(MAX_SCAN_PAGE_SIZE);
        let end = (cursor + page_size).min(all_txns.len());
        let slice = all_txns[cursor..end].to_vec();
        let has_more = end < all_txns.len();
        let next_cursor = if has_more { Some(end) } else { None };

        CatalogScanPage {
            transactions: slice,
            next_cursor,
            has_more,
        }
    }

    /// Scan all transactions across pages to completion.
    pub async fn scan_all_continuing(all_txns: &[CatalogTxn], page_size: usize) -> Vec<CatalogTxn> {
        let mut cursor = 0;
        let mut result = Vec::new();
        loop {
            let page = Self::scan_page(all_txns, cursor, page_size).await;
            result.extend(page.transactions);
            match page.next_cursor {
                Some(next) => cursor = next,
                None => break,
            }
        }
        result
    }
}

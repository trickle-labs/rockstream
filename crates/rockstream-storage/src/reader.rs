//! DbReader: read-only snapshot access for cross-shard queries.
//!
//! Provides consistent reads from a checkpoint without blocking writes.

use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::config::DbReaderOptions;
use slatedb::DbReader;
use tokio::sync::mpsc;

use crate::error::StorageError;

/// A read-only view of a shard database from a checkpoint.
///
/// Provides consistent point-in-time reads without interfering
/// with ongoing writes to the shard.
pub struct ShardReader {
    reader: DbReader,
    path: String,
}

impl ShardReader {
    /// Open a reader for a shard from the latest manifest.
    pub async fn open(
        path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, StorageError> {
        let path = path.into();
        let reader = DbReader::builder(path.clone(), object_store)
            .build()
            .await?;
        Ok(Self { reader, path })
    }

    /// Open a reader with custom options.
    pub async fn open_with_options(
        path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        options: DbReaderOptions,
    ) -> Result<Self, StorageError> {
        let path = path.into();
        let reader = DbReader::builder(path.clone(), object_store)
            .with_options(options)
            .build()
            .await?;
        Ok(Self { reader, path })
    }

    /// Return the storage path this snapshot reader was opened against.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get the value for a key from the snapshot.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        Ok(self.reader.get(key).await?)
    }

    /// Look up an idempotency key epoch.
    pub async fn get_idempotency_epoch(
        &self,
        shard_id: u32,
        key_hash: [u8; 16],
    ) -> Result<Option<u64>, StorageError> {
        let key = crate::keys::ShardKeyEncoder::idempotency_key(shard_id, key_hash);
        if let Some(bytes) = self.get(&key).await? {
            if bytes.len() >= 8 {
                let epoch = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                return Ok(Some(epoch));
            }
        }
        Ok(None)
    }

    /// Scan all key-value pairs with the given prefix from the snapshot.
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>, StorageError> {
        let mut results = Vec::new();
        let mut iter = self.reader.scan_prefix(prefix).await?;
        while let Some(entry) = iter.next().await? {
            results.push((entry.key, entry.value));
        }
        Ok(results)
    }

    /// Send prefix-scan pages through a bounded channel.
    ///
    /// Each page contains at most `max_rows` entries and `max_bytes` bytes of
    /// key/value payload.  The channel capacity is supplied by the caller so
    /// readers naturally stop at the consumer's backpressure boundary instead
    /// of accumulating a relation in memory.
    pub async fn scan_prefix_pages(
        &self,
        prefix: &[u8],
        max_rows: usize,
        max_bytes: usize,
        sender: mpsc::Sender<Result<Vec<(Bytes, Bytes)>, StorageError>>,
    ) {
        let sender = sender;
        let mut page = Vec::with_capacity(max_rows);
        let mut page_bytes = 0usize;
        let mut iter = match self.reader.scan_prefix(prefix).await {
            Ok(iter) => iter,
            Err(error) => {
                let _ = sender.send(Err(error.into())).await;
                return;
            }
        };
        loop {
            let entry = match iter.next().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(error) => {
                    let _ = sender.send(Err(error.into())).await;
                    return;
                }
            };
            let entry_bytes = entry.key.len() + entry.value.len();
            if entry_bytes > max_bytes {
                let _ = sender
                    .send(Err(StorageError::Unsupported(format!(
                        "prefix scan entry is {entry_bytes} bytes, above page budget {max_bytes}"
                    ))))
                    .await;
                return;
            }
            if !page.is_empty()
                && (page.len() == max_rows || page_bytes.saturating_add(entry_bytes) > max_bytes)
            {
                if sender.send(Ok(std::mem::take(&mut page))).await.is_err() {
                    return;
                }
                page_bytes = 0;
            }
            page_bytes = page_bytes.saturating_add(entry_bytes);
            page.push((entry.key, entry.value));
        }
        if !page.is_empty() {
            let _ = sender.send(Ok(page)).await;
        }
    }
}

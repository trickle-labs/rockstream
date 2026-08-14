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

    /// Open a reader for a specific historical checkpoint epoch.
    ///
    /// Validates retention bounds. If `epoch < min_retention_epoch`, returns `StorageError::EpochPruned`.
    pub async fn open_with_epoch(
        path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        epoch: u64,
        min_retention_epoch: u64,
    ) -> Result<Self, StorageError> {
        if epoch < min_retention_epoch {
            return Err(StorageError::EpochPruned {
                requested_epoch: epoch,
                min_retention_epoch,
            });
        }
        Self::open(path, object_store).await
    }

    /// Read operator state value for a specific key.
    pub async fn get_op_state(
        &self,
        prefix: crate::keys::ShardPrefix,
        operator_id: u64,
        suffix: &[u8],
    ) -> Result<Option<Bytes>, StorageError> {
        let key = crate::keys::ShardKeyEncoder::encode(prefix, operator_id, suffix);
        self.get(&key).await
    }

    /// Scan operator state entries with a given sub-prefix.
    pub async fn scan_op_state_prefix(
        &self,
        prefix: crate::keys::ShardPrefix,
        operator_id: u64,
        sub_prefix: &[u8],
    ) -> Result<Vec<(Bytes, Bytes)>, StorageError> {
        let mut key_prefix = crate::keys::ShardKeyEncoder::operator_prefix(prefix, operator_id);
        key_prefix.extend_from_slice(sub_prefix);
        self.scan_prefix(&key_prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::{ShardKeyEncoder, ShardPrefix};
    use crate::shard_db::ShardDb;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_arrangement_shard_reader_snapshot_and_epoch_reads() {
        let store = Arc::new(InMemory::new());
        let db = ShardDb::builder("test/arrangement_reader", store.clone())
            .build()
            .await
            .unwrap();

        let op_id = 42u64;
        let group_key = 100i64;
        let key = ShardKeyEncoder::encode(ShardPrefix::OpState, op_id, &group_key.to_be_bytes());
        db.put(&key, &[1, 2, 3, 4]).await.unwrap();
        db.flush().await.unwrap();

        let reader = ShardReader::open("test/arrangement_reader", store.clone())
            .await
            .unwrap();
        let val = reader
            .get_op_state(ShardPrefix::OpState, op_id, &group_key.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(val.unwrap().as_ref(), &[1, 2, 3, 4]);

        let scan = reader
            .scan_op_state_prefix(ShardPrefix::OpState, op_id, &[])
            .await
            .unwrap();
        assert_eq!(scan.len(), 1);

        // Epoch within retention
        let r_epoch_ok =
            ShardReader::open_with_epoch("test/arrangement_reader", store.clone(), 15, 10).await;
        assert!(r_epoch_ok.is_ok());

        // Epoch outside retention
        let r_epoch_err =
            ShardReader::open_with_epoch("test/arrangement_reader", store.clone(), 5, 10).await;
        match r_epoch_err {
            Err(StorageError::EpochPruned {
                requested_epoch,
                min_retention_epoch,
            }) => {
                assert_eq!(requested_epoch, 5);
                assert_eq!(min_retention_epoch, 10);
            }
            _ => panic!("expected EpochPruned error"),
        }
    }
}

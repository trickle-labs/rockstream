//! DbReader: read-only snapshot access for cross-shard queries.
//!
//! Provides consistent reads from a checkpoint without blocking writes.

use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use slatedb::config::DbReaderOptions;
use slatedb::DbReader;

use crate::error::StorageError;

/// A read-only view of a shard database from a checkpoint.
///
/// Provides consistent point-in-time reads without interfering
/// with ongoing writes to the shard.
pub struct ShardReader {
    reader: DbReader,
}

impl ShardReader {
    /// Open a reader for a shard from the latest manifest.
    pub async fn open(
        path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, StorageError> {
        let reader = DbReader::builder(path.into(), object_store).build().await?;
        Ok(Self { reader })
    }

    /// Open a reader with custom options.
    pub async fn open_with_options(
        path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        options: DbReaderOptions,
    ) -> Result<Self, StorageError> {
        let reader = DbReader::builder(path.into(), object_store)
            .with_options(options)
            .build()
            .await?;
        Ok(Self { reader })
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
}

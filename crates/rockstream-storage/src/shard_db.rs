//! ShardDb: per-shard database wrapper around SlateDB.
//!
//! Provides typed access to shard-local key-value storage with
//! support for write batches, merge operations, and prefix scanning.
//! Does NOT use range deletion.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use rockstream_types::merge_law::{ArrangementHeader, MergeLawId};
use slatedb::config::Settings;
use slatedb::Db;

use std::sync::atomic::{AtomicBool, Ordering};

static ALLOW_LAW_OPERAND_FALLBACK: AtomicBool = AtomicBool::new(false);

/// Set whether to allow fallback to raw bytes when a law operand is corrupted.
pub fn set_allow_law_operand_fallback(allow: bool) {
    ALLOW_LAW_OPERAND_FALLBACK.store(allow, Ordering::SeqCst);
}

/// Check if fallback to raw bytes is allowed when a law operand is corrupted.
pub fn is_allow_law_operand_fallback() -> bool {
    ALLOW_LAW_OPERAND_FALLBACK.load(Ordering::SeqCst)
}

use crate::error::StorageError;
use crate::keys::ShardKeyEncoder;
use crate::merge_registry::SumCountMergeOperator;

/// Check whether `bytes` is a valid operand for `law`.
///
/// Uses the law's identity element to probe validity: `merge(bytes, identity)`
/// must succeed. Falls back to `merge(bytes, bytes)` if the law has no
/// identity (uncommon). For the identity element itself, `is_identity` short-
/// circuits.
fn is_valid_law_operand(law: &dyn rockstream_types::merge_law::LawBundle, bytes: &[u8]) -> bool {
    let mut bytes = bytes;
    if !bytes.is_empty() {
        let tag = bytes[0];
        if tag == 0x01 || tag == 0x02 || tag == 0x03 || tag == 0x04 || tag == 0x22 || tag == 0x30 {
            bytes = &bytes[1..];
        }
    }
    if law.is_identity(bytes) {
        return true;
    }
    if let Some(identity) = law.identity() {
        law.merge(bytes, &identity).is_ok()
    } else {
        law.merge(bytes, bytes).is_ok()
    }
}

/// A per-shard database backed by SlateDB.
///
/// Each shard has its own `ShardDb` instance that provides:
/// - Key-value get/put/delete operations
/// - Atomic `WriteBatch` commits
/// - Merge operations (associative sum/count)
/// - Prefix scanning
/// - Checkpoint creation for consistent snapshots
///
/// No code path uses range deletion.
pub struct ShardDb {
    db: Db,
}

/// Builder for creating a `ShardDb`.
pub struct ShardDbBuilder {
    path: String,
    object_store: Arc<dyn ObjectStore>,
    settings: Settings,
}

impl ShardDbBuilder {
    /// Create a new builder for a shard database.
    pub fn new(path: impl Into<String>, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            path: path.into(),
            object_store,
            settings: Settings::default(),
        }
    }

    /// Set custom database settings.
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// Build and open the shard database.
    pub async fn build(self) -> Result<ShardDb, StorageError> {
        let db = Db::builder(self.path.as_str(), self.object_store)
            .with_settings(self.settings)
            .with_merge_operator(Arc::new(SumCountMergeOperator))
            .build()
            .await?;
        Ok(ShardDb { db })
    }
}

impl ShardDb {
    /// Create a builder for opening a shard database.
    pub fn builder(path: impl Into<String>, object_store: Arc<dyn ObjectStore>) -> ShardDbBuilder {
        ShardDbBuilder::new(path, object_store)
    }

    /// Get the value for a key, if it exists.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        Ok(self.db.get(key).await?)
    }

    /// Put a key-value pair.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.db.put(key, value).await?;
        Ok(())
    }

    /// Delete a key.
    pub async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        self.db.delete(key).await?;
        Ok(())
    }

    /// Perform a merge operation on a key.
    ///
    /// The value must be tagged with a `MergeTag` prefix byte.
    pub async fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.db.merge(key, value).await?;
        Ok(())
    }

    /// Write a batch of operations atomically.
    pub async fn write_batch(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.db.write(batch.inner).await?;
        Ok(())
    }

    /// Scan all key-value pairs with the given prefix.
    ///
    /// Returns key-value pairs in sorted order.
    ///
    /// **Warning:** This materializes the entire result into memory. For large
    /// arrangements, prefer `scan_prefix_bounded` with an explicit byte budget.
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>, StorageError> {
        let mut results = Vec::new();
        let mut iter = self.db.scan_prefix(prefix).await?;
        while let Some(entry) = iter.next().await? {
            results.push((entry.key, entry.value));
        }
        Ok(results)
    }

    /// Scan key-value pairs with the given prefix, up to a byte budget.
    ///
    /// Stops reading once the cumulative size of returned keys and values
    /// exceeds `max_bytes`. This prevents unbounded memory usage when scanning
    /// large arrangements.
    ///
    /// Returns `(results, truncated)` where `truncated` is true if the scan
    /// was stopped early due to the budget.
    pub async fn scan_prefix_bounded(
        &self,
        prefix: &[u8],
        max_bytes: usize,
    ) -> Result<(Vec<(Bytes, Bytes)>, bool), StorageError> {
        let mut results = Vec::new();
        let mut total_bytes: usize = 0;
        let mut iter = self.db.scan_prefix(prefix).await?;
        while let Some(entry) = iter.next().await? {
            total_bytes += entry.key.len() + entry.value.len();
            if total_bytes > max_bytes && !results.is_empty() {
                return Ok((results, true));
            }
            results.push((entry.key, entry.value));
            if total_bytes > max_bytes {
                return Ok((results, true));
            }
        }
        Ok((results, false))
    }

    /// Flush the WAL to durable storage.
    pub async fn flush(&self) -> Result<(), StorageError> {
        self.db.flush().await?;
        Ok(())
    }

    /// Validate that all merge laws referenced in arrangement headers stored
    /// in this shard are present in the given set of known law IDs.
    ///
    /// Reads the `shard_meta/law_catalog/` prefix and checks each entry.
    /// Returns `StorageError::UnknownMergeLaw` (RS-5002) if any stored law
    /// is not in `known_law_ids`. Call this immediately after opening the DB
    /// before performing any reads or writes.
    pub async fn validate_law_catalog(
        &self,
        known_law_ids: &HashSet<MergeLawId>,
    ) -> Result<(), StorageError> {
        let prefix = ShardKeyEncoder::meta_key(b"law_catalog/");
        let entries = self.scan_prefix(&prefix).await?;
        for (_, value) in entries {
            if value.len() < ArrangementHeader::WIRE_SIZE {
                continue; // malformed entry — skip (not a law catalog entry)
            }
            let buf: [u8; 4] = match value[..4].try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };
            let header = ArrangementHeader::decode(&buf);
            if !known_law_ids.contains(&header.law_id) {
                return Err(StorageError::UnknownMergeLaw {
                    law_id: header.law_id.0,
                    law_version: header.law_version.0,
                });
            }
        }
        Ok(())
    }

    /// Record that a merge law is used in this shard's arrangements.
    ///
    /// Writes a `shard_meta/law_catalog/{law_id:04x}` key so that
    /// `validate_law_catalog` can verify it on the next attach.
    pub async fn record_law_usage(&self, header: ArrangementHeader) -> Result<(), StorageError> {
        let key_suffix = format!("law_catalog/{:04x}", header.law_id.0);
        let key = ShardKeyEncoder::meta_key(key_suffix.as_bytes());
        let value = header.encode();
        self.put(&key, &value).await
    }

    /// Look up an idempotency key epoch.
    pub async fn get_idempotency_epoch(
        &self,
        shard_id: u32,
        key_hash: [u8; 16],
    ) -> Result<Option<u64>, StorageError> {
        let key = ShardKeyEncoder::idempotency_key(shard_id, key_hash);
        if let Some(bytes) = self.get(&key).await? {
            if bytes.len() == 8 {
                let epoch = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                return Ok(Some(epoch));
            }
        }
        Ok(None)
    }

    /// Add an idempotency key insert to a WriteBatch.
    pub fn put_idempotency_key(
        batch: &mut WriteBatch,
        shard_id: u32,
        key_hash: [u8; 16],
        epoch: u64,
    ) {
        let key = ShardKeyEncoder::idempotency_key(shard_id, key_hash);
        batch.put(&key, &epoch.to_be_bytes());
    }

    /// Close the database, flushing any pending writes.
    pub async fn close(self) -> Result<(), StorageError> {
        self.db.close().await?;
        Ok(())
    }

    /// Write a value with a prepended 4-byte `ArrangementHeader`.
    ///
    /// The stored format is: `[header:4][value_bytes]`.
    /// This enables `get_arrangement_header` to recover `(law_id, law_version)`
    /// from any arrangement key without loading the full value.
    ///
    /// Used by aggregate arrangements (`0xAG` key prefix) to record which
    /// law governs the stored state.
    pub async fn put_with_arrangement_header(
        &self,
        key: &[u8],
        header: ArrangementHeader,
        value: &[u8],
    ) -> Result<(), StorageError> {
        let mut stored = Vec::with_capacity(ArrangementHeader::WIRE_SIZE + value.len());
        stored.extend_from_slice(&header.encode());
        stored.extend_from_slice(value);
        self.put(key, &stored).await
    }

    /// Read back the `ArrangementHeader` stored with `put_with_arrangement_header`.
    ///
    /// Returns `None` if the key does not exist or the stored value is shorter
    /// than the 4-byte header.
    pub async fn get_arrangement_header(
        &self,
        key: &[u8],
    ) -> Result<Option<ArrangementHeader>, StorageError> {
        let raw = self.db.get(key).await?;
        match raw {
            None => Ok(None),
            Some(bytes) if bytes.len() < ArrangementHeader::WIRE_SIZE => Ok(None),
            Some(bytes) => {
                let buf: [u8; 4] = match bytes[..4].try_into() {
                    Ok(b) => b,
                    Err(_) => return Ok(None),
                };
                Ok(Some(ArrangementHeader::decode(&buf)))
            }
        }
    }

    /// Law-aware point read: fetch a stored value and interpret it through
    /// `law`.
    ///
    /// If the key exists and the stored bytes are a valid operand for `law`,
    /// the value is returned as-is and `merge_law_applied_total` is
    /// incremented.
    ///
    /// If the law cannot parse the stored bytes (malformed operand), the
    /// fallback path returns the raw bytes unchanged and increments
    /// `merge_law_fallback_total`. This ensures fail-closed behaviour: no
    /// silent data corruption.
    ///
    /// Returns `None` if the key does not exist.
    pub async fn get_merged(
        &self,
        key: &[u8],
        law: &dyn rockstream_types::merge_law::LawBundle,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let raw = self.db.get(key).await?;
        match raw {
            None => Ok(None),
            Some(bytes) => {
                let metric_key = rockstream_types::metrics::LawMetricKey {
                    law_id: law.id(),
                    law_name: law.name(),
                    law_version: law.version().0,
                };
                if is_valid_law_operand(law, &bytes) {
                    rockstream_types::metrics::inc_applied(&metric_key);
                    Ok(Some(bytes.to_vec()))
                } else {
                    rockstream_types::metrics::inc_fallback(&metric_key);
                    if is_allow_law_operand_fallback() {
                        Ok(Some(bytes.to_vec()))
                    } else {
                        Err(StorageError::OperandCorruption {
                            law_id: law.id().0,
                            law_name: law.name().to_string(),
                        })
                    }
                }
            }
        }
    }

    /// Law-aware prefix scan: fetch all values under `prefix` and interpret
    /// each through `law`.
    ///
    /// For each key-value pair:
    /// - If the stored bytes are valid for `law`, `merge_law_applied_total` is
    ///   incremented.
    /// - Otherwise the raw bytes are returned and `merge_law_fallback_total`
    ///   is incremented.
    ///
    /// Returns a list of `(key, value)` pairs in sorted key order.
    pub async fn scan_merged(
        &self,
        prefix: &[u8],
        law: &dyn rockstream_types::merge_law::LawBundle,
    ) -> Result<Vec<(Bytes, Vec<u8>)>, StorageError> {
        let entries = self.scan_prefix(prefix).await?;
        let metric_key = rockstream_types::metrics::LawMetricKey {
            law_id: law.id(),
            law_name: law.name(),
            law_version: law.version().0,
        };

        let mut results = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            if is_valid_law_operand(law, &v) {
                rockstream_types::metrics::inc_applied(&metric_key);
                results.push((k, v.to_vec()));
            } else {
                rockstream_types::metrics::inc_fallback(&metric_key);
                if is_allow_law_operand_fallback() {
                    results.push((k, v.to_vec()));
                } else {
                    return Err(StorageError::OperandCorruption {
                        law_id: law.id().0,
                        law_name: law.name().to_string(),
                    });
                }
            }
        }

        Ok(results)
    }
}

/// Atomic write batch for multiple operations.
///
/// All operations in a batch are committed atomically.
/// Does NOT support range deletion.
pub struct WriteBatch {
    inner: slatedb::WriteBatch,
    count: usize,
}

impl WriteBatch {
    /// Create a new empty write batch.
    pub fn new() -> Self {
        Self {
            inner: slatedb::WriteBatch::new(),
            count: 0,
        }
    }

    /// Add a put operation to the batch.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.inner.put(key, value);
        self.count += 1;
    }

    /// Add a delete operation to the batch.
    pub fn delete(&mut self, key: &[u8]) {
        self.inner.delete(key);
        self.count += 1;
    }

    /// Add a merge operation to the batch.
    pub fn merge(&mut self, key: &[u8], value: &[u8]) {
        self.inner.merge(key, value);
        self.count += 1;
    }

    /// Returns the number of operations in the batch.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Returns true if the batch has no operations.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Compile-time assertion: `ShardDb` does not expose any range-delete API.
/// This module uses only point-delete and scan-and-delete patterns.
#[cfg(test)]
mod no_range_delete_assertion {
    /// This test documents that we do NOT depend on range deletion.
    /// If SlateDB adds range-delete, this test serves as a reminder
    /// to NOT use it - cleanup is done via scan-and-delete.
    #[test]
    fn no_range_delete_api_exposed() {
        // ShardDb has: get, put, delete, merge, write_batch, scan_prefix, flush, close.
        // WriteBatch has: put, delete, merge.
        // None of these are range operations.
        // This is a documentation test - the real enforcement is that the types
        // don't expose any range-delete method.
    }
}

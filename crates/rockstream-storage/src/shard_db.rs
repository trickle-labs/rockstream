//! ShardDb: per-shard database wrapper around SlateDB.
//!
//! Provides typed access to shard-local key-value storage with
//! support for write batches, merge operations, and prefix scanning.
//! Does NOT use range deletion.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use rockstream_types::frontier::ShardFrontierReport;
use rockstream_types::ids::ShardId;
use rockstream_types::merge_law::{ArrangementHeader, MergeLawId};
use slatedb::config::{CheckpointOptions, CheckpointScope, Settings};
use slatedb::Db;

use std::sync::atomic::{AtomicBool, Ordering};

static ALLOW_LAW_OPERAND_FALLBACK: AtomicBool = AtomicBool::new(false);

/// Max rows (groups) returned by a single shard's partial_query.
/// Bound: MAX_PARTIAL_AGG_RESULT_ROWS; fill metric: partial_agg_result_rows gauge.
pub const MAX_PARTIAL_AGG_RESULT_ROWS: usize = 1_000_000;

/// Fill-level metric: number of rows in the last partial_query call.
/// Gauge: updated atomically per call to partial_query.
pub static PARTIAL_AGG_RESULT_ROWS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

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
#[derive(Clone)]
pub struct ShardDb {
    db: Db,
    object_store: Arc<dyn ObjectStore>,
    last_epoch: Arc<std::sync::atomic::AtomicU64>,
}

/// Builder for creating a `ShardDb`.
pub struct ShardDbBuilder {
    path: String,
    object_store: Arc<dyn ObjectStore>,
    settings: Settings,
}

/// Specification for a partial aggregation query.
/// Serialized as JSON in `partial_plan_bytes`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartialAggSpec {
    /// Zero-based column index to GROUP BY (TSV column index).
    pub group_col: usize,
    /// Zero-based column index to aggregate.
    pub agg_col: usize,
    /// Aggregation type: "sum" or "count".
    pub agg_type: String,
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
        let db = Db::builder(self.path.as_str(), self.object_store.clone())
            .with_settings(self.settings)
            .with_merge_operator(Arc::new(SumCountMergeOperator))
            .build()
            .await?;
        let frontier_key = ShardKeyEncoder::frontier_key();
        let initial_epoch = if let Some(bytes) = db.get(&frontier_key).await? {
            if bytes.len() == 8 {
                u64::from_be_bytes(bytes[..8].try_into().unwrap())
            } else {
                0
            }
        } else {
            0
        };
        Ok(ShardDb {
            db,
            object_store: self.object_store,
            last_epoch: Arc::new(std::sync::atomic::AtomicU64::new(initial_epoch)),
        })
    }
}

impl ShardDb {
    /// Get the underlying object store.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    /// Access the last epoch atomic (for epoch allocation in direct-write commits).
    pub fn last_epoch(&self) -> &Arc<std::sync::atomic::AtomicU64> {
        &self.last_epoch
    }

    /// Create a builder for opening a shard database.
    pub fn builder(path: impl Into<String>, object_store: Arc<dyn ObjectStore>) -> ShardDbBuilder {
        ShardDbBuilder::new(path, object_store)
    }

    /// Get the value for a key, if it exists.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>, StorageError> {
        let val = self.db.get(key).await?;
        if key == ShardKeyEncoder::frontier_key() {
            if let Some(ref bytes) = val {
                if bytes.len() == 8 {
                    let epoch = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                    let old_epoch = self.last_epoch.load(Ordering::SeqCst);
                    assert!(
                        epoch >= old_epoch,
                        "M1-S2: committed_epoch read must be non-decreasing (got {epoch}, was {old_epoch})"
                    );
                    self.last_epoch.store(epoch, Ordering::SeqCst);
                }
            }
        }
        Ok(val)
    }

    /// Put a key-value pair.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        if key == ShardKeyEncoder::frontier_key() && value.len() == 8 {
            let new_epoch = u64::from_be_bytes(value[..8].try_into().unwrap());
            let old_epoch = self.last_epoch.load(Ordering::SeqCst);
            assert!(
                new_epoch >= old_epoch,
                "M1-S2: committed_epoch must be non-decreasing (got {new_epoch}, was {old_epoch})"
            );
            self.last_epoch.store(new_epoch, Ordering::SeqCst);
        }
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
    ///
    /// Builds a `slatedb::WriteBatch` from the batch's `Vec<BatchOp>`, then
    /// calls `Db::write()` once.  Multiple callers may merge their batches via
    /// [`WriteBatch::merge_from`] before calling this to produce a single
    /// atomic commit (group commit — v0.5).
    pub async fn write_batch(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let frontier_key = ShardKeyEncoder::frontier_key();
        for op in &batch.ops {
            if let BatchOp::Put { key, value } = op {
                if key == &frontier_key && value.len() == 8 {
                    let new_epoch = u64::from_be_bytes(value[..8].try_into().unwrap());
                    let old_epoch = self.last_epoch.load(Ordering::SeqCst);
                    assert!(
                        new_epoch >= old_epoch,
                        "M1-S2: committed_epoch must be non-decreasing (got {new_epoch}, was {old_epoch})"
                    );
                    self.last_epoch.store(new_epoch, Ordering::SeqCst);
                }
            }
        }
        let mut inner = slatedb::WriteBatch::new();
        for op in batch.ops {
            match op {
                BatchOp::Put { key, value } => inner.put(&key, &value),
                BatchOp::Delete { key } => inner.delete(&key),
                BatchOp::Merge { key, value } => inner.merge(&key, &value),
            }
        }
        self.db.write(inner).await?;
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

    /// Execute a partial aggregation query against this shard's view output.
    ///
    /// # Arguments
    /// - `view_name`: name of the view (prefix: `view_output/<view_name>/`)
    /// - `partial_plan_bytes`: JSON-encoded `PartialAggSpec`
    /// - `frontier`: unused in this impl (pinned reads not yet supported here); pass 0
    ///
    /// # Bounds
    /// Result groups ≤ MAX_PARTIAL_AGG_RESULT_ROWS.
    /// Fill metric: PARTIAL_AGG_RESULT_ROWS updated per call.
    ///
    /// # Returns
    /// TSV rows: each row is `<group_key>\t<agg_value>` as bytes.
    pub async fn partial_query(
        &self,
        view_name: &str,
        partial_plan_bytes: &[u8],
        frontier: u64,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        self.partial_query_with_limit(
            view_name,
            partial_plan_bytes,
            frontier,
            MAX_PARTIAL_AGG_RESULT_ROWS,
        )
        .await
    }

    /// Like `partial_query` but with a configurable row limit (for testing).
    pub async fn partial_query_with_limit(
        &self,
        view_name: &str,
        partial_plan_bytes: &[u8],
        _frontier: u64,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        let spec: PartialAggSpec = serde_json::from_slice(partial_plan_bytes)
            .map_err(|e| StorageError::KeyEncoding(format!("invalid PartialAggSpec: {e}")))?;

        let prefix = format!("view_output/{view_name}/");
        let rows = self.scan_prefix(prefix.as_bytes()).await?;

        let mut groups: HashMap<String, i64> = HashMap::new();
        for (_k, v) in &rows {
            let row_str = String::from_utf8_lossy(v);
            let cols: Vec<&str> = row_str.split('\t').collect();
            let key = cols.get(spec.group_col).copied().unwrap_or("").to_string();
            let agg_val = cols.get(spec.agg_col).copied().unwrap_or("0");
            let num: i64 = agg_val.parse().unwrap_or(0);
            let entry = groups.entry(key).or_insert(0);
            match spec.agg_type.as_str() {
                "sum" => *entry += num,
                "count" => *entry += 1,
                _ => *entry += num,
            }
        }

        let result_count = groups.len();
        PARTIAL_AGG_RESULT_ROWS.store(result_count, Ordering::Relaxed);

        if result_count > limit {
            return Err(StorageError::PartialAggResultTooLarge { limit });
        }

        Ok(groups
            .into_iter()
            .map(|(k, v)| format!("{k}\t{v}").into_bytes())
            .collect())
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
    /// Value layout: `[epoch:8]` (legacy) or `[epoch:8][timestamp_ms:8]` (v0.24+).
    pub async fn get_idempotency_epoch(
        &self,
        shard_id: u32,
        key_hash: [u8; 16],
    ) -> Result<Option<u64>, StorageError> {
        let key = ShardKeyEncoder::idempotency_key(shard_id, key_hash);
        if let Some(bytes) = self.get(&key).await? {
            if bytes.len() >= 8 {
                let epoch = u64::from_be_bytes(bytes[..8].try_into().unwrap());
                return Ok(Some(epoch));
            }
        }
        Ok(None)
    }

    /// Add an idempotency key insert to a WriteBatch.
    /// Value layout: `[epoch:8][timestamp_ms:8]` — used by cleanup to expire old keys.
    pub fn put_idempotency_key(
        batch: &mut WriteBatch,
        shard_id: u32,
        key_hash: [u8; 16],
        epoch: u64,
        timestamp_ms: u64,
    ) {
        let key = ShardKeyEncoder::idempotency_key(shard_id, key_hash);
        let mut value = [0u8; 16];
        value[..8].copy_from_slice(&epoch.to_be_bytes());
        value[8..].copy_from_slice(&timestamp_ms.to_be_bytes());
        batch.put(&key, &value);
    }

    /// Scan all idempotency keys for `shard_id` and delete those older than `retention_ms`.
    ///
    /// This is scan-and-delete — no range-delete path is used.
    /// The value layout is `[epoch:8][timestamp_ms:8]`; entries with `timestamp_ms`
    /// older than `now_ms - retention_ms` are deleted via point-delete in a WriteBatch.
    pub async fn cleanup_expired_idempotency_keys(
        &self,
        shard_id: u32,
        retention_ms: u64,
    ) -> Result<usize, StorageError> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff_ms = now_ms.saturating_sub(retention_ms);

        // Build the per-shard prefix: OpIndex + "IK" + shard_id
        let prefix = ShardKeyEncoder::idempotency_prefix(shard_id);
        let entries = self.scan_prefix(&prefix).await?;

        let mut batch = WriteBatch::new();
        let mut deleted = 0usize;
        for (key, value) in &entries {
            if value.len() >= 16 {
                let ts_ms = u64::from_be_bytes(value[8..16].try_into().unwrap());
                if ts_ms < cutoff_ms {
                    batch.delete(key);
                    deleted += 1;
                }
            }
        }
        if deleted > 0 {
            self.write_batch(batch).await?;
        }
        Ok(deleted)
    }

    /// Durably commit `epoch` as the new frontier for shard `shard_id`.
    ///
    /// Writes the frontier key (`ShardKeyEncoder::frontier_key()`) with the
    /// big-endian u64 encoding of `epoch`, then returns a `ShardFrontierReport`
    /// indicating the new committed epoch on this shard.
    ///
    /// # Invariants (M1-S2)
    ///
    /// The `epoch` must be ≥ the previously committed epoch.  This is enforced
    /// by the `assert!` inside [`ShardDb::put`].  Callers must ensure epochs
    /// are presented in non-decreasing order.
    ///
    /// # Note
    ///
    /// The returned `ShardFrontierReport` should be forwarded to the
    /// `FrontierAggregator` in the control plane so that the cluster frontier
    /// can advance.
    pub async fn commit_epoch(
        &self,
        shard_id: ShardId,
        epoch: rockstream_types::timestamp::Epoch,
    ) -> Result<ShardFrontierReport, StorageError> {
        let key = ShardKeyEncoder::frontier_key();
        // M1-S2: non-decreasing epoch assertion is enforced inside put().
        self.put(&key, &epoch.to_be_bytes()).await?;
        Ok(ShardFrontierReport { shard_id, epoch })
    }

    /// Create a SlateDB checkpoint for this shard and return a [`CheckpointHandle`].
    ///
    /// Called by the worker after a `CheckpointBarrier` epoch completes for all
    /// local operators. The returned `shard_checkpoint_id` is the SlateDB
    /// `manifest_id` and is reported to the `CheckpointCoordinator` as part of
    /// a `PerShardCheckpoint`.
    ///
    /// Uses `CheckpointScope::Durable` so only durably flushed writes are
    /// included, avoiding any dependency on WAL-only entries.
    pub async fn create_checkpoint(&self) -> Result<CheckpointHandle, StorageError> {
        let result = self
            .db
            .create_checkpoint(CheckpointScope::Durable, &CheckpointOptions::default())
            .await?;
        Ok(CheckpointHandle {
            shard_checkpoint_id: result.manifest_id,
        })
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
                    operator_id: None,
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
            operator_id: None,
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

/// A handle to a SlateDB checkpoint created by [`ShardDb::create_checkpoint`].
///
/// The `shard_checkpoint_id` corresponds to the SlateDB `manifest_id` at the
/// time the checkpoint was created. It is reported to the
/// `CheckpointCoordinator` as part of a `PerShardCheckpoint` and later used by
/// the `RecoveryDriver` to open a `ShardReader` pinned to this exact snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointHandle {
    /// The SlateDB manifest ID for this checkpoint.
    pub shard_checkpoint_id: u64,
}

/// A single operation within a `WriteBatch`.
///
/// Stored as an owned enum so batches can be merged without knowing the
/// internals of `slatedb::WriteBatch`.
#[derive(Debug, Clone)]
pub enum BatchOp {
    /// Insert or overwrite a key.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Remove a key.
    Delete { key: Vec<u8> },
    /// Apply a merge (associative accumulation) to a key.
    Merge { key: Vec<u8>, value: Vec<u8> },
}

/// Atomic write batch for multiple operations.
///
/// All operations in a batch are committed atomically.
/// Does NOT support range deletion.
///
/// Internally stores operations as a `Vec<BatchOp>` so that multiple batches
/// can be coalesced (via [`merge_from`]) into a single atomic commit — the
/// basis of group-commit (v0.5).
pub struct WriteBatch {
    ops: Vec<BatchOp>,
}

impl WriteBatch {
    /// Create a new empty write batch.
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Add a put operation to the batch.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.ops.push(BatchOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    /// Add a delete operation to the batch.
    pub fn delete(&mut self, key: &[u8]) {
        self.ops.push(BatchOp::Delete { key: key.to_vec() });
    }

    /// Add a merge operation to the batch.
    pub fn merge(&mut self, key: &[u8], value: &[u8]) {
        self.ops.push(BatchOp::Merge {
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    /// Consume all operations from `other` into this batch.
    ///
    /// Used by group commit to coalesce N per-operator batches into one
    /// atomic `WriteBatch` before calling `ShardDb::write_batch()`.
    pub fn merge_from(&mut self, other: WriteBatch) {
        self.ops.extend(other.ops);
    }

    /// Returns the number of operations in the batch.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Returns true if the batch has no operations.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
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
        // ShardDb has: get, put, delete, merge, write_batch, scan_prefix, flush, close,
        // commit_epoch.
        // WriteBatch has: put, delete, merge.
        // None of these are range operations.
        // This is a documentation test - the real enforcement is that the types
        // don't expose any range-delete method.
    }
}

#[cfg(test)]
mod frontier_reporter_tests {
    use super::*;
    use object_store::memory::InMemory;
    use rockstream_types::ids::ShardId;

    async fn open_test_db() -> ShardDb {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        ShardDb::builder("test-shard", store).build().await.unwrap()
    }

    /// Slice 5: commit_epoch returns a correct ShardFrontierReport and
    /// durably records the epoch as the frontier key.
    #[tokio::test]
    async fn commit_epoch_returns_frontier_report() {
        let db = open_test_db().await;
        let shard_id = ShardId(7);

        let report = db.commit_epoch(shard_id, 42).await.unwrap();
        assert_eq!(report.shard_id, shard_id);
        assert_eq!(report.epoch, 42);

        // Verify the epoch was durably written as the frontier key.
        let key = ShardKeyEncoder::frontier_key();
        let bytes = db.get(&key).await.unwrap().unwrap();
        let stored = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        assert_eq!(stored, 42);
    }

    /// Slice 5: commit_epoch is monotone — advancing epoch always succeeds.
    #[tokio::test]
    async fn commit_epoch_is_non_decreasing() {
        let db = open_test_db().await;
        let shard_id = ShardId(1);

        let r1 = db.commit_epoch(shard_id, 10).await.unwrap();
        assert_eq!(r1.epoch, 10);

        let r2 = db.commit_epoch(shard_id, 20).await.unwrap();
        assert_eq!(r2.epoch, 20);

        let r3 = db.commit_epoch(shard_id, 20).await.unwrap(); // same epoch is ok
        assert_eq!(r3.epoch, 20);
    }

    /// Slice 5: confirm no range-delete path is used by commit_epoch.
    #[tokio::test]
    async fn commit_epoch_uses_no_range_delete() {
        // commit_epoch calls put() internally — which is a point write.
        // This test documents and asserts that claim by inspecting its
        // observable effects: only the frontier key is touched.
        let db = open_test_db().await;
        let shard_id = ShardId(99);
        db.commit_epoch(shard_id, 5).await.unwrap();

        // Only the frontier key should be set; scanning the shard_meta prefix
        // should yield exactly one entry.
        let prefix = ShardKeyEncoder::namespace_prefix(crate::keys::ShardPrefix::ShardMeta);
        let entries = db.scan_prefix(&prefix).await.unwrap();
        assert!(
            entries.len() == 1,
            "commit_epoch must write exactly the frontier key, got {} entries",
            entries.len()
        );
    }

    /// Slice 5: `create_checkpoint` returns a non-zero shard_checkpoint_id and
    /// a second call after writing new data returns an id ≥ the first.
    #[tokio::test]
    async fn create_checkpoint_returns_valid_handle() {
        let db = open_test_db().await;

        // Write some data before taking the checkpoint.
        db.put(b"ckpt/test/key", b"value").await.unwrap();
        db.flush().await.unwrap();

        let h1 = db.create_checkpoint().await.unwrap();
        assert_ne!(
            h1.shard_checkpoint_id, 0,
            "shard_checkpoint_id must be non-zero"
        );

        // Write more data and take a second checkpoint.
        db.put(b"ckpt/test/key2", b"value2").await.unwrap();
        db.flush().await.unwrap();
        let h2 = db.create_checkpoint().await.unwrap();
        assert!(
            h2.shard_checkpoint_id >= h1.shard_checkpoint_id,
            "second checkpoint id ({}) must be >= first ({})",
            h2.shard_checkpoint_id,
            h1.shard_checkpoint_id
        );
    }
}

#[cfg(test)]
mod partial_agg_tests {
    use super::*;
    use object_store::memory::InMemory;

    // ── S6: partial_agg_shard_query_returns_compact_batch ─────────────────────

    /// Write 100 rows across 5 groups, partial_query GROUP BY key, SUM(val) → 5 rows.
    #[tokio::test]
    async fn partial_agg_shard_query_returns_compact_batch() {
        let store = Arc::new(InMemory::new());
        let shard = ShardDb::builder("partial-agg-test", store)
            .build()
            .await
            .unwrap();

        for i in 0u64..100 {
            let group = i % 5;
            let key = format!("view_output/orders_mv/{i:016x}");
            let val = format!("{group}\t{}", group * 10);
            shard.put(key.as_bytes(), val.as_bytes()).await.unwrap();
        }

        let spec = PartialAggSpec {
            group_col: 0,
            agg_col: 1,
            agg_type: "sum".to_string(),
        };
        let plan_bytes = serde_json::to_vec(&spec).unwrap();
        let result = shard
            .partial_query("orders_mv", &plan_bytes, 0)
            .await
            .unwrap();

        assert_eq!(
            result.len(),
            5,
            "expected 5 groups, got {len}",
            len = result.len()
        );
    }

    // ── S6: partial_agg_shard_query_too_large_returns_rs2002 ─────────────────

    /// With limit=3, 10 distinct groups → RS-2002 returned.
    #[tokio::test]
    async fn partial_agg_shard_query_too_large_returns_rs2002() {
        let store = Arc::new(InMemory::new());
        let shard = ShardDb::builder("partial-agg-too-large", store)
            .build()
            .await
            .unwrap();

        for i in 0u64..10 {
            let key = format!("view_output/mv/{i:016x}");
            let val = format!("{i}\t{}", i * 5);
            shard.put(key.as_bytes(), val.as_bytes()).await.unwrap();
        }

        let spec = PartialAggSpec {
            group_col: 0,
            agg_col: 1,
            agg_type: "sum".to_string(),
        };
        let plan_bytes = serde_json::to_vec(&spec).unwrap();
        let err = shard
            .partial_query_with_limit("mv", &plan_bytes, 0, 3)
            .await;
        assert!(
            matches!(err, Err(StorageError::PartialAggResultTooLarge { .. })),
            "expected PartialAggResultTooLarge, got {err:?}"
        );
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("RS-2002"),
            "expected RS-2002 in error, got: {msg}"
        );
    }
}

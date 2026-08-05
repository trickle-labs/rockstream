//! Source-epoch registry with persisted partition→offset map (DESIGN.md §8.1.1, v0.21).
//!
//! Maintains a strictly-increasing `source_epoch` per connector, and records
//! the committed `{partition_id → OffsetToken}` map at each epoch boundary.
//!
//! Storage key: `connector/{id}/epoch_map/{source_epoch}` →
//!              `{ partition_id → committed_offset }`.
//!
//! ## Recovery
//!
//! On restart, the connector reads the highest committed `source_epoch`,
//! retrieves its partition→offset map, and resumes from those offsets.
//! Two operators consuming the same connector see the same `source_epoch`
//! sequence (DESIGN.md §8.1.1).

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use rockstream_storage::{keys::CatalogType, CatalogKeyEncoder, ShardDb, StorageError, WriteBatch};
use rockstream_types::ids::ConnectorId;
use rockstream_types::timestamp::Epoch;

// ─── OffsetToken ──────────────────────────────────────────────────────────────

/// Opaque serialised source position (DESIGN.md §13.3).
///
/// Kafka encodes `{partition_id → offset}`; Postgres CDC encodes an LSN;
/// S3 encodes a manifest pointer. The token is opaque to the registry.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OffsetToken(pub Vec<u8>);

impl OffsetToken {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

// ─── SourceEpochEntry ─────────────────────────────────────────────────────────

/// One committed entry in the epoch map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceEpochEntry {
    /// The monotone source epoch.
    pub source_epoch: Epoch,
    /// Committed partition→offset map for this epoch boundary.
    pub partition_offsets: BTreeMap<u64, OffsetToken>,
}

// ─── SourceEpochRegistry ──────────────────────────────────────────────────────

/// Maintains the per-connector strictly-increasing `source_epoch` and its
/// persisted `partition→offset` map (DESIGN.md §8.1.1).
///
/// The registry is pure in-memory state. The caller is responsible for
/// persisting the returned `SourceEpochEntry` atomically in the epoch
/// `WriteBatch` before advancing the epoch counter.
#[derive(Debug)]
pub struct SourceEpochRegistry {
    connector_id: ConnectorId,
    /// Current (uncommitted) source epoch.
    current_epoch: Epoch,
    /// The highest committed epoch and its partition→offset map.
    last_committed: Option<SourceEpochEntry>,
    /// Committed epoch history (bounded ring buffer, max 128 entries).
    history: BTreeMap<Epoch, SourceEpochEntry>,
}

/// The maximum number of durable prepared and committed checkpoint slots per source.
///
/// Slots are overwritten circularly, so recovery scans at most twice this many records.
pub const SOURCE_CHECKPOINT_HISTORY_MAX_ENTRIES: usize = 128;

/// Maximum bytes read by one source-runtime cleanup scan. Cleanup loops over
/// these bounded pages and deletes only the named source's point keys.
pub const SOURCE_CLEANUP_SCAN_PAGE_LIMIT: usize = 1024 * 1024;

const MAX_HISTORY: usize = SOURCE_CHECKPOINT_HISTORY_MAX_ENTRIES;

/// Durable state of a source checkpoint record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceCheckpointState {
    Prepared,
    Committed,
}

/// Versioned durable source checkpoint bound to the M3 input transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCheckpoint {
    pub version: u16,
    pub connector_id: ConnectorId,
    pub source_epoch: Epoch,
    pub token: OffsetToken,
    pub state: SourceCheckpointState,
    pub delivery_id: Option<String>,
    pub payload_digest: Option<[u8; 32]>,
}

impl SourceCheckpoint {
    pub fn prepared(connector_id: ConnectorId, source_epoch: Epoch, token: OffsetToken) -> Self {
        Self {
            version: 1,
            connector_id,
            source_epoch,
            token,
            state: SourceCheckpointState::Prepared,
            delivery_id: None,
            payload_digest: None,
        }
    }

    pub fn committed(&self) -> Self {
        let mut checkpoint = self.clone();
        checkpoint.state = SourceCheckpointState::Committed;
        checkpoint
    }
}

/// Persists source checkpoints using bounded point-write slots. Recovery reads only
/// committed records; prepared records can never advance an acknowledged source token.
#[derive(Clone)]
pub struct SourceCheckpointStore {
    db: Arc<ShardDb>,
    namespace_id: u128,
    connector_id: ConnectorId,
    cleanup_scan_pages: Arc<AtomicUsize>,
}

impl SourceCheckpointStore {
    pub fn new(db: Arc<ShardDb>, namespace_id: u128, connector_id: ConnectorId) -> Self {
        Self {
            db,
            namespace_id,
            connector_id,
            cleanup_scan_pages: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn prefix(&self, state: SourceCheckpointState) -> Vec<u8> {
        let state: &[u8] = match state {
            SourceCheckpointState::Prepared => b"source_checkpoint/prepared/",
            SourceCheckpointState::Committed => b"source_checkpoint/committed/",
        };
        CatalogKeyEncoder::encode_with_suffix(
            CatalogType::Connector,
            self.namespace_id,
            self.connector_id.0 as u128,
            state,
        )
    }

    fn key(&self, state: SourceCheckpointState, epoch: Epoch) -> Vec<u8> {
        let mut key = self.prefix(state);
        key.extend_from_slice(
            &(epoch % SOURCE_CHECKPOINT_HISTORY_MAX_ENTRIES as u64).to_be_bytes(),
        );
        key
    }

    fn encode(checkpoint: &SourceCheckpoint) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(checkpoint).map_err(|error| StorageError::KeyEncoding(error.to_string()))
    }

    /// Persist a prepared record before the M3 input commit starts.
    pub async fn prepare(&self, checkpoint: &SourceCheckpoint) -> Result<(), StorageError> {
        if checkpoint.connector_id != self.connector_id
            || checkpoint.state != SourceCheckpointState::Prepared
        {
            return Err(StorageError::KeyEncoding(
                "RS-4010: source checkpoint prepare has wrong identity or state; next steps: rebuild the checkpoint from this source's current epoch".to_string(),
            ));
        }
        let mut batch = WriteBatch::new();
        batch.put(
            &self.key(SourceCheckpointState::Prepared, checkpoint.source_epoch),
            &Self::encode(checkpoint)?,
        );
        self.db.write_batch(batch).await?;
        self.db.flush().await
    }

    /// Add the committed checkpoint to the caller's M3 input `WriteBatch`.
    pub fn append_committed(
        &self,
        batch: &mut WriteBatch,
        checkpoint: &SourceCheckpoint,
    ) -> Result<SourceCheckpoint, StorageError> {
        if checkpoint.connector_id != self.connector_id
            || checkpoint.state != SourceCheckpointState::Prepared
        {
            return Err(StorageError::KeyEncoding(
                "RS-4011: source checkpoint commit requires a prepared checkpoint for this source; next steps: prepare the source input before committing M3".to_string(),
            ));
        }
        let committed = checkpoint.committed();
        batch.put(
            &self.key(SourceCheckpointState::Committed, committed.source_epoch),
            &Self::encode(&committed)?,
        );
        batch.delete(&self.key(SourceCheckpointState::Prepared, committed.source_epoch));
        Ok(committed)
    }

    /// Commit source input and its checkpoint atomically, then make the durable
    /// commit visible to restart recovery.
    pub async fn commit_m3(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.db.write_batch(batch).await?;
        self.db.flush().await
    }

    /// Return exactly the highest valid committed checkpoint, ignoring prepared
    /// records and stale circular-history slots.
    pub async fn highest_committed(&self) -> Result<Option<SourceCheckpoint>, StorageError> {
        let records = self
            .db
            .scan_prefix(&self.prefix(SourceCheckpointState::Committed))
            .await?;
        let mut highest = None;
        for (_, value) in records {
            let checkpoint: SourceCheckpoint = serde_json::from_slice(&value)
                .map_err(|error| StorageError::KeyEncoding(error.to_string()))?;
            if checkpoint.version != 1
                || checkpoint.connector_id != self.connector_id
                || checkpoint.state != SourceCheckpointState::Committed
            {
                continue;
            }
            if highest.as_ref().is_none_or(|current: &SourceCheckpoint| {
                checkpoint.source_epoch > current.source_epoch
            }) {
                highest = Some(checkpoint);
            }
        }
        Ok(highest)
    }

    /// Number of currently retained durable checkpoint records, for the
    /// `source_checkpoint_history_entries` fill-level metric.
    pub async fn history_entries(&self) -> Result<usize, StorageError> {
        let prepared = self
            .db
            .scan_prefix(&self.prefix(SourceCheckpointState::Prepared))
            .await?;
        let committed = self
            .db
            .scan_prefix(&self.prefix(SourceCheckpointState::Committed))
            .await?;
        Ok(prepared.len() + committed.len())
    }

    /// Remove every retained checkpoint for this source through bounded prefix
    /// scans and point deletes. SlateDB range deletion is intentionally never
    /// used because it is not snapshot-safe for source recovery.
    pub async fn cleanup(&self) -> Result<usize, StorageError> {
        let mut removed = 0;
        for state in [
            SourceCheckpointState::Prepared,
            SourceCheckpointState::Committed,
        ] {
            loop {
                let (records, _) = self
                    .db
                    .scan_prefix_bounded(&self.prefix(state), SOURCE_CLEANUP_SCAN_PAGE_LIMIT)
                    .await?;
                if records.is_empty() {
                    break;
                }
                let mut batch = WriteBatch::new();
                for (key, _) in &records {
                    batch.delete(key);
                }
                self.db.write_batch(batch).await?;
                self.db.flush().await?;
                removed += records.len();
                self.cleanup_scan_pages.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(removed)
    }

    /// Cumulative bounded cleanup pages, exposed as
    /// `source_cleanup_scan_pages` for operator monitoring.
    pub fn cleanup_scan_pages(&self) -> usize {
        self.cleanup_scan_pages.load(Ordering::Relaxed)
    }
}

impl SourceEpochRegistry {
    /// Create a new registry starting at epoch 0 (no committed epochs).
    pub fn new(connector_id: ConnectorId) -> Self {
        Self {
            connector_id,
            current_epoch: 0,
            last_committed: None,
            history: BTreeMap::new(),
        }
    }

    /// Restore registry state from recovery (highest committed epoch + map).
    ///
    /// Called at startup after reading `connector/{id}/epoch_map/` from the
    /// control-plane catalog.
    pub fn restore(
        connector_id: ConnectorId,
        last_committed_epoch: Epoch,
        partition_offsets: BTreeMap<u64, OffsetToken>,
    ) -> Self {
        let entry = SourceEpochEntry {
            source_epoch: last_committed_epoch,
            partition_offsets,
        };
        let mut registry = Self {
            connector_id,
            current_epoch: last_committed_epoch,
            last_committed: Some(entry.clone()),
            history: BTreeMap::new(),
        };
        registry.history.insert(last_committed_epoch, entry);
        registry
    }

    /// Returns the connector ID.
    pub fn connector_id(&self) -> ConnectorId {
        self.connector_id
    }

    /// Returns the current (next-to-commit) source epoch.
    pub fn current_epoch(&self) -> Epoch {
        self.current_epoch
    }

    /// Returns the last committed epoch entry, or `None` if no epoch committed.
    pub fn last_committed(&self) -> Option<&SourceEpochEntry> {
        self.last_committed.as_ref()
    }

    /// Returns the partition→offset map that should be resumed from on recovery.
    ///
    /// Returns `None` if no epoch has been committed (fresh start).
    pub fn recovery_offsets(&self) -> Option<&BTreeMap<u64, OffsetToken>> {
        self.last_committed.as_ref().map(|e| &e.partition_offsets)
    }

    /// Prepare the next epoch commit entry.
    ///
    /// The caller must commit the returned `SourceEpochEntry` atomically with
    /// the epoch `WriteBatch` before calling [`commit_epoch`].
    ///
    /// `partition_offsets` maps each partition ID to the connector's current
    /// committed offset at this epoch boundary.
    pub fn prepare_commit(
        &self,
        partition_offsets: BTreeMap<u64, OffsetToken>,
    ) -> SourceEpochEntry {
        SourceEpochEntry {
            source_epoch: self.current_epoch + 1,
            partition_offsets,
        }
    }

    /// Advance to the next epoch after the `WriteBatch` has been durably flushed.
    ///
    /// `entry` must be the value returned by a previous [`prepare_commit`] call
    /// for this epoch.
    ///
    /// # Panics
    ///
    /// Panics if `entry.source_epoch != current_epoch + 1` (non-monotone advance).
    pub fn commit_epoch(&mut self, entry: SourceEpochEntry) {
        let expected = self.current_epoch + 1;
        assert_eq!(
            entry.source_epoch, expected,
            "RS-4006: source_epoch must advance monotonically: \
             connector={}, expected={expected}, got={}",
            self.connector_id, entry.source_epoch
        );
        self.current_epoch = entry.source_epoch;
        self.last_committed = Some(entry.clone());

        // Maintain bounded history ring.
        self.history.insert(entry.source_epoch, entry);
        while self.history.len() > MAX_HISTORY {
            if let Some(oldest_key) = self.history.keys().next().copied() {
                self.history.remove(&oldest_key);
            }
        }
    }

    /// Look up the partition→offset map for a specific past epoch.
    ///
    /// Returns `None` if the epoch is not in the bounded history window.
    pub fn offsets_for_epoch(&self, epoch: Epoch) -> Option<&BTreeMap<u64, OffsetToken>> {
        self.history.get(&epoch).map(|e| &e.partition_offsets)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn offsets(map: &[(&str, &str)]) -> BTreeMap<u64, OffsetToken> {
        map.iter()
            .enumerate()
            .map(|(i, (_, v))| (i as u64, OffsetToken::new(v.as_bytes().to_vec())))
            .collect()
    }

    #[test]
    fn fresh_registry_starts_at_epoch_zero() {
        let reg = SourceEpochRegistry::new(ConnectorId(1));
        assert_eq!(reg.current_epoch(), 0);
        assert!(reg.last_committed().is_none());
        assert!(reg.recovery_offsets().is_none());
    }

    #[test]
    fn commit_epoch_advances_monotonically() {
        let mut reg = SourceEpochRegistry::new(ConnectorId(1));
        let entry = reg.prepare_commit(offsets(&[("p0", "offset-10")]));
        reg.commit_epoch(entry);
        assert_eq!(reg.current_epoch(), 1);
        assert_eq!(reg.last_committed().unwrap().source_epoch, 1);
    }

    #[test]
    fn commit_epoch_ten_epochs() {
        let mut reg = SourceEpochRegistry::new(ConnectorId(2));
        for i in 0..10 {
            let offs = offsets(&[("p0", &format!("offset-{i}"))]);
            let entry = reg.prepare_commit(offs);
            reg.commit_epoch(entry);
        }
        assert_eq!(reg.current_epoch(), 10);
    }

    #[test]
    fn recovery_offsets_returns_last_committed() {
        let mut reg = SourceEpochRegistry::new(ConnectorId(3));
        let offs = offsets(&[("p0", "offset-100"), ("p1", "offset-200")]);
        let entry = reg.prepare_commit(offs.clone());
        reg.commit_epoch(entry);
        let recovered = reg.recovery_offsets().unwrap();
        assert_eq!(recovered.get(&0).unwrap().as_bytes(), b"offset-100");
        assert_eq!(recovered.get(&1).unwrap().as_bytes(), b"offset-200");
    }

    #[test]
    fn restore_sets_recovery_offsets() {
        let offs = offsets(&[("p0", "offset-42")]);
        let reg = SourceEpochRegistry::restore(ConnectorId(4), 5, offs.clone());
        assert_eq!(reg.current_epoch(), 5);
        assert_eq!(reg.last_committed().unwrap().source_epoch, 5);
        let recovered = reg.recovery_offsets().unwrap();
        assert_eq!(recovered.get(&0).unwrap().as_bytes(), b"offset-42");
    }

    #[test]
    fn offsets_for_epoch_returns_correct_entry() {
        let mut reg = SourceEpochRegistry::new(ConnectorId(5));
        for i in 0..3 {
            let entry = reg.prepare_commit(offsets(&[("p0", &format!("offset-{i}"))]));
            reg.commit_epoch(entry);
        }
        // epoch 2 should have "offset-1"
        let e2 = reg.offsets_for_epoch(2).unwrap();
        assert_eq!(e2.get(&0).unwrap().as_bytes(), b"offset-1");
    }

    #[test]
    #[should_panic(expected = "RS-4006")]
    fn commit_epoch_panics_on_non_monotone_advance() {
        let mut reg = SourceEpochRegistry::new(ConnectorId(6));
        let entry = reg.prepare_commit(BTreeMap::new());
        // Manually corrupt the epoch to skip ahead.
        let bad_entry = SourceEpochEntry {
            source_epoch: 5,
            partition_offsets: BTreeMap::new(),
        };
        reg.commit_epoch(bad_entry);
        let _ = entry; // suppress unused warning
    }

    #[test]
    fn history_is_bounded_to_max_history() {
        let mut reg = SourceEpochRegistry::new(ConnectorId(7));
        for _ in 0..(MAX_HISTORY + 10) {
            let entry = reg.prepare_commit(BTreeMap::new());
            reg.commit_epoch(entry);
        }
        assert!(reg.history.len() <= MAX_HISTORY);
    }
}

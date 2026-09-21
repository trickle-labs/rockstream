//! Shard-migration coordination and durable state (v0.46).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::Mutex;
use rockstream_storage::{ShardDb, ShardReader, WriteBatch};
use rockstream_types::audit::AuditEvent;
use rockstream_types::checkpoint::{ClusterCheckpoint, PerShardCheckpoint};
use rockstream_types::ids::ShardId;
use rockstream_types::lease::ShardLease;
use rockstream_types::migration::{
    BucketSet, MigrationRecord, MigrationState, MIGRATION_RECORD_VERSION,
};
use rockstream_types::timestamp::Epoch;
use thiserror::Error;

use crate::audit::FileAuditLog;
use crate::checkpoint::{CheckpointCoordinator, CoordinatorError};
use crate::shard::ShardManager;

/// Default `SNAPSHOTTING` timeout.
pub const DEFAULT_SNAPSHOTTING_TIMEOUT: Duration = Duration::from_secs(30);
/// Default `COPYING` timeout.
pub const DEFAULT_COPYING_TIMEOUT: Duration = Duration::from_secs(300);
/// Default `CUTOVER` timeout.
pub const DEFAULT_CUTOVER_TIMEOUT: Duration = Duration::from_secs(60);
/// Default `CATCHING_UP` lag budget.
pub const DEFAULT_CUTOVER_LAG_BUDGET: Duration = Duration::from_millis(100);
/// Default verify sample rate for small buckets.
pub const DEFAULT_VERIFY_SAMPLE_RATE: f64 = 1.0;
/// Named upper bound for verify scan buffering.
pub const MAX_VERIFY_SCAN_KEYS: usize = 1024;
/// Named upper bound for consumer-frontier tracking.
pub const MAX_CONSUMER_FRONTIERS: usize = 1024;
/// Named upper bound for bucket-map-version observer tracking.
pub const MAX_VERSION_OBSERVERS: usize = 64;
/// Maximum active migration records loaded during control restart.
pub const MAX_ACTIVE_MIGRATIONS: usize = 1024;
/// Maximum rows in one migration copy chunk.
pub const MAX_COPY_CHUNK_ROWS: usize = 256;
/// Maximum key/value bytes in one migration copy chunk.
pub const MAX_COPY_CHUNK_BYTES: usize = 1024 * 1024;

/// Fill-level metric for bounded migration buffers/maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationFillLevel {
    pub used: usize,
    pub capacity: usize,
}

impl MigrationFillLevel {
    pub fn fraction(&self) -> f64 {
        self.used as f64 / self.capacity as f64
    }
}

/// Exact counters from one bounded migration-copy pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MigrationCopyStats {
    pub chunks: usize,
    pub copied_rows: u64,
    pub copied_bytes: u64,
    pub max_chunk_rows: usize,
    pub max_chunk_bytes: usize,
}

/// Phase start timestamps for [`MigrationCoordinator::drive_planned_to_copying`],
/// bundled to keep the call within clippy's argument-count budget.
#[derive(Debug, Clone, Copy)]
pub struct PhaseClocks {
    pub snapshotting_started_at: Instant,
    pub copying_started_at: Instant,
}

/// Errors returned by migration coordination and persistence.
#[derive(Debug, Error)]
pub enum MigrationError {
    #[error(
        "{code}: illegal migration transition {from} -> {to}; \
         next_steps: {next_steps}"
    )]
    IllegalTransition {
        code: &'static str,
        from: MigrationState,
        to: MigrationState,
        next_steps: &'static str,
    },
    #[error(
        "{code}: migration state {state} exceeded timeout budget ({elapsed:?} > {budget:?}); \
         next_steps: {next_steps}"
    )]
    StateTimeout {
        code: &'static str,
        state: MigrationState,
        elapsed: Duration,
        budget: Duration,
        next_steps: &'static str,
    },
    #[error(
        "{code}: migration verify scan window full ({used}/{max}); \
         next_steps: {next_steps}"
    )]
    VerifyWindowFull {
        code: &'static str,
        used: usize,
        max: usize,
        next_steps: &'static str,
    },
    #[error(
        "{code}: bucket_map_version mismatch for {component}: expected {expected}, got {got}; \
         next_steps: {next_steps}"
    )]
    BucketMapVersionMismatch {
        code: &'static str,
        component: String,
        expected: u64,
        got: u64,
        next_steps: &'static str,
    },
    #[error(
        "{code}: tracked observer/frontier capacity exceeded ({used}/{max}); \
         next_steps: {next_steps}"
    )]
    RegistryFull {
        code: &'static str,
        used: usize,
        max: usize,
        next_steps: &'static str,
    },
    #[error(
        "RS-5033: donor reclamation is not frontier-safe in state {state}; \
         next_steps: wait until every consumer reaches the committed cutover frontier"
    )]
    ReclamationNotReady { state: MigrationState },
    #[error(
        "RS-5034: verification divergence detected for key {key_hex}; \
         next_steps: return to dual-writing, recopy the divergent bucket set, and re-run verification"
    )]
    VerificationDiverged { key_hex: String },
    #[error("RS-0003: migration storage error: {0}")]
    Storage(String),
    #[error("RS-3602: checkpoint coordinator error during migration: {0}")]
    Checkpoint(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MigrationLoadError {
    #[error("migration record is absent")]
    Missing,
    #[error("migration record is corrupt: {0}")]
    Corrupt(String),
    #[error("migration record storage is unavailable: {0}")]
    Unavailable(String),
    #[error("unsupported migration record version {found}, expected {expected}")]
    UnsupportedVersion { found: u16, expected: u16 },
}

impl PartialEq for MigrationError {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl Eq for MigrationError {}

fn illegal_transition(from: MigrationState, to: MigrationState) -> MigrationError {
    MigrationError::IllegalTransition {
        code: "RS-5030",
        from,
        to,
        next_steps: "drive the migration through the documented next state only, or resume from the persisted record instead of skipping states",
    }
}

fn timeout_error(state: MigrationState, elapsed: Duration, budget: Duration) -> MigrationError {
    MigrationError::StateTimeout {
        code: "RS-1030",
        state,
        elapsed,
        budget,
        next_steps: "check donor/recipient shard health, then retry or abort the migration; increase the timeout only if the cluster is healthy and the migration is legitimately larger than expected",
    }
}

/// A donor or recipient shard used by [`MigrationCoordinator`].
#[derive(Clone)]
pub struct MigrationShard {
    pub shard_id: ShardId,
    pub path: String,
    pub object_store: Arc<dyn ObjectStore>,
    pub db: ShardDb,
    pub frontier: Epoch,
}

/// Durable object-store-backed persistence for one migration record per key.
#[derive(Clone)]
pub struct MigrationPersistentStore {
    store: Arc<dyn ObjectStore>,
    active_prefix: Path,
    history_prefix: Path,
}

/// Durable migration state reconciled with the current shard leases after restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationRecovery {
    pub record: MigrationRecord,
    pub current_leases: BTreeMap<ShardId, Option<ShardLease>>,
}

impl MigrationPersistentStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            active_prefix: Path::from("topology/migration"),
            history_prefix: Path::from("topology/migration_history"),
        }
    }

    fn active_path(&self, migration_id: &str) -> Path {
        self.active_prefix.child(format!("{migration_id}.json"))
    }

    fn history_path(&self, migration_id: &str) -> Path {
        self.history_prefix.child(format!("{migration_id}.json"))
    }

    pub async fn load(&self, migration_id: &str) -> Result<MigrationRecord, MigrationLoadError> {
        self.load_path(&self.active_path(migration_id)).await
    }

    pub async fn load_history(
        &self,
        migration_id: &str,
    ) -> Result<MigrationRecord, MigrationLoadError> {
        self.load_path(&self.history_path(migration_id)).await
    }

    /// Load every active migration during control restart with a hard bound.
    pub async fn load_active(&self) -> Result<Vec<MigrationRecord>, MigrationLoadError> {
        let mut listing = self.store.list(Some(&self.active_prefix));
        let active_prefix = format!("{}/", self.active_prefix.as_ref());
        let mut records = Vec::new();
        while let Some(entry) = listing.next().await {
            let meta = entry.map_err(|error| {
                MigrationLoadError::Unavailable(format!("list active migrations: {error}"))
            })?;
            if !meta.location.as_ref().starts_with(&active_prefix)
                || meta
                    .location
                    .filename()
                    .is_none_or(|name| !name.ends_with(".json"))
            {
                continue;
            }
            if records.len() == MAX_ACTIVE_MIGRATIONS {
                return Err(MigrationLoadError::Unavailable(format!(
                    "active migration record limit {MAX_ACTIVE_MIGRATIONS} exceeded"
                )));
            }
            records.push(self.load_path(&meta.location).await?);
        }
        records.sort_by(|left, right| left.migration_id.cmp(&right.migration_id));
        Ok(records)
    }

    /// Load active migrations with the leases a resumed driver must revalidate.
    pub async fn recover_active(
        &self,
        shard_manager: &ShardManager,
    ) -> Result<Vec<MigrationRecovery>, MigrationLoadError> {
        Ok(self
            .load_active()
            .await?
            .into_iter()
            .map(|record| {
                let current_leases = record
                    .donor_shards
                    .iter()
                    .copied()
                    .chain(std::iter::once(record.recipient_shard))
                    .map(|shard_id| (shard_id, shard_manager.get(shard_id)))
                    .collect();
                MigrationRecovery {
                    record,
                    current_leases,
                }
            })
            .collect())
    }

    async fn load_path(&self, path: &Path) -> Result<MigrationRecord, MigrationLoadError> {
        let result = self.store.get(path).await.map_err(|error| match error {
            object_store::Error::NotFound { .. } => MigrationLoadError::Missing,
            error => MigrationLoadError::Unavailable(error.to_string()),
        })?;
        let bytes = result
            .bytes()
            .await
            .map_err(|error| MigrationLoadError::Unavailable(error.to_string()))?;
        let record: MigrationRecord = serde_json::from_slice(&bytes)
            .map_err(|error| MigrationLoadError::Corrupt(error.to_string()))?;
        if record.record_version != MIGRATION_RECORD_VERSION {
            return Err(MigrationLoadError::UnsupportedVersion {
                found: record.record_version,
                expected: MIGRATION_RECORD_VERSION,
            });
        }
        Ok(record)
    }

    pub async fn save(&self, record: &MigrationRecord) -> Result<(), MigrationError> {
        let bytes = serde_json::to_vec(record)
            .map_err(|e| MigrationError::Storage(format!("serialize migration record: {e}")))?;
        self.store
            .put(&self.active_path(&record.migration_id), bytes.into())
            .await
            .map_err(|e| MigrationError::Storage(format!("persist migration record: {e}")))?;
        Ok(())
    }

    pub async fn transition(
        &self,
        record: &mut MigrationRecord,
        next: MigrationState,
        audit: Option<&FileAuditLog>,
    ) -> Result<bool, MigrationError> {
        let mut next_record = record.clone();
        let changed = next_record
            .apply_transition(next)
            .map_err(|_| illegal_transition(record.state, next))?;
        assert_single_authoritative(&next_record); // M6-S1
        self.save(&next_record).await?;
        *record = next_record;
        emit_transition_audit(audit, record, next, changed, None);
        Ok(changed)
    }

    pub async fn archive(
        &self,
        record: &MigrationRecord,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), MigrationError> {
        let bytes = serde_json::to_vec(record)
            .map_err(|e| MigrationError::Storage(format!("serialize migration history: {e}")))?;
        self.store
            .put(&self.history_path(&record.migration_id), bytes.into())
            .await
            .map_err(|e| MigrationError::Storage(format!("persist migration history: {e}")))?;
        match self
            .store
            .delete(&self.active_path(&record.migration_id))
            .await
        {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                return Err(MigrationError::Storage(format!(
                    "remove active migration record: {error}"
                )))
            }
        }
        emit_transition_audit(audit, record, MigrationState::Done, true, Some("archived"));
        Ok(())
    }
}

/// Tracks downstream consumer frontiers for `GC_ELIGIBLE` gating.
#[derive(Default)]
pub struct MigrationConsumerFrontierTracker {
    inner: Mutex<BTreeMap<String, Epoch>>,
}

impl MigrationConsumerFrontierTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(
        &self,
        consumer: impl Into<String>,
        frontier: Epoch,
    ) -> Result<(), MigrationError> {
        let consumer = consumer.into();
        let mut guard = self.inner.lock();
        if !guard.contains_key(&consumer) && guard.len() >= MAX_CONSUMER_FRONTIERS {
            return Err(MigrationError::RegistryFull {
                code: "RS-5032",
                used: guard.len(),
                max: MAX_CONSUMER_FRONTIERS,
                next_steps: "reduce the number of tracked downstream consumers or increase the consumer-frontier tracker bound if memory headroom allows",
            });
        }
        guard.insert(consumer, frontier);
        Ok(())
    }

    pub fn minimum_frontier(&self) -> Option<Epoch> {
        self.inner.lock().values().copied().min()
    }

    pub fn fill_level(&self) -> MigrationFillLevel {
        let guard = self.inner.lock();
        MigrationFillLevel {
            used: guard.len(),
            capacity: MAX_CONSUMER_FRONTIERS,
        }
    }
}

/// Tracks which readers/receivers/gateways have observed a bucket-map version.
#[derive(Default)]
pub struct BucketMapVersionTracker {
    inner: Mutex<BTreeMap<String, u64>>,
}

impl BucketMapVersionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(
        &self,
        component: impl Into<String>,
        version: u64,
    ) -> Result<(), MigrationError> {
        let component = component.into();
        let mut guard = self.inner.lock();
        if !guard.contains_key(&component) && guard.len() >= MAX_VERSION_OBSERVERS {
            return Err(MigrationError::RegistryFull {
                code: "RS-5032",
                used: guard.len(),
                max: MAX_VERSION_OBSERVERS,
                next_steps: "reduce the number of version observers or increase the observer bound if memory headroom allows",
            });
        }
        guard.insert(component, version);
        Ok(())
    }

    pub fn fill_level(&self) -> MigrationFillLevel {
        let guard = self.inner.lock();
        MigrationFillLevel {
            used: guard.len(),
            capacity: MAX_VERSION_OBSERVERS,
        }
    }

    fn all_observed(&self, expected: u64, required: &[&str]) -> Result<bool, MigrationError> {
        let guard = self.inner.lock();
        for component in required {
            match guard.get(*component) {
                Some(got) if *got == expected => {}
                Some(got) => {
                    return Err(MigrationError::BucketMapVersionMismatch {
                        code: "RS-5032",
                        component: (*component).to_string(),
                        expected,
                        got: *got,
                        next_steps: "wait for every reader, exchange receiver, and gateway to observe the new bucket_map_version, then retry the migration step under the current version",
                    });
                }
                None => return Ok(false),
            }
        }
        Ok(true)
    }
}

/// Coordinator implementing the v0.46 migration state machine.
pub struct MigrationCoordinator {
    migration_store: Option<Arc<MigrationPersistentStore>>,
    snapshotting_timeout: Duration,
    copying_timeout: Duration,
    cutover_timeout: Duration,
    cutover_lag_budget: Duration,
    verify_sample_rate: f64,
}

impl Default for MigrationCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationCoordinator {
    pub fn new() -> Self {
        Self {
            migration_store: None,
            snapshotting_timeout: DEFAULT_SNAPSHOTTING_TIMEOUT,
            copying_timeout: DEFAULT_COPYING_TIMEOUT,
            cutover_timeout: DEFAULT_CUTOVER_TIMEOUT,
            cutover_lag_budget: DEFAULT_CUTOVER_LAG_BUDGET,
            verify_sample_rate: DEFAULT_VERIFY_SAMPLE_RATE,
        }
    }

    pub fn with_migration_store(mut self, store: Arc<MigrationPersistentStore>) -> Self {
        self.migration_store = Some(store);
        self
    }

    pub fn with_timeouts(
        mut self,
        snapshotting_timeout: Duration,
        copying_timeout: Duration,
        cutover_timeout: Duration,
    ) -> Self {
        self.snapshotting_timeout = snapshotting_timeout;
        self.copying_timeout = copying_timeout;
        self.cutover_timeout = cutover_timeout;
        self
    }

    pub fn with_cutover_lag_budget(mut self, lag_budget: Duration) -> Self {
        self.cutover_lag_budget = lag_budget;
        self
    }

    pub fn with_verify_sample_rate(mut self, verify_sample_rate: f64) -> Self {
        self.verify_sample_rate = verify_sample_rate;
        self
    }

    pub fn verify_scan_fill_level(&self, scanned: usize) -> MigrationFillLevel {
        MigrationFillLevel {
            used: scanned,
            capacity: MAX_VERIFY_SCAN_KEYS,
        }
    }

    async fn save_record(
        &self,
        record: &mut MigrationRecord,
        next_record: MigrationRecord,
    ) -> Result<(), MigrationError> {
        if let Some(store) = &self.migration_store {
            store.save(&next_record).await?;
        }
        *record = next_record;
        Ok(())
    }

    async fn transition_record_durable(
        &self,
        record: &mut MigrationRecord,
        next: MigrationState,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), MigrationError> {
        if let Some(store) = &self.migration_store {
            store.transition(record, next, audit).await?;
        } else {
            self.transition_record(record, next, audit)?;
        }
        Ok(())
    }

    pub async fn drive_planned_to_copying(
        &self,
        record: &mut MigrationRecord,
        donors: &[MigrationShard],
        recipient: &MigrationShard,
        checkpoint_coordinator: &CheckpointCoordinator,
        clocks: PhaseClocks,
        audit: Option<&FileAuditLog>,
    ) -> Result<ClusterCheckpoint, MigrationError> {
        let mut snapshot_record = record.clone();
        snapshot_record.donor_frontiers = donors
            .iter()
            .map(|donor| (donor.shard_id, donor.frontier))
            .collect();
        if let Some(donor_frontier) = donors.iter().map(|donor| donor.frontier).min() {
            snapshot_record.record_frontiers(donor_frontier, recipient.frontier);
        }
        self.save_record(record, snapshot_record).await?;
        self.transition_record_durable(record, MigrationState::Snapshotting, audit)
            .await?;
        self.abort_on_timeout(
            record,
            MigrationState::Snapshotting,
            clocks.snapshotting_started_at,
            self.snapshotting_timeout,
            audit,
        )
        .await?;

        let checkpoint_id = checkpoint_coordinator
            .begin_checkpoint(|_, _| {})
            .map_err(checkpoint_error)?;
        let mut checkpoint_started = record.clone();
        checkpoint_started.cluster_checkpoint_id = Some(checkpoint_id.0);
        self.save_record(record, checkpoint_started).await?;

        for donor in donors {
            if let (Some(&shard_checkpoint_id), Some(snapshot_id)) = (
                record.donor_checkpoints.get(&donor.shard_id),
                record.donor_checkpoint_snapshots.get(&donor.shard_id),
            ) {
                checkpoint_coordinator
                    .record_shard_checkpoint(
                        donor.shard_id,
                        PerShardCheckpoint::new(checkpoint_id, shard_checkpoint_id)
                            .with_snapshot_id(snapshot_id.clone()),
                        |_| Ok(()),
                    )
                    .map_err(checkpoint_error)?;
                continue;
            }

            let mut checkpoint_intent = record.clone();
            checkpoint_intent.checkpoint_intents.insert(donor.shard_id);
            self.save_record(record, checkpoint_intent).await?;
            let handle =
                donor.db.create_checkpoint().await.map_err(|e| {
                    MigrationError::Storage(format!("create donor checkpoint: {e}"))
                })?;
            checkpoint_coordinator
                .record_shard_checkpoint(
                    donor.shard_id,
                    PerShardCheckpoint::new(checkpoint_id, handle.shard_checkpoint_id)
                        .with_snapshot_id(handle.snapshot_id.clone()),
                    |_| Ok(()),
                )
                .map_err(checkpoint_error)?;
            let mut checkpoint_completed = record.clone();
            checkpoint_completed
                .checkpoint_intents
                .remove(&donor.shard_id);
            checkpoint_completed
                .donor_checkpoints
                .insert(donor.shard_id, handle.shard_checkpoint_id);
            checkpoint_completed
                .donor_checkpoint_snapshots
                .insert(donor.shard_id, handle.snapshot_id);
            self.save_record(record, checkpoint_completed).await?;
        }
        let cluster_checkpoint = checkpoint_coordinator
            .latest_committed()
            .ok_or_else(|| MigrationError::Checkpoint("missing committed checkpoint".into()))?;

        self.transition_record_durable(record, MigrationState::Copying, audit)
            .await?;
        self.abort_on_timeout(
            record,
            MigrationState::Copying,
            clocks.copying_started_at,
            self.copying_timeout,
            audit,
        )
        .await?;

        self.copy_bounded_chunks(record, donors, recipient).await?;
        Ok(cluster_checkpoint)
    }

    /// Copy migration state through backpressured, bounded pages.
    ///
    /// Re-running this method is safe: recipient writes are idempotent and the
    /// persisted checkpoint snapshot keeps the source view stable.
    pub async fn copy_bounded_chunks(
        &self,
        record: &mut MigrationRecord,
        donors: &[MigrationShard],
        recipient: &MigrationShard,
    ) -> Result<MigrationCopyStats, MigrationError> {
        let mut stats = MigrationCopyStats::default();
        for donor in donors {
            let completed_pages = record
                .copy_cursors
                .get(&donor.shard_id)
                .copied()
                .unwrap_or(0);
            let snapshot_id = record
                .donor_checkpoint_snapshots
                .get(&donor.shard_id)
                .cloned();
            let reader = if let Some(snapshot_id) = snapshot_id {
                ShardReader::open_with_snapshot_id(
                    donor.path.clone(),
                    donor.object_store.clone(),
                    &snapshot_id,
                )
                .await
            } else {
                ShardReader::open(donor.path.clone(), donor.object_store.clone()).await
            }
            .map_err(|e| MigrationError::Storage(format!("open donor reader: {e}")))?;
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            let producer = tokio::spawn(async move {
                reader
                    .scan_prefix_pages(b"", MAX_COPY_CHUNK_ROWS, MAX_COPY_CHUNK_BYTES, sender)
                    .await;
            });

            let mut page_index = 0;
            let copy_result: Result<(), MigrationError> = async {
                while let Some(page) = receiver.recv().await {
                    let entries =
                        page.map_err(|e| MigrationError::Storage(format!("scan donor page: {e}")))?;
                    if page_index < completed_pages {
                        page_index += 1;
                        continue;
                    }
                    let entries: Vec<_> =
                        if entries.iter().any(|(key, _)| key.starts_with(b"bucket/")) {
                            entries
                                .into_iter()
                                .filter(|(key, _)| key_in_buckets(key, &record.buckets))
                                .collect()
                        } else {
                            entries
                        };
                    let chunk_rows = entries.len();
                    let chunk_bytes: usize = entries
                        .iter()
                        .map(|(key, value)| key.len() + value.len())
                        .sum();
                    let mut intent = record.clone();
                    intent.copy_intents.insert(donor.shard_id, page_index);
                    self.save_record(record, intent).await?;
                    if !entries.is_empty() {
                        let mut batch = WriteBatch::new();
                        for (key, value) in &entries {
                            batch.put(key, value);
                        }
                        recipient.db.write_batch(batch).await.map_err(|e| {
                            MigrationError::Storage(format!("copy into recipient: {e}"))
                        })?;
                        recipient.db.flush().await.map_err(|e| {
                            MigrationError::Storage(format!("flush copy chunk: {e}"))
                        })?;
                        stats.chunks += 1;
                        stats.copied_rows += chunk_rows as u64;
                        stats.copied_bytes += chunk_bytes as u64;
                        stats.max_chunk_rows = stats.max_chunk_rows.max(chunk_rows);
                        stats.max_chunk_bytes = stats.max_chunk_bytes.max(chunk_bytes);
                    }
                    let mut completed = record.clone();
                    completed.copy_intents.remove(&donor.shard_id);
                    completed
                        .copy_cursors
                        .insert(donor.shard_id, page_index + 1);
                    completed.record_progress(
                        completed.copied_bytes.unwrap_or(0) + chunk_bytes as u64,
                        completed.copied_rows.unwrap_or(0) + chunk_rows as u64,
                    );
                    self.save_record(record, completed).await?;
                    page_index += 1;
                }
                Ok(())
            }
            .await;
            if copy_result.is_err() {
                producer.abort();
                let _ = producer.await;
                copy_result?;
            } else {
                producer
                    .await
                    .map_err(|e| MigrationError::Storage(format!("scan donor page task: {e}")))?;
            }
        }
        Ok(stats)
    }

    pub async fn begin_dual_writing(
        &self,
        record: &mut MigrationRecord,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), MigrationError> {
        self.transition_record_durable(record, MigrationState::DualWriting, audit)
            .await
    }

    pub async fn advance_to_catching_up(
        &self,
        record: &mut MigrationRecord,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), MigrationError> {
        self.transition_record_durable(record, MigrationState::CatchingUp, audit)
            .await
    }

    pub async fn advance_to_fencing_old_if_caught_up(
        &self,
        record: &mut MigrationRecord,
        donor_frontier: Epoch,
        recipient_frontier: Epoch,
        audit: Option<&FileAuditLog>,
    ) -> Result<bool, MigrationError> {
        let lag = donor_frontier.saturating_sub(recipient_frontier);
        let lag_budget_epochs = self.cutover_lag_budget.as_millis() as u64;
        if lag > lag_budget_epochs {
            return Ok(false);
        }
        let mut completed = record.clone();
        completed.donor_frontiers = completed
            .donor_shards
            .iter()
            .copied()
            .map(|shard_id| (shard_id, donor_frontier))
            .collect();
        completed.record_frontiers(donor_frontier, recipient_frontier);
        self.save_record(record, completed).await?;
        self.transition_record_durable(record, MigrationState::FencingOld, audit)
            .await?;
        Ok(true)
    }

    pub async fn await_cutover_readiness(
        &self,
        record: &mut MigrationRecord,
        observed_versions: &BucketMapVersionTracker,
        required_components: &[&str],
        started_at: Instant,
        audit: Option<&FileAuditLog>,
    ) -> Result<bool, MigrationError> {
        let committed_frontier = record.planned_frontier;
        self.await_cutover_readiness_at_frontier(
            record,
            observed_versions,
            required_components,
            committed_frontier,
            started_at,
            audit,
        )
        .await
    }

    /// Require both observer convergence and a committed frontier before cutover.
    pub async fn await_cutover_readiness_at_frontier(
        &self,
        record: &mut MigrationRecord,
        observed_versions: &BucketMapVersionTracker,
        required_components: &[&str],
        committed_frontier: Epoch,
        started_at: Instant,
        audit: Option<&FileAuditLog>,
    ) -> Result<bool, MigrationError> {
        if committed_frontier < record.planned_frontier {
            return Ok(false);
        }
        if record.state == MigrationState::FencingOld {
            self.transition_record_durable(record, MigrationState::Cutover, audit)
                .await?;
        }
        match observed_versions
            .all_observed(record.target_bucket_map_version, required_components)?
        {
            true => {
                let mut completed = record.clone();
                completed.cutover_epoch = Some(committed_frontier);
                self.save_record(record, completed).await?;
                Ok(true)
            }
            false => {
                if started_at.elapsed() > self.cutover_timeout {
                    self.abort_on_timeout(
                        record,
                        MigrationState::Cutover,
                        started_at,
                        self.cutover_timeout,
                        audit,
                    )
                    .await?;
                }
                Ok(false)
            }
        }
    }

    pub async fn verify_or_rollback(
        &self,
        record: &mut MigrationRecord,
        donor: &MigrationShard,
        recipient: &MigrationShard,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), MigrationError> {
        if record.state == MigrationState::Cutover {
            self.transition_record_durable(record, MigrationState::Verifying, audit)
                .await?;
        }
        let mut verification_started = record.clone();
        verification_started.record_verification_progress(0);
        self.save_record(record, verification_started).await?;
        let donor_entries = filtered_entries(donor, &record.buckets).await?;
        let recipient_entries = filtered_entries(recipient, &record.buckets).await?;
        let scanned = donor_entries.len().max(recipient_entries.len());
        if scanned > MAX_VERIFY_SCAN_KEYS {
            return Err(MigrationError::VerifyWindowFull {
                code: "RS-5031",
                used: scanned,
                max: MAX_VERIFY_SCAN_KEYS,
                next_steps: "reduce verify_sample_rate, split the migration into fewer buckets, or increase the verify scan bound if memory headroom allows",
            });
        }
        let _sample_rate = self.verify_sample_rate;
        if donor_entries != recipient_entries {
            self.transition_record_durable(record, MigrationState::DualWriting, audit)
                .await?;
            let key_hex = donor_entries
                .iter()
                .zip(recipient_entries.iter())
                .find_map(|(left, right)| {
                    if left != right {
                        Some(hex_key(&left.0))
                    } else {
                        None
                    }
                })
                .or_else(|| donor_entries.first().map(|(k, _)| hex_key(k)))
                .or_else(|| recipient_entries.first().map(|(k, _)| hex_key(k)))
                .unwrap_or_else(|| "none".to_string());
            return Err(MigrationError::VerificationDiverged { key_hex });
        }
        let mut verification_completed = record.clone();
        verification_completed.record_verification_progress(100);
        self.save_record(record, verification_completed).await?;
        Ok(())
    }

    pub async fn maybe_enter_gc_eligible(
        &self,
        record: &mut MigrationRecord,
        frontiers: &MigrationConsumerFrontierTracker,
        audit: Option<&FileAuditLog>,
    ) -> Result<bool, MigrationError> {
        let Some(cutover_epoch) = record.cutover_epoch else {
            return Ok(false);
        };
        let Some(min_frontier) = frontiers.minimum_frontier() else {
            return Ok(false);
        };
        if min_frontier < cutover_epoch {
            return Ok(false);
        }
        self.transition_record_durable(record, MigrationState::GcEligible, audit)
            .await?;
        Ok(true)
    }

    pub async fn finish_done(
        &self,
        record: &mut MigrationRecord,
        donor: &MigrationShard,
        store: Option<&MigrationPersistentStore>,
        audit: Option<&FileAuditLog>,
    ) -> Result<CleanupStats, MigrationError> {
        let durable_store = store.or(self.migration_store.as_deref());
        if record.state == MigrationState::Done {
            if let Some(store) = durable_store {
                store.archive(record, audit).await?;
            }
            return Ok(CleanupStats { deleted_keys: 0 });
        }
        if record.state != MigrationState::GcEligible {
            return Err(MigrationError::ReclamationNotReady {
                state: record.state,
            });
        }
        let mut cleanup_started = record.clone();
        cleanup_started.cleanup_intent = true;
        if let Some(store) = durable_store {
            store.save(&cleanup_started).await?;
            *record = cleanup_started;
        } else {
            *record = cleanup_started;
        }
        let stats = cleanup_donor_buckets(donor, &record.buckets).await?;
        if let Some(store) = durable_store {
            store
                .transition(record, MigrationState::Done, audit)
                .await?;
            store.archive(record, audit).await?;
        } else {
            self.transition_record(record, MigrationState::Done, audit)?;
        }
        Ok(stats)
    }

    fn transition_record(
        &self,
        record: &mut MigrationRecord,
        next: MigrationState,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), MigrationError> {
        let changed = record
            .apply_transition(next)
            .map_err(|_| illegal_transition(record.state, next))?;
        assert_single_authoritative(record); // M6-S1
        emit_transition_audit(audit, record, next, changed, None);
        Ok(())
    }

    async fn abort_on_timeout(
        &self,
        record: &mut MigrationRecord,
        state: MigrationState,
        started_at: Instant,
        budget: Duration,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), MigrationError> {
        let elapsed = started_at.elapsed();
        if elapsed <= budget {
            return Ok(());
        }
        let err = timeout_error(state, elapsed, budget);
        self.transition_record_durable(record, MigrationState::Aborted, audit)
            .await?;
        Err(err)
    }
}

/// Cleanup result proving scan-and-delete usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupStats {
    pub deleted_keys: usize,
}

pub fn bucket_key_prefix(bucket: u64) -> Vec<u8> {
    format!("bucket/{bucket}/").into_bytes()
}

fn key_in_buckets(key: &[u8], buckets: &BucketSet) -> bool {
    buckets
        .buckets
        .iter()
        .any(|bucket| key.starts_with(&bucket_key_prefix(*bucket)))
}

fn emit_transition_audit(
    audit: Option<&FileAuditLog>,
    record: &MigrationRecord,
    next: MigrationState,
    changed: bool,
    detail_suffix: Option<&str>,
) {
    if let Some(audit) = audit {
        let mut detail = format!("state={}, changed={changed}", next);
        if let Some(suffix) = detail_suffix {
            detail.push_str(&format!(", detail={suffix}"));
        }
        let event = AuditEvent::now(
            "control",
            "migration.transition",
            record.migration_id.clone(),
        )
        .with_detail(detail);
        let _ = audit.append(&event);
    }
}

fn checkpoint_error(err: CoordinatorError) -> MigrationError {
    MigrationError::Checkpoint(err.to_string())
}

fn assert_single_authoritative(record: &MigrationRecord) {
    let donor_authoritative = matches!(
        record.state,
        MigrationState::Planned
            | MigrationState::Snapshotting
            | MigrationState::Copying
            | MigrationState::DualWriting
            | MigrationState::CatchingUp
            | MigrationState::FencingOld
            | MigrationState::Aborted
            | MigrationState::Failed
    );
    let recipient_authoritative = matches!(
        record.state,
        MigrationState::Cutover
            | MigrationState::Verifying
            | MigrationState::GcEligible
            | MigrationState::Done
    );
    assert!(
        donor_authoritative as u8 + recipient_authoritative as u8 == 1,
        "M6-S1 violation: migration state {} must have exactly one authoritative shard",
        record.state
    );
}

async fn filtered_entries(
    shard: &MigrationShard,
    buckets: &BucketSet,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, MigrationError> {
    let mut filtered = Vec::new();
    for bucket in &buckets.buckets {
        let prefix = bucket_key_prefix(*bucket);
        let reader = ShardReader::open(shard.path.clone(), shard.object_store.clone())
            .await
            .map_err(|e| MigrationError::Storage(format!("open shard verifier: {e}")))?;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let producer = tokio::spawn({
            async move {
                reader
                    .scan_prefix_pages(&prefix, MAX_COPY_CHUNK_ROWS, MAX_COPY_CHUNK_BYTES, sender)
                    .await;
            }
        });
        while let Some(page) = receiver.recv().await {
            let entries =
                page.map_err(|e| MigrationError::Storage(format!("scan shard entries: {e}")))?;
            if filtered.len() + entries.len() > MAX_VERIFY_SCAN_KEYS {
                producer.abort();
                let _ = producer.await;
                return Err(MigrationError::VerifyWindowFull {
                    code: "RS-5031",
                    used: MAX_VERIFY_SCAN_KEYS + 1,
                    max: MAX_VERIFY_SCAN_KEYS,
                    next_steps: "reduce verify_sample_rate, split the migration into fewer buckets, or increase the verify scan bound if memory headroom allows",
                });
            }
            filtered.extend(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.to_vec(), value.to_vec())),
            );
        }
        producer
            .await
            .map_err(|e| MigrationError::Storage(format!("scan shard page task: {e}")))?;
    }
    filtered.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(filtered)
}

async fn cleanup_donor_buckets(
    donor: &MigrationShard,
    buckets: &BucketSet,
) -> Result<CleanupStats, MigrationError> {
    let mut deleted_keys = 0usize;
    for bucket in &buckets.buckets {
        let prefix = bucket_key_prefix(*bucket);
        let reader = ShardReader::open(donor.path.clone(), donor.object_store.clone())
            .await
            .map_err(|e| MigrationError::Storage(format!("open donor cleanup reader: {e}")))?;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let producer = tokio::spawn({
            async move {
                reader
                    .scan_prefix_pages(&prefix, MAX_COPY_CHUNK_ROWS, MAX_COPY_CHUNK_BYTES, sender)
                    .await;
            }
        });
        while let Some(page) = receiver.recv().await {
            let entries = page
                .map_err(|e| MigrationError::Storage(format!("scan donor cleanup prefix: {e}")))?;
            if entries.is_empty() {
                continue;
            }
            let mut batch = WriteBatch::new();
            for (key, _) in entries.into_iter() {
                batch.delete(key.as_ref());
                deleted_keys += 1;
            }
            donor
                .db
                .write_batch(batch)
                .await
                .map_err(|e| MigrationError::Storage(format!("delete donor keys: {e}")))?;
            donor
                .db
                .flush()
                .await
                .map_err(|e| MigrationError::Storage(format!("flush donor cleanup: {e}")))?;
        }
        producer
            .await
            .map_err(|e| MigrationError::Storage(format!("scan donor cleanup task: {e}")))?;
    }
    Ok(CleanupStats { deleted_keys })
}

fn hex_key(key: &[u8]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

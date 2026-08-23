//! Shard-migration coordination and durable state (v0.46).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::Mutex;
use rockstream_storage::{ShardDb, ShardReader, WriteBatch};
use rockstream_types::audit::AuditEvent;
use rockstream_types::checkpoint::{ClusterCheckpoint, PerShardCheckpoint};
use rockstream_types::ids::ShardId;
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};
use rockstream_types::timestamp::Epoch;
use thiserror::Error;

use crate::audit::FileAuditLog;
use crate::checkpoint::{CheckpointCoordinator, CoordinatorError};

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
pub struct MigrationPersistentStore {
    store: Arc<dyn ObjectStore>,
    active_prefix: Path,
    history_prefix: Path,
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

    pub async fn load(&self, migration_id: &str) -> Option<MigrationRecord> {
        let path = self.active_path(migration_id);
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.ok()?;
                serde_json::from_slice(&bytes).ok()
            }
            Err(_) => None,
        }
    }

    pub async fn load_history(&self, migration_id: &str) -> Option<MigrationRecord> {
        let path = self.history_path(migration_id);
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.ok()?;
                serde_json::from_slice(&bytes).ok()
            }
            Err(_) => None,
        }
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
        let changed = record
            .apply_transition(next)
            .map_err(|_| illegal_transition(record.state, next))?;
        assert_single_authoritative(record); // M6-S1
        self.save(record).await?;
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
        let _ = self
            .store
            .delete(&self.active_path(&record.migration_id))
            .await;
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
            snapshotting_timeout: DEFAULT_SNAPSHOTTING_TIMEOUT,
            copying_timeout: DEFAULT_COPYING_TIMEOUT,
            cutover_timeout: DEFAULT_CUTOVER_TIMEOUT,
            cutover_lag_budget: DEFAULT_CUTOVER_LAG_BUDGET,
            verify_sample_rate: DEFAULT_VERIFY_SAMPLE_RATE,
        }
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

    pub async fn drive_planned_to_copying(
        &self,
        record: &mut MigrationRecord,
        donors: &[MigrationShard],
        recipient: &MigrationShard,
        checkpoint_coordinator: &CheckpointCoordinator,
        clocks: PhaseClocks,
        audit: Option<&FileAuditLog>,
    ) -> Result<ClusterCheckpoint, MigrationError> {
        self.transition_record(record, MigrationState::Snapshotting, audit)?;
        self.abort_on_timeout(
            record,
            MigrationState::Snapshotting,
            clocks.snapshotting_started_at,
            self.snapshotting_timeout,
            audit,
        )?;

        let checkpoint_id = checkpoint_coordinator
            .begin_checkpoint(|_, _| {})
            .map_err(checkpoint_error)?;

        for donor in donors {
            let handle =
                donor.db.create_checkpoint().await.map_err(|e| {
                    MigrationError::Storage(format!("create donor checkpoint: {e}"))
                })?;
            record
                .donor_checkpoints
                .insert(donor.shard_id, handle.shard_checkpoint_id);
            record
                .donor_checkpoint_snapshots
                .insert(donor.shard_id, handle.snapshot_id.clone());
            checkpoint_coordinator
                .record_shard_checkpoint(
                    donor.shard_id,
                    PerShardCheckpoint::new(checkpoint_id, handle.shard_checkpoint_id)
                        .with_snapshot_id(handle.snapshot_id.clone()),
                    |_| Ok(()),
                )
                .map_err(checkpoint_error)?;
        }
        let cluster_checkpoint = checkpoint_coordinator
            .latest_committed()
            .ok_or_else(|| MigrationError::Checkpoint("missing committed checkpoint".into()))?;

        self.transition_record(record, MigrationState::Copying, audit)?;
        self.abort_on_timeout(
            record,
            MigrationState::Copying,
            clocks.copying_started_at,
            self.copying_timeout,
            audit,
        )?;

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

            while let Some(page) = receiver.recv().await {
                let entries =
                    page.map_err(|e| MigrationError::Storage(format!("scan donor page: {e}")))?;
                let chunk_rows = entries.len();
                let chunk_bytes: usize = entries
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum();
                let mut batch = WriteBatch::new();
                for (key, value) in &entries {
                    batch.put(key, value);
                }
                recipient
                    .db
                    .write_batch(batch)
                    .await
                    .map_err(|e| MigrationError::Storage(format!("copy into recipient: {e}")))?;
                recipient
                    .db
                    .flush()
                    .await
                    .map_err(|e| MigrationError::Storage(format!("flush copy chunk: {e}")))?;
                stats.chunks += 1;
                stats.copied_rows += chunk_rows as u64;
                stats.copied_bytes += chunk_bytes as u64;
                stats.max_chunk_rows = stats.max_chunk_rows.max(chunk_rows);
                stats.max_chunk_bytes = stats.max_chunk_bytes.max(chunk_bytes);
                record.record_progress(
                    record.copied_bytes.unwrap_or(0) + chunk_bytes as u64,
                    record.copied_rows.unwrap_or(0) + chunk_rows as u64,
                );
            }
            producer
                .await
                .map_err(|e| MigrationError::Storage(format!("scan donor page task: {e}")))?;
        }
        Ok(stats)
    }

    pub fn begin_dual_writing(
        &self,
        record: &mut MigrationRecord,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), MigrationError> {
        self.transition_record(record, MigrationState::DualWriting, audit)
    }

    pub fn advance_to_catching_up(
        &self,
        record: &mut MigrationRecord,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), MigrationError> {
        self.transition_record(record, MigrationState::CatchingUp, audit)
    }

    pub fn advance_to_fencing_old_if_caught_up(
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
        self.transition_record(record, MigrationState::FencingOld, audit)?;
        Ok(true)
    }

    pub fn await_cutover_readiness(
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
    }

    /// Require both observer convergence and a committed frontier before cutover.
    pub fn await_cutover_readiness_at_frontier(
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
            self.transition_record(record, MigrationState::Cutover, audit)?;
        }
        match observed_versions
            .all_observed(record.target_bucket_map_version, required_components)?
        {
            true => {
                record.cutover_epoch = Some(committed_frontier);
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
                    )?;
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
            self.transition_record(record, MigrationState::Verifying, audit)?;
        }
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
            self.transition_record(record, MigrationState::DualWriting, audit)?;
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
        Ok(())
    }

    pub fn maybe_enter_gc_eligible(
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
        self.transition_record(record, MigrationState::GcEligible, audit)?;
        Ok(true)
    }

    pub async fn finish_done(
        &self,
        record: &mut MigrationRecord,
        donor: &MigrationShard,
        store: Option<&MigrationPersistentStore>,
        audit: Option<&FileAuditLog>,
    ) -> Result<CleanupStats, MigrationError> {
        if record.state != MigrationState::GcEligible {
            return Err(MigrationError::ReclamationNotReady {
                state: record.state,
            });
        }
        let stats = cleanup_donor_buckets(donor, &record.buckets).await?;
        self.transition_record(record, MigrationState::Done, audit)?;
        if let Some(store) = store {
            store.save(record).await?;
            store.archive(record, audit).await?;
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

    fn abort_on_timeout(
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
        let _ = record.apply_transition(MigrationState::Aborted);
        emit_transition_audit(
            audit,
            record,
            MigrationState::Aborted,
            true,
            Some("timeout"),
        );
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
        let entries = shard
            .db
            .scan_prefix(&prefix)
            .await
            .map_err(|e| MigrationError::Storage(format!("scan shard entries: {e}")))?;
        for (key, value) in entries {
            filtered.push((key.to_vec(), value.to_vec()));
        }
    }
    filtered.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(filtered)
}

async fn cleanup_donor_buckets(
    donor: &MigrationShard,
    buckets: &BucketSet,
) -> Result<CleanupStats, MigrationError> {
    let mut batch = WriteBatch::new();
    let mut deleted_keys = 0usize;
    for bucket in &buckets.buckets {
        let prefix = bucket_key_prefix(*bucket);
        let entries = donor
            .db
            .scan_prefix(&prefix)
            .await
            .map_err(|e| MigrationError::Storage(format!("scan donor cleanup prefix: {e}")))?;
        for (key, _) in entries {
            batch.delete(&key);
            deleted_keys += 1;
        }
    }
    if deleted_keys > 0 {
        donor
            .db
            .write_batch(batch)
            .await
            .map_err(|e| MigrationError::Storage(format!("delete donor keys: {e}")))?;
    }
    Ok(CleanupStats { deleted_keys })
}

fn hex_key(key: &[u8]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}

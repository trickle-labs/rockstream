//! Disaggregated SlateDB compaction and ADR 0006 horizon safety.
//!
//! Runs standalone SlateDB compaction (`slatedb::CompactorBuilder`) against
//! shared object storage so stateful IVM workers can disable inline compaction
//! (`settings.compactor_options = None`). Strictly respects ADR 0006 horizons
//! across active readers, historical `AS OF EPOCH` queries, online backups, and
//! replay dependencies.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use object_store::ObjectStore;
use rockstream_types::ids::{ArrangementId, ShardId};
use rockstream_types::metrics::{read_storage_pressure_signals, set_storage_pressure_signals};
use slatedb::admin::AdminBuilder;
use slatedb::compactor::Compactor;
use slatedb::config::CompactorOptions;
use slatedb::CompactorBuilder;

use crate::arrangement_catalog::ArrangementCatalog;
use crate::error::StorageError;
use crate::keys::join_arrangement_bloom_filter_policy;
use crate::merge_registry::SumCountMergeOperator;
use crate::slatedb_metrics::instrumented_metrics_recorder;

/// Configuration for a standalone shard compactor.
#[derive(Debug, Clone)]
pub struct StandaloneCompactorConfig {
    pub max_concurrent_compactions: usize,
    pub poll_interval: Duration,
    pub manifest_poll_interval: Duration,
    pub max_sst_size: usize,
    pub filter_bits_per_key: Option<u32>,
}

impl Default for StandaloneCompactorConfig {
    fn default() -> Self {
        Self {
            max_concurrent_compactions: 4,
            poll_interval: Duration::from_secs(5),
            manifest_poll_interval: Duration::from_secs(1),
            max_sst_size: 256 * 1024 * 1024,
            filter_bits_per_key: Some(10),
        }
    }
}

impl From<&rockstream_types::config::CompactorConfig> for StandaloneCompactorConfig {
    fn from(cfg: &rockstream_types::config::CompactorConfig) -> Self {
        Self {
            max_concurrent_compactions: cfg.max_concurrent_compactions.max(1),
            poll_interval: Duration::from_millis(cfg.poll_interval_ms.max(50)),
            manifest_poll_interval: Duration::from_millis(cfg.manifest_poll_interval_ms.max(50)),
            max_sst_size: cfg.max_sst_size_bytes.max(1024 * 1024),
            filter_bits_per_key: Some(10),
        }
    }
}

/// Snapshot of LSM compaction debt observed on a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CompactionDebtReport {
    pub l0_sst_count: u64,
    pub pending_compaction_bytes: u64,
    pub active_checkpoints: u64,
}

/// Enforces ADR 0006 compaction horizon safety across active readers,
/// historical `AS OF EPOCH` queries, online backups/migrations, and replay.
#[derive(Debug, Default)]
pub struct CompactionHorizonGuard {
    /// Named reader/query/backup pins -> minimum required epoch.
    pins: Mutex<BTreeMap<String, u64>>,
    /// Minimum epoch required by durable replay recovery.
    replay_horizon: Mutex<Option<u64>>,
}

impl CompactionHorizonGuard {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin a named reader, `AS OF EPOCH` query, or backup/migration horizon.
    pub fn pin_horizon(&self, pin_id: impl Into<String>, epoch: u64) {
        self.pins.lock().unwrap().insert(pin_id.into(), epoch);
    }

    /// Release a previously registered horizon pin (idempotent).
    pub fn release_horizon(&self, pin_id: &str) {
        self.pins.lock().unwrap().remove(pin_id);
    }

    /// Update the replay horizon (minimum epoch still needed for replay).
    pub fn set_replay_horizon(&self, epoch: Option<u64>) {
        *self.replay_horizon.lock().unwrap() = epoch;
    }

    /// Compute the safe compaction horizon (the minimum of all active pins and
    /// the replay horizon). Compaction must not reclaim state at or above this
    /// epoch. Returns `None` if no horizons are pinned.
    pub fn safe_compaction_horizon(&self) -> Option<u64> {
        let min_pin = self.pins.lock().unwrap().values().copied().min();
        let replay = *self.replay_horizon.lock().unwrap();
        match (min_pin, replay) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// Returns `true` iff `epoch` is strictly below all active reader, query,
    /// backup, and replay horizons.
    pub fn can_reclaim_epoch(&self, epoch: u64) -> bool {
        self.safe_compaction_horizon()
            .is_none_or(|safe| epoch < safe)
    }

    /// Reclaim unreferenced arrangements in `catalog` strictly below the safe
    /// compaction horizon. No-op if no safe horizon is established.
    pub async fn reclaim_arrangements(&self, catalog: &ArrangementCatalog) -> Vec<ArrangementId> {
        let Some(safe_horizon) = self.safe_compaction_horizon() else {
            return Vec::new();
        };
        catalog.reclaim_unreferenced_arrangements(safe_horizon).await
    }
}

/// Handle to a running standalone SlateDB [`Compactor`] for a single shard.
pub struct ShardCompactorHandle {
    shard_id: ShardId,
    compactor: Compactor,
    is_running: Arc<AtomicBool>,
    last_error: Arc<Mutex<Option<String>>>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl ShardCompactorHandle {
    /// Build and spawn a standalone [`Compactor`] for `shard_id` against `object_store`.
    pub fn spawn(
        shard_id: ShardId,
        path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: StandaloneCompactorConfig,
    ) -> Self {
        let mut options = CompactorOptions::default();
        options.max_concurrent_compactions = config.max_concurrent_compactions;
        options.poll_interval = config.poll_interval;

        let mut builder = CompactorBuilder::new(path.into(), object_store)
            .with_options(options)
            .with_merge_operator(Arc::new(SumCountMergeOperator));

        if let Some(bits) = config.filter_bits_per_key {
            builder = builder.with_filter_policies(vec![join_arrangement_bloom_filter_policy(bits)]);
        }
        if let Ok(metric_shard_id) = u16::try_from(shard_id.0) {
            builder = builder.with_metrics_recorder(instrumented_metrics_recorder(metric_shard_id));
        }

        let compactor = builder.build();
        let compactor_run = compactor.clone();
        let is_running = Arc::new(AtomicBool::new(true));
        let is_running_clone = Arc::clone(&is_running);
        let last_error = Arc::new(Mutex::new(None));
        let last_error_clone = Arc::clone(&last_error);

        let task = tokio::spawn(async move {
            tracing::info!(shard = shard_id.0, "standalone shard compactor started");
            if let Err(err) = compactor_run.run().await {
                tracing::error!(shard = shard_id.0, error = %err, "standalone shard compactor exited with error");
                *last_error_clone.lock().unwrap() = Some(err.to_string());
            }
            is_running_clone.store(false, Ordering::Release);
            tracing::info!(shard = shard_id.0, "standalone shard compactor stopped");
        });

        Self {
            shard_id,
            compactor,
            is_running,
            last_error,
            task: Mutex::new(Some(task)),
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Acquire)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    /// Gracefully stop the compactor and await its background task.
    pub async fn stop(&self) {
        if let Err(err) = self.compactor.stop().await {
            tracing::warn!(shard = self.shard_id.0, error = %err, "compactor stop returned error");
        }
        let handle = self.task.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        self.is_running.store(false, Ordering::Release);
    }
}

/// Inspect a shard's SlateDB manifest to measure L0 compaction debt and active
/// checkpoints, updating `StoragePressureSignals` metrics.
pub async fn inspect_compaction_debt(
    path: impl Into<String>,
    object_store: Arc<dyn ObjectStore>,
) -> Result<CompactionDebtReport, StorageError> {
    let admin = AdminBuilder::new(path.into(), object_store).build();
    let state_view = admin
        .read_compactor_state_view()
        .await
        .map_err(StorageError::Slate)?;
    let manifest = state_view.manifest();
    let l0_sst_count = manifest.l0().len() as u64;
    let pending_compaction_bytes = manifest
        .l0()
        .iter()
        .map(|sst| sst.estimate_size())
        .sum::<u64>();
    let active_checkpoints = admin
        .list_checkpoints(None)
        .await
        .map(|checkpoints| checkpoints.len() as u64)
        .unwrap_or(0);

    let mut signals = read_storage_pressure_signals();
    signals.l0_backlog = l0_sst_count;
    signals.pending_compaction_bytes = pending_compaction_bytes;
    set_storage_pressure_signals(&signals);

    Ok(CompactionDebtReport {
        l0_sst_count,
        pending_compaction_bytes,
        active_checkpoints,
    })
}

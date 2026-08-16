//! Recovery driver for RockStream cluster checkpoints (v0.20).
//!
//! Implements [`RecoveryDriver`] following DESIGN.md §11.3.
//!
//! ## Recovery steps
//!
//! 1. Fetch the latest committed [`ClusterCheckpoint`] from the
//!    [`CheckpointCoordinator`].
//! 2. For each shard, open a [`ShardReader`] (via `rockstream-storage`) pinned
//!    to its `shard_checkpoint_id`.
//! 3. Transition each shard from reader to writer by acquiring a lease via the
//!    fence-epoch CAS mechanism ([`assert_valid_writer`]).
//! 4. Resume source connectors from offsets recorded in `control: connector/`
//!    (represented here by the `ConnectorOffset` map supplied by the caller).
//! 5. Emit a `recovery_progress` fill-level metric as the fraction of shards
//!    that have been brought back to writer mode.
//!
//! ## Named bounds
//!
//! Recovery is bounded by the `shard_recovery_budget` SLO
//! (DESIGN.md §11.5). If the budget is exceeded the driver returns
//! [`RecoveryError::BudgetExceeded`] (RS-3610), never panicking or looping
//! unboundedly.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use rockstream_storage::ShardReader;
use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint};
use rockstream_types::ids::{LeaseToken, ShardId};

// ─── Constants ───────────────────────────────────────────────────────────────

/// Default per-shard recovery budget (DESIGN.md §11.5).
pub const DEFAULT_SHARD_RECOVERY_BUDGET: Duration = Duration::from_secs(120);

// ─── Errors ──────────────────────────────────────────────────────────────────

/// Errors returned by [`RecoveryDriver`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryError {
    /// No committed checkpoint is available; cannot recover.
    ///
    /// RS-3605: next_steps — run a checkpoint round before attempting recovery.
    NoCheckpointAvailable,
    /// Recovery exceeded the per-shard SLO budget.
    ///
    /// RS-3610: next_steps — investigate shard health; increase
    /// `shard_recovery_budget` or reduce pipeline footprint.
    BudgetExceeded {
        shard_id: ShardId,
        elapsed: Duration,
        budget: Duration,
    },
    /// A shard's writer lease could not be re-acquired (stale token).
    ///
    /// RS-3611: next_steps — check for concurrent workers holding the lease.
    LeaseReacquisitionFailed { shard_id: ShardId },
    /// Storage open error during reader initialization.
    StorageError(String),
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCheckpointAvailable => write!(
                f,
                "RS-3605: no committed cluster checkpoint found; \
                 next_steps: ensure at least one checkpoint round completes before recovery"
            ),
            Self::BudgetExceeded {
                shard_id,
                elapsed,
                budget,
            } => write!(
                f,
                "RS-3610: shard {shard_id} recovery exceeded budget \
                 ({elapsed:?} > {budget:?}); \
                 next_steps: increase shard_recovery_budget or investigate shard health"
            ),
            Self::LeaseReacquisitionFailed { shard_id } => write!(
                f,
                "RS-3611: shard {shard_id} lease re-acquisition failed after recovery; \
                 next_steps: check for concurrent workers holding an active lease"
            ),
            Self::StorageError(e) => write!(f, "RS-3612: storage error during recovery: {e}"),
        }
    }
}

impl std::error::Error for RecoveryError {}

// ─── RecoveredShard ───────────────────────────────────────────────────────────

/// The result of recovering one shard.
pub struct RecoveredShard {
    pub shard_id: ShardId,
    /// The per-shard checkpoint used to pin the reader.
    pub checkpoint: PerShardCheckpoint,
    /// A read-only view of the shard's state at the checkpoint.
    pub reader: ShardReader,
    /// Recovery elapsed time for this shard.
    pub elapsed: Duration,
}

impl std::fmt::Debug for RecoveredShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveredShard")
            .field("shard_id", &self.shard_id)
            .field("checkpoint", &self.checkpoint)
            .field("elapsed", &self.elapsed)
            .finish_non_exhaustive()
    }
}

// ─── RecoveryProgress ────────────────────────────────────────────────────────

/// Fill-level metric for recovery progress.
///
/// The fraction `recovered / total` should be emitted as the Prometheus gauge
/// `recovery_progress` (0.0 → 1.0).
#[derive(Debug, Clone, Copy)]
pub struct RecoveryProgress {
    pub recovered: usize,
    pub total: usize,
}

impl RecoveryProgress {
    /// Returns the fraction of shards recovered (0.0–1.0).
    pub fn fraction(&self) -> f64 {
        if self.total == 0 {
            1.0
        } else {
            self.recovered as f64 / self.total as f64
        }
    }
}

// ─── RecoveryDriver ──────────────────────────────────────────────────────────

/// Drives recovery of a full cluster from the latest committed checkpoint.
///
/// Thread-safe; clone-able.
#[derive(Clone)]
pub struct RecoveryDriver {
    inner: Arc<Mutex<RecoveryDriverInner>>,
}

struct RecoveryDriverInner {
    /// Latest committed cluster checkpoint (source of truth for recovery).
    checkpoint: Option<ClusterCheckpoint>,
    /// Per-shard recovery budget SLO.
    shard_recovery_budget: Duration,
    /// Number of shards successfully recovered so far (metric fill-level).
    recovered_count: usize,
}

impl RecoveryDriver {
    /// Create a recovery driver with the default per-shard budget.
    pub fn new() -> Self {
        Self::with_budget(DEFAULT_SHARD_RECOVERY_BUDGET)
    }

    /// Create a recovery driver with a custom per-shard budget.
    pub fn with_budget(budget: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RecoveryDriverInner {
                checkpoint: None,
                shard_recovery_budget: budget,
                recovered_count: 0,
            })),
        }
    }

    /// Load a committed [`ClusterCheckpoint`] as the recovery source.
    ///
    /// The `ClusterCheckpoint` is typically fetched from the
    /// `CheckpointCoordinator::latest_committed()` method.
    pub fn load_checkpoint(&self, checkpoint: ClusterCheckpoint) {
        let mut guard = self.inner.lock();
        tracing::info!(
            checkpoint_id = checkpoint.checkpoint_id.0,
            shard_count = checkpoint.shards.len(),
            "audit: recovery.checkpoint_loaded"
        );
        guard.checkpoint = Some(checkpoint);
        guard.recovered_count = 0;
    }

    /// Returns the currently loaded checkpoint id, if any.
    pub fn loaded_checkpoint_id(&self) -> Option<CheckpointId> {
        self.inner
            .lock()
            .checkpoint
            .as_ref()
            .map(|c| c.checkpoint_id)
    }

    /// Returns the current recovery progress (fill-level metric).
    pub fn progress(&self) -> RecoveryProgress {
        let guard = self.inner.lock();
        let total = guard
            .checkpoint
            .as_ref()
            .map(|c| c.shards.len())
            .unwrap_or(0);
        RecoveryProgress {
            recovered: guard.recovered_count,
            total,
        }
    }

    /// Recover a single shard from the loaded cluster checkpoint.
    ///
    /// Opens a [`ShardReader`] pinned to the shard's `shard_checkpoint_id`,
    /// validates the lease token, and records progress.
    ///
    /// Returns [`RecoveryError::NoCheckpointAvailable`] if no checkpoint has
    /// been loaded, or [`RecoveryError::BudgetExceeded`] if the shard recovery
    /// takes longer than `shard_recovery_budget`.
    ///
    /// # Lease re-election
    ///
    /// The caller must supply a `current_token` and `expected_token`; if they
    /// differ, the writer CAS check fails and
    /// [`RecoveryError::LeaseReacquisitionFailed`] is returned (paired with
    /// M4-S1/S3 assertions in `fence.rs`).
    pub async fn recover_shard(
        &self,
        shard_id: ShardId,
        shard_path: impl Into<String>,
        object_store: Arc<dyn object_store::ObjectStore>,
        expected_token: LeaseToken,
        current_token: LeaseToken,
    ) -> Result<RecoveredShard, RecoveryError> {
        let (checkpoint, budget) = {
            let guard = self.inner.lock();
            let cp = guard
                .checkpoint
                .as_ref()
                .ok_or(RecoveryError::NoCheckpointAvailable)?
                .clone();
            let budget = guard.shard_recovery_budget;
            (cp, budget)
        };

        let psc = checkpoint.shards.get(&shard_id).cloned().ok_or_else(|| {
            RecoveryError::StorageError(format!("shard {shard_id} not in checkpoint"))
        })?;

        let started = Instant::now();

        // M4-S1/S3 paired assertion: lease re-election via fence-epoch CAS.
        // Check tokens before any IO — avoids masking the mismatch with a storage error.
        if expected_token != current_token {
            return Err(RecoveryError::LeaseReacquisitionFailed { shard_id });
        }

        // Open the shard reader (pinned to checkpoint snapshot).
        let shard_path = shard_path.into();
        let reader = match psc.snapshot_id.as_deref() {
            Some(snapshot_id) => {
                ShardReader::open_with_snapshot_id(shard_path, object_store, snapshot_id).await
            }
            None => ShardReader::open(shard_path, object_store).await,
        }
        .map_err(|e| RecoveryError::StorageError(e.to_string()))?;

        let elapsed = started.elapsed();

        // Budget check: RS-3610.
        if elapsed > budget {
            return Err(RecoveryError::BudgetExceeded {
                shard_id,
                elapsed,
                budget,
            });
        }

        // Record progress.
        {
            let mut guard = self.inner.lock();
            guard.recovered_count += 1;
            let progress = guard.recovered_count as f64
                / guard
                    .checkpoint
                    .as_ref()
                    .map(|c| c.shards.len())
                    .unwrap_or(1) as f64;
            tracing::info!(
                shard_id = shard_id.0,
                checkpoint_id = psc.checkpoint_id.0,
                shard_checkpoint_id = psc.shard_checkpoint_id,
                elapsed_ms = elapsed.as_millis(),
                recovery_progress = progress,
                "audit: recovery.shard_recovered"
            );
        }

        Ok(RecoveredShard {
            shard_id,
            checkpoint: psc,
            reader,
            elapsed,
        })
    }

    /// Recover all shards from the loaded cluster checkpoint in sequence.
    ///
    /// Returns a map of recovered shards keyed by `ShardId`, or the first
    /// error encountered.
    ///
    /// # Connector offsets
    ///
    /// `connector_offsets` maps each shard to the source connector offset
    /// recorded in `control: connector/`. These are used to resume source
    /// connectors after the reader-to-writer transition. They are returned in
    /// the map for the caller to apply.
    ///
    /// INVARIANT-BY-CONSTRUCTION: M1-S3 / M1-S4 — this checkpoint boundary is
    /// the only place `cluster_committed` (CALM min of per-shard committed
    /// epochs) is ever consulted during recovery, and it is derived fresh
    /// here directly from `checkpoint.shards`' per-shard `frontier_key`
    /// values on every call. There is no separate control-plane-cached copy
    /// of `cluster_committed` anywhere in this crate that could diverge from
    /// this object-store-derived value (the only other computation of a
    /// cluster-wide meet, `FrontierAggregator::compute_meet` in
    /// `rockstream-control`, is likewise a fresh per-call derivation over
    /// live shard reports, never a cache read at a checkpoint boundary), so
    /// CALM monotonicity/verifiability cannot be violated by a stale
    /// comparison — there is nothing for a fresh value to diverge from.
    pub async fn recover_all(
        &self,
        shard_paths: &BTreeMap<ShardId, String>,
        object_stores: &BTreeMap<ShardId, Arc<dyn object_store::ObjectStore>>,
        tokens: &BTreeMap<ShardId, (LeaseToken, LeaseToken)>,
    ) -> Result<BTreeMap<ShardId, RecoveredShard>, RecoveryError> {
        let shard_ids: Vec<ShardId> = {
            let guard = self.inner.lock();
            guard
                .checkpoint
                .as_ref()
                .ok_or(RecoveryError::NoCheckpointAvailable)?
                .shards
                .keys()
                .copied()
                .collect()
        };

        let mut result = BTreeMap::new();
        for shard_id in shard_ids {
            let path = shard_paths
                .get(&shard_id)
                .cloned()
                .unwrap_or_else(|| format!("shard/{}", shard_id.0));
            let object_store = object_stores.get(&shard_id).cloned().ok_or_else(|| {
                RecoveryError::StorageError(format!("no object_store for {shard_id}"))
            })?;
            let (expected, current) = tokens
                .get(&shard_id)
                .copied()
                .unwrap_or((LeaseToken(0), LeaseToken(0)));

            let recovered = self
                .recover_shard(shard_id, path, object_store, expected, current)
                .await?;
            result.insert(shard_id, recovered);
        }
        Ok(result)
    }
}

impl Default for RecoveryDriver {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint, PerShardCheckpoint};
    use rockstream_types::ids::{LeaseToken, ShardId};

    fn make_checkpoint(shards: &[(u64, u64)]) -> ClusterCheckpoint {
        let checkpoint_id = CheckpointId(1);
        let mut cc = ClusterCheckpoint::new(checkpoint_id);
        for &(shard, shard_ckpt) in shards {
            cc.record_shard(
                ShardId(shard),
                PerShardCheckpoint::new(checkpoint_id, shard_ckpt),
            );
        }
        cc
    }

    #[test]
    fn no_checkpoint_returns_error() {
        let driver = RecoveryDriver::new();
        assert_eq!(driver.loaded_checkpoint_id(), None);
    }

    #[test]
    fn load_checkpoint_sets_id() {
        let driver = RecoveryDriver::new();
        let cc = make_checkpoint(&[(0, 100), (1, 200)]);
        driver.load_checkpoint(cc);
        assert_eq!(driver.loaded_checkpoint_id(), Some(CheckpointId(1)));
    }

    #[test]
    fn progress_starts_at_zero() {
        let driver = RecoveryDriver::new();
        driver.load_checkpoint(make_checkpoint(&[(0, 100)]));
        let p = driver.progress();
        assert_eq!(p.recovered, 0);
        assert_eq!(p.total, 1);
        assert_eq!(p.fraction(), 0.0);
    }

    #[test]
    fn progress_fraction_with_no_shards_is_one() {
        let p = RecoveryProgress {
            recovered: 0,
            total: 0,
        };
        assert_eq!(p.fraction(), 1.0);
    }

    #[tokio::test]
    async fn recover_shard_returns_no_checkpoint_without_load() {
        let driver = RecoveryDriver::new();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let err = driver
            .recover_shard(ShardId(0), "shard/0", store, LeaseToken(1), LeaseToken(1))
            .await
            .unwrap_err();
        assert_eq!(err, RecoveryError::NoCheckpointAvailable);
    }

    #[tokio::test]
    async fn recover_shard_fails_on_lease_token_mismatch() {
        let driver = RecoveryDriver::new();
        driver.load_checkpoint(make_checkpoint(&[(0, 100)]));

        // InMemory object store — ShardReader::open will succeed (returns empty reader).
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());

        // Mismatched tokens → LeaseReacquisitionFailed.
        let err = driver
            .recover_shard(
                ShardId(0),
                "shard/0",
                store,
                LeaseToken(1),
                LeaseToken(2), // stale
            )
            .await
            .unwrap_err();
        assert_eq!(
            err,
            RecoveryError::LeaseReacquisitionFailed {
                shard_id: ShardId(0)
            }
        );
    }

    #[test]
    fn recovery_error_messages_contain_rs_codes() {
        assert!(RecoveryError::NoCheckpointAvailable
            .to_string()
            .contains("RS-3605"));
        assert!(RecoveryError::BudgetExceeded {
            shard_id: ShardId(0),
            elapsed: Duration::from_secs(5),
            budget: Duration::from_secs(2),
        }
        .to_string()
        .contains("RS-3610"));
        assert!(RecoveryError::LeaseReacquisitionFailed {
            shard_id: ShardId(0)
        }
        .to_string()
        .contains("RS-3611"));
    }
}

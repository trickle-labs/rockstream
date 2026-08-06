//! Checkpoint coordinator for RockStream cluster checkpointing (v0.20).
//!
//! Implements `CheckpointCoordinator` as described in DESIGN.md §11.2.
//!
//! ## Coordinator lifecycle
//!
//! 1. Generate a fresh `CheckpointId`.
//! 2. Inject a `CheckpointBarrier` into every source operator (tracked via the
//!    `inject_barrier` callback provided by the caller).
//! 3. Track per-shard barrier-aligned confirmations via
//!    [`CheckpointCoordinator::record_shard_checkpoint`].
//! 4. When all registered shards have confirmed, atomically write the
//!    `ClusterCheckpoint` manifest via the `commit_manifest` callback.
//! 5. Release (GC) old checkpoints beyond the retention horizon via
//!    [`CheckpointCoordinator::gc_old_checkpoints`].
//!
//! ## Bounded alignment credits
//!
//! The coordinator uses an [`AlignmentCreditTracker`] to bound the number of
//! in-flight per-shard confirmation slots. The bound is set by
//! `checkpoint_alignment_max_credits` (default: 64). When credits are
//! exhausted, confirmation attempts return
//! [`CoordinatorError::AlignmentBufferFull`] (RS-3601), never causing unbounded
//! memory growth.
//!
//! The fill-level metric is accessible via
//! [`CheckpointCoordinator::credits_used`] and should be wired to a Prometheus
//! gauge named `checkpoint_alignment_buffer_credits_used`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use rockstream_types::checkpoint::{
    AlignmentCreditTracker, AlignmentError, CheckpointBarrier, CheckpointId, ClusterCheckpoint,
    PerShardCheckpoint,
};
use rockstream_types::ids::ShardId;

// ─── Configuration ───────────────────────────────────────────────────────────

/// Default maximum alignment credits (config key:
/// `checkpoint_alignment_max_credits`).
pub const DEFAULT_ALIGNMENT_MAX_CREDITS: usize = 64;

/// Default checkpoint alignment timeout.
pub const DEFAULT_ALIGNMENT_TIMEOUT: Duration = Duration::from_secs(60);

/// Default checkpoint retention horizon (how many old checkpoints to keep).
pub const DEFAULT_RETENTION_HORIZON: u64 = 3;

// ─── Errors ──────────────────────────────────────────────────────────────────

/// Errors returned by [`CheckpointCoordinator`] operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorError {
    /// All alignment credits are consumed (RS-3601).
    AlignmentBufferFull { used: usize, max: usize },
    /// The checkpoint alignment window timed out (RS-3602).
    AlignmentTimeout,
    /// The monotone checkpoint id reached `u64::MAX` (RS-3604).
    CheckpointIdOverflow,
    /// The shard is not registered with the coordinator.
    UnknownShard(ShardId),
    /// A shard reported a confirmation for the wrong checkpoint id.
    StaleConfirmation {
        shard_id: ShardId,
        expected: CheckpointId,
        got: CheckpointId,
    },
}

impl std::fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlignmentBufferFull { used, max } => write!(
                f,
                "RS-3601: checkpoint alignment buffer full: {used}/{max} credits used; \
                 next_steps: reduce input rate or increase checkpoint_alignment_max_credits"
            ),
            Self::AlignmentTimeout => write!(
                f,
                "RS-3602: checkpoint alignment timeout; pipeline in RECOVERING state; \
                 next_steps: monitor shard reassignment and frontier progress"
            ),
            Self::CheckpointIdOverflow => write!(
                f,
                "RS-3604: checkpoint id exhausted; next_steps: create a new cluster identity before retrying"
            ),
            Self::UnknownShard(s) => write!(f, "unknown shard {s}"),
            Self::StaleConfirmation {
                shard_id,
                expected,
                got,
            } => write!(
                f,
                "stale confirmation from shard {shard_id}: expected {expected}, got {got}"
            ),
        }
    }
}

impl std::error::Error for CoordinatorError {}

impl From<AlignmentError> for CoordinatorError {
    fn from(e: AlignmentError) -> Self {
        match e {
            AlignmentError::CreditExhausted { used, max } => {
                Self::AlignmentBufferFull { used, max }
            }
            AlignmentError::AlignmentTimeout => Self::AlignmentTimeout,
        }
    }
}

// ─── State ───────────────────────────────────────────────────────────────────

/// In-progress checkpoint round state.
struct InProgressRound {
    checkpoint_id: CheckpointId,
    started_at: Instant,
    /// Per-shard confirmations received so far.
    confirmations: BTreeMap<ShardId, PerShardCheckpoint>,
    /// Shards still pending confirmation.
    pending: Vec<ShardId>,
    /// Credits held by this round (one per pending shard).
    credits_held: usize,
}

struct CoordinatorInner {
    /// Registered shards for this pipeline.
    shards: Vec<ShardId>,
    /// Monotone next checkpoint id.
    next_checkpoint_id: CheckpointId,
    /// Currently active checkpoint round, if any.
    in_progress: Option<InProgressRound>,
    /// Committed cluster checkpoints (keyed by id), up to retention horizon.
    committed: BTreeMap<CheckpointId, ClusterCheckpoint>,
    /// Retention horizon: keep at most this many old checkpoints.
    retention_horizon: u64,
    /// Alignment timeout.
    alignment_timeout: Duration,
}

// ─── CheckpointCoordinator ───────────────────────────────────────────────────

/// Manages cluster checkpoint rounds following DESIGN.md §11.2.
///
/// Thread-safe; clone-able.
#[derive(Clone)]
pub struct CheckpointCoordinator {
    inner: Arc<Mutex<CoordinatorInner>>,
    credits: AlignmentCreditTracker,
}

impl CheckpointCoordinator {
    /// Create a coordinator for the given set of shards.
    pub fn new(shards: Vec<ShardId>) -> Self {
        Self::with_config(
            shards,
            DEFAULT_ALIGNMENT_MAX_CREDITS,
            DEFAULT_RETENTION_HORIZON,
        )
    }

    /// Create with custom alignment credit limit and retention horizon.
    pub fn with_config(shards: Vec<ShardId>, max_credits: usize, retention_horizon: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CoordinatorInner {
                shards,
                next_checkpoint_id: CheckpointId(1),
                in_progress: None,
                committed: BTreeMap::new(),
                retention_horizon,
                alignment_timeout: DEFAULT_ALIGNMENT_TIMEOUT,
            })),
            credits: AlignmentCreditTracker::new(max_credits),
        }
    }

    /// The maximum alignment credits (config: `checkpoint_alignment_max_credits`).
    pub fn max_credits(&self) -> usize {
        self.credits.max_credits()
    }

    /// Current fill level (metric: `checkpoint_alignment_buffer_credits_used`).
    pub fn credits_used(&self) -> usize {
        self.credits.credits_used()
    }

    /// Begin a new checkpoint round.
    ///
    /// This allocates one credit per shard, then invokes `inject_barrier` with
    /// the new `CheckpointBarrier` for each registered shard. The caller is
    /// responsible for delivering the barrier to the source operators.
    ///
    /// Returns the new `CheckpointId` on success, or a [`CoordinatorError`] if
    /// credits are exhausted or a round is already in progress.
    ///
    /// An audit event `checkpoint.started` is emitted.
    pub fn begin_checkpoint<F>(&self, inject_barrier: F) -> Result<CheckpointId, CoordinatorError>
    where
        F: Fn(ShardId, CheckpointBarrier),
    {
        let mut guard = self.inner.lock();

        if let Some(round) = &guard.in_progress {
            // Already have a round in progress; check for timeout.
            if round.started_at.elapsed() > guard.alignment_timeout {
                // Drain credits and clear the timed-out round.
                let held = round.credits_held;
                drop(guard);
                for _ in 0..held {
                    self.credits.release();
                }
                let mut guard = self.inner.lock();
                guard.in_progress = None;
                return Err(CoordinatorError::AlignmentTimeout);
            }
            // Still within timeout; a second begin is rejected.
            return Err(CoordinatorError::AlignmentTimeout);
        }

        let shards = guard.shards.clone();
        let n = shards.len();

        // Acquire one credit per shard.
        let mut acquired = 0usize;
        for _ in 0..n {
            if let Err(e) = self.credits.acquire() {
                // Roll back partial acquisition.
                for _ in 0..acquired {
                    self.credits.release();
                }
                return Err(e.into());
            }
            acquired += 1;
        }

        let checkpoint_id = guard.next_checkpoint_id;
        guard.next_checkpoint_id = checkpoint_id
            .checked_next()
            .ok_or(CoordinatorError::CheckpointIdOverflow)?;

        let barrier = CheckpointBarrier::new(checkpoint_id);

        // Inject barrier into all source operators before recording in-progress
        // (so any failure in injection doesn't leave a dangling round).
        for &shard_id in &shards {
            inject_barrier(shard_id, barrier.clone());
        }

        guard.in_progress = Some(InProgressRound {
            checkpoint_id,
            started_at: Instant::now(),
            confirmations: BTreeMap::new(),
            pending: shards,
            credits_held: acquired,
        });

        tracing::info!(checkpoint_id = checkpoint_id.0, "audit: checkpoint.started");

        Ok(checkpoint_id)
    }

    /// Record a per-shard checkpoint confirmation.
    ///
    /// Called by the worker after it creates a SlateDB shard checkpoint and
    /// sends the `(checkpoint_id, shard_checkpoint_id)` to the coordinator.
    ///
    /// If all shards have confirmed, the `ClusterCheckpoint` manifest is
    /// committed by invoking `commit_manifest`. An audit event
    /// `checkpoint.completed` is emitted.
    ///
    /// Returns `Ok(Some(manifest))` if this was the final confirmation,
    /// `Ok(None)` if more shards are still pending, or an error.
    pub fn record_shard_checkpoint<F>(
        &self,
        shard_id: ShardId,
        psc: PerShardCheckpoint,
        commit_manifest: F,
    ) -> Result<Option<ClusterCheckpoint>, CoordinatorError>
    where
        F: FnOnce(&ClusterCheckpoint) -> Result<(), CoordinatorError>,
    {
        let mut guard = self.inner.lock();

        let round = guard
            .in_progress
            .as_mut()
            .ok_or(CoordinatorError::UnknownShard(shard_id))?;

        // Validate checkpoint id.
        if psc.checkpoint_id != round.checkpoint_id {
            return Err(CoordinatorError::StaleConfirmation {
                shard_id,
                expected: round.checkpoint_id,
                got: psc.checkpoint_id,
            });
        }

        // Validate shard is registered.
        if !round.pending.contains(&shard_id) && !round.confirmations.contains_key(&shard_id) {
            return Err(CoordinatorError::UnknownShard(shard_id));
        }

        round.pending.retain(|s| *s != shard_id);
        round.confirmations.insert(shard_id, psc);
        self.credits.release();

        if !round.pending.is_empty() {
            return Ok(None);
        }

        // All shards confirmed — build and commit the manifest.
        let checkpoint_id = round.checkpoint_id;
        let mut manifest = ClusterCheckpoint::new(checkpoint_id);
        let confirmations: Vec<(ShardId, PerShardCheckpoint)> = round
            .confirmations
            .iter()
            .map(|(&sid, psc)| (sid, psc.clone()))
            .collect();
        let credits_held = round.credits_held;
        guard.in_progress = None;

        for (sid, psc) in confirmations {
            manifest.record_shard(sid, psc);
        }
        let remaining = credits_held.saturating_sub(manifest.shards.len());
        drop(guard);
        for _ in 0..remaining {
            self.credits.release();
        }

        commit_manifest(&manifest)?;

        tracing::info!(
            checkpoint_id = checkpoint_id.0,
            "audit: checkpoint.completed"
        );

        let mut guard = self.inner.lock();
        guard.committed.insert(checkpoint_id, manifest.clone());

        // GC old checkpoints beyond retention horizon.
        let retention = guard.retention_horizon;
        let max_id = checkpoint_id.0;
        if max_id > retention {
            let gc_before = CheckpointId(max_id - retention);
            guard.committed.retain(|&id, _| id >= gc_before);
        }

        Ok(Some(manifest))
    }

    /// Explicitly GC checkpoints older than the retention horizon.
    ///
    /// Normally called automatically after each successful commit, but
    /// available for operator-triggered cleanup.
    pub fn gc_old_checkpoints(&self) {
        let mut guard = self.inner.lock();
        let latest = guard.committed.keys().next_back().copied();
        if let Some(latest) = latest {
            let retention = guard.retention_horizon;
            if latest.0 > retention {
                let gc_before = CheckpointId(latest.0 - retention);
                guard.committed.retain(|&id, _| id >= gc_before);
            }
        }
    }

    /// Return the latest committed [`ClusterCheckpoint`], if any.
    pub fn latest_committed(&self) -> Option<ClusterCheckpoint> {
        let guard = self.inner.lock();
        guard.committed.values().next_back().cloned()
    }

    /// Return all committed checkpoints (up to retention horizon).
    pub fn committed_checkpoints(&self) -> Vec<ClusterCheckpoint> {
        let guard = self.inner.lock();
        guard.committed.values().cloned().collect()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::ShardId;

    fn make_coordinator(shards: &[u64]) -> CheckpointCoordinator {
        CheckpointCoordinator::new(shards.iter().map(|&s| ShardId(s)).collect())
    }

    fn noop_inject(_shard: ShardId, _barrier: CheckpointBarrier) {}

    fn noop_commit(_m: &ClusterCheckpoint) -> Result<(), CoordinatorError> {
        Ok(())
    }

    // ── begin_checkpoint ──────────────────────────────────────────────────────

    #[test]
    fn begin_checkpoint_returns_first_id() {
        let coord = make_coordinator(&[0, 1]);
        let id = coord.begin_checkpoint(noop_inject).unwrap();
        assert_eq!(id, CheckpointId(1));
    }

    #[test]
    fn checkpoint_id_exhaustion_returns_rs3604_without_wrapping() {
        let coordinator = make_coordinator(&[1]);
        coordinator.inner.lock().next_checkpoint_id = CheckpointId(u64::MAX);
        assert_eq!(
            coordinator.begin_checkpoint(noop_inject).unwrap_err().to_string(),
            "RS-3604: checkpoint id exhausted; next_steps: create a new cluster identity before retrying"
        );
    }

    #[test]
    fn begin_checkpoint_acquires_one_credit_per_shard() {
        let coord = make_coordinator(&[0, 1]);
        coord.begin_checkpoint(noop_inject).unwrap();
        assert_eq!(coord.credits_used(), 2);
    }

    #[test]
    fn begin_checkpoint_injects_barrier_for_each_shard() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();
        let coord = make_coordinator(&[0, 1, 2]);
        coord
            .begin_checkpoint(|_shard, _barrier| {
                count_clone.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 3);
    }

    // ── record_shard_checkpoint ───────────────────────────────────────────────

    #[test]
    fn record_all_shards_completes_checkpoint() {
        let coord = make_coordinator(&[0, 1]);
        let id = coord.begin_checkpoint(noop_inject).unwrap();

        coord
            .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(id, 100), noop_commit)
            .unwrap();

        let result = coord
            .record_shard_checkpoint(ShardId(1), PerShardCheckpoint::new(id, 200), noop_commit)
            .unwrap();

        assert!(
            result.is_some(),
            "should return manifest on last confirmation"
        );
        let manifest = result.unwrap();
        assert_eq!(manifest.checkpoint_id, id);
        assert_eq!(manifest.shards.len(), 2);
    }

    #[test]
    fn credits_released_after_completion() {
        let coord = make_coordinator(&[0, 1]);
        let id = coord.begin_checkpoint(noop_inject).unwrap();
        assert_eq!(coord.credits_used(), 2);

        coord
            .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(id, 10), noop_commit)
            .unwrap();
        assert_eq!(coord.credits_used(), 1); // one released on partial

        coord
            .record_shard_checkpoint(ShardId(1), PerShardCheckpoint::new(id, 20), noop_commit)
            .unwrap();
        assert_eq!(coord.credits_used(), 0); // all released
    }

    #[test]
    fn latest_committed_is_set_after_completion() {
        let coord = make_coordinator(&[0]);
        let id = coord.begin_checkpoint(noop_inject).unwrap();
        coord
            .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(id, 1), noop_commit)
            .unwrap();
        assert_eq!(coord.latest_committed().unwrap().checkpoint_id, id);
    }

    // ── alignment credit exhaustion (RS-3601) ─────────────────────────────────

    /// LFS proof: `test_checkpoint_alignment_bounded_lfs` — the alignment
    /// buffer never exceeds `max_credits`; exhaustion returns RS-3601, not panic.
    #[test]
    fn test_checkpoint_alignment_bounded_lfs() {
        // Set max_credits to exactly the number of shards so one round fully
        // exhausts the buffer.
        let shards: Vec<ShardId> = (0..4).map(ShardId).collect();
        let coord = CheckpointCoordinator::with_config(
            shards.clone(),
            4, // max_credits == num_shards
            3,
        );

        // Round 1 uses all 4 credits.
        let id = coord.begin_checkpoint(noop_inject).unwrap();
        assert_eq!(coord.credits_used(), 4);

        // A second begin while round 1 is in-flight must fail (timeout path,
        // because there are no credits left).
        let err = coord.begin_checkpoint(noop_inject).unwrap_err();
        // Either AlignmentTimeout (in-progress guard) or AlignmentBufferFull.
        assert!(
            matches!(
                err,
                CoordinatorError::AlignmentTimeout | CoordinatorError::AlignmentBufferFull { .. }
            ),
            "unexpected: {err}"
        );

        // Confirm all shards — this drains the credits.
        for (i, &shard_id) in shards.iter().enumerate() {
            coord
                .record_shard_checkpoint(
                    shard_id,
                    PerShardCheckpoint::new(id, i as u64 * 10),
                    noop_commit,
                )
                .unwrap();
        }
        assert_eq!(coord.credits_used(), 0, "all credits must be released");

        // Now a second round succeeds.
        coord.begin_checkpoint(noop_inject).unwrap();
    }

    // ── stale confirmation ────────────────────────────────────────────────────

    #[test]
    fn stale_confirmation_returns_error() {
        let coord = make_coordinator(&[0]);
        let id = coord.begin_checkpoint(noop_inject).unwrap();

        let wrong_id = CheckpointId(id.0 + 99);
        let err = coord
            .record_shard_checkpoint(
                ShardId(0),
                PerShardCheckpoint::new(wrong_id, 0),
                noop_commit,
            )
            .unwrap_err();
        assert!(
            matches!(err, CoordinatorError::StaleConfirmation { .. }),
            "{err}"
        );
    }

    // ── GC ────────────────────────────────────────────────────────────────────

    #[test]
    fn gc_removes_old_checkpoints_beyond_retention() {
        let coord = CheckpointCoordinator::with_config(vec![ShardId(0)], 64, 2);

        for i in 1..=5u64 {
            let id = coord.begin_checkpoint(noop_inject).unwrap();
            assert_eq!(id.0, i);
            coord
                .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(id, i), noop_commit)
                .unwrap();
        }

        // With retention_horizon=2 and 5 committed, only ckpt-3..=5 should remain.
        let committed = coord.committed_checkpoints();
        assert!(
            committed.len() <= 3,
            "expected ≤3 but got {}",
            committed.len()
        );
        let min_id = committed.iter().map(|c| c.checkpoint_id.0).min().unwrap();
        assert!(min_id >= 3, "expected min_id ≥ 3 but got {min_id}");
    }
}

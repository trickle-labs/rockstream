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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use rockstream_types::checkpoint::{
    AlignmentCreditTracker, AlignmentError, CheckpointBarrier, CheckpointId, ClusterCheckpoint,
    PerShardCheckpoint,
};
use rockstream_types::ids::ShardId;
use rockstream_types::state_mutation::EpochStateDelta;

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
    /// A changelog contribution does not belong to this checkpoint round.
    StaleChangelogContribution {
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
            Self::StaleChangelogContribution {
                shard_id,
                expected,
                got,
            } => write!(
                f,
                "stale changelog contribution from shard {shard_id}: expected {expected}, got {got}"
            ),
        }
    }
}

/// The delta-native state contribution durably staged by one shard for an
/// aligned checkpoint. The coordinator accepts it only for its matching round.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChangelogCheckpointContribution {
    pub checkpoint_id: CheckpointId,
    pub delta: EpochStateDelta,
}

impl ChangelogCheckpointContribution {
    pub fn new(checkpoint_id: CheckpointId, delta: EpochStateDelta) -> Self {
        Self {
            checkpoint_id,
            delta,
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
    /// Per-shard barrier arrival timestamps.
    barrier_arrivals: BTreeMap<ShardId, u64>,
    /// Per-shard confirmations received so far.
    confirmations: BTreeMap<ShardId, PerShardCheckpoint>,
    /// Durable delta-native contributions received so far.
    changelog_contributions: BTreeMap<ShardId, ChangelogCheckpointContribution>,
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
    /// This allocates one reserved barrier credit per shard, then invokes
    /// `inject_barrier` with the new `CheckpointBarrier` for each registered
    /// shard. These credits are owned by the coordinator, so saturated data
    /// exchange credits cannot block aligned-barrier injection.
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
        let checkpoint_id = guard.next_checkpoint_id;
        let next_checkpoint_id = checkpoint_id
            .checked_next()
            .ok_or(CoordinatorError::CheckpointIdOverflow)?;

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

        guard.next_checkpoint_id = next_checkpoint_id;

        let barrier = CheckpointBarrier::new(checkpoint_id);

        let injected_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        rockstream_types::metrics::set_barrier_injected_at(checkpoint_id.0, injected_at_ms);

        // Inject barrier into all source operators before recording in-progress
        // (so any failure in injection doesn't leave a dangling round).
        for &shard_id in &shards {
            inject_barrier(shard_id, barrier.clone());
        }

        guard.in_progress = Some(InProgressRound {
            checkpoint_id,
            started_at: Instant::now(),
            barrier_arrivals: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            changelog_contributions: BTreeMap::new(),
            pending: shards,
            credits_held: acquired,
        });

        tracing::info!(checkpoint_id = checkpoint_id.0, "audit: checkpoint.started");

        Ok(checkpoint_id)
    }

    /// Record one shard's already-durable changelog contribution and aligned
    /// SlateDB checkpoint. The manifest callback runs only after every shard
    /// supplied both records.
    pub fn record_shard_changelog_checkpoint<F>(
        &self,
        shard_id: ShardId,
        psc: PerShardCheckpoint,
        contribution: ChangelogCheckpointContribution,
        commit_manifest: F,
    ) -> Result<Option<ClusterCheckpoint>, CoordinatorError>
    where
        F: FnOnce(
            &ClusterCheckpoint,
            &BTreeMap<ShardId, ChangelogCheckpointContribution>,
        ) -> Result<(), CoordinatorError>,
    {
        let contributions = {
            let mut guard = self.inner.lock();
            let round = guard
                .in_progress
                .as_mut()
                .ok_or(CoordinatorError::UnknownShard(shard_id))?;
            if !round.pending.contains(&shard_id) {
                return Err(CoordinatorError::UnknownShard(shard_id));
            }
            if psc.checkpoint_id != round.checkpoint_id {
                return Err(CoordinatorError::StaleConfirmation {
                    shard_id,
                    expected: round.checkpoint_id,
                    got: psc.checkpoint_id,
                });
            }
            if contribution.checkpoint_id != round.checkpoint_id {
                return Err(CoordinatorError::StaleChangelogContribution {
                    shard_id,
                    expected: round.checkpoint_id,
                    got: contribution.checkpoint_id,
                });
            }
            round.changelog_contributions.insert(shard_id, contribution);
            round.changelog_contributions.clone()
        };

        self.record_shard_checkpoint(shard_id, psc, |manifest| {
            assert_eq!(
                manifest.shards.len(),
                contributions.len(),
                "M1-S1: a changelog checkpoint manifest requires one durable contribution per shard"
            );
            commit_manifest(manifest, &contributions)
        })
    }

    /// Record when a shard receives a checkpoint barrier across exchange channels.
    ///
    /// Updates flight time tracking and records metrics into `rockstream-types::metrics`.
    pub fn record_shard_barrier_received(
        &self,
        shard_id: ShardId,
        checkpoint_id: CheckpointId,
        arrival_time_ms: u64,
    ) -> Result<(), CoordinatorError> {
        let mut guard = self.inner.lock();
        let round = guard
            .in_progress
            .as_mut()
            .ok_or(CoordinatorError::UnknownShard(shard_id))?;

        if checkpoint_id != round.checkpoint_id {
            return Err(CoordinatorError::StaleConfirmation {
                shard_id,
                expected: round.checkpoint_id,
                got: checkpoint_id,
            });
        }
        if !round.pending.contains(&shard_id) && !round.confirmations.contains_key(&shard_id) {
            return Err(CoordinatorError::UnknownShard(shard_id));
        }

        round.barrier_arrivals.insert(shard_id, arrival_time_ms);
        rockstream_types::metrics::record_shard_barrier_arrival(
            checkpoint_id.0,
            shard_id.0,
            arrival_time_ms,
        );
        Ok(())
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

        let completed_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        rockstream_types::metrics::record_checkpoint_completed(checkpoint_id.0, completed_at_ms);

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

    /// Return a live alignment snapshot for a checkpoint round.
    pub fn alignment_snapshot(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Option<CheckpointAlignmentSnapshot> {
        let guard = self.inner.lock();
        if let Some(round) = &guard.in_progress {
            if round.checkpoint_id == checkpoint_id {
                let elapsed_ms = round.started_at.elapsed().as_millis() as u64;
                let mut shards = Vec::new();
                let mut active_holder = None;
                for &shard in &guard.shards {
                    let is_confirmed = round.confirmations.contains_key(&shard);
                    let has_barrier = round.barrier_arrivals.contains_key(&shard);
                    let (state, holder) = if is_confirmed {
                        ("confirmed".to_string(), None)
                    } else if has_barrier {
                        let h = format!("shard_{}/source_0", shard.0);
                        if active_holder.is_none() {
                            active_holder = Some(h.clone());
                        }
                        ("barrier_received".to_string(), Some(h))
                    } else {
                        let h = format!("shard_{}/source_0", shard.0);
                        if active_holder.is_none() {
                            active_holder = Some(h.clone());
                        }
                        ("holding_barrier".to_string(), Some(h))
                    };
                    shards.push(ShardAlignmentSnapshot {
                        shard_id: shard,
                        operator_id: "source_0".to_string(),
                        state,
                        holder,
                        elapsed_ms,
                    });
                }
                return Some(CheckpointAlignmentSnapshot {
                    checkpoint_id,
                    status: "in_progress".to_string(),
                    shards,
                    active_holder,
                    elapsed_ms,
                });
            }
        }
        if let Some(committed) = guard.committed.get(&checkpoint_id) {
            let mut shards = Vec::new();
            for &shard in committed.shards.keys() {
                shards.push(ShardAlignmentSnapshot {
                    shard_id: shard,
                    operator_id: "source_0".to_string(),
                    state: "confirmed".to_string(),
                    holder: None,
                    elapsed_ms: 0,
                });
            }
            return Some(CheckpointAlignmentSnapshot {
                checkpoint_id,
                status: "committed".to_string(),
                shards,
                active_holder: None,
                elapsed_ms: 0,
            });
        }
        None
    }
}

/// Detailed alignment info for one shard during a checkpoint round.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardAlignmentSnapshot {
    pub shard_id: ShardId,
    pub operator_id: String,
    pub state: String,
    pub holder: Option<String>,
    pub elapsed_ms: u64,
}

/// Snapshot of checkpoint alignment state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CheckpointAlignmentSnapshot {
    pub checkpoint_id: CheckpointId,
    pub status: String,
    pub shards: Vec<ShardAlignmentSnapshot>,
    pub active_holder: Option<String>,
    pub elapsed_ms: u64,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rockstream_types::ids::ShardId;

    fn make_coordinator(shards: &[u64]) -> CheckpointCoordinator {
        CheckpointCoordinator::new(shards.iter().map(|&s| ShardId(s)).collect())
    }

    fn noop_inject(_shard: ShardId, _barrier: CheckpointBarrier) {}

    fn noop_commit(_m: &ClusterCheckpoint) -> Result<(), CoordinatorError> {
        Ok(())
    }

    fn changelog(id: CheckpointId, key: u8, value: u8) -> ChangelogCheckpointContribution {
        ChangelogCheckpointContribution::new(
            id,
            EpochStateDelta::from_mutations(vec![
                rockstream_types::state_mutation::StateMutation::Put {
                    key: vec![key],
                    value: bytes::Bytes::from(vec![value]),
                },
            ]),
        )
    }

    // ── begin_checkpoint ──────────────────────────────────────────────────────

    #[test]
    fn begin_checkpoint_returns_first_id() {
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
        let coord = make_coordinator(&[0, 1]);
        let id = coord.begin_checkpoint(noop_inject).unwrap();
        assert_eq!(id, CheckpointId(1));
    }

    #[test]
    fn checkpoint_id_exhaustion_returns_rs3604_without_wrapping() {
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
        let coordinator = make_coordinator(&[1]);
        coordinator.inner.lock().next_checkpoint_id = CheckpointId(u64::MAX);
        assert_eq!(
            coordinator.begin_checkpoint(noop_inject).unwrap_err().to_string(),
            "RS-3604: checkpoint id exhausted; next_steps: create a new cluster identity before retrying"
        );
        assert_eq!(coordinator.credits_used(), 0);
        assert_eq!(
            coordinator.inner.lock().next_checkpoint_id,
            CheckpointId(u64::MAX)
        );
    }

    proptest! {
        #[test]
        fn checkpoint_boundary_proptest(
            delta in 0u8..=1,
            shard_count in 1usize..=3,
        ) {
            let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
            let coordinator = make_coordinator(&(0..shard_count as u64).collect::<Vec<_>>());
            let initial_id = CheckpointId(u64::MAX - u64::from(delta));
            coordinator.inner.lock().next_checkpoint_id = initial_id;

            match coordinator.begin_checkpoint(noop_inject) {
                Ok(checkpoint_id) => {
                    prop_assert_eq!(delta, 1);
                    prop_assert_eq!(checkpoint_id, initial_id);
                    prop_assert_eq!(coordinator.credits_used(), shard_count);
                }
                Err(error) => {
                    prop_assert_eq!(delta, 0);
                    prop_assert_eq!(error.to_string(), "RS-3604: checkpoint id exhausted; next_steps: create a new cluster identity before retrying");
                    prop_assert_eq!(coordinator.credits_used(), 0);
                    prop_assert_eq!(coordinator.inner.lock().next_checkpoint_id, initial_id);
                }
            }
        }
    }

    #[test]
    fn begin_checkpoint_acquires_one_credit_per_shard() {
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
        let coord = make_coordinator(&[0, 1]);
        coord.begin_checkpoint(noop_inject).unwrap();
        assert_eq!(coord.credits_used(), 2);
    }

    #[test]
    fn begin_checkpoint_injects_barrier_for_each_shard() {
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
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

    #[test]
    fn reserved_barrier_lane_works_with_exhausted_data_credits() {
        let data_credits = rockstream_runtime::ExchangeCredits::new(8, 1);
        let _data = data_credits.try_acquire(8, 1).unwrap();
        let injected = Arc::new(Mutex::new(Vec::new()));
        let target = injected.clone();
        let coord = CheckpointCoordinator::with_config(vec![ShardId(0), ShardId(1)], 2, 3);

        let checkpoint_id = coord
            .begin_checkpoint(|shard_id, barrier| target.lock().push((shard_id, barrier)))
            .unwrap();

        assert_eq!(data_credits.available_bytes(), 0);
        assert_eq!(
            *injected.lock(),
            vec![
                (ShardId(0), CheckpointBarrier::new(checkpoint_id)),
                (ShardId(1), CheckpointBarrier::new(checkpoint_id)),
            ]
        );
    }

    // ── record_shard_checkpoint ───────────────────────────────────────────────

    #[test]
    fn record_all_shards_completes_checkpoint() {
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
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
    fn changelog_manifest_waits_for_every_durable_shard_contribution() {
        let coord = make_coordinator(&[0, 1]);
        let id = coord.begin_checkpoint(noop_inject).unwrap();
        let first = changelog(id, 1, 10);
        let second = changelog(id, 2, 20);

        assert_eq!(
            coord
                .record_shard_changelog_checkpoint(
                    ShardId(0),
                    PerShardCheckpoint::new(id, 10),
                    first.clone(),
                    |_, _| panic!("manifest must wait for every shard"),
                )
                .unwrap(),
            None
        );

        let manifest = coord
            .record_shard_changelog_checkpoint(
                ShardId(1),
                PerShardCheckpoint::new(id, 20),
                second.clone(),
                |manifest, contributions| {
                    assert_eq!(
                        manifest.shards,
                        BTreeMap::from([
                            (ShardId(0), PerShardCheckpoint::new(id, 10)),
                            (ShardId(1), PerShardCheckpoint::new(id, 20)),
                        ])
                    );
                    assert_eq!(
                        contributions,
                        &BTreeMap::from([(ShardId(0), first), (ShardId(1), second)])
                    );
                    Ok(())
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(manifest.checkpoint_id, id);
    }

    #[test]
    fn credits_released_after_completion() {
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
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
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
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
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
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
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
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
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
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

    #[test]
    fn test_barrier_flight_time_tracking_in_coordinator() {
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
        rockstream_types::metrics::reset_all();
        let coord = CheckpointCoordinator::new(vec![ShardId(0), ShardId(1)]);

        let id = coord.begin_checkpoint(noop_inject).unwrap();
        assert_eq!(id.0, 1);

        let inj = rockstream_types::metrics::read_barrier_flight_stats().barrier_injected_at_ms;

        // Record shard barrier arrivals
        coord
            .record_shard_barrier_received(ShardId(0), id, inj + 15)
            .unwrap();
        coord
            .record_shard_barrier_received(ShardId(1), id, inj + 35)
            .unwrap();

        // Checkpoint confirmations
        coord
            .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(id, 100), noop_commit)
            .unwrap();
        let manifest = coord
            .record_shard_checkpoint(ShardId(1), PerShardCheckpoint::new(id, 101), noop_commit)
            .unwrap();
        assert!(manifest.is_some());

        let stats = rockstream_types::metrics::read_barrier_flight_stats();
        assert_eq!(stats.last_checkpoint_id, 1);
        assert_eq!(stats.barrier_flight_time_ms, 35);
        assert!(stats.checkpoint_completion_time_ms >= stats.barrier_flight_time_ms);
    }

    #[test]
    fn test_checkpoint_show_names_barrier_holder() {
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
        let coord = CheckpointCoordinator::new(vec![ShardId(0), ShardId(1)]);
        let id = coord.begin_checkpoint(noop_inject).unwrap();

        // Shard 0 confirms; shard 1 has not confirmed and holds the barrier
        coord
            .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(id, 100), noop_commit)
            .unwrap();

        let snapshot = coord.alignment_snapshot(id).expect("snapshot present");
        assert_eq!(snapshot.status, "in_progress");
        assert_eq!(snapshot.active_holder.as_deref(), Some("shard_1/source_0"));
        let s0 = snapshot
            .shards
            .iter()
            .find(|s| s.shard_id == ShardId(0))
            .unwrap();
        assert_eq!(s0.state, "confirmed");
        assert_eq!(s0.holder, None);

        let s1 = snapshot
            .shards
            .iter()
            .find(|s| s.shard_id == ShardId(1))
            .unwrap();
        assert_eq!(s1.state, "holding_barrier");
        assert_eq!(s1.holder.as_deref(), Some("shard_1/source_0"));
    }

    #[test]
    fn test_checkpoint_show_clears_completed_holder() {
        let _lock = rockstream_types::metrics::METRICS_TEST_LOCK.lock().unwrap();
        let coord = CheckpointCoordinator::new(vec![ShardId(0), ShardId(1)]);
        let id = coord.begin_checkpoint(noop_inject).unwrap();

        coord
            .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(id, 100), noop_commit)
            .unwrap();
        let final_manifest = coord
            .record_shard_checkpoint(ShardId(1), PerShardCheckpoint::new(id, 101), noop_commit)
            .unwrap();
        assert!(final_manifest.is_some());

        let snapshot = coord.alignment_snapshot(id).expect("snapshot present");
        assert_eq!(snapshot.status, "committed");
        assert_eq!(snapshot.active_holder, None);
        for shard in &snapshot.shards {
            assert_eq!(shard.state, "confirmed");
            assert_eq!(shard.holder, None);
        }
    }
}

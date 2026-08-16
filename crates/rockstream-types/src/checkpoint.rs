//! Checkpoint types for RockStream cluster checkpointing (v0.20).
//!
//! These types model the three-level checkpoint hierarchy described in
//! DESIGN.md §11.2:
//!
//! 1. **`CheckpointId`** — monotone cluster-wide checkpoint sequence number.
//! 2. **`PerShardCheckpoint`** — per-shard checkpoint record: maps a
//!    `CheckpointId` to a SlateDB-level `shard_checkpoint_id`.
//! 3. **`ClusterCheckpoint`** — atomically committed manifest of all per-shard
//!    checkpoints for one `CheckpointId`.
//! 4. **`CheckpointBarrier`** — injected by the coordinator into every source
//!    operator to trigger aligned snapshotting.
//!
//! ## Alignment credit accounting
//!
//! The [`AlignmentCreditTracker`] bounds the number of in-flight barrier-wait
//! slots. When all `checkpoint_alignment_max_credits` are consumed and a shard
//! has not yet reported its checkpoint, the caller receives a backpressure
//! signal and must back off.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::ids::ShardId;

// ─── CheckpointId ────────────────────────────────────────────────────────────

/// Monotonically increasing cluster-wide checkpoint sequence number.
///
/// `CheckpointId(0)` is reserved and indicates "no committed checkpoint".
/// All user-visible checkpoints start at `CheckpointId(1)`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct CheckpointId(pub u64);

impl CheckpointId {
    /// The sentinel value representing "no checkpoint yet committed".
    pub const NONE: Self = Self(0);

    /// Returns the next checkpoint id in the sequence, or `None` at its
    /// representable boundary. Callers must reject rather than wrap.
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    /// Returns `true` if this is the sentinel "none" value.
    pub fn is_none(self) -> bool {
        self.0 == 0
    }
}

impl std::fmt::Display for CheckpointId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ckpt-{}", self.0)
    }
}

// ─── PerShardCheckpoint ──────────────────────────────────────────────────────

/// A per-shard record pairing the cluster checkpoint with the underlying
/// SlateDB checkpoint handle.
///
/// When a shard's barrier epoch completes, the worker calls
/// `ShardDb::create_checkpoint()` to produce a SlateDB-level checkpoint and
/// reports this struct to the coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerShardCheckpoint {
    /// The cluster-wide checkpoint this shard record belongs to.
    pub checkpoint_id: CheckpointId,
    /// The underlying SlateDB checkpoint ID for this shard at this barrier.
    pub shard_checkpoint_id: u64,
    /// The SlateDB checkpoint UUID, when available. This is the durable
    /// checkpoint handle needed to reopen the exact manifest rather than the
    /// mutable latest reader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
}

impl PerShardCheckpoint {
    /// Create a new per-shard checkpoint record.
    pub fn new(checkpoint_id: CheckpointId, shard_checkpoint_id: u64) -> Self {
        Self {
            checkpoint_id,
            shard_checkpoint_id,
            snapshot_id: None,
        }
    }

    pub fn with_snapshot_id(mut self, snapshot_id: impl Into<String>) -> Self {
        self.snapshot_id = Some(snapshot_id.into());
        self
    }
}

// ─── ClusterCheckpoint ───────────────────────────────────────────────────────

/// Atomically committed manifest of all per-shard checkpoints for one
/// cluster checkpoint round.
///
/// Written to `control: checkpoints/{checkpoint_id}` by the coordinator only
/// after every shard in the pipeline has confirmed its `PerShardCheckpoint`.
/// This is the single durable record that the recovery driver reads to
/// reconstruct pipeline state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterCheckpoint {
    /// The cluster-wide checkpoint sequence number.
    pub checkpoint_id: CheckpointId,
    /// Per-shard checkpoint records, keyed by `ShardId`.
    ///
    /// All shards in the pipeline at the time of the checkpoint are present.
    pub shards: BTreeMap<ShardId, PerShardCheckpoint>,
}

impl ClusterCheckpoint {
    /// Create an empty cluster checkpoint for `checkpoint_id`.
    pub fn new(checkpoint_id: CheckpointId) -> Self {
        Self {
            checkpoint_id,
            shards: BTreeMap::new(),
        }
    }

    /// Record a per-shard confirmation.
    pub fn record_shard(&mut self, shard_id: ShardId, psc: PerShardCheckpoint) {
        self.shards.insert(shard_id, psc);
    }

    /// Returns `true` when all `expected_shards` have been confirmed.
    pub fn is_complete(&self, expected_shards: &[ShardId]) -> bool {
        expected_shards.iter().all(|s| self.shards.contains_key(s))
    }
}

// ─── CheckpointBarrier ───────────────────────────────────────────────────────

/// A barrier message injected into every source operator by the coordinator.
///
/// When a source operator observes a `CheckpointBarrier`, it drains its
/// in-flight batches, then propagates the barrier downstream. When every
/// local operator has forwarded the barrier, the worker calls
/// `ShardDb::create_checkpoint()` and reports a `PerShardCheckpoint` to the
/// coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointBarrier {
    /// The checkpoint this barrier belongs to.
    pub checkpoint_id: CheckpointId,
}

impl CheckpointBarrier {
    /// Create a new checkpoint barrier.
    pub fn new(checkpoint_id: CheckpointId) -> Self {
        Self { checkpoint_id }
    }
}

// ─── AlignmentCreditTracker ──────────────────────────────────────────────────

/// Bounded tracker for checkpoint barrier-alignment credits.
///
/// The coordinator grants one "credit" per pending shard barrier confirmation.
/// When `checkpoint_alignment_max_credits` are exhausted, new credits are
/// denied and backpressure is applied to the upstream source operators.
///
/// Exhausting credits returns [`AlignmentError::CreditExhausted`] (RS-3601),
/// never unbounded memory growth.
///
/// ## Named upper bound
///
/// The bound is set via the `checkpoint_alignment_max_credits` config key
/// (DESIGN.md §11.2). The fill level is exposed via the
/// `checkpoint_alignment_buffer_credits_used` metric.
#[derive(Clone)]
pub struct AlignmentCreditTracker {
    /// Maximum credits (== max in-flight pending-confirmation slots).
    max_credits: usize,
    /// Currently consumed credits.
    credits_used: Arc<AtomicUsize>,
}

/// Error returned when alignment credit operations fail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlignmentError {
    /// All `max_credits` are consumed; backpressure must be applied.
    ///
    /// Operator action: stop granting upstream credits until confirmations
    /// drain the buffer.
    ///
    /// RS-3601: "Checkpoint alignment buffer overflowed; bounded buffer
    /// capacity exceeded."
    CreditExhausted { used: usize, max: usize },
    /// The alignment window timed out before all shards confirmed.
    ///
    /// RS-3602: "Cluster checkpoint recovery in progress."
    AlignmentTimeout,
}

impl std::fmt::Display for AlignmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreditExhausted { used, max } => write!(
                f,
                "RS-3601: checkpoint alignment buffer overflowed: {used}/{max} credits used; \
                 next_steps: reduce input rate or increase checkpoint_alignment_max_credits"
            ),
            Self::AlignmentTimeout => write!(
                f,
                "RS-3602: checkpoint alignment timeout; pipeline in RECOVERING state; \
                 next_steps: monitor shard reassignment and frontier progress via SHOW VIEW STATUS"
            ),
        }
    }
}

impl AlignmentCreditTracker {
    /// Create a tracker with `max_credits` capacity.
    pub fn new(max_credits: usize) -> Self {
        Self {
            max_credits,
            credits_used: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The maximum allowed credits (named upper bound).
    pub fn max_credits(&self) -> usize {
        self.max_credits
    }

    /// The current fill level (metric: `checkpoint_alignment_buffer_credits_used`).
    pub fn credits_used(&self) -> usize {
        self.credits_used.load(Ordering::Relaxed)
    }

    /// Consume one credit for a new pending shard confirmation.
    ///
    /// Returns `Ok(())` if a credit was available, or
    /// `Err(AlignmentError::CreditExhausted)` if the buffer is full.
    pub fn acquire(&self) -> Result<(), AlignmentError> {
        // Load-then-CAS loop to avoid exceeding max_credits.
        loop {
            let current = self.credits_used.load(Ordering::Relaxed);
            if current >= self.max_credits {
                return Err(AlignmentError::CreditExhausted {
                    used: current,
                    max: self.max_credits,
                });
            }
            match self.credits_used.compare_exchange(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(_) => continue, // retry on concurrent modification
            }
        }
    }

    /// Release one credit after a shard confirmation is received.
    pub fn release(&self) {
        // Saturating to 0 to guard against double-release bugs.
        let prev = self
            .credits_used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
        let _ = prev; // fetch_update always returns Ok when closure is Some
    }

    /// Returns `true` if any credits remain available.
    pub fn has_capacity(&self) -> bool {
        self.credits_used.load(Ordering::Relaxed) < self.max_credits
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── CheckpointId ──────────────────────────────────────────────────────────

    #[test]
    fn checkpoint_id_display() {
        assert_eq!(CheckpointId(1).to_string(), "ckpt-1");
        assert_eq!(CheckpointId::NONE.to_string(), "ckpt-0");
    }

    #[test]
    fn checkpoint_id_next_is_monotone() {
        let c = CheckpointId(5);
        assert_eq!(c.checked_next(), Some(CheckpointId(6)));
        assert!(c.checked_next().is_some_and(|next| next > c));
        assert_eq!(CheckpointId(u64::MAX).checked_next(), None);
    }

    #[test]
    fn checkpoint_id_none_is_sentinel() {
        assert!(CheckpointId::NONE.is_none());
        assert!(!CheckpointId(1).is_none());
    }

    // ── ClusterCheckpoint ─────────────────────────────────────────────────────

    #[test]
    fn cluster_checkpoint_is_complete_when_all_shards_present() {
        let mut cc = ClusterCheckpoint::new(CheckpointId(1));
        let shards = [ShardId(0), ShardId(1)];
        assert!(!cc.is_complete(&shards));

        cc.record_shard(ShardId(0), PerShardCheckpoint::new(CheckpointId(1), 100));
        assert!(!cc.is_complete(&shards));

        cc.record_shard(ShardId(1), PerShardCheckpoint::new(CheckpointId(1), 200));
        assert!(cc.is_complete(&shards));
    }

    #[test]
    fn cluster_checkpoint_roundtrip_json() {
        let mut cc = ClusterCheckpoint::new(CheckpointId(3));
        cc.record_shard(ShardId(0), PerShardCheckpoint::new(CheckpointId(3), 42));
        let json = serde_json::to_string(&cc).unwrap();
        let back: ClusterCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(cc, back);
    }

    // ── AlignmentCreditTracker ────────────────────────────────────────────────

    /// Credits start at zero; acquiring up to max_credits succeeds.
    #[test]
    fn alignment_credits_acquire_up_to_limit() {
        let tracker = AlignmentCreditTracker::new(3);
        assert_eq!(tracker.credits_used(), 0);
        assert!(tracker.acquire().is_ok());
        assert!(tracker.acquire().is_ok());
        assert!(tracker.acquire().is_ok());
        assert_eq!(tracker.credits_used(), 3);
    }

    /// Acquiring beyond max_credits returns CreditExhausted (RS-3601).
    #[test]
    fn alignment_credits_exhaustion_returns_error() {
        let tracker = AlignmentCreditTracker::new(2);
        tracker.acquire().unwrap();
        tracker.acquire().unwrap();
        let err = tracker.acquire().unwrap_err();
        assert!(
            matches!(err, AlignmentError::CreditExhausted { used: 2, max: 2 }),
            "unexpected: {err:?}"
        );
    }

    /// Exhaustion error message contains RS-3601.
    #[test]
    fn alignment_credit_exhausted_error_message_contains_rs3601() {
        let err = AlignmentError::CreditExhausted { used: 5, max: 5 };
        assert!(err.to_string().contains("RS-3601"), "message: {err}");
    }

    /// Timeout error message contains RS-3602.
    #[test]
    fn alignment_timeout_error_message_contains_rs3602() {
        let err = AlignmentError::AlignmentTimeout;
        assert!(err.to_string().contains("RS-3602"), "message: {err}");
    }

    /// Releasing credits makes room for new acquisitions.
    #[test]
    fn alignment_credits_release_restores_capacity() {
        let tracker = AlignmentCreditTracker::new(1);
        tracker.acquire().unwrap();
        assert!(!tracker.has_capacity());
        tracker.release();
        assert!(tracker.has_capacity());
        tracker.acquire().unwrap(); // should succeed after release
    }

    /// Double-release saturates at zero (no panic, no wrap).
    #[test]
    fn alignment_credits_double_release_saturates() {
        let tracker = AlignmentCreditTracker::new(2);
        tracker.acquire().unwrap();
        tracker.release();
        tracker.release(); // no-op, not a panic
        assert_eq!(tracker.credits_used(), 0);
    }
}

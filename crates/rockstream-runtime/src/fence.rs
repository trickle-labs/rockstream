//! Paired runtime assertions for M4 — self-fencing and lease uniqueness —
//! and M7-S3 — control-leader-epoch fence composition (v0.45.2).
//!
//! Every assertion in this module is directly paired with a FizzBee invariant
//! in `formal/m4_self_fencing.fizz` or `formal/m7_control_plane_ha.fizz`
//! (FIZZBEE_TEST_PLAN.md §3.6/§3.8).
//!
//! ## Paired assertions
//!
//! | FizzBee invariant | This module |
//! |---|---|
//! | M4-S1 / M4-S3 | [`assert_valid_writer`] — fence-epoch CAS check before every epoch commit. |
//! | M4-S1 / M4-S3 | [`assert_single_lease_holder`] — at most one worker holds an active lease per shard. |
//! | M4-S2 | [`assert_self_fence_deadline`] — partitioned worker must self-fence before deadline. |
//! | M7-S3 | [`assert_valid_control_leader_epoch`] — a shard-fence write's captured control-leader epoch must not be stale. |

use std::time::{Duration, Instant};

use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};

// ─── Constants ───────────────────────────────────────────────────────────────

/// Default self-fence deadline: a worker that cannot reach the control plane
/// for this duration MUST self-fence and terminate (DESIGN.md §11.6).
///
/// Paired with `SELF_FENCE_AFTER = 3` steps in `formal/m4_self_fencing.fizz`.
pub const DEFAULT_SELF_FENCE_DEADLINE: Duration = Duration::from_secs(30);

// ─── M4-S1 / M4-S3: Fence-epoch validation ───────────────────────────────────

/// Assert that the caller holds the current valid lease for `shard_id` before
/// committing an epoch. This is the runtime paired assertion for FizzBee
/// invariants M4-S1 (single-writer) and M4-S3 (lease uniqueness).
///
/// # Panics
///
/// Panics with an `RS-1702` message if `token` is not the current valid
/// writer token for `shard_id`.
///
/// # Usage
///
/// Call this immediately before building a `WriteBatch` for any epoch commit:
///
/// ```rust,ignore
/// assert_valid_writer(shard_id, my_token, &shard_manager);
/// // Only reached if token is valid.
/// shard_db.commit(write_batch).await?;
/// ```
pub fn assert_valid_writer(
    shard_id: ShardId,
    token: LeaseToken,
    current_token: LeaseToken,
    current_holder: Option<WorkerId>,
) {
    // M4-S1 paired assertion: fence-epoch rejection on stale write.
    assert!(
        token == current_token,
        "RS-1702: M4-S1/M4-S3 violation — stale fence epoch on commit: \
         shard={shard_id}, writer_token={token}, current_token={current_token}, \
         current_holder={current_holder:?}. \
         next_steps: Worker has been fenced out; acquire a new lease before retrying."
    );
}

/// Assert that at most one worker holds an active lease for `shard_id`.
/// This is the runtime paired assertion for FizzBee invariant M4-S3
/// (lease uniqueness).
///
/// # Panics
///
/// Panics if `holder_count > 1`.
pub fn assert_single_lease_holder(shard_id: ShardId, holder_count: usize) {
    // M4-S3 paired assertion: lease uniqueness.
    assert!(
        holder_count <= 1,
        "RS-1701: M4-S3 violation — lease uniqueness: \
         shard={shard_id} has {holder_count} simultaneous holders (expected ≤ 1). \
         next_steps: Check worker assignments; force-acquire to evict the stale holder."
    );
}

// ─── M7-S3: Control-leader-epoch fence composition ───────────────────────────

/// Extract the control-leader epoch packed into a [`LeaseToken`]'s high 32
/// bits (v0.45.2 M7-S3 — see `rockstream_control::shard::ShardManager::set_leader_epoch`
/// and `rockstream_control::raft::control_leader_epoch`, which mint tokens
/// as `(leader_epoch << 32) | counter`).
pub fn control_leader_epoch_of(token: LeaseToken) -> u64 {
    token.0 >> 32
}

/// Assert that a shard-fence write's captured control-leader epoch is not
/// stale relative to the control plane's current epoch — the control-plane
/// analogue of [`assert_valid_writer`]'s fence check, but keyed on Raft
/// leadership epoch (derived from `(raft_term, control_leader_id)`) rather
/// than lease-token identity. A control-leadership change (new Raft term or
/// newly elected leader) must strictly invalidate any in-flight write
/// issued under the old epoch — this is the M7-S3 "no dual-leader write
/// window" guarantee, composed with M4's existing per-shard fencing.
///
/// # Panics
///
/// Panics with an `RS-1731` message if `write_epoch < current_epoch`.
pub fn assert_valid_control_leader_epoch(write_epoch: u64, current_epoch: u64) {
    // M7-S3 paired assertion: control-leader-epoch fence composition.
    assert!(
        write_epoch >= current_epoch,
        "RS-1731: M7-S3 violation — stale control-leader epoch on shard-fence \
         write: write_epoch={write_epoch}, current_epoch={current_epoch}. \
         next_steps: control leadership changed mid-write (new Raft term or \
         newly elected leader); the writer must re-acquire its lease under \
         the new leader before retrying."
    );
}

// ─── M4-S2: Self-fence deadline ───────────────────────────────────────────────

/// Tracks isolation state for a worker that cannot reach the control plane.
///
/// The worker must call [`SelfFenceGuard::tick`] on every heartbeat interval
/// while `can_reach_control == false`. If the deadline passes without
/// restoring contact, [`SelfFenceGuard::must_self_fence`] returns `true` and
/// the caller **must** terminate (self-fence).
///
/// Paired assertion for FizzBee M4-S2.
///
/// INVARIANT-BY-CONSTRUCTION: M4-S4 — this guard's isolation clock is driven
/// solely by `can_reach_control` (see [`SelfFenceGuard::tick`]); nothing in
/// this module reads object-store reachability. An object-store-only
/// partition (control reachable, object store not) can therefore never
/// start the isolation clock here and so can never reach
/// [`SelfFenceGuard::must_self_fence`]'s panic path — that scenario is
/// handled exclusively by `ObjectStoreBrownoutGuard`
/// (`crates/rockstream-sim/src/brownout.rs`), whose only failure states are
/// `Stalled`/`Blocked`, never termination.
#[derive(Debug)]
pub struct SelfFenceGuard {
    /// When the worker first lost contact with the control plane.
    isolated_since: Option<Instant>,
    /// Maximum isolation duration before self-fencing.
    deadline: Duration,
}

impl SelfFenceGuard {
    /// Create a guard with the default self-fence deadline.
    pub fn new() -> Self {
        Self {
            isolated_since: None,
            deadline: DEFAULT_SELF_FENCE_DEADLINE,
        }
    }

    /// Create a guard with a custom deadline (for tests).
    pub fn with_deadline(deadline: Duration) -> Self {
        Self {
            isolated_since: None,
            deadline,
        }
    }

    /// Called each heartbeat interval with the current connectivity state.
    ///
    /// When `can_reach_control` transitions from `true` to `false`, the
    /// isolation clock starts. When it transitions back, the clock resets.
    pub fn tick(&mut self, can_reach_control: bool) {
        if can_reach_control {
            self.isolated_since = None;
        } else if self.isolated_since.is_none() {
            self.isolated_since = Some(Instant::now());
        }
    }

    /// Returns `true` if the worker has been isolated long enough that it MUST
    /// self-fence immediately.
    ///
    /// # Paired assertion (M4-S2)
    ///
    /// The caller **must** check this before any epoch commit attempt and must
    /// terminate if it returns `true`. Failing to do so is a split-brain bug.
    pub fn must_self_fence(&self) -> bool {
        self.isolated_since
            .map(|t| t.elapsed() >= self.deadline)
            .unwrap_or(false)
    }

    /// Assert that the worker does NOT need to self-fence before committing.
    ///
    /// Panics with an `RS-1702` message if the deadline has passed without
    /// restoring control-plane contact.
    ///
    /// Call this immediately before any epoch commit on a worker that has
    /// `can_reach_control == false`.
    pub fn assert_within_deadline(&self) {
        if let Some(since) = self.isolated_since {
            let elapsed = since.elapsed();
            // M4-S2 paired assertion: worker must terminate before deadline.
            assert!(
                elapsed < self.deadline,
                "RS-1702: M4-S2 violation — self-fence deadline exceeded: \
                 isolated for {elapsed:?} (deadline={:?}). \
                 next_steps: Worker must self-fence immediately. \
                 Check control-plane connectivity and worker heartbeat configuration.",
                self.deadline
            );
        }
    }

    /// Reset the isolation clock (worker has re-established control contact).
    pub fn reset(&mut self) {
        self.isolated_since = None;
    }
}

impl Default for SelfFenceGuard {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── M4-S1 / M4-S3 ─────────────────────────────────────────────────────────

    /// M4-S1: A valid writer token passes the assertion.
    #[test]
    fn assert_valid_writer_passes_on_matching_token() {
        let shard = ShardId(1);
        let token = LeaseToken(7);
        // Should not panic.
        assert_valid_writer(shard, token, token, Some(WorkerId(1)));
    }

    /// M4-S1: A stale writer token must panic (fence-epoch rejection).
    #[test]
    #[should_panic(expected = "RS-1702")]
    fn assert_valid_writer_panics_on_stale_token() {
        let shard = ShardId(1);
        let old_token = LeaseToken(5);
        let new_token = LeaseToken(7);
        assert_valid_writer(shard, old_token, new_token, Some(WorkerId(2)));
    }

    /// M4-S3: Exactly one holder is acceptable.
    #[test]
    fn assert_single_lease_holder_passes_with_one_holder() {
        assert_single_lease_holder(ShardId(0), 1);
        assert_single_lease_holder(ShardId(0), 0);
    }

    /// M4-S3: Two holders is a lease uniqueness violation.
    #[test]
    #[should_panic(expected = "RS-1701")]
    fn assert_single_lease_holder_panics_on_two_holders() {
        assert_single_lease_holder(ShardId(0), 2);
    }

    // ── M7-S3: control-leader-epoch fence composition ───────────────────────

    #[test]
    fn control_leader_epoch_of_extracts_high_bits() {
        let token = LeaseToken((7u64 << 32) | 3);
        assert_eq!(control_leader_epoch_of(token), 7);
    }

    #[test]
    fn control_leader_epoch_of_zero_epoch_token() {
        let token = LeaseToken(42);
        assert_eq!(control_leader_epoch_of(token), 0);
    }

    #[test]
    fn assert_valid_control_leader_epoch_passes_when_current_or_newer() {
        assert_valid_control_leader_epoch(2, 2);
        assert_valid_control_leader_epoch(5, 2);
    }

    #[test]
    #[should_panic(expected = "RS-1731")]
    fn assert_valid_control_leader_epoch_panics_on_stale_epoch() {
        assert_valid_control_leader_epoch(1, 2);
    }

    // ── M4-S2: Self-fence deadline ─────────────────────────────────────────────

    /// A fresh guard (never isolated) does not require self-fencing.
    #[test]
    fn self_fence_guard_not_triggered_when_connected() {
        let mut guard = SelfFenceGuard::new();
        guard.tick(true);
        assert!(!guard.must_self_fence());
        guard.assert_within_deadline(); // must not panic
    }

    /// Guard tracks isolation time and triggers after deadline passes.
    #[test]
    fn self_fence_guard_triggers_after_deadline() {
        let deadline = Duration::from_millis(10);
        let mut guard = SelfFenceGuard::with_deadline(deadline);
        guard.tick(false); // starts isolation clock
        assert!(!guard.must_self_fence()); // not yet expired
                                           // Busy-wait until deadline passes.
        std::thread::sleep(deadline + Duration::from_millis(5));
        assert!(guard.must_self_fence());
    }

    /// M4-S2 paired assertion panics when deadline is exceeded.
    #[test]
    #[should_panic(expected = "RS-1702")]
    fn assert_within_deadline_panics_after_deadline() {
        let deadline = Duration::from_millis(10);
        let mut guard = SelfFenceGuard::with_deadline(deadline);
        guard.tick(false);
        std::thread::sleep(deadline + Duration::from_millis(5));
        guard.assert_within_deadline();
    }

    /// Recovering control contact resets the isolation clock.
    #[test]
    fn self_fence_guard_resets_on_recovery() {
        let deadline = Duration::from_millis(50);
        let mut guard = SelfFenceGuard::with_deadline(deadline);
        guard.tick(false); // start isolation
        guard.tick(true); // recover
        std::thread::sleep(deadline + Duration::from_millis(5));
        // After recovery, deadline should not be triggered.
        assert!(!guard.must_self_fence());
        guard.assert_within_deadline(); // must not panic
    }
}

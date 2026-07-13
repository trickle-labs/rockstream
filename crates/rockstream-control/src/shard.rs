//! Shard lease management for the RockStream control plane.
//!
//! The [`ShardManager`] is the authoritative source of truth for which worker
//! holds a write lease on each shard. It issues monotonically increasing
//! fencing tokens that prevent stale writers from committing after a lease is
//! revoked or transferred.
//!
//! ## Fencing invariant
//!
//! For any shard S, the manager maintains an **epoch counter** that
//! monotonically increases on every `acquire` call. A writer that holds token
//! `T` for shard `S` is only permitted to commit if `is_valid_writer(S, T)`
//! returns `true`. Once a newer token `T' > T` is issued for `S`, every
//! attempt with `T` returns `false` — the old writer is fenced out.
//!
//! ## Worker death
//!
//! When a worker TCP connection is lost, `release_worker(worker_id)` atomically
//! removes all shard leases held by that worker and returns their IDs. The
//! caller (typically [`ControlService`]) then uses [`ShardScheduler`] to
//! reassign those shards to surviving healthy workers.
//!
//! [`ControlService`]: crate::service::ControlService

use std::collections::HashMap;
use std::sync::Arc;

use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
use rockstream_types::lease::ShardLease;

/// Errors returned by [`ShardManager`] operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LeaseError {
    /// The shard is currently leased by a *different* worker.
    #[error("RS-1701: shard {shard_id} is already leased by worker {holder}")]
    AlreadyLeased { shard_id: ShardId, holder: WorkerId },
    /// The provided fencing token does not match the current token.
    #[error(
        "RS-1702: stale lease token for shard {shard_id} (provided {provided}, current {current})"
    )]
    StaleToken {
        shard_id: ShardId,
        provided: LeaseToken,
        current: LeaseToken,
    },
    /// The shard has no active lease (not yet acquired).
    #[error("RS-1703: shard {shard_id} has no active lease")]
    NoLease { shard_id: ShardId },
}

struct ShardManagerInner {
    /// Active leases keyed by shard ID.
    leases: HashMap<ShardId, ShardLease>,
    /// Global monotonic counter; incremented on every `acquire`.
    next_token: u64,
    /// Control-leader epoch mixed into every newly minted [`LeaseToken`]
    /// (v0.45.2 M7-S3): `token = (leader_epoch << 32) | counter`. Defaults
    /// to `0`, which makes every token exactly equal to the raw counter —
    /// preserving byte-for-byte backward compatibility with pre-v0.45.2
    /// behavior and every pre-existing M4 fencing test. Set via
    /// [`ShardManager::set_leader_epoch`], normally derived from
    /// `raft::control_leader_epoch(term, leader_id)` whenever the
    /// control-plane Raft term or leader identity changes.
    leader_epoch: u64,
}

/// Mint the next [`LeaseToken`], mixing in the current control-leader epoch
/// (M7-S3). `next_token` is bounded to `< 2^32` so it can never collide
/// with the epoch bits packed into the high 32 bits — this is the named
/// bound for the token counter; exceeding it is a hard invariant violation
/// (panics) rather than silently wrapping into the epoch bits and forging a
/// token that looks like it came from a different control-leader term.
fn mint_token(guard: &mut ShardManagerInner) -> LeaseToken {
    assert!(
        guard.next_token < (1u64 << 32),
        "shard lease token counter exhausted (would collide with the M7-S3 \
         leader-epoch bits); this indicates the control plane has been \
         running far longer than any deployment horizon and needs a token \
         counter reset/rotation, not a silent wraparound"
    );
    let token = (guard.leader_epoch << 32) | guard.next_token;
    guard.next_token += 1;
    LeaseToken(token)
}

/// Thread-safe manager for shard write leases.
///
/// All public methods are safe to call from multiple threads simultaneously.
#[derive(Clone)]
pub struct ShardManager {
    inner: Arc<RwLock<ShardManagerInner>>,
}

impl ShardManager {
    /// Create a new, empty `ShardManager`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ShardManagerInner {
                leases: HashMap::new(),
                next_token: 1,
                leader_epoch: 0,
            })),
        }
    }

    /// Set the control-leader epoch to mix into every subsequently minted
    /// [`LeaseToken`] (v0.45.2 M7-S3). Callers derive `epoch` from
    /// `raft::control_leader_epoch(term, leader_id)` whenever the
    /// control-plane Raft term or leader identity changes. Panics if
    /// `epoch >= 2^32` (the reserved high half of a `LeaseToken`'s 64 bits).
    pub fn set_leader_epoch(&self, epoch: u64) {
        assert!(
            epoch < (1u64 << 32),
            "control-leader epoch {epoch} does not fit in the reserved high \
             32 bits of a LeaseToken"
        );
        self.inner.write().leader_epoch = epoch;
    }

    /// The control-leader epoch currently mixed into newly minted tokens.
    pub fn leader_epoch(&self) -> u64 {
        self.inner.read().leader_epoch
    }

    /// Acquire a write lease on `shard_id` for `worker_id`.
    ///
    /// - If the shard has no current lease, a new lease is issued.
    /// - If the shard is already leased by `worker_id`, the existing lease is
    ///   **renewed** with a new (higher) fencing token.
    /// - If the shard is leased by a *different* worker, returns
    ///   [`LeaseError::AlreadyLeased`].
    ///
    /// Each successful call increments the global fencing token counter so the
    /// returned [`LeaseToken`] is always strictly greater than every previously
    /// issued token for *any* shard.
    pub fn acquire(
        &self,
        shard_id: ShardId,
        worker_id: WorkerId,
    ) -> Result<ShardLease, LeaseError> {
        let mut guard = self.inner.write();
        if let Some(existing) = guard.leases.get(&shard_id) {
            if existing.worker_id != worker_id {
                return Err(LeaseError::AlreadyLeased {
                    shard_id,
                    holder: existing.worker_id,
                });
            }
        }
        let token = mint_token(&mut guard);
        let lease = ShardLease::new(shard_id, worker_id, token);
        guard.leases.insert(shard_id, lease.clone());
        Ok(lease)
    }

    /// Force-acquire a write lease, evicting any current holder.
    ///
    /// Used by the control plane for rebalancing. Returns the new lease and,
    /// if a previous holder was evicted, its `WorkerId`.
    pub fn force_acquire(
        &self,
        shard_id: ShardId,
        worker_id: WorkerId,
    ) -> (ShardLease, Option<WorkerId>) {
        let mut guard = self.inner.write();
        let evicted = guard.leases.get(&shard_id).map(|l| l.worker_id);
        let token = mint_token(&mut guard);
        let lease = ShardLease::new(shard_id, worker_id, token);
        guard.leases.insert(shard_id, lease.clone());
        (lease, evicted)
    }

    /// Release the lease for `shard_id` if the provided `token` matches.
    ///
    /// Returns `true` if the lease was released. Returns `false` if there is
    /// no lease or the token is stale.
    pub fn release(&self, shard_id: ShardId, token: LeaseToken) -> bool {
        let mut guard = self.inner.write();
        let valid = guard
            .leases
            .get(&shard_id)
            .map(|l| l.lease_token == token)
            .unwrap_or(false);
        if valid {
            guard.leases.remove(&shard_id);
        }
        valid
    }

    /// Release all shards held by `worker_id`.
    ///
    /// Returns the list of shard IDs that were released. This is called when a
    /// worker's TCP connection drops so the control plane can reassign its
    /// shards to healthy workers.
    pub fn release_worker(&self, worker_id: WorkerId) -> Vec<ShardId> {
        let mut guard = self.inner.write();
        let freed: Vec<ShardId> = guard
            .leases
            .iter()
            .filter(|(_, l)| l.worker_id == worker_id)
            .map(|(id, _)| *id)
            .collect();
        for shard_id in &freed {
            guard.leases.remove(shard_id);
        }
        freed
    }

    /// Check whether `token` is the **current** active writer for `shard_id`.
    ///
    /// This is the write fence: a worker must call this before committing a
    /// shard epoch. If it returns `false`, the commit must be aborted.
    pub fn is_valid_writer(&self, shard_id: ShardId, token: LeaseToken) -> bool {
        let guard = self.inner.read();
        guard
            .leases
            .get(&shard_id)
            .map(|l| l.lease_token == token)
            .unwrap_or(false)
    }

    /// Return a snapshot of all active leases.
    pub fn leases(&self) -> Vec<ShardLease> {
        self.inner.read().leases.values().cloned().collect()
    }

    /// Return the lease for a specific shard, if one exists.
    pub fn get(&self, shard_id: ShardId) -> Option<ShardLease> {
        self.inner.read().leases.get(&shard_id).cloned()
    }

    /// Return the number of currently active leases.
    pub fn len(&self) -> usize {
        self.inner.read().leases.len()
    }

    /// Return `true` if there are no active leases.
    pub fn is_empty(&self) -> bool {
        self.inner.read().leases.is_empty()
    }

    /// Capture a full point-in-time snapshot of this manager's state
    /// (v0.45.2 M7-S4/S5: cross-control-node lease continuity).
    ///
    /// Used to persist state to the shared control object store so that a
    /// newly-elected leader on a *different real process* can pick up where
    /// the previous leader left off, instead of starting from an empty
    /// in-memory map (which would otherwise let a new leader believe every
    /// shard is unleased and hand out a conflicting grant — a real
    /// split-brain window that only a single-process test could hide).
    pub fn snapshot(&self) -> ShardManagerSnapshot {
        let guard = self.inner.read();
        ShardManagerSnapshot {
            leases: guard.leases.clone(),
            next_token: guard.next_token,
            leader_epoch: guard.leader_epoch,
        }
    }

    /// Replace this manager's entire state with a previously-captured
    /// snapshot. Used when a node transitions to Raft leader and must adopt
    /// the shared control-plane's last-known lease state before granting any
    /// new lease (v0.45.2 M7-S4/S5).
    pub fn restore(&self, snapshot: ShardManagerSnapshot) {
        let mut guard = self.inner.write();
        guard.leases = snapshot.leases;
        guard.next_token = snapshot.next_token;
        guard.leader_epoch = snapshot.leader_epoch;
    }
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new()
    }
}

/// A serializable point-in-time snapshot of [`ShardManager`]'s state
/// (v0.45.2 M7-S4/S5).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ShardManagerSnapshot {
    pub leases: HashMap<ShardId, ShardLease>,
    pub next_token: u64,
    pub leader_epoch: u64,
}

/// Durable store for [`ShardManagerSnapshot`], backed by the same shared
/// [`ObjectStore`] the control-plane Raft group uses for its term/vote state
/// (v0.45.2 M7-S4/S5 — the "control SlateDB" DESIGN.md §3 describes: the
/// elected leader is the sole writer, and a newly-elected leader on a
/// different real process loads the last-persisted snapshot here before
/// serving its first write).
///
/// This is a bounded, single-object snapshot (not an unbounded log): each
/// `save` fully overwrites the one object at [`Self::path`] rather than
/// appending, so storage size is bounded by the current lease-table size —
/// never by write history — and no SlateDB range-delete is ever involved.
pub struct ShardPersistentStore {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

impl ShardPersistentStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            path: Path::from("control/shard_manager/state.json"),
        }
    }

    /// Load the persisted snapshot, or the empty default if nothing has been
    /// persisted yet (first-ever boot of this control group).
    pub async fn load(&self) -> ShardManagerSnapshot {
        match self.store.get(&self.path).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
                Err(_) => ShardManagerSnapshot::default(),
            },
            Err(_) => ShardManagerSnapshot::default(),
        }
    }

    /// Persist a snapshot. Panics on failure (same discipline as
    /// `raft::RaftPersistentStore::save` — a silently-dropped write here
    /// could let a newly-elected leader forget a lease that genuinely still
    /// has a live holder, which is exactly the hazard this store exists to
    /// prevent).
    pub async fn save(&self, snapshot: &ShardManagerSnapshot) {
        let bytes = serde_json::to_vec(snapshot).expect("serialize ShardManagerSnapshot");
        self.store
            .put(&self.path, bytes.into())
            .await
            .expect("RS-0003: failed to persist shard-manager lease state");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::{ShardId, WorkerId};

    fn mgr() -> ShardManager {
        ShardManager::new()
    }

    // -----------------------------------------------------------------------
    // Basic acquire / release
    // -----------------------------------------------------------------------

    #[test]
    fn acquire_new_shard_succeeds() {
        let m = mgr();
        let lease = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        assert_eq!(lease.shard_id, ShardId(1));
        assert_eq!(lease.worker_id, WorkerId(10));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn same_worker_can_reacquire() {
        let m = mgr();
        let l1 = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        let l2 = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        // Token must be strictly higher on reacquire.
        assert!(l2.lease_token.0 > l1.lease_token.0);
    }

    // -----------------------------------------------------------------------
    // M7-S3: control-leader-epoch token composition
    // -----------------------------------------------------------------------

    #[test]
    fn default_leader_epoch_is_zero_and_token_equals_raw_counter() {
        let m = mgr();
        assert_eq!(m.leader_epoch(), 0);
        let lease = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        // With epoch=0, token == raw counter value (byte-for-byte backward
        // compatible with pre-v0.45.2 behavior).
        assert_eq!(lease.lease_token.0, 1);
    }

    #[test]
    fn setting_leader_epoch_is_packed_into_high_bits_of_new_tokens() {
        let m = mgr();
        m.acquire(ShardId(1), WorkerId(10)).unwrap(); // token 1, epoch 0
        m.set_leader_epoch(7);
        assert_eq!(m.leader_epoch(), 7);
        let lease = m.acquire(ShardId(2), WorkerId(11)).unwrap();
        assert_eq!(lease.lease_token.0 >> 32, 7);
        assert_eq!(lease.lease_token.0 & 0xFFFF_FFFF, 2);
    }

    #[test]
    fn raising_leader_epoch_makes_every_subsequent_token_strictly_greater() {
        let m = mgr();
        let l1 = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        m.set_leader_epoch(1);
        let l2 = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        assert!(l2.lease_token.0 > l1.lease_token.0);
        m.set_leader_epoch(2);
        let l3 = m.acquire(ShardId(1), WorkerId(10)).unwrap();
        assert!(l3.lease_token.0 > l2.lease_token.0);
    }

    #[test]
    #[should_panic(expected = "does not fit")]
    fn set_leader_epoch_rejects_out_of_range_epoch() {
        let m = mgr();
        m.set_leader_epoch(1u64 << 32);
    }

    #[test]
    fn different_worker_cannot_acquire_held_shard() {
        let m = mgr();
        m.acquire(ShardId(1), WorkerId(1)).unwrap();
        let err = m.acquire(ShardId(1), WorkerId(2)).unwrap_err();
        assert!(matches!(
            err,
            LeaseError::AlreadyLeased {
                shard_id: ShardId(1),
                holder: WorkerId(1),
            }
        ));
    }

    #[test]
    fn release_removes_lease() {
        let m = mgr();
        let lease = m.acquire(ShardId(5), WorkerId(3)).unwrap();
        assert!(m.release(ShardId(5), lease.lease_token));
        assert!(m.is_empty());
    }

    #[test]
    fn release_with_stale_token_fails() {
        let m = mgr();
        let l1 = m.acquire(ShardId(1), WorkerId(1)).unwrap();
        // Reacquire → new token.
        m.acquire(ShardId(1), WorkerId(1)).unwrap();
        // Old token can no longer release.
        assert!(!m.release(ShardId(1), l1.lease_token));
    }

    // -----------------------------------------------------------------------
    // Two-writer fence test
    //
    // This is the core proof required by v0.29: only the holder of the current
    // fencing token is permitted to commit.
    // -----------------------------------------------------------------------

    #[test]
    fn two_writer_fence_test() {
        let m = mgr();

        // Worker A acquires shard 1.
        let lease_a = m.acquire(ShardId(1), WorkerId(1)).unwrap();

        // Simulate Worker A being replaced: control plane force-acquires for
        // Worker B.  This evicts Worker A and issues a strictly higher token.
        let (lease_b, evicted) = m.force_acquire(ShardId(1), WorkerId(2));

        assert_eq!(evicted, Some(WorkerId(1)));
        assert!(lease_b.lease_token.0 > lease_a.lease_token.0);

        // Worker A's old token is now stale — it cannot commit.
        assert!(
            !m.is_valid_writer(ShardId(1), lease_a.lease_token),
            "Worker A must be fenced out after Worker B acquired the lease"
        );

        // Worker B's new token is valid — it can commit.
        assert!(
            m.is_valid_writer(ShardId(1), lease_b.lease_token),
            "Worker B must be the valid writer"
        );
    }

    // -----------------------------------------------------------------------
    // Worker death / reassignment
    // -----------------------------------------------------------------------

    #[test]
    fn worker_death_clears_all_its_leases() {
        let m = mgr();
        // Worker 1 owns shards 1, 2, 3.
        m.acquire(ShardId(1), WorkerId(1)).unwrap();
        m.acquire(ShardId(2), WorkerId(1)).unwrap();
        m.acquire(ShardId(3), WorkerId(1)).unwrap();
        // Worker 2 owns shard 4.
        m.acquire(ShardId(4), WorkerId(2)).unwrap();

        let freed = m.release_worker(WorkerId(1));
        assert_eq!(freed.len(), 3);
        assert!(!freed.contains(&ShardId(4)));
        assert_eq!(m.len(), 1); // Only Worker 2's shard remains.
    }

    #[test]
    fn worker_death_then_reassignment_issues_fresh_token() {
        let m = mgr();
        let old_lease = m.acquire(ShardId(1), WorkerId(1)).unwrap();

        // Worker 1 dies.
        let freed = m.release_worker(WorkerId(1));
        assert_eq!(freed, vec![ShardId(1)]);

        // Shard 1 is reassigned to Worker 2.
        let new_lease = m.acquire(ShardId(1), WorkerId(2)).unwrap();

        // New token must be strictly greater than the old token.
        assert!(
            new_lease.lease_token.0 > old_lease.lease_token.0,
            "reassignment must produce a fresh fencing token"
        );

        // Old token is now invalid.
        assert!(!m.is_valid_writer(ShardId(1), old_lease.lease_token));

        // New token is valid.
        assert!(m.is_valid_writer(ShardId(1), new_lease.lease_token));
    }

    // -----------------------------------------------------------------------
    // Force acquire / eviction
    // -----------------------------------------------------------------------

    #[test]
    fn force_acquire_with_no_existing_lease_has_no_eviction() {
        let m = mgr();
        let (lease, evicted) = m.force_acquire(ShardId(1), WorkerId(99));
        assert!(evicted.is_none());
        assert_eq!(lease.worker_id, WorkerId(99));
    }

    #[test]
    fn force_acquire_evicts_existing_holder() {
        let m = mgr();
        m.acquire(ShardId(1), WorkerId(5)).unwrap();
        let (_, evicted) = m.force_acquire(ShardId(1), WorkerId(6));
        assert_eq!(evicted, Some(WorkerId(5)));
        // Shard now belongs to Worker 6.
        assert_eq!(m.get(ShardId(1)).unwrap().worker_id, WorkerId(6));
    }

    // -----------------------------------------------------------------------
    // Token monotonicity across shards
    // -----------------------------------------------------------------------

    #[test]
    fn tokens_are_globally_monotone() {
        let m = mgr();
        let t1 = m.acquire(ShardId(1), WorkerId(1)).unwrap().lease_token;
        let t2 = m.acquire(ShardId(2), WorkerId(2)).unwrap().lease_token;
        let t3 = m.acquire(ShardId(3), WorkerId(3)).unwrap().lease_token;
        assert!(t1.0 < t2.0);
        assert!(t2.0 < t3.0);
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    #[test]
    fn leases_snapshot() {
        let m = mgr();
        m.acquire(ShardId(1), WorkerId(1)).unwrap();
        m.acquire(ShardId(2), WorkerId(2)).unwrap();
        let snapshot = m.leases();
        assert_eq!(snapshot.len(), 2);
    }

    #[test]
    fn get_returns_none_for_unknown_shard() {
        let m = mgr();
        assert!(m.get(ShardId(99)).is_none());
    }

    // -----------------------------------------------------------------------
    // v0.45.2 M7-S4/S5: snapshot / restore for cross-process leader takeover
    // -----------------------------------------------------------------------

    #[test]
    fn snapshot_restore_round_trips_state() {
        let m = mgr();
        m.acquire(ShardId(1), WorkerId(10)).unwrap();
        m.set_leader_epoch(3);
        m.acquire(ShardId(2), WorkerId(20)).unwrap();
        let snap = m.snapshot();

        let m2 = mgr();
        assert!(m2.is_empty());
        m2.restore(snap.clone());
        assert_eq!(m2.len(), 2);
        assert_eq!(m2.leader_epoch(), 3);
        assert_eq!(
            m2.get(ShardId(1)).unwrap().worker_id,
            m.get(ShardId(1)).unwrap().worker_id
        );
        assert_eq!(m2.snapshot(), snap);
    }

    #[tokio::test]
    async fn shard_persistent_store_round_trips_through_object_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let persist = ShardPersistentStore::new(store.clone());

        // Nothing persisted yet → empty default.
        let loaded = persist.load().await;
        assert_eq!(loaded, ShardManagerSnapshot::default());

        let m = mgr();
        m.acquire(ShardId(5), WorkerId(50)).unwrap();
        m.set_leader_epoch(9);
        let snap = m.snapshot();
        persist.save(&snap).await;

        // A second store instance sharing the same backing object store
        // sees the persisted snapshot (models a different real process /
        // newly-elected leader).
        let persist2 = ShardPersistentStore::new(store);
        let loaded2 = persist2.load().await;
        assert_eq!(loaded2, snap);
    }
}

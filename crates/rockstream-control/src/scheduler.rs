//! Shard scheduling: distributes shards across healthy workers.
//!
//! The [`ShardScheduler`] combines the [`TopologyCatalog`] (who is alive) with
//! the [`ShardManager`] (who owns which shard) and the [`PlacementAlgorithm`]
//! (pick the best worker) to produce a consistent shard-to-worker assignment.
//!
//! ## Reassignment after worker death
//!
//! When a worker disconnects, [`ShardScheduler::on_worker_dead`] is the single
//! call-site that:
//! 1. Releases all leases held by the dead worker.
//! 2. For each freed shard, picks the next best healthy worker via
//!    [`PlacementAlgorithm::choose`].
//! 3. Issues a new (higher) fencing token via [`ShardManager::acquire`].
//! 4. Returns the new [`ShardLease`] list so `ControlService` can push
//!    [`ControlMessage::ShardAssigned`] to the new holders.
//!
//! ## v0.45.2 M7-S2: leader-only write gating
//!
//! Shard-assignment is one of the three write paths named in
//! `.claude/v0.45.2-plan.md` §"S2 — Leader-only write gating" (alongside
//! lease-grant in [`crate::service::ControlService`] and the workload
//! catalog in `rockstream_sql::workload_catalog::WorkloadCatalog`). When a
//! [`crate::raft::RaftHandle`] is attached via [`ShardScheduler::with_raft`],
//! every shard-assignment write is gated behind
//! [`crate::raft::RaftHandle::require_leader`]: a non-leader node's attempt
//! is rejected with [`SchedulerError::NotLeader`] (`RS-1731
//! control.not_leader`) before it ever reaches [`ShardManager`].

use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::lease::ShardLease;

use crate::placement::PlacementAlgorithm;
use crate::raft::RaftHandle;
use crate::shard::{LeaseError, ShardManager};
use crate::topology::TopologyCatalog;
use rockstream_types::compatibility::{ProtocolVersion, StorageFormatVersion};
use rockstream_types::topology::assignment_compatible;

/// Assignment result from [`ShardScheduler::assign_initial_shards`] or
/// [`ShardScheduler::on_worker_dead`].
#[derive(Debug, Clone)]
pub struct ShardAssignment {
    /// The new (or updated) lease.
    pub lease: ShardLease,
    /// `Some(old_worker_id)` if this assignment evicted an existing holder.
    pub evicted: Option<WorkerId>,
}

// Helper to make test assertions more readable.
#[cfg(test)]
impl ShardAssignment {
    fn worker_id(&self) -> WorkerId {
        self.lease.worker_id
    }
}

/// Errors from [`ShardScheduler`] write paths.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchedulerError {
    /// v0.45.2 M7-S2: rejected because this control node is not currently
    /// the Raft leader. Maps to `RS-1731 control.not_leader`.
    #[error(
        "RS-1731: control.not_leader — shard-assignment write rejected: \
         this control node is not the current Raft leader. \
         next_steps: retry against the elected leader; discover it via \
         the control group's current-leader RPC."
    )]
    NotLeader,
}

/// Combines topology awareness with shard-lease management.
///
/// Cloning a `ShardScheduler` is cheap: both fields are
/// `Arc`-backed and share state.
#[derive(Clone)]
pub struct ShardScheduler {
    pub(crate) catalog: TopologyCatalog,
    pub(crate) manager: ShardManager,
    raft: Option<RaftHandle>,
}

impl ShardScheduler {
    /// Create a new scheduler backed by the given catalog and manager.
    pub fn new(catalog: TopologyCatalog, manager: ShardManager) -> Self {
        Self {
            catalog,
            manager,
            raft: None,
        }
    }

    /// Attach a Raft handle so every shard-assignment write is gated behind
    /// leadership (v0.45.2 M7-S2). Without this, [`ShardScheduler`] behaves
    /// exactly as before v0.45.2 (no gating) — preserving backward
    /// compatibility for any caller that doesn't participate in a Raft
    /// control group.
    pub fn with_raft(mut self, raft: RaftHandle) -> Self {
        self.raft = Some(raft);
        self
    }

    fn require_leader(&self) -> Result<(), SchedulerError> {
        if let Some(raft) = &self.raft {
            raft.require_leader()
                .map_err(|_: crate::raft::NotLeader| SchedulerError::NotLeader)?;
        }
        Ok(())
    }

    /// Shared compatibility gate for shard and pipeline assignment.
    pub fn compatible_for_assignment(
        &self,
        protocol: ProtocolVersion,
        storage_format: StorageFormatVersion,
    ) -> bool {
        assignment_compatible(&self.catalog.healthy_workers(), protocol, storage_format)
    }

    /// Assign an initial set of shards to healthy workers.
    ///
    /// Each shard is assigned to the healthy worker with the highest
    /// `capacity_headroom`.  If no healthy workers are available, the shard is
    /// skipped and not included in the result.
    ///
    /// Shards that are already assigned to a *healthy* worker are left alone.
    /// Shards held by an *unhealthy* worker are force-reassigned.
    ///
    /// v0.45.2 M7-S2: rejected with [`SchedulerError::NotLeader`] if a Raft
    /// handle is attached (via [`Self::with_raft`]) and this node is not
    /// currently the leader — before any shard is touched.
    pub fn assign_initial_shards(
        &self,
        shard_ids: &[ShardId],
    ) -> Result<Vec<ShardAssignment>, SchedulerError> {
        self.require_leader()?;
        let workers = self.catalog.healthy_workers();
        if workers.is_empty() {
            return Ok(Vec::new());
        }

        let mut assignments = Vec::with_capacity(shard_ids.len());
        let compatible_workers: Vec<_> = workers
            .iter()
            .filter(|worker| {
                assignment_compatible(
                    &workers,
                    worker.protocol_range.max,
                    worker.storage_format_range.max,
                )
            })
            .cloned()
            .collect();
        for &shard_id in shard_ids {
            // Skip shards already held by a healthy worker.
            if let Some(existing) = self.manager.get(shard_id) {
                let holder_healthy = workers.iter().any(|w| w.worker_id == existing.worker_id);
                let holder_compatible = self
                    .catalog
                    .get(existing.worker_id)
                    .map(|worker| {
                        assignment_compatible(
                            &workers,
                            worker.protocol_range.max,
                            worker.storage_format_range.max,
                        )
                    })
                    .unwrap_or(false);
                if holder_healthy && holder_compatible {
                    continue;
                }
            }

            // Pick best worker and force-acquire (evicts any stale holder).
            if let Some(winner) = PlacementAlgorithm::choose(&compatible_workers) {
                let (lease, evicted) = self.manager.force_acquire(shard_id, winner.worker_id);
                assignments.push(ShardAssignment { lease, evicted });
            }
        }
        Ok(assignments)
    }

    /// Handle a worker disconnect: release its leases and reassign shards.
    ///
    /// Returns the list of new [`ShardAssignment`]s for the freed shards.
    /// Shards that cannot be reassigned (no healthy workers left) are omitted.
    ///
    /// v0.45.2 M7-S2: rejected with [`SchedulerError::NotLeader`] if a Raft
    /// handle is attached (via [`Self::with_raft`]) and this node is not
    /// currently the leader — before any lease is released or reassigned.
    pub fn on_worker_dead(
        &self,
        dead_worker_id: WorkerId,
    ) -> Result<Vec<ShardAssignment>, SchedulerError> {
        self.require_leader()?;
        let freed = self.manager.release_worker(dead_worker_id);
        if freed.is_empty() {
            return Ok(Vec::new());
        }

        let workers: Vec<_> = self
            .catalog
            .healthy_workers()
            .into_iter()
            .filter(|w| w.worker_id != dead_worker_id)
            .collect();
        let preferred_az = self
            .catalog
            .get(dead_worker_id)
            .map(|worker| worker.location.availability_zone);

        if workers.is_empty() {
            return Ok(Vec::new());
        }

        let mut assignments = Vec::with_capacity(freed.len());
        let compatible_workers: Vec<_> = workers
            .iter()
            .filter(|worker| {
                assignment_compatible(
                    &workers,
                    worker.protocol_range.max,
                    worker.storage_format_range.max,
                )
            })
            .cloned()
            .collect();
        for shard_id in freed {
            if let Some(winner) = PlacementAlgorithm::choose_with_preference(
                &compatible_workers,
                preferred_az.as_deref(),
            ) {
                // acquire() should always succeed here because we just released
                // the shard; no other worker holds it.
                match self.manager.acquire(shard_id, winner.worker_id) {
                    Ok(lease) => assignments.push(ShardAssignment {
                        lease,
                        evicted: Some(dead_worker_id),
                    }),
                    Err(LeaseError::AlreadyLeased { .. }) => {
                        // Race: another thread already assigned it.  Return the
                        // current lease instead.
                        if let Some(lease) = self.manager.get(shard_id) {
                            assignments.push(ShardAssignment {
                                lease,
                                evicted: Some(dead_worker_id),
                            });
                        }
                    }
                    Err(_) => {}
                }
            }
        }
        Ok(assignments)
    }

    /// Expose the underlying [`ShardManager`] for direct fence queries.
    pub fn manager(&self) -> &ShardManager {
        &self.manager
    }

    /// Expose the underlying [`TopologyCatalog`].
    pub fn catalog(&self) -> &TopologyCatalog {
        &self.catalog
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::compatibility::{SupportedStorageFormatRange, SupportedVersionRange};
    use rockstream_types::ids::{ShardId, WorkerId};
    use rockstream_types::topology::{CapacityHeadroom, NodeRole, WorkerRegistration};

    fn make_scheduler() -> ShardScheduler {
        let catalog = TopologyCatalog::new();
        let manager = ShardManager::new();
        ShardScheduler::new(catalog, manager)
    }

    fn register(sched: &ShardScheduler, id: u64, headroom: f64) -> WorkerId {
        let reg = WorkerRegistration::new(
            WorkerId(id),
            NodeRole::Worker,
            format!("127.0.0.1:{}", 8000 + id),
            CapacityHeadroom::new(headroom),
        );
        sched.catalog.register(&reg)
    }

    #[test]
    fn assign_initial_shards_to_best_worker() {
        let sched = make_scheduler();
        register(&sched, 1, 0.9); // Worker 1 has most capacity
        register(&sched, 2, 0.3); // Worker 2 less capacity

        let shards = [ShardId(1), ShardId(2), ShardId(3)];
        let assignments = sched.assign_initial_shards(&shards).unwrap();

        // All three shards should be assigned (workers are healthy).
        assert_eq!(assignments.len(), 3);
        // The best worker (highest headroom = worker 1) should get all shards
        // because placement picks max headroom each time and headroom doesn't
        // decrease in our test catalog.
        for a in &assignments {
            assert_eq!(a.worker_id(), WorkerId(1));
        }
    }

    #[test]
    fn mixed_versions_choose_a_compatible_worker() {
        let sched = make_scheduler();
        register(&sched, 1, 0.3);
        let v2_worker = WorkerRegistration::new(
            WorkerId(2),
            NodeRole::Worker,
            "127.0.0.1:8002",
            CapacityHeadroom::new(0.9),
        )
        .with_compatibility(
            SupportedVersionRange::v1_through_v2(),
            SupportedStorageFormatRange::v1_through_v2(),
        );
        sched.catalog.register(&v2_worker);

        let assignments = sched.assign_initial_shards(&[ShardId(1)]).unwrap();

        assert_eq!(assignments[0].worker_id(), WorkerId(1));
    }

    #[test]
    fn assign_no_workers_returns_empty() {
        let sched = make_scheduler();
        let assignments = sched.assign_initial_shards(&[ShardId(1)]).unwrap();
        assert!(assignments.is_empty());
    }

    #[test]
    fn worker_death_causes_reassignment() {
        let sched = make_scheduler();
        register(&sched, 1, 0.8);
        register(&sched, 2, 0.7);

        // Worker 1 owns shards 1 and 2.
        sched.manager.acquire(ShardId(1), WorkerId(1)).unwrap();
        sched.manager.acquire(ShardId(2), WorkerId(1)).unwrap();

        let old_token_1 = sched.manager.get(ShardId(1)).unwrap().lease_token;
        let old_token_2 = sched.manager.get(ShardId(2)).unwrap().lease_token;

        // Worker 1 dies.
        let reassignments = sched.on_worker_dead(WorkerId(1)).unwrap();
        assert_eq!(reassignments.len(), 2);

        // All freed shards should now be assigned to Worker 2.
        for a in &reassignments {
            assert_eq!(a.lease.worker_id, WorkerId(2));
            assert_eq!(a.evicted, Some(WorkerId(1)));
        }

        // Old tokens must be invalid.
        assert!(!sched.manager.is_valid_writer(ShardId(1), old_token_1));
        assert!(!sched.manager.is_valid_writer(ShardId(2), old_token_2));

        // New tokens must be valid.
        let new_token_1 = sched.manager.get(ShardId(1)).unwrap().lease_token;
        let new_token_2 = sched.manager.get(ShardId(2)).unwrap().lease_token;
        assert!(sched.manager.is_valid_writer(ShardId(1), new_token_1));
        assert!(sched.manager.is_valid_writer(ShardId(2), new_token_2));
    }

    #[test]
    fn worker_death_with_no_remaining_workers_frees_shards() {
        let sched = make_scheduler();
        register(&sched, 1, 1.0);

        sched.manager.acquire(ShardId(1), WorkerId(1)).unwrap();

        // Only worker dies → no reassignments possible, but shards are freed.
        let reassignments = sched.on_worker_dead(WorkerId(1)).unwrap();
        assert!(reassignments.is_empty());
        assert!(sched.manager.is_empty());
    }

    #[test]
    fn assign_skips_already_held_healthy_shards() {
        let sched = make_scheduler();
        register(&sched, 1, 0.9);

        // Pre-assign shard 1 to worker 1.
        sched.manager.acquire(ShardId(1), WorkerId(1)).unwrap();
        let existing_token = sched.manager.get(ShardId(1)).unwrap().lease_token;

        // assign_initial_shards should skip shard 1 since worker 1 is healthy.
        let assignments = sched
            .assign_initial_shards(&[ShardId(1), ShardId(2)])
            .unwrap();

        // Only shard 2 should be newly assigned.
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].lease.shard_id, ShardId(2));

        // Shard 1's token must be unchanged.
        assert_eq!(
            sched.manager.get(ShardId(1)).unwrap().lease_token,
            existing_token
        );
    }

    // -----------------------------------------------------------------------
    // v0.45.2 M7-S2: leader-only write gating for shard-assignment
    // -----------------------------------------------------------------------

    /// Without a Raft handle attached, `ShardScheduler` behaves exactly as
    /// before v0.45.2 — no gating.
    #[test]
    fn no_raft_attached_preserves_pre_v0_45_2_behavior() {
        let sched = make_scheduler();
        register(&sched, 1, 0.9);
        let assignments = sched.assign_initial_shards(&[ShardId(1)]).unwrap();
        assert_eq!(assignments.len(), 1);
    }

    /// `assign_initial_shards` and `on_worker_dead` are both rejected with
    /// `SchedulerError::NotLeader` when a Raft handle is attached and this
    /// node is not currently the leader.
    #[tokio::test]
    async fn shard_assignment_rejected_when_not_raft_leader() {
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let node = crate::raft::spawn_raft_node(
            "127.0.0.1:0",
            crate::raft::RaftConfig::new(0, Vec::new(), false),
            store,
        )
        .await
        .unwrap();
        // Never elected (bootstrap=false, sole node, no timeout fired yet):
        // still Follower immediately after spawn.
        assert!(!node.handle.is_leader());

        let sched = make_scheduler().with_raft(node.handle.clone());
        register(&sched, 1, 0.9);

        let err = sched.assign_initial_shards(&[ShardId(1)]).unwrap_err();
        assert_eq!(err, SchedulerError::NotLeader);

        let err2 = sched.on_worker_dead(WorkerId(1)).unwrap_err();
        assert_eq!(err2, SchedulerError::NotLeader);

        node.shutdown();
    }

    /// Once the attached Raft handle becomes leader, shard-assignment
    /// writes succeed.
    #[tokio::test]
    async fn shard_assignment_succeeds_once_raft_leader() {
        let store: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let node = crate::raft::spawn_raft_node(
            "127.0.0.1:0",
            crate::raft::RaftConfig::new(0, Vec::new(), true),
            store,
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(node.handle.is_leader());

        let sched = make_scheduler().with_raft(node.handle.clone());
        register(&sched, 1, 0.9);

        let assignments = sched.assign_initial_shards(&[ShardId(1)]).unwrap();
        assert_eq!(assignments.len(), 1);

        node.shutdown();
    }
}

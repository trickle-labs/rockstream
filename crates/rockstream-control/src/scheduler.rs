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

use std::collections::{BTreeMap, HashMap};

use rockstream_types::frontier::FrontierGeneration;
use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};
use rockstream_types::lease::{LeaseHeatHint, ShardLease};

use crate::placement::PlacementAlgorithm;
use crate::raft::RaftHandle;
use crate::shard::{LeaseError, ShardManager};
use crate::topology::TopologyCatalog;
use rockstream_types::compatibility::{ProtocolVersion, StorageFormatVersion};
use rockstream_types::topology::{assignment_compatible, WorkerInfo};

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

/// Policy parameters for moving already allocated shards, independent of
/// the worker count. The policy does not grant or revoke leases.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ElasticRebalanceConfig {
    pub hysteresis_frontiers: u32,
    pub minimum_residency_ms: u64,
    pub minimum_headroom_gap: f64,
}

impl Default for ElasticRebalanceConfig {
    fn default() -> Self {
        Self {
            hysteresis_frontiers: 3,
            minimum_residency_ms: 60_000,
            minimum_headroom_gap: 0.20,
        }
    }
}

/// A proposal to the canonical online movement executor, never a lease grant.
#[derive(Debug, Clone, PartialEq)]
pub struct ElasticRebalanceDecision {
    pub generation: FrontierGeneration,
    pub frontier: u64,
    pub shard_id: ShardId,
    pub donor: WorkerId,
    pub donor_lease_token: LeaseToken,
    pub recipient: WorkerId,
    pub donor_shards: usize,
    pub recipient_shards: usize,
    pub headroom_gap: f64,
}

impl ElasticRebalanceDecision {
    /// Stable operation identity for idempotent dispatch and replay.
    pub fn operation_id(&self) -> String {
        format!(
            "auto-rebalance-{}-{}-{}",
            self.shard_id.0, self.donor_lease_token.0, self.recipient.0
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RebalanceCandidate {
    shard_id: ShardId,
    donor_lease_token: LeaseToken,
    recipient: WorkerId,
}

/// A deterministic, read-only scheduler policy. Observations must be from
/// committed membership-aware frontiers. Callers must serialize dispatch
/// through the online movement executor and revalidate the donor lease there.
pub struct ElasticRebalancePolicy {
    config: ElasticRebalanceConfig,
    last_frontier: Option<(FrontierGeneration, u64)>,
    pending: Option<(RebalanceCandidate, u32)>,
}

impl ElasticRebalancePolicy {
    pub fn new(config: ElasticRebalanceConfig) -> Self {
        Self {
            config,
            last_frontier: None,
            pending: None,
        }
    }

    pub fn observe(
        &mut self,
        generation: FrontierGeneration,
        frontier: u64,
        now_ms: u64,
        workers: &[WorkerInfo],
        leases: &[ShardLease],
    ) -> Option<ElasticRebalanceDecision> {
        if let Some((old_generation, old_frontier)) = self.last_frontier {
            if generation < old_generation
                || (generation == old_generation && frontier <= old_frontier)
            {
                return None;
            }
            if generation != old_generation {
                self.pending = None;
            }
        }
        self.last_frontier = Some((generation, frontier));
        let Some(decision) = self.candidate(generation, frontier, now_ms, workers, leases) else {
            self.pending = None;
            return None;
        };
        let identity = RebalanceCandidate {
            shard_id: decision.shard_id,
            donor_lease_token: decision.donor_lease_token,
            recipient: decision.recipient,
        };
        let count = match self.pending {
            Some((previous, count)) if previous == identity => count.saturating_add(1),
            _ => 1,
        };
        if count < self.config.hysteresis_frontiers.max(1) {
            self.pending = Some((identity, count));
            return None;
        }
        self.pending = None;
        Some(decision)
    }

    fn candidate(
        &self,
        generation: FrontierGeneration,
        frontier: u64,
        now_ms: u64,
        workers: &[WorkerInfo],
        leases: &[ShardLease],
    ) -> Option<ElasticRebalanceDecision> {
        let eligible: BTreeMap<_, _> = workers
            .iter()
            .filter(|worker| {
                worker.healthy
                    && worker.lifecycle.is_active()
                    && worker.capabilities.shared_shard_store_id.is_some()
                    && worker.capacity_headroom.fraction().is_finite()
            })
            .map(|worker| (worker.worker_id, worker))
            .collect();
        let mut owned = BTreeMap::<WorkerId, usize>::new();
        for lease in leases {
            *owned.entry(lease.worker_id).or_default() += 1;
        }

        let mut best: Option<ElasticRebalanceDecision> = None;
        for lease in leases {
            let Some(donor) = eligible.get(&lease.worker_id) else {
                continue;
            };
            if now_ms.saturating_sub(lease.acquired_at_ms) < self.config.minimum_residency_ms
                || now_ms < lease.acquired_at_ms
            {
                continue;
            }
            let donor_shards = owned[&lease.worker_id];
            for (&recipient, target) in &eligible {
                if recipient == lease.worker_id
                    || target.capabilities.shared_shard_store_id
                        != donor.capabilities.shared_shard_store_id
                    || !target.protocol_range.contains(donor.protocol_range.max)
                    || !target.storage_format_range.contains(donor.storage_format_range.max)
                    || target.capacity_headroom.fraction() <= 0.0
                {
                    continue;
                }
                let recipient_shards = owned.get(&recipient).copied().unwrap_or(0);
                let gap = donor_shards.saturating_sub(recipient_shards);
                let headroom_gap =
                    target.capacity_headroom.fraction() - donor.capacity_headroom.fraction();
                // Count balancing must improve the distribution. A sustained
                // memory-pressure signal may also move one shard between
                // nearly balanced workers, but never into a more-loaded target.
                let pressure = self.config.minimum_headroom_gap.is_finite()
                    && self.config.minimum_headroom_gap > 0.0
                    && headroom_gap >= self.config.minimum_headroom_gap;
                if gap < 2 && !(donor_shards >= recipient_shards && pressure) {
                    continue;
                }
                let decision = ElasticRebalanceDecision {
                    generation,
                    frontier,
                    shard_id: lease.shard_id,
                    donor: lease.worker_id,
                    donor_lease_token: lease.lease_token,
                    recipient,
                    donor_shards,
                    recipient_shards,
                    headroom_gap,
                };
                let better = best.as_ref().is_none_or(|current| {
                    gap.cmp(&current.donor_shards.saturating_sub(current.recipient_shards))
                        .then_with(|| headroom_gap.total_cmp(&current.headroom_gap))
                        .then_with(|| current.shard_id.cmp(&decision.shard_id))
                        .then_with(|| current.recipient.cmp(&decision.recipient))
                        .is_gt()
                });
                if better {
                    best = Some(decision);
                }
            }
        }
        best
    }
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
    /// Assign shards across healthy compatible workers by current lease count.
    /// Higher reported headroom breaks ties. Counts include the new leases
    /// made in this call, so a batch does not pile onto one static winner.
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
        let mut owned: HashMap<WorkerId, usize> = HashMap::new();
        for lease in self.manager.leases() {
            *owned.entry(lease.worker_id).or_default() += 1;
        }
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

            // Prefer the least-loaded workers; use headroom only for ties.
            let least_owned = compatible_workers
                .iter()
                .map(|worker| owned.get(&worker.worker_id).copied().unwrap_or(0))
                .min();
            let candidates: Vec<_> = compatible_workers
                .iter()
                .filter(|worker| {
                    Some(owned.get(&worker.worker_id).copied().unwrap_or(0)) == least_owned
                })
                .cloned()
                .collect();
            if let Some(winner) = PlacementAlgorithm::choose(&candidates) {
                let (lease, evicted) = self.manager.force_acquire(shard_id, winner.worker_id);
                *owned.entry(winner.worker_id).or_default() += 1;
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
        let carried_heat: HashMap<_, _> = self
            .manager
            .leases()
            .into_iter()
            .filter(|lease| lease.worker_id == dead_worker_id)
            .map(|lease| (lease.shard_id, lease.heat_hint))
            .collect();
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
        // Reassignment is a batch too: preserve the initial-placement
        // balancing rule instead of repeatedly choosing one static winner.
        let mut owned: HashMap<WorkerId, usize> = HashMap::new();
        for lease in self.manager.leases() {
            *owned.entry(lease.worker_id).or_default() += 1;
        }
        for shard_id in freed {
            let least_owned = compatible_workers
                .iter()
                .map(|worker| owned.get(&worker.worker_id).copied().unwrap_or(0))
                .min();
            let candidates: Vec<_> = compatible_workers
                .iter()
                .filter(|worker| {
                    Some(owned.get(&worker.worker_id).copied().unwrap_or(0)) == least_owned
                })
                .cloned()
                .collect();
            if let Some(winner) =
                PlacementAlgorithm::choose_with_preference(&candidates, preferred_az.as_deref())
            {
                // acquire() should always succeed here because we just released
                // the shard; no other worker holds it.
                match self.manager.acquire_with_heat(
                    shard_id,
                    winner.worker_id,
                    carried_heat
                        .get(&shard_id)
                        .cloned()
                        .unwrap_or_else(LeaseHeatHint::default),
                ) {
                    Ok(lease) => {
                        *owned.entry(winner.worker_id).or_default() += 1;
                        assignments.push(ShardAssignment {
                            lease,
                            evicted: Some(dead_worker_id),
                        });
                    }
                    Err(LeaseError::AlreadyLeased { .. }) => {
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

    fn rebalance_worker(id: u64, headroom: f64, store: u8) -> WorkerInfo {
        WorkerInfo::from_registration(
            &WorkerRegistration::new(
                WorkerId(id),
                NodeRole::Worker,
                format!("127.0.0.1:{}", 8000 + id),
                CapacityHeadroom::new(headroom),
            )
            .with_capabilities(rockstream_types::topology::WorkerCapabilities {
                shared_shard_store_id: Some([store; 32]),
                ..Default::default()
            }),
        )
    }

    fn rebalance_lease(shard: u64, worker: u64, acquired_at_ms: u64) -> ShardLease {
        let mut lease = ShardLease::new(ShardId(shard), WorkerId(worker), LeaseToken(shard));
        lease.acquired_at_ms = acquired_at_ms;
        lease
    }

    #[test]
    fn elastic_scale_out_proposes_one_move_after_distinct_committed_frontiers() {
        let mut policy = ElasticRebalancePolicy::new(ElasticRebalanceConfig::default());
        let workers = [
            rebalance_worker(1, 0.4, 1),
            rebalance_worker(2, 0.4, 1),
            rebalance_worker(3, 0.9, 1),
            rebalance_worker(4, 0.9, 1),
            rebalance_worker(5, 1.0, 2),
        ];
        let mut leases = vec![
            rebalance_lease(4, 2, 0),
            rebalance_lease(3, 2, 0),
            rebalance_lease(2, 1, 0),
            rebalance_lease(1, 1, 0),
        ];
        let generation = FrontierGeneration(7);
        for frontier in [1, 1, 2, 2] {
            assert_eq!(
                policy.observe(generation, frontier, 60_000, &workers, &leases),
                None
            );
        }
        leases.reverse();
        let decision = policy
            .observe(generation, 3, 60_000, &workers, &leases)
            .unwrap();
        assert_eq!(
            decision,
            ElasticRebalanceDecision {
                generation,
                frontier: 3,
                shard_id: ShardId(1),
                donor: WorkerId(1),
                donor_lease_token: LeaseToken(1),
                recipient: WorkerId(3),
                donor_shards: 2,
                recipient_shards: 0,
                headroom_gap: 0.9 - 0.4,
            }
        );
        assert_eq!(decision.operation_id(), "auto-rebalance-1-1-3");
        assert_eq!(
            policy.observe(generation, 3, 60_000, &workers, &leases),
            None
        );
    }

    #[test]
    fn elastic_policy_resets_on_membership_change_and_respects_residency() {
        let mut policy = ElasticRebalancePolicy::new(ElasticRebalanceConfig::default());
        let workers = [rebalance_worker(1, 0.4, 1), rebalance_worker(2, 0.9, 1)];
        let leases = [rebalance_lease(1, 1, 10), rebalance_lease(2, 1, 10)];
        for frontier in 1..=3 {
            assert_eq!(
                policy.observe(FrontierGeneration(1), frontier, 60_000, &workers, &leases),
                None
            );
        }
        for frontier in 4..=5 {
            assert_eq!(
                policy.observe(FrontierGeneration(1), frontier, 60_010, &workers, &leases),
                None
            );
        }
        for frontier in 5..=6 {
            assert_eq!(
                policy.observe(FrontierGeneration(2), frontier, 60_010, &workers, &leases),
                None
            );
        }
        let decision = policy
            .observe(FrontierGeneration(2), 7, 60_010, &workers, &leases)
            .unwrap();
        assert_eq!(
            (decision.generation, decision.shard_id, decision.recipient),
            (FrontierGeneration(2), ShardId(1), WorkerId(2))
        );
    }

    #[test]
    fn transient_headroom_gap_cannot_move_a_balanced_shard() {
        let mut policy = ElasticRebalancePolicy::new(ElasticRebalanceConfig::default());
        let mut workers = [rebalance_worker(1, 0.2, 1), rebalance_worker(2, 0.9, 1)];
        let leases = [rebalance_lease(1, 1, 0), rebalance_lease(2, 2, 0)];
        let generation = FrontierGeneration(1);
        assert_eq!(
            policy.observe(generation, 1, 60_000, &workers, &leases),
            None
        );
        workers[0].capacity_headroom = CapacityHeadroom::new(0.9);
        assert_eq!(
            policy.observe(generation, 2, 60_000, &workers, &leases),
            None
        );
        workers[0].capacity_headroom = CapacityHeadroom::new(0.2);
        for frontier in 3..=4 {
            assert_eq!(
                policy.observe(generation, frontier, 60_000, &workers, &leases),
                None
            );
        }
        let decision = policy
            .observe(generation, 5, 60_000, &workers, &leases)
            .unwrap();
        assert_eq!(
            (decision.shard_id, decision.recipient),
            (ShardId(1), WorkerId(2))
        );
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
        assert_eq!(
            assignments
                .iter()
                .map(|a| a.worker_id())
                .collect::<Vec<_>>(),
            vec![WorkerId(1), WorkerId(2), WorkerId(1)]
        );
    }

    #[test]
    fn initial_placement_counts_existing_leases_and_never_double_assigns_shards() {
        let sched = make_scheduler();
        register(&sched, 1, 0.9);
        register(&sched, 2, 0.3);
        let previous = sched.manager.acquire(ShardId(99), WorkerId(1)).unwrap();

        let assignments = sched
            .assign_initial_shards(&[ShardId(1), ShardId(2), ShardId(99)])
            .unwrap();
        assert_eq!(
            assignments
                .iter()
                .map(|assignment| (assignment.lease.shard_id, assignment.worker_id()))
                .collect::<Vec<_>>(),
            vec![(ShardId(1), WorkerId(2)), (ShardId(2), WorkerId(1)),]
        );
        // Do not disturb a healthy owner's existing fencing token.
        assert_eq!(
            sched.manager.get(ShardId(99)).unwrap().lease_token,
            previous.lease_token
        );
        for id in [ShardId(1), ShardId(2), ShardId(99)] {
            assert!(sched.manager.get(id).is_some());
        }
    }

    #[test]
    fn mixed_versions_choose_a_compatible_worker() {
        let sched = make_scheduler();
        let v1_worker = WorkerRegistration::new(
            WorkerId(1),
            NodeRole::Worker,
            "127.0.0.1:8001",
            CapacityHeadroom::new(0.3),
        )
        .with_compatibility(
            SupportedVersionRange::v1_only(),
            SupportedStorageFormatRange::v1_only(),
        );
        sched.catalog.register(&v1_worker);
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
        let heat = LeaseHeatHint {
            hot_key_prefixes: vec![b"customer-42".to_vec()],
            hot_sst_ids: vec!["sst-7".to_string()],
            hot_block_ranges: vec![(10, 20)],
            expected_read_rate: 100,
        };
        assert!(sched.manager.update_heat(ShardId(1), heat.clone()));

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
        assert_eq!(
            reassignments
                .iter()
                .find(|a| a.lease.shard_id == ShardId(1))
                .unwrap()
                .lease
                .heat_hint,
            heat
        );

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
    fn worker_loss_spreads_recovered_shards_across_two_healthy_survivors() {
        let sched = make_scheduler();
        register(&sched, 1, 0.9);
        register(&sched, 2, 0.8);
        register(&sched, 3, 0.7);
        let failed = [ShardId(10), ShardId(11), ShardId(12), ShardId(13)];
        for shard in failed {
            sched.manager.acquire(shard, WorkerId(1)).unwrap();
        }
        let result = sched.on_worker_dead(WorkerId(1)).unwrap();
        assert_eq!(result.len(), 4);
        let mut recovered_shards: Vec<_> =
            result.iter().map(|item| item.lease.shard_id.0).collect();
        recovered_shards.sort_unstable();
        assert_eq!(recovered_shards, vec![10, 11, 12, 13]);
        let worker_two = result
            .iter()
            .filter(|item| item.worker_id() == WorkerId(2))
            .count();
        let worker_three = result
            .iter()
            .filter(|item| item.worker_id() == WorkerId(3))
            .count();
        assert_eq!((worker_two, worker_three), (2, 2));
        assert!(result.iter().all(|item| item.evicted == Some(WorkerId(1))));
        for item in &result {
            assert!(sched
                .manager
                .is_valid_writer(item.lease.shard_id, item.lease.lease_token));
        }
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

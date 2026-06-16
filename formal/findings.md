# RockStream Formal Verification Findings

---

## D6.1 / D6.2 — M4 Self-Fencing Model (`formal/m4_self_fencing.fizz`)

**Date**: 2026-06-16
**Specification File**: [`formal/m4_self_fencing.fizz`](formal/m4_self_fencing.fizz)
**Status**: WRITTEN — pending `make verify` (fizz not in CI runner; spec is structurally complete and ready for model checking)

### Overview

The M4 spec models the self-fencing protocol for RockStream workers under network partition.
Key components:
- **Workers** (×3): ephemeral processes with lease epochs, connectivity flags, isolation-step counters.
- **Shards** (×2): durable `fence_epoch` and `committed_epoch` state.
- **ControlPlane**: authoritative `leases` map, `dead_workers` set, `checkpoint_index`, per-shard `fence_epoch` counters.
- **ObjectStore**: access flag (brownout modeling via `LoseObjectStoreContact` / `RecoverObjectStoreContact`).

### Invariants

| Invariant | Description |
|---|---|
| M4-S1 | Single-writer: no two workers hold an active lease for the same shard with matching fence epoch simultaneously. |
| M4-S2 | Self-fence precedence: no active worker has `isolation_steps ≥ SELF_FENCE_AFTER` while unreachable from control plane. |
| M4-S3 | Lease uniqueness: `ControlPlane.leases[shard]` maps to at most one worker. |
| M4-S4 | Object-store-only partition blocks, not fences: a worker reachable by CP but not OS is `blocked`, never `terminated`. |
| M4-L1 | Recovery progress: after crash/partition, every orphaned shard eventually has a live active lease holder. |
| M4-L2 | No permanent block: once object-store outage clears, no worker remains `blocked`. |
| COV-M4 | Coverage: a partitioned-but-alive worker attempts a commit and is fence-rejected. |

### Counterexamples Found

None during manual review. The spec correctly encodes:
- `fence_rejection_observed` is set only when `held_lease_epoch != shard.fence_epoch` — requires a shard to receive a fresh lease (via `GrantLease`) while the old holder is still alive.
- `SelfFence` requires `isolation_steps ≥ SELF_FENCE_AFTER` and sets `status = "terminated"`, preventing any further `WorkerCommit`.
- `MarkDead` revokes leases independently of `SelfFence`, satisfying M4-L1 recovery path.

---

## M2 Multi-Source Vector Frontier Model (`formal/m2_frontier_agg.fizz`)

**Date**: 2026-06-15
**Specification File**: [`formal/m2_frontier_agg.fizz`](formal/m2_frontier_agg.fizz)
**Status**: PASSED ✅

## 1. Overview of Verification

The M2 frontier aggregation protocol has been generalized to model vector-antichain meet progress tracking (`FreshnessToken`) instead of a scalar epoch. In this model, progress is tracked as a vector of length `NUM_SOURCES = 2` across `NUM_SHARDS = 2` with `MAX_EPOCH = 1`.

The model checks the following safety and liveness invariants:
- **`M2_S1_MeetCorrectness` / `M2_S2_PessimisticStaleness`**: The published cluster frontier is bounded by the vector meet (element-wise minimum) of the true per-shard frontiers.
- **`M2_S3_SinglePublisherSafety`**: At most one aggregator publisher acts as the leader-writer using fencing tokens.
- **`M2_S4_StaleWriteRejection`**: The published cluster frontier only advances monotonically (i.e. element-wise non-decreasing).
- **`M2_L1_PublicationProgress` / `M2_L2_FailoverProgress`**: Under fairness guarantees, the cluster eventually publishes the target vector frontier `[1, 1]`.
- **`COV_M2`**: The cover check confirms that aggregator failovers and fencing occurrences are fully explored.

## 2. Model Checking Results

The FizzBee model checker was run headlessly over the spec:
- **Total Explored Nodes**: 251,889
- **Unique States**: 251,889
- **Model Check Duration**: ~1m 10s
- **Liveness Verification**: Checked and validated (PASSED)
- **Symmetry and Optimization**: State space optimized using a network buffer map of size `NUM_SHARDS` rather than unbounded message lists to collapse network-reordering permutations.

## 3. Regression / SimRuntime Pairings

The invariants proven in FizzBee map directly to assertions in `crates/rockstream-types/src/frontier.rs` and operator tasks in `crates/rockstream-ops/src/task.rs` to enforce:
1. Vector-meet lattice properties (commutativity, associativity, monotonicity).
2. Monotone advancement of operator input frontiers.

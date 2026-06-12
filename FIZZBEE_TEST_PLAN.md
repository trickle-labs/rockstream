# RockStream Formal Verification Plan (FizzBee)

**Status**: v1.0 — authoritative MDD specification for formal verification of
RockStream's distributed coordination protocols.

**Scope**: This document defines *what* must be formally model-checked in
[FizzBee](https://fizzbee.io) before the corresponding Rust implementation is
written, *how* RockStream's architecture maps onto FizzBee's role/channel/action
model, *which* safety and liveness invariants must hold, and *when* each
specification must pass relative to the phase gates in
[NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md).

FizzBee is a model checker with a Starlark (Python-dialect) specification
language. It is used here as a **design-time correctness oracle** that sits one
level above the Rust `SimRuntime` deterministic-simulation harness
([DESIGN.md §17](DESIGN.md)). The two are complementary:

| Layer | Tool | Verifies | Granularity |
|---|---|---|---|
| Design (this doc) | FizzBee | Protocol *logic* is correct under all interleavings and faults | Abstract state machine |
| Implementation ([DESIGN.md §17](DESIGN.md)) | `SimRuntime` + `buggify!()` | The Rust *code* faithfully implements the verified protocol | Byte-level state, real encodings |

The contract between the two layers is explicit: **every invariant proved in
FizzBee (this document) becomes a paired `assert!` in the Rust implementation**
(the "TigerBeetle-Style Assertion Discipline" of [DESIGN.md §17.3](DESIGN.md)).
FizzBee proves the invariant is *true of the design*; the runtime assertion
proves the *code never violates it*. A FizzBee model without a corresponding
runtime assertion pair is incomplete, and vice versa.

> **Guiding rule.** FizzBee is not a substitute for `SimRuntime`. It is the
> formal justification for the invariants `SimRuntime` checks. We model the
> *coordination skeleton* in FizzBee (epochs, frontiers, leases, 2PC), never the
> data-plane Z-set algebra (that is the oracle's job, per
> [NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Testing Strategy).

---

## Table of Contents

1. [Verification Scope & Strategy](#1-verification-scope--strategy)
2. [FizzBee Model Architecture](#2-fizzbee-model-architecture)
3. [Invariant Assertions](#3-invariant-assertions)
4. [Phased Implementation Roadmap](#4-phased-implementation-roadmap)
5. [Appendix A: Spec File Inventory](#appendix-a-spec-file-inventory)
6. [Appendix B: FizzBee Construct Reference](#appendix-b-fizzbee-construct-reference)

---

## 1. Verification Scope & Strategy

### 1.1 What Is In Scope

Formal modeling targets **distributed state transitions** only — the points
where correctness depends on the interleaving of independent processes,
partial failure, and message reordering. Four protocols meet this bar and are
mandatory verification targets.

#### M1 — CALM Epoch-Commit Protocol & Per-Shard `WriteBatch` Atomicity

References: [DESIGN.md §8.4](DESIGN.md), [§9](DESIGN.md),
[NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 1, Phase 4.

The model abstracts the per-shard atomic commit
([DESIGN.md §9](DESIGN.md): all `op_state`, `view_output`, `shuffle_outbox`,
`shuffle_inbox`, and `shard_meta/0x06 0xFR` frontier mutations land in one
SlateDB `WriteBatch`) and the CALM invariant
([DESIGN.md §8.4](DESIGN.md): an epoch *N* is globally committed iff every
contributing shard's persisted frontier satisfies `frontier ≥ N`, verifiable by
any reader of the object store with no coordinator).

Verification targets:
- `WriteBatch` is **all-or-nothing**: a crash mid-commit leaves the shard at
  epoch `N-1`, never a torn state where the frontier advanced but state did not
  (or vice versa).
- Epoch frontiers are **monotone per shard**: a recovered writer never publishes
  a frontier below the last durably committed one ([DESIGN.md §9.2](DESIGN.md)).
- The **cluster-committed epoch is a monotone predicate** (CALM): once globally
  committed it never retreats, and the predicate is decidable from per-shard
  persisted frontiers alone.
- **Idempotent replay**: re-running the same epoch's `WriteBatch` after crash is
  a no-op because every write is keyed by `(epoch, op_id, port, seq)`.

#### M2 — Asynchronous Frontier Aggregation Protocol

References: [DESIGN.md §3.2](DESIGN.md), [§8.3–§8.6](DESIGN.md),
[NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 5.

The model abstracts per-shard frontier reporting
([DESIGN.md §8.3](DESIGN.md)), worker-local hierarchical meet
([DESIGN.md §8.6](DESIGN.md)), the elected-publisher lease among
frontier-aggregator processes ([DESIGN.md §3.2](DESIGN.md)), and the published
cluster vector frontier.

Verification targets:
- **Meet correctness under reordering**: arbitrary reorderings/delays of
  per-shard frontier reports converge to the same cluster vector frontier as
  serial in-order delivery (associativity + commutativity of the antichain
  meet, exploited for hierarchical aggregation).
- **Pessimistic staleness only**: the published aggregate is always `≤` the true
  cluster-committed frontier; it may lag but never overshoots (no GC of state a
  reader could still observe).
- **Single-publisher safety**: the lease-based leader election
  ([DESIGN.md §3.2](DESIGN.md)) guarantees at most one process writes the
  committed-frontier key at any time, even across lease expiry + failover, using
  fencing-token semantics — **not** Raft.
- **Synchronous-flush ordering**: a new leader never reads a stale
  committed-frontier value that a fenced predecessor failed to flush
  ([DESIGN.md §3.2](DESIGN.md): `sync: true` requirement).

#### M3 — Two-Phase Commit for Exactly-Once Sinks

References: [DESIGN.md §11.4](DESIGN.md),
[NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 6.

The model abstracts the sink 2PC (`pre_commit(epoch, rows)` →
`commit(epoch, checkpoint_id)`), the `sink_state/` durable marker, and the
three `SinkIdempotencyProfile` recovery dispatch paths
([DESIGN.md §11.4](DESIGN.md): `NativeIdempotent`, `FencingTokenRequired`,
`CheckBeforeCommit`).

Verification targets:
- **Exactly-once delivery**: for every committed cluster checkpoint, each output
  epoch is delivered to the external system *exactly once* across any crash
  interleaving (no loss, no duplicate).
- **Recovery dispatch soundness**: each idempotency profile's recovery path
  (`CheckBeforeCommit` query-then-act, `FencingTokenRequired` token check,
  `NativeIdempotent` direct re-commit) is idempotent at every crash point
  (before `pre_commit`, between `pre_commit` and `commit`, during `commit`).
- **Checkpoint coupling**: a sink epoch is committed *only after* its cluster
  checkpoint is durably committed, so a checkpoint rollback can never strand a
  delivered-but-unrecorded output.

#### M4 — Worker Self-Fencing & Lease Transitions Under Partition

References: [DESIGN.md §10.4](DESIGN.md), [§11.5](DESIGN.md),
[§11.6](DESIGN.md), [NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md)
Phase 6.

The model abstracts the shard writer lease, the control-plane failure detector
(`dead_after`), worker self-fencing (`self_fence_after`,
[DESIGN.md §11.6](DESIGN.md)), SlateDB manifest fence-epoch enforcement
([DESIGN.md §9.2](DESIGN.md)), and fault-driven reassignment
([DESIGN.md §10.4](DESIGN.md)).

Verification targets:
- **Single-writer safety (no split-brain)**: at no reachable state do two
  workers both successfully commit to the same shard. This must hold even when a
  partitioned-but-alive worker races the new owner — the SlateDB fence epoch is
  the backstop, self-fencing is the optimization.
- **Self-fence timing correctness**: the inequality
  `dead_after < self_fence_after < 2 × shard_recovery_budget`
  ([DESIGN.md §11.6](DESIGN.md)) is a sufficient condition for the partitioned
  worker to stop committing before the new owner acquires the lease — modeled as
  a logical-step ordering, not wall-clock.
- **Recovery liveness**: after any single-worker partition or crash, some worker
  eventually re-acquires every orphaned shard's lease and resumes committing
  epochs (the [DESIGN.md §11.5](DESIGN.md) recovery-time invariant, modeled as
  eventual progress, not a wall-clock bound).

### 1.2 What Is Explicitly Out of Scope

The following are deliberately **not** modeled in FizzBee. They are either
data-plane concerns covered by the oracle, or implementation concerns covered by
`SimRuntime`, and modeling them in FizzBee would add state-space cost without
verifying coordination logic.

| Out of scope | Covered instead by |
|---|---|
| Z-set algebra / `incremental == batch` equivalence | `rockstream-oracle` + DataFusion ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 1–3) |
| Operator semantics (MIN/MAX, joins, windows, Top-K) | Oracle property tests |
| Arrangement byte encodings, key schemes | `SimRuntime` paired assertions ([DESIGN.md §17.3](DESIGN.md)) |
| SlateDB internal LSM/compaction behavior | SlateDB determinism gate ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 0) |
| pgwire protocol, SQL compilation, planner | Integration tests (Phase 7) |
| Performance / throughput / latency budgets | `criterion` benchmarks, real-object-store integration |
| Recursion fixed-point convergence (inner-time) | Deferred — model only if Phase 4 distributed recursion destabilizes |
| Cold-tier Iceberg snapshot lifecycle | Deferred to post-1.0 (out of the two-pillar scope) |

**Rationale.** FizzBee's value is exhaustive exploration of *interleavings and
faults* over a small abstract state machine. Data-plane correctness is a
function of single-threaded algebra, which exhaustive interleaving exploration
does not illuminate. Keeping the data plane out of the FizzBee models is what
keeps the state space tractable (Section 2.3).

### 1.3 Strategy Principles

1. **Model the skeleton, abstract the payload.** Epochs are integers; Z-set
   deltas are opaque counts or omitted entirely. State variables track only what
   the safety/liveness predicates read.
2. **One protocol per spec file**, composed only where a property genuinely
   spans two protocols (e.g. M3 depends on M1's committed epoch). Composition is
   explicit and minimal.
3. **Faults are implicit first.** Lean on FizzBee's automatic injection (message
   loss, partition, thread crash, process crash) before hand-modeling anything.
   Only message *duplication*, *disk corruption*, and *Byzantine* faults require
   explicit modeling (FizzBee does not auto-inject these), and of those only
   duplication is in scope (it directly tests M3 exactly-once and M1
   idempotency).
4. **Every FizzBee invariant maps to a Rust assertion.** The mapping table in
   Section 3.6 is a delivery artifact, not documentation.
5. **A failing seed is a permanent regression.** A FizzBee counterexample for a
   protocol is translated into a named `SimRuntime` regression seed
   ([DESIGN.md §17.6](DESIGN.md)) before the model is fixed.

---

## 2. FizzBee Model Architecture

### 2.1 Role Inventory

RockStream's topology ([DESIGN.md §3](DESIGN.md)) maps directly onto FizzBee
`role` instances. Roles are the unit of fairness scoping
([FizzBee: fairness is per-role-instance](https://fizzbee.io/design/tutorials/liveness/)),
which matches RockStream's per-process failure model.

| FizzBee `role` | RockStream component | Models | Used by |
|---|---|---|---|
| `Worker` | Worker process ([DESIGN.md §3](DESIGN.md)) | Owns shard leases; runs operator tasks; commits epochs; self-fences. | M1, M4 |
| `Shard` | One SlateDB instance ([DESIGN.md §5.1](DESIGN.md)) | Durable per-shard state: `committed_epoch`, `frontier`, `fence_epoch`. | M1, M2, M4 |
| `ControlPlane` | Control plane ([DESIGN.md §3](DESIGN.md)) | Failure detector, lease grants, shard map, checkpoint index. | M2, M3, M4 |
| `FrontierAggregator` | Frontier role ([DESIGN.md §3.2](DESIGN.md)) | Per-worker meet, lease-based publisher election, committed-frontier write. | M2 |
| `ObjectStore` | Shared object storage ([DESIGN.md §4 P4](DESIGN.md)) | Durable substrate; the CALM verifiability surface; sink `_pending/` staging. | M1, M2, M3 |
| `SinkConnector` | Sink connector ([DESIGN.md §11.4](DESIGN.md)) | 2PC `pre_commit`/`commit`/`abort`; `sink_state/` marker; idempotency profile. | M3 |
| `ExternalSystem` | Kafka/S3/Postgres sink target | Records delivered outputs; models broker-side transaction timeout (duplication / abort). | M3 |
| `CheckpointCoordinator` | Cluster checkpoint coordinator ([DESIGN.md §11.2](DESIGN.md)) | Barrier injection, alignment, atomic cluster-checkpoint commit. | M3 (composed with M1) |

**Role parameterization** uses FizzBee named constructor parameters
(`Worker(NAME=i)`) and the implicit `self.__id__`. `Shard` instances carry a
`SHARD_ID`; `Worker` instances carry a `WORKER_ID`. Symmetric roles
([FizzBee symmetry reduction](https://fizzbee.io/design/tutorials/symmetry_reduction/))
are declared where instances are interchangeable (multiple identical `Shard`s,
multiple identical `FrontierAggregator`s) to collapse the state space.

### 2.2 Durability Annotations

Crash semantics are modeled with FizzBee's `@state(ephemeral=[...])` /
`@state(durable=[...])` annotations
([FizzBee fault injection](https://fizzbee.io/design/tutorials/fault-injection/)).
This is the single most important modeling decision because it determines what a
process-crash fault destroys.

| Role | Durable state | Ephemeral state | Justification |
|---|---|---|---|
| `Shard` | `committed_epoch`, `frontier`, `fence_epoch`, `sink_state` | (none — SlateDB is the durability boundary) | A shard's state survives worker crash; it lives in object storage. |
| `Worker` | `worker_id` | `held_leases`, `current_epoch_buffer`, `can_reach_control` | In-flight epoch work and lease cache are lost on crash; the worker re-registers fresh ([DESIGN.md §11.6](DESIGN.md)). |
| `ControlPlane` | `shard_map`, `leases`, `checkpoint_index`, `dead_workers` | (none in Tier-3 single-writer abstraction) | Control SlateDB is durable ([DESIGN.md §5.2](DESIGN.md)). |
| `FrontierAggregator` | (none) | `local_meet`, `is_publisher`, `lease_token` | Stateless by design ([DESIGN.md §3.2](DESIGN.md)); loss only delays publication. |
| `ObjectStore` | everything | (none) | The universal durable substrate. |
| `SinkConnector` | `pending_handle` (mirrored in `Shard.sink_state`) | `in_flight_tx` | Pending position is checkpointed; the live transaction handle is ephemeral. |
| `ExternalSystem` | `delivered_epochs`, `tx_status` | (none) | The external system is the ground truth for exactly-once. |

The `Shard`-as-durable / `Worker`-as-ephemeral split is what makes M4's
self-fencing model honest: a crashed worker loses `held_leases` and
`can_reach_control`, forcing it through re-registration, while the shard's
`fence_epoch` persists and rejects the stale writer.

### 2.3 Modeling Asynchronous Message Passing Without State Explosion

This is the central architectural concern. FizzBee offers two mechanisms; we use
both, chosen per protocol to minimize the state space.

#### Mechanism A — Channels (default, preferred)

FizzBee [channels](https://fizzbee.io/design/tutorials/channels/) model message
passing as ordinary function calls with configurable delivery semantics. We
declare typed channels per protocol edge:

| Channel | `ordering` | `delivery` | `blocking` | Rationale |
|---|---|---|---|---|
| Frontier report (`Shard → FrontierAggregator`) | `unordered` | `atmost_once` | `fire_and_forget` | Reports are idempotent, last-writer-wins; loss only delays GC ([DESIGN.md §8.4](DESIGN.md)). |
| Lease grant (`ControlPlane → Worker`) | `pairwise` | `atmost_once` | `blocking` | Grant must be acknowledged; loss is retried. |
| Sink pre-commit (`SinkConnector → ExternalSystem`) | `ordered` | `atmost_once` | `blocking` | Order matters; the 2PC handles loss. |
| Committed-frontier write (`FrontierAggregator → ObjectStore`) | `ordered` | `exactly_once` after `sync:true` | `blocking` | Models the synchronous-flush guarantee ([DESIGN.md §3.2](DESIGN.md)). |

When the caller context is **non-atomic** (a `serial`/multi-step action),
FizzBee automatically assumes RPC semantics — the message may be dropped after
send, after receive, or after the response is generated
([FizzBee channels](https://fizzbee.io/design/tutorials/channels/)). This single
property gives us *crash-or-drop at every RPC boundary for free*, which is
exactly the M3/M4 fault surface.

#### Mechanism B — Explicit message-set state (for reordering-sensitive proofs)

For M2's "meet under arbitrary reordering" proof, we do **not** want the channel
abstraction to impose any ordering. Instead the `FrontierAggregator` holds a
`set` of received reports and an action that processes *any* element
non-deterministically:

- The set captures "all messages currently in flight," and FizzBee explores
  every processing order.
- Because the antichain meet is associative and commutative, the *value* state
  collapses identical multisets regardless of arrival order — the state space is
  bounded by the set of distinct report multisets, not by `n!` orderings.

This is the explicit-collections approach from
[FizzBee msg-delivery-guarantees](https://fizzbee.io/design/tutorials/msg-delivery-guarantees/),
chosen precisely because it lets the model checker exploit value-equivalence to
avoid combinatorial blow-up.

#### State-Space Control Tactics (mandatory)

To keep every model checkable in CI, each spec applies the following budget:

1. **Small finite bounds.** `NUM_WORKERS ≤ 3`, `NUM_SHARDS ≤ 3`,
   `NUM_AGGREGATORS ≤ 2`, `MAX_EPOCH ≤ 3`, `MAX_CHECKPOINT ≤ 2`. These suffice to
   exhibit every interleaving class (two-writer race needs 2 workers; meet
   reordering needs 2 shards; epoch monotonicity needs 3 epochs).
2. **Symmetry reduction.** Interchangeable `Shard`/`Aggregator` instances are
   declared symmetric so permutations collapse to one representative.
3. **Bound the actor count.** `options.max_actions` and
   `options.max_concurrent_actions` in the frontmatter cap exploration depth
   ([FizzBee config](https://fizzbee.io/design/tutorials/frontmatter/)); each
   spec documents the minimal values that still cover the target counterexample
   class.
4. **Abstract the payload.** Epochs/checkpoints are small integers; row data is
   never represented. Frontiers are modeled as scalar `source_epoch` integers
   for single-source models and as fixed-length integer vectors only where a
   protocol genuinely needs the antichain (M2 multi-source meet).
5. **`atomic` the non-interesting steps.** Any sub-step whose interleaving
   cannot affect a safety/liveness predicate is wrapped in `atomic` so FizzBee
   does not insert a yield point there. Yield points are reserved for the exact
   boundaries the fault model cares about (durable write, network send, lease
   check).

### 2.4 Representing `SimObjectStore` and `SimNetwork` Fault Models

The Phase 0 fault models ([DESIGN.md §17.4](DESIGN.md),
[§17.8](DESIGN.md)) are reproduced in FizzBee so that the *design-level* fault
coverage matches the *implementation-level* `SimRuntime` coverage one-to-one.
The mapping is direct because FizzBee injects most of these implicitly.

| `SimRuntime` fault ([DESIGN.md §17.4/§17.8](DESIGN.md)) | FizzBee mechanism | Notes |
|---|---|---|
| Network: delay | Implicit (non-atomic action yield) | Any non-atomic RPC may be arbitrarily delayed. |
| Network: drop | Implicit (`atmost_once` channel / RPC semantics) | Auto-injected message loss. |
| Network: reorder | `unordered` channel **or** explicit message-set (Mechanism B) | Choose per Section 2.3. |
| Network: partition | Implicit | A partition is sustained message loss between a node subset ([FizzBee fault injection](https://fizzbee.io/design/tutorials/fault-injection/)); also modeled explicitly via a `Worker.can_reach_control` flag for M4 self-fencing logic. |
| Network: duplicate | **Explicit** | FizzBee does *not* auto-inject duplication. Modeled with an `atleast_once` channel or a re-send action. Required for M3 exactly-once and M1 idempotency. |
| ObjectStore: delayed visibility | `fire_and_forget` channel + separate "visible" action | Write enqueued, visibility is a later step. |
| ObjectStore: transient error | `oneof` branch returning failure | Caller retries; tests idempotency. |
| ObjectStore: LIST throttling / staleness | Explicit stale-read action on `ObjectStore` | Models [DESIGN.md §17.8](DESIGN.md) gap; relevant to CALM manifest verifiability (M1). |
| ObjectStore: checksum/corruption | **Explicit** (out of scope unless escalated) | Maps to "crash the worker, recover by reassignment" — only modeled if M1 needs it. |
| Process: crash before/during/after durable write | Implicit (thread/process crash at yield points) | The core M1/M3/M4 fault. |
| Process: restart with old manifest | `Worker` re-init reading `Shard`/`ObjectStore` durable state | Stale lease cache is ephemeral, so re-read is forced. |
| Process: fenced writer commit attempt | Explicit `Shard.fence_epoch` check on every commit | The M4 split-brain backstop. |
| Clock/scheduling: delayed timers, reordered wakeups | Implicit (action scheduling non-determinism) | FizzBee explores all action orders. |
| Connector: duplicate batch after retry | Explicit re-send action | M3. |
| Connector: sink commit retry | Implicit (RPC retry loop) + `oneof` failure | M3. |

**`SimObjectStore` role contract.** The `ObjectStore` role exposes
`put(key, value)`, `get(key)`, and `cas(key, expected, value)` (conditional
write, [DESIGN.md §17.8](DESIGN.md): S3 conditional writes are modeled). The
CAS is the formal primitive behind both the SlateDB manifest fence
([DESIGN.md §9.2](DESIGN.md)) and the frontier-publisher lease
([DESIGN.md §3.2](DESIGN.md)). LIST is modeled as a `get` over a key-prefix set
with an optional `oneof` stale-result branch to reproduce the §17.8 gap.

**`SimNetwork` fidelity boundary.** The FizzBee model inherits the same
fidelity caveats as `SimRuntime` ([DESIGN.md §17.8](DESIGN.md)): partial object
writes, Kafka broker-side transaction timeout, and packet fragmentation are
*not* faithfully modeled at the abstract level and remain integration-test
obligations. The FizzBee model documents these as assumptions so a passing
model is never mistaken for a fragmentation/partial-write proof. Where
[DESIGN.md §17.8](DESIGN.md) lists an `[UNMITIGATED]` gap (e.g. Kafka tx
timeout, partial writes), the corresponding FizzBee spec carries a matching
`# ASSUMPTION:` comment so the boundary is explicit on both layers.

---

## 3. Invariant Assertions

Each invariant is stated as a FizzBee predicate using `always` (safety),
`always eventually` (recurrence liveness), `eventually always` (stability), or
`exists` (state-space coverage) semantics
([FizzBee liveness](https://fizzbee.io/design/tutorials/liveness/)). Liveness
properties require `fair` / `fair<strong>` annotations on the actions that must
make progress, and several genuinely non-deterministic-liveness properties
require the `liveness: nondeterministic` model-checker mode
([FizzBee non-deterministic checker](https://fizzbee.io/design/tutorials/liveness/)).

The naming convention is `<Model>-<S|L><n>`: `S` = safety, `L` = liveness.

### 3.1 M1 — CALM Epoch-Commit & `WriteBatch` Atomicity

**Safety**

- **M1-S1 — `WriteBatch` atomicity.** `always`: for every `Shard`, the persisted
  `(committed_epoch, frontier, state_summary)` tuple is internally consistent —
  there is no reachable state where `frontier` reflects epoch `N` but
  `committed_epoch` is `N-1`, or vice versa. The torn intermediate is
  unobservable because the commit is one atomic step on `ObjectStore`.

- **M1-S2 — Per-shard frontier monotonicity.** `always`: a `Shard`'s
  `committed_epoch` never decreases across any action, including post-crash
  recovery re-init. Encodes [DESIGN.md §9.2](DESIGN.md).

- **M1-S3 — CALM cluster-commit monotonicity.** `always`: define
  `cluster_committed = min over contributing shards of committed_epoch`. Once
  `cluster_committed ≥ N` holds in any state, it holds in every successor state.
  Encodes the [DESIGN.md §8.4](DESIGN.md) CALM monotone predicate.

- **M1-S4 — CALM verifiability.** `always`: the value computed by an independent
  observer reading only `ObjectStore` per-shard frontier keys equals the
  `ControlPlane`'s view of `cluster_committed`. The aggregator/coordinator is
  never required to decide commit; the object store is sufficient
  ([DESIGN.md §8.4](DESIGN.md) consequence 1 and 3).

- **M1-S5 — Idempotent replay.** `always`: applying the same epoch's commit
  action twice (crash-replay) yields the same `Shard` state as applying it once.
  Modeled by issuing a duplicate commit and asserting state equality.

**Liveness**

- **M1-L1 — Epoch progress.** `always eventually`: under `fair` source and
  `fair` commit actions, every `Shard` eventually advances `committed_epoch`
  (the system does not deadlock at an epoch boundary). Requires fairness on the
  commit action; a quiet source is modeled with the `max_epoch_ms` ceiling
  abstraction so progress is guaranteed even with no data.

### 3.2 M2 — Asynchronous Frontier Aggregation

**Safety**

- **M2-S1 — Meet correctness (order independence).** `always`: the published
  cluster frontier equals the antichain meet of the latest per-shard frontiers,
  independent of report arrival order. Verified by exploring all reorderings
  (Mechanism B) and asserting the published value equals a serially-computed
  reference held in a ghost variable.

- **M2-S2 — Pessimistic staleness.** `always`:
  `published_frontier ≤ true_cluster_committed`. The aggregate never overshoots;
  it may lag ([DESIGN.md §8.4](DESIGN.md) consequence 3). This is the safety
  backstop that makes stale frontiers safe for GC and window-close.

- **M2-S3 — Single-publisher safety.** `always`: at most one
  `FrontierAggregator` has `is_publisher == True` *and* a valid (unfenced)
  `lease_token` at any state. Even across lease expiry and re-acquisition, two
  processes never both hold a writable lease. Modeled via `ObjectStore.cas` on
  the `frontier/leader` key with a fencing token
  ([DESIGN.md §3.2](DESIGN.md)).

- **M2-S4 — Stale-write rejection (synchronous flush).** `always`: a
  newly-elected publisher never publishes a committed-frontier value lower than
  one a fenced predecessor already durably flushed. Encodes the `sync: true`
  requirement ([DESIGN.md §3.2](DESIGN.md)); a fenced predecessor's un-flushed
  write is modeled as never having occurred, and a flushed write is always
  visible to the successor's read.

**Liveness**

- **M2-L1 — Publication progress.** `always eventually`: under a `fair<strong>`
  publish action and `fair` lease acquisition, the published cluster frontier
  eventually reflects the true cluster-committed epoch (staleness is bounded in
  the sense that it does not grow forever). Because leader election under
  partition is genuinely non-deterministic-liveness, this property is checked in
  `liveness: nondeterministic` mode
  ([FizzBee non-deterministic checker](https://fizzbee.io/design/tutorials/liveness/)).

- **M2-L2 — Failover progress.** `always eventually`: if the current publisher
  crashes or is partitioned, some follower eventually acquires the lease and
  resumes publishing. Requires `fair` acquisition attempts; the FLP boundary is
  handled by the non-deterministic checker.

### 3.3 M3 — 2PC Exactly-Once Sinks

**Safety**

- **M3-S1 — No duplicate delivery.** `always`: for every epoch, the
  `ExternalSystem.delivered_epochs` multiset contains each committed epoch *at
  most once*, across every crash interleaving and every duplicate-message
  injection.

- **M3-S2 — No lost delivery (conditional).** `always`: for every cluster
  checkpoint that is durably committed, every output epoch `≤` the checkpoint is
  *eventually* present in `delivered_epochs`. Stated as a safety invariant over
  the post-recovery terminal state (combined with M3-L1 for the progress half).

- **M3-S3 — Checkpoint-coupled commit.** `always`: a sink epoch transitions to
  `committed` only if its cluster checkpoint is already `committed` in
  `ControlPlane.checkpoint_index`. No output is externally visible before its
  checkpoint is durable.

- **M3-S4 — Recovery dispatch idempotency.** `always`: for each
  `SinkIdempotencyProfile`, replaying the recovery action from any crash point
  (pre-`pre_commit`, between, during `commit`) leaves `delivered_epochs`
  identical to a non-faulty run. Three sub-models, one per profile
  ([DESIGN.md §11.4](DESIGN.md)):
  - `NativeIdempotent`: direct re-commit is a no-op if already committed.
  - `FencingTokenRequired`: re-commit with the same token is a no-op.
  - `CheckBeforeCommit`: query-then-act observes committed-vs-aborted and
    re-runs `pre_commit → commit` only on broker abort, never double-delivering.

**Liveness**

- **M3-L1 — Delivery progress.** `always eventually`: under `fair` commit and
  `fair` recovery actions, every committed-checkpoint output epoch eventually
  reaches `delivered_epochs`. Pairs with M3-S2 to give exactly-once (S1 = at most
  once, L1 + S2 = at least once).

### 3.4 M4 — Self-Fencing & Lease Transitions

**Safety**

- **M4-S1 — Single-writer (no split-brain).** `always`: no two distinct
  `Worker`s ever both succeed a commit to the same `Shard`. The `Shard`'s
  `fence_epoch` rejects any writer whose acquired fence is below the current one
  ([DESIGN.md §9.2](DESIGN.md)). This invariant must survive the
  partitioned-but-alive worker racing the new owner.

- **M4-S2 — Self-fence precedence.** `always`: if a `Worker` has
  `can_reach_control == False` for the modeled `self_fence_after` step count, it
  transitions to `terminated` and performs no further commits. Encodes
  [DESIGN.md §11.6](DESIGN.md). The timing inequality
  `dead_after < self_fence_after < 2 × shard_recovery_budget` is encoded as a
  logical ordering of the failure-detector mark and the self-fence step, and the
  invariant asserts the partitioned worker self-fences no later than the new
  owner's first commit.

- **M4-S3 — Lease uniqueness.** `always`: each `Shard` has at most one
  `Worker` holding its lease in `ControlPlane.leases` at any state.

- **M4-S4 — Object-store-only partition blocks, not fences.** `always`: a
  `Worker` that can reach `ControlPlane` but not `ObjectStore` enters `BLOCKED`
  and never commits, but is *not* marked dead (heartbeats still arrive). Encodes
  [DESIGN.md §11.6](DESIGN.md) object-store-only partition.

**Liveness**

- **M4-L1 — Recovery progress.** `always eventually`: after any single-worker
  crash or partition, every orphaned `Shard` is eventually re-leased to a live
  `Worker` that resumes committing epochs. Requires `fair` lease-grant and
  `fair<strong>` worker-register actions; checked in `liveness: nondeterministic`
  mode because failover timing is non-deterministic.

- **M4-L2 — No permanent block under transient outage.** `eventually always`:
  once the modeled object-store outage clears, a `BLOCKED` worker eventually
  resumes and remains able to commit (stability). Encodes
  [DESIGN.md §11.7](DESIGN.md) brownout recovery.

### 3.5 Cross-Cutting Coverage Assertions

To guard against vacuously-passing models (a model that never reaches the
interesting state trivially satisfies every `always`), each spec includes
`exists` coverage assertions
([FizzBee `exists`](https://fizzbee.io/design/tutorials/liveness/)):

- **COV-M1**: `exists` a state where a `Worker` crashes mid-commit and a
  different `Worker` recovers the shard.
- **COV-M2**: `exists` a state where two `FrontierAggregator`s contend for the
  publisher lease and one is fenced.
- **COV-M3**: `exists` a state where the sink crashes between `pre_commit` and
  `commit` for each idempotency profile.
- **COV-M4**: `exists` a state where a partitioned-but-alive worker attempts a
  commit and is rejected by the fence epoch.

A failing `exists` assertion means the fault is not being explored and the
corresponding `always` proofs are untrustworthy — treated as a build failure.

### 3.6 Invariant → Runtime Assertion Mapping (Delivery Artifact)

Per the TigerBeetle assertion discipline ([DESIGN.md §17.3](DESIGN.md)), every
FizzBee invariant has a corresponding Rust paired assertion. This table is a
**required deliverable** maintained alongside the specs; CI cross-checks that
every row has both a green FizzBee assertion and a present runtime `assert!`.

| FizzBee invariant | Rust paired assertion ([DESIGN.md §17.3](DESIGN.md)) | Crate |
|---|---|---|
| M1-S1, M1-S2 | Assert `committed_epoch` non-decreasing before `WriteBatch` build and after `ShardDb` decode. | `rockstream-storage` |
| M1-S3, M1-S4 | Assert object-store-derived `cluster_committed` equals control-plane view at each checkpoint. | `rockstream-runtime` |
| M1-S5 | Assert epoch-keyed replay produces byte-identical state (crash-replay test). | `rockstream-storage` |
| M2-S1, M2-S2 | Assert published frontier `≤` true frontier; assert meet associativity before publish and after read. | `rockstream-control` |
| M2-S3, M2-S4 | Assert single CAS-holder of `frontier/leader`; assert `sync:true` flush before lease-handoff read. | `rockstream-control` |
| M3-S1–S4 | Assert idempotency-key uniqueness before `prepare`; assert one external artifact per key after recovery. | `rockstream-connectors` |
| M4-S1–S3 | Assert fence-epoch rejection on stale write; assert single lease holder per shard. | `rockstream-runtime` |
| M4-S2 | Assert worker terminates before `self_fence_after` deadline when control unreachable. | `rockstream-runtime` |
| All liveness (M1-L1…M4-L2) | Liveness checked in `SimRuntime` as "a recoverable fault commits a new epoch within the recovery budget" ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 8 sim gate). | `rockstream-sim` |

---

## 4. Phased Implementation Roadmap

FizzBee modeling is sequenced to *precede* the Rust implementation of each
protocol, so the design is verified before code exists — the core value
proposition. Work aligns to the existing phase gates in
[NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md).

### 4.0 Tooling Prerequisite (during Phase 0)

Before any model is written, establish the verification toolchain as part of the
Phase 0 foundation ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md)
Phase 0):

- **D0.1** — Add a `formal/` directory at the workspace root for `.fizz` specs,
  a `fizz.yaml` config, and a `formal/README.md` indexing each spec to its
  DESIGN.md section and its M-id.
- **D0.2** — Pin a FizzBee version (container image or release binary) and add a
  `make verify` target that runs every spec headlessly and fails CI on any
  safety/liveness/`exists` violation.
- **D0.3** — Add a CI job `formal-verify` that runs `make verify` on every PR
  touching `formal/`, `DESIGN.md`, or any coordination crate
  (`rockstream-runtime`, `rockstream-control`, `rockstream-connectors`,
  `rockstream-storage`). Wire it to the same gate as `cargo test`.
- **D0.4** — Author `formal/conventions.md`: the role-naming, durability
  annotation, frontmatter-bounds, and `exists`-coverage conventions from
  Section 2, so every spec is uniform.

**Phase 0 exit add-on**: `make verify` runs in CI and is green (initially over a
trivial smoke-test spec proving the toolchain works), alongside the existing
SlateDB determinism gate.

### 4.1 Gate: M1 — Epoch-Commit (Phase 0 → Phase 1 boundary)

M1 is modeled at the *end of Phase 0 / start of Phase 1*, before the
`rockstream-runtime` epoch coordinator and `rockstream-storage` `WriteBatch`
builder are implemented ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md)
Phase 1, IVM-1 group commit).

Deliverables:
- **D1.1** — `formal/m1_epoch_commit.fizz`: `Worker`, `Shard`, `ObjectStore`,
  `ControlPlane` roles; `commit_epoch` as a non-atomic action with a crash-able
  yield point exactly at the `ObjectStore.put` boundary; duplicate-commit
  re-send action for idempotency.
- **D1.2** — Encode and pass safety M1-S1…M1-S5 and coverage COV-M1.
- **D1.3** — Encode and pass liveness M1-L1 under `fair` commit.
- **D1.4** — For any counterexample found, record it in `formal/findings.md`
  and translate it into a named `SimRuntime` regression seed stub
  ([DESIGN.md §17.6](DESIGN.md)) before fixing the model.
- **D1.5** — Populate the Section 3.6 mapping rows for M1 and confirm the
  matching Rust `assert!` sites are scheduled in the Phase 1 crash-replay exit
  criterion ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 1).

**Gate**: M1 spec green is a precondition for the Phase 1
crash-replay exit criterion ("`kill -9` mid-`WriteBatch` … output bit-identical
to an uninterrupted run"). The FizzBee model justifies that the protocol *can*
satisfy this; the Rust crash-replay test proves the code *does*.

### 4.2 Gate: M2 — Frontier Aggregation (Phase 5)

M2 is modeled at the *start of Phase 5*, before the control-plane frontier
aggregator and lease-based publisher are implemented
([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 5).

Deliverables:
- **D5.1** — `formal/m2_frontier_agg.fizz`: `Shard`, `FrontierAggregator`
  (symmetric, ×2), `ObjectStore`, `ControlPlane` roles; per-shard report via the
  explicit message-set mechanism (Section 2.3 B); publisher lease via
  `ObjectStore.cas` with a fencing token.
- **D5.2** — Encode and pass safety M2-S1…M2-S4 and coverage COV-M2.
- **D5.3** — Encode and pass liveness M2-L1, M2-L2 in `liveness:
  nondeterministic` mode with `fair<strong>` publish.
- **D5.4** — Add a multi-source antichain variant (fixed-length integer-vector
  frontier) proving the meet is correct for the vector `FreshnessToken`
  ([DESIGN.md §12.4](DESIGN.md)), not just scalar source-epochs.
- **D5.5** — Record findings; populate Section 3.6 M2 rows; align the runtime
  assertions with the Phase 5 frontier-aggregation stress test
  ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 5: "arbitrary
  frontier-report reorderings converge to the same cluster vector frontier").

**Gate**: M2 spec green is a precondition for the Phase 5 exit criteria
(multi-rate join correctness + bounded shuffle storage). The "reordering
converges" `SimRuntime` test ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md)
Phase 5) is the implementation mirror of M2-S1.

### 4.3 Gate: M3 + M4 — Fault Tolerance & Exactly-Once (Phase 6)

M3 and M4 are modeled at the *start of Phase 6*, before the cluster checkpoint
coordinator, recovery driver, 2PC sink protocol, and self-fencing are
implemented ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 6).
These are the highest-value models — Phase 6's entire premise is "survive any
single-node failure; exactly-once end-to-end," which is precisely what M3/M4
prove at the design level.

Deliverables — M4 (self-fencing) first, because M3 composes on top of a
correct single-writer guarantee:
- **D6.1** — `formal/m4_self_fencing.fizz`: `Worker` (×2–3), `Shard`,
  `ControlPlane`, `ObjectStore`; `Worker.can_reach_control` flag; failure
  detector `mark_dead` action; `self_fence` action gated on the
  `dead_after < self_fence_after` ordering; `Shard.fence_epoch` CAS on every
  commit.
- **D6.2** — Encode and pass M4-S1…M4-S4, M4-L1, M4-L2, COV-M4. M4-L1/L2 run in
  `liveness: nondeterministic` mode.
- **D6.3** — `formal/m3_sink_2pc.fizz`: `SinkConnector`, `ExternalSystem`,
  `Shard`, `CheckpointCoordinator`, `ControlPlane`, `ObjectStore`; three
  parameterized sub-models, one per `SinkIdempotencyProfile`; explicit
  duplicate-delivery injection; crash yield points before/between/during the 2PC
  steps.
- **D6.4** — Encode and pass M3-S1…M3-S4, M3-L1, COV-M3. Compose M3-S3 with M1's
  `cluster_committed` predicate (a sink commits only after the checkpoint is
  durable) — the single point where two models are composed.
- **D6.5** — Add a duplication-fault variant to the M1 model
  (`formal/m1_epoch_commit.fizz`) to confirm idempotent replay (M1-S5) holds
  under explicit message duplication, since duplication is not auto-injected.
- **D6.6** — Record findings; populate Section 3.6 M3/M4 rows; map each
  counterexample to a `BUGGIFY` site and a `SimRuntime` regression seed
  ([DESIGN.md §17.2](DESIGN.md), [§17.6](DESIGN.md)).

**Gate**: M3 and M4 specs green are preconditions for the Phase 6 exit criteria
(24-hour chaos: zero loss, zero duplicates; recovery within the 5 s/30 s/60 s
budgets). The mapping:

| Phase 6 `SimRuntime` obligation ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md)) | FizzBee model that justifies it |
|---|---|
| Every epoch-commit partial-failure permutation keeps the frontier monotonic and exactly-once intact | M1-S1…S5, M3-S1…S4 |
| 2PC sink crash points (pre-commit / between / commit) recover idempotently | M3-S4 (all three profiles) |
| Network-partition self-fencing: partitioned worker terminates before new owner commits | M4-S1, M4-S2 |
| Object-store brownout: 50-epoch blackout, zero loss/duplicates | M4-S4, M4-L2, M1-S5 |

### 4.4 Continuous Verification (Phase 6 onward)

Aligned with the [DESIGN.md §17.6](DESIGN.md) continuous-simulation soak and the
[NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 8 simulation CI
gate:

- **DC.1** — The `formal-verify` CI job runs the full spec suite on every PR
  touching coordination logic or `DESIGN.md`; a red model blocks merge.
- **DC.2** — Any change to a coordination protocol in `DESIGN.md` requires the
  corresponding `.fizz` model and the Section 3.6 mapping table to be updated in
  the same PR. CI fails if a coordination-crate change lands without a
  corresponding model touch (enforced by a path-coupling check, analogous to the
  [DESIGN.md §17.2](DESIGN.md) `buggify!()`-review rule).
- **DC.3** — Every FizzBee counterexample is archived in `formal/findings.md`
  with its trace and its paired `SimRuntime` regression seed, and is replayed on
  every build forever ([DESIGN.md §17.6](DESIGN.md)).
- **DC.4** — Pre-release gate: re-run all specs with relaxed bounds
  (`NUM_WORKERS=3`, `NUM_SHARDS=3`, `MAX_EPOCH=4`) to widen coverage beyond the
  CI-fast minimums, mirroring the "scale to millions of seeds pre-release"
  discipline ([NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) Phase 8).

### 4.5 Roadmap Summary

| RockStream phase | FizzBee deliverable | Models | Gate |
|---|---|---|---|
| Phase 0 | Toolchain, `formal/`, CI job, conventions | — | `make verify` green in CI |
| Phase 0→1 | `m1_epoch_commit.fizz` | M1 | Precondition for Phase 1 crash-replay exit |
| Phase 5 | `m2_frontier_agg.fizz` | M2 | Precondition for Phase 5 reordering/multi-rate exit |
| Phase 6 | `m4_self_fencing.fizz`, `m3_sink_2pc.fizz`, M1 duplication variant | M3, M4 | Precondition for Phase 6 chaos/exactly-once exit |
| Phase 6→8 | Continuous `formal-verify` + path-coupling | all | Pre-release relaxed-bounds sweep |

---

## Appendix A: Spec File Inventory

| File | Models | Roles | Primary invariants | DESIGN.md anchor |
|---|---|---|---|---|
| `formal/m1_epoch_commit.fizz` | M1 | `Worker`, `Shard`, `ObjectStore`, `ControlPlane` | M1-S1…S5, M1-L1 | §8.4, §9 |
| `formal/m2_frontier_agg.fizz` | M2 | `Shard`, `FrontierAggregator`, `ObjectStore`, `ControlPlane` | M2-S1…S4, M2-L1…L2 | §3.2, §8.3–§8.6 |
| `formal/m3_sink_2pc.fizz` | M3 | `SinkConnector`, `ExternalSystem`, `Shard`, `CheckpointCoordinator`, `ControlPlane`, `ObjectStore` | M3-S1…S4, M3-L1 | §11.2, §11.4 |
| `formal/m4_self_fencing.fizz` | M4 | `Worker`, `Shard`, `ControlPlane`, `ObjectStore` | M4-S1…S4, M4-L1…L2 | §10.4, §11.5, §11.6, §11.7 |
| `formal/conventions.md` | — | — | role/durability/bounds conventions | §2 of this doc |
| `formal/findings.md` | — | — | counterexample log + regression-seed map | §17.6 |

---

## Appendix B: FizzBee Construct Reference

Quick reference for the FizzBee constructs this plan relies on, with their
RockStream usage. (Sourced from the FizzBee design-verification tutorials.)

| Construct | Semantics | RockStream usage |
|---|---|---|
| `role X:` / `action Init:` | Stateful actor with initializer; state via `self`. | One role per topology component (Section 2.1). |
| `atomic` block/action | Single indivisible state transition (no yield). | Wrap non-interesting sub-steps to bound the state space (Section 2.3). |
| `serial` / non-atomic action | Multiple transitions with yield points; FizzBee injects crash/drop at each. | Model RPC + durable-write boundaries where faults matter. |
| `func` | Callable method on a role; message passing is a function call. | Cross-role messages (lease grant, frontier report, pre-commit). |
| `Channel(ordering=, delivery=, blocking=)` | Typed message channel. | Per-edge delivery semantics (Section 2.3 A). |
| `@state(ephemeral=/durable=)` | Marks role state lost vs. retained on process crash. | Worker-ephemeral / Shard-durable split (Section 2.2). |
| `always assertion` | Safety invariant over all reachable states. | All `*-S*` invariants (Section 3). |
| `always eventually assertion` | Recurrence liveness. | `*-L*` progress invariants. |
| `eventually always assertion` | Stability. | M4-L2 brownout-recovery stability. |
| `exists assertion` | At least one timeline reaches a state. | COV-M* coverage guards (Section 3.5). |
| `fair` / `fair<strong>` | Weak/strong action fairness (per role instance). | Liveness preconditions. |
| `fair any` | Fair non-deterministic choice. | Fair selection of shard/worker to make progress. |
| `liveness: nondeterministic` | Non-deterministic model-checker mode. | Leader-election / failover liveness (M2-L*, M4-L*). |
| `options.max_actions` / `max_concurrent_actions` | Exploration-depth bounds. | State-space budget per spec (Section 2.3). |
| Symmetry reduction | Collapse interchangeable role permutations. | Symmetric `Shard` / `FrontierAggregator` instances. |

> **Note on FizzBee maturity.** Channels are marked work-in-progress in the
> FizzBee docs. Where channel semantics are insufficient or unstable, specs fall
> back to the explicit-collections message-passing model
> ([FizzBee msg-delivery-guarantees](https://fizzbee.io/design/tutorials/msg-delivery-guarantees/)),
> which is the more established mechanism and is mandatory for M2's
> reordering proof regardless.

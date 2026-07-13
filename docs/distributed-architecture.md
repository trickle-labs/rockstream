# Distributed Architecture Guide

**Version**: v0.36.0  
**Phase boundary**: Distributed Alpha (v0.36)

This guide covers the core distributed systems concepts in RockStream: the
frontier protocol, checkpoint protocol, recovery budgets, shuffle transport,
and cluster bootstrap walkthrough. It serves as the mandatory documentation
deliverable for the v0.36 phase boundary (ROADMAP §Phase Boundaries).

---

## 1. Frontier Protocol

A **frontier** is the highest epoch at which a pipeline shard has committed
all input, making its output queryable. Frontiers propagate from sources
through operators to sinks and drive downstream progress.

### 1.1 Shard Frontier

Each shard maintains a monotonically increasing `epoch` counter. After a
`WriteBatch` commits all view-output rows for an epoch, the shard writes the
new epoch to `meta/frontier`. The `EpochCoordinator` reads this value to know
how far the shard has advanced.

### 1.2 Cluster Frontier

The cluster frontier is the **meet** (minimum) of all shard frontiers for a
given pipeline. A query result is consistent at epoch `E` only after all shards
have reached epoch ≥ E. The `FrontierAggregator` collects per-worker frontier
reports and computes the cluster-wide minimum.

### 1.3 Frontier Stall Detection

If the cluster frontier does not advance within the configured
`frontier_stall_threshold_ms`, the `LivenessChecker` surfaces a
`FrontierStalled` degraded state. The operator should check:
- Is any shard stuck on a slow input?
- Is the object store available? (see §4 Brownout Handling)
- Are all workers sending heartbeats?

---

## 2. Checkpoint Protocol

Checkpoints provide the durable foundation for exactly-once semantics and
fast recovery (DESIGN.md §11.3).

### 2.1 Barrier Injection

The `CheckpointCoordinator` injects a `CheckpointBarrier` into the pipeline
by writing a barrier record to each shard's WAL. Operators propagate the
barrier downstream. An epoch is not committed until all operators have seen
and acknowledged the barrier.

### 2.2 Alignment Buffer

Each shard's `AlignmentBuffer` accumulates rows received before the barrier
has propagated from all upstream shards. The buffer is bounded by
`alignment_buffer_max_rows` (default 1 000 000). Overflow surfaces `RS-3601`
and the checkpoint is aborted.

### 2.3 Atomic Commit

After all shards acknowledge the barrier (`ShardCheckpointAck`), the
coordinator atomically writes the checkpoint manifest. The commit is
all-or-nothing: partial manifest writes are detected on recovery by checking
the manifest epoch against each shard's committed frontier.

### 2.4 Checkpoint GC

Old checkpoints are removed once the cluster frontier advances past the
retention window (`checkpoint_retention_count`, default 3). The
`CheckpointGc` collects checkpoints below the GC frontier atomically within
the same `WriteBatch` as the new checkpoint commit.

---

## 3. Recovery Budgets

RockStream commits to the following recovery SLOs (DESIGN.md §11.5):

| Phase | Budget | Mechanism |
|---|---|---|
| Failure detection | **≤ 5 s** | The control plane's per-connection handler notices a worker's TCP connection close (or a failed `Heartbeat`/`RequestShard` round-trip) and calls `ShardScheduler::on_worker_dead`, which releases the worker's leases and triggers reassignment (`crates/rockstream-control/src/scheduler.rs`, `crates/rockstream-control/src/service.rs`). |
| Shard reassignment | **≤ 30 s** | Checkpoint-from-storage; no full WAL replay |
| Pipeline freshness recovery | **≤ 60 s** | Catch-up ingest at burst rate |

When the freshness recovery budget is exceeded, `RecoveringSlow` fires with
`RS-1603`. The `LivenessChecker` (`crates/rockstream-sim/src/liveness.rs`)
maps this to the `RecoveringSlow` named degraded state.

### 3.1 Self-Fencing

A worker that cannot reach the control plane starts an isolation clock via
`SelfFenceGuard::tick` (`crates/rockstream-runtime/src/fence.rs`). Once the
isolation duration reaches `DEFAULT_SELF_FENCE_DEADLINE` (30 s),
`SelfFenceGuard::must_self_fence` returns `true` and the worker must
terminate immediately rather than attempt another epoch commit
(`assert_within_deadline` panics with `RS-1702` if this is violated). This
prevents a partitioned worker from racing the new shard owner. Every epoch
commit is additionally fence-checked with `assert_valid_writer`, which
rejects a stale lease token with `RS-1702` (DESIGN.md §11.6).

---

## 4. Object Store Brownout Handling

During an object store brownout (DESIGN.md §11.7):

1. Workers stall at the epoch commit step (WAL flush blocks).
2. Up to `local_buffer_max_epochs` (default 10) epochs are buffered in the
   tokio task input queue. The `ObjectStoreBrownoutGuard` tracks this.
3. When the buffer is full, `BrownoutStatus::Blocked` fires (`RS-3003`) and
   the source connector is credit-starved, pausing Kafka consumption.
4. On recovery, buffered epochs commit in order. No data loss (writes were
   buffered, not dropped). No duplicates (epoch keys are idempotent).

---

## 5. Shuffle Transport

Shuffle (exchange) moves rows between workers in a pipeline. The path for
each exchange is recorded as an `ExchangePath` (`Elided`, `Loopback`, or
`Direct`) on its `ExchangeAnn` (`crates/rockstream-types/src/exchange.rs`).
Two transport paths exist:

- **Loopback**: source and destination are on the same worker. Rows pass
  through a bounded in-process `tokio::mpsc` channel managed by the
  `LoopbackRouter` (`crates/rockstream-runtime/src/exchange/loopback.rs`);
  zero worker-to-worker network calls are made.
- **Direct**: source and destination are on different workers. Rows are
  encoded as Arrow IPC frames (`crates/rockstream-runtime/src/exchange/
  serialization.rs`) and transmitted over the gRPC shuffle service
  (`crates/rockstream-runtime/src/exchange/service.rs`).

The `DurableShuffleWriter` (`crates/rockstream-runtime/src/exchange/
durable.rs`) ensures shuffle objects are written to the WAL before the
sending epoch commits.

---

## 6. Wire Protocol Version Skew (Rolling Upgrades)

During a rolling upgrade there is a window where some workers run version N
and others run N+1 (DESIGN.md §5.5). The wire protocol version skew contract:

- Each gRPC service announces a `protocol_version` header.
- The receiving side calls `negotiate_version(local_range, remote_version)`.
- If the remote version is outside `[local_min, local_max]`, the call is
  rejected with `RS-5003`.
- N+1 binaries must support at least version N (backward-compatible wire
  format for one version).

The current wire protocol version is **v1**. The v0.36 release targets **v2**
with N−1 backward compatibility.

---

## 7. Exactly-Once Sinks (2PC)

Sink connectors implement a two-phase commit to achieve exactly-once delivery
(DESIGN.md §11.4):

```
Pre-commit (during epoch):
  prepare(batch) → stage rows in a transactional buffer
                   (Kafka: producer transaction open;
                    S3: write to _pending/{epoch}/...;
                    Postgres: BEGIN + INSERT)

Commit (after cluster checkpoint succeeds):
  commit(epoch)  → finalize the transaction
                   (Kafka: commit_transaction();
                    S3: atomic rename _pending → final;
                    Postgres: COMMIT)

Abort (if checkpoint fails):
  abort(epoch)   → discard staged rows
                   (Kafka: abort_transaction();
                    S3: delete _pending objects;
                    Postgres: ROLLBACK)
```

The `TwoPcSinkState` data structure tracks the current phase and handles
crash recovery: if a process crashes in `PreCommitted` state, the recovery
path re-runs `commit` (idempotent).

---

## 8. Cluster Bootstrap Walkthrough

1. **Control plane starts**: initializes the shard map, worker registry, and
   (if `--raft-peers` is set) the Raft group.

2. **Workers start**: each worker opens a TCP connection to the control
   plane and sends `WorkerMessage::Register` with its `WorkerRegistration`
   (`crates/rockstream-runtime/src/client.rs`,
   `crates/rockstream-types/src/topology.rs`).

3. **Shard acquisition**: each worker sends `WorkerMessage::RequestShard`
   for its assigned shards. The control plane responds with
   `ControlMessage::ShardAssigned` carrying the lease.

4. **Shard open**: each worker opens its SlateDB instances. The latest
   checkpoint manifest is loaded; the shard replays from the checkpointed state.

5. **Heartbeats and frontiers**: workers send `WorkerMessage::Heartbeat`
   every 500 ms (`crates/rockstream-runtime/src/client.rs`) and frontier
   reports after each epoch commit.

6. **Sources start**: connectors begin polling at their committed offsets.

7. **Cluster frontier advances**: once all shards have reported their initial
   frontier, the cluster is ready to serve queries.

---

## 9. Simulation and Chaos Testing

From v0.36, all distributed system invariants are verified by the continuous
simulation soak infrastructure (`simulation-soak.yml`):

- **Law-equivalence-under-fault**: every registered merge law contributes
  seeded `SimRuntime` tests for reorder / duplicate / crash-replay / fence.
- **Chaos scenarios**: 32-shard deterministic chaos runs verify zero data loss
  and zero duplicates across simulated 24-hour periods.
- **Regression corpus**: failing seeds are minimized, stored in `SeedCorpus`,
  and block release until all replay cleanly.

The initial corpus (`build_initial_corpus()`) covers `WeightAdd/v1`,
`SumCount/v1`, `MaxRegister/v1`, `HyperLogLog/v1`, and `BloomUnion/v1`.

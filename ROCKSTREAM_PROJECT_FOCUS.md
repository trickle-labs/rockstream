# Rockstream: Recommended Project Focus and Path Forward

**Status:** Proposed project direction  
**Audience:** Maintainers, contributors, design partners, and early adopters  
**Decision horizon:** The next major release and the work required to make it production-ready

---

## Executive summary

Rockstream should focus on being a **cloud-native incremental view maintenance (IVM) service** with a deliberately small public surface:

- Continuously maintain SQL-defined materialized views.
- Store authoritative state in object storage.
- Accept data through PostgreSQL CDC, Kafka, and PostgreSQL-wire DML.
- Serve committed tables and materialized views through the PostgreSQL wire protocol.
- Recover predictably from crashes without lost, duplicated, or partially visible data.

Rockstream should **not** try to become a smaller version of a general-purpose streaming database, analytical warehouse, lakehouse platform, or PostgreSQL replacement.

The practical strategy is:

> **Promise less, make those promises extremely reliable, and keep advanced work outside the default compatibility contract.**

The project already contains many of the right technical foundations: Z-set/DBSP-style incremental operators, SlateDB-backed durable state, epochs and frontiers, coordinated checkpoints, deterministic simulation, PostgreSQL wire access, connector offset tracking, and bounded operator state. The next phase should therefore emphasize **scope control and lifecycle correctness**, rather than adding more SQL features, connectors, catalogs, or scaling mechanisms.

---

## 1. The recommended product definition

### North star

> **Rockstream maintains continuously fresh SQL materialized views over durable changing tables, using disposable compute and object-storage-backed state.**

A user should be able to:

1. Connect a PostgreSQL database or Kafka topic, or write rows through PostgreSQL wire DML.
2. Define a materialized view with a supported SQL query.
3. Query the latest globally committed result through a normal PostgreSQL client.
4. Restart, rescale, or replace workers without rebuilding authoritative state from scratch.
5. Understand exactly what consistency, recovery, and delivery guarantees apply.

### Primary use cases

Rockstream should optimize for a small set of recurring use cases:

- Live operational dashboards backed by continuously maintained aggregates.
- Application-facing projections and denormalized read models.
- Continuously maintained joins between operational tables.
- Incremental metrics and ranking views over high-change datasets.
- A freshness layer between operational change streams and downstream consumers.

These use cases benefit directly from IVM. They do not require Rockstream to provide a broad ad hoc analytical engine or a complete transactional database.

### The product boundary

Rockstream’s supported product should contain three durable user concepts:

1. **Table** — a durable append-only or keyed current-state input owned by Rockstream.
2. **Materialized view** — a continuously maintained query result.
3. **Sink** — an optional delivery target for a table or materialized view.

Connections and connector runtimes may exist in the catalog, but they do not need to become a large user-facing object model.

---

## 2. The public contract

The public contract is the set of behaviors users may safely build production systems around. It should be substantially smaller than the set of experiments or proof harnesses present in the repository.

### Supported ingestion

The default product should support only these input paths:

| Input path | Supported role |
|---|---|
| PostgreSQL logical replication | Shared CDC source for one or more upstream tables |
| Kafka | Append-only, keyed upsert, or CDC-formatted input |
| PostgreSQL wire DML | Direct `INSERT`, `UPDATE`, and `DELETE` into Rockstream tables |
| Object storage | Bounded bulk import and export, not a general continuous file-watching platform |

A strict connector cap keeps the system testable and makes end-to-end guarantees credible.

### Supported serving

The PostgreSQL wire protocol should remain Rockstream’s primary access interface. The default query path should support:

- Scans of tables and materialized views.
- Projection and filtering.
- Primary-key and bounded range lookup.
- Simple ordering and limiting.
- Introspection of tables, views, freshness, backfill status, and connector status.

Expensive ad hoc joins, grouped aggregation, subqueries, and general analytical SQL should require a materialized view or remain explicitly experimental. PostgreSQL wire compatibility should be an **access protocol**, not a promise to implement PostgreSQL or a distributed warehouse.

### Supported incremental SQL

A defensible core SQL subset is:

- Projection and filtering.
- Deterministic scalar expressions.
- Inner equi-join.
- Left equi-join.
- `GROUP BY`.
- `SUM`, `COUNT`, `AVG`, `MIN`, and `MAX`.
- `DISTINCT`.
- `UNION ALL`.
- Top-K / `ORDER BY ... LIMIT` where incrementally maintainable.
- View-on-view composition.

The initial compatibility contract should exclude:

- Recursive CTEs.
- Lateral joins and general set-returning functions.
- Full and right outer joins.
- General SQL window functions.
- Session and hopping windows.
- Arbitrary correlated subqueries.
- Full TPC-H compatibility as a product goal.

These features may remain in the codebase as experiments, but they should not drive the default architecture or release criteria.

### Consistency contract

Rockstream should state consistency in simple terms:

- An epoch is the atomic unit of input and IVM progress.
- A cluster checkpoint defines globally committed visibility.
- Queries read the newest globally committed checkpoint, never a mixture of independently advanced shards.
- A write may be accepted before it becomes query-visible.
- Clients that require synchronization use an explicit freshness token, `FLUSH`, or `WAIT FOR EPOCH` mechanism.

### Delivery contract

Rockstream should distinguish internal processing guarantees from external sink guarantees:

- Internal state and view maintenance can be exactly-once across checkpoint recovery.
- External delivery depends on the destination and connector mode.
- Each sink declares its changelog and delivery capabilities.

For example:

```rust
enum ChangelogMode {
    AppendOnly,
    Upsert { key: Vec<ColumnId> },
    Retract,
}

enum DeliveryGuarantee {
    TransactionalExactlyOnce,
    IdempotentUpsert,
    AtLeastOnce,
}
```

The planner should reject an incompatible view-to-sink combination instead of implying a universal exactly-once guarantee.

---

## 3. Explicit non-goals

The following should not be part of Rockstream’s core product direction:

- A general-purpose OLTP database.
- A distributed ad hoc OLAP engine.
- Full PostgreSQL syntax or behavioral compatibility.
- A large connector marketplace.
- An Iceberg, Delta, or lakehouse catalog platform.
- General-purpose stream-processing APIs.
- Multi-region active-active writes.
- User-defined CRDTs or general merge-law DDL.
- Automatic semantic rewriting for every hot-key case.
- Multiple specialized backfill products or selectable backfill algorithms.
- Serverless resource management as a prerequisite for the core engine.

A feature should enter the supported core only when it directly improves one of four actions:

> **Ingest durable changes, maintain materialized views, recover safely, or serve committed results.**

---

## 4. Target architecture

```text
 PostgreSQL CDC         Kafka          PostgreSQL-wire DML
       │                  │                    │
       └──────────┬───────┴───────────┬────────┘
                  ▼                   ▼
          Shared connector runtimes / input transactions
                              │
                              ▼
                    Durable base tables
                 (checkpointed in SlateDB)
                              │
                              ▼
                   Incremental operator graph
             (Z-sets, arrangements, epochs, frontiers)
                              │
                              ▼
                  Durable materialized views
                       │               │
                       ▼               ▼
               PostgreSQL serving   Durable sink outbox
                                           │
                                           ▼
                                     Kafka or export sink

 Authoritative state: object storage
 Disposable compute: gateway, worker, connector, and control roles
 Global visibility: checkpoint manifest
```

### Architectural principles

#### Durable tables are the ingestion boundary

Long-running materialized views should read Rockstream-owned tables or other materialized views, not live external connectors directly. This makes later backfill, replay, schema validation, and multi-view sharing independent of external retention settings.

#### One incremental engine and one serving truth

There should be no separate batch recomputation path that silently serves different semantics from the IVM engine. Batch execution remains useful as an oracle, bootstrap mechanism, and explicitly bounded query path, but maintained view results come from one authoritative incremental data plane.

#### Object storage is authoritative; compute is disposable

Workers own leases and caches, not irreplaceable data. Losing a worker should require reassignment and state reopening, not rebuilding the entire dataset from an upstream system.

#### Lifecycle correctness comes before feature breadth

Backfill, checkpointing, source offsets, crash recovery, sink recovery, schema transitions, and deletion are the product. A feature is not production-ready merely because its happy-path query result is correct.

#### Every resource is bounded

Every queue, buffer, catch-up log, outbox, alignment window, cache, registry, and in-memory arrangement must have:

- A named bound.
- A fill-level metric.
- A defined backpressure or rejection behavior.
- A recovery test.

#### Advanced mechanisms stay behind stable abstractions

Hot-key splitting, specialized caches, alternative backfill algorithms, extra connectors, and lakehouse integrations may evolve behind interfaces without expanding the supported contract.

---

## 5. Production-critical lifecycle paths to harden

### 5.1 Unified, resumable backfill

Backfill is the most important missing production contract. Creating a view over existing data requires a consistent transition from a historical snapshot to live changes.

Rockstream should implement one generalized state machine:

```text
CREATING
   ↓
SNAPSHOTTING(snapshot epoch E)
   ↓
CATCHING_UP(replaying changes after E)
   ↓
PUBLISHING(checkpoint-aligned cutover)
   ↓
RUNNING
```

The implementation should include:

- An atomic snapshot/offset fence.
- A deterministic primary-key scan order.
- Durable per-partition scan cursors.
- A bounded durable catch-up log for changes after the snapshot point.
- Rate limiting and backpressure.
- Checkpointed recovery of scan and catch-up progress.
- Atomic publication only after the view reaches a committed frontier.

A minimal fence could be:

```rust
struct SnapshotFence {
    snapshot_token: Vec<u8>,
    delta_offset: OffsetToken,
    schema_fingerprint: [u8; 32],
}
```

A minimal persisted progress record could be:

```rust
struct BackfillProgress {
    view_id: ViewId,
    logical_partition: u16,
    snapshot_epoch: Epoch,
    snapshot_offset: OffsetToken,
    last_primary_key: Option<Vec<u8>>,
    snapshot_rows: u64,
    catchup_epoch: Epoch,
    snapshot_complete: bool,
}
```

The view must not expose a partially built result as `RUNNING`.

### 5.2 Shared PostgreSQL CDC with transaction-preserving fan-out

One replication connection should be able to feed multiple Rockstream tables from the same upstream database. This avoids duplicate work and, more importantly, preserves cross-table transaction ordering.

The connector should emit committed transaction envelopes:

```rust
struct SourceTransaction {
    transaction_id: Vec<u8>,
    commit_offset: OffsetToken,
    commit_timestamp: Option<Timestamp>,
    changes: Vec<TableChange>,
}
```

All changes in one upstream transaction should become visible in one atomic Rockstream input commit. Oversized transactions may spill to durable input storage, but they should not be silently divided into independently visible epochs.

### 5.3 Checkpoint-coupled source offsets

Rockstream already has strong source-offset primitives. The supported contract should make the ordering explicit:

1. Decode and validate input.
2. Persist input mutations and the source checkpoint together.
3. Commit the Rockstream epoch/checkpoint.
4. Acknowledge or advance the external source offset.

A crash before step 3 must not acknowledge the source. A crash between steps 3 and 4 must retry only the acknowledgment, not duplicate the input mutation.

### 5.4 Durable sink outbox

External sink availability should not immediately stall every internal checkpoint. Materialized changes should first enter a durable outbox committed with Rockstream state, then drain independently.

```text
IVM checkpoint
   ├── commits view state
   ├── commits view output
   └── appends sink changelog to durable outbox
                         ↓
               independent sink drainer
                         ↓
           external commit + cursor advance
```

The outbox needs:

- Stable record identity such as `(sink_id, epoch, sequence)`.
- An independently checkpointed drain cursor.
- A hard byte and age limit.
- Fill-level and lag metrics.
- Defined escalation from decoupled operation to upstream backpressure.
- Recovery for a crash after external commit but before cursor advancement.

### 5.5 Barrier and checkpoint priority

Control progress must not be starved behind ordinary data. Barrier delivery should use a dedicated bounded channel or a prioritized lane so a large batch or backfill cannot indefinitely prevent checkpoint completion.

Track separately:

- Barrier propagation latency.
- Alignment wait time.
- Checkpoint persistence latency.
- Global commit latency.
- Data-processing latency.

### 5.6 Storage and compaction pressure

Object-storage-backed state remains healthy only when compaction keeps pace with logical writes. Compute scaling that increases write pressure without accounting for compaction debt can make the system slower.

Rockstream should expose and act on:

- L0 file count and bytes.
- Oldest uncompacted L0 age.
- Pending compaction bytes.
- Logical and compacted bytes written.
- Write amplification.
- Cache hit ratio.
- Object-store request latency and failure rate.
- Read amplification where available.

A normalized control signal can be defined as:

```text
storage_debt = max(
  l0_bytes / target_l0_bytes,
  oldest_l0_age / target_compaction_age,
  pending_compaction_bytes / target_pending_bytes,
  write_amplification / target_write_amplification
)
```

Storage debt should influence connector credits, backfill rate, admission control, and bounded compaction concurrency.

---

## 6. Scaling model

### Supported v1 model: stable logical partitions

The supported scaling model should use a fixed logical partition space per table or namespace:

```text
logical_partition = stable_hash(key) % partition_count
```

Durable state keys include the logical partition. The control plane maps logical partitions to workers and may move them without changing query semantics or key identity.

This gives Rockstream:

- Stable recovery units.
- Bounded movement during rescaling.
- Per-partition backfill progress.
- Per-partition checkpoint and skew metrics.
- A clean separation between logical state and physical worker placement.

### Experimental model: automatic hot-key rewriting

Automatic hot-key salting and partial-state recombination should remain experimental. It requires operator-specific proofs and special behavior for non-composable state such as `DISTINCT`, outer joins, and Top-K.

The supported contract should initially be:

- Detect and report hot keys.
- Spill bounded state safely.
- Move logical partitions.
- Allow an operator override or manual repartitioning decision.

Automatic semantic rewriting may be promoted only after it is demonstrably needed by production workloads.

---

## 7. Managing the existing repository

The project does not need to discard advanced work. It needs to classify it and prevent it from expanding the default maintenance burden.

### Core

The default build and compatibility contract should include:

- Z-set and DBSP-derived incremental semantics.
- The core operator subset.
- SlateDB-backed arrangements and view output.
- Epochs, frontiers, checkpoints, fencing, and recovery.
- PostgreSQL wire serving and direct DML.
- PostgreSQL CDC and Kafka.
- Bounded object-storage import/export.
- Oracle, property, simulation, integration, and chaos tests.
- Operational status, metrics, and support tooling.

### Experimental

Keep behind feature flags, separate binaries, or a clearly marked experimental workspace:

- General ad hoc joins and grouped analytical queries.
- Secondary indexes beyond primary serving needs.
- Recursive CTEs and lateral joins.
- General window functions and advanced event-time windows.
- Automatic hot-key virtual bucketing.
- Advanced autoscaling controllers.
- Iceberg and Delta output.
- Lakehouse catalog integrations.

Suggested feature families:

```text
experimental-analytics
experimental-lakehouse
experimental-elasticity
experimental-sql-advanced
```

### Remove or extract from the default product

Connector and catalog code that is not part of the core contract should leave the default dependency graph. Options include:

- A separate `rockstream-extras` repository.
- Non-default workspace members.
- Independently versioned integration crates.

This reduces build time, dependency risk, security surface, CI cost, and accidental compatibility commitments.

### Documentation and version discipline

The README, Cargo version, roadmap, release notes, and capability matrix must describe the same release. A feature should not be advertised as supported merely because a proof harness or sign-off document exists.

Every feature should have one status:

- **Supported** — stable compatibility contract and release-gated lifecycle tests.
- **Experimental** — available without compatibility guarantees.
- **Not planned** — intentionally outside the product direction.

---

## 8. Phased path to the target

### Phase 0 — Scope freeze and compatibility reset

**Goal:** Establish one authoritative product contract before adding more functionality.

Actions:

- Approve a project-focus ADR based on this document.
- Publish the Supported / Experimental / Not planned capability matrix.
- Label every connector, SQL feature, control loop, and catalog integration.
- Move non-core features out of the default build.
- Align README, roadmap, Cargo version, and release status.
- Replace broad benchmark goals such as full TPC-H compatibility with an IVM-focused correctness corpus.

Exit criteria:

- The default binary cannot dispatch an unsupported connector.
- Unsupported ad hoc analytical SQL is rejected with a clear error.
- The default dependency graph contains only core integrations.
- The documentation has one unambiguous product definition.

### Phase 1 — Unified backfill and publication

**Goal:** Make view creation over existing data safe, bounded, resumable, and atomic.

Actions:

- Add the atomic snapshot fence to the source contract.
- Add durable per-partition backfill progress.
- Add the bounded catch-up log.
- Add checkpoint-aligned publication.
- Extend view lifecycle states to represent snapshotting and catch-up separately.
- Reject or conservatively restart schema changes during active backfill.

Required tests:

- Crash after snapshot output but before cursor commit.
- Crash after cursor commit but before reading the next page.
- Crash after snapshot completion but before catch-up starts.
- Crash midway through catch-up.
- Insert, update, and delete of a key before and after its snapshot position.
- Repeated crash/recovery with byte-identical final output.
- Backfill buffer and catch-up log reaching their configured bounds.

Exit criteria:

- A view never publishes a partial result as running.
- Recovery resumes from the last durable cursor rather than restarting blindly.
- Incremental output after cutover matches a full batch oracle.

### Phase 2 — Transactional shared-source ingestion

**Goal:** Make connector-backed tables a stable, reusable input boundary.

Actions:

- Implement shared PostgreSQL CDC runtimes.
- Add committed transaction envelopes and cross-table fan-out.
- Couple consistent snapshot creation to the WAL offset fence.
- Require stable primary keys for current-state CDC tables.
- Define append-only and upsert Kafka table semantics.
- Preserve the existing checkpoint-before-acknowledgment ordering.

Required proof:

> A transaction that changes multiple upstream tables becomes visible in all corresponding Rockstream tables and dependent views in the same committed Rockstream epoch.

Exit criteria:

- Multiple tables can share one PostgreSQL replication connection.
- Source reconnection cannot expose half of a committed upstream transaction.
- Replaying a committed source offset produces no duplicate visible state.

### Phase 3 — Explicit sink semantics and durable outbox

**Goal:** Make output guarantees accurate and isolate internal progress from temporary sink failures.

Actions:

- Add changelog and delivery capability declarations.
- Implement the durable sink outbox.
- Implement Kafka transactional and idempotent-upsert modes.
- Define bounded object-storage export semantics.
- Remove universal exactly-once wording from the generic sink trait.

Required tests:

- External commit succeeds and the worker crashes before cursor advancement.
- The destination is unavailable while internal checkpoints continue.
- The outbox reaches its hard bound and applies predictable backpressure.
- Recovery dispatch differs correctly by delivery capability.

Exit criteria:

- The system reports the actual end-to-end guarantee for every sink.
- No sink can silently request a changelog shape it cannot represent.
- Temporary sink failure does not immediately block internal checkpoint progress.

### Phase 4 — Storage-pressure and serving isolation

**Goal:** Keep the IVM loop healthy under real cloud-storage and query pressure.

Actions:

- Complete SlateDB metrics wiring.
- Introduce `storage_debt` and integrate it with admission and backfill rate.
- Separate CPU and concurrency budgets for streaming, gateway queries, connectors, sinks, and compaction.
- Keep gateway queries from starving checkpoint and operator execution.
- Add an optional non-authoritative local cache when local disk is configured.

Exit criteria:

- Sustained compaction debt triggers bounded corrective action before unbounded L0 growth.
- Query load cannot indefinitely block checkpoints.
- All caches and queues expose byte bounds and fill metrics.

### Phase 5 — Scaling simplification

**Goal:** Make rescaling predictable without making advanced skew rewriting part of the core promise.

Actions:

- Introduce or standardize the stable logical partition space.
- Persist partition-to-worker assignments in the catalog.
- Move logical partitions through checkpointed migration.
- Track backfill, state size, and lag per logical partition.
- Keep automatic hot-key bucketing behind an experimental feature.

Exit criteria:

- Worker replacement and scale-out do not change row identity or query semantics.
- A failed migration resumes or rolls back deterministically.
- Rescaling does not require replaying an entire external source.

### Phase 6 — Production release gate

**Goal:** Release a narrow product with evidence-backed lifecycle guarantees.

Release gates:

- Core operator oracle tests remain bit-identical to batch computation.
- Crash and checkpoint tests show no lost or duplicate committed input.
- Backfill fault matrix passes on local object storage and a real S3-compatible backend.
- Shared PostgreSQL CDC transaction tests pass.
- Sink outbox recovery tests pass.
- Storage-pressure soak remains bounded.
- The core connector count remains within the approved cap.
- The capability matrix and default build agree.

---

## 9. Feature admission rules

A proposed feature should enter the supported core only when every answer below is satisfactory.

### Product fit

- Does it directly improve durable ingestion, IVM, recovery, or serving?
- Is it required by a concrete target workload?
- Can the same need be met through a materialized view rather than a broader ad hoc engine?

### Semantic clarity

- Is its consistency behavior defined?
- Is its changelog behavior defined for inserts, updates, and deletes?
- Is its interaction with backfill and schema change defined?
- Can `EXPLAIN INCREMENTAL` describe the behavior honestly?

### Operational safety

- Are memory, disk, queue, and retry bounds named?
- Are fill-level and lag metrics present?
- Is backpressure or rejection behavior defined?
- Does it recover correctly after interruption at every durable boundary?

### Scope cost

- Does it add a connector family, external protocol, catalog, or major dependency?
- Does it require a second execution engine?
- Does it create a compatibility obligation beyond IVM?
- Can it live in an experimental crate without burdening the core?

### Proof

- Is there an incremental-versus-batch oracle test?
- Is there a deterministic simulation test for coordination changes?
- Are real-network and real-object-store tests present where needed?
- Are negative and recovery tests release-gating rather than optional?

---

## 10. Measures of success

The project is moving in the right direction when the following become true:

### Scope

- The core connector set remains intentionally small.
- The default build excludes experimental lakehouse and analytical dependencies.
- The README can describe the complete supported product on one page.
- Every public feature has a clear support status.

### Correctness

- Every supported operator has incremental-versus-batch property coverage.
- Backfill and CDC recovery produce byte-identical committed results after repeated crashes.
- No query can observe a partially published view or cross-shard checkpoint mixture.

### Operability

- Operators can distinguish source lag, compute lag, checkpoint lag, sink lag, and compaction debt.
- Every bounded resource reports its current usage and limit.
- Failures produce a named state and an actionable next step.

### Reliability

- Worker replacement does not require full external-source replay.
- Source offsets never advance ahead of durable Rockstream state.
- Sink recovery behavior matches the declared delivery capability.
- Storage pressure is controlled before it becomes an outage.

### User experience

- The path from empty installation to a maintained view requires only a few stable concepts.
- PostgreSQL clients can query committed results without a custom driver.
- Users do not need to understand shards, antichains, arrangements, or compaction to operate a normal workload.

---

## 11. Immediate decisions and actions

1. **Adopt the product definition:** Rockstream is a cloud-native IVM service, not a general streaming database.
2. **Freeze core scope:** no new connector, SQL family, catalog, or broad query feature enters the default product until the lifecycle milestones are complete.
3. **Create the capability matrix:** classify the current repository into Supported, Experimental, and Not planned.
4. **Update the roadmap:** replace feature-expansion milestones with backfill, CDC transactionality, sink outbox, and storage-pressure milestones.
5. **Write the backfill protocol specification:** make it the next normative architecture document and implementation focus.
6. **Gate non-core modules:** remove lakehouse, advanced analytical SQL, and automatic skew rewriting from the default build.
7. **Align version and documentation:** one release number, one status, and one public compatibility story.

---

## Final recommendation

Rockstream’s differentiator is not the number of connectors or SQL features it can accumulate. Its differentiator is the combination of:

- Theory-grounded incremental computation.
- Durable object-storage-backed state.
- Disposable cloud-native compute.
- Familiar PostgreSQL access.
- Deterministic, evidence-driven correctness testing.

The project should protect that differentiator by narrowing its promise and concentrating engineering effort on the lifecycle paths that determine whether users can trust it in production.

The intended result is simple:

> **A small, reliable system that ingests durable changes, maintains a well-defined set of materialized views, survives failure predictably, and serves committed answers through tools users already know.**

---

## Reference material

This recommendation is informed by the current Rockstream architecture and implementation documents, including:

- [Rockstream README](https://github.com/trickle-labs/rockstream/blob/main/README.md)
- [Rockstream Architecture](https://github.com/trickle-labs/rockstream/blob/main/ARCHITECTURE.md)
- [Rockstream IVM Design](https://github.com/trickle-labs/rockstream/blob/main/IVM.md)
- [Rockstream Focused Implementation Plan](https://github.com/trickle-labs/rockstream/blob/main/NEW_IMPLEMENTATION_PLAN.md)
- [Rockstream Roadmap](https://github.com/trickle-labs/rockstream/blob/main/NEW_ROADMAP.md)

It also incorporates selected architectural lessons from RisingWave’s public documentation, particularly around durable tables, barriers and checkpoints, CDC ingestion, backfill, sink delivery, and compute/storage separation:

- [RisingWave introduction](https://docs.risingwave.com/get-started/intro)
- [RisingWave architecture](https://docs.risingwave.com/get-started/architecture)
- [Source, table, materialized view, and sink](https://docs.risingwave.com/get-started/source-table-mv-sink)
- [CDC ingestion](https://docs.risingwave.com/ingestion/cdc-with-risingwave)
- [Sink delivery](https://docs.risingwave.com/delivery/overview)

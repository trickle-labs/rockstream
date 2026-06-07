# RockStream Focused Implementation Plan

A deliberately narrow, test-first roadmap from an empty repository to a
production-grade system built around **two core pillars** and nothing else:

1. **A cloud-native Incremental View Maintenance (IVM) engine** — DBSP-native
   operators over Z-sets, sharded across many SlateDB instances backed by
   object storage, with exactly-once semantics and frontier-tracked progress.
2. **A PostgreSQL wire access layer** — applications read materialized views,
   write base-table rows, and subscribe to change streams over the standard
   Postgres protocol.

Everything else in [DESIGN.md](DESIGN.md) and the original
[IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) is **explicitly deferred** (see
[Out of Scope](#out-of-scope-for-this-plan)). This document replaces breadth
with depth: a smaller surface, implemented in strict dependency order, with a
named test gate at every step.

> **Read alongside**:
> - [DESIGN.md](DESIGN.md) — system architecture.
> - [IVM.md](IVM.md) — the IVM engine internals (PlanIR, differentiation pass,
>   operator runtime). The milestone tags `IVM-n` below reference IVM.md §13.

---

## Guiding Principles for This Plan

1. **Correctness before scale; scale before features.** The single-shard engine
   must be provably correct before any distribution work begins. Distribution
   must be correct and fault-tolerant before the Postgres layer is built on top.
2. **Every step ships with its tests.** No deliverable is "done" until its
   oracle, property, simulation, and exit-criteria tests are green. Tests are
   not a separate phase.
3. **The oracle is the source of truth.** For every operator and every query,
   `incremental(query, deltas) == batch(query, accumulated)` is asserted against
   a batch reference engine (DataFusion). This is the DBSP soundness theorem and
   it is checked continuously.
4. **Deterministic simulation from day one.** All I/O goes through a `Runtime`
   abstraction so the whole system can run single-threaded under a seeded RNG.
   Bugs are reproducible by seed; fixed bugs become permanent regression seeds.
5. **One binary, one CLI, one config.** Every node role is a flag on the same
   `rockstream` binary. Users interact with views and sources, never with shards
   or antichains.
6. **Narrow public surface.** Only the SQL subset, CLI commands, and system
   tables required by the two pillars are exposed. A smaller surface is a
   feature.

---

## Out of Scope for This Plan

The following are intentionally **not** part of this plan. They are deferred,
not rejected; each can be added later behind the same abstractions without a
rewrite. Removing them is what makes the core deliverable achievable and
well-tested.

- User-visible CRDT column types (`COUNTER`, `LWW`, `OR_SET`, `MV_REGISTER`)
  and the general `CREATE MERGE LAW` facility. *(The internal Z-set weight
  algebra is kept; user-facing CRDTs are not.)*
- Cold-tier Iceberg/Delta sinks, the native Iceberg REST catalog server, and
  the DuckLake catalog server.
- Secondary indexes and shard-column-statistics scatter pruning.
- Optimistic / `SERIALIZABLE LOCAL` transactions and the transaction-shape
  classifier.
- A broad connector matrix and external connector/plugin APIs.
- Advanced auto-tuning and SLO-driven adaptive control loops beyond simple,
  bounded epoch-sizing and parallelism defaults.
- Recursion (`WITH RECURSIVE`), lateral / set-returning functions, and advanced
  windowing beyond the essential set.
- Multi-region operation and active-active writes.
- Historical (`AS OF`) full-collection scans beyond the checkpoint-bounded
  point/range reads needed for correctness.

A focused 1.0 ships the two pillars, correct and operable. The deferred work is
sequenced post-1.0.

---

## Testing Strategy (Cross-Cutting, Non-Negotiable)

This is the backbone of the plan. Every phase below references this ladder and
must satisfy the relevant rungs before it exits.

| Rung | What it proves | Mechanism |
|---|---|---|
| **Unit** | Each module behaves per its contract. | `cargo test` per crate. |
| **Oracle / property** | `incremental == batch` for random input sequences. | `rockstream-oracle` + `proptest`: random insert/update/delete/retract sequences, compared against a DataFusion batch run of the same query. |
| **Deterministic simulation** | The distributed system is correct under reordering, partial failure, and crash-replay — reproducibly. | `rockstream-sim`: `SimRuntime` (seeded RNG, single-threaded), `buggify!()` fault injection, paired assertions at durable/network boundaries. |
| **Integration** | Real processes, real network, real object store. | `testcontainers` (MinIO/Postgres/Kafka); multi-host where stated. |
| **Chaos** | Survives arbitrary single-node faults end-to-end. | Process kills, network partitions, object-store throttling. |
| **Soak** | No drift, leak, or unbounded cost over long runs. | 24h–7d continuous workloads with reference comparison. |
| **Benchmark** | Performance is measured and does not regress. | `criterion`; CI fails on >10% regression. |

**Rules that hold for every phase:**

- A bug found by simulation is checked in as a regression seed and replayed in
  CI forever.
- Every operator added has an oracle property test before it is considered
  complete.
- Every coordination code path (epoch commit, frontier merge, sink 2PC, fencing)
  carries `buggify!()` annotations and is exercised by `SimRuntime`.
- No code path may depend on SlateDB range deletion; cleanup is scan-and-delete
  or snapshot-safe compaction filters, and a test asserts this.

---

## Phase Map

| Phase | Pillar | Focus | Exit gate |
|---|---|---|---|
| 0 | Foundation | Workspace, storage wrappers, simulation + oracle harness | Determinism gate through SlateDB |
| 1 | IVM | Single-shard core: filter/project/map, aggregates, MIN/MAX | Oracle green at 100k seeds; crash-replay bit-identical |
| 2 | IVM | SQL frontend, joins, set operations | Plain-SQL view DDL end-to-end; TPC-H join parity |
| 3 | IVM | Essential operators + single-shard correctness soak | TPC-H 22/22 incremental == batch |
| 4 | IVM | Multi-shard execution + exchange | Distributed output bit-identical to single-shard |
| 5 | IVM | Frontier protocol & progress tracking | Multi-rate joins correct; bounded shuffle storage |
| 6 | IVM | Fault tolerance & exactly-once | 24h chaos: zero loss, zero duplicates |
| 7 | Postgres | PostgreSQL wire gateway: read, DML, subscribe | `psql` + ORM round-trip; read-your-writes |
| 8 | Both | Minimal connectors + production hardening | 24h E2E exactly-once; security review |

Distribution (Phases 4–6) deliberately precedes the Postgres layer (Phase 7):
the gateway must serve a correct, fault-tolerant, distributed engine, not a
moving target.

---

## Phase 0 — Foundation

**Goal**: A workspace that builds, tests, and simulates. The two harnesses that
gate every later phase — the **oracle** and the **deterministic simulator** —
exist and work before any operator is written.

**Deliverables**

- Cargo workspace with the focused crate set:
  - `rockstream-types` — shared types: timestamp, frontier (antichain), Z-set
    row `(row, weight: i64)`, schema. Catalog key encoders include
    `namespace_id` from day one (DESIGN.md §5.2).
  - `rockstream-storage` — SlateDB wrappers (`ShardDb`): key encoders/decoders,
    `WriteBatch` builders, `DbReader` snapshot reads, checkpoint helpers, WAL
    tail reader with a listing cache, and scan-and-delete cleanup. **No** range
    deletion anywhere.
  - `rockstream-sim` — the `Runtime` trait abstracting `now`, `spawn`, `sleep`,
    `object_store`, and `network`; a production `TokioRuntime` and an in-memory,
    seeded `SimRuntime`; the `buggify!()` macro (no-op in release). Threaded
    through every other crate; no I/O may bypass it.
  - `rockstream-oracle` — a batch reference engine (DataFusion) plus a
    property-test harness asserting `incremental(q, Δ) == batch(q, accumulated)`.
  - `rockstream-plan` — `PlanNode` (PlanIR, IVM.md §5) and physical `OpNode`.
  - `rockstream-diff` — the `DiffCtx` differentiation pass (IVM.md §6–7).
  - `rockstream-ops` — the `Operator` trait and per-operator implementations.
  - `rockstream-sql` — SQL frontend (Phase 2).
  - `rockstream-runtime` — worker, scheduler, epoch-commit coordinator, exchange.
  - `rockstream-control` — control-plane service.
  - `rockstream-gateway` — Postgres wire gateway (Phase 7).
  - `rockstream-connectors` — connectors (Phase 8).
  - `rockstream-cli` — the `rockstream` binary.
- CI: `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`,
  `cargo deny`, coverage.
- `rockstream-errors`: every error is an `RS-XXXX` code; CI fails on any
  returned `Error` or logged `error!` without a code.
- Hot-path metrics emitter from day one (object-store latency, SST count,
  WAL replay bytes, frontier lag) so "why is it slow?" is answerable on a laptop.
- Dev container with SlateDB, MinIO, and Postgres preinstalled.

**Testing**

- Storage API validation tests proving only supported SlateDB features are used
  (single-writer fencing, `WriteBatch`, `DbReader`, checkpoints, `MergeOperator`,
  TTL, compaction filters, WAL reader, segments).
- The oracle harness can drive a no-op pipeline and confirm equivalence.

**Exit criteria**

- `cargo test --workspace` passes.
- `make e2e` brings up a local cluster (MinIO + 1 worker + 1 control) and tears
  it down.
- **SlateDB determinism gate**: a write-heavy `ShardDb` workload (mixed `put`,
  `merge`, `WriteBatch` commit, `DbReader` read, WAL tail) run twice under
  `SimRuntime` at the same seed produces bit-identical key-value state and WAL
  sequence. Any non-deterministic SlateDB background surface must be found and
  constrained before Phase 1 begins. This is the proof that deterministic
  simulation holds *through* SlateDB, not merely around it.

---

## Phase 1 — Single-Shard IVM Core

**Goal**: A single-process engine that incrementally maintains views built from
filter, projection, map, algebraic aggregates, and non-invertible aggregates.
Plans are hard-coded (the SQL parser arrives in Phase 2).

**Z-set algebra note**: the engine's only merge semantics at this stage are the
Z-set weight group (`(row, weight)` addition with negative weights for
retractions) and the SUM/COUNT monoid for aggregates. There is no general
merge-law catalog and no user-facing CRDT surface; those are out of scope.

### IVM-1 — Filter / Project / Map

- `PlanNode` variants: `Source`, `Filter`, `Project`, `Map`, `ViewSink`,
  `Exchange` (stub).
- `DiffCtx::diff` with the linear-operator rules for filter/project/map.
- `Operator` trait, `EpochOutput`, and an `OperatorTask` event loop: one async
  task per operator instance emitting fragments to a shard-level commit
  coordinator.
- **Shard-level group commit**: coalesce ready operator fragments into one
  atomic SlateDB `WriteBatch` covering state, view output, and frontiers.
- Per-shard SlateDB namespaces (`op_state`, `view_output`, `shard_meta`) via
  `ShardDb`.
- An async, ownership-free scheduler driven by data arrival and frontier
  updates, with credit-based backpressure.
- Embedded runtime profile: `rockstream start --role=all --storage=./data` wires
  control, worker, and gateway in-process; the single-shard hot path issues no
  gRPC and creates no shuffle objects.
- A built-in row-generator source (`GENERATE ROWS ... RATE ...`) and a
  `Vec<RecordBatch>` delta source with the `_weight: i64` convention.

**Testing**: oracle property test for
`SELECT a, b * 2 AS c FROM t WHERE c > 10` over random insert/delete sequences.

### IVM-2 — Algebraic aggregates (SUM / COUNT / AVG)

- `Aggregate` PlanNode + the aggregate arrangement (DESIGN.md §6.2).
- `diff_aggregate`: group delta by key, `merge` `(Δsum, Δcount)` into the
  arrangement, read previous/current, emit `(old, -1) ⊎ (new, +1)`.

**Testing**: oracle property test for
`SELECT k, SUM(v), COUNT(*), AVG(v) FROM t GROUP BY k` over random
insert/update/delete and group-churn sequences.

### IVM-3 — Non-invertible aggregates (MIN / MAX)

- Indexed-multiset arrangement (DESIGN.md §6.3) + cached extremum.
- `diff_minmax`: insert merges the multiset and updates the cache on a new
  extremum; delete of the extremum prefix-scans the sorted multiset for the
  replacement.

**Testing**: oracle property test for groups churning across MIN/MAX
transitions; the cached extremum must match the multiset's true extremum after
every batch.

**Exit criteria for Phase 1**

- Throughput floors on a laptop (in-memory object store): ≥1M rows/s filter;
  ≥200k rows/s `GROUP BY SUM`; ≥100k rows/s `GROUP BY MIN`. On local-filesystem
  object store: ≥500k / ≥100k / ≥50k respectively.
- **Crash-replay**: `kill -9` injected mid-`WriteBatch`; on restart the shard
  reads its persisted frontier, reprocesses the failed epoch, and produces
  output bit-identical to an uninterrupted run.
- Group commit reduces durability events ≥5× vs. one commit per operator.
- Oracle property tests run green for ≥100k randomized scenarios per operator
  combination.

**Operability**

- Single binary; `rockstream start --storage=./data` is zero-config.
- `SimRuntime` adopted everywhere: every operator, scheduler, and storage call
  site is parameterised on the `Runtime` trait; tests use seeded `SimRuntime`.
- `buggify!()` on every race-prone path (partial `WriteBatch` failure, fenced
  commit, manifest publish delay).

---

## Phase 2 — SQL Frontend & Joins

**Goal**: Real SQL in, incrementally-maintained joins and set operations out.

**SQL frontend (always-on hereafter)**

- `rockstream-sql`: DataFusion-based parse → bind → logical optimize.
- Custom DataFusion extension nodes for incremental operators
  (`IncAggregate`, `IncJoin`, `IncDistinct`).
- Lowering pass: `LogicalPlan` → `PlanNode`.
- Distribution pass: annotate each node with `partition_key`, insert `Exchange`
  where partitioning differs (no-ops in single-shard; prep for Phase 4).
- Schema-version catalog in `control: schema/`; compatible changes accepted
  online, breaking changes blocked with `RS-1002` pending a blue/green plan.
- SQL coverage lands incrementally: filter → project → group-by → inner join →
  outer/semi/anti → set ops → subqueries (decorrelated) → CASE/CAST.

### IVM-4 — Inner equi-join

- `InnerJoin` PlanNode + dual arrangements (DESIGN.md §6.4).
- Stable source-derived `row_id` so replay rewrites the same arrangement key.
- DBSP-native two-arrangement join with the correct bilinear expansion
  (`ΔL ⋈ R` split into insert/delete parts, plus the `L₀ ⋈ ΔR` and correction
  terms). Arrangements reflect epoch `e-1` during processing; updated at commit.
- Distribution pass inserts `Exchange` when the join key differs from the
  child's partition key (no-op in single shard; verified by tests).

**Testing**: oracle property test for random 3-way joins; TPC-H Q1, Q3, Q5, Q6
plan-level parity.

### IVM-5 — Outer / semi / anti joins

- `LeftJoin`, `RightJoin`, `FullJoin`, `SemiJoin`, `AntiJoin`.
- One extra arrangement per side tracking unmatched rows so transitions emit the
  right NULL-padding retractions.

**Testing**: TPC-H Q11, Q21 (the SemiJoin corner cases) against the oracle.

### IVM-6 — Distinct / Union / Intersect / Except

- Weight-based distinct arrangement (DESIGN.md §6.6); emit deltas on
  zero-crossing transitions (`0→+n ⇒ +1`, `+n→0 ⇒ −1`).
- Intersect / Except with both set and bag semantics.

**Testing**: oracle property tests on set semantics with random sequences.

**Exit criteria for Phase 2**

- A user can submit
  `CREATE VIEW v AS SELECT ... FROM t1 JOIN t2 ON ... GROUP BY ...`; the engine
  compiles, deploys, and maintains it incrementally.
- TPC-H Q1, Q3, Q5, Q6, Q11, Q21 pass plan-level parity (row-level parity is
  Phase 3's soak).
- The oracle harness covers every operator combination implemented so far.

**Operability**

- `EXPLAIN INCREMENTAL` prints the annotated operator tree against live stats.
- `EXPLAIN INCREMENTAL ESTIMATE` runs the planner/cost model without deploying
  and reports predicted state size and per-operator `epoch_ms`.

### Gate: Storage Operational Budget (before Phase 3)

Prove the SlateDB budgets (DESIGN.md §5.4) hold under real object-store latency
at shard sizes >1 GB: PUT/GET/LIST p99 at 1 GB and 5 GB; manifest write cadence
under steady and bursty load; WAL listing-cache hit ratio >99%; measured write
amplification; `min_epoch_ms` floor prevents manifest churn. Any budget exceeded
by >2× requires a tracked mitigation before advancing.

---

## Phase 3 — Essential Operators & Single-Shard Correctness Soak

**Goal**: Complete the operator set needed for the analytical 1.0 subset, then
prove the single-shard engine is production-correct *before* distribution.

**Essential operators only** (advanced windowing, recursion, and lateral
functions are deferred — see [Out of Scope](#out-of-scope-for-this-plan)):

### IVM-7 — Window functions (partition-recompute strategy)

- `Window` PlanNode + ordered arrangement (DESIGN.md §6.7).
- Partition-based recomputation: when any row in a partition changes, re-evaluate
  the partition and diff against the previously-emitted output.
- Implement ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, and sliding SUM/AVG.
- `EXPLAIN INCREMENTAL` flags windows over partitions large enough that
  recomputation exceeds the freshness target.

### IVM-8 — Tumbling time windows

- TUMBLE windows keyed by `window_id`, with event-time TTL on arrangement
  entries and a frontier-aware compaction filter that removes state only after
  event-time expiry. *(HOP/SESSION deferred.)*

### IVM-9 — Top-K

- Value-descending arrangement maintaining `K + ε` entries; on delete of a
  top-K entry, scan one past `K` to refill and emit displacement deltas.

### IVM-10 — Bootstrap / snapshot mode

- Source connectors emit each base-table row once at weight `+1`, either in one
  epoch or streamed across bootstrap epochs; the circuit processes them like any
  delta. Output frontier advances past `bootstrap_complete` only when every
  chunk is ingested.

### IVM-11 — View-on-view DAG

- `ViewRef` PlanNode subscribing to an upstream view's `view_output/` CDC via
  `WalReader`. Diamond consistency is structural (every multi-input operator's
  frontier meet enforces it); no extra mechanism is needed.
- Cycle detection at compile time (Kahn's algorithm).

**Single-shard correctness soak (the IVM "done" gate)**

- **TPC-H 22/22**: all 22 queries (SF=0.01) run incrementally with
  bit-identical results vs. DataFusion batch.
- **Random query fuzzer**: a SQL generator over a synthetic schema runs each
  query incrementally and as batch; any divergence fails.
- **Deterministic simulation**: ≥100k seeds with bit-identical output across
  reruns; paired assertions on every durable write, frontier advance, and epoch
  replay.
- **Storage correctness audit**: prove every cleanup path works without range
  deletion and every compaction filter is snapshot-safe (failing tests for
  unsafe resurrection cases).
- **Performance regression suite** in CI (fails on >10% regression).

**Exit criteria for Phase 3**

- 22/22 TPC-H queries: identical results vs. batch.
- ≥10× measured speedup vs. batch at a 1% change rate.
- Fuzzer runs ≥1 hour without finding divergence.
- DST harness passes 100k seeds bit-identically.

After Phase 3 the IVM engine is feature-complete and correct for a single shard.
Phases 4–6 make it distributed, durable, and fault-tolerant.

---

## Phase 4 — Multi-Shard Execution & Exchange

**Goal**: Move from single process to distributed execution with no change in
results.

**Deliverables**

- **Shard manager**: a worker owns N shards, each with its own `Arc<Db>`; lease
  acquisition via control-plane transactions; SlateDB fence-epoch enforcement
  (two writers cannot commit to the same shard — verified by integration test).
- **Exchange subsystem**:
  - gRPC direct shuffle (`proto/shuffle.proto`).
  - Path classifier: `elided`, `loopback`, `direct`, `durable` (DESIGN.md §7.5).
  - Same-worker loopback over bounded in-process channels, keeping durable
    outbox/inbox metadata for replay.
  - Worker-to-worker stream multiplexing (one stream per peer per traffic class).
  - Coalesced durable shuffle objects with an index footer; receivers never LIST
    the shuffle prefix on the hot path.
  - Credit-based backpressure.
- **Rendezvous hashing** with virtual nodes; property tests for re-balance
  minimality.
- **Distribution-aware execution**: operator instances addressed by
  `(op_id, instance_idx)`; locality-aware placement; cross-shard arrangement
  reads forbidden on the hot path (the distribution pass guarantees every
  stateful operator's inputs share its `partition_key`).

**Testing**

- Re-run the entire Phase 1–3 oracle + TPC-H suite against the distributed
  cluster; results must be **bit-identical** to single-shard runs.
- Loopback path produces identical output to direct gRPC with zero network calls
  for co-located exchanges.
- 1,000-shard exchange stress test stays within connection and shuffle-object
  budgets (connection count bounded by worker count, not shard count).

**Exit criteria**

- A 16-shard cluster on ≥4 hosts (real network) runs TPC-H with near-linear
  throughput for partitionable queries; skew and shuffle limits documented.
- Killing one worker re-leases its shards to another; processing continues with
  output equal to an uninterrupted run.

> **Simulation-compensated waiver**: if a 4-host real-network test is not
> feasible at phase completion, it may be waived only with `SimNetwork` latency
> injection (median 10 ms, p99 100 ms, ±5 ms jitter) across ≥10,000 seeds, a
> documented rationale, and a commitment to run the real-network test before
> Phase 8. The real test becomes blocking before production hardening.

---

## Phase 5 — Frontier Protocol & Progress Tracking

**Goal**: Correct progress tracking across multi-input operators and bounded
shuffle storage.

**Deliverables**

- `rockstream-types::Frontier`: full antichain with product-order timestamps;
  property tests for meet/join/advance.
- Per-shard frontier reporter bundled into every epoch commit.
- Control-plane frontier aggregator: consumes per-shard summaries, computes the
  per-operator cluster frontier, publishes to `frontier/op_id`, and can rebuild
  summaries after worker loss.
- Operator frontier consumers use the input frontier to close windows, detect
  convergence, and release shuffle inbox entries.
- Exchange GC: senders observe consumed frontiers and reclaim outbox/inbox
  entries via bounded scan-and-delete or snapshot-safe compaction filters.

**Testing**

- A join over two sources at different ingestion rates produces correct output:
  no premature emission, no infinite buffering.
- Frontier aggregation stress test over thousands of shards × hundreds of
  operators converges without the control plane subscribing to each shard feed.
- `SimRuntime`: arbitrary frontier-report reorderings converge to the same
  cluster vector frontier as serial delivery.

**Exit criteria**

- Multi-rate join correctness holds under simulation and integration.
- Shuffle storage usage is bounded under sustained throughput.

**Operability**

- The control plane derives `min_epoch_ms`, `max_epoch_ms`, and initial
  parallelism from each pipeline's declared freshness target (simple, bounded;
  no advanced auto-tuning). Every derived value and any manual override is
  audit-logged.

---

## Phase 6 — Fault Tolerance & Exactly-Once

**Goal**: Survive any single-node failure; deliver exactly-once end-to-end.

**Deliverables**

- **Cluster checkpoint coordinator**: barrier injection at sources; bounded
  barrier alignment at multi-input operators (tied to shuffle credits — exhausted
  credits backpressure rather than grow memory); one per-shard `Checkpoint` after
  all local operators commit through the barrier; atomic cluster-checkpoint
  commit; old-checkpoint GC.
- **Recovery driver**: from a cluster checkpoint, bring up every shard via
  `DbReader` pinned to its per-shard checkpoint, then re-elect writers.
- **Exactly-once sink protocol**: `pre_commit(epoch, rows)` / `commit(epoch,
  checkpoint_id)`; transactional Kafka sink; object-store sink via
  `_pending/` → atomic rename.
- **Connector offset integration**: sources record offsets in the epoch commit
  batch; recovery replays from recorded offsets. Per-connector strictly-increasing
  `source_epoch` with a persisted partition→offset map (DESIGN.md §8.1.1).
- **Worker self-fencing** (DESIGN.md §11.6): a worker that cannot reach the
  control plane for `self_fence_after` terminates before the new owner acquires
  its leases.
- **Object-store brownout handling** (DESIGN.md §11.7): buffer up to
  `local_buffer_max_epochs` then backpressure sources; transparent recovery.

**Testing (simulation-first)**

- Under `SimRuntime` with `BUGGIFY` enabled:
  - Every epoch-commit partial-failure permutation leaves the cluster frontier
    monotonic and exactly-once intact.
  - 2PC sink crash points (pre-commit / between / commit) all recover
    idempotently.
  - Network-partition self-fencing: the partitioned worker terminates before the
    new owner commits.
  - Object-store brownout: a 50-epoch blackout produces zero loss/duplicates.
- **Chaos**: random process kills, partitions, disk-full, and object-store
  throttling, with output compared against a non-faulty reference.

**Exit criteria**

- 24-hour chaos run on a 32-shard cluster with continuous input/output: zero
  data loss, zero duplicates, output matches reference.
- Recovery from full cluster outage in <60 s for state <1 TB.
- Recovery-time invariants hold at `target_shard_state_bytes` (default 20 GB):
  failure detection ≤5 s p99, single-shard reassignment ≤30 s p99, freshness
  recovery ≤60 s p99 — measured under the chaos suite.
- ≥100k seeded `SimRuntime` runs pass; failing seeds are checked in as
  regression tests. A scheduled CI job runs new seeds against `main` around the
  clock from here on.

> A minimal elasticity slice (online shard split at a size threshold and a
> worker-drain protocol) may be implemented here if needed for the soak, but is
> otherwise deferred. Full elasticity (merge, clone/blue-green, skew rebalancing)
> is out of scope for this plan.

---

## Phase 7 — PostgreSQL Wire Gateway

**Goal**: Serve and write materialized views over the Postgres wire protocol.
This is the second pillar, built on the now-correct, fault-tolerant engine.

**Design constraint**: the gateway's `ViewReader` trait is defined with a
`ViewReadStrategy` enum (`HotOnly` | future `TwoTier`), but only `HotOnly` is
implemented. This keeps a future cold tier addable without a gateway rewrite,
at zero cost now.

### 7.1 — Postgres read gateway (core)

- **pgwire gateway** (stateless, horizontally scalable): startup, simple query,
  extended query (parse/bind/execute), copy-out, terminate flows.
- Routes point lookups and range scans to the correct shards via `DbReader`,
  pinning all handles for one query to a single published vector frontier
  (causally consistent multi-shard reads).
- Ad-hoc SQL over views via DataFusion on a snapshot.
- Connection pooling, query timeouts, rate limiting.
- **Postgres catalog stubs** for ORM compatibility: `pg_catalog.pg_tables`,
  `pg_views`, `pg_class`, `pg_attribute`, `pg_namespace`, `pg_type`;
  `information_schema.tables` / `.columns`; `SHOW server_version`,
  `SHOW transaction_isolation`, `SET search_path`.
- **Postgres type OID mapping**: every view column carries a native OID in the
  row-description message so JDBC/ODBC drivers decode without round-trips.
- **Isolation levels** (DESIGN.md §12.6): `READ COMMITTED` (each statement pins
  the latest published vector frontier); `REPEATABLE READ` (`BEGIN` captures a
  vector frontier held for the transaction); `SERIALIZABLE` rejected with
  `RS-2003`.
- **Inline views**: `CREATE VIEW` / `CREATE OR REPLACE VIEW` / `DROP VIEW` store
  a definition without operator state; the binder expands them at plan time;
  cycle detection returns `RS-1011`.

**Testing / exit criteria**

- `psql` connects and `SELECT * FROM my_view LIMIT 10` returns in <10 ms.
- SQLAlchemy (or equivalent ORM) reflects view schemas without error.
- `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` returns `RS-2003`.
- `CREATE MATERIALIZED VIEW mv AS SELECT * FROM v` inlines `v` and starts IVM.

### 7.2 — Direct-write DML (internal source)

- **Internal direct-write source connector** (DESIGN.md §13.5):
  `INSERT`/`UPDATE`/`DELETE` over the wire append to a per-connection buffer;
  `COMMIT` flushes an atomic Z-set delta via `WriteBatch` to a base-table shard
  and receives the shard's next `source_epoch`; `ROLLBACK` discards the buffer.
- **`INSERT ... RETURNING`**: single round-trip write + read-back, including the
  multi-row `INSERT ... SELECT ... RETURNING` form.
- **Idempotency**: a direct write to a non-idempotent aggregate must carry an
  exactly-once source-epoch envelope or a caller idempotency key; missing both
  returns `RS-2007`. The idempotency-key table is per-shard and time-bounded.

**Testing / exit criteria**

- `psql` runs `INSERT INTO t VALUES (...); COMMIT` and the view reflects it
  within the freshness target.
- A non-idempotent write missing both an envelope and a key returns `RS-2007`.

### 7.3 — Subscribe & freshness (read-your-writes)

- **Subscribe API**: a gRPC/streaming endpoint tailing view changes via
  `WalReader`; the gateway proxies it (raw shard access never exposed). Each
  change row carries `mz_timestamp`, `mz_diff` (+1/−1), and the projected
  columns; updates are retraction/insertion pairs. Supports `AS OF NOW WITH
  SNAPSHOT`, `AS OF EPOCH <n>` (within `CHANGE_RETENTION`, default 1 h),
  server-side `WHERE`, and column projection.
- **Freshness tokens**: query responses return the vector frontier used;
  `wait_for=<token>` gives read-your-writes with a timeout and an explicit
  satisfied/not-satisfied response.
- **Session-scoped read-your-writes**: after `COMMIT`, the session's
  `last_written_epoch` is recorded and subsequent `SELECT`s in the same
  connection auto-apply `wait_for` with no client action.

**Testing / exit criteria**

- Read-your-writes demo passes; `wait_for=<token>` resolves within the SLO.
- A subscribe stream survives a gateway restart with no data loss.
- `SUBSCRIBE orders_mv AS OF NOW WITH SNAPSHOT` delivers the current state then
  live deltas with no gap; `WHERE`/projection filter correctly; `AS OF EPOCH`
  outside retention returns `RS-2005`.

### 7.4 — Gateway cross-cutting

- **Authentication/authorization**: OIDC/bearer-token or mTLS at the gateway;
  per-view RBAC (`viewer` / `pipeline_owner` / `admin`) in the control-plane
  catalog; `--auth=off` for local dev. Namespace isolation enforced.
- **Cross-shard partial aggregation pushdown** (DESIGN.md §12.3.1): for
  `SELECT agg, key FROM mv GROUP BY key`, push partial aggregation to shards so
  the gateway receives O(distinct groups × shards) rows, not O(view rows).
  `EXPLAIN` names the effect.

**Exit criteria for Phase 7 (Postgres pillar complete)**

- An application using a standard Postgres driver can create a materialized view,
  write rows, read them back with read-your-writes, and subscribe to changes —
  end to end, against the distributed fault-tolerant engine.
- Unauthenticated requests are rejected; cross-namespace access is denied; the
  audit log records `actor` on every control action.

---

## Phase 8 — Minimal Connectors & Production Hardening

**Goal**: Connect to the real world with a small, fully-tested connector set,
and prove production readiness. Breadth is intentionally avoided.

**Connectors (minimal set only)**

- **Sources**: the internal direct-write source (Phase 7), Kafka
  (consumer-group, offsets in control plane), and Postgres logical replication
  (`pgoutput`).
- **Sinks**: Kafka (transactional) and object-store/Parquet (idempotent rename).
- **Connector contract** (DESIGN.md §13.3): `discover_schema`, `start_snapshot`,
  `poll_delta`, `commit_offset`, `prepare`, `commit`, `abort`, `should_flush`,
  opaque `OffsetToken`, `watermark`, and `credits_available()`.
- **Dead-letter queue**: per-record decode errors become `RS-1003` events routed
  to a DLQ sink, surfaced via a `rockstream_catalog.dead_letter_queue` table with
  replay/dismiss commands; the IVM core never sees malformed records.
- **Schema evolution**: connectors publish schema versions before data;
  incompatible drift returns `RS-1002` and blocks before any offset advances.

**Hardening**

- **Observability**: Prometheus metrics (per-operator throughput/latency/state,
  shuffle bytes, frontier lag, checkpoint duration); OpenTelemetry per-epoch and
  source-to-sink traces; structured JSON logs.
- **Admin CLI**: `pipeline create/start/pause/delete`, `cluster status/scale`,
  `cluster workers {list,drain,status}`, `shard list/migrate`,
  `checkpoint list/restore`, `debug arrangement <view> <op> <key>`.
- **Simulation CI gate**: every commit runs N seeded `SimRuntime` executions
  across the coordination suite with `BUGGIFY`; pre-release scales to millions of
  seeds. The gate checks both safety (oracle divergence, invariant assertions)
  and liveness (a recoverable fault commits a new epoch within the recovery
  budget or surfaces a named degraded state).
- **Error-code documentation**: every `RS-XXXX` has a published page with cause,
  detection signal, remediation, and a `next_steps` field (CI-enforced).
- **Security**: mTLS everywhere (worker↔control, worker↔worker, gateway↔client)
  with documented rotation; at-rest encryption via object-store features;
  `CREATE SECRET` with envelope encryption and worker-side resolution; auth
  integration tests.
- **Rolling-upgrade**: storage-format version gate on shard open (`RS-5001`);
  N→N+1 upgrade one worker at a time with no epoch loss.

**Exit criteria (production readiness)**

- **End-to-end exactly-once**: Postgres CDC → RockStream IVM → Kafka, sustained
  at 100k rows/s for 24 hours, zero loss, zero duplicates.
- 99.9%+ availability over a multi-day soak on a 32–64-shard cluster.
- Documented disaster-recovery procedure executed successfully.
- Independent security review passes.
- The public surface (SQL subset, CLI, system tables, metrics, error codes) is
  audited, classified stable/internal, and minimal.

---

## Cross-Cutting Concerns

### Performance targets

| Workload | Single-shard | 64-shard cluster |
|---|---|---|
| Filter+project throughput | 5M rows/s | 250M rows/s |
| GROUP BY SUM throughput | 1M rows/s | 50M rows/s |
| Equi-join throughput | 500k rows/s | 25M rows/s |
| End-to-end frontier lag (source→view) | <100 ms | <200 ms |
| Recovery time (1 TB state) | n/a | <60 s |

### Top risks and mitigations

| Risk | Mitigation |
|---|---|
| SlateDB single-writer is too restrictive | Sharding; per-shard batched writes. |
| Per-operator commits overwhelm object storage | Shard-level group commit; `min_epoch_ms`/`min_epoch_bytes` floors; commit-cost benchmark. |
| No range-delete API | Scan-and-delete or snapshot-safe compaction filters; absence asserted by test. |
| Compaction filters break snapshot safety | Filters are retention-only; explicit deletes for correctness; safety proofs + stale-reader tests. |
| Frontier protocol bugs | Heavy property testing; pure-logic reference for comparison; `SimRuntime` reorder tests. |
| Object-store cost dominates | Local SST cache; coalesced writes; WAL listing cache. |
| Exactly-once gaps under failure | 2PC sink protocol; source-epoch offset map; `SimRuntime` crash-point coverage; 24h chaos. |
| Distributed result divergence | Re-run the full single-shard oracle/TPC-H suite against the cluster; require bit-identical output. |
| pgwire driver incompatibility | Catalog stubs + native type OIDs; ORM reflection tests in CI. |

### Open questions (resolved early via prototype)

1. **Exchange transport**: start gRPC; benchmark and revisit.
2. **Arrangement state format**: Arrow IPC per value vs. Arrow Row for
   point-access arrangements; benchmark in Phase 3.
3. **Control-plane HA**: a 3/5-node Raft group elects one control-SlateDB writer
   lease; followers serve catalog reads via `DbReader`. Hardened in Phase 8.
4. **Frontier-aggregator staleness budget**: async with `frontier_agg_interval`;
   pick a default and confirm it meets window-close, shuffle-GC, and freshness
   SLOs in Phase 5.

---

## Why This Plan Is Narrower Than `IMPLEMENTATION_PLAN.md`

The original plan carries 14 phases through v1.0 and beyond, including
user-facing CRDTs, cold-tier Iceberg/Delta sinks, an Iceberg REST catalog
server, secondary indexes, optimistic transactions, scatter-pruning statistics,
and a broad connector matrix. This plan keeps only what the two core pillars
require and sequences it so that:

1. The IVM engine is **proven correct on one shard** (Phase 3) before it is
   distributed.
2. The engine is **distributed, progress-tracked, and fault-tolerant**
   (Phases 4–6) before the Postgres layer depends on it.
3. The **Postgres wire layer** (Phase 7) is built once against a stable engine.
4. **Production hardening and a minimal connector set** (Phase 8) close it out.

The deferred capabilities are not lost: every one of them slots behind an
abstraction this plan already builds (`ViewReader`/`ViewReadStrategy`, the
connector contract, the merge/Z-set algebra, the gateway), so they can be added
post-1.0 without reworking the core.

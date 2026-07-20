# RockStream: Massively-Parallel Incremental View Maintenance on SlateDB

A design for a horizontally-scalable, full-SQL incremental view maintenance (IVM)
system inspired by DBSP and differential dataflow theory
— built on a mesh of SlateDB instances backed by
object storage.

> **Status**: Design v3.28. v3 reframed the engine around DBSP-native operators
> with pg_trickle as a correctness oracle and SlateDB's real API surface as a
> hard constraint. v3.1 added causal time, async scheduling, and explicit
> SlateDB operational budgets. v3.2 added the operability foundation
> (deployment ladder, `EXPLAIN INCREMENTAL`, frontier-lag diagnostics, four
> knobs). **v3.3 raises operability to a first-class system property**:
>
> 1. **SLO-driven, not knob-driven.** Operators state a freshness target;
>    the system tunes epoch sizing, parallelism, and scheduling to meet it.
>    Manual knobs remain as overrides, not primary controls.
> 2. **Self-tuning by default.** Adaptive parallelism, adaptive epoch
>    sizing, and adaptive scheduling are on out of the box. Operators set
>    intent (SLOs, quotas, priorities); the control plane decides
>    mechanism.
> 3. **One binary, one CLI, one config.** Every node role is a flag on the
>    same `rockstream` binary. The CLI surface is workloads, schemas, views,
>    and sources — never shards or antichains.
> 4. **Unable to surprise you.** Cost preview before deploy
>    (`EXPLAIN INCREMENTAL ESTIMATE`), enforced per-workload quotas, an
>    auditable event log of every control action, a single-command support
>    bundle, and a documented error-code taxonomy. Views that cannot
>    meet their SLO degrade with a named reason instead of failing
>    silently.
>
> Carrying forward from earlier revisions:
> 1. **Time is causal, not global** (P13).
> 2. **Scheduling is async** (P14).
> 3. **SlateDB is respected as-is** (P12).
>
> **v3.4 is a coherence and hardening pass.** It resolves the control-plane
> HA contradiction, specifies merge-backed read semantics, outer-join state,
> stable row identity, schema evolution, connector contracts, bounded barrier
> alignment, shard-level checkpointing, shuffle fan-out limits, query freshness
> tokens, backfill/recovery lifecycle states, and the production storage
> profiles needed for predictable operation.
>
> **v3.5 fills the remaining design gaps**: authentication and authorization,
> source statistics and cost-model inputs for `EXPLAIN ESTIMATE`, cluster
> bootstrap and worker discovery, storage format versioning and rolling
> upgrades, late-data policy connected to the frontier model, fault-driven
> shard-state recovery path, the adaptive scheduling loop replaced with a
> concrete source-rate throttle loop, and multi-region added as an explicit
> non-goal.
>
> **v3.6 adds Postgres wire protocol compatibility**: pgwire gateway layer,
> `READ COMMITTED` / `REPEATABLE READ` isolation via the vector-frontier model,
> `pg_catalog` / `information_schema` stubs for ORM compatibility, Postgres type
> OID mapping, and an internal source connector so clients can write rows
> directly without an external Kafka or Postgres. `SERIALIZABLE` is explicitly
> out of scope (§1.1, §12.6). Positioning: a distributed streaming-SQL
> platform, not a Postgres drop-in.
>
> **v3.7 is a future-proofing pass inspired by FoundationDB**: deterministic
> simulation testing as a first-class testing strategy (§17), an explicit
> recovery-time invariant (§11.5), proactive shard splitting at a target size
> (§10.6), separation of the frontier aggregator from the Raft control plane
> for scale-out (§3), per-connector source-epoch vector semantics for
> multi-partition sources (§8.1.1), and a view output retention policy (§5.7).
>
> **v3.8 closes three gaps in the connector contract** (§13.3) before any
> connector is implemented: sources must emit an event-time watermark
> alongside delta batches so time-window operators (§6.9) can close windows
> correctly; the source offset is an opaque `OffsetToken` (serialisable
> bytes) so multi-partition sources like Kafka and Kinesis fit without
> contract changes; and the source operator exposes a `credits_available()`
> signal so the connector's poll rate is governed by downstream backpressure
> rather than runaway ingestion.
>
> **v3.9 adds two lakehouse-driven connector contract additions** (§13.3):
> partition-filter pushdown lets the planner hand column predicates to
> `start_snapshot` and `poll_delta` so Iceberg/Delta/Hudi connectors can
> skip non-matching partition directories rather than scanning and discarding
> them in the operator layer; and a `should_flush` signal on sink connectors
> lets file-format sinks (Iceberg, Delta Lake, Parquet) buffer across epochs
> before physically writing, solving the small-files problem without
> sacrificing exactly-once semantics.
>
> **v3.10 adopts TigerBeetle-style safety discipline** (§17): deterministic
> simulation remains the foundation, but the simulator is backed by paired
> assertions on durable/network boundaries, an explicit fault model,
> liveness checks tied to the recovery SLOs, continuous long-run simulation
> soak, and a "bounded everything" rule for queues, buffers, and scan windows.
>
> **v3.11 adds three foundation-level changes**: namespace-scoped catalog keys
> (§5.2) required from day one for multi-tenancy without storage migration;
> a worker drain protocol (§10.7) enabling graceful scale-in; and cluster
> autoscaling signals with worker capacity model (§10.8) bridging the control
> plane to infrastructure autoscalers.
>
> **v3.12 fills distributed-systems gaps** identified by systematic review of
> prior art: network partition self-fencing so a partitioned-but-alive worker
> cannot race the new owner (§11.6); object store brownout handling with local
> buffering and backpressure rather than data loss (§11.7); thundering-herd
> mitigation via staggered startup jitter and lease-grant rate limiting
> (§11.8); cooperative scheduling yield points so expensive operator epochs
> cannot starve heartbeats (§9.3); wire-protocol version skew contract for
> rolling upgrades (§5.5); tombstone accumulation bounds and proactive
> compaction (§5.4); cross-shard partial aggregation pushdown (§12.3.1); and
> the `debug arrangement` IVM debugger (§14.7.1).
>
> **v3.13 adds performance elasticity across the workload range**: an
> embedded/local runtime profile that elides distributed boundaries for tiny
> workloads (§3.1); exchange elision, same-worker loopback, pre-shuffle
> combiners, and hierarchical exchange for lower latency and lower network
> amplification (§7.5); exact hierarchical frontier summaries for very large
> clusters (§8.6); and concrete hot-key virtual buckets for skewed joins and
> aggregates (§10.5).
>
> **v3.14 adds temporal queries, system introspection, and read-path caching**:
> historical view queries via `AS OF EPOCH <n>` / `AS OF TIMESTAMP <t>`
> bounded by the view's retention window (§12.4.1); the `rockstream` system
> schema exposing epoch history, pipeline state, schema evolution, and the
> audit log as queryable SQL tables (§12.6.1); and a per-worker arrangement
> segment cache that exploits SST immutability between checkpoints to reduce
> cross-shard read latency for joins and gateway queries (§5.4).
>
> **v3.15 documents the two-tier view storage decision** (§12.7): the storage
> model is row-oriented at rest (SlateDB LSM) and columnar in flight (Arrow),
> which is optimal for IVM's primary workload but slow for full-collection scans
> and ad-hoc analytics not aligned with the partition key. A cold Parquet/Iceberg
> tier (periodic columnar snapshots written via the existing checkpoint path) is
> the natural extension; the gateway merges hot LSM tail + cold snapshot via a
> union-with-deduplication on `row_id`. The cold tier is **not** a Phase 9
> deliverable, but the `ViewReader` gateway abstraction is made cold-tier-aware
> in Phase 9 so the cold tier can be added later without a gateway rewrite.
>
> **v3.16 fills in the external consumption story**: the cold tier writes valid
> Iceberg v2 tables (§12.7.2) to object storage so downstream tools (DuckDB,
> Trino, Spark) can query view snapshots directly — no RockStream
> in the read path. §13.6 adds the Iceberg/Delta cold-tier sink connector as a
> first-class built-in, with `CREATE SINK` syntax, the checkpoint-to-manifest
> lifecycle, and the external consumption contract. This firmly establishes
> RockStream's role as a freshness layer that feeds the data lake rather than
> competing with columnar analytics tools on ad-hoc scans.
>
> **v3.17 adds catalog registration to the cold-tier sink** (§13.6.5): writing
> Parquet files is not enough for tools that look up tables by name rather than
> path. The `CREATE SINK` syntax gains a `catalog` option that governs whether
> RockStream also calls an external catalog API after each snapshot commit:
> `filesystem` (default — self-contained Iceberg metadata, no external service),
> `glue` (AWS Glue Data Catalog), `rest` (any Iceberg REST catalog: Polaris,
> Unity Catalog, Gravitino), `hive` (Hive Metastore via Thrift), and `ducklake`
> (DuckLake catalog backed by a DuckDB/MotherDuck database). The catalog call is
> step 6 of the snapshot lifecycle, idempotent, and failure-isolated — a catalog
> API error degrades the sink to `CATALOG_WARN` state without blocking IVM.
>
> **v3.18 adds RockStream as a native Iceberg REST Catalog server** (§13.7):
> rather than calling an external catalog, RockStream can *be* the catalog.
> The gateway exposes `/iceberg/v1/` (Iceberg REST Catalog spec) backed
> directly by the existing control-plane catalog and cold snapshot manifests.
> Any Iceberg-native tool (Spark, Flink, Trino, DuckDB catalog config) can
> point at the RockStream gateway and discover views by name with no
> additional infrastructure. The HTTP routing slot is reserved in Phase 9
> (`--role=gateway` serves both pgwire and HTTP on separate ports) so the
> endpoint can be implemented when the cold tier ships without a gateway
> rewrite.
>
> **v3.19 adds a §13.8 scope note and DuckLake native server placeholder**:
> §13.7 is Iceberg REST only. Delta Lake table discovery is covered by path
> access (§13.6) and Unity Catalog via `catalog = 'rest'` (§13.6.5). A native
> DuckLake 1.0 catalog server is architecturally feasible but deferred: the
> Iceberg REST catalog (§13.7) already covers DuckDB table-by-name discovery;
> a native DuckLake server adds value only for deployments that are
> DuckLake-first and want zero external endpoints. §13.8 documents the
> design decision and the conditions under which it should be revisited.
>
> **v3.20 is a coherence pass addressing identified design gaps**: frontier
> aggregator leader election via lease-based fencing (§3.2); first-class
> secrets management with envelope encryption and worker-side token resolution
> (§14.18); `rockstream.epochs` scalability via pre-aggregated checkpoint
> summaries with mandatory filters (§12.6.1); `AS OF EPOCH` granularity
> disclosure — resolution is checkpoint-bounded, not epoch-bounded (§12.4.1);
> downstream lag inheritance validation at `CREATE MATERIALIZED VIEW` and in
> `EXPLAIN INCREMENTAL ESTIMATE` (§14.9); namespace corrected from "analogous
> to PostgreSQL schema" to "analogous to PostgreSQL database" with pgwire
> routing semantics (§5.2); targeted compaction fallback when range-targeted
> API is unavailable (§5.4); GA vs. Data Lake GA positioning scope clarified
> (§15); Phase 12 decision gate given explicit "no" criteria
> (IMPLEMENTATION_PLAN.md); data quality/expectations explicitly deferred to
> post-1.0 with extension-point design (§15.1).
>
> **v3.21 adds three HTAP ergonomics gaps** that were absent from the
> design despite being directly enabled by existing machinery: session-scoped
> automatic read-your-writes so applications never thread freshness tokens
> manually (§12.8); `INSERT ... RETURNING` so applications receive written
> rows back without a second round-trip (§12.8.2); and secondary indexes so
> non-primary-key lookups on base tables do not require a full shard scan or
> a user-managed materialized view (§13.9). Version schedule updated:
> v0.46 extended to cover §12.8 features; secondary indexes land at v0.53
> (new slot); v0.40–v0.42 wire-protocol completion versions inserted;
> v0.43 FizzBee cold-tier model + simulator fidelity version inserted;
> downstream Phase 12–16 renumbered +4 (v0.44–v0.54).
>
> **v3.22 adds three HTAP and distributed-coordination improvements** grounded
> in the existing frontier algebra: a `max_staleness` session parameter for
> analytical sessions that bounds read latency without blocking (§12.8.3);
> shard-level column statistics (min/max bounds, Bloom filters, HLL cardinality)
> piggybacking on the `WorkerFrontierSummary` to enable OLAP scatter pruning in
> the gateway planner (§8.7, §12.3.1); and a formal statement of the CALM
> epoch-commit invariant (§8.4) — making the committed epoch verifiable by any
> observer with object-store read access, with no gateway or coordinator
> required, and exposing this guarantee to the Iceberg REST catalog (§13.7).
>
> **v3.24 adds inline views** (§4.3): `CREATE VIEW` (without `MATERIALIZED`)
> stores a query definition in the catalog and expands it as a macro at query
> or materialized-view compilation time. No operator state, no arrangement
> shards, no `view_output/` storage — just a named SQL alias. This is the
> Postgres-standard `CREATE VIEW` semantics and covers ad-hoc query composition,
> building blocks for materialized views, and schema abstraction. Ships v0.44.
> Error codes `RS-1010`–`RS-1011` added. §5.7 and §12.1 updated.
>
> **v3.25 aligns user-facing terminology with four accepted ADRs** (workloads,
> four-knobs, view-lifecycle, explain-levels — now incorporated): `CREATE PIPELINE`
> is removed from all user-visible surfaces and replaced by `CREATE WORKLOAD` (a named
> resource policy declared separately) plus `CREATE MATERIALIZED VIEW ... WITH (WORKLOAD =
> name)`. The CLI surface changes from “pipelines and views” to “workloads, schemas, views,
> and sources”. Metrics rename from `pipeline_slo_compliance` to `view_slo_compliance`.
> §14.3, §14.4, §14.6–§14.7, §14.9–§14.13, and §14.16 updated throughout.
>
> **v3.26 integrates six accepted ergonomics and observability ADRs**
> (first-run-ergonomics, observability-ergonomics,
> subscribe-ergonomics, application-ergonomics,
> dead-letter-queue, resource-usage-visibility — now incorporated):
> built-in `GENERATE ROWS` data source for zero-friction first run (§13.5.0);
> backfill cost preview prompt before long DDL with `WITHOUT CONFIRMATION`
> escape hatch (§14.9); subscribe API gains `CHANGE_RETENTION`, `AS OF NOW
> WITH SNAPSHOT`, column projection and `WHERE` server-side filtering (§12.3);
> `CREATE REPLACEMENT MATERIALIZED VIEW` for zero-downtime view replacement
> (§4.2); user-facing DLQ surface with `rockstream_catalog.dead_letter_queue`,
> `REPLAY DEAD_LETTER_QUEUE`, and `DISMISS DEAD_LETTER_QUEUE` (§13.3.1);
> `SHOW RESOURCE USAGE` commands and `rockstream_catalog.view_resource_usage` /
> `workload_resource_usage` catalog tables (§14.19); `SHOW SCHEMA_EVOLUTION
> STATUS` and `SHOW SCHEMA_EVOLUTION HISTORY` for proactive incompatibility
> detection (§4.2); every `RS-XXXX` error must include a `next_steps` field
> enforced in CI (§14.14); proactive NOTICE at 80% (`RS-5018`) and WARNING at
> 95% (`RS-5019`) resource utilisation thresholds (§14.19).
>
> **v3.27 completes ADR coverage** with remaining gaps filled:
> `EXPLAIN INCREMENTAL VERBOSE` and `ANALYZE` output levels (§14.8);
> `BUILDING` and `REPLACING` lifecycle states in §14.10; `SHOW BACKFILL STATUS`,
> `SHOW VIEW STATUS`, `SHOW REPLACEMENT STATUS` commands; `WAIT FOR
> MATERIALIZED VIEW ... TO BE READY TIMEOUT` and `SET BACKGROUND_DDL = ON`
> (§14.10); `ALTER SCHEMA ... PAUSE/RESUME`; `rockstream.write_fence()` cross-
> session token and `/*+ ALLOW_STALE */` per-query hint (§12.8.1);
> `ALTER ... DISCARD REPLACEMENT` (§4.2); `replay_attempt` counter and
> `dlq_warn_threshold` configuration in DLQ surface (§13.3.1);
> `workload_source` column in `rockstream_catalog.views`.
>
> **v3.28 is a contracts and consistency pass** that integrates the
> [plans/plan-assessment-v0.4.md](plans/plan-assessment-v0.4.md) review.
> It tightens semantic edge cases and operational contracts without changing
> the overall architecture:
>
> 1. **Latency classes** (§3.0) make the freshness contract explicit:
>    `local_visible`, `local_durable`, `distributed_fresh`,
>    `distributed_exact_sink`, and `analytical_cold` each have a named
>    target and a designated frontier. The dual-frontier model
>    (`visible_frontier` vs `durable_frontier`) lets embedded mode reach
>    sub-millisecond visibility without overpromising durable distributed
>    latency.
> 2. **Stable CDC row identity** (§6.4): `row_id` for keyed CDC sources is
>    `(source_id, table_id, primary_key_bytes)`. LSNs and source offsets
>    are version metadata only; they never participate in arrangement
>    identity. Keyless mutable sources without a stable identity column
>    are rejected for retraction-capable views.
> 3. **Versioned Z-set cold/hot merge** (§12.7.1): the hot/cold tier merge
>    is a signed Z-set merge ordered by epoch, not a blind `row_id` dedup.
>    `row_id` deduplication is permitted only for insert-only views whose
>    root operator proves monotonicity.
> 4. **Virtual buckets** (§7.1, §10.2, §10.6) replace ambiguous key-range
>    splitting. Keys hash to many virtual buckets; buckets are the unit of
>    rebalancing, hot-key salting, and online migration. Rendezvous
>    hashing now maps virtual buckets to physical shards, not raw keys.
> 5. **Bucket migration state machine** (§10.2): `PLANNED →
>    SNAPSHOTTING → COPYING → DUAL_WRITING → CATCHING_UP →
>    FENCING_OLD → CUTOVER → VERIFYING → GC_ELIGIBLE → DONE`. Every
>    transition is idempotent and audit-logged. Cleanup is forbidden
>    until the bucket's consumer frontier passes the cutover epoch.
> 6. **Watermark fail-closed** (§6.9, §13.3): event-time windows require
>    an explicit `WATERMARK = PROCESSING_TIME | EXTERNAL '<source>' |
>    NONE` policy when the source cannot supply a watermark. The default
>    is to reject the DDL with `RS-1005 connector.watermark_required`
>    rather than silently leave windows open forever.
> 7. **Vector freshness tokens** (§12.4): `FreshnessToken` now carries a
>    `BTreeMap<SourceId, SourceProgress>` so views over multiple sources
>    and view-on-view DAGs can express read-your-writes correctly. The
>    scalar form is retired.
> 8. **Namespace, not schema** (§5.2, §14): the user-facing surface uses
>    `NAMESPACE` consistently. Inside a namespace there is no PostgreSQL
>    schema layer; dotted names appear only in cross-namespace admin
>    commands. `ALTER SCHEMA` is removed in favor of `ALTER NAMESPACE`.
> 9. **System schema consolidation** (§12.6.1): the single canonical
>    user-facing system schema is `rockstream_catalog`. The historical
>    `rockstream.*` names are documented as legacy aliases scheduled for
>    removal in v0.54.
> 10. **Error-code ranges and uniqueness** (§14.14): every `RS-XXXX` code
>     lives in one reserved owner range and must be globally unique. CI
>     fails on duplicates. Colliding coordinator and cold-tier codes are
>     reassigned into their owner ranges (`RS-25xx` for coordinator
>     group, `RS-40xx` for cold-tier quota).
> 11. **Hot-path metrics** (§14.15): added object-store p99 latency,
>     SlateDB manifest/WAL/SST detail, write amplification, compaction
>     debt, event-time watermark lag, `windows_held_without_watermark_total`,
>     visible vs durable frontier lag, and migration-state duration.
> 12. **Section reference fixes**: §13.10 and §13.6.6 (renamed from
>     §13.6.2.1) are corrected. §3.4 frontier-aggregator references are
>     redirected to §3.2.
>
> **Companion documents**:
> - [IVM.md](IVM.md) — deep design of the incremental-view-maintenance engine
>   itself (PlanIR, the differentiation pass, the per-operator rules, the
>   circuit runtime, arrangements on SlateDB). This DESIGN document tells you
>   *what* the system is; IVM.md tells you *how the IVM core works*.
> - [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) — phased build plan that
>   operationalizes both.

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Theoretical Foundation: DBSP & Differential Dataflow](#2-theoretical-foundation-dbsp--differential-dataflow)
3. [System Topology](#3-system-topology)
4. [SQL Compilation Pipeline](#4-sql-compilation-pipeline)
5. [Per-Shard SlateDB Storage Layout](#5-per-shard-slatedb-storage-layout)
6. [Operator Catalog & State Encodings](#6-operator-catalog--state-encodings)
    - [6.11 Algebraic Merge Laws and CRDTs](#611-algebraic-merge-laws-and-crdts)
7. [The Exchange (Shuffle) Subsystem](#7-the-exchange-shuffle-subsystem)
8. [Frontier Protocol & Progress Tracking](#8-frontier-protocol--progress-tracking)
9. [Atomic Epoch Commit Protocol](#9-atomic-epoch-commit-protocol)
10. [Elasticity: Adding, Removing, and Rebalancing Shards](#10-elasticity-adding-removing-and-rebalancing-shards)
11. [Fault Tolerance & Exactly-Once Semantics](#11-fault-tolerance--exactly-once-semantics)
12. [Query Serving](#12-query-serving)
    - [12.7 Two-Tier View Storage](#127-two-tier-view-storage-design-decision)
    - [12.8 OLTP Session Ergonomics](#128-oltp-session-ergonomics)
13. [Connectors & External I/O](#13-connectors--external-io)
    - [13.6 Iceberg/Delta Cold-Tier Sink](#136-icebergdelta-cold-tier-sink)
    - [13.7 Native Iceberg REST Catalog](#137-native-iceberg-rest-catalog)
    - [13.8 Native DuckLake Catalog Server (deferred)](#138-native-ducklake-catalog-server-deferred)
    - [13.9 Secondary Indexes](#139-secondary-indexes)
14. [Operations: Deploy, Monitor, Diagnose](#14-operations-deploy-monitor-diagnose)
15. [Design Positioning](#15-design-positioning)
16. [Optimality Assessment (v3.7)](#16-optimality-assessment-v37)
17. [Simulation Testing](#17-simulation-testing)
18. [Appendix: Key Encoding Reference](#appendix-key-encoding-reference)

---

## 1. Design Principles

| # | Principle | Consequence |
|---|---|---|
| P1 | **Compute and storage are separated.** | Workers are stateless; all state lives in SlateDB. Workers can be added, removed, or replaced freely. |
| P2 | **Storage is sharded across many SlateDB databases.** | No global write bottleneck. Each shard is independent; SlateDB's single-writer constraint applies only within a shard. |
| P3 | **Exchanges are first-class operators.** | Re-partitioning between stages is an explicit dataflow node, not a hidden bulk transfer. The same primitive handles joins, group-bys, distincts, and recursion. |
| P4 | **Object storage is the universal substrate.** | State (SlateDB SSTs), shuffle payloads, checkpoints, and the WAL all live in S3/GCS/ABS. No node owns data exclusively. |
| P5 | **Frontiers, not watermarks.** | Progress is tracked as an antichain of timestamps per operator input, following Differential Dataflow. This handles multi-input operators and out-of-order data correctly. |
| P6 | **DBSP semantics.** | Every operator is a Z-set transformer. Updates are `(row, weight)` pairs. Negative weights express retractions. This gives mathematically provable equivalence with batch SQL. |
| P7 | **Full SQL via a real compiler.** | SQL → DataFusion logical plan → incremental physical plan (with explicit exchanges) → distributed plan → operator graph. No DSL, no Turing-incomplete subset. |
| P8 | **Exactly-once end-to-end.** | Source offsets and sink commits are integrated into the epoch commit protocol via a two-phase commit on connector state. |
| P9 | **Adaptive parallelism per operator.** | Different operators in the same query can run at different widths. A hot aggregation can use 100 shards while a small lookup uses 4. |
| P10 | **Idempotent everything.** | Every side effect — shuffle write, sink write, state mutation — is keyed so that replay is a no-op. |
| P11 | **DBSP is the runtime truth.** | Native DBSP-style operators define behavior. pg_trickle's SQL delta engine is used as an oracle and regression corpus, not copied blindly as runtime machinery. |
| P12 | **Respect SlateDB's real constraints.** | We rely on supported features (single-writer fencing, WriteBatch, DbReader, checkpoints, merge operators, TTL, compaction filters, WAL reader, segments) and do not assume missing APIs such as range deletion. |
| P13 | **Causal time.** | Progress is an antichain over `(shard_id, source_epoch)` pairs. There is no global LSN. The cluster frontier is computed asynchronously from per-shard frontiers and is allowed to lag by a bounded budget. |
| P14 | **Async scheduling.** | Operators are long-lived async tasks. There is no synchronous global scheduler tick and no per-stream ownership checker. Backpressure flows via credits; progress flows via frontiers. |
| P15 | **Bounded staleness for cross-shard reads.** | Query gateways pin to a published cluster frontier (a vector of per-shard checkpoints) rather than to wall-clock "fresh"; the staleness budget is documented and observable. |
| P16 | **Operability is a first-class system property.** | The system is SLO-driven (operators state intent; the control plane chooses mechanism), self-tuning by default, deployable as a single binary, observable by construction, and unable to surprise its operator: every degradation has a named reason, every control action is auditable, and every pipeline runs inside enforced quotas. One number (frontier lag against the SLO) answers "is it healthy?"; everything else is a drill-down. |
| P17 | **Scale down before scaling out.** | A one-shard local pipeline must avoid distributed overhead; a thousand-shard pipeline must avoid all-to-all amplification. Placement, exchange, and frontier aggregation optimize for locality first, then parallelism. |
| P18 | **Algebraic merge laws are a database-wide contract.** | A single registered `MergeLaw` catalog in `rockstream-types` describes every commutative monoid, join-semilattice CRDT, and operation CRDT used by the engine. Storage, planner, exchange, frontier, gateway, connectors, compaction, and `EXPLAIN INCREMENTAL` all consume the same catalog; none of them redefines its own merge semantics. See §6.11 and [ideas/crdts.md](ideas/crdts.md). |

### 1.1 Non-Goals (Explicit)

The following are intentionally **out of scope** because they conflict with
horizontal scale on object storage, and the v3.1 review confirmed that
attempting them would compromise the rest of the design:

- **Distributed IMMEDIATE / synchronous IVM.** pg_trickle's IMMEDIATE mode
  takes table-level locks and runs inside one PostgreSQL transaction; it does
  not generalize to a sharded cluster. RockStream does not support IMMEDIATE
  mode at any scope. The architecture has no write-transaction hook, no trigger
  layer, and no global write-sequence number; there is no path to holding an
  INSERT open across the connector, operator graph, and view commit without
  compromising the async-scheduling (P14) and causal-time frontier (P13) model
  the rest of the design depends on. A tight freshness SLO (50–200 ms) and
  frontier-based diamond consistency cover the same practical requirements
  without synchronous coupling.
- **Global linearizable snapshots across all shards in the hot path.** Reads
  see a causally consistent vector frontier, not a single global LSN. Queries
  that demand global linearizability must opt in to a cluster checkpoint and
  accept higher latency.
- **SERIALIZABLE isolation via cross-shard conflict detection.** True
  `SERIALIZABLE` requires tracking read-write conflicts across shards via a
  global conflict detector or per-shard SILock tables. This needs a global write
  sequence number, which is an explicit non-goal (see below). `READ COMMITTED`
  and `REPEATABLE READ` are fully supported by the existing vector-frontier
  model (§12.6) and cover the vast majority of analytical and streaming
  workloads. `SERIALIZABLE LOCAL` (single-shard, planner-proven) is scheduled
  for v0.52 (Phase 16). Optimistic exact-key guarded writes for non-CRDT columns
  and blind commutative writes for CRDT columns are planned pre-1.0 (§13.5.1).
  A *global* cross-shard `SERIALIZABLE` coordinator (one covering every shard)
  is an explicit non-goal. See
  [ideas/optimistic-locking-crdts.md](ideas/optimistic-locking-crdts.md).
- **A global write sequence number.** SlateDB's per-DB sequence is local. We
  do not synthesize a cluster-wide sequence on top of it.
- **Loading or linking pg_trickle at runtime.** It is not a Cargo
  dependency. They are reference material and test oracles only.
- **Active-active multi-region writes.** The single-writer fence per shard is
  a hard constraint against concurrent writers in different regions. Multi-region
  active-passive (read replicas via `DbReader` on a cross-region object-store
  bucket) is future work, not v1. The §6.11 merge-law catalog is structured so
  an idempotent join-semilattice column could become a region-spanning surface
  later, but no version through 1.0 promises that path.
- **Arbitrary user-defined merge functions before the built-in CRDT catalog
  ships.** `CREATE MERGE LAW` is gated on a post-1.0 release of the built-in
  catalog and shared property-test suite (§6.11; [ideas/crdts.md](ideas/crdts.md)).
- **Per-query cost accounting ($/query) in the hot path.** Cost visibility in
  `EXPLAIN ESTIMATE` is a design goal; per-query billing middleware and
  chargeback to tenants is an application-layer concern out of scope.

---

## 2. Theoretical Foundation: DBSP & Differential Dataflow

We adopt the **DBSP** formalism (Budiu et al., VLDB 2023) for the operator semantics
and the **Differential Dataflow** progress model (Murray, McSherry et al., SOSP 2013)
for distributed coordination.

### Z-Sets

A **Z-set** is a multiset with integer multiplicities. Every collection in the system
— base tables, intermediate results, view outputs — is conceptually a Z-set. An
*insert* contributes `(row, +1)`; a *delete* contributes `(row, -1)`; an *update* is
`(old_row, -1)` plus `(new_row, +1)`. Aggregations sum weights group-wise.

### Incremental Operators

For every relational operator `f`, DBSP defines its incremental form `f^Δ` such that

```
f^Δ(C, ΔC) = f(C ⊎ ΔC) - f(C)
```

— it computes the change in output given the current collection `C` and a delta `ΔC`,
without re-reading all of `C`. The DBSP paper proves this works compositionally for
the entire relational algebra including recursion.

### Timestamps & Frontiers

Each Z-set entry carries a **logical timestamp** (a vector that can include epoch,
iteration count for recursion, and source-position metadata). A **frontier** is an
antichain of timestamps such that an operator promises not to emit any future updates
at timestamps `≤` any element of the frontier. Frontiers advance monotonically.

This is the only correct way to track progress through multi-input operators
(joins, unions, recursive queries).

### Recursion

Recursive queries (`WITH RECURSIVE`, transitive closure, graph algorithms) use
**semi-naive evaluation** inside a fixed-point loop. DBSP gives this a clean
formalization via the `I` (integrate) and `D` (differentiate) operators applied
in nested time scopes.

The `IterativeCircuit` pattern (`crates/dbsp/src/operator/recursive.rs`) is
**local to one circuit on one worker**. RockStream lifts this into the
distributed setting by allowing `Exchange` operators inside a recursive scope
so the inner-time iteration can re-partition data each round. The iteration
frontier (`Timestamp::iteration`) participates in the same antichain protocol
as the outer source epoch. Cross-shard convergence is detected when the
iteration component of the cluster frontier stops advancing, not via a
synchronous barrier.

---

## 3. System Topology

```
                       ┌────────────────────────────┐
                       │Control Plane (1/3/5 nodes) │
                       │   (single writer in dev;    │
                       │    Raft-elected writer in   │
                       │    production; DbReader fan-│
                       │    out for query routing)   │
                       │                            │
                       │  • SQL compiler            │
                       │  • Plan optimizer          │
                       │  • Cluster topology        │
                       │  • Shard placement         │
                       │  • Frontier aggregator     │
                       │  • Connector orchestrator  │
                       └─────────┬──────────────────┘
                                 │ assignments
       ┌─────────────────────────┼─────────────────────────┐
       │                         │                         │
┌──────▼──────┐         ┌────────▼─────┐          ┌───────▼──────┐
│  Worker 0    │         │   Worker 1   │          │   Worker N   │
│              │ ◄─────► │              │ ◄──────► │              │
│ pin: shards  │ shuffle │ pin: shards  │  shuffle │ pin: shards  │
│  S0, S3, S6  │         │  S1, S4, S7  │          │  S2, S5, S8  │
└──┬─┬──┬──────┘         └──┬──┬──┬─────┘          └──┬──┬──┬─────┘
   │ │  │                   │  │  │                   │  │  │
   │ │  │                   │  │  │                   │  │  │   (one SlateDB
   ▼ ▼  ▼                   ▼  ▼  ▼                   ▼  ▼  ▼    instance per
 ┌──┐┌──┐┌──┐             ┌──┐┌──┐┌──┐             ┌──┐┌──┐┌──┐  shard; single-
 │S0││S3││S6│             │S1││S4││S7│             │S2││S5││S8│  writer rule
 └──┘└──┘└──┘             └──┘└──┘└──┘             └──┘└──┘└──┘  holds locally)

       Shared Object Storage (S3/GCS/ABS) — holds:
         • all SlateDB SSTs (per-shard prefixed)
         • all WAL files (per-shard)
         • all shuffle payloads (per-exchange / per-epoch)
         • all checkpoint manifests
         • all connector offset stores
```

### Three Logical Tiers

1. **Control plane** (1 node in Tier 1/2; 3 or 5 nodes in Tier 3).
  The durable control state is a dedicated **control SlateDB** holding the
  catalog, cluster membership, shard-placement map, audit log, and checkpoint
  index. In Tier 3, control nodes form a small Raft group that elects exactly
  one control writer lease for that SlateDB. Followers serve reads via
  `DbReader`, replay the control WAL, and can take over the writer lease after
  Raft leadership changes. Raft protects *control-plane leadership and leases*;
  SlateDB remains the storage engine for the catalog. This removes any hidden
  dependency on a global data-plane transaction manager.

2. **Worker plane** (elastic, N ≫ 1).
   Each worker hosts some number of **shards**. A shard is the unit of placement
   and writer-exclusivity. A worker process opens the SlateDB for each shard it
   owns as the sole writer. Other workers may open the same shard as readers
   (`DbReader`) for joins/lookups.

3. **Storage plane** (object storage).
   Object storage holds *all* durable state. Workers and the control plane have
   no local persistent state (modulo a small write-through cache).

### 3.0 Latency Classes and Frontier Semantics

RockStream's freshness contract is explicit per execution path, not a single
number. Each materialized view and each query is classified into one of five
**latency classes**, each with its own target and its own frontier semantics:

| Class | Applies to | Target | Frontier used |
|---|---|---:|---|
| `local_visible` | Embedded direct writes and in-process reads on the same worker. | sub-ms to low-ms | `visible_frontier` |
| `local_durable` | Local filesystem SlateDB commit-to-read in `embedded` or `single_worker`. | low-ms to tens-of-ms | `durable_frontier` |
| `distributed_fresh` | Distributed source-to-view freshness over object storage. | 10–250 ms | published cluster vector frontier |
| `distributed_exact_sink` | External sink with exactly-once semantics (Kafka EOS, Iceberg snapshot). | checkpoint-bounded (seconds) | cluster checkpoint frontier |
| `analytical_cold` | Iceberg/Delta full scans, ad-hoc analytics. | seconds (throughput-optimized) | cold snapshot epoch |

**The dual-frontier model.** A view advances two frontiers, not one:

- `visible_frontier` advances when deltas are folded into the in-memory
  arrangement cache (§4.2 of the hot-arrangement cache pattern) and become
  queryable to in-process readers. In `embedded` mode this can be sub-ms.
- `durable_frontier` advances only after the corresponding SlateDB
  `WriteBatch` is acknowledged. This is the frontier used for external
  sinks, replay, checkpointing, cross-worker reads, recovery, and
  freshness tokens issued to clients.

The visible frontier may lead the durable frontier only in runtime profiles
that explicitly allow it (`embedded`, and `single_worker` for same-process
reads). Distributed reads, exactly-once sinks, recovery, and any client-side
`FreshnessToken` always reference the **durable frontier**. This separation
is what lets the laptop story honestly claim sub-millisecond commit-to-read
while the distributed story honestly claims 10–250 ms freshness without
either number lying about the other.

`EXPLAIN INCREMENTAL ESTIMATE` prints the latency class for every view and
every query path, alongside the freshness target it commits to.

### 3.1 Runtime Profiles: Tiny to Massive

Storage profile (§5.6) controls durability/cost assumptions; **runtime
profile** controls how much distributed machinery is actually in the hot path.

| Runtime profile | Typical shape | Hot-path behavior |
|---|---|---|
| `embedded` | One process, one shard, local filesystem | Control plane, frontier aggregator, worker, and gateway are in-process services. Exchange nodes whose input/output partitioning is unchanged are elided, and gateway reads use local `DbReader` handles. |
| `single_worker` | One process or host, many shards | Shards still provide parallelism, but worker-to-worker gRPC is replaced by same-process channels or same-worker loopback. Durable outbox/inbox metadata remains in SlateDB for replay. |
| `distributed` | Many workers, object storage | Full placement, direct shuffle, durable fallback, hierarchical frontier aggregation, and autoscaling are enabled. |

`rockstream start --role=all --storage=./data` defaults to `embedded`. A
pipeline can move from `embedded` to `single_worker` to `distributed` by
changing placement and shard maps; the logical plan and persisted state format
do not change. The control plane records the active runtime profile per
pipeline, and `EXPLAIN INCREMENTAL` shows which exchanges were elided, looped
back, or sent over the network.

### 3.1.1 Runtime Profile Transitions

A pipeline transitions between runtime profiles via a control-plane command
(`ALTER WORKLOAD ... SET RUNTIME_PROFILE = 'distributed'`). Transitions are
**epoch-aligned**: the control plane waits for the current epoch to commit,
quiesces the pipeline (no new source data accepted), re-plans operator
placement, and resumes at the next epoch. State is not migrated — it remains
in the same SlateDB shards on the same object-store prefix. Only the
placement map and exchange topology change.

Transitions are valid in one direction only: `embedded → single_worker →
distributed`. Downgrade is not supported; operators must destroy and recreate
the pipeline at a lower profile. This prevents the ambiguity of merging
multiple distributed shards back into one embedded shard.

The `visible_frontier` semantics do NOT change mid-pipeline. A pipeline
created in `embedded` mode that transitions to `distributed` retains its
`visible_frontier` on the originating worker; distributed reads use
`durable_frontier`. The transition command emits a `NOTICE` explaining
this semantic change.

### Why Shards (and not "one SlateDB")

SlateDB is single-writer per database. To exceed one writer's throughput we run
**many SlateDBs**. A *shard* is one SlateDB instance. The system is a mesh of
hundreds or thousands of shards. Each operator instance pins to a shard for its
state. Throughput scales with shard count for partitionable workloads; the real
limits are hot keys, shuffle fan-out, object-store request rates, compaction
debt, and external source/sink throughput.

### What a "Worker" Owns

A worker is a process (typically one per host or container). It:
- Runs the writer for each of its assigned shards.
- Hosts operator instances whose state lives on those shards.
- Maintains a network port for shuffle send/receive.
- Reports frontiers and metrics to the control plane.

### Cluster Bootstrap and Worker Discovery

The control-plane address is passed as `--control=<url>` (or `ROCKSTREAM_CONTROL_URL`
in the environment). Workers join by calling `control.register(worker_id, addr,
capacity)` on startup; the control plane adds them to `topology/worker/` and begins
assigning shards. No pre-configured member list is required beyond the control URL.

For Tier 3 Raft bootstrap, the first control node starts with
`rockstream start --role=control --bootstrap --storage=s3://...`. Subsequent
control nodes start without `--bootstrap` and join the Raft group via the same
control URL. Once a quorum of control nodes is formed, the Raft leader opens the
control SlateDB for writing and the cluster is ready to accept workers.

Network security: all inter-node gRPC (worker↔control, worker↔worker, gateway↔worker)
and the public SQL port must use **mutual TLS (mTLS)**. Certificate rotation is handled
out-of-band; the control plane enforces that every worker presents a valid certificate
before being admitted into `topology/worker/`.

---

### 3.2 Frontier Aggregator as a Separable Process

In Tier 3, the control-plane Raft group owns the *authoritative* shard map,
schema catalog, and view and workload lifecycle decisions. It does **not** need to be on
the hot path of frontier reporting. As shard count grows past a few hundred,
per-shard frontier updates (§8.3) at every epoch dominate control-plane traffic.

The frontier aggregator is therefore a separable role:

```
rockstream start --role=frontier --control=<raft-url> --storage=s3://...
```

Frontier-role processes are stateless: they subscribe to per-shard frontier
reports (§8.3), maintain the cluster vector frontier in memory, persist the
committed frontier to control SlateDB on a low-frequency cadence (§8.4), and
serve `GET cluster.frontier` to query gateways. They can be scaled
horizontally; loss of a frontier process delays freshness-token issuance but
does not block ingest or compromise correctness. This keeps the Raft group
small (3–5 nodes) and its proposal rate independent of shard count.

**Leader election among frontier processes.** When multiple frontier-role
processes run, exactly one is the *elected publisher* — the process that
writes the committed frontier to control SlateDB. Election uses a
**lease-based leader** pattern on the control SlateDB:

1. Each frontier process attempts to acquire a time-limited writer lease on
   the `frontier/leader` key in the control DB (a compare-and-swap on a
   lease-holder UUID with a TTL, e.g. 10 s).
2. The holder refreshes the lease at `TTL / 3` intervals. If a refresh fails
   (fenced or expired), the process demotes itself to follower immediately.
3. Followers serve cached reads of the cluster frontier (stale by at most
   `frontier_agg_interval`) but do not write to SlateDB.
4. On holder loss (TTL expiry), any follower may attempt acquisition; the
   first successful CAS becomes the new publisher.

This is explicitly **not Raft** — there is no log replication, no quorum, and
no membership protocol. The frontier role stores no durable state of its own;
the lease exists only to prevent concurrent writers to the committed-frontier
key. The mechanism is identical to a distributed lock with fencing tokens and
is well-suited to the stateless, crash-tolerant nature of the frontier role.

**Synchronous frontier writes.** Frontier writes to control SlateDB must use
`WriteBatchOptions { sync: true }` (synchronous WAL flush). Without this, a
new leader that acquires the lease after a crash could read control SlateDB
before the previous leader's last frontier write was flushed, computing a
lagging cluster-wide frontier. The performance cost is bounded by the frontier
publication interval (default 100 ms) — one synchronous flush adds at most one
object-store round-trip (≤ 50 ms p99 for co-located control plane), within
the frontier aggregation latency budget (§3.0: ≤ 100 ms). A frontier write
that cannot flush within `frontier_write_timeout_ms` (default 5 000 ms) must
cause the leader to resign its lease and trigger a new election rather than
silently dropping the update.

---

## 4. SQL Compilation Pipeline

```
SQL text
   │
   ▼
[1] Parse + bind (sqlparser-rs + custom catalog binder)
   │
   ▼
[2] Logical plan (Apache DataFusion LogicalPlan)
   │
   ▼
[3] Rule-based optimizer (DataFusion's optimizer + custom IVM rules:
     predicate pushdown, projection pruning, constant folding,
     join reordering, subquery decorrelation)
   │
   ▼
[4] Incrementalization pass (SQL → DBSP):
     • Replace every relational op with its incremental form
     • Insert I/D operators around recursion blocks
     • Lower window functions to incremental windowed Z-sets
     • Lower aggregates to (group-key → partial state) maps
   │
   ▼
[5] Distribution pass:
     • Annotate every operator with its required input partitioning
     • Insert Exchange operators wherever partitioning differs
     • Assign per-operator parallelism width (cost-based)
   │
   ▼
[6] Physical plan (DAG of (Operator, parallelism, partition-key))
   │
   ▼
[7] Shard placement:
     • Map each operator instance to a shard
     • Reuse shards across operators where partition keys align
     • Co-locate operators that share state to avoid cross-shard reads
   │
   ▼
[8] Deployment:
     • Write the plan to the catalog
     • Push operator-instance assignments to workers
     • Workers materialize empty operator state on their SlateDB shards
     • Connectors start feeding source operators
```

### 4.0 Source Statistics and Cost-Model Inputs

Step [5] (distribution pass) and `EXPLAIN INCREMENTAL ESTIMATE` both require
cardinality estimates. The planner obtains them in priority order:

1. **Connector-reported statistics**: connectors expose `discover_stats()`
   returning `{row_count, avg_row_bytes, key_cardinality, update_rate_per_s}`
   after `discover_schema()`. Kafka connectors count committed offsets; Postgres
   CDC connectors read `pg_class.reltuples`; S3/Parquet connectors read footer
   metadata.
2. **Cached catalog statistics**: the control plane stores the last-known stats
   in `control: catalog/table/{id}/stats`, refreshed on each connector attach
   and on `ANALYZE TABLE <name>`.
3. **Heuristic fallback**: when neither is available, the planner uses
   configurable defaults (`default_row_count = 1_000_000`,
   `default_update_rate = 1000/s`) and marks the estimate as
   `confidence=low` in `EXPLAIN INCREMENTAL ESTIMATE` output.

Estimate accuracy is tracked over time: after 60 s of operation the real metrics
feed back into the catalog stats, and `EXPLAIN INCREMENTAL` shows both the
original estimate and the observed values side-by-side.

### Why DataFusion (not Calcite)

- Rust-native: integrates directly with the rest of the codebase, no JVM.
- Mature SQL frontend with full ANSI coverage.
- Pluggable logical plan; we extend it with DBSP-specific physical nodes.
- Substrait support for cross-language tooling.
- Active community; used by InfluxDB IOx, Comet, Ballista, etc.

We use the DBSP `sql-to-dbsp` approach as the reference for SQL-to-DBSP semantics and
pg_trickle as the reference for concrete SQL edge cases. The RockStream runtime
does not execute generated SQL and does not copy pg_trickle's CTEs into the hot path.
It compiles SQL into native DBSP-style operators and validates their behavior
against batch DataFusion, PostgreSQL, and pg_trickle test oracles.

### 4.1 The Differentiation Pass & PlanIR

The incrementalization step (4) and operator runtime are specified in detail in
[IVM.md](IVM.md). In summary:

- The logical plan is lowered into **PlanIR** — an explicit enum with one
  variant per operator (Scan, Filter, Project, InnerJoin, Aggregate, Distinct,
  Window, TopK, TimeWindow, Recursive, Exchange, …). PlanIR is modelled on
  pg_trickle's `OpTree`.
- A **`DiffCtx`** walks PlanIR and emits a runtime operator graph (`OpNode`s).
  The operator semantics are DBSP-native. pg_trickle's `diff_*` functions are
  used to identify edge cases and build regression tests: the EC-01 join fix,
  Q07 double-counting correction, Q21 SemiJoin correction, FULL JOIN NULL
  handling in SUM, recursive DRed fallback, and similar cases.
- The runtime is a **long-lived circuit of typed operators** (DBSP's model)
  rather than per-epoch SQL re-execution (pg_trickle's model). Each operator
  is a long-lived async task that consumes `RecordBatch` deltas and maintains
  one or more **arrangements** (indexed Z-sets) on its assigned SlateDB shard.
- We do not use generated Rust artifacts as the v1 deployment model. Instead,
  workers interpret a fixed physical plan; the vectorized `RecordBatch` inner
  loop is fast enough via DataFusion's expression executor. Code generation can
  be added later for hot paths without changing semantics.

See [IVM.md §4–7](IVM.md#4-the-rockstream-ivm-architecture) for the full
operator catalogue, runtime trait, and per-operator rules.

### 4.2 Schema Evolution and Plan Replacement

Every source, view, and pipeline carries an explicit schema version. Connectors
publish schemas into `control: schema/` before they emit data, and every
`RecordBatch` delta carries the schema version used to decode it.

Schema changes are classified before they reach the runtime:

| Change | Handling |
|---|---|
| Add nullable column or add column with default | Compatible. Existing arrangements keep their old row encoding; readers project the new column as NULL/default until rewritten by fresh deltas. |
| Widen numeric type or compatible string/binary widening | Compatible if DataFusion can cast losslessly; recorded as a schema-version edge. |
| Rename, drop, narrow, or change join/group/window key type | Breaking. Requires `CREATE REPLACEMENT MATERIALIZED VIEW v2 FOR v1` or `ALTER MATERIALIZED VIEW v1 APPLY REPLACEMENT v2`, producing a blue/green plan clone (§10.5). |
| Connector reports unexpected incompatible schema | View transitions to `BLOCKED(RS-1002)` and stops consuming new offsets until the operator approves a migration. |

Online replacement uses a checkpoint/clone path: create the new plan at a
published frontier, backfill only state whose encoding changed, run old and new
plans in parallel until the new plan reaches the old frontier, then flip query
routing atomically in the catalog. This is the default mechanism for `ALTER
VIEW`, join-key changes, and breaking source-schema updates.

**Replacement lifecycle commands:**

```sql
-- Create a replacement that hydrates in the background
CREATE REPLACEMENT MATERIALIZED VIEW v2 FOR reporting.daily_summary AS
  SELECT ...;

-- Monitor replacement progress
SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW reporting.daily_summary;
-- Columns: replacement_name, state, backfill_progress_pct, frontier_lag,
--          estimated_ready_at

-- Apply replacement atomically when ready
ALTER MATERIALIZED VIEW reporting.daily_summary APPLY REPLACEMENT v2;

-- Abandon a replacement (drops the shadow plan and its state)
ALTER MATERIALIZED VIEW reporting.daily_summary DISCARD REPLACEMENT v2;
```

During replacement the original view continues serving queries at full SLO.
The system transitions the view state to `REPLACING` (§14.10) and back to
`HEALTHY` once `APPLY REPLACEMENT` completes.

**Proactive schema evolution detection.** Operators can inspect upcoming
incompatibilities before they block consumption:

```sql
-- Show pending incompatible schema versions across all sources in a schema
SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA <name>;

-- Show the full evolution history for a specific view
SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW <name>;
```

When a connector detects an incompatible upstream schema change that has not yet
been applied, the system emits `RS-6001 schema.incompatible_evolution` as a
proactive `NOTICE` — giving operators time to prepare a `CREATE REPLACEMENT`
before consumption blocks on `RS-1002`.

### 4.3 Inline Views (Query-Time Macro Expansion)

RockStream supports two kinds of views:

| Kind | DDL | Semantics | Storage |
|---|---|---|---|
| **Materialized view** | `CREATE MATERIALIZED VIEW v AS …` | Continuously maintained by the IVM engine. Result set is pre-computed in `view_output/`. | Arrangement shards |
| **Inline view** | `CREATE VIEW v AS …` | Stored as a query definition in the catalog. Expanded (inlined) into the referencing query or materialized view at plan-compilation time. | Catalog only (no arrangement, no shard) |

**Key distinction:** `CREATE VIEW` never allocates operator state, arrangement
shards, or `view_output/` storage. The definition lives in `catalog/views/` in
the control-plane SlateDB. At query time or materialized-view compilation, the
planner substitutes the inline view's AST into the referencing query, exactly
like a SQL macro.

**Syntax.**

```sql
-- Inline view: just a named query alias, no materialization
CREATE VIEW recent_orders AS
  SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '1 day';

-- Materialized view: continuously maintained by IVM
CREATE MATERIALIZED VIEW order_totals AS
  SELECT account_id, SUM(amount) AS total FROM orders GROUP BY account_id;
```

**Use cases.**

1. **Ad-hoc query composition.** Define reusable query fragments without paying
   the cost of maintaining them incrementally:
   ```sql
   CREATE VIEW active_accounts AS
     SELECT * FROM accounts WHERE status = 'active';

   -- Ad-hoc: expanded inline at query time
   SELECT * FROM active_accounts WHERE balance > 1000;
   ```

2. **Building blocks for materialized views.** Inline views can compose into
   materialized view definitions. The planner inlines them before the
   incrementalization pass (§4 step [4]):
   ```sql
   CREATE VIEW high_value_orders AS
     SELECT * FROM orders WHERE amount > 10000;

   -- Materialized: IVM maintains this, with high_value_orders inlined
   CREATE MATERIALIZED VIEW hv_summary AS
     SELECT account_id, COUNT(*) FROM high_value_orders GROUP BY account_id;
   ```
   The compiled pipeline for `hv_summary` sees the filter from
   `high_value_orders` as part of its plan — no intermediate arrangement.

3. **Schema abstraction.** Present a stable interface over evolving base tables
   or materialized views:
   ```sql
   CREATE VIEW customer_profile AS
     SELECT id, name, email FROM users;
   ```

**Planner interaction.** Inline view expansion happens at step [2] of the SQL
compilation pipeline (logical plan construction). When the binder encounters a
reference to an inline view, it substitutes the view's stored `LogicalPlan`
subtree. This is identical to how DataFusion handles its `CREATE VIEW` today.
After expansion, the downstream steps (optimization, incrementalization,
distribution) are unaware that an inline view was involved.

**Interaction with materialized views.** When an inline view is referenced
inside a `CREATE MATERIALIZED VIEW`, the inline view's definition is frozen at
the materialized view's compilation time. If the inline view is later
`CREATE OR REPLACE`'d, existing materialized views that reference it are NOT
automatically recompiled. The operator must explicitly `ALTER MATERIALIZED VIEW
… RECOMPILE` or use the plan-replacement path (§4.2).

**DDL operations.**

| Operation | Behavior |
|---|---|
| `CREATE VIEW v AS …` | Stores definition in `catalog/views/{v}`. No pipeline, no arrangement. |
| `CREATE OR REPLACE VIEW v AS …` | Overwrites the stored definition. Does not affect live materialized views. |
| `DROP VIEW v` | Removes the definition. Fails with `RS-1010` if any materialized view references `v`. |
| `ALTER VIEW v RENAME TO w` | Renames in catalog. References in materialized views are by ID, not name. |

**Backwards compatibility.** The `CREATE VIEW` form (without `MATERIALIZED`) is
the Postgres-standard DDL. Applications migrating from Postgres get inline views
by default — matching their existing expectations. Only `CREATE MATERIALIZED
VIEW` opts into RockStream's IVM engine.

**Error codes:**

| Code | Name | Meaning |
|---|---|---|
| `RS-1010` | `view.inline_referenced_by_materialized` | Cannot drop inline view; one or more materialized views reference it. |
| `RS-1011` | `view.inline_cycle_detected` | Inline view definition creates a circular reference. |

**Ships**: v0.44.

---

## 5. Per-Shard SlateDB Storage Layout

Each shard has its own SlateDB. Within a shard, we use a layout designed
specifically for the operator catalog and the shuffle subsystem.

When a shard is created, RockStream configures a SlateDB segment extractor that
uses the namespace + arrangement prefix. This lets SlateDB's segment-aware LSM
layout isolate operator/shuffle/view state inside one shard without requiring a
separate SlateDB database per operator. The segment extractor is immutable after
database creation, so the prefix scheme is part of the storage format contract.

### 5.1 Shard-Local Namespaces

```
Prefix  Namespace            Purpose
──────  ─────────────────    ──────────────────────────────────────────────────
0x01    op_state/            All operator state for operators placed on this shard
0x02    op_index/            Secondary indexes over op_state (sorted MIN/MAX, etc.)
0x03    view_output/         Materialized view outputs whose partition lives here
0x04    shuffle_inbox/       Incoming shuffle batches awaiting consumption
0x05    shuffle_outbox/      Outgoing shuffle batches awaiting upload/ack
0x06    shard_meta/          Per-shard frontiers, epoch markers, connector offsets
```

### 5.2 Control-Plane SlateDB

A dedicated control SlateDB (one writer, >=2 readers in Tier 3) holds:

```
0x01    catalog/             Tables, views, pipelines, schemas (namespace-scoped)
0x02    plan/                Compiled physical plans, operator-instance assignments
0x03    topology/            Worker registry, shard placement, lease state
0x04    frontier/            Aggregated per-operator frontier (driven by workers)
0x05    checkpoints/         Cluster-wide checkpoint references
0x06    connector/           External-source offsets, sink commit state
0x07    audit/               Durable event log for every control-plane action
0x08    state_accounting/    Per-view state bytes, shard count, workload quota usage
0x09    schema/              Versioned source/view schemas and compatibility data
0x0A    namespace/           Namespace definitions, quotas, worker-pool affinity
```

**Namespace dimension.** Every catalog object (table, view, pipeline, schema)
belongs to exactly one namespace. The `namespace_id` is encoded into the key
immediately after the catalog type byte:

```
catalog/table:     0x01 0x01 namespace_id(16) table_id(16)
catalog/view:      0x01 0x02 namespace_id(16) view_id(16)
catalog/pipeline:  0x01 0x03 namespace_id(16) pipeline_id(16)
namespace/def:     0x0A 0x01 namespace_id(16) → { name, quotas, worker_pool }
```

A namespace is the isolation boundary within one cluster — **analogous to a
PostgreSQL database** (the `dbname` in the connection string), not a PostgreSQL
schema. Each namespace is a separate connection target: the pgwire gateway
routes connections to a namespace based on the `database` field in the startup
message. Objects within a namespace are not schema-qualified further (there is
no schema layer inside a namespace). Cross-namespace queries are not possible
within a single connection; clients must reconnect to a different namespace.
This matches Postgres `dbname` semantics (cross-database references require
`dblink` or `postgres_fdw`) and differs from Postgres schemas (where
`schema_a.table` and `schema_b.table` are accessible in the same connection
via `search_path`). The control plane enforces that cross-namespace data
sharing is only allowed where explicitly permitted (e.g. a shared source
namespace that multiple tenant namespaces can read from). The default namespace
is `default` with `namespace_id = 0`; single-tenant deployments never need to
think about namespaces.

This key scheme is required from day one. Retrofitting a namespace prefix into
the catalog after data has been written would require a storage format migration.

Workers read this database (via `DbReader` pinned to fresh checkpoints) on
startup and subscribe to its CDC feed (`WalReader`) for plan changes and
topology updates. Writes to the control DB go through the control-plane leader;
in Tier 3, that leader must hold the current Raft-issued writer lease before it
can open the control SlateDB for writing.

### 5.3 SlateDB API Constraints Used by This Design

The design assumes the following SlateDB features because they exist in the
current implementation: single-writer fencing, `WriteBatch`, `DbReader`, MVCC
snapshots, transactions, checkpoints/clones, merge operators, TTL, compaction
filters, WAL reader, and segment extractors.

The design deliberately does **not** assume range deletion or database
split/merge APIs. Cleanup and rebalancing therefore use one of three patterns:

- **Scan-and-delete** for bounded key ranges and correctness-sensitive cleanup.
- **Frontier-aware compaction filters** for retention where dropping old entries
  cannot make older values visible again.
- **Checkpoint/clone/projection** for large shard movement or blue/green plan
  replacement.

Compaction filters are never treated as a correctness shortcut. SlateDB warns
that filters can affect snapshot consistency and that dropping entries can
resurrect older versions. RockStream uses explicit deletes for zero-crossing
state transitions and reserves compaction filters for retention after the
frontier proves no reader can observe the removed versions.

### 5.4 SlateDB Operational Budgets

These SlateDB realities are first-class budget items in this design rather than
things to discover at runtime:

- **WAL listing is expensive at high retention.** `WalReader` documents that
  listing thousands of WAL files is costly. Every worker keeps a per-shard
  WAL listing cache, invalidated only on WAL rotation, and tails via
  `WalReader::get(latest_id + 1)` rather than repeated `list()`.
- **Manifest writes are not free.** Every flush/compaction/GC updates the
  manifest, which is an object-store write. Epoch sizes have a configurable
  minimum (`min_epoch_ms`, `min_epoch_bytes`) so manifest churn stays bounded
  even when source rate spikes. `manifest_poll_interval` on readers is tuned
  to match the cluster's frontier-staleness budget.
- **Merge operators must be associative.** SlateDB does not verify
  associativity. RockStream registers only operators whose associativity is
  proved by construction (integer add for weights, `(sum, count)` tuples for
  algebraic aggregates). MIN/MAX/Top-K/window/recursive state is maintained
  as explicit sorted arrangements, not merge operands.
- **Compaction-filter snapshot consistency** is preserved by gating any `Drop`
  decision on the per-shard checkpoint frontier; a filter never drops data
  that an active `DbReader` snapshot could observe.
- **`DbReader` is the cross-worker read path.** Joins and lookups that read
  state owned by another shard use `DbReader` pinned to a published
  checkpoint, never an undefined "live" read.
- **Arrangement segment cache.** `DbReader` lookups for join operators and
  cross-shard view queries pay object-store latency on every segment miss.
  Because arrangement SST segments are immutable between the checkpoint at
  which they were created and the compaction that rewrites them, they are safe
  to cache with zero coordination. Each worker maintains a per-process LRU
  **arrangement segment cache** keyed by `(shard_id, segment_id)`:
  - Bounded by `segment_cache_bytes` (default: 512 MB per worker, tunable per
    pipeline via the auto-tuner or manual override).
  - Populated on `DbReader` segment fetches; evicted LRU.
  - Invalidated on compaction: when a shard's manifest advances and a segment
    is no longer referenced, the cache entry is dropped. Manifest-poll already
    runs at `manifest_poll_interval`; invalidation piggybacks on that signal.
  - Shared across all `DbReader` handles on the same worker, regardless of
    which shard or pipeline they serve.
  - Reported as `segment_cache_hit_ratio` and `segment_cache_bytes_used` in
    the metrics reference (§14.15).
  For hot join arrangements whose working set fits in cache, this reduces
  cross-shard read latency from object-store round-trip (10–100 ms) to
  in-process memory access (µs). The cache is especially effective for
  skewed joins where a small set of keys dominates lookups.
- **Tombstone accumulation is bounded.** Z-set retractions produce LSM
  tombstones. For high-churn views (frequent inserts + deletes on the same
  key), tombstones accumulate faster than background compaction can clear
  them, degrading read latency. Each shard reports a `tombstone_density`
  metric (tombstone count / total key count). When `tombstone_density >
  tombstone_compaction_threshold` (default 0.25), the worker schedules a
  targeted compaction on that key range via SlateDB's
  `manual_compaction(key_range)` API (which triggers compaction of all SSTs
  overlapping the specified range). **Note**: if SlateDB does not expose a
  range-targeted `manual_compaction` API at implementation time, the fallback
  is a full `manual_compaction()` of the shard — acceptable because each shard
  is already bounded to `target_shard_state_bytes` (§10.6) and full compaction
  of a single shard is bounded work. The key insight is that compaction is
  requested *per shard* (each shard is its own SlateDB instance), so "targeted
  compaction on that key range" never means compacting another shard's data.
  Compaction filters clear Z-set entries whose weight is zero AND whose epoch
  is older than the committed checkpoint frontier — this is safe because no
  reader can observe a zero-weight row at a past epoch.
- **Write amplification is bounded.** Each shard tracks its logical-to-physical
  write ratio (`write_amplification_ratio`). The default budget is
  `target_write_amplification = 10` (10 physical bytes per logical byte
  including compaction rewrites). Shards exceeding `2 × target` trigger an
  operator NOTICE (`RS-5022 storage.write_amplification_high`) and a
  compaction tuning adjustment (larger SST target size to reduce level count).
  `EXPLAIN INCREMENTAL ESTIMATE` reports predicted write amplification per
  operator based on its arrangement type (merge-append-heavy workloads predict
  lower amp; delete-heavy workloads predict higher amp from tombstone rewrites).

### 5.5 Storage Format Versioning and Rolling Upgrades

Every shard carries a one-byte format version at a fixed key
(`shard_meta/0x06 0xFV`). The current format is **version 1**.

Compatibility rules:
- A binary that supports format versions `[min, max]` will refuse to open a
  shard whose stored version is outside that range, printing `RS-5001`.
- New binaries must support at least the previous format version to enable
  rolling upgrades (one worker restarted at a time).
- Breaking format changes require a bump to the version and a migration tool
  (`rockstream migrate --from=N --to=M --storage=s3://...`) that writes the new
  format offline before the new binary is deployed.

Rolling upgrade procedure: (1) deploy new binary to one worker; (2) verify it
acquires its shards and processes epochs; (3) roll forward. The control plane
rejects shard-lease acquisition from a binary whose supported version range does
not overlap the shards' stored version, preventing silent data corruption.

**Wire protocol version skew.** During a rolling upgrade there is a window
where shard A runs binary version N+1 and shard B runs version N. They may
exchange shuffle frames and gRPC messages over the same pipeline. Each gRPC
service announces a `protocol_version` header; the receiving side rejects
requests with a higher protocol version than it supports (returns `RS-5021
protocol.version_not_supported`). The upgrade contract is therefore:

- The N+1 binary must be able to *send* messages that N can parse (backward
  compatible wire format for one version).
- If a new message field is required, it must be added in a preparatory N
  release before the field is used in N+1.
- The control plane will not assign a pipeline across workers of incompatible
  versions; it waits until enough N+1 workers are available to host all
  affected operator instances.

This is the same N−1/N compatibility contract Kafka uses for inter-broker
protocol negotiation.

### 5.6 Storage Profiles and Autotuner Defaults

The same binary supports two storage profiles with different default budgets:

| Profile | Used by | Default tuning |
|---|---|---|
| `local_fs` | Tier 1 (`rockstream start --storage=./data`) | Low-latency epochs, aggressive flush cadence, small local cache. Intended to feel like an embedded database on a laptop. |
| `object_store` | Tier 2/3 (`s3://`, `gs://`, `az://`) | Larger `min_epoch_ms`, request-rate budget enforcement, WAL listing cache, coalesced shuffle objects. Intended to minimize PUT/LIST/manifest churn. |

The control plane detects the profile from the storage URL and seeds the
auto-tuner with the correct latency/cost assumptions. Operators can still
override SLOs and quotas, but they should not have to know whether a dev laptop
or a 1,000-shard S3 cluster is underneath them.

---

### 5.7 View Output Retention and Garbage Collection

Materialized view outputs grow without bound unless retention is specified.
By default:

- **`MATERIALIZED VIEW`**: retained forever (the view *is* the answer).
- **`VIEW` (inline)**: has no storage; nothing to retain or GC (§4.3).
- **`VIEW` declared incremental for streaming consumers only**: retained for
  30 days, configurable via `CREATE MATERIALIZED VIEW WITH (retention = '7d')`.

Retention is enforced by a SlateDB TTL plus a compaction filter that
drops view-output rows whose commit timestamp is older than the retention
window AND whose key is not the current value for that primary key (so a
slow-changing dimension stays available even if older than the window).

Retention bytes count against the pipeline's `state_budget_gb` quota
(§14.13). `EXPLAIN INCREMENTAL ESTIMATE` includes a projected steady-state
view-output footprint based on the declared retention and the source-rate
estimate.

---

## 6. Operator Catalog & State Encodings

Every operator instance has an `op_id` (16-byte ULID assigned by the compiler).
State keys begin with the op_id so different operators on the same shard never
collide. The state behind each operator is an **arrangement** — an indexed,
sorted Z-set whose key is the lookup column(s) for the operator. Arrangements
are specified in [IVM.md §9](IVM.md#9-arrangements-state-on-slatedb).

### 6.1 Stateless Operators

`Map`, `Filter`, `Project`: no SlateDB state. Pure functions applied to incoming
deltas; output deltas forwarded to the next operator (possibly through an
Exchange).

### 6.2 Aggregation (`SUM`, `COUNT`, `AVG` decomposed to SUM/COUNT)

```
0x01 0xAG op_id(16) group_key(var) → partial_state_bytes
```

Updates use `db.merge()` with an associative `AggregateMergeOp`. Output deltas
are computed lazily: on read, finalize partial state, compare with last-emitted
value (kept in `op_index/`), emit `(old, -1), (new, +1)`.

Operators never read merge-backed keys through raw SlateDB `get()`. They read
through `ShardDb::get_merged()` / `ShardDb::scan_merged()`, which resolve the
base value plus all visible merge operands at the epoch snapshot being used.
If a future SlateDB API cannot provide read-path merge resolution for a storage
profile, RockStream falls back to a batched read-modify-write for that
arrangement kind and marks the profile as lower throughput in `EXPLAIN
INCREMENTAL ESTIMATE`. This makes merge laziness a performance choice, not a
correctness assumption.

### 6.3 Aggregation with Retractions: `MIN`, `MAX`, `MEDIAN`, `PERCENTILE`

These cannot use a pure merge operator because retraction (`weight = -1`) may
require knowing all current values. We maintain an indexed multiset:

```
0x01 0xMM op_id(16) group_key(var) value_bytes(var) row_hash(8) → i64 weight
0x02 0xMM op_id(16) group_key(var) → current_extremum
```

On insert with weight +1: scan to find new min/max; if changed, update the cached
extremum and emit a delta.
On delete with weight -1: if the deleted value was the extremum, scan the indexed
multiset (sorted by value within the group prefix) to find the new extremum.

### 6.4 Equi-Join

Two-sided join state, indexed by join key. The `Exchange` operator before each
side guarantees both inputs are partitioned by the join key, so each shard sees
matching pairs locally.

`row_id` is a stable 128-bit identity assigned by the source connector, never a
fresh random value at replay time. The rule is **strict** because join, set-op,
distinct, and outer-join match accounting all depend on retraction matching the
original insert byte-for-byte:

- **Append-only log sources** (Kafka, append-only files): `row_id =
  hash(source_id, partition, offset, row_ordinal)`. The LSN/offset is part of
  identity because the row genuinely is "the row at this offset".
- **Keyed CDC sources** (Postgres logical replication, Debezium, MySQL
  binlog, RockStream's own CDC sink): `row_id = hash(source_id, table_id,
  primary_key_bytes)`. The LSN/offset is **version metadata stored alongside
  the row** (`source_lsn`, `source_epoch`), never part of `row_id`. An
  `UPDATE` that changes a non-key column must retract the previous row and
  insert the new row at the **same** `row_id`; using the LSN would silently
  break this.
- **Keyless snapshot/file sources**: `row_id = hash(snapshot_id, file_path,
  row_group, row_ordinal)`. Idempotent re-scan rewrites the same arrangement
  key.
- **Keyless mutable sources without a stable identity column** are rejected
  at `CREATE SOURCE` time with `RS-1006 source.no_stable_identity` for any
  view that needs retractions. The error message names the connector and
  asks the operator either to declare a `PRIMARY KEY (...)` clause or to
  mark the source `INSERT_ONLY = true` so the planner can restrict the
  downstream operator set.

Idempotent replay therefore rewrites the same arrangement key instead of
duplicating rows, regardless of the LSN at which the record happens to be
re-delivered.

```
0x01 0xJL op_id(16) join_key(var) row_id(16) → row_bytes  (left arrangement)
0x01 0xJR op_id(16) join_key(var) row_id(16) → row_bytes  (right arrangement)
```

For each incoming left delta `(row_L, +1)`:
- Scan `0x01 0xJR op_id(16) join_key(L)..` for matching right rows.
- Emit `(row_L ⋈ row_R, +1)` for each match.
- Insert `(row_L, +1)` into the left arrangement.

Retractions handled symmetrically with -1.

### 6.4.1 Outer Join Match Accounting

LEFT, RIGHT, and FULL OUTER JOIN need explicit match-count state so null-padded
rows flip correctly when the last match appears or disappears:

```
0x01 0xJL op_id(16) join_key(var) row_id(16) → row_bytes
0x01 0xJR op_id(16) join_key(var) row_id(16) → row_bytes
0x02 0xJM op_id(16) side(1) row_id(16)       → i64 match_count
```

For LEFT JOIN, a left insert with no right matches emits `(left,NULL,+1)` and
stores `match_count=0`. A right insert scans left rows for the key: for each
left row whose count was 0, it first retracts `(left,NULL,-1)`, increments the
count, and emits `(left,right,+1)`. A right delete emits `(left,right,-1)`,
decrements the left count, and if the count reaches 0 emits `(left,NULL,+1)`.
RIGHT JOIN is symmetric. FULL JOIN runs the same accounting on both sides so
both null-padded projections are retracted or restored exactly once. This is
the native operator state required by the EC-01 / FULL JOIN NULL cases in the
oracle suite.

### 6.5 Theta-Join / Cross-Join

Falls back to broadcast: one side is broadcast to all shards of the other side.
The compiler picks the smaller side. Broadcast happens via Exchange with target
list = `[all shards]`.

### 6.6 Distinct / Union (Set Semantics)

```
0x01 0xDS op_id(16) row_hash(16) → i64 weight
```

`MergeOperator` sums weights. Output emits delta when weight transitions
between zero and non-zero. When a key reaches zero, the operator emits an
explicit delete/tombstone for the arrangement entry when correctness requires
immediate invisibility. A compaction filter may later remove obsolete merge
operands after the frontier proves no snapshot can need them.

### 6.7 Window Functions (ROW_NUMBER, RANK, LAG, LEAD, sliding aggregates)

```
0x01 0xWN op_id(16) partition_key(var) order_key(var) row_id(16) → row_bytes
```

The order_key in the key gives natural ordering. For LAG/LEAD, scan the
neighboring entries. For sliding aggregates, maintain a segment tree per
partition; segment-tree nodes are stored under `op_index/`.

### 6.8 Recursion (`WITH RECURSIVE`, fixed points)

```
0x01 0xRC op_id(16) row_hash(16) iteration(4 BE) → i64 weight
```

State for the recursive variable is stored as a weighted set. Iteration is
driven by the operator scheduler: each iteration produces new deltas that
feed back as input deltas at the next iteration timestamp. The frontier
protocol naturally handles the inner-time dimension (the `iteration` component
of the timestamp vector).

Convergence detection: iteration stops when the recursive delta distincts to
empty and the inner frontier advances past the iteration timestamp. The compiler
also classifies recursive terms for monotonicity. INSERT-only monotone recursion
uses semi-naive evaluation. **DRed (delete-and-rederive) was evaluated in v0.22
and proved unsound under concurrent deletes; non-monotone recursive terms are
rejected with `RS-1009 recursion.non_monotone_not_supported`.** DRed may be
revisited as a future optimization once the distributed recursion surface
(Phase 4) stabilizes. Non-monotone or unsupported recursive terms fall back to
full recomputation. See [IVM.md §11](IVM.md#11-recursion-with-recursive).

### 6.9 Time Windows (Tumbling, Hopping, Session)

```
0x01 0xTW op_id(16) window_id(16) key(var) → partial_state
```

`window_id` is computed from the event-time of the row. Window expiry uses
SlateDB **TTL** based on event-time-derived deadlines and a frontier-aware
compaction filter. The filter is only allowed to remove data older than both
the event-time expiry and the relevant input/output frontiers.

The operator's **event-time input frontier** is advanced by watermarks
emitted by source connectors (§13.3). Without a connector-supplied
watermark, event-time semantics degrade to best-effort guesswork; the
connector contract therefore makes watermark emission a first-class return
value of `poll_delta`, separate from the source-epoch dimension.

**Watermark fail-closed policy.** Event-time windows are silent-data-loss
prone if the source cannot supply a watermark, because windows would either
stay open forever (memory blow-up) or be closed by wall-clock heuristics
(silently dropping legitimate late data). The DDL contract is therefore
fail-closed:

- `CREATE SOURCE` records whether the connector's `poll_delta` returns a
  watermark in its `WATERMARK_CAPABILITY` metadata
  (`NATIVE` | `EXTERNAL_HINT` | `NONE`).
- `CREATE MATERIALIZED VIEW` over any TUMBLE/HOP/SESSION window must specify
  one of:
  - `WATERMARK = NATIVE` — require `WATERMARK_CAPABILITY = NATIVE` on every
    contributing source; otherwise reject with
    `RS-1005 connector.watermark_required`.
  - `WATERMARK = PROCESSING_TIME` — fall back to wall-clock at the operator;
    drops late data silently and is logged at `WARN` on every window close.
    Allowed only when explicitly named.
  - `WATERMARK = EXTERNAL '<channel>'` — accept a watermark from a separate
    control-plane channel (heartbeats, manual advance).
  - `WATERMARK = NONE WITH WINDOW_CLOSE_DISABLED` — explicitly opt into a
    monotonically-growing windowed arrangement (the operator never closes
    windows; reads see all accumulated state). Required to be paired with
    `MEMORY_LIMIT` on the workload; otherwise rejected.
- The default — omitting the clause — fails the DDL with
  `RS-1005 connector.watermark_required` rather than picking any of the above
  for the user. There is no silent fallback.

The chosen policy is printed by `EXPLAIN INCREMENTAL` and shown by `SHOW
VIEW STATUS`. Metric `windows_held_without_watermark_total` counts windows
that have been open longer than `2 × window_size` without watermark advance,
making silent-stuck-window scenarios observable.

**Late-data policy.** A row is *late* for window `W` if its event-time falls
below the operator's current input frontier minus `allowed_lateness`. Per
pipeline or per time-window operator, the operator declares:

| Policy | Behaviour |
|---|---|
| `drop` | Late rows are silently discarded before reaching the window operator. |
| `update` | Late rows are applied to the window arrangement if it has not yet been garbage-collected; the window emits a corrected delta. |
| `route_to_sink` | Late rows are forwarded to a designated dead-letter sink connector with the original event-time preserved. |

The `allowed_lateness` budget is a duration attached to the time-window operator;
default is 0 (no late data accepted). It is surfaced in `EXPLAIN INCREMENTAL` and
counted as `connector_late_rows_total` in metrics.

### 6.10 Top-K

Sorted index keyed by value-descending:

```
0x01 0xTK op_id(16) partition_key(var) value_desc_bytes(var) row_id(16) → row_bytes
```

`scan(prefix..).take(K)` returns the top-K. Maintenance is incremental:
insert/delete updates the index; if the change crosses the K-th boundary, emit a
delta replacing the displaced entry.

### 6.11 Algebraic Merge Laws and CRDTs

Many operators in §6.2, §6.6, the watermark plumbing in §6.9, the
exchange combiner in §7.5, and the gateway pushdown in §12.3.1 share
the same underlying question: *when can two updates be merged without
coordination?* RockStream answers that question once, in a single
catalog of **merge laws** (commutative monoids, join-semilattice CRDTs,
and operation CRDTs) that every layer consumes. The full strategy lives
in [ideas/crdts.md](ideas/crdts.md); the design-level commitments are:

1. **The catalog lives in `rockstream-types`**, not in storage. Every
   law is registered with a stable `MergeLawId`, a `version`, and a
   `LawBundle` carrying the encoder, the SlateDB merge function, a
   frontier-aware compaction filter, an optional gateway combiner for
   partial-aggregation pushdown, and an `EXPLAIN` formatter.

2. **Every persisted arrangement and every persisted plan stores
   `(law_id, law_version)`.** A shard mount that cannot resolve the
   bundle refuses with `RS-5002 unknown merge law` rather than
   silently mis-merging. Law versions interact with the storage-format
   versioning of §5.5: compaction never folds across a version boundary
   unless the law declares it safe.

3. **Commutative monoids are not idempotent CRDTs.** `SumCount/v1` and
   `WeightAdd/v1` are associative and commutative but require the
   exactly-once epoch envelope of §11 and the connector contract of
   §13.3. Replay-tolerance lives only in laws explicitly tagged
   `idempotent` (e.g. `MaxRegister/v1`, `GSet/v1`, `HyperLogLog/v1`,
   `BloomUnion/v1`). The two classes never get conflated in code, in
   storage, or in `EXPLAIN INCREMENTAL`.

4. **Combiner eligibility comes from the planner.** §7.5's pre-shuffle
   combiner and the hierarchical-exchange variant consume the
   planner-attached `MergeLawId` on each `Exchange` node. The v0.4
   SUM/COUNT/AVG allowlist disappears in v0.30 in favour of generic
   law-driven combining, with an uncombined-equivalence property test
   per registered law.

5. **`EXPLAIN INCREMENTAL` always prints either the law or the reason
   no law applies.** The set of `not_merge_safe_reason` strings is a
   closed enum in `rockstream-types`.

6. **Retraction-aware operators are not CRDTs.** MIN/MAX with deletes
   (§6.3), exact Top-K (§6.10), windows (§6.9), and recursive DRed
   state (§6.8) keep their explicit arrangements. A retraction-aware
   operator *may* use a registered law for a cached subcomponent
   (e.g. a `MaxRegister/v1` slot inside MIN/MAX), but the operator as
   a whole is not a pure CRDT.

7. **User-visible CRDT column types** (`COUNTER`, `MAX_REGISTER`,
   `MIN_REGISTER`, `LWW`, `G_SET`, `OR_SET`, plus `APPROX_*` sketches)
   are post-1.0 once the internal law contract is proven.
   `CREATE MERGE LAW` for user-defined laws is deferred post-1.0 and
   the built-in catalog property suite must accept it before it can be
   used in a `PlanNode`. Non-idempotent laws written through the
   direct-write gateway require either exact-once source offsets or an
   idempotency key; if a write carries neither, the gateway mints a
   fresh server-generated idempotency key for that commit (v0.51.1) so
   the write still succeeds, rather than rejecting it with `RS-2007`.

8. **Active-active multi-region writes remain a non-goal through 1.0.**
   The law contract is structured so an idempotent join semilattice
   could become a region-spanning column later, but no public surface
   promises it and no §11 invariant relaxes for it.

The reserved `MergeLawId` block and built-in catalog (tag bytes, law
classes, lands-in versions) are listed in [ideas/crdts.md §6](ideas/crdts.md).

### 6.12 Arrangement Working Set and Memory Pressure

Operators access their arrangements via `ShardDb`. For hot-path performance,
each operator maintains a bounded in-process **arrangement cache** (LRU by key)
configurable as `operator_cache_mb` (default: auto-derived from workload
`MEMORY_LIMIT / operator_count`). Cache misses fall through to SlateDB
`get()` / `scan()` — correct but slower by the object-store round-trip
latency amortized by the segment cache (§5.4).

| Signal | Action |
|---|---|
| `arrangement_cache_hit_ratio < 0.5` for 5 min | Emit `RS-5021 arrangement.cache_thrashing` NOTICE |
| Working set estimate > 2× available cache | Emit NOTICE at `CREATE MATERIALIZED VIEW` |
| Worker RSS > `worker_memory_limit × 0.9` | Evict coldest arrangement caches first |

The auto-tuner redistributes cache budget across operators on the same worker
proportional to observed access frequency. The working-set estimate is derived
from the arrangement's key cardinality × average value size, reported by the
storage layer at each checkpoint.

Metrics: `arrangement_cache_hit_ratio{op_id}`,
`arrangement_cache_evictions_total{op_id}`,
`arrangement_cache_bytes_used{op_id}`.

`EXPLAIN INCREMENTAL ESTIMATE` reports estimated working-set size vs available
cache per operator. Operators whose estimated working set exceeds 2× available
cache emit `RS-5021` as a NOTICE at deploy time so operators can increase the
workload memory budget or accept higher read latency.

---

## 7. The Exchange (Shuffle) Subsystem

Exchange is the operator that re-partitions a stream from upstream's
partition key to downstream's partition key. It is the only mechanism that
crosses shard boundaries.

### 7.1 Partition Function

Partitioning is **two-level**: keys hash to virtual buckets, virtual buckets
map to physical shards. This is the only correct way to handle online
re-sharding, hot-key salting, and skew rebalancing without scanning the
entire keyspace at every change.

```
virtual_bucket_id = hash(partition_key) mod B
physical_shard_id = rendezvous_hash(virtual_bucket_id, live_shards, W)
worker_id         = lease(physical_shard_id)
```

`B` (virtual bucket count) is fixed for the lifetime of the pipeline at
`B = 16 × max_expected_shards`, default `B = 4096`. `W` is the current
physical shard count and is the only value that changes at scale events.

- **Rendezvous hashing** maps virtual buckets to physical shards so that
  changing `W` moves only `1/W` of the buckets, never partial keys.
- **Buckets, not keys, are the unit of online migration** (§10.2). All
  migration state, fence epochs, and shuffle-outbox metadata are
  bucket-scoped. The migration plan never has to enumerate keys.
- **Hot-key salting** is expressed as a per-bucket override: a known hot key
  is salted across `S` virtual buckets, all of which route the bucket-level
  pre-aggregation through a registered associative `MergeLawId` (§6.11) and
  re-aggregate at the consumer. The salt is recorded in the catalog so it
  survives restart and replay.
- **Skew rebalancing** moves whole buckets between physical shards via the
  §10.2 state machine; it never requires a key-range scan.

A bucket-to-shard map version (`bucket_map_version`) is stamped into every
exchange frame and every commit. Stale shuffle traffic referencing an old
map version is rejected at the receiver and re-routed via the fallback path.

### 7.2 Hybrid Transport: Direct + Object-Store

Each shuffle has two paths:

**Fast path (default)**: gRPC stream directly between worker processes. Low
latency (≈ network RTT), no object-store cost. Each batch is buffered in the
sender's `shuffle_outbox/` (in SlateDB on the sender's shard) until the
receiver ACKs.

The fast path is **worker-to-worker**, not shard-to-shard. A worker opens at
most one pooled bidirectional stream to each peer worker per traffic class and
multiplexes all shard/exchange batches over it. The framing header carries
`exchange_id`, `src_shard`, `target_shard`, `epoch`, and `seq`. This bounds
connection count at `O(workers^2)` instead of `O(shards^2)` and keeps large
clusters operable.

**Durable path (fallback / recovery / large batches)**: sender uploads the batch
as a coalesced object to
`s3://bucket/shuffle/{exchange_id}/{epoch}/{src_worker}/{target_worker}/{part}.arrow`.
One object may contain many `(src_shard,target_shard,seq)` frames plus an index
footer. The object key is written into the sender's `shuffle_outbox/`; the
receiver learns about it through direct notification or by tailing the sender's
outbox metadata. Receivers do **not** LIST the shuffle prefix on the hot path.

The fast path is used for small low-latency batches; the durable path is used
when the receiver is unreachable, when batches exceed a threshold, or as the
backup for fault tolerance. Either way, the canonical record is in object
storage — direct delivery is an optimization.

**Exchange path selection thresholds:**

| Condition | Path chosen | Rationale |
|---|---|---|
| Batch < `exchange_direct_threshold_bytes` (default 64 KiB) AND receiver reachable | gRPC direct delivery | Lowest latency for small deltas. |
| Batch ≥ `exchange_direct_threshold_bytes` OR receiver unreachable | Durable object path | Bounded memory on sender; guaranteed delivery. |
| Total epoch shuffle volume > `exchange_spill_threshold_mb` (default 256 MiB) per exchange | Durable object path + back-pressure signal | Prevents OOM from large redistributions. |
| `exchange_force_durable = true` (pipeline-level config) | Always durable | For correctness testing or compliance. |

The planner annotates each exchange edge with the expected path at `EXPLAIN
INCREMENTAL` time based on operator cardinality estimates. Actual path
selection is re-evaluated per batch at runtime.

#### v0.49 locality/capability-gated route classifier

**Ships**: v0.49

The runtime classifier consumes the planner's existing `ExchangeAnn`, worker
`host_id` / `availability_zone` metadata, capability bits, reachability, and
the `exchange_*` thresholds above. `Elided` and `Loopback` remain hard planner
constraints; `Direct` is refined to same-host shared memory or same-AZ gRPC,
and cross-AZ traffic is forced onto the durable object-store path. Missing or
stale locality metadata never drops traffic: the classifier emits an operator
warning and falls back to the safe legacy-compatible route.

### 7.3 Outbox & Inbox Encoding

```
shuffle_outbox/ key:
  0x05 exchange_id(16) target_shard(4) epoch(8 BE) seq(8 BE)
  value: Arrow IPC batch (compressed: LZ4 on the direct/loopback path,
         ZSTD on the durable path)

shuffle_inbox/ key:
  0x04 exchange_id(16) src_shard(4) epoch(8 BE) seq(8 BE)
  value: Arrow IPC batch (compressed: LZ4 on the direct/loopback path,
         ZSTD on the durable path)
```

Entries are deleted only after the consuming operator's frontier advances past
the epoch (see §8).

**Ships**: v0.49 (NEW_ROADMAP.md Phase 14). Until then, outbox/inbox values
are uncompressed Arrow IPC — found unimplemented and unscheduled anywhere by
the 2026-07-11 network-efficiency review, despite this section documenting
compression as already decided.

### 7.4 Why Arrow

Arrow gives us:
- Columnar, vectorized in-memory format that operators can process at SIMD speed.
- Zero-copy slicing for sub-batches.
- IPC format that doubles as the wire and on-disk format.
- Native interop with DataFusion expression evaluation.

### 7.5 Exchange Fast Paths and Combiners

The best shuffle is the one the compiler can prove is unnecessary. During
physical planning, every exchange is classified as one of four paths:

| Path | Used when | Effect |
|---|---|---|
| **Elided** | Upstream and downstream partitioning are identical and the operator instances are co-located on the same shard. | No serialization, no outbox, no inbox; the shard-level coordinator passes `EpochOutput` fragments directly to the downstream task before the next commit. |
| **Loopback** | Source and target shards are owned by the same worker. | Uses an in-process bounded channel instead of gRPC; durable outbox/inbox keys are still written so replay is identical to the distributed path. |
| **Direct** | Different live workers, batch below the durable threshold. | Existing gRPC fast path. |
| **Durable** | Receiver unavailable, batch too large, or recovery path. | Existing coalesced object-store payload. |

For associative operators, the exchange can insert a **pre-shuffle combiner**
on the sender side. The combiner groups deltas by `(target_shard, key)` within
an epoch, cancels equal-and-opposite Z-set weights, and emits one compact batch
per target shard. This is legal only for algebraically safe fragments:
`SUM`, `COUNT`, `AVG` partials, duplicate-weight cancellation, and other
compiler-certified associative/commutative payloads. Joins, Top-K, windows,
and non-invertible aggregates use the normal row-preserving path unless the
operator has an explicit partial-state encoding.

For very large clusters, direct exchange switches to a **hierarchical exchange**
when worker fan-out crosses the configured `exchange_domain_size` (default
64 workers). Workers are grouped into exchange domains. Sender-side combiners
first reduce traffic inside the local domain, domain routers forward coalesced
Arrow batches across domains, and the destination domain fans into target
workers. The hierarchy is a transport optimization only: every frame still
carries `(exchange_id, src_shard, target_shard, epoch, seq)`, and replay uses
the same outbox/inbox idempotency keys.

#### v0.49 same-host shared-memory fast path

**Ships**: v0.49

When two different worker processes advertise the same-host Arrow SHM
capability and share a `host_id`, the classifier selects a bounded shared-memory
transport ahead of gRPC. The zero-copy segment is only a transport optimization:
the canonical outbox/inbox bytes are still written and ACK semantics stay
identical to the existing direct path. Segment allocation, pool exhaustion, or
handshake failure logs a registered error code and falls back once to gRPC.

#### v0.49 AZ-aware hierarchical domains

**Ships**: v0.49

`exchange_domain_size` domains are built inside each availability zone as
`(availability_zone, domain_idx)` rather than as one flat global worker set.
Same-AZ peers may stay on the direct path, but cross-AZ worker-to-worker
traffic is routed through the durable object-store path so wide shuffles avoid
opening steady-state cross-AZ gRPC payload streams.

---

## 8. Frontier Protocol & Progress Tracking

### 8.1 Timestamp Type

A timestamp is a vector:

```
Timestamp {
  source_epoch: u64,   // monotonic epoch from ingestion
  iteration:    u32,   // for recursion; 0 outside recursive scopes
}
```

Ordering is product order: `t1 ≤ t2` iff every component of `t1` ≤ corresponding
component of `t2`. Two timestamps may be incomparable — hence we need
antichains. Additional nested scopes are represented by a stack of timestamp
components in the in-memory type (`Vec<ScopeTime>`), but the storage encoding
starts with the two components above because they cover the v1 hot path:
source epochs and recursive iterations. A new timestamp component is a storage
format change and must be added through the schema/format compatibility policy.

### 8.1.1 Multi-Partition Source Epoch Semantics

`source_epoch` is monotonic *per connector*, not per source partition. A Kafka
connector reading 32 partitions still emits one strictly increasing
`source_epoch` per epoch boundary it declares. The mapping from
`source_epoch` to underlying partition offsets is recorded by the connector in
a side table:

```
control: connector/{id}/epoch_map/{source_epoch}
  → { partition_id → committed_offset, ... }
```

This table is the source of truth for connector replay (§13.3) and for
exactly-once recovery: on restart, the connector reads the highest committed
`source_epoch`, looks up the partition offsets, and resumes from there. Two
operators consuming the same connector see the same source_epoch sequence,
and the frontier model treats it as a single dimension regardless of physical
partition count.

`FreshnessToken` (§12.4) carries the opaque `source_epoch` only; clients never
see partition offsets and need not know the connector's physical layout.

### 8.2 Frontier

A frontier is a minimal antichain of timestamps. An operator's *input frontier*
on a given input is the smallest antichain `F` such that no future delta on
that input will have a timestamp `t` with `t ≮ any f ∈ F`. The operator's
*output frontier* on a given output is derived from its input frontiers and its
operator-specific delay (most operators: identity; recursion: increment iteration).

### 8.3 Per-Shard Reporting

Every shard maintains, per operator instance and per output:

```
shard_meta/ key:
  0x06 0xFR op_id(16) output_port(2) → encoded_frontier (antichain bytes)
```

The shard's writer task periodically (e.g., every 10 ms or every epoch boundary)
flushes its current output frontier to its shard SlateDB and pushes a small
delta to the control plane.

### 8.4 Control-Plane Aggregation

The control plane subscribes via `WalReader` to each shard's `shard_meta/0x06 0xFR`
changes (cheap; one tiny write per operator per flush). It computes the
**cluster frontier** for each operator as the meet (greatest lower bound) of all
shard frontiers, and publishes:

```
control: frontier/op_id → cluster_frontier
```

Downstream operators read input frontiers from the control plane (cached &
subscribed). When the input frontier advances, the operator may:
- Release retained state (e.g., close a window).
- Compact garbage in its arrangements.
- Acknowledge upstream shuffle batches for deletion.

Frontier aggregation is **explicitly asynchronous**. It is not on the hot path
of any operator: operators decide what to do for the next epoch from their last
observed input frontier, even if it is a few aggregation rounds stale. The
aggregator's batching interval (`frontier_agg_interval`) is a tunable budget,
typically 50–500 ms. A frontier that is up to one budget interval stale is
still correct for garbage collection, window closing, and shuffle cleanup; it
only affects how quickly those reclamations happen.

**The CALM epoch-commit invariant.** The cluster-committed epoch is a monotone
predicate: once epoch *N* is committed on all shards, no shard frontier ever
retreats below *N*. This means the commit decision is verifiable by any observer
with read-only access to object storage, without coordination:

> An epoch *N* is globally committed if and only if every shard's
> `shard_meta/0x06 0xFR` entry in its most recent object-store manifest
> satisfies `frontier ≥ N`. This is a pure scan, requires no gateway
> connection, and is consistent with the control plane's published frontier.

Practical consequences:

1. **External tools** (DuckDB via cold snapshots, Trino via the Iceberg REST
   Catalog in §13.7) can verify that a cold snapshot at epoch *N* is safe to
   query by checking that all contributing shards' manifests satisfy the
   invariant — no API call to the RockStream gateway is needed.
2. **Freshness tokens survive gateway downtime.** A `FreshnessToken` with
   `source_epoch = N` can be self-verified against the object store. If the
   gateway is temporarily unreachable, a client can confirm token satisfaction
   independently.
3. **The frontier aggregator is a derived view, not the source of truth.** The
   per-shard `shard_meta/0x06 0xFR` entries are authoritative. The aggregator's
   published value is always ≤ the true cluster-committed epoch and converges
   within `frontier_agg_interval`. Staleness in the aggregator is always
   pessimistic — it may delay garbage collection and window closing, but it
   never causes correctness violations.

### 8.5 Garbage Collection of Shuffle Buffers

When a receiver operator's input frontier on exchange `E` advances past epoch
`e`, the receiver writes:

```
control: frontier/exchange_e/consumed → e
```

Senders observe this and enqueue exact cleanup for all `shuffle_outbox/` entries
with `epoch ≤ e`. Receivers do the same for `shuffle_inbox/`. Because SlateDB
does not currently expose range deletion, cleanup is implemented as bounded
prefix scan + batched deletes, with a frontier-aware compaction-filter fallback
for very old retained data. This is exact in semantics; it is not implemented
by a single range-delete API call.

### 8.6 Hierarchical Frontier Summaries

At small scale, frontier aggregation is in-process and nearly free. At very
large scale, the frontier role must not subscribe to `shards × operators`
updates one by one. The meet operation is associative, so RockStream aggregates
frontiers hierarchically without changing semantics:

1. Each shard persists its exact frontier in `shard_meta/0x06 0xFR` as before.
2. Each worker computes a worker-local meet for the shards it owns and sends
    one compact `WorkerFrontierSummary` per `(pipeline, operator, output_port)`
    per `frontier_agg_interval`.
3. Frontier-role processes compute the cluster meet from worker summaries.
4. In clusters with multiple frontier roles, one elected publisher (§3.2) writes the
  committed frontier to control SlateDB; followers serve cached reads.

This keeps control-plane traffic proportional to active workers and active
operators per interval rather than raw shard count. The persisted per-shard
frontiers remain the recovery source of truth, so losing a worker summary only
delays publication by one aggregation interval.

### 8.7 Shard Column Statistics for OLAP Scatter Pruning

At each cluster checkpoint, each worker piggybacks a compact `ShardColumnStats`
message alongside its `WorkerFrontierSummary`. Unlike the frontier summary
(updated every `frontier_agg_interval`), column stats update only at checkpoint
frequency — roughly every 30–128 epochs — and are stored in the control-plane
catalog:

```
control: topology/shard_stats/{view_id}/{shard_id} → ShardColumnStats
```

**Structure.** For each non-partition-key column nominated for skipping:

```rust
pub struct ShardColumnStats {
    pub shard_id:          ShardId,
    pub view_id:           ViewId,
    pub checkpoint_epoch:  u64,
    pub col_stats:         Vec<ColumnStats>,
}

pub struct ColumnStats {
    pub col_idx:            u16,
    pub min_bytes:          Option<Bytes>,  // serialized min value; None if unknown
    pub max_bytes:          Option<Bytes>,  // serialized max value
    pub bloom_filter:       Option<Bytes>,  // blocked Bloom / XOR filter, budget-bounded
    pub null_count:         u64,
    pub distinct_count_hll: Bytes,          // HyperLogLog/v1 sketch (MergeLaw 0x0008)
}
```

The Bloom filter is capped at `shard_bloom_budget_bytes` per shard per column
(default 64 KB, tunable per pipeline). Each shard maintains its own independent
filter; the gateway evaluates filters independently — shard filters are **not**
merged into a cluster-wide filter.

**Gateway planner integration** (extends §12.3.1). When planning a multi-shard
scatter, the gateway reads `shard_stats` from its cached control-plane
`DbReader`. For each candidate shard it evaluates each `WHERE` predicate against
the column stats:

- If `predicate_val < min_bytes` or `predicate_val > max_bytes` → prune.
- If the Bloom test returns false for an equality predicate → prune.
- Otherwise → include in scatter set.

`EXPLAIN` reports: `shard_scan: 8/100 shards (pruned by column statistics on
status, region)`. Pruning is an optimization only — a Bloom false positive causes
a wasted round-trip; a false negative is impossible by Bloom filter construction.

**Stats freshness guard.** If `shard_stats` for a view are older than
`shard_stats_max_age` (default: `5 × checkpoint_interval`), the gateway skips
scatter pruning for that view and falls back to full scatter. A warning notice
`RS-2017 shard_stats.too_stale` is emitted (never blocks a query).

**Secondary indexes provide high-quality stats.** When `CREATE INDEX` (§13.9)
builds an index on a column, the resulting arrangement frontier and partition map
supply precise min/max bounds and an exact Bloom filter for the indexed column at
build completion. These are automatically published to `shard_stats` at each
subsequent checkpoint.

**Metrics:**

| Metric | Description |
|---|---|
| `scatter_shards_total` | Shards considered for scatter before pruning. |
| `scatter_shards_pruned_total` | Shards skipped by column statistics. |
| `shard_bloom_false_positive_total` | Shards included by Bloom that returned no matching rows. |

**Ships**: v0.48 (Phase 14 — Advanced DML & Scatter Pruning). Secondary
indexes — noted above as a source of high-quality stats — already shipped at
v0.32, so that dependency is already satisfied. (Corrected 2026-07-11: this
line previously cited a version-numbering scheme that predates this
roadmap's Phase 13 insertion.)

---

## 9. Atomic Epoch Commit Protocol

Each shard commits the mutations produced by all ready operator instances for an
epoch as one or more coalesced SlateDB `WriteBatch`es. Operator tasks return
`EpochOutput`; the shard-level epoch coordinator groups these outputs by shard
to reduce WAL/manifest/object-store write amplification. A batch includes:

1. All `op_state/` puts/deletes/merges produced by the operator.
2. All `op_index/` updates.
3. All `view_output/` puts/deletes if this is a leaf operator.
4. All `shuffle_outbox/` puts for batches that will be sent.
5. All `shuffle_inbox/` deletes for batches just consumed.
6. The new `shard_meta/0x06 0xFR` output frontier.

```rust
let mut batch = WriteBatch::new();

// state updates (using merge where associative)
for (k, delta) in agg_deltas    { batch.merge(k, delta); }
for (k, row)   in join_inserts  { batch.put(k, row); }
for k          in join_deletes  { batch.delete(k); }

// view outputs (for leaf operators)
for (k, v) in view_upserts { batch.put(k, v); }
for k       in view_deletes { batch.delete(k); }

// new shuffle outbox
for (k, batch_bytes) in outbox_writes { batch.put(k, batch_bytes); }

// consumed shuffle inbox cleanup
for k in inbox_acks { batch.delete(k); }

// frontier advance — included atomically so crash = retry same epoch
batch.put(frontier_key, encode_frontier(&new_output_frontier));

shard_db.write(batch).await?;
```

This is the **only** durability event per epoch per shard group, not per
operator. SlateDB's WAL guarantees each `WriteBatch` is atomic. Recovery is
automatic: on restart, the shard reads its current frontier and processes inputs
from that frontier forward. Every write is idempotently keyed by epoch,
operator, port, and sequence.

### 9.1 Epoch Sizing

Epoch size is a tuning parameter, not a constant. The shard-level coordinator
enforces `min_epoch_ms` and `min_epoch_bytes` floors so a bursty source cannot
drive manifest/WAL writes faster than object storage can amortize them. It
also enforces a `max_epoch_ms` ceiling so a quiet source still publishes
frontier progress on a predictable cadence. All three are exposed per pipeline.

### 9.2 Crash Semantics

On worker death, the new worker opens each lost shard as the single writer
(SlateDB fences the previous writer via the manifest epoch), reads
`shard_meta/0x06 0xFR` to recover the last committed epoch frontier, and
re-runs source inputs from that frontier forward. Because every write inside a
shard `WriteBatch` is idempotently keyed by `(epoch, op_id, port, seq)`,
replay is a no-op.

Partial-shard failures (one shard commits epoch `e`, another does not) are
resolved by the frontier protocol: downstream operators that depend on both
shards simply do not advance their input frontier past `e` until both shards
publish it. There is no cross-shard 2PC.

### 9.3 Cooperative Scheduling and Yield Points

Workers run operator tasks as tokio async tasks on a shared thread pool. A
large join recomputation, a recursive operator doing many iterations, or a
backfill epoch with millions of rows can hold the tokio executor for long
enough to starve heartbeat sends, shuffle credit grants, and frontier reports
— causing spurious heartbeat timeouts that look like worker failures.

To prevent this, every operator loop is bounded by a **records-per-quantum**
limit (default 64k rows per poll). When an operator has more work remaining
after consuming its quantum, it emits a partial `EpochOutput`, yields via
`tokio::task::yield_now()`, and is re-scheduled by the executor. The
heartbeat sender and frontier reporter run as separate tokio tasks with
higher priority in the scheduler, so they are always serviced between quanta.

**Priority inversion mitigation:** Higher priority alone is insufficient —
under sustained CPU saturation (all work-stealing threads occupied by
operator quanta) a higher-priority task that was not yet polled cannot pre-empt
a running quantum. To guarantee liveness, heartbeat and frontier tasks are
spawned on a **dedicated single-thread tokio runtime** (`rt_control`) separate
from the operator runtime (`rt_work`). This runtime is never used for operator
work, so heartbeat sends complete within their 50 ms deadline even when all
operator threads are saturated. The cost is one reserved OS thread per worker
(~8 KiB stack).

The quantum size is tunable per pipeline (`max_rows_per_quantum`, default
65536). Lowering it increases scheduling responsiveness at the cost of more
task context switches. Raising it is appropriate for CPU-bound pure aggregation
workloads where yield overhead dominates. The auto-tuner adjusts quantum size
by observing `scheduler_yield_ratio` (fraction of epochs that hit the quantum
limit); if above 0.8, it halves the quantum.

---

## 10. Elasticity: Adding, Removing, and Rebalancing Shards

> **Implementation status (2026-07-11 usability/scalability review):** this
> entire section was, until this review, fully designed but not scheduled on
> any roadmap version and not present anywhere in `crates/` (verified by
> grepping for the state names below and for `hot_key`/`shard_split`/
> `cluster_worker_pressure`). §10.1–§10.4 (the migration state machine) and
> §10.7 (worker drain) now ship at **NEW_ROADMAP.md v0.46**; §10.5–§10.6
> (hot-key virtual buckets, proactive splitting) and §10.8 (the autoscaling
> pressure signal) now ship at **v0.47**. Until those versions land, none of
> this section's mechanisms exist in the running system — shards are placed
> once at pipeline creation and rebalanced only via capacity-aware placement
> of existing shards (the `capacity_headroom` signal below, which IS
> implemented). **Architecture constraint added by the 2026-07-11 four-area
> compliance audit**: the adaptive parts of §10.5 below (hot-key detection
> and the re-splitting decision, points 1–2) must be implemented in
> `rockstream-control`, never in `rockstream-plan` — `rockstream-ops`/
> `rockstream-sql`/`rockstream-diff` already depend on `rockstream-plan` for
> its `PlanNode`/`OpNode` IR, so a `rockstream-plan → rockstream-ops` edge
> would close an unbuildable cycle. See NEW_ROADMAP.md's v0.47 row for the
> full reasoning.

### 10.1 The Shard Map

The control plane holds a versioned **shard map** for each exchange:

```
control: topology/shard_map/exchange_id → ShardMap {
  version: u64,
  ring: Vec<(virtual_node_hash, shard_id)>,   // rendezvous ring
}
```

When a new shard is added, the control plane bumps the version and publishes
a new ring. Workers observe the version change and gracefully cut over at an
epoch boundary.

### 10.2 Adding a Shard

Migration is **bucket-scoped** (§7.1) and runs through a strict state
machine. Buckets, not key ranges, are the unit of work; the migration plan
never enumerates individual keys.

```
PLANNED → SNAPSHOTTING → COPYING → DUAL_WRITING → CATCHING_UP →
         FENCING_OLD → CUTOVER → VERIFYING → GC_ELIGIBLE → DONE
```

Each transition is **idempotent**, **persisted in the control catalog**
under `topology/migration/{migration_id}`, and **emits an audit event**.
Each transition publishes a `migration_state_duration_seconds` histogram
sample so stalled migrations are observable.

| State | Done when |
|---|---|
| `PLANNED` | Migration request accepted; donor/recipient shards, bucket set, source frontier `F_plan`, and target shard map version computed. |
| `SNAPSHOTTING` | A SlateDB `Checkpoint` exists on every donor shard that pins the bucket's state at `F_plan`. |
| `COPYING` | Recipient has ingested the snapshot via `DbReader`. Recipient's local arrangement matches donor's at `F_plan`. |
| `DUAL_WRITING` | Both donor and recipient receive new writes for the migrating buckets through the exchange. Recipient buffers writes; donor remains authoritative for reads. |
| `CATCHING_UP` | Recipient's frontier has reached `F_plan`. The dual-write tail is being replayed into the recipient until its frontier reaches donor's current frontier within `cutover_lag_budget` (default 100 ms). |
| `FENCING_OLD` | Donor is fenced for the migrating buckets via shard-map version bump (`bucket_map_version + 1`). New writes route only to recipient. Old in-flight writes referencing the prior version are rejected at the receiver. |
| `CUTOVER` | All readers, all exchange receivers, and the gateway have observed the new `bucket_map_version`. Recipient is authoritative. |
| `VERIFYING` | A scan compares donor and recipient state at the cutover epoch for the migrated buckets. Any divergence aborts (rolls back to `DUAL_WRITING`); otherwise proceed. Verification is bounded by `verify_sample_rate` (default 1.0 for buckets ≤ 1 GB, sampled above). |
| `GC_ELIGIBLE` | The migration's consumer frontier — the minimum of every downstream operator's input frontier for the migrating buckets — has passed the cutover epoch. Only now is donor cleanup allowed to start. Cleanup before this state is forbidden; the §11 frontier protocol is the gate. |
| `DONE` | Donor cleanup complete. Migration record is closed and moved to history. |

**Per-state timeout budget.** Each state has a configurable maximum duration.
If a state exceeds its timeout, the control plane emits `RS-1030
migration.state_timeout` and moves to `ABORTED` (except for `GC_ELIGIBLE`
which is merely logged as slow but not auto-aborted, since it depends on
downstream frontier advancement).

| State | Default timeout | Notes |
|---|---:|---|
| `PLANNED` | 30 s | Purely control-plane bookkeeping. |
| `SNAPSHOTTING` | 5 min | Checkpoint creation proportional to arrangement size. |
| `COPYING` | 30 min | Bounded by bucket data volume / network throughput. |
| `DUAL_WRITING` | unbounded | Cleared by `CATCHING_UP` entry; alarm if > 10 min. |
| `CATCHING_UP` | 10 min | Must converge within `cutover_lag_budget`. |
| `FENCING_OLD` | 30 s | Fast shard-map propagation. |
| `CUTOVER` | 60 s | Waiting for all observers. |
| `VERIFYING` | 15 min | Full scan for small buckets; sampled for large. |
| `GC_ELIGIBLE` | unbounded | Alarm if > 30 min (frontier stall). |

Timeouts are configurable via `migration_state_timeout_<state>`. The
`migration_state_duration_seconds` histogram signals stalls before the hard
timeout fires.

**Cleanup safety rule.** No bucket-scoped delete, compaction filter, or
checkpoint discard fires until the migration reaches `GC_ELIGIBLE`. This
is what makes the absence of a SlateDB range-delete API safe: cleanup is a
frontier-gated, audit-logged action, not a side effect of the cutover.

**Rollback.** Any state from `PLANNED` through `VERIFYING` can transition
to `ABORTED`, which restores the previous `bucket_map_version` and removes
the recipient's partial state. `FENCING_OLD` and beyond are committed; a
post-cutover rollback is a new migration in the reverse direction.

**Online split, online merge, and worker drain** (§10.4–§10.7) all use
this state machine; they differ only in how the bucket set is computed.

### 10.3 Removing a Shard (Graceful)

Reverse of the above. The shard's keys are migrated to other shards via
checkpoint reads, then the SlateDB is decommissioned. SST GC will eventually
reclaim its object-store footprint.

### 10.4 Fault-Driven Reassignment

If a worker dies, its shards are re-leased to another worker. SlateDB's
single-writer enforcement (via the manifest fence epoch) prevents split-brain:
the old writer cannot commit after a new writer opens the same shard.

State transfer on fault-driven reassignment differs from proactive rebalancing:
there is no live donor to create a targeted checkpoint. Instead, the new worker
obtains state by opening each shard's SlateDB directly (no snapshot needed —
the shard's last durably committed epoch is already in the WAL). The new writer
replays any WAL entries beyond the last manifest checkpoint, then resumes
processing from the recovered epoch frontier. Recovery latency is bounded by
the last shard checkpoint age (`checkpoint_age_seconds`), not by live data
transfer. Proactive rebalancing (§10.2) uses checkpoint-based migration;
fault-driven reassignment uses WAL replay. Both paths surface the pipeline as
`RECOVERING` until the frontier catches up.

### 10.5 Per-Operator Parallelism

Operator parallelism is independent of the cluster's shard count. A small
aggregation might pin to 4 shards; a hot join might span 200 shards. The
compiler picks parallelism per operator based on:
- Estimated cardinality.
- Available cluster capacity.
- Historical execution statistics (collected via the observability stack).

Placement is locality-aware. If two adjacent operators have compatible
partitioning, the placement solver tries to co-locate their instances on the
same shard or worker so §7.5 can elide or loop back the exchange. It is allowed
to prefer locality over maximum parallelism when the SLO model predicts that
serialization and network cost dominate CPU.

Adaptive re-planning: if an operator's metrics show skew, the control plane can
re-shard that operator's state online while the rest of the pipeline keeps
running. For hot keys, re-sharding the whole operator is not enough, because
one logical key may still dominate one shard. RockStream therefore supports
**hot-key virtual buckets**:

1. Detect a key whose per-epoch CPU, bytes, or state writes exceed the
   `hot_key_factor` threshold over the median shard.
2. Split that logical key into `B` virtual buckets by salting with a stable
   hash of row identity or source partition.
3. Maintain partial state per `(logical_key, bucket)`.
4. Add a final unsalted combiner that merges the `B` partials before emitting
    view output.

For algebraic aggregates this is exact partial aggregation. For equi-joins,
the planner may split the large side and replicate the small/hot side across
the buckets, or decline the split if replication would exceed the state quota.
For operators without an exact partial-state encoding, the plan remains
unsplit and reports `SKEW_BOUND` if the SLO cannot be met.

**Exception: non-composable aggregates must not use bucket salting.**
A merge law is non-composable (`LawDescriptor.composable = false`) if it
requires access to the *full* key weight before emitting output. `WeightAdd/v1`
(DISTINCT) is non-composable: it fires output only on zero-crossings of the
total weight across all rows for a key. With bucket salting, per-bucket shards
each see a partial weight independently. A key with total weight 0 expressed
as (+1, bucket-0) and (−1, bucket-1) causes bucket-0 to emit a spurious +1
delta and bucket-1 to emit a spurious −1 delta before the combiner sees both.
If these deltas cross an epoch boundary (due to shuffle reordering, §7.1),
a downstream subscriber can receive and commit one delta before the combiner
cancels it, violating DISTINCT semantics.

For hot keys whose operator uses a non-composable law, the planner **must**
route all rows for that key to a single designated spill shard. The spill
shard applies the full-weight computation before redistributing the result.
Single-shard throughput is the fundamental ceiling for non-composable
aggregates on hot keys; this tradeoff cannot be avoided without weakening the
aggregate semantics. The planner emits `SKEW_BOUND_NON_COMPOSABLE` rather
than attempting bucket salting.

---

### 10.6 Proactive Shard Splitting

Shards are split *before* they become hot, not after. Each shard reports its
total state footprint (sum of all `op_state/*` and `view_output/*` ranges)
to the control plane on every epoch. When a shard's footprint crosses
`1.5 × target_shard_state_bytes` (default `target = 20 GB`), the control plane
schedules a background split:

1. Pick a midpoint key by sampling the shard's hash-range.
2. Allocate a new shard id; copy the upper half via SlateDB checkpoint + replay
   of writes since the checkpoint (§10.2).
3. Atomically update `shard_map/v{n+1}` to assign the new range to the new
   shard.

Splits are throttled (one per minute per shard) and respect the cluster's
adaptive-tuner budget. Operators never see "shard X is too large" pages,
because the split happens at 30 GB — well before any operational threshold.
`target_shard_state_bytes` is itself tunable per storage profile (§5.6).

The reverse operation — merging two cold shards — is also background and
keyed to a `min_shard_state_bytes` floor (default 4 GB) to prevent fragmentation
at low load.

### 10.7 Worker Drain Protocol

Scale-in requires gracefully removing a worker. The protocol is explicit:

1. Control plane marks the worker as `DRAINING` — no new shard assignments.
2. Each shard on the worker migrates to another worker via the §10.2
   checkpoint-copy flow.
3. Once all shards have been migrated and cutover is confirmed, the control
   plane marks the worker as `DECOMMISSIONED`.
4. The worker process exits (or the infrastructure terminates it).

The CLI surface is:

```
rockstream cluster workers drain <worker-id>   # initiate drain
rockstream cluster workers status <worker-id>  # shows DRAINING / DECOMMISSIONED
```

Without an explicit drain protocol, scale-in either kills the worker immediately
(losing in-flight state) or has no defined end condition. The drain state is
recorded in `topology/worker/` and audited. A worker in `DRAINING` state still
processes its remaining shards normally until migration completes — it is not
paused, merely ineligible for new placements.

### 10.8 Cluster Autoscaling Signals

**Cluster worker pressure.** The control plane continuously computes a pressure
signal that bridges intra-cluster adaptation and infrastructure provisioning:

```
cluster_worker_pressure = max over all pipelines of:
  (demanded_shard_count / placed_shard_count)
```

When `cluster_worker_pressure > 1.0` for `T` seconds (configurable,
default 60 s), the control plane emits a scale-out recommendation. When
it stays below a low-water threshold for `T_in` seconds (default 300 s),
it emits a scale-in recommendation.

**Infrastructure integration.** RockStream exports `cluster_worker_pressure`,
`demanded_shard_count`, and `placed_shard_count` as Prometheus metrics. The k8s
HPA or KEDA reads them and drives the autoscaler. Zero new RockStream code
beyond shipping the metrics; standard cloud-native pattern. The control plane
does not call infrastructure APIs directly.

**Worker capacity model.** Each worker reports `capacity_headroom` to the
control plane — the remaining available shards based on observed resource
utilisation (memory, I/O throughput, CPU). The placement algorithm respects
this signal: a worker at zero headroom receives no new shard assignments
regardless of hash affinity. Without this, the control plane can silently
overload a worker.

```
control: topology/worker/ worker_id → {
  ...,
  state: ACTIVE | DRAINING | DECOMMISSIONED,
  capacity_headroom: u32,       // remaining shard slots
  last_heartbeat: Timestamp,
}
```

---

## 11. Fault Tolerance & Exactly-Once Semantics

### 11.1 The Three Boundaries

| Boundary | Mechanism |
|---|---|
| **Within an epoch on one shard group** | Coalesced `WriteBatch` commit is atomic. |
| **Across operators in the same cluster** | Frontier protocol + idempotent operator state keyed by source epoch. |
| **External sources & sinks** | Two-phase commit on connector state; sink writes are keyed by `(source_epoch, output_position)`. |

### 11.2 Cluster Checkpoints

Every `T` seconds (or every `N` epochs), the control plane runs a
**barrier-based** checkpoint inspired by Flink Chandy-Lamport:

1. Inject a checkpoint barrier into every source operator with a fresh
   `checkpoint_id`.
2. Barriers flow through the DAG, aligned at multi-input operators (the operator
   waits until the barrier arrives on all inputs).
3. Each shard tracks which local operators have observed the barrier. When all
   operators on that shard have durably committed through the barrier, the
   shard creates **one** SlateDB `Checkpoint` and records
   `(checkpoint_id, shard_checkpoint_id)` in the control plane. Checkpoints are
   per shard, not per operator, which bounds manifest-write bursts.
4. Barrier alignment buffers are bounded by the same credit system as shuffle:
   if a fast input reaches its alignment credit limit while waiting for a slow
   input's barrier, the operator stops granting upstream credits and
   backpressure propagates to sources. A `checkpoint_alignment_timeout` turns
   excessive waiting into `RECOVERING` or `BLOCKED`, never unbounded memory.
5. When all shards have reported, the control plane commits the cluster
   checkpoint atomically: writes `control: checkpoints/{checkpoint_id}` with the
   full map of per-shard checkpoints. **Ships**: v0.49 — the
   `CheckpointManifestStore` decode path remains backward-compatible with legacy
   plain JSON manifests during rolling upgrade, then switches writes to a
   ZSTD-framed manifest once the control-plane capability floor advertises
   `checkpoint_manifest_codec_v1`.
6. Old cluster checkpoints (beyond the retention horizon) are released, allowing
   SlateDB GC to reclaim SSTs. **Ships**: v0.49 — manifest retention is
   implemented as point `put` + explicit scan/delete of old
   `control: checkpoints/` objects; no SlateDB or object-store range delete is
   required.

### 11.3 Recovery

To recover the cluster:
1. Pick the latest committed cluster checkpoint.
2. Open every shard's `DbReader` pinned to its recorded checkpoint.
3. Each worker brings up its assigned shards as writers, starting from the
   checkpointed state. **Ships**: v0.49 — recovery accepts both legacy JSON and
   ZSTD-framed cluster manifests from `control: checkpoints/`, so mixed-version
   clusters stay restart-safe throughout the upgrade.
4. Source connectors resume from offsets recorded in `control: connector/`.
5. Frontiers held in the checkpoint resume; processing continues.

### 11.4 Exactly-Once Sinks

Sink connectors implement the standard two-phase commit:

```
Pre-commit (during epoch):
  - Stage outgoing rows in a sink-specific transactional buffer.
    For Kafka: producer transaction, no flush yet.
    For S3:    write to "_pending/{epoch}/..." path.
  - Stage atomically committed in the shard's WriteBatch via a
    sink_state/ entry recording the pending position.

Commit (after cluster checkpoint succeeds):
  - For Kafka: commit producer transaction.
  - For S3:    atomic rename _pending → final.
  - Update sink_state/ to mark epoch as committed.
```

Replay after crash: on recovery, the connector inspects `sink_state/`:
- If committed: nothing to do; epoch already delivered exactly once.
- If pre-committed but not committed: recovery behavior depends on the
  sink's `SinkIdempotencyProfile` (see below).
- If neither: epoch's data will be reproduced via normal pre-commit; nothing to do.

**Sink idempotency profiles.** Because external systems differ in their
crash-recovery semantics, each sink implementation must declare one of the
following profiles. The recovery driver reads the profile at recovery time
and dispatches accordingly:

- `NativeIdempotent` — The external system supports idempotent re-commit
  natively (e.g., S3 conditional PUT using `If-None-Match`, Postgres
  `COMMIT PREPARED` after `PREPARE TRANSACTION` on a named transaction).
  Recovery calls `commit(epoch, checkpoint_id)` directly.

- `FencingTokenRequired` — The external system requires a fencing token to
  distinguish first commit from re-commit. The sink stores
  `hash(worker_id || shard_id || epoch)` as the idempotency key and checks
  it before applying the commit. Recovery calls `commit` with the same token.

- `CheckBeforeCommit` — The external system does not safely support re-commit
  after a broker/server-side abort (e.g., Kafka transactions are aborted by
  the broker after `transaction.timeout.ms`). Recovery must first query the
  external system's commit status (Kafka `describe_transactions`,
  Postgres `pg_prepared_xacts`). If the transaction was already committed,
  recovery only updates `sink_state/`. If the transaction was aborted,
  recovery must re-run `pre_commit → commit` against a new transaction.

Sink implementations **must** declare their `SinkIdempotencyProfile`. The
`KafkaSink` uses `CheckBeforeCommit`; `S3Sink` uses `NativeIdempotent`;
`PostgresSink` uses `NativeIdempotent` (via named `PREPARE TRANSACTION`).
A sink missing a declared profile is rejected at registration time.

---

### 11.5 Recovery Time as a Design Invariant

Recovery is not exceptional — it is steady-state behavior that the system must
hit reliably. RockStream commits to the following budgets at
`target_shard_state_bytes` (§10.6):

| Phase | Budget | Mechanism |
|---|---|---|
| Failure detection (worker silence → control-plane mark dead) | **≤ 5 s** | Heartbeat with `dead_after = 3 × interval`, default `interval = 1.5 s`. |
| Single-shard reassignment (mark dead → new owner serving reads at last committed frontier) | **≤ 30 s** | Stateless workers + checkpoint-from-storage (§11.3); no per-shard WAL replay larger than the last epoch's writes. |
| Pipeline freshness recovery (new owner serving → frontier within SLO) | **≤ 60 s** | Catch-up ingest at burst rate, bounded by `source_rate_max_burst` (§14.3). |

These budgets are first-class metrics:

```
failure_detection_seconds          (histogram)
shard_recovery_seconds             (histogram, by shard size bucket)
pipeline_freshness_recovery_seconds (histogram, by pipeline)
```

A pipeline that misses the 60 s budget surfaces the `RECOVERING_SLOW` named
degraded state (§14.10) and pages the operator. Phase 6 of the implementation
plan must demonstrate the budgets hold under simulated worker death, network
partition, and object-store throttling.

### 11.6 Network Partition and Worker Self-Fencing

The failure detector (§11.5) handles the case where a worker *dies*. A harder
case is a worker that is *alive* but partitioned from the control plane:

- The worker can still write to its SlateDB shards (object store reachable).
- The control plane cannot hear its heartbeats and eventually marks it dead.
- The control plane assigns the shards to a new worker.
- Now two workers may attempt to write the same shard.

SlateDB's manifest fence epoch prevents a committed double-write, but the
write races are wasteful and the partitioned worker would keep burning CPU.

**Self-fencing rule.** A worker that fails to deliver a heartbeat to the
control plane for `self_fence_after` seconds (default `2 × dead_after` =
30 s) proactively terminates itself. It does not drain or checkpoint; it
shuts down immediately so the new owner can acquire the lease without a fence
race. The value must satisfy:

```
self_fence_after > dead_after   (so the control plane marks it dead first)
self_fence_after < 2 × shard_recovery_budget  (so recovery still fits SLO)
```

The partitioned worker also stops processing new epochs the moment it detects
it cannot contact the control plane, so its shard states do not advance. When
it can reconnect, it re-registers as a new worker rather than resuming as the
old identity.

**Object-store-only partition.** If a worker can reach the control plane but
not the object store, it cannot commit epoch WALs. It reports
`BLOCKED(RS-3003 storage.object_store_unreachable)` and stalls. The control
plane does not mark it dead (heartbeats still arrive); it waits for recovery.
If the object store outage exceeds `object_store_stall_timeout` (default 5 min),
the operator is alerted via the degraded state channel.

### 11.7 Object Store Brownout

The system assumes object storage is available the vast majority of the time,
but a complete or partial outage must not cause data loss or duplicate output.

**During an object store brownout:**
- Workers stall at the epoch commit step (WAL flush blocks).
- Workers continue accepting connector credits up to `local_buffer_max_epochs`
  (default 10 epochs) in the tokio task input queue before applying
  backpressure to the source connector.
- After `local_buffer_max_epochs`, the source connector is credit-starved;
  Kafka consumption pauses; HTTP push sources return `429 Too Many Requests`.
- Frontiers stop advancing; pipeline transitions to `STRESSED` then
  `BLOCKED(RS-3003)`.
- The control plane surfaces `cluster_storage_stalled = true` in metrics
  and the degraded-state channel.

**Recovery once the object store returns:**
- Workers resume WAL flushes; buffered epochs commit in order.
- Frontiers advance normally; sources resume at the committed offset.
- No data loss (writes were buffered, not dropped).
- No duplicates (epoch keys are idempotent; re-flushing the same WriteBatch
  is a no-op if the WAL segment already exists).

This is the same "local buffer → backpressure → pause source" pattern used
by Flink's checkpoint alignment mechanism.

### 11.8 Thundering Herd on Cluster Restart

After a control plane failover or cluster-wide restart, all workers
simultaneously attempt to:
1. Open their SlateDB instances (each calls `get_latest_manifest()` on object
   storage — one PUT/GET per shard).
2. Replay their WAL tail.
3. Send heartbeats + frontier reports to the control plane.

With hundreds of workers and thousands of shards, the simultaneous spike in
object-store requests can trigger S3/GCS throttling (HTTP 429/503), which
delays manifest reads, which delays heartbeats, which triggers false failure
detection — a cascade.

**Staggered recovery.** Workers are assigned a startup jitter window based
on `worker_id mod jitter_buckets` (default 20 buckets × 1 s = 20 s spread).
A worker in bucket `k` waits `k × (jitter_window_ms / jitter_buckets)` ms
before beginning shard acquisition. Buckets are randomized per boot to prevent
synchronized re-collisions on repeated cluster restarts.

The control plane also implements a **lease grant rate limit**: it will
not issue more than `max_lease_grants_per_second` (default 50) shard leases
per second cluster-wide, ensuring shard openings are spread over time even
if all workers start simultaneously.

---

## 12. Query Serving

### 12.1 Four Query Modes

| Mode | Mechanism | Latency |
|---|---|---|
| **Materialized view lookup** | `DbReader` on the shard holding the view-output partition | µs–ms |
| **Materialized view range scan** | `scan()` on the relevant shard(s); merge results on the gateway | ms |
| **Ad-hoc SQL over views** | DataFusion query executes against a `Snapshot` of materialized views (no incremental engine involvement). Inline views (§4.3) are expanded before planning. | ms–s |
| **Historical view query** | Resolve epoch or timestamp to a checkpoint manifest; open `DbReader` at that snapshot across all relevant shards | ms–s (warm cache), s (cold) |

### 12.2 Query Gateway

A stateless query gateway service:
1. Parses incoming SQL.
2. Looks up which views satisfy the query (or rejects if none).
3. Routes range scans to the appropriate shards via `DbReader` connections.
4. Merges results with DataFusion's local executor.

Gateways are stateless and horizontally scalable. For each query they pin to a
**published vector frontier** — the antichain of per-shard checkpoints
associated with the most recent committed cluster frontier. All `DbReader`
handles for one query open at that vector, so multi-shard reads in the same
query see a causally consistent snapshot even though no global LSN exists. The
vector frontier's age is exposed in query metadata so clients can decide
whether to retry against a fresher one.

### 12.3 Subscribe / Streaming Queries

Clients can subscribe to a view's change stream. Implemented by tailing the
shard's `WalReader` filtered to `view_output/` for the requested view-id prefix.
Subscription connections are authenticated via the same token/certificate checked
by the gateway (§12.5); the gateway proxies the subscription rather than exposing
raw shard access to clients.

**Subscribe syntax and semantics:**

```sql
SUBSCRIBE <view>
  [AS OF NOW WITH SNAPSHOT]    -- bootstrap: emit current snapshot then live deltas
  [AS OF EPOCH <n>]            -- resume from a known epoch (within retention)
  [WHERE <predicate>]          -- server-side row filtering
  [(<col1>, <col2>, ...)]      -- column projection
```

Each change row contains: `mz_timestamp` (the epoch), `mz_diff` (`+1` for
insertion, `-1` for retraction), and the projected view columns. Updates are
delivered as retraction/insertion pairs at the same timestamp. Server-side
`WHERE` and column projection reduce network bandwidth for clients that need
only a subset of the view.

**`CHANGE_RETENTION`:** per-view retention for the change stream log:

```sql
CREATE MATERIALIZED VIEW orders_mv AS ...
  WITH (CHANGE_RETENTION = '1 hour');
```

Default: 1 hour. Subscribes that request `AS OF EPOCH <n>` outside the
retention window receive `RS-2005 history.epoch_before_retention`. The
retention period bounds the storage cost of keeping historical deltas for
late-joining subscribers.

### 12.3.1 Cross-Shard Read Pushdown

For aggregation queries over a distributed view (e.g., `SELECT COUNT(*), region
FROM orders_mv GROUP BY region`), naively routing all rows to the gateway
before aggregating wastes network bandwidth proportional to view cardinality.
The gateway instead pushes the partial aggregation to each shard:

1. The gateway decomposes the query into a **partial plan** (per-shard
   `GROUP BY + partial SUM/COUNT`) and a **merge plan** (gateway-side
   final aggregation).
2. Each shard executes the partial plan against its `view_output/` snapshot
   and returns a compact partial-aggregate batch (one row per group key).
3. The gateway merges the partial batches and returns the final result.

This reduces network bytes from O(view rows) to O(distinct group keys × shards)
— for a 1B-row view with 100 regions across 100 shards, the gateway receives
10,000 rows instead of 1B.

The partial plan is a subset of DataFusion's logical plan; no new operator
types are needed. The shard exposes a `partial_query(plan_bytes)` gRPC
method that accepts a Substrait fragment and returns an Arrow batch.
Pushdown is used only for safe, side-effect-free aggregation fragments;
joins and updates always use the full scan path.

**Shard-statistics scatter pruning** (§8.7) is orthogonal to partial aggregation
pushdown. Pushdown reduces *result size* (O(distinct groups) vs O(view rows));
shard-stats pruning reduces *scatter width* (K matching shards vs N total shards
for selective predicates). Both can apply to the same query: the planner prunes
the scatter set first using column statistics, then pushes the partial aggregate
to the remaining shards. `EXPLAIN` reports both effects separately.

### 12.4 Freshness Tokens and Read-Your-Writes

Every source commit, sink commit, and query response can carry a **freshness
token**. The token is **vector-valued**: a single materialized view can have
many sources upstream (joins, unions, view-on-view DAGs), and "fresh enough"
requires committing to a point on each contributing source, not on the
scalar that happened to commit last.

```rust
pub struct FreshnessToken {
    /// One entry per source that feeds the view, directly or transitively.
    /// Empty for views with no source dependencies (constant views).
    pub source_progress: BTreeMap<SourceId, SourceProgress>,
    /// Hash of the published cluster vector frontier this token was issued
    /// against. Used for fast equality on the gateway hot path; not a
    /// substitute for source_progress.
    pub cluster_frontier_hash: u64,
}

pub struct SourceProgress {
    /// Per-connector strictly-increasing source epoch (§8.1.1, §13.3).
    pub source_epoch: u64,
    /// Optional event-time watermark at the point of issue (§6.9). Used by
    /// AS OF MONOTONE PARTIAL and by analytics that need to know how stale
    /// the wall-clock view of the source is.
    pub event_time_watermark_ms: Option<i64>,
}
```

For normal low-latency reads, the gateway pins to the freshest published
vector frontier and returns the token it used. For read-your-writes, clients
pass `wait_for=<FreshnessToken>`; the gateway waits until **every entry** in
`source_progress` is dominated by the published vector frontier or until a
caller-supplied timeout expires. The query result then explicitly says
whether the token was satisfied.

Tokens compose: a `COMMIT` from one connection returns a token; the same or
another client may pass that token to a `SELECT` against any view that
transitively depends on the same source. View-on-view DAGs (§5.7) propagate
the source set through the catalog so a token for an upstream commit is
honored by any downstream view automatically. Tokens of disjoint source sets
are independent; passing an unrelated token is a no-op (the wait condition
is vacuously satisfied).

The scalar single-source form previously used in v0.46 is retained on the
wire only as the special case `source_progress.len() == 1`; the gateway
always serializes the vector form going forward.

### 12.4.1 Historical Queries (`AS OF`)

Any query against a materialized view can be qualified with a temporal clause
to read a past state of the view:

```sql
SELECT * FROM orders_mv AS OF EPOCH 4201;
SELECT * FROM orders_mv AS OF TIMESTAMP '2026-01-15T09:00:00Z';
```

**Resolution.** The gateway resolves the requested point to the nearest
committed cluster checkpoint whose frontier dominates the requested epoch:

- `AS OF EPOCH <n>`: look up `control: checkpoints/` for the checkpoint whose
  committed epoch frontier dominates epoch `n` on all relevant shards.
- `AS OF TIMESTAMP <t>`: look up the checkpoint whose commit wall-clock time
  is the greatest value ≤ `t`.

**Granularity disclosure.** The effective granularity of `AS OF` is
**checkpoint-bounded, not epoch-bounded**. If the cluster checkpoint interval
is 30 seconds and epochs are 100 ms, then `AS OF EPOCH 4201` and
`AS OF EPOCH 4250` may resolve to the same checkpoint and return the same
snapshot. The query response includes a `resolved_checkpoint_epoch` field so
clients can observe the actual resolution point. Two queries with different
epoch arguments that resolve to the same checkpoint are guaranteed to return
identical results. Clients that need finer-grained history should reduce the
checkpoint interval (at the cost of higher object-store writes) or use the
subscribe API for epoch-level deltas.

If no matching checkpoint exists (too old, or before the pipeline was created),
the gateway returns `RS-2005 history.epoch_before_retention`.

**Execution.** The gateway opens `DbReader` handles at the resolved checkpoint
manifest rather than at the current frontier. Multi-shard scans pin all handles
to the same historical checkpoint vector, preserving causal consistency at the
past point. The query then proceeds identically to a live range scan or ad-hoc
SQL query.

**Retention boundary.** Historical queries are bounded by the view's retention
policy (§5.7). Data beyond the retention window is removed by compaction
filters and is unavailable regardless of whether the checkpoint manifest still
exists. Checkpoint manifests themselves are garbage-collected by the cluster
checkpoint GC (§11.3) according to a configurable
`checkpoint_retention_count` (default: 128 checkpoints) and
`checkpoint_retention_duration` (default: equal to the view's retention or 7
days, whichever is shorter).

**Interaction with freshness tokens.** `AS OF` and `wait_for=<token>` are
mutually exclusive on the same query. `AS OF` reads a past snapshot;
`wait_for` waits for the frontier to advance to a future point.

### 12.5 Authentication and Authorization

All external interfaces (SQL port, gRPC subscribe, REST/HTTP) enforce the same
auth layer:

- **Authentication**: bearer tokens (JWTs signed by a configurable OIDC
  issuer) or mutual TLS client certificates. Tier 1 (`--auth=off`) skips auth
  for local development.
- **Authorization**: per-resource RBAC stored in the control-plane catalog
  (`catalog/acl/`). Principals are identified by the JWT `sub` or certificate
  CN. Default roles:

| Role | Can do |
|---|---|
| `viewer` | `SELECT` from granted views; subscribe to granted views. |
| `pipeline_owner` | Deploy, alter, pause, resume, drop views they own; all viewer rights on owned views. |
| `admin` | Everything, including granting roles and viewing all audit-log entries. |

- **Multi-tenancy isolation**: views are namespaced (§5.2). A
  principal with `pipeline_owner` on namespace A cannot see or affect namespace
  B's views and workloads. Quota enforcement (§14.13) is per-workload and per-namespace,
  so tenants cannot starve each other by default. Hard isolation is available
  via worker-pool affinity (`CREATE NAMESPACE ... WITH (worker_pool = '...')`).
- **Audit trail**: every query, subscription, deploy, and alter carries the
  authenticated principal as the `actor` in the audit log (§14.11).

`rockstream` CLI uses the same OIDC token flow (`rockstream login`) or a
service-account key file. Unauthenticated requests are rejected at the gateway
before they reach any shard or control-plane write path.

### 12.6 Postgres Wire Protocol Compatibility

The query gateway speaks the **PostgreSQL wire protocol** (via the `pgwire`
crate). Applications can connect with `psql`, standard JDBC/ODBC drivers, and
any ORM that supports Postgres — without code changes for read-heavy and
streaming workloads.

**Isolation levels supported:**

| Postgres level | RockStream implementation |
|---|---|
| `READ COMMITTED` | Each statement pins to the latest published vector frontier at statement start. |
| `REPEATABLE READ` | `BEGIN` pins the session to a specific vector frontier; all statements in the transaction see that snapshot. |
| `SERIALIZABLE` | **Not supported** for cross-shard transactions (requires cross-shard conflict detection; see §1.1). Returns `RS-2003 isolation.serializable_not_supported`. |
| `SERIALIZABLE LOCAL` | v0.52: when the planner proves all reads/writes touch one shard, delegates to per-shard SlateDB transaction semantics. |

**Optimistic write semantics** (§13.5.1): direct-write transactions may use
optimistic exact-key guards (`RS-2008` on conflict) and blind CRDT writes
without requesting `SERIALIZABLE`. These are orthogonal to isolation level
and documented as stale-overwrite protection, not as an ANSI isolation
guarantee.

**Postgres catalog compatibility** required for ORMs:

- `pg_catalog.pg_tables`, `pg_views`, `pg_class`, `pg_attribute`,
  `pg_namespace`, `pg_type` — populated from the control-plane catalog.
- `information_schema.tables`, `information_schema.columns` — generated views.
- Postgres native **type OIDs** sent in row description messages so drivers can
  decode column types without metadata round-trips.
- `SET search_path`, `SHOW server_version`, `SHOW transaction_isolation` —
  stub responses sufficient for ORM connection probes.

**Postgres wire protocol does NOT imply a Postgres drop-in.** DDL (`CREATE
TABLE`, `ALTER TABLE`) is handled via `CREATE MATERIALIZED VIEW` / `CREATE VIEW` /
`CREATE WORKLOAD` semantics. Write DML goes through the internal source connector (§13.5).
`COPY FROM STDIN` (text format) is supported as of v0.27 (§12.6.2). `LISTEN`/`NOTIFY` and advisory locks remain out of scope.

### 12.6.1 The `rockstream_catalog` System Schema

The canonical user-facing system schema is `rockstream_catalog`. In addition
to `pg_catalog` and `information_schema`, the gateway exposes
`rockstream_catalog` containing virtual tables materialized from the
control-plane catalog and checkpoint index. These tables are read-only and
require no additional storage — they project existing control-plane state
through the standard SQL interface.

> **Legacy aliases.** Older specifications and earlier roadmap versions
> referred to these tables under the unqualified `rockstream.*` prefix.
> `rockstream.*` is accepted as a read-only alias through v0.49 and removed
> in v0.54. The DLQ table previously named `rockstream_catalog.dead_letter_queue`
> stays under `rockstream_catalog`; that naming was already correct.

| Table | Source | Purpose |
|---|---|---|
| `rockstream_catalog.epochs` | `control: checkpoints/` + `shard_meta/0x06` | Per-view committed epoch history: epoch number, commit timestamp, frontier hash, shard count, row delta count. |
| `rockstream_catalog.workloads` | `control: catalog/workload` | Workload metadata, resource policy, current quota utilisation, priority. |
| `rockstream_catalog.views` | `control: catalog/view` | View definitions, retention policy, arrangement count, state bytes, lifecycle state, `workload_source` (`view` \| `namespace_default` \| `system_default`) indicating how the workload assignment was resolved. |
| `rockstream_catalog.shards` | `control: topology/` | Shard placement, worker assignment, frontier position, state size. |
| `rockstream_catalog.connectors` | `control: connector/` | Connector status, latest committed offset, lag. |
| `rockstream_catalog.audit_log` | `control: audit/` | Recent audit events (bounded by a configurable window, default 7 days). |
| `rockstream_catalog.schema_history` | `control: schema/` | Per-view schema version history: version, applied timestamp, actor, change summary. |

**`rockstream.epochs` scalability.** On a busy cluster (e.g. 1,000 shards ×
10 rows/s/shard × 100 ms epochs = 864 billion logical rows/day), storing one
row per shard per epoch is infeasible for unbounded query. The system schema
therefore provides `rockstream.epochs` as a **pre-aggregated, pipeline-scoped
summary** — one row per *committed cluster checkpoint*, not per shard-epoch.
At a 30-second checkpoint interval, this is ~2,880 rows/day regardless of
shard count. Queries against `rockstream.epochs` **require** a `pipeline_id`
filter and are served from the checkpoint index, not from raw shard metadata.
Unfiltered `SELECT * FROM rockstream.epochs` is rejected with `RS-2006
system_table.requires_filter` to prevent accidental full scans. For per-shard
epoch detail, `rockstream.shard_epochs` is available as a detail table with
mandatory `pipeline_id` + time-range predicates and cursor-based pagination
(default page size 10,000 rows).

Example queries:

```sql
-- Recent epoch history for a pipeline
SELECT epoch_num, committed_at, row_delta_count, shard_count
FROM   rockstream.epochs
WHERE  pipeline_id = 'orders'
ORDER  BY epoch_num DESC LIMIT 20;

-- Schema evolution audit trail
SELECT version, applied_at, actor, change_summary
FROM   rockstream.schema_history
WHERE  view_id = 'orders_mv'
ORDER  BY version DESC;

-- Current workload health at a glance
SELECT workload_name, memory_limit, memory_used_gb, priority
FROM   rockstream.workloads;

-- Per-view SLO compliance
SELECT view_name, workload_name, state, slo_compliance, degraded_reason
FROM   rockstream.views;
```

These tables are available in all runtime profiles. In `embedded` mode they
query in-process state; in `distributed` mode they query the control-plane
`DbReader`. Access is subject to the same RBAC rules as user views (§12.5):
`viewer` can see metadata for views they have access to; `admin` can see all
entries including the full audit log.

**Positioning**: with the Postgres wire layer plus the internal source connector
(§13.5), RockStream operates as a *streaming SQL platform with Postgres-compatible
read access*, not as a general-purpose transactional Postgres replacement. Clients
write rows directly; the IVM engine keeps views fresh; `psql` queries views.

### 12.6.2 COPY Protocol

As of v0.27, `COPY <table> FROM STDIN` (text format, tab-separated values) is
supported directly via the Postgres wire layer.

**Flow:**
1. Client sends `COPY <table> [(col1, col2, ...)] FROM STDIN`.
2. Gateway parses the statement, resolves column list from the catalog if not
   specified, and sends `CopyInResponse`.
3. Client streams `CopyData` messages (TSV rows terminated by `\.\n`).
4. Client sends `CopyDone`; gateway flushes the final batch and responds with
   `CommandComplete COPY N`.

**Memory safety (auto-flush):**  
Each per-connection COPY IN buffer is bounded by two named limits:
- `MAX_COPY_IN_BATCH_ROWS = 10_000 rows`
- `COPY_IN_FLUSH_BYTES = 64 MiB`

Whichever limit is reached first triggers an auto-flush: the current batch is
committed to the shard as a `WriteBatch` (put-only, no range-deletion), and the
buffer is reset. The fill-level metric `COPY_IN_BUFFER_ROWS` (gauge) tracks the
current row count across all active COPY IN connections.

**Error codes:**
| Code | Slug | Trigger |
|------|------|---------|
| RS-2500 | `copy.table_not_found` | COPY target table not in catalog |
| RS-2501 | `copy.column_count_mismatch` | TSV field count ≠ declared column count |

**Auth:** `pipeline_owner` role required (RS-2400/RS-2401 otherwise). See §12.5.

**No range-delete dependency.** Flush uses `WriteBatch::put` only (identical to
the regular `COMMIT` path). No scan-and-delete is required.

---

### 12.7 Two-Tier View Storage (Design Decision)

RockStream's storage is **row-oriented at rest** (SlateDB LSM) and **columnar
in flight** (Apache Arrow `RecordBatch`). This is the right choice for the
primary workload: incremental delta processing operates on individual rows, and
SlateDB's point/range semantics match that exactly.

However, this creates a performance asymmetry for secondary workloads:

| Workload | Hot LSM | Cold Parquet |
|---|---|---|
| Incremental delta merge | fast (point/range ops) | n/a — write path stays LSM |
| Point lookup on materialized view | µs–ms | n/a |
| Range scan by partition key | fast (key-prefix scan) | fast (column scan + pushdown) |
| Full collection scan (billions of rows) | slow (row-by-row LSM) | fast (vectorized columnar I/O) |
| Ad-hoc SQL not aligned with partition key | slow (full LSM scan) | fast (DataFusion Parquet reader) |

#### 12.7.1 The Two-Tier Model

The gateway can serve view queries from two tiers:

```
Hot tier (SlateDB LSM):      recent epochs not yet snapshotted
                             — point lookups, range scans, delta merges
                             — O(recent deltas) size

Cold tier (Parquet/Iceberg): periodic columnar snapshots of view output
                             — full scans, ad-hoc analytics, DataFusion Parquet reader
                             — full collection size, compressed columnar
```

For a full-collection query the gateway merges both tiers: read the cold
snapshot (fast columnar scan) plus the hot LSM tail (small, recent deltas).
The merge is a **versioned signed Z-set merge**, not a blind union with
`row_id` dedup: every cold-tier row and every hot-tier delta carries
`(row_id, weight, version)`, where `version` is the commit epoch and `weight`
is the Z-set sign (±1). The merge folds rows by `row_id` taking the **latest
version's weight**, then filters to weight ≠ 0. This is the same algebra the
in-memory operators use; it preserves CDC update semantics (an `UPDATE` that
arrives in the hot tail after the cold snapshot correctly retracts and
re-emits) and OR-Set / counter / register semantics through the same
`LawBundle::merge_fn` (§6.11) the IVM hot path uses.

A simple `row_id` dedup union (cold ∪ hot keyed by `row_id`) is permitted
only for **insert-only views** whose root operator declares monotonicity in
`EXPLAIN INCREMENTAL` (e.g. monotone reachability, append-only logs). The
planner refuses the cheap dedup path for any other view; the §12.7.3
`ViewReader::TwoTier` always reports which merge mode it chose so
operators can audit it. This is the difference between "structurally
identical to Paimon/Hudi MOR" and "actually correct for retraction-bearing
views" — and we choose correct.

#### 12.7.2 The Cold Tier Is a Natural Extension of Checkpointing

The cold tier is generated by the existing checkpoint mechanism (§11.2) with a
different output format: Parquet files written to object storage instead of (or
alongside) SlateDB checkpoints. The gateway's DataFusion execution already reads
Parquet natively. No new distributed protocols are needed.

Cold snapshots are written as a valid **Iceberg v2 table** (default) or Delta
Lake commit to `views/{view_id}/iceberg_table/` under the pipeline's
object-store prefix. The Iceberg table layout follows the standard spec:

```
views/{view_id}/iceberg_table/
  metadata/
    v1.metadata.json          # table schema, partition spec, sort order
    snap-{epoch}.avro         # snapshot manifest list (one per cold snapshot)
  data/
    {partition_key=value}/
      {shard_id}-{epoch}.parquet   # data files, one per shard partition
```

Each committed cold snapshot is a valid Iceberg snapshot with its own
`snapshot-id` and `sequence-number`. The manifest list points to manifest
files; manifest files point to Parquet data files with column-level statistics
(min/max, null count) for predicate pushdown.

This makes every cold snapshot a **first-class Iceberg table** that external
tools can consume directly — with no RockStream gateway in the read path:

```sql
-- DuckDB reads the latest cold snapshot directly from object storage
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('s3://bucket/views/orders_mv/iceberg_table');

-- Or register it once and query by name
CREATE VIEW orders_mv AS
  SELECT * FROM iceberg_scan('s3://bucket/views/orders_mv/iceberg_table');
SELECT region, SUM(total) FROM orders_mv GROUP BY region;

```

For the hot LSM tail (epochs after the last cold snapshot), external tools
either tolerate bounded staleness equal to the snapshot interval, or query
RockStream's gateway for the tail and merge client-side. The snapshot interval
is a per-view knob (`cold_snapshot_interval`, default: every 128 epochs or
5 minutes, whichever comes first). See §13.6 for the sink connector that
writes these snapshots.

#### 12.7.3 Cold-Tier-Aware Gateway Interface

**This is the critical design constraint**: the gateway's `ViewReader` trait
must be cold-tier-aware from Phase 9, even if the cold tier is not yet
implemented. Adding the cold-tier read path after the gateway ships forces a
gateway rewrite.

The `ViewReader` trait exposes two read strategies, resolved at query planning
time:

```rust
pub enum ViewReadStrategy {
    /// Read entirely from hot LSM (default; always available).
    HotOnly,

    /// Read cold Parquet snapshot for epochs ≤ snapshot_epoch, then merge
    /// the hot LSM tail for epochs > snapshot_epoch.
    /// Requires cold_tier_enabled = true on the view.
    TwoTier {
        snapshot_manifest: SnapshotManifest,
        hot_tail_from_epoch: u64,
    },
}
```

The gateway planner selects `TwoTier` when all of the following hold:

1. The view has `cold_tier_enabled = true` in the view catalog entry.
2. A cold snapshot exists for the requested query frontier.
3. The estimated scan cost exceeds `cold_tier_scan_threshold` (default: 10M
   rows), detected via the DataFusion planner's row-count estimate on the hot
   LSM path.

For all other queries (point lookups, range scans by partition key, subscribe),
the gateway always uses `HotOnly` regardless of whether a cold tier exists.

In Phase 7 (v0.26 ✅ done), only `HotOnly` is implemented. The `ViewReadStrategy`
enum and the `ViewReader` trait are defined in full so that the cold-tier
implementation (Phase 10 / v0.33) slot-fits without touching the gateway
planner.

#### 12.7.4 Competitive Position

| Workload | RockStream (hot only) | RockStream (two-tier) | DuckDB/Trino |
|---|---|---|---|
| Incremental view freshness | **10–250 ms** | **10–250 ms** | n/a |
| Point lookup on view | **µs–ms** | **µs–ms** | n/a |
| Pre-aggregated dashboard read | fast | fast | n/a |
| Full scan over 1B-row view | slow | **fast** | fast |
| Ad-hoc analytics | slow | **fast** | fast |

Without the cold tier, RockStream's natural role is producing fresh,
pre-computed results and serving point/range lookups on them — **feeding**
columnar analytics tools (DuckDB, Trino) via live Iceberg snapshots
rather than competing with them on full-scan queries. With the cold tier, it
becomes competitive for ad-hoc analytics over its own data as well.

#### 12.7.5 Implementation Scope

The `HotOnly` gateway path shipped in Phase 7 (v0.26, ✅ done). The cold-tier
`TwoTier` path and the cold-snapshot checkpoint writer ship at v0.33 (Phase 10
of NEW_ROADMAP.md — Cold-Tier Sinks & Simulator Fixes).

---

### 12.8 OLTP Session Ergonomics

Two capabilities make the direct-write path feel transactional to application
developers without requiring them to manage freshness tokens or issue extra
read round-trips.

#### 12.8.1 Session-Scoped Automatic Read-Your-Writes

v0.25 added `wait_for=<FreshnessToken>` as an explicit per-query opt-in.
For OLTP patterns — write a row, then read it back — clients must thread the
token from the write response into the next read. Standard database drivers
do not do this, so application developers face stale reads unless they add
custom middleware.

**Session-scoped tracking.** The gateway session maintains:

```rust
struct SessionState {
    // ...existing fields...
    last_written_epoch: Option<FreshnessToken>,
}
```

On any successful `COMMIT` through the internal source connector (§13.5),
the gateway updates `last_written_epoch` with the freshness token for that
commit. On any subsequent `SELECT` in the same session, the gateway
automatically applies `wait_for = last_written_epoch` before pinning the
vector frontier — exactly as if the client had passed the token explicitly.

**Visibility contract.** Within a session:
- Any row written by a prior `COMMIT` in the same session is visible to all
  subsequent `SELECT` statements, with no client-side coordination.
- If the frontier has not yet advanced (e.g. under high write rate), the
  gateway waits up to `session_wait_for_timeout` (default: equal to the
  pipeline's freshness SLO) before returning. Timeout returns `RS-2012
  session.wait_for_timeout` and the query proceeds at the current frontier.
- The token is reset on explicit `SET TRANSACTION ISOLATION LEVEL READ
  COMMITTED` / `REPEATABLE READ` or on connection close.

**Opt-out.** A session can disable automatic wait-for with
`SET rockstream.session_wait_for = off` (equivalently
`SET session_read_after_write = OFF`). This is useful for analytical
sessions that perform bulk reads after streaming ingestion and do not need
read-your-writes. A per-query escape hatch is also available:

```sql
SELECT /*+ ALLOW_STALE */ * FROM order_summary WHERE order_id = 42;
```

**Cross-session write fence.** For applications that need to pass a write
token to a different session or service (e.g. a microservice that writes and
then notifies a reader service):

```sql
-- Writer session: obtain a fence token after a write
SELECT rockstream.write_fence() AS fence;

-- Reader session: wait for that specific write to be visible
SELECT * FROM order_summary WHERE rockstream.after_fence(:fence);
```

The manual token API remains available for cross-session consistency. Session-
scoped read-after-write is the default experience for the common case.

**No new distributed machinery.** Automatic session wait-for is purely
gateway session bookkeeping. It reuses the existing `wait_for` code path;
the only new state is one `Option<FreshnessToken>` per session and the
`session_wait_for_timeout` SLO parameter.

**Metrics added:**

| Metric | Description |
|---|---|
| `session_wait_for_triggered_total` | Count of queries where implicit wait-for was applied. |
| `session_wait_for_satisfied_ms` | Histogram: time from trigger to frontier satisfaction. |
| `session_wait_for_timeout_total` | Count of queries that exceeded the SLO and fell through to current frontier. |

**Shipped**: at v0.26 (the Postgres Pillar milestone), ahead of the version
this section originally cited — see `SessionState::last_written_epoch` and
the `SESSION_WAIT_FOR_*` counters in `rockstream-gateway/src/session.rs` /
`server.rs`. (Corrected 2026-07-11.)

#### 12.8.2 `INSERT ... RETURNING`

The standard OLTP pattern is: write a row with an auto-assigned identity
(e.g. a UUID primary key or a sequence), then use that identity in a
subsequent operation. Without `RETURNING`, the client must either pre-generate
the key client-side or issue a second `SELECT` after commit.

**Syntax:**

```sql
INSERT INTO orders (customer_id, total)
VALUES (42, 199.99)
RETURNING order_id, created_at;
```

**Execution.** The gateway:
1. Executes the `INSERT` through the internal source connector, receiving the
   committed `FreshnessToken`.
2. Sets `last_written_epoch` (§12.8.1) and waits for the frontier to advance
   to the written epoch.
3. Executes a point read `SELECT ... WHERE <pk> = <inserted_pk>` against the
   shard at the satisfied frontier.
4. Returns the projected columns to the client inline with the `INSERT`
   response.

From the client's perspective, `INSERT ... RETURNING` is a single round-trip:
the response contains the written row as if it were a `SELECT` result.

**Constraints and error handling:**

| Case | Behaviour |
|---|---|
| Auto-generated primary key (default UUID or `gen_random_uuid()`) | Key is assigned before the `WriteBatch` commit; `RETURNING` reads by that key. |
| Sequence column | Sequence is incremented atomically in the `WriteBatch`; `RETURNING` reads the assigned value. |
| Multi-row `INSERT ... SELECT` | `RETURNING` returns one row per inserted row in insertion order. |
| Wait-for timeout | Returns `RS-2012`; partial row may be committed but not readable; client must retry with idempotency key. |

`INSERT ... RETURNING` does **not** extend to `UPDATE ... RETURNING` or
`DELETE ... RETURNING`. Those variants require the gateway to read old state
before the write, which adds read-modify-write latency. They ship at v0.48
(Phase 14 — Advanced DML & Scatter Pruning; corrected 2026-07-11 — this line
previously cited a version-numbering scheme that predates this roadmap's
Phase 13 insertion).

**Shipped** (literal-echo form): at v0.24, for client-supplied literal
`VALUES` — see `returning_rows` in `rockstream-gateway/src/server.rs`.
**Not yet shipped** (this subsection's actual new content: reflecting back a
*server-assigned* key, e.g. a default UUID or sequence, via the
point-read-back flow described above): `parse_insert` today only echoes the
client's literal values, so a caller cannot retrieve a server-generated key.
**Ships**: v0.45 (found unscheduled by the 2026-07-11 control-plane/
multi-tenancy review; see NEW_ROADMAP.md).

#### 12.8.3 Session-Scoped Max-Staleness for Analytical Queries

OLTP sessions use `session_wait_for` (§12.8.1) to ensure read-your-writes.
Analytical sessions have the opposite need: they want low-latency reads
against a recent-enough snapshot and do not need to block on a specific write
propagating. `SET rockstream.max_staleness` provides this guarantee.

**Session parameter:**

```sql
SET rockstream.max_staleness = '10s';   -- accept snapshots up to 10 s old
SET rockstream.max_staleness = '0';     -- require the freshest published frontier
SET rockstream.max_staleness = 'none';  -- no bound (default; equivalent to '0')
```

**Behaviour.** Before pinning the vector frontier for a `SELECT`, the gateway
checks the wall-clock age of the most recently published cluster frontier. If
`age ≤ max_staleness`, it pins immediately without waiting. If
`age > max_staleness`, the session emits a `NOTICE` (`RS-2018
session.staleness_exceeded`) and still uses the current frontier — it **never
blocks**. The query result includes a `frontier_age_ms` field in the response
metadata so clients can observe the actual staleness.

**Interaction with `session_wait_for`.** These session knobs are mutually
exclusive:

| Setting | Behaviour |
|---|---|
| `session_wait_for = on` (default) | OLTP mode: implicit `wait_for` after every `COMMIT`; blocks up to `session_wait_for_timeout`. |
| `max_staleness = '<duration>'` | OLAP mode: disables implicit `wait_for`; accepts any snapshot within the staleness bound. |
| `session_wait_for = off` | Opt-out of `wait_for`; no staleness bound either. |

Setting `max_staleness` implicitly sets `session_wait_for = off` for the
session. Setting `session_wait_for = on` implicitly clears `max_staleness`.
The active mode is visible via `SHOW rockstream.session_mode`.

**No new distributed machinery.** `max_staleness` is purely gateway session
bookkeeping. The frontier age is derived from the publication timestamp already
attached to the cached cluster frontier.

**New error code:** `RS-2018 session.staleness_exceeded` — emitted as a
`NOTICE` (not an error) when the published frontier is older than
`max_staleness` and the query proceeds with the stale frontier.

**Metrics added:**

| Metric | Description |
|---|---|
| `session_staleness_exceeded_total` | Count of queries where `max_staleness` was exceeded and the session fell through to the stale frontier. |
| `session_frontier_age_ms` | Histogram: frontier age at `SELECT` time for sessions with `max_staleness` set. |

**Ships**: v0.45 (corrected 2026-07-11 — this line previously cited a
version-numbering scheme that predates this roadmap's Phase 13 insertion;
found unscheduled by the 2026-07-11 control-plane/multi-tenancy review,
since `max_staleness` has zero references anywhere in `crates/` today).

---

## 13. Connectors & External I/O

### 13.1 Source Connectors

Each source operator is connected to an external system via a **source
connector** (Kafka, Postgres logical replication, S3 + manifest, HTTP webhook,
…). The connector:
- Reads from the external source.
- Decodes records into Z-set deltas.
- Assigns each delta a source epoch (typically the connector's native offset
  packed into the `source_epoch` field).
- Pushes deltas into the source operator (which lives on some shard).
- Updates `control: connector/{connector_id}/offset` atomically with the
  shard's commit.

### 13.2 Sink Connectors

Symmetric: a sink operator collects committed view-output deltas (from
`WalReader`), buffers them per the 2PC protocol in §11.4, and commits to the
external system after cluster checkpoints.

### 13.3 Connector Contract

Connector types are pluggable, but the contract is fixed. Built-in connectors
implement it as Rust traits; external connectors use the same protocol over
gRPC so they can run in a separate connector tier.

The contract is split into two tiers:

| Tier | Required for | Features |
|---|---|---|
| **Tier 1** | All connectors | Opaque `OffsetToken`, event-time watermark channel, backpressure feedback, DLQ routing, `prepare`/`commit`/`abort` on sinks |
| **Tier 2** | File-format sources/sinks only (opt-in) | Partition-filter pushdown, `should_flush` buffering override |

A connector that does not implement Tier 2 features advertises this via
`partition_filter_support() -> bool` (returns `false`) and by not overriding
`should_flush` (the default implementation returns `true`, flushing every
epoch). The planner skips pushdown for connectors that report no support;
operator-layer filtering produces identical results. Tier 1 connectors pass
the full contract test suite without implementing any Tier 2 surface.

**Opaque offset type.** Source positions are carried as an `OffsetToken`
(serialisable opaque bytes), not as a scalar. Kafka encodes a
`{ partition_id → offset }` map; Postgres CDC encodes an LSN; S3 / Iceberg /
Delta encode a manifest pointer; Kinesis encodes a shard-sequence map.
Making the type opaque keeps the trait stable across every realistic
source and is what `control: connector/{id}/offset` and the epoch_map
entries in §8.1.1 already store.

**Event-time watermark channel.** Source-offset progress (`OffsetToken`) is
processing-time progress. It is not sufficient for time-window operators
(§6.9), which need to know when it is safe to close a window over
out-of-order event streams. Source connectors therefore emit a second,
independent signal: a monotonic `EventTimeWatermark` returned alongside
the delta batch. The source operator propagates this as an event-time
antichain advance through the frontier protocol (§8).

**Watermark capability declaration is mandatory and fail-closed.** Every
connector declares `watermark_capability() -> WatermarkCapability` in
`discover_schema`:

| Value | Meaning |
|---|---|
| `Native` | Connector produces a watermark on every `poll_delta` from the underlying source (Kafka with embedded watermarks, Postgres CDC LSN as event-time proxy, etc.). |
| `ExternalHint` | Connector accepts watermarks from an out-of-band control channel (heartbeat connector, manual `ALTER SOURCE ... ADVANCE WATERMARK`). |
| `None` | Connector cannot produce a watermark under any conditions. |

`CREATE MATERIALIZED VIEW` on any TUMBLE/HOP/SESSION window must pick a
matching `WATERMARK = NATIVE | PROCESSING_TIME | EXTERNAL '<src>' | NONE
WITH WINDOW_CLOSE_DISABLED` policy (§6.9). Mismatches (e.g.
`WATERMARK = NATIVE` against a source whose capability is `None`) are
rejected at DDL time with `RS-1005 connector.watermark_required`. The
fail-closed default — silently accepting `None` and leaving windows open
forever — is the v3.27 behavior this section deprecates.

**Backpressure feedback.** The credit-based backpressure system (§7.2, P14)
governs operator-to-operator flow, but the connector sits upstream of the
operator graph and would otherwise consume at full source rate while
downstream is saturated. The source operator therefore exposes
`credits_available() -> usize` (in the Rust trait, a `tokio::sync::Semaphore`
permit count; over gRPC, a flow-controlled stream). `poll_delta` must check
this before consuming and stop polling when the pool runs dry. This bounds
the in-flight memory footprint at the connector boundary regardless of
source burst rate.

**[Tier 2] Partition-filter pushdown.** When a source reads a partitioned table
format (Iceberg, Delta Lake, Hudi, Parquet-manifest), the planner's
predicate-pushdown pass may already know which partition columns to restrict.
Rather than scanning all partitions and discarding non-matching rows in the
operator layer, the planner passes a `PartitionFilter` — a conjunction of
simple column predicates — directly to `start_snapshot` and `poll_delta`.
Connectors that support pushdown skip non-matching partition directories
entirely; connectors that do not simply ignore the filter (or return `None`)
and fall back to operator-layer filtering. The filter type is defined in the
connector contract and does not depend on DataFusion internals:

```
/// Partition-column predicates pushed from the planner to skip directories
/// the pipeline cannot match.  Predicates are ANDed together.
enum PartitionPredicate {
    Eq   { column: String, value: ScalarValue },
    In   { column: String, values: Vec<ScalarValue> },
    Range{ column: String,
           lo: Option<ScalarValue>, hi: Option<ScalarValue> },
}
type PartitionFilter = Vec<PartitionPredicate>;
```

`ScalarValue` is RockStream's own minimal scalar type (bool, integer, float,
string, date, timestamp); it is serialisable over gRPC and does not carry a
DataFusion type-system dependency.

**[Tier 2] Sink file aggregation.** The epoch-commit protocol checkpoints state
every epoch for exactly-once recovery, but physical file writes to
Iceberg/Delta/Hudi must be large (128 MB–1 GB) to avoid the small-files
problem. File-format sinks may override `should_flush` to separate
*checkpoint granularity* from *physical write granularity*. When
`should_flush` returns false, pending rows are staged as
`connector/{id}/pending_buffer` in the shard SlateDB and participate in the
epoch checkpoint, so they survive a crash between epochs. Physical file writes
happen only when the connector decides the buffer is large enough. The
epoch-commit protocol guarantees exactly-once regardless of the flush policy.
The default implementation of `should_flush` returns `true` (flush every
epoch), which is always correct and is the right behavior for non-file-format
sinks (Kafka, Postgres).

Source connectors must provide (Tier 1 required; Tier 2 optional):

```
discover_schema()                         -> SchemaVersion
start_snapshot(frontier,
               partition_filter: Option<PartitionFilter>)  // Tier 2; pass None if unsupported
  -> SnapshotStream
poll_delta(after: OffsetToken,
           max_bytes: usize,
           credits_available: usize,
           partition_filter: Option<PartitionFilter>)      // Tier 2; pass None if unsupported
  -> { batches: Vec<RecordBatchDelta>,
       new_offset: OffsetToken,
       watermark: Option<EventTimeWatermark> }
commit_offset(epoch, offset: OffsetToken) -> IdempotentResult
pause(reason) / resume()
partition_filter_support() -> bool        // Tier 2; default: false
```

Sink connectors must provide (Tier 1 required; Tier 2 optional):

```
prepare(epoch, rows)                       -> pending_handle
commit(epoch, pending_handle,
       checkpoint_id)                      -> IdempotentResult
abort(epoch, pending_handle)               -> IdempotentResult
should_flush(bytes_buffered: u64,
             epochs_buffered: u32)         -> bool  // Tier 2; default: true (flush every epoch)
```

Every emitted row includes the stable `row_id` rules from §6.4 and the schema
version from §4.2. Connector failures use the `RS-1xxx` error range; schema
drift that cannot be applied online becomes `BLOCKED(RS-1002)`. Per-record
decode errors are routed to a configurable dead-letter sink as `RS-1003`
events; this is a connector-tier concern and does not enter the IVM core.

#### 13.3.1 Dead-Letter Queue User Surface

Records routed to the DLQ are stored in a per-source catalog table and exposed
to users through standard SQL:

```sql
SELECT * FROM rockstream_catalog.dead_letter_queue
  WHERE source_name = 'kafka_orders';
```

Columns: `arrived_at`, `source_name`, `source_offset`, `error_code`,
`error_message`, `raw_bytes_hex`, `replay_attempt`.

The `replay_attempt` counter starts at 0 and increments each time a record
is replayed via `ALTER SOURCE ... REPLAY DEAD_LETTER_QUEUE`. This lets
operators distinguish fresh decode failures from records that have been
retried multiple times.

**Proactive alerting:** When a source accumulates DLQ entries exceeding
`dlq_warn_threshold` per hour (default 100), the system emits
`RS-1004 connector.dlq_growing` as a proactive warning (`NOTICE` level).
This surfaces growing decode problems before they affect downstream freshness.

```sql
-- Configure per-source DLQ warning threshold
ALTER SOURCE kafka_orders SET (dlq_warn_threshold = 50);
```

**Recovery commands:**

```sql
-- Replay failed records (re-decode after a schema fix or connector update)
ALTER SOURCE kafka_orders REPLAY DEAD_LETTER_QUEUE [SINCE <ts> UNTIL <ts>];

-- Dismiss records that are known-bad and should not be retried
ALTER SOURCE kafka_orders DISMISS DEAD_LETTER_QUEUE WHERE error_code = 'RS-1003';
```

**Retention:** `DLQ_RETENTION` per source (default 7 days). Expired entries
are removed by the control-plane GC. Configurable at source creation:

```sql
CREATE SOURCE kafka_orders FROM KAFKA ...
  WITH (DLQ_RETENTION = '14 days');
```

### 13.4 Connector Catalog and Isolation

The control plane catalogs available connector types and routes connector
instances to workers. Connector processes are independent of operator
processes; they can be co-located for low latency or run as a separate
"connector tier" for isolation.

### 13.5 Internal (Direct-Write) Source Connector

Clients do not need an external Kafka or Postgres to feed a pipeline. The
**internal source connector** accepts DML (`INSERT`, `UPDATE`, `DELETE`) issued
directly over the Postgres wire protocol (§12.6) and converts them to Z-set
deltas on a dedicated **base-table shard**.

```
Client (psql / JDBC)  ──INSERT──►  Gateway  ──delta──►  Base-table shard
                                                               │
                                                    Internal source connector
                                                               │
                                                        Pipeline IVM engine
```

**Write semantics:**

- Each `INSERT`/`UPDATE`/`DELETE` is appended to a per-connection write buffer
  (analogous to a Postgres transaction buffer).
- On `COMMIT`, the buffer is flushed as a single atomic Z-set delta to the
  base-table shard via `WriteBatch`. The delta receives the shard's next
  `source_epoch`, identical to any other source connector.
- On `ROLLBACK`, the buffer is discarded without touching the shard.
- On `BEGIN`, the session pins to the current published vector frontier for
  `REPEATABLE READ` reads within the same transaction.

**Write admission control.** The gateway tracks per-shard pending write bytes
(`direct_write_pending_bytes{shard_id}`). When pending bytes exceed
`direct_write_buffer_limit_bytes` (default 64 MB per shard), new `COMMIT`
operations on sessions targeting that shard are blocked with a
`RS-2019 write.shard_backpressure` NOTICE after `direct_write_notice_ms`
(default 1000 ms) and rejected with the same error code after
`direct_write_timeout_ms` (default 30000 ms). This prevents unbounded
write-buffer growth when downstream IVM processing or object-store writes
cannot keep pace with application writes.

The admission signal is the shard's `credits_available()` equivalent for the
internal source: when the shard's uncommitted epoch buffer exceeds the limit,
credits are exhausted and writes queue. This mirrors the Kafka source's
credit-based flow control from §13.3.

**Isolation guarantees:**

| Isolation | Within a session | Across sessions |
|---|---|---|
| `READ COMMITTED` | Reads see every committed delta before statement start. | ✅ |
| `REPEATABLE READ` | Reads see the snapshot at `BEGIN`. Writes are session-local until `COMMIT`. | ✅ |
| `SERIALIZABLE` | Not supported (§1.1, §12.6). | ✗ |

**Self-contained operation**: with the internal source connector, a pipeline
needs no external broker or database. Deploy RockStream, issue SQL `INSERT`
statements, query IVM views — no Kafka, no Postgres, no infrastructure beyond
object storage.

#### 13.5.0 Built-in Row Generator Source

For zero-friction first-run experiences and local development, RockStream
provides a built-in data generator that produces synthetic rows without
requiring any external system:

```sql
CREATE SOURCE demo.orders FROM GENERATE ROWS AS (
  order_id   BIGINT GENERATED,
  product_id INT    UNIFORM(1, 1000),
  quantity   INT    UNIFORM(1, 20),
  price      DECIMAL(10,2) UNIFORM(1.00, 500.00),
  region     TEXT   PICK('us-east', 'us-west', 'eu-central', 'ap-south')
) RATE = 100 PER SECOND;
```

The generator source implements the standard Tier 1 connector contract
(§13.3) with deterministic output (seeded RNG) for reproducibility in tests.
All constructs default safely when omitted: `public` schema is assumed if
unqualified; a system workload is used if none is specified. A developer can
start RockStream, create this source, and have a working materialized view
within two minutes.

#### 13.5.1 Optimistic Transaction Protocol

The direct-write connector is the natural interception point for **optimistic
locking** that combines CRDT merge laws with per-key version checks. The full
research lives in
[ideas/optimistic-locking-crdts.md](ideas/optimistic-locking-crdts.md); this
section states the design-level commitments.

**Transaction shape classifier.** The gateway classifies every direct-write
transaction into one of five shapes:

| Shape | Description | Pre-1.0? |
|---|---|---:|
| `ShardLocalSerializable` | Planner proves all reads/writes touch one shard; delegates to SlateDB transaction. | v0.41 |
| `BlindCommutative` | All writes are registered CRDT operands with `read_dependent = false`. | v0.44+ |
| `OptimisticExactKey` | Non-CRDT exact-key writes validated against per-row versions. | v0.41 |
| `MixedCrdtAndOptimisticExactKey` | CRDT writes skip validation; non-CRDT exact-key writes validate. | post-1.0 |
| `Unsupported` | Predicate reads, range reads, cross-shard uniqueness, foreign keys, or any shape requiring general serializability. Returns `RS-2009`. | No |

**Row-version metadata.** Each direct-write base-table row carries a
monotonically-incrementing `row_version: u64` and a
`last_modified_frontier: EncodedFrontier`. Stored in:

```text
op_state/txn_meta/table/{table_id}/pk/{pk_hash} → RowVersionMeta
```

**Read footprint tracking.** While a transaction is open, the gateway records
every exact primary key read as `(shard_id, table_id, pk_hash,
observed_row_version, observed_frontier)`. Range and predicate reads are
recorded but force the transaction to `Unsupported` for validation purposes
pre-1.0.

**Validation protocol.** At `COMMIT`:

1. Blind CRDT writes with `read_dependent = false` skip validation entirely.
2. Read-dependent CRDT writes validate the reads they depended on.
3. Non-CRDT writes require exact-key version validation per shard.
4. Any range/predicate footprint rejects the transaction (`RS-2009`).
5. Validation RPCs are parallel across participant shards.

**Atomic visibility for multi-shard CRDT batches.** If a transaction touches
multiple shards and requires all-or-nothing visibility, it uses a
`TxnEnvelope` written to a home shard. Participant shards apply operands as
pending; a commit marker promotes them to visible. Without the envelope, multi-
shard CRDT writes are documented as **idempotent write batches** with eventual
convergence, not as atomic SQL transactions.

**Error codes:**

| Code | Name | Meaning |
|---|---|---|
| `RS-2008` | `transaction.optimistic_conflict` | Row version changed between read and commit. |
| `RS-2009` | `transaction.unsupported_shape` | Transaction shape cannot be validated pre-1.0. |
| `RS-2010` | `transaction.visibility_pending` | Multi-shard envelope not yet committed (internal). |
| `RS-2011` | `transaction.ambiguous_commit_retry_with_idempotency_key` | Crash recovery cannot confirm; retry with same key. |

**What this is NOT.** The optimistic protocol does not claim cross-shard
`SERIALIZABLE`. It prevents stale overwrites on tracked exact keys and makes
CRDT writes coordination-light. Write-skew cycles across shards are not
detected. Users requiring true cross-shard serializability must use
`SERIALIZABLE LOCAL` (single-shard) or accept `RS-2009` for shapes
that require a general coordinator.

---

### 13.5.2 `INSERT ... RETURNING` Implementation

See §12.8.2 for the design. This subsection records the implementation
constraints specific to the internal source connector path.

**Key assignment before commit.** When the `INSERT` statement targets a column
with `DEFAULT gen_random_uuid()` or a sequence, the gateway generates or
advances the value *inside the transaction buffer* before calling
`WriteBatch`. This ensures `RETURNING` can read by the known key without a
read-before-write on the shard.

**Point-read after frontier advance.** After the `WriteBatch` commits, the
gateway issues a `ShardDb::get()` call on the base-table shard at the
frequently-satisfied frontier. This is a single key lookup — O(1) cost — not a
scan. If the key is not found at the expected frontier (e.g. due to an
indexed delete in the same epoch), the gateway returns `RS-2013
transaction.returning_key_not_found` and the client should re-read.

**Interaction with §13.5.1 optimistic validation.** `INSERT ... RETURNING`
for a `BlindCommutative` or `ShardLocalSerializable` transaction shape does
not trigger extra validation — the post-commit read is a read at the
already-committed frontier, not a read inside the commit protocol.

---

### 13.6 Iceberg/Delta Cold-Tier Sink

The cold-tier sink is a **built-in Tier 2 sink connector** that writes
committed view-output snapshots as Iceberg v2 (or Delta Lake) tables to object
storage. It is the production mechanism for the cold tier described in §12.7.

#### 13.6.1 Declaring a Cold-Tier Sink

```sql
-- Attach an Iceberg cold-tier sink to an existing view
CREATE SINK orders_mv_iceberg
  FOR VIEW orders_mv
  TO ICEBERG 's3://bucket/views/orders_mv/iceberg_table'
  WITH (
    snapshot_interval_epochs = 128,      -- write a new snapshot every N epochs
    snapshot_interval_ms     = 300000,   -- or every 5 min, whichever is first
    parquet_row_group_bytes  = 134217728, -- 128 MB row groups
    format_version           = 2,         -- Iceberg v2 (default)
    partition_by             = ARRAY['region', 'date_trunc(''day'', created_at)'],

    -- Catalog registration (optional; default = 'filesystem')
    catalog                  = 'rest',
    catalog_endpoint         = 'https://polaris.example.com/api/catalog',
    catalog_warehouse        = 'analytics',
    catalog_namespace        = 'reporting',
    catalog_table            = 'orders_mv'
  );
```

The sink can also be declared as a `DELTA` table:

```sql
CREATE SINK orders_mv_delta
  FOR VIEW orders_mv
  TO DELTA 's3://bucket/views/orders_mv/delta_table'
  WITH (
    snapshot_interval_epochs = 64,
    catalog                  = 'glue',
    catalog_database         = 'analytics',
    catalog_table            = 'orders_mv'
  );
```

Sinks are registered in the control-plane connector catalog and are visible
via `rockstream.connectors`.

#### 13.6.2 Snapshot Lifecycle

The cold-tier sink integrates with the epoch-commit protocol (§9) and the
`should_flush` Tier 2 signal:

```
Epoch N committed
  │
  ├─ should_flush() → false          ← buffer rows into pending_buffer (in shard)
  │  (N < snapshot_interval threshold)
  │
  └─ should_flush() → true           ← threshold reached; flush
       │
       ├─ 1. Write Parquet data files to object storage (one per shard partition)
       ├─ 2. Write Iceberg manifest files referencing data files + column stats
       ├─ 3. Write manifest list (snapshot) file
       ├─ 4. Atomically commit new metadata.json pointer (Iceberg atomic swap)
       ├─ 5. Update connector offset in control plane: last_snapshot_epoch = N
       └─ 6. If catalog ≠ 'filesystem': call catalog API to register/update table
              (idempotent; failure → CATALOG_WARN, does not block IVM)
```

Steps 1–3 are idempotent (files are keyed by `{shard_id}-{epoch}`). Step 4
uses the Iceberg spec's optimistic concurrency commit (compare-and-swap on the
version hint file). If the process crashes after step 3 but before step 4, the
next epoch's flush re-runs steps 1–4; existing data files are reused (their
content is deterministic from the epoch's Z-set output).

**Exactly-once guarantee**: pending rows are staged in the shard SlateDB as
`connector/{id}/pending_buffer` and participate in the epoch checkpoint before
any Parquet file is written. A crash-recovery replay re-drives `should_flush`
from the same committed state and produces identical Parquet files.

#### 13.6.2.1 Cold Snapshot Garbage Collection

Cold snapshots accumulate over time. Without GC, object-store costs grow
unboundedly. Each cold-tier sink has a retention policy:

```sql
CREATE SINK orders_mv_iceberg FOR VIEW orders_mv
  TO ICEBERG '...'
  WITH (
    ...
    cold_snapshot_retention_count    = 32,    -- keep at most N snapshots
    cold_snapshot_retention_duration = '7d'   -- keep snapshots for at most 7 days
  );
```

**GC rules** (whichever bound is reached first):

1. After each successful snapshot commit, the sink evaluates retention.
2. Snapshots older than `cold_snapshot_retention_duration` OR beyond the
   `cold_snapshot_retention_count` most recent snapshots are expired.
3. For each expired snapshot:
   - Remove its manifest list file.
   - Remove manifest files not referenced by any retained snapshot.
   - Remove Parquet data files not referenced by any retained manifest.
4. Update `metadata.json` to drop expired snapshot entries.
5. Metrics emitted: `cold_gc_bytes_reclaimed`, `cold_gc_last_run_epoch`.

**Safety guarantees**:

- GC is idempotent: re-running after a crash cannot delete live data.
- A data file referenced by *any* retained snapshot is never deleted, even
  if another expired snapshot also references it (Iceberg's sharing semantics).
- GC never runs concurrently with a snapshot commit on the same sink.
- External readers currently scanning an expired snapshot may get 404s on
  deleted data files — this is an accepted tradeoff documented in the
  operator guide. To avoid it, set `cold_snapshot_retention_count` high
  enough for external readers' query latency.

**Defaults**: 32 snapshots / 7 days. At the default snapshot interval
(every 128 epochs or 5 minutes), 32 snapshots covers roughly 2.5 hours.
Operators adjust based on external reader SLAs.

#### 13.6.3 External Tool Consumption

Once a snapshot is committed, external tools can query it with **no RockStream
gateway involvement**:

| Tool | Access pattern |
|---|---|
| **DuckDB** | `iceberg_scan('s3://...')` or `delta_scan('s3://...')` — full predicate/column pushdown via Parquet statistics |
| **Apache Spark / Trino** | Iceberg catalog pointing at the metadata path; reads via the standard Iceberg REST or Hive catalog |
| **Apache Flink** | Iceberg source connector; can also subscribe to the Iceberg changelog for incremental reads |
| **dbt** | Reads Iceberg/Delta tables as sources; runs `dbt run` against the cold snapshots |

The staleness of an external read equals the snapshot interval
(`cold_snapshot_interval`) plus Parquet write latency (typically seconds for
reasonable partition sizes). For workloads where sub-second freshness is not
required, external tools get full analytical performance with no latency
budget spent on RockStream's gateway.

#### 13.6.4 Positioning: RockStream as a Freshness Layer

This makes the architectural split explicit:

```
                      ┌─────────────────────────────────────┐
  Source data         │         RockStream                  │
  (Kafka, Postgres,   │  IVM engine: incremental SQL views  │
   direct DML)  ────► │  SlateDB hot LSM (10–250 ms fresh)  │ ──► psql / app point lookups
                      │  Checkpoint → Iceberg cold snapshots│ ──► DuckDB / Trino
                      └─────────────────────────────────────┘       (full analytical scans)
```

RockStream's primary value is **view freshness** — incremental updates at
10–250 ms latency that no columnar engine can match. The Iceberg cold tier
then makes that fresh data **accessible** to any tool in the data ecosystem
without requiring those tools to integrate with RockStream's gateway. There
is no tension between the two: they serve different queries.

#### 13.6.5 Catalog Registration

Writing Parquet files to a known S3 path is enough for tools that accept a
path (`iceberg_scan('s3://...')`). Tools that look up tables **by name** from
a central registry need a catalog API call after each snapshot commit.

The `catalog` option in `CREATE SINK` selects the registration backend:

| `catalog` value | Mechanism | Tools that benefit |
|---|---|---|
| `filesystem` (default) | Self-contained `metadata.json` in the object store prefix. No external service. | DuckDB `iceberg_scan` |
| `glue` | AWS Glue Data Catalog API — creates/updates the table in the specified Glue database. Credentials from the node's IAM role or `catalog_credentials` secret. | Athena, Redshift Spectrum, Glue ETL, any tool using Glue as Hive Metastore |
| `rest` | Iceberg REST Catalog spec (`POST /namespaces/{ns}/tables` / `POST /namespaces/{ns}/tables/{table}/snapshots`). Compatible with Polaris, Apache Gravitino, Unity Catalog, Nessie, and any spec-compliant REST catalog. | Spark, Flink, Trino, DuckDB with `iceberg` extension catalog config |
| `hive` | Hive Metastore Thrift API — `AlterTable` on each snapshot commit. | Spark (legacy), Hive, Presto |
| `ducklake` | DuckLake catalog API — registers or updates the Iceberg table entry in the DuckLake metadata database (DuckDB / MotherDuck). Each snapshot commit appends a new entry to the DuckLake snapshot log. | DuckDB with DuckLake extension, MotherDuck |

**Failure isolation.** The catalog API call (step 6) happens *after* the
Iceberg metadata commit (step 4) is durable. A catalog API failure does not
block IVM or the next epoch. The sink transitions to `CATALOG_WARN` state,
visible via `rockstream.connectors`, and retries the catalog call on the next
successful snapshot flush. The data is always readable by path even if the
catalog registration is temporarily behind.

**Credential management.** Catalog credentials (`catalog_credentials`) are
stored as a named secret in the control-plane catalog (`catalog/secrets/`,
§14.18), never in plain text in the `CREATE SINK` statement. Credentials are
encrypted at rest using the cluster's key material (§14.18) and injected into
the sink process at runtime via the secret-token mechanism.

**DuckLake detail.** DuckLake differs from other catalogs in that it stores
both table metadata *and* Iceberg snapshot history in a DuckDB database (local
file or MotherDuck cloud). RockStream's DuckLake backend calls the DuckLake
catalog API to append a new snapshot entry after each flush. The Parquet data
files remain on S3; the DuckLake database holds only metadata. This means:

```sql
-- After RockStream registers the table in DuckLake:
LOAD ducklake;
ATTACH 'md:analytics' AS dl;      -- or a local .duckdb file
SELECT * FROM dl.reporting.orders_mv;   -- table discovered by name, not path
```

### 13.7 Native Iceberg REST Catalog

RockStream can act as an **Iceberg REST Catalog server** — serving the
standard [Iceberg REST Catalog spec](https://iceberg.apache.org/rest-spec/)
directly from its control-plane metadata and cold snapshot manifests. This
makes all views with cold-tier sinks discoverable by name to any
Iceberg-native tool, with no external catalog service required.

#### 13.7.1 Why It's a Thin Layer

RockStream already holds everything the Iceberg REST spec needs:

| Iceberg REST concept | Source in RockStream |
|---|---|
| Namespace | `catalog/namespace/` in control-plane SlateDB (§5.2) |
| Table name + schema | `catalog/view/` — view definition, column types, partition spec |
| Snapshot list | Cold snapshot manifests, one per flush epoch |
| Table location (S3 prefix) | `catalog` field in the sink definition |
| Partition spec | `partition_by` from `CREATE SINK` |
| Sort order | Optional `sort_by` from `CREATE SINK` |

The catalog endpoint is a stateless HTTP adapter over the control-plane
`DbReader`. No new storage, no new coordination.

**Snapshot safety via the CALM invariant (§8.4).** Every snapshot served by
the catalog corresponds to a cluster-committed epoch verified by the CALM
epoch-commit invariant: the snapshot is safe to query if and only if all
contributing shards' `shard_meta/0x06 0xFR` entries satisfy `frontier ≥ N`.
The catalog endpoint only surfaces snapshots whose epoch satisfies this
condition. External tools (DuckDB, Trino, Spark) can independently verify
snapshot safety by reading per-shard manifests from object storage directly —
no live RockStream gateway connection is required.

#### 13.7.2 Endpoint and Auth

The gateway serves the Iceberg REST catalog on a dedicated HTTP port
(default `8181`), separate from the pgwire SQL port (`5432`):

```
--role=gateway   →  pgwire on :5432   (SQL queries)
                    HTTP   on :8181   (Iceberg REST catalog + future REST API)
```

The `/iceberg/v1/` prefix is reserved in Phase 9 even before the catalog
is implemented (returns `501 Not Implemented`). This ensures the gateway's
HTTP routing is catalog-aware from the start.

Authentication uses the same bearer-token / mTLS layer as the SQL gateway
(§12.5). A principal with `viewer` on a view can read its catalog metadata;
`admin` can see all namespaces.

#### 13.7.3 API Coverage

| Endpoint | Behaviour |
|---|---|
| `GET /v1/config` | Returns warehouse location and auth endpoint. |
| `GET /v1/namespaces` | Lists RockStream namespaces the caller can see. |
| `POST /v1/namespaces` | Creates a RockStream namespace (proxies `CREATE NAMESPACE`). |
| `GET /v1/namespaces/{ns}/tables` | Lists views with a cold-tier sink in the namespace. |
| `GET /v1/namespaces/{ns}/tables/{tbl}` | Returns schema, partition spec, and latest snapshot. |
| `POST /v1/namespaces/{ns}/tables/{tbl}` | Commits a new snapshot (used by Spark/Flink writers; not the primary path). |
| `GET /v1/namespaces/{ns}/tables/{tbl}/snapshots` | Full snapshot history within retention window. |

Views without a cold-tier sink are not listed — they have no Iceberg
snapshots. If a caller requests such a table, the catalog returns
`404 NoSuchTableException`.

#### 13.7.4 Client Configuration

```python
# PySpark
spark = SparkSession.builder \
    .config("spark.sql.catalog.rs",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.rs.type", "rest") \
    .config("spark.sql.catalog.rs.uri",
            "http://rockstream-gateway:8181/iceberg/v1") \
    .config("spark.sql.catalog.rs.token", "<bearer-token>") \
    .getOrCreate()

df = spark.table("rs.reporting.orders_mv")
```

```sql
-- Trino
CREATE CATALOG rockstream USING iceberg
WITH (
  "iceberg.catalog.type"    = 'rest',
  "iceberg.rest-catalog.uri" = 'http://rockstream-gateway:8181/iceberg/v1',
  "iceberg.rest-catalog.security" = 'OAUTH2',
  "iceberg.rest-catalog.oauth2.token" = '${ENV:RS_TOKEN}'
);
SELECT * FROM rockstream.reporting.orders_mv;
```

```sql
-- DuckDB
INSTALL iceberg; LOAD iceberg;
CREATE SECRET rs (
    TYPE iceberg_rest,
    ENDPOINT 'http://rockstream-gateway:8181/iceberg/v1',
    TOKEN '<bearer-token>'
);
SELECT * FROM iceberg_catalog('rs', 'reporting.orders_mv');
```

#### 13.7.5 Relationship to §13.6.5 Catalog Registration

These two features solve different halves of the discovery problem:

| | §13.6.5 Catalog Registration | §13.7 Native Iceberg REST Catalog |
|---|---|---|
| **Direction** | RockStream *pushes* metadata to an external catalog | External tools *pull* metadata from RockStream |
| **Use when** | Your org already has a central catalog (Glue, Unity, DuckLake) | RockStream is the catalog; no external service needed |
| **Infrastructure** | External catalog must exist and be reachable | Nothing extra — the gateway serves it |
| **Best for** | Enterprise data platforms with existing catalog governance | Self-contained deployments, new projects, edge deployments |

Both can be active simultaneously: a sink registers with Glue *and* RockStream
serves the same table via its REST catalog. Tools choose which to consult.

#### 13.7.6 Implementation Scope

The `/iceberg/v1/` HTTP routing slot is reserved in Phase 9. Full
implementation is a future deliverable, sequenced after the cold-tier sink
(§13.6) ships, since the catalog endpoint serves cold snapshot metadata.
The implementation is a single `rockstream-catalog` module inside
`rockstream-gateway` — no new crate, no new binary.

**Scope boundary**: §13.7 covers the Iceberg REST Catalog spec only.
Delta Lake table discovery is covered by path access (`delta_scan`) via
the §13.6 Delta cold-tier sink, and by Unity Catalog via `catalog = 'rest'`
in §13.6.5. DuckLake native catalog server support is addressed in §13.8.

---

### 13.8 Native DuckLake Catalog Server (Deferred)

#### 13.8.1 What It Would Mean

DuckLake (released 2025) uses a **DuckDB database as the catalog layer**:
table metadata and snapshot history are rows in a DuckDB file (local or
MotherDuck cloud) rather than files in object storage. Clients `ATTACH` the
database and query tables by name. This is architecturally distinct from
the Iceberg REST spec — there is no REST endpoint to implement; RockStream
would instead maintain a DuckDB metadata database and keep it in sync.

Being a native DuckLake catalog server would mean:

1. After each cold snapshot flush, RockStream appends new table/snapshot
   metadata rows to a dedicated DuckLake database.
2. The DuckLake database lives at a configured location: a local `.duckdb`
   file on object storage, or a MotherDuck account.
3. DuckDB clients `ATTACH` that database and see all RockStream views as
   named tables, with full snapshot history, alongside any other DuckLake
   tables from other systems.

#### 13.8.2 No Architectural Blockers

Nothing in the current design prevents this:

- The sync step is a natural step 7 appended to the §13.6.2 snapshot
  lifecycle, after the catalog API call in step 6.
- The DuckLake database is a derived artifact, like Iceberg snapshots.
  It can be fully rebuilt from the RockStream control-plane metadata on
  recovery.
- Failure isolation follows the §13.6.5 pattern: if the DuckLake write
  fails, the sink enters `CATALOG_WARN` and retries at the next flush.
  IVM is never blocked.
- Credentials for MotherDuck use the same named-secret mechanism as
  §13.6.5 `catalog_credentials`.

#### 13.8.3 Why It Is Deferred

The native Iceberg REST catalog (§13.7) already solves the primary
table-discovery use case for DuckDB:

```sql
-- DuckDB discovers RockStream views by name via §13.7, no DuckLake needed
CREATE SECRET rs (TYPE iceberg_rest, ENDPOINT 'http://rockstream:8181/iceberg/v1', ...);
SELECT * FROM iceberg_catalog('rs', 'reporting.orders_mv');
```

A native DuckLake server adds value only in a narrower case: the deployment
is **DuckLake-first** (other tables from Spark, dbt, etc. already live in
a DuckLake database), and the operator wants RockStream views to appear
in that same database alongside those other tables — without configuring
an Iceberg REST catalog endpoint.

**Conditions to revisit:**
- A production deployment explicitly requests DuckLake-first discovery.
- The DuckLake 1.0 write protocol is formally specified and stable.
- The ROI justifies the operational footprint of a second metadata store.

Until then, `catalog = 'ducklake'` in §13.6.5 (push registration into an
existing DuckLake database) covers the integration use case, and §13.7
covers self-contained DuckDB discovery.

---

### 13.9 Secondary Indexes

Without secondary indexes, looking up base-table rows by a non-primary-key
column requires either a full shard scan or a manually-declared materialized
view. Both are painful for OLTP applications that need `SELECT ... WHERE
customer_id = ?` to run at µs–ms latency.

#### 13.9.1 Design: A Secondary Index Is a Materialized IVM View

A secondary index is syntactic sugar for a system-managed materialized view
over the base table, arranged by the index column(s):

```sql
CREATE INDEX orders_by_customer ON orders (customer_id);
-- Internally equivalent to:
-- CREATE MATERIALIZED VIEW __idx_orders_by_customer AS
--   SELECT * FROM orders ARRANGE BY (customer_id, <pk>);
```

The index view:
- Is maintained incrementally by the IVM engine, like any other view.
- Stores rows arranged as `(index_key, pk_cols) → row_bytes` in `view_output/`
  on the shards assigned to the index operator.
- Advances its frontier with the base-table source; index freshness lags base
  table by at most one epoch (same guarantee as any derived view).
- Is invisible to users as a queryable view (`SHOW VIEWS` does not list it);
  it is accessible only through the planner's index-selection path.

The index name is reserved in the catalog namespace. It cannot conflict with
existing table or view names.

#### 13.9.2 Planner Integration

The SQL compiler's optimizer recognizes index-scannable predicates:

```sql
SELECT * FROM orders WHERE customer_id = 42;
-- Planner choice: base-table shard scan vs. index on customer_id
```

The planner chooses the index path when:
1. An index exists on the predicate column(s).
2. The estimated selectivity (predicate matches / total rows) is below
   `index_prefer_selectivity_threshold` (default 0.01 — prefer index if the
   predicate matches fewer than 1% of rows).
3. The index frontier satisfies the query's freshness requirement (i.e.
   `index_frontier >= wait_for` if set).

If condition 3 is not met (index is behind the base table frontier), the
planner falls back to the base-table scan path.

`EXPLAIN` output shows `index_scan(orders_by_customer, customer_id = 42)` vs.
`shard_scan(orders)` and the selectivity estimate used.

#### 13.9.3 Partial Indexes

An index may declare a `WHERE` predicate to index only matching rows:

```sql
CREATE INDEX active_orders ON orders (customer_id) WHERE status = 'active';
```

The IVM view for the partial index filters the base-table delta before
arranging, so only `status = 'active'` rows are stored. This reduces index
state size for high-cardinality filtered columns.

Partial index queries must include the index predicate (or a stronger
predicate) in the query's `WHERE` clause for the planner to use the index.

#### 13.9.4 Lifecycle

**Online creation.** `CREATE INDEX` triggers a backfill: the base-table
checkpoint is scanned once to build the initial arrangement, then live deltas
are applied. During backfill, the index is in `BUILDING` state and the planner
does not use it. Once the index frontier catches up to the base-table frontier,
it transitions to `READY`.

**Concurrent writes.** Writes to the base table during backfill are buffered
as normal IVM deltas and applied to the index after the initial scan. No write
blocking occurs.

**`DROP INDEX`.** Removes the catalog entry and the system view. The IVM
operator is torn down; arrangement state is GC'd by a frontier-aware
compaction filter after the index frontier is closed.

**`REBUILD INDEX`.** Re-runs the backfill from the current base-table
checkpoint. Useful when compaction debt has degraded index read performance.

#### 13.9.5 State and Storage Budget

Each index view is a separate arrangement. Its state bytes count against the
pipeline's `state_budget_gb` quota (§14.13). `EXPLAIN INCREMENTAL ESTIMATE`
reports projected index state size based on source cardinality and the column
value distribution (from connector stats, §4.0).

`rockstream.views` (§12.6.1) lists system index views with `view_type =
'INDEX'` alongside user views, so operators can observe state size and
frontier lag per index.

#### 13.9.6 Error Codes

| Code | Name | Meaning |
|---|---|---|
| `RS-2014` | `index.building` | Query used index path but index is still in `BUILDING` state (should not reach client; planner falls back). |
| `RS-2015` | `index.frontier_lag` | Index frontier lags base-table frontier by more than `index_max_lag_ms`; query fell back to base-table scan. |
| `RS-2016` | `index.name_conflict` | `CREATE INDEX` name conflicts with existing table, view, or index name. |

**Ships**: v0.50.

---

## 14. Operations: Deploy, Monitor, Diagnose

This section is a contract, not aspiration. Every primitive below has a
corresponding milestone in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md);
the design is not considered shipped until they exist and stay working.

### 14.1 The Operator's Mental Model

The operator interacts with **two nouns** and **one verb**:

- **Pipelines**: a named compiled plan (a set of views fed by named
  connectors). The unit of deployment, quota, priority, and SLO.
- **Views**: SQL views maintained by a pipeline. The unit of query.
- **Deploy**: submit/replace a pipeline. Everything else (sharding, shuffle,
  recovery, compaction, GC, rebalancing) is implicit.

The operator never names a shard, an operator instance, an arrangement, a
frontier, a checkpoint, or a WAL segment in normal work. Those terms appear
only inside drill-downs (`EXPLAIN INCREMENTAL`, support bundle, audit log)
and only when something is wrong.

### 14.2 Deployment Ladder

One binary (`rockstream`), one config schema, three tiers. Every tier uses
the same storage layout (§5) and the same SQL interface.

```
Tier 1 — Single process (dev / eval)
  rockstream start --storage=./data
  In-process control plane, one worker, local FS, a handful of shards.
  Zero config needed. Survives crashes via local SlateDB.

Tier 2 — Single host (small production)
  rockstream start --role=all --storage=s3://bucket/...
  Control plane and workers in one process; shared object storage.
  Survives worker restarts; horizontally scales within one host.

Tier 3 — Multi-host cluster (full production)
  On each control node:  rockstream start --role=control --storage=s3://...
  On each worker node:    rockstream start --role=worker  --storage=s3://...
  Elastic. Workers and control nodes can be added or removed online without
  config rewrites; the control plane discovers and admits them.
```

Moving from Tier 1 to Tier 3 is purely additive: the same data files
produced by Tier 1 against MinIO open in Tier 3 against S3. There is no
migration step because there is no node-local state to migrate.

### 14.3 SLO-Driven Configuration

The operator declares **what** they want; the control plane decides **how**:

```sql
-- Step 1: define a named resource policy (optional; views fall back to
--         the schema's default workload, or the system default workload).
CREATE WORKLOAD analytics
WITH (
    FRESHNESS_SLO  = '1s',        -- views must be ≤ 1 s stale
    MEMORY_LIMIT   = '200GB',     -- soft cap on total arrangement state
    PRIORITY       = normal       -- low | normal | high
);

-- Step 2: create sources and views, assigning them to the workload.
CREATE SOURCE orders FROM kafka (...);

CREATE MATERIALIZED VIEW sales_by_product
WITH (WORKLOAD = analytics)
AS
    SELECT product_id, SUM(quantity) AS qty
    FROM   orders
    GROUP BY product_id;

CREATE MATERIALIZED VIEW sales_by_region
WITH (WORKLOAD = analytics)
AS
    SELECT region, SUM(quantity) AS qty
    FROM   orders
    GROUP BY region;
```

Multiple views can share a workload. The compiler builds one shared operator
DAG so common subplans are maintained once and fanned out to multiple view
sinks. Omitting `WITH WORKLOAD` inherits the schema-level default
(`ALTER NAMESPACE ... SET DEFAULT WORKLOAD = name`). `CREATE REPLACEMENT
MATERIALIZED VIEW v2 FOR v1` and `ALTER MATERIALIZED VIEW v1 APPLY
REPLACEMENT v2` use the schema/plan replacement path from §4.2.

The control plane auto-tunes the mechanism knobs (§14.6) to satisfy the
SLO inside the workload's constraints. Operators do not normally set those
knobs; they set intent. If the SLO cannot be met inside the constraints, the
view transitions to a named degraded state (§14.10) instead of silently
missing the target.

### 14.4 The One Signal: SLO Compliance

For every view the control plane reports a single rolling indicator:

```
view_slo_compliance{view="sales_by_product", workload="analytics"}  =  0.0 .. 1.0
```

Value `1.0` means the freshness target has been met for the full window
(default 5 min). Anything below is the fraction of time it was met. A single
Grafana panel showing this number per view is enough to answer “is the
platform healthy?” without operator training.

When SLO compliance dips, the corresponding `view_degraded_reason` label
reports a named reason from §14.10. Drill-down metrics break the reason down
by operator and shard.

### 14.5 Self-Tuning by Default

Five control loops are designed to run continuously in the control plane,
each independently disableable per view (`autotune.* = off`) for audited
manual control. **Implementation status (2026-07-11 review): the first four
rows ship today; "adaptive skew splitting" is design-only until
NEW_ROADMAP.md v0.47** — it was previously described here as already on by
default, which was not true (verified: no `hot_key`/`skew_buckets` code
exists as of v0.42.3). See §10.5–§10.6 for the underlying mechanism.

| Loop | Adjusts | Trigger | Bounds |
|---|---|---|---|
| **Adaptive parallelism** | `operator.*.parallelism` | Operator `epoch_ms` p95 trends above SLO budget for > 30 s | `min_parallelism` ≤ N ≤ `max_parallelism` (per workload) |
| **Adaptive epoch sizing** | `min_epoch_ms`, `max_epoch_ms` | Object-store write rate trends above quota, or SLO compliance < target | Floor: 10 ms; ceiling: 5 s |
| **Adaptive source throttle** | Per-connector `max_poll_bytes_per_epoch` | `frontier_lag_ms` trends above `freshness_target_ms * 1.5` for > 20 s, indicating ingestion is outpacing processing | Minimum 1 row/epoch; maximum = connector's native batch ceiling |
| **Adaptive locality** | Operator placement and exchange path (`elided`, `loopback`, `direct`, `durable`) | Exchange serialization/network time is a material fraction of `epoch_ms`, or a small view can fit on fewer workers without missing SLO | Never moves state outside quota; no placement that increases predicted p95 lag above SLO |
| **Adaptive skew splitting** | `operator.*.skew_buckets` for hot keys | Worst-shard load exceeds `hot_key_factor × median` for > 30 s | `1 ≤ B ≤ max_skew_buckets`; enabled only for operators with exact partial-state semantics |

Every adjustment is recorded in the audit log (§14.11) with the metric
reading that triggered it. Operators see *what the system decided and why*,
not opaque magic.

### 14.6 Manual Override Knobs

For the cases auto-tuning cannot solve, the same primary knobs remain
available as per-view or per-operator overrides:

| Knob | Auto default | When to override |
|---|---|---|
| `min_epoch_ms` | adaptive (10 ms–250 ms) | You have a known cost ceiling object storage cannot exceed. |
| `max_epoch_ms` | = `FRESHNESS_SLO / 2` | You want freshness tighter than what the SLO loop derives. |
| `frontier_agg_interval` | 100 ms | Very large clusters (≥ 1000 shards) may relax to 500 ms. |
| `operator.*.parallelism` | adaptive | `EXPLAIN INCREMENTAL` shows a specific operator stuck ⚠ and you want to pin it. |
| `operator.*.skew_buckets` | adaptive | One logical key is hot and you want to pre-split it instead of waiting for detection. |

Manual overrides are sticky and visible in `SHOW VIEW STATUS` output so the
next operator does not have to guess why a value was set.

### 14.7 The CLI Surface

Everything is one binary, one CLI:

```
rockstream start           --role=all|control|worker|gateway
rockstream workload {list, show, create, alter, drop}
rockstream view     {list, show, query, subscribe, pause, resume, status}
rockstream schema   {list, show, create, drop}
rockstream source   {list, show, pause, resume, drop}
rockstream explain  <view> [--estimate]
rockstream cluster  {status, workers, quotas}
rockstream cluster  workers {list, drain, status}
rockstream resource {usage, usage --workload=<name>, cluster}
rockstream schema-evolution {status --schema=<name>, history --view=<name>}
rockstream support  bundle [--view=<name>]        # see §14.12
rockstream audit    {tail, query}                 # see §14.11
rockstream sql      "<query>"                        # parse, lower to PlanNode IR, and print
                                                   # EXPLAIN INCREMENTAL against the catalog
rockstream debug    arrangement <view> <op_id> <key>  # see §14.7.1
```

#### 14.7.1 IVM Arrangement Debugger

When a view produces a *wrong answer* (not just a slow one), the operator needs
to inspect the intermediate Z-set state rather than just the pipeline metrics.
The `debug arrangement` command reads a specific key from a live arrangement
using `DbReader` pinned to the latest committed frontier:

```
$ rockstream debug arrangement orders_mv agg_op_3f2a "product_id=42"
op_id:       agg_op_3f2a  (SUM(quantity) GROUP BY product_id)
shard:       shard-07 (s3://bucket/shards/07/)
epoch:       1492 (committed at 2026-05-28T10:14:23Z)
key:         product_id=42
state:       { sum_quantity: 1840, row_count: 23 }
weight:      +1
last_delta:  epoch 1489  (+120 quantity, +3 rows)
```

This reads directly from the arrangement state encoding (§6.2) via `DbReader`;
it does not block the live pipeline. The command also supports `--epoch=N`
to inspect historical state at a past committed epoch (within the checkpoint
retention window).

No separate `rockstream-control`, `rockstream-worker`, `rockstream-gateway`
binaries; no separate config files for each role. A node decides which roles
it plays from its `--role` flag and what it can see from its `--storage` URL.
A single uniform binary makes packaging, image building, and version
upgrades trivial.

### 14.8 Diagnosing a Slow or Stuck View

`EXPLAIN INCREMENTAL` has three output levels plus a cost-preview mode:

#### Level 1 — Default (human-readable summary)

Run `rockstream explain <view>` (equivalently `EXPLAIN INCREMENTAL <view>`)
to get a human-readable summary of the view's operator graph annotated with
the latest per-operator statistics:

```
VIEW  sales_by_product  [SLO: 1000 ms]  [lag: 450 ms ✅]  state: 12 GB / 200 GB
 ├─ AGG  SUM(quantity) GROUP BY product_id  [avg_epoch: 3 ms]  [shards: 8]
 │   └─ EXCHANGE  hash(product_id)  [depth: 0 batches]  [throughput: 1.2 M rows/s]
 │       └─ JOIN  orders ⋈ products ON product_id  [avg_epoch: 180 ms ⚠]  [shards: 32 → 64 (adapting)]
 │           ├─ EXCHANGE  hash(product_id)  [depth: 14 batches ⚠]  [throughput: 800 k rows/s]
 │           │   └─ SCAN  orders  [connector_lag: 0 ms]
 │           └─ SCAN  products  [connector_lag: 0 ms]
```

The `⚠` flags draw attention to operators whose `avg_epoch_ms` or
`shuffle_outbox_depth` exceed thresholds. The `→ 64 (adapting)` annotation
shows the adaptive-parallelism loop is already responding. Operators almost
never need to touch these manually; the value of the tree is *understanding
what the system is doing*, not driving it.

**Default output rules**: no internal IDs, no antichain notation, no raw byte
counts. Human-readable units (GB, MB), ✓/⚠/✗ visual indicators for SLO
compliance.

#### Level 2 — VERBOSE (full plan with resource detail)

```sql
EXPLAIN INCREMENTAL VERBOSE reporting.daily_summary;
```

Adds merge-law annotations, combiner status, per-operator shard counts,
parallelism utilisation, workload detail (memory used vs. limit), and frontier
timestamps. This level is for operators diagnosing resource or performance
issues who need to see law IDs, shard allocation, and workload attribution.

#### Level 3 — ANALYZE (live runtime statistics)

```sql
EXPLAIN INCREMENTAL ANALYZE reporting.daily_summary;
```

Adds live per-operator statistics collected over the last 60 seconds: rows
processed, state reads, RMW-avoidance ratio, hot groups, p99 latency, decode
errors, and DLQ entries. This level requires a live round-trip to workers and
may take slightly longer than other levels.

### 14.9 Cost Preview Before Deploy

```
rockstream explain <view> --estimate
```

or

```sql
EXPLAIN INCREMENTAL ESTIMATE <CREATE VIEW …>;
```

produces a predicted operator tree with:

- estimated state size per operator (from source-table statistics);
- estimated steady-state object-store request rate;
- estimated minimum frontier lag at the requested SLO;
- whether the view fits within its workload's declared `MEMORY_LIMIT`, and if
  not, by how much.
- **minimum achievable freshness through the DAG** — for views that depend
  on upstream views, the estimate propagates the upstream view's
  `FRESHNESS_SLO` (or observed lag if already running) forward through
  the dependency graph and reports the cumulative minimum lag at the leaf
  view. If `view B` has `FRESHNESS_SLO = '100ms'` but it depends on
  `view A` with `FRESHNESS_SLO = '60s'`, the estimate reports
  `minimum_achievable_lag_ms: 60100` and flags the SLO as **structurally
  unachievable** (`RS-4003 slo.downstream_lag_exceeds_target`). This
  validation also runs at `CREATE MATERIALIZED VIEW` time: if the declared
  `FRESHNESS_SLO` is below the minimum achievable lag from upstream
  dependencies, the view is created in `BLOCKED(RS-4003)` state rather
  than silently missing its SLO.

**Backfill cost prompt.** When a `CREATE MATERIALIZED VIEW` requires
backfilling a large source (estimated time > 30 seconds or state > 1 GB),
the system presents the cost estimate interactively and waits for
confirmation before proceeding. This prevents accidental deployment of
expensive views:

```
⚠ Estimated backfill: 12 GB state, ~4 min at current source rate.
  Proceed? [y/N]
```

Programmatic clients and CI pipelines can bypass the prompt with
`WITHOUT CONFIRMATION`:

```sql
CREATE MATERIALIZED VIEW big_view AS ...
  WITHOUT CONFIRMATION;
```

`EXPLAIN INCREMENTAL ESTIMATE CREATE MATERIALIZED VIEW ...` produces the
same cost information without executing the deployment, allowing scripts
to check costs before committing.

This is the single biggest operator surprise eliminated: nobody has to
deploy a view to discover it needs 4 TB of arrangement state.

### 14.10 Named Degraded States

When a view cannot meet its SLO inside its workload's constraints, it transitions
to a **named** degraded state. The control plane never fails silently and never
drops data without an explicit, surfaced reason. States:

| State | Meaning | Operator action |
|---|---|---|
| `HEALTHY` | SLO met, quota margin available. | None. |
| `BUILDING` | View is performing initial backfill. Queryable (returns backfilled rows so far; may be incomplete). SLO compliance is not counted yet. | Wait; monitor with `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW <name>`. |
| `BACKFILLING` | View is loading historical source data. SLO compliance is not counted yet; `backfill_progress` is shown separately. | Wait or raise bootstrap parallelism/quota. |
| `RECOVERING` | Worker or shard recovery is replaying from a checkpoint. SLO compliance is temporarily excluded from alerting until `recovery_deadline`. | Watch recovery progress; investigate only if deadline expires. |
| `STRESSED` | SLO met, quota ≥ 80% utilised. | Plan capacity addition. |
| `OVER_BUDGET_RELAXED` | SLO relaxed by the system because state budget is full. Freshness is degraded but data is correct. | Raise `MEMORY_LIMIT` on the workload or revise view to reduce state. |
| `RPS_THROTTLED` | SLO relaxed because object-store quota is the bottleneck. | Raise `object_store_rps` or revise SLO. |
| `PAUSED` | View explicitly paused (`PAUSE MATERIALIZED VIEW`), or paused by admission control to free capacity for higher-priority work. | Resume when ready. |
| `REPLACING` | A replacement view is hydrating via `CREATE REPLACEMENT MATERIALIZED VIEW`. The original continues serving. | Monitor with `SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW <name>`; apply with `ALTER ... APPLY REPLACEMENT` when ready; discard with `ALTER ... DISCARD REPLACEMENT`. |
| `BLOCKED` | A non-recoverable error (e.g. connector authentication, schema mismatch). | Inspect `view_blocked_reason`; fix; resume. |

Every state transition is in the audit log (§14.11) with the metric or
event that caused it. SLO compliance §14.4 dips together with the state
transition so the dashboard tells the same story.

**View status commands:**

```sql
-- Status for one view (backfill progress, freshness, SLO, state)
SHOW BACKFILL STATUS FOR MATERIALIZED VIEW reporting.daily_summary;

-- Status for all views in a namespace (shows state, SLO, workload, workload_source)
SHOW VIEW STATUS FOR NAMESPACE reporting;
-- workload_source column: 'view' | 'namespace_default' | 'system_default'
-- indicating how the view's workload assignment was resolved.

-- Status for all views in the cluster
SHOW VIEW STATUS;

-- Replacement readiness
SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW reporting.daily_summary;
```

**Background DDL and waiting.** `CREATE MATERIALIZED VIEW` runs backfill in
the background by default; the issuing session does not need to stay open.
For sessions that want explicit background signalling:

```sql
SET BACKGROUND_DDL = ON;
CREATE MATERIALIZED VIEW reporting.large_view AS SELECT ...;
-- Returns immediately with an INFO message and job_id.

-- Optionally wait for completion:
WAIT FOR MATERIALIZED VIEW reporting.large_view TO BE READY TIMEOUT '1 hour';
```

**Namespace-level lifecycle.** Pause or resume all views in a namespace atomically:

```sql
ALTER NAMESPACE reporting PAUSE;
ALTER NAMESPACE reporting RESUME;
```

(In v3.27 and earlier these commands were spelled `ALTER SCHEMA`; the keyword `SCHEMA` is accepted as a deprecated alias for `NAMESPACE` through v0.46 and removed in v0.51.)

### 14.11 Audit Log

Every control-plane action is appended to a durable, queryable audit log in
the control SlateDB:

```
control: audit/{ulid} → {
  timestamp, actor ("system" | user_id), action, target (workload/view/schema/source),
  before, after, reason, related_metric
}
```

Actions captured: view deploy/replace/pause/resume/drop, workload create/alter/drop,
autotuner parallelism change, autotuner epoch-size change, admission-control pause,
shard add/remove/rebalance, worker join/leave, checkpoint commit, degraded-
state transition.

`rockstream audit tail` follows the log; `rockstream audit query` supports
filters by view, workload, time range, action type. The log is the single source
of truth for "what changed in the cluster yesterday at 03:00 UTC?".

### 14.12 Support Bundle

One command collects everything needed to debug an issue without ad-hoc
requests for logs, metrics, plans, and configs:

```
rockstream support bundle --view=sales_by_product --since=1h --out=bundle.tar.gz
```

Includes: pipeline definition, last N compiled plans, last N audit-log
entries scoped to the pipeline, the live `EXPLAIN INCREMENTAL` output, the
relevant Prometheus metric series for the time window, recent worker logs,
recent checkpoint references, anonymised sample of recent connector
offsets, and the cluster topology snapshot. Sensitive values (credentials,
user data) are redacted by default; `--include-secrets=false` is the
default and cannot be overridden by config (only by an explicit CLI flag).

### 14.13 Quotas and Multi-Tenancy

Every workload declares its resource policy at creation. The control
plane enforces these as constraints on the views assigned to the workload:

| Knob | Enforced by |
|---|---|
| `MEMORY_LIMIT` | Sum of `op_state_bytes` across all views in the workload; over-limit transitions affected views to `OVER_BUDGET_RELAXED`. |
| `MAX_PARALLELISM` | Upper bound for the adaptive-parallelism loop across the workload's views. |
| `PRIORITY` | Used by admission control (§14.16) to choose which workload's views to pause first under contention. |
| `FRESHNESS_SLO` | p99 freshness target; auto-tunes epoch sizing, parallelism, and scheduling across the workload's views. |

Resource policies are declared in `CREATE WORKLOAD` and can be altered with
`ALTER WORKLOAD ... SET (...)`. They are visible in `SHOW WORKLOAD STATUS`
and in the audit log when changed.

**Ships**: v0.45.1. Until then, only the single cluster-wide `state_budget_gb`
floor (§5.6, `OVER_BUDGET_RELAXED`) exists; `rockstream-types::workload`
carries the `WorkloadPriority`/`FreshnessSlo`/`MemoryLimit` data model but no
control-plane catalog, SQL parser, or gateway code reads or writes it yet
(found by the 2026-07-11 control-plane/multi-tenancy review).

### 14.14 Error Code Taxonomy

Every error returned to a user, written to a log, or recorded as a
`view_blocked_reason` carries a stable, **globally unique** `RS-XXXX` code
with a published doc URL. CI enforces uniqueness: any duplicate code, or
any new code outside its owner's reserved range, fails the build.

**Reserved code ranges.** Each owner area has a contiguous range. Numbers
allocate from the bottom of the range; gaps are normal and never reused
out-of-range to prevent accidental collisions with future owner additions.

| Range | Owner |
|---|---|
| `RS-1000–1499` | Connector / source / sink ingestion |
| `RS-1500–1999` | Schema / DDL validation |
| `RS-2000–2099` | Gateway / SQL / session / freshness |
| `RS-2100–2199` | Index / planner |
| `RS-2200–2299` | Direct-write transactions (optimistic, exact-key) |
| `RS-2300–2399` | History / `AS OF` / retention |
| `RS-2400–2499` | Reserved (gateway expansion) |
| `RS-3000–3499` | Shard / runtime / placement |
| `RS-3500–3999` | Merge laws / arrangements |
| `RS-4000–4099` | Control plane / quotas |
| `RS-4100–4499` | Cold tier (sink, catalog, quota) |
| `RS-5000–5499` | Storage format / version |
| `RS-5500–5999` | Resource budgets / autotune surfaces |
| `RS-6000–6499` | Schema evolution |
| `RS-9000–9999` | Internal / fallback (never user-visible without escalation) |

**Canonical registry (illustrative; full list maintained in
`crates/rockstream-types/src/error_code.rs`).**

```
RS-1001  connector.authentication_failed
RS-1002  connector.schema_drift
RS-1003  connector.decode_error (DLQ routed)
RS-1004  connector.dlq_growing
RS-1005  connector.watermark_required
RS-1006  source.no_stable_identity
RS-1010  view.dependent_inline_view_exists
RS-1011  view.inline_cycle_detected
RS-1009  recursion.non_monotone_not_supported
RS-2001  view.unsupported_sql_construct
RS-2002  view.state_budget_exceeded
RS-2003  isolation.serializable_not_supported
RS-2005  history.epoch_before_retention
RS-2006  system_table.requires_filter
RS-2007  write.idempotency_key_required
RS-2008  transaction.optimistic_conflict
RS-2009  transaction.unsupported_shape
RS-2012  session.wait_for_timeout
RS-2013  session.returning_unsupported_shape
RS-2014  index.building
RS-2015  index.frontier_lag
RS-2016  index.name_conflict
RS-2017  shard_stats.too_stale
RS-2018  session.staleness_exceeded
RS-2021  copy.malformed_stdin_statement
RS-2501  coordinator.leader_unavailable        (was RS-2012)
RS-2502  coordinator.prepare_conflict          (was RS-2013)
RS-2503  coordinator.cross_group_rejected      (was RS-2017 collision)
RS-2504  coordinator.quorum_lost               (was RS-2018 collision)
RS-3001  shard.fence_lost
RS-3002  shard.recovery_replay_failed
RS-3009  merge.malformed_operand
RS-3011  durable.rate_limit_exhausted
RS-3012  durable.object_store_io_failure
RS-3013  durable.buffer_capacity_exceeded
RS-3014  durable.footer_serialization_failed
RS-3015  durable.footer_deserialization_failed
RS-3016  durable.footer_corrupt_or_undersized
RS-3017  exchange.ipc_shuffle_decode_error
RS-3018  exchange.loopback_shard_not_active
RS-3501  merge.law_accumulator_decode_error
RS-4001  control.quota_violation
RS-4002  control.autotune_bounds_exhausted
RS-4101  cold_tier.not_enabled                 (was RS-4001 collision)
RS-4110  cold_tier.quota_exceeded
RS-5001  storage.format_version_incompatible
RS-5002  merge.unknown_law
RS-5018  resource.budget_warning_80pct
RS-5019  resource.budget_critical_95pct
RS-5020  merge.law_version_mismatch            (was RS-5002 overload)
RS-5021  protocol.version_not_supported        (was RS-5002 informal reuse)
RS-6001  schema.incompatible_evolution
RS-6002  schema.evolution_not_applied          (was RS-6001 overload)
```

**Collisions resolved in v3.28.** The following codes were previously
overloaded across owner areas; each has been reassigned into its owner's
range and the prior occupant retained for its original meaning:

- `RS-2012`, `RS-2013`, `RS-2017`, `RS-2018` previously double-booked for
  session-state errors; these are retained for their original session-layer
  meanings.
- `RS-4001` previously denoted both `control.quota_violation` and
  `cold_tier.not_enabled`. Cold-tier moves to `RS-41xx`.
- `RS-5002` previously denoted both `merge.unknown_law` and
  `merge.law_version_mismatch`. Version mismatch moves to `RS-5020`.
- `RS-6001` previously denoted both `schema.incompatible_evolution` (a
  NOTICE) and a separate `evolution_not_applied` error. The latter moves
  to `RS-6002`.
- `RS-5002` was also used informally by §5.5's wire-protocol-version-skew
  text for `protocol.version_not_supported`, even though the canonical table
  above already reserved `RS-5002` for `merge.unknown_law` — found during the
  <=v0.42.3 roadmap review (2026-07-11). `protocol.version_not_supported`
  moves to `RS-5021`, implemented and tested at `NEW_ROADMAP.md` v0.55.

**`next_steps` requirement.** Every `RS-XXXX` error must include a
`next_steps` field containing actionable remediation guidance. This is
enforced in CI: the error-code registry test fails if any code lacks a
non-empty `next_steps` entry. The field is included in structured log
output, CLI error display, and the published error-code documentation.
Users should never receive an error without knowing what to do next.

`rockstream` exits non-zero on any RS-coded error and prints a one-line
remediation pointer. The codebase has a single error-code registry; CI
fails if a new code is introduced without a doc entry or outside its
reserved owner range. "Internal error" without a code is itself a bug.

### 14.15 Metrics Reference

Every shard, operator instance, and pipeline reports:

**Workload/SLO surface:**
- `view_slo_compliance` — the primary indicator (§14.4).
- `view_degraded_reason` — label when below 1.0 (§14.10).
- `frontier_lag_ms` — raw lag, per view.
- `visible_frontier_lag_ms` — visible-frontier lag (§3.0), per view.
- `durable_frontier_lag_ms` — durable-frontier lag (§3.0), per view.
- `event_time_watermark_lag_ms` — wall-clock minus event-time watermark, per source.
- `windows_open_total` — count of open windows per time-window operator.
- `windows_held_without_watermark_total` — count of windows open longer than `2 × window_size` without a watermark advance (§6.9).
- `backfill_progress` — for snapshot-mode connectors.
- `recovery_progress` — fraction of shards whose recovered epoch frontier ≥ cluster checkpoint epoch.

**Throughput / scheduling:**
- `rows_in_per_sec`, `rows_out_per_sec` — throughput.
- `epoch_ms` — per operator, processing time per epoch.
- `op_state_bytes`, `op_state_rows` — arrangement size.
- `shuffle_outbox_depth` — pending batches on each exchange sender.
- `connector_lag_ms` — age of the oldest unread event in the source.

**Storage hot path (SlateDB / object store):**
- `object_store_request_duration_seconds` — histogram per `(op=get|put|list|delete, status)`. p50/p99 are the primary cost and latency signal.
- `slatedb_manifest_write_duration_seconds` — histogram per shard; spikes here mean manifest contention or object-store slowness.
- `slatedb_wal_replay_bytes` — counter incremented during recovery; non-zero outside recovery is a bug.
- `slatedb_sst_count` — gauge per shard; growing without bound indicates compaction starvation.
- `write_batch_bytes` — histogram of per-epoch `WriteBatch` size; sized against `min_epoch_bytes` floor.
- `write_amplification_ratio` — bytes-written / bytes-ingested per shard, sampled per compaction cycle.
- `compaction_backlog_bytes` — SST bytes awaiting compaction.
- `compaction_debt_seconds` — estimated time at current throughput to drain the backlog; the alert signal.
- `checkpoint_age_seconds`, `checkpoint_duration_seconds`, `checkpoint_lag_ms` — recovery planning and checkpoint health.
- `object_store_rps` — PUT+GET+LIST+DELETE per second per shard.

**Caches / autotune / migration:**
- `autotune_decisions_total` — counter labeled by `(loop, direction)`.
- `segment_cache_hit_ratio` — per-worker hit rate for the arrangement segment cache.
- `segment_cache_bytes_used` — current memory consumption of the segment cache.
- `historical_query_count` — counter of `AS OF` queries served, labeled by view.
- `migration_state_duration_seconds` — histogram per `(migration_id, state)`; stuck migrations are visible here long before they affect freshness.

Exported via Prometheus / OpenTelemetry. A starter Grafana dashboard ships
in `deploy/dashboards/rockstream-overview.json` and contains exactly one
panel above the fold per pipeline: SLO compliance over time.

**Core hot-path metrics ship in v0.10 and v0.11**, not in the Phase 10
observability roll-up: `object_store_request_duration_seconds`,
`slatedb_manifest_write_duration_seconds`, `slatedb_wal_replay_bytes`,
`write_batch_bytes`, `compaction_debt_seconds`, `visible_frontier_lag_ms`,
and `durable_frontier_lag_ms`. The single-binary developer story is not
acceptable without a way to see why something is slow.

#### 14.15.1 Metric Cardinality Budget

To prevent metric explosion at scale, all RockStream-emitted metrics are
subject to a **cardinality budget** of **≤ 50 000 active time series per
worker** under default label configuration. Label dimensions are bounded as
follows:

| Label | Max distinct values | Notes |
|---|---:|---|
| `pipeline_id` | 256 *(full breakdown; see note below)* | Bounds how many pipelines receive a full per-pipeline metric breakdown at once — does not bound how many pipelines the cluster may host. |
| `op_id` | 512 per pipeline | Each operator emits O(1) series. |
| `shard_id` | 1024 per exchange | Bounded by cluster size. |
| `migration_id` | 64 concurrent | Active migrations only; closed → dropped. |
| `connector_id` | 128 | Bounded at connector registration. |

**This is a metrics-emission bound, not a capacity limit.** An earlier
revision of this table stated the `pipeline_id` row as a hard rejection of
pipeline creation beyond 256 — read literally, the cluster could never host
a 257th materialized-view pipeline, directly contradicting this project's
own "bottomless capacity"/"massively parallel" positioning (`README.md`).
Found and corrected by the 2026-07-11 network-efficiency/metrics-scalability
review: the control-plane catalog has no pipeline-count ceiling; only the
metrics exporter's per-label cardinality does. **Fix (scheduled v0.45,
NEW_ROADMAP.md Phase 12)**: the metrics exporter maintains a full
per-pipeline label breakdown only for an LRU/recent-traffic working set of
up to 256 pipelines, and rolls every pipeline outside that working set into
one aggregate `pipeline_id="other"` series rather than refusing to create it
or silently dropping its metrics. **Implementation note (2026-07-11
audit)**: today's `MetricRegistry` (`crates/rockstream-types/src/metrics.rs`)
stores each per-label map as a plain, unordered `HashMap` with no
recency-tracking or eviction API (only a full, test-only `reset_all()`
exists) — this is a structural change (e.g. promoting the
already-transitively-available `lru` crate to a direct dependency), not a
config-only toggle.

Custom labels (e.g., user-defined pipeline tags) are limited to 8 additional
labels with ≤ 64 distinct values each. Labels violating these limits are
silently dropped from emission and logged at WARN level once per hour.

The `metrics_cardinality_total` gauge tracks current active series; if it
exceeds `metrics_cardinality_warning_threshold` (default 40 000 per worker),
`RS-1410 metrics.cardinality_high` is emitted as a NOTICE.

### 14.16 Backpressure and Admission Control

Backpressure is cooperative credit flow: receivers grant credits to senders;
senders block on credit exhaustion; this propagates upstream as growing
`frontier_lag_ms` long before any data loss is possible. No operator blocks
on a sibling's progress; only on its own credits and its own input
frontier. This is the structural reason RockStream does not adopt a
single-process `DynamicScheduler` ownership model.

Admission control sits in front of every `CREATE MATERIALIZED VIEW`, every
`CREATE WORKLOAD`, and every autotuner expansion. It refuses requests that
would push cluster utilisation past configured thresholds, and it pauses
lower-priority views (by workload priority) when higher-priority ones request
capacity that is otherwise unavailable. Both decisions are recorded in the
audit log with the relevant metric readings.

### 14.17 Failure Injection (`rockstream chaos`)

A built-in fault-injection subcommand makes the recovery story testable in
the same environment as production. Inject worker kills, object-store
latency, shard fence loss, or connector stalls and watch SLO compliance,
degraded-state transitions, and the audit log respond. Recovery is not a
story told in docs; it is a button anyone can press.

### 14.18 Secrets Management

Every connector in the system needs credentials: Kafka SASL, Postgres
replication users, AWS access keys, catalog API tokens. RockStream provides a
first-class secrets subsystem rather than deferring credential handling to
individual connectors.

**Storage.** Secrets are stored in the control SlateDB under the key prefix
`catalog/secrets/`:

```
catalog/secrets:   0x01 0x0B namespace_id(16) secret_name(var) → encrypted_blob
```

Each secret value is envelope-encrypted: a per-secret data encryption key (DEK)
encrypts the credential payload; the DEK is itself encrypted by the cluster's
key encryption key (KEK). The KEK source is configured at cluster bootstrap:

| `secret_kek_source` | Description |
|---|---|
| `env` (default) | KEK loaded from `RS_SECRET_KEK` environment variable on the control-plane process. Suitable for single-node and dev. |
| `aws_kms` | KEK is an AWS KMS key ARN; envelope encrypt/decrypt via KMS API. |
| `gcp_kms` | KEK is a GCP Cloud KMS key resource; envelope encrypt/decrypt via Cloud KMS API. |
| `vault` | KEK retrieved from HashiCorp Vault transit engine. |

**DDL surface:**

```sql
CREATE SECRET kafka_prod (
    TYPE = 'sasl_plain',
    USERNAME = 'rockstream-ingest',
    PASSWORD = '...'     -- value is encrypted before storage; never persisted in plaintext
);

CREATE SOURCE orders FROM KAFKA (
    brokers = 'kafka:9092',
    topic = 'orders',
    secret = 'kafka_prod'
);
```

**Worker-side resolution.** Workers never read raw secret values from the
control DB. At pipeline startup, the control plane issues a short-lived
*secret token* (an encrypted blob containing the decrypted credential,
encrypted to the requesting worker's identity, with a TTL). Workers decrypt
the token using their node key (derived from mTLS identity) and hold the
credential in memory only for the lifetime of the connector process. Secret
tokens are not written to logs, audit events, support bundles, or shard state.

**Audit.** Every `CREATE SECRET`, `ALTER SECRET`, `DROP SECRET`, and secret-
token issuance is recorded in the audit log with actor, timestamp, and target
secret name (never the value). `SHOW SECRETS` displays names and types but
never values.

**Rotation.** `ALTER SECRET <name> SET (PASSWORD = '...')` updates the
encrypted blob. Active connectors using that secret receive a rotation signal
and re-acquire a fresh token on their next epoch boundary — no pipeline
restart required.

### 14.19 Resource Usage Visibility

Operators and applications need to see resource consumption before limits are
hit. RockStream exposes resource usage at three levels of granularity:

```sql
-- Per-workload summary: state bytes, memory, SLO health
SHOW RESOURCE USAGE;

-- Per-view breakdown within a specific workload
SHOW RESOURCE USAGE FOR WORKLOAD <name>;

-- Cluster-wide aggregate
SHOW CLUSTER RESOURCE USAGE;
```

**Catalog tables** for programmatic access:

- `rockstream_catalog.view_resource_usage` — per-view state bytes, memory,
  rows, SLO compliance, degraded reason.
- `rockstream_catalog.workload_resource_usage` — per-workload aggregate with
  budget utilisation percentage.
- Both tables also carry an `estimated_cost_per_hour` column, computed by
  applying a small operator-supplied cloud-pricing table to metrics the
  system already collects (object-store request/storage/egress rates from
  `object_store_request_duration_seconds` and transferred bytes, worker
  compute-hours, and the Spot-vs-on-demand mix). The pricing table is
  declared once, cluster-wide, via a `[pricing]` block in `rockstream.toml`
  (unit costs only — no cloud-billing API integration); omitting it leaves
  `estimated_cost_per_hour` null rather than guessing. **Ships**: v0.45
  (found missing by the 2026-07-11 network-efficiency/metrics-scalability/
  cost-visibility review — until then, cost visibility is a one-time TCO
  benchmark comparison, not a live per-workload figure an operator can read
  off a running cluster).

**Proactive alerting thresholds:**

| Utilisation | Severity | Error code | Action |
|---|---|---|---|
| ≥ 80% of any budget | `NOTICE` | `RS-5018 resource.budget_warning_80pct` | Plan capacity addition |
| ≥ 95% of any budget | `WARNING` | `RS-5019 resource.budget_critical_95pct` | Immediate action required |

Thresholds are configurable per workload via
`ALTER WORKLOAD ... SET (WARNING_THRESHOLD = 0.75, CRITICAL_THRESHOLD = 0.90)`.
The NOTICE/WARNING is delivered to active pgwire sessions and recorded in the
audit log.

---

## 15. Design Positioning

RockStream's design point relative to other streaming-SQL and differential-dataflow
systems in this space:

| Aspect | RockStream |
|---|---|
| **SQL coverage** | Full ANSI + recursion |
| **Theoretical model** | DBSP + DD frontiers |
| **State backend** | **SlateDB** (S3-native) |
| **Compute-storage split** | **Fully decoupled** |
| **Horizontal scale** | **Excellent** |
| **Object-storage native** | **Yes (end-to-end)** |
| **Postgres wire protocol** | **Yes (§12.6)** |
| **Direct DML writes** | **Yes (§13.5)** |
| **SERIALIZABLE isolation** | **No (§1.1)** |
| **Open source** | Yes |

The unique positioning: **end-to-end object-storage native** (no NVMe required,
no local-state assumptions) **+ full SQL via DBSP** (correctness guarantees) **+
adaptive per-operator parallelism**.

**GA vs. Data Lake GA scope.** At v1.0 (v0.54 RC1), the cold-tier
Iceberg/Delta sink (v0.44) and DuckDB/Spark/Trino table discovery via
registered external catalogs ship before the RC1 gate. The Iceberg REST
Catalog server (§13.7) and the DuckLake catalog server (§13.8) remain
post-1.0. The v1.0 positioning rests on: object-storage-native IVM, full SQL,
adaptive parallelism, Postgres wire access, direct DML writes, secondary
indexes, and the connector and cold-tier ecosystem. Marketing materials must
not claim the Iceberg REST Catalog or DuckLake server as shipping capabilities
until they are released.

### 15.1 Explicitly Deferred: Data Quality / Expectations

Declarative data-quality expectations — row-level assertions, column
constraints, freshness checks, and anomaly detection integrated into the
pipeline lifecycle — are a well-established pattern in modern data pipeline
tooling. RockStream does **not** include a data-quality subsystem in
v1.0.

**Rationale for deferral.** Data-quality expectations are valuable but
orthogonal to the IVM correctness guarantee. RockStream's core contract is
"the view is a correct incremental materialization of the SQL definition."
Quality assertions ("no NULL in `email`", "order_total > 0") are
application-domain rules, not IVM semantics. Implementing them well requires
a dedicated assertion language, DLQ routing, per-row vs. batch-level
semantics, and integration with alerting — each of which is a meaningful
scope addition.

**Planned phase.** Data quality is scheduled for **v0.50–v0.51** (Phase 15 —
Declarative Data Governance), designed as a plugin/extension layer:

- `CREATE EXPECTATION <name> ON <view> AS <predicate>` DDL.
- Failing rows routed to a configurable DLQ sink with the expectation name
  and failure context.
- Pipeline-level quality metrics (`expectation_pass_ratio`,
  `expectation_fail_count`) exposed in `rockstream.expectations`.
- Integration with the existing `BLOCKED` / degraded-state machinery: a view
  whose quality drops below a threshold can transition to
  `DEGRADED(RS-6001 quality.below_threshold)`.

This is an explicit deferral, not an oversight. The extension point is the
operator-layer hook after view-output commit and before sink delivery.

---

## 16. Optimality Assessment (v3.7)

The v3.4 review asks whether the design is coherent, easy to operate, and
optimal enough to build. Each answer is a structural commitment encoded in this
document; the open risks list what remains to validate in implementation.

### 16.1 Is the storage substrate used correctly?

**Yes, with explicit budgets.** SlateDB's single-writer fence, `WriteBatch`,
`DbReader`, checkpoints, segment extractors, TTL, compaction filters and WAL
reader are all real and used as documented. Range deletion is absent and is
therefore not assumed. Cleanup is scan-and-delete plus frontier-gated
compaction filters (§5.3, §8.5). Manifest and WAL costs are explicit budgets
(§5.4, §9.1).

### 16.2 Does the runtime model fit a sharded object-store backend?

**Yes, with three deliberate design choices.** RockStream uses the
circuit-of-typed-operators design from DBSP but (a) schedules operators
asynchronously per shard rather than via a synchronous
`DynamicScheduler`, (b) treats arrangements as SlateDB-backed indexed Z-sets
rather than in-memory Spines, and (c) makes `Exchange` first-class so
cross-shard ownership is never an `OwnershipConflict` error.

### 16.3 Are pg_trickle's correctness rules honored?

**Yes, as oracle-driven test obligations.** EC-01 join split, Q07
double-counting correction, Q21 SemiJoin context, FULL JOIN NULL handling in
SUM, `has_key_changed` metadata, distinct-as-multiplicity, EXCEPT/INTERSECT
per-branch counts, DRed recursion, recomputation fallback, diamond
consistency, and cadence inheritance all appear as planner metadata or test
vectors in IVM.md. The runtime does not execute pg_trickle's SQL; it must
match its behavior.

### 16.4 Does the time model scale?

**Yes, because it is causal.** Per-shard frontiers compose into a cluster
vector frontier; there is no global LSN to contend on. Aggregation is async
with a documented staleness budget (§8.4). Query reads pin to a published
vector frontier (§12.2). Recursion participates in the same antichain via the
inner `iteration` component.

### 16.5 Is the system understandable and operable?

**Yes, with an explicit operating contract.** Operators deploy pipelines and
query views; shards, antichains, arrangements, checkpoints, and WAL segments
are internal unless a drill-down is requested. SLO compliance is the primary
signal (§14.4). `EXPLAIN INCREMENTAL`, `EXPLAIN INCREMENTAL ESTIMATE`, the
audit log, named degraded states, support bundles, and freshness tokens make
the system explain itself before and during failure.

### 16.6 Where is the design still at risk?

The following items are the explicit validation backlog and feed the
implementation plan's Phase 3.5 and Phase 4 acceptance criteria:

- **Hot-key skew** in joins and aggregates. Virtual-bucket hot-key splitting,
  pre-shuffle combiners, locality-aware placement, and adaptive re-sharding
  (§7.5, §10.5) must keep worst-shard load within a documented factor of
  median.
- **Object-store request budget** under sustained load (PUT/GET/LIST/DELETE
  per second per shard) including WAL, manifest, SST, shuffle, and
  checkpoints.
- **Frontier-aggregator throughput** with thousands of shards × hundreds of
  operators. The aggregator must be CPU- and memory-bounded, never blocking.
- **Distributed recursion cost**. Each inner iteration is a full shuffle
  round; convergence detection across shards must not require a synchronous
  global barrier.
- **Bootstrap and recovery time** for state sizes ≥ 1 TB. Recovery uses
  `DbReader` against per-shard checkpoints; base-table ingest uses snapshot
  mode (IVM.md §12).
- **Compaction-filter safety proofs** for distinct/union retention and
  windowed expiry. Every filter has a written argument that no `Drop`
  decision could resurrect a version observable by an active reader.
- **Control-plane HA**. A single SlateDB writer with hot readers is good
  enough for Tier 1/2; production uses a Raft-elected writer lease over the
  control SlateDB (§3). The remaining risk is implementation complexity, not an
  architectural gap. **Scheduled**: v0.45.2 (NEW_ROADMAP.md Phase 12.5) —
  found unscheduled by the 2026-07-11 control-plane/multi-tenancy review;
  `crates/rockstream-control` has zero `raft`/`Raft` references today, and
  Phase 8 shipped without the Raft hardening `NEW_IMPLEMENTATION_PLAN.md`
  originally promised for it.
- **Schema evolution and blue/green plan replacement**. Compatible changes are
  straightforward; breaking changes require clone/backfill/flip and must prove
  they preserve exactly-once source offsets.
- **Exchange object count and connection fan-out**. The design now bounds both
  with worker-level multiplexing and coalesced durable shuffle objects (§7.2),
  but Phase 4 must validate the request-rate math at thousands of shards.
- **Barrier alignment under skew**. Checkpoint buffering is bounded by credits
  (§11.2), but Phase 6 must prove it does not starve quiet inputs or trigger
  false recovery under bursty sources.
- **Auto-tuner stability**. Hysteresis and auditability are specified (§14.5),
  but workloads with step-function traffic still need soak tests to ensure the
  tuner does not oscillate.
- **Coordination correctness under message reordering**. The deterministic
  simulation harness (§17) must cover the epoch commit, frontier aggregation,
  checkpoint barrier alignment, and 2PC sink paths from Phase 1 onward.
- **Recovery time invariants** (§11.5). Chaos tests must demonstrate the 5 s /
  30 s / 60 s budgets hold at the target shard size on every release.

The design is considered structurally sound modulo these validations.

---

## 17. Simulation Testing

Distributed-systems bugs in RockStream are dominated by message reordering,
shard restart sequences, partial failures during epoch commit, and network
partitions. None of these are reliably exercised by integration tests or by
`rockstream chaos` (§14.17), which can find symptoms but cannot enumerate the
underlying races.

RockStream adopts the **deterministic simulation** strategy pioneered by
FoundationDB: the entire distributed system runs in a single process, with the
network, the object store, and the wall clock replaced by deterministic
in-memory fakes driven by a seeded random number generator. A failing seed
replays the exact same execution every time, so any bug found is reproducible
and bisectable.

### 17.1 The `SimRuntime` Abstraction

Every I/O surface in RockStream is mediated by a small trait:

```rust
trait Runtime {
    fn now(&self) -> Instant;
    fn spawn(&self, fut: BoxFuture<'static, ()>);
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;
    fn object_store(&self) -> Arc<dyn ObjectStore>;
    fn network(&self) -> Arc<dyn Network>;
}
```

Production uses a `TokioRuntime` over real S3 and gRPC. Tests use a
`SimRuntime` over an in-memory `ObjectStore` and an in-memory message-passing
`Network`, both with seeded latency/error/reorder injection.

The traits are introduced in Phase 1 and threaded through every subsequent
phase. Retrofitting them after Phase 8 would be prohibitively expensive;
introducing them early is cheap.

### 17.2 `BUGGIFY`

Production code paths contain `buggify!()` macros (no-op in release builds, hot
in simulation) that randomly inject faults at known race-prone points: partial
`WriteBatch` failures, dropped shuffle frames, delayed manifest publication,
out-of-order epoch commits, fenced-writer attempts to commit after eviction.
Every `buggify!()` site has a comment explaining the race it simulates. CI
requires that any new race-prone code adds a `buggify!()` annotation reviewed
by a second engineer.

### 17.3 TigerBeetle-Style Assertion Discipline

The simulator is a force multiplier for invariants already stated in code; it
is not a substitute for them. RockStream therefore requires **paired
assertions** on every correctness property that crosses a durable or network
boundary: assert the property before the boundary and assert it again after the
boundary is observed.

Required assertion pairs include:

- Arrangement writes: assert `(row_id, schema_version, weight)` validity before
  constructing the SlateDB `WriteBatch`; assert the same invariant after
  decoding through `ShardDb::get_merged()` / `scan_merged()`.
- Frontier movement: assert antichain monotonicity before publishing a shard
  frontier; assert monotonicity again after reading the control-plane frontier.
- Epoch commits: assert that every persisted connector offset, frontier, and
  state mutation carries the same `epoch`; assert the equality again during
  recovery replay.
- Sink commits: assert idempotency key uniqueness before `prepare`; assert the
  same key maps to exactly one committed external artifact after recovery.

Assertion failures indicate a programmer or storage-corruption bug, not an
operating condition. The worker crashes, the shard lease is released, and
recovery proceeds through the normal reassignment path (§10.4, §11.5).

### 17.4 Explicit Fault Model

Simulation coverage is defined by an enumerated fault model, not by a vague
"random failures" label. The first simulator release must cover:

- Network: delay, drop, duplicate, reorder, partition, and reconnect.
- Object store / SlateDB boundary: delayed visibility, transient errors,
  stale reads where the API permits them, checksum/corruption errors surfaced
  by SlateDB, and LIST throttling.
- Process lifecycle: crash before, during, and after each durable write;
  restart with an old manifest view; fenced writer attempts after eviction.
- Clock and scheduling: delayed timers, reordered task wakeups, slow workers,
  credit exhaustion, and barrier skew.
- Connector behavior: source stalls, duplicate batches after retry, invalid
  records routed to DLQ, sink commit retry, and file-sink buffering across
  epochs.

Every new `buggify!()` site names the fault-model entry it exercises. A fault
not named here is treated as uncovered until the list and simulator are both
extended.

### 17.5 What Simulation Tests Cover

| Subsystem | What the simulator must demonstrate |
|---|---|
| Epoch commit (§9) | Every interleaving of per-shard `WriteBatch` outcomes leaves the cluster frontier monotonic and the exactly-once contract intact. |
| Frontier protocol (§8) | Arbitrary reorderings of per-shard frontier reports converge to the same cluster vector frontier as serial delivery. |
| Cluster checkpoint (§11.2) | Barrier alignment under arbitrary credit exhaustion never deadlocks; checkpoint always either succeeds or surfaces `RECOVERING`. |
| Fault-driven reassignment (§10.4) | Killing any subset of workers and restarting them in any order recovers to the same final state as no failure. |
| Schema evolution (§4.2) | A schema-version change concurrent with an in-flight epoch produces no row decoded with the wrong version. |
| 2PC sinks (§11.4) | Any crash during pre-commit, between pre-commit and commit, or during commit recovers idempotently. |
| Liveness under faults (§11.5) | After any injected recoverable fault, the cluster commits at least one new epoch within the 5 s / 30 s / 60 s recovery-time budgets or surfaces a named degraded state. |
| Resource bounds (§7.2, §14.10) | Shuffle queues, barrier buffers, connector inboxes, and arrangement scan windows hit explicit limits and apply backpressure or transition to `BLOCKED`; they never grow without bound. |
| Storage-boundary corruption (§5.3) | Any checksum/corruption error surfaced by SlateDB crashes the affected worker and recovers by shard reassignment; corrupted bytes are never interpreted as valid arrangement state. |
| Optimistic transactions (§13.5.1) | Gateway crash mid-validation, participant apply failures, concurrent row-version bumps, transaction envelope recovery, and pending-operand compaction safety all converge to correct terminal state. |

### 17.6 Continuous Simulation Soak

Every commit runs a bounded number of deterministic seeds in CI. In addition,
RockStream maintains a continuous simulation job that runs new seeds around the
clock against the current `main` branch. Failing seeds are minimized, checked in
as regression tests, and replayed on every subsequent build. Pre-release gates
scale the seed count to millions across the coordination suite.

The continuous job tracks both safety failures (oracle divergence, invariant
assertion, invalid recovery state) and liveness failures (no committed epoch
within the recovery budget after a recoverable fault). A simulator that only
checks final output equivalence is incomplete.

### 17.7 Why This Is Worth the Cost

FoundationDB's defining property in production is that *correctness bugs are
rare*. The cause is not exceptional discipline; it is a test harness that runs
millions of seeded executions on every commit. RockStream's coordination
surface (epoch commit + frontier protocol + 2PC + checkpoint barrier) is small
enough that a similar harness can exhaustively explore it. The investment
pays back the first time a multi-shard race ships to production.

### 17.8 Simulation Fidelity Boundaries

`SimRuntime` models an idealised object store. The following table documents
which real-object-store behaviors are modeled and which require integration
tests against a real endpoint (MinIO minimum) at the Phase 4 and Phase 6 gates.

| Behavior | Modeled by SimRuntime | Real S3 |
|----------|----------------------|--------|
| Latency distribution | Uniform random (configurable) | Long-tailed, prefix-dependent |
| Conditional writes (If-Match) | Yes (in-memory CAS) | Yes (S3 conditional writes) |
| LIST consistency after PUT | Configurable staleness via `list_staleness_epochs` (v0.43) | Strong (since Dec 2020 for S3; varies for other providers) |
| Rate limiting (HTTP 429) | Via `buggify!()` injection | Provider-specific, prefix-scoped |
| Partial object writes | Yes, via `partial_write_probability` (v0.43) | Can occur on large PUTs |

Behaviors not modeled by `SimRuntime` must be covered by integration tests
against real object storage at the Phase 4 (v0.30) and Phase 6 (v0.36) gates.

**Known simulation fidelity gaps introduced at v0.36 (target mitigations listed):**

1. **Partial object writes** — `SimObjectStore` atomically commits writes in
   memory; real S3/GCS may serve truncated bytes for in-progress or crashed
   MPU uploads. Consequence: cold-tier Parquet sink crash-recovery is not
   fully exercised in simulation. Mitigation target: add
   `partial_write_probability: f64` to `SimObjectStore`'s fault model by
   v0.37; add a `PartialWriteRecoveryTest` to the law-faults corpus.
   Status: **[MITIGATED — v0.43]** — `partial_write_probability: f64` added
   to `SimObjectStore` (`crates/rockstream-sim/src/object_store.rs`),
   `object_store.partial_write` registered in the law-faults corpus
   (`PartialWriteRecoveryTest`, `crates/rockstream-sim/src/law_faults.rs`),
   and formalized in `formal/m5_cold_tier_sink.fizz` (M5-S1/S2/S3, M5-L1).
   Evidence: `crates/rockstream-connectors/tests/partial_write_recovery_tests.rs`
   (`test_partial_write_recovery_lfs`, `test_partial_write_recovery_minio_tc`),
   `assert_commit_pointer_atomic` in `sink_connector.rs`.

2. **Kafka transactional broker timeout** — `SimNetwork` can inject message
   drops but does not model Kafka broker-side transaction timeout (which
   aborts open transactions after `transaction.timeout.ms`). Consequence:
   the `CheckBeforeCommit` recovery path for `KafkaSink` is not exercised in
   simulation. Mitigation: add a `kafka_tx_timeout_probability` fault
   parameter to the Kafka connector simulator before v0.44.
   Status: **[MITIGATED — v0.43]** — `kafka_tx_timeout_probability` fault
   parameter added to `KafkaSink` (`crates/rockstream-connectors/src/kafka_sink.rs`),
   `kafka.tx_timeout` registered in the law-faults corpus
   (`KafkaTxTimeoutRecoveryTest`, `crates/rockstream-sim/src/law_faults.rs`).
   Evidence: `crates/rockstream-connectors/tests/kafka_tx_timeout_tests.rs`
   asserts the `CheckBeforeCommit` recovery path delivers exactly once after
   a forced tx-timeout.

3. **S3 LIST consistency delays** — `SimObjectStore` returns synchronously
   consistent LIST results; real S3 may return stale LIST responses for
   several seconds after a PUT. Consequence: the CALM epoch manifest
   verifiability property (§8.4) is not tested against LIST staleness.
   Mitigation: add `list_staleness_epochs` fault parameter to
   `SimObjectStore` by v0.43 (pulled forward from a stale "v0.42 Simulator
   Maturity" reference — that milestone is now v0.53 in the current roadmap
   numbering; `NEW_ROADMAP.md`'s v0.43 scope explicitly says "pull forward
   `list_staleness_epochs` if not yet landed").
   Status: **[MITIGATED — v0.43]** — `list_staleness_epochs` fault parameter
   added to `SimObjectStore` (`crates/rockstream-sim/src/object_store.rs`),
   `object_store.list_staleness` registered in the law-faults corpus
   (`ListStalenessRegressionTest`, `crates/rockstream-sim/src/law_faults.rs`).
   Informational only, confirmed by
   `list_staleness_does_not_affect_sink_connector_asserts` in
   `object_store.rs`'s unit tests: no `assert_*` in `sink_connector.rs`
   depends on LIST — CALM epoch manifest reads are always direct-key reads.

4. **Network packet fragmentation** — TCP segmentation of large shuffle
   frames is not exercised in simulation. Consequence: the `wire_version`
   negotiation handshake (§5.5) is not tested against fragmented reads.
   Mitigation: integration test with real gRPC and large frame sizes.
   Status: **[DEFERRED]** — low risk in practice; gRPC handles reassembly.

Each `[UNMITIGATED]` gap must be resolved before the corresponding feature
reaches the Integration Beta gate (v0.46). Gaps are tracked in the
simulation coverage matrix and the Phase 6 known-gaps list in
IMPLEMENTATION_PLAN.md.

---

## Appendix: Key Encoding Reference

```
Per-shard SlateDB:
  op_state/agg:        0x01 0xAG op_id(16) group_key(var)
  op_state/minmax:     0x01 0xMM op_id(16) group_key(var) value(var) row_hash(8)
  op_state/join_L:     0x01 0xJL op_id(16) join_key(var) row_id(16)
  op_state/join_R:     0x01 0xJR op_id(16) join_key(var) row_id(16)
  op_index/join_match: 0x02 0xJM op_id(16) side(1) row_id(16)
  op_state/distinct:   0x01 0xDS op_id(16) row_hash(16)
  op_state/window:     0x01 0xWN op_id(16) part_key(var) order_key(var) row_id(16)
  op_state/timewin:    0x01 0xTW op_id(16) window_id(16) key(var)
  op_state/topk:       0x01 0xTK op_id(16) part_key(var) value_desc(var) row_id(16)
  op_state/recursion:  0x01 0xRC op_id(16) row_hash(16) iteration(4 BE)

  op_index/cached_extremum: 0x02 0xMM op_id(16) group_key(var)
  op_index/segtree:         0x02 0xST op_id(16) part_key(var) node_id(8)

  view_output:         0x03 view_id(16) output_key(var)

  shuffle_inbox:       0x04 exchange_id(16) src_shard(4) epoch(8 BE) seq(8 BE)
  shuffle_outbox:      0x05 exchange_id(16) target_shard(4) epoch(8 BE) seq(8 BE)

  shard_meta/frontier: 0x06 0xFR op_id(16) output_port(2 BE)
  shard_meta/sink:     0x06 0xSK connector_id(16) epoch(8 BE)
  shard_meta/epoch:    0x06 0xEP

Control-plane SlateDB:
  catalog/table:       0x01 0x01 namespace_id(16) table_id(16)
  catalog/view:        0x01 0x02 namespace_id(16) view_id(16)
  catalog/pipeline:    0x01 0x03 namespace_id(16) pipeline_id(16)

  plan/physical:       0x02 0x01 pipeline_id(16)
  plan/assignment:     0x02 0x02 pipeline_id(16) op_id(16) instance(4 BE)

  topology/worker:     0x03 0x01 worker_id(16)
  topology/shard:      0x03 0x02 shard_id(16)
  topology/shard_map:  0x03 0x03 exchange_id(16) → versioned ShardMap

  frontier/op:         0x04 op_id(16) output_port(2 BE)
  frontier/consumed:   0x04 0xEX exchange_id(16)

  checkpoints/cluster: 0x05 checkpoint_id(16)

  connector/offset:    0x06 0x01 connector_id(16)
  connector/sink:      0x06 0x02 connector_id(16)

  audit/event:          0x07 ulid(16)
  state_accounting:    0x08 pipeline_id(16) metric_id(2)
  schema/source:        0x09 0x01 source_id(16) version(8 BE)
  schema/view:          0x09 0x02 view_id(16) version(8 BE)
  namespace/def:        0x0A 0x01 namespace_id(16)
```

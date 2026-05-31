# RockStream Roadmap

This document turns the design into an implementable path. It complements:

- [DESIGN.md](DESIGN.md): what RockStream is and why the architecture works.
- [IVM.md](IVM.md): how the incremental view maintenance engine works.
- [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md): detailed phase-by-phase engineering plan.
- [ideas/crdts.md](ideas/crdts.md): how CRDTs and merge laws should be used
  across the database.

The roadmap is intentionally patient. There is no rush to 1.0. RockStream is a
distributed database-like system, and the fastest credible path is to build it in
small, evidence-producing versions. Each roadmap version below is sized at about
**10 person-weeks** of implementation effort. That can mean one person for ten
weeks, two people for five weeks, five people for two weeks, or any other mix.

The version number is not a promise of public release quality. It is a planning
unit. A version is done only when its proof is done.

Version effort varies. Foundation versions (v0.1–v0.4) are typically
under-budget; gateway and connector versions (v0.40–v0.50) are typically
over-budget due to integration surface area. Teams should allocate 1.5× for
integration-heavy versions and 0.7× for kernel-focused versions.

---

## Roadmap Philosophy

1. **Evidence over dates.** A roadmap version ends when tests, benchmarks,
   simulations, and documentation prove the new capability works.
2. **Correctness before scale.** A fast incorrect incremental engine is not an
   asset. The single-shard IVM core must be boringly correct before distribution
   is allowed to make the problem harder.
3. **Simulation from the beginning.** The deterministic `SimRuntime` and
   `buggify!()` discipline are not hardening work. They are foundation work.
4. **Operability is never deferred.** Every capability arrives with diagnostics,
   error codes, audit events, and at least one clear operator-facing signal.
5. **Prefer thin vertical slices.** A version should leave a human able to do
   something real, or leave the project with stronger proof that a hard thing is
   safe.
6. **No accidental Postgres clone.** Postgres wire compatibility is an access
   layer, not the product goal. RockStream is for live SQL views and streaming
   analytics, not high-concurrency OLTP.
7. **Algebraic merge laws are a database-wide contract.** Starting at v0.5
  the `MergeLaw` / `LawBundle` catalog in `rockstream-types` is consumed by
  every layer (storage, planner, exchange, frontier, gateway, connectors,
  compaction, `EXPLAIN INCREMENTAL`). Internal laws ship first; user-visible
  CRDT column types land in v0.43–v0.46; user-defined laws via
  `CREATE MERGE LAW` are gated until v0.51. See [ideas/crdts.md](ideas/crdts.md)
  and DESIGN.md §6.11.
8. **Split before rushing.** If a version cannot fit into roughly 10
   person-weeks, split it. The roadmap is allowed to grow.

---

## Common Definition of Done

Every roadmap version must satisfy this baseline before it can be considered
complete:

- `cargo fmt --check`, `cargo clippy -D warnings`, and `cargo test --workspace`
  pass.
- New behavior has unit tests and either property tests, simulation tests, or an
  integration test, depending on risk.
- Any user-visible or operator-visible failure has an `RS-XXXX` error code.
- Any control-plane action writes an audit event.
- Any new performance claim has a benchmark or measurement note.
- Any new public surface is documented in the relevant doc or CLI help.
- Any new distributed coordination path has at least one seeded `SimRuntime`
  test before it is considered done.
- Any new queue, buffer, or scan window has a named upper bound, a metric
  reporting current fill level, and a backpressure or error path when the
  bound is reached. Unbounded in-memory accumulation is never acceptable.
- Any new simulation test added in a version must survive at least one
  subsequent version’s commit range without failure (“simulation continuity”).
  A test that passes at v0.X but fails at v0.X+1 blocks v0.X+1 signoff.
- Main remains runnable through the single `rockstream` binary.
- A sign-off file `sign-offs/vX.Y.md` exists with all checklist items marked, confirming the Proof criteria were verified. Run `make approve VERSION=X.Y` to generate the template; CI blocks merging a `✅ Done` marker if the sign-off is missing or has unchecked items.

Long soaks are gates, not loopholes. If a version needs a 24-hour or 30-day run,
the engineering work still fits the version budget, but the version is not
accepted until the soak result is clean.

---

## Public Milestones

These names are for orientation. They are not calendar commitments.

| Milestone | Version | Meaning |
|---|---:|---|
| Developer Alpha | v0.10 | Local single-shard engine can maintain simple views and survive crash/replay. |
| Developer Preview | v0.27 | Single-shard SQL engine demo-able to external users. Blog post + feedback loop. |
| SQL Alpha (internal) | v0.18 | Core SQL views, joins, set ops, and `EXPLAIN` work on one shard. Internal-only milestone; not externally advertised. |
| Single-Shard Beta | v0.27 | Advanced IVM is feature-complete enough for serious single-node testing. |
| Distributed Alpha | v0.36 | Multi-shard execution, frontier protocol, recovery, and exactly-once basics work. |
| Integration Beta | v0.48 | Postgres access, direct writes, and major external connectors work end to end. |
| Production Beta | v0.55 | Observability, auth, upgrades, security review, and long soaks are ready for a pilot. |
| Data Lake GA | v0.58 | Cold-tier Iceberg/Delta sinks, Iceberg REST catalog, external tool consumption proven. |
| 1.0 | after v0.58 | Tagged only after a real production handoff succeeds without design exceptions. |

---

## Version Roadmap

Each row is about 10 person-weeks. The "proof" column is the important part:
without that proof, the version is not done.

### Foundation

| Version | Status | Focus | Scope | Proof |
|---|---|---|---|---|
| v0.1 | ✅ Done | Repository workbench | Cargo workspace, core crates, CI, dev container, `rockstream` binary stub, basic `tracing`, pinned MSRV, dependency policy. | Clean CI on an empty no-op binary; `rockstream --help` works; repository has no hidden local setup step. |
| v0.2 | ✅ Done | Runtime abstraction and simulation seed | `rockstream-sim`, `Runtime` trait, `TokioRuntime`, `SimRuntime`, in-memory object store and network, seeded clock, first `buggify!()` macro, explicit fault-model registry, paired-assertion helper pattern. | A deterministic test replays the same seed byte-for-byte; changing the seed changes event order; production build compiles with `buggify!()` as no-op; every `buggify!()` site names a fault-model entry. |
| v0.3 | ✅ Done | SlateDB storage contract | `rockstream-storage`, key encoders (including `namespace_id` in all catalog keys from day one), `ShardDb`, `WriteBatch` builders, `DbReader`, WAL reader smoke tests, merge operator registry, no range-delete dependency. | Storage API validation suite proves only supported SlateDB features are used; unsupported operations fail at compile/test time; catalog key encoders include namespace dimension. **SlateDB determinism test**: two `SimRuntime` runs at the same seed, driving SlateDB against the in-memory `ObjectStore` facade, must produce bit-identical key–value state and WAL sequences; any internal SlateDB async behavior that bypasses the seeded scheduler causes the test to fail. This is the gate that validates the FoundationDB simulation property holds *through* SlateDB, not merely around it. |
| v0.4 | ✅ Done | No-op pipeline and local CLI | `rockstream start --storage=./data`, no-op source, no-op operator, no-op view sink, support-bundle skeleton, audit-log skeleton, error-code registry. | `make e2e` starts a local process, runs a no-op pipeline, emits audit events, writes a support bundle, and shuts down cleanly. |

### Single-Shard IVM Kernel

| Version | Status | Focus | Scope | Proof |
|---|---|---|---|---|
| v0.5 | ✅ Done | Z-set, PlanIR kernel, and MergeLaw contract | Shared Z-set types, `PlanNode`, `OpNode`, `Operator`, `EpochOutput`, fixed in-memory source, shard-local scheduler loop, and the foundational **`MergeLaw` / `LawBundle` catalog** in `rockstream-types` with `WeightAdd/v1` as the first registered law; shared law property-test harness. | Property test verifies simple Z-set insert/delete/update algebra against `WeightAdd/v1`; law-harness property tests (associativity, commutativity, identity, idempotence-where-declared, serialization round-trip, fail-closed malformed operand) pass for the registered laws; no storage yet beyond in-memory execution. |
| v0.6 | ✅ Done | Filter, project, map; law-aware storage reads | `DiffCtx`, filter/project/map differentiation, Arrow batch handling, `_weight` convention, DataFusion expression evaluation; `LawBundle` wired through `ShardDb::merge`, `get_merged`, `scan_merged`; merge-read fallback path with `merge_law_fallback_total` metric. | Random insert/delete sequences match DataFusion batch for filter/project/map queries; merge-read fallback is exercised by a profile that disables read-path resolution and the metric increments correctly. |
| v0.7 | ✅ Done | Algebraic aggregates on the law contract | `SUM`, `COUNT`, `AVG`, `COUNT(*)`, aggregate arrangement, `SumCount/v1` registered, `AggregateMergeOp` re-implemented on top of `LawBundle`, arrangement headers carry `(law_id, law_version)`, last-emitted cache. | >=100k randomized aggregate scenarios match the batch oracle; every `0xAG` arrangement reads back its `(law_id, law_version)` header on mount; benchmark captures baseline throughput and per-law RMW-avoidance ratio. |
| v0.8 | ✅ Done | Non-invertible aggregates with cached law slots | `MIN`, `MAX`, indexed multiset state, cached extremum backed by `MaxRegister/v1` / `MinRegister/v1` as a *sub-component* law (the operator itself stays retraction-aware), delete path via prefix scan, merge-backed read correctness. | Random group churn tests match batch; stale merge operands cannot hide from `get_merged()` / `scan_merged()`; cached-slot law usage is reported in `EXPLAIN INCREMENTAL`. |
| v0.9 | ✅ Done | Epoch commit and replay | Shard-level group commit, persisted frontier, crash mid-epoch, idempotent replay, WAL listing cache in the hot path, cooperative scheduling with yield points (`max_rows_per_quantum`, DESIGN.md §9.3). | Kill-injected mid-commit run restarts to bit-identical output; WAL hot path issues no object-store `list()`; a single expensive operator epoch cannot starve heartbeat sends (verified by scheduler yield ratio metric). |
| v0.10 | ✅ Done | Developer Alpha loop | One-binary local workflow, embedded runtime profile, first `rockstream explain` (prints merge law / `not_merge_safe_reason` for every operator), support bundle includes plan/log/shard stats, docs for local view development; **built-in `GENERATE ROWS` source** for zero-friction first-run (DESIGN.md §13.5.0) — a developer can create a synthetic data source and maintain a working view in under two minutes. | A developer can start RockStream, feed records, maintain a simple aggregate view, crash it, restart it, and inspect what happened; `rockstream explain` shows the merge law for SUM/COUNT/AVG/DISTINCT and a `not_merge_safe_reason` for MIN/MAX; embedded fast-path benchmark reports p50/p95 freshness with zero gRPC shuffle calls; `CREATE SOURCE demo.orders FROM GENERATE ROWS AS (…) RATE = 100 PER SECOND` produces rows immediately with no external dependencies. |

### Core SQL

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.11 | ✅ Done | DataFusion SQL lowering with law propagation | Parser, binder, logical optimizer integration, `LogicalPlan` to `PlanNode`, basic `CREATE VIEW`, scalar expressions, casts, CASE; every lowered aggregate / set op / monotone term carries a `MergeLawId` or an explicit `not_merge_safe_reason`. | SQL and hard-coded PlanIR produce identical physical plans for the Phase 1 operators; every aggregate node in the plan dump shows a registered law or a `not_merge_safe_reason` from the closed enum. |
| v0.12 | ✅ Done | Catalog and plan persistence | Source/view schema catalog, Substrait + RockStream extension encoding, schema-version storage, compatible-change rules; persisted plans store `(law_id, law_version)` per operator. | Plans round-trip through storage; compatible schema change succeeds; incompatible drift returns `RS-1002`; replaying a persisted plan against an unknown law returns `RS-5002`. |
| v0.13 | ✅ Done | Inner joins | Dual arrangements, stable row identity, pre-change snapshot semantics, join metadata, TPC-H Q1/Q3/Q5/Q6 subset. | Random three-way join property tests and selected TPC-H queries match DataFusion batch. |
| v0.14 | ✅ Done | Outer, semi, anti joins | Left/right/full outer joins, semi/anti joins, unmatched-row state, pg_trickle Q21 and FULL JOIN aggregate edge cases. | Q11/Q21 and randomized NULL-heavy tests match batch and pg_trickle-derived expectations. |
| v0.15 | ✅ Done | Distinct and set operations on `WeightAdd/v1` | Distinct, union, intersect, except, bag/set semantics, zero-crossing state, snapshot-safe cleanup rules; `WeightAdd/v1` drives every set-op arrangement; min-clamp (EXCEPT/INTERSECT) is documented as `not_merge_safe_reason=clamp_not_a_law`. | Random set-operation sequences match batch; compaction filter is disabled until safety proof exists; law-equivalence test compares combined vs. uncombined paths over set-op workloads. |
| v0.16 | ✅ Done | Workload DDL and view lifecycle | `CREATE WORKLOAD` with `FRESHNESS_SLO`, `MEMORY_LIMIT`, `PRIORITY`; workload assignment on `CREATE MATERIALIZED VIEW` (`WITH WORKLOAD = name`); namespace-level workload default (`ALTER NAMESPACE ... SET DEFAULT WORKLOAD`); per-view `PAUSE`/`RESUME MATERIALIZED VIEW`; view dependency metadata; `SHOW VIEW STATUS FOR NAMESPACE`; `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW`. | A workload can be created; a view can be created referencing it, paused, and resumed with audit events for every transition; `SHOW VIEW STATUS` reports current state and SLO. |
| v0.17 | ✅ Done | Explain and estimates | `EXPLAIN INCREMENTAL`, `EXPLAIN INCREMENTAL ESTIMATE`, `EXPLAIN INCREMENTAL VERBOSE`, `EXPLAIN INCREMENTAL ANALYZE`, source stats hooks, estimated state size, request rate, confidence labels; `EXPLAIN INCREMENTAL` law-annotation contract is **finalized** (`merge_law=<name>/v<n>`, `law_class`, `idempotent`, `duplicate_policy`, `compaction`, `combiner`, `partial_pushdown`, `not_merge_safe_reason`). The set of `not_merge_safe_reason` strings becomes a closed enum in `rockstream-types`. **Three explain levels** (DESIGN.md §14.8): default human-readable summary with ✓/⚠/✗ indicators; `VERBOSE` adds merge-law annotations, shard counts, parallelism, frontier timestamps; `ANALYZE` adds live per-operator runtime statistics (rows/s, state reads, RMW ratio, p99 latency, DLQ entries) requiring a live worker round-trip. **Backfill cost preview prompt** (DESIGN.md §14.9): `CREATE MATERIALIZED VIEW` presents estimated cost and waits for confirmation before proceeding when backfill is expensive; `WITHOUT CONFIRMATION` bypasses the prompt for CI/programmatic use; `EXPLAIN INCREMENTAL ESTIMATE CREATE ...` provides the same information without executing. | Estimates are produced before deployment; observed vs. estimated error is tracked for representative workloads; explain output names merge-safe and non-merge-safe operators with closed-enum reasons; CI test enumerates every `not_merge_safe_reason` against the registry; cost preview prompt fires for a view with >1 GB estimated state; `VERBOSE` output includes shard counts and parallelism; `ANALYZE` output includes live p99 latency and rows/s. |
| v0.18 | ✅ Done | SQL Alpha soak | Core SQL correctness pass across filter/project/map, aggregates, joins, set ops, DDL, explain, catalog. `PlanNode::Join` lowering added to `SqlFrontend`; inner and cross joins lower to `PlanNode::Join` with AND-folded equijoin conditions or constant-true for cross joins. `rockstream sql "<query>"` subcommand added to the binary: registers built-in demo schema (`orders`, `products`, `events`), plans SQL via DataFusion, lowers to PlanNode IR, and prints `EXPLAIN INCREMENTAL`. Deterministic SQL Alpha fuzzer (`SEED=0x5EED_1850_ADEF_0018`, 256 iterations) generates random Phase 1 PlanNode trees and verifies: no panic, every aggregate annotated, explain output stable across re-runs. v0.18 catalog proof tests verify join round-trip and DiffCtx no-divergence across catalog load/in-memory path. | One-hour fuzzer finds no divergence (CI: 256 deterministic iterations with fixed seed, no failures); SQL Alpha demo runs from `rockstream sql`; join plans round-trip through catalog codec; all Phase 1 operators (Source, Filter, Project, Map, Aggregate×5, Join, Union) produce consistent law annotations across DiffCtx runs. |

### Advanced IVM

| Version | Focus | Scope | Proof |
|---|---|---|---|
| v0.19 | ✅ Done | Window functions | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`, sliding SUM/AVG, partition recomputation; sliding-aggregate sub-components reuse `SumCount/v1`. **Escape hatch**: if sliding-aggregate incremental maintenance proves intractable, fall back to partition-scoped recomputation for all window functions (correct, slower). | Window-heavy randomized tests match batch; partition recomputation cost is measured and documented; sliding-aggregate law re-use is reported in `EXPLAIN`. |
| v0.20 | ✅ Done | Time windows, watermarks, and `MaxRegister/v1` | TUMBLE, HOP, SESSION, event-time TTL, late-data policies, frontier-aware retention hooks; watermarks register and emit through `MaxRegister/v1` / `MinRegister/v1` (semilattice, idempotent); event-time frontier driven by connector watermark channel from §13.3. **Escape hatch applied**: TUMBLE only implemented; HOP/SESSION deferred to the next version. | Late data test matrix proves `drop`, `update`, and `route_to_sink` behavior; TTL never removes visible state; a synthetic source that emits watermarks closes tumbling windows exactly once even under out-of-order input; duplicate-watermark replay test proves idempotence. |
| v0.21 | ✅ Done | Top-K and approximate sketches | Top-K detection, `K + epsilon` state, delete refill path, delta swaps, partitioned Top-K; `HyperLogLog/v1` registered for internal cardinality estimation in the planner cost model. Escape hatch not triggered: HLL accuracy was sufficient for cost-model correctness. | Random insert/update/delete Top-K tests match batch; delete from current top K refills correctly; HLL sketch-union law tests prove idempotence under reorder and duplicate replay; partitioned Top-K verified with independent partition state. |
| v0.22 | ✅ Done | Recursion with monotone partial progress | Recursive PlanNodes, nested timestamp scheduler, semi-naive and DRed strategies, convergence detection, safety caps; monotone (insert-only) recursive terms publish `complete_through` via `WeightAdd/v1`'s monotone declaration. Escape hatch applied: DRed proved unsound under concurrent deletes; non-monotone terms rejected with `RS-1509` and flagged `not_merge_safe_reason=recursion_dred_required` in `EXPLAIN INCREMENTAL`. | Transitive closure and hierarchy examples converge; cyclic graph tests produce correct deltas with no infinite loop; a monotone reachability view emits partial progress before full convergence; `RecursiveOp` matches `RecursiveOracle` for randomised edge sequences. |
| v0.23 | ✅ Done | Bootstrap and snapshot mode | Snapshot sources, streamed bootstrap epochs, bootstrap frontier, reconciliation after connector position loss. `SnapshotOp` delivers a pre-loaded relation as insert-only batches; `resume_from(N)` skips committed rows to prevent duplication on restart; `is_complete()` signals the bootstrap frontier. `RS-1010` registered for position-loss interruption. | 100k-row equivalent synthetic snapshot matches `BootstrapOracle` batch reference; restart during bootstrap (resume_from) does not duplicate or skip rows; idempotent epoch replay verified. |
| v0.24 | ✅ Done | View-on-view DAG | `ViewRef`, upstream view CDC, cadence inheritance, diamond consistency (structural via frontier meet — no explicit group API), cycle detection. `PlanNode::ViewRef` and `OpKind::ViewRef` added; `dag` module with `detect_cycle` (RS-1011) and `topological_order`; `ViewDagOracle` reference implementation; 11 proof tests. | Five-level DAG and diamond topology converge under continuous input; cycles are rejected at compile time. |
| v0.25 | ✅ Done | Lateral, SRF, UDF, and approximate-aggregate surface | Lateral functions/subqueries, row-scoped recomputation, scalar UDF hooks, UDAF interface sketch; `APPROX_COUNT_DISTINCT(v)` exposed via `HyperLogLog/v1`, `APPROX_MEMBERSHIP(v)` via `BloomUnion/v1`. | JSON/unnest/generate_series style examples match batch; `APPROX_*` aggregates pass sketch-union law tests; UDAF requirements (including a future `MergeLaw` annotation slot) are documented before implementation. |
| v0.26 | ✅ Done | IVM correctness freeze with law equivalence | TPC-H 22/22 single-shard, Nexmark subset, random query fuzzer, pg_trickle side-by-side where supported; law-equivalence corpus compares law-using and law-bypassed execution of the same query. | 22/22 TPC-H queries match batch; fuzzer runs at least one hour without divergence; law-equivalence corpus reports zero divergence. |
| v0.27 | ✅ Done | Single-shard performance profile | Criterion suite, object-store request budget, manifest churn budget, merge-read fallback, state-budget enforcement; per-law RMW-avoidance ratio published. | >=10x speedup vs. batch at 1% change rate for representative queries, or documented gaps with follow-up issues; per-law metric proves `SumCount/v1` and `WeightAdd/v1` avoid RMW on the hot path. |

### Distributed Core

| Version | Status | Focus | Scope | Proof |
|---|---|---|---|---|
| v0.28 | ✅ Done | Control plane and worker discovery | Control service, worker registration (with `capacity_headroom` reporting), topology catalog, bootstrap command, mTLS scaffolding, role flags. | Tier 1 and Tier 2 start flows work; workers join through `--control=<url>`; topology changes are audited; placement algorithm respects reported capacity. |
| v0.29 | ✅ Done | Shard leasing and scheduling | Shard manager, per-shard SlateDB handles, lease acquisition, writer fencing, distributed operator placement. | Two-writer fence test proves only one writer can commit; killing a worker causes clean lease release/reassignment. |
| v0.30 | ✅ Done | Direct exchange with planner-driven combiners | gRPC shuffle service, exchange path classifier (`elided` / `loopback` / `direct` / `durable`), worker-level multiplexing, same-worker loopback, pre-shuffle combiners driven **entirely** by planner-attached `MergeLawId` (the v0.4-style hand-coded SUM/COUNT/AVG allowlist is deleted), Arrow serialization, credit backpressure. | 16-shard single-host cluster runs partitioned TPC-H subset with bounded connection count; loopback avoids network calls for co-located exchanges; combiner benchmark documents bytes avoided per registered law and proves uncombined equivalence (CI property test, one entry per law). |
| v0.31 | ✅ Done | Durable shuffle fallback with law-aware re-merge | Object-store fallback path, coalesced shuffle objects, outbox/inbox metadata, receiver notifications, no LIST hot path; durable receiver re-merges per-target operands via the same `LawBundle`. | Inject receiver failure and large batch; sender falls back durably and receiver catches up without duplicates; durable + direct paths produce bit-identical state across every registered law. |
| v0.32 | ✅ Done | Frontier protocol with law-aware partial progress | Antichain type, per-shard frontier reporter, worker-level frontier summaries, separable `--role=frontier`, cluster frontier publication, shuffle GC; monotone laws may publish `complete_through` ahead of cluster frontier. | Multi-input join with uneven sources produces no premature output; aggregator stress test covers thousands of shards × hundreds of operators without direct per-shard subscriptions; a monotone-recursion view emits partial progress with a frontier-tagged completeness token. |
| v0.33 | ✅ Done | Distributed recursion and skew stress | Exchange inside recursive scopes, inner frontier convergence, skewed inputs, per-shard recompute fallback. | Sharded 10M-edge reachability benchmark converges; stalled inner frontier surfaces a named error. |
| v0.34 | ✅ Done | Cluster checkpoints | Barrier injection, bounded alignment buffers, per-shard checkpoint creation, atomic cluster checkpoint commit, old checkpoint GC. | Checkpoint under slow input and credit exhaustion never grows unbounded and either succeeds or reports `RECOVERING`. |
| v0.35 | ✅ Done | Recovery driver and SLO metrics | Recovery from cluster checkpoint, shard reassignment, failure detection, recovery histograms, `RECOVERING_SLOW`, worker self-fencing on control-plane partition (DESIGN.md §11.6), staggered restart / lease-grant rate limit to prevent thundering herd (DESIGN.md §11.8). | At target shard size: failure detection <=5s, shard reassignment <=30s, pipeline freshness recovery <=60s in chaos runs; 32-worker simultaneous restart triggers no false failure detections. |
| v0.36 | ✅ Done | Exactly-once, chaos alpha, and law-equivalence under fault | 2PC sink protocol, Kafka/S3/Postgres sink stubs, simulation suite for epoch/frontier/checkpoint/2PC interleavings, liveness checks tied to recovery SLOs, object store brownout handling (DESIGN.md §11.7), wire protocol version skew contract (DESIGN.md §5.5); **law-equivalence-under-fault** corpus: every registered law contributes seeded `SimRuntime` tests for reorder/duplicate/crash/replay/fence. **Continuous simulation soak infrastructure**: scheduled CI job that runs new seeded `SimRuntime` executions against `main` around the clock from this version onward; failing seeds are minimized, stored as regression seeds, and block release. | >=100k simulation seeds pass; every recoverable injected fault either commits a new epoch within the 5s/30s/60s budgets or surfaces a named degraded state; a 24-hour 32-shard chaos run has zero data loss and zero duplicates; 60-second object-store blackout test recovers cleanly; **continuous soak CI job is running, has produced its first regression-seed corpus, and the corpus includes at least one seed per registered law**. |

### Elasticity, Gateway, and Connectors

| Version | Status | Focus | Scope | Proof |
|---|---|---|---|---|
| v0.37 | ✅ Done | Online split and merge with law-aware compaction | Online shard split, cold shard merge, checkpoint-copy-replay, shard-map version bump, cleanup after cutover; arrangement law headers and `TombstoneGc` compaction policy preserved across ownership transfer. | Sustained workload continues through split/merge; output remains equal to uninterrupted reference; OR-Set arrangement under sustained add/remove survives a split without losing causal-stability invariants. |
| v0.38 | ✅ Done | Proactive scaling and rebalancing | `target_shard_state_bytes`, proactive splitter, worker scale-out, worker drain protocol (DRAINING → DECOMMISSIONED), `cluster_worker_pressure` metric for infrastructure autoscaling, skew detection, adaptive re-sharding, hot-key virtual buckets. | Drive one shard to 30GB; split starts before alert threshold and no freshness SLO is missed; a skewed-key benchmark stays within the documented worst-shard/median load factor; drain a 4-shard worker within 120s with no epoch loss; `cluster_worker_pressure` metric is exposed and HPA-consumable. |
| v0.39 | ✅ Done | Clone, schema evolution, and law-version upgrade | Pipeline clone, blue/green plan replacement, atomic flip, compatible/incompatible schema workflows; incompatible `MergeLaw` version upgrades take the blue/green path. | Breaking schema change goes through clone/backfill/flip without source offset loss; a forced `MergeLaw` version bump for an existing view re-encodes via clone without loss. |
| v0.40 | ✅ Done | Postgres read gateway | pgwire startup/query/extended-query, row descriptions with Postgres OIDs, catalog stubs (`pg_catalog`, `information_schema`), snapshot reads, connection pooling, query timeouts, rate limiting; **inline views** (DESIGN.md §4.3): `CREATE VIEW` stores a query definition in the catalog and expands it as a macro at query or materialized-view compilation time — no arrangement, no shard, Postgres-standard semantics. | `psql` and SQLAlchemy can read views; view schema reflects without ORM errors; `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` returns `RS-2003`; gateway reads complete in < 10 ms p99 for a local cluster; `CREATE VIEW v AS SELECT …; SELECT * FROM v` returns correct results via inline expansion; `DROP VIEW v` with a dependent materialized view returns `RS-2004`. |
| v0.41 | ✅ Done | Gateway introspection and law-driven read performance | `LawBundle::gateway_combiner`-driven cross-shard partial aggregation pushdown (DESIGN.md §12.3.1, §6.11); `rockstream_catalog` system schema virtual tables (`rockstream_catalog.epochs`, `rockstream_catalog.pipelines`, `rockstream_catalog.shards`, `rockstream_catalog.merge_laws`, `rockstream_catalog.audit_log`, etc., DESIGN.md §12.6.1); per-worker arrangement segment cache keyed by `(shard_id, segment_id)` (DESIGN.md §5.4). The historical `rockstream.*` prefix is accepted as a read-only alias through the 0.45 release and removed in the 0.50 release. | `SELECT COUNT(*), region FROM mv GROUP BY region` pushes partial agg to shards; gateway receives O(groups) rows, not O(view rows); explain output reports the merge law used for the pushed aggregate; `SELECT * FROM rockstream_catalog.merge_laws` returns the registered catalog with `(id, name, version, class, idempotent, …)`; segment cache hit ratio > 80% for hot-join workloads in benchmarks. |
| v0.42 | ✅ Done | Freshness, subscribe, isolation, historical and monotone-partial queries | `READ COMMITTED`, `REPEATABLE READ`, freshness tokens, `wait_for=<token>`, subscribe API, gateway restart behavior, `AS OF EPOCH <n>` / `AS OF TIMESTAMP <t>` historical queries (DESIGN.md §12.4.1), `AS OF MONOTONE PARTIAL` opt-in for monotone-law views, `checkpoint_retention_count` / `checkpoint_retention_duration` configuration; **subscribe ergonomics** (DESIGN.md §12.3): `SUBSCRIBE <view>` opens a live change stream with columns `mz_timestamp`, `mz_diff`, and projected view columns; `AS OF NOW WITH SNAPSHOT` for bootstrap + live; `AS OF EPOCH <n>` for resumption; server-side `WHERE` and column projection; per-view `CHANGE_RETENTION` (default 1 hour) controlling how far back a subscriber can resume. | Read-your-writes demo passes; subscribe stream survives gateway restart without gaps or duplicates; `SELECT * FROM orders_mv AS OF EPOCH <past>` returns the correct historical snapshot; `AS OF MONOTONE PARTIAL` returns a result with an explicit `complete_through` token for a monotone reachability view; queries beyond retention return `RS-2005`; `SUBSCRIBE orders_mv AS OF NOW WITH SNAPSHOT` delivers current state then live deltas; `SUBSCRIBE ... WHERE region = 'us-east'` reduces network traffic to matching rows only. |
| v0.43 |  | DML Alpha: Core pgwire DML | INSERT, UPDATE, DELETE, `INSERT ... RETURNING` over pgwire; Optimistic transactions with conflict detection; Session max-staleness. | `psql` runs DML successfully; conflict detection returns `RS-2008` (optimistic conflict); session max-staleness NOTICE emits correctly on stale frontier; fuzzer exercises concurrent optimistic transaction abort paths without oracle divergence. |
| v0.44 |  | DML Hardening: Transactions and DDL | Idempotency enforcement (client idempotency keys), `RS-2007` on missing keys; Write-fence tokens; Background DDL (`WAIT`/`NO WAIT`); Zero-downtime view replacement (`CREATE REPLACEMENT VIEW`); Namespace lifecycle (`CREATE/DROP NAMESPACE`). | 1M concurrent counter increments with idempotency keys land exact total; zero-downtime replacement swaps view routing atomically without disconnect; background DDL returns immediately; namespace commands audit successfully. |
| v0.45 |  | CRDT Columns Alpha | `COUNTER` backed by `PNCounter/v1`, `MAX_REGISTER`/`MIN_REGISTER` backed by `MaxRegister/v1`/`MinRegister/v1`, `LWW` backed by `LWWRegister/v1` column types; Session-scoped automatic read-your-writes. | `CREATE TABLE ... (amount COUNTER)` succeeds; updates round-trip; read-your-writes wait-for resolves within SLO; HLL and Bloom sketch-union laws validated on direct-write paths. |
| v0.46 |  | CRDT Columns Beta & Integration Beta Gate | `ORSet/v1`, `MVRegister/v1` column types; Integration Beta exit criteria sign-off. | OR-Set arrangement add/remove survives split and compaction; Integration Beta checklist and physical 4-host network test commitment signed off in `plans/phase9-signoff.md`. |
| v0.47 |  | External source/sink set, OR-Set, and CRDT-aware connectors (Phase 2 of user-visible CRDTs) | Kafka source/sink, Postgres CDC source, S3/table-format source and sink, HTTP push/webhook; every source implements the §13.3 Tier 1 contract (opaque `OffsetToken`, `watermark: Option<EventTimeWatermark>`, `credits_available()`) and routes per-record decode errors to a DLQ sink as `RS-1003`; every sink implements `prepare`/`commit`/`abort` with a default `should_flush` that flushes every epoch; `OR_SET` (`ORSet/v1`) column type with `TombstoneGc` compaction; connectors advertise built-in CRDT columns in `discover_schema`; **DLQ user surface** (DESIGN.md §13.3.1): `rockstream_catalog.dead_letter_queue` exposes failed records with `arrived_at`, `source_offset`, `error_code`, `error_message`, `raw_bytes_hex`, `replay_attempt`; `RS-1004 connector.dlq_growing` proactive warning at configurable `dlq_warn_threshold` (default 100) entries/hour; `ALTER SOURCE ... REPLAY DEAD_LETTER_QUEUE [SINCE ... UNTIL ...]` for re-decode after fixes (increments `replay_attempt`); `ALTER SOURCE ... DISMISS DEAD_LETTER_QUEUE WHERE ...` for known-bad records; `DLQ_RETENTION` per source (default 7 days). | Postgres CDC → RockStream IVM → Kafka sustains 100k rows/s for 24 hours exactly once; Kafka source closes a 1-minute tumbling window correctly under deliberate clock skew; under sustained downstream saturation, Kafka consumption rate tracks downstream credits with bounded inbox memory; OR-Set add/remove soak survives shard split with tombstone GC intact; DLQ: a source with 200 decode errors/hour emits `RS-1004`; `SELECT * FROM rockstream_catalog.dead_letter_queue` returns failed records; `REPLAY` re-processes entries after a schema fix. |
| v0.48 |  | Connector lifecycle, SDK, Tier 2 contract, CRDT schema metadata | Connector pause/resume/delete, external gRPC connector protocol, SDK, examples, isolation options; Tier 2 contract additions: `partition_filter: Option<PartitionFilter>` on source `start_snapshot`/`poll_delta` (opt-in; connectors that do not support it return `None` and fall back to operator-layer filtering), `should_flush(bytes_buffered, epochs_buffered)` override for file-format sinks (Iceberg/Delta/Parquet), and `LawSchemaMetadata` so connectors declare which columns advertise which built-in `MergeLawId`; **extended `LawSchemaMetadata`** includes write-classification fields (`blind_delta`, `read_dependent_delta`, `exact_key_guarded_delta`, `source_exactly_once_protected`) so external sources participate in the optimistic validation protocol without inventing a gateway-only path. | Third-party example connector passes Tier 1 contract tests; Iceberg sink implementing Tier 2 `should_flush` with a 10ms epoch produces ≤ 2 files/minute (≥ 256 MB each); a Tier 1 connector (e.g. Kafka) passes contract tests with the default flush-every-epoch `should_flush`; connector-declared CRDT columns round-trip through schema discovery and `EXPLAIN`; SDK example shows a connector declaring a `COUNTER` column end-to-end; `partition_filter_support() -> bool` returns false on connectors that do not implement pushdown and operator-layer filtering is verified to produce identical output; connector write-classification metadata surfaces in `EXPLAIN TRANSACTION`. |

### Production Beta

| Version | Status | Focus | Scope | Proof |
|---|---|---|---|---|
| v0.49 |  | Auth, RBAC, secrets, and transport security | OIDC/bearer auth, service accounts, per-view RBAC, mTLS everywhere, certificate rotation docs, secrets management (DESIGN.md §14.18): `CREATE SECRET` DDL, envelope encryption, KEK source config, worker-side token resolution, rotation without restart. | Auth integration tests reject unauthenticated and cross-tenant access; audit log has actor on every event; secrets are encrypted at rest; `SHOW SECRETS` never exposes values; rotation test updates a Kafka credential and active connector re-acquires without pipeline restart. |
| v0.50 |  | Observability, admin surface, and law diagnostics | Prometheus metrics, OTEL traces, JSON logs, admin CLI, support bundle completeness, dashboard template, `rockstream debug arrangement` IVM debugger (DESIGN.md §14.7.1) decodes arrangement law headers, tombstone density metric and proactive compaction trigger (DESIGN.md §5.4); full `merge_law_*` metric family live (applied, fallback, compaction-bytes-reclaimed, duplicate-dropped, tombstone-bytes, monotone-partial-lag); **actionable error messages** (DESIGN.md §14.14): every `RS-XXXX` error includes a `next_steps` field with remediation guidance, enforced in CI; **resource usage visibility** (DESIGN.md §14.19): `SHOW RESOURCE USAGE`, `SHOW RESOURCE USAGE FOR WORKLOAD <name>`, `SHOW CLUSTER RESOURCE USAGE` commands; `rockstream_catalog.view_resource_usage` and `workload_resource_usage` catalog tables; proactive NOTICE at 80% (`RS-5018`) and WARNING at 95% (`RS-5019`) of any budget; **schema evolution detection** (DESIGN.md §4.2): `SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA <name>` and `SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW <name>`; `RS-6001 schema.incompatible_evolution` proactive notice. | Operator can diagnose a slow pipeline from SLO compliance -> degraded reason -> explain -> support bundle; can inspect a specific arrangement key, including its merge law, without stopping the pipeline; support bundle contains per-law statistics for the last 24h; every error in the registry has a non-empty `next_steps` field (CI enforced); `SHOW RESOURCE USAGE` returns per-workload state/memory/SLO summary; a workload at 82% memory utilisation triggers `RS-5018` NOTICE; `SHOW SCHEMA_EVOLUTION STATUS` surfaces pending incompatible changes before they block. |
| v0.51 |  | Auto-tuner hardening | Adaptive parallelism, epoch sizing, source throttle, hysteresis, stability tests, override docs. | Random workload property tests converge without oscillation; every tuning action is audit logged. |
| v0.52 |  | Secondary indexes | `CREATE INDEX <name> ON <table> (<col>[, …])` DDL backed by the IVM engine: system-managed materialized view with `ARRANGE BY (index_cols, pk_cols)`, `BUILDING` → `READY` backfill lifecycle; `CREATE INDEX … WHERE <pred>` (partial indexes); `DROP INDEX` with frontier-aware arrangement GC; `REBUILD INDEX`; planner selects `index_scan` vs `shard_scan` using `index_prefer_selectivity_threshold` (default 0.01) and falls back when frontier lag exceeds `index_max_lag_ms`; index state bytes in `EXPLAIN INCREMENTAL ESTIMATE` and charged against `state_budget_gb`; error codes `RS-2014` (`index.building`), `RS-2015` (`index.frontier_lag`), `RS-2016` (`index.name_conflict`); simulation tests for backfill under concurrent writes, shard split during backfill, and crash/recovery. | `SELECT * FROM orders WHERE customer_id = 42` uses `index_scan` path when selectivity < threshold; planner falls back to `shard_scan` when index is in `BUILDING` state; partial index stores fewer rows and returns correct results; `DROP INDEX` GCs arrangement state within 2 epochs; index state bytes appear in `EXPLAIN INCREMENTAL ESTIMATE`; simulation: no data loss or duplicate rows after shard split during backfill. |
| v0.53 |  | Shard column statistics, upgrades, migration, and security review | Storage format gate, rolling upgrade test, migration skeleton, disaster recovery drill, independent security review; **shard column statistics for OLAP scatter pruning** (DESIGN.md §8.7, §12.3.1): per-shard min/max bounds, blocked Bloom filters (64 KB budget per column per shard), and HLL cardinality sketches piggybacked on checkpoint `WorkerFrontierSummary`; gateway planner prunes scatter set using column stats before scatter; `shard_stats_max_age` freshness guard with `RS-2017 shard_stats.too_stale` fallback; secondary-index stat injection (indexed columns publish stats automatically at build completion); metrics `scatter_shards_total`, `scatter_shards_pruned_total`, `shard_bloom_false_positive_total`. | N → N+1 rolling upgrade loses no epoch; incompatible format fails safely with `RS-5001`; security review issues are triaged; a selective query on a 100-shard view with 8 matching shards uses ≤ 12 shards in `EXPLAIN`; `RS-2017` is emitted as `NOTICE` when stats are stale; after `CREATE INDEX` the next checkpoint publishes stats for the indexed column; property test verifies Bloom never produces false negatives over 10k randomized workloads. |
| v0.54 |  | Long production soak, user-defined merge laws, and optimistic transaction subset | 30-day 64-shard soak, 1,000-shard control/exchange stress, Nexmark/TPC-H continuous, chaos automation, continuous simulation soak scaled to millions of seeds (infrastructure started at v0.36), release-blocking defect burn-down; **`CREATE MERGE LAW`** DDL behind a feature flag, gated on the shared property-test suite, explicit `duplicate_policy` / `compaction_policy`, registered `EXPLAIN` formatter, and one fault-model entry; **`--experimental-optimistic-crdt-transactions`** flag enabling: `SERIALIZABLE LOCAL` when planner proves one shard, optimistic exact-key guarded writes (`RS-2008` on conflict), CRDT-only transaction envelope prototype (if atomic visibility is implemented), and clear rejection (`RS-2009`) for unsupported shapes. | 99.99% availability target met or miss is understood and fixed; no correctness divergence; large-cluster stress stays within exchange and frontier budgets; all historical failing simulator seeds replay in CI; a sample user-defined law (e.g. min-clamped counter) passes the harness and survives 24h of fault injection; `SERIALIZABLE LOCAL` commits succeed for planner-proven single-shard transactions; optimistic exact-key conflict returns `RS-2008` on stale version; unsupported shapes return `RS-2009`; simulation corpus includes optimistic-validation crash interleavings. |
| v0.55 |  | Production beta handoff | Helm/Terraform packaging, deployment playbooks, SQL reference, connector guide, operator guide, reference architecture. | First pilot workload runs with support agreement, documented runbook, rollback plan, and known limitations. |

### Cold Tier & Data Lake Integration

| Version | Status | Focus | Scope | Proof |
|---|---|---|---|---|
| v0.56 |  | Cold-tier Parquet/Iceberg sink with law metadata | Iceberg v2 cold-tier sink writer (DESIGN.md §13.6): `CREATE SINK ... TO ICEBERG` with `snapshot_interval_epochs`/`snapshot_interval_ms`, Parquet data files with column stats, manifest files and manifest lists, atomic `metadata.json` commit, `should_flush`-gated buffering with pending rows staged in shard SlateDB, exactly-once via idempotent file keys; cold-tier snapshots embed `(law_id, law_version)` per CRDT column and write *finalized* values (folded counters, register winners, set memberships) so external readers never see raw operands. `ViewReader` `TwoTier` variant functional: gateway can merge cold snapshot + hot LSM tail. Cold snapshot GC (§13.6.2.1). | Cold-tier sink writes valid Iceberg v2 table readable by DuckDB `iceberg_scan`; full-scan query over a 100M-row view uses cold tier and completes 10x faster than hot-only LSM scan; snapshot GC keeps ≤ `cold_snapshot_retention_count` snapshots per view; crash mid-flush produces no orphan data files; a CRDT column read via DuckDB shows the finalized value. |
| v0.57 |  | Catalog registration and Iceberg REST catalog server | Catalog registration backends (§13.6.5): `filesystem` (self-contained, already functional), `glue`, `rest`, `hive`, `ducklake`. Native Iceberg REST catalog server (§13.7) on gateway HTTP port 8181: `/iceberg/v1/` serves namespaces, tables, snapshots backed by control-plane metadata. Auth token/mTLS passed through. | Spark/Trino/DuckDB discover views by name via `catalog.uri=http://rockstream:8181/iceberg/v1`; Glue catalog shows table within 30s of snapshot commit; `CATALOG_WARN` state surfaces cleanly when external catalog is unreachable; catalog API failures never block IVM. |
| v0.58 |  | Cold-tier soak, Delta Lake, law-version upgrade replay, and mixed optimistic transaction soak | Delta Lake cold-tier sink variant (`CREATE SINK ... TO DELTA`), cold-tier + hot tail merge correctness soak (randomized inserts/updates/deletes, compare cold+hot read vs. hot-only accumulated state); cold-tier law-version upgrade replay test (cold snapshot at law v1 + hot tail at law v2 must read consistently); snapshot interval tuning, cost-accounting (cold-tier storage bytes in `EXPLAIN INCREMENTAL ESTIMATE` and quota system); **mixed optimistic transaction soak**: mixed exact-key + CRDT validation under randomized concurrent writes, transaction envelope recovery from cold + hot tail, row-version metadata preserved in cold snapshots, compaction safety for pending/committed transaction operands; decision gate — if simulation finds no partial visibility and abort rates are explainable, promote optimistic subset to pre-1.0 documented behavior, otherwise keep experimental. | 7-day cold-tier soak with continuous writes shows no merge divergence; Delta `_delta_log/` is readable by DuckDB `delta_scan`; `EXPLAIN INCREMENTAL ESTIMATE` reports cold-tier storage cost within 20% of actual; cold-snapshot bytes count against pipeline `state_budget_gb`; law-version upgrade replay test passes for every registered law that declares cross-version compatibility; mixed optimistic transaction abort rate is < 5% under representative contention; no partial-visibility leaks observed over 7-day soak; `crdt_txn_pending_visible_total` stays at zero when atomic visibility is enabled. |

---

## Documentation Budget

Phase-boundary versions (v0.27, v0.36, `v0.48`, `v0.55`) each include a mandatory
documentation pass. The documentation deliverable for each boundary is:

| Version | Documentation scope |
|---|---|
| v0.27 | IVM operator reference: every operator in the catalog with its arrangement layout, merge-law annotation, and `EXPLAIN` output format. |
| v0.36 | Distributed architecture guide: frontier protocol, checkpoint protocol, recovery budgets, shuffle transport, and cluster bootstrap walkthrough. |
| `v0.48` | Connector developer guide: Tier 1 and Tier 2 contracts, SDK usage, DLQ configuration, and end-to-end example connector. |
| `v0.55` | Operator runbook: deployment playbooks, monitoring dashboards, upgrade procedures, disaster recovery drills, and troubleshooting flowcharts. |

Each documentation deliverable is listed as an explicit exit criterion in the
corresponding version's sign-off checklist. A version is not accepted until
the documentation is reviewed and merged.

---

## 1.0 Gate

RockStream should not tag 1.0 simply because v0.55 is complete. The 1.0 gate is
evidence-based:

- At least one real production or production-like workload has run long enough
  to exercise failure, recovery, upgrade, and backfill paths.
- No P0/P1 correctness bugs are open.
- Every supported SQL feature has an oracle test or batch-equivalence test.
- The deterministic simulator runs millions of seeds before release, and any
  historical failing seed is replayed in CI.
- Continuous simulation has run against `main` long enough to cover both safety
  and liveness failures across the explicit fault model.
- Recovery-time invariants hold at the target shard size.
- The upgrade path from the previous beta is tested and documented.
- The public docs state what RockStream is not: not an OLTP Postgres clone, not
  a global cross-shard `SERIALIZABLE` system, not active-active multi-region
  writes.
- A new operator can debug the system using the docs, dashboard, CLI, audit log,
  and support bundle without reading source code first.

If any of these are not true, keep shipping v0.x releases. That is not failure;
that is the discipline this project needs.

---

## Decision Gates

These are explicit places to pause, learn, and possibly reshape the roadmap.

| Gate | After | Question | Outcome |
|---|---:|---|---|
| Architecture sanity | v0.4 | Do SlateDB, the runtime abstraction, and local developer ergonomics still fit the design? | ✅ Passed. Confirmed. |
| IVM kernel confidence | v0.10 | Is the core delta engine simple enough to debug, and does replay work cleanly? | ✅ Passed. Full assessment in plans/full-assessment-v0.10.md. |
| Storage operational budget | v0.10 | Do SlateDB operational budgets (write amp, get_merged p99, compaction debt) hold at 5GB+ shard sizes on real object storage? | ⚠ Partial — validated under SimRuntime. Real S3 validation pending as Phase 4 entry condition. |
| SQL scope control | v0.18 | Are we still building the right SQL subset first, or have edge cases started to dominate? | ✅ Passed. SQL Alpha soak clean. |
| Single-shard correctness | v0.27 | Is the IVM engine correct and fast enough to justify distribution work? | ✅ Passed. TPC-H 22/22, fuzzer, law-equivalence corpus clean. |
| Distributed architecture | v0.36 | Does the shard/exchange/frontier/checkpoint model actually hold under simulation and chaos? | ✅ **Decided 2026-05-31 by Geir Ove Grønmo (Principal Architect).** Decision: **single-region only for Integration Beta (`v0.48`).** Multi-region deferred to v1.0+. Rationale: 100k simulation seeds and 32-shard 24h chaos runs validate the current single-region topology. Multi-region would require rewriting the frontier aggregation topology (§3.2 — currently a single control SlateDB instance) and adding cross-region consensus before the product has a single external pilot user. This violates the project principle of not designing for hypothetical scale. Phase 7–9 scope is set accordingly: single-region shuffle latency budgets, single control-plane frontier store. |
| CRDT value | v0.36 | Are internal merge-law annotations reducing read-modify-write, shuffle, and gateway scan cost enough to confirm the v0.43–v0.45 user-visible CRDT column rollout? If not, the column types are deferred and the version slots reassessed. | ✅ **Decided 2026-05-31 by Geir Ove Grønmo (Principal Architect).** Decision: **include CRDTs in v0.43–v0.46 on the decomposed schedule** (see v0.43 decomposition note in Version Roadmap). Rationale: `MergeLaw`/`LawBundle` annotations already drive pre-shuffle combiners (v0.30), gateway read pushdown (v0.41 scope), and law-equivalence-under-fault testing (v0.36). The foundational CRDT infrastructure is production-grade; deferring user-visible column types to v1.0 would strand this investment and remove Integration Beta's primary differentiating surface. v0.43 is decomposed into DML Alpha + DML Hardening + CRDT Alpha + CRDT Beta as documented. |
| User-defined laws readiness | `v0.48` | Is the built-in CRDT catalog (`COUNTER`, `LWW`, `G_SET`, `OR_SET`, `MAX/MIN_REGISTER`, `APPROX_*`) stable enough — under property tests, chaos, connector schema, and cold-tier — to open `CREATE MERGE LAW` to users in `v0.54`? | |
| Product wedge | `v0.48` | Is the `psql` + live views + connectors experience compelling enough for pilot users? | |
| Production readiness | `v0.55` | Is the system operable by someone who did not build it? | |
| Data lake integration | `v0.58` | Does the cold-tier + catalog story deliver real value, or is feeding external tools sufficient without the cold tier? | |
| Optimistic transactions | `v0.58` | Does the mixed optimistic + CRDT transaction subset work reliably under simulation and soak (no partial visibility, explainable abort rates), or should it stay experimental past 1.0? | |

At each gate, the default action is not to accelerate. The default action is to
remove uncertainty.

**Design freeze after v0.27.** The design freeze was originally specified for
v0.10 but was deferred through the single-shard implementation phase
(v0.11–v0.27) to allow design refinements informed by implementation
experience. As of v0.27, DESIGN.md has stabilized at v3.28. From this point
forward, new sections may not be added to DESIGN.md or IVM.md unless they are
required to unblock a specific coded milestone. The documents become
implementation references, not the primary work product. Gaps discovered during
coding are tracked as GitHub issues and resolved with targeted corrections to
the relevant section — not as numbered design-revision passes. The test: after
v0.27, every DESIGN.md commit should be small and targeted ("correct §7.5:
exchange loopback threshold"), not broad ("v3.X adds Y, Z").

**Enforcement.** The design freeze is enforced by CI: any PR that touches
DESIGN.md or IVM.md must include a `freeze-exception: <issue-url>` trailer
linking a GitHub issue that justifies the change. PRs without this trailer
that modify more than 10 net lines in either document are blocked. Typo fixes
and cross-reference corrections (≤ 10 net lines) are exempt.

---

## Parallel Work Tracks

The version order is the critical path, but work can proceed in parallel once the
interfaces are stable.

| Track | Can start seriously after | Notes |
|---|---:|---|
| Storage and simulation | v0.1 | This is foundational and should lead the rest of the project. |
| SQL compiler | v0.5 | Can prototype against in-memory operators before storage is mature. |
| Operator implementations | v0.6 | Each operator must come with oracle/property tests. |
| CRDT / merge-law semantics | v0.5 | The `MergeLaw` / `LawBundle` contract lands in v0.5 and is the substrate for every later algebraic feature. Internal laws (`WeightAdd`, `SumCount`, `MaxRegister`, `HyperLogLog`, `BloomUnion`) ship v0.5–v0.25; (`COUNTER`, `LWW`, `G_SET`, `OR_SET`, sketches) ship v0.43–v0.46; `CREATE MERGE LAW` is gated until v0.54. |
| Control plane | v0.12 | Catalog and plan persistence create the control-plane substrate. |
| Distributed runtime | v0.27 | Do not start serious distribution before the single-shard engine is frozen. |
| Gateway and pgwire | v0.18 | Can build against single-shard snapshots before distributed reads exist. |
| Connectors | v0.23 | Snapshot/bootstrap semantics should be stable first. |
| Observability/SRE | v0.4 | Should be threaded through all versions, not saved for the end. |
| Docs and examples | v0.4 | Every version should improve an example or runbook. |

---

## Things To Keep Out Until After 1.0

These may be good ideas later, but they dilute the first implementation:

- Active-active multi-region writes. The `MergeLaw` catalog is structured so an
  idempotent join-semilattice column could become a region-spanning surface
  later, but no version through 1.0 promises that path.
- Cross-shard `SERIALIZABLE` isolation. Optimistic exact-key guards and CRDT
  blind writes are pre-1.0 (§13.5.1), but a *global* cross-shard coordinator
  covering every shard is an explicit non-goal.
- Full OLTP compatibility with Postgres.
- Arbitrary user-defined CRDT merge functions before v0.51. `CREATE MERGE LAW`
  is gated on the built-in catalog (v0.43–v0.46) and the shared property-test
  suite proven in production soaks.
- A large web console before the CLI and metrics are excellent.
- Per-query billing or chargeback.
- Arbitrary user-defined distributed transactions.
- Performance-only rewrites before correctness evidence is strong.
- New storage backends before SlateDB is fully proven.

---

## First Six Versions In More Detail

The first six versions decide whether the whole project has the right bones.
They deserve extra care.

### v0.1: Repository Workbench

The goal is not to write a lot of product code. The goal is to make every later
change cheap and safe. CI, formatting, dependency checks, tracing, and the crate
layout should be in place before the system becomes interesting.

The best possible outcome is boring: a new contributor can clone the repo, run
one command, and see green tests.

### v0.2: Runtime Abstraction and Simulation Seed

This is the FoundationDB lesson applied immediately. If `Runtime` is not in the
first real code paths, it will be painful to retrofit. The simulator can start
small: deterministic clock, deterministic task scheduling, in-memory object
store, in-memory network, seeded failure injection.

The important proof is reproducibility. A failing seed must fail the same way
every time.

### v0.3: SlateDB Storage Contract

This version draws a hard line around what RockStream may assume about SlateDB.
It should make unsupported assumptions impossible or loud. No hidden range
delete. No casual reliance on global sequence numbers. No hot-path listing.

The adapter should feel small, explicit, and test-heavy.

### v0.4: No-op Pipeline and Local CLI

This is the first slice of the operator experience. Even with a no-op pipeline,
the single-binary promise, audit log, error registry, and support bundle become
real. That matters because every later feature will naturally plug into these
surfaces instead of inventing its own diagnostics.

### v0.5: Z-set, PlanIR Kernel, and the MergeLaw Contract

This is where RockStream starts to become an IVM system and where the
database-wide **merge-law contract** comes online. Keep the runtime in-memory
and small. Prove the Z-set algebra against the `WeightAdd/v1` law, ship the
`MergeLaw` / `LawBundle` types in `rockstream-types`, and stand up the shared
property-test harness that every later law must pass. From this version
forward, no merge operator ships without a registered law and an
`EXPLAIN`-visible identity.

### v0.6: Filter, Project, Map

The first useful operators should be intensely tested, not grand. A simple view
that stays correct through arbitrary inserts and deletes is the seed crystal for
the rest of the system.

---

## Roadmap Maintenance

This document should be updated whenever the project learns something material:

- A version was larger than 10 person-weeks and had to split.
- A risk moved from theoretical to proven.
- A proof became insufficient and needs a stronger gate.
- A public milestone changed meaning.
- A non-goal became tempting enough that it needs to be restated.

The roadmap should stay honest. Its job is not to make the project look fast.
Its job is to make the project buildable.
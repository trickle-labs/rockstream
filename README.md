# RockStream

**Keep your reports and dashboards up-to-date automatically as your workload grows — and query them with the database tools you already know.**

---

## The Problem

Imagine you run a business and you have a dashboard that shows today's sales, inventory
levels, and customer activity. Every time someone asks "what's the current total?", your
system has to go away, dig through all the historical data, add everything up from scratch,
and come back with an answer. If you have millions of records, that can take seconds — or
minutes. And the moment it finishes, the data is already slightly out of date.

This is how most traditional databases work. It's like tearing down and rebuilding a
scoreboard from scratch every time a player scores a point.

## The Solution

RockStream keeps a *live*, pre-computed answer for every question you care about. When
new data arrives — a new sale, a sensor reading, a customer action — RockStream figures
out *only what changed* and updates the answer in the blink of an eye. The scoreboard
stays up, and only the relevant numbers tick.

This technique is called **Incremental View Maintenance** (IVM). Instead of recomputing
everything, RockStream processes only the *difference* — the new or deleted records —
and applies it to the existing result. Think of it as keeping a running total rather than
re-adding every number on every receipt every time.

## How It Works (Simply)

```
New data arrives
      │
      ▼
RockStream figures out what changed (the "delta")
      │
      ▼
It updates only the affected parts of your pre-computed results
      │
      ▼
Your dashboards and reports quickly reflect the new reality
```

No routine full re-scans. Less waiting. Fresher data.

## What Makes RockStream Different

### Built on Cloud Storage

RockStream stores everything in object storage — the same kind of storage that powers
services like Amazon S3 or Google Cloud Storage. This means:

- **Bottomless capacity**: your data can grow without limits.
- **High durability**: your data is replicated automatically and won't disappear.
- **Low cost**: object storage is far cheaper than running dedicated database servers.

The underlying storage engine is [SlateDB](https://slatedb.io/), a modern database built
from the ground up for the cloud era.

### Scales With Your Workload — Without Touching Knobs

RockStream splits the work across as many workers as you need. Each worker is responsible
for a slice of the data. When traffic grows, the system adds parallelism to the slow
operators automatically. When it quiets down, it reduces it. Workers coordinate
automatically — no manual reconfiguration required.

For small workloads, RockStream can run as one local process and avoid distributed
network hops entirely. For large workloads, it uses shard-level parallelism,
pre-shuffle combining, hot-key splitting, and hierarchical coordination so a cluster
can grow without every component talking to every other component.

You tell RockStream *what* you want — "keep this view fresh within 1 second; do not
exceed 200 GB of state" — and the system figures out *how*: how many shards each
operator needs, how often to commit to object storage, when to scale up or back. The
mechanism knobs are still there if you need them, but you should not need to reach for
them first.

The design is intentionally honest about the limits of scale: object-storage request
rates, skewed keys, network shuffle, and source/sink throughput still matter. RockStream's
goal is to remove the single database writer as the central bottleneck by splitting state
across many independent SlateDB-backed shards, and to give you a clear, named reason
when any of those underlying limits become the binding constraint.

### Never Loses Work

If a worker crashes, another one picks up exactly where it left off. Every step is
carefully saved in a way that allows the system to restart without re-processing anything
it already handled, and without skipping anything.

### Speaks SQL

You define what you want to track using ordinary SQL queries — the same language used in
spreadsheets and most databases. Write a query like:

```sql
SELECT product_id, SUM(quantity) AS total_sold
FROM orders
GROUP BY product_id
```

…and RockStream will maintain the answer to that query continuously, as new orders
arrive.

### Use the Database Tools You Already Know

RockStream speaks the **Postgres wire protocol**. That means you can connect with
`psql`, your favourite BI tool, or any client library that already knows how to talk
to a Postgres database. Reading from a view looks exactly like reading from a
Postgres table:

```bash
psql -h rockstream.example.com -U app -d analytics \
  -c "SELECT * FROM sales_by_product ORDER BY total_sold DESC LIMIT 10;"
```

You can also **insert, update, and delete rows directly** into RockStream tables —
no separate database or Kafka topic is required to get data in. The change feeds your
views automatically, the same way an external source would. RockStream is not aiming
to replace Postgres for high-concurrency transactional workloads; it sits in the same
tier as streaming-SQL systems, with the convenience
of being reachable through standard Postgres tooling.

SQL also supports standard `CREATE VIEW` — a **inline view** stored as a reusable
query fragment in the catalog and expanded at compile time. Inline views carry no
IVM overhead, no arrangement shards, and no `view_output/` storage. They are the
right tool for ad-hoc query composition and as building blocks inside a `CREATE
MATERIALIZED VIEW`.

### Algebraic Aggregates and CRDTs

RockStream treats aggregation as an algebraic contract. Every operator node in a
query plan carries either a registered **merge law** — a named, versioned algebraic
rule (SUM, COUNT, AVG, MIN/MAX, or a similar internal CRDT-style rule) verified for
associativity and commutativity — or an explicit machine-readable reason why it
cannot.

When a merge law is in effect, RockStream can apply partial aggregates directly in
storage without a read-modify-write cycle, push combining steps to the producer
side of a network shuffle, and prune compaction safely. User-visible **CRDT column
types** — counters, sets, last-write-wins registers you declare directly in
`CREATE TABLE` — and a general `CREATE MERGE LAW` facility for your own laws are
on the design docs as a **post-1.0** goal, once the internal law contract has
proven itself; they are intentionally out of scope for the current roadmap (see
[NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md)).

`EXPLAIN INCREMENTAL` always shows the law name, or the exact reason it is absent,
for every node in your plan.

### Self-Healing

RockStream keeps itself in good shape without operator intervention:

- **Shards split before they get too big** *(planned; not yet implemented)*.
  When a slice of your data approaches the configured size limit, RockStream
  will quietly split it into two smaller slices in the background, so you
  never see a "shard is too large" page.
- **Workers recover in seconds, not minutes.** If a worker process dies, another one
  takes over its slice in under 30 seconds, and your dashboards are back to fresh
  within a minute. These targets are validated by the v0.31 Recovery SLO soak
  (see [sign-offs/v0.31.md](sign-offs/v0.31.md)) — tested on every release, not aspirational.
- **Quiet by default.** If everything is meeting its freshness target, you hear
  nothing. If something slips, you get a named reason — not a wall of metrics.

### Tested Like a Flight Simulator

Distributed systems are full of rare timing bugs that show up only when the network
is slow, a disk is full, and two workers crash within milliseconds of each other.
Catching these in production is painful and expensive.

RockStream borrows a technique from [FoundationDB](https://www.foundationdb.org/):
the entire distributed system runs inside a deterministic simulator that replaces
the network, the storage, and the wall clock with deterministic fakes. Before every
release, this simulator runs **millions of seeded scenarios** with random crashes,
message reorderings, and partial failures. Any bug it finds is reproducible from a
single seed number, so it can be fixed once and never come back.

The practical result: when you put RockStream into production, the kinds of
distributed-systems surprises that usually fill a runbook have already been
discovered — and fixed — on a developer's laptop.

### Data-lake export

The Iceberg, Delta, and object-store sink frontends are removed. Use
RockStream to Kafka with a downstream writer; see
[connector migration](docs/connector-migration.md).

## Inspiration

RockStream is inspired by production systems and implementation research:

| System | What it does |
|---|---|
| **[DBSP](https://github.com/vmware-research/database-stream-processor)** | Mathematical theory that guarantees incremental results are always identical to what a full re-computation would produce |
| **[pg_trickle](https://github.com/trickle-labs/pg-trickle)** | Shows how to turn SQL views into practical per-operator delta rules, with many hard correctness cases worked through in PostgreSQL |
| **[SlateDB](https://slatedb.io/)** | Provides the cloud-native object-storage-backed LSM that RockStream uses as its durable shard and arrangement store |

RockStream brings these ideas to an open, cloud-native storage foundation.

## Key Concepts (Jargon-Free)

| Term | Plain-English Meaning |
|---|---|
| **Materialized view** | A saved, pre-computed answer to a query |
| **Delta / change** | Only the new or removed records since the last update |
| **Epoch** | A small batch of changes processed together, like a transaction |
| **Worker** | A process that handles one slice of the data |
| **Frontier** | A marker that says "no future change before this point is expected"; this is the distributed version of a watermark |
| **Checkpoint** | A saved snapshot of progress so the system can recover after a crash |
| **CDC** | Change Data Capture — streaming the changes out to other systems |
| **Inline view** | A reusable query fragment stored in the catalog; expands at compile time — no IVM overhead, no operator state, no arrangement storage |
| **Merge law** | A named algebraic rule (e.g. SUM, a CRDT) that the system can apply safely without a read-modify-write cycle; every operator carries one, or an explicit reason it does not |

## Status

This project is under active, evidence-driven implementation — every roadmap
version ships only once its tests, benchmarks, and (for coordination
protocols) FizzBee models prove the new capability works. The current
released version is **v0.42** ("Wire Protocol End-User Complete" — see
[sign-offs/v0.42.md](sign-offs/v0.42.md)): the single-shard IVM engine, the
distributed frontier/fault-tolerance protocols, and the PostgreSQL wire
gateway (auth, transactions/savepoints, LISTEN/NOTIFY, and a certified
driver-compatibility matrix) are all done and proven. Work is proceeding
through the supported Kafka-based export path toward the v1.0
release candidate at v0.59. Four documents describe the system in
progressively more detail:

| Document | Audience | What it covers |
|---|---|---|
| [DESIGN.md](DESIGN.md) | Engineers / architects | Full system architecture: storage layout, operator state, worker coordination, fault tolerance, scaling model, deployment ladder, operational guide |
| [IVM.md](IVM.md) | IVM specialists | How the incremental-view-maintenance engine itself works — DBSP-native operators, the differentiation pass, the circuit runtime, arrangements on SlateDB, and pg_trickle as a correctness oracle |
| [NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) | Implementers | The focused, two-pillar engineering plan (cloud-native IVM engine + PostgreSQL wire access layer) this roadmap implements |
| [NEW_ROADMAP.md](NEW_ROADMAP.md) | Builders / planners | Version-by-version delivery roadmap, with each roadmap version sized at about 6 person-weeks and tied to concrete proof |
| [ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) | Maintainers / design partners | Strategic scope past v0.51.26: what stays core (Tier A), what stays maintained-only (Tier B), and the admission rule new breadth (Tier C) must pass |

## Roadmap

RockStream is built in phases, each delivering a working, testable increment.
Each roadmap version is sized at roughly **6 person-weeks** of implementation
effort. See [NEW_ROADMAP.md](NEW_ROADMAP.md) for the full version-by-version
breakdown and proof obligations, and [sign-offs/](sign-offs/) for the
evidence behind every completed version.

### Public Milestones

| Milestone | Version | What it means |
|---|---:|---|
| Foundation Ready | v0.3 | Workspace, runtime abstraction, simulation, oracle harness, and the SlateDB determinism gate all pass |
| Single-Shard Alpha ✅ Done | v0.6 | A local engine maintains filter/aggregate/MIN-MAX views and survives crash-replay |
| SQL Engine (Phase 2 entry) ✅ Done | v0.10 | Plain-SQL views with joins and set operations maintained incrementally on one shard |
| IVM Correct (Single-Shard) ✅ Done | v0.14 | TPC-H 22/22 incremental == batch; the engine is feature-complete and correct on one shard |
| Distributed Engine ✅ Done | v0.17 | Multi-shard execution with exchange; distributed output bit-identical to single-shard |
| Progress-Tracked ✅ Done | v0.19 | Frontier protocol correct across multi-input operators; bounded shuffle storage |
| Fault-Tolerant ✅ Done | v0.22 | Exactly-once end-to-end; 24h chaos with zero loss/duplicates; recovery SLOs met |
| Postgres Pillar ✅ Done | v0.26 | Read, write, subscribe, and read-your-writes over the Postgres wire protocol |
| Soaks Complete ✅ Done | v0.31 | Ingestion connectors live; failure-detection, shard-reassignment, and freshness-recovery SLOs validated under real cloud pressure |
| Private Beta Ready ✅ Done | v0.32 | Secondary indexes enable single-digit-ms point lookups on non-primary keys; open for early-adopter onboarding |
| Nexmark Correctness Complete ✅ Done | v0.36 | Nexmark q0–q9 and q12–q22 bit-identical to the DataFusion batch oracle |
| Wire Protocol Complete ✅ Done | v0.39 | Extended query protocol, full Postgres type OID coverage, ORM driver compatibility, PgBouncer pooling, protocol fuzzing |
| Wire Protocol End-User Complete ✅ Done | v0.42 | SCRAM/MD5 auth, full transaction/savepoint state machine, LISTEN/NOTIFY, a certified driver-compatibility matrix, and an unmodified reference app running end to end over pgwire |
| Elastically Scalable | v0.47 | Hot keys split into virtual buckets and shards split before they get too big, entirely in the background; the cluster exports autoscaling signals a standard Kubernetes HPA/KEDA can act on — no manual re-sharding |
| Public Demo Ready | v0.51.6 | A vanilla, autocommitting psql/ORM/BI-tool connection round-trips data with zero private ritual; ad hoc queries honor WHERE/JOIN/GROUP BY; the gateway serves views through the real incremental engine on one unified data plane instead of a disconnected batch recompute; the wire is TLS-terminated for a standard client |
| Operationally Complete | v0.56.1 | Full operator CLI + arrangement debugger, explainable freshness lag, internal mTLS, secrets management, an independent security review, and a proven rolling-upgrade/disaster-recovery path |
| v1 Contract Published | v0.57.1 | Every capability carries a strategic tier (Core/Maintain/Experimental), every Core operator's incremental/backfill/recovery/state-growth/failure semantics are documented, and CI fails if the published capability matrix drifts from the roadmap |
| 1.0 Release | v0.59 | All v0.1–v0.58 features integrated; the seven v1 release gates each signed off against a named artifact; 2-week continuous chaos cycle passes with zero P0/P1 bugs; `v1.0.0` tagged |

### Phase Summary

| Phase | Focus |
|---|---|
| 0 | Repository, deterministic simulator (`SimRuntime` + `buggify!()`), SlateDB storage contract, no-op pipeline |
| 1 | Single-shard IVM core: filter, project, map, algebraic aggregates (SUM/COUNT/AVG), MIN/MAX; crash-replay |
| 2 | DataFusion SQL frontend, inner/outer/semi/anti joins, distinct and set operations, storage budget gate |
| 3 | Advanced operators: window functions, tumbling time windows, Top-K, bootstrap and view-on-view DAGs; single-shard correctness soak (TPC-H 22/22, fuzzer, DST harness) |
| 4 | Multi-shard execution, gRPC shuffle, durable shuffle fallback, rendezvous hashing with virtual nodes |
| 5 | Frontier protocol, frontier aggregator, bounded shuffle storage, progress tracking |
| 6 | Fault tolerance, exactly-once end-to-end, cluster checkpoints, chaos testing, all four FizzBee models (M1–M4) green |
| 7 | PostgreSQL wire gateway: read/write/subscribe, inline views, freshness tokens, read-your-writes |
| 8 | External connectors (Kafka, Postgres CDC, S3) and "the crucible" cloud-pressure soaks: failure-detection, shard-reassignment, and freshness-recovery SLOs |
| 9 | Operational HTAP ergonomics: secondary indexes, session controls, private-beta readiness |
| 10 | Nexmark correctness suite: q0–q9, q12–q22 bit-identical to batch under mixed INSERT/UPDATE/DELETE |
| 11 | PostgreSQL wire protocol hardening: extended query protocol, full type/OID and `pg_catalog` coverage, SCRAM/MD5 auth, transactions/savepoints, LISTEN/NOTIFY, reference-app and driver-matrix certification |
| 12 | Kafka-based downstream export and data-lake writer integrations |
| 13 | Elastic scaling and skew handling: online shard migration, hot-key virtual buckets, proactive shard splitting, cluster autoscaling signals |
| 14 | Network efficiency and advanced DML: scatter pruning, zero-copy IPC, AZ-aware shuffle |
| 15 | Complex analytics and compute tuning: recursive CTEs, lateral joins, hopping/session windows, hot-path optimizations |
| 15.5 | Standard wire compatibility and the real incremental serving path: standard autocommit/idempotency/immediate-view-population semantics, query-time WHERE/JOIN/GROUP BY execution, gateway+runtime unified into one incrementally-served data plane, TLS termination, binary wire format, bounded session state |
| 16 | Ingestion failure containment: a durable, bounded, inspectable, replayable connector quarantine |
| 17 | Production readiness and 1.0 finalization: the operator CLI + arrangement debugger, freshness explainability, mTLS/secrets/security review, rolling-upgrade + disaster-recovery proof, the written v1 contract, the production failure matrix, the v1.0 release candidate |

## Crate Architecture

The project is a Cargo workspace of purpose-built crates:

| Crate | Purpose |
|---|---|
| `rockstream-types` | Shared types: timestamps, frontiers, Z-set rows, schemas |
| `rockstream-storage` | SlateDB wrappers, key encoders, merge operator registry, checkpoint helpers |
| `rockstream-plan` | `PlanNode` IR and physical `OpNode` graph |
| `rockstream-diff` | `DiffCtx` differentiation pass — turns SQL plans into incremental delta plans |
| `rockstream-ops` | `Operator` trait and per-operator implementations |
| `rockstream-sql` | SQL frontend built on DataFusion |
| `rockstream-runtime` | Worker process, circuit executor, async scheduler, exchange subsystem |
| `rockstream-control` | Control-plane service (topology, shard leasing, placement) |
| `rockstream-gateway` | Postgres wire protocol gateway |
| `rockstream-connectors` | Connector implementations; removed connector frontends remain compiled for catalog compatibility. |
| `rockstream-oracle` | Batch reference engine and property-test harness (DBSP soundness tests) |
| `rockstream-sim` | Deterministic simulation harness: `SimRuntime`, `buggify!()`, fault model |
| `rockstream-cli` | Operator CLI — `rockstream start` today; `shard migrate`/`cluster workers drain` land at v0.46, `explain`/`resource` at v0.45, the workload quota/admission-control substrate at v0.45.1, control-plane Raft HA at v0.45.2, and the rest (`workload`/`view`/`schema`/`source`/`checkpoint`/`audit`/`support bundle`/`debug arrangement`) at v0.53 |
## How Do I Know It’s Working?

The system exposes one primary health indicator per pipeline: **SLO compliance** —
the fraction of the recent window for which your declared freshness target was met.

- **SLO compliance = 1.0**: pipeline is healthy.
- **SLO compliance < 1.0**: a `degraded_reason` label says *why* in plain terms
  (`BACKFILLING`, `RECOVERING`, `OVER_BUDGET_RELAXED`, `RPS_THROTTLED`,
  `BLOCKED`, etc.) so you know whether to wait, add capacity, raise a quota,
  or fix a connector.
- **`rockstream explain <view>`**: shows the operator tree with per-operator
  diagnostics and what the auto-tuner is currently doing.
- **`rockstream explain <view> --estimate`**: previews cost, state size, and
  achievable freshness *before* you deploy a new view.
- **Freshness tokens**: query responses can include the source frontier they
  observed; clients that need read-your-writes behavior can ask the gateway to
  wait until that frontier is visible.
- **`rockstream support bundle --pipeline=foo`**: collects everything needed to
  debug an issue in one command.
- **`rockstream audit tail`**: shows every control-plane action — every scale
  decision, every degraded-state transition, every pipeline change — with the
  metric reading that triggered it. No silent changes.

A Grafana dashboard template ships with the project at
`deploy/dashboards/rockstream-overview.json`. The above-the-fold panel is a single
number per pipeline: SLO compliance over time.

## One Binary, One Config, Three Tiers

There is one `rockstream` binary. Node roles are flags, not separate executables.

- **Laptop / evaluation**: `rockstream start --storage=./data` — zero config; survives crashes.
- **Single host (small production)**: `rockstream start --role=all --storage=s3://bucket/...`.
- **Multi-host cluster**: `--role=control` on control nodes, `--role=worker` on workers.

Moving up the ladder is additive: the same data files produced at Tier 1 against MinIO
open at Tier 3 against S3. There is no data migration step because there is no
node-local state to migrate.

At every tier, you connect with `psql` (or any Postgres client). The development loop
for a new view is: start the binary, open `psql`, write the SQL, insert a few rows,
watch the view update in real time — the same loop you would use with a regular
database, except the view stays current as data keeps arriving.

## Contributing

Contributions, feedback, and questions are welcome. Open an issue or start a discussion!

## License

Apache 2.0

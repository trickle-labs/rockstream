# RockStream Focused Roadmap

This roadmap turns [NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md)
into an ordered, evidence-producing build sequence. It complements:

- [DESIGN.md](DESIGN.md) — what RockStream is and why the architecture works.
- [IVM.md](IVM.md) — how the incremental view maintenance engine works.
- [NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) — the focused,
  two-pillar engineering plan this roadmap implements.

It is deliberately narrow. The only goals are the **cloud-native IVM engine**
and the **PostgreSQL wire access layer**. Everything outside those two pillars
is out of scope (see the plan's *Out of Scope* section).

Each version below is sized at about **6 person-weeks** of implementation
effort. That can mean one person for six weeks, two people for three weeks, or
any other mix. The version number is a planning unit, not a release-quality
promise: a version is done only when its proof is done.

Versions are strictly ordered. Each builds on the one before it; nothing in a
later version may be started until its predecessor's proof is complete.

---

## Roadmap Philosophy

1. **Evidence over dates.** A version ends when its tests, benchmarks,
   simulations, and docs prove the new capability works.
2. **Correctness before scale; scale before features.** The single-shard engine
   is proven correct (through v0.14) before any distribution. The distributed
   engine is fault-tolerant (through v0.22) before the Postgres layer is built
   on it.
3. **Simulation from the beginning.** `SimRuntime` and `buggify!()` are
   foundation work, established in v0.2 and used by every later version.
4. **The oracle is the source of truth.** `incremental(q, Δ) == batch(q, accumulated)`
   is asserted for every operator and query against a DataFusion batch reference.
5. **One binary, one CLI, one config.** Every role is a flag on the same
   `rockstream` binary; `main` remains runnable through it at every version.
6. **Thin vertical slices.** Each version leaves a human able to do something
   real, or leaves the project with stronger proof that a hard thing is safe.
7. **Split before rushing.** If a version cannot fit in ~6 person-weeks, split
   it. The roadmap is allowed to grow.

---

## Testing Conventions (Binding for Every Version)

All automated tests in this roadmap fall into exactly one of three categories.
No other test infrastructure is permitted.

1. **Unit tests** — pure, in-process `cargo test`. Use the in-memory object
   store for storage-touching logic that does not need durability semantics.
2. **SlateDB local-filesystem backend tests** — exercise `ShardDb` and any
   durability/WAL/checkpoint/compaction path against a real SlateDB instance
   backed by the **local filesystem** object store. These prove on-disk
   correctness without a container.
3. **SlateDB S3 (MinIO) backend tests** — exercise the same paths against a
   real S3-compatible object store provided by **MinIO**. Required for anything
   that depends on S3 semantics: list/get/put latency, conditional writes,
   multipart, retries, brownout, and coalesced shuffle objects.

**Integration tests always use [TestContainers].** Any test that needs a real
external process — MinIO, Postgres (as a CDC source, a sink, or a pgwire client
target via `psql`/`tokio-postgres`), or Kafka — provisions it through
TestContainers. There are no hand-managed external services and no shared test
infrastructure; every integration test brings up and tears down its own
containers.

**Per-version rules** (in addition to the Common Definition of Done below):

- Every new operator ships an **oracle property test** (`incremental == batch`)
  before it is considered complete.
- Every new durability path (commit, replay, checkpoint, compaction, WAL) ships
  **both** a local-filesystem-backend test **and** a MinIO-backend test.
- Every new distributed-coordination path ships at least one seeded `SimRuntime`
  test; a fault found by simulation is checked in as a permanent regression seed.
- No code path may depend on SlateDB range deletion; a test asserts this.

[TestContainers]: https://testcontainers.com/

---

## Common Definition of Done

Every version must satisfy this baseline before it can be marked done:

- `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test --workspace` pass.
- New behavior has unit tests plus, depending on risk, an oracle/property test,
  a local-filesystem-backend test, a MinIO-backend test, or a TestContainers
  integration test — per the Testing Conventions above.
- Any user-visible or operator-visible failure has an `RS-XXXX` error code with
  actionable `next_steps` text.
- Any control-plane action writes an audit event.
- Any new performance claim has a `criterion` benchmark or measurement note; CI
  fails on >10% regression once a baseline exists.
- Any new public surface (SQL syntax, CLI command, config key, system table) is
  documented in `docs/`.
- Any new queue, buffer, or scan window has a named upper bound, a fill-level
  metric, and a backpressure or error path. Unbounded in-memory accumulation is
  never acceptable.
- Any new distributed-coordination path has at least one seeded `SimRuntime`
  test before it is done.
- `main` remains runnable through the single `rockstream` binary.
- A sign-off file `sign-offs/vX.Y.md` exists with all checklist items marked.

Long soaks are gates, not loopholes. A version that needs a 24-hour or multi-day
run still fits the 6-person-week budget, but is not accepted until the soak is
clean.

---

## Public Milestones

These names orient readers; they are not calendar commitments.

| Milestone | Version | Meaning |
|---|---:|---|
| Foundation Ready | v0.3 | Workspace, runtime abstraction, simulation, oracle harness, and the SlateDB determinism gate all pass. |
| Single-Shard Alpha | v0.6 | A local engine maintains filter/aggregate/MIN-MAX views and survives crash-replay. |
| SQL Engine | v0.10 | Plain-SQL views with joins and set operations maintained incrementally on one shard. |
| IVM Correct (Single-Shard) | v0.14 | TPC-H 22/22 incremental == batch; the engine is feature-complete and correct on one shard. |
| Distributed Engine | v0.17 | Multi-shard execution with exchange; distributed output bit-identical to single-shard. |
| Progress-Tracked | v0.19 | Frontier protocol correct across multi-input operators; bounded shuffle storage. |
| Fault-Tolerant | v0.22 | Exactly-once end-to-end; 24h chaos with zero loss/duplicates; recovery SLOs met. |
| Postgres Pillar | v0.26 | Read, write, subscribe, and read-your-writes over the Postgres wire protocol. |
| Production Ready | v0.30 | Minimal connectors, observability, security, rolling upgrades; surface frozen for 1.0. |

---

## Version Roadmap

Each row is about 6 person-weeks. The **Proof** column is the binding part:
without that proof, the version is not done. The **Backends** column names the
required test backends beyond plain unit tests (LFS = SlateDB local-filesystem
backend; MinIO = SlateDB S3 backend via TestContainers; TC = TestContainers
integration with another process).

### Phase 0 — Foundation

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.1 | Workspace and CI | Cargo workspace with the focused crate set (`rockstream-types`, `-storage`, `-sim`, `-oracle`, `-plan`, `-diff`, `-ops`, `-sql`, `-runtime`, `-control`, `-gateway`, `-connectors`, `-cli`); CI (`fmt`, `clippy -D warnings`, `test`, `deny`, coverage); `rockstream-errors` registry with CI enforcement; hot-path metrics emitter; dev container with SlateDB/MinIO/Postgres. | Clean CI on a no-op binary; `rockstream --help` works; CI fails the build on any returned `Error` or logged `error!` without an `RS-XXXX` code; no hidden local setup step. | Unit |
| v0.2 | Runtime abstraction, simulation, and oracle | `rockstream-sim`: `Runtime` trait (`now`/`spawn`/`sleep`/`object_store`/`network`), `TokioRuntime`, in-memory seeded `SimRuntime`, `buggify!()` macro, fault-model registry, paired-assertion helper. `rockstream-oracle`: DataFusion batch reference + `proptest` harness asserting `incremental == batch`. | A deterministic test replays the same seed byte-for-byte; a different seed changes event order; production build compiles `buggify!()` as a no-op; the oracle harness confirms equivalence on a trivial no-op pipeline. | Unit |
| v0.3 | SlateDB storage contract and determinism gate | `rockstream-storage` (`ShardDb`): key encoders with `namespace_id` in all catalog keys; `WriteBatch` builders; `DbReader` snapshot reads; WAL tail reader with listing cache; checkpoint helpers; scan-and-delete cleanup. No range deletion anywhere. `rockstream start --role=all --storage=./data` runs a no-op pipeline. | Storage API validation suite proves only supported SlateDB features are used. **SlateDB determinism gate**: a write-heavy `ShardDb` workload run twice at the same seed produces bit-identical key-value state and WAL sequence; any non-deterministic SlateDB background surface is found and constrained before v0.4. `make e2e` brings up MinIO + 1 worker + 1 control and tears it down. | LFS, MinIO, TC |

### Phase 1 — Single-Shard IVM Core

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.4 | Filter / project / map (IVM-1) | Z-set types `(row, weight: i64)`; `PlanNode` (`Source`/`Filter`/`Project`/`Map`/`ViewSink`/`Exchange` stub); `DiffCtx` linear-operator rules; `Operator` trait + `EpochOutput`; `OperatorTask` event loop; async ownership-free scheduler with credit backpressure; embedded runtime profile (in-process control/worker/gateway, no gRPC, no shuffle objects); built-in `GENERATE ROWS` source and a `Vec<RecordBatch>` delta source. | Oracle property test for `SELECT a, b*2 AS c FROM t WHERE c > 10` over random insert/delete sequences passes ≥100k scenarios; embedded hot path issues zero gRPC calls and creates zero shuffle objects. | Unit, LFS |
| v0.5 | Algebraic aggregates and epoch commit (IVM-2) | `Aggregate` PlanNode + aggregate arrangement; `diff_aggregate` (group by key, merge `(Δsum, Δcount)`, emit `(old,-1) ⊎ (new,+1)`) for SUM/COUNT/AVG/COUNT(*); shard-level group commit coalescing fragments into one atomic `WriteBatch`; per-shard namespaces (`op_state`/`view_output`/`shard_meta`); persisted frontier. | Oracle property test for `SELECT k, SUM(v), COUNT(*), AVG(v) FROM t GROUP BY k` over random insert/update/delete + group churn; group commit reduces durability events ≥5× vs. one commit per operator (measured on LFS and MinIO). | Unit, LFS, MinIO |
| v0.6 | MIN / MAX and crash-replay (IVM-3) | Indexed-multiset arrangement + cached extremum; `diff_minmax` (insert merges multiset and updates cache; delete of extremum prefix-scans for the replacement); WAL listing cache validated on the hot path. **Single-Shard Alpha.** | Oracle property test for groups churning across MIN/MAX transitions; cached extremum matches the multiset's true extremum after every batch. **Crash-replay**: `kill -9` injected mid-`WriteBatch`; on restart the shard replays from its persisted frontier to bit-identical output (verified on LFS and MinIO). | Unit, LFS, MinIO |

### Phase 2 — SQL Frontend & Joins

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.7 | SQL frontend and catalog | `rockstream-sql`: DataFusion parse/bind/optimize; custom extension nodes (`IncAggregate`/`IncJoin`/`IncDistinct`); lowering `LogicalPlan → PlanNode`; distribution pass annotating `partition_key` and inserting `Exchange` no-ops; schema-version catalog in `control: schema/` (compatible changes online, breaking → `RS-1002`); `CREATE VIEW`; `EXPLAIN INCREMENTAL` and `EXPLAIN INCREMENTAL ESTIMATE`. | SQL and hard-coded PlanIR produce identical physical plans for the Phase 1 operators; plans round-trip through catalog storage; an incompatible schema change returns `RS-1002`; `EXPLAIN INCREMENTAL ESTIMATE` reports predicted state size and per-operator `epoch_ms` without deploying. | Unit, LFS |
| v0.8 | Inner equi-join (IVM-4) | `InnerJoin` PlanNode + dual arrangements; stable source-derived `row_id` so replay rewrites the same key; DBSP-native two-arrangement join with correct bilinear expansion (`ΔL⋈R` insert/delete split, `L₀⋈ΔR`, correction term); arrangements reflect epoch `e-1` during processing, updated at commit. | Oracle property test for random 3-way joins; TPC-H Q1/Q3/Q5/Q6 plan-level parity; crash-replay of a join view restores bit-identical arrangement state. | Unit, LFS |
| v0.9 | Outer / semi / anti joins (IVM-5) | `LeftJoin`/`RightJoin`/`FullJoin`/`SemiJoin`/`AntiJoin`; one extra arrangement per side tracking unmatched rows so transitions emit the right NULL-padding retractions. | TPC-H Q11/Q21 (SemiJoin corner cases) and randomized NULL-heavy join tests match the batch oracle. | Unit, LFS |
| v0.10 | Distinct / set ops and storage budget gate (IVM-6) | Weight-based distinct arrangement; zero-crossing emission (`0→+n ⇒ +1`, `+n→0 ⇒ −1`); Intersect/Except with set and bag semantics. **Storage Operational Budget gate** (DESIGN.md §5.4): prove PUT/GET/LIST p99 at 1 GB and 5 GB, manifest cadence, WAL listing-cache hit ratio >99%, and write amplification against MinIO. **SQL Engine** milestone. | Oracle property tests on set semantics; `CREATE VIEW v AS SELECT … JOIN … GROUP BY …` compiles, deploys, and maintains incrementally end-to-end; storage budgets hold against MinIO (any budget exceeded by >2× requires a tracked mitigation before v0.11). | Unit, LFS, MinIO |

### Phase 3 — Essential Operators & Single-Shard Correctness Soak

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.11 | Window functions (IVM-7) | `Window` PlanNode + ordered arrangement; partition-based recomputation (re-evaluate a changed partition, diff against previously-emitted output); ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, sliding SUM/AVG; `EXPLAIN INCREMENTAL` flags oversized partitions. | Window-heavy randomized tests match the batch oracle; partition-recompute cost is measured and documented; oversized-partition NOTICE fires in `EXPLAIN`. | Unit, LFS |
| v0.12 | Tumbling time windows and Top-K (IVM-8, IVM-9) | TUMBLE windows keyed by `window_id` with event-time TTL and a frontier-aware compaction filter that removes state only after event-time expiry (HOP/SESSION deferred); Top-K value-descending arrangement keeping `K+ε` entries with delete-refill and displacement deltas. | Late-data and TTL tests prove no visible state is removed early (verified on LFS and MinIO); Top-K random insert/update/delete tests match batch; delete from the current top-K refills correctly. | Unit, LFS, MinIO |
| v0.13 | Bootstrap and view-on-view DAG (IVM-10, IVM-11) | Snapshot-mode sources emitting each base-table row once at weight `+1` (single or streamed bootstrap epochs); bootstrap frontier advances only when every chunk is ingested; `ViewRef` PlanNode tailing an upstream view's `view_output/` CDC via `WalReader`; structural diamond consistency; compile-time cycle detection (Kahn). | 100M-row-equivalent snapshot view matches the batch result; a mid-bootstrap connector restart neither duplicates nor skips rows; a 5-level view-on-view DAG and a diamond topology converge under continuous input; a cycle is rejected at compile time. | Unit, LFS, MinIO |
| v0.14 | Single-shard correctness soak | TPC-H 22/22 (SF=0.01) incremental; random SQL fuzzer over a synthetic schema; DST harness; storage-correctness audit (no range-delete dependency, snapshot-safe compaction filters); performance regression suite in CI. **IVM Correct (Single-Shard)** milestone. | 22/22 TPC-H queries return bit-identical results vs. DataFusion batch; ≥10× speedup vs. batch at a 1% change rate; the fuzzer runs ≥1 hour without divergence; the DST harness passes 100k seeds bit-identically; every cleanup path proven without range deletion. | Unit, LFS, MinIO |

### Phase 4 — Multi-Shard Execution & Exchange

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.15 | Control plane, leasing, and fencing | `rockstream-control` service; worker registration via `--control=<url>`; topology catalog; shard manager (a worker owns N shards, each with its own `Arc<Db>`); lease acquisition; SlateDB fence-epoch enforcement; role flags (`--role=control|worker|gateway|all`). | A two-writer fence test proves only one writer can commit to a shard; killing a worker releases its leases and they are reassigned cleanly; topology changes are audited. | Unit, LFS, TC |
| v0.16 | Direct exchange | gRPC shuffle service (`proto/shuffle.proto`); path classifier (`elided`/`loopback`/`direct`/`durable`); worker-to-worker stream multiplexing (one stream per peer per traffic class); same-worker loopback over bounded in-process channels with durable outbox/inbox metadata; Arrow serialization; credit-based backpressure. | A 16-shard cluster runs a partitioned TPC-H subset with connection count bounded by worker count, not shard count; the loopback path produces output identical to direct gRPC with zero network calls for co-located exchanges. | Unit, LFS, TC |
| v0.17 | Durable shuffle fallback and distributed parity | Object-store fallback writer/reader; coalesced durable shuffle objects with an index footer (no LIST on the hot path); rendezvous hashing with virtual nodes and re-balance-minimality property tests; full Phase 1–3 oracle + TPC-H suite re-run against the distributed cluster. **Distributed Engine** milestone. | Distributed results are **bit-identical** to single-shard runs across the entire oracle/TPC-H suite; an injected receiver failure forces durable fallback and the receiver catches up with no duplicates; a 1,000-shard exchange stress test stays within connection and shuffle-object budgets against MinIO. | Unit, LFS, MinIO, TC |

### Phase 5 — Frontier Protocol & Progress Tracking

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.18 | Frontier protocol | `rockstream-types::Frontier` antichain with product-order timestamps and meet/join/advance property tests; per-shard frontier reporter bundled into every epoch commit; control-plane frontier aggregator consuming per-shard summaries and publishing the per-operator cluster frontier; separable `--role=frontier`. | `SimRuntime`: arbitrary frontier-report reorderings converge to the same cluster vector frontier as serial delivery; a frontier-aggregation stress test over thousands of shards × hundreds of operators converges without the control plane subscribing to each shard feed. | Unit, LFS, TC |
| v0.19 | Frontier consumers and exchange GC | Operator frontier consumers using the input frontier to close windows, detect convergence, and release shuffle inbox entries; exchange GC reclaiming outbox/inbox entries via bounded scan-and-delete or snapshot-safe compaction filters; control-plane-derived `min_epoch_ms`/`max_epoch_ms`/initial parallelism from the declared freshness target (simple, bounded). **Progress-Tracked** milestone. | A join over two sources at different ingestion rates produces correct output with no premature emission and no infinite buffering; shuffle storage usage stays bounded under sustained throughput (measured against MinIO). | Unit, LFS, MinIO, TC |

### Phase 6 — Fault Tolerance & Exactly-Once

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.20 | Cluster checkpoints and recovery | Cluster checkpoint coordinator (barrier injection at sources; bounded barrier alignment tied to shuffle credits; one per-shard `Checkpoint` after all local operators commit through the barrier; atomic cluster-checkpoint commit; old-checkpoint GC); recovery driver bringing up every shard via `DbReader` pinned to its per-shard checkpoint, then re-electing writers. | Checkpointing under slow input and credit exhaustion never grows unbounded and either succeeds or reports `RECOVERING`; recovery from a cluster checkpoint reproduces pre-failure state bit-identically (verified on LFS and MinIO). | Unit, LFS, MinIO, TC |
| v0.21 | Exactly-once sinks and resilience | Exactly-once sink protocol (`pre_commit`/`commit`); transactional Kafka sink; object-store sink via `_pending/` → atomic rename; per-connector source-epoch with persisted partition→offset map recorded in the epoch commit; worker self-fencing on control-plane partition (DESIGN.md §11.6); object-store brownout buffering + backpressure (DESIGN.md §11.7). | `SimRuntime`: 2PC sink crash points (pre-commit/between/commit) all recover idempotently; a network-partitioned worker fences before the new owner commits; a 50-epoch object-store blackout produces zero loss and zero duplicates; the Kafka sink delivers exactly once under injected broker faults. | Unit, LFS, MinIO, TC |
| v0.22 | Chaos and recovery SLO gate | Chaos suite (random process kills, partitions, disk-full, object-store throttling) with output compared against a non-faulty reference; continuous simulation soak CI job running new seeds against `main`; recovery-time instrumentation. **Fault-Tolerant** milestone. | A 24-hour chaos run on a 32-shard cluster has zero data loss and zero duplicates with output matching reference; recovery from full outage in <60 s for state <1 TB; at `target_shard_state_bytes` (20 GB): failure detection ≤5 s p99, shard reassignment ≤30 s p99, freshness recovery ≤60 s p99; ≥100k seeded `SimRuntime` runs pass and the soak job has its first regression-seed corpus. | Unit, LFS, MinIO, TC |

### Phase 7 — PostgreSQL Wire Gateway

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.23 | Postgres read gateway (7.1) | `rockstream-gateway`: pgwire startup / simple query / extended query / copy-out / terminate; `ViewReader` trait with `ViewReadStrategy` (`HotOnly` implemented; `TwoTier` reserved); multi-shard `DbReader` reads pinned to one published vector frontier; catalog stubs (`pg_catalog.pg_tables`/`pg_views`/`pg_class`/`pg_attribute`/`pg_namespace`/`pg_type`, `information_schema.tables`/`.columns`, `SHOW`/`SET`); native Postgres type OIDs in row descriptions; isolation levels (`READ COMMITTED`, `REPEATABLE READ`, `SERIALIZABLE` → `RS-2003`); inline views with cycle detection (`RS-1011`). | `psql` connects and `SELECT * FROM my_view LIMIT 10` returns <10 ms p99 for a local cluster; an ORM (e.g. SQLAlchemy via TestContainers) reflects view schemas without error; `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` returns `RS-2003`; `CREATE MATERIALIZED VIEW mv AS SELECT * FROM v` inlines `v` and starts IVM. | Unit, LFS, TC |
| v0.24 | Direct-write DML (7.2) | Internal direct-write source connector (`INSERT`/`UPDATE`/`DELETE` buffered per connection; `COMMIT` flushes an atomic Z-set delta via `WriteBatch` to a base-table shard and receives the next `source_epoch`; `ROLLBACK` discards); `INSERT … RETURNING` including the multi-row `INSERT … SELECT … RETURNING` form; idempotency enforcement (exactly-once source-epoch envelope or caller key; missing both → `RS-2007`) with a per-shard, time-bounded idempotency-key table. | `psql` runs `INSERT INTO t VALUES (…); COMMIT` and the view reflects it within the freshness target; a non-idempotent write missing both an envelope and a key returns `RS-2007`; idempotent replay of a committed write is a no-op (verified on LFS and MinIO). | Unit, LFS, MinIO, TC |
| v0.25 | Subscribe and read-your-writes (7.3) | Subscribe endpoint tailing view changes via `WalReader`, gateway-proxied (`mz_timestamp`, `mz_diff`, projected columns; `AS OF NOW WITH SNAPSHOT`, `AS OF EPOCH <n>` within `CHANGE_RETENTION` default 1 h; server-side `WHERE`; column projection); freshness tokens returned on query responses; `wait_for=<token>` read-your-writes with timeout and explicit satisfied/not-satisfied; session-scoped automatic read-your-writes after `COMMIT`. | Read-your-writes demo passes and `wait_for=<token>` resolves within the SLO; a subscribe stream survives a gateway restart with no gaps or duplicates; `SUBSCRIBE orders_mv AS OF NOW WITH SNAPSHOT` delivers current state then live deltas; `WHERE`/projection reduce traffic to matching rows/columns; `AS OF EPOCH` outside retention returns `RS-2005`. | Unit, LFS, TC |
| v0.26 | Auth, RBAC, and read pushdown (7.4) | OIDC/bearer-token and mTLS auth at the gateway (`--auth=off` for local dev); per-view RBAC (`viewer`/`pipeline_owner`/`admin`) in the control-plane catalog; namespace isolation; cross-shard partial aggregation pushdown (DESIGN.md §12.3.1) so `SELECT agg, key FROM mv GROUP BY key` returns O(distinct groups × shards) rows. **Postgres Pillar** milestone. | An application using a standard Postgres driver creates a view, writes rows, reads them back with read-your-writes, and subscribes to changes end-to-end against the distributed engine; unauthenticated requests are rejected; cross-namespace access is denied; the audit log records `actor` on every control action; `EXPLAIN` names the pushdown effect. | Unit, LFS, TC |

### Phase 8 — Minimal Connectors & Production Hardening

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.27 | Kafka and Postgres CDC connectors | Kafka source (consumer-group; offsets in the control plane) and Postgres logical-replication source (`pgoutput`); the §13.3 connector contract (`discover_schema`/`start_snapshot`/`poll_delta`/`commit_offset`/`prepare`/`commit`/`abort`/`should_flush`, opaque `OffsetToken`, `watermark`, `credits_available()`); dead-letter queue (`RS-1003` events → DLQ sink; `rockstream_catalog.dead_letter_queue` with replay/dismiss). | An end-to-end Postgres CDC → IVM → Kafka pipeline runs against TestContainers (Postgres + Kafka + MinIO); a Kafka source closes a tumbling window correctly under deliberate clock skew; under downstream saturation, consumption tracks credits with bounded inbox memory; decode errors land in the DLQ and `REPLAY` re-processes them after a fix. | Unit, LFS, MinIO, TC |
| v0.28 | Object-store sink, schema evolution, observability | Object-store/Parquet sink (idempotent rename); connector schema-version publication with incompatible-drift block (`RS-1002`) before any offset advances; Prometheus metrics (per-operator throughput/latency/state, shuffle bytes, frontier lag, checkpoint duration); OpenTelemetry per-epoch and source-to-sink traces; admin CLI (`pipeline`, `cluster status/scale`, `cluster workers`, `shard`, `checkpoint`, `debug arrangement`). | A schema-incompatible upstream change blocks consumption with `RS-1002` before any offset advances; the Parquet sink writes idempotently under crash-mid-flush (verified on MinIO); metrics and traces are emitted and scrapeable in a TestContainers integration run. | Unit, LFS, MinIO, TC |
| v0.29 | Security and rolling upgrades | mTLS everywhere (worker↔control, worker↔worker, gateway↔client) with documented rotation; at-rest encryption via object-store features; `CREATE SECRET` with envelope encryption and worker-side resolution; storage-format version gate on shard open (`RS-5001`); N→N+1 rolling upgrade one worker at a time; full `RS-XXXX` doc pages with `next_steps` (CI-enforced); simulation CI gate (safety + liveness). | Unauthenticated/cross-tenant requests are rejected with `actor` audited; a rolling N→N+1 upgrade loses no epochs and the format-version gate fires on an incompatible binary; every registered error code has a published doc page with non-empty `next_steps`. | Unit, LFS, MinIO, TC |
| v0.30 | Production soak and surface freeze | 24-hour end-to-end exactly-once soak (Postgres CDC → IVM → Kafka at 100k rows/s); multi-day availability run on a 32–64-shard cluster; disaster-recovery runbook executed; independent security review; public-surface audit and classification (SQL subset, CLI, system tables, metrics, error codes) with the stable surface frozen and minimal. **Production Ready** milestone → 1.0 gate. | The 24-hour E2E soak shows zero loss and zero duplicates; ≥99.9% availability over the multi-day soak; the DR procedure completes successfully; the security review passes; the stable public surface is frozen, classified, and documented. | Unit, LFS, MinIO, TC |

---

## How This Roadmap Maps to the Plan

| Plan phase | Roadmap versions |
|---|---|
| Phase 0 — Foundation | v0.1 – v0.3 |
| Phase 1 — Single-Shard IVM Core | v0.4 – v0.6 |
| Phase 2 — SQL Frontend & Joins | v0.7 – v0.10 |
| Phase 3 — Essential Operators & Soak | v0.11 – v0.14 |
| Phase 4 — Multi-Shard & Exchange | v0.15 – v0.17 |
| Phase 5 — Frontier Protocol | v0.18 – v0.19 |
| Phase 6 — Fault Tolerance & Exactly-Once | v0.20 – v0.22 |
| Phase 7 — PostgreSQL Wire Gateway | v0.23 – v0.26 |
| Phase 8 — Minimal Connectors & Hardening | v0.27 – v0.30 |

Thirty versions at ~6 person-weeks each is the full path from an empty
repository to a production-ready, two-pillar 1.0. The order is fixed:
correctness on one shard is proven before distribution, distribution is made
fault-tolerant before the Postgres layer depends on it, and only a minimal,
fully-tested connector set and hardening pass close out 1.0.

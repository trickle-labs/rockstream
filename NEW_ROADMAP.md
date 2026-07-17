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
- Every new distributed-coordination *protocol* (from v0.18 onward) ships a green
  FizzBee model (`formal/*.fizz`) with its safety, liveness, and `exists`-coverage
  assertions, plus the paired runtime `assert!`, before it is done
  ([FIZZBEE_TEST_PLAN.md](FIZZBEE_TEST_PLAN.md) §3.6, §3.7).
- No code path may depend on SlateDB range deletion; a test asserts this.

**Formal verification (binding from v0.18).** Distributed-coordination protocols
are model-checked in [FizzBee] **before** their Rust implementation, per
[FIZZBEE_TEST_PLAN.md](FIZZBEE_TEST_PLAN.md). FizzBee is not a fourth test
backend — it is a design-time correctness oracle that sits one level above
`SimRuntime`. The contract is explicit: every safety/liveness invariant proved
in a `.fizz` model becomes a paired runtime `assert!` in the implementation
([FIZZBEE_TEST_PLAN.md](FIZZBEE_TEST_PLAN.md) §3.7), and every FizzBee
counterexample is archived in `formal/findings.md` and translated into a
permanent `SimRuntime` regression seed before the model is fixed. The
`formal-verify` CI job runs the full spec suite on every PR that touches a
coordination crate (`rockstream-runtime`, `-control`, `-connectors`,
`-storage`) or `DESIGN.md`; a red model blocks merge. The full model→version
mapping is the *Formal Verification Track (FizzBee)* section below.

[FizzBee]: https://fizzbee.io
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
- Any change to a distributed-coordination protocol (epoch commit, frontier
  aggregation, 2PC sink, lease/fencing) has a green FizzBee model and its paired
  runtime `assert!` ([FIZZBEE_TEST_PLAN.md](FIZZBEE_TEST_PLAN.md) §3.7); from
  v0.18 onward, `make verify` is part of the gate.
- `main` remains runnable through the single `rockstream` binary.
- A sign-off file `sign-offs/vX.Y.md` exists with all checklist items marked.

Long soaks are gates, not loopholes. A version that needs a 24-hour or multi-day
run still fits the 6-person-week budget, but is not accepted until the soak is
clean.

Waivers are dated debts, not exemptions. Any `Simulation-compensated waiver`
(e.g. NEW_IMPLEMENTATION_PLAN.md Phase 4's real-network-test waiver) must name
the exact version by which the waived real-world proof will run.
`scripts/check-exit-criteria.sh` fails the build if that version — or any
later version — is marked `✅ Done` while the waived proof is still absent from
its sign-off, so an overdue waiver blocks new roadmap progress instead of
silently persisting for multiple phases (found overdue by fifteen versions for
the Phase 4 network waiver — see the Dependency-integrity review note in
Phase 13 below — before this rule existed).

---

## Public Milestones

These names orient readers; they are not calendar commitments.

| Milestone | Version | Meaning |
|---|---:|---|
| Foundation Ready | v0.3 | Workspace, runtime abstraction, simulation, oracle harness, and the SlateDB determinism gate all pass. |
| Single-Shard Alpha ✅ Done | v0.6 | A local engine maintains filter/aggregate/MIN-MAX views and survives crash-replay. |
| SQL Engine (Phase 2 entry) | v0.10 | Plain-SQL views with joins and set operations maintained incrementally on one shard. |
| IVM Correct (Single-Shard) | v0.14 | TPC-H 22/22 incremental == batch; the engine is feature-complete and correct on one shard. |
| Distributed Engine | v0.17 | Multi-shard execution with exchange; distributed output bit-identical to single-shard. |
| Progress-Tracked | v0.19 | Frontier protocol correct across multi-input operators; bounded shuffle storage; FizzBee M2 frontier-aggregation model green. |
| Fault-Tolerant ✅ Done¹ | v0.22 | Exactly-once end-to-end; 24h chaos with zero loss/duplicates; recovery SLOs met; all four FizzBee models (M1–M4) green. |
| Postgres Pillar ✅ Done | v0.26 | Read, write, subscribe, and read-your-writes over the Postgres wire protocol. |
| Soaks Complete ✅ Done | v0.31 | Ingestion connectors live; failure-detection, shard-reassignment, and freshness-recovery SLOs validated under real cloud pressure. |
| Private Beta Ready ✅ Done | v0.32 | Secondary indexes enable single-digit-ms point lookups on non-primary keys; open for early-adopter onboarding. |
| Nexmark Correctness Complete | v0.36 | Nexmark q0–q9 and q12–q22 bit-identical to DataFusion batch oracle; retraction/UPDATE correctness proven via Z-set INSERT+UPDATE+DELETE sequences through all Nexmark views. (q10 Parquet sink: Phase 12; q11 session windows: v0.50.) |
| Wire Protocol Complete | v0.39 | Extended query protocol, full Postgres type OID coverage, ORM driver compatibility (SQLAlchemy, Prisma, Hibernate), PgBouncer transaction-mode pooling, protocol fuzzing, and concurrent-connection stress; any standard Postgres driver works without workarounds. |
| Wire Protocol End-User Complete² | v0.42 | SCRAM-SHA-256/MD5 password auth, driver session bootstrap, full transaction/savepoint state machine, LISTEN/NOTIFY, and a green driver-compatibility matrix (psql, psycopg3, tokio-postgres, pgx, PgJDBC, node-postgres, SQLAlchemy, Prisma); a reference application runs end-to-end over pgwire unmodified, with a ≥90% gateway coverage gate. |
| Elastically Scalable ✅ Done | v0.47 | Hot keys split into virtual buckets and shards split before they get too big, entirely in the background; the cluster exports `cluster_worker_pressure` so a standard Kubernetes HPA/KEDA can drive worker count — no manual re-sharding, no touching mechanism knobs. |
| Operationally Complete | v0.57 | The full operator CLI surface (workload/view/schema/source/cluster/resource lifecycle, the IVM arrangement debugger), internal mTLS, secrets management, an independent security review, a proven rolling-upgrade path, and a rehearsed disaster-recovery drill are all done; an operator can run and heal a cluster using only documented commands. |
| v1.0 Release | v0.59 | All v0.1–v0.58 features integrated; 2-week continuous chaos cycle passes with zero P0/P1 bugs; `v1.0.0` tagged. |

¹ **Re-opened by the <=v0.42 implementation review (2026-07-10).** "All four FizzBee models green" was never actually true: the `formal-verify` CI job has never successfully installed the `fizzbee` binary (wrong release-asset filename), never runs on pull requests (its `if:` guard tests a non-existent field), and carries `continue-on-error: true`, so nothing has ever blocked a merge on a red or crashing model. Running the models directly for the first time found M1 failing its `M1_S5_IdempotentReplay` invariant and M3/M4 crashing outright (undefined-variable Starlark errors) — only M2 has ever genuinely passed. The distributed-engine Rust implementation and its `SimRuntime`/chaos test suite are unaffected by this finding; only the *formal-verification* proof of v0.18–v0.22 is unverified. **Update (v0.42.1, same day):** the CI toolchain, M1, and M3 are now fixed and genuinely green (M1: 72 states; M3: 1,168 states; both safety+liveness); M2 remains genuinely green (251,889 states); M4's crash and a real self-fencing race are fixed, but exhaustive verification of M4 does not yet terminate in reasonable time, so M4 stays non-blocking pending **v0.42.3**. **Update (v0.42.1, same day, CI-robustness follow-up):** the very first real CI run of this fix showed the M4 step itself get a `cancelled` conclusion (consistent with the runner's OOM killer acting on an exploding process), which `continue-on-error` does not suppress and which flipped the whole hard-gate job to `failure` — the "M4 is non-blocking" contract was not actually true in a real run. Fixed by running M4 under a per-subshell `ulimit -v` memory cap plus a wall-clock `timeout`, swallowing its exit code into a step output instead of the step's own outcome, so the job can never fail because of M4 regardless of how it terminates. **Update (v0.42.3, same day): fully resolved.** `MAX_OUTAGES` (not worker/shard count) was the dominant driver of the explosion; lowering it from 2 to 1 (every other bound unchanged) makes exhaustive BFS complete in ~5.4s (31,456 nodes). This also surfaced a real liveness bug — `GrantLease` could re-grant a lease to a worker its own failure detector had already declared dead, letting an adversarial-but-fair schedule starve every other worker forever — now fixed with an explicit `require worker_id not in cp.dead_workers` guard. `M4_S1`–`M4_S4`, `COV_M4`, `M4_L1_RecoveryProgress`, and `M4_L2_NoPermanentBlock` all now pass under exhaustive BFS; the CI special-casing (`continue-on-error`/`ulimit -v`/`timeout`) is removed and M4 is folded back into the single hard-gate step. See `formal/findings.md` ("Post-v0.42 Review", "Post-v0.42.1 Remediation Results", and "Post-v0.42.3 Remediation Results") and roadmap versions **v0.42.1** and **v0.42.3** below.

² **Coverage gate discrepancy found by the <=v0.42 review — resolved in v0.42.2.** The v0.42 sign-off stated the gateway coverage gate is "≥90% line / ≥85% branch" with fabricated supporting evidence, but `.github/workflows/ci.yml` has always actually enforced `--fail-under-lines 70` / `--fail-under-regions 70` (region, not branch, coverage — `cargo-llvm-cov` has no `--fail-under-branches` flag), matching what the test that was supposed to lock this in (`conformance_doc_tests::test_coverage_gate_config_is_present`) actually asserts (the *70%* strings). `sign-offs/v0.42.md` is now corrected to document the real, actually-enforced 70/70 gate instead of silently carrying the false claim. A real multi-row `INSERT ... VALUES (...), (...)` parsing bug in the gateway (silently corrupted/dropped the last column, no error) was also found and is now fixed — see roadmap version **v0.42.2** below.

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
| v0.1 | Workspace and CI ✅ Done | Cargo workspace with the focused crate set (`rockstream-types`, `-storage`, `-sim`, `-oracle`, `-plan`, `-diff`, `-ops`, `-sql`, `-runtime`, `-control`, `-gateway`, `-connectors`, `-cli`); CI (`fmt`, `clippy -D warnings`, `test`, `deny`, coverage); `rockstream-types::error_code` registry with CI enforcement; hot-path metrics emitter; dev container with SlateDB/MinIO/Postgres. | Clean CI on a no-op binary; `rockstream --help` works; CI fails the build on any returned `Error` or logged `error!` without an `RS-XXXX` code; no hidden local setup step. | Unit |
| v0.2 | Runtime abstraction, simulation, and oracle ✅ Done | `rockstream-sim`: `Runtime` trait (`now`/`spawn`/`sleep`/`object_store`/`network`), `TokioRuntime`, in-memory seeded `SimRuntime`, `buggify!()` macro, fault-model registry, paired-assertion helper. `rockstream-oracle`: DataFusion batch reference + `proptest` harness asserting `incremental == batch`. | A deterministic test replays the same seed byte-for-byte; a different seed changes event order; production build compiles `buggify!()` as a no-op; the oracle harness confirms equivalence on a trivial no-op pipeline. | Unit |
| v0.3 | SlateDB storage contract and determinism gate ✅ Done | `rockstream-storage` (`ShardDb`): key encoders with `namespace_id` in all catalog keys; `WriteBatch` builders; `DbReader` snapshot reads; WAL tail reader with listing cache; checkpoint helpers; scan-and-delete cleanup. No range deletion anywhere. `rockstream start --role=all --storage=./data` runs a no-op pipeline. | Storage API validation suite proves only supported SlateDB features are used. **SlateDB determinism gate**: a write-heavy `ShardDb` workload run twice at the same seed produces bit-identical key-value state and WAL sequence; any non-deterministic SlateDB background surface is found and constrained before v0.4. `make e2e` brings up MinIO + 1 worker + 1 control and tears it down. | LFS, MinIO, TC |

### Phase 1 — Single-Shard IVM Core

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.4 | Filter / project / map (IVM-1) ✅ Done | Z-set types `(row, weight: i64)`; `PlanNode` (`Source`/`Filter`/`Project`/`Map`/`ViewSink`/`Exchange` stub); `DiffCtx` linear-operator rules; `Operator` trait + `EpochOutput`; `OperatorTask` event loop; async ownership-free scheduler with credit backpressure; embedded runtime profile (in-process control/worker/gateway, no gRPC, no shuffle objects); built-in `GENERATE ROWS` source and a `Vec<RecordBatch>` delta source. | Oracle property test for `SELECT a, b*2 AS c FROM t WHERE c > 10` over random insert/delete sequences passes ≥100k scenarios; embedded hot path issues zero gRPC calls and creates zero shuffle objects. | Unit, LFS |
| v0.5 | Algebraic aggregates and epoch commit (IVM-2) ✅ Done | `Aggregate` PlanNode + aggregate arrangement; `diff_aggregate` (group by key, merge `(Δsum, Δcount)`, emit `(old,-1) ⊎ (new,+1)`) for SUM/COUNT/AVG/COUNT(*); shard-level group commit coalescing fragments into one atomic `WriteBatch`; per-shard namespaces (`op_state`/`view_output`/`shard_meta`); persisted frontier. | Oracle property test for `SELECT k, SUM(v), COUNT(*), AVG(v) FROM t GROUP BY k` over random insert/update/delete + group churn; group commit reduces durability events ≥5× vs. one commit per operator (measured on LFS and MinIO). | Unit, LFS, MinIO |
| v0.6 | MIN / MAX and crash-replay (IVM-3) ✅ Done | Indexed-multiset arrangement + cached extremum; `diff_minmax` (insert merges multiset and updates cache; delete of extremum prefix-scans for the replacement); WAL listing cache validated on the hot path. **Single-Shard Alpha.** | Oracle property test for groups churning across MIN/MAX transitions; cached extremum matches the multiset's true extremum after every batch. **Crash-replay**: `kill -9` injected mid-`WriteBatch`; on restart the shard replays from its persisted frontier to bit-identical output (verified on LFS and MinIO). | Unit, LFS, MinIO |

### Phase 2 — SQL Frontend & Joins

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.7 | SQL frontend and catalog ✅ Done | `rockstream-sql`: DataFusion parse/bind/optimize; custom extension nodes (`IncAggregate`/`IncJoin`/`IncDistinct`); lowering `LogicalPlan → PlanNode`; distribution pass annotating `partition_key` and inserting `Exchange` no-ops; schema-version catalog in `control: schema/` (compatible changes online, breaking → `RS-1002`); `CREATE VIEW`; `EXPLAIN INCREMENTAL` and `EXPLAIN INCREMENTAL ESTIMATE`. | SQL and hard-coded PlanIR produce identical physical plans for the Phase 1 operators; plans round-trip through catalog storage; an incompatible schema change returns `RS-1002`; `EXPLAIN INCREMENTAL ESTIMATE` reports predicted state size and per-operator `epoch_ms` without deploying. | Unit, LFS |
| v0.8 | Inner equi-join (IVM-4) ✅ Done | `InnerJoin` PlanNode + dual arrangements; stable source-derived `row_id` so replay rewrites the same key; DBSP-native two-arrangement join with correct bilinear expansion (`ΔL⋈R` insert/delete split, `L₀⋈ΔR`, correction term); arrangements reflect epoch `e-1` during processing, updated at commit. | Oracle property test for random 3-way joins; TPC-H Q1/Q3/Q5/Q6 plan-level parity; crash-replay of a join view restores bit-identical arrangement state. | Unit, LFS |
| v0.9 | Outer / semi / anti joins (IVM-5) ✅ Done | `LeftJoin`/`RightJoin`/`FullJoin`/`SemiJoin`/`AntiJoin`; one extra arrangement per side tracking unmatched rows so transitions emit the right NULL-padding retractions. | TPC-H Q11/Q21 (SemiJoin corner cases) and randomized NULL-heavy join tests match the batch oracle. | Unit, LFS |
| v0.10 | Distinct / set ops and storage budget gate (IVM-6) ✅ Done | Weight-based distinct arrangement; zero-crossing emission (`0→+n ⇒ +1`, `+n→0 ⇒ −1`); Intersect/Except with set and bag semantics. **Storage Operational Budget gate** (DESIGN.md §5.4): prove PUT/GET/LIST p99 at 1 GB and 5 GB, manifest cadence, WAL listing-cache hit ratio >99%, and write amplification against MinIO. **SQL Engine** milestone. | Oracle property tests on set semantics; `CREATE VIEW v AS SELECT … JOIN … GROUP BY …` compiles, deploys, and maintains incrementally end-to-end; storage budgets hold against MinIO (any budget exceeded by >2× requires a tracked mitigation before v0.11). | Unit, LFS, MinIO |

### Phase 3 — Essential Operators & Single-Shard Correctness Soak

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.11 | Window functions (IVM-7) ✅ Done | `Window` PlanNode + ordered arrangement; partition-based recomputation (re-evaluate a changed partition, diff against previously-emitted output); ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, sliding SUM/AVG; `EXPLAIN INCREMENTAL` flags oversized partitions. | Window-heavy randomized tests match the batch oracle; partition-recompute cost is measured and documented; oversized-partition NOTICE fires in `EXPLAIN`. | Unit, LFS |
| v0.12 | Tumbling time windows and Top-K (IVM-8, IVM-9) ✅ Done | TUMBLE windows keyed by `window_id` with event-time TTL and a frontier-aware compaction filter that removes state only after event-time expiry (HOP/SESSION deferred); Top-K value-descending arrangement keeping `K+ε` entries with delete-refill and displacement deltas. | Late-data and TTL tests prove no visible state is removed early (verified on LFS and MinIO); Top-K random insert/update/delete tests match batch; delete from the current top-K refills correctly. | Unit, LFS, MinIO |
| v0.13 | Bootstrap and view-on-view DAG (IVM-10, IVM-11) ✅ Done | Snapshot-mode sources emitting each base-table row once at weight `+1` (single or streamed bootstrap epochs); bootstrap frontier advances only when every chunk is ingested; `ViewRef` PlanNode tailing an upstream view's `view_output/` CDC via `WalReader`; structural diamond consistency; compile-time cycle detection (Kahn). | 100M-row-equivalent snapshot view matches the batch result; a mid-bootstrap connector restart neither duplicates nor skips rows; a 5-level view-on-view DAG and a diamond topology converge under continuous input; a cycle is rejected at compile time. | Unit, LFS, MinIO |
| v0.14 | Single-shard correctness soak ✅ Done | TPC-H 22/22 (SF=0.01) incremental; random SQL fuzzer over a synthetic schema; DST harness; storage-correctness audit (no range-delete dependency, snapshot-safe compaction filters); performance regression suite in CI. **IVM Correct (Single-Shard)** milestone. | 22/22 TPC-H queries return bit-identical results vs. DataFusion batch; ≥10× speedup vs. batch at a 1% change rate; the fuzzer runs ≥1 hour without divergence; the DST harness passes 100k seeds bit-identically; every cleanup path proven without range deletion. | Unit, LFS, MinIO |

### Phase 4 — Multi-Shard Execution & Exchange

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.15 | Control plane, leasing, and fencing ✅ Done | `rockstream-control` service; worker registration via `--control=<url>`; topology catalog; shard manager (a worker owns N shards, each with its own `Arc<Db>`); lease acquisition; SlateDB fence-epoch enforcement; role flags (`--role=control|worker|gateway|all`). | A two-writer fence test proves only one writer can commit to a shard; killing a worker releases its leases and they are reassigned cleanly; topology changes are audited. | Unit, LFS, TC |
| v0.16 | Direct exchange ✅ Done | gRPC shuffle service (`proto/shuffle.proto`); path classifier (`elided`/`loopback`/`direct`/`durable`); worker-to-worker stream multiplexing (one stream per peer per traffic class); same-worker loopback over bounded in-process channels with durable outbox/inbox metadata; Arrow serialization; credit-based backpressure. | A 16-shard cluster runs a partitioned TPC-H subset with connection count bounded by worker count, not shard count; the loopback path produces output identical to direct gRPC with zero network calls for co-located exchanges. | Unit, LFS, TC |
| v0.17 | Durable shuffle fallback and distributed parity ✅ Done | Object-store fallback writer/reader; coalesced durable shuffle objects with an index footer (no LIST on the hot path); rendezvous hashing with virtual nodes and re-balance-minimality property tests; full Phase 1–3 oracle + TPC-H suite re-run against the distributed cluster. **Distributed Engine** milestone. | Distributed results are **bit-identical** to single-shard runs across the entire oracle/TPC-H suite; an injected receiver failure forces durable fallback and the receiver catches up with no duplicates; a 1,000-shard exchange stress test stays within connection and shuffle-object budgets against MinIO. | Unit, LFS, MinIO, TC |

### Phase 5 — Frontier Protocol & Progress Tracking

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.18 | Frontier protocol ✅ Done | `rockstream-types::Frontier` antichain with product-order timestamps and meet/join/advance property tests; per-shard frontier reporter bundled into every epoch commit; control-plane frontier aggregator consuming per-shard summaries and publishing the per-operator cluster frontier; separable `--role=frontier`. **FizzBee toolchain bootstrap** (catch-up from Phase 0, FIZZBEE_TEST_PLAN.md §4.0): `formal/` directory, pinned FizzBee binary, `make verify`, the `formal-verify` CI job, and `formal/conventions.md`. **M1 retro-model** `formal/m1_epoch_commit.fizz` (the epoch-commit/`WriteBatch` protocol already shipped in v0.5/v0.6/v0.15) back-filling its paired assertions. **M2 model** `formal/m2_frontier_agg.fizz`: `Shard`/`FrontierAggregator`/`ObjectStore`/`ControlPlane` roles; per-shard report via the explicit message-set mechanism; publisher lease via `ObjectStore.cas` with a fencing token. | `SimRuntime`: arbitrary frontier-report reorderings converge to the same cluster vector frontier as serial delivery; a frontier-aggregation stress test over thousands of shards × hundreds of operators converges without the control plane subscribing to each shard feed. **FizzBee**: `make verify` is green in CI; M1 safety M1-S1…S5, liveness M1-L1, and coverage COV-M1 pass; M2 safety M2-S1…S4 (meet order-independence, pessimistic staleness, single-publisher, stale-write rejection), liveness M2-L1…L2 (`liveness: nondeterministic`), and coverage COV-M2 pass; each invariant maps to a paired runtime `assert!` in `rockstream-control`. | Unit, LFS, TC |
| v0.19 | Frontier consumers and exchange GC ✅ Done | Operator frontier consumers using the input frontier to close windows, detect convergence, and release shuffle inbox entries; exchange GC reclaiming outbox/inbox entries via bounded scan-and-delete or snapshot-safe compaction filters; control-plane-derived `min_epoch_ms`/`max_epoch_ms`/initial parallelism from the declared freshness target (simple, bounded). **M2 multi-source antichain variant** (FIZZBEE_TEST_PLAN.md D5.4): a fixed-length integer-vector frontier proving the meet is correct for the vector `FreshnessToken`, not just scalar source-epochs; findings recorded in `formal/findings.md` with paired `SimRuntime` regression seeds. **Progress-Tracked** milestone. | A join over two sources at different ingestion rates produces correct output with no premature emission and no infinite buffering; shuffle storage usage stays bounded under sustained throughput (measured against MinIO). **FizzBee**: the M2 multi-source meet model is green and its `SimRuntime` mirror (arbitrary frontier-report reorderings converge to the same cluster vector frontier) passes; the M2 §3.7 mapping rows are populated. | Unit, LFS, MinIO, TC |

### Phase 6 — Fault Tolerance & Exactly-Once

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.20 | Cluster checkpoints and recovery ✅ Done | Cluster checkpoint coordinator (barrier injection at sources; bounded barrier alignment tied to shuffle credits; one per-shard `Checkpoint` after all local operators commit through the barrier; atomic cluster-checkpoint commit; old-checkpoint GC); recovery driver bringing up every shard via `DbReader` pinned to its per-shard checkpoint, then re-electing writers. **M4 model** `formal/m4_self_fencing.fizz` (modeled before the self-fencing code in v0.21, FIZZBEE_TEST_PLAN.md §4.3, M4 first): `Worker`×2–3/`Shard`/`ControlPlane`/`ObjectStore`; `can_reach_control` flag; failure-detector `mark_dead`; `Shard.fence_epoch` CAS on every commit; `CheckpointCoordinator` role composed with M1's `cluster_committed` predicate. | Checkpointing under slow input and credit exhaustion never grows unbounded and either succeeds or reports `RECOVERING`; recovery from a cluster checkpoint reproduces pre-failure state bit-identically (verified on LFS and MinIO). **FizzBee**: M4 safety M4-S1…S4 (single-writer/no split-brain, self-fence precedence, lease uniqueness, object-store-only block), liveness M4-L1…L2 (`liveness: nondeterministic`), and coverage COV-M4 pass, justifying the recovery driver's writer re-election; paired runtime `assert!`s scheduled in `rockstream-runtime`. | Unit, LFS, MinIO, TC |
| v0.21 | Exactly-once sinks and resilience ✅ Done | Exactly-once sink protocol (`pre_commit`/`commit`); transactional Kafka sink; object-store sink via `_pending/` → atomic rename; per-connector source-epoch with persisted partition→offset map recorded in the epoch commit; worker self-fencing on control-plane partition (DESIGN.md §11.6); object-store brownout buffering + backpressure (DESIGN.md §11.7). **M3 model** `formal/m3_sink_2pc.fizz`: `SinkConnector`/`ExternalSystem`/`Shard`/`CheckpointCoordinator`/`ControlPlane`/`ObjectStore`; three parameterized sub-models, one per `SinkIdempotencyProfile` (`NativeIdempotent`/`FencingTokenRequired`/`CheckBeforeCommit`); explicit duplicate-delivery injection; crash yield points before/between/during the 2PC steps. **M1 duplication variant** confirming idempotent replay (M1-S5) under explicit message duplication. | `SimRuntime`: 2PC sink crash points (pre-commit/between/commit) all recover idempotently; a network-partitioned worker fences before the new owner commits; a 50-epoch object-store blackout produces zero loss and zero duplicates; the Kafka sink delivers exactly once under injected broker faults. **FizzBee**: M3 safety M3-S1…S4 (no duplicate, no lost, checkpoint-coupled, recovery-dispatch idempotency for all three profiles), liveness M3-L1, and coverage COV-M3 pass; M3-S3 is composed with M1's `cluster_committed` predicate; the partitioned-worker self-fence matches M4-S2; paired runtime `assert!`s land in `rockstream-connectors` and `rockstream-runtime`. | Unit, LFS, MinIO, TC |
| v0.22 | Chaos and recovery SLO gate ✅ Done | Chaos suite (random process kills, partitions, disk-full, object-store throttling) with output compared against a non-faulty reference; continuous simulation soak CI job running new seeds against `main`; recovery-time instrumentation. **Continuous formal verification** (FIZZBEE_TEST_PLAN.md §4.4): every FizzBee counterexample archived in `formal/findings.md` with its trace and paired `SimRuntime` regression seed; the `formal-verify` path-coupling check fails any coordination-crate or `DESIGN.md` change that lands without a corresponding model touch; a pre-release relaxed-bounds sweep (`NUM_WORKERS=3`, `NUM_SHARDS=3`, `MAX_EPOCH=4`). **Fault-Tolerant** milestone. | A 24-hour chaos run on a 32-shard cluster has zero data loss and zero duplicates with output matching reference; recovery from full outage in <60 s for state <1 TB; at `target_shard_state_bytes` (20 GB): failure detection ≤5 s p99, shard reassignment ≤30 s p99, freshness recovery ≤60 s p99; ≥100k seeded `SimRuntime` runs pass and the soak job has its first regression-seed corpus. **FizzBee**: all four models (M1–M4) are green at CI-fast and at the relaxed pre-release bounds; every archived counterexample replays clean as a permanent regression seed; the `formal-verify` gate is wired to the same merge gate as `cargo test`. **Fault-Tolerant** requires all four models green. | Unit, LFS, MinIO, TC |

### Phase 7 — PostgreSQL Wire Gateway

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.23 | Postgres read gateway (7.1) ✅ Done | `rockstream-gateway`: pgwire startup / simple query / extended query / copy-out / terminate; `ViewReader` trait with `ViewReadStrategy` (`HotOnly` implemented; `TwoTier` reserved); multi-shard `DbReader` reads pinned to one published vector frontier; catalog stubs (`pg_catalog.pg_tables`/`pg_views`/`pg_class`/`pg_attribute`/`pg_namespace`/`pg_type`, `information_schema.tables`/`.columns`, `SHOW`/`SET`); native Postgres type OIDs in row descriptions; isolation levels (`READ COMMITTED`, `REPEATABLE READ`, `SERIALIZABLE` → `RS-2003`); inline views with cycle detection (`RS-1011`). | `psql` connects and `SELECT * FROM my_view LIMIT 10` returns <10 ms p99 for a local cluster; an ORM (e.g. SQLAlchemy via TestContainers) reflects view schemas without error; `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` returns `RS-2003`; `CREATE MATERIALIZED VIEW mv AS SELECT * FROM v` inlines `v` and starts IVM. | Unit, LFS, TC |
| v0.24 | Direct-write DML (7.2) ✅ Done | Internal direct-write source connector (`INSERT`/`UPDATE`/`DELETE` buffered per connection; `COMMIT` flushes an atomic Z-set delta via `WriteBatch` to a base-table shard and receives the next `source_epoch`; `ROLLBACK` discards); `INSERT … RETURNING` including the multi-row `INSERT … SELECT … RETURNING` form; idempotency enforcement (exactly-once source-epoch envelope or caller key; missing both → `RS-2007`) with a per-shard, time-bounded idempotency-key table. | `psql` runs `INSERT INTO t VALUES (…); COMMIT` and the view reflects it within the freshness target; a non-idempotent write missing both an envelope and a key returns `RS-2007`; idempotent replay of a committed write is a no-op (verified on LFS and MinIO). | Unit, LFS, MinIO, TC |
| v0.25 | Subscribe and read-your-writes (7.3) ✅ Done | Subscribe endpoint tailing view changes via `WalReader`, gateway-proxied (`mz_timestamp`, `mz_diff`, projected columns; `AS OF NOW WITH SNAPSHOT`, `AS OF EPOCH <n>` within `CHANGE_RETENTION` default 1 h; server-side `WHERE`; column projection); freshness tokens returned on query responses; `wait_for=<token>` read-your-writes with timeout and explicit satisfied/not-satisfied; session-scoped automatic read-your-writes after `COMMIT`. | Read-your-writes demo passes and `wait_for=<token>` resolves within the SLO; a subscribe stream survives a gateway restart with no gaps or duplicates; `SUBSCRIBE orders_mv AS OF NOW WITH SNAPSHOT` delivers current state then live deltas; `WHERE`/projection reduce traffic to matching rows/columns; `AS OF EPOCH` outside retention returns `RS-2005`. | Unit, LFS, TC |
| v0.26 | Auth, RBAC, and read pushdown (7.4) ✅ Done | OIDC/bearer-token and mTLS auth at the gateway (`--auth=off` for local dev); per-view RBAC (`viewer`/`pipeline_owner`/`admin`) in the control-plane catalog; namespace isolation; cross-shard partial aggregation pushdown (DESIGN.md §12.3.1) so `SELECT agg, key FROM mv GROUP BY key` returns O(distinct groups × shards) rows. **Postgres Pillar** milestone. | An application using a standard Postgres driver creates a view, writes rows, reads them back with read-your-writes, and subscribes to changes end-to-end against the distributed engine; unauthenticated requests are rejected; cross-namespace access is denied; the audit log records `actor` on every control action; `EXPLAIN` names the pushdown effect. | Unit, LFS, TC |

### Phase 8 — Ingestion & The Crucible Soaks

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.27 | Bulk Historical Load (`COPY` Protocol) ✅ Done | Implement the standard Postgres `COPY` protocol via the pgwire layer; enables migration of massive historical datasets into distributed base tables without external streaming pipelines. | Users can execute multi-gigabyte `COPY FROM` commands into distributed base tables without memory exhaustion. | Unit, LFS, MinIO, TC |
| v0.28 | First-Party Source Connectors ✅ Done | Native, highly optimized Kafka (consumer-group; offsets in the causal frontier) and AWS S3 source connectors for continuous streaming ingestion; the §13.3 connector contract (`discover_schema`/`start_snapshot`/`poll_delta`/`commit_offset`/`prepare`/`commit`/`abort`/`should_flush`). | Kafka offsets and S3 file pointers are correctly tracked within the causal frontier protocol for exactly-once ingestion; a Kafka source closes a tumbling window correctly under deliberate clock skew. | Unit, LFS, MinIO, TC |
| v0.29 | Object Store Limit Soak ✅ Done | Maximize sustained pressure to validate shard-level group commits, SST/WAL caches, and coalesced durable fallback paths against cloud provider rate limits; native OpenTelemetry/Prometheus metrics (`/metrics`) utilized for monitoring flush latencies. | The cluster maintains high throughput for 72 hours without HTTP 429 (Too Many Requests) throttling errors from the cloud provider. | Unit, LFS, MinIO, TC |
| v0.30 | Auto-Tuner Chaos Soak ✅ Done | Subject the cluster to extreme step-function traffic spikes to test the default "self-tuning" adaptive control loops (epoch sizing, source throttling, and parallelism); prove control loops do not overcorrect and enter destabilizing oscillation. | Control loops settle to a stable steady-state within 3 epochs of a 10× traffic spike. | Unit, LFS, MinIO, TC |
| v0.31 | Recovery SLO Validation ✅ Done | Induce hard network partitions and simulated object-store brownouts under continuous Kafka streaming load; instrument recovery timings end-to-end. **Soaks Complete** milestone. | Failure detection ≤5 s p99, shard reassignment ≤30 s p99, freshness recovery <60 s p99 for 1 TB of state. | Unit, LFS, MinIO, TC |

### Phase 9 — Operational HTAP Ergonomics

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.32 | Secondary Indexes ✅ Done | System-managed materialized IVM views acting transparently as secondary indexes for point queries on non-primary keys; eliminates full shard scans for highly concurrent application reads. **Private Beta Ready** milestone. | Point lookups on indexed non-primary columns execute in single-digit milliseconds. | Unit, LFS, MinIO, TC |

### Phase 10 — Nexmark Correctness Suite

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.33 | Nexmark Schemas & Deterministic Event Generator ✅ Done | Define `person`, `auction`, `bid` SQL DDL; implement `NexmarkGenerator` in `rockstream-sim` (seeded, configurable event rates, correct NEXMark distribution: ~46 bids : 3 auctions : 1 person per event); wire the generator through the Postgres wire protocol — events are fed as batched `INSERT` statements via pgwire and base tables are created with standard `CREATE TABLE` DDL. Test scaffold in `rockstream-sql/tests/nexmark/` covers schema creation, ingestion, and basic round-trip reads, all operating end-to-end over the full pgwire + SlateDB stack (LFS and MinIO). | Same seed produces an identical event stream across two runs; generator distribution is within 1% of target ratios; all three tables accept `INSERT` via pgwire and return correct row counts via `SELECT`; schema round-trips through the catalog; verified on LFS and MinIO via TestContainers. | Unit, LFS, MinIO, TC |
| v0.34 | Nexmark q0–q9: Projections, Filters, Joins & Basic Aggregates ✅ Done | End-to-end implementation of the first ten Nexmark queries as `CREATE VIEW` statements maintained by the IVM engine, all exercising the full stack exclusively via the Postgres wire protocol: q0 (pass-through `SELECT *`), q1 (currency conversion — multiply price), q2 (selection filter — `MOD(auction, 123) = 0`), q3 (local item suggestion — inner join Auction × Person filtered by state and category), q4 (average price per category — windowed join + `AVG`), q5 (hot items — sliding-window bid `COUNT` + top-N), q6 (average selling price by seller — last-10-auctions window), q7 (highest bid — tumbling-window `MAX`), q8 (new-user monitoring — join Person × Auction within event-time window), q9 (winning bids — join Bid at closing price). Views created via `CREATE VIEW`; events fed via `INSERT`; results consumed via `SELECT` and `SUBSCRIBE` — no in-process shortcuts. | Each of q0–q9 produces results **bit-identical** to the DataFusion batch oracle over static Nexmark snapshots; incremental results match batch results after every event batch under randomised generator seeds; `SUBSCRIBE` delivers correct deltas as events arrive; all queries verified end-to-end over pgwire on LFS and MinIO. | Unit, LFS, MinIO, TC |
| v0.35 | Nexmark q12–q22: Dedup, TOP-N & Complex Analytics ✅ Done | End-to-end implementation of eleven Nexmark queries as `CREATE VIEW` statements, all via the full pgwire + SlateDB stack: q12 (processing-time tumbling windows — wall-clock tumble), q13 (bounded side-input join — stream enriched from a static lookup table), q14 (`CASE WHEN` + `CAST` complex projection), q15 (bidding statistics — multiple `COUNT(DISTINCT …)` with price-tier filters), q16 (channel statistics — multi-key `COUNT(DISTINCT …)`), q17 (auction statistics — unbounded `GROUP BY` date), q18 (find last bid — deduplication via `LAST_VALUE` / retract-and-replace), q19 (auction TOP-10 prices — `TOP-N` descending), q20 (expand bid with auction — filter + equi-join), q21 (add channel ID — `CASE WHEN` + `REGEXP_EXTRACT`), q22 (URL directories — `SPLIT_INDEX`). Note: q10 (Parquet sink) is covered in Phase 12 (v0.43 cold-tier sinks); q11 (session windows) is deferred to v0.50 where the `SESSION` operator is implemented. | q12–q22 results are **bit-identical** to the DataFusion batch oracle on static Nexmark snapshots; q18 deduplication and q19 TOP-N maintain correctness under retraction storms; all eleven queries pass on LFS and MinIO via pgwire. | Unit, LFS, MinIO, TC |
| v0.36 | Nexmark Retraction & Z-Set Correctness ✅ Done | Exercise all Nexmark views (q0–q9, q12–q22) with INSERT+UPDATE+DELETE event sequences to prove Z-set retraction propagates correctly through every operator combination — the correctness property that append-only benchmarks cannot test. Retraction test harness in `rockstream-oracle/tests/nexmark_retraction.rs`: for each query, generate a random base snapshot, verify `incremental == batch`, then apply a mix of price updates, bid retractions, and auction cancellations and re-verify. Criterion micro-benchmarks in `rockstream-ops/benches/nexmark.rs` measure delta propagation cost (µs/row and delta amplification factor) for each query at 0.1%/1%/10% change rates — descriptive only, no CI throughput gates. **Nexmark Correctness Complete** milestone. | Every Nexmark view (q0–q9, q12–q22) maintains `incremental == batch` after mixed INSERT+UPDATE+DELETE sequences under 100 randomised seeds; delta amplification factor ≤10× for all stateful queries; criterion benchmarks record baseline numbers; all tests pass on LFS and MinIO. | Unit, LFS, MinIO, TC |

### Phase 11 — PostgreSQL Wire Protocol Hardening

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.37 | Extended Query Protocol & Prepared Statements ✅ Done | Replace `NoopQueryParser` in `rockstream-gateway` with a stateful `PreparedStatementCache`: maps statement name → SQL text + inferred parameter types (`$N` OIDs inferred from DataFusion's logical plan; explicit `$N::type` casts honoured). Implement the full Parse/Bind/Describe/Execute/Close/Sync/Flush state machine: `Describe(Statement)` returns `ParameterDescription` + `RowDescription`; `Describe(Portal)` returns `RowDescription`; `Execute` honours `max_rows` and emits `PortalSuspended` when the row limit is reached, resuming on the next `Execute`; `Flush` sends buffered messages without `ReadyForQuery`; `Sync` finishes the pipeline and emits `ReadyForQuery` regardless of prior errors (abort-on-error pipeline semantics). `DEALLOCATE [ALL]` statement handling. Multi-statement simple query (semicolon-separated): each statement produces its own result set in order. `EmptyQueryResponse` for empty/whitespace-only queries. SSL negotiation: respond to `SSLRequest` with `'N'` so TLS-first clients downgrade gracefully. Add `ParameterStatus` startup messages for `server_version` (`"14.9 (RockStream)"`), `integer_datetimes`, `standard_conforming_strings`, `DateStyle`, `IntervalStyle`, `client_encoding`. | `psql -c '\d my_view'` reflects the correct column list without a `protocol error`; `PREPARE p AS SELECT * FROM mv WHERE id = $1; EXECUTE p(1)` returns the correct rows; `tokio-postgres` `.prepare()` + `.query()` with typed parameters returns correct rows (new `gateway_extended_query_tests.rs` TC test with a real psql binary); portal suspension: `Execute(max_rows=10)` on a 1 000-row result returns exactly 10 rows and `PortalSuspended`, subsequent `Execute(max_rows=0)` returns the remaining 990; multi-statement `"SELECT 1; SELECT 2"` emits two `CommandComplete` messages; `SSLRequest` does not cause connection failures from TLS-first clients; all existing `gateway_proof_tests.rs` and `gateway_integration_tests.rs` tests pass. | Unit, TC |
| v0.38 | Type System Completeness & pg_catalog Depth ✅ Done | Extend `arrow_type_to_pg_oid` and `pg_type_from_name` to cover all SQL types needed for ORM compatibility: INT2 (21), FLOAT4 (700), DATE (1082), TIME (1083), TIMESTAMPTZ (1184), UUID (2950), NUMERIC/DECIMAL (1700), JSON (114), JSONB (3802), VARCHAR/CHAR (1043/1042), INTERVAL (1186); and array variants `_int4` (1007), `_int8` (1016), `_text` (1009), `_float8` (1022), `_bool` (1000), `_uuid` (2951). Binary result format encoding for all numeric types (INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL) — clients requesting binary format receive correct network-order bytes. `pg_catalog` additions: `pg_proc` (built-in aggregate function entries so ORMs do not error on function lookup), `pg_constraint` (PK/FK/UNIQUE/CHECK stubs; initially empty for views), `pg_index` (linked to `CatalogIndexEntry`), `pg_description` (empty rows), `pg_enum` (empty rows), `pg_aggregate` (COUNT/SUM/AVG/MIN/MAX entries), `pg_roles`/`pg_user` stubs (authenticated principal as one row). `information_schema` additions: `key_column_usage`, `table_constraints`, `column_privileges`, `referential_constraints` stubs. Extend `pg_catalog.pg_type` with entries for all new OIDs. ORM compatibility via TestContainers: SQLAlchemy 2.x `inspect(engine).get_columns()`, Prisma `db pull`, jOOQ/Hibernate `DatabaseMetaData.getColumns()`. | `arrow_type_to_pg_oid("Date32")` returns `1082`; binary INT4/INT8/FLOAT8/BOOL format is bit-identical to Postgres 14 wire encoding (compared against a reference Postgres 14 TC instance); SQLAlchemy `inspect(engine).get_columns('my_view')` returns correct column names and Python types without `SAWarning`; `prisma db pull` exits 0 and the resulting `schema.prisma` is parseable; Hibernate `getColumns(null, "public", "my_view", "%")` returns at least one row with correct `COLUMN_NAME` and `DATA_TYPE`; new `type_system_tests.rs` unit tests assert OID round-trips for every new type. | Unit, TC |
| v0.39 | Wire Protocol Hardening & Connection Pool Compatibility ✅ Done | **CancelRequest**: gateway emits `BackendKeyData` (PID = connection counter, random secret) at session start; a `CancelRequest` on a separate TCP connection aborts the target query within 200 ms with `RS-2050`/SQLSTATE `57014`; the connection accepts new queries after cancellation. **Named cursors**: `DECLARE … CURSOR FOR`, `FETCH [FORWARD] n FROM`, `FETCH ALL FROM`, `MOVE`, `CLOSE`; connection-scoped, cleaned up on `ROLLBACK` or connection close. **Streaming result delivery**: `ViewReader::read_view` gains a row-iterator interface; the gateway pumps rows in batches of 1 000 without collecting a `Vec<Vec<u8>>`; per-connection peak memory bounded at 64 MiB. **Full SQLSTATE mapping**: every `RS-XXXX` error code in `error.rs` maps to a 5-char Postgres SQLSTATE; `ErrorResponse` fields `Detail`, `Hint`, `Position`, and `Where` populated for every query-level error. **PgBouncer compatibility**: correct `ReadyForQuery` transaction-status byte (`'I'`/`'T'`/`'E'`); `DISCARD ALL` and `RESET ALL` clear session state; portal and cursor cleanup on `Sync` outside a transaction; `application_name` tracked per session. **`pg_stat_activity`** virtual table: `pid`, `usename`, `application_name`, `state`, `query`, `query_start`, `client_addr`. **Protocol fuzzer**: `proptest`-driven structured fuzzer generating random well-formed pgwire message sequences; assert no panics, no hangs (bounded by `tokio::time::timeout`), no malformed responses. **Concurrent-connection stress**: 1 000 simultaneous `tokio-postgres` connections × 100 queries with zero connection-level errors and peak RSS < 2 GiB. | PgBouncer 1.21 in transaction-pooling mode fronting the gateway delivers correct results for 10 000 transactions across 50 client connections with zero errors (TC); cancellation test: a slow query is aborted in <200 ms and the connection reused successfully; cursor test: `FETCH 100` / `FETCH ALL` / `CLOSE` lifecycle passes end-to-end; 2 000 000-row streaming query completes with peak heap growth <64 MiB; fuzzer runs 50 000 random sequences without panics or hangs; 1 000-connection stress test completes without any connection error; every `RS-XXXX` code has a unit test asserting its SQLSTATE. | Unit, LFS, TC |
| v0.40 | Password Authentication & Driver Session Bootstrap ✅ Done | Make any standard Postgres driver able to *connect* the way it does to real Postgres — no `--auth=off` requirement. Add `AuthMode::Scram` and `AuthMode::Md5` to `auth.rs` and implement the SCRAM-SHA-256 server flow (RFC 5802/7677): `AuthenticationSASL` → client `SASLInitialResponse` (client-first) → `AuthenticationSASLContinue` (server-first: server nonce + salt + iteration count) → client `SASLResponse` (client-final) → verify `ClientProof`, send `AuthenticationSASLFinal` + `AuthenticationOk`; plus an `AuthenticationMD5Password` fallback with a per-connection salt. Store password verifiers (SCRAM `StoredKey`/`ServerKey`, or the `md5` hash) in a control-plane `pg_authid`-style role catalog managed by `CREATE ROLE … LOGIN PASSWORD '…'` / `ALTER ROLE … PASSWORD` / `DROP ROLE`, mapped onto the existing `Principal`/RBAC model; `--auth=scram\|md5\|oidc\|mtls\|off`. **Driver session bootstrap**: implement the catalog functions every driver/ORM probes at connect time — `version()`, `current_database()`, `current_schema()`, `current_schemas(bool)`, `current_user`/`session_user`/`user`, `pg_backend_pid()`, `pg_is_in_recovery()`, `current_setting(text[, bool])`, `set_config(text,text,bool)`, `pg_postmaster_start_time()`, `txid_current()` — and a working GUC round-trip so `SET search_path = app` then `SHOW search_path` agree, with `search_path`-aware resolution of unqualified view names; `SHOW ALL`; `client_encoding`/`server_encoding` fixed to `UTF8`. | psql, psycopg3, pgx (Go), PgJDBC, node-postgres, and tokio-postgres each complete a SCRAM-SHA-256 handshake and finish their full startup-probe sequence with zero protocol errors (new `auth_scram_tests.rs` TestContainers driver matrix); a wrong password returns SQLSTATE `28P01`; the MD5 path authenticates a legacy `psql` client; `SELECT version()` returns a libpq-parseable Postgres 14 banner; `SET search_path='app'; SHOW search_path` round-trips and unqualified `SELECT * FROM my_view` resolves through it; every bootstrap function's result shape is compared against a reference Postgres 14 TC instance. | Unit, TC |
| v0.41 | Transaction State Machine, Savepoints & LISTEN/NOTIFY ✅ Done | Give applications the transactional control surface a real OLTP app expects. Promote `session.rs` to a full wire-level transaction state machine: implicit single-statement transactions, explicit `BEGIN [TRANSACTION]`/`COMMIT`/`END`/`ROLLBACK` blocks, `SAVEPOINT`/`RELEASE SAVEPOINT`/`ROLLBACK TO SAVEPOINT` implemented as nested write-buffer checkpoints layered over `write_buffer.rs`, and `SET LOCAL`/`SET TRANSACTION`. **Aborted-transaction semantics**: a query error inside a block flips the `ReadyForQuery` status byte to `'E'` and every subsequent command returns SQLSTATE `25P02` (`in_failed_sql_transaction`) until `ROLLBACK`/`ROLLBACK TO`, after which the session is fully reusable. **LISTEN/NOTIFY**: per-session channel registry bridged to the existing CDC/subscribe layer (`subscribe_handler.rs`/`change_log.rs`) so `NOTIFY chan, 'payload'` and the `pg_notify(text,text)` function deliver `NotificationResponse` (`A`) messages to every listening connection at transaction commit (at-least-once on the issuing connection); `LISTEN`/`UNLISTEN`/`UNLISTEN *` with channel cleanup on `DISCARD ALL` and disconnect. `PREPARE TRANSACTION`/`COMMIT PREPARED` (XA two-phase) are explicitly rejected with `RS-2003`/SQLSTATE `0A000` and documented unsupported. | A transactional workflow `BEGIN; INSERT …; SAVEPOINT s; INSERT …; ROLLBACK TO s; COMMIT` leaves only the pre-savepoint write visible (new `transaction_savepoint_tests.rs`, LFS + TC); an error mid-transaction blocks further commands with `25P02` until `ROLLBACK`, then the connection is reused successfully; `LISTEN events; NOTIFY events, 'hi'` delivers a `NotificationResponse` to a second connection within the freshness SLO, verified with psycopg3 `connection.notifies` (new `listen_notify_tests.rs` TC); the `'I'`/`'T'`/`'E'` transaction-status byte is correct through the entire lifecycle and a psql `\set AUTOCOMMIT off` transcript matches Postgres. | Unit, LFS, TC |
| v0.42 | Reference Application & Driver-Matrix Certification ✅ Done | Prove — and permanently lock — that an end user can build a *complete* project on the pgwire surface with no RockStream-specific workarounds. Ship a reference OLTP+analytics application under `crates/rockstream-gateway/tests/reference_app/`: schema migrations (`CREATE TABLE`/`CREATE VIEW`/`CREATE MATERIALIZED VIEW`), transactional writes with savepoints, materialized-view reads, `SUBSCRIBE`/`LISTEN` change feeds, prepared statements, and pooled connections — driven entirely through a mainstream ORM/migration toolchain over the wire (Prisma `migrate` + client, plus a SQLAlchemy 2.x / Alembic variant). **Driver-compatibility matrix** in CI (TestContainers): psql 14/16, libpq, psycopg3, tokio-postgres, pgx, PgJDBC, node-postgres, SQLAlchemy, Prisma — each runs the same conformance smoke suite (connect+SCRAM auth, simple + extended query, typed params, transactions + savepoints, named cursors, `COPY`, LISTEN/NOTIFY, CancelRequest). **Golden wire-byte snapshots**: capture exact server→client byte streams for representative exchanges and assert them byte-for-byte against checked-in goldens to catch silent protocol regressions. **Coverage gate**: raise `rockstream-gateway` line coverage to ≥90% (enforced by the existing CI coverage job) and branch coverage on `protocol.rs`/`server.rs`/`session.rs`/`auth.rs` to ≥85%. A `docs/pgwire-conformance.md` corpus enumerates every supported message/statement with a linked proof test. **Wire Protocol End-User Complete** milestone. | The Prisma reference app runs `prisma migrate deploy`, seeds data, reads materialized views, subscribes to changes, and completes a transactional workflow against RockStream unmodified (CI, TC); the same application logic passes on the SQLAlchemy/Alembic variant; all nine drivers in the matrix pass the conformance smoke suite green; golden wire-byte snapshots match exactly; `rockstream-gateway` coverage ≥90% line / ≥85% branch is enforced and fails CI on regression; `docs/pgwire-conformance.md` lists every supported surface with a linked test. | Unit, LFS, MinIO, TC |

### Phase 11.5 — Post-v0.42 Remediation (found by the <=v0.42 implementation review, 2026-07-10; mandatory before v0.43)

The v0.28–v0.42 "project restart integration" re-implemented the whole roadmap
from v0.1 in a single pass. A subsequent thorough re-review of that work found
two real gaps that were marked done without being true, on top of an existing
known SQL-parsing bug. v0.42.1 fixed the CI toolchain and made smoke/M1/M2/M3
a genuine, verified hard gate; v0.42.2 fixed the gateway multi-row `INSERT`
gap and reconciled the coverage-gate documentation; v0.42.3 closed the one
remaining item (M4's state-space size and a real liveness bug it uncovered).

**Follow-up <=v0.42.3 implementation review (2026-07-10, same day) found and
fixed two process gaps** in how the Common Definition of Done's sign-off
requirement is enforced, on top of confirming the v0.42.1–v0.42.3 functional
claims above are genuinely true against running code: (1)
`scripts/check-exit-criteria.sh`'s version-extraction regex only matched
two-component version numbers (`v0.42`), so it silently never checked
three-component remediation sub-versions (`v0.42.1`/`v0.42.2`/`v0.42.3`) at
all — not "missing", just never evaluated — which is how those three
versions were marked `✅ Done` in this roadmap with no `sign-offs/vX.Y.Z.md`
file for weeks without CI ever flagging it. Fixed by extending the regex to
accept an optional third dot-separated component. (2) `DESIGN.md` §17's
known-simulation-fidelity-gaps table had two gaps marked
`Status: [MITIGATED in v0.43]` — a future version that had not been
implemented yet — and a third gap's mitigation target referenced a stale
"v0.42 Simulator Maturity" milestone name/number left over from an earlier
roadmap revision (that milestone is v0.56 in the current numbering). Fixed by
correcting all three to `[UNMITIGATED — scheduled for v0.43]`, consistent
with `NEW_ROADMAP.md`'s own v0.43 scope text. (3) `formal/README.md`'s spec
index never listed `m3_sink_2pc.fizz`/`m4_self_fencing.fizz` (added in
v0.20/v0.21) and its links pointed at a stale, non-existent absolute path
(`file:///Users/grove/projects/rockstream/formal/...`) instead of the actual
repository. **Done**:
`scripts/check-exit-criteria.sh` fixed and re-run clean;
`sign-offs/v0.42.1.md`, `sign-offs/v0.42.2.md`, `sign-offs/v0.42.3.md`
created with full proof-claim verification against `formal/findings.md` and
the actual gateway/CI code; `DESIGN.md` §17 gap statuses corrected;
`formal/README.md`'s spec index now lists all five specs with working
relative links.
All three versions are now closed, unblocking Phase 12, since v0.43 explicitly
requires a *working* `make verify` gate for its new M5 model.

**>=v0.43 roadmap review (2026-07-11).** A thorough re-review of Phases
12–16 against DESIGN.md and the original `NEW_IMPLEMENTATION_PLAN.md` found
three operational-readiness commitments that were fully designed
(DESIGN.md §5.5 rolling upgrades, §14.7/§14.7.1/§14.19 the operator CLI and
arrangement debugger, §14.18 secrets management) or explicitly required by
the original plan's Phase 8 exit criteria (an admin CLI, "mTLS everywhere",
an independent security review, a documented disaster-recovery drill), but
had never been scheduled as an actual roadmap version with a proof
obligation — a real gap for a project whose stated goal includes being
"easy to use and easy to understand and operate". Added three new versions
to Phase 16 to close this gap before v1.0: **v0.53** (admin CLI + IVM
arrangement debugger + resource-usage visibility), **v0.54** (internal mTLS,
`CREATE SECRET` envelope encryption, and an independent security review),
and **v0.55** (the `rockstream migrate` tool, an end-to-end proven
rolling-upgrade path, and a rehearsed disaster-recovery/backup-restore
drill — now the **Operationally Complete** milestone). This also surfaced a
real `RS-5002` error-code numbering collision in DESIGN.md between
`merge.unknown_law` (the canonical table) and an informal
`protocol.version_not_supported` use in §5.5; the latter is reassigned to
`RS-5021` and implemented at v0.55. The previously-final `v0.53`/`v0.54`
(Simulator Maturity, v1.0 RC1) are renumbered to `v0.56`/`v0.57`; every
cross-reference in this document, `README.md`, and the `RS-5002` collision
note in `DESIGN.md` has been updated accordingly. No changes were made to
any `✅ Done` version (v0.1–v0.42.3); this review only affects unstarted
work at v0.43 and later.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.42.1 | FizzBee Toolchain Remediation & Hard Gate ✅ Done | The `formal-verify` CI job has never actually worked: (1) its FizzBee download URL (`fizzbee-linux-amd64.tar.gz`) does not match any real release asset (real assets are versioned, e.g. `fizzbee-v0.5.2-linux_x86.tar.gz`); (2) even if downloaded, the job only extracts the bare `fizzbee` binary, but `Makefile`'s `verify`/`verify-relaxed` targets invoke the `fizz` wrapper script, which requires sibling files (`fizz.env`, `parser/`, `mbt_gen.zip`) that were never installed; (3) the job's PR trigger condition (`contains(github.event.pull_request.changed_files, 'formal/')`) tests a GitHub Actions field that is an integer count, not a file list, so the job has only ever run on `push` to `main`, never pre-merge; (4) `continue-on-error: true` means none of the above would block a merge even if fixed and red. Running the real toolchain for the first time (this review) found: M2 genuinely passes (251,889 states); M1 (incl. the v0.21 duplication variant) fails its `M1_S5_IdempotentReplay` invariant with a reproducible counterexample; M3 and M4 crash with Starlark "undefined variable" errors (`externalsystem` at `m3_sink_2pc.fizz:207`, `workers` at `m4_self_fencing.fizz:236`) and have never completed a single run. **Done**: the CI installer now resolves the real per-OS/arch asset from the GitHub Releases API and installs the full `fizz` wrapper directory; the PR trigger condition now does a real `git diff` against the merge-base; `formal/m1_epoch_commit.fizz`'s `M1_S5_IdempotentReplay` scoping bug is fixed and the spec is genuinely green (72 states, safety+liveness); `formal/m3_sink_2pc.fizz`'s undefined-variable crash, Starlark set-literal syntax error, a `sink_pending_epoch` reset bug, and an unbounded-crash liveness starvation are all fixed and the spec is genuinely green (1,168 states, safety+liveness); `formal/m4_self_fencing.fizz`'s undefined-variable crash and a real heartbeat/self-fence race (a state existed where a worker was isolated past `SELF_FENCE_AFTER` while still `"active"`) are fixed, and a missing `RestartWorker` transition (terminated workers had no path back to active, an operational-reality gap) is added; `continue-on-error` is removed for smoke/M1/M2/M3, which are now a real hard gate. **Not done at the time this row was written, completed under v0.42.3**: M4's exhaustive (BFS) verification does not terminate in reasonable time/memory at the committed bounds (`NUM_WORKERS=3`) — still growing past 800k nodes and 10+ GB RSS at a *reduced* `NUM_WORKERS=2` after several minutes in local testing, and bounded-simulation liveness results for M4 are not trustworthy (a reported `M4_L2_NoPermanentBlock` failure trace contained zero occurrences of any worker ever being blocked, indicating a simulation-mode artifact, not a real counterexample). M4 stays `continue-on-error: true` pending a state-space fix — see v0.42.3. | `make verify` runs smoke/M1/M2/M3 green in CI on a real pull request (not just `push`) as a hard gate; a deliberately-broken spec among those four is proven to block merge; `formal/findings.md` is updated with the real, current pass/fail status for every model (superseding the inaccurate pre-review "no counterexamples found" claims, which were written without ever running the model checker); the M1 and M3 fixes are backed by concrete before/after run evidence (state counts, liveness results) recorded in `formal/findings.md` "Post-v0.42.1 Remediation Results". | Unit |
| v0.42.2 | Gateway Multi-Row INSERT & Coverage-Gate Reconciliation ✅ Done | `parse_insert` in `crates/rockstream-gateway/src/server.rs` only handles a single-row `VALUES (v1, v2, ...)` tuple; a multi-row list like `VALUES (1,'a'),(2,'b'),(3,'c')` is silently mis-split (text is taken from the first `(` to the *last* `)` in the whole clause), corrupting or dropping later columns with **no error raised** — violating the Common Definition of Done's "any user-visible failure has an `RS-XXXX` code" rule, and undermining the v0.39/v0.42 "any standard driver works without workarounds" claim, since ORM bulk-insert helpers (SQLAlchemy `bulk_insert_mappings`, Prisma `createMany`) emit multi-row `VALUES` by default. Separately, the v0.42 sign-off documents the `rockstream-gateway` coverage gate as "≥90% line / ≥85% branch", but `.github/workflows/ci.yml`'s `coverage` job actually enforces `--fail-under-lines 70` / `--fail-under-regions 70` (region coverage, since `cargo-llvm-cov` has no `--fail-under-branches` flag), and `conformance_doc_tests::test_coverage_gate_config_is_present` asserts the *70%* strings — the documented 90/85 commitment has never been enforced. Scope: implement correct multi-row `VALUES` parsing (each row becomes its own buffered `DmlOp::Insert`, reusing existing single-row semantics per row, with a hard parse error instead of silent corruption on any malformed row); either raise the CI gate and `conformance_doc_tests` assertions to match the documented 90% line / 85% region commitment, or explicitly revise `sign-offs/v0.42.md` down to the achievable, actually-enforced number — one or the other, not silence. **Done**: `parse_insert`/`split_value_tuples` now split a `VALUES (...), (...), ...` clause into one row per tuple (tracking paren depth and quote state so commas/parens inside string literals never get mistaken for row separators), reusing single-row semantics per row; a row whose arity doesn't match the declared column list (or the first row's arity, when no column list is given) is a hard `RS-2056` parse error, not silent corruption; `INSERT ... RETURNING` now returns every written row, not just the first. `sign-offs/v0.42.md` is corrected down to the actually-enforced `--fail-under-lines 70` / `--fail-under-regions 70` gate (the previous "≥90% line / ≥85% branch" claim, including its cited CI line numbers, was fabricated — no such flags, nor a `--fail-under-branches` flag, have ever existed in `cargo-llvm-cov`); `ci.yml` was left unchanged since it already enforced the real 70/70 gate. `docs/tutorial-30-minutes.md`'s per-row-INSERT workaround note is removed and `docs/pgwire-conformance.md` gains a multi-row-INSERT proof-test link. | `INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')` produces three correct rows end-to-end (new test, LFS); a malformed multi-row `VALUES` list returns an `RS-XXXX` error instead of silently corrupting data; `docs/tutorial-30-minutes.md`'s existing per-row-INSERT workaround note is removed now that multi-row VALUES works; the coverage numbers in `ci.yml`, `conformance_doc_tests.rs`, and `sign-offs/v0.42.md` all agree with each other. | Unit, LFS |
| v0.42.3 | M4 State-Space Tractability & Liveness Closure ✅ Done | `formal/m4_self_fencing.fizz`'s exhaustive (BFS) state space does not visibly terminate at the committed CI-fast bounds (`NUM_WORKERS=3, NUM_SHARDS=2`); a reduced `NUM_WORKERS=2` run was still exploring >800k nodes and >10 GB RSS after several minutes before being aborted. Scope: apply FizzBee's symmetry-reduction support (the toolchain ships a `16-05-nominal-symmetry` example, suggesting workers/shards can be declared interchangeable to collapse permutation-equivalent states) and/or tighten `SELF_FENCE_AFTER`/`MAX_EPOCH`/`MAX_CHECKPOINT`/`MAX_OUTAGES` until exhaustive `make verify` terminates in CI in a few minutes; once it does, positively confirm (not just fail to disprove) `M4_L1_RecoveryProgress` and `M4_L2_NoPermanentBlock`, fixing whatever the exhaustive run reveals; remove M4's `continue-on-error: true` once green, completing the v0.18 binding contract for the last of the four models. **Done**: isolating each bound one at a time (see `formal/findings.md` "Post-v0.42.3 Remediation Results") found `MAX_OUTAGES` — not worker/shard count — is the dominant driver of the explosion (`MAX_OUTAGES=2` alone was ~16× the node count of `MAX_OUTAGES=1` at otherwise-equal-or-larger bounds); lowered `MAX_OUTAGES` from 2 to 1 while keeping every other bound at its originally committed value (`NUM_WORKERS=3, NUM_SHARDS=2, SELF_FENCE_AFTER=3, MAX_EPOCH=2, MAX_CHECKPOINT=2`). No symmetry-reduction feature was needed. This also surfaced (and fixed) a real liveness bug masquerading as a tractability problem: `GrantLease` could re-grant a shard's lease back to a worker already in `cp.dead_workers` because it only checked `w.status == "active"` (true until self-fence), not failure-detector state, letting an adversarial-but-fair schedule that always re-elects the same dead worker starve every other worker forever (`M4_L1_RecoveryProgress` genuinely failed under exhaustive BFS before this fix — FizzBee's `fair action` fairness is per-action, not per-`oneof`-parameter-binding). Added `require worker_id not in cp.dead_workers` to `GrantLease`. Exhaustive `make verify` on `formal/m4_self_fencing.fizz` now completes in ~5.4s exploration + ~0.25s liveness check (31,456 nodes, 12,946 valid, 1,966 unique states); `M4_S1`–`M4_S4`, `COV_M4`, `M4_L1_RecoveryProgress`, and `M4_L2_NoPermanentBlock` all pass. `.github/workflows/ci.yml`'s M4 step is folded back into the single hard-gate step with smoke/M1/M2/M3; the `continue-on-error`/`ulimit -v`/`timeout` special-casing is removed; job timeout lowered from 45 to 20 minutes. | Exhaustive `make verify` on `formal/m4_self_fencing.fizz` completes in CI within a documented time/memory budget; `M4_S1`–`M4_S4` (safety), `M4_L1`, `M4_L2` (liveness), and `COV_M4` all pass under exhaustive BFS, not just bounded simulation; `formal/findings.md` records the final node/state counts as evidence; `continue-on-error` is removed from M4's CI step. | Unit |

### Phase 12 — The Data Lake Bridge & FinOps

**>=v0.44 roadmap review (2026-07-11).** A follow-up review of Phases 12–16
(everything after the already-reviewed v0.43) against `DESIGN.md` found two
issues:

1. A stale internal cross-reference: the v0.35 row above said Nexmark q11
   (session windows) was "deferred to v0.47 where the `SESSION` operator is
   implemented", but v0.47's actual scope (Zero-Copy IPC & AZ-Aware Shuffle)
   has nothing to do with windowing — the `SESSION` operator is v0.48's scope
   (Advanced Streaming Analytics), consistent with the Public Milestones table
   above and `README.md`. Fixed the v0.35 cross-reference to say v0.48.
2. A real scheduling gap in the same shape as the <=v0.43 review's findings:
   `DESIGN.md` §13.3.1 fully designs a connector-tier per-record
   decode-failure Dead Letter Queue — `rockstream_catalog.dead_letter_queue`,
   `ALTER SOURCE ... REPLAY|DISMISS DEAD_LETTER_QUEUE`, `RS-1003`/`RS-1004`,
   and `DLQ_RETENTION`/`dlq_warn_threshold` source options — but no roadmap
   version ever scheduled it; only a config field
   (`dlq_warn_threshold` in `crates/rockstream-types/src/config.rs`) exists
   today, unused. v0.51 as previously scoped only covered a *different* DLQ
   concept (routing rows that fail a v0.50 `CREATE EXPECTATION` check).
   Expanded v0.51's scope to explicitly cover both, reconciled onto one
   table via a `failure_source` discriminant column, so the fully-designed
   connector DLQ surface isn't silently dropped before v1.0.

No other gaps were found in Phases 12–16: v0.52's `SERIALIZABLE LOCAL` is
correctly single-shard/planner-proven (matches `DESIGN.md` line ~2726) and
needs no new FizzBee model; v0.53–v0.55 (added by the <=v0.43 review) already
close the operator-CLI/mTLS/rolling-upgrade gaps. A pre-existing, unrelated
stale reference was also noted but intentionally left alone to avoid
`DESIGN.md`-wide scope creep (already tracked as a known follow-up from the
<=v0.43 review): §13.9.6's secondary-index error codes say "Ships: v0.50",
but secondary indexes are the v0.32 feature (`✅ Done`), not v0.50.

**v0.44-specific follow-up (same day).** A closer pass at the v0.44 row itself
(not just Phases 12–16 as a whole) found two more issues, both fixed in place:
1. The `CREATE SINK` DDL mention added by the earlier pass used invented
   shorthand (`CREATE SINK <name> AS ICEBERG|DELTA (...)`) that doesn't match
   `DESIGN.md` §13.6.1's actual grammar (`CREATE SINK <name> FOR VIEW <view>
   TO ICEBERG|DELTA '<path>' WITH (...)`, confirmed against
   `docs/language-features.md`'s `CREATE SINK ... TO ICEBERG` phrasing).
   Corrected to the real syntax with its key `WITH` options named.
2. `DESIGN.md` §13.6.2.1 fully designs cold-snapshot garbage collection
   (retention count/duration options, expiry rules, and a "never delete a
   file referenced by a retained snapshot" safety guarantee) as an integral
   part of the cold-tier sink — DESIGN.md itself notes that without it
   "object-store costs grow unboundedly" — but it was never mentioned in any
   roadmap version. Confirmed §13.7 (native Iceberg REST catalog server) and
   §13.8 (DuckLake catalog server) are *not* a gap: `NEW_IMPLEMENTATION_PLAN.md`
   explicitly lists both as out of scope for this plan, deferred beyond v1.0.
   Added the GC scope and a dedicated safety-test proof line to v0.44 rather
   than a new version, since it's inseparable from the sink's snapshot
   lifecycle.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.43 | FizzBee Cold-Tier Protocol Model & Simulator Fidelity ✅ Done | Before any cold-tier sink code is written, model-check the cold-tier exactly-once commit protocol in FizzBee (binding pre-implementation requirement from v0.18); simultaneously close all three UNMITIGATED simulation fidelity gaps from DESIGN.md §17 that block cold-tier and Kafka exactly-once claims. Deliverables: `formal/m5_cold_tier_sink.fizz` with safety invariants (no duplicates, no data loss under partial-write faults, manifest-pointer atomicity) and liveness (`committed_epoch` always advances); paired runtime `assert!`s in `rockstream-connectors`; add `partial_write_probability: f64` to `SimObjectStore` and a `PartialWriteRecoveryTest` to the law-faults corpus (gap 1); add `kafka_tx_timeout_probability` fault parameter to the Kafka connector simulator (gap 2); pull forward `list_staleness_epochs` if not yet landed in v0.42 (gap 3); update DESIGN.md §17 gap statuses to `[MITIGATED]`. | `formal/m5_cold_tier_sink.fizz` is green with all safety and liveness invariants at CI-fast bounds; injecting `partial_write_probability=0.5` triggers truncated bytes and the cold-tier recovery test passes without duplicate output (new `partial_write_recovery_tests.rs`, LFS + MinIO); `kafka_tx_timeout_probability` exercises the `CheckBeforeCommit` recovery path; all three §17 simulation gaps are updated to `[MITIGATED]`; all existing workspace tests pass. | Unit, LFS, MinIO, TC |
| v0.44 | Cold-Tier Sinks ✅ Done | Iceberg and Delta sinks exporting periodic columnar Parquet snapshots to object storage; bridges RockStream to the Data Lakehouse for external engines (DuckDB, Trino, Databricks). The simulator fidelity foundation from v0.43 backs all crash-recovery paths with proper partial-write fault injection. **DDL surface** (DESIGN.md §13.6.1): `CREATE SINK <name> FOR VIEW <view> TO ICEBERG|DELTA '<path>' WITH (snapshot_interval_epochs, snapshot_interval_ms, parquet_row_group_bytes, format_version, partition_by, catalog = filesystem\|glue\|rest\|hive\|ducklake, ...)`, documented in `docs/` alongside the existing `CREATE SOURCE`/`CREATE VIEW` DDL so cold-tier sinks are discoverable the same way everything else is, not a bolt-on feature only visible in code. **Found missing from any roadmap version during the >=v0.44 review**: DESIGN.md §13.6.2.1 fully designs cold-snapshot garbage collection (`cold_snapshot_retention_count`/`cold_snapshot_retention_duration` sink options, the "whichever bound is reached first" expiry rule, and the safety guarantee that a data file referenced by any retained snapshot is never deleted) as part of the same cold-tier sink feature — without it object-store costs grow unboundedly, per DESIGN.md's own framing — but no roadmap version ever scheduled it. Bundled into v0.44's scope rather than split into a separate version, since it is inseparable from the sink's snapshot lifecycle and has no independent value without it. | External engines can query RockStream-generated Iceberg tables with zero data corruption; `partial_write_probability` fault injection passes without any duplicate or missing rows in the sink output; `docs/` documents `CREATE SINK` end-to-end with a runnable example, and `EXPLAIN INCREMENTAL` on a sunk view shows the sink target; cold-snapshot GC expires snapshots beyond `cold_snapshot_retention_count`/`cold_snapshot_retention_duration` and emits `cold_gc_bytes_reclaimed`/`cold_gc_last_run_epoch`, while a new `cold_gc_safety_tests.rs` proves a data file shared by a retained and an expired snapshot is never deleted and GC never runs concurrently with a snapshot commit. | Unit, LFS, MinIO, TC |
| v0.45 | Deep FinOps Optimizations & Cost/Diagnostics Visibility ✅ Done | Route latency-sensitive metadata (`shard_meta/`) to AWS S3 Express One Zone; tier older compacted SSTs to S3 Standard-IA; validate running the stateless worker pool entirely on Spot/preemptible instances. **Found missing during the 2026-07-11 usability/cost review**: the gateway's live `EXPLAIN <query>` handler (`crates/rockstream-gateway/src/server.rs`) is a one-line pushdown/index-annotation stub, completely disconnected from the fully-built `rockstream-sql::explain_incremental`/`explain_incremental_for_sql`/`explain_incremental_estimate` (DESIGN.md §14.8-§14.9's rich operator tree, ⚠ flags, and cost preview) — an operator typing `EXPLAIN INCREMENTAL <view>` in `psql` today gets none of the documented output. Rather than wait for the full admin CLI/resource-visibility surface at v0.55, wire `EXPLAIN INCREMENTAL`/`EXPLAIN INCREMENTAL VERBOSE`/`EXPLAIN INCREMENTAL ANALYZE`/`EXPLAIN INCREMENTAL ESTIMATE` into the gateway's real query dispatch (reusing the existing library functions, no new planning logic), and ship a minimal `SHOW RESOURCE USAGE` / `SHOW RESOURCE USAGE FOR WORKLOAD <name>` / `SHOW CLUSTER RESOURCE USAGE` (§14.19) backed by new `rockstream_catalog.view_resource_usage`/`workload_resource_usage` tables populated from metrics that already exist (`workload_memory_bytes`, `state_budget_bytes`, write-amplification, segment-cache hit ratio) — both needed to make this version's own "per-tier cost-visibility metric" claim and the FinOps win actually visible to an operator via SQL/CLI, not just `/metrics`. Also: turn the v0.36 Nexmark criterion benchmarks from "descriptive only" into a real CI regression gate (per the Common Definition of Done's own rule, "CI fails on >10% regression once a baseline exists" — the v0.36 baseline has existed since 2026-06-25 and was never gated). **Found missing during the 2026-07-11 control-plane/multi-tenancy review**: three small, already-fully-designed OLTP/analytical session-ergonomics gaps in DESIGN.md §12.8 that no version ever scheduled — `SET rockstream.max_staleness` for analytical sessions (skip the read-your-writes frontier wait and accept a bounded-stale snapshot instead: a direct latency-for-freshness trade-off the design doc itself describes as pure gateway session bookkeeping with no new distributed machinery); the cross-session `rockstream.write_fence()` / `after_fence(:token)` token pair; and completing `INSERT ... RETURNING` for server-assigned primary keys (today `parse_insert`'s `returning_rows` in `crates/rockstream-gateway/src/server.rs` only echoes back the client's own literal `VALUES`, so a caller can never get a server-generated UUID or sequence value back). All three are folded into this version rather than given their own, since each is gateway-only with no new distributed protocol. **Found missing during the 2026-07-11 network-efficiency/metrics-scalability/cost-visibility review**: (a) DESIGN.md §14.15.1's `pipeline_id` metrics-cardinality budget doubles as a hard, unconditional 256-pipeline cluster-wide creation limit, contradicting this project's own scalability positioning — decouple cardinality management (cap full per-pipeline label emission to an LRU/recent-traffic working set, rolling every pipeline outside it into one aggregate `pipeline_id="other"` series) from the functional pipeline-count limit, which the control-plane catalog itself never enforced. (b) Cost visibility today is a one-time TCO benchmark, not a running number: add `estimated_cost_per_hour` to `rockstream_catalog.view_resource_usage`/`workload_resource_usage` and to `SHOW RESOURCE USAGE` output, computed from a small operator-supplied cloud-pricing table (a `[pricing]` block in `rockstream.toml` — object-store request/storage/egress unit costs, compute $/core-hour, Spot-vs-on-demand mix) applied to metrics already collected, so the FinOps win is visible live, per workload, without re-running a benchmark. | TCO benchmarks show >50% reduction in steady-state operational costs compared to v0.31; a per-tier cost-visibility metric ships on `/metrics` and the starter Grafana dashboard so an operator can see the FinOps win without re-running the TCO benchmark; `EXPLAIN INCREMENTAL my_view` typed into `psql` returns the documented operator tree (not the old pushdown-only stub), verified byte-for-byte against `explain_incremental_for_sql`'s output; `SHOW RESOURCE USAGE` returns live per-workload state-byte/memory/SLO figures matching `view_resource_usage`; CI fails a nightly Nexmark run that regresses delta-amplification or propagation latency by >10% against the v0.36 baseline; a cluster that has created more than 256 materialized-view pipelines over its lifetime continues to accept new `CREATE MATERIALIZED VIEW` statements, with per-worker metrics cardinality bounded by the LRU working-set size rather than by total pipeline count; `SHOW RESOURCE USAGE` and `rockstream_catalog.workload_resource_usage` return a non-null `estimated_cost_per_hour` once a `[pricing]` profile is configured, and the figure changes visibly when a workload's `MEMORY_LIMIT` or shard count changes. | Unit, LFS, MinIO, TC |

**Usability/scalability/cost-effectiveness review (2026-07-11).** A holistic
review of the project against scalability, latency, throughput, usability,
and cost-effectiveness found that DESIGN.md §10.1–§10.4 (the online
shard-migration state machine: add/remove shard, checkpoint-copy rebalancing,
the worker-drain protocol), §10.5–§10.6 (hot-key virtual buckets, proactive
shard splitting), §10.8 (the `cluster_worker_pressure` autoscaling signal),
and the "adaptive skew splitting" control loop listed as on-by-default in
§14.5 are fully designed but **were never scheduled in any roadmap version,
and none of it exists in code** (verified: zero references to `hot_key`,
`shard_split`, `cluster_worker_pressure`, or the migration state names
`SNAPSHOTTING`/`DUAL_WRITING`/`FENCING_OLD` anywhere in `crates/`). This is the
same shape of gap the <=v0.42.3 review found for the admin-CLI/mTLS/
rolling-upgrade surface — in fact v0.53's admin CLI (below, renumbered to
v0.55) lists `shard {list,migrate}` and `cluster workers drain` as wrapping
"already-shipped" APIs that, until this review, did not actually exist. Added
as a new **Phase 13 — Elastic Scaling & Skew Handling** below, renumbering
every subsequent version by +2 (old v0.46–v0.57 → v0.48–v0.59;
"Operationally Complete" moves to v0.57, "1.0 Release"/v1.0 RC1 moves to
v0.59). Earlier review-narrative paragraphs above this point (the
`>=v0.43`/`>=v0.44` roadmap reviews and the v0.44 row deep-dive) are left
untouched and still refer to the pre-this-review numbering (e.g. "v0.53"
meaning Admin CLI) — consistent with this document's existing practice of
preserving prior review records as a historical account rather than rewriting
them in place; only the live tables, the Public Milestones list, the Formal
Verification Track, and the Plan-mapping table below are kept current. Also
pulled a thin slice of resource-visibility and `EXPLAIN INCREMENTAL`
wire-protocol reachability forward into v0.45 above (rather than waiting for
the full admin-CLI version at v0.55), since the gateway's live `EXPLAIN`
handler is currently an unrelated one-line stub disconnected from the
already-built `rockstream-sql::explain_incremental`.

**Follow-up control-plane/multi-tenancy review (2026-07-11, second pass).** A
second pass over the same five dimensions (scalability, latency, throughput,
usability, cost-effectiveness) — this time reading DESIGN.md §3, §3.2, and
§14.13 against `crates/` rather than against `NEW_ROADMAP.md` — found two
more instances of the exact gap shape the reviews above already established:
a capability is fully designed, DESIGN.md talks about it as already real, and
grepping the crate confirms it is not, and no roadmap version ever scheduled
building it.

1. **Control-plane HA.** §3's topology diagram and §3.2 both describe control
   nodes as forming "a small Raft group" that "elects exactly one"
   control-SlateDB writer, and `NEW_IMPLEMENTATION_PLAN.md`'s own "Open
   questions" section promises this is "Hardened in Phase 8." Grepping
   `crates/rockstream-control` for `raft`/`Raft` returns zero matches, Phase 8
   (v0.27–v0.31, ✅ Done) shipped without it, and no version from v0.1 through
   v0.59 ever scheduled it — the control plane is, today, a single
   non-replicated process. That is a real single point of failure at exactly
   the "massive" end of the scale ladder this project's own README promises
   ("bottomless capacity... scales with your workload"): losing the one
   control-plane process stalls shard leasing, frontier publication, and every
   autoscaling decision Phase 13 below adds, until it is manually restarted.
   `causalmesh-report.md` §5 — this project's own idea document for the
   frontier/antichain math — independently confirms the intended division of
   labor: tracking progress "still needs a control plane or something like
   Raft. CausalMesh does not, and should not pretend to, solve it." The
   frontier algebra was always designed assuming something else supplies
   control-plane consensus; nothing ever has.
2. **Workload quotas and multi-tenancy.** DESIGN.md §14.13 fully specifies
   `CREATE WORKLOAD` / `ALTER WORKLOAD` / per-workload `MEMORY_LIMIT` /
   `MAX_PARALLELISM` / `PRIORITY`-based admission control, and
   `rockstream-types::workload` already carries the `WorkloadPriority` /
   `FreshnessSlo` / `MemoryLimit` data model — but nothing reads or writes it:
   `rockstream-sql` has no `CREATE WORKLOAD` parser, `rockstream-control` has
   no workload catalog or admission-control logic (grep only turns up a code
   comment referencing "a workload target"), and `rockstream-gateway` never
   mentions the word "workload" anywhere. Only a single *global, cluster-wide*
   memory budget (`state_budget_gb` in `rockstream.toml`, DESIGN.md §5.6's
   `OVER_BUDGET_RELAXED` state) exists today — the same blunt instrument for
   every pipeline on the cluster, with no per-workload isolation and no
   priority-based shedding when two pipelines contend for the same cluster.
   This is the same shape of gap the <=v0.42.3 review found for the admin CLI
   and the Phase-13-insertion review above found for shard migration: the
   already-drafted v0.55 admin CLI's `workload {list,show,create,alter,drop}`
   is specified as a thin wrapper over a control-plane API that does not exist.

Both close before Phase 13's elastic-scaling machinery is asked to run at
"massive" scale on top of them — a control plane that cannot fail over, and a
cluster that cannot safely host more than one pipeline's worth of trust, are
both scale ceilings lower than anything Phase 13 fixes. Rather than trigger
another full renumbering of Phases 13–17 for two versions that slot cleanly
before Phase 13 begins, this review reuses the decimal-sub-version mechanism
already established by v0.42.1–v0.42.3 (this roadmap's own words: "a version
is a planning unit, not a release-quality promise") to add **Phase 12.5 —
Control-Plane Hardening & Multi-Tenancy** (v0.45.1–v0.45.2) directly below,
between Phase 12 and Phase 13. No whole-number version from v0.46 onward
changes meaning, so Phases 13–17, the Public Milestones table, and README.md's
roadmap tables are unaffected by this review (only the one-line CLI crate
description in README.md is touched, to name the two new versions).

While re-reading DESIGN.md §12.8 (OLTP Session Ergonomics) and §8.7 (OLAP
scatter pruning) to confirm neither had this same "designed but unscheduled"
problem, this review also found both sections' `**Ships**: vX.Y` annotations
name version numbers from a numbering scheme that predates one or more of
this roadmap's own renumbering passes: §12.8's session-scoped `wait_for`
tracking, annotated as landing at "v0.47," is in fact already implemented and
shipped with the v0.26 Postgres Pillar milestone (see
`rockstream-gateway/src/session.rs`'s `last_written_epoch` field and the
`SESSION_WAIT_FOR_*` counters in `server.rs`); §8.7's scatter-pruning
statistics, annotated as landing at "v0.54 ... after secondary indexes land
at v0.53," are the same feature already scheduled at **v0.48** below, and
secondary indexes shipped at v0.32, not v0.53. All five stale annotations
found across §8.7 and §12.8 are corrected directly in DESIGN.md rather than
left as another "intentionally left alone" note, since they were cheap to fix
and directly concern this review's own subject matter (session-ergonomics
latency and scatter-pruning latency). The two genuinely-unshipped-and-
unscheduled pieces of §12.8 (`max_staleness`, the cross-session write fence,
and the server-assigned-key gap in `INSERT ... RETURNING`) are folded into
v0.45 above rather than given their own version, since DESIGN.md itself
describes them as "purely gateway session bookkeeping" with "no new
distributed machinery."

**Network-efficiency, metrics-scalability, and cost-visibility review
(2026-07-11, third pass).** A third pass over the same five dimensions
(scalability, latency, throughput, usability, cost-effectiveness), reading
DESIGN.md §7.3, §14.15.1, and §14.19 against `crates/` and the repository's
own build tooling, found one already-shipped-code defect (fixed immediately,
no roadmap slot needed) and two more instances of the "designed but
unscheduled" gap shape the reviews above already established, plus one
cost-visibility gap that is new in kind rather than in shape.

1. **The reference `Dockerfile` shipped a debug binary.** It built with plain
   `cargo build --bin rockstream` and copied `target/debug/rockstream` into
   the runtime image — never `--release`. Debug builds carry none of the
   optimizations every latency/throughput number this project publishes
   assumes, and are materially larger; nothing in
   `.github/workflows/ci.yml` ever ran `docker build .`, so this had zero test
   coverage and could not have been caught. This is a one-line correctness
   bug, not a design tradeoff, so it is fixed directly rather than scheduled:
   `Dockerfile` now builds with `--release` and copies
   `target/release/rockstream`; a new `docker-build` CI job builds and
   smoke-runs the image on every push/PR (and greps for the `--release` flag
   directly) so this exact regression cannot recur silently.
2. **Shuffle payloads are documented as compressed but are not.** §7.3's
   `shuffle_outbox/`/`shuffle_inbox/` key encoding states the value as "Arrow
   IPC batch (compressed)" as settled fact. Grepping the entire workspace for
   `compress` outside unrelated test-assertion prose returns nothing, the
   workspace `tonic` dependency enables no compression feature, and no
   roadmap version from v0.1 through v0.59 ever schedules implementing it —
   the same "DESIGN.md describes it as already real; `crates/` confirms it
   is not; no version ever scheduled it" shape the §10 elasticity review and
   the control-plane/workload reviews above already found twice. Every
   uncompressed shuffle byte is a direct, avoidable tax on exactly what
   Phase 14 already exists to fix: shuffle latency, the network throughput
   ceiling, and (per v0.49's own cross-AZ cost argument) egress cost. Added
   to **v0.49** below, alongside zero-copy IPC and AZ-aware shuffle, since
   all three are the same "spend less moving bytes" phase; no new FizzBee
   model is required because compression is a wire-encoding change beneath
   the existing coordination protocols, not a new protocol.
3. **A Prometheus label-cardinality budget silently caps the entire cluster
   at 256 pipelines, forever.** §14.15.1's cardinality table sets
   `pipeline_id | 256 | Hard limit; reject pipeline creation beyond this.`
   A per-worker metrics-cardinality bound is reasonable; using it as a hard
   ceiling on a *functional* capability — how many materialized-view
   pipelines the cluster may ever host, cluster-wide, forever — directly
   contradicts this project's own "bottomless capacity"/"massively parallel"
   positioning (`README.md`) for exactly the large multi-team platform
   deployment that positioning targets. No roadmap version has reconciled
   this, despite three prior reviews specifically hunting for scalability
   ceilings in this document. Added to **v0.45** below: decouple
   metrics-cardinality management (full per-pipeline label breakdowns for an
   LRU/recent-traffic working set only, rolling every pipeline outside it
   into one aggregate `pipeline_id="other"` series) from the functional
   pipeline-count limit, since the control-plane catalog itself has no such
   ceiling — only the metrics exporter does.
4. **Cost visibility is a one-time benchmark, not a running number.** v0.45's
   own proof clause is "TCO benchmarks show >50% reduction... compared to
   v0.31" — something an operator has to re-run to see, not something they
   can read off a live cluster. `SHOW RESOURCE USAGE` (§14.19, also shipping
   at v0.45) reports bytes, memory, and SLO compliance, but never a dollar
   figure, even though every input a cost estimate needs
   (`object_store_request_duration_seconds`, bytes transferred, worker-hours,
   Spot-vs-on-demand mix) is already collected. Added to **v0.45** below: an
   `estimated_cost_per_hour` field on `rockstream_catalog.view_resource_usage`
   / `workload_resource_usage` and in `SHOW RESOURCE USAGE` output, computed
   by applying a small operator-supplied cloud-pricing table (a `[pricing]`
   block in `rockstream.toml` — unit costs only, no cloud-billing API
   integration) to metrics that already exist. This is what turns "the
   FinOps optimization happened" into "here is what it is saving you, right
   now, per workload" — the most direct cost-effectiveness gap this pass
   found.

### Phase 12.5 — Control-Plane Hardening & Multi-Tenancy

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.45.1 | Workload Quotas, Priority Admission Control & Multi-Tenancy ✅ Done | Wire the existing-but-unused `rockstream-types::workload` data model into a real control-plane workload catalog and the gateway: `CREATE WORKLOAD <name> WITH (MEMORY_LIMIT=..., MAX_PARALLELISM=..., PRIORITY=..., FRESHNESS_SLO=...)`, `ALTER WORKLOAD ... SET (...)`, `DROP WORKLOAD`, `SHOW WORKLOAD STATUS` (DESIGN.md §14.13); per-workload `MEMORY_LIMIT` transitions over-budget views to `OVER_BUDGET_RELAXED` independently of (and nested inside) the existing global `state_budget_gb` floor, which remains as the cluster-wide backstop; `MAX_PARALLELISM` caps the existing per-operator auto-tuner (v0.30) on a per-workload basis; `PRIORITY`-driven admission control (§14.16) pauses or defers lower-priority workloads' views under contention instead of degrading every pipeline on the cluster equally. This is the control-plane substrate the v0.55 admin CLI's `workload {list,show,create,alter,drop}` and `cluster {status,workers,quotas}` commands assume already exists. | Two workloads (`PRIORITY=HIGH`, `PRIORITY=LOW`) sharing one deliberately under-provisioned cluster: under sustained contention the low-priority workload's views transition to `PAUSED`/`OVER_BUDGET_RELAXED` while the high-priority workload keeps meeting its `FRESHNESS_SLO`; a view that exceeds its own workload's `MEMORY_LIMIT` (but not the global budget) transitions to `OVER_BUDGET_RELAXED` and is visible in `SHOW WORKLOAD STATUS`; every quota decision and admission-control pause is audited (§14.11). | Unit, LFS, TC |
| v0.45.2 | Control-Plane High Availability (Raft-Elected Writer Lease) ✅ Done | Before any consensus code is written, model-check the control-plane leader-election/fencing integration in FizzBee (binding pre-implementation rule from v0.18): **M7 model** `formal/m7_control_plane_ha.fizz` — `ControlNode` ×3–5 / `ObjectStore` roles, composed with M4's shard-fencing predicate, proving a leadership change never lets two control nodes simultaneously grant the same shard lease, admit a workload write, or publish conflicting frontiers. Implement the 3/5-node Raft group over the control SlateDB using an existing, well-tested Raft crate rather than hand-rolled consensus, closing `NEW_IMPLEMENTATION_PLAN.md`'s "Open questions #3" and DESIGN.md §16.6's "Control-plane HA" risk: the Raft leader holds the control-SlateDB writer lease; followers serve cached catalog/topology/workload reads via `DbReader`; the existing §3.2 frontier-aggregator lease election is explicitly composed on top of (not duplicating) Raft leadership. Ships the `--role=control --bootstrap` / multi-node join flow §3 already documents as if it were runnable code. | `formal/m7_control_plane_ha.fizz` is green at CI-fast bounds (no dual-leader window; no lease, workload-catalog write, or shard assignment accepted from a non-leader); a real 3-node control-plane cluster (TC) survives a leader kill with shard leasing and frontier publication resuming within the existing §11.5 recovery-time budgets and zero split-brain shard grants, verified by both a `SimRuntime` scenario and the TestContainers drill; a one-at-a-time rolling restart of all three control nodes never drops a worker's lease or a workload's quota state. | Unit, LFS, MinIO, TC |

**Testing-quality & code-quality review (2026-07-11).** Every review pass
above hunted for scalability/latency/cost capabilities that DESIGN.md
describes as real but `crates/` does not implement. This pass asks a
different question of the testing and quality *machinery* the Common
Definition of Done and this roadmap repeatedly promise ("CI fails on >10%
regression once a baseline exists", coverage gates, "`cargo clippy -D
warnings` must pass") — is it actually running, and running on the code that
most needs it? Five gaps were found, all confirmed directly against the real
CI config, `Makefile`, and source, none previously scheduled anywhere:

1. **No performance-regression gate has ever existed, for anything.** The
   Common Definition of Done requires "a `criterion` benchmark or measurement
   note; CI fails on >10% regression once a baseline exists", and versions
   v0.14, v0.36, and v0.45 (above) all repeat variants of this promise. In
   reality: only `crates/rockstream-ops/benches/` contains any benchmark code
   at all (`minmax_bench.rs`, `perf_regression.rs`, `nexmark.rs` — confirmed
   by grepping every crate's `Cargo.toml` for `[[bench]]`); `.github/
   workflows/ci.yml` never invokes `cargo bench`; and no baseline-storage
   mechanism (a checked-in `critcmp`/criterion snapshot or otherwise) exists
   anywhere in the repository. Every throughput number this roadmap has ever
   published — "≥10× speedup vs. batch" (v0.14), "throughput increases by
   >30%" (v0.51) — has shipped with zero automated protection against silent
   regression, and DESIGN.md §16.6's own explicitly-flagged open risk
   ("frontier-aggregator throughput... must be CPU- and memory-bounded, never
   blocking") has no benchmark measuring it at all, nor does the exchange/
   shuffle subsystem that Phase 14 is entirely about making cheaper.
2. **Code coverage is gated for exactly one of thirteen crates.** `ci.yml`'s
   `coverage` job produces a workspace-wide `lcov.info` but only ever passes
   `--fail-under-lines`/`--fail-under-regions` to `--package
   rockstream-gateway`. `rockstream-diff` — the crate ARCHITECTURE.md itself
   calls "precisely the place where incremental correctness is won or lost" —
   has no coverage floor at all, and neither do `rockstream-ops`,
   `rockstream-storage`, `rockstream-runtime`, `rockstream-control`,
   `rockstream-connectors`, or `rockstream-sql`.
3. **`Makefile`'s own `coverage-gate` target is broken and stale.** It still
   hard-codes `--fail-under-lines 90` and `--fail-under-branches 85` — the
   exact numbers `sign-offs/v0.42.2.md` already proved false and corrected
   everywhere else in this repository to the real 70%/70% line/region gate —
   and `--fail-under-branches` is not a flag `cargo-llvm-cov` has ever
   supported (the same v0.42.2 finding, which fixed `ci.yml` and the sign-offs
   but never touched this fourth copy of the same numbers because it sits in
   tooling rather than prose). Because no CI job calls `make coverage-gate`
   (CI invokes `cargo llvm-cov` directly with the correct flags), a
   contributor running the one documented, human-friendly local command would
   either get a confusing CLI argument error (the branch flag does not exist)
   or, once fixed, numbers that still would not match CI unless corrected.
4. **The gateway silently exempts itself from the workspace clippy gate.**
   `CONTRIBUTING.md` and the `Makefile` both state
   `cargo clippy --workspace --all-targets -- -D warnings` must pass, and CI
   runs exactly that command — but `crates/rockstream-gateway/src/lib.rs`
   opens with `#![allow(clippy::all, unused_variables, dead_code)]`. The
   workspace-wide command still exits 0, but only because clippy was told not
   to look at the gateway at all, not because its protocol/session/query code
   is actually clean — the one crate parsing arbitrary client-supplied SQL
   text and wire-protocol bytes directly off the network is the one crate
   clippy silently never checks.
5. **Dependency-advisory scanning only ever runs when a PR happens to touch
   the repo.** `DEPENDENCY_POLICY.md` states "Dependabot or Renovate keeps
   dependencies current" as settled fact, but there is no
   `.github/dependabot.yml`, no `renovate.json`, and `cargo deny check` (the
   only advisory check that exists) has no `schedule:` trigger in `ci.yml` —
   it runs only on `push`/`pull_request`. A CVE disclosed against an
   already-merged, otherwise-untouched dependency generates no signal at all
   until the next unrelated PR happens to run CI. `DEPENDENCY_POLICY.md` is
   corrected in place (this review) to say so plainly; the mechanism itself is
   scheduled below.

None of these five are new capabilities to design — they are gaps in the
verification machinery this roadmap already commits to elsewhere. Added as a
new **Phase 12.6 — Testing, Coverage & Performance-Verification Hardening**
(v0.45.3–v0.45.4) directly after Phase 12.5 and before Phase 13, using the
same decimal sub-version mechanism as v0.42.1–v0.42.3 and v0.45.1–v0.45.2 so
that no version from v0.46 onward changes meaning.

### Phase 12.6 — Testing, Coverage & Performance-Verification Hardening

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.45.3 | Coverage-Gate Expansion, Lint-Suppression Cleanup & Scheduled Dependency Audit ✅ Done | Extend the `coverage` CI job's fail-under gate (starting floor set from each crate's own current measured baseline, never below 70%, ratcheting upward release over release — never loosened) to every remaining ungated crate in the workspace: `rockstream-diff`, `rockstream-ops`, `rockstream-storage`, and `rockstream-runtime` first (the hot path between a SQL plan and a durable write), then `rockstream-sql`, `rockstream-control`, `rockstream-connectors`, `rockstream-types`, `rockstream-plan`, `rockstream-sim`, and `rockstream-cli` — all within this same version, not an open-ended future commitment (**found by the 2026-07-11 dependency-integrity review**: the original wording deferred the latter group to an unscheduled "then the remaining crates" with no version or proof line, the same open-ended-promise shape the v0.44/v0.45 reviews flagged elsewhere); fix `Makefile`'s `coverage-gate` target to match the real, CI-enforced flags (`--fail-under-lines 70`/`--fail-under-regions 70`) and add a `conformance_doc_tests`-style lock (mirroring the existing gateway one) so the `Makefile` and `ci.yml` numbers can never silently drift apart again; remove `rockstream-gateway/src/lib.rs`'s crate-wide `#![allow(clippy::all, unused_variables, dead_code)]`, replacing it only with narrow, individually-commented `#[allow(...)]`s where a real false positive exists; add a scheduled (cron, mirroring `simulation-soak.yml`'s pattern) `cargo deny check` workflow independent of PR activity, plus a real `.github/dependabot.yml` (cargo ecosystem, weekly) so `DEPENDENCY_POLICY.md`'s claim becomes true instead of aspirational. | `cargo llvm-cov` fails CI if any of the thirteen workspace crates' coverage drops below its newly-set floor (not just the four hot-path crates); `make coverage-gate`'s output matches `ci.yml`'s actual enforced thresholds exactly, asserted by a new test analogous to `test_coverage_gate_config_is_present`; `cargo clippy --workspace --all-targets -- -D warnings` passes with the gateway's blanket suppression removed and every remaining suppression individually justified; a scheduled workflow run (not just a PR) fails within 24h of a newly-published advisory against an unchanged `Cargo.lock` (simulated by temporarily un-ignoring a resolved advisory in CI); `dependabot.yml` opens a real version-bump PR in a fork smoke-test. | Unit |
| v0.45.4 | Performance-Regression CI Gate & Benchmark Coverage Expansion ✅ Done | Add a `benchmark` CI job that runs the existing `rockstream-ops` criterion suite plus new benches for the three subsystems DESIGN.md §16.6 and this roadmap repeatedly flag as throughput-critical but never benchmark today — SlateDB read/write/merge-operator latency (`rockstream-storage`), exchange/shuffle serialization and credit-flow throughput (`rockstream-runtime`), and control-plane frontier-aggregation throughput at simulated shard/operator counts (`rockstream-control`) — against a checked-in baseline (`critcmp`-compatible JSON, refreshed only via an explicit, code-reviewed `make bench-baseline-update` step), failing the build on a >10% regression per the Common Definition of Done's own long-standing and, until now, entirely unenforced rule. | A deliberately-introduced 15% slowdown in each of the four newly-benchmarked subsystems fails the `benchmark` CI job; an intentional, reviewed improvement updates the baseline via `make bench-baseline-update` and the next run passes; the job completes within a documented, bounded wall-clock budget suitable for per-PR execution. | Unit |

**Usability review: user-facing documentation accuracy (2026-07-11, round 2).**
Round 1 above audited internal CI/test/coverage machinery. This pass audits a
different surface entirely — the `docs/` folder a real evaluator or operator
reads to decide what RockStream can do today — against the same "verify
against real code, don't trust prose" standard this document has applied to
`DESIGN.md` throughout. The result is more serious than round 1's findings:
several docs don't just lag reality, one of them actively overstates it.

- **`docs/language-features.md`'s "Implemented Today" section listed multiple
  SQL statements that do not exist anywhere in `rockstream-sql` or
  `rockstream-gateway`.** Confirmed by grepping the actual parser/lowering
  crate (zero matches for each): `CREATE WORKLOAD`/`WITH (WORKLOAD = ...)`
  (real target: v0.45.1, unstarted), `CREATE SECRET`/envelope
  encryption/KEK config (real target: v0.56, unstarted), user-visible CRDT
  column types (`CREATE TABLE ... (amount COUNTER)`, `MAX_REGISTER`,
  `MIN_REGISTER`, `LWW`, `OR_SET`, `MV_REGISTER` — these directly contradict
  `README.md`'s own explicit "post-1.0 goal... intentionally out of scope for
  the current roadmap" framing and `NEW_IMPLEMENTATION_PLAN.md`'s "Out of
  Scope" list), `WITH RECURSIVE`/semi-naive/DRed recursion (real target:
  v0.50, unstarted), `SHOW RESOURCE USAGE` and its variants plus the
  `rockstream_catalog.view_resource_usage`/`workload_resource_usage` tables
  they read from (real target: v0.45, currently being wired, per this same
  roadmap's own v0.45 row above), and optimistic-conflict detection /
  `RS-2008` (real target: v0.54, unstarted). Not everything in the document
  was wrong — `REBUILD INDEX`, for example, is genuinely implemented,
  confirmed by real parser code and a passing unit test — which is precisely
  what makes a document that mixes verified and fabricated claims worse than
  one that is uniformly stale: a reader has no way to tell which bullet to
  trust without doing the same grep this review just did. **Fixed directly in
  this review**: every claim confirmed false above is moved out of
  "Implemented Today" and into "Documented / Planned Surface" with its real
  target version named, since this is a prose correction (not new
  engineering), following the same precedent as this roadmap's own direct
  `DESIGN.md` §17/§8.7/§12.8 corrections.
- **`docs/cli.md` documents only the v0.1 no-op CLI** — `--storage`/`--role`
  with only two error codes (`RS-0002`/`RS-0003`) — and never mentions
  `--control`, `--auth`, `--metrics-addr`, or `--listen`, nor that the
  `gateway`/`all` roles start a real, long-running PostgreSQL wire server
  (all confirmed present and real in `crates/rockstream-cli/src/main.rs` and
  demonstrated working in `docs/tutorial-30-minutes.md`). This is the
  opposite failure mode from `language-features.md` — understating current
  capability rather than overstating it — but the same root cause: nobody
  re-generates or re-checks `docs/cli.md` against the real `clap` `Command`
  enum as the CLI grows.
- **`docs/configuration.md` contradicts the actual shipped `rockstream.toml`
  and the real `RockstreamConfig` schema** (`crates/rockstream-types/src/
  config.rs`). It invents a `[memory]` section and a
  `checkpoint_retention_duration_sec` key that do not exist in the real
  `ClusterConfig` struct, omits real fields that do exist
  (`dlq_retention_days`, `autotuner`, `index_prefer_selectivity_threshold`,
  `index_max_lag_ms`), and lists defaults that do not match the repository's
  own reference `rockstream.toml` (`min_epoch_ms` 100 vs. the real file's 10;
  `state_budget_gb` 8 vs. 10; `segment_cache_bytes` 1 GB vs. 512 MB;
  `max_rows_per_quantum` 10000 vs. 1000). An operator who configures a
  deployment by copying this doc's example would get a file that doesn't
  parse the way they expect. (Incidentally, `rockstream.toml`'s own header
  comment self-labels it "(v0.50)" and `config.rs`'s self-labels "(v0.49)" —
  neither matches the other, let alone the project's real current version;
  harmless on its own, but a small extra symptom of the same drift.)
- **`docs/sre-operations.md` documents a CLI command that does not exist.**
  It says "the support bundle is generated via `rockstream support-bundle`"
  as a `tar.gz` containing separate `audit.jsonl`/`config.toml`/`metrics.json`
  files. In reality (confirmed directly in `crates/rockstream-cli/src/
  main.rs`, whose only subcommand today is `start`, and in `lib.rs`'s
  `write_support_bundle`): there is no `support-bundle` subcommand at all
  (it is roadmapped as `rockstream support bundle` at v0.55); the bundle is
  written automatically as a side effect of `rockstream start`, as a single
  `support-bundle-<timestamp>.json` file containing `audit_events` and
  `system_info` fields directly — not a tarball, not three separate files.
  This is the exact document an on-call engineer reaches for mid-incident, so
  the mismatch is an operational risk, not just an inaccuracy.

None of this requires new engineering — it requires the same discipline this
roadmap already applies to its own internal documents (`DESIGN.md`,
`NEW_ROADMAP.md`) applied to `docs/`, plus (per the pattern round 1 already
established for coverage numbers) an automated check so it cannot silently
re-drift. Added as a new **Phase 12.7 — User-Facing Documentation Accuracy &
Drift Prevention** (v0.45.5), directly after Phase 12.6 and before Phase 13.

**Follow-up spot-check (2026-07-11, round 3).** A third pass re-examined
operator-facing docs and a few internal engineering conventions, hunting for
issues in the same shape as rounds 1–2 plus some new angles (per-operator
state locking, formal-spec index currency, error-code-enforcement tooling).
Most came back clean, which is useful signal in its own right:
`formal/README.md`'s spec index is accurate and correctly cross-referenced;
`docs/pgwire-conformance.md` already has its own automated conformance-lock
test (`test_conformance_doc_has_linked_tests`), confirming that the fix
pattern proposed for v0.45.5 above is already proven, low-risk, and merely
needs to be replicated rather than invented; `docs/ivm-operators.md`,
`scripts/check-error-codes.sh`, and `rust-toolchain.toml`'s version pin
(matches `ci.yml`'s `toolchain: "1.88"` exactly) all check out; and
`AggregateOp`'s per-operator-instance `Mutex<AggState>` is a correct,
non-bottlenecking design (each operator's epoch processing is already
inherently sequential, and the mutex never spans shards or operators), not a
scalability risk. One small extension of round 2's finding did turn up:
`docs/concepts.md` §32 ("Resource Visibility and Alerts") presents
`SHOW CLUSTER RESOURCE USAGE` and `SELECT * FROM
rockstream_catalog.view_resource_usage`/`.workload_resource_usage` as working
SQL examples with no planned/future caveat — the same not-yet-implemented
v0.45 surface round 2 already found and fixed in `docs/language-features.md`,
now confirmed in a fourth location. Folded into v0.45.5's scope below rather
than opened as its own version, since it is the same fix applied to one more
file.

### Phase 12.7 — User-Facing Documentation Accuracy & Drift Prevention

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.45.5 | Documentation-Reality Reconciliation & Conformance Locks ✅ Done | Complete the line-by-line audit `docs/language-features.md`'s accuracy note (added this review) commits to, plus the same treatment for `docs/cli.md`, `docs/configuration.md`, and `docs/concepts.md` (§32's `SHOW CLUSTER RESOURCE USAGE`/`view_resource_usage`/`workload_resource_usage` example, found by the round-3 follow-up spot-check, needs the same not-yet-implemented caveat): every remaining "Implemented Today" / reference-table / worked-example claim across all four documents is individually re-verified against `rockstream-sql`, `rockstream-gateway`, `rockstream-cli`, and `rockstream-types::config`, and corrected in place. Regenerate `docs/cli.md` from the real `clap` `Command` definition (or generate it, so it cannot drift again) covering every flag (`--control`, `--auth`, `--metrics-addr`, `--listen`) and the real error-code surface; regenerate `docs/configuration.md`'s reference table directly from `RockstreamConfig`'s real field list and `Default` impl (not hand-copied); fix `docs/sre-operations.md`'s support-bundle section to match the real, already-shipped `support-bundle-<timestamp>.json` artifact and remove the nonexistent `rockstream support-bundle` command reference (correct to the roadmapped v0.55 `rockstream support bundle` name, with a note that it is not yet available). **Found by the 2026-07-11 dependency-integrity/testing-sufficiency review**: `docs/cli.md`/`docs/configuration.md`/`docs/concepts.md`/`docs/language-features.md`/`docs/sre-operations.md` were the only five files any prior documentation-drift round ever checked — `docs/distributed-architecture.md` and `docs/grafana-dashboard.json` were never audited, despite the former being the primary operator-facing reference for exactly the frontier/checkpoint/shuffle protocols this roadmap treats as most safety-critical. A spot-check against the real code confirms it has drifted too: §1.2 names a `WorkerAggregator` that does not exist anywhere in `crates/` (the real type is `FrontierAggregator` in `crates/rockstream-control/src/frontier.rs`), and §2.2 says alignment-buffer overflow "surfaces `RS-1601`" when the real code (`crates/rockstream-control/src/checkpoint.rs`) raises `RS-3601` for `CoordinatorError::AlignmentBufferFull`. Add both files to this version's audit scope, correcting the confirmed `WorkerAggregator`/`RS-1601` errors and re-verifying the rest of `distributed-architecture.md` component-by-component against `crates/rockstream-control` and `crates/rockstream-runtime`. Then add the automated lock this review found missing: a `docs_conformance_tests.rs`-style test (mirroring the existing `conformance_doc_tests.rs`/`docs/pgwire-conformance.md` linked-proof-test pattern already proven at v0.42) that parses `docs/language-features.md`'s "Implemented Today" bullets for backtick-quoted SQL keywords and asserts each one is recognized by `rockstream-sql`'s parser, and a second test that diffs `docs/configuration.md`'s documented keys/defaults against `RockstreamConfig::default()` at compile time. | The new conformance tests fail if a future PR adds a keyword to "Implemented Today" that `rockstream-sql` does not parse, or lets `docs/configuration.md` drift from `RockstreamConfig`'s real fields/defaults; `docs/cli.md` and `docs/configuration.md` match `crates/rockstream-cli`/`rockstream-types::config` exactly, verified by a human diff read at sign-off; `docs/sre-operations.md`'s support-bundle section matches the real JSON artifact format byte-for-byte in a worked example; `docs/distributed-architecture.md` names only real types/error codes present in `crates/rockstream-control`/`crates/rockstream-runtime` (`WorkerAggregator`→`FrontierAggregator` and `RS-1601`→`RS-3601` fixed, and no further mismatches remain after a full re-read), and `docs/grafana-dashboard.json`'s panels/metric names are confirmed present in the real Prometheus exporter. | Unit |

**Dependency-integrity review (2026-07-11).** `NEW_IMPLEMENTATION_PLAN.md`'s
Phase 4 exit criteria make the real ≥4-host network throughput test a *binding,
dated* commitment if simulation-waived: "a commitment to run the real-network
test before Phase 8. The real test becomes blocking before production
hardening." Phase 8 is v0.27–v0.31 (Ingestion & The Crucible Soaks) per this
roadmap's own "How This Roadmap Maps to the Plan" table. In reality the test
was never run until **v0.46** (Phase 13) — fifteen versions and every one of
Fault-Tolerant (v0.22), Postgres Pillar (v0.26), and Soaks Complete (v0.31)
past its own committed deadline — and no sign-off between v0.17 and v0.45 ever
flagged the overdue commitment. This is a genuine dependency-integrity gap of
the same shape as the coverage-gate and documentation-drift findings elsewhere
in this document: a binding proof obligation quietly expired without any CI or
sign-off check catching it. v0.46 (below) does now discharge the obligation,
so no further version change is needed, but the **Common Definition of Done**
is amended (see the "Long soaks are gates, not loopholes" paragraph above) to
require that any `Simulation-compensated waiver` record its own committed
version, and that `scripts/check-exit-criteria.sh` fail the build if that
version is marked `✅ Done` without the waived criterion's real-world proof
present in its sign-off — closing the same class of "waiver silently never
redeemed" risk before it can recur for a future phase.

**Formal-verification & error-code compliance review (2026-07-11, fourth
pass).** A fourth review pass — a structured four-area audit (FizzBee
invariant-to-runtime-assertion mapping, error-code registry compliance,
dependency-graph cycle risk for Phase 13's not-yet-built virtual-bucket
routing, and metrics-registry cardinality feasibility for v0.45's own LRU
proposal above) rather than the five-dimension scalability/latency/
throughput/usability/cost lens the three passes above used — found two
compliance gaps serious enough to schedule immediately, one small
doc/reality naming mismatch fixed directly, and one scope clarification each
for v0.45 (above) and v0.47 (below).

1. **The Formal Verification Track table's own CI claim (below) was false.**
   It stated "CI cross-checks that every mapped invariant has both a green
   FizzBee assertion and a present runtime `assert!`." In reality, neither
   `scripts/check-exit-criteria.sh` (verifies a version's sign-off checklist
   is complete, nothing about invariants) nor `scripts/check-path-coupling.sh`
   (verifies only that *some* file under `formal/` changed alongside a
   `rockstream-{runtime,control,connectors,storage}`/DESIGN.md change — a
   path-coupling gate, not an invariant-coverage gate) parses a single
   invariant ID or checks for a paired Rust assertion. Corrected in place
   below — same "fix the false claim directly, schedule the real fix"
   precedent as the §8.7/§12.8 stale `Ships:` corrections and the v0.42.2
   coverage-gate correction.
2. **`M2-S3` (`M2_S3_SinglePublisherSafety` — at most one frontier-aggregator
   publisher acts as leader-writer via its fencing token) has no runtime
   assertion anywhere in `crates/`**, confirmed workspace-wide, not just in
   `rockstream-control`. This is not a new model to write — `formal/
   m2_frontier_agg.fizz` already models and passes it, and
   [FIZZBEE_TEST_PLAN.md](FIZZBEE_TEST_PLAN.md) §3.7's own delivery-artifact
   table already prescribes exactly what to build ("assert single CAS-holder
   of `frontier/leader`; assert `sync:true` flush before lease-handoff
   read") — it was simply never implemented, unlike `M2-S1`/`S2`/`S4`, which
   `crates/rockstream-control/src/frontier.rs` does enforce (`M2-S4` with a
   literal defensive `assert!`; `M2-S1`/`S2` structurally, by only ever
   publishing a frontier that cannot exceed the true meet of registered
   shard epochs). `M1-S1`/`S3`/`S4`/`S5`, `M4-S4`, and `M5-S1` also have zero
   code-level reference anywhere in the workspace; `M2-S3` is the only one of
   these paired with both a passing FizzBee model and an already-prescribed
   Rust-side design, which is why it is scheduled as concrete new code below
   rather than left to the new audit script's own triage pass.
3. **Naming/reality mismatch, fixed directly (no version needed)**: this
   document's own v0.1 row and `NEW_IMPLEMENTATION_PLAN.md`'s Phase 0 section
   both described a `rockstream-errors` registry crate. No such crate has
   ever existed — the real, working registry is
   `crates/rockstream-types/src/error_code.rs` (an `ErrorCode` newtype plus
   ~100 registered `RS-XXXX` constants, exactly as `scripts/
   check-error-codes.sh`'s own comments already correctly describe). Both
   references are corrected in place.
4. **`scripts/check-error-codes.sh` only checks `error!()` log call sites,
   never `Err(...)` construction.** Every structured `tracing::error!()` call
   in the workspace does carry a code (the script genuinely enforces this,
   and genuinely passes) — but `Result`-returning parse/validation errors
   built as ad hoc strings are entirely invisible to it. Confirmed concrete
   instances: `crates/rockstream-gateway/src/copy_state.rs`'s
   `parse_copy_from_stmt` and `server.rs`'s `CREATE SINK` option parsing
   return bare `format!("...")` strings with no `RS-XXXX` code at all;
   separately, `crates/rockstream-runtime/src/exchange/durable.rs` reuses the
   single code `RS_3010` across at least six distinct failure modes
   (rate-limit exhaustion, generic object-store failure, buffer-capacity
   exceeded, serialization failure, undersized footer, invalid footer
   length) — every one carries *a* code, just not a distinguishing one, and
   being `Err(format!(...))` rather than `error!()`, today's script would not
   catch a regression here either way.

Both compliance gaps get their own version below — **Phase 12.8 — Invariant
& Error-Code Compliance Hardening** (v0.45.6–v0.45.7), using the same
decimal-sub-version mechanism as Phases 12.5–12.7, so nothing from v0.46
onward is renumbered. Two scope clarifications land on existing rows instead
of new versions: **v0.45's own metrics-cardinality LRU proposal above
(`pipeline_id="other"` rollup) is a structural rewrite of `MetricRegistry`
(`crates/rockstream-types/src/metrics.rs`), not a config change** — its
per-label maps are plain `HashMap<String, Counter>` with no ordering,
recency-tracking, or eviction API today (the only removal path is a full,
test-only `reset_all()`); an LRU cap needs an ordered/bounded structure
(e.g. promoting the already-transitively-present `lru` crate, pulled in
today via `slatedb`, to a direct dependency) plus a rollup accumulator on
every per-pipeline write path and the Prometheus exporter — noted directly
in v0.45's scope above so the version is not under-estimated as a pure
config toggle. And **v0.47 below (Hot-Key Virtual Buckets) gets an explicit
architecture constraint**: the adaptive hot-key-detection/re-splitting logic
it schedules must be implemented in `rockstream-control`, never in
`rockstream-plan`, to avoid an unbuildable dependency cycle with
`rockstream-ops` — see the constraint noted directly in that row.

### Phase 12.8 — Invariant & Error-Code Compliance Hardening

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.45.6 | Formal-Verification Invariant Coverage & Real CI Cross-Check ✅ Done | Add the missing `M2-S3` runtime assertion to `crates/rockstream-control` (fencing-token CAS check on the frontier-aggregator's publisher role, plus a `sync:true`-flush-before-lease-handoff-read assertion, per FIZZBEE_TEST_PLAN.md §3.7's already-prescribed design — no new FizzBee model needed, `formal/m2_frontier_agg.fizz` already models and passes `M2_S3_SinglePublisherSafety`). Build the real CI cross-check the Formal Verification Track table has always claimed exists: a new `scripts/check-invariant-pairs.sh` that parses every named invariant out of `formal/*.fizz`, greps `crates/` for a comment referencing that exact invariant ID, and fails if any invariant modeled in a currently-green `.fizz` file has no Rust-side reference — with a structured escape hatch (a `// INVARIANT-BY-CONSTRUCTION: <ID> — <reason>` comment, for cases like `M2-S1`/`S2` where the guarantee is structural rather than a redundant runtime `assert!`) so the script does not force pointless defensive asserts onto code that is already correct by construction. Run the new script once against the current tree and triage every invariant it flags among `M1-S1`/`S3`/`S4`/`S5`, `M4-S4`, `M5-S1`: each gets either a real `assert!`/`debug_assert!` or a justified `INVARIANT-BY-CONSTRUCTION` comment — none may be left with neither. | `scripts/check-invariant-pairs.sh` runs in CI and fails on a deliberately-reverted `M2-S3`/`M4-S1` assertion-removal test case; it passes on the real tree only after every one of `M1-S1`/`S3`/`S4`/`S5`/`M2-S3`/`M4-S4`/`M5-S1` carries either a runtime check or a reviewed `INVARIANT-BY-CONSTRUCTION` comment; the Formal Verification Track table's CI claim is true, not aspirational; a real 3-aggregator `SimRuntime` scenario proves a stale-fenced publisher can never re-publish a frontier after a new leader's token supersedes it. | Unit |
| v0.45.7 | Error-Code Registry Enforcement Hardening ✅ Done | Fix `crates/rockstream-gateway/src/copy_state.rs`'s `parse_copy_from_stmt` and `server.rs`'s `CREATE SINK` option-parsing errors to return a registered `RS-XXXX` code instead of a bare string (new codes as needed); split `crates/rockstream-runtime/src/exchange/durable.rs`'s single overloaded `RS_3010` into distinct codes per failure mode (rate-limit exhaustion, object-store I/O failure, buffer-capacity exceeded, serialization failure, corrupt/undersized footer) so operators can distinguish them without reading source. Extend `scripts/check-error-codes.sh` (or add a companion script invoked alongside it in CI) to also flag `Err(format!(...))`/`Err(String::from(...))`/bare-string `Result` construction sites lacking an `RS-XXXX` reference — propagated (`?`-operator) errors and errors already wrapping a coded type remain exempt; only newly constructed ad hoc errors are in scope. | The extended `check-error-codes.sh` (or its companion) fails CI on a deliberately-reintroduced bare `Err(format!("boom"))` anywhere in `crates/`; `copy_state.rs` and the `CREATE SINK` option parser's error paths each carry a distinct, registered code with actionable `next_steps` text (new `copy_error_code_tests.rs`); `durable.rs`'s six previously-`RS_3010`-only failure modes are independently distinguishable in logs/responses (new `durable_shuffle_error_code_tests.rs`). | Unit |

### Phase 13 — Elastic Scaling & Skew Handling

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.46 | Online Shard Migration & Elasticity ✅ Done | Before any splitting/rebalancing code is written, model-check the shard-migration protocol in FizzBee (binding pre-implementation requirement from v0.18): **M6 model** `formal/m6_shard_migration.fizz` with safety invariants (no dual-writer window — a bucket is authoritative on exactly one shard at any committed epoch; no lost writes across `DUAL_WRITING`→`CUTOVER`; `GC_ELIGIBLE` never fires before the migration's consumer frontier passes cutover) and liveness (`migration_state` always eventually reaches `DONE` or `ABORTED`, never stuck). Implement DESIGN.md §10.1–§10.4's full migration state machine (`PLANNED → SNAPSHOTTING → COPYING → DUAL_WRITING → CATCHING_UP → FENCING_OLD → CUTOVER → VERIFYING → GC_ELIGIBLE → DONE`, with per-state timeout budgets and `ABORTED` rollback through `VERIFYING`) for online shard add/remove, and the §10.7 worker-drain protocol (`DRAINING` → `DECOMMISSIONED`). This is the prerequisite substrate the (already-drafted) v0.55 admin CLI's `shard {list,migrate}` and `cluster workers drain` commands assume exists — until this version, they would have been thin wrappers over nothing. Also closes the Phase 4 exit-criteria waiver: the original real ≥4-host network throughput test (waived at Phase 4 with a `SimNetwork`-only proof and "a commitment to run the real-network test before Phase 8") is executed here for the first time against a live shard migration, since no prior sign-off records it having been run. | `formal/m6_shard_migration.fizz` is green at CI-fast bounds (all safety and liveness invariants hold; any counterexample archived in `formal/findings.md` and replayed as a permanent `SimRuntime` regression seed); a real ≥4-host cluster (TC, real network) migrates a bucket range from a donor to a recipient shard with zero reads or writes lost, verified by a scan comparing donor/recipient state at cutover; killing the donor mid-`DUAL_WRITING` and mid-`CUTOVER` both recover correctly (`ABORTED` rollback or completed `DONE`, never a stuck or split-brained bucket); `rockstream cluster workers drain <id>` (added as a CLI stub here, ahead of the rest of the admin CLI at v0.55) completes a full worker drain with zero shard downtime; the previously-waived Phase 4 real-network commitment is marked closed in this sign-off. | Unit, LFS, MinIO, TC |
| v0.47 | Hot-Key Virtual Buckets, Proactive Shard Splitting & Autoscaling Signals ✅ Done | Implement DESIGN.md §10.5's hot-key detection (per-key CPU/bytes/state-write tracking against `hot_key_factor`) and virtual-bucket salting with the final unsalted combiner, including the non-composable-law exception (`SKEW_BOUND_NON_COMPOSABLE` routing to a single spill shard for laws like `WeightAdd/v1`/DISTINCT); §10.6's proactive shard splitting at `1.5 × target_shard_state_bytes` (built on v0.46's migration machinery) and its reverse (cold-shard merge at the `min_shard_state_bytes` floor); the "adaptive skew splitting" control loop from §14.5, which DESIGN.md currently (incorrectly) documents as already on by default; and §10.8's `cluster_worker_pressure`/`demanded_shard_count`/`placed_shard_count` Prometheus export with a real Kubernetes HPA (or KEDA) integration test proving an actual scale-out and scale-in happen from these signals, not just that the metrics exist. **Architecture constraint (2026-07-11 compliance audit)**: the adaptive hot-key-detection and re-splitting decision logic above must be implemented in `rockstream-control` — DESIGN.md §10.5 already names the control plane as the decision-maker — and never in `rockstream-plan`; `rockstream-ops`/`rockstream-sql`/`rockstream-diff` already depend on `rockstream-plan` for its `PlanNode`/`OpNode` IR, so a `rockstream-plan → rockstream-ops` edge (e.g. to read live per-operator skew statistics) would close an unbuildable cycle. `rockstream-plan` is limited to the deterministic virtual-bucket `OpNode` variants and the pure rendezvous-hash routing function; `rockstream-control` takes a new, verified-safe dependency on `rockstream-plan` to emit/mutate them. | A synthetic hot key at 50× the median shard's load is detected and split into virtual buckets within `hot_key_factor` breach + 30s, with output remaining bit-identical to the non-split oracle; a shard crossing `1.5 × target_shard_state_bytes` splits in the background with zero operator action and zero pipeline downtime; a non-composable aggregate (DISTINCT) on a hot key correctly routes to a single spill shard and never double-emits under bucket salting (oracle property test); a real k8s cluster with HPA configured against the exported `cluster_worker_pressure` metric adds a worker within 2 minutes of a sustained 10× shard-demand spike and removes it within 10 minutes of the spike ending. | Unit, LFS, MinIO, TC |

### Phase 14 — Network Efficiency & Advanced DML

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.48 | Advanced DML & Scatter Pruning | `UPDATE … RETURNING` and `DELETE … RETURNING` (read-modify-write semantics); piggyback min/max bounds and Bloom filters onto the frontier summary to prune unneeded shards during multi-shard point lookups. | Multi-shard point reads safely bypass >90% of shards based on frontier summary Bloom filters. | Unit, LFS, MinIO, TC |
| v0.49 | Zero-Copy IPC, AZ-Aware Shuffle & Shuffle Compression | Upgrade same-host gRPC loopbacks to Apache Arrow Flight Shared Memory; make the hierarchical exchange subsystem aware of physical availability zones (AZs) to eliminate cross-AZ egress during shuffle. **Found missing during the 2026-07-11 network-efficiency review**: DESIGN.md §7.3 documents the `shuffle_outbox/`/`shuffle_inbox/` wire value as "Arrow IPC batch (compressed)" as if already decided, but no compression codec is implemented anywhere in `crates/` and no roadmap version ever scheduled it (the workspace `tonic` dependency enables no compression feature). Add pluggable shuffle-payload compression, selected automatically by the same path classifier that already picks `elided`/`loopback`/`direct`/`durable` (§7.5): a low-CPU-overhead codec (LZ4) on the latency-sensitive direct/loopback gRPC path, and a higher-ratio codec (ZSTD) on the durable object-store shuffle path and cluster checkpoints; never adding a compression step to the `elided` path, since it already skips serialization entirely. | Zero byte-copying observed in CPU profiles for same-host worker exchanges; cross-AZ traffic drops to near zero during shuffle phases; shuffle-payload compression reduces measured cross-worker network bytes by ≥40% on a representative wide-shuffle TPC-H/Nexmark workload with zero divergence from the oracle, and per-epoch CPU overhead on the direct gRPC path stays within the SLO budget (the auto-tuner can disable compression per exchange if it does not). | Unit, LFS, MinIO, TC |

### Phase 15 — Complex Analytics & Compute Tuning

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.50 | Advanced Streaming Analytics | SQL compiler support for recursive CTEs (`WITH RECURSIVE`) for graph algorithms and fixed-point IVM, lateral joins for nested JSON/arrays, and hopping/session windows. | Transitive closures and sessionization queries incrementally maintain state correctly against the correctness oracle. | Unit, LFS, MinIO, TC |
| v0.51 | Hot-Path Compute Optimizations | WAL elision for derived intermediate operator shards; link `max_rows_per_quantum` directly to network buffer depth to provide tight backpressure coupling. | Throughput on complex DAGs increases by >30% due to reduced intermediate WAL write amplification. | Unit, LFS, MinIO, TC |

### Phase 16 — Declarative Data Governance

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.52 | Inline Expectations & Lineage Diagnostics | `CREATE EXPECTATION` syntax; specialized "Expectation Operator" injected into the DAG to evaluate rows and zero out Z-set weights for failed records before they reach `ViewSink`; hooks into `EXPLAIN INCREMENTAL ANALYZE`. | Malformed records injected into upstream sources never reach downstream `ViewSink` outputs. | Unit, LFS, MinIO, TC |
| v0.53 | DLQ Routing & State Degradation | Transactionally forward failed rows to an internal base-table shard (canonical Dead Letter Queue); implement state degradation policies (`warn`, `degrade`, `block`); guarantee exactly-once processing for failed rows. **Found missing from any roadmap version during the >=v0.44 review**: DESIGN.md §13.3.1 fully designs a *separate* connector-tier per-record decode-failure DLQ — `rockstream_catalog.dead_letter_queue` (`arrived_at`/`source_name`/`source_offset`/`error_code`/`error_message`/`raw_bytes_hex`/`replay_attempt` columns), `ALTER SOURCE ... REPLAY DEAD_LETTER_QUEUE [SINCE .. UNTIL ..]`, `ALTER SOURCE ... DISMISS DEAD_LETTER_QUEUE WHERE ...`, the `RS-1003 connector.decode_error` / `RS-1004 connector.dlq_growing` codes, and `DLQ_RETENTION`/`dlq_warn_threshold` source options — none of which had ever been scheduled (only the `dlq_warn_threshold` config field exists today, in `crates/rockstream-types/src/config.rs`, unused). Ship this alongside the expectation-failure DLQ from v0.50, reconciling both onto `rockstream_catalog.dead_letter_queue` with a `failure_source` (`connector` \| `expectation`) discriminant column so the schema serves both without ambiguity. | Failed records are durably queryable in `rockstream_catalog.dead_letter_queue` alongside exactly-once commit boundaries; a source's decode failures are independently replayable/dismissable via `ALTER SOURCE ... REPLAY\|DISMISS DEAD_LETTER_QUEUE` (new `dlq_connector_tests.rs`); exceeding `dlq_warn_threshold` emits `RS-1004` as a `NOTICE`; `replay_attempt` increments correctly across repeated replays. | Unit, LFS, MinIO, TC |

### Phase 17 — Enterprise Validation & 1.0 Finalization

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.54 | Isolation & Validation Hooks | Validate non-CRDT exact-key writes against per-row versions to prevent blind overwrites; support single-shard `SERIALIZABLE LOCAL` isolation via SlateDB transactions for standard ACID transactional workflows. | Concurrent conflicting writes to the same key on a single shard correctly trigger serialization anomalies/aborts. | Unit, LFS, MinIO, TC |
| v0.55 | Admin CLI, Arrangement Debugger & Resource Visibility | **Found missing in the <=v0.42.3 review**: DESIGN.md §14.7/§14.7.1/§14.19 fully specify an operator CLI surface and an IVM arrangement debugger, but no roadmap version ever implemented them — `rockstream-cli` still only has a `start` subcommand as of v0.42.3 (shard migration/drain now ships at v0.46, `EXPLAIN INCREMENTAL`/`SHOW RESOURCE USAGE` reachability now ships at v0.45, and the workload quota/admission-control substrate now ships at v0.45.1; this version wraps the remaining workload/view/schema/source/checkpoint/audit/support-bundle/debug-arrangement commands). Ship the full surface as thin wrappers over already-shipped control-plane/catalog/`DbReader` APIs (no new services, no new config files, one binary): `workload {list,show,create,alter,drop}`, `view {list,show,query,subscribe,pause,resume,status}`, `schema {list,show,create,drop}`, `source {list,show,pause,resume,drop}`, `explain <view> [--estimate]`, `cluster {status,workers,quotas}` and `cluster workers {list,drain,status}`, `shard {list,migrate}`, `checkpoint {list,restore}`, `resource {usage,usage --workload=<name>,cluster}` (§14.19), `schema-evolution {status,history}`, `audit {tail,query}`, `support bundle [--view=<name>]`, and `sql "<query>"` (parse/lower/`EXPLAIN INCREMENTAL` against the catalog without deploying). **IVM arrangement debugger** (§14.7.1): `rockstream debug arrangement <view> <op_id> <key> [--epoch=N]` reads live or historical (within checkpoint retention) intermediate Z-set state via `DbReader` without touching the live pipeline — the tool an operator reaches for when a view is *wrong*, not just slow. Every subcommand supports `--json` for scripting and human-readable text by default; `SHOW VIEW STATUS`/`SHOW RESOURCE USAGE` are wired through pgwire as the SQL-native equivalents. | An operator reproduces and fixes a "view has a wrong answer" incident using only `rockstream explain`, `rockstream debug arrangement`, and `rockstream cluster workers drain` — no source reading, no ad-hoc scripts (new `cli_operator_scenarios_tests.rs`); `debug arrangement orders_mv agg_op_3f2a "product_id=42"` returns correct live state without blocking the pipeline, and `--epoch=N` returns bit-identical historical state within retention; `support bundle` produces a secret-redacted diagnostic archive; every subcommand has a golden-output test and is documented in `docs/cli.md`. | Unit, LFS, TC |
| v0.56 | mTLS Everywhere, Secrets Management & Independent Security Review | **Found missing in the <=v0.42.3 review**: the original implementation plan's Phase 8 exit criteria required "mTLS everywhere", `CREATE SECRET` envelope encryption (DESIGN.md §14.18), and an independent security review before production readiness, but no roadmap version through v0.52 ever scheduled them — only gateway-facing auth (`--auth=scram\|md5\|oidc\|mtls\|off`, v0.26/v0.40) exists today. Ship internal mTLS (control↔worker, worker↔worker gRPC/shuffle) with documented certificate rotation; `CREATE SECRET`/`ALTER SECRET`/`DROP SECRET`/`SHOW SECRETS` DDL with envelope encryption across `env`/`aws_kms`/`gcp_kms`/`vault` KEK backends; worker-side short-lived secret-token resolution derived from mTLS node identity (workers never read raw secret values); `CREATE SOURCE ... secret = <name>` / `CREATE SINK ... secret = <name>` replacing inline plaintext credentials; zero-restart secret rotation; full audit coverage of every secret lifecycle event (value never logged, never in support bundles). Commission an independent third-party security review of the complete attack surface (auth, RBAC, secrets, mTLS, gateway SQL-injection surface, TLS rotation); triage and fix every P0/P1 finding. | A worker or peer connection presenting an invalid/absent mTLS client cert is rejected with an audited denial and never processes shard data; `CREATE SECRET kafka_prod (...)` never appears in plaintext in logs, audit events, or support bundles (new `secrets_redaction_tests.rs`); `ALTER SECRET` rotates a live Kafka source's credentials with zero pipeline restarts (TC); the independent security review's report is attached to the sign-off and every P0/P1 finding is closed. | Unit, LFS, MinIO, TC |
| v0.57 | Rolling Upgrade Proof & Disaster Recovery | **Found missing in the <=v0.42.3 review**: DESIGN.md §5.5 fully designs a storage-format version gate, a `rockstream migrate` tool, and an N/N+1 rolling-upgrade contract, and the original implementation plan required "a documented disaster-recovery procedure executed successfully", but no roadmap version ever implemented or end-to-end tested either. Ship `rockstream migrate --from=N --to=M --storage=<url>` as the offline shard-format migration tool; an end-to-end mixed-version cluster test proving the rolling-upgrade contract: the shard-format-version gate (`RS-5001`) refuses to open out-of-range shards; the gRPC `protocol_version` header gate rejects incompatible peers under its own dedicated error code (resolving the `RS-5002` numbering collision between `merge.unknown_law` and `protocol.version_not_supported` flagged during this review — protocol-version rejection is reassigned to `RS-5021`); the control plane withholds cross-version pipeline/shard assignment until enough N+1 workers are available, verified by a `SimRuntime` mixed-version scenario and a real two-binary-version TestContainers upgrade drill. **Disaster recovery**: cluster-checkpoint **export** to a distinct bucket/region (not just in-place crash recovery) plus a **restore-into-a-new-cluster** procedure; a `docs/disaster-recovery.md` runbook covering full-region-loss recovery, target RPO/RTO, and a periodic restore-drill recommendation. | A 3-worker cluster rolls from binary N to N+1 one worker at a time with zero epoch loss and zero downtime for in-flight pipelines (new `rolling_upgrade_tests.rs`, TC with two built binaries); a worker on an out-of-range shard format is refused with `RS-5001`; an incompatible-protocol peer is rejected with `RS-5021`, never the collided `RS-5002`; a full-cluster checkpoint exported to a second bucket is restored into a freshly provisioned cluster in a separate region/namespace, reproducing pre-disaster state bit-identically, executed as a documented and rehearsed drill. **Operationally Complete** milestone. | Unit, LFS, MinIO, TC |
| v0.58 | Simulator Maturity & Auto-Tuning Lock | Finalize shift from bounded defaults to fully SLO-driven adaptive control loops; close final known testing gap for external-system edge cases. | Simulator accurately reproduces and recovers from all remaining external-system edge-cases. | Unit, LFS, MinIO, TC |
| v0.59 | v1.0 Release Candidate (RC1) | Activate all features from v0.1 through v0.58 simultaneously; run comprehensive chaos, performance, and scaling soak under maximum cluster pressure within a single cloud region. **v1.0 Release** milestone → tag `v1.0.0`. | No P0 or P1 bugs discovered during a 2-week continuous automated chaos cycle. | Unit, LFS, MinIO, TC |

---

## Formal Verification Track (FizzBee)

[FIZZBEE_TEST_PLAN.md](FIZZBEE_TEST_PLAN.md) is the authoritative specification
for design-time formal verification. Because v0.1–v0.17 shipped before the
FizzBee toolchain existed, its Phase 0 toolchain and the M1 epoch-commit model
are folded into v0.18 as catch-up; the remaining models are then woven into the
versions that build each protocol — always **modeled before the Rust code**, so
the design is verified before the implementation exists.

| Version | FizzBee deliverable | Models / specs | Invariants | Gate |
|---|---|---|---|---|
| v0.18 | Toolchain bootstrap (D0.1–D0.4) + M1 retro-model (D1.1–D1.5) + M2 scalar model (D5.1–D5.3) | `formal/conventions.md`, `formal/m1_epoch_commit.fizz`, `formal/m2_frontier_agg.fizz`, `fizz.yaml`, `formal-verify` CI job | M1-S1…S5, M1-L1, COV-M1; M2-S1…S4, M2-L1…L2, COV-M2 | `make verify` green; M1 + M2 models green; paired `assert!`s in `rockstream-storage`, `-runtime`, `-control` |
| v0.19 | M2 multi-source antichain variant (D5.4–D5.5) | `formal/m2_frontier_agg.fizz` (vector frontier) | M2-S1 over the vector `FreshnessToken` | Multi-source meet model green; §3.7 M2 rows populated; `SimRuntime` reordering mirror passes |
| v0.20 | M4 self-fencing model (D6.1–D6.2) | `formal/m4_self_fencing.fizz` | M4-S1…S4, M4-L1…L2, COV-M4 | M4 model green (justifies writer re-election); paired `assert!`s in `rockstream-runtime` |
| v0.21 | M3 sink-2PC model (D6.3–D6.4) + M1 duplication variant (D6.5) | `formal/m3_sink_2pc.fizz` (×3 idempotency profiles), `formal/m1_epoch_commit.fizz` (duplication) | M3-S1…S4, M3-L1, COV-M3; M1-S5 under duplication | M3 model green (M3-S3 composed with M1 `cluster_committed`); paired `assert!`s in `rockstream-connectors`, `-runtime` |
| v0.22 | Continuous verification + findings (D6.6, DC.1–DC.4) | all `.fizz` specs, `formal/findings.md` | all M1–M4 safety + liveness | All four models green at CI-fast and relaxed bounds; path-coupling check live; counterexamples replayed forever as regression seeds |
| v0.42.1 | Toolchain remediation — CI never actually ran the models (found by <=v0.42 review) | `.github/workflows/ci.yml` `formal-verify` job, `Makefile` `verify`/`verify-relaxed`, `formal/m3_sink_2pc.fizz`, `formal/m4_self_fencing.fizz`, `formal/m1_epoch_commit.fizz` | Re-verifies M1-S5 (currently failing), M3-S1…S4 (spec currently crashes), M4-S1…S4 (spec currently crashes) | `make verify` genuinely green for all five specs in CI, on pull requests, with `continue-on-error` removed |
| v0.43 | M5 cold-tier exactly-once model (pre-implementation) | `formal/m5_cold_tier_sink.fizz` | M5-S1 (no duplicates under partial-write fault), M5-S2 (no data loss), M5-S3 (manifest-pointer atomicity), M5-L1 (`committed_epoch` always advances), COV-M5 | M5 model green at CI-fast bounds; all safety and liveness assertions hold; paired `assert!`s added to `rockstream-connectors`; any counterexample archived in `formal/findings.md` and replayed as a permanent `SimRuntime` regression seed |
| v0.45.2 | M7 control-plane HA model (pre-implementation) | `formal/m7_control_plane_ha.fizz` | M7-S1 (no dual leader — at most one control node holds the writer lease at any time), M7-S2 (no lease/workload-write/shard-assignment accepted from a non-leader), M7-S3 (composed with M4's shard-fencing predicate — a leadership change never races an in-flight shard fence), M7-L1 (a live leader always exists eventually after a kill), COV-M7 | M7 model green at CI-fast bounds; all safety and liveness assertions hold; paired `assert!`s added to `rockstream-control`; any counterexample archived in `formal/findings.md` and replayed as a permanent `SimRuntime` regression seed |
| v0.46 | M6 shard-migration model (✅ Done, pre-implementation gate) | `formal/m6_shard_migration.fizz` | M6-S1 (single authoritative shard per bucket at any committed epoch — no dual-writer window), M6-S2 (no lost writes across `DUAL_WRITING`→`CUTOVER`), M6-S3 (`GC_ELIGIBLE` never precedes the migration consumer frontier passing cutover), M6-L1 (`migration_state` always reaches `DONE` or `ABORTED`), COV-M6 | M6 model green at CI-fast bounds; all safety and liveness assertions hold; paired `assert!`s added to `rockstream-control`; any counterexample archived in `formal/findings.md` and replayed as a permanent `SimRuntime` regression seed |
| v0.57 | Rolling-upgrade / mixed-version cluster scenario | `SimRuntime` mixed-version scenario (not a new `.fizz` model — the version-gate logic is a simple monotonic refuse-if-incompatible check, not a new distributed race) | Cross-version pipeline assignment never happens until enough N+1 workers are available; no epoch loss during a simulated rolling upgrade | Mixed-version `SimRuntime` scenario passes; real two-binary TestContainers upgrade drill passes |
| v0.23–v0.59 | Continuous `formal-verify` + path-coupling (DC.1–DC.2); pre-release relaxed-bounds sweep (DC.4) | all `.fizz` specs | all M1–M7 | A coordination-protocol change without a model touch fails CI; the v0.59 RC1 gate re-runs the relaxed-bounds sweep |

Every row above maps each FizzBee invariant to a paired runtime `assert!` per
[FIZZBEE_TEST_PLAN.md](FIZZBEE_TEST_PLAN.md) §3.7. **Correction (2026-07-11
compliance audit)**: the sentence that used to stand here claimed "CI
cross-checks that every mapped invariant has both a green FizzBee assertion
and a present runtime `assert!`" — that has never been true; neither
`scripts/check-exit-criteria.sh` nor `scripts/check-path-coupling.sh` parses
invariant IDs or verifies a paired assertion (path-coupling only checks that
*some* `formal/` file changed alongside a coordination-protocol change). The
real cross-check is built at **v0.45.6** (Phase 12.8 below), which also
closes the one invariant this audit found with a passing FizzBee model but no
Rust-side counterpart at all: `M2-S3`. A FizzBee counterexample for any
protocol becomes a named `SimRuntime` regression seed before the model is
fixed, and is replayed on every build thereafter.

**Scheduling-risk note (2026-07-11 architecture review): M6/M7 collapse
model-then-code into one version, unlike M1–M5.** Every prior model (M1–M5)
either got its own dedicated pre-implementation version (M5: modeled at v0.43,
implemented at v0.44 — a full version apart) or was modeled in a version that
precedes the version implementing the protocol it gates (M4: modeled v0.20,
self-fencing code v0.21). M6 (shard migration) and M7 (control-plane HA)
instead schedule "model-check in FizzBee" *and* "implement the [10-state
migration state machine / 3–5-node Raft group]" as scope within the *same*
single ~6-person-week version (v0.46 and v0.45.2 respectively). Given M4's own
history — its exhaustive state space did not terminate at committed bounds and
took three follow-up sub-versions (v0.42.1–v0.42.3) to close — a new model of
comparable or greater complexity (M6 models a 10-state machine; M7 models
multi-node leader election) sharing a version with its own Rust implementation
risks either the model-checking step being rushed to protect the
implementation schedule, or the version silently overrunning its budget the
way M4 did. Recommend splitting v0.46 into a model-only version followed by an
implementation version (and likewise for v0.45.2) if either model's state
space does not terminate quickly at CI-fast bounds, rather than compressing
both into one version under schedule pressure.

---

## How This Roadmap Maps to the Plan

| Plan phase | Roadmap versions | FizzBee model (FIZZBEE_TEST_PLAN.md) |
|---|---|---|
| Phase 0 — Foundation | v0.1 – v0.3 | Toolchain folded into v0.18 (catch-up) |
| Phase 1 — Single-Shard IVM Core | v0.4 – v0.6 | M1 epoch-commit (retro-modeled in v0.18) |
| Phase 2 — SQL Frontend & Joins | v0.7 – v0.10 | — |
| Phase 3 — Essential Operators & Soak | v0.11 – v0.14 | — |
| Phase 4 — Multi-Shard & Exchange | v0.15 – v0.17 | — |
| Phase 5 — Frontier Protocol | v0.18 – v0.19 | M2 frontier aggregation |
| Phase 6 — Fault Tolerance & Exactly-Once | v0.20 – v0.22 | M3 sink 2PC, M4 self-fencing |
| Phase 7 — PostgreSQL Wire Gateway | v0.23 – v0.26 | Continuous verification |
| Phase 8 — Ingestion & The Crucible Soaks | v0.27 – v0.31 | Continuous verification + relaxed-bounds sweep |
| Phase 9 — Operational HTAP Ergonomics | v0.32 | Continuous verification |
| Phase 10 — Nexmark Correctness Suite | v0.33 – v0.36 | Continuous verification |
| Phase 11 — PostgreSQL Wire Protocol Hardening | v0.37 – v0.42 | Continuous verification |
| Phase 11.5 — Post-v0.42 Remediation | v0.42.1 – v0.42.3 | Toolchain remediation (v0.42.1); M4 state-space & liveness closure (v0.42.3) |
| Phase 12 — The Data Lake Bridge & FinOps | v0.43 – v0.45 | Continuous verification |
| Phase 12.5 — Control-Plane Hardening & Multi-Tenancy | v0.45.1 – v0.45.2 | M7 control-plane HA |
| Phase 12.6 — Testing, Coverage & Performance-Verification Hardening | v0.45.3 – v0.45.4 | — (CI/tooling hardening; no new coordination protocol) |
| Phase 12.7 — User-Facing Documentation Accuracy & Drift Prevention | v0.45.5 | — (documentation reconciliation; no new coordination protocol) |
| Phase 12.8 — Invariant & Error-Code Compliance Hardening | v0.45.6 – v0.45.7 | M2-S3 (already modeled in `formal/m2_frontier_agg.fizz`; no new model) |
| Phase 13 — Elastic Scaling & Skew Handling | v0.46 – v0.47 | M6 shard migration |
| Phase 14 — Network Efficiency & Advanced DML | v0.48 – v0.49 | Continuous verification |
| Phase 15 — Complex Analytics & Compute Tuning | v0.50 – v0.51 | Continuous verification |
| Phase 16 — Declarative Data Governance | v0.52 – v0.53 | Continuous verification |
| Phase 17 — Enterprise Validation & 1.0 Finalization | v0.54 – v0.59 | Continuous verification + relaxed-bounds sweep at RC1; v0.57 additionally proven by a `SimRuntime` mixed-version scenario |

Fifty-nine versions at ~6 person-weeks each is the full path from an empty
repository to a production-ready, enterprise-grade 1.0. The order is fixed:
correctness on one shard is proven before distribution, distribution is made
fault-tolerant before the Postgres layer depends on it, ingestion connectors
and crucible soaks validate real-world pressure before HTAP ergonomics, the
Nexmark suite certifies end-to-end correctness (including retraction/Z-set semantics) on the full stack,
PostgreSQL wire protocol hardening (v0.37–v0.39) and end-user certification (v0.40–v0.42) ensure any standard driver works without workarounds and that a complete application can be built on the wire protocol alone,
and the data lake bridge (with its FizzBee pre-model and simulator fidelity foundation at v0.43), control-plane hardening and multi-tenancy (v0.45.1–v0.45.2), invariant and error-code compliance hardening (v0.45.6–v0.45.7), elastic shard migration and hot-key/skew handling (v0.46–v0.47), network optimizations, complex analytics, and data
governance layers, an operator-grade CLI and arrangement debugger, internal mTLS and secrets management, and a proven rolling-upgrade/disaster-recovery story close out 1.0 through v0.59.

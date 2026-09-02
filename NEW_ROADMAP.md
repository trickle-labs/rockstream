# RockStream Focused Roadmap

This roadmap turns [NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md)
into an ordered, evidence-producing build sequence. It complements:

- [DESIGN.md](DESIGN.md) — what RockStream is and why the architecture works.
- [IVM.md](IVM.md) — how the incremental view maintenance engine works.
- [NEW_IMPLEMENTATION_PLAN.md](NEW_IMPLEMENTATION_PLAN.md) — the focused,
  two-pillar engineering plan this roadmap implements.
- [ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) — the strategic
  classification (Tier A/B/C) and roadmap admission rule that new milestones
  below must pass before they are added to this roadmap.

It is deliberately narrow. The only goals are the **cloud-native IVM engine**
and the **PostgreSQL wire access layer**. Everything outside those two pillars
is out of scope (see the plan's *Out of Scope* section).

Each version below is initially sized at about **6 person-weeks** of
implementation effort. That can mean one person for six weeks, two people for
three weeks, or any other mix. During implementation planning, any mandatory
slice estimated above two person-weeks, requiring an independently reviewable
durable format, or carrying its own formal model becomes a numbered sub-version.
The total program estimate is regenerated from the resulting slice estimates;
the parent estimate is not retained merely to preserve the original count. The
version number is a planning unit, not a release-quality promise: a version is
done only when its proof is done.

Version sign-offs are strictly ordered. Each version builds on the one before
it, and no version may be marked done until its predecessor's proof is
complete. Implementation work on an independent stream may begin once its
declared prerequisites are complete; the final qualification and sign-off
sequence remains ordered.

The v0.59 qualification program from v0.59.4 through v0.59.24 contains 21
planning units and an initial estimate of approximately 126 person-weeks. That
number is not a binding staffing estimate: the split rule above requires the
program total to be regenerated after mandatory slices are estimated. The
dependency graph below identifies work that can proceed in parallel without
weakening the ordered sign-offs.

```
CLI/config -> delta state -> shared arrangements -> factorized IVM
                                            |-> shared windows + skew
                                            |-> adaptive runtime/storage
frozen engine -> introspection + error catalog -> structured diagnostics
product surface -> golden path -> executable docs -> scenario/test closure
SQL contract -> type completeness; lifecycle -> deployment profiles
architecture evidence + reference workloads -> capacity -> final qualification
```

The diagram is explanatory; these are the binding v0.59 prerequisite edges.
Implementation prerequisites identify the work that must exist before a stream
can be implemented. Proof prerequisites identify the harness or contract that
must exist before that stream can be signed off. The global sign-off order still
applies even when implementation starts in parallel.

| Version | Implementation prerequisites | Proof prerequisites |
|---|---|---|
| v0.59.4 | v0.59.3 | v0.59.3 |
| v0.59.5 | v0.59.4 | v0.59.4 |
| v0.59.6 | v0.59.5 | v0.59.5 |
| v0.59.7 | v0.59.6 | v0.59.6 |
| v0.59.8 | v0.59.6, v0.59.7, R1 | v0.59.6, v0.59.7, R1 |
| v0.59.9 | v0.59.5, v0.59.6, v0.59.7, v0.59.8 | v0.59.5, v0.59.6, v0.59.7, v0.59.8 |
| v0.59.10 | v0.59.9 | v0.59.9, R2 |
| v0.59.11 | v0.59.9 | v0.59.9 |
| v0.59.12 | v0.59.9 | v0.59.9 |
| v0.59.13 | v0.59.4, v0.59.10, v0.59.11, v0.59.12 | v0.59.4, v0.59.10, v0.59.11, v0.59.12 |
| v0.59.14 | v0.59.4, v0.59.13 | v0.59.4, v0.59.13 |
| v0.59.15 | v0.59.13, v0.59.14 | v0.59.14 |
| v0.59.16 | v0.59.10, v0.59.12 | v0.59.10, v0.59.12 |
| v0.59.17 | v0.59.13 | v0.59.13 |
| v0.59.18 | v0.59.14, v0.59.15, v0.59.16, v0.59.17 | v0.59.14, v0.59.15, v0.59.16, v0.59.17 |
| v0.59.19 | v0.59.13 | v0.59.17 |
| v0.59.20 | v0.59.19 | v0.59.17, v0.59.19 |
| v0.59.21 | v0.59.17 | v0.59.17 |
| v0.59.22 | v0.59.4, v0.59.21 | v0.59.14, v0.59.21 |
| v0.59.23 | v0.59.9, v0.59.20, v0.59.22 | v0.59.9, v0.59.17, v0.59.20, v0.59.22 |
| v0.59.24 | v0.59.9, v0.59.23 | v0.59.2, v0.59.5, v0.59.6, v0.59.7, v0.59.8, v0.59.9, R2, v0.59.17, v0.59.18, v0.59.20, v0.59.21, v0.59.22, v0.59.23 |

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
7. **Split before rushing.** Any mandatory slice estimated above two
   person-weeks, requiring an independently reviewable durable format, or
   carrying its own formal model becomes a numbered sub-version. Regenerate the
   overall estimate from those slices; the roadmap is allowed to grow.
8. **Maintainer sustainability is part of done.** The project keeps a test
   taxonomy and standard commands, contributor guidance for adding SQL,
   operators, errors, catalogs, configuration, and scenarios, ADRs for binding
   architectural decisions, archived historical plans, and dependency and
   compile-time hygiene. Large dispatcher or catalog modules are decomposed
   when their size makes ownership or review unsafe.
9. **Design-partner validation is parallel work.** Once the golden path exists,
   independent users exercise local evaluation, Kafka ingestion, PostgreSQL
   CDC, high-cardinality views, and restore or upgrade drills. Findings are
   classified as `block v0.59 qualification`, documentation, diagnostics/defaults,
   an explicit v0.59 limitation, or v0.60+ deferment. Only P0/P1 defects or
   repeated fundamental usability failures block qualification.
10. **The v0.59 scope is frozen after approval.** No new v0.59 capability
   milestone may be added. The freeze becomes authoritative only after the
   approved roadmap SHA is recorded and the repository's `main` branch or
   equivalent ruleset demonstrably enforces required checks, no force pushes,
   and ownership review for release workflows, formal specifications, security
   policy, and capability contracts. New work must split an oversized
   milestone, correct a violated v1 contract, resolve a P0/P1 defect, or move
   to v0.60 or later. RC tags remain signed. Promotion to v1.0 is unscheduled.

**Protection admission status (2026-08-19).** The live GitHub checks for
`trickle-labs/rockstream` report `main` as unprotected and report no repository
ruleset covering it. Therefore the scope-freeze baseline is not yet admitted:
required checks, no force pushes, and ownership review must be enabled and then
rechecked before an approved roadmap SHA can be called authoritative. This is a
repository-policy prerequisite, not a reason to add another roadmap milestone.

---

## Testing Conventions (Binding for Every Version)

Every automated test executes in exactly one of the following three backend
categories. Supporting scenario harnesses, model checkers, fuzzers, benchmark
frameworks, and orchestration libraries are permitted, but they do not create
additional test-result categories.

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
- Every public platform, client, backend, and version combination is labeled
   `Supported`, `Compatible, unverified`, or `Unsupported`; the label describes
   the tested environment contract, not an optimistic inference from a protocol
   implementation. Capability commitment remains a separate `Core`, `Maintain`,
   `Experimental`, or `Removed` tier under the v0.57 contract.
- Maintainer-facing additions include the relevant test command, contributor
   guidance, and an ADR when they make a durable architectural commitment.
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

Extended soaks are optional supplemental evidence, not release gates. Required
verification must run in a bounded, repeatable automated suite against the
candidate artifacts, fail on skipped prerequisites, and independently check
the claimed correctness, recovery, upgrade, restore, resource, security, and
performance outcomes. Scheduled or operator-run multi-hour and multi-day soaks
remain useful when resources permit, but their absence does not block a version
or v0.59 qualification.

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
| Public Demo Ready | v0.51.6 | A vanilla, autocommitting `psql`/ORM/BI-tool connection round-trips data with zero private ritual (no mandatory `SET`, no manual `COMMIT`, materialized views populate immediately); ad hoc `SELECT`s honor `WHERE`/`JOIN`/`GROUP BY`; the gateway serves views through the real incremental engine on one unified data plane, not a disconnected full-table batch recompute; the wire is TLS-terminated and binary-format-capable for a standard client. |
| Standard-Client & Enterprise-Honest | v0.51.13 | A current `psql`/libpq (protocol 3.2) connects; ordinary `int`/`text`/`float` `GROUP BY`/`SUM` materialized views compile and maintain incrementally; a `SELECT` from an absent relation errors instead of returning empty `OK`; `--auth` is actually enforced (fail-closed); at least one real native Kafka/S3 source ingests exactly-once through `CREATE SOURCE`; operators can debug a stalled or wrong view over the wire; multi-tenant quotas and control-plane lease HA are enforced and proven against real processes; operational edge cases carry machine-checkable specs validated by a real multi-worker chaos/SLO soak; and ad hoc `SELECT ... WHERE`/`JOIN`/`GROUP BY` queries return complete, correct results against any table regardless of how its rows are physically sharded, with no silent single-shard blind spot and no hard result-set-size wall. |
| Hardened Against Untrusted Input & Silent Corruption | v0.51.20 | Every network-facing decoder (SQL parser, pgwire wire bytes, Postgres-CDC and webhook connector payloads) is fuzz-tested and returns a proper `RS-XXXX` error instead of panicking on malformed input; the shipped release binary catches arithmetic overflow instead of silently wrapping it; no shared lock can cascade a single connection's panic into a fleet-wide outage; a real, multi-hour process-level soak proves flat memory and file-descriptor usage under sustained churn. |
| Every Claim Backed by Running Code | v0.51.27 | No connector is an in-memory mock and no proof is a text grep — the Kafka source/sink and object-store sink drive real brokers and real buckets; every test file in the repo actually compiles and runs in CI, and a missing Docker daemon fails the build instead of reporting green; operators measure and report their own arrangement size, so quota admission and shard splitting key off real state instead of a value nothing produces; an arrangement that outgrows its memory budget spills to disk with bit-identical results instead of killing the process; no blocking call, detached task, or unbounded registry survives on the async data path; and no shipped branch returns a silently-wrong answer, panics on a race it exists to handle, or acknowledges a control message and discards it. |
| View Lifecycle Durable | v0.52.2 | Creating a materialized view over a large existing table is a resumable streaming operation, not a one-shot scan: a crash mid-backfill resumes from the last committed per-partition cursor, a view is published only once it has caught up to a committed cluster frontier, and a new view's backfill cannot starve the freshness of views already running. An upstream PostgreSQL transaction touching several imported tables is applied inside a single epoch commit, so it can never be observed half-applied, and an upstream schema change either evolves compatibly or blocks the source with `RS-1002` instead of silently mis-decoding rows. |
| Connector Surface Final | v0.52.5 | RockStream's supported external integration boundary is two sources (PostgreSQL CDC, Kafka) and one sink (Kafka), and everything else is deleted rather than deprecated: the S3 source, the HTTP/webhook source, the object-store sink, the Iceberg and Delta cold-tier sinks, cold-tier GC, and external lakehouse catalog registration are gone from the codebase, along with the dependencies that existed only for them. Every removed surface fails closed with `RS-4017` and a named replacement path instead of silently doing nothing. The three survivors carry a published, machine-checked guarantee table and a real-broker/real-Postgres failure matrix, and a new connector cannot enter the repository without an admission record. The PostgreSQL wire interface and RockStream's own object-storage-backed durable state are untouched. |
| Operationally Complete | v0.56.1 | The full operator CLI surface (workload/view/schema/source/cluster/resource lifecycle, the IVM arrangement debugger), freshness lag decomposed into separately-attributable causes with an enumerated reason behind every stall, internal mTLS, secrets management, an independent security review, a proven rolling-upgrade path, and a rehearsed disaster-recovery drill are all done; an operator can run, diagnose, upgrade, and heal a cluster using only documented commands. |
| v1 Contract Published | v0.57.1 | Every capability carries a strategic tier (`Core`/`Maintain`/`Experimental`); every `Core` operator documents its incremental, backfill, recovery, state-growth, and failure semantics; PostgreSQL CDC and Kafka are named as the release-gated connectors; and CI fails if the published capability matrix drifts from this roadmap. |
| v0.59 Engineering Complete ✅ Done | v0.59 | All v0.1–v0.58.3 implementation work and the short CI gate completed; final qualification remains open because the evidence is not yet artifact-bound or sufficiently end to end. |
| v0.59 Release Qualification | v0.59.1–v0.59.3 | Release identity and evidence become immutable and SHA-bound; one mandatory no-skip multi-process suite verifies the real data path, recovery, rolling upgrade, disaster restore, resource bounds, and performance; release engineering, security provenance, and public contracts are reconciled. |
| v0.59 CLI & Configuration | v0.59.4 | CLI usability (demo, doctor, completions), configuration validation/resolution, stable JSON output, and the deterministic workload used to begin honest performance baselining. |
| v0.59 Performance Architecture | v0.59.5–v0.59.9 | Baseline the current engine, make state persistence delta-native, share durable arrangements, factorize and filter high-amplification IVM, share window slices, bound skew with heavy/light execution and micro-migration, then move the hot path to shard-owned actors with SLO-adaptive execution, checkpointing, storage, compaction, and serving. The physical architecture freezes at v0.59.9. |
| v0.59 Product Polish | v0.59.10–v0.59.12 | Runtime introspection over the final engine (capabilities, version, arrangement sharing, amplification, skew, checkpoint, cache, and system catalogs), SQL ergonomics (UPDATE/DELETE RETURNING, IF EXISTS modifiers, common scalar functions), and error-reference generation. |
| v0.59 Product Experience & Quality | v0.59.13–v0.59.23 | Single-source product surface manifest, golden-path project templates, current executable documentation, structured diagnostics across all surfaces, public-path scenario/differential framework, full lifecycle/client/backend test closure, SQL semantics and type completeness, production lifecycle and health contracts, supported deployment profiles, and capacity guidance calibrated against the final shared and factorized architecture. |
| Final Horizontal Scale & Performance Qualification | v0.59.24 | Create and freeze `v1.0.0-rc.1`, change no architecture, and qualify those exact signed artifacts for real 1/2/4/8-worker scale, absolute capacity, tail latency, hot-key behavior, state-over-RAM operation, overload recovery, migration, checkpoint, and compaction behavior with an external oracle and immutable raw evidence on fixed reference environments. |
| v1.0 Release | Unscheduled | Promotion is postponed indefinitely. This roadmap assigns no v1.0 version or date, and v0.60 or later work does not depend on promotion. |
| Typed Semantics & Feature Delivery | v0.64 | Capability Contract v2, typed scalar/key/state semantics, common ordering, typed stateful operators, explicit preview activation, and raw-pgwire reachability gates are complete. |
| Essential Operators Core | v0.69 | Admitted aggregate, grouping, join, set-operation, and analytic-window cells pass public, durability, distributed, upgrade, resource, and capacity qualification. |
| Ordering & Temporal Analytics | v0.75 | Ad hoc sort, maintained bounded ordering, HOP, SESSION, advanced frames, and `NTILE` are public and measured. |
| LATERAL & Recursion | v0.81 | Table-function and bounded correlated `LATERAL`, monotone distributed recursion, and deletion-aware recursion are publicly reachable with explicit limits. |
| Durable Time & Algebra | v0.86 | Durable timers, processing-time retractions, built-in CRDT columns, and restricted custom merge laws pass algebra and recovery qualification. |
| Serializable Transactions | v0.92 | Direct pgwire transactions provide qualified local and distributed serializable execution with complete conflict coverage. |
| Regional Resilience | v0.97 | Replicated checkpoints, warm standby, frontier-pinned reads, and fenced active-passive failover pass regional fault qualification. |
| Scoped Active-Active | v0.102 | Home-region ownership, merge-law multi-writer state, and global serializable mode pass partition, migration, convergence, and history checks. |

¹ **Re-opened by the <=v0.42 implementation review (2026-07-10).** "All four FizzBee models green" was never actually true: the `formal-verify` CI job has never successfully installed the `fizzbee` binary (wrong release-asset filename), never runs on pull requests (its `if:` guard tests a non-existent field), and carries `continue-on-error: true`, so nothing has ever blocked a merge on a red or crashing model. Running the models directly for the first time found M1 failing its `M1_S5_IdempotentReplay` invariant and M3/M4 crashing outright (undefined-variable Starlark errors) — only M2 has ever genuinely passed. The distributed-engine Rust implementation and its `SimRuntime`/chaos test suite are unaffected by this finding; only the *formal-verification* proof of v0.18–v0.22 is unverified. **Update (v0.42.1, same day):** the CI toolchain, M1, and M3 are now fixed and genuinely green (M1: 72 states; M3: 1,168 states; both safety+liveness); M2 remains genuinely green (251,889 states); M4's crash and a real self-fencing race are fixed, but exhaustive verification of M4 does not yet terminate in reasonable time, so M4 stays non-blocking pending **v0.42.3**. **Update (v0.42.1, same day, CI-robustness follow-up):** the very first real CI run of this fix showed the M4 step itself get a `cancelled` conclusion (consistent with the runner's OOM killer acting on an exploding process), which `continue-on-error` does not suppress and which flipped the whole hard-gate job to `failure` — the "M4 is non-blocking" contract was not actually true in a real run. Fixed by running M4 under a per-subshell `ulimit -v` memory cap plus a wall-clock `timeout`, swallowing its exit code into a step output instead of the step's own outcome, so the job can never fail because of M4 regardless of how it terminates. **Update (v0.42.3, same day): fully resolved.** `MAX_OUTAGES` (not worker/shard count) was the dominant driver of the explosion; lowering it from 2 to 1 (every other bound unchanged) makes exhaustive BFS complete in ~5.4s (31,456 nodes). This also surfaced a real liveness bug — `GrantLease` could re-grant a lease to a worker its own failure detector had already declared dead, letting an adversarial-but-fair schedule starve every other worker forever — now fixed with an explicit `require worker_id not in cp.dead_workers` guard. `M4_S1`–`M4_S4`, `COV_M4`, `M4_L1_RecoveryProgress`, and `M4_L2_NoPermanentBlock` all now pass under exhaustive BFS; the CI special-casing (`continue-on-error`/`ulimit -v`/`timeout`) is removed and M4 is folded back into the single hard-gate step. See `formal/findings.md` ("Post-v0.42 Review", "Post-v0.42.1 Remediation Results", and "Post-v0.42.3 Remediation Results") and roadmap versions **v0.42.1** and **v0.42.3** below.

² **Coverage gate discrepancy found by the <=v0.42 review — resolved in v0.42.2.** The v0.42 sign-off stated the gateway coverage gate is "≥90% line / ≥85% branch" with fabricated supporting evidence, but `.github/workflows/ci.yml` has always actually enforced `--fail-under-lines 70` / `--fail-under-regions 70` (region, not branch, coverage — `cargo-llvm-cov` has no `--fail-under-branches` flag), matching what the test that was supposed to lock this in (`conformance_doc_tests::test_coverage_gate_config_is_present`) actually asserts (the *70%* strings). `sign-offs/v0.42.md` is now corrected to document the real, actually-enforced 70/70 gate instead of silently carrying the false claim. A real multi-row `INSERT ... VALUES (...), (...)` parsing bug in the gateway (silently corrupted/dropped the last column, no error) was also found and is now fixed — see roadmap version **v0.42.2** below.

---

## Version Roadmap

Each row is initially sized at about 6 person-weeks. The split rule and
regenerated slice estimate are binding; the **Proof** column is the binding
delivery requirement:
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
| v0.28 | First-Party Source Connectors ✅ Done | Native Kafka (consumer-group; offsets in the causal frontier) and AWS S3 source connector abstractions for continuous streaming ingestion; the §13.3 connector contract (`discover_schema`/`start_snapshot`/`poll_delta`/`commit_offset`/`prepare`/`commit`/`abort`/`should_flush`). *(Lineage Note: v0.28 shipped mock connectors; real native `rdkafka`/`aws-sdk-s3` connectors & `CREATE SOURCE` DDL completed in v0.51.9).* | Kafka offsets and S3 file pointers are correctly tracked within the causal frontier protocol for exactly-once ingestion; a Kafka source closes a tumbling window correctly under deliberate clock skew. | Unit, LFS, MinIO, TC |
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
| v0.45 | Deep FinOps Optimizations & Cost/Diagnostics Visibility ✅ Done | Route latency-sensitive metadata (`shard_meta/`) to AWS S3 Express One Zone; tier older compacted SSTs to S3 Standard-IA; validate running the stateless worker pool entirely on Spot/preemptible instances. **Found missing during the 2026-07-11 usability/cost review**: the gateway's live `EXPLAIN <query>` handler (`crates/rockstream-gateway/src/server.rs`) is a one-line pushdown/index-annotation stub, completely disconnected from the fully-built `rockstream-sql::explain_incremental`/`explain_incremental_for_sql`/`explain_incremental_estimate` (DESIGN.md §14.8-§14.9's rich operator tree, ⚠ flags, and cost preview) — an operator typing `EXPLAIN INCREMENTAL <view>` in `psql` today gets none of the documented output. Rather than wait for the full admin CLI/resource-visibility surface at v0.53, wire `EXPLAIN INCREMENTAL`/`EXPLAIN INCREMENTAL VERBOSE`/`EXPLAIN INCREMENTAL ANALYZE`/`EXPLAIN INCREMENTAL ESTIMATE` into the gateway's real query dispatch (reusing the existing library functions, no new planning logic), and ship a minimal `SHOW RESOURCE USAGE` / `SHOW RESOURCE USAGE FOR WORKLOAD <name>` / `SHOW CLUSTER RESOURCE USAGE` (§14.19) backed by new `rockstream_catalog.view_resource_usage`/`workload_resource_usage` tables populated from metrics that already exist (`workload_memory_bytes`, `state_budget_bytes`, write-amplification, segment-cache hit ratio) — both needed to make this version's own "per-tier cost-visibility metric" claim and the FinOps win actually visible to an operator via SQL/CLI, not just `/metrics`. Also: turn the v0.36 Nexmark criterion benchmarks from "descriptive only" into a real CI regression gate (per the Common Definition of Done's own rule, "CI fails on >10% regression once a baseline exists" — the v0.36 baseline has existed since 2026-06-25 and was never gated). **Found missing during the 2026-07-11 control-plane/multi-tenancy review**: three small, already-fully-designed OLTP/analytical session-ergonomics gaps in DESIGN.md §12.8 that no version ever scheduled — `SET rockstream.max_staleness` for analytical sessions (skip the read-your-writes frontier wait and accept a bounded-stale snapshot instead: a direct latency-for-freshness trade-off the design doc itself describes as pure gateway session bookkeeping with no new distributed machinery); the cross-session `rockstream.write_fence()` / `after_fence(:token)` token pair; and completing `INSERT ... RETURNING` for server-assigned primary keys (today `parse_insert`'s `returning_rows` in `crates/rockstream-gateway/src/server.rs` only echoes back the client's own literal `VALUES`, so a caller can never get a server-generated UUID or sequence value back). All three are folded into this version rather than given their own, since each is gateway-only with no new distributed protocol. **Found missing during the 2026-07-11 network-efficiency/metrics-scalability/cost-visibility review**: (a) DESIGN.md §14.15.1's `pipeline_id` metrics-cardinality budget doubles as a hard, unconditional 256-pipeline cluster-wide creation limit, contradicting this project's own scalability positioning — decouple cardinality management (cap full per-pipeline label emission to an LRU/recent-traffic working set, rolling every pipeline outside it into one aggregate `pipeline_id="other"` series) from the functional pipeline-count limit, which the control-plane catalog itself never enforced. (b) Cost visibility today is a one-time TCO benchmark, not a running number: add `estimated_cost_per_hour` to `rockstream_catalog.view_resource_usage`/`workload_resource_usage` and to `SHOW RESOURCE USAGE` output, computed from a small operator-supplied cloud-pricing table (a `[pricing]` block in `rockstream.toml` — object-store request/storage/egress unit costs, compute $/core-hour, Spot-vs-on-demand mix) applied to metrics already collected, so the FinOps win is visible live, per workload, without re-running a benchmark. | TCO benchmarks show >50% reduction in steady-state operational costs compared to v0.31; a per-tier cost-visibility metric ships on `/metrics` and the starter Grafana dashboard so an operator can see the FinOps win without re-running the TCO benchmark; `EXPLAIN INCREMENTAL my_view` typed into `psql` returns the documented operator tree (not the old pushdown-only stub), verified byte-for-byte against `explain_incremental_for_sql`'s output; `SHOW RESOURCE USAGE` returns live per-workload state-byte/memory/SLO figures matching `view_resource_usage`; CI fails a nightly Nexmark run that regresses delta-amplification or propagation latency by >10% against the v0.36 baseline; a cluster that has created more than 256 materialized-view pipelines over its lifetime continues to accept new `CREATE MATERIALIZED VIEW` statements, with per-worker metrics cardinality bounded by the LRU working-set size rather than by total pipeline count; `SHOW RESOURCE USAGE` and `rockstream_catalog.workload_resource_usage` return a non-null `estimated_cost_per_hour` once a `[pricing]` profile is configured, and the figure changes visibly when a workload's `MEMORY_LIMIT` or shard count changes. | Unit, LFS, MinIO, TC |

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
| v0.45.1 | Workload Quotas, Priority Admission Control & Multi-Tenancy ✅ Done | Wire the existing-but-unused `rockstream-types::workload` data model into a real control-plane workload catalog and the gateway: `CREATE WORKLOAD <name> WITH (MEMORY_LIMIT=..., MAX_PARALLELISM=..., PRIORITY=..., FRESHNESS_SLO=...)`, `ALTER WORKLOAD ... SET (...)`, `DROP WORKLOAD`, `SHOW WORKLOAD STATUS` (DESIGN.md §14.13); per-workload `MEMORY_LIMIT` transitions over-budget views to `OVER_BUDGET_RELAXED` independently of (and nested inside) the existing global `state_budget_gb` floor, which remains as the cluster-wide backstop; `MAX_PARALLELISM` caps the existing per-operator auto-tuner (v0.30) on a per-workload basis; `PRIORITY`-driven admission control (§14.16) pauses or defers lower-priority workloads' views under contention instead of degrading every pipeline on the cluster equally. This is the control-plane substrate the v0.53 admin CLI's `workload {list,show,create,alter,drop}` and `cluster {status,workers,quotas}` commands assume already exists. | Two workloads (`PRIORITY=HIGH`, `PRIORITY=LOW`) sharing one deliberately under-provisioned cluster: under sustained contention the low-priority workload's views transition to `PAUSED`/`OVER_BUDGET_RELAXED` while the high-priority workload keeps meeting its `FRESHNESS_SLO`; a view that exceeds its own workload's `MEMORY_LIMIT` (but not the global budget) transitions to `OVER_BUDGET_RELAXED` and is visible in `SHOW WORKLOAD STATUS`; every quota decision and admission-control pause is audited (§14.11). | Unit, LFS, TC |
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
  `README.md`'s then-current explicit "post-1.0 goal... intentionally out of
  scope for the current roadmap" framing and `NEW_IMPLEMENTATION_PLAN.md`'s "Out of
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
| v0.45.5 | Documentation-Reality Reconciliation & Conformance Locks ✅ Done | Complete the line-by-line audit `docs/language-features.md`'s accuracy note (added this review) commits to, plus the same treatment for `docs/cli.md`, `docs/configuration.md`, and `docs/concepts.md` (§32's `SHOW CLUSTER RESOURCE USAGE`/`view_resource_usage`/`workload_resource_usage` example, found by the round-3 follow-up spot-check, needs the same not-yet-implemented caveat): every remaining "Implemented Today" / reference-table / worked-example claim across all four documents is individually re-verified against `rockstream-sql`, `rockstream-gateway`, `rockstream-cli`, and `rockstream-types::config`, and corrected in place. Regenerate `docs/cli.md` from the real `clap` `Command` definition (or generate it, so it cannot drift again) covering every flag (`--control`, `--auth`, `--metrics-addr`, `--listen`) and the real error-code surface; regenerate `docs/configuration.md`'s reference table directly from `RockstreamConfig`'s real field list and `Default` impl (not hand-copied); fix `docs/sre-operations.md`'s support-bundle section to match the real, already-shipped `support-bundle-<timestamp>.json` artifact and remove the nonexistent `rockstream support-bundle` command reference (correct to the roadmapped v0.53 `rockstream support bundle` name, with a note that it is not yet available). **Found by the 2026-07-11 dependency-integrity/testing-sufficiency review**: `docs/cli.md`/`docs/configuration.md`/`docs/concepts.md`/`docs/language-features.md`/`docs/sre-operations.md` were the only five files any prior documentation-drift round ever checked — `docs/distributed-architecture.md` and `docs/grafana-dashboard.json` were never audited, despite the former being the primary operator-facing reference for exactly the frontier/checkpoint/shuffle protocols this roadmap treats as most safety-critical. A spot-check against the real code confirms it has drifted too: §1.2 names a `WorkerAggregator` that does not exist anywhere in `crates/` (the real type is `FrontierAggregator` in `crates/rockstream-control/src/frontier.rs`), and §2.2 says alignment-buffer overflow "surfaces `RS-1601`" when the real code (`crates/rockstream-control/src/checkpoint.rs`) raises `RS-3601` for `CoordinatorError::AlignmentBufferFull`. Add both files to this version's audit scope, correcting the confirmed `WorkerAggregator`/`RS-1601` errors and re-verifying the rest of `distributed-architecture.md` component-by-component against `crates/rockstream-control` and `crates/rockstream-runtime`. Then add the automated lock this review found missing: a `docs_conformance_tests.rs`-style test (mirroring the existing `conformance_doc_tests.rs`/`docs/pgwire-conformance.md` linked-proof-test pattern already proven at v0.42) that parses `docs/language-features.md`'s "Implemented Today" bullets for backtick-quoted SQL keywords and asserts each one is recognized by `rockstream-sql`'s parser, and a second test that diffs `docs/configuration.md`'s documented keys/defaults against `RockstreamConfig::default()` at compile time. | The new conformance tests fail if a future PR adds a keyword to "Implemented Today" that `rockstream-sql` does not parse, or lets `docs/configuration.md` drift from `RockstreamConfig`'s real fields/defaults; `docs/cli.md` and `docs/configuration.md` match `crates/rockstream-cli`/`rockstream-types::config` exactly, verified by a human diff read at sign-off; `docs/sre-operations.md`'s support-bundle section matches the real JSON artifact format byte-for-byte in a worked example; `docs/distributed-architecture.md` names only real types/error codes present in `crates/rockstream-control`/`crates/rockstream-runtime` (`WorkerAggregator`→`FrontierAggregator` and `RS-1601`→`RS-3601` fixed, and no further mismatches remain after a full re-read), and `docs/grafana-dashboard.json`'s panels/metric names are confirmed present in the real Prometheus exporter. | Unit |

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
| v0.46 | Online Shard Migration & Elasticity ✅ Done | Before any splitting/rebalancing code is written, model-check the shard-migration protocol in FizzBee (binding pre-implementation requirement from v0.18): **M6 model** `formal/m6_shard_migration.fizz` with safety invariants (no dual-writer window — a bucket is authoritative on exactly one shard at any committed epoch; no lost writes across `DUAL_WRITING`→`CUTOVER`; `GC_ELIGIBLE` never fires before the migration's consumer frontier passes cutover) and liveness (`migration_state` always eventually reaches `DONE` or `ABORTED`, never stuck). Implement DESIGN.md §10.1–§10.4's full migration state machine (`PLANNED → SNAPSHOTTING → COPYING → DUAL_WRITING → CATCHING_UP → FENCING_OLD → CUTOVER → VERIFYING → GC_ELIGIBLE → DONE`, with per-state timeout budgets and `ABORTED` rollback through `VERIFYING`) for online shard add/remove, and the §10.7 worker-drain protocol (`DRAINING` → `DECOMMISSIONED`). This is the prerequisite substrate the (already-drafted) v0.53 admin CLI's `shard {list,migrate}` and `cluster workers drain` commands assume exists — until this version, they would have been thin wrappers over nothing. Also closes the Phase 4 exit-criteria waiver: the original real ≥4-host network throughput test (waived at Phase 4 with a `SimNetwork`-only proof and "a commitment to run the real-network test before Phase 8") is executed here for the first time against a live shard migration, since no prior sign-off records it having been run. | `formal/m6_shard_migration.fizz` is green at CI-fast bounds (all safety and liveness invariants hold; any counterexample archived in `formal/findings.md` and replayed as a permanent `SimRuntime` regression seed); a real ≥4-host cluster (TC, real network) migrates a bucket range from a donor to a recipient shard with zero reads or writes lost, verified by a scan comparing donor/recipient state at cutover; killing the donor mid-`DUAL_WRITING` and mid-`CUTOVER` both recover correctly (`ABORTED` rollback or completed `DONE`, never a stuck or split-brained bucket); `rockstream cluster workers drain <id>` (added as a CLI stub here, ahead of the rest of the admin CLI at v0.55) completes a full worker drain with zero shard downtime; the previously-waived Phase 4 real-network commitment is marked closed in this sign-off. | Unit, LFS, MinIO, TC |
| v0.47 | Hot-Key Virtual Buckets, Proactive Shard Splitting & Autoscaling Signals ✅ Done | Implement DESIGN.md §10.5's hot-key detection (per-key CPU/bytes/state-write tracking against `hot_key_factor`) and virtual-bucket salting with the final unsalted combiner, including the non-composable-law exception (`SKEW_BOUND_NON_COMPOSABLE` routing to a single spill shard for laws like `WeightAdd/v1`/DISTINCT); §10.6's proactive shard splitting at `1.5 × target_shard_state_bytes` (built on v0.46's migration machinery) and its reverse (cold-shard merge at the `min_shard_state_bytes` floor); the "adaptive skew splitting" control loop from §14.5, which DESIGN.md currently (incorrectly) documents as already on by default; and §10.8's `cluster_worker_pressure`/`demanded_shard_count`/`placed_shard_count` Prometheus export with a real Kubernetes HPA (or KEDA) integration test proving an actual scale-out and scale-in happen from these signals, not just that the metrics exist. **Architecture constraint (2026-07-11 compliance audit)**: the adaptive hot-key-detection and re-splitting decision logic above must be implemented in `rockstream-control` — DESIGN.md §10.5 already names the control plane as the decision-maker — and never in `rockstream-plan`; `rockstream-ops`/`rockstream-sql`/`rockstream-diff` already depend on `rockstream-plan` for its `PlanNode`/`OpNode` IR, so a `rockstream-plan → rockstream-ops` edge (e.g. to read live per-operator skew statistics) would close an unbuildable cycle. `rockstream-plan` is limited to the deterministic virtual-bucket `OpNode` variants and the pure rendezvous-hash routing function; `rockstream-control` takes a new, verified-safe dependency on `rockstream-plan` to emit/mutate them. | A synthetic hot key at 50× the median shard's load is detected and split into virtual buckets within `hot_key_factor` breach + 30s, with output remaining bit-identical to the non-split oracle; a shard crossing `1.5 × target_shard_state_bytes` splits in the background with zero operator action and zero pipeline downtime; a non-composable aggregate (DISTINCT) on a hot key correctly routes to a single spill shard and never double-emits under bucket salting (oracle property test); a real k8s cluster with HPA configured against the exported `cluster_worker_pressure` metric adds a worker within 2 minutes of a sustained 10× shard-demand spike and removes it within 10 minutes of the spike ending. | Unit, LFS, MinIO, TC |

### Phase 14 — Network Efficiency & Advanced DML

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.48 | Advanced DML & Scatter Pruning ✅ Done | `UPDATE … RETURNING` and `DELETE … RETURNING` (read-modify-write semantics); piggyback min/max bounds and Bloom filters onto the frontier summary to prune unneeded shards during multi-shard point lookups. | Multi-shard point reads safely bypass >90% of shards based on frontier summary Bloom filters. | Unit, LFS, MinIO, TC |
| v0.49 | Zero-Copy IPC, AZ-Aware Shuffle & Shuffle Compression ✅ Done | Upgrade same-host gRPC loopbacks to Apache Arrow Flight Shared Memory; make the hierarchical exchange subsystem aware of physical availability zones (AZs) to eliminate cross-AZ egress during shuffle. **Found missing during the 2026-07-11 network-efficiency review**: DESIGN.md §7.3 documents the `shuffle_outbox/`/`shuffle_inbox/` wire value as "Arrow IPC batch (compressed)" as if already decided, but no compression codec is implemented anywhere in `crates/` and no roadmap version ever scheduled it (the workspace `tonic` dependency enables no compression feature). Add pluggable shuffle-payload compression, selected automatically by the same path classifier that already picks `elided`/`loopback`/`direct`/`durable` (§7.5): a low-CPU-overhead codec (LZ4) on the latency-sensitive direct/loopback gRPC path, and a higher-ratio codec (ZSTD) on the durable object-store shuffle path and cluster checkpoints; never adding a compression step to the `elided` path, since it already skips serialization entirely. | Zero byte-copying observed in CPU profiles for same-host worker exchanges; cross-AZ traffic drops to near zero during shuffle phases; shuffle-payload compression reduces measured cross-worker network bytes by ≥40% on a representative wide-shuffle TPC-H/Nexmark workload with zero divergence from the oracle, and per-epoch CPU overhead on the direct gRPC path stays within the SLO budget (the auto-tuner can disable compression per exchange if it does not). | Unit, LFS, MinIO, TC |

### Phase 15 — Complex Analytics & Compute Tuning

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.50 | Advanced Streaming Analytics ✅ Done | SQL compiler support for recursive CTEs (`WITH RECURSIVE`) for graph algorithms and fixed-point IVM, lateral joins for nested JSON/arrays, and hopping/session windows. | Transitive closures and sessionization queries incrementally maintain state correctly against the correctness oracle. | Unit, LFS, MinIO, TC |
| v0.51 | Hot-Path Compute Optimizations ✅ Done | WAL elision for derived intermediate operator shards; link `max_rows_per_quantum` directly to network buffer depth to provide tight backpressure coupling. | Throughput on complex DAGs increases by >30% due to reduced intermediate WAL write amplification. | Unit, LFS, MinIO, TC |

**Live end-to-end wire-protocol review (2026-07-19).** Every review paragraph
above audited `DESIGN.md`/`NEW_IMPLEMENTATION_PLAN.md` claims against
`crates/` source. This is the first review to instead drive the actual
shipped `target/debug/rockstream` binary end-to-end through a real `psql`
client over the wire, at v0.50 (full report:
[IMPLEMENTATION_STATUS_20260719.md](IMPLEMENTATION_STATUS_20260719.md)). It
found the individual building blocks are real and mostly correct — SlateDB
durability, the broad pgwire surface, and the DBSP/Z-set incremental-operator
library (`rockstream-ops`) all work and are well-tested in isolation — but
the *serving path a standard client actually uses* has five blocking
correctness/usability gaps and one severe architectural gap, none previously
scheduled:

1. **Autocommit does not persist writes.** A vanilla autocommitting
   `psql`/ORM/BI-tool connection issuing `INSERT` then `SELECT` as separate
   statements sees 0 rows with no error, because writes are buffered and only
   flushed by an explicit `COMMIT` (`crates/rockstream-gateway/src/server.rs`).
2. **Every write requires the non-standard `SET rockstream.idempotency_key`**
   (or `source_epoch`) or is rejected with `RS-2007` — with the buffered rows
   silently discarded while the `INSERT` still reports success. No stock
   client ever sends this `SET`. `NEW_IMPLEMENTATION_PLAN.md` Phase 7.2's own
   design only requires this "for a direct write to a non-idempotent
   aggregate," not universally, so this is an implementation gap against the
   plan's own stated intent, not a deliberate design choice.
3. **`CREATE MATERIALIZED VIEW ... AS SELECT ...` performs no initial
   population** — the view is empty until the *next* commit touches a source
   table, contradicting both standard PostgreSQL semantics and this plan's own
   Phase 7.2 exit criterion ("`CREATE MATERIALIZED VIEW mv AS SELECT * FROM v`
   inlines `v` and starts IVM").
4. **Query-time `WHERE`/`JOIN`/`GROUP BY`/subqueries/CTEs are silently
   ignored** on an ad hoc `SELECT` (`read_view_response`, `server.rs` ~line
   2705, applies only column projection, `ORDER BY`, and `LIMIT`) — a
   predicate like `SELECT * FROM v WHERE region = 'US'` returns every row.
5. **Multi-row `INSERT ... VALUES (...), (...)` without an explicit column
   list produces a single all-`NULL` phantom row** (data loss), verified
   live — distinct from and not fixed by v0.42.2, which corrected multi-row
   tuple *splitting* (`parse_insert`/`build_row_key` already parse and key a
   column-list-free multi-row `VALUES` list correctly, confirmed by
   `parse_insert_malformed_row_without_column_list_returns_rs2056`); the
   defect is in the later stage that must resolve positional values against
   the target table's declared catalog column order when no column list is
   given, and does not.

**The architectural root cause, and the most important finding in the
report: the gateway and the real IVM engine are two disconnected data
planes.** `crates/rockstream-gateway/Cargo.toml` depends on only
`rockstream-types`, `-storage`, `-sql`, and `-control` — `rockstream-runtime`,
`rockstream-ops`, and `rockstream-diff` appear only as `[dev-dependencies]`,
for tests. Running `rockstream start --role all` opens **two independent
SlateDB shards that never communicate**: a `rockstream-runtime` worker shard
(which acquires a shard-1 lease a code comment admits is only "to demonstrate
fencing setup," `crates/rockstream-cli/src/lib.rs` ~line 566) and a separate
`<storage>/gateway-shard/` that alone serves psql clients (same file, ~line
223). Views are served by
`crates/rockstream-gateway/src/view_materializer.rs`, whose own module doc
already states it performs "batch re-evaluation on every commit — correct and
simple, not incremental," re-scanning the entire source table through
DataFusion's in-memory engine on every commit. The well-tested DBSP/Z-set
operator library this project's headline pitch rests on is proven correct in
the `rockstream-oracle`/`rockstream-sql` test harnesses but **never runs on
the live serving path** a psql user actually exercises. A related smell:
`view_materializer.rs` contains a hardcoded exact-string rewrite
(`rewrite_session_sql`, ~line 175) for one specific Nexmark SESSION-window
query.

Closing all of this is what converts RockStream from, in the report's own
words, "impressive internals with a fragile front door" into a system a
skeptical public audience can drive with standard `psql`/ORM/BI tooling with
zero prior knowledge of a private protocol. Added as a new **Phase 15.5 —
Standard Wire Compatibility & the Real Incremental Serving Path** below
(inserted between Phase 15 and Phase 16, using the same decimal-version
mechanism as Phases 11.5/12.5–12.8 so nothing at v0.52 or later is
renumbered), closing with a new **Public Demo Ready** milestone.

### Phase 15.5 — Standard Wire Compatibility & the Real Incremental Serving Path

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.51.1 | PostgreSQL-Standard Write & DDL Semantics ✅ Done | Make the front door behave like PostgreSQL for the exact transcript in `IMPLEMENTATION_STATUS_20260719.md`, with zero private ritual. **Implicit autocommit**: outside an explicit `BEGIN`, every statement commits immediately on success (an explicit `BEGIN…COMMIT`/`ROLLBACK` block keeps buffering as today). **Server-generated idempotency envelope**: when a write's session has not `SET rockstream.idempotency_key`/`rockstream.source_epoch`, the gateway generates one internally instead of returning `RS-2007`; the explicit `SET` remains available for a client that needs an idempotency guarantee spanning an explicit multi-statement transaction, matching `NEW_IMPLEMENTATION_PLAN.md` Phase 7.2's original "for a direct write to a non-idempotent aggregate" scoping, which today's code does not honor. **Immediate materialized-view population**: `CREATE MATERIALIZED VIEW ... AS SELECT ...` runs one `view_materializer` pass against current source-table state before returning, instead of waiting for the next commit. **No-column-list multi-row INSERT fix**: when `parse_insert` returns an empty column list, resolve each row's positional values against the target table's catalog-declared column order before writing, eliminating the all-`NULL` phantom-row corruption, reusing the existing `RS-2056` malformed-row error path when the table schema cannot be resolved at all. | The exact `IMPLEMENTATION_STATUS_20260719.md` transcript now works verbatim over a vanilla autocommitting `psql` connection with no `SET` and no explicit `COMMIT`: `CREATE TABLE t (id int, name text); INSERT INTO t VALUES (1, 'alice'); SELECT * FROM t;` returns the one row correctly; a freshly `CREATE MATERIALIZED VIEW`'d view reflects its source table's current data immediately, before any further write; `INSERT INTO t VALUES (3,'carol'),(4,'dave')` (no column list) inserts two correctly-populated rows, not a NULL phantom row (new `insert_no_column_list_multi_row_tests.rs`); the existing 100/100 `gateway_proof_tests` and the explicit-transaction buffering path both remain green. | Unit, LFS, MinIO, TC |
| v0.51.2 | Query-Time Predicate/Join/Aggregate Execution, Standard EXPLAIN Parity & Real Secondary Indexes ✅ Done | Route ad hoc `SELECT` statements against already-materialized tables/views through the same DataFusion engine the view *definition* already uses at materialization time, so query-time `WHERE`, `JOIN` (across two base tables/views), `GROUP BY`/aggregates, subqueries, and CTEs are actually evaluated instead of only column-projection/`ORDER BY`/`LIMIT` (`read_view_response`, `server.rs` ~line 2705). Standard `EXPLAIN <query>` — the bare syntax every ORM/BI tool actually emits, no `INCREMENTAL` keyword — returns a real DataFusion-generated plan instead of the one-line pushdown/index-annotation stub (`server.rs` ~line 1624); `EXPLAIN INCREMENTAL` (v0.45) is unchanged and remains the enhanced, RockStream-specific extension. Wire the already-tracked `CatalogIndexState::Building`/`Ready` secondary-index metadata (`server.rs` ~line 3183, v0.32) into this new query-time execution path as a real point/range-lookup accelerator, so `CREATE INDEX` (the standard DDL) stops being metadata-only — distinct from and complementary to v0.32's already-shipped *automatic*, system-managed materialized-view indexes. | `SELECT * FROM v WHERE region = 'US'` against mixed-region data returns only matching rows (verified live); an ad hoc two-table `JOIN` and an ad hoc `GROUP BY` (not baked into a view definition) return correct, oracle-matching results; plain `EXPLAIN SELECT ...` returns a real plan tree naming actual scan/filter/join nodes; a `CREATE INDEX idx ON t(col)` that reaches `Ready` measurably avoids a full-table scan for a matching point lookup, at single-digit-millisecond latency matching v0.32's bar (new `query_time_execution_tests.rs`, `create_index_query_planning_tests.rs`). | Unit, LFS, MinIO, TC |
| v0.51.3 | Gateway↔Runtime Unification: One Data Plane ✅ Done | Close the two-disconnected-data-planes architecture gap at its root. Merge `rockstream start --role all`'s two independent SlateDB shards (the `rockstream-runtime` worker shard and the gateway's own `<storage>/gateway-shard/`) into one data plane: DML written over pgwire is registered as real input to a `rockstream-runtime` worker's operator DAG, and `SELECT` reads the worker's `ViewSink` output, not a gateway-local-only table. `rockstream-gateway`'s `Cargo.toml` takes on genuine non-dev dependencies on `rockstream-runtime`, `rockstream-plan`, `rockstream-ops`, and `rockstream-diff` (promoted out of `[dev-dependencies]`, where they exist only for `gateway_proof_tests`/oracle-style tests today). `CREATE VIEW`/`CREATE MATERIALIZED VIEW` issued over pgwire compiles through the real `rockstream-sql` lowering → `rockstream-plan` `PlanNode`/`OpNode` IR → `rockstream-diff` differentiation → `rockstream-runtime` scheduling pipeline — the machinery already proven correct in the oracle/`rockstream-sql` harnesses since v0.7–v0.14 — instead of the standalone DataFusion `view_materializer`. Purely a plumbing/wiring version: it does not yet make view refresh incremental (v0.51.4); a commit may still trigger a full recompute through the newly-connected real engine. No new FizzBee model is needed — the epoch-commit/frontier/fencing protocols (M1–M4) are unchanged; this version is proven by re-running the full existing `SimRuntime`/FizzBee-paired-assertion regression suite against the unified data plane. | `rockstream start --role all` opens exactly one data plane servicing both the worker DAG and pgwire reads (no second, unreferenced SlateDB shard directory is created); the existing 100/100 `gateway_proof_tests` now execute against the real `rockstream-runtime` worker rather than a gateway-local DataFusion-only path, and still pass 100/100; a live psql `INSERT`→`COMMIT`→`SELECT` round-trip is observably serviced by the real operator DAG; every pre-existing FizzBee-paired `SimRuntime` regression seed and the M1–M4 models still pass unchanged. | Unit, LFS, MinIO, TC |
| v0.51.4 | True Incremental Maintenance on the Serving Path & Removal of the Hardcoded Nexmark Rewrite ✅ Done | Using v0.51.3's unified data plane, replace `view_materializer.rs`'s full-source-table DataFusion batch re-evaluation-per-commit with genuine incremental maintenance: each commit produces a bounded Z-set delta (not a full re-scan) applied to the view's arranged operator state via the real DAG, and only the resulting delta is written to `view_output/{view}/`. `CREATE MATERIALIZED VIEW`'s immediate initial population (v0.51.1) and every subsequent commit's refresh now go through the exact same incremental code path, eliminating the "two materializer implementations" divergence risk. Remove the hardcoded `rewrite_session_sql` exact-string match for one specific Nexmark SESSION-window query (`view_materializer.rs` ~line 175); session windows must be served through the general `SESSION` operator (v0.50) with no special-cased query text anywhere, enforced by a new grep-based CI check in the style of `scripts/check-invariant-pairs.sh`/`scripts/check-error-codes.sh`. | A commit that changes 1 row out of 1,000,000 in a source table causes measured view-refresh work (operator invocations, rows touched) proportional to the delta, not the source table's full size — a regression benchmark analogous to v0.14's "≥10x speedup vs. batch" oracle proof, this time measured on the live pgwire serving path; the Nexmark q11 SESSION query and an equivalent, differently-worded session-window query both produce results bit-identical to the DataFusion batch oracle with zero hardcoded query-text matching anywhere in `crates/` (new `scripts/check-no-hardcoded-query-rewrites.sh` fails on a deliberately reintroduced exact-string rewrite); all pre-existing Nexmark q0–q22 correctness tests (v0.33–v0.36, v0.50) still pass through the new incremental serving path. | Unit, LFS, MinIO, TC |
| v0.51.5 | Gateway-Facing TLS Termination & Binary Wire Format ✅ Done | Terminate real TLS at the pgwire gateway. Today `SSLRequest` is unconditionally answered `'N'` (`server.rs` ~line 6346/6419, asserted by `gateway_extended_query_tests.rs`'s own test), so every connection is plaintext and the already-implemented `--auth=mtls` mode (`crates/rockstream-gateway/src/auth.rs`, which extracts a client-certificate CN into a `Principal::CertCn`) can never actually be exercised, since extracting a client cert requires a TLS handshake that never happens — this is the prerequisite that makes that existing auth mode functional at all, distinct from v0.55's *internal* control↔worker/worker↔worker mTLS. Add a `rustls`-based TLS listener (configurable cert/key path in `rockstream.toml`) that negotiates `SSLRequest` with `'S'` and upgrades the connection before startup-message processing continues; `--auth=mtls` validates the negotiated peer certificate. Also implement real binary wire-format encoding (`FormatCode::Binary`) for every OID already supported in text (int2/4/8, float4/8, bool, text/varchar, timestamp(tz), date, time, uuid, numeric, json/jsonb, interval, and the supported array types) instead of silently downgrading to text. | A `psql "sslmode=require"` connection completes a real TLS handshake, and a raw-socket TC test capturing the wire bytes after `SSLRequest` confirms they are encrypted, not plaintext SQL (new `gateway_tls_tests.rs`); `--auth=mtls` end-to-end authenticates a real client certificate over the now-functional TLS channel; a client requesting `FormatCode::Binary` for a result column of every currently-text-supported OID receives correctly binary-encoded values that decode to the identical value as the text-mode response, including NULL and boundary values (new `binary_format_round_trip_tests.rs`). | Unit, LFS, TC |
| v0.51.6 | Session-State Bounding, Isolation-Level Honesty & Aggregate Correctness ✅ Done | Bound per-connection prepared-statement/portal cache state with LRU eviction (today unbounded) and guarantee cleanup on abnormal disconnect — today only the graceful `DISCARD ALL`/`RESET ALL` path (v0.37) is covered; a dropped TCP connection can leak cached statements/portals. Make `REPEATABLE READ` honest: either genuinely pin and hold the transaction's vector frontier for its duration, as `NEW_IMPLEMENTATION_PLAN.md` Phase 7.1 already specifies ("`BEGIN` captures a vector frontier held for the transaction"), so a concurrent commit is provably invisible mid-transaction, or explicitly reject it with a documented `RS-XXXX` code the same honest way `SERIALIZABLE` already is (`RS-2003`) rather than silently accepting it today without enforcing it. Fix the `avg` aggregate to use correct floating-point/`numeric` division instead of truncating integer division, matching PostgreSQL's `avg()` semantics. | A connection that opens 10,000 prepared statements without `DISCARD ALL` stays bounded in memory with eviction observable via a metric (new `prepared_statement_lru_tests.rs`); a TC test that opens many connections and kills them at the TCP level (no graceful close) shows zero growth in server-side session-state memory after cleanup runs (new `abnormal_disconnect_cleanup_tests.rs`); `REPEATABLE READ` either passes a real concurrent-commit snapshot-isolation anomaly test or is rejected with a correct, documented, tested error code, never silently accepted-but-unenforced; `SELECT avg(qty) FROM ...` on a non-integer-mean input returns the correct fractional average, not a truncated integer (new `avg_aggregate_precision_tests.rs`). | Unit, LFS, MinIO, TC |

### Phase 15.6 — Standard-Client Reachability, Serving-Path Completeness & Honest Enterprise Enforcement

The **2026-08-03 implementation-status review** (`IMPLEMENTATION_STATUS_20260803.md`)
drove the shipped `target/debug/rockstream` binary live over the PostgreSQL wire
protocol and found that the v0.51.1–v0.51.6 work, while real, left the system
**not demo-safe for a nit-picking public audience**: a modern `psql` (protocol
3.2) cannot connect at all; the most basic `GROUP BY`/`SUM` materialized view
over ordinary `int` columns no longer compiles (a regression); `SELECT` from a
failed/absent relation silently returns empty `OK`; the `--auth` flag is parsed
but ignored so a "secured" gateway runs open; the native ingestion connectors are
mocks with no `CREATE SOURCE` DDL; operator-level observability cannot debug a
stalled or wrong view; the multi-tenancy and control-plane-HA claims are internal
accounting/election without distributed enforcement or a real leader-kill proof;
and the enterprise-validation milestones are simulation, not sustained real-cluster
chaos. Phase 15.6 closes all four blockers and all six structural areas that
review flagged, **before** the v0.52+ data-governance and 1.0-finalization work
proceeds — because a skeptical user hits every one of these in the first five
minutes with standard tooling. Following the same decimal-version mechanism as
Phases 11.5/12.5–12.8/15.5, nothing at v0.52 or later is renumbered.

**Extension (2026-08-03): ad hoc query execution is essential and was left
incomplete.** v0.51.2 shipped ad hoc `SELECT ... WHERE`/`JOIN`/`GROUP BY`
execution against un-materialized tables/views via a query-time DataFusion
path, but it only ever scans the local gateway process's own single-shard
`ShardDb` partition of each referenced relation — never fanning out through
the `MultiShardReader` scatter-gather machinery that compiled
materialized-view reads already use — and hard-fails outright on any relation
past a 1,000,000-row/64 MiB cap instead of degrading gracefully. Both are
real, previously-unscheduled gaps against a feature this project treats as a
basic, must-work capability, not an edge case. Closed at **v0.51.13** below,
using the same decimal-version mechanism so v0.52 onward is still not
renumbered.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.51.7 | Modern-Client Wire Reachability, Honest Relation & Auth Semantics ✅ Done | Close the three front-door blockers a standard client hits before seeing a single row (Blockers 1, 3, 4 of `IMPLEMENTATION_STATUS_20260803.md`). **Protocol 3.2 negotiation (Blocker 1):** psql 18.4 / libpq ≥ 18 open with wire protocol **3.2 (196610)**; the gateway wraps `pgwire = "0.28"`, which understands only **3.0 (196608)**, and today — with **zero** `NegotiateProtocolVersion`/`protocol_version` handling anywhere in `crates/rockstream-gateway/src/` — **silently closes the socket with 0 bytes and no `ErrorResponse`** instead of downgrading, so a stock client gets "server closed the connection unexpectedly" and never connects. Implement `NegotiateProtocolVersion`: on a startup message advertising a minor version > the supported 3.0, reply advertising 3.0 and echoing any unsupported `_pq_.*` protocol-extension parameters back as not-understood, then continue the 3.0 handshake — matching the real PostgreSQL server behavior byte-for-byte. **Honest relation semantics (Blocker 3):** a `SELECT` against a relation that does not exist (including one whose `CREATE` just failed, e.g. after a rejected aggregate MV) currently returns a bare `CommandComplete` with no rows **and no error**, so a reviewer cannot distinguish "empty view" from "view was never created"; resolve every `SELECT`/DML target against the catalog first and raise a proper `RS-XXXX` "relation does not exist" `ErrorResponse` (mirroring PostgreSQL `42P01`) instead of silent empty `OK`. **Auth enforcement (Blocker 4):** `rockstream start --role gateway\|all --auth scram\|md5\|oidc\|mtls` parses into `StartOptions.auth_mode`, but `crates/rockstream-cli/src/lib.rs` **never reads it** — `start_gateway_with_shard` builds the server via `GatewayServer::with_shard_db(...)`, which hardcodes `AuthMode::Off` (`server.rs:1494`), so every connection becomes `Principal::System` regardless of the flag. Route `opts.auth_mode` to the matching `*_and_auth`/`*_and_mtls_auth` constructor, **fail closed** on an unknown/unsupported mode (documented `RS-XXXX`), and log the effective auth mode at startup so a "secured" deployment can never silently run open. | `psql "host=… sslmode=disable" -c "SELECT 1"` from **psql 18.4** completes the handshake and returns the row (new `protocol_negotiation_tests.rs`, driven by both a real psql 18 in TC and a hand-written 3.2-startup wire client that asserts a `NegotiateProtocolVersion` reply, not an EOF); a hand-written 3.0 client still works unchanged; `SELECT * FROM does_not_exist` and `SELECT` from a view whose `CREATE` just failed both return a `relation does not exist` `ErrorResponse`, never an empty `OK` (new `absent_relation_error_tests.rs`); `rockstream start --auth scram` rejects an unauthenticated connection and authenticates a valid SCRAM credential, `--auth <garbage>` refuses to start, and the startup log names the active mode (new `cli_auth_enforcement_tests.rs`). | Unit, LFS, MinIO, TC |
| v0.51.8 | General Serving-Path Aggregate & Type Coverage — Wiring the Real Physical-Plan Path ✅ Done | Close Blocker 2, a **functional regression** from the July transcript. The v0.51.4 serving path routes every `CREATE (MATERIALIZED) VIEW` through the direct fast-path compiler (`compile_plan`), whose `AggregateOp` **only supports `Int64` keys/values**; because a standard SQL `int` maps to Arrow `Int32` (not `Int64`), the single most basic streaming query — `CREATE MATERIALIZED VIEW mv AS SELECT k, SUM(v) FROM t GROUP BY k` over ordinary `int` columns — is **rejected at CREATE time with `RS-1019 → RS-1013`** ("AggregateOp only supports Int64 keys/values … requires the DiffCtx/OpNode physical-plan path"). The old batch `view_materializer` that handled arbitrary types via DataFusion was deleted in v0.51.4, and the general `DiffCtx`/`OpNode` physical-plan path the error message points at **exists in the engine but is not reachable from the gateway**, so a user must declare **every** column `BIGINT` to get a working aggregate view — an undocumented, non-obvious constraint the Nexmark corpus hides by using `BIGINT` throughout. Wire the general `rockstream-plan` `OpNode`/`DiffCtx` physical-plan path into the serving compiler as the fallback whenever the `Int64`-only fast path cannot lower a node (or widen the fast path to cover all standard Arrow key/value types), so `int`/`smallint`/`bigint`/`text`/`varchar`/`float`/`numeric`/`bool`/`date`/`timestamp` group-by keys and `SUM`/`AVG`/`COUNT`/`MIN`/`MAX` aggregate values all compile and maintain incrementally, with correct type promotion (e.g. `SUM(int)` → `bigint`, `AVG(int)` → `numeric`/`double` matching PostgreSQL). Add a serving-path aggregate **test matrix** (key type × value type × aggregate) so no future refactor can silently narrow the supported shapes again. | The exact rejected transcript now works: `CREATE TABLE o (cust int, amt int); INSERT INTO o VALUES (1,10),(1,5),(2,3); CREATE MATERIALIZED VIEW mv AS SELECT cust, SUM(amt) FROM o GROUP BY cust; SELECT * FROM mv;` returns `(1,15),(2,3)` over ordinary `int` columns with **no** `RS-1019`/`RS-1013` (new `serving_path_aggregate_matrix_tests.rs` covering `int`/`text`/`float`/`numeric`/`date` keys × `SUM`/`AVG`/`COUNT`/`MIN`/`MAX`, each cross-checked bit-identical against the DataFusion batch oracle); a `text`-keyed `SELECT product, SUM(qty) … GROUP BY product` and an `AVG` view both compile and return oracle-matching fractional results; all pre-existing Nexmark `BIGINT` views still pass unchanged; a CI guard fails if the serving compiler regains an `Int64`-only rejection path for a standard aggregate shape. | Unit, LFS, MinIO, TC |
| v0.51.9 | Real Native Ingestion Connectors & `CREATE SOURCE` DDL ✅ Done | Close Area D — the reviewer's "the system relies on SQL interfaces for ingestion." Today `crates/rockstream-connectors/src/kafka_source.rs` and `s3_source.rs` are labelled **"Mock … source connector"** in their own first line; `kafka_sink.rs` admits "in production this would wrap a real Kafka producer (e.g. `rdkafka`)"; the crate's `Cargo.toml` has **no `rdkafka`, no `aws-sdk`, no Postgres-CDC dependency**; and there is **no `CREATE SOURCE` DDL in the gateway at all**, so a user cannot register a streaming source over SQL. Ship at least one **real** first-party streaming source behind real DDL: a production `rdkafka`-backed Kafka consumer source (consumer-group offsets, partition assignment, backpressure) and a real `aws-sdk-s3`/`object_store`-backed S3/GCS object-arrival source, both reusing the already-proven exactly-once 2PC state machine (M3) — but now driven against a **real broker/object store**, not the in-memory mock. Add `CREATE SOURCE <name> TYPE kafka\|s3 (…options…) FORMAT json\|avro\|csv`, `ALTER SOURCE … {PAUSE\|RESUME\|DROP}`, and `SHOW SOURCES`/`SHOW SOURCE STATUS` over pgwire, wired into the unified data plane (v0.51.3) so ingested rows become real operator-DAG input. Credentials are referenced by name (deferring envelope-encrypted `CREATE SECRET` to v0.55) but must never be logged in plaintext. Reposition the v0.28 "First-Party Source Connectors [Done]" claim honestly in this row's lineage note, since that milestone shipped mocks. | A real Kafka broker in TestContainers publishes records that a `CREATE SOURCE … TYPE kafka` ingests exactly-once into a materialized view, surviving a mid-stream worker kill with zero loss/duplicates (new `kafka_source_exactly_once_tests.rs`, real-broker TC — the first real-Kafka proof in the repo); a real MinIO/S3 object drop is ingested by `CREATE SOURCE … TYPE s3` and reflected in a downstream view (new `s3_source_ingestion_tests.rs`); `SHOW SOURCES` lists registered sources with live lag/offset; `grep -ri "mock.*source connector" crates/rockstream-connectors/src` returns zero matches for any shipped (non-test) source; `Cargo.toml` carries real `rdkafka` + `aws-sdk`/`object_store` dependencies (verified by a CI dependency-presence check). | Unit, LFS, MinIO, TC |
| v0.51.10 | Distributed Enforcement: Worker-Side Quotas & Consensus-Replicated Lease HA ✅ Done | Close Areas A and B — turn multi-tenancy and control-plane HA from **accounting/election** into **enforced/proven** guarantees. **Worker-side quota enforcement (Area B):** today quota checks live only in the gateway (`catalog_stubs.rs` `budget.try_acquire`), never in the runtime operators; a worker can allocate far beyond a workload's `MEMORY_LIMIT` with no rejection, and enforcement is **reactive** (`OverBudgetRelaxed` after the fact) rather than **prospective**. Make workers consult a distributed quota ledger **before** allocating a batch's arrangement/state, rejecting or shedding a batch that would exceed the tenant's `memory_limit`/`max_parallelism` **prospectively**, with cross-worker quota coordination so a noisy tenant provably cannot starve another across real worker processes. **Consensus-replicated lease HA (Area A):** today shard-lease grants live only in an in-memory `HashMap` in `ShardManager` and are **not durably replicated across control nodes** — a single control node is the SPOF for lease state, and the v0.45.2 failover proof is a `SimRuntime` scenario, not a real multi-process leader kill. Replicate shard-lease grants through the existing Raft group (`crates/rockstream-control/src/raft.rs`, `RaftPersistentStore`) so an in-flight lease survives a leader change, gated by `RaftHandle::require_leader()`, consistent with the M7 model (`formal/m7_control_plane_ha.fizz`) and its M7-S2/M7-S3 safety invariants. Both proofs must run against **real, separate processes**, not a co-located single-process `SimRuntime`. | A multi-worker TestContainers cluster runs two tenants where a hostile tenant attempts to allocate 10× its `MEMORY_LIMIT`; the well-behaved tenant's `freshness_slo` is provably held and the offender's over-limit batches are rejected **before** allocation, not relaxed after (new `distributed_quota_enforcement_tests.rs`, real multi-process TC); a real multi-process control-plane leader kill (SIGKILL the leader binary) shows a follower assume shard-lease authority with **continuity of in-flight lease grants** and zero dual-writer window, cross-checked against M7-S2/S3 paired assertions (new `lease_ha_leader_kill_tests.rs`, TC — the first real leader-kill drill, replacing the `SimRuntime`-only v0.45.2 proof). | Unit, LFS, MinIO, TC |
| v0.51.11 | Operator-Grade Runtime Diagnostics: Live Op-Stats & Pipeline-Stall Inspector ✅ Done | Close Area C's runtime half — "an operator can scrape metrics and read a plan, but cannot debug a stalled or wrong view without reading Rust source." (The full operator CLI + IVM arrangement debugger remains v0.53; this version delivers the runtime data those tools need, and a minimal inspector, so the gap is not blocking until then.) **Fix `EXPLAIN INCREMENTAL ANALYZE` op-stats:** today it calls `collect_operator_stats(0)` with a **hardcoded `0`** against an empty `TopologyCatalog`, so per-operator timings/row-counts come back empty; wire it to the live running topology so it returns real per-operator invocation counts, rows touched, and time spent. **Pipeline-stall diagnostics** (the reviewer's exact "why isn't epoch N advancing?" concern): expose, over pgwire and `/metrics`, per-operator/per-shard frontier positions, the slowest-advancing input, and the operator holding back the committed frontier, so an operator can localize a stall to a specific operator/shard/source without reading source. **Minimal arrangement peek:** a read-only `SHOW ARRANGEMENT <view> <op_id> <key>` (a thin precursor to v0.55's full `rockstream debug arrangement`) that reads live intermediate Z-set state via `DbReader` without perturbing the pipeline — the tool for "this view has a *wrong* answer." No new services or config; all surfaces are thin wrappers over already-shipped catalog/`DbReader`/topology APIs. | `EXPLAIN INCREMENTAL ANALYZE <view>` returns non-empty, correct per-operator stats matching an independently instrumented run (new `explain_analyze_opstats_tests.rs`), never the hardcoded-`0` empty result; a deliberately stalled pipeline (a wedged operator in a TC scenario) is localized to the exact operator/shard via the stall-diagnostic surface without reading Rust (new `pipeline_stall_diagnostics_tests.rs`); `SHOW ARRANGEMENT` returns the same intermediate Z-set weights an operator would compute by hand for a known input, proving a "wrong view" can be root-caused from the wire (new `arrangement_peek_tests.rs`). | Unit, LFS, MinIO, TC |
| v0.51.12 | Operational Edge-Case Validation Specs & Real-Cluster Chaos/SLO Soak ✅ Done | Close Areas E and F — expand the machine-checkable spec surface beyond the M1–M7 happy path, and replace simulation-only enterprise validation with a **real multi-worker chaos + absolute-SLO soak**. **Edge-case validation specs (Area E):** today the FizzBee models (`formal/m1…m7`) cover only happy-path core coordination; the operational edge cases the reviewer named — quota exhaustion, connector-source failure, object-store brownout with buffer exhaustion, misconfiguration rejection, late-arriving data — live **only in DESIGN.md prose** with no machine-checkable model, and the error-code registry (~75 codes) is a lookup table, not a behavioral spec of *when* each code fires or *how* to recover. Add machine-checkable models (FizzBee where a distributed race exists, otherwise deterministic `SimRuntime` scenario specs) for each named edge case, each paired with a runtime `assert!` per FIZZBEE_TEST_PLAN.md §3.7, and broaden the `formal-verify` CI trigger so these run on the PRs that touch the relevant crates (today it only triggers on `formal/`, `DESIGN.md`, or specific crates, so most PRs skip it). **Real-cluster chaos & absolute-SLO soak (Area F):** today the only "soak" (`.github/workflows/simulation-soak.yml`, 6-hourly) runs `cargo test -p rockstream-sim --test chaos_tests` — a fast in-process deterministic simulation whose `thirty_two_shard_24h`-style names denote **simulated**, not real, wall-clock; there is no TestContainers multi-worker chaos job, no real Kafka/S3 under sustained pressure, and the performance gates are **regression** (slower vs. baseline?) not **absolute SLO**. Add a real multi-worker TestContainers chaos job (worker/control-node kills, network partitions, object-store brownouts) driving real Kafka/MinIO under sustained load, publishing **absolute** freshness/throughput/recovery-SLO numbers — the real-cluster complement to the simulation soak, and the enterprise-validation evidence v0.58/v0.59 build on. | Each named edge case (quota exhaustion, source failure, object-store brownout + buffer exhaustion, misconfiguration rejection, late data) has a green machine-checkable model **and** a paired runtime `assert!`, verified by the existing `scripts/check-invariant-pairs.sh` mechanism extended to the new invariant IDs (new `formal/` specs + `edge_case_recovery_tests.rs`); a PR touching a covered crate now actually triggers `formal-verify` (CI-config test asserts the widened trigger); a real multi-worker TC cluster survives a fixed chaos schedule under real Kafka/MinIO load with published absolute SLO numbers meeting documented targets, not just a no-regression delta (new `real_cluster_chaos_soak_tests.rs`, TC), giving v0.58/v0.59 a real-cluster baseline instead of a simulation-only one. | Unit, LFS, MinIO, TC |
| v0.51.13 | Full Ad Hoc Query Support: Multi-Shard Scatter-Gather & Removal of Hard Result-Set Caps ✅ Done | Ad hoc query execution is an **essential, must-work feature** — a bare `SELECT ... WHERE`/`JOIN`/`GROUP BY` against base tables/views with no `CREATE MATERIALIZED VIEW`, the first thing any SQL user tries. v0.51.2 shipped the query-time DataFusion path (`query_time_datafusion_select`, `server.rs`) and it is real, but incomplete in two ways not previously scheduled. **(1) Single-shard only, silently:** the path resolves every referenced relation by scanning only `self.shard_db` — the local gateway process's own `ShardDb` partition (`scan_prefix_bounded`) — with no fan-out through the already-shipped `multi_shard_reader::MultiShardReader::scatter_read`/`scatter_read_partial_agg` gather machinery that compiled materialized-view reads already use (`read_compiled_view_rows`, published via `write_view_directory_entry`). In any real multi-shard deployment — this project's own core distribution model since v0.15 — an ad hoc query against a table whose rows are partitioned across N shards returns only the rows that happen to live on the shard the client's connection landed on, with no error and no warning: a silent correctness gap, not merely a performance one. **(2) Hard, ungraceful result-set caps:** any referenced relation exceeding `MAX_QUERY_TIME_ROWS` (1,000,000 rows) or `MAX_QUERY_TIME_SCAN_BYTES` (64 MiB) hard-fails the whole query with `RS-2025 query.query_time_result_set_too_large` rather than degrading gracefully, because the current implementation materializes every referenced relation into one in-memory Arrow `MemTable` before DataFusion runs at all — so ad hoc analytics over anything past a small demo table are unsupported outright, not just slow. Fix both: wire `query_time_datafusion_select` to scatter each referenced relation's scan across every shard that owns a piece of it via `MultiShardReader`, merging per-shard batches before DataFusion executes the query, so ad hoc `WHERE`/`JOIN`/`GROUP BY` produce complete, correct results regardless of how a base table is physically sharded; and replace the single-`MemTable`-materialization strategy with bounded, streamed per-shard batch ingestion so the practical size ceiling for ad hoc analytics is raised well past today's 1M-row/64MiB wall, while keeping an explicit, documented, user-facing limit for genuinely pathological queries (never a silent truncation). | A 3+ shard TestContainers cluster with a base table whose rows are physically split across every shard returns the exact same result set for an ad hoc `SELECT * FROM t WHERE ...`, an ad hoc two-table `JOIN`, and an ad hoc `GROUP BY`/aggregate as a single-shard oracle run over the same logical data (new `query_time_multi_shard_scatter_tests.rs`, real multi-process TC — the first genuinely distributed ad hoc query proof in the repo); a relation exceeding today's 1,000,000-row/64 MiB cap completes an ad hoc aggregate query correctly instead of failing with `RS-2025` (new `query_time_large_relation_tests.rs`); the existing `query_time_execution_tests.rs`/`create_index_query_planning_tests.rs` (v0.51.2) continue to pass unmodified. | Unit, LFS, MinIO, TC |
| v0.51.14 | Real Postgres CDC & HTTP Webhook/Push Source Connectors | Extend the real-connector work started in v0.51.9 (Kafka, S3/GCS) to the two source shapes DESIGN.md §13.1/§13.3 name but no version has ever scheduled: a **keyed CDC source** and a **push-driven source**. **Postgres logical replication CDC:** a production connector using `tokio-postgres`'s replication protocol against a `pgoutput`/`wal2json` publication slot, decoding INSERT/UPDATE/DELETE into Z-set deltas with `row_id = hash(source_id, table_id, primary_key)` per §6.4's keyed-CDC rule (not the append-only-log rule Kafka/S3 use), and the replication slot's LSN packed as the connector's opaque `OffsetToken` per §13.3 — restart resumes from the last committed LSN with no gap or replay of already-applied rows. Declares `watermark_capability() = Native` (LSN commit timestamp as event-time proxy). Handles logical-replication failure modes explicitly rather than assuming a happy-path stream: a dropped/invalidated replication slot (`wal_sender` timeout, publication dropped, or the slot's retained WAL exceeding `max_slot_wal_keep_size`) surfaces as a distinct `BLOCKED` state with an automatic resnapshot-and-resubscribe path, not a silent hang or crash loop; and a **slow subscriber** — the connector falling behind the primary's WAL generation rate — trips the same credit-based backpressure/pause signal Kafka uses (§13.3's `credits_available()`) so the connector sheds load before the primary's `pg_wal` retention grows unbounded, instead of forcing an operator to drop the slot by hand under disk pressure. Both `pgoutput` (the built-in, dependency-free logical-decoding plugin) and `wal2json` (the extension several managed-Postgres offerings ship instead) are exercised as independently tested decode paths, not merely alternative `FORMAT` grammar values. **HTTP webhook/push source:** the gateway's `--role=gateway` HTTP routing slot (reserved since Phase 9, DESIGN.md line ~160) is finally wired to accept inbound `POST` webhook deliveries at a per-source, token-authenticated path (`/webhook/<source_name>`), decoding `json`/`csv` bodies into deltas exactly like a poll-based source's `poll_delta`, but driven by inbound HTTP instead of an outbound poll loop. Implements the exact backpressure contract §11.7 already specifies but no connector implements today: once the source's local buffer exceeds `local_buffer_max_epochs`, the endpoint returns **HTTP 429 Too Many Requests** rather than accepting and buffering unboundedly, so a slow downstream pipeline degrades the sender's throughput instead of RockStream's memory. Declares `watermark_capability() = ExternalHint`, consistent with `ALTER SOURCE ... ADVANCE WATERMARK` (§13.3) since arrival order at a webhook endpoint is not a reliable event-time proxy. Add `CREATE SOURCE <name> TYPE postgres_cdc (…slot, publication, connection options…) FORMAT pgoutput\|wal2json` and `CREATE SOURCE <name> TYPE http_webhook (…auth token, path…) FORMAT json\|csv`, both reusing the same `ALTER SOURCE {PAUSE\|RESUME\|DROP}` / `SHOW SOURCES`/`SHOW SOURCE STATUS` DDL surface v0.51.9 already shipped for Kafka/S3, and both routed through the same M3 exactly-once 2PC state machine. Credentials (replication user password, webhook auth token) are referenced by name, never logged in plaintext, per the same policy as v0.51.9 (deferring envelope encryption to v0.55). | A real Postgres 18 instance in TestContainers has its logical replication slot consumed by `CREATE SOURCE … TYPE postgres_cdc`, and a mid-stream worker kill followed by restart resumes from the last committed LSN with zero lost or duplicated rows, proving the keyed-CDC `row_id` rule against UPDATE/DELETE, not just INSERT (new `postgres_cdc_exactly_once_tests.rs`, real-Postgres TC); the same TestContainers Postgres proves identical CDC output against both `pgoutput` and `wal2json` decoding plugins (new `postgres_cdc_format_matrix_tests.rs`); a deliberately invalidated/dropped replication slot is detected and recovered via automatic resnapshot rather than a silent stall, and a connector held deliberately far behind the primary's WAL generation rate trips backpressure before `pg_wal` retention grows unbounded (new `postgres_cdc_failure_and_slow_subscriber_tests.rs`); a webhook `POST`ed to `/webhook/<name>` is ingested exactly-once into a materialized view (new `http_webhook_ingestion_tests.rs`); a deliberately stalled downstream pipeline causes the webhook endpoint to return `429` once `local_buffer_max_epochs` is exceeded, and the sender's retried request after recovery is not double-counted (new `http_webhook_backpressure_tests.rs`); `SHOW SOURCES` lists both new source types with correct status transitions across `PAUSE`/`RESUME`/`DROP`. | Unit, LFS, MinIO, TC |
| v0.51.15 | Durable Source Runtime Foundation ✅ Done | The v0.51.14 DDL/parser/listener work remains **unshipped** until a source can be owned, committed, acknowledged, and recovered by the real runtime. Build durable source-epoch and offset checkpoints on SlateDB LocalFileSystem and MinIO/S3; atomically prepare and commit M3 input before any upstream acknowledgement; recover from the highest committed checkpoint; and make owner registration, PAUSE/RESUME/DROP cleanup, and status reflect live runtime state. Add gateway ingress that commits webhook epochs before acknowledging deliveries. | Crash injection at every prepare/commit/ack boundary recovers bit-identically on LFS and MinIO; `SHOW SOURCE STATUS` reports live owner, checkpoint, lag, buffer, and blocked state without credentials; seeded SimRuntime ownership/failover/retry preserves one committed token per epoch and exactly-once webhook delivery. New proofs: `source_durability_lfs_tests.rs`, `source_durability_minio_tests.rs`, `source_coordination_sim_tests.rs`, and `source_cleanup_tests.rs`. | Unit, LFS, MinIO, SimRuntime, TC |
| v0.51.16 | Production Postgres CDC & HTTP Webhook Sources ✅ Done | Complete the source implementations on the durable runtime from v0.51.15: PostgreSQL 18 logical-replication snapshot/subscription workers for `pgoutput` and `wal2json`, keyed INSERT/UPDATE/DELETE Z-set deltas, bounded credit/WAL-lag handling, and slot invalidation recovery; plus authenticated JSON/CSV webhook ingestion whose deliveries are acknowledged only after M3 commit and whose bounded buffer returns 429 before enqueue. `CREATE SOURCE`, `ALTER SOURCE`, and `SHOW SOURCE STATUS` must drive the live runtime end-to-end. | Real Postgres 18 Testcontainers proves snapshot, INSERT/UPDATE/DELETE, restart at committed LSN, `pgoutput` and `wal2json`, invalidated-slot recovery, and slow-subscriber backpressure. Gateway pgwire plus HTTP tests prove raw `POST /webhook/<source>` reachability, RS-coded negative responses, exact JSON/CSV delivery/retry behavior, 429 recovery, and live lifecycle/status. Every proof is rerun on LFS, MinIO, and seeded crash/failover simulation. | Unit, LFS, MinIO, SimRuntime, TC |

> **v0.51.14 status: superseded — not released.** Its parser/catalog/listener work is not a production source runtime and must not be signed off or shipped as Postgres CDC or webhook ingestion. The work is carried forward by v0.51.15 and v0.51.16; no delivery is accepted as durable until those versions' M3 commit, checkpoint, recovery, and real-system proof gates pass.

### Phase 15.7 — Post-v0.51.16 Hardening Review: Panic Safety, Arithmetic Safety, Lock Hygiene & Resource-Leak Soak

A **2026-08-05 hardening review** — conducted after v0.51.16 closed the last
scheduled correctness/reachability gap before Phase 16's data-governance work
— audited the already-shipped codebase for latent robustness gaps that no
roadmap version had ever targeted: production-build panic safety on
network-facing input, arithmetic-overflow behavior in release builds,
lock-poisoning blast radius, and long-duration resource-leak coverage. None of
these are new user-visible features — deliberately, per the same "correctness
before scale before features" philosophy this roadmap opens with — they are
defects and load-bearing gaps in the *already-shipped* engine that a hostile
or merely unlucky production workload could still trigger. Four findings,
none previously scheduled: **(1)** `crates/rockstream-gateway/src/server.rs`
alone carries 208 `.unwrap()` and 30 `.expect()` call sites — more than any
other crate — reachable from raw, unauthenticated bytes on the wire before a
single validation step, and the SQL parser (`crates/rockstream-sql/src/frontend.rs`)
and the Postgres-CDC/webhook connector decoders
(`crates/rockstream-connectors/src/postgres_cdc.rs`, `webhook_source.rs`,
shipped at v0.51.15/v0.51.16) carry the same risk on bytes supplied by an
external source/webhook client; the project has never had a single
coverage-guided fuzz harness (no `cargo-fuzz`/libFuzzer target, no `fuzz/`
directory anywhere in the repo) — `rockstream-oracle/src/sql_fuzzer.rs` is a
*query-generation* differential-correctness tool for the oracle, not a
crash-safety fuzzer for untrusted bytes — so this bug class has zero
automated detection today. **(2)** The workspace `Cargo.toml` has no
`[profile.release]` section at all, so the actual release binary the
`Dockerfile` builds and ships runs with `overflow-checks = false` by Rust's
own default — silently wrapping any epoch/offset/checkpoint-index/row-count
arithmetic bug instead of the loud panic every `cargo test` run (debug
profile, `overflow-checks = true` by default) would already have caught,
meaning this exact bug class is provably invisible to CI today and only
reachable in production. **(3)** Ten call sites hold a poisoning
`std::sync::Mutex`/`RwLock` shared across concurrent connections/tasks —
including the gateway's `webhook_sources` registry and OIDC `JwksCache`
(`server.rs`, `auth.rs`), the control plane's `namespace.rs`/`acl.rs` state,
the SQL frontend's `snapshot_tables` tracking, and a single process-wide
`GLOBAL_DLQ` (`rockstream-types/src/dlq.rs`) — so one panic while any of
these locks is held permanently poisons it, cascading a single bad request
into every other tenant/connection sharing that lock being unable to proceed
until the whole process restarts, the opposite of the fault isolation the
rest of this architecture is built around. **(4)** The only wall-clock,
real-process chaos coverage (`real-cluster-chaos.yml`) runs one fixed
scenario for a single CI job's duration once a week, and the 6-hourly
`simulation-soak.yml` is a deterministic in-process `SimRuntime` run — neither
ever asserts flat process-level memory/file-descriptor usage over a long real
run under sustained connection/source churn, so a slow leak has no gate that
would ever catch it before a customer's cluster does. Closing all four, in
the same decimal-version mechanism as Phases 11.5/12.5–12.8/15.5/15.6 so
nothing at v0.52 or later is renumbered.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.51.17 | Panic Safety on Untrusted-Input Paths & a First Fuzzing Harness ✅ Done | Close finding (1). Audit and eliminate every reachable panic (`.unwrap()`, `.expect()`, panicking slice/index access, integer-cast panics) on the three boundaries that parse bytes supplied directly by an external, potentially adversarial actor before any validation: pgwire startup/extended-query message decoding and dispatch (`crates/rockstream-gateway/src/server.rs`), the SQL statement parser entry point (`crates/rockstream-sql/src/frontend.rs`), and the Postgres logical-replication (`pgoutput`/`wal2json`) and JSON/CSV webhook body decoders shipped at v0.51.15/v0.51.16 (`crates/rockstream-connectors/src/postgres_cdc.rs`, `webhook_source.rs`). Every panic found on these boundaries becomes an `RS-XXXX` `ErrorResponse` (pgwire/SQL) or an `RS-4XXX` rejection (connector/webhook payloads) instead of an unhandled panic, per the Common Definition of Done's own binding rule that every user/operator-visible failure already carries an `RS-XXXX` code — this closes a violation of the project's own standing rule, not a new one. Add the project's first coverage-guided fuzz harness (`cargo-fuzz`, new `fuzz/` workspace member) with one target per boundary: SQL statement text, raw pgwire wire bytes at the startup/extended-query message layer, `pgoutput`/`wal2json` replication-message bytes, and webhook JSON/CSV request bodies. Run it as a new scheduled CI job (mirroring `simulation-soak.yml`'s cadence) with a fixed time/iteration budget; any crash found during harness bring-up is fixed and its minimized input checked into a permanent regression corpus, the same discipline this roadmap already applies to FizzBee counterexamples and `SimRuntime` seeds. Add a scoped `#![deny(clippy::unwrap_used, clippy::expect_used)]` (module-level, not workspace-wide) to the specific decoder modules identified so a future PR cannot silently reintroduce a panic on this exact boundary. | Each of the four fuzz targets runs for a fixed CI time budget with zero panics/crashes/hangs on the corpus accumulated so far (new `fuzz/` targets plus a `fuzz_corpus_regression_tests.rs` wrapper that replays every previously-crashing input as a permanent regression test); a deliberately reintroduced `.unwrap()` in one of the four identified modules fails the new scoped clippy lint gate in CI; a malformed/truncated/adversarial input at each of the four boundaries (new `untrusted_input_panic_safety_tests.rs`) returns a correct `RS-XXXX`/`RS-4XXX` `ErrorResponse` and leaves the connection/worker able to serve the next request, never an unhandled panic. | Unit, TC |
| v0.51.18 | Arithmetic Overflow Safety in Release Builds ✅ Done | Close finding (2). Add `[profile.release] overflow-checks = true` to the workspace `Cargo.toml` — today's default (`false`) means the exact binary the `Dockerfile` builds and ships can silently wrap an epoch/LSN-offset/checkpoint-index/row-count arithmetic bug that every existing `cargo test` run (debug profile) would already panic on and catch, so this closes a gap between what CI proves and what production runs, not a new correctness requirement. Benchmark the throughput cost via the existing `criterion` harness on the hot commit/ingest path and document it as a new baseline (a real, measured cost is acceptable; an *undetected* one is not). For the specific monotonic counters where a production panic is a worse outcome than the bug it would catch — epoch numbers, LSN/offset connector tokens, checkpoint indices, and the admission-control byte/row quota counters on the hot ingest path (v0.51.10) — convert the arithmetic to explicit `checked_add`/`checked_sub`/`saturating_add` with a dedicated `RS-XXXX` rejection on the checked path's overflow case, so these specific paths are neither a silent wrap nor an uncontrolled panic even with `overflow-checks` on. Add property tests that deliberately drive each identified counter toward its numeric boundary (`u32::MAX`/`u64::MAX` minus a small delta) and assert the documented, tested behavior. | `cargo test --workspace --release` passes with `overflow-checks = true` in the release profile; a new `criterion` benchmark shows the overflow-checked hot commit/ingest path's cost against the pre-existing baseline, within a documented, accepted percentage (no silent regression — measured, not assumed); a new `overflow_boundary_tests.rs` proptest suite drives each of the four identified counter classes to its numeric boundary and asserts a clean, tested `RS-XXXX` rejection or a benign no-op, never a silent wrap and never an uncontrolled panic. | Unit, LFS |
| v0.51.19 | Lock-Poisoning Elimination & Cascading-Panic Containment ✅ Done | Close finding (3). Ten call sites hold a poisoning `std::sync::Mutex`/`RwLock` shared across concurrently-running connection/task handlers: the gateway's `webhook_sources` registry and OIDC `JwksCache` (`crates/rockstream-gateway/src/server.rs`, `auth.rs`), the control plane's `namespace.rs`/`acl.rs` state, the SQL frontend's `snapshot_tables` tracking (`crates/rockstream-sql/src/frontend.rs`), and — the highest-blast-radius case — the single process-wide `GLOBAL_DLQ` (`crates/rockstream-types/src/dlq.rs`) that every connector's dead-letter path (v0.52's connector DLQ depends on this exact primitive) writes through. A panic while any of these is held permanently poisons it; every subsequent `.lock()` on that same `Mutex`/`RwLock` anywhere else in the process then panics too, so one malformed request or one connector decode bug cascades into every other tenant/connection sharing that lock being unable to proceed until the whole process is restarted — the opposite of the fault isolation (per-connection task, per-shard state) the rest of the architecture is built around. Replace each with the workspace's already-pervasively-used `parking_lot::Mutex`/`RwLock` (non-poisoning by design) wherever the guarded value's invariants can tolerate a mid-mutation panic (audited case by case and documented per site), or add an explicit, audited `PoisonError::into_inner()` recovery path with an emitted audit event where poisoning must remain observable rather than silently swallowed. Two files (`delta_sink.rs`, `iceberg_sink.rs`) already use a documented, scoped `std::sync::Mutex` specifically to avoid holding a `parking_lot` guard across an `.await` point — these are excluded from this migration (a real, already-reasoned-about tradeoff, not an oversight) and are instead re-verified as still narrowly scoped and never shared across connections. | A chaos test that deliberately panics one connection/task mid-critical-section while other concurrent connections read/write the same previously-shared state (`GLOBAL_DLQ`, the webhook-source registry, the ACL/namespace maps) shows zero impact — no `PoisonError`, no stalled lock acquisition, no degraded service — on any of the other connections (new `lock_poisoning_containment_tests.rs`); a new grep-based CI check in the `scripts/check-*.sh` style (`scripts/check-no-poisoning-locks.sh`) fails on any new `std::sync::{Mutex,RwLock}` introduced in the audited crates without the same explicit `.await`-across-guard justification comment `delta_sink.rs`/`iceberg_sink.rs` already carry, preventing regression. | Unit, SimRuntime, TC |
| v0.51.20 | Long-Duration Real-Process Resource-Leak Soak ✅ Done | Close finding (4). Today's only real-wall-clock, real-process chaos coverage (`real-cluster-chaos.yml`) runs exactly one fixed correctness scenario (`real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle`) once a week for a single CI job's duration, and the 6-hourly `simulation-soak.yml` is deterministic in-process `SimRuntime` — real OS-level resources (heap, file descriptors, sockets) are never actually exercised over a long run, so a slow leak in connection/session teardown (v0.51.6 bounded the prepared-statement/portal cache and its abnormal-disconnect cleanup, but never measured *aggregate* long-run growth), source pause/resume/drop lifecycle (v0.51.15/16), or checkpoint/compaction churn has no gate that would ever catch it before a real customer cluster does. Add a new long-duration real-process soak — target a sustained ~4 real wall-clock hours, comfortably within GitHub-hosted runners' 6-hour job limit, run on its own nightly schedule rather than per-PR — that drives the actual `rockstream` unified binary under sustained, realistic churn: thousands of short-lived pgwire connections opening and closing (including the abnormal-disconnect path), sources repeatedly paused/resumed, and materialized views continuously refreshed. Sample and assert **flat** process RSS, open file-descriptor count, and open socket count at regular intervals (no unbounded upward trend beyond a documented, generous tolerance band), publishing the same kind of SLO-summary artifact `real-cluster-chaos.yml` already produces so a slow regression is visible in every run's job summary, not just a pass/fail bit. | A new scheduled `long-duration-resource-soak.yml` CI job runs the real binary for the full ~4h window under sustained connection/source churn and publishes a resource-usage summary showing RSS, open-FD, and open-socket counts stay within the documented flat-usage tolerance band for the entire run, with zero unbounded growth (new `resource_leak_soak_tests.rs` plus the workflow's own sampling/assertion script); a deliberately introduced leak (a connection-teardown path that skips deregistration) reproducibly fails the same gate, proving the gate actually detects what it claims to. | Unit, LFS, MinIO, SimRuntime, TC |

### Phase 15.8 — Post-v0.51.20 Substance Review: Real Connectors, Whole-Suite CI Reachability, Bounded IVM State & Async-Runtime Hygiene

A second **2026-08-05 review** — run immediately after Phase 15.7 was scheduled,
this time auditing not for *latent* robustness gaps but for **claims the shipped
code does not actually back** and **load-bearing wiring that was never
connected** — found seven findings, none previously scheduled and none a new
feature. Every one is a place where RockStream's own sign-off record says a
capability is `✅ Done` while the code, the dependency graph, or the CI
invocation says otherwise. In the same "correctness before scale before
features" spirit this roadmap opens with, and using the same decimal-version
mechanism as Phases 11.5/12.5–12.8/15.5–15.7, **nothing at v0.52 or later is
renumbered**.

**(1) The connector tier is still substantially in-memory, and its only
"proof" is a text grep.** `crates/rockstream-connectors/Cargo.toml` declares
`rdkafka`, `aws-sdk-s3`, and `aws-config` as real non-dev dependencies, but a
workspace-wide search for `use rdkafka`/`rdkafka::` and
`use aws_sdk_s3`/`aws_sdk_s3::`/`aws_config::` returns **zero hits in any
`src/` file** — all three crates are declared and never imported. The single
test standing behind v0.51.9's "real first-party streaming source" claim,
`crates/rockstream-connectors/tests/dependency_presence_test.rs`, reads
`Cargo.toml` as a **string** and asserts `content.contains("rdkafka")`; it
would pass identically against a manifest listing a dependency no line of code
ever calls — which is exactly the state it is asserting today.
`kafka_source.rs` still declares itself `/// A mock KafkaSource implementing
SourceConnector.`, `kafka_sink.rs` still says `/// In production this would
wrap a real Kafka producer (e.g. rdkafka).` over a `BTreeSet`-backed state
machine, and `object_store_sink.rs` still says `/// In production this would
call a real object-store client. Here it uses an in-memory representation of
_pending/ and final/ namespaces for testing.` The *sinks* — the exactly-once
2PC egress path formally modelled as M3/M5 — have **never** been driven
against a real broker or a real bucket.

**(2) 19 of 184 test files never compile in CI, and 45 more can silently
self-skip.** `.github/workflows/ci.yml` runs `cargo test --workspace` with
**no `--features`**, while 14 test files carry `#![cfg(feature = "simulation")]`,
4 carry `#![cfg(feature = "testcontainers")]`, and 1 carries
`#![cfg(feature = "docker_tests")]` — so those whole files are not compiled,
not run, and produce no failure signal. `simulation-soak.yml` invokes
`cargo test -p rockstream-sim --test chaos_tests` with the same omission.
Separately, 45 test files contain a runtime Docker-availability check that
`eprintln!("SKIP …: Docker not available")` and **returns success**, so a
runner without Docker reports a fully green durability/TC suite having
executed none of it. The `coverage` job measures `cargo llvm-cov --workspace`
over that same reduced set, so every per-crate `--fail-under-lines` floor is
computed against a test suite smaller than the one the project believes it
has. Two of the tests policing this (`conformance_doc_tests.rs`) assert on
`ci.yml`'s and the docs' **file text**, not on behavior.

**(3) Operator state size is never measured, so the entire memory-admission
chain is driven by a number nothing produces.** `set_pipeline_state_bytes`
(`rockstream-types/src/metrics.rs`) is called from exactly two non-test
sites, both of which are setters awaiting a caller
(`catalog_stubs.rs::set_view_state_bytes`, `admission.rs`) — **no operator in
`rockstream-ops` ever computes or reports the byte size of its own
arrangement**. Downstream, `WorkerQuotaManager::try_allocate_batch` — v0.51.10's
prospective, pre-allocation worker-side quota check — is called **only from
tests** (`quota.rs`'s own `#[cfg(test)]`, `distributed_quota_enforcement_tests.rs`,
`edge_case_recovery_tests.rs`); the real batch-processing path never consults
it. The skew/split machinery (`rockstream-control/src/skew.rs`,
`target_shard_state_bytes = 32 GiB`, `split_trigger_fraction`) and the
`OverBudget*` view-state ladder therefore all key off state sizes that no
production code path ever supplies.

**(4) IVM arrangements are unbounded in-process `HashMap`s with no spill
path.** A workspace-wide search for `spill_to_disk`/`SpillableArrangement`
returns **zero hits**. `join.rs`'s `left_arr`/`right_arr:
HashMap<Vec<u8>, HashMap<u128, ArrRow>>`, `aggregate.rs`'s `entries: HashMap<…>`,
`distinct.rs`, and the window/topk arrangements all grow monotonically with
distinct live keys, with no byte budget, no eviction, and no degradation
path — the operator either fits in RAM or the process dies. The one operator
that *does* have a cap, `topk.rs`'s `TOPK_BUFFER_LIMIT` (100,000), converts
exceeding it into a **hard permanent epoch failure**
(`OpError::topk_buffer_overflow`) with no recovery other than reducing input
cardinality. For a system whose entire value proposition is *incremental*
maintenance of long-lived state, "the arrangement is bounded by distinct live
keys" is a description, not an enforcement.

**(5) Blocking calls, detached tasks and hardcoded timeouts on the async data
path.** `delta_sink.rs` and `iceberg_sink.rs` each call
`tokio::task::block_in_place(|| handle.block_on(future))` from inside an
`async fn`; `s3_source.rs::sync_files_from_store` spawns a **fresh
`std::thread` plus a fresh current-thread `tokio` runtime on every poll**;
`cold_gc.rs` calls `std::thread::sleep(Duration::from_millis(20))` inside a
synchronous per-file delete callback. On the shuffle path,
`exchange/service.rs` uses a hardcoded `mpsc::channel(64)` and 13 `tokio::spawn`
call sites whose `JoinHandle`s are dropped (task failures are unobserved and
survive shutdown), and `exchange/pool.rs` hardcodes a 250 ms `connect_timeout`
with **no per-RPC `tokio::time::timeout`, no retry budget, and no circuit
breaker**, while its `clients` map is never pruned when a peer dies.

**(6) Long-lived registries grow with churn and are never pruned.**
`exchange/flow_control.rs`'s `channels`/`notifiers: HashMap<CreditKey, …>` gain
an entry per `(exchange_id, src_shard, target_shard)` tuple and lose none;
`exchange/multiplexer.rs`'s `streams: HashMap<WorkerId, Sender<ShuffleFrame>>`
retains dead workers across every rolling restart and autoscale event;
`webhook_source.rs`'s `pending: HashSet<String>` retains an identity forever
if a delivery is never acknowledged. `tls.rs`'s `MTLS_CN_BY_PEER_ADDR` *is*
capped, but its cap **silently drops** the CN of any connection past
`MAX_CONNECTIONS` (`if MTLS_CN_BY_PEER_ADDR.len() < MAX_CONNECTIONS { … }`)
instead of rejecting it — a mTLS-identity loss under load, not merely a cache
miss. v0.51.20 adds the soak *gate* that would eventually observe this class;
this finding is the set of *defects* that gate would report.

**(7) Silent-wrong-answer and silently-ignored branches survive in shipped
code.** `rockstream-storage/src/tiered_store.rs` ends its tier-fallback
helper with `primary.get(location).await.map(|_| unreachable!())` — a
**panic** on the exact "object present in the primary tier after the fallback
scan" race the function exists to handle. `rockstream-ops/src/window.rs`
implements `WindowFunc::Ntile(_)` as `for i in 0..n { out[i] = 0; }` — the SQL
layer honestly rejects `NTILE` with `RS-1016`, so this is unreachable *today*,
but it is a silently-wrong result rather than a rejection sitting one wiring
change away from the serving path. `rockstream-ops/src/index_arrange.rs`'s
`point_lookup` is documented `(first index column only for now)`, so a
multi-column `CREATE INDEX` accelerates only its leading column.
`rockstream-control/src/service.rs` acknowledges `WorkerMessage::DrainAck` and
`WorkerMessage::LifecycleState` with nothing but a `tracing::info!` under the
comment *"v0.38 drain / lifecycle messages — acknowledged but not yet fully
handled by the control-plane service stub"*, so a worker's drain progress is
logged and then discarded by the coordinator that is supposed to act on it.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.51.21 | Real Kafka & Object-Store Connector Clients — Retiring the Last In-Memory Mocks ✅ Done | Close finding (1). Replace the three remaining in-memory connector state machines with real clients over the dependencies the manifest already declares but no code imports: a real `rdkafka` consumer in `kafka_source.rs` (consumer-group membership, partition assignment/revocation, offset commit driven by the M3 `OffsetToken`, credit-based backpressure) replacing the file's self-declared `/// A mock KafkaSource`; a real `rdkafka` **transactional producer** in `kafka_sink.rs` (`init_transactions`/`begin_transaction`/`send_offsets_to_transaction`/`commit_transaction`) backing the already-modelled `CheckBeforeCommit` profile, where recovery re-reads the epoch marker from the **real** topic rather than a `BTreeSet`; and a real `object_store`/`aws-sdk-s3` client in `object_store_sink.rs` whose `_pending/{epoch}/ → final/` transition is a genuine conditional PUT/copy (`If-None-Match`) against a real bucket, backing the `NativeIdempotent` profile. The exactly-once state machines, their `assert_*` runtime invariants, and the M3/M5 FizzBee models are **unchanged** — this version changes only what sits behind them, so the existing paired-assertion suite is the regression gate. Delete `tests/dependency_presence_test.rs` outright: a test that greps `Cargo.toml` for a string is a proof of nothing, and its removal must be accompanied by the real-system proofs below, never by simply dropping the assertion. Extend `scripts/check-no-hardcoded-query-rewrites.sh`'s grep-gate discipline with a new `scripts/check-no-mock-connectors.sh` that fails if any file under `crates/rockstream-connectors/src/` describes itself as a mock/simulated/in-production-this-would implementation, so this exact drift cannot silently return. | Every existing M3/M5 sink proof and every `SinkConnector` runtime assertion passes **unchanged** against the real clients; a real Kafka broker in TestContainers receives a transactionally-committed epoch that survives a mid-commit worker kill with zero duplicates and zero loss, verified by an independent consumer reading the topic (new `kafka_sink_exactly_once_tests.rs`, real-broker TC — the first real-Kafka **sink** proof in the repo); a real MinIO bucket shows the `_pending/ → final/` rename is genuinely conditional, with a deliberately re-driven commit producing byte-identical objects and no duplicate (new `object_store_sink_real_bucket_tests.rs`); `cargo tree -p rockstream-connectors` shows no declared-but-unimported dependency, enforced by a compile-time gate rather than a text assertion; `scripts/check-no-mock-connectors.sh` fails on a deliberately reintroduced mock docstring. | Unit, LFS, MinIO, TC |
| v0.51.22 | Whole-Suite CI Reachability: Every Test Actually Compiles and Runs ✅ Done | Close finding (2). Make the CI invocation cover the test suite the project believes it has. Run the workspace test job with the feature matrix that compiles all 19 currently-uncompiled feature-gated test files (`simulation`, `testcontainers`, `docker_tests`) — either via `--all-features` or an explicit, documented matrix — so `cargo test` stops silently omitting the entire simulation, TestContainers, and Docker suites; apply the same fix to `simulation-soak.yml`, whose "100,000 seeds" claim currently runs a `chaos_tests` binary compiled without its own crate's `simulation` feature. Replace the 45 files' `eprintln!("SKIP …: Docker not available"); return;` pattern with a single shared helper that **fails** when a new `ROCKSTREAM_REQUIRE_DOCKER=1` (set in every CI job that provisions Docker) is present, and skips only for local developer convenience — so a green CI run can never again mean "Docker was missing." Re-measure every per-crate `--fail-under-lines`/`--fail-under-regions` floor against the now-complete suite and raise each to `max(current_floor, floor(new measured baseline))`, never lowering one. Replace the two text-assertion tests in `conformance_doc_tests.rs` (`test_coverage_gate_config_is_present`, `test_all_gated_crates_present_in_ci_coverage_job`) and the `fn <name>` grep in `test_conformance_doc_has_linked_tests` with checks that parse `ci.yml` as YAML and assert every workspace crate has an enforced floor, and that each doc-linked test **is a real `#[test]` that ran**, via the JSON test-result output — a stub body must fail. Broaden the `formal-verify` and `real-cluster-chaos.yml` path filters so no crate is excluded (`rockstream-plan`, `rockstream-types`, `rockstream-oracle`, `rockstream-cli`, `rockstream-diff` are all silently exempt today). | The CI test job's reported test count strictly exceeds today's by at least the 19 previously-uncompiled files' tests, and a deliberately failing assertion added to any one of them fails the build (proving the file is genuinely compiled and run); a CI job run with Docker deliberately unavailable **fails** rather than reporting green; the re-measured coverage floors are committed and every crate is gated at or above its new baseline; a stub-bodied `fn` matching a documented conformance test name fails `conformance_doc_tests.rs`; a PR touching only `crates/rockstream-plan/` triggers both `formal-verify` and the real-cluster chaos job. | Unit, LFS, MinIO, SimRuntime, TC |
| v0.51.23 | Real Operator State-Size Accounting & Wiring the Admission/Skew Control Loop ✅ Done | Close finding (3). Give every stateful operator in `rockstream-ops` (`join`, `outer_join`, `aggregate`, `minmax`, `distinct`, `topk`, `window`, `time_window`, `index_arrange`, `lateral`, `recursion`) an incrementally-maintained `state_bytes()` — updated on insert/retract in O(1), never by walking the arrangement — and report it through the already-defined `set_pipeline_state_bytes` metric that today is fed **only by tests and by two setters awaiting a caller**. Wire the real batch-processing path to consult `WorkerQuotaManager::try_allocate_batch` **before** allocating a batch's arrangement growth, so v0.51.10's prospective quota check — currently reachable only from `#[cfg(test)]` code and three test files — actually governs production allocation, with the documented `OverBudgetRejected` outcome and its `RS-5003`/`RS-9001` audit event. Feed the same measured numbers into `rockstream-control/src/skew.rs`'s `ShardFootprint`, so `target_shard_state_bytes`/`split_trigger_fraction`/`min_shard_state_bytes` and the v0.46 split/merge decisions key off real state instead of a value no production path supplies. Add a CI gate in the style of `scripts/check-invariant-pairs.sh` that fails if a stateful operator gains a new arrangement field without a corresponding `state_bytes()` contribution, so accounting cannot silently go stale. | A view whose join arrangement holds a known number of rows of known width reports a `pipeline_state_bytes` within a documented tolerance of an independently computed size, measured over five different operator shapes (new `operator_state_accounting_tests.rs`); the v0.51.10 hostile-tenant multi-process TC proof is re-run driving the **real** ingest path (not a direct `try_allocate_batch` call) and the offender's over-limit batches are still rejected before allocation; a shard driven past `split_trigger_bytes` by real data triggers a real v0.46 split with no test-only setter anywhere in the path; the new accounting gate fails on a deliberately added unaccounted arrangement field. | Unit, LFS, MinIO, SimRuntime, TC |
| v0.51.24 | Bounded IVM Arrangements: Spill-to-Disk & Graceful Degradation Instead of OOM ✅ Done | Close finding (4) — the single largest gap between "an IVM engine" and "a rock-solid IVM engine." Using v0.51.23's real per-operator byte accounting, give every stateful arrangement a **spill path**: when an operator's live state exceeds its share of the workload's `memory_limit_bytes`, the coldest portion of the arrangement is evicted to the already-present SlateDB shard (`DbWriter`/`DbReader`, the same durable substrate checkpoints use) and transparently faulted back on lookup, so a long-running high-cardinality join or `GROUP BY` **degrades in latency rather than killing the process**. Today a workspace-wide search for any spill primitive returns zero hits and the only cap that exists — `topk.rs`'s `TOPK_BUFFER_LIMIT` — converts overflow into a permanent unrecoverable epoch failure; replace that with the same spill path so a high-cardinality `TOP K` partition becomes slower, not fatal. Correctness is non-negotiable and is the whole proof: a spilled arrangement must produce **bit-identical** output to an unspilled one for the same input, and the spilled state must participate in checkpoint/recovery exactly as in-memory state does. No new SQL surface, no new config beyond a documented spill threshold and directory, and no change to any operator's incremental semantics. | A join and a `GROUP BY` driven to 10× their configured memory budget complete successfully with output **bit-identical** to the same query run with an unlimited budget, and to the DataFusion batch oracle (new `arrangement_spill_correctness_tests.rs`); a mid-spill crash recovers to the identical arrangement contents on both LFS and MinIO (new `arrangement_spill_recovery_tests.rs`); the previously-fatal `TOPK_BUFFER_LIMIT` overflow scenario now completes with correct results and a published spill metric instead of `OpError::topk_buffer_overflow`; a `criterion` benchmark documents the measured latency cost of the spilled path against the in-memory baseline as a new accepted number; process RSS stays within the configured budget throughout, asserted by the v0.51.20 soak harness. | Unit, LFS, MinIO, SimRuntime, TC |
| v0.51.25 | Async-Runtime Hygiene: Blocking-Call Elimination, Task Supervision & Real Timeout/Retry Budgets ✅ Done | Close finding (5). Remove every blocking call from an async execution path: `delta_sink.rs`'s and `iceberg_sink.rs`'s `block_in_place(\|\| handle.block_on(future))` become genuinely `async` (this is the same pair v0.51.19 deliberately exempted from the `parking_lot` migration *because* they hold a `std::sync::Mutex` across what should be an `.await` — resolving the blocking call resolves that exemption's root cause too); `s3_source.rs::sync_files_from_store`'s per-poll `std::thread::spawn` + fresh current-thread `tokio::runtime::Builder` becomes a plain `async fn` on the caller's runtime, eliminating unbounded thread creation under multi-source polling; `cold_gc.rs`'s `std::thread::sleep(20ms)` per deleted file becomes an async, concurrency-limited delete with a bounded in-flight window. Bring the shuffle data path under supervision: replace the 13 fire-and-forget `tokio::spawn` sites in `exchange/service.rs` and `exchange/multiplexer.rs` with a tracked `JoinSet`/`TaskTracker` plus a `CancellationToken` honored on shutdown, so a task's error is logged with its `RS-XXXX` code and shutdown is not racing detached work. Make the exchange's network policy explicit and configurable instead of a single hardcoded constant: `exchange/pool.rs`'s 250 ms `connect_timeout` becomes a documented `rockstream.toml` knob, every shuffle RPC gains a `tokio::time::timeout` and a bounded retry budget with jittered backoff, a repeatedly-failing peer is circuit-broken rather than retried into a cascade, and `service.rs`'s hardcoded `mpsc::channel(64)` is sized from the existing `ExchangeConfig` row budget so the frame channel and the credit-based flow controller agree. | A `tokio-console`/instrumented run of a sustained Delta and Iceberg sink workload shows **zero** blocked-worker events, where the current code shows them under the same load (new `async_hygiene_tests.rs`); polling four S3 sources for 10,000 iterations creates a bounded, constant number of OS threads, not one per poll; a shutdown issued mid-shuffle completes with every spawned task joined or cancelled and zero orphaned tasks observed, versus detached tasks surviving today; a deliberately unreachable peer is circuit-broken after its documented retry budget instead of stalling the shuffle, and every timeout/retry parameter is settable from `rockstream.toml` with its default asserted; a throughput benchmark shows the resized frame channel removes the 64-frame stall observed under a high-fan-out shuffle. | Unit, LFS, MinIO, SimRuntime, TC |
| v0.51.26 | Leak-Free Long-Lived Registries & Identity-Safe Caps ✅ Done | Close finding (6) — the concrete defects v0.51.20's soak gate is designed to catch, fixed so that gate can be green rather than merely honest. Give every long-lived registry an explicit lifecycle: `exchange/flow_control.rs`'s `channels`/`notifiers` maps drop a `CreditKey`'s entries when its exchange is torn down; `exchange/multiplexer.rs`'s `streams` map and `exchange/pool.rs`'s `clients` map evict a `WorkerId` on worker death/drain (the control plane already knows this — v0.51.27 delivers the signal, and this version consumes it); `webhook_source.rs`'s `pending: HashSet<String>` gains a documented TTL so an identity from a delivery that is never acknowledged cannot be retained forever. Fix `tls.rs`'s `MTLS_CN_BY_PEER_ADDR` cap, which today **silently discards** the client CN of any connection arriving past `MAX_CONNECTIONS` (`if MTLS_CN_BY_PEER_ADDR.len() < MAX_CONNECTIONS { insert }`) — under load this quietly loses an authenticated mTLS identity instead of rejecting the connection, a security-relevant failure mode, not a cache miss; the connection must instead be refused with a documented `RS-XXXX` code, and the entry must be removed on abnormal as well as graceful disconnect (v0.51.6 covered session state; this covers the TLS identity map). Every registry that survives a connection or a worker gets a fill-level gauge on `/metrics` so its growth is observable before it is fatal. | A test that creates and tears down 50,000 exchanges, 10,000 worker registrations, and 100,000 unacknowledged webhook deliveries shows every registry's fill-level gauge return to its baseline, with zero net growth (new `registry_lifecycle_tests.rs`); a connection arriving when the mTLS identity map is full is **rejected with its documented `RS-XXXX` code** and never authenticated without a recorded CN, and a TCP-level abnormal disconnect removes its entry; the v0.51.20 long-duration soak runs green — flat RSS, flat FD count, flat socket count — over its full window against the real binary, which is the acceptance bar for this version, not a separate one. | Unit, LFS, MinIO, TC |
| v0.51.27 | Honest Failure Semantics: No Silent-Wrong-Answer or Acknowledged-and-Discarded Branches ✅ Done | Close finding (7). Eliminate the four shipped branches that produce a wrong answer, a panic, or a silent no-op where the code's own comment admits an unfinished path. `rockstream-storage/src/tiered_store.rs`'s tier-fallback helper ends in `primary.get(location).await.map(\|_\| unreachable!())` — a process **panic** on precisely the "object reappeared in the primary tier during the fallback scan" race the helper exists to service; return the object (or a documented `RS-XXXX`) instead. `rockstream-ops/src/window.rs`'s `WindowFunc::Ntile(_) => { out[i] = 0; }` returns a silently-wrong constant; make the operator layer reject it with the same `RS-1016` the SQL layer already returns honestly, so the engine cannot produce a wrong `NTILE` answer if it is ever wired up — defense in depth for the exact "code exists but is never wired to dispatch" gap class v0.59's unscoped sweep is chartered to find. `rockstream-ops/src/index_arrange.rs`'s `point_lookup` is documented `(first index column only for now)`: either accelerate the full composite key or have `CREATE INDEX` on multiple columns state, in `EXPLAIN` and in `docs/language-features.md`, exactly which prefix is accelerated — never silently accelerate one column while the user believes all are indexed. `rockstream-control/src/service.rs` handles `WorkerMessage::DrainAck` and `WorkerMessage::LifecycleState` with a bare `tracing::info!` under its own comment *"acknowledged but not yet fully handled by the control-plane service stub"*: make the coordinator actually consume them — drain progress advances the v0.46 drain state machine to completion, and a worker's lifecycle transition updates `ShardManager`'s view and emits the worker-death signal v0.51.26's registry eviction consumes. Sweep `crates/*/src/` for every remaining `unreachable!()` on a reachable input-dependent branch and convert each to a coded error. | The tiered-store race is driven deterministically by a `SimRuntime` seed and returns the object instead of panicking, with the pre-fix code panicking on the identical seed (new `tiered_store_fallback_race_tests.rs`); an `NTILE` request constructed directly against the operator layer returns `RS-1016`, never `0`; a two-column `CREATE INDEX` either measurably accelerates a two-column point lookup or `EXPLAIN` and `docs/language-features.md` state the accelerated prefix exactly, verified by the v0.51.22-hardened doc-conformance test; a real multi-worker TC drain completes because the control plane acted on `DrainAck`, and the worker's death evicts its multiplexer/pool entries within a documented bound (new `drain_lifecycle_handling_tests.rs`); zero `unreachable!()` remain on an input-reachable branch anywhere in `crates/*/src/`, enforced by a new grep-based CI gate. | Unit, LFS, MinIO, SimRuntime, TC |

### Phase 16 — Ingestion Failure Containment

**Post-v0.51.26 strategic rebaseline (2026-08-11).**
[ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) re-evaluated
everything scheduled after v0.51.27 against one question — *does this turn the
system RockStream already is into an exceptionally reliable cloud-native IVM
product?* — and the answer redirected the last eight versions away from new
product surface and toward the properties a v1 promise actually rests on:
correctness, operability, security, upgradeability, recoverability, and proof.
Unlike every prior review recorded in this document, this one **does**
renumber v0.52 onward, because the change is a reprioritization of unstarted
work rather than an insertion of newly-found gaps. Per this document's standing
convention, the live tables, the Public Milestones list, the Formal
Verification Track, and the Plan-mapping table are kept current, while earlier
review-narrative paragraphs are preserved as a historical account and still
refer to their own contemporaneous numbering. Three concrete changes:

1. **The declarative-governance product line is cut.** `CREATE EXPECTATION`,
   the expectation operator, the `warn`/`degrade`/`block` state-degradation
   policies, and lineage diagnostics were a new policy language and a new
   product pillar — Tier C "new breadth" per the focus document §3 — with no
   concrete workload requiring them. What survives is the operational half the
   ingestion lifecycle genuinely needs and DESIGN.md §13.3.1 already designs in
   full: a bounded, durable, inspectable, replayable connector quarantine.
   That is v0.52, and it is the whole of Phase 16.
2. **Broader transactional semantics are deferred, not scheduled.** The
   previously-drafted v0.54 (`SERIALIZABLE LOCAL`, per-row version validation
   for non-CRDT exact-key writes) moves RockStream toward being the
   transactional authority — an OLTP database — which is explicitly not what
   this project optimizes for (focus document §2, §6). It is removed from the
   version table and recorded under "Deferred by decision" after Phase 17
   below, with the evidence that would readmit it.
3. **Every remaining version closes a production-readiness gap**, in the order
   the focus document's §10 staging requires: operability (v0.53–v0.54),
   security (v0.55), upgrade and disaster recovery (v0.56), the v1 contract
   itself (v0.57), the failure matrix (v0.58), and the release gate (v0.59).
   No version from v0.52 onward adds a new SQL family, a new connector family,
   a new catalog, or a new policy subsystem.

**Nothing already shipped is removed.** Iceberg/Delta cold-tier sinks,
secondary indexes, hot-key virtual bucketing, autoscaling signals, advanced ad
hoc SQL, advanced DML, and the object-store/webhook sources all stay supported,
regression-tested, and secure. They are simply no longer growth areas — and
v0.57 writes that distinction down as a public, machine-checked contract
instead of leaving it implicit.

> **Partially superseded (2026-08-12, Phase 16.6 below).** The clause above
> still holds for secondary indexes, hot-key virtual bucketing, autoscaling
> signals, advanced ad hoc SQL, and advanced DML. It no longer holds for the
> connector surface: [ROCKSTREAM_CONNEXTORS_CLEANUP.md](ROCKSTREAM_CONNEXTORS_CLEANUP.md)
> was accepted, and the S3 source, HTTP/webhook source, object-store sink, and
> the Iceberg/Delta cold-tier sink family are **deleted** at v0.52.3–v0.52.5
> rather than carried to v1 as `Maintain` tier. The reasoning is recorded in
> Phase 16.6; the short version is that `Maintain` is not free, and the
> decision is better made before v0.57 freezes the contract than after.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.52 | Durable Connector Quarantine: Bounded, Inspectable, Replayable DLQ ✅ Done | **Narrowed by the 2026-08-11 rebaseline** from the previously-drafted v0.52 (`CREATE EXPECTATION`, the expectation operator, lineage diagnostics) and v0.53 (DLQ routing plus `warn`/`degrade`/`block` state degradation) down to the one part of that scope a production ingestion lifecycle genuinely needs: a malformed source record must not stall a source, must not vanish, and must not break exactly-once. Ship DESIGN.md §13.3.1's connector-tier per-record decode-failure quarantine, and only that. A record a source cannot decode is written to `rockstream_catalog.dead_letter_queue` (`arrived_at`/`source_name`/`source_offset`/`error_code`/`error_message`/`raw_bytes_hex`/`replay_attempt`) inside the **same M3 commit** that advances that source's `OffsetToken`, so quarantining a poison record is exactly-once with respect to ingestion rather than a side effect that can be lost or double-applied on recovery. Operator path: `ALTER SOURCE ... REPLAY DEAD_LETTER_QUEUE [SINCE .. UNTIL ..]` and `ALTER SOURCE ... DISMISS DEAD_LETTER_QUEUE WHERE ...`; the `RS-1003 connector.decode_error` and `RS-1004 connector.dlq_growing` codes; and **enforced** — not merely declared — bounded retention through the already-present-but-unused `DLQ_RETENTION`/`dlq_warn_threshold`/`dlq_retention_days` options, so the quarantine cannot become an unbounded table nobody owns. The write path is bounded and back-pressured: a source producing nothing but undecodable records degrades to the documented `BLOCKED` state with `RS-1004`, never an unbounded in-memory buffer. **Explicitly out of scope and removed from this roadmap**: `CREATE EXPECTATION`, the expectation operator, state-degradation policies, lineage diagnostics, and the `failure_source` (`connector` \| `expectation`) discriminant column they required — `connector` is now the only producer. A policy language is Tier C new breadth per [ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §3 and is admissible only against a concrete design-partner requirement. | A malformed record injected into a **real** Kafka broker and into a **real** Postgres CDC stream is durably queryable in `rockstream_catalog.dead_letter_queue` while that source's committed offset/LSN still advances exactly once past it, proven by a `kill -9` at every prepare/commit boundary of the quarantine write recovering with neither a lost quarantined row nor a re-applied offset (new `dlq_connector_tests.rs`, LFS + MinIO + real-broker TC); `ALTER SOURCE ... REPLAY DEAD_LETTER_QUEUE SINCE .. UNTIL ..` re-ingests exactly the selected records and increments `replay_attempt` correctly across repeated replays; `DISMISS ... WHERE` removes exactly the matching rows and nothing else; exceeding `dlq_warn_threshold` emits `RS-1004` as a `NOTICE` and a metric, and a source quarantining continuously past that threshold reaches `BLOCKED` rather than buffering; a source quarantining past `DLQ_RETENTION` shows its oldest rows expired and the table's on-disk size flat rather than growing (new `dlq_retention_tests.rs`). | Unit, LFS, MinIO, TC |

### Phase 16.5 — View Lifecycle & Source Transaction Correctness (found by the 2026-08-11 future-roadmap review; mandatory before v0.57)

**Why these two versions are inserted here rather than deferred past v1.0.**
The [recommended-future-roadmap.md](recommended-future-roadmap.md) review
proposed six future work items. Four were folded into existing versions or
deferred (see that document for the full disposition); two turned out to be
prerequisites of the **v0.57 v1 contract** rather than successors to it, and
are scheduled here in the only slot that works — after ingestion failure
containment (v0.52) and before the operability, security, and contract
versions that assume the lifecycle is already correct. Per this document's
standing convention for review-found gaps (v0.42.x, v0.45.x, v0.51.x), they are
inserted as decimal sub-versions so nothing downstream is renumbered.

v0.57 freezes a public contract committing to document, for every `Core`
operator, its incremental, **backfill**, checkpoint/recovery, state-growth, and
failure semantics — and it names **PostgreSQL CDC** as one of the two `Core`
connectors. Neither clause can be written honestly today:

1. **Backfill is a non-resumable one-shot.** `MigrationState`'s `Snapshotting`
   and `CatchingUp` phases belong to *shard migration* (v0.46), not to view
   creation; there is no durable backfill cursor anywhere; and `SHOW BACKFILL
   STATUS`, though documented in `docs/concepts.md`, was found unwired to
   gateway dispatch by the v0.45.5 documentation audit and is still unwired.
   Creating a materialized view over a large existing table is the first thing
   every user does, and today a crash during it restarts the entire scan.
2. **Postgres CDC has no transaction envelope.** `pgoutput` decoding
   (v0.51.16) is row-at-a-time, so a single upstream transaction touching two
   imported tables can be observed half-applied by a reader. Freezing that as a
   release-gated `Core` connector is the wrong order of operations.

**No new SQL family, connector family, catalog, or policy subsystem is added**,
so the Phase 16 rebaseline rule still holds: v0.52.1 is materialized-view
lifecycle correctness and v0.52.2 deepens an already-shipped connector.

**One structural constraint binds the two.** The snapshot/delta fence built in
v0.52.1 is the *same primitive* a durable snapshot-plus-live `SUBSCRIBE`
protocol needs (recorded as an unscheduled candidate after Phase 17). It ships as a
named, reusable primitive with its own tests, not as private backfill
machinery, so it is built once rather than twice with subtly different
semantics.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.52.1 | Resumable Online Backfill & the Snapshot/Delta Fence ✅ Done | Make materialized-view creation a durable streaming operation instead of a one-shot bootstrap. Ship the **snapshot/delta fence** as a first-class, independently-tested primitive: an atomic binding of an initial snapshot to the exact source position (Kafka offset / Postgres LSN / object-store listing watermark) from which live changes resume, with no gap and no overlap, committed inside the same M3 epoch commit that records the snapshot. On top of it: a **durable per-partition backfill cursor** (last processed key plus source position) checkpointed with the pipeline, so `kill -9` mid-backfill resumes from the last committed cursor instead of rescanning; **interleaving** of historical snapshot rows with live changes rather than pausing ingestion for the duration of a large backfill; explicit `SNAPSHOTTING` → `CATCHING_UP` → `RUNNING` lifecycle phases on the view itself (distinct from, and not reusing, v0.46's shard-migration state machine); a **separate backfill resource budget** in the v0.45.1/v0.51.23 admission controller so a new view's backfill cannot starve the freshness of already-running views; and a **publication gate** — a view becomes queryable only once its backfill has caught up to a committed cluster frontier, never exposing a partially-populated relation. Wire `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW <name>` into the real gateway dispatch table (closing the v0.45.5 finding) reporting phase, per-partition cursor position, rows remaining, and an estimate, plus the matching `rockstream view status` fields for v0.53/v0.54 to consume. Backfill stalls and rejections surface enumerated `RS-XXXX` codes, not free text. | A backfill of a multi-million-row table is `kill -9`'d at three different points (mid-snapshot, mid-interleave, mid-catch-up) and each time resumes from the last committed cursor without rescanning already-processed partitions, with the finished view bit-identical to the DataFusion batch oracle (new `backfill_resume_tests.rs`); the fence primitive has its own gap/overlap property test asserting every source record is applied exactly once across the snapshot-to-live boundary under injected restarts; a `SELECT` against a view in `SNAPSHOTTING`/`CATCHING_UP` never returns partial contents (it blocks or errors with its documented code — never a wrong answer); an already-running view's freshness SLO holds while a large backfill runs concurrently (TC, multi-worker); `SHOW BACKFILL STATUS` is reachable over raw pgwire through the real dispatcher, not a unit test importing private modules. | Unit, LFS, MinIO, TC |
| v0.52.2 | Transaction-Preserving PostgreSQL CDC & Upstream Schema Evolution ✅ Done | Consolidate PostgreSQL ingestion around shared logical-replication streams that preserve upstream transaction boundaries. The binding correctness rule: **a transaction modifying multiple imported tables must never become visible as only a subset of those changes.** Ship a **transaction-level CDC envelope** decoded from `pgoutput`'s `BEGIN`/`COMMIT` frames carrying the upstream xid, commit LSN, and all affected rows across all imported tables; **atomic application** of that envelope, with the explicit invariant that *an upstream transaction never spans a RockStream epoch boundary* — every row of one Postgres transaction lands in exactly one epoch commit, which is what makes all-or-nothing visibility a consequence of the already-verified M3 commit protocol rather than a second mechanism; **shared replication slots and connections** so one replication runtime feeds every table imported from the same Postgres source, instead of one slot per table; and **bounded large-transaction handling** that reuses the v0.51.24 arrangement spill rather than inventing a second spill path — a transaction larger than the memory budget spills durably and still applies atomically, never exposing partial state and never growing an unbounded in-memory buffer. **Upstream schema evolution** is in scope because it becomes a first-order concern the moment `pgoutput` is the canonical `Core` path: a `Relation` message describing a changed upstream table is decoded and classified as compatible (applied, recorded in `schema-evolution history`) or incompatible (`RS-1002`, source degrades to the documented `BLOCKED` state with an operator-actionable message) — never silently mis-decoded into wrong column positions. `pgoutput` becomes the canonical PostgreSQL CDC path; the other wire formats already supported stay `Maintain` tier and are not growth areas. | An upstream transaction updating two imported tables is never observed partially: a reader polling both tables across the commit sees either all changes or none, at every epoch, under injected worker restarts and replication-stream disconnects (new `cdc_transaction_atomicity_tests.rs`, TC with real PostgreSQL); an upstream transaction larger than the configured memory budget spills and still applies atomically with the resulting tables bit-identical to the batch oracle; a single replication slot demonstrably feeds N imported tables (slot count on the real server asserted, not inferred); an upstream `ALTER TABLE ADD COLUMN` continues streaming and is recorded in schema-evolution history, while an incompatible `ALTER TABLE` returns `RS-1002` and blocks the source rather than corrupting decoded rows; source restart after each of these resumes from the committed LSN with no loss and no duplicates. | Unit, LFS, MinIO, TC |

### Phase 16.6 — Connector Surface Reduction (accepted from ROCKSTREAM_CONNEXTORS_CLEANUP.md, 2026-08-12; mandatory before v0.57)

**This phase deliberately reverses the Phase 16 rebaseline's "nothing already
shipped is removed" clause — for the connector surface only.**
[ROCKSTREAM_CONNEXTORS_CLEANUP.md](ROCKSTREAM_CONNEXTORS_CLEANUP.md) was
accepted. RockStream's supported external integration boundary becomes two
sources and one sink:

```text
Sources: PostgreSQL CDC, Kafka
Sink:    Kafka
```

The S3 source, the HTTP/webhook source, the object-store sink, the Iceberg
sink, the Delta Lake sink, the generic cold-tier sink infrastructure,
connector-specific cold-tier GC, and external lakehouse catalog registration
are **deleted from the codebase**, not deprecated, hidden behind a flag, or
left in maintenance mode.

**Why this is scheduled here and not left as `Maintain` tier.** The Phase 16
rebaseline chose `Maintain` over removal, on the reasoning that proven code
should not be ripped out merely to look smaller. That reasoning is sound and
still governs the non-connector Tier B list. It does not survive contact with
v0.57. `Maintain` is not free: every retained connector is a permanent
compatibility commitment, a dependency, CI wall time, an attack surface, a set
of failure and recovery semantics, and a row that **v0.58's failure matrix and
v0.59's unscoped reachability sweep must both cover** — a `Maintain`-tier
connector still has to be correct under object-store brownout, checkpoint
interruption, and sink-commit failure, or the v1 gates are not honest. Paying
that permanently, for connectors whose value Kafka already carries, is a worse
trade than deleting them once. Deciding at v0.52.3 rather than discovering it
at v0.57 means the v1 contract is written over the surface that actually
exists.

**Three things this phase explicitly does not touch.** (1) The PostgreSQL wire
protocol is RockStream's native query and application interface, not an
optional connector, and is unaffected. (2) RockStream's internal use of object
storage — SlateDB state, checkpoints, spill, recovery, disaster-recovery export
— is unaffected; removing an object-store *sink* is not removing object storage
from the storage architecture, and conflating the two is the main way this
change could be implemented wrongly. (3) The sink two-phase-commit machinery
and the M3 model that verifies it stay, now covering the single remaining sink.

**Ordering.** The proposal's five migration phases collapse into three
versions: announce and fail closed at the frontend (v0.52.3), delete the
implementations, dependencies, and dead abstractions (v0.52.4), then spend the
reclaimed budget on the three survivors (v0.52.5). Frontend before
implementation is the binding order — an operator upgrading must meet a coded
migration error, never a silent no-op against a connector that quietly stopped
existing. Per this document's standing convention for review-found and
accepted-proposal work, these ship as decimal sub-versions, so **nothing at
v0.53 or later is renumbered**.

**One formal-verification consequence.** `formal/m5_cold_tier_sink.fizz` models
cold-tier exactly-once for an implementation that will no longer exist. It is
**retired**, not deleted — moved to `formal/retired/` with a header recording
why — and dropped from the `make verify` set at v0.52.4. M1–M4, M6, and M7 are
unaffected.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.52.3 | Connector Surface Freeze: Announced Removal & Fail-Closed Migration Errors ✅ Done | The proposal's Phase 1 and Phase 2 — announce the narrowed boundary and remove the *creation* path, while the implementations are still present, so the frontend gate is proven in isolation before anything is deleted. Publish the boundary in `README.md`, `docs/concepts.md`, `docs/language-features.md`, and `docs/configuration.md`, plus a new `docs/connector-migration.md` giving the concrete replacement for each removed connector: S3 source → an external loader writing through pgwire or Kafka; HTTP/webhook source → an external HTTP→Kafka (or HTTP→PostgreSQL) adapter outside the RockStream server; object-store, Iceberg, and Delta sinks → RockStream → Kafka → a downstream writer that owns that format. Make every removed surface **fail closed** under one new enumerated code, `RS-4017 connector.removed`, whose message names the replacement path: `CREATE SINK ... TO ICEBERG\|DELTA` and its whole `WITH (...)` option grammar (`snapshot_interval_epochs`, `snapshot_interval_ms`, `parquet_row_group_bytes`, `format_version`, `partition_by`, `catalog=filesystem\|glue\|hive\|rest\|ducklake`) — today validated by `RS-4007` in `handle_create_sink`; `CREATE SOURCE` of the S3 and webhook types together with the webhook HTTP endpoint itself; and the cold-tier fields of `rockstream.toml`'s `StorageTieringConfig`. An existing catalog row for a removed source or sink must **load into a visible `REMOVED` state** reported by `SHOW SOURCES`/`SHOW SINKS` with `RS-4017` — never silently dropped, and never a startup failure — so an operator upgrading discovers the change from the running system rather than from a changelog. `docs/language-features.md` gains a **Removed** table (keyword, removal version, replacement) and the existing documentation-drift gate (v0.45.5, v0.51.11) checks it, so a removed keyword cannot drift back to "implemented". | Every removed DDL form returns `RS-4017` with its documented replacement path through the **real pgwire dispatcher** — raw SQL over the wire, not a unit test importing private modules (new `connector_removal_tests.rs`); a catalog written by v0.52.2 containing an Iceberg sink, an object-store sink, an S3 source, and a webhook source starts cleanly under v0.52.3 and reports all four as `REMOVED` with `RS-4017`, on LFS and MinIO; the webhook HTTP endpoint returns a coded rejection rather than accepting and discarding a delivery; `scripts/check-error-codes.sh` accepts `RS-4017` and the docs audit finds zero references to a removed connector outside `docs/connector-migration.md` and the Removed table; the **entire existing suite is still green with every implementation still compiled in**, proving this version changed only the frontend. | Unit, LFS, MinIO |
| v0.52.4 | Deletion: Removed Connectors, the Cold-Tier Family & Their Dependencies ✅ Done | The proposal's Phases 3–5 — delete the implementations, the dependencies, and the abstractions that existed only to serve them. From `rockstream-connectors`: `s3_source.rs`, `object_store_sink.rs`, `iceberg_sink.rs`, `delta_sink.rs`, `cold_tier_sink.rs`, `cold_gc.rs`, `partition_spec.rs`, `catalog_registrar.rs` (Glue/Hive/REST/DuckLake registration) and their `lib.rs` re-exports. From `rockstream-gateway`: `webhook_source.rs` and its `server.rs` wiring — the `webhook_sources` registry and the create/pause/resume/drop/ingest handlers. The crate's public surface becomes exactly `source_connector`, `source_epoch`, `source_runtime`, `postgres_cdc`, `kafka_source`, `sink_connector`, `kafka_sink`, plus `fault_injecting_store`, which the retained Kafka and CDC fault tests use. **Dependency audit, arbitrated by `cargo tree`/`cargo deny` rather than judgement**: remove `iceberg`, `deltalake`, `aws-sdk-s3`, `aws-config`, and `csv` from the crate and from the workspace root where nothing else needs them; remove `parquet` and `object_store` *from this crate only* if unused there, and retain them wherever `rockstream-storage` or another core subsystem legitimately requires them — the objective is a smaller **effective** dependency graph, never an artificial architectural change to force a removal. **Dead-abstraction sweep**: every hook on `SinkConnector`/`SourceConnector` that existed only because a removed connector needed it is deleted rather than kept for hypothetical extensibility, explicitly including the `ColdGcCatalog` trait and the per-sink pending-epoch accessors only the removed sinks implemented. Retire `formal/m5_cold_tier_sink.fizz` to `formal/retired/` with a header recording why, and drop M5 from `make verify`. Delete `docs/cold-tier-sinks.md`, redirected from `docs/connector-migration.md`. | `rockstream-connectors`'s `lib.rs` exports exactly the retained modules and the workspace builds under `-D warnings` with **no `#[allow(dead_code)]` added** to absorb the deletion; `cargo tree -i` returns no path from any workspace crate to `iceberg`, `deltalake`, `aws-sdk-s3`, `aws-config`, or `csv`; the sign-off publishes a measured before/after of total dependency count, `cargo build --release` wall time, and CI wall time — a deletion version's value has to be measurable, not merely tidy; the **full retained suite** (exactly-once, recovery, chaos, Nexmark, pgwire conformance) is green on LFS, MinIO, and TC with **zero tests disabled, deleted, or `#[ignore]`d to make the deletion pass** — any test that must go is one whose only subject was a removed connector, enumerated by name in the sign-off; `make verify` is green with M5 retired and M1–M4/M6/M7 byte-for-byte unchanged; a repo-wide grep across `crates/`, `docs/`, `scripts/`, and `formal/` finds zero live reference to a deleted module. | Unit, LFS, MinIO, TC |
| v0.52.5 | Depth Over Breadth: The Three-Connector Guarantee Matrix ✅ Done | Spend the reclaimed budget on the connectors that remain, and write the result down as a **guarantee** rather than a test count. Publish `docs/connectors.md` stating, for each of PostgreSQL CDC, Kafka source, and Kafka sink, its delivery guarantee, offset/LSN recovery contract, buffering and backpressure bounds, degradation states, and enumerated failure codes — the same five-axis discipline v0.57 requires of every `Core` operator, applied to the three `Core` connectors, so v0.57 cites it instead of re-deriving it. Fill the proposal's redirected test matrix against real systems. **PostgreSQL CDC**: snapshot→stream handoff over v0.52.1's fence, every mutation type, restart at every commit boundary, WAL lag, malformed replication records, replication-slot loss, publication loss, backpressure, long-running recovery. **Kafka source**: consumer rebalance mid-epoch, partition expansion, offset recovery, broker interruption, bounded buffers, duplicate prevention, transactional source/sink interaction. **Kafka sink**: crash before commit, crash during commit, uncertain broker response, transaction timeout, recovery re-run, duplicate prevention, checkpoint coupling. Every cell runs against a real broker and a real PostgreSQL under TestContainers — v0.51.21 removed the mocks and `scripts/check-no-mock-connectors.sh` keeps them out. Make the proposal's **Extensibility Policy enforceable rather than aspirational**: a new source or sink module in `rockstream-connectors` fails CI unless the change carries the six-point admission record (`scripts/check-connector-admission.sh`, built in the same style as the existing `check-no-mock-connectors.sh` and its paired `.test.sh`). | Every cell of the published matrix maps to a named passing test in the sign-off, with no cell marked N/A without a written reason; every failure-injection cell asserts a **recovery outcome** — no loss, no duplicates, bounded recovery time against the v0.22 and v0.31 SLOs — never merely "did not crash"; `docs/connectors.md`'s failure-code table is machine-checked against the `RS-XXXX` registry in both directions, so a documented code that does not exist and a connector code that is undocumented each fail CI; a PR adding a synthetic new sink module fails `check-connector-admission.sh` and passes only once the admission record is present, with `check-connector-admission.test.sh` proving both directions per the repo's script-test convention; `rockstream-connectors` coverage meets the same 70/70 line/region gate the gateway carries. | Unit, LFS, MinIO, TC |

### Phase 17 — Production Readiness & Qualification


Every version below is a production-readiness gate, sequenced per
[ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §10: an operator
must be able to *inspect* the engine (v0.53–v0.53.2) and get a *straight answer*
out of it (v0.54–v0.54.1) before it is worth *securing* (v0.55–v0.55.2), and all
three before the system can credibly promise *upgrades and disaster recovery*
(v0.56–v0.56.1) — which is what makes a written v1 contract (v0.57–v0.57.1) a
measurable commitment rather than an aspiration, and what the failure matrix
(v0.58–v0.58.3) and the release gate (v0.59) then verify. None of these versions
adds a new SQL family, connector family, catalog, or policy subsystem; every one
of them makes the already-shipped system safer to run.

**Realism review of v0.53–v0.59 (2026-08-12).** The seven versions in this phase
were sized as *themes* rather than as deliverable units of work, and four of
them bundled three independent projects apiece behind a single sign-off. This
review keeps every commitment and changes none of the sequencing. It splits the
oversized versions into units that can each be planned, implemented, proven, and
signed off on their own, and it corrects five scheduling assumptions that could
not hold as written. Per this document's standing convention for review-found
adjustments (v0.42.x, v0.45.x, v0.51.x, v0.52.x), the splits are decimal
sub-versions, so **nothing is renumbered and `v1.0.0` still ships at v0.59**.

1. **Four versions were three projects each.** v0.53 bundled a read-only
   inspection CLI, a mutating cluster-control CLI, and an engine-level
   arrangement debugger; v0.55 bundled internal mTLS, a secrets subsystem with
   four KEK backends, and a third-party security audit; v0.56 bundled a
   version-compatibility gate plus upgrade drill and a cross-region
   export/restore story; v0.58 bundled a simulated failure matrix, its
   real-backend counterpart, and a storage-pressure control loop. Each is now
   its own version, in dependency order. A version whose exit criteria span
   three unrelated subsystems cannot be honestly blocked or honestly signed
   off — the failing third drags the finished two with it.
2. **The release candidate contained discovery work.** v0.59 chartered an
   *unscoped* reachability, dispatch-wiring, and silent-wrong-answer sweep whose
   findings were, by its own exit criteria, RC1 blockers. An audit whose fix
   list is unknown when it starts cannot sit inside the gate that measures
   whether the release is ready. The sweep moves to **v0.58.3** and becomes an
   *entry criterion* for RC1; v0.59 becomes a pure gate — activate, soak, sign
   off the seven gates, tag.
3. **A third-party engagement cannot block a sequential pipeline.** The
   independent security review has vendor selection, scheduling, and an unknown
   remediation scope, none of which is engineering work. It is now commissioned
   at the start of **v0.53** purely for lead time, its remediation is
   **v0.55.2**, and if the report has not landed the pipeline continues: the
   closed-findings requirement is carried as one of v0.59's named release gates,
   where it already appears.
4. **Two scope items were unbuildable or untestable as written.** Four KEK
   backends (`env`/`aws_kms`/`gcp_kms`/`vault`) each need real credentials and a
   real integration target; `env` plus one cloud KMS ship as `Core` behind a
   provider trait, and `gcp_kms`/`vault` move to *Deferred by decision*. And
   `rockstream migrate` cannot be proven by a version that introduces no format
   change, so v0.56 ships a synthetic N→N+1 format-bump fixture and migrates
   real shards across it, rather than shipping an unexercised tool.
5. **Two proofs hid the real deliverable.** v0.54's "the decomposed components
   sum to end-to-end lag within a documented tolerance" is not a metrics-export
   task but **per-stage timestamp propagation** through the whole pipeline, and
   the tolerance has to be published *from* measurement rather than asserted
   before it. And the arrangement debugger has to decode every arrangement key
   encoding the engine actually uses — `GroupKeyPacker` surrogates,
   `Utf8ColumnPacker` columns, window ids, join-side row encodings — or it
   prints bytes an operator cannot act on. Both are now stated as the work.

One ordering hazard is recorded rather than fixed by moving work: v0.53's CLI
talks to the control plane before v0.55 gives that channel mTLS. v0.53 therefore
builds its transport behind an identity-pluggable seam and v0.55 wires client
certificates into it without a redesign. Building an unauthenticated CLI
transport first and retrofitting it after the security version is the sequence
that ships an insecure default.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.53 | Operator CLI: Substrate & Read-Only Inspection Surface ✅ Done | **Split by the 2026-08-12 realism review**: the mutating lifecycle commands move to v0.53.1 and the IVM arrangement debugger to v0.53.2. What stays here is what an operator needs first anyway — the ability to *see* — plus the transport, output-contract, and test substrate the other two build on. **Strategic core** ([ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §5.2): the engine is already sophisticated, and the next thing a production user needs is not more hidden machinery but the ability to inspect and diagnose what is already there. **Found missing in the <=v0.42.3 review**: DESIGN.md §14.7/§14.7.1/§14.19 fully specify an operator CLI surface and an IVM arrangement debugger, but no roadmap version ever implemented them — `rockstream-cli` still only has a `start` subcommand as of v0.42.3 (shard migration/drain now ships at v0.46, `EXPLAIN INCREMENTAL`/`SHOW RESOURCE USAGE` reachability now ships at v0.45, and the workload quota/admission-control substrate now ships at v0.45.1; this version wraps the remaining workload/view/schema/source/checkpoint/audit/support-bundle/debug-arrangement commands). Ship the substrate once — a control-plane/catalog/`DbReader` client behind an **identity-pluggable transport seam**, so v0.55's internal mTLS wires client certificates into it without a redesign; a `--json`-or-human-text output contract; and a golden-output test harness — then the read-only inspection commands as thin wrappers over already-shipped APIs (no new services, no new config files, one binary): `view {list,show,status}`, `source {list,show}`, `schema {list,show}`, `workload {list,show}`, `cluster {status,quotas}` and `cluster workers {list,status}`, `shard list`, `checkpoint list`, `resource {usage,usage --workload=<name>,cluster}` (§14.19), `schema-evolution {status,history}`, `audit {tail,query}`, `explain <view> [--estimate]`, and `sql "<query>"` (parse/lower/`EXPLAIN INCREMENTAL` against the catalog without deploying). Every subcommand supports `--json` for scripting and human-readable text by default; `SHOW VIEW STATUS`/`SHOW RESOURCE USAGE` are wired through pgwire as the SQL-native equivalents. **The independent third-party security review whose findings are remediated at v0.55.2 is commissioned at the start of this version**, purely for lead time — it is the one item in this phase that cannot be scheduled by finishing the version before it. | An operator answers *which views are stale, which worker owns which shard, and what is this workload consuming* against a running cluster using only documented read-only commands — no source reading, no ad hoc scripts (new `cli_read_only_tests.rs`, TC); every subcommand has a golden-output test in both `--json` and text form, and its `--json` output validates against a checked-in schema; no read-only command takes a pipeline lock or perturbs progress, asserted by comparing freshness lag during a continuous polling loop against a control run; every subcommand is documented in `docs/cli.md`, enforced by a doc-coverage test that fails when a subcommand ships undocumented. | Unit, LFS, TC |
| v0.53.1 | Mutating Operator Commands: Lifecycle Control, Support Bundle & Safe-by-Default Semantics ✅ Done | **Split out of v0.53 by the 2026-08-12 realism review**: the half of DESIGN.md §14.7's surface that *changes* cluster state needs an authorization, confirmation, idempotency, and audit story the read-only half does not, and bundling the two hid that work behind a command list. Ship `view {pause,resume,query,subscribe}`, `source {pause,resume,drop}`, `schema {create,drop}`, `workload {create,alter,drop}`, `cluster workers drain`, `shard migrate`, `checkpoint restore`, and `support bundle [--view=<name>]` on v0.53's transport. Every mutating invocation authenticates as the identity the transport presents, authorizes through the already-shipped RBAC, and emits one audit event naming the operator and the exact arguments. Every command is **idempotent or explicitly refuses**: a re-issued `shard migrate` against an in-flight migration returns an enumerated refusal, never a second migration. Destructive commands require interactive confirmation or `--yes`, and every refusal is an `RS-XXXX` code with a documented remedy rather than free text. `support bundle` produces the v0.45.5 diagnostic artifact with secret redaction and a documented size bound (closing the stale `rockstream support-bundle` reference that audit found in `docs/sre-operations.md`). `view subscribe` is a long-lived streaming client and inherits the existing `RS-2006` cursor semantics rather than inventing its own. | Every mutating subcommand runs end to end against a live multi-worker cluster (new `cli_mutating_commands_tests.rs`, TC): each emits exactly one audit event carrying the invoking identity and arguments; each is immediately re-issued and either reaches an identical end state or refuses with its documented code; `cluster workers drain` and `shard migrate` are interrupted mid-flight and leave the cluster in a documented, resumable state rather than a stuck one; `support bundle` contains no secret material and stays under its documented size cap on a loaded cluster; an unauthorized identity is refused and audited for every command in the set, with no command missing from the check because the test enumerates the dispatch table rather than a hand-written list. | Unit, LFS, TC |
| v0.53.2 | IVM Arrangement Debugger ✅ Done | **Split out of v0.53 by the 2026-08-12 realism review**: DESIGN.md §14.7.1's `rockstream debug arrangement <view> <op_id> <key> [--epoch=N]` — the tool an operator reaches for when a view is *wrong*, not just slow — is an engine capability, not a CLI wrapper, and its one-line scope hid three separate problems. **(1) Addressability**: `rockstream explain <view> --op-ids` prints the compiled pipeline's stable `OperatorId`s, without which no key can be addressed at all. **(2) Key decoding**: the debugger accepts and prints *user-level* keys, which means reversing every arrangement key encoding the engine actually uses — `GroupKeyPacker` composite surrogates, `Utf8ColumnPacker` columns, window ids, and join-side row encodings — and where a family cannot be reversed it refuses with an enumerated code naming that family, never printing raw bytes an operator cannot act on. **(3) Non-perturbing reads**: live reads go through `DbReader` without taking a pipeline lock or advancing any frontier, and `--epoch=N` reads historical state within checkpoint retention, refusing with a documented code outside it. | `debug arrangement orders_mv <op_id> "product_id=42"` returns the correct Z-set weight for every stateful operator family that ships (`Aggregate`, `MinMax`, `Join`, `TopK`, `Distinct`, and the `Tumble`/`Hop`/`Session` window operators), verified against the same value computed by the DataFusion batch oracle (new `arrangement_debugger_tests.rs`); a composite `GROUP BY` key and a `Utf8` key both round-trip through the surrogate encodings and print as user-level values, and a family with no decoder refuses by name instead of printing bytes; `--epoch=N` inside retention returns bit-identical historical state and outside it fails with its documented code; the debugger polls a loaded pipeline continuously with no freshness-lag regression against the same run without it. | Unit, LFS, TC |
| v0.54 | Per-Stage Freshness Lag Accounting & Barrier Flight Time ✅ Done | **Split by the 2026-08-12 realism review**: the enumerated degradation-reason taxonomy and its runbook lock move to v0.54.1; this version builds the measurement those reasons name. The deliverable is **per-stage timestamp propagation** through ingest, compute, checkpoint alignment, and sink commit — not a metrics export over numbers the engine already has — and the summation tolerance is published *from* this version's measurements rather than asserted before them. **New in the 2026-08-11 rebaseline** ([ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §5.2 and §10 Stage 2): v0.53 gives an operator the tools; this version makes the system answer the question itself. Decompose end-to-end view freshness lag into separately-attributable components — source lag, decode/ingest lag, compute lag, checkpoint-alignment lag, sink-commit lag, spill activity, and object-store/compaction pressure — each exported on `/metrics` and readable through `SHOW VIEW STATUS <view>` and `rockstream view status`. Naming the single **dominant** contributor, rather than leaving a human to infer it by correlating seven charts, is v0.54.1's job; this version is what makes that attribution possible and correct. The reason taxonomy these components feed — the enumerated blocked/degraded set, dominant-cause attribution, live drain and shard-migration progress, and `rockstream checkpoint show`'s barrier holder — ships at **v0.54.1**. **Barrier flight time is measured separately from checkpoint completion time** (folded in from the 2026-08-11 future-roadmap review, item 5), so "the checkpoint is slow" can be distinguished from "the barrier is stuck behind data traffic" — today control messages and data share the same exchange channels with no prioritization and no way to tell the two apart. Giving control messages a reserved channel or credit budget is deliberately **not** scheduled here: it is admitted only if this measurement shows barriers are in fact being delayed behind ordinary data, and it would strengthen the existing checkpoint architecture rather than introduce a second one. This is a **read path over data the engine already produces** (v0.51.11 stall diagnostics, v0.51.23 real state-byte accounting, v0.51.24 spill metrics, v0.46 migration states): no new services, no new configuration, no new SQL family. | Under sustained load the decomposed lag components sum to the independently measured end-to-end freshness lag within a tolerance **published from this version's own measurements** rather than asserted in advance (new `lag_attribution_tests.rs`, multi-worker TC), and the same holds under an induced source stall, an induced spill, and an induced sink block, so no component silently absorbs another's time; barrier flight time is reported separately from checkpoint completion time, and the two are shown to diverge under a deliberately saturated data channel — the measurement that decides whether control messages ever get a reserved channel; every component is exported on `/metrics` with a documented unit, is non-negative, and is monotonic wherever the design says it must be. | Unit, LFS, MinIO, TC |
| v0.54.1 | Enumerated Degradation Reasons, Dominant-Cause Attribution & Live Progress ✅ Done | **Split out of v0.54 by the 2026-08-12 realism review**: turning v0.54's measurements into an *answer* is a distinct deliverable from producing them, and it is the half that touches every status surface. Replace free-text status strings with an enumerated, documented set of blocked/degraded reasons, each carrying its own `RS-XXXX` code and a linked runbook step: waiting on source, quota-admission rejected (v0.51.23), spilling (v0.51.24), over-budget-relaxed, checkpoint alignment stalled, sink blocked, shard migration or worker drain in progress (v0.46), and recovering. Name the single **dominant** contributor from v0.54's decomposition, and surface reason, code, and dominant contributor through `SHOW VIEW STATUS <view>` and `rockstream view status` alike. Worker drain and shard migration expose live progress — migration-state-machine phase, bytes/rows remaining, and an estimate — instead of only a terminal state, and `rockstream checkpoint show` reports per-shard alignment and names the shard and operator holding the barrier. No new services, no new configuration, no new SQL family: this is a read path over data v0.54 already produces. | Each of the eight enumerated degradation causes is induced deliberately and named correctly and unambiguously by `SHOW VIEW STATUS` with its documented code and dominant-contributor attribution, with zero `unknown` or free-text outcomes (new `freshness_explainability_tests.rs`, multi-worker TC); two causes induced simultaneously yield a deterministic dominant contributor rather than an arbitrary one; a stalled drain and an interrupted shard migration each report monotonically advancing progress rather than a static state; every enumerated reason has a linked remediation step in `docs/sre-operations.md`, enforced by a conformance-lock test in the style of `test_conformance_doc_has_linked_tests`, so a new reason cannot ship without a runbook entry. | Unit, LFS, MinIO, TC |
| v0.55 | Internal mTLS & Node Identity ✅ Done | **Split by the 2026-08-12 realism review** into three versions: this one ships the node-identity substrate, secrets management is v0.55.1, and the independent security review's remediation is v0.55.2 (with the review itself commissioned back at v0.53 for lead time). Secrets that workers must resolve without reading raw values *require* a node identity, so the order is forced rather than chosen. **Production hygiene for the system RockStream already is, not enterprise feature breadth** ([ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §5.3): a cloud-native IVM service cannot call itself production-ready while internal communication is implicitly trusted and connector credentials are handled as a development convenience. **Found missing in the <=v0.42.3 review**: the original implementation plan's Phase 8 exit criteria required "mTLS everywhere", `CREATE SECRET` envelope encryption (DESIGN.md §14.18), and an independent security review before production readiness, but no roadmap version through v0.52 ever scheduled them — only gateway-facing auth (`--auth=scram\|md5\|oidc\|mtls\|off`, v0.26/v0.40) exists today. Ship internal mTLS on every internal channel — control↔worker, worker↔worker gRPC/shuffle, and the v0.53 CLI's control-plane connection, wired into that version's identity-pluggable transport seam — with node identity derived from the presented certificate, a documented issuance and rotation procedure, and an audited denial for every rejected peer. Rotation is proven under load rather than only at startup: a certificate rolls over on a live cluster with no epoch loss and no pipeline restart. Secrets management ships at **v0.55.1**; the independent security review is commissioned at v0.53 and remediated at **v0.55.2**. | A worker or peer connection presenting an invalid, expired, or absent mTLS client certificate is rejected with an audited denial and never processes shard data (new `internal_mtls_tests.rs`, TC); a certificate rotation completes on a 3-worker cluster under sustained load with zero epoch loss, zero pipeline restarts, and no window in which an already-authenticated peer is spuriously refused; the v0.53 CLI reaches the control plane with a client certificate and is refused without one, with no change to its command surface — proving the transport seam held. | Unit, LFS, MinIO, TC |
| v0.55.1 | Secrets Management & Envelope Encryption ✅ Done | **Split out of v0.55 by the 2026-08-12 realism review**, and **narrowed**: DESIGN.md §14.18's secret lifecycle on top of v0.55's node identity — `CREATE SECRET`/`ALTER SECRET`/`DROP SECRET`/`SHOW SECRETS` DDL with envelope encryption; worker-side short-lived secret-token resolution derived from mTLS node identity, so workers never read raw secret values; `CREATE SOURCE ... secret = <name>` / `CREATE SINK ... secret = <name>` replacing inline plaintext credentials; zero-restart rotation of a live source's credentials; and full audit coverage of every secret lifecycle event, with the value never logged, never in an audit payload, never in an error message, and never in a support bundle. **KEK backend breadth is cut from four to two**: `env` and one cloud KMS (`aws_kms`) ship as `Core` behind a `KekProvider` trait, each with a real integration test; `gcp_kms` and `vault` move to *Deferred by decision* below. Four backends each need real credentials and a live integration target, and a release-gated backend nobody can test is a claim, not a feature — the trait keeps either one cheap to add against a workload that asks for it. | `CREATE SECRET kafka_prod (...)` never appears in plaintext in logs, audit events, `SHOW SECRETS` output, error messages, or support bundles — asserted by scanning every emitted artifact for the secret's literal bytes, not by inspecting known fields (new `secrets_redaction_tests.rs`); `ALTER SECRET` rotates a live Kafka source's credentials with zero pipeline restarts and zero failed batches (TC); a worker holds only a short-lived token, proven by asserting the raw value is absent from its on-disk state and from every API a worker can reach; both KEK backends pass one interchangeable conformance test through the trait, and a KEK rotation re-wraps every DEK while leaving every existing secret readable. | Unit, LFS, MinIO, TC |
| v0.55.2 | Security Hardening & Independent Review Remediation ✅ Done | **Split out of v0.55 by the 2026-08-12 realism review, which also fixed its scheduling**: a third-party engagement has vendor selection, calendar, and an unknown remediation scope, none of which is engineering work, so it cannot gate a sequential pipeline. The self-directed half ships regardless and first: a written threat model covering every trust boundary of the complete attack surface (auth, RBAC, secrets, mTLS, gateway SQL-injection surface, TLS rotation, connector credentials), each boundary naming the control that enforces it; a `cargo audit`/`cargo deny` advisory gate in CI with a documented exception process; extension of the v0.51.17 fuzz corpus to every remaining network-facing decoder; and authorization negative tests across every mutating surface v0.53.1 shipped. Then triage and fix every P0/P1 finding from the review commissioned at v0.53. **If the report has not landed when the engineering work is complete, this version signs off on the self-directed half with the outstanding engagement named in the sign-off**, and the "closed with zero open P0/P1 findings" requirement is carried where it already belongs — as one of v0.59's seven named release gates. | The threat model enumerates every trust boundary and names an enforcing control per boundary, each linked to a passing test; `cargo audit`/`cargo deny` fail CI on a deliberately introduced known-vulnerable dependency fixture; every mutating CLI and DDL surface refuses an unauthorized identity with an audited denial, with no gap, because the test enumerates the dispatch table rather than a hand-written list; every received P0/P1 finding is closed and linked to its fix, and any pending engagement is named in the sign-off and recorded against the v0.59 security gate rather than silently dropped. | Unit, LFS, TC |
| v0.56 | Version Compatibility Gates & the Rolling-Upgrade Drill ✅ Done | **Split by the 2026-08-12 realism review**: disaster recovery moves to v0.56.1, which also carries the **Operationally Complete** milestone. Upgrading a running cluster and restoring a lost one share a motivation but no machinery, and each is a full version of work. **The proof that makes object-storage-backed disposable compute a credible production promise** ([ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §5.4) — more important to v1 than any additional query or governance feature, and the last gate before the **Operationally Complete** milestone. **Found missing in the <=v0.42.3 review**: DESIGN.md §5.5 fully designs a storage-format version gate, a `rockstream migrate` tool, and an N/N+1 rolling-upgrade contract, and the original implementation plan required "a documented disaster-recovery procedure executed successfully", but no roadmap version ever implemented or end-to-end tested either. Ship `rockstream migrate --from=N --to=M --storage=<url>` as the offline shard-format migration tool; an end-to-end mixed-version cluster test proving the rolling-upgrade contract: the shard-format-version gate (`RS-5001`) refuses to open out-of-range shards; the gRPC `protocol_version` header gate rejects incompatible peers under its own dedicated error code (resolving the `RS-5002` numbering collision between `merge.unknown_law` and `protocol.version_not_supported` flagged during this review — protocol-version rejection is reassigned to `RS-5021`); the control plane withholds cross-version pipeline/shard assignment until enough N+1 workers are available, verified by a `SimRuntime` mixed-version scenario and a real two-binary-version TestContainers upgrade drill. **`rockstream migrate` is proven against a real format change**: a tool that migrates shard formats cannot be tested by a version that introduces no format change, so this version ships a synthetic N→N+1 shard-format bump fixture and migrates populated shards across it. Shipping the tool without a format to migrate would ship an unexercised tool. **Disaster recovery ships at v0.56.1.** | A 3-worker cluster rolls from binary N to N+1 one worker at a time with zero epoch loss and zero downtime for in-flight pipelines (new `rolling_upgrade_tests.rs`, TC with two built binaries); a worker on an out-of-range shard format is refused with `RS-5001`; an incompatible-protocol peer is rejected with `RS-5021`, never the collided `RS-5002`; `rockstream migrate --from=N --to=M` migrates populated shards across this version's synthetic format bump, and the migrated shards open, recover, and produce bit-identical query results, while a migration interrupted halfway is re-runnable and never leaves a shard unopenable. | Unit, LFS, MinIO, TC |
| v0.56.1 | Disaster Recovery: Checkpoint Export & Restore Into a New Cluster ✅ Done | **Split out of v0.56 by the 2026-08-12 realism review**: the half of the production-readiness promise that in-place crash recovery does not cover, and the one that has never been rehearsed. Ship cluster-checkpoint **export** to a distinct bucket, account, or region on a documented schedule, with the export itself consistent against the M3 epoch commit rather than a file copy of a moving target; a **restore-into-a-freshly-provisioned-cluster** procedure whose only input is that bucket, requiring no surviving control-plane state; RPO and RTO **measured by the drill and published as numbers**, not asserted as targets in advance; and a `docs/disaster-recovery.md` runbook covering full-region-loss recovery, the restore procedure, and a periodic restore-drill recommendation. Export and restore run through the v0.53.1 CLI, not through a private script that only its author can operate. | A full-cluster checkpoint exported to a second bucket in a separate region is restored into a freshly provisioned cluster, reproducing pre-disaster committed state bit-identically for every view (new `disaster_recovery_tests.rs`, MinIO + TC); the drill is executed end to end by following `docs/disaster-recovery.md` and nothing else, and the RPO and RTO it measures are published in that runbook; an export taken while the cluster is under sustained write load restores to a single committed epoch, with no partial epoch and no torn view; a restore from a deliberately truncated export fails closed with an enumerated code rather than producing a partially populated cluster. **Operationally Complete** milestone. | Unit, LFS, MinIO, TC |
| v0.57 | The v1 Public Contract: Tiering & the Machine-Checked Substrate ✅ Done | **Split by the 2026-08-12 realism review**: writing five documented behaviours for every `Core` operator is the bulk of this version and is *content* work, so it moves to v0.57.1; what stays here is the contract's substrate and the tier assignment that makes the content job finite. **New in the 2026-08-11 rebaseline** ([ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §7, §9.3–§9.4): write the v1 promise down *before* the RC gate measures it, and make it machine-checked rather than prose. **(1) One product promise**, stated identically in `README.md`, `DESIGN.md`, and `NEW_IMPLEMENTATION_PLAN.md`: RockStream ingests changing data, continuously maintains a documented SQL subset as durable materialized views, and serves globally committed results to PostgreSQL-compatible clients while surviving ordinary distributed-system failures without losing or silently corrupting committed state. **(2) A strategic tier per capability** — `Core` (release-gated, part of the compatibility contract), `Maintain` (shipped, regression-tested, secure, not a growth area), `Experimental` (no continuity guarantee) — assigned to every entry in `docs/language-features.md` and to every connector and sink, replacing today's implemented-versus-planned axis, which cannot express "supported but frozen." **(3) The enumerated `Core` operator inventory**, derived from the dispatch table rather than hand-written — the list itself is what turns v0.57.1's semantics documentation into a finite, checkable job instead of an open-ended one. The five documented behaviours per operator (incremental, backfill, checkpoint/recovery, state-growth, failure) ship at **v0.57.1**, because the metric that matters is not how many operators exist but whether the project can state what each one does under change, replay, and failure. **(4) Core connectors are PostgreSQL CDC and Kafka**, and after Phase 16.6 they are the *only* connectors — there is no `Maintain`-tier connector surface left to describe, and `docs/connectors.md` (v0.52.5) is the guarantee document this clause ratifies rather than re-derives; the contract additionally states that `RS-4017` and `docs/connector-migration.md` are the permanent, documented answer for a removed connector. **(5) A capability matrix generated from a structured, checked-in source of truth** (`capabilities.toml`) that CI cross-checks against this roadmap's version table, so the matrix cannot drift the way the annotations corrected in the 2026-07-11 reviews did. The 2026-08-12 realism review rejected generating it by parsing this document's cells: they are paragraphs, not data, and a generator built on prose would be the most fragile part of the contract it exists to protect. **(6) A deprecation and compatibility policy for `Maintain`-tier features**, so "not a growth area" never silently becomes "removed without notice", plus the focus document's §8 admission rule as a required checklist for any future version proposing new product surface. No engine behaviour changes in this version; it is a contract, its enforcement, and the documentation reconciliation that follows from both. | A new `scripts/check-capability-contract.sh` and its paired `.test.sh` self-test (per the convention established by `check-error-codes.sh`/`check-invariant-pairs.sh`) fail CI if any `Core`-tier statement, keyword, connector, or sink lacks a tier or lacks a linked passing e2e proof test — proven by deliberately deleting one tier assignment and one proof link and observing both fail (the same gate runs in full semantics mode at v0.57.1); the capability matrix regenerates byte-identically from `NEW_ROADMAP.md` in CI, so a roadmap status change not reflected in the matrix fails the build; the product-promise paragraph is byte-identical across `README.md`, `DESIGN.md`, and `NEW_IMPLEMENTATION_PLAN.md`, asserted by a text-lock test; a mock roadmap version submitted without a completed admission-rule checklist fails `scripts/check-exit-criteria.sh`. | Unit |
| v0.57.1 | Core Operator Semantics: Documented Behaviour Under Change, Replay & Failure ✅ Done | **Split out of v0.57 by the 2026-08-12 realism review**: writing, for every operator in v0.57's `Core` inventory, all five behaviours the contract commits to — incremental semantics (what a delta does), backfill semantics (what the v0.52.1 snapshot/delta fence does for it), checkpoint/recovery semantics (what a restart restores and what it recomputes), state-growth semantics (what bounds its arrangement and what happens when the bound is hit, through v0.51.23 accounting and the v0.51.24 spill path), and failure semantics (what error it raises and what state it leaves behind). Each statement links to a passing e2e test that demonstrates it. **Where a behaviour cannot be stated honestly, the operator's tier is lowered rather than the statement softened** — the tier list shrinking to what the project can actually promise is a successful outcome of this version, not a failure of it, and discovering that here rather than during the RC is exactly why it precedes v0.58. | v0.57's `scripts/check-capability-contract.sh` runs in full semantics mode and passes: every `Core` operator has all five behaviours documented and every documented behaviour links to a passing e2e test, proven by deliberately deleting one behaviour statement and one proof link and observing both fail; at least one tier change (up or down) is recorded with its reason, demonstrating the exercise was falsifiable rather than a transcription of what was already assumed; `docs/language-features.md` and the generated capability matrix agree byte-for-byte after regeneration. | Unit |
| v0.58 | Failure Matrix: Enumeration & Deterministic Simulation Coverage ✅ Done | **Split by the 2026-08-12 realism review** into four versions in dependency order: this one enumerates and publishes the matrix and covers every cell deterministically in `SimRuntime`; the real-backend counterparts are **v0.58.1**; the storage-pressure signals and the auto-tuning lock are **v0.58.2**; and the unscoped reachability sweep pulled out of the release candidate is **v0.58.3**. A simulated matrix, a real-cluster chaos suite, and a control loop share a motivation but not a day's work. **Rescoped in the 2026-08-11 rebaseline** from "Simulator Maturity & Auto-Tuning Lock", whose exit criterion — "all remaining external-system edge-cases" — was unfalsifiable and whose implied objective was a larger test count rather than coverage of anything in particular. Replace it with a **published, enumerated failure matrix** covering exactly the production failure modes an object-storage-native IVM system must survive ([ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §5.5): worker loss, control-node loss, exchange interruption and retry-budget exhaustion, source disconnect with offset/LSN recovery, object-store brownout and throttling, spill and compaction pressure, checkpoint interruption, sink failure during commit **and** during recovery, shard-migration interruption, rolling upgrade, and resource exhaustion with recovery. Every cell gets (a) a deterministic `SimRuntime` scenario with a permanent seed corpus, (b) a real-process, real-backend counterpart in the existing `real-cluster-chaos.yml` job wherever the failure mode is only observable against a real system, and (c) an asserted **recovery outcome** — no loss, no duplicates, bounded recovery time against the v0.22 and v0.31 SLOs — never merely "did not crash". Finish the shift from bounded defaults to SLO-driven adaptive control loops **only** to the extent it keeps the IVM freshness SLO healthy under these failures: the auto-tuner is a means to the freshness contract, not an elasticity product surface, and no new tuning knobs or control surfaces ship here. **Storage-pressure signals are folded in here** (from the 2026-08-11 future-roadmap review, item 4) rather than given their own version, because they extend the controller v0.45.1 built and v0.51.23 wired to real state bytes: L0 backlog, pending compaction bytes, flush latency, write amplification, and object-store latency and failure rate become admission inputs alongside state bytes, with the documented shedding order — throttle backfills (v0.52.1) first, reduce source ingestion second, refuse parallelism increases that would worsen compaction debt third — so a large backfill can never make already-running views indefinitely stale by overwhelming the storage layer. The signals stay **separately attributable and individually exported**; the review's proposed single composite `storage_debt` scalar is explicitly rejected, because collapsing five heterogeneous signals into one number produces a control loop that oscillates with no way to explain why, and v0.54 requires every degradation to name its dominant cause. | The failure matrix is published in `docs/failure-matrix.md`, every cell naming its scenario, its asserted recovery outcome, and the version that owns its proof; every cell has a passing deterministic `SimRuntime` scenario with a permanent seed corpus, and a deliberately deleted scenario fails a new matrix-coverage CI gate; every asserted outcome is a recovery property — no loss, no duplicates, bounded recovery time against the v0.22 and v0.31 SLOs — and a scenario asserting only "did not crash" fails the same gate; every historical FizzBee counterexample seed and chaos seed still replays clean. | Unit, LFS, MinIO, TC |
| v0.58.1 | Real-Backend Failure Proof & Published Recovery Numbers ✅ Done | **Split out of v0.58 by the 2026-08-12 realism review**: every cell of the v0.58 matrix whose failure mode is only observable against a real system gets a real-process, real-backend counterpart in the existing `real-cluster-chaos.yml` job — object-store brownout and throttling, source disconnect with offset/LSN recovery, sink failure during commit **and** during recovery, control-node loss, exchange interruption and retry-budget exhaustion, and resource exhaustion with recovery. Each publishes **absolute** detection, recovery, and freshness numbers against the v0.22 and v0.31 SLOs rather than a relative regression check. **Scheduling correction**: this suite is wall-clock expensive and does not belong on every pull request. It runs on a documented schedule with a per-cell time budget and a published total runtime, and a cell that cannot fit its budget is split rather than quietly dropped — the failure mode this version exists to prevent is a chaos suite so slow that it gets disabled. | Every real-backend cell runs green on the scheduled job, with its absolute numbers published as a checked-in artifact and compared against the previous run; a deliberately injected recovery delay past the SLO fails the job rather than being absorbed as noise; total runtime stays within the documented budget; zero cells enter the v0.59 RC gate marked "not covered" or "simulation only" without an explicit, reasoned exemption recorded in the matrix itself. | Unit, LFS, MinIO, TC |
| v0.58.2 | Storage-Pressure Admission Signals & the SLO Auto-Tuning Lock ✅ Done | **Split out of v0.58 by the 2026-08-12 realism review**: a control loop is not a test suite and cannot be signed off by the same criteria. Extend the controller v0.45.1 built and v0.51.23 wired to real state bytes with storage-pressure signals — L0 backlog, pending compaction bytes, flush latency, write amplification, and object-store latency and failure rate become admission inputs alongside state bytes — with the documented shedding order: throttle backfills (v0.52.1) first, reduce source ingestion second, refuse parallelism increases that would worsen compaction debt third, so a large backfill can never make already-running views indefinitely stale by overwhelming the storage layer. Finish the shift from bounded defaults to SLO-driven adaptive control **only** to the extent it keeps the IVM freshness SLO healthy: the auto-tuner is a means to the freshness contract, not an elasticity product surface, and no new tuning knobs or control surfaces ship here. The signals stay **separately attributable and individually exported**; the single composite `storage_debt` scalar proposed by the 2026-08-11 future-roadmap review is explicitly rejected, because collapsing five heterogeneous signals into one number produces a control loop that oscillates with no way to explain why, and v0.54.1 requires every degradation to name its dominant cause. | Each of the five storage-pressure signals is induced independently and each triggers the documented shedding step in the documented order, with the acting signal named through v0.54.1's dominant-cause attribution (new `storage_pressure_admission_tests.rs`, MinIO + TC); a large backfill against a compaction-saturated store degrades backfill throughput while already-running views stay inside the freshness SLO; the loop converges rather than oscillates under both a sustained pressure step and an oscillating one, measured over a documented settling window; no new user-facing tuning knob is added, asserted by a configuration-surface lock test. | Unit, LFS, MinIO, TC |
| v0.58.3 | Unscoped Reachability, Dispatch-Wiring & Silent-Wrong-Answer Sweep ✅ Done | **Moved out of v0.59 by the 2026-08-12 realism review**: an audit whose fix list is unknown when it starts cannot live inside the gate that decides whether the release is ready. Every per-version `implement-version-orient` Pass C audit is deliberately scope-limited to that version's Scope, so no single version has ever re-audited the whole committed SQL surface. Run that same Pass C check **unscoped**, over every keyword, DDL statement, and `SHOW` command `docs/language-features.md` claims as implemented across v0.1–v0.58.2, closing the "code exists but is never wired to dispatch" gap class that shipped in v0.51.2. Re-run the v0.51.27 silent-wrong-answer sweep unscoped over the whole tree: every `unreachable!()`, `todo!()`, silently-wrong constant, and acknowledged-but-discarded branch on a reachable input-dependent path. **Fixing what the sweep finds is this version's work, not a follow-up**: each finding is either fixed, or the capability is retiered and the v0.57 contract updated to match, before this version signs off. | Every SQL/DDL/`SHOW` keyword documented as implemented in `docs/language-features.md` has a passing e2e pgwire reachability test (raw SQL through the real dispatcher, not a unit test importing private modules); the unscoped dispatch-wiring audit reports zero `MISSING` parser→dispatcher→executor→response paths anywhere in the documented surface; the unscoped silent-wrong-answer sweep reports zero reachable branches producing a wrong answer, a panic, or a silent no-op; and the audit itself is checked in as a repeatable script with a paired self-test, so v0.59 re-runs it as an entry criterion instead of re-deriving it by hand. | Unit, LFS, MinIO, TC |
| v0.59 | v1.0 Release Candidate (RC1) ✅ Done | **Rescoped by the 2026-08-12 realism review to a pure gate.** The unscoped reachability and silent-wrong-answer sweep, and every fix it produces, move to v0.58.3 and become an **entry criterion**: RC1 begins only when that script re-runs clean, so the release candidate contains no discovery work. Two scheduling rules the version left implicit are now stated: the 2-week continuous chaos cycle is **wall-clock bound and its clock restarts** on any P0/P1 fix or any other merged change, so a late fix moves the tag rather than shortening the soak. Activate all features from v0.1 through v0.58.3 simultaneously; run comprehensive chaos, performance, and scaling soak under maximum cluster pressure within a single cloud region. **The gate is the v0.57 contract, not feature completeness** ([ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §10 Stage 5): RC1 passes on correctness, recovery, bounded resources, operability, upgradeability, security, and performance stability of the core maintained-view workloads — not on how much SQL or how many connectors exist. **The unscoped reachability/dispatch-wiring sweep ships at v0.58.3, not here**, for the reason above; its charter was, and remains: every per-version `implement-version-orient` Pass C audit (`docs/language-features.md` vs. actual parser/dispatch/lowering code) is deliberately scope-limited to that version's Scope, so no single version ever re-audits the whole committed SQL surface — run that same Pass C check unscoped, over every keyword/DDL/`SHOW` command `docs/language-features.md` claims as implemented across v0.1–v0.58, closing exactly the "code exists but is never wired to dispatch" gap class that shipped in v0.51.2. Qualification evidence remains valid. Promotion to `v1.0.0` is unscheduled. | Entry criteria are met before the soak starts: v0.58.3's audit script re-runs clean, v0.58's matrix has zero uncovered cells, and every version through v0.58.3 is signed off. No P0 or P1 bug is discovered during a 2-week continuous automated chaos cycle whose clock restarted on the last merged change — a P0/P1 fix restarts the soak rather than shortening it. Each of the seven v0.59 qualification gates is signed off against a named artifact: no known silent-wrong-answer path (the v0.51.27 sweep re-run unscoped at v0.58.3), no lost or duplicated committed state under the v0.58/v0.58.1 failure matrix, bounded memory and state under v0.51.23/v0.51.24 accounting and spill plus v0.58.2's storage-pressure shedding, every degradation explainable through v0.54.1's enumerated reasons, a rehearsed v0.56 rolling upgrade and v0.56.1 restore drill, a closed v0.55.2 security review with zero open P0/P1 findings — including any engagement carried forward from that version — and a documented performance envelope for the core maintained-view workloads. | Unit, LFS, MinIO, TC |

**Post-v0.59 qualification correction (2026-08-18).** The v0.59 row above is
retained as the historical scope that was implemented and signed off, but its
RC1, final-release, and mandatory two-week-soak assertions are superseded. The
v0.59 result is reclassified as an engineering-complete technical preview: its
short checks remain useful implementation evidence, but they do not establish
an immutable release unit or the end-to-end behavior claimed by all seven v1
gates. Feature expansion remains frozen while the following qualification work
closes those gaps.

**Pre-v1 performance-architecture reordering (2026-08-19).** The scalability
review in [rockstream-pre-v1-scalability.md](rockstream-pre-v1-scalability.md)
found that the former v0.59.19 combined an external benchmark harness, five
engine redesigns, live scale-out work, and final release qualification in one
planning unit. Those concerns are now separated and reordered. v0.59.5–v0.59.9
establish RockStream's final physical IVM architecture immediately after the
CLI/configuration work: delta-native state, durable shared arrangements,
factorized and filtered maintenance, shared windows and skew-aware execution,
then an SLO-adaptive runtime and storage path. The former v0.59.5–v0.59.18
product, SQL, quality, lifecycle, deployment, and capacity work shifts to
v0.59.10–v0.59.23. v0.59.24 remains a pure, blocking qualification gate over
the finished product and may not introduce engine architecture.

The order is binding for two reasons. First, v0.59.5 was intended to establish
real, independently identified workers and capture two baselines: B0 as the
exact v0.59.4 artifact under the existing one-worker topology, and B1 as the
v0.59.5 S1 artifact with the new worker substrate at 1/2/4/8 workers. That
artifact capture did not occur, and the v0.59.5 implementation combined worker
registration and delta-native changes in one commit. R1 does not fabricate or
retroactively label a B1 artifact. Its developer-profile correction below
rebuilds B0 source for a one-worker ordinary-workload comparison and uses
paired current-candidate measurements for sharing, factorization, and local
worker scaling. R2 restores the full production-profile comparison before the
architecture freezes. Second, observability and capacity calibration must
describe the architecture that will ship: arrangement sharing, factorization,
delta persistence, commit grouping, and skew routing all change what should be
measured and how state is sized. The large architectural choices freeze after
v0.59.9; later work may expose, stabilize, and tune them, but any change that
invalidates their proof returns to the owning architecture gate before final
qualification.

The architecture gate controls merge and sign-off, not all parallel discovery.
No architecture-dependent later version may merge or sign off before v0.59.9.
Independent work on static error catalogs, documentation tooling, scenario
DSLs, SQL semantics contracts, and other surfaces that do not freeze runtime,
storage, metric, deployment, or capacity assumptions may proceed in parallel.
It must rebase and requalify against the v0.59.9 architecture before sign-off.

**Binding benchmark contract.** Production-capacity artifacts, R2, and
v0.59.24 record CPU model and core count, CPU affinity and NUMA placement,
memory and storage topology, placement of workers, Kafka, PostgreSQL, and
MinIO, and whether infrastructure services share worker hosts. They also record
warm-up duration, repetition count, variance and confidence interval, the
open-loop offered-load schedule, load-generator and sink-consumer headroom,
Kafka and MinIO utilization, input and checkpoint backlog slopes,
compaction-debt slope, completed checkpoint and compaction cycles, and cost per
million accepted changes. The production harness must prove that the load
generator, Kafka, MinIO, PostgreSQL, gateway client, oracle, and sink consumer
each independently sustain at least 1.25x the candidate's maximum accepted rate
at the same payload shape; otherwise the measured saturation point of that
component is an explicit upper bound on candidate capacity. Sustainable
throughput means the maximum offered rate at which the workload's p99 freshness
SLO holds, input and checkpoint backlog slopes are non-positive, memory and
queues remain bounded, and the declared minimum checkpoint and compaction cycle
counts complete. R1 uses the bounded developer-profile exception below and
makes no production-capacity claim.

To make the estimate auditable, v0.59.5 and v0.59.9 have mandatory internal
sign-off slices, and the split rule above applies to each one:

- **v0.59.5 S1:** real-worker topology, external harness, and honest baseline;
   **S2:** `OperatorEpochResult`/`StateMutation` plus aggregate and distinct;
   **S3:** join, window, Top-K, and key-packer conversion; **S4:** durable-format
   migration, checkpoint, recovery, and compaction.
- **v0.59.9 S1:** shard actors and stateless fusion; **S2:** byte/time credits,
   morsels, and physical commit grouping; **S3:** freshness control and
   compaction separation; **S4:** checkpoint changes. Unaligned checkpoints,
   localized arrangement rebuilds, and frontier-pinned serving replicas are an
   optional S5 admitted only by the measurements named in the v0.59.9 row.

**Scalability Value Review - R1 developer profile.** This remains a mandatory
proof prerequisite after v0.59.7 and before v0.59.8, but it answers an
architecture-value question rather than certifying production capacity. Run it
on the named `MBP-M5Pro-48GB-v1` profile: a 16-inch MacBook Pro with M5 Pro and
48 GB RAM, on AC power with Low Power Mode disabled. Record the exact CPU/core
layout, macOS and filesystem versions, build identity, available memory, Docker
Desktop version/allocation when used, and whether a platform control such as
CPU affinity, NUMA placement, or hardware counters is unsupported. Unsupported
platform controls are recorded rather than fabricated.

Use native LFS for scored storage measurements and public PGWire/process
surfaces for end-to-end checks. Kafka, PostgreSQL CDC, MinIO, eight-worker,
state-over-RAM, multi-host network, cost, and production headroom measurements
remain mandatory at R2/v0.59.24 but are not R1 prerequisites. Rebuild
`a4e4ad4` on the same profile as the one-worker ordinary-workload baseline; do
not invent B1. Run five alternating paired repetitions for timing/resource
rows, giving five samples per comparison side. Publish every raw sample and
require coefficient of variation no greater than 15%. For the five samples,
use sample CV `sqrt(sum((x_i - mean)^2) / 4) / abs(mean)`; five all-zero values
have CV zero and a zero mean with any nonzero value is invalid. Deterministic
structural rows use exact counters and complete output.

The local corpus is fixed before measurement: 1K/100K/10M live groups for the
one-key persistence proof; 100K source rows for one-versus-twenty arrangement
sharing; at least 10K changed rows and fan-out 100 for factorized
join-to-aggregate; 100K rows for ordinary aggregate/join regression; and 100K
live groups plus a fixed change stream for 1/2/4 real-worker scaling. Every
workload includes inserts, updates, deletes/retractions, and complete multiset
comparison against an independent oracle.

Publish `sign-offs/scalability-value-review-v0.59.7.md` plus machine-readable
local raw evidence with a `GREEN`, `YELLOW`, or `RED` decision. `GREEN` requires
every row below to pass. `YELLOW` means all correctness and structural rows pass
but exactly one timing/resource row needs one focused profiling cycle. `RED`
means any wrong result, full-state scan, missing evidence, idle declared worker,
measurement that remains unstable after one complete clean rerun, or more than
one failed timing/resource row. v0.59.8 may not begin until GREEN.

| R1 developer-profile property | GREEN gate |
|---|---|
| One-key persistence at 1K/100K/10M live groups | Same mutation count per operation, ≤10% logical-byte variation, zero full-state entries visited |
| Twenty shared consumers versus one at 100K source rows | ≤1.5× logical and LFS state; at least 80% less state than twenty private arrangements |
| Shared source-index maintenance | ≤1.5× key/trace work and ≤1.75× process CPU per accepted change |
| Factorized intermediate reduction at fan-out 100 | ≥10× fewer flattened intermediate tuples |
| Factorized throughput or resource improvement | ≥1.5× throughput or both CPU and encoded exchange bytes per accepted, query-visible change reduced by ≥33% |
| One-worker ordinary aggregate/join versus rebuilt B0 | Throughput and p99 freshness regress by ≤15% |
| Real-worker uniform aggregate at 1/2/4 workers | All workers own shards and do nonzero work; GREEN ≥2.0× at four workers; YELLOW ≥1.5× and <2.0×; RED <1.5× |

**Architecture Exit Review — R2.** This is a mandatory proof prerequisite
after v0.59.9 and before v0.59.10 or any architecture-dependent later sign-off.
Re-run the complete 1/2/4/8-worker aggregate, join, window, skew,
state-over-RAM, overload, checkpoint, compaction, migration, and worker-loss
matrix. Publish `sign-offs/architecture-exit-review-v0.59.9.md`. Every
architecture-related final qualification target must already pass in the
pre-RC environment; v0.59.24 remains exact-artifact qualification, not
architectural discovery. Failure reopens the owning architecture version and
invalidates affected downstream evidence.

R2 requires uniform aggregate throughput of at least 1.7× at two workers,
3.2× at four workers, and 5.6× at eight workers, plus at least 4× at eight
workers for the shuffle-heavy join. It also requires bounded queues and memory,
non-positive backlog slopes at sustainable load, automatic overload recovery,
state larger than RAM inside its freshness SLO, hot-key mitigation restoring at
least 80% of corresponding non-skew throughput, no global lock dominating
profiles, bounded checkpoint and compaction debt, and zero wrong results during
split and migration.

A version is complete only after every mandatory slice passes its own proof;
optional work cannot delay or substitute for a mandatory slice.

**Qualification identity contract.** Every release artifact carries three
distinct identities. `CandidateIdentity` contains the product semantic version,
candidate id, source SHA, binary/image digests, toolchain, lockfile digest, and
enabled features. RC artifacts use product semantic version `1.0.0` and a
separate candidate id such as `rc.1`; `rockstream --version`,
`rockstream_version()`, image labels, support bundles, compatibility checks, and
telemetry report both fields rather than embedding a prerelease semantic version
in the product version. `QualificationProfile` contains hardware, topology,
configuration, workload, fault policy, and thresholds. `QualificationRun`
contains the workflow/run id, timestamps, and raw evidence digests. A source,
dependency, toolchain, lockfile, enabled-feature, or artifact change creates the
next candidate/RC. A replacement host or a change to hardware, topology,
configuration, workload, fault policy, or thresholds creates a new profile
revision and run for the same candidate unless the candidate identity itself
changed. A failed host therefore does not manufacture a new release candidate;
the replacement profile and run remain comparable to the original candidate.
The final `v1.0.0` tag is a signed promotion alias to the same qualified
artifacts, with `candidate_id=rc.1` retained in the manifest and release
metadata.

The v0.59.23 threshold manifest records provenance for every absolute floor and
ceiling as one of: a design-partner requirement, a published reference-profile
objective, or a measured baseline plus a stated safety margin. A threshold may
not be selected merely because it is slightly below an already observed result.

**Active formal-model correction.** The continuous-verification requirement for
v0.23-v0.59.24 is all active base models: M1-M4 and M6-M7, plus every admitted
v0.59.x protocol variant. M5 was retired by v0.52.4 and is not a release
prerequisite.

**Active R1 proof-status correction.** The `Done` labels on v0.59.5-v0.59.7
mean their implementation slices and then-declared tests were completed. Their
scalability-value proof is reopened by the R1 review and they are not complete
under this roadmap's proof definition until local R1 is GREEN. Local R1
supersedes only their unavailable developer-profile evidence. Their full
production-profile performance obligations remain binding at R2/v0.59.24.

| Version | Focus | Scope | Proof | Backends |
|---|---|---|---|---|
| v0.59.1 | Evidence Integrity & Honest Release State ⚠️ Protection Pending | Reclassify v0.59 publicly as an engineering-complete technical preview and amend its sign-off to separate implementation, short-CI, security-review, artifact-publication, and final-authorization status. Establish one `CandidateIdentity` across the workspace package, `rockstream --version`, image labels, support bundles, build-info metrics, documentation, manifests, and candidate tags with `product_semver = 1.0.0` and `candidate_id = rc.N` (for example, `rc.1`), plus the commit SHA, build timestamp, compiler version, lockfile digest, and enabled features. Both identity fields appear in `--version`, SQL, telemetry, image labels, and evidence manifests. A final `v1.0.0` tag may be a signed promotion alias to an already-qualified candidate and exact artifact digests; it records that alias mapping without changing the candidate identity. Protect release history with required checks, no force pushes, signed release tags, and ownership review for release workflows, formal specifications, security policy, and capability contracts. Replace checked-in self-attestation with a machine-readable evidence manifest binding candidate SHA, artifact digests, workflow run, environment and workload digests, test pass/fail/skip counts, raw metrics, and regenerated summaries. | A release-identity conformance test fails on any package/binary/image/manifest mismatch and rejects a final promotion alias unless it names `product_semver = 1.0.0`, its candidate id, and the same source commit and qualified artifact digests. The RC validator's mutation suite rejects a changed SHA or artifact digest, any skipped mandatory prerequisite, missing raw data, and a summary that cannot be regenerated from its raw artifact; checked-in baselines define targets only and cannot satisfy a measured-result field. Branch and tag policy is verified before release authorization; as of 2026-08-19, the live GitHub API reports `main` unprotected and no equivalent repository ruleset, so this row and the scope-freeze baseline remain pending until policy is enabled and rechecked. | Unit, LFS, MinIO, TC |
| v0.59.2 | True Automated End-to-End Release Qualification ✅ Done | Replace nominal high-level proofs with one bounded, repeatable qualification suite running separate control nodes, workers, gateway, Kafka, MinIO, fault injection, an independent workload generator, and an independent correctness auditor. Drive schema, sources, views, and the Kafka sink through public pgwire/CLI surfaces; ingest inserts, updates, deletes, out-of-order events, skewed keys, and high-cardinality state through real Kafka and PostgreSQL CDC paths; compare full result multisets, committed frontiers, and independently consumed sink output with an external batch oracle. Observe actual heartbeat loss, shard ownership, fencing epoch, selected checkpoint, source offset/LSN, view frontier, sink transaction, and first correct post-recovery query. Include two-digest N→N+1 rolling upgrade with mixed versions under load, fail-closed incompatible-format behavior, fresh-cluster disaster restore from an independent export, corrupt-export rejection, and declared-hardware performance/resource measurements. Keep pull-request shards short and deterministic and run the complete bounded suite for a candidate; missing Docker, images, credentials, backends, or test selection is a failure, never a successful return. | One command produces a SHA- and digest-bound evidence manifest with zero failed and zero skipped mandatory scenarios. Fault mutations prove each recovery observation and SLO can fail independently; the external oracle catches loss, duplicates, stale frontiers, and wrong sink output; N and N+1 are distinct immutable images; all original cluster-local state is destroyed before restore; measured throughput, latency, RSS, descriptors, sockets, queue depth, state size, and object-store request counts are regenerated from raw output. Extended soaks may rerun this harness when resources permit, but are optional and cannot substitute for this suite. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.3 | Security Provenance, Reproducible Releases & Contract Reconciliation ✅ Done | Complete the assessor-issued security report or attestation and publish its identity, scope, dates, exclusions, report digest, and verified zero-open-P0/P1 status; until that exists, describe the current record as an internal security readiness review. Build reproducible Linux x86-64 and ARM64 binaries plus a multi-architecture OCI image with checksums, SBOM, vulnerability results, signed provenance, signed tag/image, release notes, configuration reference, known limitations, SQL support matrix, N/N+1 matrix, restore runbook, and artifact-to-source reproduction instructions. Treat `capabilities.toml` as the source of truth for generated README, SQL, tier, connector, compatibility, and deprecation tables. Rebase dependency upgrades without lowering coverage or performance gates, resolve the full feature matrix, and use genuinely different old/new outputs in v0.59.2's upgrade test. | Two clean builds reproduce the release artifacts or document and verify every allowed source of nondeterminism; every published digest appears in the evidence manifest and verifies against the downloadable artifact; signature, SBOM, provenance, and vulnerability-policy checks pass; the assessor artifact is verifiable; generated contract documents have zero diff; the complete v0.59.2 suite passes against the final signed candidate family. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.4 | CLI & Configuration Usability ✅ Done | Implement `rockstream demo` with a deterministic embedded orders scenario (`UX-01`); implement `rockstream doctor` bounded diagnostic command with redaction and deadlines (`UX-02`); add semantic validation and unknown-key reporting to `rockstream config validate` (`CFG-01`); add shared configuration resolver and source-origin reporting to `rockstream config print-effective` (`CFG-02`); standardize `--output json` and streaming behavior across all finite CLI commands (`CLI-01`); add shell completions for Bash, Zsh, and Fish (`CLI-02`). | `rockstream demo` runs deterministic orders scenario end-to-end through pgwire and asserts incremental maintenance; `rockstream doctor` runs bounded non-destructive diagnostic checks within deadline and redacts secrets; `config validate` reports syntax, unknown keys, deprecated keys, and semantic diagnostics; `config print-effective` resolves identical config as `start` with origin tracking; CLI JSON output matches schema across all finite commands; completions generate for bash/zsh/fish. | Unit, LFS |
| v0.59.5 | Delta-Native State Foundation ✅ Done | First make `rockstream start --role worker` a long-lived service whose stable, unique `WorkerId` is assigned or presented explicitly, with control observing independent heartbeats and shard ownership. Establish the external benchmark/oracle harness before optimizing and capture a reproducible v0.59.5 S1 pre-optimization baseline: the new worker-process plumbing runs unchanged v0.59.4 engine behavior for uniform and high-cardinality aggregation plus joins at 1, 2, 4, and 8 real workers, recording the exact source and artifact identities, throughput, p50/p95/p99 freshness, CPU profiles, lock time, RSS, network bytes, SlateDB bytes, object-store requests, and logical versus physical write amplification. Introduce `OperatorEpochResult`, `EpochStateDelta`, and explicit upsert/tombstone `StateMutation`s; incrementally track dirty keys and logical state bytes in every stateful operator; persist only changed aggregate, distinct, join, window, Top-K, and arrangement entries; encode safe associative laws as SlateDB merge operands with law/version tags; retain ordered or replacement updates where no safe merge law exists; and use checked arithmetic for weights and aggregate state. Ordinary commit, checkpoint contribution, and recovery bookkeeping must consume the same delta representation rather than reconstructing one by scanning live state. Version every state value and checkpoint manifest; define deterministic migration from a v0.59.4 checkpoint/state directory, the mixed N/N+1 read/write policy, fail-closed behavior for unsupported formats, and the documented rollback boundary without relying on identical SQL recompiling to identical operator IDs. | The harness accepts a declared worker count only when every worker has a distinct PID/process or cgroup identity and `WorkerId`, owns real shards, processes real input, and reports nonzero work; missing real multi-worker capability is `UNAVAILABLE`, never simulated by handles or repeated logical labels. The harness imports no RockStream crate, independently computes complete expected multisets, and publishes raw inputs, profiles, environment identity, and results under the binding benchmark contract. Against arrangements containing 1,000, 100,000, and 10,000,000 groups, the same one-key insert/update/delete produces approximately constant state mutations and logical write bytes; no stateful operator walks its full arrangement on an ordinary commit; crash/recovery, retractions, negative weights, overflow boundaries, and compaction remain oracle-identical on LFS and MinIO. Upgrade from v0.59.4 state, restore an old export into the new binary, mixed-version operation, rollback at the documented boundary, and crashes at every migration boundary preserve data or fail closed. Replaying the S1 baseline quantifies the delta-native gain without replacing unavailable, failed, or regressed measurements with estimates. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.6 | Durable Shared Arrangement Fabric ✅ Done | Define a canonical `ArrangementSpec` containing `tenant_id`, `security_policy_digest`, normalized source identity and source-schema generation, key expressions and types, value projection, predicate, null semantics, decimal scale, collation identifier and version, time-zone/time-domain semantics, merge-law identifier and version, and partitioning, then hash it into a stable `ArrangementId`. Add a durable arrangement catalog, immutable consolidated trace batches, per-consumer read frontiers, a compaction frontier derived from the slowest live consumer, reference-counted lifecycle and reclamation, and frontier-safe new-view installation that pins frontier `F`, snapshots the shared trace through `F`, buffers later deltas, then attaches without rescanning the source. Move decoded-block, index, and object caches into a worker-wide storage context so shards and views reuse bytes under explicit memory budgets; expose arrangement identity, consumer count, shared bytes, bytes saved, and compaction frontier through `EXPLAIN INCREMENTAL`. Version every trace manifest and define mixed N/N+1 read/write, fail-closed incompatibility, migration, restore, crash-boundary, and rollback behavior for conversion from private v0.59.5 state to the shared trace. | Twenty semantically equivalent consumers with harmless syntax/cast differences inside the same isolation boundary canonicalize to one physical arrangement and produce oracle-identical output; adding the twenty-first view attaches at a declared frontier without a source rescan or visibility gap. Equivalent SQL shares, while a different tenant, security policy, predicate, collation version, decimal scale, source-schema generation, merge-law version, null semantics, or time-domain policy never shares. Cross-tenant and row-security data are never visible through a shared catalog, trace, or cache. Restart and consumer lag preserve frontier and compaction safety; dropping the last consumer eventually reclaims catalog, trace, cache, and storage state. Upgrade, mixed-version operation, old-export restore, rollback, and crashes at every trace-migration boundary preserve data or fail closed. Measured memory and object-store reads grow with unique physical arrangements rather than view or shard count, remain within declared budgets, and improve against the exact v0.59.5 workload and artifact format. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.7 | Factorized & Filtered IVM ✅ Done | Build canonical key capsules once per row and reuse the same typed bytes and stable hash for arrangement lookup, partitioning, shuffle, skew detection, and persistence. Add factorized PK/FK join-to-aggregate maintenance and bounded star-join payload trees so downstream algebraic aggregates update compact payloads instead of materializing high-fan-out joined rows. Add a Delta Amplification Governor that measures input deltas, probes, shuffled bytes, intermediate tuples, output deltas, and state writes per operator and selects a documented fallback before amplification breaches its budget. Transfer selective predicates across joins as frontier-versioned filters with explicit insertion, retraction, NULL, and outer-join safety rules. Choose the classic or factorized plan at compile/deploy time. The benchmark harness may build alternatives from shared arrangements and compare them against the external oracle in shadow mode, but v1 does not add a production live-cutover protocol. | High-fan-out join-to-aggregate workloads do not emit the full joined intermediate and remain oracle-identical under inserts, updates, deletes, retractions, NULLs, and recovery; selective joins materially reduce measured probes and shuffle bytes without false negatives; every amplification-budget breach produces a bounded fallback or coded refusal rather than unbounded work. Classic and factorized plans are compared on identical input, shadow evidence demonstrates that the compile/deploy-time choice follows the declared selection rule, and the repeated baseline publishes join amplification, filter selectivity, CPU, memory, and freshness deltas from v0.59.6. Live adaptive plan replacement remains deferred unless separately admitted with resource-headroom, durable switch-generation, crash-recovery, rollback, reader-retention, formal-model, and freshness-SLO proofs. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.8 | Shared Windows & Skew-Aware Execution ✅ Done | Represent overlapping windows as shared time slices keyed by source, partitioning, time column, slice width, predicate, and merge law; choose law-specific rolling structures instead of rebuilding every correlated window independently. Add heavy/light execution for aggregates and joins, distributed hot-key bucket combining for mergeable laws, deterministic power-of-two routing, and factorized handling where a heavy join would otherwise enumerate a cross product. Replace stop-the-world hot-key moves with frontier-scheduled micro-migration: copy bounded chunks, dual-route by migration epoch, cut over at a committed frontier, and reclaim only after all consumers pass it. Carry decayed key heat in shard leases so reassignment does not forget the condition that caused it. | Correlated windows of different widths reuse physical slices and remain bit-identical to independently maintained windows across lateness, expiration, retractions, checkpoint, and recovery. Under a sustained 50× hot key, p99 freshness stays within 2× the uniform baseline while queues and per-worker state remain bounded; migration has a declared and enforced p99 latency-spike bound, and kill/restart at every copy/dual-route/cutover boundary produces no loss, duplicates, or dual ownership. Replaying the common benchmark reports hot-key bucket count, state-sharing savings, network balance, and tail-latency change from v0.59.7. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.9 | SLO-Adaptive Runtime & Storage ✅ Done | Run the ordinary hot path on shard-owned actors and bounded mailboxes inside the long-lived worker substrate established by v0.59.5; fuse adjacent stateless stages; size morsels and exchange credits from measured milliseconds and bytes of queued work rather than fixed batch counts. Separate logical visibility epochs from adaptive physical commit groups while preserving frontier and durability semantics. Reuse the worker-wide shared cache under explicit budgets, isolate compaction workers from foreground compute, build changelog checkpoints from the delta-native mutation stream, and reserve barrier capacity under data saturation. Add a non-oscillating freshness controller over source lag, compute time, queue age, memory, checkpoint cost, compaction debt, and object-store latency; let it adjust commit grouping, morsels, credits, checkpoint mode, and admission within documented bounds. Before optional S5 work is admitted, measure whether the reserved barrier lane still breaches its alignment bound, ordinary rebuild still breaches its recovery bound, or the ordinary query path still breaches its p99 bound at declared concurrency. Only the corresponding measured breach may admit, respectively, unaligned checkpoints, localized arrangement rebuild, or frontier-pinned serving replicas. | Profiles show no global or operator lock dominating the reference workload and attribute CPU to useful operator/storage work; the controller converges inside a declared settling window under step, burst, and oscillating load without violating correctness or creating an unbounded queue. Checkpoint alignment, changelog growth, backpressure, overload recovery, compaction pressure, worker loss, and state larger than memory stay within published bounds. For each optional S5 mechanism, evidence either records that the prerequisite bound did not breach and the mechanism remains absent, or demonstrates the breach and proves the admitted mechanism under correctness, crash, recovery, and resource bounds; any admitted replica returns only globally committed results. The complete v0.59.5 benchmark matrix is republished with raw before/after evidence, and the physical architecture freezes here: any later change that invalidates these results reopens this gate. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.10 | Runtime Introspection & Operator Clarity ✅ Done | Add `SHOW ROCKSTREAM CAPABILITIES` backed by the embedded runtime capability registry (`OBS-01`) and `SELECT rockstream_version()` exposing `product_version`, `candidate_id`, `source_sha`, and `artifact_digest` from `CandidateIdentity` over pgwire (`OBS-02`). Enrich `SHOW VIEW STATUS` and `EXPLAIN INCREMENTAL` with stable raw facts from the final engine: `arrangement_id`, `arrangement_consumer_count`, `shared_state_bytes`, `bytes_saved_by_sharing`, `delta_amplification`, `join_amplification`, `merge_operand_count`, `dirty_key_count`, `logical_write_bytes`, `physical_write_amplification`, `hot_key_bucket_count`, `factorization_strategy`, `predicate_filter_selectivity`, `cache_hit_rate`, `epoch_group_size`, checkpoint mode, compaction debt, `degradation_reason`, `reason_code`, `dominant_contributor`, source/compute lag, spill bytes, checkpoint id, frontier, and `recommended_action_key` (`OBS-03`). Add bounded read-only `rockstream_catalog` tables for `nodes`, `sources`, `views`, `operators`, `arrangements`, `checkpoints`, and `capabilities` (`CAT-01`). | Capability SQL and catalog output agree with `capabilities.toml`; `rockstream_version()` exposes all four candidate identity fields over pgwire and matches the immutable candidate manifest; arrangement/operator facts reconcile with the immutable v0.59.5–v0.59.9 benchmark artifacts and with independently counted consumers, mutations, and bytes; equivalent shared views report one arrangement and the same frontier; every catalog has an enforced cardinality and scan bound, secrets are redacted, and stable identifiers survive restart. No handcrafted human advice is introduced here because v0.59.16 owns interpretation and rendering. | Unit, LFS, MinIO, TC |
| v0.59.11 | SQL Ergonomics & Common Expression Completeness ✅ Done | Harden `UPDATE ... RETURNING` across simple/extended query protocols and prepared statements (`SQL-01`); harden `DELETE ... RETURNING` across simple/extended query protocols and prepared statements (`SQL-02`); add consistent `IF EXISTS` and `IF NOT EXISTS` handling across all admitted DDL object families (`SQL-03`); add typed, null-preserving common scalar functions across string, null-handling, and date/time categories (`SQL-04`). | Full `UPDATE ... RETURNING` and `DELETE ... RETURNING` conformance matrix passes across simple/extended query protocols and prepared statements; `IF EXISTS` / `IF NOT EXISTS` handle present and missing objects consistently without error; scalar function matrix passes with null preservation, and incremental function evaluation matches batch oracle. | Unit, LFS, TC |
| v0.59.12 | Error Catalog Foundation & Dispatch Conformance ✅ Done | Create the static `RS-XXXX` catalog and generator for Rust constants and reference documentation (`DOC-01`). Each `ErrorDescriptor` owns the stable code, key, title, severity, SQLSTATE, retry class, default next steps, and documentation anchor; add conformance checks and drift gates, then sweep all new SQL, SHOW, and catalog dispatch paths. | Catalog metadata, generated Rust constants, and generated error documentation agree with zero drift; every public code resolves to one descriptor with validated SQLSTATE and next-step text; dispatch-wiring checks confirm zero missing paths across all new SQL, SHOW, and catalog surfaces. | Unit |
| v0.59.13 | Single-Source Product Surface ✅ Done | Add `rockstream-docgen`, normalized `ProductSurfaceManifest` (`DOC-001`), CLI/config/function/catalog/metric/error contributors, and the SQL contract contributor that consumes `contracts/sql-type-matrix.toml`; produce deterministic generated JSON and drift gates (`DOC-004` foundation). | Manifest is deterministic; all public IDs and every matrix cell resolve; generated output is clean; mutations to any public registry or contract source fail the drift gate. | Unit |
| v0.59.14 | Golden Path Complete ✅ Done | Add `rockstream init` (`GP-001`–`GP-006`), local/Kafka/PostgreSQL-CDC templates, maintained Compose profiles, verifier and cleanup services, canonical examples, and a reference application. Keep `rockstream demo` ephemeral and zero-configuration, while `rockstream init` creates a persistent project users continue developing and operating. | Every template runs from a clean directory, produces exact maintained-view results, fails clearly under common setup errors, cleans up, and runs again; the demo and scaffold have distinct documented contracts. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.15 | Current, Simple, Executable Documentation ✅ Done | Reorganize docs by user persona (`DOC-002`), shorten README, separate history, generate references (`DOC-004`), add executable snippets/transcripts (`DOC-003`), publish schema-evolution cookbooks for compatible and incompatible changes, and validate links, terminology, and contributor workflows. Add the standard test command taxonomy, contributor guides for SQL/operators/errors/catalogs/configuration/scenarios, ADRs for binding choices, archived historical plans, and dependency/compile-time hygiene checks. | README and getting-started paths execute against the v0.59.14 golden path; generated references match the manifest; old public links resolve; schema cookbooks run; no current doc claims unsupported behavior; contributor and maintainer checks are executable. | Unit, LFS, TC |
| v0.59.16 | Structured Diagnostics Everywhere ✅ Done | Consume v0.59.12's static catalog and create runtime `DiagnosticOccurrence` values containing a descriptor reference, correlation ID, message, safe context, optional retry-after, and causal occurrence; add redaction, renderers, lookup surfaces, and migrate every public error path. Move `rockstream support diagnose` here as a diagnostic consumer that emits a bounded redacted support bundle. | No manual user-facing error strings; pgwire/CLI/JSON/log/support outputs agree; every occurrence resolves to the v0.59.12 catalog; correlation and causal data are preserved; redaction mutation tests pass; induced stale-view, lag, and dependency failures produce matching diagnoses. | Unit, LFS, TC |
| v0.59.17 | Public-Path Scenario & Differential Framework ✅ Done | Add typed scenario DSL (`TST-001`–`TST-006`), process/Docker drivers, typed transcripts, independent oracles, capability proof levels, differential and metamorphic suites, and reproducibility artifacts. | Every Core capability meets minimum proof level; differential corpus passes; injected mismatches produce minimized reproducible artifacts. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.18 | Lifecycle, Client, Documentation & Backend Test Closure ✅ Done | Close the existing lifecycle race/restart/recovery, external-client, real-backend, documentation, golden-path, and high-level test program (`TST-007`–`TST-010`). Exercise real Kafka, PostgreSQL, and MinIO paths, run current docs and golden-path scenarios, complete the Core behavior ledger across required proof levels and backends, and enforce zero-hidden-skip behavior. | Lifecycle, client, and backend workflows pass through public surfaces; all golden paths and current docs run; the Core behavior ledger is complete; required tests fail when prerequisites are missing, skipped, or retried instead of reporting green; the public surfaces agree on the observed result. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.19 | SQL Semantics, PostgreSQL Compatibility & System Limits ✅ Done | Define and publish the exact v1 SQL contract before type-completeness implementation: numeric precision/scale, overflow and casts, NULL propagation and three-valued logic, duplicate rows, unmatched DML, transaction visibility, timestamp precision/time zones/DST, identifier folding, deterministic collation and string ordering, text and temporal key behavior, prepared-statement arrays and `ANY($1)`, and PostgreSQL compatibility boundaries. Select the versioned `rockstream_binary_v1` collation, whose byte ordering is explicitly defined and independent of host libc and locale; define the admitted decimal bounds and temporal time-zone policy. Generate `docs/sql-semantics.md` and `docs/sql-type-matrix.md` from the authoritative `contracts/sql-type-matrix.toml` contributor, and generate `docs/limits.md` from authoritative limit metadata. Pin PostgreSQL 18.0 as the differential reference using `postgres:18.0@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c` in that contract source, with architecture-specific digests recorded for each tested platform. | Every admitted semantic has executable conformance coverage against the batch oracle and the pinned PostgreSQL reference; every published limit comes from runtime/configuration metadata or an explicitly measured bound; exceeding a bound fails with its documented `RS-XXXX` code; generated documents, manifest contribution, and the checked-in type matrix are drift-checked; unsupported locale-sensitive collations, unadmitted temporal keys, and floating-point equality joins fail clearly rather than producing an unverified result. | Unit, LFS, TC |
| v0.59.20 | Common SQL & Type Completeness ✅ Done | Implement and prove exactly the operation/type cells admitted by the v0.59.19 matrix: integers, Boolean, text, UUID, date, timestamp, timestamptz, bounded decimal/numeric, and common array parameter forms across expressions, filters, joins, grouping, windows, DML, prepared statements, and incremental maintenance. Cover numeric/decimal arithmetic, overflow and casts, NULL-preserving expressions and three-valued logic, text-key aggregates and joins, temporal keys, array parameters and `ANY($1)`, and deterministic collation/order behavior. Floating-point equality joins remain unsupported unless separately admitted with a new proof obligation. | The checked-in matrix has a passing test for every `required` cell and a tested rejection for every `unsupported` cell; numeric behavior matches the v0.59.19 contract; arrays round-trip through simple and extended protocols; text and temporal keys produce oracle-identical results; unsupported cells return their documented error and never silently fall through to an unverified implementation. | Unit, LFS, TC |
| v0.59.21 | Graceful Lifecycle, Shutdown & Health Contract ✅ Done | Define SIGTERM/SIGINT, crash, eviction, drain, and role-shutdown behavior; stop new work, finish or abort epochs safely, flush durable state, release leases, close clients, enforce a configurable deadline, and expose structured `/live`, `/ready`, and `/health` endpoints with startup, draining, dependency, and degraded-state semantics. | Binary-level lifecycle tests start a workload, terminate gateway/control/worker roles, and verify committed rows survive, leases move, subscriptions close cleanly, and the old process exits within its deadline; readiness transitions are asserted during startup, dependency loss, and drain, while health responses include role, version, dependencies, and actionable reasons. | Unit, LFS, MinIO, TC |
| v0.59.22 | Supported Platforms & Deployment Profiles ✅ Done | Publish supported CPU architectures, Linux distributions and libc expectations, OCI image and `docker run`/Compose profiles, non-root container behavior, ports, storage and object-store requirements, resource recommendations, upgrade-safe configuration, and reference systemd and Kubernetes deployment profiles with a minimal Helm chart. Classify environments as `Supported`, `Compatible, unverified`, or `Unsupported`; `rockstream doctor` warns for the middle category and startup rejects only known unsafe or incompatible combinations. | Clean-machine runs pass for each supported profile; x86-64 and ARM64 binaries and multi-architecture images start the same smoke workload; container, systemd, and Kubernetes profiles run as non-root with documented persistence and networking; the matrix distinguishes tested versions from protocol-compatible but unverified S3-compatible, PostgreSQL, and Kafka versions, and rejects only known incompatibilities. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.23 | Capacity Planning & Estimator Calibration ✅ Done | Add measured small/medium/large sizing profiles over the frozen engine and explain the drivers of unique physical arrangement state, sharing fan-out and saved bytes, factorized payloads, delta and join amplification, window slices, hot-key buckets, cache, spill, commit grouping, checkpoints, compaction, network shuffle, and object-store cost. Make `EXPLAIN INCREMENTAL ESTIMATE` reason from canonical arrangements and selected physical strategies: twenty views sharing three arrangements are estimated as three maintained arrangements plus twenty consumers, never twenty independent copies, and factorized joins estimate compact payload state rather than a flat intermediate that is not materialized. Calibrate against reproducible uniform, high-cardinality, join, window, skew, and state-over-RAM workloads and publish observed error ranges tied to values users can inspect before deployment. Before the first v0.59.24 RC is built, freeze per-reference-profile floors for minimum sustainable rows per second and updates per second per core, plus ceilings for CPU-seconds, object-store requests, and cost per million accepted updates and p99 query latency at declared concurrency. | Every profile compares estimated and measured private/shared state bytes, RSS, spill, cache hit rate, epoch and commit-group duration, p99 freshness, shuffle bytes, logical/physical writes, object-store requests, checkpoint cost, and compaction debt. Estimates change in the correct direction when sharing, factorization, selectivity, skew, or freshness targets change; raw measurements regenerate all sizing guidance and error ranges; deliberately restoring a per-view arrangement multiplier or flat-join cardinality assumption fails calibration before v0.59.24 begins. The absolute floors, ceilings, hardware profile, concurrency, and workload digests are justified by raw measurements, signed into the threshold manifest, and immutable once the RC is built. | Unit, LFS, MinIO, Postgres, Kafka, TC |
| v0.59.24 | Final Horizontal Scale & Performance Qualification ✅ Done | **Blocking v0.59 qualification gate; this version changes no engine architecture.** At the start of this version, create `v1.0.0-rc.1` from one protected source SHA and publish the exact signed binary and image artifacts to be qualified. Freeze their digests, toolchain, lockfile, deployment topology, configuration, workload corpus, fault policy, hardware identity, and v0.59.23 thresholds before measurement. Run Kafka/PostgreSQL CDC → real RockStream workers → incremental operators → SlateDB/MinIO → PGWire/Kafka sink on fixed reference hardware at 1, 2, 4, and 8 workers. Exercise uniform and high-cardinality aggregation, factorized and unfactorized shuffle-heavy joins, correlated shared windows, Zipf skew with a sustained 50× hot key, state substantially larger than worker RAM, 120% offered-load recovery, worker loss, online split and micro-migration, checkpoint alignment, and compaction pressure. Requalify the v0.47 elasticity claims and every v0.59.5–v0.59.9 architecture gate through the final v0.59.10 observability and v0.59.23 capacity surfaces. Profile operator CPU, useful work, lock wait, Arrow encode/decode, key construction, SlateDB and object-store I/O, shuffle, checkpoint, compaction, cache, and gateway work. A candidate-identity change creates the next RC number. A qualification-input change creates a new `QualificationProfile` revision and `QualificationRun` for the same candidate. Any threshold change invalidates prior qualification evidence and requires a complete rerun, but does not create a new RC unless the candidate artifacts also changed. A failure that requires redesign returns to the owning architecture version and invalidates affected downstream evidence; thresholds are never relaxed inside the gate to make a candidate pass. | An external benchmark/oracle harness that imports no RockStream crate timestamps generated changes, independently computes complete expected materialized-view multisets, queries PGWire, and consumes Kafka sink output. Maximum sustainable throughput follows the binding benchmark contract and must hold for 30 minutes with p99 freshness ≤1 second, bounded memory, queues, checkpoint backlog, and compaction debt, the declared checkpoint and compaction cycles complete, and no wrong results, lost changes, duplicate sink outputs, failed or rejected committed writes, or OOMs. Proof requires every absolute floor and ceiling frozen by v0.59.23 **and** ≥1.7×, ≥3.2×, and ≥5.6× single-worker sustainable throughput at 2, 4, and 8 workers for partitionable uniform aggregation; ≥4× at 8 workers for the shuffle-heavy join; no worker above 1.5× median sustained CPU under uniform load; hot-key mitigation recovering ≥80% of corresponding non-skew throughput without manual intervention; online split or migration with zero incorrect results and ≤20% temporary throughput loss; and automatic freshness recovery after five minutes at 120% offered load. One-row changes against 1K, 100K, and 10M-state arrangements remain approximately constant in state mutations and SlateDB writes; state larger than RAM remains inside its declared freshness SLO. Raw measurements record throughput, scale factor, p50/p95/p99 freshness, query latency, CPU/core, RSS, shared and private state bytes, spill, cache hit rate, network bytes, object-store requests, checkpoint duration, logical and physical write amplification, queue depth, compaction debt, and per-worker load. Harness mutations must fail when two declared workers are one process, WorkerIds repeat, one worker performs all useful work, a declared worker owns no shard, the generator saturates, the sink consumer falls behind, a result is stale but otherwise correct, output is duplicated or lost, raw timestamps are replaced by constants, a required workload is skipped, or CPU affinity or hardware identity changes mid-run. Inputs, environment, candidate digests, raw observations, and regenerated summaries are immutable evidence; every external bottleneck is demonstrated and quantified; constants or manually supplied samples cannot satisfy a result; future candidates regress by no more than 10% sustainable throughput or p99 freshness without explicit baseline approval. | Unit, LFS, MinIO, Postgres, Kafka, multi-process TC/Kubernetes, fixed multi-host performance environment |

v0.59.24 qualifies the exact signed RC artifact digests created at its start.
A candidate-identity change creates the next RC number. A qualification-input
change creates a new `QualificationProfile` revision and `QualificationRun` for
the same candidate. Any threshold change invalidates prior qualification
evidence and requires a complete rerun, but does not create a new RC unless the
candidate artifacts also changed. If
every automated gate passes with zero open P0/P1 defects, authorize and sign
`v1.0.0` against the same source commit and artifact digests; no release artifact
is rebuilt after qualification. A multi-hour or multi-day run of the same
harness is welcome supplemental evidence when capacity permits, but is not a
release prerequisite and does not replace the mandatory suite.

#### Deferred by decision (not scheduled)

The 2026-08-11 rebaseline removed the following from the version table. They are
recorded here rather than deleted, with the evidence that would readmit them
under the [ROCKSTREAM_PROJECT_FOCUS.md](ROCKSTREAM_PROJECT_FOCUS.md) §8
admission rule. "Deferred" means no roadmap slot and no v1 commitment; it does
not mean rejected forever, and it does not affect anything already shipped.

| Deferred item | Was | Readmission evidence required |
|---|---|---|
| Live adaptive replacement between classic and factorized plans | Removed from v0.59.7 before implementation | A measured production workload where compile/deploy-time selection is insufficient, plus resource-headroom admission, a durable switch generation, crash recovery in every phase, rollback, retention of the old plan until all readers pass the switch, a dedicated formal model, and proof that running two plans does not violate the freshness SLO. Shadow comparison in the benchmark harness does not itself admit production cutover. |
| `CREATE EXPECTATION`, the expectation operator, `warn`/`degrade`/`block` state-degradation policies, lineage diagnostics | v0.52 (Inline Expectations & Lineage Diagnostics) | A design partner requiring declarative data-quality policy *in the engine* rather than upstream of it. The operational half — durable, bounded, replayable quarantine — ships at v0.52 regardless. |
| Additional connector families beyond PostgreSQL CDC and Kafka | never scheduled; recurring pressure | **Tightened 2026-08-12 (Phase 16.6).** The `Maintain`-tier object-store and webhook paths no longer exist as a fallback, so the bar is now the proposal's six-point Extensibility Policy, machine-enforced by `scripts/check-connector-admission.sh` at v0.52.5: the workload must materially improve core IVM, must be unable to reach RockStream through Kafka or PostgreSQL as the integration boundary, must have demonstrated production demand, must have failure/recovery semantics meeting RockStream's correctness standard, must carry acceptable maintenance burden, and must be worth permanently widening the compatibility contract. Default answer: build it outside RockStream. |
| S3 source, HTTP/webhook source, object-store sink, Iceberg sink, Delta Lake sink, cold-tier sink infrastructure, cold-tier GC, external lakehouse catalog registration (Glue/Hive/REST/DuckLake) | shipped at v0.27–v0.29, v0.44, v0.51.16; classified `Maintain` by the 2026-08-11 rebaseline | **Removed, not deferred** — deleted at v0.52.3–v0.52.5 per [ROCKSTREAM_CONNEXTORS_CLEANUP.md](ROCKSTREAM_CONNEXTORS_CLEANUP.md). Readmission is governed by the connector-admission row above, not by the fact that the code once existed. Replacement paths are documented in `docs/connector-migration.md`: external loader via pgwire/Kafka for file ingestion, an external HTTP→Kafka adapter for webhooks, and RockStream → Kafka → a downstream format-owning writer for lakehouse output. |
| Iceberg REST catalog server (§13.7), DuckLake catalog server (§13.8), and any further lakehouse-management responsibility | already out of scope in `NEW_IMPLEMENTATION_PLAN.md`; reaffirmed here | Unscheduled. Phase 16.6 removed the cold-tier sink that could have justified a catalog server. RockStream → Kafka → a dedicated lakehouse writer is the supported path. |
| `gcp_kms` and `vault` KEK backends for `CREATE SECRET` envelope encryption | v0.55 (four backends: `env`/`aws_kms`/`gcp_kms`/`vault`), narrowed to two by the 2026-08-12 realism review | A deployment that requires one of them. Both remain cheap to add behind v0.55.1's `KekProvider` trait; what is deferred is release-gating a backend that has no credentials and no live integration target to test against, which would make the contract's `Core` tier mean less than it says. |

---

#### Unscheduled candidates (accepted direction, no assigned version)

Distinct from the table above: these are not deferred pending evidence — the
direction is accepted — but they are not v1-gated and hold no roadmap slot
before `v1.0.0`. They are recorded so the versions that *do* ship before v1
avoid foreclosing them.

**Durable `SUBSCRIBE` history** (2026-08-11 future-roadmap review, item 3).
Today `ViewChangeLog` (`crates/rockstream-gateway/src/change_log.rs`) is a
bounded in-memory `VecDeque` of `CHANGE_LOG_MAX_ENTRIES` (10,000) entries per
view, with cursors valid only within the current session; falling outside the
retained window already returns `RS-2006`. The candidate moves that history
into durable object-storage-backed state: a per-view changelog written
atomically with each committed view update, stable cursors keyed by view
identity + epoch + sequence number, snapshot-plus-live subscription, resume
from a persisted cursor across reconnects, and retention bounded by time
and/or bytes with `RS-2006` retained as the too-old-cursor failure.

Three things make this a genuine strategic candidate rather than a feature
request. First, its delivery contract is honest and falsifiable — **gap-free,
ordered, resumable delivery within retention**, not a claim of universal
end-to-end exactly-once. Second, a strong durable subscription protocol is the
generic egress mechanism that *reduces* pressure to add destination-specific
sink connectors, which turns the connector guardrail above from a refusal into
a plan. Third, it consumes the **v0.52.1 snapshot/delta fence** rather than
reinventing it: snapshot-plus-live for a subscriber and snapshot-plus-live for
a backfill are the same problem, which is exactly why v0.52.1 ships the fence
as a reusable primitive.

Two constraints to record before anyone starts: cross-shard fanout needs a
global ordering key, and `(epoch, seq)` is the only one available — which means
subscription latency is floor-bounded by epoch commit, and that must be stated
as part of the contract rather than discovered later. And retention,
compaction, and cursor validity make this a storage subsystem, not a gateway
change; it earns its own version when it is scheduled.

**Unowned entirely:** cost and object-storage write amplification. v0.52's DLQ,
v0.52.1's backfill cursors, and this changelog all add durable writes, and no
scheduled version owns per-view cost or write-amplification budgets.

---

## Essential features implementation program

This section adopts `rockstream-essential-features-implementation-plan.md` as
the post-v0.59.24 roadmap. Existing versions through v0.59.24 remain unchanged.
The next planning unit is v0.60. Promotion to v1.0 is unscheduled.

**Repository baseline:** `trickle-labs/rockstream` `main` at `b9fea7a1ec5ac98824d0ed50b8c9d57c1f20b73b`  
**Workspace baseline:** `0.59.15`  
**Primary objective:** Make the essential SQL, incremental-processing, transactional, and regional capabilities real, publicly reachable, durable, bounded, observable, and release-qualified.  
**Explicit non-goal:** No new connector families. The supported integration boundary remains PostgreSQL CDC and Kafka sources, plus the Kafka sink.

---

### 1. Executive summary

RockStream already has much of the difficult infrastructure required for the requested feature set:

- Delta-native operator state and dirty-key persistence.
- Durable shared arrangements.
- Factorized and filtered incremental view maintenance.
- Shared window slices and skew-aware execution.
- Bounded spill to SlateDB.
- Checkpointing, fencing, recovery, shard migration, and formal verification.
- PostgreSQL wire serving.
- Internal merge-law metadata.
- Logical PlanIR nodes and operator implementations for several features that are not yet consistently surfaced.

The remaining problem is not simply “write more operators.” It is that implementation status is fragmented across layers. A feature may exist in PlanIR, in SQL lowering, or as a Rust operator without being compiled by the maintained-view path, reachable through pgwire, documented accurately, or qualified under failure.

This plan therefore starts with two mandatory foundations:

1. **A typed physical semantics layer** shared by grouping, joins, sorting, windows, indexes, state persistence, and exchange.
2. **An end-to-end feature delivery contract** that proves the complete path:

```text
SQL grammar
  -> binding and semantic validation
  -> DataFusion logical plan
  -> RockStream PlanIR
  -> incremental differentiation / physical selection
  -> maintained-view compiler
  -> runtime operator
  -> durable state and recovery
  -> pgwire result encoding
  -> catalogs, EXPLAIN, metrics, documentation, and upgrade contract
```

A feature is not “implemented” until that path is complete. An internal PlanIR node or operator does not count as public availability.

The program is divided into eight release trains:

1. Finish the existing v0.59 closure through `v0.59.24`.
2. Build the typed semantics and feature-delivery foundation.
3. Graduate aggregates, joins, set operations, and core analytic windows.
4. Implement general `ORDER BY`/`LIMIT`, Top-K, HOP, SESSION, and advanced window semantics.
5. Surface `LATERAL` and recursive CTEs.
6. Add durable time-driven predicates and user-visible algebra/CRDT support.
7. Add single-shard and distributed serializable transactions.
8. Add multi-region standby, failover, and explicitly scoped active-active modes.

The connector surface remains frozen throughout.

---

### 2. Goals and non-goals

#### 2.1 Goals

The program must deliver all of the following:

- Promote useful, precisely defined aggregate, relational, and analytic capability cells from `Experimental` to `Core`.
- Support typed equality, hashing, sorting, persistence, and PostgreSQL-compatible semantics across admitted types.
- Support ad hoc and continuously maintained `ORDER BY`/`LIMIT`.
- Support deterministic multi-column analytic ordering, richer frames, and `NTILE`.
- Make HOP and SESSION windows publicly reachable and production-qualified.
- Support table-function and correlated `LATERAL`.
- Support recursive CTEs, beginning with monotone recursion and progressing to deletion-aware recursion.
- Support processing-time temporal predicates that retract rows without new source input.
- Expose built-in CRDT columns and safe custom merge laws.
- Support serializable direct transactions.
- Support staged multi-region resilience and selected active-active operation.
- Ensure every feature is surfaced through pgwire, catalogs, `EXPLAIN`, metrics, diagnostics, generated documentation, and capability contracts.
- Preserve bounded memory, state, queues, timers, retries, shuffle, and output amplification.
- Preserve crash safety, mixed-version safety, rollback boundaries, and object-store durability.

#### 2.2 Non-goals

This program does **not** include:

- New source or sink connector families.
- A connector marketplace or generic connector SDK expansion program.
- Lakehouse sink or catalog expansion.
- PostgreSQL feature parity merely for parity’s sake.
- Silent fallback from incremental maintenance to full recomputation.
- Unbounded correlated nested-loop execution.
- Unbounded ordered materialized relations with implicit full re-emission after every update.
- Arbitrary unsandboxed native code in storage merge operators.
- Active-active behavior without a named consistency mode.
- Features marked “implemented” based only on parser, PlanIR, operator, or documentation presence.

---

### 3. Verified baseline and current discontinuities

The baseline below is the reason the plan is organized as integration and semantic closure rather than a complete rewrite.

| Area | Current asset | Blocking discontinuity |
|---|---|---|
| Capability contract | `capabilities.toml`, generated capability matrix, dispatch evidence, semantic ledgers | Aggregates, relational operators, and analytics are represented as broad families. One unsupported type cell demotes the entire family. |
| SQL type contract | `contracts/sql-type-matrix.toml` | Operation-level statuses are still broader than specific function, key, frame, join-kind, and retraction combinations. |
| Aggregates | Delta-native mutation emission, group-key packers, distinct lanes, factorized join-to-aggregate, durable recovery | Several paths remain Int64-centric or use surrogate packing; decimal, text, floating, NULL, and multi-lane combinations do not share one complete typed kernel. |
| Joins | Inner/outer/semi/anti IR and operators, shared arrangements, factorization, amplification governor | Typed equality and row payload support remain uneven. Some join forms and type cells reject late in compilation. |
| Set operations | Distinct, union, intersect/except semantics in IR and operator history | Typed full-row equality and public operation-specific capability cells are incomplete. |
| Analytic windows | Ranking, navigation, sliding sum/avg, partition recomputation, state accounting | Generic windows are Int64-oriented, descending order is rejected, only limited `ROWS` frames are admitted, and `NTILE` is rejected. |
| Top-K | Incremental Top-K with refill and spill support | One Int64 rank column, descending order only, no general multi-column `SortSpec`, no general `OFFSET`/`WITH TIES`. |
| HOP/SESSION | PlanIR variants, lowering helpers, compiler arms, runtime operators | Public reachability is inconsistent with documentation; typed time and payload support is narrow; session partitioning is inferred; multi-aggregate HOP composition has known gaps. |
| `LATERAL` | `PlanNode::Lateral`, `LateralOp`, lowering of DataFusion `Unnest` | The maintained-view compiler does not compile `PlanNode::Lateral`; explicit correlated `LATERAL` needs an `Apply` model and decorrelation. |
| Recursive CTE | `PlanNode::Recursion`, `RecursionOp`, DataFusion `RecursiveQuery` lowering | The maintained-view compiler does not compile recursion. The current operator recomputes the fixed point from base state per epoch and is Int64/set oriented. |
| Processing-time predicates | Runtime clock abstraction, frontiers, epochs, durable state | No durable timer source exists to emit retractions when wall-clock time passes without new input. |
| Merge laws | `LawBundle`, law IDs/versions, algebraic properties, compaction/frontier policies | No user-visible CRDT type system or safe `CREATE MERGE LAW` surface. |
| Transactions | pgwire transaction state machine, savepoints, idempotency envelopes, read-your-writes | `SERIALIZABLE` is explicitly unsupported; no general MVCC/certification or distributed transaction-decision protocol exists. |
| Multi-region | Checkpoint export/restore and single-region failure machinery | No regional generation/fencing protocol, warm standby frontier, region failover, or active-active consistency model. |

#### 3.1 Baseline source references

The implementation program should keep these files as explicit baseline references:

- `capabilities.toml`
- `contracts/sql-type-matrix.toml`
- `docs/capability-matrix.md`
- `docs/language-features.md`
- `NEW_ROADMAP.md`
- `crates/rockstream-plan/src/lib.rs`
- `crates/rockstream-sql/src/lower.rs`
- `crates/rockstream-ops/src/compile.rs`
- `crates/rockstream-ops/src/window.rs`
- `crates/rockstream-ops/src/topk.rs`
- `crates/rockstream-ops/src/lateral.rs`
- `crates/rockstream-ops/src/recursion.rs`
- `crates/rockstream-types/src/merge_law.rs`
- `crates/rockstream-gateway/src/session.rs`

---

### 4. Program rules that prevent hidden or half-surfaced features

#### 4.1 Separate implementation state from strategic tier

The current `Core`/`Maintain`/`Experimental` tier is a compatibility commitment. It must not be overloaded to describe whether code is internally present.

Add an independent `availability` field:

```toml
availability = "absent"       # no implementation
availability = "internal"     # code exists, not publicly reachable
availability = "preview"      # publicly reachable with explicit opt-in
availability = "production"   # publicly reachable by default
```

The legal combinations are:

| Availability | Tier | Meaning |
|---|---|---|
| `absent` | any | Planned or rejected; no public path. |
| `internal` | `Experimental` | IR/operator/prototype exists, but no public contract. |
| `preview` | `Experimental` | Public and usable, but opt-in and without continuity/performance guarantees. |
| `production` | `Core` | Release-gated compatibility contract. |
| `production` | `Maintain` | Supported and secure, but not a growth area. |

A broad capability must never be demoted solely because one neighboring cell is unsupported. Instead, split it into independently promotable variants.

#### 4.2 Capability Contract v2

Extend `capabilities.toml` rather than creating a competing source of truth.

A capability variant should contain at least:

```toml
[[capability.variant]]
id = "aggregate.sum.exact"
family = "language.aggregates"
availability = "production"
tier = "Core"
semantic_version = 1

syntax = ["SUM(expr)", "SUM(DISTINCT expr)"]
types_contract = "contracts/sql-type-matrix.toml#aggregate.sum.exact"
retraction_semantics = "supported"
distributed_semantics = "partial-combine"
state_format = "aggregate-state-v3"

dispatch = [
  "query_async_entry",
  "sql_lowering",
  "plan_compilation",
  "response_encoding",
]

compiler_symbol = "compile_typed_aggregate"
operator_symbol = "TypedAggregateOp"

limits = [
  "workload_state_budget",
  "group_cardinality_budget",
]

metrics = [
  "aggregate_state_bytes",
  "aggregate_dirty_key_count",
  "aggregate_spill_bytes",
]

errors = [
  "RS-1012",
  "RS-1019",
  "RS-5003",
]

proofs = [
  "unit",
  "oracle",
  "pgwire",
  "lfs_recovery",
  "minio_recovery",
  "multi_worker",
  "upgrade",
  "performance",
]

limitations = [
  "locale-sensitive collations are not admitted",
]
```

The generator must produce:

- `docs/capability-matrix.md`
- `docs/sql-support.md`
- `docs/experimental-features.md`
- `docs/limitations.md`
- `rockstream_catalog.capabilities`
- `SHOW ROCKSTREAM CAPABILITIES`
- validation data used by the SQL planner.

#### 4.3 Mandatory end-to-end reachability record

Every publicly claimed SQL feature must reference concrete anchors for every required layer:

| Layer | Required evidence |
|---|---|
| Grammar/parser | Named parser or DataFusion plan shape test |
| Binding/types | Named type and semantic-validation test |
| Logical lowering | Named `LogicalPlan -> PlanNode` test |
| Physical selection | Named `PlanNode -> physical stages` test |
| Runtime | Named operator or pipeline test |
| Persistence | State codec/version and recovery test |
| Public dispatch | Raw pgwire end-to-end test |
| Encoding | Row description, OID, text/binary result test |
| Diagnostics | `EXPLAIN`, status, metric, and error-code test |
| Documentation | Generated capability entry and executable example |
| Upgrade | N/N+1 state and feature-contract test |

Add:

```text
scripts/check-feature-completeness.py
scripts/check-public-reachability.py
scripts/check-capability-cells.py
```

These scripts must fail when:

- A public capability has no compiler anchor.
- A PlanIR variant has no maintained-view compiler arm and is marked preview/production.
- A compiler arm has no public test.
- A capability is documented as implemented but has no dispatch path.
- A required proof is missing or skipped.
- An unsupported cell falls through to another path instead of returning its declared error.
- A feature’s state format changes without migration and rollback declarations.

#### 4.4 Explicit preview activation

“Available, but explicitly experimental” should be a real product state.

Add both cluster and session activation:

```toml
[experimental]
enabled = ["recursive_cte", "lateral_correlated"]
```

```sql
SET rockstream.experimental_features = 'recursive_cte,lateral_correlated';
```

Rules:

- Preview features are disabled unless activated.
- `CREATE MATERIALIZED VIEW` using a preview feature emits a `NOTICE`.
- The view catalog persists the feature IDs and semantic versions used by the plan.
- `EXPLAIN INCREMENTAL` labels each preview node.
- `SHOW ROCKSTREAM CAPABILITIES` reports availability, tier, semantic version, required opt-in, and limitations.
- Startup or upgrade fails closed if a persisted view requires a preview semantic version the binary cannot execute.
- Preview still requires correctness, boundedness, crash recovery, and coded failure. What it does not require is continuity, complete type coverage, or final performance qualification.

#### 4.5 Feature Definition of Done

A feature or capability cell is complete only when all applicable items pass:

- [ ] Public SQL syntax works through raw pgwire.
- [ ] Simple, extended, and prepared-statement protocols are tested where parameters apply.
- [ ] Type and semantic validation happens before state mutation.
- [ ] Insert, update, delete, duplicate, NULL, retraction, and key-change behavior match an independent oracle.
- [ ] Backfill uses the snapshot/delta fence and produces the same result as live ingestion.
- [ ] State is bounded, spillable where appropriate, and observable.
- [ ] LFS and MinIO recovery pass for durable state.
- [ ] Multi-worker execution is oracle-identical to single-worker execution.
- [ ] Failure injection produces no wrong answer, loss, or duplicate.
- [ ] Mixed-version operation, migration, rollback, and old-export restore are defined.
- [ ] `EXPLAIN`, status, catalogs, and metrics expose the selected strategy and limits.
- [ ] Unsupported neighboring cells return the documented `RS-XXXX` code.
- [ ] Documentation is generated and the runnable example passes.
- [ ] The capability record names every proof.
- [ ] Performance and capacity are measured on a declared profile.
- [ ] The sign-off includes raw evidence and zero hidden skips.

---

### 5. Shared architecture foundation

All major features depend on one typed physical semantics layer. Building separate ad hoc encodings for aggregates, joins, windows, sorting, transactions, and regions would recreate the current unevenness.

#### 5.1 Typed scalar and row model

Add to `rockstream-types`:

```rust
pub struct TypeDescriptor {
    pub sql_type: SqlType,
    pub arrow_type: DataType,
    pub nullable: bool,
    pub decimal: Option<DecimalDescriptor>,
    pub collation: Option<CollationDescriptor>,
    pub time_zone_policy: Option<TimeZonePolicy>,
}

pub enum CanonicalScalarRef<'a> {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal128 { value: i128, precision: u8, scale: i8 },
    Utf8(&'a str),
    Bytes(&'a [u8]),
    Uuid([u8; 16]),
    Date32(i32),
    Timestamp { value: i64, unit: TimeUnit, zone: TimeZoneId },
}
```

Requirements:

- Preserve SQL type identity; do not infer everything from byte width.
- Preserve NULL separately from an empty byte string.
- Preserve decimal precision and scale.
- Preserve timestamp unit and time-zone policy.
- Canonicalize values according to the pinned PostgreSQL compatibility contract.
- Provide zero-copy views over Arrow arrays on hot paths where possible.

#### 5.2 Distinct key concepts

Do not use one byte encoding for every semantic purpose.

Implement:

```rust
pub trait EqualityKeyCodec
pub trait HashKeyCodec
pub trait SortKeyCodec
pub trait PersistentRowCodec
```

The same type may have different equality and sort treatment.

Each key must include:

- Type identity.
- NULL tag and NULL equality mode.
- Collation ID and version for text.
- Decimal scale policy.
- Timestamp/time-zone policy.
- Floating-point normalization required by the SQL contract.
- State-codec version.

#### 5.3 Common `SortSpec`

Add to `rockstream-plan`:

```rust
pub struct SortSpec {
    pub expressions: Vec<SortExpr>,
    pub stable_tie_breaker: TieBreaker,
}

pub struct SortExpr {
    pub expr: TypedExpr,
    pub direction: SortDirection,
    pub nulls: NullPlacement,
    pub collation: Option<CollationId>,
}
```

Every ordering feature must use this one structure:

- Ad hoc sort.
- Top-K.
- Window ordering.
- Index ordered scans.
- Range partitioning.
- Merge-sort exchange.
- Transaction predicate/range conflicts where ordering is relevant.

#### 5.4 Richer PlanIR

Add or extend:

```rust
PlanNode::Sort {
    input,
    spec,
}

PlanNode::Limit {
    input,
    offset,
    fetch,
    with_ties,
}

PlanNode::Apply {
    outer,
    inner,
    kind,
    bindings,
    max_matches_per_outer,
}

PlanNode::TimerFilter {
    input,
    predicate,
    expiry_expr,
    clock_domain,
}

PlanNode::RecursiveUnion {
    base,
    step,
    recursive_binding,
    semantics,
    max_iterations,
    max_state_bytes,
}

WindowExpr {
    function,
    partition_by,
    order_by: SortSpec,
    frame: WindowFrame,
}
```

Every stateful PlanIR node must carry enough typed metadata to compile without guessing an output type from “anything else defaults to Int64.”

#### 5.5 Stateful operator interface

Refactor stateful operators around:

```rust
pub trait StatefulOperator {
    fn process_epoch(&mut self, input: EpochInput) -> Result<OperatorEpochResult, OpError>;
    fn state_descriptor(&self) -> StateDescriptor;
    fn append_state_delta(&self, batch: &mut WriteBatch) -> Result<(), OpError>;
    fn restore(&mut self, reader: &DbReader) -> Result<(), OpError>;
    fn state_bytes(&self) -> u64;
    fn spill_status(&self) -> SpillStatus;
}
```

Keep the existing delta-native mutation stream. Do not reintroduce full-state checkpoint scans.

#### 5.6 Durable format rules

Every new state family must define:

- State format ID and version.
- Key and value codecs.
- Mixed N/N+1 read/write policy.
- Migration algorithm.
- Crash points.
- Rollback boundary.
- Old-export restore behavior.
- Compaction behavior.
- Law/collation/time-zone version dependencies.
- A fail-closed error for unknown versions.

---

### 6. Roadmap structure

#### 6.1 Planning-unit rule

Use the existing roadmap convention:

- One planning unit starts at approximately six person-weeks.
- Any mandatory slice estimated above two person-weeks becomes its own sub-version.
- Any new durable format or formal model gets an independently signable sub-version.
- A release train is complete only when every mandatory sub-version is signed off.
- Performance qualification is not allowed to introduce architecture.

#### 6.2 Dependency graph

```text
Finish v0.59.24 qualification
        |
Capability Contract v2 + public reachability gate
        |
Typed value/key/state semantics
        |-----------------------------|
        |                             |
Core aggregate/join/window       Transaction MVCC foundation
        |                             |
Sort/Top-K + HOP/SESSION         Serializable certification
        |                             |
LATERAL + recursion              Distributed transaction commit
        |                             |
Timers + CRDT/merge laws --------|----|
                                      |
                         Multi-region standby/failover
                                      |
                       Scoped active-active consistency modes
```

#### 6.3 Existing v0.59 closure: complete without feature expansion

Do not derail the current release path by inserting the new feature program into v0.59 qualification.

| Existing version | Required outcome for this plan |
|---|---|
| `v0.59.16` | Structured diagnostics used by all later feature errors. |
| `v0.59.17` | Public-path scenario/differential framework becomes the standard feature proof harness. |
| `v0.59.18` | Lifecycle, external client, backend, and no-hidden-skip closure. |
| `v0.59.19` | Exact SQL semantics, limits, collation, time, NULL, numeric, and compatibility contract. Also introduce operation-specific capability cells rather than only broad families. |
| `v0.59.20` | Implement and prove every admitted common type/operation cell. |
| `v0.59.21` | Graceful shutdown and health semantics. |
| `v0.59.22` | Supported deployment profiles. |
| `v0.59.23` | Calibrated capacity and estimator. |
| `v0.59.24` | Exact-artifact multi-worker qualification. No automatic v1 promotion. |

Architecture and ADR work for the post-v0.59.24 program may be prepared in parallel, but feature code must rebase onto and requalify against the exact v0.59.24 architecture.

---

### Part I — v0.60–v0.64: Feature delivery and typed semantics foundation

### 7. v0.60 — Capability Contract v2 and public reachability ledger

#### Scope

- Add `availability`, `semantic_version`, `compiler_symbol`, `operator_symbol`, `state_format`, `limits`, `metrics`, `errors`, and `proofs`.
- Split broad families into variants.
- Add generated limitations and preview docs.
- Add CI feature-completeness and reachability checks.
- Add capability rows to `rockstream_catalog`.
- Persist feature IDs/versions with materialized-view definitions.

#### Initial family decomposition

At minimum:

```text
aggregate.count
aggregate.sum.exact
aggregate.sum.float
aggregate.avg.exact
aggregate.avg.float
aggregate.minmax
aggregate.distinct
grouping.single-key
grouping.composite
join.inner.equi
join.outer.equi
join.semi-anti
join.cross
set.union
set.intersect
set.except
window.ranking
window.navigation
window.rolling.rows
window.advanced-frames
topk.maintained
sort.snapshot
time.tumble
time.hop
time.session
lateral.table-function
lateral.correlated
recursion.monotone
recursion.deletion-aware
temporal.processing-time
algebra.builtin-crdt
algebra.custom-law
transaction.serializable.local
transaction.serializable.distributed
region.warm-standby
region.active-passive
region.active-active.merge-law
region.active-active.serializable
```

#### Proof

- Mutating any layer anchor makes CI fail.
- A synthetic parser-only feature cannot be marked preview.
- A synthetic operator-only feature cannot be marked preview.
- The current recursion and lateral discontinuities are detected automatically.
- Generated docs and runtime catalogs agree byte-for-byte.

---

### 8. v0.61 — Typed scalar, equality, hash, and row codecs

#### Scope

- Add `TypeDescriptor` and canonical scalar access.
- Add equality/hash/persistent row codecs.
- Implement the semantics pinned by `v0.59.19`.
- Cover integer, Boolean, text, UUID, date, timestamp, timestamptz, decimal/numeric, float, and arrays used as parameters.
- Replace empty-byte NULL conventions in stateful paths.
- Introduce `rockstream_binary_v1` collation as a versioned codec.

#### Migration

- Define a new typed arrangement key version.
- Keep old Int64/surrogate formats readable for one N/N+1 window.
- Write only the new format after the migration frontier.
- Make rollback legal only before the first new-format checkpoint is committed.

#### Proof

- Equality and hash are consistent for every admitted type.
- Sort equality and equality-key equality differ only where the SQL contract requires.
- PostgreSQL differential corpus covers NaN, signed zero, decimal scales, text bytes, timestamps, DST boundaries, and NULL.
- LFS/MinIO restore and compaction preserve typed keys.

---

### 9. v0.62 — Common `SortSpec`, ordered keys, and typed window specification

#### Scope

- Add multi-column direction and NULL placement.
- Add deterministic collation and stable tie-breaker.
- Add range and peer-group semantics.
- Replace window `order_by: Vec<usize>` with typed `SortSpec`.
- Add explicit `ROWS`, `RANGE`, and `GROUPS` frame representation.
- Add typed navigation values and defaults.

#### Proof

- Sort-key order matches the pinned PostgreSQL reference for every admitted type.
- Mixed ASC/DESC and NULL placement pass.
- Sort keys survive persistence, exchange, and restart.
- Peer groups are deterministic across workers.

---

### 10. v0.63 — Typed stateful operator API and format migration

#### Scope

Refactor these operators to consume typed row/key codecs:

- Aggregate.
- Min/Max.
- Distinct.
- Inner/outer/semi/anti join.
- Top-K.
- Analytic window.
- TUMBLE/HOP/SESSION.
- Index arrangements.

The refactor must preserve:

- Delta-native dirty-key mutation counts.
- Shared arrangement identity.
- Factorized plan behavior.
- Spill behavior.
- State accounting.
- Existing formal invariants.

#### Proof

- Existing v0.59 architecture benchmarks do not regress beyond the declared threshold.
- One-key changes stay approximately constant at 1K/100K/10M state.
- Every old-format checkpoint either migrates or fails closed.
- No operator silently falls back to batch recomputation.

---

### 11. v0.64 — Preview activation, public transcript harness, and release gate

#### Scope

- Implement cluster/session preview activation.
- Emit preview notices.
- Persist feature dependencies in view metadata.
- Add generic raw-pgwire scenario generation from capability records.
- Add generic negative tests for unsupported cells.
- Add `EXPLAIN` feature/tier/semantic-version annotations.

#### Exit criteria

No later feature train begins until:

- An internal-only operator cannot be advertised as public.
- A public preview cannot exist without a raw-pgwire proof.
- A Core cell cannot exist without all mandatory proof levels.
- Upgrade tests detect missing preview semantic versions.

---

### Part II — v0.65–v0.69: Graduate the essential existing operator families

### 12. v0.65 — Exact aggregates and typed grouping

#### Public surface

- `COUNT(*)`
- `COUNT(expr)`
- `SUM`
- `AVG`
- `MIN`
- `MAX`
- Global aggregates.
- Single and composite `GROUP BY`.
- Grouping keys across admitted exact, text, UUID, decimal, date, and temporal types.

#### Runtime work

- Replace fixed `(i64 key, i64 value)` assumptions.
- Implement typed accumulator descriptors.
- Use wide checked state for integer/decimal accumulation.
- Preserve group disappearance semantics.
- Restore typed keys without runtime-only surrogate state.
- Integrate partial combining with merge-law metadata.

#### Acceptance

- Random insert/update/delete/retraction sequences.
- Empty input and empty group behavior.
- NULL input behavior.
- Overflow and cast behavior.
- Text, UUID, decimal, date, timestamp, and timestamptz groups.
- Single/multi-worker equivalence.
- Spill and recovery.

---

### 13. v0.66 — Floating aggregates, DISTINCT, and NULL completeness

#### Scope

- Define and implement PostgreSQL-compatible float behavior, including NaN and signed zero.
- Complete `COUNT(DISTINCT ...)` and `SUM(DISTINCT ...)`.
- Add typed MIN/MAX multisets.
- Support multiple aggregate lanes without Int64-only intermediate joins.
- Eliminate sentinel-based SQL NULL approximations where typed NULL state can be used directly.

#### Boundaries

- Approximate aggregates are out of scope unless separately admitted.
- Exact distinct remains bounded by state budget and spills rather than silently approximating.

#### Acceptance

- PostgreSQL differential corpus.
- Duplicate storms and over-retractions.
- Removal of the current extremum.
- Multi-aggregate groups where one lane has no matching value.
- Restart after partial spill.

---

### 14. v0.67 — Typed joins and set operations

#### Scope

- Inner, left, right, full, semi, anti, and cross joins.
- Typed composite equality keys.
- NULL semantics and `IS NOT DISTINCT FROM` where admitted.
- Residual predicates after equi-key probing.
- Typed row payloads in arrangements.
- `UNION`, `INTERSECT`, `EXCEPT`, and `DISTINCT`, with set and bag variants.
- Float equality joins only after their exact PostgreSQL semantics are admitted and proven.

#### Runtime work

- Typed shared arrangements.
- Outer unmatched-row state using typed row identity.
- Key-changing update handling.
- Factorized join-to-aggregate over typed payloads.
- Amplification governor estimates by physical strategy.
- Spill for heavy keys and outer unmatched state.

#### Acceptance

- NULL-heavy randomized join matrix.
- Duplicate multiplicity.
- Matched -> unmatched -> matched transitions.
- Key changes.
- High fan-out and skew.
- Worker loss and micro-migration.
- Set-operation zero crossings and bag weights.

---

### 15. v0.68 — Core analytic windows

#### Initial Core contract

- `ROW_NUMBER`
- `RANK`
- `DENSE_RANK`
- `LAG`
- `LEAD`
- Bounded `ROWS` sliding `SUM` and `AVG`
- Multi-column ordering.
- ASC/DESC.
- NULLS FIRST/LAST.
- Typed partition and order keys.
- Typed navigation values and defaults.

#### Runtime work

- Replace Int64 rows and outputs.
- Use the common `SortSpec`.
- Separate peer-group equality from total row tie-breaking.
- Replace full partition sorting where possible with an ordered trace.
- Preserve a bounded partition-recompute fallback with explicit amplification limits.

#### Acceptance

- Insert/delete before and inside a ranked partition.
- Duplicate peers.
- Tie-break stability across restart and reshuffle.
- Navigation with NULL/default.
- Large partitions, spill, and bounded refusal.
- Multi-worker deterministic output.

---

### 16. v0.69 — Core graduation qualification

Promote only the cells that pass:

- Public pgwire differential tests.
- LFS and MinIO durability.
- Multi-worker equivalence.
- Upgrade and rollback.
- Resource bounds.
- Capacity profile.
- Generated contract consistency.

A capability family may contain a mix of Core and Experimental variants. That is expected.

---

### Part III — v0.70–v0.75: Ordering, Top-K, advanced windows, HOP, and SESSION

### 17. v0.70 — General ad hoc `ORDER BY`

#### Plan and execution

- Lower DataFusion `Sort` to `PlanNode::Sort`.
- Use distributed local sort plus merge.
- Use bounded in-memory runs and SlateDB spill.
- Push `LIMIT` into local Top-K where legal.
- Preserve snapshot/frontier pinning for every shard.
- Stream result batches to pgwire rather than materializing the full result.

#### Public surface

```sql
SELECT ...
ORDER BY a DESC NULLS LAST, b ASC
LIMIT 100
OFFSET 20;
```

#### Acceptance

- Multi-shard complete ordering.
- Mixed types and collations.
- Spill larger than worker memory.
- CancelRequest and timeout.
- Stable prepared-statement behavior.
- No hard demo-sized result cap.

---

### 18. v0.71 — Maintained `ORDER BY ... LIMIT`

#### Semantics

A materialized ordered view is admitted only when bounded by `LIMIT`.

```sql
CREATE MATERIALIZED VIEW top_customers AS
SELECT customer_id, revenue
FROM customer_revenue
ORDER BY revenue DESC, customer_id ASC
LIMIT 100;
```

#### Runtime

- Rewrite `Sort + Limit` to generalized typed Top-K.
- Multi-column `SortSpec`.
- `OFFSET`.
- `WITH TIES`.
- Duplicate multiplicity.
- Per-shard candidates plus globally merged Top-K.
- Candidate fringe sufficient for delete refill.
- Spill instead of fixed fatal buffer overflow.

#### Boundary

`ORDER BY` without `LIMIT` on a materialized view does not imply an incrementally maintained global row sequence. The relation remains logically unordered and is sorted when read, unless a separately admitted ordered-index capability is requested.

---

### 19. v0.72 — Public HOP windows

#### Work

- Reconcile actual lowering/compiler reachability with documentation.
- Define one official SQL syntax and compatibility form.
- Use typed time columns.
- Use shared time slices from the v0.59 architecture.
- Support single and multiple aggregates.
- Define alignment, origin, time zone, watermark, and late-data semantics.
- Restore and compact slice state by frontier.

#### Acceptance

- Rows assigned to every correct overlapping window.
- Inserts, updates, deletes, and late changes.
- Multiple window widths sharing slices.
- Recovery at close/expiry boundaries.
- Multi-aggregate HOP.
- Multi-worker equivalence and state-sharing measurement.

---

### 20. v0.73 — Public SESSION windows

#### Plan change

Make partitioning explicit:

```rust
PlanNode::SessionWindow {
    input,
    partition_by,
    time_expr,
    gap,
    late_data_policy,
}
```

Do not infer the session key as “all columns except time.”

#### Runtime

- Ordered event index per partition.
- Neighbor lookup.
- Merge sessions on insertion.
- Split sessions on retraction or key/time update.
- Stable session identity.
- Localized affected-component rebuild.
- Spill for very large sessions.
- Explicit worst-case amplification refusal.

#### Acceptance

- Bridging insertion merges two sessions.
- Retraction of a bridging event splits one session.
- Out-of-order events.
- Late-data policies.
- Recovery during merge/split.
- Skewed session key.
- Exact output against a batch sessionization oracle.

---

### 21. v0.74 — Advanced window frames and `NTILE`

#### Scope

- `ROWS` with bounded/unbounded endpoints.
- `RANGE`.
- `GROUPS`.
- `FIRST_VALUE`.
- `LAST_VALUE`.
- `NTH_VALUE`.
- `NTILE`.
- Aggregate windows over admitted functions.

#### Runtime

- Peer-group index.
- Prefix/segment structures for rolling algebraic aggregates.
- Law-specific inverse or recompute strategy.
- Explicit handling for one change that affects an unbounded suffix.
- Amplification budget and refusal/notice.

#### Acceptance

- PostgreSQL differential frame corpus.
- Peer and NULL ordering.
- Deletes that change frame membership.
- Large partitions and spill.
- Restart and compaction.

---

### 22. v0.75 — Ordering and temporal qualification

Publish measured:

- Sort throughput and spill cost.
- Top-K update amplification.
- Window partition amplification.
- HOP slice sharing.
- SESSION merge/split cost.
- State-over-RAM behavior.
- Multi-worker scale.
- Exact capacity boundaries shown by `EXPLAIN INCREMENTAL ESTIMATE`.

---

### Part IV — v0.76–v0.81: `LATERAL` and recursive CTEs

### 23. v0.76 — Table-function `LATERAL` and compiler wiring

#### Immediate integration closure

- Add `PlanNode::Lateral` to `compile_node`.
- Add its name to compiler diagnostics.
- Compile `LateralOp` as a stateless stage.
- Support current `UNNEST` lowering through the maintained-view path.
- Add official SQL syntax for:
  - `UNNEST`.
  - Literal/typed `GENERATE_SERIES`.
  - JSON array expansion where admitted.

#### Boundaries

- Per-row expansion has a configurable maximum output count and byte budget.
- Expansion beyond the bound returns a coded error or applies backpressure; it never accumulates unbounded output.
- Volatile/non-deterministic table functions are rejected in materialized views.

#### Acceptance

- Input retraction retracts exactly the produced rows.
- NULL and empty collections.
- Nested arrays.
- Typed outputs.
- Raw pgwire CREATE MATERIALIZED VIEW and recovery.

---

### 24. v0.77 — Correlation IR and decorrelation

#### Add

```rust
PlanNode::Apply {
    outer,
    inner,
    kind: Cross | Left,
    bindings: Vec<CorrelationBinding>,
    max_matches_per_outer,
}
```

#### Planner

- Resolve correlated references to explicit slots.
- Decorrelate:
  - Equality-correlated subqueries to joins.
  - Correlated aggregates to grouped joins.
  - Top-1/order subqueries to partitioned Top-K.
  - `EXISTS`/`NOT EXISTS` to semi/anti joins.
- Preserve `LEFT JOIN LATERAL` NULL extension.

#### Acceptance

- Planner equivalence tests.
- Correct behavior with zero, one, and many inner matches.
- NULL correlated values.
- Retractions and key changes.

---

### 25. v0.78 — Indexed parameterized `Apply`

For queries that cannot be decorrelated:

- Require an indexable bounded inner lookup.
- Compile parameter bindings into typed arrangement probes.
- Track dependencies from each outer row to produced inner rows.
- Retract exactly the previous correlated output.
- Cache parameterized results under a bounded policy.
- Reject unindexed/unbounded nested loops before deployment.

Expose in `EXPLAIN`:

```text
Apply strategy: indexed-parameterized
Max matches per outer row: 1000
Arrangement: arr-...
Fallback: reject
```

---

### 26. v0.79 — Monotone recursive CTEs

#### Immediate integration closure

- Add recursion to `compile_node`.
- Replace inferred recursive source naming with an explicit `recursive_binding`.
- Introduce a dedicated recursion stage/pipeline.
- Persist fixed-point and iteration state.
- Surface `WITH RECURSIVE` through raw pgwire.
- Enforce one recursive self-reference in the first production subset.

#### Execution

- True semi-naive delta iteration.
- `UNION` set semantics.
- Insert-only recursive facts.
- Iteration and state budgets.
- `complete_through` frontier token.
- Checkpoint and resume an unfinished fixed point.

#### Acceptance

- Reachability/transitive closure.
- Hierarchy expansion.
- Cycles.
- Duplicate edges.
- Multi-worker convergence.
- Kill/restart at every iteration.
- Max-iteration and state-limit errors.

---

### 27. v0.80 — Distributed recursion protocol

#### Formal work

Add a new model, for example:

```text
formal/m8_recursive_frontier.fizz
```

Prove:

- No fixed point is published before every participating shard reports an empty delta at the same iteration frontier.
- Restart cannot lose a derived fact or publish a partial iteration.
- Duplicate iteration messages are idempotent.
- Reassignment preserves iteration ownership.
- Bounded fallback cannot silently change semantics.

#### Runtime

- Per-iteration distributed frontier.
- Deterministic termination detection.
- Skew handling.
- Iteration work accounting.
- Recovery and migration.

---

### 28. v0.81 — Deletion-aware recursion

#### Strategy

Implement one of these explicitly; do not mix them invisibly:

1. Support-count/provenance maintenance.
2. DRed deletion and re-derivation.
3. Bounded affected-component rebuild.

The planner selects the strategy based on query shape and available bounds.

#### Scope progression

- Positive relational recursion with base deletions.
- Multiple derivations of one fact.
- Key-changing updates.
- Later: stratified negation and admitted aggregates.

#### Boundary

Non-stratified, non-convergent, or unbounded recursive forms are rejected with a specific reason and remediation.

---

### Part V — v0.82–v0.86: Durable time-driven predicates and algebraic extensibility

### 29. v0.82 — Durable timer service

#### Architecture

Add a timer subsystem to `rockstream-runtime`:

```rust
pub struct DurableTimer {
    timer_id: TimerId,
    owner: OperatorId,
    fire_at: Timestamp,
    payload_ref: PersistentRowId,
    generation: u64,
}
```

Required behavior:

- Timer registration is committed atomically with operator state.
- Fired timers enter the ordinary epoch/delta path.
- A timer fires at most once for one generation.
- Overdue timers are recovered after restart.
- Timer ownership migrates with the shard.
- Timer backlog is bounded and observable.
- Processing time uses a monotonic runtime clock plus an explicit wall-clock mapping.
- `SimRuntime` controls time deterministically.

#### Formal model

Add a timer ownership/firing model proving:

- No duplicate firing after crash.
- No lost expiry.
- Old owners cannot fire after migration.
- Backlog recovery eventually progresses.

---

### 30. v0.83 — Processing-time temporal predicates

#### Public surface

```sql
CREATE MATERIALIZED VIEW recent_events AS
SELECT *
FROM events
WHERE occurred_at > CURRENT_TIMESTAMP - INTERVAL '1 hour';
```

#### Compilation

- Detect time-dependent predicates.
- Produce `PlanNode::TimerFilter`.
- On insertion, compute expiry.
- On update, cancel/re-register by generation.
- On expiry, emit a synthetic retraction.
- On deletion, cancel the timer.
- Backfill schedules future expiries and immediately excludes already-expired rows.

#### Semantics

Document:

- Processing-time versus event-time behavior.
- Clock jumps and monotonicity.
- Transaction timestamp semantics.
- Pause/restart behavior.
- Time zone and precision.
- Timer delay under overload.
- Whether late firing changes correctness or only freshness.

---

### 31. v0.84 — Built-in CRDT column types

#### Initial types

- `COUNTER`
- `MAX_REGISTER`
- `MIN_REGISTER`
- `LWW_REGISTER`
- `OR_SET`
- Optional `MV_REGISTER` after the first set is proven.

#### SQL surface

```sql
CREATE TABLE account_counters (
    account_id UUID PRIMARY KEY,
    balance_delta COUNTER
);
```

Operations must be type-specific and explicit.

#### Runtime

- Stable law ID/version.
- Typed operand/state codecs.
- Merge and inverse behavior.
- Compaction policy.
- Frontier policy.
- Cross-shard combinability.
- Upgrade and rollback.
- `SHOW MERGE LAWS`.

---

### 32. v0.85 — Safe `CREATE MERGE LAW`

#### First release: restricted declarative law IR

Do not accept arbitrary native code.

A law definition references a small set of deterministic typed primitives:

```sql
CREATE MERGE LAW bounded_counter_v1
INPUT TYPE BIGINT
STATE TYPE BIGINT
CLASS ABELIAN_GROUP
IDENTITY 0
MERGE ADD
INVERSE NEGATE
VERSION 1;
```

The allowed IR must be:

- Deterministic.
- Total over admitted inputs.
- I/O free.
- Clock/randomness free.
- Bounded in time and memory.
- Versioned.
- Sandboxed.

#### Admission

- Property tests over a generated domain.
- Structural verification where possible.
- Declared law properties are checked against the IR.
- The planner only enables partial combine, factorization, retraction, or merge-on-compaction when properties justify them.
- A law change requires a new version and migration.

#### Later extension

A WASM law ABI may be admitted only after:

- Fuel metering.
- Memory limits.
- Deterministic host ABI.
- Reproducible build identity.
- Signature/trust policy.
- Crash isolation.
- State migration protocol.

---

### 33. v0.86 — Algebra qualification

- Fault-inject merge ordering.
- Duplicate and replay operands.
- Compaction/recovery.
- Mixed law versions.
- Cross-shard combine.
- Hot-key bucket combine.
- Upgrade and rollback.
- Deliberately false law declarations must fail the property/structural gate.
- No law may enter `Core` solely because it compiled.

---

### Part VI — v0.87–v0.92: Serializable transaction authority

Serializable transactions change RockStream’s product role. They require a dedicated release train, not a hidden addition to gateway session work.

### 34. Transaction contract

Define before implementation:

- Which relations participate.
- Whether materialized views are synchronously transaction-visible.
- Own-write visibility.
- Snapshot selection.
- Commit order.
- Read-only versus read-write serializable semantics.
- Constraint timing.
- Retry and uncertain-commit behavior.
- Interaction with connector epochs.
- Interaction with `SUBSCRIBE`.
- Interaction with shard migration and failover.

Recommended contract:

- Direct pgwire DML participates in RockStream transactions.
- A transaction reads a globally committed snapshot.
- Its own buffered writes are overlaid on base-table and affected maintained-view reads.
- Commit publishes base and derived deltas in one logical visibility epoch.
- Connector epochs are separate external transactions; they do not join a client transaction.
- `COMMIT` returns the existing freshness token.
- Serialization failure uses a stable SQLSTATE and retry classification.

---

### 35. v0.87 — MVCC storage and transaction identity

#### Storage

Add:

- `TxnId`.
- Read timestamp/frontier.
- Commit timestamp/frontier.
- Versioned row/index entries.
- Tombstones.
- Transaction status record.
- Oldest-active-snapshot GC frontier.
- State-format migration.

#### Proof

- Snapshot reads.
- Own-write overlay.
- Aborted writes never visible.
- Restart during write buffering/preparation.
- Index and base-table visibility agree.

---

### 36. v0.88 — Snapshot isolation and transaction-local view overlay

#### Gateway/runtime

- Pin a transaction snapshot.
- Overlay session writes on base reads.
- Incrementally evaluate buffered changes through affected view DAGs for own-write view reads.
- Preserve savepoint rollback.
- Bound transaction-local overlay state.
- Spill large transactions.

#### Acceptance

- Read-your-own-write across base tables and views.
- Savepoint rollback.
- Concurrent writers.
- Long transaction GC pressure.
- Crash and reconnect semantics.

---

### 37. v0.89 — Single-shard serializable certification

Recommended initial strategy: optimistic certification with predicate/range tracking.

Track:

- Read versions.
- Write keys.
- Predicate/range reads.
- Index ranges.
- Phantom conflicts.
- Dangerous structures or equivalent serializable validation.

On commit:

- Validate under shard ownership/fence.
- Assign serialization point.
- Commit base and derived deltas atomically.
- Return retryable serialization failure when validation fails.

Use a generated history checker, not final-state assertions alone.

---

### 38. v0.90 — Distributed transaction coordinator

#### Protocol

Add a control-plane Raft-backed transaction decision log:

```text
OPEN
 -> PREPARING
 -> COMMIT_DECIDED | ABORT_DECIDED
 -> APPLIED
 -> GC_ELIGIBLE
```

Participants:

- Validate and prepare under current shard fence.
- Persist prepare state.
- Never commit without a durable decision.
- Reconcile after coordinator or participant restart.
- Reject stale owners.

#### Formal model

Add:

```text
formal/m9_distributed_txn.fizz
```

Prove:

- Atomic commit.
- No split decision.
- No stale participant apply.
- Decision recovery.
- Idempotent replay.
- Liveness after bounded failures.
- Correct interaction with shard migration.

---

### 39. v0.91 — Constraints and complete conflict coverage

- Unique and primary-key constraints.
- Transactional secondary indexes.
- Foreign keys only after their cross-shard cost and locking model are accepted.
- Predicate/range conflicts.
- DDL/transaction interaction.
- Deadlock or retry policy.
- Cancellation and timeout.
- Transaction size and duration limits.

---

### 40. v0.92 — Serializable public qualification

Required evidence:

- Standard pgwire `SERIALIZABLE`.
- Driver matrix.
- Generated concurrent histories.
- External serializability checker.
- Process kills at every protocol phase.
- Shard migration during transaction.
- Control leader failure.
- LFS/MinIO recovery.
- Mixed-version upgrade.
- Bounded conflict metadata.
- Throughput and p99 commit latency.

Only after this version should `IsolationLevel::Serializable` stop returning the existing unsupported error.

---

### Part VII — v0.93–v0.97 and v0.98–v0.102: Multi-region

### 41. Multi-region consistency profiles

Do not ship a single ambiguous “multi-region” mode.

Expose explicit profiles:

| Profile | Writes | Reads | Consistency |
|---|---|---|---|
| `dr-export` | Primary region only | Primary only | Cross-region checkpoint RPO/RTO |
| `warm-standby` | Primary region only | Optional standby health reads | Asynchronous applied frontier |
| `active-passive` | One fenced writer region | Region-local committed reads | Failover with one global generation |
| `home-region` | One writer region per shard/key range | Region-local reads at declared frontier | Serializable within ownership model |
| `merge-law-active-active` | Multi-writer only for admitted merge-law state | Region-local/global merged reads | Law-defined convergence |
| `global-serializable` | Multi-region writes | Globally ordered reads | WAN consensus / global transaction order |

---

### 42. v0.93 — Region identity and replicated checkpoint manifest

Add:

- `RegionId`.
- `ClusterGeneration`.
- Region-qualified worker/shard identity.
- Replicated checkpoint/export manifest.
- Region applied frontier.
- Cross-region object verification.
- Region-aware encryption and secrets.
- RPO/RTO metrics.

No write failover yet.

---

### 43. v0.94 — Warm standby

- Continuously ingest committed manifests/state changes into a standby.
- Restore control/catalog state.
- Keep standby workers non-authoritative.
- Verify complete state at a named frontier.
- Expose lag, missing objects, and recovery readiness.
- Regularly query the standby in read-only verification mode.

---

### 44. v0.95 — Frontier-pinned regional read replicas

- Serve only data through the region’s applied committed frontier.
- Report staleness.
- Preserve tenant/security isolation.
- Route read-only sessions by requested staleness.
- Reject a read requiring a frontier the replica has not applied.
- Keep subscription semantics explicit and region-qualified.

---

### 45. v0.96 — Active-passive failover and fencing

#### Protocol

Use a globally durable generation/fencing authority.

Failover:

1. Stop or fence the old region.
2. Advance cluster generation.
3. Confirm standby applied frontier.
4. Acquire connector and sink ownership.
5. Publish new routing.
6. Resume writes.
7. Prevent the old region from rejoining as writer without rebootstrap.

#### Formal model

Add:

```text
formal/m10_region_failover.fizz
```

Prove:

- At most one writer generation.
- No stale-region commit.
- Safe partial failover recovery.
- Connector/sink ownership transfer.
- Eventual progress after a region loss.

---

### 46. v0.97 — Regional resilience qualification

- Region isolation.
- Control-plane partition.
- Object replication delay.
- Failover and failback.
- Old-region resurrection.
- Data corruption/truncated export.
- RPO/RTO and freshness.
- Subscription and client reconnection.
- Upgrade across standby/primary.

---

### 47. v0.98 — Home-region active-active ownership

Support ordinary writes in multiple regions by assigning each shard/key range one writer region.

- Region-aware shard leases.
- Cross-region routing for non-home writes.
- Online home-region migration.
- Transaction coordinator integration.
- Clear cross-region latency behavior.
- No concurrent ordinary writers for the same ownership range.

---

### 48. v0.99 — Merge-law active-active

Allow multi-writer only for capability cells whose law supports it.

- Region-tagged operands.
- Deduplication identity.
- Causal/epoch metadata.
- Law-version compatibility.
- Convergent compaction.
- Region-local and global read frontier.
- Explicit non-support for non-composable laws.

Built-in CRDT work from v0.82–v0.86 is a prerequisite.

---

### 49. v0.100 — Global serializable mode

This is separate from merge-law active-active.

- Global transaction ordering or WAN consensus.
- Cross-region prepare/decision.
- Region failure recovery.
- Published latency envelope.
- Explicit availability behavior under partition.
- No automatic downgrade to weaker semantics.

This mode may be expensive. The product must state the trade-off rather than conceal it.

---

### 50. v0.101 — Active-active migration and conflict observability

- Move home ownership safely.
- Inspect pending cross-region transactions.
- Inspect merge-law convergence.
- Region-specific frontiers.
- Conflict/retry metrics.
- `EXPLAIN` regional routing.
- Operator runbooks.

---

### 51. v0.102 — Active-active qualification

- Region partition.
- Simultaneous writes.
- Region loss during transaction.
- Old-generation replay.
- Law-version mismatch.
- Home-region migration.
- Global serializable histories.
- Bounded cross-region queues and object-store cost.
- No split-brain or silent semantic downgrade.

---

### 52. Feature-specific public surfacing requirements

Every feature train must ship all applicable surfaces.

#### 52.1 SQL and pgwire

- Parser and binder.
- Simple query.
- Extended query.
- Prepared statements.
- Correct PostgreSQL OIDs.
- Text and binary result formats.
- SQLSTATE and `RS-XXXX`.
- CancelRequest and timeout where long-running.
- Transaction status.

#### 52.2 `EXPLAIN INCREMENTAL`

Show:

- Capability ID and tier.
- Preview status.
- Typed key and sort specification.
- Physical strategy.
- State format.
- Shared arrangement IDs.
- Estimated state.
- Spill policy.
- Amplification budget.
- Timer/iteration/transaction/region ownership where relevant.
- Unsupported reason and remediation.

#### 52.3 Runtime catalogs

At minimum:

```text
rockstream_catalog.capabilities
rockstream_catalog.feature_limitations
rockstream_catalog.operator_state
rockstream_catalog.timers
rockstream_catalog.recursive_queries
rockstream_catalog.merge_laws
rockstream_catalog.transactions
rockstream_catalog.regions
rockstream_catalog.region_frontiers
```

Every table must have cardinality and scan bounds.

#### 52.4 Metrics

Every stateful or queued subsystem exposes:

- Current bytes/rows.
- Budget.
- Fill ratio.
- Spill bytes and fault-ins.
- Queue age.
- Input/output/amplification counts.
- Backpressure/refusal count.
- Recovery phase.
- Dominant degradation reason.

Feature-specific examples:

```text
recursive_iteration
recursive_delta_rows
timer_backlog
timer_fire_lag_ms
apply_probe_count
apply_matches_per_outer
transaction_conflict_count
transaction_prepare_age_ms
region_applied_frontier_lag_ms
region_generation
```

#### 52.5 Documentation

Generated:

- Availability and tier.
- SQL syntax.
- Exact semantics.
- Supported type cells.
- State-growth rule.
- Failure and recovery behavior.
- Upgrade policy.
- Limits and errors.
- Preview activation.
- Executable examples.
- Known limitations.

---

### 53. Proof matrix

Legend:

- **U** — unit/property tests.
- **O** — independent batch/PostgreSQL oracle.
- **L** — SlateDB local-filesystem durability.
- **M** — MinIO/S3 durability.
- **S** — deterministic `SimRuntime`.
- **F** — FizzBee formal model.
- **P** — raw pgwire public-path test.
- **D** — multi-process/distributed test.
- **Q** — performance/capacity qualification.

| Capability | U | O | L | M | S | F | P | D | Q |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Typed aggregates | ✓ | ✓ | ✓ | ✓ |  |  | ✓ | ✓ | ✓ |
| Typed joins/set ops | ✓ | ✓ | ✓ | ✓ | ✓ | existing models | ✓ | ✓ | ✓ |
| Analytic windows | ✓ | ✓ | ✓ | ✓ |  |  | ✓ | ✓ | ✓ |
| Sort/Top-K | ✓ | ✓ | ✓ | ✓ |  |  | ✓ | ✓ | ✓ |
| HOP | ✓ | ✓ | ✓ | ✓ | ✓ | existing frontier models | ✓ | ✓ | ✓ |
| SESSION | ✓ | ✓ | ✓ | ✓ | ✓ | existing frontier models | ✓ | ✓ | ✓ |
| Table-function LATERAL | ✓ | ✓ |  |  |  |  | ✓ | ✓ | ✓ |
| Correlated LATERAL | ✓ | ✓ | ✓ | ✓ | ✓ | if new distributed protocol | ✓ | ✓ | ✓ |
| Monotone recursion | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Deletion recursion | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Durable timers | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Built-in CRDT | ✓ | ✓ | ✓ | ✓ | ✓ | law fault suite | ✓ | ✓ | ✓ |
| Custom merge laws | ✓ | property oracle | ✓ | ✓ | ✓ | where coordination changes | ✓ | ✓ | ✓ |
| Serializable local | ✓ | history checker | ✓ | ✓ | ✓ |  | ✓ | ✓ | ✓ |
| Serializable distributed | ✓ | history checker | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Warm standby/failover | ✓ | full multiset | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Active-active | ✓ | history/convergence checker | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

A proof cell may not be waived silently. A waiver must name the version that closes it, and the existing exit-criteria tooling must block progress when that version passes without the proof.

---

### 54. Work organization

#### 54.1 Four standing workstreams

##### A. Semantics and SQL

Owns:

- Capability contracts.
- PostgreSQL semantic reference.
- Type matrix.
- Parser/binder/lowering.
- PlanIR.
- Public syntax and documentation.

##### B. Operators and state

Owns:

- Typed operator kernels.
- Spill.
- State codecs.
- Migration.
- Checkpoint/recovery.
- Performance.

##### C. Transactions and regions

Owns:

- MVCC.
- Certification.
- Distributed transaction protocol.
- Region fencing.
- Standby/failover.
- Active-active modes.
- Formal models.

##### D. Proof and product surface

Owns:

- Scenario DSL.
- Public pgwire tests.
- Differential oracles.
- Catalogs/metrics/EXPLAIN.
- Generated docs.
- No-hidden-skip and reachability gates.
- Release qualification.

Every feature has one feature lead responsible for all four workstreams’ completion. Layer ownership does not allow a feature to be handed off as “done” before the public path closes.

#### 54.2 Pull-request structure

For each feature:

1. Contract and ADR.
2. PlanIR and semantic validation.
3. Compiler and operator.
4. Durable state and migration.
5. Public pgwire and observability.
6. Failure, upgrade, and performance proof.
7. Capability promotion and sign-off.

No PR may mark a feature public before the raw pgwire test exists. No release note may call it implemented before the capability record moves from `internal` to `preview` or `production`.

#### 54.3 Required ADRs

At minimum:

- Typed equality/hash/sort semantics.
- Stable state key encoding.
- General ordered execution.
- Correlated `Apply`.
- Recursive fixed-point protocol.
- Durable timer ownership.
- User-defined merge-law sandbox.
- MVCC and serializable certification.
- Distributed transaction decision log.
- Regional generation and fencing.
- Active-active consistency profiles.

---

### 55. Initial program estimate

Using the current roadmap convention of approximately six person-weeks per planning unit:

| Release train | Initial units | Initial person-weeks |
|---|---:|---:|
| Remaining v0.59 closure (`v0.59.16`–`v0.59.24`) | 9 | 54 |
| v0.60–v0.64 typed semantics and delivery foundation | 5 | 30 |
| v0.65–v0.69 Core aggregate/join/window graduation | 5 | 30 |
| v0.70–v0.75 ordering and temporal analytics | 6 | 36 |
| v0.76–v0.81 LATERAL and recursion | 6 | 36 |
| v0.82–v0.86 timers and algebra | 5 | 30 |
| v0.87–v0.92 serializable transactions | 6 | 36 |
| v0.93–v0.97 regional standby/failover | 5 | 30 |
| v0.98–v0.102 active-active modes | 5 | 30 |
| **Post-v0.59.24 subtotal** | **43** | **258** |

This is an initial program estimate, not a staffing or date commitment. The mandatory split rule will likely increase the count, especially for:

- Typed state-format migration.
- Recursive-distribution formal modeling.
- Durable timers.
- Custom-law sandboxing.
- Distributed transactions.
- Region failover.
- Active-active operation.

A realistic program should reserve 25–40% additional capacity for mandatory durable-format and formal-model splits, performance remediation, and findings from public-path differential testing.

---

### 56. Principal risks and controls

#### 56.1 Typed-kernel regression

**Risk:** Replacing Int64-oriented fast paths reduces throughput or destabilizes the proven architecture.

**Control:**

- Keep current benchmarks as immutable baselines.
- Migrate operator families incrementally.
- Run old/new typed kernels in the external oracle harness.
- Do not retain two permanent execution paths; remove the old path after migration.
- Reopen the owning architecture gate on a material regression.

#### 56.2 Two implementations of one feature

**Risk:** Ad hoc queries use DataFusion while maintained views use a different partial semantics.

**Control:**

- Share type, sort, frame, and semantic validation.
- Permit different physical execution only behind one contract.
- Differential tests must compare both paths.
- `EXPLAIN` must name the physical path.

#### 56.3 Retraction complexity

**Risk:** SESSION, recursion, outer joins, and windows appear correct for append-only workloads but fail under deletes.

**Control:**

- Every feature’s primary oracle corpus includes inserts, updates, deletes, over-retractions, and key changes.
- Append-only evidence cannot promote a capability to Core.
- Maintain support counts, indexed multisets, or bounded rebuilds explicitly.

#### 56.4 Recursion explosion

**Risk:** Fixed-point work or state becomes unbounded.

**Control:**

- Iteration, state, delta, and per-epoch work limits.
- `EXPLAIN` estimates and refusal.
- Semi-naive execution.
- Skew-aware distributed iteration.
- Explicit non-monotone admission.

#### 56.5 Transaction scope creep

**Risk:** Serializable transactions silently turn every connector and external system into one global transaction.

**Control:**

- Direct pgwire DML only.
- Connector epochs remain separate.
- Publish the exact derived-view visibility contract.
- Use retryable serialization errors rather than weakening semantics.

#### 56.6 Split-brain multi-region

**Risk:** An old region continues writing after failover.

**Control:**

- Global generation/fencing.
- Formal model before implementation.
- Object-store and control-plane checks on every commit.
- Old-region resurrection tests.
- No automatic downgrade from serializable to eventual consistency.

#### 56.7 Preview features become permanent unqualified surface

**Risk:** Experimental opt-in becomes an excuse not to finish the feature.

**Control:**

- Every preview has an owning release train and promotion/removal target.
- `SHOW ROCKSTREAM CAPABILITIES` reports age and target.
- Preview semantic versions are persisted.
- Preview cannot be enabled by default.
- A preview that misses two target trains requires explicit re-admission.

#### 56.8 Documentation drift

**Risk:** Documentation again claims parser-only or operator-only support.

**Control:**

- Generate public matrices.
- Parse capability records in CI.
- Run every executable snippet.
- Raw pgwire test required for every “available” feature.
- No hand-edited status tables.

---

### 57. Immediate next actions

These actions can begin without adding connector scope:

1. **Keep `v0.59.16`–`v0.59.24` unchanged as the v0.59 qualification path.**
2. **Approve this program as the accepted post-v0.59.24 direction** and remove contradictory language that treats the requested SQL/transaction/region work as merely speculative.
3. **Draft the Capability Contract v2 ADR** and extend `capabilities.toml`.
4. **Split the broad aggregate, relational, and analytics records** into operation-specific variants during `v0.59.19`.
5. **Add a generated end-to-end reachability ledger** and make it detect the present recursion/lateral compiler gaps.
6. **Write raw pgwire probes for HOP, SESSION, recursive CTE, and UNNEST** to establish the exact current public boundary from executable evidence.
7. **Add explicit negative tests for current compiler gaps** so an unsupported node cannot silently fall back or produce a misleading success.
8. **Draft the typed equality/hash/sort/state ADRs** against the pinned PostgreSQL semantic contract.
9. **Create immutable performance baselines** for aggregate, join, window, Top-K, HOP, and SESSION before the typed refactor.
10. **Assign one feature lead per release train** with responsibility through public surfacing and sign-off.
11. **Create formal-model placeholders and invariant IDs** for recursion, distributed transactions, timers, and regional failover before their Rust implementation starts.
12. **Freeze the connector module inventory in CI** so this program cannot accidentally expand the connector surface.

---

### 58. Program success criteria

The program is successful when:

- Aggregates, joins, set operations, and analytic windows are no longer represented by one coarse Experimental family; each useful capability cell has an honest Core or Experimental status.
- Every Core cell is publicly reachable through raw pgwire and has complete semantic, durability, failure, upgrade, and capacity evidence.
- `ORDER BY`/`LIMIT`, Top-K, HOP, SESSION, `LATERAL`, and recursive CTEs are usable in maintained views through the production compiler.
- Processing-time predicates retract rows correctly without source input.
- Built-in CRDT columns and custom merge laws have safe, versioned, bounded execution.
- `SERIALIZABLE` is a real, history-checked isolation level rather than an accepted-but-unenforced session setting.
- Multi-region modes have explicit consistency names and cannot split-brain or silently weaken semantics.
- No feature is described as implemented merely because a PlanIR node, parser branch, Rust operator, or design document exists.
- Every unsupported boundary is visible in capabilities, `EXPLAIN`, documentation, and a stable coded rejection.
- The supported connector boundary has not expanded.

---

### 59. Final recommendation

Adopt this as a **program roadmap**, not a single mega-version.

The fastest credible route is:

1. Complete the current v0.59.24 qualification.
2. Build the typed semantics and delivery-contract foundation.
3. Graduate the existing operator families.
4. Surface ordering, temporal analytics, `LATERAL`, and recursion.
5. Add durable timers and algebraic extensibility.
6. Treat serializable transactions and multi-region operation as dedicated programs with formal models and public contracts.

That order maximizes reuse of the architecture RockStream has already built while eliminating the exact failure mode that has repeatedly created ambiguity: code existing somewhere in the repository without the complete, publicly reachable, durable, and proven product path.

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
| v0.56 | Rolling-upgrade / mixed-version cluster scenario | `SimRuntime` mixed-version scenario (not a new `.fizz` model — the version-gate logic is a simple monotonic refuse-if-incompatible check, not a new distributed race) | Cross-version pipeline assignment never happens until enough N+1 workers are available; no epoch loss during a simulated rolling upgrade | Mixed-version `SimRuntime` scenario passes; real two-binary TestContainers upgrade drill passes |
| v0.52.4 | M5 retirement (cold-tier sink family deleted — Phase 16.6) | `formal/m5_cold_tier_sink.fizz` moved to `formal/retired/` with a header recording why; dropped from `make verify` | M5-S1…S3, M5-L1, COV-M5 retired — the implementation they gate no longer exists | `make verify` green over M1–M4, M6, M7 with M5 absent; M3 (sink 2PC) unchanged and now covering the single remaining sink; `scripts/check-invariant-pairs.sh` no longer expects M5 pairs, and its `.test.sh` proves the retired IDs are neither required nor silently ignorable |
| v0.51.12 | Operational edge-case specs (found missing by the 2026-08-03 review — Area E) | New machine-checkable specs for quota exhaustion, connector-source failure, object-store brownout + buffer exhaustion, misconfiguration rejection, and late-arriving data (FizzBee where a distributed race exists; otherwise deterministic `SimRuntime` scenario specs) | EDGE-QUOTA, EDGE-SOURCEFAIL, EDGE-BROWNOUT, EDGE-MISCONFIG, EDGE-LATE (each a safety/recovery invariant paired with a runtime `assert!` per §3.7) | Each edge-case spec green at CI-fast bounds; paired `assert!`s present and enforced by `scripts/check-invariant-pairs.sh` extended to the new IDs; `formal-verify` trigger widened so covered-crate PRs actually run them |
| v0.59.5 | Re-prove M1 over delta-native mutation, upsert/tombstone, migration, and commit semantics | `formal/m1_epoch_commit.fizz` delta-state variant plus migration-boundary `SimRuntime` cases | One logical mutation is committed at most once; tombstones cannot resurrect state; crash recovery observes either the old or new format, never a partial conversion | M1 variant green before delta-native persistence ships; paired runtime assertions and crash-at-every-boundary tests pass |
| v0.59.6 | Model shared-trace consumer frontiers, attachment, reclamation, and no premature GC | `formal/m2_frontier_agg.fizz` shared-consumer/trace-GC variant | Compaction never passes the slowest live consumer; attach-at-`F` has no gap or duplicate; trace reclamation begins only after the last consumer is safe | Shared-trace variant green before arrangement sharing ships; paired catalog, compaction, and reclamation assertions pass |
| v0.59.7 | Record that compile/deploy-time classic-versus-factorized selection adds no distributed lifecycle protocol | Existing M1/M2 models plus external-oracle equivalence; a dedicated cutover model is required if live replacement is later admitted | Classic and factorized execution remain equivalent at committed frontiers; no dual active production graph or switch generation exists in v1 | Oracle equivalence passes; path-coupling rejects any later live-cutover implementation without its dedicated model and crash-phase invariants |
| v0.59.8 | Extend M6 for bounded chunk copy, migration epochs, dual routing, frontier cutover, and delayed reclamation | `formal/m6_shard_migration.fizz` micro-migration variant | Existing M6 single-authority/no-loss invariants hold for every chunk and routing epoch; heat metadata survives reassignment | Extended M6 green before micro-migration ships; kill/restart at every copy/dual-route/cutover boundary replays as permanent regression cases |
| v0.59.9 | Extend M1 for logical visibility epochs versus physical commit groups; model checkpoint-mode transition only if unaligned fallback is admitted | `formal/m1_epoch_commit.fizz`; checkpoint transition variant when S5 admission evidence requires it | Commit grouping cannot expose a partial logical epoch or acknowledge non-durable state; aligned/unaligned transition cannot lose or duplicate in-flight data | Extended M1 green for the mandatory core; any admitted unaligned fallback is blocked on its checkpoint-transition model and paired runtime assertions |
| v0.23–v0.59.24 | Continuous `formal-verify` + path-coupling (DC.1–DC.2); pre-release relaxed-bounds sweep (DC.4) | all `.fizz` specs | all active base models: M1-M4, M6-M7, plus every admitted v0.59.x protocol variant | A coordination-protocol change without a model touch fails CI; the automated qualification suite re-runs the relaxed-bounds sweep against the candidate SHA |
| v0.80–v0.81 | M8 recursive-frontier model | `formal/m8_recursive_frontier.fizz` | No early fixed-point publication, no lost derived facts, idempotent iteration replay, safe reassignment, and bounded fallback | M8 and paired `SimRuntime` assertions pass before distributed or deletion-aware recursion becomes public |
| v0.82–v0.86 | Durable-timer model and merge-law fault suite | Timer ownership/firing model plus generated algebra checks | No duplicate or lost timer firing, no stale-owner firing, eventual backlog progress, and declared laws hold under replay and compaction | Timer model, paired runtime assertions, and false-law mutation tests pass before temporal predicates or merge laws become public |
| v0.90–v0.92 | M9 distributed-transaction model | `formal/m9_distributed_txn.fizz` | Atomic commit, one durable decision, no stale participant apply, idempotent recovery, bounded-failure liveness, and migration safety | M9, history checking, and crash-at-every-phase tests pass before `SERIALIZABLE` becomes public |
| v0.96–v0.102 | M10 region-failover model plus admitted active-active protocol variants | `formal/m10_region_failover.fizz` and each later coordination variant | One writer generation, no stale-region commit, safe ownership transfer, convergence where promised, and no silent consistency downgrade | Regional models, paired runtime assertions, partition histories, and resurrection tests pass before each regional mode becomes public |

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
| Phase 15.5 — Standard Wire Compatibility & the Real Incremental Serving Path | v0.51.1 – v0.51.6 | Continuous verification (M1–M4 regression re-proof; no new model) |
| Phase 15.6 — Standard-Client Reachability, Serving-Path Completeness & Honest Enterprise Enforcement | v0.51.7 – v0.51.16 | v0.51.14 is superseded and not released: its unshipped source surface is carried forward. New operational edge-case specs at v0.51.12 (quota exhaustion, source failure, object-store brownout, misconfiguration, late data); M7 lease-HA re-proven against real processes at v0.51.10; ad hoc query execution made genuinely multi-shard and cap-free at v0.51.13; durable source runtime added at v0.51.15; real Postgres CDC and HTTP webhook/push source connectors completed at v0.51.16 |
| Phase 15.7 — Post-v0.51.16 Hardening Review: Panic Safety, Arithmetic Safety, Lock Hygiene & Resource-Leak Soak | v0.51.17 – v0.51.20 | Continuous verification (no new coordination protocol; a fuzzing harness, a release-profile overflow-checks gate, a lock-poisoning containment chaos test, and a long-duration resource-leak soak are added) |
| Phase 15.8 — Post-v0.51.20 Substance Review: Real Connectors, Whole-Suite CI Reachability, Bounded IVM State & Async-Runtime Hygiene | v0.51.21 – v0.51.27 | Continuous verification (no new coordination protocol; real connector clients, whole-suite CI reachability, real operator state accounting, arrangement spill, async-runtime hygiene, leak-free registries, and honest failure semantics) |
| Phase 16 — Ingestion Failure Containment | v0.52 | Continuous verification (the M3 commit boundary is re-proven for the quarantine write path; no new model) |
| Phase 16.5 — View Lifecycle & Source Transaction Correctness | v0.52.1 – v0.52.2 | Continuous verification (no new coordination protocol; the M3 commit boundary is re-proven for the snapshot/delta fence and for whole-transaction CDC apply, whose atomicity is a consequence of the already-verified epoch commit rather than a second mechanism) |
| Phase 16.6 — Connector Surface Reduction | v0.52.3 – v0.52.5 | M5 cold-tier sink model retired at v0.52.4 (its implementation is deleted); M3 sink 2PC unchanged and re-proven against the single remaining sink; no new model |
| Phase 17 — Production Readiness & Qualification | v0.53 – v0.59.24 (including the v0.53.1–v0.59.24 sub-versions added by the 2026-08-12, 2026-08-18, and 2026-08-19 reviews) | Continuous verification + artifact-bound, no-skip automated qualification of RC1; CLI/configuration at v0.59.4; physical performance architecture and per-step benchmark evidence at v0.59.5–v0.59.9; product polish, diagnostics, executable documentation, and public-path proof closure through v0.59.18; SQL semantics and system-limits proof at v0.59.19; type-completeness proof at v0.59.20; lifecycle and deployment work through v0.59.22; capacity-estimator calibration at v0.59.23; and pure blocking horizontal scale-out and performance qualification at v0.59.24; optional extended soaks are supplemental only; v0.56 additionally proven by a `SimRuntime` mixed-version scenario |
| Post-v0.59.24 Part I — Typed semantics and feature delivery | v0.60 – v0.64 | Existing models plus capability-contract, public-reachability, typed-codec, migration, and raw-pgwire gates |
| Post-v0.59.24 Part II — Essential operator graduation | v0.65 – v0.69 | Existing commit, frontier, fencing, and migration models plus public differential qualification |
| Post-v0.59.24 Part III — Ordering and temporal analytics | v0.70 – v0.75 | Existing frontier models plus spill, amplification, recovery, and distributed equivalence proofs |
| Post-v0.59.24 Part IV — LATERAL and recursion | v0.76 – v0.81 | M8 recursive-frontier model for distributed and deletion-aware recursion |
| Post-v0.59.24 Part V — Durable time and algebra | v0.82 – v0.86 | Durable-timer ownership/firing model and merge-law fault suite |
| Post-v0.59.24 Part VI — Serializable transactions | v0.87 – v0.92 | M9 distributed-transaction model plus external serializability histories |
| Post-v0.59.24 Part VII — Multi-region | v0.93 – v0.102 | M10 regional-failover model plus every admitted active-active coordination variant |

One hundred twenty-three versions at about 6 person-weeks each cover the path
from an empty repository through v0.102. The order is fixed:
correctness on one shard is proven before distribution, distribution is made
fault-tolerant before the Postgres layer depends on it, ingestion connectors
and crucible soaks validate real-world pressure before HTAP ergonomics, the
Nexmark suite certifies end-to-end correctness (including retraction/Z-set semantics) on the full stack,
PostgreSQL wire protocol hardening (v0.37–v0.39) and end-user certification (v0.40–v0.42) ensure any standard driver works without workarounds and that a complete application can be built on the wire protocol alone,
and the data lake bridge (with its FizzBee pre-model and simulator fidelity foundation at v0.43), control-plane hardening and multi-tenancy (v0.45.1–v0.45.2), invariant and error-code compliance hardening (v0.45.6–v0.45.7), elastic shard migration and hot-key/skew handling (v0.46–v0.47), network optimizations, complex analytics, a live end-to-end wire-protocol pass that makes the serving path standard-PostgreSQL-honest and genuinely incremental (v0.51.1–v0.51.6), a standard-client-reachability and honest-enterprise-enforcement pass that lets a modern `psql` connect, compiles ordinary `int`/`text`/`float` aggregate views, enforces `--auth` and tenant quotas, ships real native ingestion, proves control-plane HA and edge-case recovery against real clusters, makes ad hoc query execution genuinely multi-shard and cap-free, and rounds out first-party ingestion with real Postgres CDC and HTTP webhook/push source connectors (v0.51.7–v0.51.14), and a durable
connector quarantine, a deliberate reduction of the external connector surface to two sources and one sink (v0.52.3–v0.52.5), an operator-grade CLI and arrangement debugger, explainable freshness, internal mTLS and secrets management, a proven rolling-upgrade/disaster-recovery story, and a written, machine-checked v1 contract establish the v0.59 technical preview. Evidence integrity, true end-to-end automated qualification, reproducible release engineering, security provenance, and contract reconciliation (v0.59.1–v0.59.3) come first; CLI/configuration usability at v0.59.4 then supplies the deterministic baseline surface. Delta-native state, durable shared arrangements, factorized and filtered IVM, shared-window and skew-aware execution, and the SLO-adaptive runtime/storage architecture (v0.59.5–v0.59.9) make the engine fast and scalable before product polish and proof closure (v0.59.10–v0.59.18), SQL semantics and type completeness (v0.59.19–v0.59.20), graceful lifecycle and deployment profiles (v0.59.21–v0.59.22), and capacity-estimator calibration against the final architecture (v0.59.23). Pure blocking horizontal scale-out and performance qualification at v0.59.24 proves the exact signed RC artifacts. Promotion to v1.0 remains unscheduled.

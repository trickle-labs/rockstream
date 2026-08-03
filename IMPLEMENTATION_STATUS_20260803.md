# RockStream — Implementation Status Report

**Date:** 2026-08-03
**Repository state:** `main` @ `cb9b9b3`, latest sign-off **v0.51.6**
(Session-State Bounding, Isolation Honesty & Aggregate Correctness)
**Method:** Static source review + targeted subagent code audits of the
third-party reviewer's six flagged areas + a **live end-to-end test of the
shipped `target/debug/rockstream` binary** driven over the PostgreSQL wire
protocol (both `psql` 18.4 and a hand-written protocol-3.0 client).

> This report is deliberately blunt about weaknesses. The stated goal is a
> system that works end-to-end and survives a nit-picking public audience
> using standard PostgreSQL tooling. It supersedes
> [IMPLEMENTATION_STATUS_20260719.md](IMPLEMENTATION_STATUS_20260719.md),
> which reviewed v0.50 before the v0.51.1–v0.51.6 serving-path work landed.
> **Read the Executive Summary and Top Blockers first.**

---

## Executive Summary

Since the 2026-07-19 review, the project shipped six point releases
(v0.51.1–v0.51.6) that **genuinely fixed the front-door blockers that review
called out**: implicit autocommit, the mandatory `SET
rockstream.idempotency_key` ritual, query-time `WHERE`/`JOIN`/`GROUP BY`,
no-column-list multi-row `INSERT`, gateway↔runtime unification onto one data
plane, TLS termination, binary wire format, bounded session state, and a
correct fractional `avg`. Those fixes are real and I verified several of them
live (see "What genuinely works").

**But the system is not currently demo-safe for a nit-picking public
audience, for two newly-verified, high-severity reasons:**

1. **The shipped gateway cannot be reached by the current default `psql`
   (v18) at all.** psql 18 negotiates PostgreSQL wire **protocol 3.2**; the
   gateway (pgwire 0.28) understands only 3.0 and, instead of replying with a
   `NegotiateProtocolVersion` downgrade, **silently closes the socket with
   zero bytes and no `ErrorResponse`.** A reviewer who runs `brew install
   libpq && psql …` in 2026 gets *"server closed the connection
   unexpectedly"* and never sees a single row. **Verified live and reduced to
   a one-line repro** (protocol 3.2 startup → immediate EOF).

2. **The "true incremental serving path" (v0.51.4) only compiles a narrow
   subset of aggregate shapes.** The single most basic streaming query —
   `CREATE MATERIALIZED VIEW mv AS SELECT k, SUM(v) FROM t GROUP BY k` over
   ordinary `int` columns — **is rejected at CREATE time with `RS-1019` /
   `RS-1013`** because the fast-path `AggregateOp` "only supports Int64
   keys/values". The old batch `view_materializer` that handled arbitrary
   types was **deleted** in v0.51.4, and the general `DiffCtx`/`OpNode`
   physical-plan path the error message points at **is not wired into the
   serving path**. This is a **functional regression** from the exact
   `SELECT product, SUM(qty) … GROUP BY product` transcript the 2026-07-19
   report certified as working.

So the honest one-line status is: **the engine still computes correct
incremental answers over the wire for the shapes it supports, and the front
door is far more standards-compliant than in July — but a modern `psql` can't
connect, and the most ordinary `GROUP BY`/`SUM` materialized view over `int`
columns no longer compiles.** Both are the first things a skeptical reviewer
will hit.

Separately, the third-party reviewer's six structural concerns (control-plane
HA, multi-tenancy, diagnostics, native connectors, validation specs,
enterprise validation) are **substantially accurate**: several are roadmap
entries marked `✅ Done` whose *internal mechanisms* work but whose
*user-reachable surface* or *distributed enforcement* is missing, and the
enterprise-validation milestones (v0.52–v0.59, including the v1.0 RC) **do not
exist as signed-off deliverables**.

---

## What genuinely works now (verified live, protocol 3.0)

The v0.51.1–v0.51.6 work is real. Driving the shipped binary
(`rockstream start --role all`) over a protocol-3.0 client:

| July 2026 blocker | Status now | Evidence |
|---|---|---|
| Autocommit doesn't persist writes | ✅ **Fixed** | `INSERT INTO t VALUES (1,'alice')` then `SELECT * FROM t` returns the row with **no** explicit `COMMIT` and **no** `SET idempotency_key`. |
| Mandatory `SET rockstream.idempotency_key` | ✅ **Fixed** | Writes succeed with no session `SET`; the gateway auto-generates the envelope. |
| Query-time `WHERE` ignored | ✅ **Fixed** | `SELECT * FROM t WHERE id = 2` returns only the matching row. |
| No-column-list multi-row `INSERT` corrupts rows | ✅ **Fixed** | `INSERT INTO s VALUES ('apple',10),('apple',5),('pear',3)` → `INSERT 0 3`, no NULL phantom row. |
| Query-time aggregate | ✅ Works | `SELECT avg(qty) FROM s` returns a value via DataFusion. |
| Two disconnected data planes | ✅ **Unified** (v0.51.3) | `--role all` opens a single shared `ShardDb`; pgwire reads the worker's `ViewSink`. |
| SSL always refused (`'N'`) | ⚠️ **Partially fixed** | TLS termination exists (v0.51.5) **when a cert is configured**; with no cert the gateway still answers `SSLRequest` with `'N'` (plaintext), which is the default in `--role all`. |
| Truncating integer `avg` | ✅ **Fixed** (v0.51.6) | `avg` now encodes `Float64`. |

The underlying IVM engine, SlateDB durability, the broad pgwire surface, and
the FizzBee-verified core protocols remain real strengths, exactly as the
July report described.

---

## Top Blockers (must fix before a public demo with standard tooling)

### 1. Modern `psql` (protocol 3.2) cannot connect — silent socket close
**Severity: critical.** psql 18.4 (current Homebrew default) opens with wire
protocol **3.2**. The gateway wraps `pgwire = "0.28"`, which handles only
protocol 3.0, and there is **no `NegotiateProtocolVersion` handling anywhere**
in `crates/rockstream-gateway/src/` (grep: zero matches for
`NegotiateProtocol`/`protocol_version`). Instead of downgrading, the
connection is dropped.

Verified live:
- `psql "…sslmode=disable" -c "SELECT 1"` → *"server closed the connection
  unexpectedly."*
- Hand-written startup with **protocol 3.0 (196608)** → full handshake
  (`AuthenticationOk`, `ParameterStatus`×7, `BackendKeyData`,
  `ReadyForQuery`) and correct query results.
- Hand-written startup with **protocol 3.2 (196610)** → **immediate EOF, 0
  bytes, no `ErrorResponse`.**

Impact: a nit-picking user on any current PostgreSQL client library (libpq
≥ 18, and increasingly others) cannot connect at all, and gets no diagnostic.
Fix: implement `NegotiateProtocolVersion` (reply advertising 3.0 and echoing
unsupported `_pq_.*` params), or upgrade/patch the pgwire layer to negotiate.

### 2. Basic `GROUP BY` / `SUM` materialized views are rejected at CREATE
**Severity: critical (regression).** The v0.51.4 serving path routes every
`CREATE (MATERIALIZED) VIEW` through the direct fast-path compiler
(`compile_plan`). That compiler's `AggregateOp` **only supports `Int64`
keys/values**. Verified live, all three rejected with `RS-1019 →
RS-1013`:

```sql
CREATE TABLE o (cust int, amt int);
INSERT INTO o VALUES (1,10),(1,5),(2,3);
CREATE MATERIALIZED VIEW mv_int AS
  SELECT cust, SUM(amt) total FROM o GROUP BY cust;
-- ERROR: [RS-1019] view.compile_failed … [RS-1013] Plan node not supported by
-- the direct operator compiler: Aggregate over a non-Int64 group-by or
-- aggregate-value expression (AggregateOp only supports Int64 keys/values);
-- … this query shape requires the DiffCtx/OpNode physical-plan path, not the
-- v0.51.3 fast-path compiler.
```

`SELECT product, SUM(qty) … GROUP BY product` (text key) and
`… AVG(qty) …` are rejected for the same reason. A standard SQL `int` maps to
Arrow `Int32`, not `Int64`, so a user must declare **every** column `BIGINT`
to get a working aggregate view — an undocumented, non-obvious constraint.
The Nexmark test corpus passes only because it uses `BIGINT` keys throughout.

This regressed from July: the deleted batch `view_materializer` handled
arbitrary types via DataFusion; the `DiffCtx`/`OpNode` path the error points
at exists in the engine but **is not reachable from the gateway**.

### 3. `SELECT` from a non-existent / failed-to-create view returns empty `OK`
**Severity: high (correctness/honesty).** After the failed `CREATE` above,
`SELECT * FROM mv_int` returns a bare `CommandComplete` with **no rows and no
error** — the relation does not exist, yet the query silently "succeeds". A
reviewer cannot distinguish "view is empty" from "view was never created."
This is the same class of silent-empty-result surprise the July report
flagged, now surfaced via failed DDL.

### 4. The `--auth` flag is silently ignored (runs open)
**Severity: high (security).** `rockstream start --role gateway|all
--auth scram|md5|oidc|mtls` parses the flag into `StartOptions.auth_mode`,
but `crates/rockstream-cli/src/lib.rs` **never reads it** —
`start_gateway_with_shard` constructs the server via
`GatewayServer::with_shard_db(...)`, which hardcodes `AuthMode::Off`
(`crates/rockstream-gateway/src/server.rs:1494`). Every connection becomes
`Principal::System` regardless of the configured mode. The `*_and_auth` /
`*_and_mtls_auth` constructors that would honor it are never called from the
CLI. A "secured" deployment is, in fact, unauthenticated, with no warning.

---

## Assessment of the third-party reviewer's six concerns

The reviewer's screenshot flags six areas. Each was audited against the code.
Verdicts below distinguish **internal mechanism** (does the code work when
called from a Rust test?) from **user reachability / distributed
enforcement** (can a public user or operator actually rely on it?).

### A. Control Plane High Availability — *real election, but lease state is a SPOF*
Roadmap: v0.45.2 "Control-Plane HA (Raft-Elected Writer Lease)" `✅ Done`.

- **Real:** `crates/rockstream-control/src/raft.rs` implements genuine Raft
  leader election with randomized timeouts, `RequestVote`, majority voting,
  and **durable** term/vote via `RaftPersistentStore`; `RaftHandle::
  require_leader()` gates writes; the CLI exposes `--raft-peers`,
  `--raft-node-id`, `--raft-bind`, `--raft-bootstrap`; a FizzBee model
  (`formal/m7_control_plane_ha.fizz`) exists.
- **Gap:** **shard-lease grants live only in an in-memory `HashMap` in
  `ShardManager`** and are **not durably replicated across control nodes.** A
  new leader re-indexes leases from a shared store but in-flight grants are
  not consensus-replicated. The failover proof in `sign-offs/v0.45.2.md` is a
  `SimRuntime` scenario, **not** a real multi-process TestContainers drill
  that kills a live leader and observes a follower take over shard-lease
  authority.
- **Verdict:** (b) real but limited — **a single control node remains the SPOF
  for shard-lease state.** A nit-picking audience will ask to see a real
  leader kill with continuity of lease authority; that proof does not exist.

### B. Multi-Tenancy / Resource Isolation — *accounting & admission, not enforcement*
Roadmap: v0.45.1 "Workload Quotas, Priority Admission Control &
Multi-Tenancy" `✅ Done`.

- **Real:** `rockstream-types::workload` (`WorkloadDef` with `memory_limit`,
  `freshness_slo`, `max_parallelism`, `priority`); `CREATE/ALTER/DROP
  WORKLOAD` and `SHOW WORKLOAD STATUS` over pgwire; an `AdmissionController`
  that **pauses** lower-priority views under contention.
- **Gap:** quota checks live in the **gateway** (`catalog_stubs.rs`
  `budget.try_acquire`), **not in the runtime operators.** Workers never call
  a quota ledger before allocating; an operator can allocate far beyond a
  workload's `MEMORY_LIMIT` with no rejection. Enforcement is **reactive**
  (mark a view `OverBudgetRelaxed` after the fact), never **prospective**
  ("this batch would exceed quota, reject"). All tests are single-shard
  co-located gateway+worker; there is **no distributed quota coordination
  across real worker processes.**
- **Verdict:** (c) largely metadata/accounting — **a worker can violate a
  tenant's quota without any runtime check.** True isolation (a noisy tenant
  cannot starve another) is not demonstrated.

### C. Diagnostic Tools / Operator Observability — *SQL-level yes, operator-level no*
Roadmap: the full operator CLI + IVM arrangement debugger is **v0.55, not
started** (no `sign-offs/v0.55.md`).

- **Real:** a Prometheus `/metrics` endpoint (`--metrics-addr`);
  `EXPLAIN INCREMENTAL` (VERBOSE/ANALYZE/ESTIMATE) and `SHOW RESOURCE USAGE
  [FOR WORKLOAD] [CLUSTER]` over pgwire.
- **Gap:** the `rockstream` CLI still exposes **only `start` and `cluster
  workers drain`** — no `view`/`workload`/`source`/`schema`/`audit`/`support
  bundle`/`debug arrangement` subcommands. There is **no IVM arrangement
  debugger** (the tool for "the view has a *wrong* answer"), and **no
  pipeline-stall diagnostics** ("why isn't epoch N advancing?" — the
  reviewer's exact concern). `EXPLAIN INCREMENTAL ANALYZE` calls
  `collect_operator_stats(0)` with a **hardcoded `0`** against an empty
  `TopologyCatalog`, so per-operator timings come back empty.
- **Verdict:** an operator can scrape metrics and read a plan, but **cannot
  debug a stalled or wrong view** without reading Rust source. Matches the
  reviewer's "operator-facing observability remains weak."

### D. Native Ingestion Connectors — *mocks, and not user-reachable*
Roadmap: v0.28 "First-Party Source Connectors" (Kafka + S3) `✅ Done`;
v0.21 exactly-once sinks; v0.44 cold-tier Iceberg/Delta sinks.

- **Gap (severe):** `crates/rockstream-connectors/src/kafka_source.rs` and
  `s3_source.rs` are labelled **"Mock … source connector"** in their own
  first line; `kafka_sink.rs` says "In production this would wrap a real
  Kafka producer (e.g. `rdkafka`)." The crate's `Cargo.toml` has **no
  `rdkafka`, no `aws-sdk`, no Postgres-CDC dependency** (verified). There is
  **no `CREATE SOURCE` DDL in the gateway** (grep: 0 matches) — a user
  **cannot register a Kafka/S3 source over SQL at all.** No real-Kafka
  TestContainers proof exists (S3 has a MinIO test; Kafka does not).
- **Real:** cold-tier **sinks** (Iceberg/Delta via `object_store`/`iceberg`/
  `deltalake`) and `CREATE SINK` are genuinely implemented and MinIO-tested;
  the exactly-once sink 2PC state machine is real (but tested against an
  in-memory mock, not a broker).
- **Verdict:** exactly matches the reviewer — **"the system relies on SQL
  interfaces for ingestion."** Native streaming ingestion is a mock, not a
  shippable feature.

### E. Validation Specifications — *happy-path protocols modeled; edge cases prose-only*
- **Real:** FizzBee models `formal/m1…m7` cover the core coordination
  protocols; `scripts/check-invariant-pairs.sh` enforces every FizzBee
  invariant has a paired runtime `assert!`; `check-error-codes.sh` enforces
  RS-XXXX coverage; the `error_code` registry (~75 codes) is machine-readable.
- **Gap:** the models cover **happy-path core protocols only.** Operational
  edge cases the reviewer names — quota exhaustion, connector-source failure,
  object-store brownout with buffer exhaustion, misconfiguration rejection,
  late-arriving data — live **only in DESIGN.md prose**, with no
  machine-checkable model. The error-code registry is a lookup table, **not a
  behavioral spec** of *when* each code fires or *how* to recover. The
  `formal-verify` CI job only triggers on changes to `formal/`, `DESIGN.md`,
  or specific crates, so most PRs skip it.
- **Verdict:** the *reviewer is right* — the spec surface must expand to cover
  operational edge cases, not just the M1–M7 core.

### F. Enterprise Validation — *simulation, not sustained real-cluster chaos*
Roadmap: v0.57 Rolling Upgrade/DR, v0.58 Simulator Maturity, v0.59
"v1.0 RC1 — 2-week continuous chaos cycle."

- **Fact:** the latest sign-off is **v0.51.6**. **v0.52 through v0.59 have no
  sign-off files** — including v0.59, the v1.0 RC with the "2-week continuous
  chaos cycle." They are roadmap entries, not verified deliverables.
- **Gap:** the CI "soak" (`.github/workflows/simulation-soak.yml`, 6-hourly)
  runs `cargo test -p rockstream-sim --test chaos_tests` — a **fast,
  in-process deterministic simulation** (100k seeds, completes in
  seconds–minutes). Names like `thirty_two_shard_24h` denote **simulated**
  wall-clock, not real elapsed time. There is **no** TestContainers
  multi-worker chaos job, **no** real Kafka/S3 under sustained pressure, **no**
  rolling-upgrade drill, and **no** disaster-recovery drill. The performance
  CI gates are **regression** gates (did we get slower vs. baseline?), not
  **absolute SLO** validation.
- **Verdict:** exactly the reviewer's point — **final performance testing and
  continuous chaos soaks have not been completed.** Simulation coverage is
  strong; real-cluster enterprise validation is absent.

---

## Secondary gaps and weaknesses

| Area | Status | Detail |
|---|---|---|
| **pgwire protocol negotiation** | ❌ Missing | No `NegotiateProtocolVersion`; incompatible with libpq ≥ 18 (Blocker 1). |
| **Serving-path aggregate coverage** | ❌ Narrow | Fast-path `AggregateOp` is Int64-only; `DiffCtx`/`OpNode` general path not wired to the gateway (Blocker 2). |
| **`--auth` flag** | ❌ Ignored | Gateway always runs `AuthMode::Off` (Blocker 4). |
| **Failed-DDL query semantics** | ⚠️ Silent | `SELECT` from a never-created view returns empty `OK` (Blocker 3). |
| **`avg` display scale** | ⚠️ Minor | `avg` is now `Float64` internally, but exact-integer means render as `6` (PG renders `6.0000000000000000`). |
| **Native sources** | ❌ Mock | No real Kafka/S3 client, no `CREATE SOURCE` (Area D). |
| **Operator diagnostics** | ⚠️ Stub | `EXPLAIN INCREMENTAL ANALYZE` op-stats hardcoded to `0`; no arrangement debugger (Area C). |
| **Distributed quota enforcement** | ⚠️ Gateway-only | Workers don't enforce quotas (Area B). |
| **Control-plane lease HA** | ⚠️ SPOF | Lease grants not consensus-replicated (Area A). |

---

## Reproduction notes (for verification by others)

```bash
cargo build --bin rockstream
rm -rf /tmp/rs && mkdir -p /tmp/rs
./target/debug/rockstream start --storage /tmp/rs --role all \
  --listen 127.0.0.1:5459 --auth off &

# BLOCKER 1 — modern psql cannot connect:
psql "host=127.0.0.1 port=5459 user=rockstream dbname=test sslmode=disable" \
  -c "SELECT 1"
#   psql: error: server closed the connection unexpectedly     (psql 18.x)
# A hand-written protocol-3.0 startup, by contrast, completes the handshake
# and returns rows. A protocol-3.2 (196610) startup gets an immediate 0-byte
# EOF with no ErrorResponse.

# BLOCKER 2 — basic aggregate MV rejected (use a protocol-3.0 client):
#   CREATE TABLE o (cust int, amt int);
#   INSERT INTO o VALUES (1,10),(1,5),(2,3);
#   CREATE MATERIALIZED VIEW mv AS SELECT cust, SUM(amt) FROM o GROUP BY cust;
#   -> ERROR [RS-1019]/[RS-1013] AggregateOp only supports Int64 keys/values
#   SELECT * FROM mv;   -> bare "OK", no rows, no error (Blocker 3)

# BLOCKER 4 — auth flag ignored:
./target/debug/rockstream start --storage /tmp/rs --role gateway \
  --listen 127.0.0.1:5460 --auth scram &
# still accepts every connection as Principal::System (AuthMode::Off).
```

*(The July report's `psql`-driven transcript could not be re-run directly
here because Blocker 1 prevents psql 18 from connecting; the positive results
above were confirmed with a protocol-3.0 wire client.)*

---

## Recommended priorities to reach a clean end-to-end public demo

1. **Speak protocol 3.2 (or negotiate down).** Implement
   `NegotiateProtocolVersion` so current `psql`/libpq can connect. Nothing
   else matters until a stock client can reach the server. *(Blocker 1)*
2. **Wire the general `DiffCtx`/`OpNode` path into the serving compiler, or
   widen the fast path to non-`Int64` keys/values**, so ordinary
   `SELECT k, SUM(v) FROM t GROUP BY k` materialized views compile. Add a
   serving-path test matrix over `int`/`text`/`float` keys and
   `SUM`/`AVG`/`COUNT`. *(Blocker 2)*
3. **Make failed/absent relations error on `SELECT`** instead of returning
   empty `OK`. *(Blocker 3)*
4. **Honor `--auth` in the CLI** (route `opts.auth_mode` to the matching
   `GatewayServer` constructor) and fail closed on an unknown mode.
   *(Blocker 4)*
5. **Ship at least one real native source connector** (`rdkafka` or
   `aws-sdk-s3`) behind real `CREATE SOURCE` DDL, with a real-broker
   TestContainers exactly-once proof — or **stop advertising v0.28 as
   "First-Party Source Connectors" and reposition it honestly.** *(Area D)*
6. **Add operator-level diagnostics**: fix `EXPLAIN INCREMENTAL ANALYZE`
   op-stats, and ship a minimal arrangement/stall inspector so an operator can
   answer "why is this view stuck/wrong?". *(Area C)*
7. **Prove distributed enforcement** for quotas (worker-side) and
   control-plane lease HA (real multi-process leader kill), not just
   `SimRuntime`. *(Areas A, B)*
8. **Run one real sustained chaos + performance soak** on a multi-worker
   TestContainers cluster with real MinIO/Kafka, and publish the numbers,
   before claiming a v1.0 RC. *(Area F)*

Closing items 1–4 restores the "a skeptical user can drive it with a standard
client" property the v0.51.x work was aiming for; items 5–8 close the
structural gaps the third-party reviewer correctly identified.

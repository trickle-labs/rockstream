# RockStream — Implementation Status Report

**Date:** 2026-07-19
**Repository state:** `main`, latest sign-off **v0.50** (Advanced Streaming Analytics)
**Method:** Static source review + a **live end-to-end test of the shipped
`rockstream` binary** driven through a real `psql` client over the PostgreSQL
wire protocol.

> This report is deliberately blunt about weaknesses. The stated goal is a
> system that works end-to-end and survives a nit-picking public audience using
> standard PostgreSQL tooling. Read the **Executive Summary** and the **Top
> Blockers** first; they are the difference between a clean public demo and an
> embarrassing one.

---

## Executive Summary

RockStream is a large, genuinely impressive codebase: real object-storage
durability (SlateDB), a broad PostgreSQL wire surface, a well-tested incremental
view-maintenance (IVM) operator library, and formal (FizzBee) verification of
its core protocols. **Under the hood, the individual building blocks are real
and mostly work.**

However, **the end-to-end user experience over the wire protocol does not yet
work "out of the box" for a standard PostgreSQL client.** A vanilla user who
connects with `psql` (or any BI tool / ORM) and runs the most natural possible
sequence —

```sql
CREATE TABLE t (id int, name text);
INSERT INTO t VALUES (1, 'alice');
SELECT * FROM t;          --  <-- returns 0 rows
```

— gets **nothing back, with no error**. Two non-obvious, non-standard
requirements must be satisfied for any write to become visible:

1. The client must issue a **non-standard `SET rockstream.idempotency_key = '…'`
   before every write**, otherwise the commit is rejected (`RS-2007`) and the
   buffered rows are silently discarded.
2. The client must issue an **explicit `COMMIT`**. Writes are *buffered* and are
   **not auto-committed**, so psql's default autocommit mode (which every
   standard client uses) never persists anything.

Once a client follows RockStream's private write ritual, the pipeline **does**
work end to end and produces **correct** results, including aggregating
materialized views:

```sql
CREATE TABLE s (product text, qty int);
SET rockstream.idempotency_key = 'k1';
INSERT INTO s (product, qty) VALUES ('apple', 10);
INSERT INTO s (product, qty) VALUES ('apple', 5);
INSERT INTO s (product, qty) VALUES ('pear', 3);
COMMIT;
CREATE MATERIALIZED VIEW mv AS
  SELECT product, SUM(qty) AS total FROM s GROUP BY product;
SET rockstream.idempotency_key = 'k2';
INSERT INTO s (product, qty) VALUES ('apple', 100);
COMMIT;
SELECT * FROM mv ORDER BY product;
--  product | total
-- ---------+-------
--  apple   |   115     <-- correct: 10 + 5 + 100
--  pear    |     3     <-- correct
```

So the honest one-line status is: **the engine is real and computes correct
answers, but the serving layer speaks a dialect of PostgreSQL that standard
clients do not, and the "incremental" story is not the one that actually runs in
the serving path.**

---

## How the three requested capabilities actually behave

| Capability | Works? | Notes |
|---|---|---|
| **Data stored in object storage** | ✅ Yes | SlateDB is a real dependency and durably persists to a real object store (local filesystem verified live; S3/MinIO verified in tests). |
| **End-user queries executed and returned** | ⚠️ Partial | Works for `SELECT [cols] FROM <view/table> [ORDER BY] [LIMIT]`. **Query-time `WHERE`, `JOIN`, `GROUP BY`, subqueries, and CTEs are ignored/unsupported** in the read path. |
| **End-user materialized views updated** | ⚠️ Partial | Views *are* refreshed on commit and produce **correct aggregates/joins** — but via **full DataFusion batch recompute, not incrementally**, and only after a *subsequent* commit (no initial population at `CREATE`). |
| **100% over the PostgreSQL wire protocol** | ❌ No | Autocommit is broken and a non-standard `SET` is mandatory for writes; a standard client cannot round-trip data without app changes. |

---

## Top Blockers (must fix before a public demo with standard tooling)

### 1. Autocommit does not persist writes (violates PostgreSQL semantics)
Writes are buffered and only flushed by an **explicit `COMMIT`**. In PostgreSQL,
every statement autocommits by default. Verified live:

```sql
SET rockstream.idempotency_key = 'c1';
INSERT INTO c VALUES (7);   -- reports "INSERT 0 1"
SELECT * FROM c;            -- returns 0 rows
```

Impact: a nit-picking user running `psql -c "INSERT …"` followed by
`psql -c "SELECT …"` (two autocommitting connections) sees an empty table and no
error. Every BI tool, ORM, and driver defaults to autocommit.

### 2. Mandatory non-standard `SET rockstream.idempotency_key` before every write
Source: `crates/rockstream-gateway/src/server.rs` (commit path, ~line 3570). If
neither `rockstream.idempotency_key` nor `rockstream.source_epoch` is set, the
commit returns `RS-2007` and the write buffer is cleared — **the write is
dropped while the `INSERT` still reported success**. No stock PostgreSQL client
emits this `SET`.

### 3. `CREATE MATERIALIZED VIEW` performs no initial population
The view is **empty until the next commit touches one of its source tables**.
Verified: querying a freshly created materialized view returns 0 rows even
though its source table already has data. Standard `CREATE MATERIALIZED VIEW …
AS SELECT …` in PostgreSQL populates immediately.

### 4. Query-time `WHERE` / `JOIN` / `GROUP BY` on a `SELECT` are ignored
Read path: `read_view_response` (`server.rs` ~line 2705) applies only column
projection, `ORDER BY`, and `LIMIT`. A predicate such as
`SELECT * FROM v WHERE region = 'US'` returns **all** rows (verified live). Joins
and aggregates only take effect inside a **view definition** (evaluated by
DataFusion at materialization time), never at query time.

### 5. Multi-row `VALUES` without an explicit column list can corrupt/lose rows
`INSERT INTO t VALUES (3,'carol'),(4,'dave')` (no column list) produced a single
all-`NULL` phantom row in live testing, whereas
`INSERT INTO t (a, b) VALUES …` works correctly. The no-column-list path needs
fixing (or must hard-error rather than silently corrupt).

---

## Architectural finding: two disconnected data planes

This is the most important structural observation and directly undercuts the
project's headline "incremental view maintenance" claim **for the serving
path**.

- The gateway crate's non-dev dependencies are **only** `rockstream-types`,
  `rockstream-storage`, `rockstream-sql`, and `rockstream-control`
  (`crates/rockstream-gateway/Cargo.toml`). It does **not** depend on
  `rockstream-runtime`, `rockstream-ops`, or `rockstream-diff` — the crates that
  contain the real differential-dataflow IVM engine.
- Running `rockstream start --role all` opens **two independent SlateDB
  databases**:
  - a **runtime worker** shard (`db`) — the worker registers with the control
    plane and acquires "shard 1 lease *to demonstrate fencing setup*"
    (`crates/rockstream-cli/src/lib.rs`), and
  - a **separate gateway** shard (`gateway`) that actually serves psql clients.
  These two planes are **not connected**. DML written through psql lands only in
  the gateway shard.
- The gateway serves views through
  `crates/rockstream-gateway/src/view_materializer.rs`, whose own module doc
  states it is a **"batch re-evaluation on every commit — correct and simple,
  not incremental."** On each commit it scans the *entire* source table from
  object storage, runs the view SQL through DataFusion's in-memory engine, and
  overwrites the view output.

**Consequence:** the sophisticated, well-tested IVM operator library
(`rockstream-ops`: Z-set deltas, DBSP bilinear joins, incremental aggregates,
windows, recursion) is exercised in the `rockstream-oracle`/`rockstream-sql`
**test harnesses**, but is **not on the live pgwire serving path**. The
"incremental" value proposition is currently proven in tests, not delivered to
the psql user.

### Related smell: a hardcoded benchmark query
`view_materializer.rs` contains a **hardcoded exact-string match** for one
specific Nexmark SESSION-window query (`rewrite_session_sql`, ~line 175) that
rewrites it into a windowed CTE. A nit-picking reviewer will find this
immediately; it suggests at least one benchmark result is served by a
special-case rather than the general engine.

---

## What is genuinely strong

These are real strengths and should be highlighted (accurately) in any public
presentation.

### Object storage & durability — real, not a stub
- **SlateDB** (`slatedb = "0.13"`) is a real dependency; `ShardDb`
  (`crates/rockstream-storage/src/shard_db.rs`) wraps `slatedb::Db` with atomic
  write batches, monotonic-epoch enforcement, and durable checkpoints
  (`CheckpointScope::Durable`).
- Multiple object-store backends are wired: **S3 / S3Express, local filesystem,
  and in-memory** (`crates/rockstream-storage/src/tiered_store.rs`), with a
  tiered hot/cold routing layer. MinIO/TestContainers tests exercise real
  S3-compatible durability.
- **Recovery from object storage** is implemented: `RecoveryDriver`
  (`crates/rockstream-runtime/src/recovery.rs`) reopens shards pinned to a
  checkpoint manifest and performs a fenced reader→writer transition.

### PostgreSQL wire protocol surface — broad and mostly solid
- Startup + authentication: **SCRAM-SHA-256, MD5, OIDC/JWT, mTLS, and off**
  modes all implemented (`crates/rockstream-gateway/src/auth.rs`).
- **Simple and extended query** protocols (Parse/Bind/Execute/Describe/Close,
  portals, prepared-statement caches, parameter type inference).
- **COPY … FROM STDIN / TO STDOUT**, **LISTEN/NOTIFY/UNLISTEN**, **named
  cursors** (DECLARE/FETCH/MOVE/CLOSE), **transactions and savepoints**
  (BEGIN/COMMIT/ROLLBACK/SAVEPOINT/RELEASE/ROLLBACK TO), and query
  **cancellation**.
- A wide Postgres **OID/type map** (int2/4/8, float4/8, text/varchar, bool,
  timestamp(tz), date, time, uuid, numeric, json/jsonb, interval, and several
  array types).

### Incremental view-maintenance engine (in isolation) — real and well-tested
- A full operator catalog in `rockstream-ops`: **filter/project/map, aggregate
  (with retractions), inner/outer joins (DBSP bilinear rule), min/max, distinct/
  intersect/except, tumble/hop/session windows, top-k, lateral/UNNEST,
  recursion, secondary-index arrange**. The Z-set delta calculus (including
  retractions) is implemented and unit-tested.
- **100 / 100** gateway integration proof tests pass
  (`gateway_proof_tests`), demonstrating correct in-harness INSERT→COMMIT→SELECT
  round-trips and view serving when the shard is shared.

### Formal verification & test discipline
- FizzBee models (`formal/`) for epoch commit, frontier aggregation, sink 2PC,
  self-fencing, cold-tier sink, shard migration, and control-plane HA, hard-gated
  in CI (per prior remediation).
- Large, per-crate test suites and a documented "common definition of done."

---

## Secondary gaps and weaknesses

| Area | Status | Detail |
|---|---|---|
| **SSL/TLS** | ❌ Stub | `SSLRequest` is always answered with `'N'` — connections are plaintext; no TLS negotiation server-side. |
| **Binary wire format** | ⚠️ Text only | Result columns are always encoded as text even when a client requests binary; silently downgraded. |
| **`EXPLAIN` / `EXPLAIN INCREMENTAL`** | ⚠️ Stub/partial | Returns annotation text, not a real executed plan tree; the rich `explain_incremental` library exists but is not faithfully surfaced. |
| **`CREATE INDEX`** | ⚠️ Metadata-only | Catalog tracks `BUILDING→READY`, but no index is built and queries never use one. |
| **Isolation / 2PC** | ⚠️ Limited | `SERIALIZABLE` rejected (`RS-2003`); `REPEATABLE READ` accepted but not enforced; `PREPARE TRANSACTION`/`COMMIT PREPARED` unsupported (`RS-2404`). |
| **Distributed exchange** | ⚠️ Loopback | `rockstream-diff` exchange is a loopback stub; no real inter-shard data movement in the diff path. |
| **`avg` aggregate** | ⚠️ Truncates | Integer division truncates rather than rounds. |
| **Admin CLI** | ⚠️ Minimal | `rockstream` exposes `start` and `cluster workers drain`; no `explain`/`audit`/`support bundle` subcommands despite historical doc claims. |
| **Session state** | ⚠️ Unbounded | Prepared statements/portals cached per connection with no LRU eviction; abnormal disconnects can leak state. |

---

## Reproduction notes (for verification by others)

```bash
cargo build --bin rockstream
rm -rf /tmp/rs && mkdir -p /tmp/rs
./target/debug/rockstream start --storage /tmp/rs --role gateway --listen 127.0.0.1:5439 &

# FAILS (vanilla autocommit) — returns 0 rows, no error:
psql -h 127.0.0.1 -p 5439 -U rockstream -d test \
  -c "CREATE TABLE t (id int, name text);" \
  -c "INSERT INTO t VALUES (1,'alice');" \
  -c "SELECT * FROM t;"

# WORKS (RockStream's required ritual):
psql -h 127.0.0.1 -p 5439 -U rockstream -d test <<'SQL'
CREATE TABLE ord (id bigint, amount bigint);
SET rockstream.idempotency_key = 'ck1';
INSERT INTO ord (id, amount) VALUES (42, 99);
INSERT INTO ord (id, amount) VALUES (43, 100);
COMMIT;
SELECT * FROM ord ORDER BY id;   -- returns the two rows correctly
SQL
```

---

## Recommended priorities to reach a clean end-to-end public demo

1. **Make autocommit persist writes** the way PostgreSQL does (implicit commit
   per statement outside an explicit transaction block). This single change
   removes the biggest surprise.
2. **Stop requiring `SET rockstream.idempotency_key` for interactive writes.**
   Auto-generate an idempotency key server-side when the client does not supply
   one; reserve the explicit key for clients that need exactly-once semantics.
3. **Populate materialized views at `CREATE` time** (run one materialization
   pass immediately), matching PostgreSQL behavior.
4. **Honor query-time `WHERE`/`JOIN`/`GROUP BY`** — either by pushing them into
   DataFusion at read time or by documenting the exact supported grammar loudly.
5. **Wire the real IVM engine (or a clearly-scoped subset) into the serving
   path**, or explicitly reposition the current serving layer as
   "batch-recompute today, incremental next" — and **remove the hardcoded
   Nexmark query rewrite**.
6. **Terminate TLS** (accept `SSLRequest`) so the wire is not plaintext.
7. Fix the **no-column-list multi-row `INSERT`** path (correct it or hard-error).

Closing these — especially items 1–3 — converts RockStream from "impressive
internals with a fragile front door" into a system a skeptical audience can
drive with `psql` and standard tools without prior knowledge of its private
protocol.

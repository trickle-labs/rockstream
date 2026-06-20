# RockStream in 30 Minutes: From Theory to a Running Engine

Welcome. The next half hour takes you from *never having heard of incremental
view maintenance* to *running the RockStream engine on your own machine and
watching its correctness proofs execute in front of you*.

This is a hands-on tutorial, and every command in it really runs. That matters,
because RockStream is a young project in the middle of an honest, evidence-driven
build-out. Rather than show you a glossy demo of features that don't exist yet,
this guide does two things:

1. **Explains the ideas** that make RockStream different — incremental view
   maintenance, Z-sets, frontiers, merge laws, and bottomless cloud state.
2. **Lets you reproduce reality.** You will build the workspace, boot a node,
   read the audit trail and support bundle it writes, and then watch the actual
   IVM engine, SQL compiler, and Postgres wire gateway prove themselves through
   the project's test harness.

> **Where the project is right now.** RockStream is built version by version,
> and each version is "done" only when its proof is done (see
> [NEW_ROADMAP.md](../NEW_ROADMAP.md)). The incremental engine, the SQL
> frontend, multi-shard execution, fault tolerance, and the PostgreSQL wire
> gateway are all implemented and proven by tests. The single `rockstream`
> binary today boots a real node — control plane, worker registration, shard
> leasing and fencing, and a SlateDB store — and runs an embedded *no-op*
> pipeline end to end. The SQL-over-Postgres serving path is wired and verified
> in the test harness rather than exposed as a long-running server command. This
> tutorial is precise about which is which, so nothing here will surprise you.

---

## Part 1 — Why RockStream Exists

Before any commands, it helps to understand *why* RockStream thinks about data
the way it does. If you come from a traditional relational database (PostgreSQL,
MySQL) or a batch warehouse (Snowflake, BigQuery, Redshift), you are used to a
**pull-based, batch-oriented** world.

In that world, data sits quietly on disk. When you want a report, you run a
query: the engine scans, filters, joins, aggregates, and returns a result. For
small data this is instant. But as data grows into millions or billions of rows,
those scans get slower and costlier. If a dashboard refreshes every few seconds,
re-scanning everything each time is like tearing down and rebuilding a stadium
scoreboard from scratch after every single point.

### Incremental View Maintenance: the ticking scoreboard

RockStream is **push-based and incremental**. Instead of waiting for you to ask,
it pre-computes the answers to the queries you care about and keeps them
continuously fresh. When a row is inserted, updated, or deleted, RockStream does
*not* re-run your query. It computes only the **difference** — the delta — and
applies it to the already-computed result. The scoreboard stays up; only the
numbers touched by the new event tick forward.

The work the engine does is proportional to the size of the *change*, not the
size of the *history*. Ten million historical rows cost nothing at read time,
because the answer is already sitting in storage.

### The mathematics of change: Z-sets

Under the hood, RockStream represents every change as a **Z-set**: a set whose
elements carry an integer weight. A normal set says an element is present or not.
A Z-set says an element is present *with weight $w \in \mathbb{Z}$* — positive,
negative, or zero. Formally, a Z-set over a domain $D$ is a function
$Z : D \to \mathbb{Z}$ with finitely many non-zero weights.

This is exactly the right shape for database changes:

- An **insert** of row $r$ is the Z-set $\{r \mapsto +1\}$.
- A **delete** of row $r$ is $\{r \mapsto -1\}$.
- An **update** from $r_{old}$ to $r_{new}$ is $\{r_{old} \mapsto -1,\; r_{new} \mapsto +1\}$.

Because relational operators are linear (or bilinear) over Z-set addition,
RockStream can compile SQL into a graph of operators that process these weights
directly:

- **Filter** $\sigma_p$: keep each element's weight if the predicate holds.
  $\sigma_p(X + Y) = \sigma_p(X) + \sigma_p(Y)$.
- **Project** $\pi_f$: apply $f$; sum the weights of elements that collide.
  $\pi_f(X + Y) = \pi_f(X) + \pi_f(Y)$.
- **Join** $\bowtie$ is bilinear, so a change to either side expands cleanly:
  $$d(X \bowtie Y) = (dX \bowtie Y) + (X \bowtie dY) + (dX \bowtie dY).$$

The payoff is a hard guarantee: RockStream's incremental output is always
*bit-identical* to what a full batch re-computation would produce. No drift, no
approximation. This guarantee is not a slogan — it is asserted as a property test
for **every** operator against a DataFusion batch reference, which you will run
yourself in Part 5.

### Coordination without locks: frontiers

In a sharded system, the hard part is agreeing on *progress* without a central
bottleneck. RockStream uses **frontiers**: monotonic markers that say "everything
up to logical epoch $N$ is done." If a source emits a frontier of `epoch=42`, it
promises never to emit anything older than 42. A join with two inputs at epochs
42 and 41 knows it can safely process through 41, and advances when the lagging
input catches up.

That gives **diamond consistency**: if a query fans out into two paths and merges
again at a join, the join always sees a perfectly aligned snapshot — never an
event from epoch 42 against state from epoch 41 — and it does so with no global
lock.

### Bottomless state: SlateDB on object storage

Many streaming databases keep all their state in RAM or on local SSDs. That is
fast but expensive, and recovery means copying gigabytes over the network before
a new worker can resume.

RockStream stores operator state (arrangements) and view outputs in
[**SlateDB**](https://slatedb.io/), an LSM engine that writes directly to cloud
object storage (S3, GCS, MinIO). State can grow past any single machine, bounded
only by your bucket. Because the state lives in the bucket, a failed worker's
shards can be picked up by another worker that simply reads the same path — no
state migration.

### The algebraic safety net: merge laws

To maintain an aggregate incrementally, every aggregate operator carries a
**merge law** — a named, versioned algebraic contract such as `WeightAdd` (for
`SUM`/`COUNT`) or `MaxRegister` (for `MAX`). RockStream verifies the properties
that make incremental maintenance safe:

1. **Associativity** — $(a \oplus b) \oplus c = a \oplus (b \oplus c)$.
2. **Commutativity** — $a \oplus b = b \oplus a$ (updates can arrive out of order).
3. **Identity** — an empty state $e$ with $a \oplus e = a$.
4. **Inverse** (when it exists) — $a \oplus (-a) = e$, which lets deletions
   subtract instead of recomputing.

`SUM` and `COUNT` have an inverse, so a deletion is a cheap subtraction. `MAX`
and `MIN` form a semilattice with *no* inverse — you cannot "un-max" a value — so
the engine records that a deletion of the current extremum needs a read to find
the replacement. Keeping this explicit is what lets the compiler reason about
cost and safety instead of guessing.

```
        Incoming change (a Z-set delta)
                     │
                     ▼
         ┌───────────────────────┐
         │   SQL frontend        │  parse · bind · optimize (DataFusion)
         │   (rockstream-sql)    │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │   PlanNode IR         │  logical/physical plan
         │   (rockstream-plan)   │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │   Differentiation     │  DiffCtx inserts arrangements
         │   (rockstream-diff)   │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │   Operator graph      │  executes deltas, commits to SlateDB
         │   (rockstream-ops)    │
         └───────────────────────┘
```

With the ideas in place, let's run something.

---

## Part 2 — Build the Workspace

RockStream is a Rust workspace of focused crates. You need a recent stable Rust
toolchain; the repository pins one in [rust-toolchain.toml](../rust-toolchain.toml),
so `rustup` will fetch the right version automatically.

From the repository root, build everything:

```bash
cargo build --workspace
```

The crates you just compiled map directly onto the architecture from Part 1:

| Crate | Responsibility |
|---|---|
| `rockstream-types` | Z-sets, frontiers, IDs, the `RS-XXXX` error registry, audit events |
| `rockstream-storage` | The SlateDB-backed `ShardDb`: WAL, checkpoints, compaction |
| `rockstream-plan` / `rockstream-diff` | Plan IR and the differentiation pass |
| `rockstream-ops` | Physical operators (filter, join, aggregate, window, top-K, …) |
| `rockstream-sql` | DataFusion-based SQL frontend, catalog, `EXPLAIN INCREMENTAL` |
| `rockstream-runtime` / `rockstream-control` | Worker runtime, control plane, leasing/fencing |
| `rockstream-gateway` | The PostgreSQL wire protocol gateway |
| `rockstream-sim` / `rockstream-oracle` | Deterministic simulator and the `incremental == batch` oracle |
| `rockstream-cli` | The single `rockstream` binary |

Confirm the binary built and inspect its surface:

```bash
cargo run --bin rockstream -- --help
```

```text
The single `rockstream` binary: one CLI for every node role

Usage: rockstream <COMMAND>

Commands:
  start  Start a RockStream node. At v0.1 this runs an embedded no-op node: it
         brings the node up, runs a no-op pipeline to completion, writes an audit
         log and a support bundle under the storage directory, and exits
  help   Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

One binary, every role as a flag — that is a deliberate design rule: `main`
stays runnable at every version of the roadmap. Today that binary boots a node
and runs an embedded no-op pipeline. Let's watch it do exactly that.

---

## Part 3 — Boot a Node and Read What It Leaves Behind

Pick a clean storage directory and start a node in the combined `all` profile,
which runs the control plane, a worker, and the storage engine inside one
process:

```bash
cargo run --bin rockstream -- start --storage ./rockstream-data
```

You will see real lifecycle logs. The interesting lines (abridged) are:

```text
INFO rockstream_control::service: control service listening addr=127.0.0.1:63179
INFO rockstream_control::service: control: worker registered worker_id=worker-1 headroom=1.00
INFO rockstream_runtime::client: Worker client registered successfully as WorkerId(1)
INFO rockstream_control::service: control: shard lease granted worker_id=worker-1 shard_id=shard-1 token=1
INFO rockstream_runtime::client: Received ShardAssigned lease for ShardId(1)
INFO slatedb::db::builder: opening SlateDB database [path=db, ...]
INFO rockstream: rockstream: embedded no-op node ran to completion
       audit=./rockstream-data/audit.jsonl bundle=./rockstream-data/support-bundle-….json events=7
```

That short burst exercised a surprising amount of the real system:

1. **The control plane** came up and started listening (here on an ephemeral
   port). It owns the topology catalog and hands out shards.
2. **A worker** registered itself with the control plane and reported its
   capacity headroom.
3. **A shard lease** was granted with a fencing token. This is the same leasing
   and fencing machinery that, in a real cluster, guarantees only one writer can
   ever commit to a shard — no split-brain.
4. **SlateDB** opened a real store under `./rockstream-data`.
5. The **embedded no-op pipeline** ran to completion, and the node shut down
   cleanly, writing an audit log and a support bundle.

### The audit trail

Every control-plane action writes an audit event — a hard rule across the whole
project. Look at what the run recorded:

```bash
cat ./rockstream-data/audit.jsonl
```

```json
{"timestamp_ms":…, "actor":"system",  "action":"server.started",     "resource":"rockstream",     "detail":"role=all"}
{"timestamp_ms":…, "actor":"system",  "action":"pipeline.created",    "resource":"noop-pipeline",  "detail":"embedded no-op pipeline"}
{"timestamp_ms":…, "actor":"system",  "action":"pipeline.started",    "resource":"noop-pipeline"}
{"timestamp_ms":…, "actor":"control", "action":"worker.registered",   "resource":"worker-1",       "detail":"address=127.0.0.1:0, headroom=1.00"}
{"timestamp_ms":…, "actor":"control", "action":"shard.lease_granted", "resource":"shard-1",        "detail":"worker=worker-1, token=lease-1"}
{"timestamp_ms":…, "actor":"system",  "action":"pipeline.stopped",    "resource":"noop-pipeline"}
{"timestamp_ms":…, "actor":"system",  "action":"server.stopped",      "resource":"rockstream"}
```

Notice that every event names an `actor` (`system` or `control`) and a
`resource`. When real DDL and DML flow through the gateway, the same trail will
record *who* created a view or wrote a row.

### The support bundle

The node also wrote a self-contained diagnostic snapshot:

```bash
cat ./rockstream-data/support-bundle-*.json
```

```json
{
  "generated_at_ms": …,
  "system_info": { "version": "0.52.10", "os": "macos", "arch": "aarch64", "role": "all" },
  "metrics":     { "uptime_ms": 160, "audit_events_emitted": 7 },
  "audit_events": [ … the full audit log … ]
}
```

This is the artifact you would attach to a bug report: version, platform, a
metrics snapshot, and the complete audit history in one file.

And the storage directory itself now contains a real SlateDB layout:

```bash
ls ./rockstream-data
# audit.jsonl   shards/   support-bundle-….json
```

---

## Part 4 — Splitting the Roles

The same binary runs each role separately, which is how a real cluster is shaped.
Booting a control node and a worker that registers with it looks like this:

```bash
# Terminal 1 — control plane on a fixed port
rockstream start --role=control --storage ./rs-control

# Terminal 2 — a worker that joins it
rockstream start --role=worker --control=127.0.0.1:8000 --storage ./rs-worker
```

A `worker` or `gateway` role requires `--control=<url>`; omit it and you get a
clear, actionable error:

```text
RS-0002 role `worker` requires --control=<url>
  next steps: Provide the control plane URL via the --control argument.
```

Every operator-visible failure in RockStream carries an `RS-XXXX` code with a
`next steps` line — CI fails the build on any error path that doesn't. The full
registry lives in
[crates/rockstream-types/src/error_code.rs](../crates/rockstream-types/src/error_code.rs),
and the CLI surface is documented in [docs/cli.md](cli.md).

If you want the project's own end-to-end check that boots a control node and a
worker, verifies their audit logs, and runs the local-filesystem and MinIO
storage suites, run:

```bash
make e2e
```

---

## Part 5 — Watch the IVM Engine Prove Itself

Here is the honest, powerful part. The incremental engine — filters, joins,
aggregates, windows, recursion, the whole DBSP machinery from Part 1 — is
implemented and continuously verified. The way you *watch it work today* is by
running the proofs. These are not toy unit tests; they are property tests that
generate thousands of random insert/update/delete sequences and assert that the
incremental result equals a full batch re-computation, every time.

### The oracle: `incremental == batch`

The `rockstream-oracle` crate holds the source-of-truth equivalence harness. Run
the operator oracles:

```bash
cargo test -p rockstream-oracle
```

Each module — [filter_oracle.rs](../crates/rockstream-oracle/src/filter_oracle.rs),
[join_oracle.rs](../crates/rockstream-oracle/src/join_oracle.rs),
[aggregate_oracle.rs](../crates/rockstream-oracle/src/aggregate_oracle.rs),
[minmax_oracle.rs](../crates/rockstream-oracle/src/minmax_oracle.rs),
[window_oracle.rs](../crates/rockstream-oracle/src/window_oracle.rs), and more —
feeds the operator a random stream of deltas and checks its output against the
DataFusion batch reference. When this passes, the bilinear join expansion and the
merge-law arithmetic from Part 1 are not just plausible — they are demonstrated.

### A real view: join + group-by, maintained incrementally

The SQL frontend compiles a query into the operator graph and maintains it. The
end-to-end test in
[crates/rockstream-sql/tests/sql_engine_e2e.rs](../crates/rockstream-sql/tests/sql_engine_e2e.rs)
takes this view:

```sql
CREATE VIEW revenue AS
SELECT o.region, SUM(l.amount)
FROM orders o
JOIN lineitem l ON o.order_id = l.order_id
GROUP BY o.region;
```

…deploys it, streams changes into `orders` and `lineitem`, and asserts the
maintained `revenue` view matches the batch answer after every epoch. Run it:

```bash
cargo test -p rockstream-sql --test sql_engine_e2e
```

```text
test sql_engine_create_view_join_group_by ... ok
test sql_engine_window_row_number_over_view ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

The second test maintains a `ROW_NUMBER() OVER (PARTITION BY k ORDER BY v)`
window incrementally — the same window functions a campaign leaderboard or a
"top product per region" dashboard would need.

### Operator durability and the full SQL surface

The physical operators each have a SlateDB-backed test that proves they survive a
crash and replay to bit-identical state. These live in
[crates/rockstream-ops/tests](../crates/rockstream-ops/tests) — for example
`lfs_join.rs`, `lfs_outer_join.rs`, `lfs_minmax.rs`, `lfs_distinct.rs`,
`lfs_topk.rs`, and `lfs_time_window.rs`:

```bash
cargo test -p rockstream-ops
```

And the SQL engine's correctness soak runs the TPC-H query set incrementally and
checks each result against batch:

```bash
cargo test -p rockstream-sql --test tpch_plans
```

This is the concrete meaning of the roadmap's *IVM Correct (single-shard)*
milestone: 22/22 TPC-H queries return bit-identical results versus DataFusion
batch.

---

## Part 6 — The PostgreSQL Wire Gateway

RockStream speaks the Postgres wire protocol so you can eventually point `psql`,
a BI tool, or any Postgres driver at it. The gateway (`rockstream-gateway`) is
implemented and proven against a **real** Postgres client (`tokio-postgres`) in
its integration suite. Run it:

```bash
cargo test -p rockstream-gateway --test gateway_integration_tests
```

```text
test server_starts_and_accepts_connection ... ok
test extended_query_protocol_parse_bind_execute ... ok
test proof_pg_catalog_schema_reflection_queries ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

Those three tests stand up a `GatewayServer` on a real socket, connect with a
genuine Postgres client, and exercise:

- **Connection + simple query** — `SELECT 1` round-trips over the wire.
- **The extended query protocol** — parse / bind / execute, the path every
  driver and ORM actually uses.
- **Catalog reflection** — `pg_catalog.pg_tables`, `pg_views`, `pg_class`, so
  tools that introspect schemas (SQLAlchemy, JDBC) see your views as tables.

The deeper proof suite in
[crates/rockstream-gateway/tests/gateway_proof_tests.rs](../crates/rockstream-gateway/tests/gateway_proof_tests.rs)
goes further, demonstrating the behaviors that make the gateway usable from real
applications:

- `proof_psql_select_limit_10_under_10ms_p99` — point reads stay fast.
- `create_table_registers_in_catalog` and `insert_accumulates_in_write_buffer` —
  `CREATE TABLE` and buffered `INSERT`/`UPDATE`/`DELETE` DML.
- `copy_out_streams_view_rows` — the Postgres `COPY` path for bulk export.
- `proof_serializable_returns_rs2003` — isolation levels return precise
  `RS-XXXX` codes when a mode is unsupported.

Authentication, RBAC, and read-your-writes are covered alongside in
[auth_proof_tests.rs](../crates/rockstream-gateway/tests/auth_proof_tests.rs).

> **What this means for you.** The query examples above describe exactly what the
> engine maintains and serves; they are exercised by the suites you just ran, not
> typed into a live psql session. Wiring the gateway, the catalog, and a live
> view reader together behind a single long-running `rockstream serve` command is
> the productization step that turns these proven parts into a server you connect
> to directly. Until then, the test harness is the faithful, reproducible way to
> see the Postgres layer behave.

---

## Part 7 — Where This Is Going

You have now seen the real RockStream: a node that boots, leases and fences
shards, and writes an auditable trail; an incremental engine whose correctness is
proven against a batch oracle; and a Postgres gateway verified against a real
client. The roadmap's design is **one binary, one config, three tiers**, and the
same artifacts you built scale along it:

1. **Evaluation (laptop).** A single process with local storage — exactly what
   you ran in Part 3.
2. **Single-host production.** The same process, but `--storage` points at an
   object store (`s3://…` or MinIO). If the host dies, a new one boots against
   the same bucket and recovers, because all durable state lives there.
3. **Distributed cluster.** `--role=control`, `--role=worker`, and
   `--role=gateway` on separate nodes, sharing object storage, exchanging data
   over gRPC shuffles, and coordinating through the frontier protocol.

The build sequence that gets there — single-shard correctness, multi-shard
execution, the frontier protocol, exactly-once fault tolerance, the Postgres
pillar, ingestion connectors, and secondary indexes — is laid out, version by
version with its proof obligations, in [NEW_ROADMAP.md](../NEW_ROADMAP.md). Every
"Done" version has a sign-off file under [sign-offs/](../sign-offs/) listing the
exact tests that back its claims.

### Keep exploring

- [DESIGN.md](../DESIGN.md) — what RockStream is and why the architecture holds.
- [IVM.md](../IVM.md) — a deeper look at the incremental engine.
- [docs/concepts.md](concepts.md) — the vocabulary in one place.
- [docs/cli.md](cli.md) — the current CLI surface, documented honestly.
- [docs/ivm-operators.md](ivm-operators.md) — the operator catalog.
- [docs/language-features.md](language-features.md) — the SQL surface.

### What you did in 30 minutes

1. Learned how incremental view maintenance, Z-sets, frontiers, and merge laws
   let RockStream keep answers fresh by processing only change.
2. Built the workspace and inspected the single `rockstream` binary.
3. Booted a node, watched the control plane, worker registration, shard leasing,
   and SlateDB come up, and read the audit log and support bundle it produced.
4. Saw the roles split the way a real cluster is shaped.
5. Ran the oracle and SQL proofs that demonstrate `incremental == batch` for
   real operators and a real `JOIN … GROUP BY` view.
6. Verified the PostgreSQL wire gateway against a genuine Postgres client.

Welcome to RockStream. The scoreboard is already ticking — now you know what
makes it tick.

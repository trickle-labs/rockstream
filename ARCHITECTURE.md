# RockStream Architecture

This document explains how RockStream is built and how it works. It is written
to be read from top to bottom: it starts with the core idea, follows a single
piece of data on its journey through the system, and then opens up each major
subsystem in turn. If the [README](README.md) tells you *what* RockStream does
and *why*, this document tells you *how*. The two deeper specifications —
[DESIGN.md](DESIGN.md) for the full system and [IVM.md](IVM.md) for the
incremental engine — remain the authoritative references; this is the map that
helps you navigate them.

---

## 1. The One Idea Everything Hangs On

Almost every interesting property of RockStream falls out of a single
commitment: **never recompute an answer from scratch when you can compute only
what changed.** A traditional database, asked for "total sales per region,"
re-scans every order, re-groups it, and re-sums it on every query. RockStream
instead keeps the answer materialized and, when a handful of new orders arrive,
works out the *difference* those orders make to the existing answer and applies
just that difference. The scoreboard stays lit; only the numbers that moved tick
over.

This technique is called **Incremental View Maintenance** (IVM), and the precise
mathematical form RockStream uses comes from a body of work called **DBSP** — the
theory behind [Feldera](https://feldera.com/) and, in spirit, the differential
dataflow that powers [Materialize](https://materialize.com/). The reason
RockStream leans so hard on a *theory* rather than a bag of hand-written rules is
that incremental computation is treacherous. It is easy to write an
update-the-total shortcut that is correct for inserts but subtly wrong for
deletes, or for an outer join, or when a late record arrives out of order. DBSP
gives a single guarantee that makes the whole system trustworthy:

> For any query `Q` and any stream of changes `Δ`, the incrementally maintained
> result is *bit-for-bit identical* to what you would get by re-running `Q` over
> all the accumulated data.

RockStream does not merely hope this holds — it *tests* it continuously, with a
dedicated correctness oracle described in §9. Everything else in the
architecture exists to make this idea fast, durable, distributed, and reachable
through tools you already own.

### Changes as numbers: the Z-set

To make "the difference a change makes" something a computer can add and
subtract cleanly, RockStream represents data not as rows but as **Z-sets**. A
Z-set is a collection of rows where each row carries an integer **weight**. A
weight of `+1` means "this row was inserted"; `-1` means "this row was deleted";
an update is simply a `-1` for the old version paired with a `+1` for the new
one. Aggregates, joins, and filters all become arithmetic over these weights,
and — crucially — the operations are associative and commutative, so changes can
be reordered, batched, and merged without changing the final answer. In the
code, a Z-set is an Arrow `RecordBatch` with an extra weight column, which means
the engine gets columnar, vectorized data layout for free. This single
representation is what lets the same delta flow correctly whether it is processed
now or replayed after a crash, on one machine or shuffled across thirty.

---

## 2. A Day in the Life of a Change

Before dissecting the parts, it helps to watch the whole machine move. Suppose
you have created a materialized view — `SELECT region, SUM(amount) FROM orders
GROUP BY region` — and a new order arrives.

1. **The change enters.** It arrives either through a **connector** (Kafka, an
   S3 file, a Postgres CDC stream) or because a client issued a direct `INSERT`
   over the Postgres wire protocol into the **gateway**. Either way it becomes a
   `+1`-weighted row in a Z-set.

2. **It joins an epoch.** RockStream does not process one row at a time. It
   batches changes into a small unit called an **epoch** — think of it as a
   micro-transaction. The epoch is the atom of progress, recovery, and
   consistency; everything the system commits, it commits one whole epoch at a
   time.

3. **It flows through the circuit.** The view's query was compiled, once, into a
   graph of **operators** — a "circuit." Our order's delta enters at the source
   node and flows downward. The `Filter` and `Project` operators pass deltas
   through untouched (they are *linear*); the `Aggregate` operator is where the
   real work happens: it reads the current `SUM` for that region from storage,
   adds the delta, and emits the *change to the output* — say, region "EMEA"
   went from 4,200 to 4,350, so it emits `-1×(EMEA,4200)` and `+1×(EMEA,4350)`.

4. **State is persisted.** The aggregate's running total lives in an
   **arrangement** — an indexed, persistent key-value structure backed by
   SlateDB on object storage. The new total is written as part of the epoch's
   atomic **WriteBatch**.

5. **The epoch commits.** Once every operator in the circuit has processed the
   epoch and all writes are durable, the shard advances its **frontier** — a
   marker meaning "every change up to here is committed and queryable." In a
   multi-shard cluster, the **control plane** waits until *all* shards reach the
   epoch before declaring the cluster-wide frontier has advanced.

6. **The answer is served.** A moment later you run `SELECT * FROM
   sales_by_region` through `psql`. The gateway reads the materialized view
   straight from the arrangement — no scanning of raw orders, no re-summing — and
   hands back the fresh number. If you need read-your-writes certainty, you can
   ask it to wait until the frontier covering your write is visible.

That entire loop — change in, delta computed, state updated, epoch committed,
answer served — is the heartbeat of RockStream. The rest of this document is the
anatomy behind that heartbeat.

---

## 3. The Shape of the System: Crates and Layers

RockStream is a single Cargo workspace of thirteen purpose-built crates, and a
single binary (`rockstream`) that can play any role in a cluster depending on
the flags you pass it. The crates stack into clean layers, and the dependency
arrows only ever point downward — a discipline that keeps the foundational types
free of execution concerns and the execution engine free of distribution
concerns.

```
                       ┌──────────────────────┐
   user-facing  ──────▶│   rockstream-cli     │  one binary, role = flag
                       └──────────┬───────────┘
                                  │
        ┌─────────────────────────┼──────────────────────────┐
        ▼                         ▼                           ▼
┌───────────────┐      ┌────────────────────┐     ┌────────────────────┐
│ rockstream-   │      │  rockstream-gateway│     │ rockstream-control │
│   sql (front) │      │  (Postgres wire)   │     │  (cluster brain)   │
└──────┬────────┘      └─────────┬──────────┘     └─────────┬──────────┘
       │                         │                          │
       ▼                         ▼                          │
┌───────────────┐      ┌────────────────────┐               │
│ rockstream-   │      │ rockstream-runtime │◀──────────────┘
│   diff (∂)    │      │ (worker + exchange)│
└──────┬────────┘      └─────────┬──────────┘
       │                         │
       ▼                         ▼
┌───────────────┐      ┌────────────────────┐     ┌────────────────────┐
│ rockstream-   │─────▶│  rockstream-ops    │────▶│ rockstream-storage │
│   plan (IR)   │      │  (operators)       │     │  (SlateDB wrapper) │
└───────────────┘      └────────────────────┘     └────────────────────┘
                                  │
        ┌─────────────────────────┴──────────────────────────┐
        ▼                                                     ▼
┌────────────────────┐                            ┌────────────────────┐
│rockstream-connectors│                           │  rockstream-types  │
│ (Kafka/S3/CDC/...)  │                           │  (the lingua franca)│
└────────────────────┘                            └────────────────────┘

   testing & validation, used by everything above:
   rockstream-oracle (correctness)   rockstream-sim (deterministic chaos)
```

At the very bottom sits **rockstream-types**, which depends on nothing else in
the workspace and is depended on by everything. It is the shared vocabulary of
the system: epochs and event-time watermarks, frontiers, Z-set batches, schema
definitions and their evolution rules, identity types for workers and operators,
the merge-law descriptors that underpin algebraic aggregation, ACLs, checkpoint
coordination types, the view lifecycle state machine, and the error codes that
appear in operator diagnostics. Because every other crate speaks in these terms,
a frontier means exactly one thing whether it is being computed in an operator,
shuffled across the network, or reported up to the control plane.

The remaining crates fall into four bands. **The compilation front-end**
(`rockstream-plan`, `rockstream-sql`, `rockstream-diff`) turns SQL into an
executable circuit. **The execution engine** (`rockstream-ops`,
`rockstream-storage`, `rockstream-runtime`) runs that circuit and persists its
state. **The distributed and user-facing layer** (`rockstream-control`,
`rockstream-gateway`, `rockstream-connectors`, `rockstream-cli`) coordinates a
cluster, speaks Postgres, moves data in and out, and provides the operator's
command line. And **the validation harness** (`rockstream-oracle`,
`rockstream-sim`) exists purely to prove the other three bands correct. The
sections that follow walk these bands in the order a query travels through them.

---

## 4. From SQL to a Circuit: The Compilation Front-End

When you write `CREATE MATERIALIZED VIEW sales AS SELECT ...`, three crates
collaborate to turn that text into a running incremental circuit, and they do so
exactly once — at deploy time, not on every change.

**rockstream-sql** is the front door. Rather than reinvent SQL parsing and
optimization, it stands on the shoulders of [Apache
DataFusion](https://datafusion.apache.org/): it parses your statement, binds it
against the schema catalog, and runs DataFusion's optimizer to produce a clean
logical plan. It then *lowers* that plan into RockStream's own intermediate
representation. Along the way it recognizes which operations must be
incrementally maintained and marks them with custom extension nodes
(`IncAggregate`, `IncJoin`, `IncDistinct`), runs a **distribution pass** that
annotates each operator with the key it should be partitioned on and inserts
`Exchange` markers where data must be shuffled between shards, and consults a
versioned **schema catalog** so it can tell a backward-compatible change from a
breaking one. This crate is also where the operator-facing diagnostics
`EXPLAIN INCREMENTAL` (the annotated operator tree) and `EXPLAIN INCREMENTAL
ESTIMATE` (a static cost-and-state-size preview you can run *before* deploying)
are produced.

**rockstream-plan** holds the two intermediate representations that everything
downstream agrees on. The `PlanNode` enum is the *logical* IR — declarative
nodes like `Source`, `Filter`, `Project`, `Aggregate`, `Join`, `Union`,
`Distinct` — and the `OpNode` graph is the *physical* IR, the concrete operator
graph that will actually execute. Keeping this contract in its own tiny crate
means the SQL front-end and the execution engine can evolve independently as long
as they keep speaking `PlanNode` and `OpNode`.

**rockstream-diff** is the mathematical heart of compilation, and it earns its
name. Its single differentiation pass — the `∂` of DBSP — walks the logical
`PlanNode` tree and emits the physical `OpNode` circuit, applying the DBSP delta
rules as it goes. For *linear* operators like filter, project, and map, the
incremental rule is beautifully simple: the change to the output is just the
operator applied to the change in the input, so the delta rule is essentially the
identity. For *stateful* operators like aggregates, it is subtler: the pass wires
up the arrangement state and the read-modify-emit logic that turns a delta on the
input into a delta on the running result. This is precisely the place where
incremental correctness is won or lost, which is why it is small, focused, and
guarded by the oracle.

The product of these three crates is a physical operator graph — a circuit —
ready to be handed to the runtime and executed against live deltas.

---

## 5. Running the Circuit: The Execution Engine

### 5.1 Operators (`rockstream-ops`)

If the front-end builds the circuit, **rockstream-ops** is the library of parts
the circuit is made from, plus the scheduler that drives them. Every node in the
graph implements a common `Operator` trait and runs inside an `OperatorTask`
event loop that consumes input deltas and produces output deltas. The crate
implements the full operator catalog: the stateless linear operators (filter,
project, map); the stateful ones that maintain arrangements (aggregate with its
DBSP delta rules, min/max via indexed arrangements, distinct, top-K, time
windows, inner and outer joins); the source operators that introduce data; and
the `ViewSink` that writes finished results out.

Two pieces of machinery here are worth calling out because they shape the
system's behavior. The **credit scheduler** is how RockStream meters work: an
operator runs only when it has been granted credits, which lets the system pace
ingestion, apply backpressure, and hit freshness targets rather than simply
running flat-out and falling over. The **group-commit** mechanism coalesces many
small `WriteBatch`es into fewer, larger writes to storage, which matters
enormously when your durable store is object storage and every write has latency
and cost. The crate also ships an `EmbeddedRuntime` that runs a whole circuit in
a single process — the engine you get on a laptop, and the engine the tests
exercise.

### 5.2 State and durability (`rockstream-storage`)

Operator state and view results all live in **arrangements**, and arrangements
live in **SlateDB** — an LSM-tree key-value store designed to sit directly on
object storage (S3, GCS, MinIO). This choice is foundational rather than
incidental: it is what lets RockStream treat compute and storage as separate,
independently scalable tiers, and it is why moving from a laptop to a cluster
involves no data migration — the same files written against MinIO open against
S3.

**rockstream-storage** is the disciplined wrapper around SlateDB's real API
surface. It owns the **key encoding scheme** that namespaces every shard's data
so nothing collides; the `ShardDb` abstraction for per-shard reads and writes;
the `WriteBatch` builders that make an epoch's writes atomic; a `DbReader` for
consistent cross-shard snapshot reads; and a **merge-operator registry** that
teaches SlateDB how to combine partial aggregates (a `SUM`, a `COUNT`) directly
in the store, so the engine can often avoid an expensive read-modify-write
round-trip entirely. It also manages the write-ahead log and a WAL-listing cache
that keeps the hot path from paying for expensive object-store `LIST` calls. A
deliberate constraint runs through this crate: it assumes only what SlateDB
actually offers (for example, there is no range-delete, so cleanup is done by
scan-and-delete or compaction filters), which keeps the design honest about its
real foundation.

### 5.3 The worker and the exchange (`rockstream-runtime`)

**rockstream-runtime** is what a worker process actually *is*. It wraps the
operator scheduler with everything needed to participate in a cluster: a client
that registers the worker with the control plane, acquires and renews the
**leases** that grant it ownership of particular shards, and sends heartbeats. It
houses the **recovery driver** that brings a shard back from its last checkpoint,
and a **self-fencing** mechanism that forces a worker which has lost contact with
the control plane to stop committing, so it cannot race a newly-appointed owner
of the same shard.

Its largest responsibility is the **exchange** — the subsystem that moves rows
between operators that live on different shards (a shuffle, in the parlance of
distributed query engines). When the producer and consumer happen to be on the
same worker, exchange is a fast in-memory `Loopback` channel. When they are on
different workers, rows are encoded as Arrow IPC frames and sent over gRPC, and a
`DurableShuffleWriter` ensures those shuffle objects are persisted to the WAL
*before* the sending epoch is allowed to commit — so a crash mid-shuffle loses
nothing. The exchange layer also handles flow control, connection pooling,
multiplexing, and the wire-version negotiation that makes rolling upgrades safe.

---

## 6. Coordinating a Cluster: The Control Plane

A single worker can maintain views happily on its own, but RockStream is built to
scale horizontally, and **rockstream-control** is the brain that makes a fleet of
workers behave like one system. It is deliberately lean — it depends only on
`rockstream-types` — because the control plane must be the most trustworthy
component in the system.

It maintains the **topology catalog** of which workers exist and what they are
running; a **shard manager** that hands out shard leases protected by **fencing
tokens** (a monotonically increasing number that lets storage reject a write from
a stale, fenced owner); a **shard scheduler and placement algorithm** that
decides which worker should host which shard based on capacity; and a
**namespace catalog** that organizes views and shards. Two of its jobs are
especially central to correctness. The **frontier aggregator** collects each
worker's per-shard frontier reports and computes the cluster-wide frontier as
their *meet* (the minimum) — the single value that defines what epoch a query can
be answered consistently at. And the **checkpoint coordinator** drives the
protocol that gives RockStream exactly-once semantics and fast recovery, which
deserves its own section.

The control plane also keeps an **audit log** (file-backed JSONL) of every action
it takes — every scaling decision, every degraded-state transition, every
pipeline change, each stamped with the metric reading that triggered it. This is
a design principle, not a feature bolt-on: nothing changes silently, and
`rockstream audit tail` will always tell you *why* something happened.

### The checkpoint protocol, briefly

Exactly-once processing in a distributed system is hard precisely because crashes
can happen between any two steps. RockStream's answer is a barrier-based
checkpoint protocol. The coordinator injects a **checkpoint barrier** into each
shard's WAL; operators propagate it downstream and acknowledge it; rows that
arrive at a shard before the barrier has propagated from all of its upstreams
wait in a bounded **alignment buffer**. Only once every shard has acknowledged
the barrier does the coordinator atomically write the checkpoint manifest — an
all-or-nothing commit, so a partial write is detectable on recovery by comparing
the manifest epoch against each shard's committed frontier. Old checkpoints are
garbage-collected once the cluster frontier moves safely past them. On top of
this foundation, sink connectors layer a **two-phase commit** (prepare during the
epoch, commit only after the cluster checkpoint succeeds, abort if it fails) to
extend exactly-once all the way out to Kafka, S3, or Postgres.

The system commits to concrete recovery budgets and tests them every release:
failure detection within 5 seconds, shard reassignment within 30 seconds (from
the checkpoint in storage — no full WAL replay), and pipeline freshness restored
within 60 seconds. If any budget is missed, a named degraded state fires rather
than a silent slowdown.

---

## 7. The Front Door: The Postgres Gateway

The decision that makes RockStream immediately usable is that it speaks the
**Postgres wire protocol**. **rockstream-gateway** implements that protocol via
`pgwire`, which means `psql`, your BI tool, and any Postgres client library can
connect to RockStream as if it were a Postgres database — no special driver, no
new query language.

The gateway plays two roles. On the **read** side it serves OLAP queries against
maintained views: a `ViewReader` pulls results straight from the arrangements,
and for views spread across many shards a `MultiShardReader` performs a scatter
read pinned to a single frontier so the answer is internally consistent. On the
**write** side it accepts direct `INSERT`/`UPDATE`/`DELETE` DML, buffering it in a
bounded `WriteBuffer` and feeding it into the engine as a change stream — exactly
as if it had arrived from an external source. This is what lets you get data in
without standing up a separate database or a Kafka topic. To keep Postgres tools
happy, the gateway also stubs out enough of `pg_catalog` and `information_schema`
for clients to introspect, and it manages session state and isolation levels per
connection.

Beyond plain queries it offers the features that make a streaming system pleasant
to use: a `SUBSCRIBE` handler that streams a view's changes to a client as CDC,
`AS OF EPOCH` historical queries, **freshness tokens** that let a client request
read-your-writes behavior by waiting for a specific frontier to become visible,
and authentication via OIDC or mTLS. The same gateway can also expose a native
**Iceberg REST Catalog** endpoint so external lakehouse tools can discover
RockStream views by name.

---

## 8. Getting Data In and Out: Connectors

**rockstream-connectors** is the system's I/O boundary. It defines two contracts
— a `SourceConnector` trait for bringing data in and a `SinkConnector` trait for
writing results out — and ships built-in implementations: Kafka source and sink,
S3 source, object-store sink, and (per the roadmap) Postgres CDC and
Iceberg/Delta sources.

The interesting engineering here is all about exactly-once correctness across the
boundary. On the source side, a `SourceEpochRegistry` records exactly which input
partitions and offsets contributed to each epoch, with an `OffsetToken` that
allows a connector to resume from precisely the right place after a restart —
never replaying committed data, never skipping uncommitted data. On the sink
side, connectors implement the two-phase commit described above, so a row is
delivered to Kafka or S3 *exactly* once even across crashes. The crate carries
explicit recovery assertions — idempotent dispatch, no duplicates or losses after
a checkpoint, epochs committed downstream only after the cluster checkpoint — and
exercises them against real Kafka and MinIO via testcontainers.

For the lakehouse story, RockStream can act as a **freshness layer** that
periodically snapshots views to object storage as Iceberg v2 or Delta Lake
tables, after which DuckDB, Trino, or Spark can query them directly with
RockStream entirely out of the read path.

---

## 9. Why You Can Trust It: Oracle and Simulator

Two crates exist for one reason — to prove the rest of the system correct — and
together they are the strongest statement of RockStream's engineering values.

**rockstream-oracle** is the guardian of the central DBSP promise. Its job is to
relentlessly check that `incremental(query, Δ) == batch(query, accumulated)`. It
does this by accumulating Z-set deltas, running the *same* query as an ordinary
one-shot DataFusion batch computation as a reference answer, and comparing the
two. It drives this comparison with property tests over every operator — filter,
project, map, the aggregates (SUM/COUNT/AVG), min/max, distinct, top-K, time
windows, outer joins — and with a SQL fuzzer that throws generated queries at the
engine, plus TPC-H data generation for realistic shapes. If the incremental
engine ever disagrees with the batch reference by even a single weighted row, the
oracle fails the build. This is how a small, theory-derived differentiation pass
earns the right to be trusted.

**rockstream-sim** is the flight simulator, a technique borrowed from
[FoundationDB](https://www.foundationdb.org/). The trick is that the entire
system is written against a `Runtime` abstraction for time, task spawning,
storage, and network I/O. In production it is backed by a real Tokio runtime; in
testing it is backed by a `SimRuntime` whose clock, network, and object store are
all deterministic and driven by a single seeded random-number generator. Inside
that simulated world, a `buggify!` macro injects faults at the nastiest
moments — network partitions, replica failures, object-store brownouts, messages
reordered and duplicated, two workers crashing milliseconds apart. Because
everything is deterministic, *any* bug the simulator finds is reproducible from a
single seed number: failing seeds are minimized, stored in a corpus, and a
release is blocked until every one of them replays cleanly. Millions of seeded
scenarios run before each release, which means the rare timing bugs that normally
fill a production runbook get discovered and fixed on a developer's laptop
instead.

The payoff of these two crates is the confidence that lets RockStream make
quantitative promises — bit-exact incremental results, exactly-once delivery,
sub-minute recovery — rather than hedged ones.

---

## 10. One Binary, Three Tiers: Deployment

All of this is delivered as a single executable. **rockstream-cli** builds the
`rockstream` binary, and a node's *role* is a command-line flag rather than a
separate program. The same binary can be the whole system, a gateway, a worker,
or a control node:

- **Laptop / evaluation:** `rockstream start --storage=./data` — zero
  configuration, runs the embedded single-process engine, and survives crashes.
- **Single host (small production):** `rockstream start --role=all
  --storage=s3://bucket/...` — the same engine, now durable on object storage.
- **Multi-host cluster:** `--role=control` on the coordinating nodes and
  `--role=worker` on the rest.

Moving up this ladder is purely *additive*. Because no node keeps authoritative
state on local disk — everything durable lives in SlateDB on object storage —
there is no data migration when you scale up: the files a laptop wrote against
MinIO are opened, unchanged, by a cluster against S3. At every tier you connect
the same way, with `psql` or any Postgres client, and the development loop is the
same one you would use with an ordinary database: start the binary, open a SQL
prompt, write a view, insert a few rows, and watch the answer stay current as
data keeps flowing. The CLI also surfaces the operability tools — `rockstream
explain` for the annotated operator tree and its pre-deploy cost estimate,
`rockstream audit tail` for the running record of every control-plane decision,
and `rockstream support bundle` to collect everything needed to debug a pipeline
in one command.

---

## 11. The Threads That Tie It Together

A few principles recur across every layer, and noticing them is the fastest way
to understand why the code looks the way it does.

**The epoch is the universal unit.** Ingestion batches into epochs, the circuit
processes epochs, storage commits epochs atomically, frontiers advance by epoch,
checkpoints align on epochs, and connectors track their offsets by epoch. Once
you see the epoch as the system's clock-tick, the coordination protocols stop
looking arbitrary.

**Algebra is the safety net.** Z-sets make changes addable and subtractable;
merge laws make aggregates combinable in storage and across a shuffle without
read-modify-write; and the DBSP differentiation pass is correct because the
algebra underneath it is. Where an operation *cannot* be expressed as a clean
algebraic law, the system records an explicit, machine-readable reason rather than
guessing — and `EXPLAIN INCREMENTAL` will show it to you.

**Compute and storage are separate tiers.** SlateDB-on-object-storage is the
hinge that makes the deployment ladder seamless, recovery fast (reload from a
checkpoint instead of replaying a log), and scaling a matter of moving leases
rather than moving data.

**Nothing changes silently, and nothing is promised that isn't tested.** The
audit log records every control-plane action with its trigger; the oracle proves
incremental results match batch results; the simulator manufactures the rare
failures that would otherwise surface only in production. The recovery budgets,
the exactly-once guarantees, and the freshness SLOs are all things the test suite
checks on every release rather than aspirations in a document.

Read together, these threads explain the architecture's character: a small,
theory-grounded core wrapped in the operational machinery needed to run it at
scale, delivered through an interface deliberately chosen to be one you already
know.

# RockStream Concepts Guide

A friendly tour of how RockStream thinks about data, time, change, and
consistency — written for people who want to understand the system without
wading through implementation details.

---

## Part 1 — The Big Picture

### 1. What RockStream Is, In Plain Words

Imagine you have a dashboard that shows the total revenue per region for an
online store. Every time someone places an order, that dashboard ought to
update. The simplest way to make this happen is to ask the database the same
question over and over again: *"What is the total revenue per region right
now?"* The database obediently scans every order, groups them by region, sums
the amounts, and hands you back the answer. This works fine when you have a
thousand orders. It becomes painful when you have ten million. It becomes
impossible when you have a billion and the dashboard needs to refresh every
second.

There is a better way, and it has a name: *incremental view maintenance*. The
insight is simple. When a single new order arrives, the total for that order's
region goes up by the order's amount. Nothing else changes. So instead of
recomputing the entire answer from scratch, you can take the previous answer
and adjust it by the difference. The work you do is proportional to the
change, not to the size of the underlying data. A million orders coming in
costs you a million tiny adjustments rather than a single enormous scan.

RockStream is a system that does this for you, automatically, at scale. You
write SQL that describes what answer you want. RockStream figures out how to
maintain that answer as new data arrives, in a way that is fast, correct, and
runs across many machines without you having to think about which machine
does what. You get continuously fresh materialized views. The system keeps
them fresh by computing only what changed.

What makes RockStream different from the other systems in this space is the
combination it picks. PostgreSQL has materialized views, but you have to
refresh them manually or pay the cost of triggers; it doesn't scale beyond
one machine. Other streaming systems are built around
this idea, but they hold a lot of state in memory and have their own runtime
to manage. Declarative incremental-view features in cloud data warehouses
get you a similar feel but live inside a
warehouse with its own pricing model. Pg-trickle adds incremental views to
PostgreSQL itself, which is wonderful for a single-database deployment but
inherits PostgreSQL's single-machine ceiling. RockStream sits at a particular
intersection: it speaks the PostgreSQL wire protocol so familiar tools work,
it stores all its data in cheap object storage like S3, and it shards work
across many machines so it can scale horizontally as your data grows.

The promise, in one sentence: SQL in, fresh views out, on a cluster that
grows with you, paying object-storage prices.

### 2. The Mental Model

When you use RockStream today, the SQL-reachable objects are mainly
**workloads**, **tables**, and **views**. **Sources** and connector-managed
ingest still exist in the architecture, but they are not yet a pgwire SQL DDL
surface: `crates/rockstream-gateway/src/server.rs` dispatches `CREATE
WORKLOAD`, `CREATE TABLE`, `CREATE VIEW`, and `CREATE MATERIALIZED VIEW`, but
no `CREATE SOURCE` family. Everything else — sharding, shuffling, recovery,
compaction, garbage collection, rebalancing — happens for you, behind a
curtain, and you don't need to think about it unless something is wrong.

A **view** is a SQL query that has a name. It might be a tiny one-liner that
just selects a subset of a table, or it might be a complicated join with
window functions and — in the planned surface described later — recursion.
From the outside, a view looks like a table:
you can run `SELECT * FROM my_view` against it just like you would against
`orders` or `customers`. The difference is that the view's contents are
computed from other data, and RockStream keeps those contents fresh as the
underlying data changes.

A **workload** is a named resource policy. It groups together a set of related
views under a shared freshness SLO, memory budget, and priority. It is the
unit that gets a freshness target ("my views should be at most one second
stale"), a resource limit ("don't use more than two hundred gigabytes of
state"), and a priority ("this workload matters more than that one when the
cluster is busy"). When you tell RockStream what you want, you tell it at the
workload level. The system then makes all the small decisions — how many
machines to use, how big to make each batch, when to flush to disk — to honor
what you asked for.

The verb is **create**. You write `CREATE MATERIALIZED VIEW`, you submit it,
and the system goes to work. If you want to change something today, you
generally use `CREATE OR REPLACE VIEW` / `CREATE OR REPLACE MATERIALIZED VIEW`
or create a new object name; the blue/green replacement workflow described
later in this guide is still documented/planned rather than SQL-dispatched.
The mental model deliberately stops there. You never have to ask "which shard
does this row live on?" or "is
operator instance number 47 healthy?" Those questions exist, but they exist
inside diagnostic tools that you only reach for when something has gone wrong.
In normal life, you have workloads and views, and that's it.

### 3. A Tour Through a Single View

Let's follow one row from the moment it lands in a source table to the
moment it shows up in a materialized view. This is the most useful single
exercise for building intuition, so we'll do it slowly and look at each step.

Suppose you have a view defined like this:

```sql
CREATE MATERIALIZED VIEW revenue_by_region AS
  SELECT region, SUM(amount) AS revenue
  FROM   orders
  GROUP  BY region;
```

You also have a connector that brings rows from a Kafka topic called
`orders` into a RockStream-managed table. A new order arrives on that Kafka
topic: region `"NORTH"`, amount `42.00`. What happens next?

First, the **connector** for the orders source reads the new message off
Kafka. The connector knows it's part of an in-flight batch — what RockStream
calls an **epoch**. Maybe the epoch has been open for fifty milliseconds.
The connector decides whether to keep accumulating messages or to close the
current epoch and start a new one. This decision is driven by a cadence: in
this case, suppose the workload runs at a hundred-millisecond cadence, so
the connector waits another fifty milliseconds and then closes the epoch.
The result is a small batch of rows representing all the changes that
happened to the orders source during that hundred-millisecond window.

Next, the batch flows into the **operator graph**. For our view, the graph
is straightforward: scan, group, aggregate, store. The interesting trick is
that the operators don't process the entire orders table. They process only
the changes. The new order in the `"NORTH"` region produces a single delta:
"add 42.00 to the running total for `NORTH`." The aggregate operator reads
its existing state for `NORTH`, applies the delta, and writes the new total
back. If `NORTH` previously had revenue of 1058.00, it now has 1100.00. One
arithmetic operation, no scan of historical orders.

The new total gets written into the view's storage. RockStream stores
materialized view outputs in something called SlateDB, which is a key-value
store designed to live on object storage like S3. The write doesn't go to
S3 immediately; it accumulates in a local write-ahead log, and the entire
epoch's worth of writes — for this view, for other views, for the operator
state, for everything — gets bundled into a single atomic batch and
committed together. This is important: either everything from epoch 42
landed safely, or none of it did. There is no "halfway through epoch 42"
state visible to anyone.

Once the batch commits, the operator publishes a small piece of metadata
called a **frontier**: a promise that says "I have finished epoch 42 and
I will not send you any older updates." Downstream consumers — other
operators, other views, query gateways — read this frontier and know they
can safely process or query data at epoch 42 or earlier.

A user opens a dashboard and asks for `SELECT region, revenue FROM
revenue_by_region`. The query gateway picks up the request, looks at the
latest committed frontier, and reads the view's storage at that frontier.
The `NORTH` row shows revenue of 1100.00. The user sees the updated number.
From the moment the order arrived on Kafka to the moment the dashboard
updated, perhaps two hundred milliseconds passed. No full scans were
performed. Only one row was actually computed.

That, in slow motion, is what RockStream does. The remaining chapters take
each of these steps and dig into why they work the way they do.

---

## Part 2 — How Data Moves

### 4. Sources, Sinks, and Connectors

Data has to come from somewhere and go somewhere. In RockStream, *somewhere*
is always behind an interface called a **connector**. A connector is a small
piece of code that knows how to talk to one specific external system — Kafka,
PostgreSQL via logical replication, S3 as a stream of Parquet files, Iceberg
tables, a simple HTTP webhook, or RockStream's own internal write API for
when clients want to push rows directly. From the engine's point of view,
they all look the same: a connector produces deltas (insertions, deletions)
on the source side, or accepts deltas on the sink side. The engine doesn't
care whether those bytes started life in Kafka or in S3.

This uniformity is more important than it sounds. It means the same view
definition works no matter where the data comes from. If you start with
data in PostgreSQL and later move it to Kafka, your views don't need to
change. If you eventually want to write your view's output back to an
Iceberg table for downstream analytics, you add a sink connector and the
engine handles it.

There are two contracts on top of this idea that are worth knowing about.
The first is the **offset token**. When a connector reads from a source,
it remembers where it is by handing the engine an opaque blob of bytes —
its position. For Kafka, that might be a per-partition offset. For
PostgreSQL, it's a log sequence number. For Iceberg, it's a snapshot ID.
The engine never inspects this blob; it just stores it durably and hands it
back when the connector restarts, so the connector can resume from exactly
where it left off. This is how exactly-once semantics work: replay a
connector from its last committed offset and the engine deduplicates the
re-delivered messages using the same idempotency keys it always uses.

The second contract is **backpressure**. A connector that's reading faster
than the engine can process will eventually run out of buffer space. Rather
than dropping data or running out of memory, the engine asks the connector
to slow down. The connector exposes a "credits available" signal: when
credits are at zero, the connector stops fetching. When the downstream
clears its backlog, credits replenish and the connector resumes. This same
mechanism is what makes features like source gating (covered in chapter 21)
possible.

The takeaway is that you don't usually think about connectors much. In the
current SQL frontend, you create tables and views, while connector-managed
sources remain a Rust/control-plane concept rather than a `CREATE SOURCE` SQL
surface (`crates/rockstream-gateway/src/server.rs`,
`crates/rockstream-sql/src/frontend.rs`). For quick SQL-only experimentation,
the live ingest path today is `CREATE TABLE` plus `INSERT`/`UPDATE`/`DELETE`
over pgwire; `CREATE SOURCE ... FROM GENERATE ROWS` is documented/planned, not
currently dispatched.

**Dead-letter queue.** The DLQ is real as a Rust data model
(`rockstream-types::dlq`), but it is not yet a SQL-queryable catalog table and
there is no `ALTER SOURCE ... REPLAY/DISMISS DEAD_LETTER_QUEUE` dispatch in
`crates/rockstream-gateway/src/server.rs`. So the operational concept is
accurate, but the SQL snippets that earlier revisions of this guide showed were
ahead of the current frontend.

### 5. Deltas and Z-sets

To understand how RockStream maintains views efficiently, you need to
internalize one small idea: data is represented as a series of *changes*,
not as a series of *snapshots*. The change might be "this row was inserted"
or "this row was deleted" or, for an update, "this row was deleted and this
new row was inserted." Every change carries a weight: typically +1 for an
insertion or -1 for a deletion, although weights can be any integer when
the same row needs to be added multiple times.

This representation has a fancy name — a **Z-set**, short for "set with
integer weights" — but the idea is humble. Think of it as a shopping list
where each item has a quantity, and quantities can be negative. The
quantity `+1` next to "milk" means you bought one carton of milk. The
quantity `-1` next to "bread" means you returned one loaf of bread. Add up
the list and you get your net change in groceries.

The reason this is powerful is that almost every relational operation
commutes with addition of Z-sets in a clean way. If your view is
`SELECT region, SUM(amount) FROM orders GROUP BY region`, and you receive
a delta that says "one new order in NORTH with amount 42," you can compute
the change to the view by running the same SQL against just the delta:
group the delta by region, sum the amounts. The result is itself a delta —
"NORTH's revenue went up by 42" — that you can add to the view's current
state. You never touch the orders table itself. You never recompute the
whole sum. You take the previous answer and add the change.

There are edge cases that complicate this picture. Some operations (like
`SELECT DISTINCT`) are sensitive to the difference between a count of two
and a count of three in a way that makes the math trickier. Some
aggregates (like `MIN` and `MAX`) cannot be incrementally updated when a
row disappears — you need to know what the second-smallest value was, and
you have to keep that around. Some queries (recursive CTEs, certain
outer joins) need clever fixed-point machinery. RockStream handles the
supported cases today and documents recursion as planned surface, but the
underlying idea remains the same: maintain a view by
processing the *changes* to its inputs rather than by re-running the query.

For most workloads, the practical consequence is that view maintenance is
fast and cheap. A view fed by a high-volume stream stays current with
modest hardware because the work per incoming row is small and bounded.

### 6. Epochs: Batches of Work

You might wonder why RockStream doesn't process each delta one at a time.
After all, that's the cleanest mental model. The answer is efficiency.
Every time you commit work to durable storage, you pay a fixed cost — a
write to a log, a flush to S3, a metadata update. If you commit after
every row, you spend most of your time on overhead. If you commit after
every hundred thousand rows, you waste no time on overhead but your data
might be a long time stale. The middle ground is the **epoch**: a small
batch of work that gets processed and committed together.

An epoch is bounded by two things. There is a floor — the system won't
close an epoch earlier than `min_epoch_ms` (typically ten milliseconds) or
before it has accumulated `min_epoch_bytes` of data — because closing
epochs too often is wasteful. There is a ceiling — the system won't keep
an epoch open longer than `max_epoch_ms` (typically a second or two) —
because keeping epochs open too long makes the data stale. Inside that
range, the system chooses dynamically based on what it sees: a bursty
source closes epochs more often, a quiet source less often. Operators
generally don't tune these numbers themselves; they tell the system what
freshness target they want, and the system picks epoch sizes that meet it.

The magic of an epoch is what happens when it commits. Every change that
belongs to that epoch — every state update, every view output, every
shuffle buffer entry that needs to flow to the next operator, every piece
of metadata — gets bundled into a single atomic batch and written to
SlateDB in one shot. If the batch succeeds, all of it succeeds. If the
batch fails (a crash, a network blip), none of it took effect, and the
system simply replays the epoch from its last good frontier. There is no
"partially committed epoch" state. Either you see epoch 42 fully, or you
see the world as of the end of epoch 41.

This is what makes materialized views in RockStream feel coherent. When a
client queries the view at epoch 42, every row reflects the same set of
input changes. There's no anomaly where one row updated and another
didn't. The atomic epoch commit is the foundation that makes the rest of
the consistency story work.

---

## Part 3 — How the System Stays in Sync

### 7. Frontiers: Telling Other Operators You're Done

In a distributed system that runs without a global clock and without
distributed transactions, how do operators know when it's safe to do
something? How does a join operator know that it has seen all the inputs
for a particular epoch and can produce the join's output? How does a
query gateway know that the view it's about to read is consistent? The
answer, in RockStream, is the **frontier**.

A frontier is metadata. It is a tiny piece of information that says: "I
have processed everything up to epoch *N* and I promise never to send you
updates at any timestamp earlier than that." That's the whole concept.
Operators publish frontiers. Operators read other operators' frontiers.
When the frontier on an input advances, the consuming operator knows it
can act on data at that frontier without worrying that something earlier
will arrive later. Frontiers are how the system tells itself that work is
done.

This may sound abstract, so consider a concrete analogy. Imagine three
people in three offices, each one assembling a part of a larger product.
Each person has a whiteboard outside their door that they update with a
number: "I have finished part 1, 2, ... up to and including part *N*."
The fourth person, who assembles the final product, walks past all three
whiteboards before starting their work. If all three whiteboards say
"finished through part 42," the assembler can confidently start on part
42's final assembly. If one whiteboard says "finished through 41," the
assembler waits. Nobody is on the phone with anyone. Nobody is holding a
lock. The whiteboards do all the coordination.

That is exactly what frontiers do in RockStream. They replace the locks
and the two-phase commits and the global clocks that a more traditional
system would use. Because frontiers are just metadata, they propagate
cheaply. Because they are monotonic — they only ever advance, never
retreat — operators can act on them without fear of contradiction. And
because the protocol allows for inputs to be at different frontiers at
the same time, the system never has to stop the world to take a snapshot.

The thing to internalize is that frontiers are the system's way of
substituting metadata for coordination. The cost of checking a frontier
is roughly the cost of reading a number. The cost of holding a
distributed lock is the cost of a global handshake. The difference is
enormous, and it is the reason RockStream can scale to many shards
without the coordination overhead crushing the throughput.

### 8. The Frontier Protocol in Action

It is one thing to describe frontiers and another to see them at work.
Let's walk through three examples that cover the most important cases.

**The first example is a simple chain.** Suppose you have a source
called `orders`, a view called `revenue_by_region` that aggregates from
`orders`, and another view called `top_regions` that selects the five
highest-revenue regions from `revenue_by_region`. The data flows in one
direction. The `orders` source publishes a frontier as it closes epochs:
"finished through epoch 42." The `revenue_by_region` operator reads that
frontier, sees that input epoch 42 is complete, processes its own work
for epoch 42, and publishes its own frontier saying it's now done through
42. The `top_regions` operator reads that frontier and does the same. The
chain propagates progress forward one link at a time. Nothing is forced
to wait longer than the time it takes its inputs to finish.

**The second example is a join across two shards.** Suppose you join
`orders` (sharded by `region`) with `customers` (sharded by `customer_id`).
Because the join keys are different, the join operator has to receive
data from both shards and combine them. The join's input frontier is
literally the minimum of the two source frontiers. If `orders` is at
epoch 42 and `customers` is at epoch 41, the join can only safely
process up to epoch 41. As soon as `customers` advances to 42, the join
can advance too. There is no global lock; the join simply takes the
minimum and acts on it.

**The third example is the diamond.** Suppose you have a base table
called `events`, two views `events_by_user` and `events_by_session` that
both aggregate from `events`, and a third view `user_session_summary`
that joins the two. Now consider what happens when a single batch of
events arrives. Both `events_by_user` and `events_by_session` will
process the same epoch in parallel. They may not finish at the same
instant — one might take longer due to different work shapes — but they
both eventually publish a frontier for the epoch. The downstream join
reads both frontiers and waits for both to reach the same epoch before
processing. The result is that `user_session_summary` sees a snapshot in
which the two intermediate views are perfectly aligned with each other.
This is the *diamond consistency* guarantee. The frontier protocol
delivers it without the user having to ask for it and without any
coordination overhead.

### 9. Causal Time, Not Wall-Clock Time

Distributed systems and wall-clock time get along badly. Different
machines have slightly different clocks. Network delays mean that even if
the clocks agreed, the order in which events arrive at different
observers can disagree. Anyone who has chased a bug caused by clock skew
knows that "what time is it?" is a much harder question than it sounds.

RockStream sidesteps this problem by tracking progress as a *causal*
quantity rather than a *temporal* one. The epochs we have been discussing
aren't tied to wall-clock seconds. They are tied to causal positions in
the stream of input changes. Epoch 42 means "the state of the world after
the first forty-two batches of input." Two observers can disagree about
what time it is, but they cannot disagree about which input batches have
been processed. That is the basis on which the entire consistency story
is built.

There is a separate, parallel notion of time called the **watermark**,
which connectors emit alongside data to tell time-window operators when
they can close a window. A watermark says, "I believe no further events
with timestamps earlier than *T* will arrive." If you have a query like
"give me the count of events in each one-minute window," you need
watermarks to know when to finalize each window's count. Watermarks are
about *event time* — when did the event actually happen out there in the
world — while frontiers are about *processing progress* — how much input
has the system absorbed so far. They serve different jobs and they live
side by side.

For most everyday work, you don't need to think about either explicitly.
You write SQL, the engine maintains your views, the result is consistent
and reasonably fresh. The time-aware machinery comes out when you do
things like windowed aggregations or temporal joins. Even then, the
operator chooses what watermark policy to use, and the engine handles
the rest.

---

## Part 4 — Views and Workloads

### 10. Materialized Views: The Star of the Show

A materialized view in RockStream is a SQL query whose result is
continuously kept up to date. You define it once, the system computes it
once, and from then on the system maintains it incrementally as the
underlying data changes. Querying it is the same as querying a regular
table: `SELECT * FROM revenue_by_region` returns the current contents,
which reflect every input change that has been committed at the time of
the query.

This is similar in spirit to a PostgreSQL materialized view, but with
important differences. In Postgres, materialized views are refreshed by
running the defining query from scratch — either manually with `REFRESH
MATERIALIZED VIEW` or via triggers that fire on every write. The first
approach is slow; the second is expensive and serializes writes. In
RockStream, the view is maintained continuously and incrementally. There
is no per-write trigger. The view simply stays fresh as a side effect of
the engine processing input epochs. The gateway does also recognize
`REFRESH MATERIALIZED VIEW` (`crates/rockstream-gateway/src/server.rs`), but
that's an explicit command on top of the continuous incremental path, not
the primary maintenance mechanism.

Storage-wise, a materialized view occupies space in the system. The
view's contents are stored in a shard of SlateDB (or across many shards
for large views), keyed by the view's primary key. Querying the view
reads from this storage. Updating the view writes to this storage. The
size of the view on disk is roughly the size of its result set, plus a
small overhead for indexing.

The view also occupies *state*. State is the intermediate data that the
operators maintain to support incremental computation. An aggregate
needs to remember the running sum and count for each group. A join needs
to remember every row on each side, indexed by the join key, so it can
look up matches when new rows arrive. A windowed aggregate needs to
remember rows that haven't yet rolled out of the window. This state is
the price you pay for incremental maintenance. It is bounded by your
workload's memory limit (chapter 16), and the system reports how much
state each view consumes so you can plan.

You can query a materialized view at the latest committed epoch with
ordinary `SELECT`. Historical replay is currently exposed through
`SUBSCRIBE ... AS OF NOW WITH SNAPSHOT` / `SUBSCRIBE ... AS OF EPOCH <n>`
(chapter 14), not through plain `SELECT ... AS OF ...` syntax. You can
also subscribe to a view's output to receive a stream of changes as they
happen (chapter 14), which is useful when you want to forward updates to
external systems.

### 11. Inline Views: Just Named Queries

Not every view needs to be materialized. Sometimes you just want a named
query — a piece of SQL you can reuse — without paying for continuous
maintenance and storage. RockStream calls these **inline views**, and
they're defined with the standard `CREATE VIEW` syntax (no `MATERIALIZED`
keyword).

An inline view is essentially a macro. When the planner encounters a
reference to one, it substitutes the view's definition in place and
proceeds. There is no storage. There is no operator state. The view
contributes nothing to the cluster's state budget. It exists purely as a
textual convenience and a way to share query fragments across
materialized view definitions.

Use inline views when you want to:

- Compose a complex query out of smaller, named pieces without paying for
  intermediate materialization
- Present a stable interface to clients that abstracts over the details
  of the underlying tables ("`customer_profile` always projects these
  columns regardless of how we restructure the underlying tables")
- Build reusable filter or transformation fragments that get inlined into
  multiple materialized views

The trade-off is that the work is done at query time, not maintained
ahead of time. If many materialized views all reference the same inline
view, each of them does the work independently. If you find yourself
writing the same expensive subquery into multiple views, it might be
worth promoting it to a materialized view of its own so the computation
is shared.

The key thing to remember is that `CREATE VIEW` and `CREATE MATERIALIZED
VIEW` are very different. One creates a piece of stored, continuously
maintained data. The other creates a named SQL fragment. Both are useful;
they solve different problems.

### 12. Workloads: The Unit of Resource Policy

A **workload** is a named resource policy that groups related views under
a shared freshness SLO, memory budget, and priority. Workloads are
declared separately from the views that use them. A view references a
workload at creation time; the workload is not a container you deploy —
it is a constraint policy the system enforces.

A typical setup looks like this:

```sql
-- Declare the resource policy once.
CREATE WORKLOAD sales_analytics WITH (
    FRESHNESS_SLO_MS = 1000,
    MEMORY_LIMIT     = 107374182400,
    PRIORITY         = DEFAULT
);

-- Assume the input table already exists (for example via CREATE TABLE +
-- pgwire DML, or via a connector path outside today's SQL surface).

CREATE MATERIALIZED VIEW revenue_by_region
WITH WORKLOAD = 'sales_analytics'
AS
    SELECT region, SUM(amount) FROM orders GROUP BY region;

CREATE MATERIALIZED VIEW revenue_by_product
WITH WORKLOAD = 'sales_analytics'
AS
    SELECT product_id, SUM(amount) FROM orders GROUP BY product_id;
```

Both views are assigned to the `sales_analytics` workload. They share
the same freshness target and memory budget. The system builds a single
shared operator graph that fans out to both views, which is more
efficient than maintaining two independent computation paths.

The workload's freshness SLO — one second, in this example — applies
to all views in the workload. The system tunes its epoch sizing and
scheduling so that each view's output is at most one second behind the
input. The memory limit of one hundred gigabytes is shared across all
views' operator state. If one view is small and the other is large,
that's fine, as long as the total stays under the limit.

You create a new workload when:

- You want **independent SLOs**: one set of views needs sub-second
  freshness, another can tolerate five-minute lag, and you don't want
  the faster one to starve the slower one or vice versa.
- You want **independent resource budgets**: you don't want a single
  rogue view to consume all the cluster's memory at the expense of
  other views.
- You want **independent priorities**: different tenants or different
  applications should have different priorities under contention.
- You want **multi-tenant isolation**: different tenants get different
  workloads, possibly in different namespaces, with constraints enforced
  independently.

Views in the same workload share fate with respect to resource
constraints. They share the SLO, the budget, and the priority. If the
workload's memory limit is exceeded, all views in the workload are
affected. This encourages you to group related views with similar
operational requirements, which usually matches how teams actually
operate them.

If you omit `WITH WORKLOAD` when creating a view, the current SQL frontend
does not offer a schema-level `ALTER SCHEMA ... SET DEFAULT WORKLOAD`
fallback. Earlier design notes described that inheritance path, but the
live parser only recognizes the per-view `WITH WORKLOAD = '<name>'` clause
(`crates/rockstream-gateway/src/server.rs`: `parse_create_view_workload()`).

### 13. Composing Views Out of Views

Materialized views can read from other materialized views, not just from
base tables. This is what lets you build sophisticated derived datasets
out of small, focused pieces. A common pattern looks like this:

```sql
CREATE MATERIALIZED VIEW recent_orders AS
  SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days';

CREATE MATERIALIZED VIEW recent_revenue_by_region AS
  SELECT region, SUM(amount) FROM recent_orders GROUP BY region;

CREATE MATERIALIZED VIEW revenue_alerts AS
  SELECT region,
         revenue,
         CASE WHEN revenue > 1000 THEN true ELSE false END AS is_large
  FROM recent_revenue_by_region;
```

Each view does one thing. The first filters; the second aggregates; the
third classifies. From the user's perspective, each view stands on its own:
you can query `recent_orders` directly, or `recent_revenue_by_region`,
or `revenue_alerts`. From the system's perspective, the three views
form a chain. When new orders arrive, only deltas flow through the
chain. The filter view forwards only the orders that match its
predicate. The aggregate view applies the deltas to its running totals.
The final view derives one more column from those totals.

The frontier protocol coordinates the chain. Each view publishes its
frontier as it commits each epoch. Downstream views consume frontiers
from upstream views the same way they consume frontiers from base
tables. The fact that a view depends on a view rather than a table is
invisible at the protocol level.

There are two patterns worth highlighting because they come up often.
The **chain** pattern is the linear case above: view A → view B → view C.
Latency adds up across the chain — each step takes some time — but the
total is bounded by the cadence and the per-step processing time, so a
three-step chain at a 100 ms cadence still updates in roughly half a
second end to end. The **fan-out** pattern is when multiple downstream
views read from the same upstream view. Here the upstream view's
maintenance is shared, which is more efficient than maintaining the
same computation in multiple places.

The harder pattern is the **diamond**: two views share a common
upstream, and a third view joins those two. We covered the consistency
implications in chapter 8. The mechanical detail to understand is that
when a downstream view depends on multiple upstream views, the system
automatically tracks the dependency graph and ensures the downstream
sees a consistent snapshot. You write the SQL; the engine handles the
consistency.

### 14. Subscribing to View Changes

A materialized view is not just something you query on demand. You can
also **subscribe** to its output and receive a continuous stream of
changes as they happen. This is useful when you want to forward updates
to an external system (push notifications, a cache invalidation layer,
a downstream Kafka topic) or when you want to build an application that
reacts to data changes in real time.

The interface uses standard SQL over the PostgreSQL wire protocol:

```sql
SUBSCRIBE revenue_by_region;
```

This opens a long-lived connection that streams rows to the client as the
view updates. Each row includes the view's projected columns plus two
metadata columns: `mz_timestamp` (the epoch at which the change was
committed) and `mz_diff` (+1 for an insertion, -1 for a deletion).

**Starting with a snapshot.** If you want the current state of the view
followed by live updates, use:

```sql
SUBSCRIBE revenue_by_region AS OF NOW WITH SNAPSHOT;
```

This delivers the full current contents (as a batch of +1 rows) and then
continues with live deltas. It's the subscribe equivalent of "give me
everything, then keep me updated."

**Resuming from a position.** If your subscriber disconnects and
reconnects, it can resume from where it left off:

```sql
SUBSCRIBE revenue_by_region AS OF EPOCH 12345;
```

This skips everything before epoch 12345 and streams changes from there
forward. How far back you can resume is controlled by the in-memory
**change log** (`crates/rockstream-gateway/src/change_log.rs`, default
`CHANGE_LOG_MAX_ENTRIES = 10_000`). If you try to resume from a position
older than the retained earliest epoch, the subscribe handler returns
`RS-2006` (`crates/rockstream-gateway/src/subscribe_handler.rs`), not
`RS-2005`.

**Server-side filtering.** You can push filtering to the server so you
only receive the changes you care about:

```sql
SUBSCRIBE revenue_by_region WHERE region = 'NORTH';
```

Column projection works too — only named columns are sent over the wire.
This reduces network traffic and client-side processing for subscribers
that only need a subset of the view's output.

**Retention configuration.** Earlier design notes described a per-view
`CHANGE_RETENTION` option, but the current gateway/parser do not recognize
that clause anywhere. The live subscribe retention knob today is the
engine-level entry bound in `crates/rockstream-gateway/src/change_log.rs`.

---

## Part 5 — Freshness and Cost

### 15. Cadences: How Often Things Update

The **cadence** is how often a workload closes epochs and propagates
updates. It is the knob that controls how fresh your views are. Faster
cadences mean fresher data and more overhead. Slower cadences mean
staler data and less overhead. RockStream offers four cadence modes,
each suited to a different workload pattern.

**Deferred low-latency** is the default. Source connectors close epochs
frequently — typically every ten to a hundred milliseconds — and the
engine drains them as fast as the SLO requires. This is what you want
for dashboards, real-time analytics, and any workload where freshness in
the sub-second range matters but you don't need synchronous, in-line
updates. Most workloads run in this mode.

**Periodic** mode closes epochs at a fixed wall-clock interval — every
five seconds, every minute, every hour. This is what you reach for when
you want predictable batching, when the upstream produces data in
predictable bursts, or when you want to amortize cost across larger
batches. A periodic cadence trades freshness for throughput and is
appropriate for workloads where "fresh within a minute" is good enough
and you'd rather not pay for sub-second freshness.

**Calculated** mode lets the downstream cadence be inherited from the
demands of the consumers. If three downstream views all read from an
upstream view and the strictest of them needs one-second freshness, the
upstream runs at one-second cadence. If the strictest only needs ten
seconds, the upstream runs at ten seconds. This avoids the situation
where you over-maintain an upstream view "just in case" — the system
figures out what's needed based on actual consumer requirements.

You usually don't set cadence directly. You set a freshness SLO and the
system picks a cadence that meets it. The cadence shows up in
diagnostics and `EXPLAIN INCREMENTAL` output, so you can see what the
system chose, but you only override it when you have a specific reason.

### 16. SLOs: Telling the System What You Want

The key idea behind RockStream's operational interface is **intent-based
configuration**. You don't tell the system how to do its job. You tell
it what outcome you want, and the system figures out the how. The
configuration mechanism for this is the **SLO** (service-level
objective), which lives on the workload.

A workload declares its SLO in the `CREATE WORKLOAD` statement:

```sql
CREATE WORKLOAD my_workload WITH (
    FRESHNESS_SLO_MS = 1000,
    MEMORY_LIMIT     = 214748364800,
    PRIORITY         = DEFAULT
);
```

The **freshness SLO** is the headline number: how stale your views are
allowed to be. A target of one second means the system commits to
keeping the views' output within one second of the most recent input,
under normal conditions. The system tunes its epoch sizing, its
parallelism, and its scheduling to meet this number. If it can't meet
the target (because the input rate is too high, or the cluster is too
small, or there's a transient problem), the view transitions to a named
degraded state and reports the reason. You always know whether your SLO
is being honored, and if not, why.

The **memory limit** is a soft cap on how much arrangement state the
workload's views are collectively allowed to consume. This is the
storage for the intermediate state operators maintain (the indexed join
keys, the aggregated subtotals, the windowed buffers) — not the output
rows themselves. If a view's plan requires more state than the budget
allows, it is rejected at deploy time with a clear error. This protects
you from accidentally deploying a query that would consume a terabyte of
state when you only meant to maintain a small running total.

The **priority** decides which workload's views win when the cluster is
contended. A high-priority workload gets preferential access to cluster
resources; a low-priority workload yields. Most workloads are normal;
the priority knob is there for the cases where you really do need to
guarantee that one workload comes first.

The SLO is enforced; it's not a hope. The system reports a single
**SLO compliance** metric per view — a number between 0.0 and 1.0
representing the fraction of time the SLO was met over a rolling window.
This is the one number you put on a dashboard to answer "is my
view healthy?" Drill-down metrics tell you what to look at if it
dips, but the headline is always the same shape.

### 17. Self-Tuning by Default

Inside the SLO envelope, RockStream tunes itself. The mechanisms it
adjusts include:

- **Operator parallelism**: how many parallel instances of each operator
  to run. More parallelism means higher throughput but more overhead;
  the system raises parallelism when latency budgets start to slip and
  lowers it when there's slack.
- **Epoch sizing**: the floor and ceiling on epoch duration. The system
  shortens epochs to chase tight freshness targets and lengthens them
  to amortize overhead when there's no pressure.
- **Source throttling**: the rate at which connectors poll their
  upstream. If the engine starts to fall behind, it throttles the
  connector so the input rate stops outpacing the processing rate.
- **Placement and locality**: where each operator instance runs. The
  system prefers to co-locate operators that share data to avoid
  network shuffles, but it spreads them when locality conflicts with
  meeting the SLO.

The tuner runs continuously. It observes metrics — frontier lag, epoch
duration, queue depths, write rates — and adjusts the knobs in small
steps. You normally don't see it work. You see the SLO compliance number
stay near 1.0 and the system silently does whatever it takes.

When you want to override the tuner, you can. Every adaptive knob has a
manual setting. You might pin parallelism to a specific value if you're
benchmarking, or set an epoch ceiling lower than the SLO loop would
choose if you have a strict freshness requirement that's tighter than
the budget. Overrides survive across restarts and show up in workload
status / resource-usage inspection rather than a distinct `SHOW
WORKLOAD` command (`crates/rockstream-gateway/src/server.rs` dispatches
`SHOW WORKLOAD STATUS`, not `SHOW WORKLOAD`).

The philosophy is that tuning is the system's job, not yours, and
overrides are escape hatches for the cases where you know something the
system doesn't. In ordinary operation, you set an SLO and walk away.

### 18. The Trade-Offs Triangle

There are three things you can ask for in a streaming system: low
latency, high throughput, and low cost. You can have any two, but not
all three. This is a hard physical truth and RockStream cannot make it
go away. What it can do is make the trade-off visible and tunable.

If you want **low latency and high throughput**, you pay for it with
hardware and object-store bandwidth: many parallel workers, frequent
small commits, lots of network traffic. Set a tight freshness SLO and a
generous state budget, and the system delivers.

If you want **low latency and low cost**, you accept that throughput
will be limited: a tight freshness SLO with a constrained state budget
and a modest cluster size means you can only handle workloads up to a
certain input rate. The system tells you when that ceiling is reached by
surfacing an SLO violation, resource-budget warnings such as `RS-5018` /
`RS-5019`, or recovery/backpressure states elsewhere in the diagnostics
surface.

If you want **high throughput and low cost**, you accept higher latency:
a loose freshness SLO (say, one minute instead of one second) lets the
system batch much more aggressively, amortize commits across larger
units, and skip work that would otherwise be wasted. This is what
periodic cadence (chapter 15) is for.

RockStream's intent-based interface helps you make this trade-off
consciously. You write down what you want — freshness target, state
budget, cost cap — and the system tells you if those are achievable. If
they're not, you adjust your expectations, not the system's internals.

---

## Part 6 — When Things Get Interesting

### 19. Diamond Consistency Without 2PC

The diamond pattern came up briefly in chapter 8 and chapter 14. It
deserves its own chapter because it is one of the cases where RockStream
quietly does something hard.

Recall the setup: you have a base table, two views that both depend on
it, and a third view that joins those two. The classic concern is that
when an update lands in the base table, the two intermediate views
update at slightly different times. If the third view runs its join in
the gap between those two updates, it sees an inconsistent snapshot —
one view reflects the update, the other doesn't. The result is
gibberish.

PostgreSQL would solve this with a transaction that wraps all three
view refreshes together. Pg-trickle does something similar: it groups
the views into a `DiamondConsistency::Atomic` group and refreshes them
inside a single savepoint, so external observers see all three updates
or none. This works because Postgres has transactions and locks
internally; the system can pause writes while it refreshes.

In a distributed system, you can't afford to pause writes across many
shards while a downstream view refreshes. The latency would be terrible.
The throughput would collapse. The cluster's whole reason for being
would be defeated. So RockStream solves the diamond problem differently:
it uses the frontier protocol.

The third view's join operator reads frontiers from both intermediate
views. It only processes epoch *N* when both intermediates have
published a frontier of *N* or higher. Once that happens, the engine
guarantees the two intermediates are *both* at epoch *N* — by the
frontier promise — and so the join can safely combine their state. The
result is an output for epoch *N* that reflects a consistent snapshot
of the underlying base table at epoch *N*.

There is no lock. There is no two-phase commit. There is no pause in
the write path. The two intermediates may have committed at different
wall-clock times; that's fine. What matters is that their frontiers
align at the moment the downstream consumes them. The frontier protocol
substitutes metadata coordination for synchronous coordination, and the
result is the same correctness with vastly better scaling properties.

This is the secret sauce. Anywhere in your view graph that you have a
diamond — wherever two paths from a common source rejoin — RockStream
quietly enforces consistency through frontiers. You don't write
anything special. You just write the SQL and the engine handles it.

### 20. IMMEDIATE Mode and What It Means in a Distributed World

Pg-trickle has an IMMEDIATE refresh mode that maintains a view
synchronously within the same transaction as the source DML. When you
`INSERT` into a base table, the view updates before the `INSERT`
returns. This is wonderful for sub-millisecond freshness on a single
PostgreSQL machine, and it is the right answer for many OLTP-style
workloads where you want read-your-writes consistency.

The reason it works in pg-trickle is that PostgreSQL has transactions
that span all the relevant work and triggers that fire within those
transactions. The system can hold a lock on the view, copy the
transition tables into temp storage, run the delta computation, apply
the result to the view's storage, and release the lock — all inside the
same transaction. No other process sees a partially updated state. The
writer waits for the whole thing.

This does not generalize cleanly to a distributed cluster. To do the
same thing across many shards, you would need a distributed transaction
spanning the write and all the downstream views — and any shards they
touch. That means distributed locking, two-phase commit, and global
coordination on every write. RockStream is built specifically to avoid
those things, because they are what makes systems hard to scale beyond
a single machine.

RockStream does not support IMMEDIATE mode. Unlike pg-trickle, there is no
write-transaction hook in the distributed architecture — writes arrive through
source connectors polling CDC logs, not through triggers that fire inside an
open transaction. Holding an INSERT open across a connector process boundary,
an async operator graph, and a view commit would require a write-transaction
hook, a trigger layer, and a global write-sequence number: none of those exist,
and adding them would be incompatible with the async scheduling and causal-time
frontier model that the rest of the design depends on.

The right answer for workloads where you think you want IMMEDIATE is the
combination of a tight freshness SLO and the diamond consistency guarantee. If
you set your SLO to a hundred milliseconds, the system delivers updates within
a hundred milliseconds. Because every multi-input operator enforces frontier
alignment automatically, you get cross-view consistency for free wherever your
view graph joins two branches that share a common upstream. The practical
difference between "synchronous immediate" and "asynchronous within 100 ms with
cross-view consistency" is small for most workloads, and the scalability cost
of the former is large.
RockStream picks the trade-off that lets you scale; pg-trickle picks the
trade-off that gives you the strongest single-machine semantics. Both are right
for their use case.

### 21. Bulk Loads and Source Gating

A common operational problem: you need to load ten million historical
rows into a base table. If your view is being maintained at a
hundred-millisecond cadence, that load is going to trigger thousands of
intermediate refreshes, each one applying a tiny delta to your view's
state. You'll end up doing a lot of work that gets immediately undone by
the next batch. You'd much rather pause maintenance, do the bulk load,
and resume — letting the system catch up with one large coalesced
refresh at the end.

Pg-trickle has a feature for exactly this called **source gating**. You
gate the source table; the scheduler skips downstream view refreshes
that depend on the gated source; you bulk-load; you ungate the source;
maintenance resumes. RockStream supports the same pattern, built on top
of the credit-based backpressure system you met in chapter 4.

The mechanism is: a gated source has its credits set to zero. With zero
credits, the connector cannot emit new epochs into the engine. The
in-flight epoch (if any) is allowed to complete, but no new ones start.
Downstream views' frontiers stop advancing because their inputs aren't
producing new data. The bulk load proceeds against the underlying
source storage. When you ungate, credits restore, the connector picks
up where it left off, and the accumulated changes flow through as
either one large epoch or a small number of large epochs — much more
efficient than processing them in small bites.

At the mechanism level, this is real: a connector with zero credits stops
emitting new epochs. But the current pgwire surface does **not** dispatch
`PAUSE SOURCE` / `RESUME SOURCE`, so the SQL below is target-shape
documentation rather than a runnable command today:

```sql
PAUSE SOURCE my_source;
-- bulk load proceeds here, perhaps via COPY or direct ingest
RESUME SOURCE my_source;
```

There is also a more sophisticated variant called **watermark gating**
that pauses maintenance until multiple sources' event-time watermarks
align within a tolerance window. This is useful when you have a view
that joins data from multiple ETL-fed sources and you want to wait for
them all to catch up to roughly the same event time before producing
output. The mechanism is the same — credits suppress epoch emission —
but the trigger is alignment rather than an explicit pause.

The takeaway is that the engine already has the machinery to support
operational features like gating. The user-facing API is small and the
underlying mechanism is the same one that handles backpressure in
ordinary operation.

### 22. Reading Historical Data

A materialized view is a moving target. Most of the time you want the
latest committed state, and that's what `SELECT * FROM my_view`
returns. Today, the SQL surface for historical replay is narrower than
some earlier drafts of this guide implied:

```sql
SUBSCRIBE my_view AS OF NOW WITH SNAPSHOT;
SUBSCRIBE my_view AS OF EPOCH 12345;
```

Those are the two real historical/streaming read forms parsed by
`crates/rockstream-gateway/src/subscribe_parser.rs`. Plain `SELECT * FROM
my_view AS OF EPOCH ...` and `AS OF TIMESTAMP ...` are not dispatched by
`crates/rockstream-gateway/src/server.rs`, and `rockstream-sql` has no
lowering support for them.

Replay depth is currently bounded by the in-memory change log entry
window (`CHANGE_LOG_MAX_ENTRIES`) rather than a per-view SQL `retention`
option. Falling behind that window returns `RS-2006`
(`crates/rockstream-gateway/src/subscribe_handler.rs`).

For ad-hoc analytics over a longer history, export to Kafka and use a
downstream writer. The prior cold-tier connector surface is removed; see
[connector migration](connector-migration.md).

### 23. Recursive Queries and Graphs

Some queries don't fit the straightforward "scan, filter, join,
aggregate" mold. Transitive closure of a graph. Hierarchical roll-ups
through a tree of categories. Reachability in a network. These all
require **recursion**: the answer depends on the answer applied to
itself.

In SQL, you express recursion with `WITH RECURSIVE`:

```sql
WITH RECURSIVE descendants AS (
  SELECT id, parent_id FROM categories WHERE id = 42
  UNION
  SELECT c.id, c.parent_id
  FROM   categories c
  JOIN   descendants d ON c.parent_id = d.id
)
SELECT * FROM descendants;
```

This says: start with category 42, then keep adding its children, and
its children's children, until no new rows come in. Doing this
incrementally — keeping the answer fresh as the underlying tree
changes — is genuinely harder than maintaining a flat aggregate. New
edges might create new reachability paths. Deleted edges might remove
paths. The system has to figure out which paths in the answer are
still valid after every change.

Those are still important design goals, but they are not yet part of the
live SQL frontend. `crates/rockstream-sql/src/lower.rs` reports
`LogicalPlan::RecursiveQuery` as unsupported, so `WITH RECURSIVE`
belongs in the documented/planned surface rather than the implemented
surface today. The semi-naive/DRed discussion remains useful as the
architectural direction for future recursion support.

---

## Part 7 — Operations Without Drama

### 24. The One Signal: SLO Compliance

If you only look at one metric for your view, it should be **SLO
compliance**. This is a number between 0.0 and 1.0 that represents the
fraction of time the view met its freshness target over a rolling
window (by default five minutes). A value of 1.0 means the SLO has
been met for the entire window. A value of 0.8 means it was missed 20%
of the time. A value of 0.0 means it was never met.

You put this single number on your dashboard, one per view. If
they're all at 1.0, everything is fine and you don't need to look at
anything else. If one dips, you click into it and the system shows you
the **degradation reason** or related warning/error code. The current
source tree has concrete budget and recovery signals such as `RS-5018`,
`RS-5019`, `RS-3602`, and `RS-3603`; the exact public taxonomy is still
evolving, but the intent is the same: you shouldn't have to infer the
problem from raw counters alone.

This is a deliberate operational stance. Most monitoring systems give
you a thousand metrics and let you figure out which ones matter.
RockStream gives you one metric per view that summarizes whether
your intent is being honored, and then drill-downs that tell you what
to do if it's not. The aim is that an on-call engineer with no specific
RockStream training should be able to answer "is my workload healthy?"
within ten seconds and "what's wrong?" within a minute.

### 25. Failure and Recovery

Things break. Workers crash. Networks partition. Object storage has
brownouts. RockStream is designed to recover from all of these without
manual intervention and within bounded time.

When a **worker dies**, its shards are re-leased to other workers. The
SlateDB single-writer mechanism prevents split-brain: the old writer's
manifest is fenced, so even if it comes back online it cannot commit.
The new writer opens the shard, reads its last committed frontier,
replays any WAL entries beyond the last checkpoint, and resumes
processing from there. The pipeline enters a `RECOVERING`
state during this process and returns to normal running operation when the frontier
catches up. The recovery time is bounded by the cluster's recovery
SLO, typically under sixty seconds.

When **object storage is slow**, the system backs off. Operators that
can't flush their writes accumulate them in local buffers. Connectors
throttle their input rate so the buffers don't grow unbounded. The
view reports a degraded state with the reason `OBJECT_STORE_SLOW`.
When storage recovers, the buffers drain and the view returns to
normal. No data is lost, but freshness lags during the brownout.

When a **network partition** isolates a worker, the partitioned worker
detects its isolation through missing heartbeats and self-fences,
giving up its shard leases voluntarily. The control plane reassigns
the shards to a still-connected worker. This is critical: a
partitioned-but-alive worker that didn't self-fence could race a new
owner and cause divergent commits. The self-fencing rule prevents this
class of bug entirely.

For the operator, the visible effect of all these failures is the same:
SLO compliance dips during recovery and rises again afterward. The
degradation reason tells you what kind of failure happened. You usually
don't have to do anything; the system handles it. When you do need to
intervene — say, to add capacity if a workload has outgrown the cluster
— the documentation tells you exactly what action each reason calls for.

### 26. Scaling Up, Scaling Down, Scaling Out

The same RockStream binary serves three deployment tiers, and you can
move between them additively without rewriting your configuration or
migrating your data.

**Tier 1** is a single process on your laptop or a CI runner. You point
it at a local filesystem directory and you're up and running. The
control plane, the worker, the frontier aggregator, and the gateway are
all in one process. Latency is low because everything is in-memory or
on local disk. This is what you use for development, evaluation, and
small test workloads.

**Tier 2** is a single host configured to use shared object storage.
You launch one process with `--role=all --storage=s3://...` and now your
data survives across restarts and is durable beyond any single machine.
Within this single host, you can run as many worker threads and shards
as the hardware supports. This tier is suitable for small production
workloads where one machine is enough.

**Tier 3** is the full multi-host cluster. Control plane processes run
on a few nodes (typically three to five for high availability); worker
processes run on as many nodes as you need for your workload. You add
nodes online, the control plane discovers them and admits them, and
workloads start using the new capacity. You remove nodes by draining
them gracefully and the shards they own get migrated elsewhere.

The same data files written in Tier 1 against MinIO can be opened in
Tier 3 against S3. There is no migration step because there is no
node-local state. The deployment ladder is purely additive: you add
machines, you add roles, you don't reconfigure.

This matters because it means you can start small. You don't have to
design a cluster on day one. You build your workloads against a laptop
deployment, you deploy them to a single production host when you're
ready, and you scale out to a cluster when the load demands it. The
mental model and the SQL never change.

### 27. Multi-Tenancy: Namespaces and Quotas

If multiple teams share a cluster, you'll want each team's workloads to
be isolated from the others. RockStream supports this through
**namespaces**, which are roughly analogous to PostgreSQL databases:
each namespace has its own catalog of tables, views, and workloads.

Access control is also per-namespace. A user with `pipeline_owner`
rights on namespace A can deploy, alter, and drop views in
namespace A, but cannot even see what exists in namespace B. The current
gateway surface uses `CREATE NAMESPACE` and session namespace switching
through `SET search_path = <namespace>` (`crates/rockstream-gateway/src/server.rs`);
it does not expose a separate "namespace quotas" SQL statement family in
the gateway today.

For most single-team deployments, you don't need to think about
namespaces; there's a default namespace and everything goes there. For
multi-tenant deployments, namespaces are the boundary you administer.

### 28. Schema Evolution and View Replacement

Source schemas change over time. Columns get added, types get widened,
fields get renamed. RockStream classifies each schema change and handles
it accordingly:

- **Compatible changes** (adding a nullable column, widening a numeric
  type) are applied automatically. Existing arrangements keep their old
  encoding; reads project the new column as NULL or a default until fresh
  deltas rewrite the rows.
- **Breaking changes** (rename, drop, narrow, change a join key type)
  require explicit action. At the current SQL/frontend boundary, the
  concrete guarantee is that incompatible view changes surface `RS-1002`.

At the current SQL/frontend layer, the guaranteed behavior is narrower:
incompatible view changes return `RS-1002` from the SQL/frontend path, and
the blue/green replacement workflow is still documented/planned rather
than dispatched. `crates/rockstream-gateway/src/server.rs` has no
`replacement` or `schema_evolution` statement family, even though
`rockstream-types::error_code` already reserves `RS-3607`/`RS-3608`/`RS-3609`
for the future clone/backfill/flip lifecycle.

### 29. View Lifecycle States

A materialized view isn't just "running" or "stopped." The current data
model in `crates/rockstream-types/src/view_lifecycle.rs` already defines a
small lifecycle vocabulary, even though the richer `SHOW VIEW STATUS` SQL
surface described in earlier drafts is not yet dispatched by
`crates/rockstream-gateway/src/server.rs`.

| State | What it means | What to do |
|---|---|---|
| `RUNNING` | The view is actively processing deltas. | Nothing. |
| `BACKFILLING(from epoch N)` | Initial or catch-up backfill is running. | Wait for the frontier to advance. |
| `OVER_BUDGET_RELAXED` | State-budget pressure has relaxed the freshness target. | Raise `MEMORY_LIMIT` or revise the query/workload. |
| `PAUSED` | The view is paused in the lifecycle model. | Resume through the control-plane path that paused it. |

Every state transition is recorded in the audit log with the metric or
event that caused it. The SLO compliance number dips together with the
state transition so your dashboard tells the same story.

The `ViewStatus` / `BackfillStatus` structs are already defined in
`rockstream-types/src/view_lifecycle.rs`, but `SHOW VIEW STATUS` and
`SHOW BACKFILL STATUS` are not yet wired into the gateway dispatch table.

### 30. Diagnosing Your Views

When a view isn't performing as expected, `EXPLAIN INCREMENTAL` is your
primary diagnostic tool. It has three output levels:

**Level 1 — Default summary.** Shows the operator tree with per-operator
statistics: epoch time, shuffle depth, merge law used, parallelism. Uses
human-readable units (GB, MB) and visual indicators (✓/⚠/✗) for SLO
compliance. No internal IDs or antichain notation.

```sql
EXPLAIN INCREMENTAL revenue_by_region;
```

**Level 2 — VERBOSE.** Adds merge-law annotations, combiner status,
per-operator shard counts, parallelism utilisation, workload detail
(memory used vs. limit), and frontier timestamps. For operators
diagnosing resource or performance issues.

```sql
EXPLAIN INCREMENTAL VERBOSE revenue_by_region;
```

**Level 3 — ANALYZE.** Adds live per-operator runtime statistics
collected over the last 60 seconds: rows processed, state reads,
RMW-avoidance ratio, hot groups, p99 latency, decode errors, and DLQ
entries. Requires a live round-trip to workers.

```sql
EXPLAIN INCREMENTAL ANALYZE revenue_by_region;
```

**Cost preview.** Before deploying a new view, you can estimate its cost
without executing:

```sql
EXPLAIN INCREMENTAL ESTIMATE
SELECT region, SUM(amount) FROM orders GROUP BY region;
```

This reports predicted state size and estimated operator cost for the
query you pass to the frontend. The current gateway path does not have
an interactive confirmation / `WITHOUT CONFIRMATION` workflow around
`CREATE MATERIALIZED VIEW`; that text in earlier revisions described a
planned control flow, not today's parser/dispatch behavior.

### 31. Session Ergonomics: Read-After-Write and Staleness

RockStream is an eventually-consistent streaming system, but it defaults
to **read-your-writes consistency** for the common OLTP case. When a
session writes a row (via the internal source connector) and then
queries a view that depends on that source, the system automatically
waits for the view's frontier to advance past the written epoch before
returning the query result. No application-level coordination needed.

This is the default behavior. You write a row, you query the view, and
your own write is visible. The system tracks this per-session via
`session_wait_for`.

**Opting out.** Analytical sessions that don't need read-your-writes can
disable it:

```sql
SET rockstream.session_wait_for = off;
```

There is no per-query `/*+ ALLOW_STALE */` hint parser in the current
gateway; session-level `SET rockstream.max_staleness = ...` is the real
staleness control surface.

**Cross-session coordination.** When one service writes and a different
service reads, use a write fence token:

```sql
-- Writer session: get a fence after writing
SELECT rockstream.write_fence() AS fence;

-- Pass 'fence' to the reader service via your application protocol

-- Reader session: wait for that specific write
SELECT * FROM order_summary
WHERE rockstream.after_fence('{"table_name":"orders","source_epoch":42}');
```

**Bounded staleness for analytics.** Sessions that accept a bounded-stale
snapshot without blocking:

```sql
SET rockstream.max_staleness = '5s';
```

This disables implicit `wait_for` and accepts any snapshot within the
given age. Useful for dashboards and analytical queries where "close
enough" is fine and you don't want to block on frontier advancement.

### 32. Resource Visibility and Alerts

You can always see what your cluster's resources are doing. The
2026-07-11 caveat about this section being "target shape only" is now
obsolete: these statements are dispatched in
`crates/rockstream-gateway/src/server.rs` and backed by
`crates/rockstream-gateway/src/catalog_stubs.rs`:

```sql
-- Cluster-wide summary
SHOW CLUSTER RESOURCE USAGE;

-- Per-view detail
SELECT * FROM rockstream_catalog.view_resource_usage;

-- Per-workload detail
SELECT * FROM rockstream_catalog.workload_resource_usage;
```

**Proactive thresholds.** The system doesn't wait for you to check
dashboards. When resource utilisation crosses warning or critical
thresholds, it emits actionable notices:

- At **80% utilisation**: `RS-5018` proactive `NOTICE` with the resource
  that's approaching limits and suggested actions.
- At **95% utilisation**: `RS-5019` `WARNING` indicating imminent
  degradation.

These fire automatically and appear in the SQL session, the audit log,
and any configured alerting integration.

**Actionable errors.** RockStream errors are designed to include
human-readable next-step guidance. You never get a raw error without
helpful context: the message tells you what happened, why it happened,
and what to try next.

### 33. Background DDL and Schema-Level Lifecycle

Earlier revisions of this guide described `SET BACKGROUND_DDL`,
`WAIT FOR MATERIALIZED VIEW ... TO BE READY`, and schema-level
`ALTER SCHEMA ... PAUSE/RESUME` commands. None of those statement shapes
appear in the current gateway dispatch table, so treat them as planned
surface rather than runnable SQL today.

---

## Part 8 — Reference Material

### 34. Glossary

**Arrangement** — The internal data structure an operator maintains to
support incremental updates. Think of it as an indexed table that lives
inside the engine, keyed in exactly the way the operator needs. A join
operator, for example, keeps one arrangement per join side: the left
side is indexed by the join key so that when a right-side row arrives,
the operator can instantly look up all matching left-side rows without
scanning anything. An aggregation operator keeps one arrangement per
group key, storing the running partial result (e.g. the current `SUM`
and row count). Arrangements are what make incremental maintenance fast
— rather than re-scanning source data on every update, operators read
and update only the relevant entries. Arrangement size is the primary
cost of a materialized view; `EXPLAIN INCREMENTAL VERBOSE` shows you
exactly how much each operator is using.

**Backpressure** — The mechanism by which a slow consumer tells a fast
producer to slow down. Without backpressure, a connector reading from
Kafka faster than the engine can process would eventually exhaust memory.
RockStream handles this via a credit system: each connector has a credit
balance representing how many bytes of data it is allowed to hand to the
engine. As the engine processes data, it replenishes credits. When
credits reach zero, the connector pauses and waits. Backpressure is
entirely automatic; operators never configure it. Its visible effect is
a slightly lower observed input rate when the engine is under pressure
— which is exactly the right behavior.

**Cadence** — How often a workload closes epochs and commits their
results to storage. Cadence is the knob between freshness and cost: more
frequent commits mean fresher data but more per-row overhead; less
frequent commits mean staler data but more work coalesced per commit.
RockStream offers three modes. *Deferred low-latency* (the default)
closes epochs as fast as the SLO requires, typically every ten to a few
hundred milliseconds. *Periodic* closes epochs at a fixed wall-clock
interval regardless of load. *Calculated* derives the cadence from what
downstream consumers actually need, so you don't over-maintain an
upstream view. In practice you set a freshness SLO and let the system
choose its cadence.

**Change retention** — How long a view's change history is kept
available for subscribers to resume from after a disconnect. In the
current gateway implementation, this is bounded by the in-memory change
log entry count (`CHANGE_LOG_MAX_ENTRIES`) rather than a per-view SQL
`CHANGE_RETENTION` clause. If a subscriber asks for an epoch older than
the retained earliest epoch, the subscribe path returns `RS-2006` and
the client must restart with a fresh snapshot.

**Connector** — A piece of code that bridges one external system and the
RockStream engine. Source connectors translate external events (Kafka
messages, Postgres CDC rows, S3 Parquet files, HTTP webhook payloads)
into the engine's internal delta format. Sink connectors do the reverse:
they accept committed view deltas and write them to external systems
(Kafka topics, Iceberg tables, downstream databases). All connectors
implement the same interface — opaque offset tokens for exactly-once
replay and a backpressure credit signal — so the engine doesn't care
what's on the other end. RockStream ships built-in connectors for Kafka,
PostgreSQL CDC, S3, Iceberg, and an internal direct-write path.
Third-party connectors can be built against the published SDK.

**Coordination group** — The structural consistency guarantee the engine
enforces when a downstream view joins two or more views that share a
common upstream source (the diamond pattern). The classic case: two
views both read from the same upstream source, and a third view joins
those two. Without coordination, the third view could see one
intermediate at epoch 42 and the other at epoch 41, mixing data from
different instants. RockStream enforces the invariant automatically
through the frontier protocol: a multi-input operator computes the meet
(minimum) of all its input frontiers and will not process epoch 42
until every input has published a frontier of at least 42. This is a
structural property of every multi-input operator, not a user-declared
API or configuration option. There is no `CREATE COORDINATION GROUP`
syntax; you write the SQL and the guarantee follows from the operator
graph.

**Dead-letter queue (DLQ)** — A per-source safety net for records that
failed to decode. The DLQ exists today as a Rust data model, but not yet
as a SQL-queryable `rockstream_catalog.dead_letter_queue` table or a
`REPLAY` / `DISMISS` SQL command family. So the concept is real; the SQL
management surface remains planned.

**Delta** — A description of change, not a description of state. Rather
than saying "the NORTH region's revenue is 1100," a delta says "the
NORTH region's revenue went up by 42." This is the fundamental
representation RockStream uses internally: instead of storing snapshots
and recomputing from scratch, operators receive deltas and apply them to
their existing state. A delta is expressed as a Z-set — a set of rows
each carrying an integer weight: +1 for an insertion, -1 for a
deletion. An update is modeled as a -1 row (old value) plus a +1 row
(new value). The reason this matters is that almost every relational
operation can be applied to a delta to produce a new delta — a filter
applied to a delta produces a delta of filtered changes, an aggregate
applied to a delta produces a delta of aggregate changes. This property
is what makes the entire incremental engine work.

**Epoch** — An atomic batch of input changes that gets processed and
committed together. When an epoch ends, every change in that epoch —
every view row update, every arrangement state write, every piece of
operator metadata — is written to SlateDB as a single atomic batch.
Either all of it lands, or none of it does. This all-or-nothing property
is what lets the system recover from crashes without losing data or
producing partial results. Epochs also bound the freshness: if your
SLO is 200 ms and the epoch is 100 ms, the worst-case lag from input
to visible output is roughly 300 ms (one full epoch plus processing
time). Short epochs improve freshness; long epochs reduce overhead. The
system tunes epoch duration to stay inside the SLO.

**Frontier** — A lightweight, monotonically advancing progress marker
published by each operator. An operator's frontier says: "I have
finished processing everything up to epoch N, and I will never produce
updates at any epoch earlier than that." Downstream operators read their
inputs' frontiers before they act — they only process epoch N when all
their inputs have published a frontier of N or higher. This substitutes
cheap metadata reads for expensive distributed locking. Frontiers are
monotonic: they can only ever advance, never retreat. That monotonicity
is what lets consumers act on a frontier without fear of contradiction.
The frontier protocol is the core coordination mechanism of the entire
engine.

**Inline view** — A view created with `CREATE VIEW` (without
`MATERIALIZED`). It is stored as a SQL fragment in the catalog and
expanded — inlined — into any query or materialized view that references
it at plan compilation time. It consumes no storage, no arrangement
state, and no operator instances. Nothing about an inline view runs
continuously; it is purely a macro that saves copy-pasting the same SQL.
The trade-off is that every materialized view that references the same
inline view independently performs the inline view's computation. If
that computation is expensive and shared across many materialized views,
it is more efficient to promote it to a materialized view of its own so
the computation is shared.

**Lifecycle state** — A named, machine-readable label describing what a
materialized view is currently doing and whether operator attention is
needed. The current `rockstream-types::view_lifecycle::ViewState` enum is
small but real: `RUNNING`, `PAUSED`, `OVER_BUDGET_RELAXED`, and
`BACKFILLING(from epoch N)`. A richer `SHOW VIEW STATUS` SQL surface is
documented, but not yet gateway-dispatched.

**Materialized view** — A SQL query whose result is continuously
maintained by the engine as the underlying source data changes. Unlike
a table, you never write to it directly — the engine writes to it for
you, applying deltas as inputs arrive. Continuous maintenance is the
main model, although the gateway also recognizes `REFRESH MATERIALIZED
VIEW`. It occupies storage proportional
to its result size, plus arrangement state for the intermediate
computation. Querying it is no different from querying a table. The
cost model is inverted compared to a regular query: you pay upfront at
write time (maintaining the view incrementally) and each read is cheap.
This is the right trade-off when many clients read the same result
frequently.

**Namespace** — An isolation boundary that groups related catalog
objects, workloads, and users together, roughly analogous to a database
in PostgreSQL. Access control is scoped to namespaces — a user can have
full rights in namespace A and no visibility into namespace B. The
current gateway exposes namespace creation and switching via `CREATE
NAMESPACE` and `SET search_path = <namespace>`. For single-team
deployments the default namespace is fine; namespaces become important
when multiple teams share a cluster.

**Quota** — A hard or soft resource cap applied at the workload or
namespace level, preventing any single workload or tenant from consuming
more than its share of the cluster. The most important quota is the
memory limit (`MEMORY_LIMIT`), which caps how much arrangement state the
views in a workload can collectively hold. That memory-budget path is
concrete today, and when a workload approaches its limit the system emits proactive warnings
(RS-5018 at 80%, RS-5019 at 95%). If the limit is exceeded, the system
degrades freshness gracefully rather than crashing or dropping data.

**Shard** — A partition of the cluster's state. Each shard is a
self-contained SlateDB instance, owned by exactly one worker at any
given moment. Sharding lets RockStream scale beyond a single machine:
different shards can live on different workers, and work is parallelized
across them. When a worker dies, its shards are reassigned to other
workers. The number of shards for a workload can be expanded through a
rebalance. More shards mean more parallelism and higher throughput, but
also more overhead from cross-shard shuffles and metadata management.
The system manages shard placement automatically.

**Sink** — A destination for a view's output. Sinks are optional: you
can query a materialized view directly without ever configuring one.
When you want to forward view output to an external system — writing
updated rows to a Kafka topic, appending new partitions to an Iceberg
table, pushing changes to a downstream database — you add a sink
connector. Sinks implement the same exactly-once commit protocol as
sources: they receive `prepare`/`commit`/`abort` calls aligned with the
engine's epoch boundaries, so every external write is atomic and
idempotent with respect to crashes.

**SLO (service-level objective)** — A workload-level declaration of the
outcome you want, rather than a configuration of how to achieve it. The
freshness SLO says "I want my views to be at most N seconds behind their
inputs." The memory limit says "I don't want to spend more than X GB on
arrangement state." The priority says "when the cluster is contended,
this workload matters more than lower-priority ones." The engine then
tunes its internal knobs — epoch size, parallelism, source throttling,
shard placement — to honor those targets. SLO compliance (a 0.0–1.0
metric per view) tells you whether the target is being met. When it
falls below 1.0, the diagnostics surface is meant to tell you what to
change — today via workload/resource status, budget warnings, and
related recovery/error codes rather than a single finalized public enum.

**Source** — The input side of a pipeline. Sources and their connectors
are real engine concepts, but `CREATE SOURCE` / `PAUSE SOURCE` /
`RESUME SOURCE` are not part of today's pgwire SQL surface. The current
SQL-reachable ingest path is `CREATE TABLE` plus DML, while connector
sources live in the Rust/control-plane layer.

**Subscribe** — A long-lived SQL connection that streams view changes
to the client as the engine commits them, instead of requiring the
client to poll with repeated `SELECT` statements. A subscriber sees
every change to the view in real time, with `mz_timestamp` / `mz_diff`
metadata. Subscribers can apply server-side filters and column
projections, and can resume from a past epoch as long as it is still
within the retained change-log window.

**View replacement** — The documented zero-downtime upgrade workflow for
materialized views. The idea is real and the error-code registry already
reserves blue/green lifecycle codes, but the SQL commands (`CREATE
REPLACEMENT ...`, `APPLY REPLACEMENT`, `DISCARD REPLACEMENT`, `SHOW
REPLACEMENT STATUS`) are not yet wired into the current gateway.

**Watermark** — Event-time metadata emitted by source connectors to tell
time-window operators when they can safely close a window. A watermark
says "I believe no further events with event timestamps earlier than T
will arrive." This is distinct from a frontier, which tracks processing
progress (how many epochs have been committed). Watermarks track event
time — when did something actually happen out in the world — while
frontiers track how far the engine has advanced through the input
stream. Both are needed for correct windowing: you need the watermark
to know when to finalize a window's result, and the frontier to know
when that finalized result is visible to consumers. For non-windowed
queries, watermarks are irrelevant and the engine ignores them.

**Workload** — A named resource policy that groups related materialized
views under shared operational intent. A workload specifies a freshness
SLO, a memory limit, and a priority. Workloads also form the unit of
multi-tenancy: different workloads have independent resource budgets and
priorities. The live view-assignment syntax is `WITH WORKLOAD =
'<name>'`; schema-level default-workload inheritance is still
documented/planned rather than implemented in the gateway parser.

**Write fence** — A cross-session coordination token that lets one
service tell another "wait until you can see the write I just made."
Within a single SQL session, RockStream already handles read-after-write
automatically — a `SELECT` issued after an `INSERT` in the same session
will always see the inserted row reflected in maintained views, with no
extra work. The fence is for cross-service handoffs: service A inserts a
row, calls `rockstream.write_fence()` to get a token, passes the token
to service B (via a message queue, a REST response, a job record), and
service B uses the token to wait only until that specific write is
visible. Without the fence, service B would have to guess how long to
sleep or poll repeatedly. With the fence, it waits exactly as long as
necessary and no longer.

**Z-set** — A multiset where each distinct row carries an integer weight.
The weight represents how many times that row logically appears: +1
means inserted, -1 means deleted, +2 means the same value appears in
two upstream source rows. Z-sets are the fundamental unit of data that
flows between operators in RockStream. The math works out cleanly:
applying an SQL operation to the union of a current Z-set and a delta
Z-set produces the same result as applying the operation to the current
Z-set and then merging in the result of applying the operation to the
delta alone. This distributivity property is what allows the engine to
process only changes rather than full tables on every update.

### 35. Decision Trees

**"Should I create a new workload?"**

Start by asking three questions: Do these views need the same freshness
target? Should they share a memory budget? Should they have the same
priority under contention? If the answer to all three is yes, put them
in the same workload — the engine will build a shared operator graph for
them, which is more efficient than independent pipelines. If any answer
is no, use separate workloads. The most common reason to split is
divergent SLOs: a real-time dashboard might need sub-second freshness
while a nightly reporting view is fine with five-minute lag. Mixing
them in one workload forces the system to run at the stricter target,
wasting resources for the views that don't need it. Separate workloads
also protect views from each other: if one workload's views consume a
lot of memory, only that workload degrades; the other continues normally.

**"Which cadence should I pick?"**

You almost certainly shouldn't pick one directly. Set a freshness SLO
and let the system choose. The SLO-driven tuner picks epoch sizes and
cadence automatically and responds to changing load in real time. That
said, if you have a specific reason to override: use *periodic* when
your data arrives in predictable bursts (nightly ETL loads, hourly
batch jobs) and you'd rather amortize processing into one large commit
than many tiny ones. Use *calculated* when you have a chain of views at
different freshness levels — the upstream view runs at the cadence the
strictest downstream consumer actually needs, not faster. For every
other case, deferred low-latency plus a freshness SLO is the answer.

**"Does RockStream support IMMEDIATE mode?"**

No. IMMEDIATE mode (committing a view update synchronously within the
same transaction as the source write) does not generalize to a
distributed cluster. RockStream has no write-transaction hook — writes
arrive through source connectors polling CDC logs, not through triggers
that fire inside an open transaction. Adding that hook would require a
global write-sequence number and synchronous coupling across the
connector, operator graph, and view commit, which conflicts with the
async-scheduling and causal-time frontier model the system is built on.
See chapter 20 for the full explanation.

If your reason for wanting IMMEDIATE is "I want fresh data fast," set a
tight SLO of 50–200 ms instead; the asynchronous path delivers this
without any restrictions on the query. If your reason is "I need
cross-view consistency," the diamond consistency guarantee (structural
frontier alignment) gives you that for free across any number of views
without any user configuration.

**"Should I use a materialized view or an inline view?"**

Ask whether the result will be read more than once. A materialized view
pays a one-time cost to maintain the result incrementally, so each read
is fast regardless of how large the underlying data is. An inline view
pays nothing upfront but re-runs the defining query every time it's
referenced. For a view that's queried thousands of times a second, or
joined into other maintained views, materialization is almost always the
right choice. For a view that exists purely as a SQL abstraction layer
— a stable interface hiding messy underlying tables — an inline view is
fine, and the zero maintenance cost matters. The ambiguous case is a
view referenced across many materialized views as a common subplan. In
that case, materializing it once and having other views read from it is
more efficient than each view independently inlining and recomputing it.

**"How do I export to a data lake?"**

Use RockStream to Kafka and a downstream writer. The removed connector
frontends return `RS-4017`; see [connector migration](connector-migration.md).

**"How tight should my freshness SLO be?"**

Tighter SLOs cost more: they require more frequent epoch commits, more
parallelism, and more object-store writes per second. The right approach
is to figure out the loosest SLO your use case can tolerate and set
that. Ask: if the dashboard is five seconds stale, does anyone notice?
If yes, set 1–2 seconds. If not, set 5 seconds and save the resources.
A common mistake is setting a 100 ms SLO because it sounds impressive
when 1 second would be indistinguishable to users and half the cost.
Remember that the SLO is enforced — if the cluster can't meet it, you'll
get degradation events. Setting a realistic SLO means those events are
meaningful signals rather than constant noise.

**"Do I need a write fence?"**

Only in one specific situation: a write happens in one service (or
session) and a read happens in a different service (or session), and the
reader must see the writer's changes before proceeding. Within a single
SQL session, RockStream already handles read-after-write automatically
— a `SELECT` issued after an `INSERT` in the same session will always
see the inserted row reflected in maintained views, with no extra work.
The write fence is for cross-service handoffs: service A inserts a row,
calls `rockstream.write_fence()` to get a token, passes it to service B
(via a message queue, a REST response, a job record), and service B uses
the token to wait only until that specific write is visible. Without the
fence, service B would have to guess how long to sleep or poll
repeatedly. With the fence, it waits exactly as long as necessary.

**"Should I use SUBSCRIBE or poll with SELECT?"**

If your consumer reacts to changes as they happen — forwarding updates
to a cache, notifying downstream services, writing to another Kafka
topic, powering a WebSocket feed — use SUBSCRIBE. You get changes pushed
to you in real time with exactly-once semantics and the ability to
resume from a past position after a disconnect. If your consumer asks
"what is the state right now?" on demand — a dashboard that renders on
page load, an API endpoint returning a current count, a batch job that
runs every hour — use SELECT. The distinction is push versus pull.
SUBSCRIBE is more efficient for high-frequency change consumers because
it avoids the overhead of repeated query planning and result scanning;
polling is simpler for infrequent, stateless reads.

**"When should I use view replacement vs. DROP + CREATE?"**

Today, if you need a production-safe migration path, prefer creating a
new view name alongside the old one and cutting consumers over
explicitly; the fully named replacement workflow is still planned rather
than SQL-dispatched. `DROP + CREATE` is fine for local/dev cases, but in
production it creates exactly the visibility gap the planned replacement
surface is meant to avoid.

**"How do I know if a view needs attention?"**

Start with the surfaces the gateway actually exposes today: `SHOW
WORKLOAD STATUS`, `SHOW RESOURCE USAGE`, `SHOW CLUSTER RESOURCE USAGE`,
and `EXPLAIN INCREMENTAL`. For a specific query or maintained view,
`EXPLAIN INCREMENTAL` / `... VERBOSE` / `... ANALYZE` are the main
frontdoor diagnostics. The richer `SHOW VIEW STATUS` surface exists in
the lifecycle data model, but not yet in gateway dispatch.

### 36. Further Reading

For the architectural deep dive, see [DESIGN.md](../DESIGN.md) — the
authoritative description of the system, its principles, and the
trade-offs behind each design decision.

For the incremental view maintenance specifics, see
[IVM.md](../IVM.md) — how RockStream's IVM is based on DBSP, what it
takes from pg-trickle as a correctness oracle, and the per-operator
differentiation rules.

For the phased build-out, see
[NEW_IMPLEMENTATION_PLAN.md](../NEW_IMPLEMENTATION_PLAN.md) — the roadmap from
the current state to a production-grade system, with milestones and
deliverables.

For the underlying ideas, the foundational papers are:

- **DBSP** (Budiu et al.): the mathematical framework for incremental
  computation that RockStream's runtime is modeled on.
- **Differential Dataflow** (McSherry et al.): the original work on
  frontier-based progress tracking that inspired RockStream's
  coordination model.
- **FoundationDB**: the source of the deterministic-simulation testing
  discipline that RockStream adopts for correctness.
- **The CALM theorem** (Hellerstein, Alvaro): the formal basis for the
  monotone-frontier commit invariant.

The other systems worth reading for comparison are streaming SQL systems with
single-node memory-resident state, and
**pg-trickle** (incremental
maintenance inside PostgreSQL), and declarative incremental-maintenance
features in cloud data warehouses.

---

## v0.5 Implementation Reference — Algebraic Aggregates and Group Commit

### Algebraic Aggregates (IVM-2)

Starting from v0.5, RockStream maintains `SUM`, `COUNT(*)`, and `AVG` views
incrementally using the DBSP aggregate delta rule.

**Operator**: `AggregateOp`
- **Input schema**: `(k: Int64, v: Int64)` — group key and value to aggregate.
- **Output schema**: `(k: Int64, sum_v: Int64, count: Int64, avg_v: Int64)`
  where `avg_v = sum_v / count` (truncating integer division).
- **State**: an in-memory `HashMap<k, (sum, count)>` persisted to the `op_state`
  namespace of `ShardDb` after each epoch commit.

**DBSP delta rule**:
```
for each input delta (k, v, weight):
  old = state[k]   -- (sum, count), or (0,0) if k is new
  new_sum   = old.sum   + v * weight
  new_count = old.count + weight
  if old.count != 0 → emit (k, old.sum, old.count, avg(old)) with weight -1
  if new_count != 0 → emit (k, new_sum, new_count, avg(new)) with weight +1
  state[k] = (new_sum, new_count)  if new_count != 0, else remove k
```

This rule is proven correct by the oracle property test
`oracle_aggregate_100k` (≥100k random insert/delete/group-churn scenarios).

**Error codes**:
- `RS-1015`: Group-commit queue full (capacity = `GROUP_COMMIT_MAX_BATCHES = 64`).
  Back-pressure applied; reduce epoch rate or increase shard count.
- `RS-1016`: historically used in the operator-level registry for aggregate
  overflow, while the current SQL frontend also emits `RS-1016` for
  unsupported window functions such as `NTILE`; check the emitting crate
  (`rockstream-types` / `rockstream-ops` vs. `rockstream-sql`) for the
  exact call site.

### Group Commit (shard-level epoch coalescing)

Each epoch, all operator write-batch fragments are coalesced by `GroupCommit`
into a single atomic `Db::write()` call:

- **Named bound**: `GROUP_COMMIT_MAX_BATCHES = 64` pending batches.
- **Fill-level metric**: `GroupCommit::fill_level()` — monitor to detect
  back-pressure episodes.
- **Durability events**: `GroupCommit::commit_count()` — total `Db::write()`
  calls issued.  For N ≥ 5 operators per epoch, group commit reduces durability
  events by a factor of N (proven ≥5× in `lfs_group_commit_reduces_durability_events`
  and `minio_group_commit_reduces_durability_events`).

### Persisted Frontier

After each epoch commit, the shard writes its current epoch number to the
`shard_meta` namespace (key `frontier`).  On restart, `load_frontier(db)`
reads this value so the operator can resume from the correct epoch without
re-processing already-committed data.

---

## SQL Frontend (v0.7)

RockStream v0.7 adds a full SQL frontend built on [DataFusion](https://datafusion.apache.org/).
You can now write views in standard SQL rather than constructing `PlanNode`
trees by hand. This appendix is historical milestone context; for the
current authoritative SQL surface, see `docs/language-features.md`.

### CREATE VIEW

In the current gateway/frontend split, `CREATE VIEW` registers an inline
macro-expanded view, while `CREATE MATERIALIZED VIEW` registers the
storage-backed maintained view:

```sql
CREATE MATERIALIZED VIEW orders_summary AS
  SELECT region, SUM(amount) AS total, COUNT(*) AS n
  FROM orders
  GROUP BY region;
```

The SQL frontend:
1. Parses the query using DataFusion's SQL parser.
2. Lowers the DataFusion `LogicalPlan` to a RockStream `PlanNode` tree.
3. Applies the distribution pass (inserts `Exchange[Loopback]` no-ops for
   single-shard mode).
4. Stores the plan and output schema in the `SchemaCatalog` (backed by
   `ShardDb` under `CatalogType::View` keys).

### Supported SQL (Phase 1 operator set)

| SQL construct | PlanNode |
|---|---|
| `SELECT cols FROM t` | `Source` + `Project` |
| `WHERE predicate` | `Filter` |
| `GROUP BY k, SUM(v)` | `Aggregate[WeightAdd/v1]` |
| `GROUP BY k, COUNT(*), AVG(v)` | `Aggregate[WeightAdd/v1]` |
| `GROUP BY k, MIN(v), MAX(v)` | `Aggregate[IndexedMultiset/v1]` |

Expressions supported in predicates and projections: column references,
integer/boolean/string literals, `+`, `-`, `*`, `/`, `>`, `<`, `>=`, `<=`,
`=`, `<>`, `AND`, `OR`.

That v0.7 table is only the original minimal slice. The current lowering
path in `crates/rockstream-sql/src/lower.rs` also covers joins,
`UNION`/`INTERSECT`/`EXCEPT`, `DISTINCT`, window functions such as
`ROW_NUMBER`/`RANK`/`DENSE_RANK`/`LAG`/`LEAD`, sliding `SUM`/`AVG`,
`TRY_CAST`, and `TUMBLE`; it still rejects `LIMIT`, `ORDER BY` as
top-level plan nodes, `DISTINCT ON`, and `WITH RECURSIVE`.

### Schema Evolution

When you re-register a view with a new column list:

- **Compatible** (accepted in-place, schema version increments):
  - Same schema as before
  - New nullable columns appended at the end
- **Incompatible** (returns `RS-1002`):
  - Any existing column renamed, removed, or reordered
  - Any existing column type changed

### EXPLAIN INCREMENTAL

Show the annotated plan tree for a query without deploying it:

```
EXPLAIN INCREMENTAL
──────────────────────────────────────────────────────
✓ Aggregate  merge_law=WeightAdd/v1
    Exchange[Loopback]  loopback (single-shard)
      ⚠ Source[orders]  stateless
```

Stateless operators show `⚠` (no arrangement state).  Stateful algebraic
aggregates show `✓ merge_law=WeightAdd/v1`.  Non-invertible aggregates (MIN/MAX)
show `✗ extremum_requires_rmw`.

### EXPLAIN INCREMENTAL ESTIMATE

Produce a static cost estimate **without deploying any operators** or accessing
storage.  Useful for capacity planning before creating a view:

```
EXPLAIN INCREMENTAL ESTIMATE
───────────────────────────────────────────────────
Operator                        state_bytes   epoch_ms
───────────────────────────────────────────────────
Source[orders]                            0       0.00
Exchange[Loopback]                        0       0.00
Aggregate                             24000      10.00
───────────────────────────────────────────────────
(estimates only; no operators deployed)
```

The estimate uses throughput floors from the Phase 1 exit criteria:

| Operator | Throughput (in-memory) | State per group |
|---|---|---|
| Filter / Project | 5 M rows/s | 0 B |
| Aggregate SUM/COUNT/AVG | 1 M rows/s | 24 B |
| Aggregate MIN/MAX | 100 k rows/s | 32 B |

### Custom Extension Nodes

The frontend registers three DataFusion extension nodes for incremental
operators.  In v0.7 these nodes are transparent (they lower to the same
`PlanNode` as their standard DataFusion equivalents); in later versions they
carry dual-arrangement markers, bilinear-join cost hints, and other
incremental-specific metadata.

| Extension node | Purpose |
|---|---|
| `IncAggregate` | GROUP BY maintained via DBSP arrangement |
| `IncJoin` | Equi-join with dual arrangements (v0.8) |
| `IncDistinct` | DISTINCT / set operations (v0.10) |

### Outer, Semi, and Anti Joins (v0.9 — IVM-5)

RockStream v0.9 adds full incremental support for five additional join types
beyond the inner equi-join introduced in v0.8:

| Join type | SQL syntax | Semantics |
|---|---|---|
| **Left outer** | `LEFT JOIN … ON …` | All left rows; NULL-pad right columns when unmatched |
| **Right outer** | `RIGHT JOIN … ON …` | All right rows; NULL-pad left columns when unmatched |
| **Full outer** | `FULL OUTER JOIN … ON …` | All rows from both sides; NULL-pad where unmatched |
| **Semi** | `WHERE col IN (SELECT …)` or `WHERE EXISTS (SELECT …)` | Left rows that have at least one matching right row (right columns not emitted) |
| **Anti** | `WHERE col NOT IN (SELECT …)` or `WHERE NOT EXISTS (SELECT …)` | Left rows that have no matching right row |

#### NULL encoding

Unmatched columns in outer-join output are encoded as `0i64` in the Arrow
Int64 output schema.  The operator distinguishes NULL-pad rows from real zero
values by tracking per-key match counts in the `right_key_weight` (and
`left_key_weight` for RIGHT/FULL) maps.

#### Incremental algorithm

Each outer join variant extends the inner-join bilinear rule with match-count
tracking:

- **RIGHT_KEY_WEIGHT[k]** — net Z-set weight of all right rows for key `k`.
  When this transitions from 0 to non-zero (a key gains its first right match),
  any previously NULL-padded left rows for that key are retracted.  When it
  transitions from non-zero to 0 (key loses its last right match), NULL-pad
  rows are re-emitted.
- **LEFT_KEY_WEIGHT[k]** — symmetric for RIGHT and FULL joins.

The state persistence format uses the same arrangement key encoding as inner
join (`[0x01][0x4A4C/0x4A52][op_id:8][key][row_id:16]`), plus two new prefixes
for match-count state (`[0x01][0x4F52]` for right weights, `[0x01][0x4F4C]`
for left weights).  All persistence uses only point puts — no range deletion.

### Error Codes

- `RS-1002`: Incompatible schema change — rename, drop, or retype an existing
  column.  Use a new view name or follow the blue/green procedure.
- `RS-1012`: SQL parse error — check syntax against the supported SQL subset.
- `RS-1013`: Unsupported plan node — the query uses a feature not yet supported
  by the incremental planner (e.g. `RightSemi`/`RightAnti` joins, correlated
  subqueries with non-equi conditions, window functions with `GROUPS` framing).

---

## A Final Note

The concepts in this guide are deliberately presented from the user's
side of the curtain. The implementation is much more elaborate than the
mental model: there are dozens of subsystems, hundreds of metrics, and
thousands of lines of carefully reasoned code that all exist so that
the user-facing experience can be simple. This is intentional. The
test of a good system is not how clever its insides are; it's how easy
it is to use its outside.

When you write a SQL view in RockStream and it stays fresh while your
data grows from a thousand rows to a billion, you're seeing the payoff
of every architectural decision in this guide. The frontiers, the
epochs, the shards, the coordination groups, the SLO loop, the
self-tuning, the namespaces — all of it exists so that the SQL you
wrote keeps working as your world gets bigger.

That is the promise. The rest of the system is the machinery that
delivers it.

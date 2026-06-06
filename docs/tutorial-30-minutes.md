# RockStream 30-Minute Starter Tutorial: Building Real-Time Analytics Pipelines

Welcome to the RockStream starter tutorial. This guide is designed to take you from a complete beginner—with no prior experience in Incremental View Maintenance (IVM) or stream processing—to an intermediate builder capable of designing, executing, and diagnosing complex, multi-stage materialized view DAGs. Over the next 30 minutes, we will explore the underlying mathematical and architectural principles of RockStream, bootstrap a local development cluster, connect to it using standard database tools, define workloads, and construct a real-world Campaign Attribution and Referral tracking pipeline. Finally, we will demonstrate how to query this pipeline using SQL, stream changes out using the Postgres wire protocol, and diagnose internal behaviors using RockStream's CLI tools.

---

## Part 1 — Paradigms, Concepts, and the Scoreboard Analogy

Before we write a single line of code or run a single command, we must understand *why* RockStream exists and *how* it thinks about data. If you are coming from a traditional relational database (like PostgreSQL, MySQL, or SQL Server) or a batch-based data warehouse (like Snowflake, BigQuery, or Redshift), you are accustomed to a **pull-based, batch-oriented paradigm**. 

In a traditional database, data sits quietly in tables on disk. When you want to see a report or update a dashboard, you execute a query. The database engine wakes up, performs index scans or full table scans, filters rows, aggregates them, and returns the result. If your data size is small, this happens in milliseconds. But as your data size grows and you accumulate millions or billions of records, these scans become progressively slower, costlier, and more resource-intensive. If your dashboard needs to refresh every few seconds, running full table scans repeatedly is like tearing down and rebuilding a stadium scoreboard from scratch every time a player scores a point. It is inefficient, redundant, and does not scale.

### Incremental View Maintenance (IVM) and the Ticking Scoreboard

RockStream is built on a **push-based, incremental paradigm**. Instead of waiting for you to query the database, RockStream pre-computes the answers to your queries and keeps them continuously fresh. When a new row is added, or an existing row is updated or deleted, RockStream does not rerun your query. Instead, it computes *only the difference*—the delta—and applies that difference directly to the pre-computed result. The scoreboard stays up, and only the numbers affected by the new event tick forward. 

The work performed by the engine is proportional to the volume of *incoming changes*, not the *total size of the historical data*. A stream of ten million historical records costs you nothing at query time because the final result is already sitting in storage, ready to be read in O(1) or O(log N) time.

### The Mathematics of Change: Z-Sets and DBSP

Under the hood, RockStream represents all changes using a formal mathematical framework called **DBSP** (Database Stream Processing) and a data structure called a **Z-set** (sets with integer weights). 

In standard set theory, a set either contains an element or it does not (membership is binary). In bag (or multiset) theory, a set can contain multiple copies of the same element, meaning membership maps elements to positive integers ($\mathbb{N}$). A Z-set generalizes this by allowing the count (or weight) of any element to be *any* integer—positive, zero, or negative. Mathematically, a Z-set over a domain $D$ is a function:
$$Z: D \to \mathbb{Z}$$
where only a finite number of elements in $D$ have non-zero weights.

This algebraic structure forms a commutative group under addition. This is incredibly powerful because it means we can represent database changes directly:
- An **insertion** of a row $r$ is represented as a Z-set containing $r$ with a weight of `+1`.
- A **deletion** of a row $r$ is represented as a Z-set containing $r$ with a weight of `-1`.
- An **update** of a row from $r_{old}$ to $r_{new}$ is represented as the sum of a deletion and an insertion: $\{r_{old} \mapsto -1, r_{new} \mapsto +1\}$.

Because relational operators like Filter ($\sigma$), Project ($\pi$), and Join ($\bowtie$) are linear (or bilinear) with respect to Z-set addition, RockStream can compile your SQL query into a directed acyclic graph (DAG) of physical operators that process these weights directly. 

Let's look at how this linearity plays out:
- **Filter ($\sigma_p$):** To filter a Z-set $X$ with predicate $p$, the operator simply evaluates $p$ on each element. If $p(x)$ is true, the element $x$ is emitted with its original weight $w$; if false, it is omitted. Thus:
  $$\sigma_p(X + Y) = \sigma_p(X) + \sigma_p(Y)$$
- **Project ($\pi_f$):** To project a Z-set $X$ using function $f$, the operator applies $f$ to each element. If multiple elements map to the same projected value, their weights are summed. This aggregation of weights handles duplicates automatically:
  $$\pi_f(X + Y) = \pi_f(X) + \pi_f(Y)$$
- **Join ($\bowtie$):** Joins are bilinear. If we have two streams of changes $dX$ and $dY$, the change in the join output is computed by joining the new changes with the accumulated historical states $X$ and $Y$:
  $$d(X \bowtie Y) = (dX \bowtie Y) + (X \bowtie dY) + (dX \bowtie dY)$$

This mathematical formulation guarantees that RockStream's incremental outputs are always *mathematically identical* to the results you would get from running a full batch query from scratch. There are no approximations, no heuristics, and no synchronization drift.

```
                  ┌─────────────────┐
                  │   Incoming DDL  │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │   DataFusion    │  (SQL Parsing & AST Generation)
                  │  SQL Frontend   │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │    PlanNode     │  (Logical/Physical Plan representation)
                  │ Intermediate IR │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │     DiffCtx     │  (Differentiation Pass: builds delta plan)
                  │ Differentiation │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  OpNode Graph   │  (Physical Executable Operator Graph)
                  └─────────────────┘
```

The compilation process is managed by `rockstream-diff` via the `DiffCtx` (differentiation context) pass. It takes a static logical plan of `PlanNode`s and differentiates it, inserting stateful **Arrangements** (index tables) where operators need to remember historical values (such as the inputs to a Join or GroupBy).

### Coordination Without Locks: The Frontier Protocol

In a distributed, sharded system, coordinating progress across multiple machines without bottlenecks is a major challenge. Traditional databases use distributed locks or two-phase commit protocols, which introduce significant latency and network overhead. RockStream replaces coordination with a metadata-driven protocol using **frontiers**.

A frontier is a monotonic marker that progress has been completed up to a specific logical epoch. For example, if a source operator emits a frontier of `epoch=42`, it promises that it will never emit any future changes with an epoch smaller than 42. Downstream operators monitor the frontiers of all their inputs. A join operator with two inputs at `epoch=42` and `epoch=41` knows it can safely process data up to `epoch=41`. Once the second input advances to `epoch=42`, the join advances as well. 

This guarantees **diamond consistency**:
If a query splits into two parallel paths (e.g., performing a filter on one side and a projection on the other) and merges later via a join, the join operator is guaranteed to see a perfectly aligned, consistent snapshot of the world. It will never join an event from epoch 42 with a state from epoch 41. The frontier protocol delivers this consistency dynamically without holding any global locks, making it highly scalable.

### The State Engine: SlateDB and Object Storage

Many streaming databases keep all their state in RAM or rely on local SSDs for storage. This makes them fast, but it makes them expensive and difficult to scale. If a worker node crashes, you must copy gigabytes of state over the network to a new worker before it can resume processing.

RockStream stores all intermediate operator state (arrangements) and final view outputs in **SlateDB**, an LSM-tree storage engine designed specifically to write directly to cloud object storage (like AWS S3, Google Cloud Storage, or MinIO). 

- **Bottomless State:** Your state can grow beyond the memory or disk capacity of a single machine, bounded only by the capacity of your cloud storage bucket.
- **Cheap Durability:** Object storage is significantly cheaper than provisioned SSDs or RAM, saving up to 90% in storage costs.
- **Zero Local-State Migration:** Because all state is stored durably in the cloud, worker nodes do not have local state. If a worker node fails, another worker can instantly take over its shards by reading directly from the same object storage path, resuming work in seconds.
- **LSM Compaction in the Cloud:** SlateDB writes immutable SSTable files to S3. Compaction is coordinated by RockStream's control plane, which delegates the CPU-heavy compaction work to background worker tasks without affecting the query-serving gateway nodes.

### The Algebraic Safety Net: Merge Laws

To maintain state incrementally without reading and writing the entire dataset, RockStream requires every aggregate operator to carry a **merge law**. 

A merge law is a named, versioned algebraic contract (such as `WeightAdd/v1` for SUM/COUNT or `MaxRegister/v1` for MAX) that defines how state merges. 

RockStream verifies three key mathematical properties for every merge law:
1. **Associativity:** $(a \oplus b) \oplus c = a \oplus (b \oplus c)$. This allows updates to be applied in any batch size or hierarchy.
2. **Commutativity:** $a \oplus b = b \oplus a$. This allows updates to be applied out of order, which is common in distributed networks where sharded data arrives at different times.
3. **Identity Element:** There exists an element $e$ such that $a \oplus e = a$. This represents the default empty state.
4. **Inverse (Optional but highly desired):** There exists an element $-a$ such that $a \oplus (-a) = e$. 

This inverse allows RockStream to handle deletions efficiently: if an input row is deleted, the engine subtracts its value from the running total without recalculating the sum of all other rows. This is known as an **Abelian Group**.

For operations like `MAX` and `MIN`, which are semilattices and lack an inverse (you cannot "subtract" a number from a maximum to find the previous maximum), RockStream's catalog explicitly records that they require a read-modify-write (RMW) cycle. This allows the compiler to generate warnings or optimize state compaction accordingly.

RockStream supports several CRDT (Conflict-free Replicated Data Type) columns built on these merge laws, including:
- `COUNTER`: Accumulates integers using addition (Abelian Group).
- `MAX_REGISTER` / `MIN_REGISTER`: Tracks the maximum/minimum value observed (Semilattice).
- `LWW` (Last-Write-Wins): Tracks the value with the highest physical timestamp.
- `OR_SET` (Observed-Remove Set): Maintains a set of unique elements.

---

## Part 2 — Preparing Your Environment and Bootstrapping the Node

Let's begin the practical portion of this tutorial. We will start by setting up our environment and booting a single-node RockStream instance in evaluation mode.

### Directory Structure

First, create a clean directory in your workspace where RockStream will store its data files and write-ahead logs (WALs):

```bash
mkdir -p ./rockstream-data
```

### Bootstrapping the Server

We will start the single `rockstream` binary. In a production deployment, you would run separate instances with specific roles (`--role=control`, `--role=worker`, `--role=gateway`). For local development and evaluation, we use the combined profile `--role=all`, which launches all three services inside a single process.

Run the following command in your terminal. Since we are adhering to our token-optimization protocol, we prefix the shell command with `rtk` (if running in an environment with the Rust Token Killer proxy):

```bash
rtk cargo run --release --bin rockstream -- start --role=all --storage=./rockstream-data
```

If you are running the pre-compiled binary directly:

```bash
rtk rockstream start --role=all --storage=./rockstream-data
```

Let's analyze the startup log output. You will see JSON logs formatted as follows:

```json
{"timestamp":"2026-06-06T20:15:00Z","level":"INFO","fields":{"message":"starting rockstream","storage":"./rockstream-data","role":"all"},"target":"rockstream"}
{"timestamp":"2026-06-06T20:15:00.100Z","level":"INFO","fields":{"message":"starting control service","bind_addr":"127.0.0.1:7700"},"target":"rockstream_control"}
{"timestamp":"2026-06-06T20:15:00.200Z","level":"INFO","fields":{"message":"starting worker service","control_addr":"127.0.0.1:7700"},"target":"rockstream_runtime"}
{"timestamp":"2026-06-06T20:15:00.300Z","level":"INFO","fields":{"message":"starting postgres wire gateway","listen_addr":"127.0.0.1:5432"},"target":"rockstream_gateway"}
```

What just happened?
1. The **Control Plane** initialized. It acts as the coordinator of the cluster, managing the catalog (table schemas, view definitions, workloads) and assigning partitions (shards) to workers. It listens on port `7700`.
2. The **Worker** started up. It automatically registered itself with the control plane on port `7700`. It is now waiting to receive operator graphs and shards to execute.
3. The **Gateway** started up. It is the public face of the cluster, listening on port `5432` (the standard Postgres port). It accepts SQL statements, compiles them, registers them with the control plane, and streams results back using the Postgres wire protocol.
4. The **Storage Engine** initialized in `./rockstream-data`. A default database catalog and a SlateDB storage manager were initialized on the local disk.

The system is now fully booted and waiting for client connections.

---

## Part 3 — Connecting to the Gateway via the Postgres Wire Protocol

Because RockStream speaks the standard Postgres wire protocol, you do not need any custom client libraries or specialized query tools. You can use your favorite database client—be it `psql`, DBeaver, pgAdmin, or libraries in Python (`psycopg2`), Go (`pgx`), Node.js (`pg`), or Rust (`tokio-postgres`).

### Connecting via `psql`

Open a new terminal window and connect to the local RockStream gateway using the standard `psql` command-line utility.

We must supply an authentication token. RockStream uses OIDC-compliant bearer tokens passed in the password field to establish identity, tenant boundaries, and RBAC roles. For this local tutorial, we will use the administrative token `bearer admin:any`, which grants full read/write access across all tenant namespaces.

Run the following command:

```bash
PGPASSWORD="bearer admin:any" psql -h 127.0.0.1 -p 5432 -U alice -d mydb
```

You should see the standard Postgres welcome message and command prompt:

```
psql (14.5, server 0.52.10-rockstream)
Type "help" for help.

mydb=>
```

Let's run a quick query to verify that the connection is active and that we are talking to RockStream:

```sql
SELECT version();
```

Output:
```
                    version                    
-----------------------------------------------
 RockStream 0.52.10 (SlateDB Cloud LSM Engine)
(1 row)
```

### Introspecting the System Catalog

RockStream exposes its internal state, schemas, and metrics through virtual tables in the `rockstream_catalog` schema. Let's run a query to inspect the registered merge laws available in the system:

```sql
SELECT name, class, associative, commutative, has_inverse FROM rockstream_catalog.merge_laws;
```

Output:
```
     name     |    class     | associative | commutative | has_inverse 
--------------+--------------+-------------+-------------+-------------
 WeightAdd    | AbelianGroup | t           | t           | t
 SumCount     | AbelianGroup | t           | t           | t
 MaxRegister  | Semilattice  | t           | t           | f
 MinRegister  | Semilattice  | t           | t           | f
 LwwRegister  | Semilattice  | t           | t           | f
 OrSet        | Semilattice  | t           | t           | f
(6 rows)
```

Take a look at the output. Notice that `WeightAdd` (used for standard additions and sums) has an inverse (`has_inverse = t`), meaning it can process deletions by simply subtracting the deleted value. On the other hand, `MaxRegister` (used for `MAX` aggregations) does not have an inverse (`has_inverse = f`), indicating that deleting a row might require scanning other values to find the new maximum (unless optimized via a tree structure).

Let's check the pipeline metadata table:

```sql
SELECT id, name, status, shard_count FROM rockstream_catalog.pipelines;
```

Right now, this returns 0 rows because we haven't defined any views or workloads yet. Let's build them!

---

## Part 4 — Creating Base Tables, Sources, and Workloads

Before we define our Campaign Attribution DAG, we must set up a **workload** to govern its resource limits and freshness targets. Then, we will define our source tables.

### 1. Defining the Workload

In RockStream, views are grouped into workloads. A workload allows us to define a Service Level Objective (SLO) for data freshness, set memory limits to prevent out-of-memory errors, and specify scheduling priorities.

Execute the following DDL statement in your `psql` session:

```sql
CREATE WORKLOAD campaign_attribution WITH (
    FRESHNESS_SLO = '1s',
    MEMORY_LIMIT = '10GB',
    PRIORITY = normal
);
```

This workload establishes that:
1. Any materialized view registered under it should stay within `1 second` of the source data under normal operating conditions.
2. The collective intermediate arrangement state of all views in this workload is capped at `10 gigabytes` in RAM before spooling to SlateDB disk compaction.
3. The priority is set to `normal`.

### 2. Creating the Base Tables

Now we will define the schema for our streaming tables. Unlike traditional databases, RockStream distinguishes between standard tables and tables that accept raw streaming inputs from external connectors (like Kafka or Debezium CDC). For this tutorial, we will create five base tables to represent our application data:

1. **`users`**: A dimension table storing user names and their marketing group IDs.
2. **`products`**: A dimension table storing product names and their unit prices.
3. **`clicks`**: A streaming table capturing user click events on marketing campaigns.
4. **`purchases`**: A streaming table capturing transaction details.
5. **`referrals`**: A streaming table representing a user-to-user referral tree.

Run the following DDL commands:

```sql
-- Dimensions
CREATE TABLE users (
    user_id INT8 PRIMARY KEY,
    name VARCHAR,
    group_id INT8
);

CREATE TABLE products (
    product_id INT8 PRIMARY KEY,
    name VARCHAR,
    price INT8
);

-- Streaming Event Tables
CREATE TABLE clicks (
    click_id INT8 PRIMARY KEY,
    user_id INT8,
    campaign_id INT8,
    ts INT8
);

CREATE TABLE purchases (
    purchase_id INT8 PRIMARY KEY,
    user_id INT8,
    product_id INT8,
    amount INT8,
    ts INT8
);

CREATE TABLE referrals (
    referrer_id INT8,
    referee_id INT8,
    ts INT8,
    PRIMARY KEY (referrer_id, referee_id)
);
```

### 3. Understanding CRDT Columns and Schema Validation

In addition to standard SQL types, RockStream supports CRDT (Conflict-free Replicated Data Type) column types such as `COUNTER`, `MAX_REGISTER`, and `OR_SET`. These columns allow multiple writers to update the same row concurrently, and the storage engine will automatically merge the updates using the registered merge laws.

For example, if we were building an account balance table, we might write:
```sql
CREATE TABLE account_balances (
    account_id VARCHAR PRIMARY KEY,
    balance COUNTER
);
```

When you define a source backed by a connector (like Kafka), RockStream performs **schema compatibility validation** (`discover_schema()`). If you attempt to map a connector source column to a `COUNTER` type in RockStream, but the connector's schema metadata reports that the source column contains non-merge-safe operations, RockStream will reject the creation with error code `RS-1002` (Schema Mismatch). This prevents runtime data corruption.

---

## Part 5 — Designing the Campaign Attribution & Referral DAG

Now we are ready to build our multi-stage analytical DAG. This pipeline will track how marketing campaigns drive product purchases, rank campaigns based on total revenue, and track user referral chains recursively.

```
       [ clicks ]                    [ purchases ]         [ users ]    [ products ]
            │                              │                   │             │
            │                              └───────────┬───────┴─────────────┘
            │                                          ▼
            │                               ┌──────────────────────┐
            │                               │ mv_purchases_enriched│
            │                               └──────────┬───────────┘
            │                                          │
            └──────────────────────┬───────────────────┘
                                   ▼
                       ┌──────────────────────┐
                       │ mv_conversion_funnel │
                       └───────────┬──────────┘
                                   ▼
                     ┌──────────────────────────┐
                     │  mv_campaign_performance │
                     └─────────────┬────────────┘
                                   ▼
                       ┌──────────────────────┐
                       │   mv_top_campaigns   │
                       └──────────────────────┘

       [ referrals ] ───►  [ mv_referral_depth ]  (Recursive CTE)
```

We will build five materialized views, each representing a stage in the DAG. All views will be assigned to our `campaign_attribution` workload so they share the same execution context and SLO.

### View 1: Enriching Purchases (`mv_purchases_enriched`)

The first stage of our pipeline enriches raw purchase events with user names and product prices. We calculate `total_amount` by multiplying the product price by the quantity purchased.

Run the following query:

```sql
CREATE MATERIALIZED VIEW mv_purchases_enriched
WITH (WORKLOAD = campaign_attribution) AS
SELECT 
    p.purchase_id,
    p.user_id,
    u.name AS user_name,
    pr.name AS product_name,
    pr.price,
    p.amount,
    TRY_CAST((pr.price * p.amount) AS DOUBLE) AS total_amount,
    p.ts
FROM purchases p
INNER JOIN users u ON p.user_id = u.user_id
INNER JOIN products pr ON p.product_id = pr.product_id;
```

*Concept Check:* This is an `INNER JOIN` across three relations (one event stream and two static dimensions). RockStream translates this into a physical plan containing a join operator. The operator maintains state (arrangements) for `users` and `products` in SlateDB, allowing it to quickly enrich incoming purchases as they arrive.

### View 2: Campaign Conversion Funnel (`mv_conversion_funnel`)

Next, we join the click events with the enriched purchases to attribute purchases to the marketing campaigns that drove them. We perform a `LEFT JOIN` to ensure we capture all clicks, even those that did not result in a purchase. We match events based on `user_id`.

Run the following DDL:

```sql
CREATE MATERIALIZED VIEW mv_conversion_funnel
WITH (WORKLOAD = campaign_attribution) AS
SELECT 
    c.click_id,
    c.user_id,
    c.campaign_id,
    pe.purchase_id,
    pe.total_amount,
    c.ts,
    CASE 
        WHEN pe.purchase_id IS NOT NULL THEN true 
        ELSE false 
    END AS matched
FROM clicks c
LEFT JOIN mv_purchases_enriched pe ON c.user_id = pe.user_id;
```

*Concept Check:* Notice that `mv_conversion_funnel` reads directly from `mv_purchases_enriched`. This is a **view-on-view reference**. When a new purchase is enriched in View 1, the resulting delta is pushed directly into the input channel of View 2. The data flows through the DAG like a physical river, with each stage computing only the differences.

### View 3: Campaign Performance Rollup (`mv_campaign_performance`)

Now we want to roll up our conversions to measure the performance of each campaign. We will calculate the total number of clicks and the sum of purchase revenue for each campaign.

Because this is a streaming pipeline, we cannot aggregate over all time without our state growing indefinitely. Instead, we partition time into non-overlapping blocks using a **Tumbling Window**. In this case, we group events into 1-hour windows based on their timestamps.

To represent event times, we convert our integer timestamps (`ts`) into formatted date strings:

```sql
CREATE MATERIALIZED VIEW mv_campaign_performance
WITH (WORKLOAD = campaign_attribution) AS
SELECT 
    campaign_id,
    COUNT(click_id) AS clicks_count,
    SUM(COALESCE(total_amount, 0.0)) AS total_amount,
    -- Group into 1-hour tumbling windows (simulated via timestamp division)
    TRY_CAST(FROM_UNIXTIME(ts - (ts % 3600)) AS VARCHAR) AS window_start
FROM mv_conversion_funnel
GROUP BY campaign_id, ts - (ts % 3600);
```

*Concept Check:* The `SUM` and `COUNT` aggregations are compiled using RockStream's `WeightAdd` and `SumCount` merge laws. Because these laws possess algebraic inverses, RockStream can update the totals incrementally when a click is updated or retracted, without having to scan historical clicks.

### View 4: Campaign Leaderboard (`mv_top_campaigns`)

Next, we rank our campaigns based on the total revenue they generated. This allows us to see our top-performing campaigns in real time. We will use the `DENSE_RANK()` window function to assign ranks.

Run the following query:

```sql
CREATE MATERIALIZED VIEW mv_top_campaigns
WITH (WORKLOAD = campaign_attribution) AS
SELECT 
    campaign_id,
    total_amount,
    DENSE_RANK() OVER (ORDER BY total_amount DESC) AS rank_val
FROM mv_campaign_performance;
```

*Concept Check:* Under the hood, this window function is translated into a physical ranking operator. As the revenue of campaigns changes in the upstream `mv_campaign_performance` view, the ranking operator adjusts the ranks of the affected campaigns. If a campaign moves from rank 5 to rank 4, it emits a delta of `-1` for the old rank and `+1` for the new rank.

### View 5: Recursive Referral Tracking (`mv_referral_depth`)

Finally, let's show off one of RockStream's most powerful advanced features: **monotone insert-only recursion**. 

In social networks or viral marketing campaigns, users often refer other users, who in turn refer more users, creating a tree of referrals. If we want to find the referral depth and path for every user, we need a recursive query. In a traditional database, recursive queries are extremely slow because they require iterative scans. In RockStream, recursive queries are maintained incrementally: as new referral links are added to the database, RockStream walks only the new paths and appends them to the result set.

Run the following query:

```sql
CREATE MATERIALIZED VIEW mv_referral_depth
WITH (WORKLOAD = campaign_attribution) AS
WITH RECURSIVE referral_tree AS (
    -- Anchor member
    SELECT 
        referrer_id,
        referee_id,
        1 AS depth,
        TRY_CAST(CONCAT(referrer_id, '->', referee_id) AS VARCHAR) AS path
    FROM referrals
    
    UNION ALL
    
    -- Recursive member
    SELECT 
        t.referrer_id,
        r.referee_id,
        t.depth + 1 AS depth,
        TRY_CAST(CONCAT(t.path, '->', r.referee_id) AS VARCHAR) AS path
    FROM referral_tree t
    INNER JOIN referrals r ON t.referee_id = r.referrer_id
)
SELECT referrer_id, referee_id, depth, path FROM referral_tree;
```

*Concept Check:* RockStream uses semi-naive evaluation to compile recursive queries. It tracks the "new" rows produced in the previous recursion step and joins only those new rows with the base `referrals` relation in the next step, ensuring that computation is minimal and incremental.

---

## Part 6: Draining and Subscribing: Verifying Data Flow

With our tables and views successfully created, let's populate them with data and verify that RockStream's incremental engine is working as expected.

### 1. Setting the Session Configuration

Before inserting data, we must address **idempotency**. In distributed streaming systems, network failures or client retries can lead to duplicate writes. RockStream enforces write safety by requiring clients to set an **idempotency key** before performing write operations on tables that require it.

Let's configure our session parameters:

```sql
-- Enforce Read Committed isolation level
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- Set the client-side idempotency key for our inserts
SET rockstream.idempotency_key = 'tutorial-session-key-001';
```

If you attempt to write to tables without setting an idempotency key, RockStream will reject the query with error code `RS-2007`, protecting your tables from duplicate writes.

### 2. Inserting Seed Data

Let's populate our dimension tables and insert a few events to trigger our DAG. We will bundle these inserts into a transaction block:

```sql
BEGIN;

-- Insert dimension rows
INSERT INTO users (user_id, name, group_id) VALUES 
(1, 'Bob', 100),
(2, 'Charlie', 101),
(3, 'Dave', 100);

INSERT INTO products (product_id, name, price) VALUES 
(201, 'Widget', 15),
(202, 'Gadget', 25);

-- Insert clicks (attributing to Campaign 10)
INSERT INTO clicks (click_id, user_id, campaign_id, ts) VALUES 
(5001, 1, 10, 1717574400);

-- Insert purchases
INSERT INTO purchases (purchase_id, user_id, product_id, amount, ts) VALUES 
(1001, 1, 201, 2, 1717574400);

-- Insert referrals (User 1 referred User 2, who referred User 3)
INSERT INTO referrals (referrer_id, referee_id, ts) VALUES 
(1, 2, 1717574400),
(2, 3, 1717574400);

COMMIT;
```

When you commit, the gateway bundles all inserts into a single epoch. The worker takes the new data, processes it through the operators of our five materialized views, and writes the updated results into SlateDB storage. All of this happens in less than a second.

### 3. Querying the Materialized Views

Let's verify that the views have been updated.

#### Query 1: Enriched Purchases
Query `mv_purchases_enriched` to confirm the purchase was enriched with the product name ('Widget') and price ($15), and the total amount ($30.0) was calculated:

```sql
SELECT * FROM mv_purchases_enriched;
```

Output:
```
 purchase_id | user_id | user_name | product_name | price | amount | total_amount |     ts     
-------------+---------+-----------+--------------+-------+--------+--------------+------------
        1001 |       1 | Bob       | Widget       |    15 |      2 |           30 | 1717574400
(1 row)
```

#### Query 2: Conversion Funnel
Query the funnel view to verify that the click event was successfully matched with the purchase event:

```sql
SELECT * FROM mv_conversion_funnel;
```

Output:
```
 click_id | user_id | campaign_id | purchase_id | total_amount |     ts     | matched 
----------+---------+-------------+-------------+--------------+------------+---------
     5001 |       1 |          10 |        1001 |           30 | 1717574400 | t
(1 row)
```

#### Query 3: Campaign Performance and Leaderboard
Query the campaign performance and leaderboard views to see the aggregated revenue and rank for Campaign 10:

```sql
SELECT * FROM mv_campaign_performance;
```

Output:
```
 campaign_id | clicks_count | total_amount |    window_start     
-------------+--------------+--------------+---------------------
          10 |            1 |           30 | 2026-06-05 08:00:00
(1 row)
```

```sql
SELECT * FROM mv_top_campaigns;
```

Output:
```
 campaign_id | total_amount | rank_val 
-------------+--------------+----------
          10 |           30 |        1
(1 row)
```

#### Query 4: Referral Depth
Query the recursive referral view. Notice how RockStream recursively traversed the referral chain (`1 -> 2 -> 3`), calculating a depth of 2:

```sql
SELECT * FROM mv_referral_depth;
```

Output:
```
 referrer_id | referee_id | depth |  path   
-------------+------------+-------+---------
           1 |          3 |     2 | 1->2->3
(1 row)
```

### 4. Streaming Live Changes using `SUBSCRIBE`

Querying views using `SELECT` is useful for verifying state, but for building responsive, real-time applications, you want to receive changes as they occur. RockStream enables this using the `SUBSCRIBE` statement.

Run the following command in `psql`:

```sql
SUBSCRIBE mv_conversion_funnel AS OF NOW WITH SNAPSHOT;
```

This starts a subscription stream. The gateway will first dump the current state of the view, followed by a live stream of changes. The connection will remain open:

```
 mz_timestamp | mz_diff | click_id | user_id | campaign_id | purchase_id | total_amount |     ts     | matched 
--------------+---------+----------+---------+-------------+-------------+--------------+------------+---------
           42 |       1 |     5001 |       1 |          10 |        1001 |           30 | 1717574400 | t
```

Here:
- `mz_timestamp` is the epoch in which the change occurred.
- `mz_diff` indicates the nature of the change (`1` for insertion, `-1` for deletion).

If you open a separate terminal, connect to the database, and insert another click event, the open `SUBSCRIBE` session will instantly print the new row with `mz_diff = 1` as soon as the transaction commits. Press `Ctrl+C` to terminate the subscription session when you are finished.

### 5. Optimistic Transaction Conflict (RS-2008)

What happens if multiple sessions try to modify the same row in RockStream? 

RockStream does not support heavy two-phase locking because it degrades write performance in sharded storage. Instead, it uses **optimistic transaction concurrency control**. Each transaction registers the set of keys it reads and writes. If another transaction writes to a key read by the current transaction before it commits, the gateway rejects the commit with error code `RS-2008` (Optimistic Write Conflict). The client must then retry the transaction. This ensures consistency without locking.

---

## Part 7 — Under the Hood Diagnostics & Troubleshooting

When you are developing complex pipelines, you need visibility into how RockStream compiles and executes your queries. RockStream provides CLI subcommands and catalog tables to help you inspect and troubleshoot your system.

### 1. ASCII View Inspection using `describe`

If you want to view the physical operator graph of a view and check how it is sharded and placed across worker nodes, use the CLI `describe` subcommand.

Open your system terminal and run:

```bash
rtk rockstream describe mv_conversion_funnel
```

This prints a structured Unicode DAG showing the physical layout of the view:

```
Materialized View: mv_conversion_funnel (ID: VIEW-902)
Status: RUNNING
Workload: campaign_attribution (Freshness SLO: 1s)

[Source: clicks] ────────► [Join: HashJoin (user_id)] ────────► [Sink: SlateDB]
                                ▲
                                │
[View: mv_purchases_enriched] ──┘
```

This graph confirms that `mv_conversion_funnel` is running, is bound to the `campaign_attribution` workload, and executes a HashJoin on `user_id` between the `clicks` source and the `mv_purchases_enriched` view.

### 2. Inspecting Operator Plans using `explain`

To see the lowered operators and their merge laws, use the `explain` subcommand:

```bash
rtk rockstream explain mv_campaign_performance
```

This prints the physical operator plan annotated with merge laws and warnings:

```
Projection: campaign_id, clicks_count, total_amount, window_start
  HashAggregate: group=[campaign_id, window_start], aggs=[COUNT(click_id), SUM(total_amount)]
    MergeLaw: COUNT -> SumCount/v1 (Associative: true, Commutative: true, Inverse: true)
    MergeLaw: SUM -> WeightAdd/v1 (Associative: true, Commutative: true, Inverse: true)
    Filter: clicks_count > 0
      TableScan: mv_conversion_funnel
```

Notice the `MergeLaw` annotations. They verify that both the `COUNT` and `SUM` aggregates have successfully bound to their respective algebraic laws, ensuring they will be updated incrementally without performance overhead.

If we run `EXPLAIN INDEX` on our query, we can see if RockStream uses optimized indexes to speed up scans:

```sql
EXPLAIN INDEX SELECT * FROM purchases WHERE user_id = 1;
```

This will print the physical indexing plan:

```
IndexScan: idx_purchases_user_id (Index State: READY, lag: 0ms)
  Filter: user_id = 1
    TableScan: purchases
```

This confirms that RockStream is using a dedicated index to scan only rows with `user_id = 1` rather than performing a full scan of the `purchases` table.

### 3. State Introspection using `debug arrangement`

If you suspect that state is accumulating incorrectly or you want to inspect raw keys in SlateDB, you can query worker arrangements directly from the CLI.

For example, to inspect keys in the join arrangement for `mv_conversion_funnel` on worker node `worker-01`, run:

```bash
rtk rockstream debug arrangement mv_conversion_funnel 3f2a "user_id=1"
```

This decodes the SlateDB keys and prints the raw arrangement records, their timestamp, and weight (+1 or -1). This is a powerful tool for troubleshooting complex state issues.

### 4. Monitoring Resource Usage

You can monitor memory usage, state size, and freshness lag for your views by querying the system catalog tables:

```sql
SELECT view_name, state_bytes, memory_bytes, freshness_lag_ms FROM rockstream_catalog.view_resource_usage;
```

Output:
```
        view_name        | state_bytes | memory_bytes | freshness_lag_ms 
-------------------------+-------------+--------------+------------------
 mv_purchases_enriched   |        2048 |          512 |               45
 mv_conversion_funnel    |        4096 |         1024 |               52
 mv_campaign_performance |        1024 |          256 |               30
 mv_top_campaigns        |         512 |          128 |               12
 mv_referral_depth       |        8192 |         2048 |               85
(5 rows)
```

This output shows:
1. The memory and storage footprint of each view.
2. The current **freshness lag** in milliseconds. In this case, all views are updating in under 100 milliseconds, easily meeting our 1-second workload SLO.

### 5. Troubleshooting with the Dead Letter Queue (DLQ)

If an external connector encounters record format errors or schema incompatibilities, it routes the malformed records to the dead letter queue instead of crashing the pipeline.

You can inspect these records by querying the DLQ:

```sql
SELECT source_name, error_code, error_message, raw_bytes_hex FROM rockstream_catalog.dead_letter_queue;
```

If you fix the schema issue or data producer, you can instruct RockStream to replay the failed records:

```sql
ALTER SOURCE kafka_purchases REPLAY DEAD_LETTER_QUEUE SINCE 1717574000;
```

### 6. Auto-Tuning Loop and Hysteresis Config

What makes RockStream unique is its ability to self-tune. Under the hood, a background daemon executes an **auto-tuning loop** that monitors:
- The rate of incoming changes.
- Shard size in SlateDB.
- CPU saturation on worker nodes.
- Freshness SLO compliance.

If the freshness lag of a workload starts to slip (e.g., if the lag exceeds 75% of the SLO target), the auto-tuner automatically:
1. Increases the parallelism of hot operator nodes (sharding the operator across more worker threads).
2. Increases the epoch commit interval to increase write throughput.

To prevent the system from constantly scaling up and down due to tiny transient spikes in workload (a behavior known as *thrashing*), the auto-tuner uses a **hysteresis configuration** (with scale-up and scale-down thresholds). You can override this hysteresis using the `tune` subcommand:

```bash
rtk rockstream tune --override hysteresis_up=0.85 hysteresis_down=0.40
```

This configures the system to scale up only when CPU utilization or lag exceeds 85%, and scale back down only when it falls below 40%, ensuring cluster stability.

---

## Part 8: Climbing the Scaling Ladder

Now that you have built a working pipeline locally, let's discuss how to transition it to production. RockStream is designed with a **one binary, one config, three-tier philosophy**. This means you use the same binary and SQL statements whether you are testing on your laptop or deploying a large, distributed production cluster.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Tier 3: Distributed Cluster                     │
│                                                                        │
│             ┌──────────────┐               ┌──────────────┐            │
│             │ Control Node │               │ Gateway Node │            │
│             └──────┬───────┘               └──────┬───────┘            │
│                    │                              │                    │
│                    ├──────────────────────────────┤                    │
│                    ▼                              ▼                    │
│             ┌──────────────┐               ┌──────────────┐            │
│             │ Worker Node  │               │ Worker Node  │            │
│             └──────┬───────┘               └──────┬───────┘            │
│                    │                              │                    │
│                    └──────────────┬───────────────┘                    │
│                                   ▼                                    │
│             ┌─────────────────────────────────────────────┐            │
│             │          Object Storage (S3 / MinIO)        │            │
│             │  Shared LSM State, Catalog, Sinks & Metadata │            │
│             └─────────────────────────────────────────────┘            │
└────────────────────────────────────────────────────────────────────────┘
```

### The Three Tiers of Deployment

1. **Tier 1: Evaluation (Laptop)**
   - Start option: `rockstream start --role=all --storage=./data`
   - All services (control plane, worker, gateway) run inside a single local process.
   - Storage is saved directly to local disk.
2. **Tier 2: Single-Host Production**
   - Start option: `rockstream start --role=all --storage=s3://my-bucket/production`
   - Services run in a single process, but state and logs are saved to an external object store (like AWS S3 or MinIO). If the host crashes, you can boot a new host pointing to the same S3 path, and it will recover instantly without loss of data.
3. **Tier 3: Distributed Cluster**
   - We separate services onto dedicated nodes for horizontal scale:
     - Boot Control Nodes: `rockstream start --role=control --control-bind=10.0.0.1:7700 --storage=s3://my-bucket/cluster`
     - Boot Worker Nodes: `rockstream start --role=worker --control=10.0.0.1:7700 --storage=s3://my-bucket/cluster`
     - Boot Gateway Nodes: `rockstream start --role=gateway --control=10.0.0.1:7700 --storage=s3://my-bucket/cluster`
   - Shards are distributed across workers, and workers exchange data using gRPC shuffles.

### Integrating with the Data Lake: Iceberg and Delta Lake

Once your views are calculated, you can stream their outputs to external systems using sinks. 

RockStream includes native sinks for **Apache Iceberg v2** and **Delta Lake**. At regular intervals (such as every 5 minutes), RockStream writes view snapshots to your cloud storage bucket as optimized columnar Parquet files, committing the metadata directly to your Iceberg REST catalog.

This enables downstream engines like DuckDB, Trino, or Apache Spark to query the latest view results directly from the cloud storage bucket—without putting any load on your active RockStream processing cluster.

### Administrative Support Bundles

If you run into issues on a production cluster, you can use the CLI to compile a diagnostic support bundle:

```bash
rtk rockstream support-bundle --output=/tmp/support-bundle.tar.gz
```

This compiles system logs, worker CPU profiles, database catalogs, config overrides, and active audit history into a single compressed tarball. You can share this bundle with database administrators or the RockStream community to debug cluster issues efficiently.

---

## Conclusion & Next Steps

Congratulations! You have completed the 30-minute RockStream starter tutorial.

In this guide, you have:
1. Explored the mathematical foundations of Incremental View Maintenance, DBSP, Z-sets, frontiers, and merge laws.
2. Bootstrapped a local evaluation instance of RockStream.
3. Connected to the gateway using standard Postgres tools.
4. Defined workloads and created base tables.
5. Constructed a multi-stage Campaign Attribution and Referral tracking DAG containing inner joins, left joins, tumbling window aggregates, rank window functions, and recursive CTEs.
6. Populated the DAG with data and verified correctness.
7. Subscribed to live updates and debugged execution using the RockStream CLI.

To continue your journey:
- Check out the [Concepts Guide](file:///Users/grove/projects/rockstream/docs/concepts.md) for a deeper look into the internals.
- Read the [CLI Subcommands Reference](file:///Users/grove/projects/rockstream/docs/cli.md) to explore administrative capabilities.
- Review the [Language Features Reference](file:///Users/grove/projects/rockstream/docs/language-features.md) to see the full list of supported SQL operators.
- Join our community Slack channel or Github Discussions to connect with other builders!

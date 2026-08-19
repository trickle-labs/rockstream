# Main conclusion

I reviewed the current code and the newly added **v0.59.19** roadmap entry. The biggest performance opportunity is not another scheduler knob or a faster hash function.

It is this:

> **Change RockStream from “each view owns mutable hash-map state” into “many views share durable, immutable, mergeable traces of state.”**

That would unlock several improvements at once:

* Updates persist only changed keys.
* Multiple views reuse the same indexes.
* Joins avoid materializing giant intermediate results.
* Windows share slices instead of storing the same events repeatedly.
* State can be compacted outside the compute workers.
* Workers can mutate state without global locks.
* New views can attach to existing arrangements instead of rebuilding everything.

The individual ingredients come from shared arrangements, differential dataflow, factorized IVM, streaming state migration, adaptive checkpointing, and object-store-native LSM systems. The interesting opportunity is combining them around **RockStream’s frontiers, merge laws, SlateDB storage, and PostgreSQL interface**. I am not claiming the combination is academically proven or universally novel; it is a RockStream-specific synthesis that should be tested rigorously.

---

# What the current code tells us

RockStream already has many of the abstractions needed for a high-performance system, but they are not yet connected end to end.

The aggregate operator keeps its state in a `Mutex<HashMap<...>>`. When persisted, `encode_as_write_batch` walks every live aggregate entry and writes it into the batch. Distinct follows the same broad pattern: its persistence code writes every nonzero entry. Join state uses nested hash maps, serializes rows into separately allocated byte vectors, and persists all live entries. Thus, one changed key can result in work proportional to the total arrangement size.

At the same time, the storage layer already installs a SlateDB merge operator, and RockStream’s `LawBundle` abstraction already knows whether an operation is associative, commutative, invertible, composable, and eligible for merge-on-compaction. That is almost exactly the information a physical optimizer needs to choose an efficient state representation. But the production aggregate path still materializes and rewrites maps rather than emitting law-native delta operands.

The runtime also creates one Tokio task and one bounded channel between adjacent operators. Every batch is received, processed, allocated into another `ArrowZSet`, and sent again. The channel capacity is statically fixed at 16 batches. This is correct and pleasantly simple, but it creates task scheduling, channel, allocation, and cache-locality overhead for pipelines containing many small stateless operators.

Finally, every `ShardDb` currently creates its own block and metadata caches with default capacities. That can fragment memory as shard count grows, while preventing hot data in one shard from borrowing unused cache capacity from another.

These are not minor implementation details. They are the points where horizontal scaling can stop working even if the high-level distributed architecture is correct.

---

# Ranked recommendations

| Priority     | Technique                                                 | Main benefit                                                      | Risk        |
| ------------ | --------------------------------------------------------- | ----------------------------------------------------------------- | ----------- |
| P0           | Law-native delta persistence                              | Removes O(total state) writes                                     | Medium      |
| P0           | Shard-owned state and stateless operator fusion           | Lower CPU, locking and scheduling overhead                        | Medium      |
| P1           | Durable shared mergeable arrangements                     | Reuses state across views; dramatically lowers memory and storage | High        |
| P1           | Factorized join-to-aggregate maintenance                  | Prevents join-output explosions                                   | Medium–high |
| P1           | Frontier-versioned predicate transfer                     | Reduces shuffle before it happens                                 | Medium      |
| P1           | Heavy/light skew routing and micro-migration              | Protects p99 latency from hot keys                                | High        |
| P1           | Shared window slice fabric                                | Reuses state across overlapping windows                           | Medium–high |
| P1           | Dual-timescale epochs and adaptive buffering              | Balances latency, throughput and object-store cost                | Medium      |
| P1           | Worker-wide cache, remote compaction and serving replicas | Lower object-store and query latency                              | Medium      |
| P2           | Changelog and adaptive unaligned checkpoints              | Lower checkpoint tail latency under backpressure                  | High        |
| P2           | Hybrid incremental versus partition rebuild               | Avoids pathological IVM updates                                   | High        |
| Experimental | Faster replicated WAL profile                             | Lower durability latency                                          | Very high   |

The first two should happen before trusting any 1/2/4/8-worker benchmark. Otherwise, the benchmark may simply measure full-state rewrites and runtime overhead rather than RockStream’s intended architecture.

---

# 1. Use merge laws as the actual storage format

## The idea

Instead of persisting the new value of every aggregate group after each epoch, persist only an operand representing what changed.

For a `SUM`/`COUNT` aggregate:

```text
key:   arrangement / operator / group-key
value: SumCountDelta {
    delta_sum,
    delta_count
}
```

Each epoch first consolidates all changes for a key:

```text
(+10, +1)
(+20, +1)
(-10, -1)
------------
(+20, +1)
```

Then it emits exactly one SlateDB merge operand for that key.

SlateDB can resolve associative merge operands during reads, flushes, or compaction. Its API explicitly supports `WriteBatch::merge`, and RockStream already installs a merge operator. ([SlateDB][1])

## Important implementation details

RockStream should add a first-class `EpochStateDelta` interface:

```rust
pub enum StateMutation {
    Merge {
        key: EncodedKey,
        law: MergeLawId,
        operand: Bytes,
    },
    Put {
        key: EncodedKey,
        value: Bytes,
    },
    Delete {
        key: EncodedKey,
    },
}
```

Each stateful operator returns:

```rust
pub struct OperatorEpochResult {
    pub output_delta: ArrowZSet,
    pub state_mutations: Vec<StateMutation>,
    pub metrics: OperatorEpochMetrics,
}
```

The runtime combines every operator’s mutations into the same atomic epoch commit. There is no separate “walk the operator’s state later” persistence phase.

For `SUM`, `COUNT`, and similar group-like laws:

* Use signed deltas.
* Consolidate duplicate keys once per epoch.
* Write the identity value when a group reaches zero.
* Let a compaction filter remove identities once the retention frontier permits it.
* Do not read the existing value merely to persist a change.

For operators that cannot safely use merge operands, maintain a dirty-key journal:

```text
dirty upserts
dirty deletes
```

Only those keys are emitted at commit.

## Fix arithmetic semantics at the same time

The current SlateDB merge implementation uses wrapping arithmetic for sum and count. That is fast, but SQL overflow must not silently wrap. A durable merge operator should use a wider internal representation—likely `i128` for integer sum state—and fail the epoch with the correct SQL error if the declared output range is exceeded.

## Proof criteria

The decisive test is not rows per second. It is:

> A one-row update against arrangements containing 1,000, 100,000 and 10,000,000 groups produces approximately the same number of state mutations and logical write bytes.

Also require:

* 1,000 changes to one key in one epoch produce one consolidated merge operand.
* Restart restores exactly the same state.
* Write amplification is measured separately for logical operands, WAL, flush and compaction.
* Deletes eventually reclaim space once the compaction frontier permits it.
* No full arrangement scan occurs on the commit hot path.

This is likely the single highest-confidence performance improvement.

---

# 2. Build Durable Shared Mergeable Arrangements

Shared arrangements are a known technique in streaming dataflow: multiple operators or queries reuse one maintained indexed representation instead of independently building equivalent state. Differential dataflow exposes arranged collections specifically so communication, computation and memory can be shared, and research on shared arrangements reports large reductions in resource use and response time for concurrent queries. ([arXiv][2])

RockStream can extend that idea in an object-storage-native direction.

## Arrangement identity

Today, plan nodes largely carry plan-specific arrangement IDs. Instead, define arrangements by their logical meaning:

```rust
pub struct ArrangementSpec {
    pub input_fingerprint: PlanFingerprint,
    pub key_expressions: Vec<CanonicalExpr>,
    pub value_projection: Vec<CanonicalExpr>,
    pub type_representation_version: TypeEncodingVersion,
    pub collation_version: CollationVersion,
    pub time_domain: TimeDomain,
    pub law_id: MergeLawId,
    pub law_version: MergeLawVersion,
    pub partitioning: PartitioningSpec,
}
```

The catalog hashes this structure into a stable `ArrangementId`.

Two views asking for the same arrangement reuse it.

Examples:

```sql
SELECT customer_id, SUM(amount) ...
SELECT customer_id, COUNT(*) ...
SELECT customer_id, SUM(amount), COUNT(*) ...
```

All three may reuse the same input keyed by `customer_id`, and potentially the same combined `SumCount` payload.

## Canonicalization is essential

Sharing only works when logically equivalent expressions receive the same fingerprint. RockStream should normalize:

* No-op casts.
* Equivalent integer representations.
* Commutative expression argument ordering where safe.
* Aliases and projection order.
* Redundant maps and projects.
* Equivalent key encodings.
* Collation and time-zone semantics.

Materialize has described how semantically harmless casts and type distinctions can prevent common-subexpression and arrangement reuse; one reported customer workload saw a 25% memory reduction after related canonicalization improvements. That should not be treated as a RockStream forecast, but it demonstrates that planner normalization can have material physical consequences. ([Materialize][3])

## Immutable trace layout

I would not make the shared arrangement one enormous mutable hash map.

Use a differential-style trace:

```text
Arrangement
├── compacted snapshot through frontier F
├── immutable sorted delta batch F+1
├── immutable sorted delta batch F+2
├── immutable sorted delta batch F+3
└── subscriber frontiers
```

An epoch creates one sorted, consolidated delta batch. Readers see:

```text
snapshot_F + all deltas through requested frontier
```

Background compaction advances the snapshot frontier when every subscriber has moved beyond the old history.

This structure has useful consequences:

* No in-place shared mutation.
* Readers can pin a consistent frontier.
* New views can attach at an existing frontier.
* Compaction is independent of query execution.
* Historical reads naturally use retained batches.
* Recovery refers to immutable manifests.

## New-view installation

When a new view needs an existing arrangement:

1. Pin arrangement frontier `F`.
2. Read its compacted snapshot through `F`.
3. Subscribe to deltas after `F`.
4. Build only the view-specific downstream state.
5. Publish the view after it reaches the cluster frontier.

RockStream already has the snapshot/delta-fence concept needed to prevent gaps or duplicates.

## Proof criteria

Create 20 views sharing the same source keying:

* Physical arrangement count remains one.
* State bytes stay below roughly 1.3× the one-view case, excluding truly view-specific output.
* CPU required for source-key maintenance stays roughly constant as views are added.
* Installing view 20 does not rescan the source relation.
* Dropping one view does not delete shared state.
* Dropping the last consumer eventually reclaims the arrangement.
* `EXPLAIN INCREMENTAL` reports `arrangement_id`, consumer count and bytes saved through reuse.

This has a higher implementation cost than dirty-key persistence, but it could become one of RockStream’s strongest differentiators.

---

# 3. Make keys once and reuse them everywhere

RockStream currently repeatedly turns columns into map keys or row byte vectors. Join state, for example, serializes each row into a new `Vec<u8>` and generates separately allocated key material.

Introduce a **canonical key capsule** carried with the batch:

```rust
pub struct KeyCapsule {
    pub key_spec_id: KeySpecId,
    pub encoded_rows: Arc<BinaryArray>,
    pub hashes: Arc<UInt64Array>,
}
```

A batch envelope becomes:

```rust
pub struct BatchEnvelope {
    pub data: RecordBatch,
    pub weights: WeightArray,
    pub keys: SmallVec<[KeyCapsule; 2]>,
    pub frontier: FreshnessToken,
}
```

The planner recognizes that exchange, join, group-by and storage need the same key expression. It computes and encodes that key once.

Then:

* Exchange uses the cached hash.
* Join uses the encoded key.
* Aggregate uses the same encoded key.
* Storage uses the same key bytes.
* The skew detector uses the same hash.
* Debugging can decode the canonical key through one registry.

For collision-sensitive operations, the hash narrows the search and the full normalized bytes establish equality.

## Dual-path batch consolidation

Use different consolidation paths depending on batch shape:

* Small batch: stack-friendly hash table or small vector.
* Large batch: radix-sort encoded keys, then run-length sum weights.
* Already sorted batch: linear consolidation.
* High duplication: pre-combine aggressively before exchange.

Sorted large batches are particularly attractive because the same ordering can be consumed directly by trace insertion and storage batching.

## Proof criteria

Measure:

* Bytes allocated per input row.
* Key encodings per input row.
* Hash computations per input row.
* CPU in key packing and hashing.
* Cache misses.
* Temporary row-copy bytes.

A multi-stage `exchange → join → aggregate → view sink` should encode each relevant logical key once, not once per stage.

---

# 4. Replace per-operator mutexes with shard-owned actors

A mutex around state is convenient even when only one task is meant to mutate that state. But it imposes locking and makes it easy to accidentally share one state structure across too much parallel work.

Instead:

> One shard actor owns each shard’s mutable overlay. Nobody else locks or mutates it.

```text
ShardActor
├── input mailbox
├── operator overlays
├── dirty mutations
├── current epoch
├── storage writer
└── local metrics
```

Each actor processes one state mutation stream sequentially. Parallelism comes from having many independently owned shards and virtual buckets.

This is similar to the principle behind partitioned state machines:

* No hot-path mutex.
* No lock poisoning.
* Better cache locality.
* Easier epoch ownership.
* Easier deterministic replay.
* Easier profiling.

## Fuse stateless operators

The current runtime creates a Tokio task and channel for every operator. Fuse adjacent stateless stages:

```text
Filter → Map → Project → KeyPack → Consolidate
```

into one executable kernel:

```text
FusedKernel → stateful boundary
```

Keep task/channel boundaries only at:

* Stateful ownership boundaries.
* Exchange boundaries.
* Source/sink boundaries.
* Explicit scheduling or isolation boundaries.

A 12-operator logical pipeline might become three runtime actors instead of 12 tasks.

## Adaptive morsels

For CPU-heavy stateless work, divide batches into cache-sized morsels. Worker threads can process those morsels in parallel, but all state mutations return to the shard owner in deterministic key order.

This combines parallel computation with single-owner state.

## Proof criteria

* Lock-wait time below 1% of worker wall time.
* No global or operator-wide mutex consumes more than 5% of profiles.
* Channels crossed per input row decrease substantially.
* Fused and unfused results remain bit-identical.
* Tiny-batch p50 latency improves, while large-batch throughput does not regress.
* State replay remains deterministic.

---

# 5. Factorize join-to-aggregate maintenance

This may produce the largest gain for realistic analytical views.

Consider:

```sql
SELECT c.region, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.id
GROUP BY c.region;
```

A conventional incremental join can emit one joined row for every matching order and then aggregate those rows.

A factorized plan maintains:

```text
orders_by_customer[customer_id] = SUM(amount)
customer_region[customer_id]    = region
region_sum[region]               = SUM(customer payloads)
```

When an order changes:

```text
orders_by_customer[cid] += delta
region_sum[customer_region[cid]] += delta
```

When a customer moves regions:

```text
payload = orders_by_customer[cid]

region_sum[old_region] -= payload
region_sum[new_region] += payload
customer_region[cid] = new_region
```

It does not enumerate all of that customer’s orders.

Factorized IVM represents intermediate results as compact payloads in an algebraic structure rather than flat joined rows. Research on F-IVM and higher-order maintenance reports orders-of-magnitude gains on suitable workloads, especially when joins feed aggregates. Related work shows that avoiding materialized intermediate joins can eliminate polynomial intermediate blowups. These results are workload-specific, but the structural lesson applies directly to RockStream. ([arXiv][4])

## Start with narrow, valuable patterns

Do not attempt a universal factorized SQL engine first.

Implement in this order:

1. Primary-key/foreign-key join feeding `SUM`, `COUNT` or `AVG`.
2. Star-schema joins feeding grouped aggregates.
3. Semijoin and antijoin views.
4. Two-sided keyed aggregates.
5. General acyclic join trees.
6. Cyclic joins only after the earlier forms are proven.

RockStream’s `LawBundle` can provide the payload algebra.

## Add a Delta Amplification Governor

Every operator should measure:

```text
row amplification   = output delta rows / input delta rows
byte amplification  = output bytes / input bytes
probe amplification = state matches examined / input row
shuffle amplification
```

When amplification exceeds predicted limits, the system can identify why:

```text
JOIN_FANOUT
HOT_KEY
WINDOW_OVERLAP
RETRACTION_CASCADE
UNSHARED_ARRANGEMENT
```

The optimizer can then choose:

* Factorized join-to-aggregate.
* Predicate transfer.
* Heavy/light routing.
* Partition rebuild.
* Additional pre-shuffle combining.

This turns performance degradation into a named, actionable condition rather than a vague “compute lag.”

## Frontier-safe adaptive plan switching

Adaptive factorization research shows that runtime cardinality or degree sketches can select between classic and factorized join strategies. RockStream can do this safely at frontier boundaries rather than changing a live plan in place. ([DuckDB][5])

A safe switch:

1. Build an alternate plan against shared arrangements at frontier `F`.
2. Run both plans for several bounded epochs.
3. Compare output multiset digests.
4. Cut the catalog pointer over atomically at a committed frontier.
5. Retain the old plan until all readers have passed the cutover.
6. Abort the switch on any mismatch.

No opaque reinforcement-learning controller is needed. Start with explainable thresholds based on key degree, delta amplification, state size and shuffle bytes.

---

# 6. Use frontier-versioned predicate transfer

Bloom filters are commonly used at one join, but predicate transfer pushes membership information across a multi-join graph so rows that cannot possibly contribute are removed before expensive joins and exchanges. Recent work reports substantial reductions in multi-join execution time compared with more local Bloom-filter approaches. ([mail.vldb.org][6])

RockStream’s challenge is deletions: a stale Bloom filter could otherwise cause a false negative.

Frontiers provide a clean solution.

## Safe filter design

At committed frontier `F`, build:

```text
bloom_F = all keys present at F
```

Then track exactly:

```text
keys_added_after_F
```

For an event at a later frontier:

```rust
fn might_match(key: K) -> bool {
    keys_added_after_f.contains(key) || bloom_f.might_contain(key)
}
```

A deleted key can remain in `bloom_F`, causing a harmless false positive.

A newly added key is found in `keys_added_after_F`, preventing a false negative.

Periodically rebuild the Bloom filter at a later frontier and clear the added-key overlay.

This avoids a complicated distributed counting Bloom filter while remaining deletion-safe.

## Where to apply it

* Before cross-worker exchanges.
* Before probing large join arrangements.
* Between multiple joins.
* On source connectors when safe key predicates can be pushed down.
* Before building expensive window or aggregate state.

## Proof criteria

* Zero false negatives under insert, update, delete and crash replay.
* At least 50% shuffle-byte reduction on a deliberately selective multi-join reference workload.
* Filter construction cost included in the optimizer.
* Filters are disabled when their CPU or memory cost exceeds saved work.
* `EXPLAIN INCREMENTAL` shows filter frontier, false-positive estimate and bytes avoided.

---

# 7. Redesign skew handling around heavy/light execution

The current `BucketedAggregateOp` still updates a central combined state map under a mutex, so salting the partials may not remove the final hot update point. I also found construction references primarily in oracle and durability tests rather than the ordinary production compilation path.

## Distributed heavy-key aggregate

For a heavy key:

```text
(key, bucket_0) → partial state
(key, bucket_1) → partial state
...
(key, bucket_n) → partial state
```

Do not recombine on every record.

Recombine:

* Once per logical epoch.
* Through a tree, not one central worker.
* On demand for reads when permissible.
* Incrementally through merge operands.

For eight buckets:

```text
0 ─┐
1 ─┴─ A ─┐
2 ─┐     │
3 ─┴─ B ─┴─ root
4 ─┐     │
5 ─┴─ C ─┤
6 ─┐     │
7 ─┴─ D ─┘
```

## Power-of-two routing

For composable aggregates, choose two candidate buckets and route to the less loaded one. Research on heavy-hitter-aware routing and multiple choices shows that this can substantially improve load distribution under skew. ([arXiv][7])

Because RockStream must replay deterministically, the choice cannot depend on an unrecorded instantaneous load measurement.

Use one of:

* Record the chosen bucket in the durable source-epoch record.
* Commit a versioned load snapshot and routing generation.
* Use deterministic per-generation quotas.

The first option is simplest and safest.

## Heavy/light join

For a heavy join key:

* Partition the high-cardinality side.
* Replicate the small side across its buckets.
* Avoid replicating both sides.
* If the downstream operation is algebraic, keep the result factorized rather than generating every pair.

The heavy/light decision should use:

```text
key frequency
left degree
right degree
update rate
row width
downstream law
```

## Latency-bounded micro-migration

Do not move an entire shard in one operation. Megaphone subdivides state and schedules migration at a controlled granularity, reducing latency spikes during reconfiguration. RockStream should migrate small virtual bins at successive frontiers. ([arXiv][8])

A migration schedule might be:

```text
frontier 1001 → bin 0
frontier 1002 → bin 1
frontier 1003 → bin 2
...
```

Only one small portion is in expensive dual-write/copy state at a time.

## Heat-Carrying Leases

When a shard lease moves, include a non-authoritative heat summary:

```rust
pub struct LeaseHeatHint {
    pub hot_key_prefixes: Vec<KeyPrefix>,
    pub hot_sst_ids: Vec<SstId>,
    pub hot_block_ranges: Vec<BlockRange>,
    pub expected_read_rate: u64,
}
```

The receiving worker prefetches these blocks into its shared cache before cutover. The state remains authoritative in SlateDB; the hints merely reduce cold-start latency.

## Proof criteria

* A key at 50× median load does not force one worker to 50× CPU.
* Hot-key p99 freshness stays within 2× the uniform baseline.
* Migration never causes more than the declared temporary latency or throughput degradation.
* Recipient cache hit rate is high immediately after cutover.
* Routing is bit-identical after replay.
* No manual resharding command is required.

---

# 8. Build a shared window slice fabric

Separate hopping, tumbling, sliding and session windows often retain overlapping copies of the same events or aggregate state.

A better physical design is:

```text
source events
      ↓
shared time slices
      ↓
 ┌────┼─────┬────────┐
10s   1m    5m       session
view  view  view      view
```

Scotty demonstrates general stream slicing that can support multiple window types and share aggregate state. FiBA provides efficient sliding-window aggregation for slightly out-of-order streams. Factor Windows explores cost-based auxiliary windows shared by multiple user windows. The published gains vary by workload, but all point to the same principle: correlated windows should not maintain fully independent histories. ([doi.org][9])

## Choose the data structure from the merge law

For an invertible group law:

```text
window(a,b) = prefix(b) - prefix(a)
```

For an associative but non-invertible law:

* Two-stack aggregation.
* Segment tree.
* Finger tree.
* FiBA-like structure for out-of-order updates.

For many known window sizes:

* Use shared slices.
* Choose slice boundaries based on workload.
* Materialize auxiliary “factor windows” where the cost model says reuse is worthwhile.

For session windows:

* Store gap-delimited slices.
* Merge adjacent slices when a late event bridges the gap.
* Retain only frontier-relevant slice boundaries.

## Planner surface

`EXPLAIN INCREMENTAL` should report:

```text
window_algorithm = prefix_difference
shared_slice_arrangement = arr-0017
base_slice_width = 1000ms
consumer_windows = [10s, 60s, 300s]
estimated_state_reuse = 83%
```

## Proof criteria

* Twenty correlated windows use much less than 20× one-window state.
* Late and out-of-order events remain oracle-identical.
* Shared-window maintenance does not increase p99 freshness.
* The planner can decline sharing when window shapes or laws make it more expensive.

---

# 9. Separate logical epochs from physical storage commits

RockStream needs small epochs for freshness but larger writes for object-store efficiency.

Those do not have to be the same unit.

## Dual-timescale execution

```text
Logical epoch:
    progress, consistency, output visibility

Physical commit group:
    object-store batching and WAL amortization
```

For example:

```text
logical epochs:  E101 E102 E103 E104
physical write:  [E101 | E102 | E103 | E104]
```

The physical object is durable once. Its index records the boundaries of all four logical epochs. The system may then publish their frontiers in order.

This preserves logical semantics while amortizing PUT latency.

SlateDB guidance notes that object-store writes have much higher latency than local cache hits, making batching and caching central to performance. ([SlateDB][10])

## Replace “16 batches” with “milliseconds of queued work”

The current channel capacity is fixed at 16 batches regardless of whether a batch represents 10 rows or one million rows.

Use a byte and time budget:

```text
target_inflight_ms
max_inflight_bytes
max_inflight_rows
```

If average downstream service time is 5 ms and the target is 50 ms:

```text
credit window ≈ 10 batches
```

As batch size and service time change, the window changes.

Flink’s buffer-debloating work follows a related principle: control the amount of in-flight data in terms of target processing time rather than relying solely on static buffer counts. ([nightlies.apache.org][11])

## SLO controller

Inputs:

```text
freshness slack
queue fill
CPU saturation
object-store PUT latency
checkpoint backlog
compaction debt
delta amplification
```

Outputs:

```text
logical epoch duration
physical commit group size
source credits
operator morsel size
exchange batch size
```

Use a bounded, explainable controller such as AIMD or a conservative PID loop. Every decision should be logged:

```text
physical_commit_epochs: 3 → 5
reason: object_store_put_latency
freshness_slack_ms: 620
```

More workers can hurt very small differential updates because coordination overhead outweighs parallel work, so parallelism should also be adaptive rather than always maximal. ([GitHub][12])

---

# 10. Use changelog checkpoints, with unaligned fallback only when needed

RockStream currently models aligned checkpoints: sources drain, barriers propagate and state is checkpointed after alignment. Alignment is bounded, but under backpressure it can cause long checkpoint tails.

## Changelog checkpointing

Once state writes are delta-native, continuously durable operator mutations become a checkpoint changelog:

```text
base checkpoint at F
+ state deltas F+1…G
= recovery state at G
```

Creating a checkpoint then mostly writes a manifest referencing:

* Base snapshot.
* Delta range.
* Source offsets.
* Sink transaction state.
* Frontier.
* Format versions.

Flink’s changelog state backend similarly moves work from checkpoint time into continuous state-change uploads, lowering checkpoint latency at the cost of ongoing I/O and potentially more recovery replay. ([nightlies.apache.org][13])

## Adaptive unaligned fallback

Start aligned.

If:

```text
barrier_flight_time > threshold
and channel bytes are bounded
and object-store headroom is healthy
```

switch that checkpoint to unaligned mode.

Store:

```text
channel sequence
buffer contents
input frontier
watermark
sender and receiver identity
```

On recovery, restore operator state and replay the captured channel tails exactly once.

Unaligned checkpoints can make checkpoint duration less dependent on pipeline backpressure, but they increase checkpoint bytes and recovery complexity. They should be an escape hatch selected by measured conditions, not the default. ([nightlies.apache.org][11])

## Reserve barrier capacity first

Before implementing unaligned checkpoints, add a dedicated control/barrier lane or reserved credits so barriers cannot sit behind ordinary data indefinitely. Then measure whether alignment remains a real bottleneck.

---

# 11. Share cache across the worker and separate compaction

SlateDB supports caches that can be shared by many databases and readers, including disk-backed object-store caching. It also supports a standalone compactor. ([SlateDB][14])

## Worker-wide storage context

```rust
pub struct WorkerStorageContext {
    pub block_cache: Arc<dyn DbCache>,
    pub object_cache: Arc<dyn ObjectStoreCache>,
    pub cache_budget: CacheBudget,
    pub compactor_client: CompactorClient,
}
```

Every `ShardDb` on the worker receives this shared context.

Benefits:

* One shard can borrow unused cache capacity.
* Identical SST blocks opened by multiple readers are stored once.
* Reassigned shards can use already warm blocks.
* Memory is bounded at the worker level rather than multiplied by shard count.
* Cache policy can prioritize active views over backfills or scans.

Use admission policies that resist cache pollution from one-time scans. Pin metadata, indexes and filters more aggressively than data blocks.

## Local NVMe object cache

Object storage remains authoritative.

Local NVMe stores:

```text
immutable SST blocks
indexes
filters
checkpoint manifests
shuffle objects where safe
```

On cache loss, RockStream simply refetches them.

## Separate compaction workers

Compaction should not unexpectedly steal CPU, memory bandwidth or object-store request budget from freshness-critical compute.

A dedicated compactor role can:

* Compact many shards.
* Prioritize based on L0 backlog and read amplification.
* Pause when object-store latency rises.
* Respect view freshness priority.
* Run on cheaper or differently sized compute.

## Frontier-pinned serving replicas

For frequently queried materialized views, maintain an evictable gateway-local serving replica:

```text
view output deltas
      ↓
gateway-local compacted replica at frontier F
```

A query can use the replica when:

```text
replica_frontier >= requested_frontier
```

Otherwise it waits or falls back to the authoritative multi-shard read.

This can remove scatter latency for popular dashboards without making the gateway authoritative.

## Proof criteria

* Cache memory remains constant as shard count increases.
* Remote GETs fall materially under repeated query and recovery workloads.
* p99 query latency improves for hot views.
* Compaction pressure no longer produces unexplained compute-lag spikes.
* Lease migration with prewarming avoids a cold-cache latency cliff.
* Serving replicas never answer behind an explicit freshness token.

---

# 12. Choose between incremental maintenance and localized rebuilding

Incremental maintenance is not always cheapest.

A one-row update should be incremental. But if a bulk load changes 60% of a partition and causes enormous join fan-out, rebuilding that partition may cost less.

Modern materialized-view systems increasingly use cost models to choose between maintenance strategies across a portfolio of views rather than blindly applying one delta plan. Databricks’ Enzyme work is one recent example of cost-based strategy selection and shared refresh planning. ([arXiv][15])

## RockStream strategy choices

```rust
pub enum MaintenanceStrategy {
    Incremental,
    RebuildKeyRange(KeyRange),
    RebuildPartitions(Vec<PartitionId>),
    FullRebuild,
}
```

The planner estimates:

```text
delta rows
affected keys
join degrees
predicted output amplification
state probes
shuffle bytes
object-store writes
rebuild scan bytes
available freshness slack
```

## Localized rebuild protocol

1. Identify affected key ranges or partitions.
2. Pin source snapshot and frontier `F`.
3. Build replacement state for only those ranges.
4. Continue collecting live deltas after `F`.
5. Catch the replacement up.
6. Atomically swap the range pointer at a frontier.
7. Retire old state after readers pass the cutover.

This is the same safe replacement pattern as view backfill, applied to part of an arrangement.

## Proof criteria

* The strategy is visible in `EXPLAIN INCREMENTAL`.
* Historical measurements calibrate the estimator.
* Deliberately pathological bulk updates select localized rebuild.
* Small updates never accidentally select rebuild.
* Both paths produce identical output.
* A strategy change cannot occur without a frontier-safe cutover.

---

# 13. Optional future work: a faster replicated WAL

Object-store durability imposes a latency floor. SlateDB has explored pluggable WAL backends, including a Kafka-backed prototype that reported substantially lower durable-to-reader visibility latency than ordinary object-store PUTs in its test environment. That work is still draft-level and should not be treated as a production promise. ([SlateDB][16])

A future RockStream low-latency profile could use:

```text
replicated log for WAL
+
object storage for LSM/SST/checkpoints
```

Possible implementations:

* Kafka/Redpanda.
* A dedicated replicated WAL service.
* A low-latency object-store tier such as S3 Express.

I would not put this before v1. It adds a new correctness-critical dependency and requires formal verification of:

* Writer fencing.
* Log truncation.
* Checkpoint/WAL garbage collection.
* Recovery across log and object-store divergence.
* Rolling upgrades.
* Multi-tenant isolation.

First eliminate RockStream’s avoidable full-state writes and scheduler overhead. Otherwise, a faster WAL only makes inefficient work somewhat faster.

---

# The architecture I would aim for

```text
                     source deltas
                           │
                           ▼
              canonicalize + key capsules
                           │
                           ▼
                 consolidate per epoch
                           │
             ┌─────────────┴─────────────┐
             ▼                           ▼
   shared mergeable traces       frontier-versioned
   on SlateDB                    predicate filters
             │                           │
             └─────────────┬─────────────┘
                           ▼
       factorized joins / shared window slices
                           │
                           ▼
             shard-owned state actors
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
        view output trace       Kafka sink deltas
              │
     ┌────────┴────────┐
     ▼                 ▼
gateway serving   ordinary shard
   replica             reads

Background:
- worker-wide block/object cache
- standalone compaction
- changelog checkpoints
- latency-bounded bin migration
- SLO controller
```

The central invariant is:

> **Compute creates immutable, consolidated deltas. Storage and compaction merge them. Views and readers share them at explicit frontiers.**

---

# I would revise the roadmap again

The newly added v0.59.19 is directionally right and already includes truthful external measurement, delta-proportional persistence, live scaling proof and 1/2/4/8-worker gates.

But it now combines:

* A benchmark harness.
* State-storage redesign.
* Lock removal.
* Real hot-key routing.
* Automatic splitting.
* HPA integration.
* Large-state operation.
* Overload recovery.
* Full scale qualification.

That is not one approximately six-person-week version. The roadmap itself says oversized work should be split rather than rushed.

I would use the following sequence.

## v0.59.19 — Delta-Native State Foundation

Scope:

* External benchmark/oracle skeleton.
* Baseline profile on one real worker.
* `EpochStateDelta`.
* Dirty-key persistence for every stateful operator.
* Law-native SlateDB merge operands for sum/count and other safe laws.
* Logical versus physical write-amplification metrics.
* Checked arithmetic.

Gate:

* One-key updates are independent of total state size.
* No operator walks all state during ordinary commit.
* Baseline numbers are externally measured and reproducible.

## v0.59.20 — Durable Shared Arrangement Fabric

Scope:

* Canonical expression and key fingerprints.
* Shared arrangement catalog.
* Immutable trace batches.
* Consumer frontiers and compaction frontiers.
* New-view attachment to existing arrangements.
* Worker-wide memory and disk caches.
* Arrangement reuse in `EXPLAIN`.

Gate:

* Twenty equivalent consumers share one arrangement.
* New views attach without rescanning the source.
* State and cache memory remain bounded as shard and view counts rise.

## v0.59.21 — Factorized and Filtered IVM

Scope:

* PK/FK join-to-aggregate factorization.
* Star-join payload trees.
* Delta Amplification Governor.
* Frontier-versioned predicate transfer.
* Canonical key capsules.
* Safe shadow-plan comparison and frontier cutover.

Gate:

* High-fan-out join-to-aggregate no longer emits the full joined intermediate.
* Selective joins reduce shuffle substantially.
* Adaptive switches remain oracle-identical.

## v0.59.22 — Shared Windows and Skew-Aware Execution

Scope:

* Shared time-slice arrangements.
* Law-specific window algorithms.
* Distributed hot-key bucket combining.
* Heavy/light joins.
* Deterministic power-of-two routing.
* Frontier-scheduled micro-migration.
* Heat-carrying leases.

Gate:

* Correlated windows reuse state.
* A sustained 50× hot key remains within the declared freshness envelope.
* Migration has a hard p99 latency-spike bound.

## v0.59.23 — SLO-Adaptive Runtime and Storage

Scope:

* Shard-owned actors.
* Stateless operator fusion.
* Adaptive morsels.
* Time/byte-based credit windows.
* Logical epochs versus physical commit groups.
* Changelog checkpoints.
* Reserved barrier capacity.
* Adaptive unaligned fallback.
* Standalone compaction.
* Frontier-pinned serving replicas.

Gate:

* Lock and channel overhead is negligible in profiles.
* The freshness controller converges without oscillation.
* Backpressure and checkpoint stress stay bounded.
* Query latency improves for replicated hot views.

## v0.59.24 — Pure Horizontal Scale Qualification

This version should change no engine architecture.

It only runs the exact signed candidate through:

* 1, 2, 4 and 8 workers.
* Uniform aggregation.
* High-cardinality aggregation.
* Factorized and unfactorized joins.
* Shared windows.
* Zipf/skew.
* State larger than memory.
* 120% offered-load recovery.
* Worker loss, migration and compaction pressure.

The existing v0.59.19 numerical scale gates can move here. This preserves the crucial distinction:

> Engineering versions improve the engine. The final version proves the engine.

If extending the version table is undesirable, use these as five mandatory internal sign-off slices of v0.59.19, and do not allow the final scale run to pass until every preceding slice has its own immutable evidence.

---

# What I would implement first

The first concrete patch sequence should be:

1. Add `OperatorEpochResult` and `StateMutation`.
2. Instrument every stateful operator’s dirty keys, logical write bytes and full-state iterations.
3. Convert aggregate state to delta-only persistence.
4. Convert distinct, join, windows and Top-K to dirty-key persistence.
5. Introduce a worker-owned shared cache.
6. Remove mutexes from the ordinary shard-owner execution path.
7. Fuse stateless stages.
8. Establish the honest one-worker baseline.
9. Implement arrangement canonicalization and sharing.
10. Implement the first factorized PK/FK join-to-aggregate.

That order matters. Shared arrangements built on full-map persistence would merely share an inefficient representation. Adaptive checkpoints built before delta-native state would have a much larger state-transfer burden. Horizontal benchmarking before either fix would produce numbers dominated by known structural inefficiencies.

---

# What I would not do

I would avoid four tempting distractions:

**Do not begin with machine learning.** Degree sketches and explicit thresholds are easier to debug, reproduce and audit.

**Do not add ten more tuning knobs.** The system should measure service time, queue depth, freshness slack and storage pressure, then choose bounded values automatically.

**Do not chase SIMD first.** Vectorization is valuable, but eliminating full-state writes, duplicate arrangements and join explosions is likely to dominate any instruction-level gain.

**Do not make unaligned checkpoints the default.** First reserve control capacity and measure barrier flight. Switch modes only when alignment is demonstrably the bottleneck.

---

# Bottom line

The most powerful RockStream-specific design is:

> **Durable shared arrangements composed from immutable, law-tagged delta batches, processed by single-owner shard actors and consumed by factorized, amplification-aware operators.**

Everything else—cache sharing, micro-migration, shared windows, predicate transfer, changelog checkpoints, serving replicas and adaptive batching—fits naturally around that core.

The top three investments are therefore:

1. **Law-native delta persistence.**
2. **Durable shared arrangements with canonicalized keys.**
3. **Factorized join-to-aggregate execution with an amplification governor.**

Those three attack the fundamental quantities that determine whether RockStream scales:

```text
work per changed row
state copies per logical fact
intermediate rows per input delta
```

Get those close to constant, and adding workers has a strong chance of helping. Leave them proportional to total state or join fan-out, and no amount of orchestration will produce consistently good throughput or latency.

[1]: https://slatedb.io/docs/design/merge-operators/ "https://slatedb.io/docs/design/merge-operators/"
[2]: https://arxiv.org/abs/1812.02639 "https://arxiv.org/abs/1812.02639"
[3]: https://materialize.com/blog/no-classification-without-representation/ "https://materialize.com/blog/no-classification-without-representation/"
[4]: https://arxiv.org/abs/2303.08583 "https://arxiv.org/abs/2303.08583"
[5]: https://duckdb.org/library/factorization/ "https://duckdb.org/library/factorization/"
[6]: https://mail.vldb.org/cidrdb/2024/predicate-transfer-efficient-pre-filtering-on-multi-join-queries.html "https://mail.vldb.org/cidrdb/2024/predicate-transfer-efficient-pre-filtering-on-multi-join-queries.html"
[7]: https://arxiv.org/abs/1510.05714 "https://arxiv.org/abs/1510.05714"
[8]: https://arxiv.org/abs/1812.01371 "https://arxiv.org/abs/1812.01371"
[9]: https://doi.org/10.1145/3433675 "https://doi.org/10.1145/3433675"
[10]: https://slatedb.io/docs/operations/tuning/ "https://slatedb.io/docs/operations/tuning/"
[11]: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpointing_under_backpressure/ "https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpointing_under_backpressure/"
[12]: https://github.com/TimelyDataflow/differential-dataflow "https://github.com/TimelyDataflow/differential-dataflow"
[13]: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/state_backends/ "https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/state_backends/"
[14]: https://slatedb.io/docs/design/caching/ "https://slatedb.io/docs/design/caching/"
[15]: https://arxiv.org/abs/2603.27775 "https://arxiv.org/abs/2603.27775"
[16]: https://slatedb.io/rfcs/0030-pluggable-wal/ "https://slatedb.io/rfcs/0030-pluggable-wal/"

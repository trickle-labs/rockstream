# RockStream IVM Design

How RockStream's incremental view maintenance works, derived from a deep
reading of two production IVM systems:

1. **DBSP** (`../dbsp`) — a mathematical framework for streaming dataflow.
   Compiles SQL via Apache Calcite into a *circuit* of strongly typed operators
   that process Z-set batches in memory and on local disk. Provably correct via
   the DBSP calculus.
2. **pg_trickle** (`../pg-trickle1`) — a PostgreSQL 18 extension. Parses each
   view's SQL into an `OpTree`, runs a per-operator differentiation pass that
   emits a single SQL `WITH` chain (the "delta query"), and executes that
   query inside Postgres to compute deltas from change buffer tables.

We adopt the **semantic model** from DBSP (native Z-set operators,
long-lived circuits, traces/arrangements, frontier-based scheduling) and use
pg_trickle as the **correctness oracle** for hard SQL edge cases. pg_trickle's
per-operator SQL differentiation rules are reference material, not runtime
code; they supply tests, metadata requirements, and known failure cases such as
EC-01 joins, Q07 double-counting, Q21 SemiJoin interactions, aggregate rescans,
and recursive DRed fallback.

> Cross-references: [DESIGN.md](DESIGN.md) (system architecture),
> [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) (phased roadmap).

---

## Table of Contents

1. [What the Two Reference Systems Actually Do](#1-what-the-two-reference-systems-actually-do)
2. [Side-By-Side Comparison](#2-side-by-side-comparison)
3. [What RockStream Inherits From Each](#3-what-rockstream-inherits-from-each)
4. [The RockStream IVM Architecture](#4-the-rockstream-ivm-architecture)
5. [Intermediate Representation: PlanIR](#5-intermediate-representation-planir)
6. [The Differentiation Pass](#6-the-differentiation-pass)
7. [Per-Operator Differentiation Rules](#7-per-operator-differentiation-rules)
8. [The Circuit Runtime](#8-the-circuit-runtime)
9. [Arrangements: State on SlateDB](#9-arrangements-state-on-slatedb)
10. [Scheduling & The Epoch Loop](#10-scheduling--the-epoch-loop)
11. [Recursion (`WITH RECURSIVE`)](#11-recursion-with-recursive)
12. [Bootstrap, Backfill & Snapshot Reconciliation](#12-bootstrap-backfill--snapshot-reconciliation)
13. [Implementation Plan for IVM](#13-implementation-plan-for-ivm)
14. [Testing Strategy](#14-testing-strategy)
15. [Open Questions](#15-open-questions)

---

## 1. What the Two Reference Systems Actually Do

### DBSP

```
SQL (Calcite parse + optimize)
   │
   ▼
RelNode tree
   │
   ▼
CalciteToDBSPCompiler → DBSPCircuit (Rust circuit IR)
   │
   ▼
CircuitOptimizer + incrementalization (the I/D operator transform)
   │
   ▼
ToRustVisitor → emits Rust source code that builds the circuit at runtime
   │
   ▼
Compiled Rust binary uses the `dbsp` crate to:
   - assemble a graph of dynamically-typed operators (algebra/, operator/, trace/)
   - feed input as Z-set batches via InputHandle
   - schedule operator activations per epoch (circuit/schedule/)
   - store arrangements (Trace = indexed Z-set with history) in Spine
   - persist arrangements via the storage/ layer (file-backed batches)
   - output deltas via OutputHandle
```

Key building blocks observed in source:
- **Z-set / IndexedZSet** (`algebra/`, `typed_batch/`): collections with
  integer weights, the universal data type between operators.
- **Trace** (`trace/spine_async/`): a Spine — an LSM-like merging structure of
  Z-set batches that accumulates history of a stream. The arrangement an
  operator queries against during join.
- **Circuit** (`circuit/circuit_builder.rs`, `Runtime`): a fixed graph
  constructed once at startup. Workers (a `Runtime` is a thread pool) each run
  a copy of the circuit on their slice of the input.
- **Operators** (`operator/`): one file per primitive — `join.rs`,
  `aggregate.rs`, `distinct.rs`, `recursive.rs`, `time_series/`, etc. Each
  exposes a typed `Stream<C, ZSet<K,V>>` API.
- **Recursive circuits** (`operator/recursive.rs`, `IterativeCircuit`):
  nested-time-scope feedback loops with `Z1` (delay) and `Distinct`.
- **Checkpointer** (`circuit/checkpointer.rs`): periodic durable snapshot of
  every Spine + circuit state to a `dbsp_storage` path.

Strengths: provably correct, supports the entire DBSP calculus, very fast
per-core, mature SQL coverage. Weaknesses: tight compute-storage coupling,
hard to scale storage independently, single-language (Rust output).

### pg_trickle

```
CREATE STREAM TABLE (sql, schedule)
   │
   ▼
PostgreSQL raw_parser + parse_analyze → parse tree
   │
   ▼
src/dvm/parser → OpTree (sealed enum, ~20 variants)
   │
   ▼
src/dvm/diff.rs DiffContext walks OpTree
   │
   ▼
Per-operator diff_X(...) functions emit CTEs
   │
   ▼
Final delta SQL: WITH cte1 AS (...), cte2 AS (...), ... SELECT * FROM final
   │
   ▼
MERGE INTO stream_table USING (delta_sql) ON pk_match
     WHEN MATCHED action='D' THEN DELETE
     WHEN MATCHED action='I' THEN UPDATE ...
     WHEN NOT MATCHED action='I' THEN INSERT ...
```

Key observations from source:
- **`OpTree`** (`src/dvm/parser/types.rs:880`): 20+ variants — Scan, Project,
  Filter, InnerJoin, LeftJoin, FullJoin, Aggregate, Distinct, UnionAll,
  Intersect, Except, Subquery, CteScan, RecursiveCte, RecursiveSelfRef,
  Window, LateralFunction, LateralSubquery, SemiJoin, AntiJoin.
- **Per-operator diff functions** (`src/dvm/operators/*.rs`): each takes a
  `DiffContext` + `OpTree` node, recursively differentiates children, emits
  CTEs implementing the DBSP rule for that node type. E.g. `join.rs`
  implements the bilinear expansion `ΔI(Q ⋈ R) = (ΔQ_I ⋈ R₁) + (ΔQ_D ⋈ R₀) + (Q₀ ⋈ ΔR)`
  with documented EC-01 correctness fix.
- **Delta source abstraction** (`DeltaSource`): `ChangeBuffer` (CDC tables
  filtered by LSN range) or `TransitionTable` (statement-trigger ephemeral
  named relations used by pg_trickle's synchronous refresh path).
- **DAG** (`src/dag.rs`): tracks dependencies between stream tables, supports
  nested ST-on-ST, includes scheduling (EDF, demand-driven cadence
  propagation) and diamond consistency groups.
- **Caching** (`src/dvm/mod.rs`): delta SQL templates are cached per stream
  table with placeholders (`__PGS_PREV_LSN_{oid}__`) for LSN values, so the
  expensive SQL generation runs once per query change.
- **MERGE for delta application**: the final step uses PostgreSQL's `MERGE`
  to apply (insert / update / delete) actions atomically.

Strengths: simple, debuggable (you can `EXPLAIN` the generated SQL),
zero-infrastructure (lives inside Postgres), reuses Postgres planner & types.
Weaknesses: bounded by what one Postgres backend can do, no cross-shard
scaling, recursion has stack-depth caveats.

---

## 2. Side-By-Side Comparison

| Dimension | DBSP | pg_trickle | RockStream |
|---|---|---|---|
| **Query input** | SQL (Calcite) | SQL (Postgres parser) | SQL (DataFusion) |
| **Plan IR** | Calcite RelNode → DBSPCircuit | `OpTree` (~20 variants) | `PlanIR` (DataFusion LogicalPlan + IVM annotations) |
| **Incrementalization style** | Whole-circuit transform via I/D operator insertion | Per-operator SQL-generation rules walking OpTree | DBSP-native operators over PlanIR, with pg_trickle-derived metadata and regression tests |
| **Runtime data type** | Typed Z-set batches (`IndexedZSet<K,V>`) | SQL rows + `__pgt_action` column ('I'/'D') | Arrow `RecordBatch` with `_weight: i64` column |
| **Operator execution** | Long-lived Rust functions in a fixed circuit | Generated SQL re-run per refresh by Postgres planner | Long-lived Rust operator instances, vectorized expression eval via DataFusion physical expressions |
| **State (arrangements)** | Spine of file-backed batches on local NVMe | Stream table itself = state; auxiliary `__pgt_count` columns | SlateDB shard, indexed Z-set encoded as KV (see §9) |
| **Scheduling** | Fixed graph + work scheduler | EDF + demand-driven cadence + diamond groups | Frontier-driven dataflow scheduler (§10) |
| **Recursion** | `IterativeCircuit` with nested timestamps, `Z1`, `Distinct` | Generated WITH RECURSIVE in delta SQL | Same nested-timestamp approach as DBSP |
| **Bootstrap** | Replay inputs through circuit | Initial full materialize then differential | "Snapshot mode" diff with no prev state, then switch to delta (§12) |
| **Checkpointing** | Whole-circuit checkpoint via storage layer | Postgres WAL gives durability for free | Per-shard SlateDB checkpoint + cluster barrier (DESIGN.md §11) |
| **Distribution** | `Runtime` thread pool, single-process | Single Postgres backend | Mesh of shards across workers (DESIGN.md §3) |

---

## 3. What RockStream Inherits From Each

### From pg_trickle — the *oracle and edge-case catalogue*

- **Per-operator diff rules as executable documentation**. The pg_trickle
  source contains battle-tested differentiation rules for every relational
  operator, with documented edge cases (EC-01 fix in join, Q21 SemiJoin
  regression, Q07 double-counting correction, FULL JOIN NULL handling in
  aggregates, recursive DRed fallback, etc.). These rules become regression
  tests and planner metadata requirements. We do **not** translate its SQL CTEs
  directly into native code; we implement the corresponding DBSP operators
  and validate them against pg_trickle and batch SQL.
- **OpTree-style plan IR with one variant per operator** — much easier to
  pattern-match against than a generic optimizer's plan node.
- **Delta-source abstraction** (`DeltaSource::ChangeBuffer` vs
  `TransitionTable`). RockStream generalizes this to source deltas, view-output
  deltas, and arrangement snapshots. The `TransitionTable` variant is reference
  material for understanding pg_trickle's correctness model; RockStream does not
  expose synchronous transition-table semantics because there is no
  write-transaction hook in the distributed architecture.
- **Caching the *plan* with parameterized inputs**. pg_trickle's
  `__PGS_PREV_LSN_{oid}__` placeholders show how to compile-once-execute-many.
  We do the analogous thing with our physical plan: compile a query into a
  fixed circuit; each epoch only changes the input batches.
- **DAG with cascade scheduling**. pg_trickle's `dag.rs` cleanly models stream
  tables depending on stream tables with diamond consistency via explicit
  `DiamondConsistency::Atomic` groups. RockStream achieves the same guarantee
  structurally: every multi-input operator's frontier meet enforces it
  automatically, with no explicit group concept or user API.

### From DBSP — the *runtime*

- **Long-lived circuit of typed operators**. Compiling SQL to a fresh
  generated-Rust binary is too operationally heavy for a
  cloud service; but the *runtime model* — a fixed dataflow graph of
  long-lived operator instances that consume typed batches — is exactly what
  we want.
- **Arrangement / Spine concept**. An *arrangement* is a current indexed
  Z-set that an operator can query at any time. Joins query their other
  side's arrangement; aggregations query their own previous output. Our
  arrangements live in SlateDB (see §9) instead of the DBSP Spine, but the
  abstraction is identical.
- **Nested timestamp / recursive circuit pattern** (the DBSP
  `IterativeCircuit` + `Z1`). Adopted wholesale for `WITH RECURSIVE` — it's
  the cleanest known solution.
- **Differential-style frontier semantics** for progress tracking.
- **Batched processing** rather than per-row. Operators always work on
  Arrow `RecordBatch`es, never single rows. Vectorized expression eval comes
  for free from DataFusion.

### What we deliberately reject

- **The generated-Rust artifact as the primary deployment model**.
  The DBSP compiler can serialize circuits as Rust and the DBSP runtime is
  dynamic enough to construct circuits programmatically. RockStream chooses a
  fixed interpreted `OpKind` graph for operational simplicity; code generation
  may be added later for hot paths, but is not required for v1.
- **pg_trickle's SQL-string-as-runtime model**. Re-parsing and re-planning a
  giant `WITH` chain on every refresh would be silly when we control the
  whole stack. We *generate* the equivalent operator graph at compile time
  and execute it natively.
- **Single-machine state assumptions** (both systems). RockStream's state
  is sharded across SlateDB instances; arrangements are partitioned by the
  operator's input partition key, and operators access only their own shard
  except via explicit Exchange operators.

---

## 4. The RockStream IVM Architecture

```
                          SQL view definition
                                  │
                                  ▼
                  ┌───────────── Compiler ─────────────┐
                  │  1. Parse  (sqlparser-rs)          │
                  │  2. Bind   (catalog lookup)        │
                  │  3. Plan   (DataFusion LogicalPlan)│
                  │  4. Optimize (DF rules + IVM rules)│
                  │  5. Lower → PlanIR                 │
                  │  6. Differentiate (§6)             │
                  │  7. Distribute (insert Exchange)   │
                  │  8. Assign parallelism             │
                  │  9. Serialize to control plane     │
                  └─────────────────┬──────────────────┘
                                    │ PhysicalPlan
                                    ▼
                  ┌───────────── Deployer ─────────────┐
                  │  • Allocate op_ids                  │
                  │  • Place operator instances on shards│
                  │  • Create empty arrangements on each │
                  │    target SlateDB shard              │
                  │  • Bootstrap from base tables (§12)  │
                  └─────────────────┬──────────────────┘
                                    │
                                    ▼
              ┌─────────── Per-Worker Runtime ──────────┐
              │  CircuitExecutor                        │
              │   • One long-lived task per operator    │
              │     instance assigned to this worker    │
              │   • Each task owns an Arc<ShardDb>      │
              │   • Each task consumes input batches    │
              │     and produces output batches         │
              │   • Shard-level epoch coordinator       │
              │     coalesces operator mutations into   │
              │     atomic WriteBatch commits           │
              │  Exchange dispatcher (gRPC + S3)         │
              │  Frontier reporter → control plane       │
              └─────────────────────────────────────────┘
```

The IVM "engine" is the **compiler + circuit executor**. Everything else in
the system (control plane, exchange subsystem, gateway, connectors) is
orchestration around it.

---

## 5. Intermediate Representation: PlanIR

```rust
/// A node in the incremental physical plan.
/// Mirrors pg_trickle's OpTree, extended for distribution and arrangements.
pub enum PlanNode {
    // ── Sources ────────────────────────────────────────────────────
    /// Base table; reads deltas from the input connector.
    Source { table_id: TableId, schema: SchemaRef, partition_key: PartitionKey },

    /// Reference to another view's output (view-on-view).
    /// At runtime, this taps the upstream view's output stream via SlateDB CDC.
    ViewRef { view_id: ViewId, partition_key: PartitionKey },

    /// Reference to an arrangement maintained by another operator instance.
    /// Used by join children that need the "current" state of the other side.
    ArrangementRef { arrangement_id: ArrangementId, key_schema: SchemaRef },

    // ── Stateless ──────────────────────────────────────────────────
    Filter   { predicate: PhysicalExpr,  child: Box<PlanNode> },
    Project  { exprs:    Vec<NamedExpr>, child: Box<PlanNode> },
    Map      { mapper:   PhysicalMap,    child: Box<PlanNode> },

    // ── Stateful (have arrangements) ───────────────────────────────
    Aggregate {
        group_by:    Vec<PhysicalExpr>,
        aggregates:  Vec<AggSpec>,         // SUM/COUNT/AVG/MIN/MAX/...
        invertible:  Vec<bool>,            // per-aggregate: algebraic invert?
        child:       Box<PlanNode>,
        arrangement: ArrangementId,        // group_key → AggState
    },

    Distinct {
        keys:        Vec<PhysicalExpr>,
        child:       Box<PlanNode>,
        arrangement: ArrangementId,        // row_hash → i64 weight
    },

    InnerJoin {
        left_key:    Vec<PhysicalExpr>,
        right_key:   Vec<PhysicalExpr>,
        residual:    Option<PhysicalExpr>, // post-equi-join filter
        left:        Box<PlanNode>,
        right:       Box<PlanNode>,
        left_arrangement:  ArrangementId,  // join_key → left rows
        right_arrangement: ArrangementId,  // join_key → right rows
        semantics: JoinSemantics,          // EC-01/Q07/Q21 metadata
    },
    OuterJoin { side: JoinSide, /* ... */ },
    SemiJoin  { /* ... */ },
    AntiJoin  { /* ... */ },

    Window {
        partition_by: Vec<PhysicalExpr>,
        order_by:     Vec<PhysicalSort>,
        functions:    Vec<WindowFn>,
        frame:        WindowFrame,
        child:        Box<PlanNode>,
        arrangement:  ArrangementId,
    },

    TopK {
        partition_by: Vec<PhysicalExpr>,
        order_by:     Vec<PhysicalSort>,
        k:            usize,
        child:        Box<PlanNode>,
        arrangement:  ArrangementId,
    },

    TimeWindow {
        kind:        TimeWindowKind,       // Tumbling | Hopping | Session
        size:        Duration,
        slide:       Option<Duration>,
        event_time:  PhysicalExpr,
        child:       Box<PlanNode>,
        arrangement: ArrangementId,
    },

    // ── Set ops ────────────────────────────────────────────────────
    UnionAll { children:  Vec<PlanNode> },
    Union    { children:  Vec<PlanNode>, dedupe_arrangement: ArrangementId },
    Intersect{ left: Box<PlanNode>, right: Box<PlanNode>, all: bool, arr: ArrangementId },
    Except   { left: Box<PlanNode>, right: Box<PlanNode>, all: bool, arr: ArrangementId },

    // ── Recursion ──────────────────────────────────────────────────
    Recursive {
        id:        RecursionId,
        base:      Box<PlanNode>,
        step:      Box<PlanNode>,          // contains RecursiveSelfRef nodes
        result_arrangement: ArrangementId,
      strategy:  RecursionStrategy,      // SemiNaive | DRed | Recompute
    },
    RecursiveSelfRef { id: RecursionId, schema: SchemaRef },

    // ── Distribution ───────────────────────────────────────────────
    Exchange {
        partition_key: PartitionKey,        // target partitioning
        target_width:  usize,               // number of downstream instances
        kind:          ExchangeKind,        // Hash | Broadcast | Single | Range
        child:         Box<PlanNode>,
    },

    // ── Sinks ──────────────────────────────────────────────────────
    ViewSink { view_id: ViewId, pk: Vec<usize>, child: Box<PlanNode> },
}
```

**Key types**:

- `ArrangementId`: 16-byte ULID. Every operator that maintains state declares
  one or more arrangements. The id becomes a prefix in SlateDB keys.
- `PartitionKey`: a list of column indexes the data is partitioned by, plus a
  `PartitionFn` (hash, range). Two operators with the same `PartitionKey` can
  be co-located on the same shard.
- Every `PlanNode` carries a `partition_key: PartitionKey` (computed from
  the operator's semantics: join keys, group-by keys, …). The distribution
  pass inserts `Exchange` whenever a child's `partition_key` differs from
  the parent's required key.
- Stateful nodes carry **semantic annotations** computed once by the compiler:
  monotonicity, whether they are inside SemiJoin/AntiJoin context, join depth,
  whether an update changes key columns or only aggregate arguments, and whether
  a recursive term is non-monotone. These annotations prevent every worker from
  re-deriving subtle pg_trickle-style context locally.

---

## 6. The Differentiation Pass

The differentiation pass is the heart of IVM. It is **informed by
pg_trickle's `DiffContext`** but emits native DBSP operator descriptors, not a
SQL CTE plan. A debug/explain mode may emit a SQL sketch for comparison, but
that SQL is not the runtime implementation.

```rust
pub struct DiffCtx<'a> {
    plan: &'a PlanNode,
    arrangements: ArrangementRegistry,
    inside_semijoin: bool,
  join_context: JoinContext,
  key_change_tracker: KeyChangeTracker,
    in_recursion: Option<RecursionId>,
  // ... (keep only the pg_trickle context that affects native semantics)
}

pub fn differentiate(plan: PlanNode) -> Result<PhysicalPlan> {
    let mut ctx = DiffCtx::new(&plan);
    let root = ctx.diff(&plan)?;
    Ok(PhysicalPlan { root, arrangements: ctx.arrangements })
}

impl DiffCtx<'_> {
    fn diff(&mut self, node: &PlanNode) -> Result<OpNode> {
        match node {
            PlanNode::Source       { .. } => self.diff_source(node),
            PlanNode::Filter       { .. } => self.diff_filter(node),
            PlanNode::Project      { .. } => self.diff_project(node),
            PlanNode::Aggregate    { .. } => self.diff_aggregate(node),
            PlanNode::InnerJoin    { .. } => self.diff_inner_join(node),
            PlanNode::LeftJoin     { .. } => self.diff_outer_join(node, JoinSide::Left),
            PlanNode::Distinct     { .. } => self.diff_distinct(node),
            PlanNode::UnionAll     { .. } => self.diff_union_all(node),
            PlanNode::Window       { .. } => self.diff_window(node),
            PlanNode::TopK         { .. } => self.diff_topk(node),
            PlanNode::TimeWindow   { .. } => self.diff_time_window(node),
            PlanNode::Recursive    { .. } => self.diff_recursive(node),
            PlanNode::Exchange     { .. } => self.diff_exchange(node),
            // ...
        }
    }
}
```

`OpNode` is the *runtime* operator graph node:

```rust
pub struct OpNode {
    pub op_id:     OpId,
    pub kind:      OpKind,                 // physical operator implementation
    pub inputs:    Vec<OpId>,
    pub output_schema: SchemaRef,
    pub partition_key: PartitionKey,
    pub arrangements:  Vec<ArrangementId>, // state this op owns
    pub reads_arrangements: Vec<ArrangementId>, // state this op reads
}
```

The runtime is just an interpreter for `OpKind`. Compilation is one-time; the
op graph is durable in the control-plane SlateDB and is what each worker
loads at startup.

`OpKind` is static and closed over the native runtime implementations. It may
store DataFusion physical expressions and planner annotations, but it must not
store SQL strings that are re-parsed per epoch.

---

## 7. Per-Operator Differentiation Rules

We implement DBSP-native operators and validate them against pg_trickle's SQL
rules. Each rule below references the source file where pg_trickle implements
the equivalent behavior for traceability and test-case extraction.

### 7.1 Scan (`diff_scan`, [`pg-trickle1/src/dvm/operators/scan.rs`](../pg-trickle1/src/dvm/operators/scan.rs))

**DBSP**: `Δ(Scan(T)) = ΔT` (just the input delta).

**RockStream**: a `Source` node receives a `RecordBatch` per epoch containing
`(rows, weights)` from a connector. UPDATEs are split into `(old, -1) ⊎ (new, +1)`
by the connector layer. The Source operator just forwards.

### 7.2 Filter / Project / Map (linear operators)

**DBSP**: linear operators commute with deltas — `Δ(f(C)) = f(ΔC)`.

**RockStream**: simply apply the predicate / projection batch-wise via
DataFusion's physical expression evaluator. No arrangement needed.

### 7.3 Inner Join (`diff_inner_join`, [`join.rs`](../pg-trickle1/src/dvm/operators/join.rs))

**DBSP** (bilinear):
```
Δ(L ⋈ R) = ΔL ⋈ R₁  +  L₀ ⋈ ΔR  +  ΔL ⋈ ΔR
         = ΔL_I ⋈ R₁  +  ΔL_D ⋈ R₀  +  L₀ ⋈ ΔR
```

pg_trickle documents several correctness fixes (EC-01, Q07 double-counting,
Q21 SemiJoin regression). RockStream must preserve the semantics of these
fixes, but the native implementation uses versioned/staged arrangements rather
than SQL `EXCEPT ALL` subqueries.

**RockStream runtime**:
- Two arrangements: `left_arr[op_id][join_key] → left rows`, symmetric for
  right.
- On a left delta batch:
  1. Look up matching right rows in `right_arr` (SlateDB prefix scan).
  2. Emit join results with appropriate weight.
  3. Insert / delete in `left_arr` (merge-style via SlateDB
     `MergeOperator` on the weight).
- The `R₀` / `L₀` "pre-change snapshot" subtlety: arrangements are
  updated *at the end* of epoch commit. During processing of epoch *e*,
  the arrangement reflects state at end of epoch *e-1* = pre-change.
  Symmetric handling of left/right deltas in the same epoch is done by
  staging both sides' updates in memory before either commit; the operator
  computes `ΔL ⋈ ΔR` once at the end.
- `JoinSemantics` records whether the join is inside SemiJoin/AntiJoin context,
  whether either child is itself a join, and whether correction output must be
  produced by this node or an ancestor. This is the native replacement for
  pg_trickle's `inside_semijoin`, `is_join_child`, and Part-3 generation logic.

**Planner optimizations adopted from pg_trickle** (each is implemented as
planner metadata on the native join, not as SQL CTE rewriting):

- **SemiJoin `R_old` materialization** (Q21 fix): when a SemiJoin's right side
  is itself a join, mark the right-side arrangement as "materialize across
  iterations of this epoch" so it is computed once per epoch instead of once
  per EXISTS check. Driven by `JoinSemantics::r_old_materialize`.
- **DI-6 equi-join key filter pushdown**: when computing the right-side
  pre-change view for a SemiJoin or AntiJoin, restrict it to join keys that
  appear in either delta side. Implemented as a key-set filter on the
  arrangement scan, not as a SQL `WHERE` rewrite.
- **Merge-safe dedup**: when the operator chain above a Scan-Filter-Project
  contains no join/aggregate/distinct, the planner sets `merge_safe_dedup =
  true` so the sink can skip the deduplication sort.

### 7.4 Outer Joins (LEFT / RIGHT / FULL)

pg_trickle has dedicated implementations
([`outer_join.rs`](../pg-trickle1/src/dvm/operators/outer_join.rs),
[`full_join.rs`](../pg-trickle1/src/dvm/operators/full_join.rs)) that handle
unmatched-side NULL padding and the matched→unmatched transitions.

**RockStream**: same logic; one extra match-count arrangement per side tracks
whether each row is currently unmatched:

```
left_arr[join_key, row_id]  -> row bytes
right_arr[join_key, row_id] -> row bytes
match_count[side, row_id]  -> i64
```

LEFT JOIN rules:
- Left insert: scan right. If no matches, emit `(left,NULL,+1)` and store
  `match_count=0`; otherwise emit `(left,right,+1)` for each right row and
  store the count.
- Right insert: scan left. For every left row whose previous count was 0,
  retract `(left,NULL,-1)` before emitting `(left,right,+1)`, then increment
  the count.
- Right delete: emit `(left,right,-1)`, decrement the left count, and if the
  count reaches 0 emit `(left,NULL,+1)`.

RIGHT JOIN is symmetric. FULL JOIN applies the same accounting to both sides.
This is the native operator state behind the EC-01 and FULL JOIN NULL-padding
oracle cases.

### 7.5 Semi-Join / Anti-Join (`semi_join.rs`, `anti_join.rs`)

Two-part delta:
- Left-side changes filtered by existence/non-existence against the
  *current* right arrangement.
- Right-side changes trigger re-evaluation of affected left rows (we look up
  left rows whose key matches the changed right keys).

### 7.6 Aggregate with GROUP BY (`diff_aggregate`, [`aggregate.rs`](../pg-trickle1/src/dvm/operators/aggregate.rs))

Two categories:

**Algebraic / invertible** (SUM, COUNT, AVG = SUM/COUNT):
- Maintain `(sum, count)` per group as `MergeOperator` state in SlateDB.
- For each input delta, compute `(Δsum, Δcount)` and `merge` into the group's
  current state.
- Emit output delta:
  - group new (`count` was 0, now > 0) → `(group_value, +1)`.
  - group vanished (`count` was > 0, now ≤ 0) → `(old_value, -1)`.
  - value changed → `(old_value, -1) ⊎ (new_value, +1)`.
- For SUM, the cache of last-emitted value lives in a sibling key
  (`op_index/`), so we know what to retract.
- The compiler propagates key-change and value-change metadata from scans
  through projections and joins. This is required to distinguish value-only
  updates from group-key or join-key updates, mirroring pg_trickle's
  `has_key_changed` signal.

**Non-invertible** (MIN, MAX, MEDIAN, PERCENTILE):
- Maintain a sorted indexed multiset per group:
  `op_state/0xMM op_id group_key value row_id → weight`.
- On insert: scan to update the extremum.
- On delete: if the deleted value was the extremum, scan the sorted prefix to
  find the new extremum (single SlateDB `scan().next()` call).

**FULL JOIN NULL caveat** (pg_trickle's note): SUM over a FULL JOIN's
nullable column may need a *rescan* when transitions cross matched↔unmatched.
The compiler detects this via `child_has_full_join` and inserts a rescan
fallback (read the current arrangement and re-aggregate that group).

### 7.7 Distinct / Union (set semantics) (`distinct.rs`)

- Arrangement: `row_hash → i64 weight`.
- Merge each incoming delta's weight via `MergeOperator`.
- Emit output delta when weight transitions across zero:
  - 0 → positive: emit `(row, +1)`.
  - positive → 0: emit `(row, -1)`.
- The operator writes an explicit tombstone for zero-crossing entries when the
  row must disappear immediately. Compaction filters are only used later to
  remove obsolete merge operands after the frontier proves no snapshot can need
  them.

### 7.8 Intersect / Except (`intersect.rs`, `except.rs`)

Combine two distinct-style arrangements per side; emit deltas at min/diff of
weights.

### 7.9 Window Functions (`window.rs`)

pg_trickle's strategy: **partition-based recomputation** — when any row in a
partition changes, recompute the entire partition. We adopt the same
strategy but vectorized:

- Arrangement: `partition_key → all rows in partition, sorted by order_by`.
- For each affected partition (computed from incoming delta's partition
  keys), read all rows, evaluate the window function batch-wise, diff against
  previously-emitted output (cached as part of the arrangement).

For sliding-window aggregates, the segment-tree variant
([DESIGN.md §6.7](DESIGN.md#67-window-functions-row_number-rank-lag-lead-sliding-aggregates))
is an optimization added later.

### 7.10 Time Windows

Same as DESIGN.md §6.9: state keyed by `window_id`, with event-time TTL and a
frontier-aware compaction filter. TTL alone is not a correctness rule; the
operator also checks input/output frontiers before state becomes reclaimable.

### 7.11 Top-K (`detect_topk_pattern` in pg_trickle)

Sorted secondary index keyed by `value_desc`. Maintain only K+ε entries; on
delete of one of the top K, scan one entry past K to refill. Emit a delta
that swaps the displaced entry for the new one.

### 7.12 Lateral Function / Lateral Subquery

pg_trickle uses row-scoped recomputation
([`lateral_function.rs`](../pg-trickle1/src/dvm/operators/lateral_function.rs),
[`lateral_subquery.rs`](../pg-trickle1/src/dvm/operators/lateral_subquery.rs)):
for each changed outer row, delete the previous expansion and re-expand
against the new outer row.

**RockStream**: implement Lateral as a stateless operator that, per incoming
outer-delta row, evaluates the lateral expression (a DataFusion physical
plan) and emits the expanded rows with the appropriate weight.

---

## 8. The Circuit Runtime

A **Circuit** is the deployed physical plan. Each worker hosts a portion of
the circuit — the operator instances whose `op_id` is assigned to that
worker's shards.

### 8.1 Operator Trait

```rust
#[async_trait]
pub trait Operator: Send + Sync {
    /// Stable identifier (16-byte ULID).
    fn id(&self) -> OpId;

    /// Schema of this operator's output batches.
    fn output_schema(&self) -> &SchemaRef;

    /// Process input deltas for one epoch.
    ///
    /// Inputs are received via the OpInputs handle (one queue per input port).
    /// Output deltas + state mutations are accumulated in EpochOutput.
    /// At end of epoch, the shard-level epoch coordinator commits outputs from
    /// all ready operators on the shard in one or more coalesced WriteBatches.
    async fn process_epoch(
        &mut self,
        epoch:  Epoch,
        inputs: &mut OpInputs,
        ctx:    &mut OpCtx,
    ) -> Result<EpochOutput>;

    /// Called once at startup. Operator opens its arrangements
    /// (handles to its SlateDB keyspace).
    async fn initialize(&mut self, shard: Arc<ShardDb>) -> Result<()>;

    /// Notify the operator that an input frontier has advanced. Operators
    /// may use this to flush state (close windows, declare recursion convergence,
    /// release retained tuples).
    async fn on_input_frontier(
        &mut self,
        input: InputPort,
        frontier: Frontier,
        ctx: &mut OpCtx,
    ) -> Result<EpochOutput>;
}

pub struct EpochOutput {
    /// Output deltas to send to downstream operators.
    pub deltas: Vec<(OutputPort, RecordBatch)>,
    /// State mutations (encoded as SlateDB writes).
    pub state_writes: Vec<StateOp>,
    /// New output frontier advertised by this operator.
    pub frontier: Option<Frontier>,
}
```

### 8.2 Each Operator Is a Long-Lived Tokio Task

```
struct OperatorTask {
    op: Box<dyn Operator>,
    inputs:  HashMap<InputPort, mpsc::Receiver<EpochBatch>>,
    outputs: HashMap<OutputPort, BroadcastSender<EpochBatch>>,
    shard:   Arc<ShardDb>,
    commit_tx: mpsc::Sender<ShardCommitFragment>,
}

async fn run(self) {
    loop {
        let epoch = self.next_epoch().await;
        let mut inputs = OpInputs::collect(&mut self.inputs, epoch).await;
        let output = self.op.process_epoch(epoch, &mut inputs, &mut self.ctx).await?;

        // Send mutations to the shard-level epoch coordinator. The coordinator
        // coalesces all ready operators' fragments into WriteBatch commits.
        self.commit_tx.send(ShardCommitFragment::new(self.op.id(), epoch, output)).await?;

        // Distribute outputs after the commit coordinator confirms durability.
        for (port, batch) in output.deltas {
            self.outputs[&port].broadcast(EpochBatch { epoch, batch })?;
        }
    }
}
```

This is **the DBSP circuit-runtime model** adapted to async tasks and SlateDB,
with shard-level group commit added to avoid one object-store/WAL durability
event per operator per epoch.

### 8.3 Code Generation vs. Interpretation

We do **not** generate Rust source code per query (the DBSP generated-Rust approach). Every
operator is a polymorphic Rust struct parameterized by:
- DataFusion `PhysicalExpr` for filters / projections / aggregates / join
  residuals.
- Arrow `RecordBatch` as the universal data type.

The inner loop is fast because DataFusion's expression executor is vectorized
and benefits from LLVM autovectorization. The cost of interpretation is
amortized over thousands of rows per batch.

This is the same approach taken by other distributed streaming SQL systems and is operationally simpler than
per-query Rust compilation.

---

## 9. Arrangements: State on SlateDB

An **arrangement** is a sorted-by-key indexed Z-set whose key is the lookup
column(s) for the operator that owns it.

### 9.1 Encoding

```
SlateDB key:
  arr_prefix(0x01) | arrangement_kind(1) | arrangement_id(16)
  | key_bytes(var) | tiebreak_id(16)
SlateDB value:
  Arrow IPC row(s) packed as bytes  +  weight: i64
```

The `tiebreak_id` is a 16-byte stable row identifier that makes keys unique
even when multiple rows share the same arrangement key (e.g., the join key).
It is never generated randomly during replay. Source connectors derive it from
durable input identity: `(source_id, partition, offset, row_ordinal)` for log
sources, table primary key plus source LSN for CDC, or `(snapshot_id,
file_path, row_group, row_ordinal)` for keyless snapshots. This keeps replay
idempotent: the same source row rewrites the same arrangement entry.

### 9.2 Standard Encodings (Mirrors [DESIGN.md §6](DESIGN.md#6-operator-catalog--state-encodings))

| Arrangement kind | Use | Key | Value |
|---|---|---|---|
| `0xAG` | SUM/COUNT/AVG group state | `group_key` | `(sum: i128, count: i64, …)` |
| `0xMM` | MIN/MAX sorted multiset | `group_key + value_bytes + row_id` | `weight: i64` |
| `0xJL` / `0xJR` | Join arrangement (left / right side) | `join_key + row_id` | `row Arrow bytes` |
| `0xJM` | Outer-join match count | `side + row_id` | `i64 match_count` |
| `0xDS` | Distinct/union | `row_hash + row_id` | `weight: i64` |
| `0xWN` | Window function | `partition_key + order_key + row_id` | `row Arrow bytes` |
| `0xTW` | Time-window state | `window_id + key` | `partial_state` |
| `0xTK` | Top-K (sorted desc) | `partition_key + value_desc + row_id` | `row Arrow bytes` |
| `0xRC` | Recursive variable | `row_hash + iteration` | `weight: i64` |

### 9.3 Why MergeOperator Matters Here

For algebraic aggregates and distinct/union, `MergeOperator` lets us issue
`db.merge(key, delta)` without a read-modify-write cycle. The merge is applied
lazily, so every runtime read of a merge-backed arrangement goes through
`ShardDb::get_merged()` / `ShardDb::scan_merged()` rather than raw SlateDB
`get()` / `scan()`. That wrapper resolves the base value plus all visible merge
operands at the epoch snapshot being read. If a storage profile cannot provide
correct read-path merge resolution, that arrangement kind falls back to batched
read-modify-write and `EXPLAIN INCREMENTAL ESTIMATE` reports the throughput
penalty.

Concretely, our `AggregateMergeOp` decodes both operands as `(sum, count)`
tuples and emits their sum. For distinct/union, the merge is `i64` addition,
but a zero-crossing must still be materialized as a visible delete/tombstone if
readers need the row to disappear immediately.

Merge operators are only used for associative state. MIN/MAX, Top-K, window
rankings, and recursive DRed state are maintained as explicit sorted
arrangements because their retractions require reading sibling entries.

### 9.4 Arrangement Lookups Are Always SlateDB `scan()` or `get()`

- Point lookup: `db.get(arr_key(id, k))`.
- Prefix lookup (e.g., "all left rows for join_key K"):
  `db.scan(arr_prefix(id, k)..)`.
- Range scan: same pattern.
- Cross-shard lookup is forbidden in the hot path; if needed, the operator
  must be preceded by an Exchange so the data is co-located.
- Query gateways and `DbReader`-based join lookups from other workers benefit
  from the per-worker arrangement segment cache (DESIGN.md §5.4) which
  exploits segment immutability between checkpoints.

### 9.5 Segments, Retention, and Cleanup

Each shard should be created with a SlateDB segment extractor keyed by namespace
and arrangement prefix. Segmenting keeps large join arrangements, shuffle
buffers, and view outputs from sharing one undifferentiated compaction policy.

RockStream does not rely on SlateDB range deletion. Cleanup is expressed as:

- explicit point deletes/tombstones for correctness-sensitive zero-crossings;
- bounded prefix scan + batched deletes for shuffle and migration cleanup;
- frontier-aware compaction filters for old merge operands or expired windows;
- checkpoint/clone/projection for large rebalancing jobs.

Compaction filters must honor `retention_min_seq`/frontier constraints and must
avoid `Drop` decisions that could resurrect older versions of the same key.

---

## 10. Scheduling & The Epoch Loop

The runtime's scheduler is **data-driven**: an operator runs when (a) it has
input batches for the current epoch, and (b) all its required input
frontiers have advanced.

### 10.1 Per-Worker Scheduler

```
loop {
    let ready = self.operators
        .iter_mut()
        .filter(|op| op.has_input_for(self.current_epoch))
        .collect::<Vec<_>>();
    
    // Process each ready operator concurrently
    let futures = ready.into_iter().map(|op| op.process_epoch(self.current_epoch));
    let outputs = join_all(futures).await;
    
    // Coalesce ready operators' outputs by shard and commit as grouped batches.
    self.shard_commit_coordinator.commit(self.current_epoch, outputs).await?;
    
    self.current_epoch += 1;
}
```

The scheduler is intentionally **async and ownership-free**. The
`DynamicScheduler` in single-process DBSP (see `crates/dbsp/src/circuit/schedule.rs`) tracks
stream ownership and rejects topologies with `OwnershipConflict` so it can
statically choose owned-vs-borrowed reads. That model is right for a single
worker process; in a distributed shard mesh, the same logical stream is
consumed from many shards and many query gateways. RockStream therefore
does not enforce single-ownership of streams. Operators consume from
arrangements via `DbReader` (which is multi-reader by construction) and
receive data via the Exchange subsystem, which makes "ownership" a
non-question.

### 10.2 Stream-Level Cadence (Inspired by pg_trickle's DAG)

pg_trickle exposes per-stream-table schedules (`1s`, `100ms`, `IMMEDIATE`,
`CALCULATED`). RockStream keeps the cadence concept but uses three modes
(IMMEDIATE is not supported — see below):

- **DEFERRED LOW-LATENCY** (default): source connectors close frequent epochs
  (for example 10-100 ms) and the runtime drains them through the circuit.
  This preserves scale without distributed source-transaction coupling.
- **PERIODIC(d)**: produce one epoch per `d` ms; batches incoming data into
  that window for higher throughput.
- **CALCULATED**: a downstream's cadence is the min of its consumer-facing
  views' cadences. This preserves pg_trickle's demand-driven scheduling idea
  while using RockStream frontiers for distributed consistency.

IMMEDIATE mode is not supported. pg_trickle's IMMEDIATE relies on
statement-level triggers that fire inside the write transaction — a mechanism
that does not exist in RockStream's distributed architecture (no
write-transaction hook, no trigger layer, no global write-sequence number).
A tight freshness SLO (50–200 ms) and frontier-based diamond consistency
cover the same practical requirements without synchronous coupling.

The cadence is enforced by the source connector (it decides when to close an
epoch and emit the deltas).

### 10.3 Diamond Consistency via the Frontier Protocol

When two views share an upstream base table and a third view joins those two
views, the third view sees a *consistent snapshot* only if both upstream
refreshes finish before the join is computed. pg_trickle solves this by having
users declare a `DiamondConsistency::Atomic` group, which wraps the refreshes
in a shared SAVEPOINT. RockStream has no equivalent API. Instead, diamond
consistency is a structural property: the join operator computes the meet of
all its input frontiers and will not process epoch *N* until every input has
published a frontier of *N* or higher. This is unconditional — it applies to
every multi-input operator regardless of whether its inputs share ancestry. No
explicit group needs to be declared; you write the SQL and the invariant holds.

### 10.4 Per-Shard Adaptive Cost Model

pg_trickle's adaptive model (B-4) chooses between FULL recomputation and
DIFFERENTIAL refresh by measuring `avg_ms_per_delta` against `avg_ms_per_full`.
RockStream lifts this decision **to the shard**, not the cluster:

- Each shard records local `ms_per_delta` and `ms_per_full` exponentially
  weighted moving averages per operator.
- The shard-local planner picks DIFFERENTIAL when its own EWMA says so;
  otherwise it requests a snapshot rebuild for its partition only.
- A hot shard can recompute while sibling shards continue with deltas. The
  rebuilt shard publishes a frontier discontinuity that downstream operators
  handle the same way they handle a backfill (§12).

This avoids the centralized-scheduler assumption baked into pg_trickle and
matches the per-shard autonomy that the rest of the design depends on.

---

## 11. Recursion (`WITH RECURSIVE`)

The runtime shape is adopted from the DBSP `IterativeCircuit`, with the
strategy selection lessons from pg_trickle:

```
Recursive {
    base:     PlanNode P_base,
    step:     PlanNode P_step,           // contains RecursiveSelfRef
    result_arrangement: Arr,
}
```

The compiler first classifies the recursive query:

- **Monotone + insert-only delta**: semi-naive incremental propagation.
- **Monotone + mixed insert/delete/update delta**: DRed (delete-and-rederive).
- **Non-monotone term** (`EXCEPT`, aggregate, distinct, window, intersect, or
  unsupported multiple self-reference): full recomputation fallback.
- **Column mismatch between recursive CTE and stored output**: full
  recomputation fallback.

At runtime for the semi-naive path:

```
// Outer time: source_epoch (advancing once per ingestion epoch)
// Inner time: iteration (resets to 0 at each outer epoch)

result := apply(P_base, input_delta)   // iteration 0
emit_arrangement_delta(Arr, result, ts: { source_epoch, iteration: 0 })

iteration := 1
loop {
    delta := apply(P_step, [Arr at iteration-1, input_delta at iteration 0])
    delta := distinct(delta)             // standard DBSP requirement
    if delta is empty { break }          // fixed-point reached
    emit_arrangement_delta(Arr, delta, ts: { source_epoch, iteration })
    iteration += 1
}

// Output frontier on this operator advances to { source_epoch + 1, 0 }
// after the inner loop converges.
```

Convergence detection uses both data and progress: the inner loop terminates
when the change distinct-collapses to empty and the inner frontier can advance.
Every recursive operator also has a configured maximum iteration count and a
frontier-stall alarm; exceeding either fails the epoch conservatively instead
of looping forever.

This is the same model used in DBSP (`crates/dbsp/src/operator/recursive.rs`,
`IterativeCircuit`, `Z1`), extended with pg_trickle's practical DRed and
recomputation fallback rules.

### 11.1 Distributed Recursion

The single-process DBSP `IterativeCircuit` is **local to one worker** — the inner loop
runs on a single thread with shared in-process state. RockStream allows the
recursive scope to span shards by lifting two pieces into the distributed
layer:

1. **`Exchange` operators inside the recursive scope.** Each inner iteration
   can re-partition data on a different key, so `Arr` itself is sharded.
   The exchange runs once per iteration; backpressure is the normal
   credit-based mechanism.
2. **Per-iteration inner frontier.** The `Timestamp::iteration` component is
   aggregated across shards by the same frontier aggregator that handles
   source epochs. Convergence is declared when the cluster-level inner
   frontier stops advancing and every participating shard reports an empty
   delta for that iteration. There is no synchronous global barrier; the
   exit condition is observed via frontier progress.

Distributed recursion has additional safety controls beyond single-worker
recursion:

- **Max-iteration cap** per recursion node fails the epoch if exceeded.
- **Inner-frontier stall timeout** fails the epoch if the inner frontier
  fails to advance for a configured wall-clock window (catches dead shards
  inside the loop).
- **Per-shard cost guard**: if any shard's iteration cost spikes beyond a
  multiple of the cluster median, the planner falls back to recomputation on
  that shard.

Non-monotone recursion (EXCEPT, aggregate, distinct, window, intersect in the
recursive term) always falls back to `RecursionStrategy::Recompute`, exactly
as pg_trickle's `recursive_cte.rs` does.

---

## 12. Bootstrap, Backfill & Snapshot Reconciliation

When a new view is created over existing base tables, we cannot wait for a
delta to accumulate — we need the *current* answer immediately.

Three modes (model from pg_trickle's "initial materialize"):

### 12.1 Snapshot Mode (one-shot full compute)

For each base table, the source connector emits a single giant epoch
containing every row at weight +1. The circuit processes this exactly like
any other epoch — every operator's arrangement gets fully populated, and the
view sinks emit the initial output. After this, the source switches to
delta mode.

This works because the DBSP calculus is the same for snapshots and deltas:
applying the whole input as a single +1-weighted batch *is* a valid delta
against an empty starting state.

### 12.2 Streaming Bootstrap

For large base tables that don't fit in one epoch, we chunk the snapshot into
multiple epochs. The first N epochs carry slices of the base table; later
epochs carry actual deltas. Frontier protocol naturally orders this: the
output frontier doesn't advance past "bootstrap complete" until all slices
are processed.

### 12.3 Reconciliation (recover from CDC gaps)

If a connector loses its position (e.g., Postgres replication slot dropped),
the operator can re-snapshot the affected source. The arrangements absorb
the difference via the standard delta merge — any rows that no longer exist
in the source produce -1 weights, new rows produce +1 weights.

---

## 13. Implementation Plan for IVM

This expands [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) Phases 2 and 3
(SQL Frontend + Full Operator Library) with IVM-specific detail.

### Milestone IVM-1: Skeleton — Filter / Project / Map

**Scope**: pipeline accepts `SELECT a, b * 2 FROM t WHERE c > 10`.

- Define `PlanNode` enum with the variants in §5.
- Implement DataFusion → PlanIR lowering for: TableScan, Filter, Projection,
  no joins, no aggregates.
- Implement `differentiate` for Source / Filter / Project / Map (trivial —
  these are linear).
- Implement `Operator` trait + `OperatorTask` + `Circuit` runtime + shard-level
  epoch commit coordinator.
- Hard-code a single-shard runtime (no Exchange) for now.
- Implement `ShardDb` with SlateDB `WriteBatch`, `DbReader`, checkpoints,
  WAL reader smoke tests, segment extractor configuration, and explicit
  scan-and-delete cleanup helpers.
- Source connector that feeds a `Vec<RecordBatch>` as delta batches.
- View sink that writes Arrow-encoded rows to `view_output/`.
- **Reference oracle**: implement a brute-force "compute the view from
  scratch each epoch" path; run property test asserting incremental output
  == oracle output for random insert/delete sequences.

### Milestone IVM-2: Aggregation (Algebraic)

- Add `Aggregate` PlanNode with SUM, COUNT, AVG.
- Implement `AggregateMergeOp` and register with SlateDB.
- Implement `diff_aggregate` for invertible aggregates:
  - Group input batch by group_key.
  - Compute per-group `(Δsum, Δcount)`.
  - Merge into `op_state/0xAG` arrangement.
  - Read the *previous* and *current* aggregate, emit `(old, -1) ⊎ (new, +1)`.
- Cache the last-emitted value in `op_index/0xAG` so we know what to retract.
- Property tests: `SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k` against
  randomly-generated insert/update/delete sequences, compared to brute-force
  oracle.

### Milestone IVM-3: Non-invertible Aggregates (MIN/MAX)

- Add `MinMax` arrangement encoding (sorted multiset).
- Implement `diff_minmax` with extremum re-scan on delete.
- Property test against oracle.

### Milestone IVM-4: Inner Equi-Join

- Add `InnerJoin` PlanNode.
- Distribution pass: insert `Exchange` whenever the join key differs from the
  child's partition key. (No-op in single-shard mode; just placeholder.)
- Implement two-arrangement DBSP join with staged pre-change/current-change
  state and correction metadata derived from
  [`pg-trickle1/src/dvm/operators/join.rs`](../pg-trickle1/src/dvm/operators/join.rs)
  (EC-01 fix, Q07 correction, Q21 SemiJoin context).
- Add `JoinSemantics` metadata: inside-semi/anti context, child join depth,
  pre-change snapshot mode, key-change tracking, and correction ownership.
- Property test: 3-way join against oracle.
- Run TPC-H Q1 (filter+aggregate), Q3 (joins+agg), Q5 (5-way join).

### Milestone IVM-5: Outer / Semi / Anti Joins

- Add OuterJoin / SemiJoin / AntiJoin variants.
- Implement DBSP-native operators and validate against pg_trickle's implementations
  ([`outer_join.rs`](../pg-trickle1/src/dvm/operators/outer_join.rs),
  [`semi_join.rs`](../pg-trickle1/src/dvm/operators/semi_join.rs),
  [`anti_join.rs`](../pg-trickle1/src/dvm/operators/anti_join.rs)).
- Track matched/unmatched side state explicitly so NULL-padding transitions
  emit retractions and insertions deterministically.
- Run TPC-H Q11, Q21 (notorious for SemiJoin corner cases).

### Milestone IVM-6: Distinct / Union / Intersect / Except

- Implement weight-based arrangement with merge.
- Explicit deletes/tombstones for zero-crossing entries; optional
  frontier-aware compaction filter only after snapshot-safety audit.
- Property tests on set semantics.

### Milestone IVM-7: Window Functions

- Implement `Window` operator with partition-based recomputation.
- Add segment-tree variant for sliding aggregates (later optimization).
- Test ROW_NUMBER, RANK, LAG, LEAD, sliding SUM.

### Milestone IVM-8: Time Windows

- TUMBLE, HOP, SESSION windows.
- Event-time TTL on arrangement entries.
- Frontier-aware compaction filter against input/output frontiers; TTL is a
  retention hint, not the correctness rule.

### Milestone IVM-9: Recursion

- Implement `Recursive` operator with nested-time scheduling.
- Compiler classification: semi-naive for monotone insert-only recursion,
  DRed for monotone mixed changes, recomputation fallback for non-monotone or
  unsupported recursive terms.
- Convergence detection via inner frontier + empty distinct delta; add maximum
  iteration and frontier-stall safeguards.
- Test: transitive closure on a 1M-edge graph; recursive employee hierarchy.

### Milestone IVM-10: Bootstrap & Snapshot Mode

- Implement source-connector snapshot mode (§12.1).
- Implement streaming bootstrap (§12.2).
- Test: create a view over a 100M-row table; verify initial output equals
  batch query result.

### Milestone IVM-11: View-on-View (DAG)

- Implement `ViewRef` PlanNode that subscribes to an upstream view's CDC.
- Model pg_trickle's DAG semantics with native frontier coordination: cadence
  propagation, diamond consistency (structural via frontier meet — no explicit
  group API), and cycle rejection.
- Test: 5-level chain of views; each one is delta-driven by its parent.

### Milestone IVM-12: Lateral / Set-Returning Functions

- Implement Lateral operator with row-scoped recomputation.
- Required for JSON-heavy workloads and `unnest()` patterns.

### Milestone IVM-13: Correctness Soak

- Run the adapted pg_trickle TPC-H test suite (22 queries) on
  RockStream and compare results to a non-incremental reference.
- Run Nexmark for streaming-specific patterns.
- Random query-fuzz harness: generate random SQL, run incremental vs.
  batch, compare.
- Storage validation: prove no milestone relies on SlateDB range deletion;
  audit every compaction filter for snapshot safety; benchmark shard-level
  group commit against per-operator commit.

After IVM-13, the IVM engine is feature-complete for single-shard. Phase 4
of [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) (Multi-Shard & Exchange)
makes everything distributed.

---

## 14. Testing Strategy

### 14.1 The DBSP Correctness Property

For every operator `f`:

```
∀ initial_state S, ∀ sequence of deltas (Δ₁, Δ₂, …, Δₙ):
    apply_incremental(f, S, [Δ₁,…,Δₙ])
  ==
    apply_batch(f, S ⊎ Δ₁ ⊎ Δ₂ ⊎ … ⊎ Δₙ)
```

This is the DBSP soundness theorem made executable. Encode it as a
`proptest` strategy:

```rust
proptest! {
    #[test]
    fn incremental_equals_batch(
        initial in arb_dataset(),
        deltas  in arb_delta_sequence(),
        query   in arb_query(),
    ) {
        let inc   = run_incremental(&query, &initial, &deltas);
        let batch = run_batch(&query, &accumulate(initial, deltas));
        assert_eq!(inc.sort(), batch.sort());
    }
}
```

Run this property for every operator and every operator combination, with
the random query generator constrained to operators we've implemented so
far.

### 14.2 Reference Implementations

Two "ground truth" reference engines:
1. **In-process DataFusion**: runs the query as a batch over the accumulated
   collection. The arbiter of truth.
2. **pg_trickle**: where available, run the same query on a Postgres
   instance with pg_trickle and compare results. Useful as a second oracle.

### 14.3 TPC-H Conformance

Adapt pg_trickle's TPC-H test suite (22 queries, 3 modes in the reference suite
at SF=0.01). Required to pass:
- 22 / 22 queries produce identical results to DataFusion batch.
- All produce identical results across deferred differential mode and snapshot
  bootstrap.
- ≥ 10× speedup over batch at 1% change rate.

### 14.4 Determinism Tests (DST-style)

SlateDB has `slatedb-dst` for deterministic simulation testing. Adopt the
same pattern for RockStream: a single-thread, deterministic-RNG harness
that drives source connectors with a fixed seed and verifies bit-identical
output across runs.

The operator harness follows DESIGN.md §17.3: the simulator amplifies explicit
invariants rather than replacing them. Every operator that writes arrangement
state must assert its encoded `(row_id, schema_version, weight)` contract before
the write and assert the same contract after readback through the storage
wrapper. Every frontier-producing operator asserts monotonicity before emission;
the downstream consumer asserts it again on receipt.

---

## 15. Open Questions

1. **Reuse pg_trickle's SQL-generation rules as a debug oracle?**
   The exact SQL strings pg_trickle emits, when run on the same data,
   should produce the same deltas as our native operator implementations.
   This is an extremely cheap second oracle. Worth investing in a
   side-by-side test harness early.

2. **Where to draw the line between DataFusion physical operators and
   custom IVM operators?**
   DataFusion's `HashJoinExec`, `HashAggregateExec` etc. are excellent for
   the batch path (snapshot mode, ad-hoc queries). Should our incremental
   `InnerJoin` operator reuse pieces of `HashJoinExec` for the in-memory
   probe step, or implement from scratch? Initial decision: reuse only the
   expression evaluation; implement the dataflow scaffold ourselves.

3. **Code generation later?**
   If profiling shows the interpretation overhead dominates for trivial
   queries, we can add a per-circuit code-gen pass that emits a specialized
   Rust function — but only as an optimization, not as the primary model.
   The DBSP reference implementation suggests interpretation is fine for non-trivial
   queries.

4. **Arrangement format: Arrow rows vs. typed-batch?**
   Storing entire Arrow `RecordBatch` slices in SlateDB values is convenient
   but pays the IPC framing cost per row. Alternative: row-format encoding
   (similar to Apache Arrow Row format) for arrangement values, since
   arrangements are mostly point-accessed. Benchmark in Phase 3.

5. **Materialize-style compaction of arrangements?**
  Materialize aggressively compacts its arrangements past the consumer
  frontier (drops historical versions no one will query). SlateDB compaction
  filters can help only after a snapshot-safety proof; we may still need
  active arrangement consolidation for long-running queries.

These are explicitly open and will be answered with prototypes during
Milestones IVM-1 through IVM-5.

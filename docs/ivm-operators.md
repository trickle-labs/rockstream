# IVM Operator Reference

> **v0.27 documentation deliverable** (ROADMAP documentation budget).
>
> Every operator in the catalog with its arrangement layout, merge-law
> annotation, and `EXPLAIN INCREMENTAL` output format.

---

## Overview

RockStream's IVM (Incremental View Maintenance) engine is organized as a
pipeline of **operators**. Each operator receives a **Z-set delta batch**
(`ZSetBatch`) and emits a corresponding output delta. Operators either maintain
**arrangement state** (per-key aggregation state backed by SlateDB) or are
**stateless** (no arrangement, pure row-level transformation).

All arrangement state is tagged with an `ArrangementHeader` (4 bytes: `law_id
|| law_version`). The header drives compaction, exchange combiners, and the
cost model. Operators that cannot use merge-safe reads declare a
`not_merge_safe_reason` from the closed enum in `rockstream-types::explain`.

---

## Merge-Law Annotation Contract

Every operator node in `EXPLAIN INCREMENTAL` carries:

| Field | Description |
|---|---|
| `merge_law` | `<name>/v<n>` or `none` for stateless operators |
| `law_class` | `AbelianGroup`, `Semilattice`, or `—` |
| `idempotent` | `true` / `false` |
| `duplicate_policy` | `Merge`, `Reject`, `LastWriterWins` |
| `compaction` | `MergeOnCompact`, `TombstoneGc`, `RetainAll` |
| `combiner` | `yes` (exchange combiner available) or `no` |
| `partial_pushdown` | `yes` / `no` |
| `not_merge_safe_reason` | closed-enum reason, or omitted if merge-safe |

---

## Operator Catalog

### Filter

**Crate**: `rockstream-ops::filter`  
**Struct**: `FilterOperator`

| Property | Value |
|---|---|
| Merge law | `none` |
| Law class | — |
| Idempotent | — |
| Arrangement | None (stateless) |
| `EXPLAIN` indicator | ✓ (stateless) |
| `not_merge_safe_reason` | `stateless` |

**Description**: Applies a predicate `(key: &[u8], value: &[u8]) -> bool` to
each row in the incoming Z-set. Rows where the predicate returns `false` are
dropped; rows where it returns `true` are passed through with their weights
unchanged. No arrangement state is maintained.

**Arrangement layout**: N/A.

**EXPLAIN INCREMENTAL output**:
```
Filter(name=<name>)
  merge_law=none  law_class=—  not_merge_safe_reason=stateless
  rows_processed=<n>  rows_emitted=<m>
```

---

### Project

**Crate**: `rockstream-ops::project`  
**Struct**: `ProjectOperator`

| Property | Value |
|---|---|
| Merge law | `none` |
| Law class | — |
| Idempotent | — |
| Arrangement | None (stateless) |
| `EXPLAIN` indicator | ✓ (stateless) |
| `not_merge_safe_reason` | `stateless` |

**Description**: Transforms the key and/or value of each row via user-provided
projection functions. No arrangement state. Weight passthrough.

**Arrangement layout**: N/A.

---

### Map

**Crate**: `rockstream-ops::map`  
**Struct**: `MapOperator`

| Property | Value |
|---|---|
| Merge law | `none` |
| Law class | — |
| Arrangement | None (stateless) |
| `not_merge_safe_reason` | `stateless` |

**Description**: Like `Project` but replaces both key and value. Stateless,
weight passthrough.

---

### Aggregate (SUM / COUNT / AVG)

**Crate**: `rockstream-ops::aggregate`  
**Struct**: `AggregateMergeOp`

| Property | Value |
|---|---|
| Merge law | `SumCount/v1` (0x0002) |
| Law class | `AbelianGroup` |
| Idempotent | false |
| Duplicate policy | `Merge` |
| Compaction | `TombstoneGc` |
| Frontier policy | `AnyAdvancement` |
| Arrangement | Per-group-key → 16-byte `(sum: i64, count: i64)` |
| RMW required | **No** (blind merge — abelian group) |
| `EXPLAIN` indicator | ✓ (merge-safe) |

**Description**: Maintains one 16-byte accumulator per group key. Each delta
row contributes `(sum_contribution × weight, count_contribution × weight)` via
`SumCount/v1::merge`. The output is a `(group_key → avg/sum/count)` delta.

Groups whose accumulator reaches `(0, 0)` are tombstoned via `TombstoneGc`.

**Arrangement layout**:
```
Key: group_key_bytes
Value: [ArrangementHeader: 4 bytes][sum: i64 BE][count: i64 BE]
Total value size: 4 + 8 + 8 = 20 bytes
```

**EXPLAIN INCREMENTAL output**:
```
Aggregate(name=<name>, fn=SUM/COUNT/AVG)
  merge_law=SumCount/v1  law_class=AbelianGroup  idempotent=false
  compaction=TombstoneGc  combiner=yes  partial_pushdown=yes
  groups=<n>  state_bytes=<n × 20>
```

---

### Aggregate (MIN / MAX)

**Crate**: `rockstream-ops::min_max`  
**Struct**: `MinMaxOp`

| Property | Value |
|---|---|
| Merge law | `MaxRegister/v1` (0x0003) or `MinRegister/v1` (0x0004) as sub-component |
| Law class | `Semilattice` (sub-component) |
| Idempotent | true (sub-component) |
| Arrangement | Per-group-key → indexed multiset + cached extremum |
| RMW required | **Yes** (retraction requires prefix scan) |
| `EXPLAIN` indicator | ⚠ (not merge-safe) |
| `not_merge_safe_reason` | `extremum_requires_rmw` |

**Description**: Maintains a full multiset per group to support retraction
(deletion of the current MIN/MAX). When a retraction arrives, the operator
performs a prefix scan to find the next extremum. The cached extremum is backed
by `MaxRegister/v1` or `MinRegister/v1` as a sub-component law, but the
operator itself is retraction-aware and cannot avoid a read on delete.

**Arrangement layout**:
```
Key: [group_key_bytes][value: i64 BE]
Value: [refcount: i64 BE]  (number of times this value appears)

Cached extremum key: group_key_bytes (special prefix)
Cached extremum value: [ArrangementHeader: 4 bytes][extremum: i64 BE]
```

**EXPLAIN INCREMENTAL output**:
```
MinMax(name=<name>, kind=MIN|MAX)
  merge_law=MaxRegister/v1  law_class=Semilattice  idempotent=true
  not_merge_safe_reason=extremum_requires_rmw
  compaction=RetainAll  combiner=no  partial_pushdown=no
  groups=<n>  multiset_entries=<m>
```

---

### Distinct

**Crate**: `rockstream-ops::distinct`  
**Struct**: `DistinctOp`

| Property | Value |
|---|---|
| Merge law | `WeightAdd/v1` (0x0001) |
| Law class | `AbelianGroup` |
| Idempotent | false |
| Duplicate policy | `Merge` |
| Compaction | `TombstoneGc` |
| Arrangement | Per-distinct-key → 8-byte i64 weight |
| RMW required | **No** (abelian group) |
| `EXPLAIN` indicator | ✓ (merge-safe) |

**Description**: Tracks cumulative weight per distinct key. Emits `+1` when
weight first exceeds 0 and `-1` when it drops to 0. Uses `WeightAdd/v1` for
the weight accumulator; `TombstoneGc` reclaims zero-weight entries.

**Arrangement layout**:
```
Key: distinct_key_bytes
Value: [ArrangementHeader: 4 bytes][weight: i64 BE]
Total value size: 12 bytes
```

---

### Set Operations (UNION / INTERSECT / EXCEPT)

**Crate**: `rockstream-ops::set_ops`  
**Struct**: `UnionOp`, `IntersectOp`, `ExceptOp`

| Property | Value |
|---|---|
| Merge law (UNION) | `WeightAdd/v1` (0x0001) — weight sum |
| Merge law (INTERSECT/EXCEPT) | `WeightAdd/v1` with min-clamp |
| `not_merge_safe_reason` (INTERSECT/EXCEPT) | `clamp_not_a_law` |
| Arrangement | Per-key weight accumulator |
| `EXPLAIN` indicator (UNION) | ✓ |
| `EXPLAIN` indicator (INTERSECT/EXCEPT) | ⚠ |

**Description**: All three operators use weight arithmetic. UNION sums weights;
INTERSECT/EXCEPT clamp weights at a minimum (zero for INTERSECT, negative for
EXCEPT). Clamping is not an associative law (`min-clamp(a, min-clamp(b, c)) ≠
min-clamp(min-clamp(a, b), c)` in general), so INTERSECT/EXCEPT are not
merge-safe and declare `clamp_not_a_law`.

---

### Inner Join

**Crate**: `rockstream-ops::join`  
**Struct**: `HashJoinOp`

| Property | Value |
|---|---|
| Merge law | `WeightAdd/v1` per arrangement side |
| Law class | `AbelianGroup` |
| Arrangement | Two sides: left-key → rows, right-key → rows |
| `EXPLAIN` indicator | ✓ per arrangement |

**Description**: Maintains two arrangements (left and right). When a delta
arrives on one side, it probes the opposite side's arrangement to generate
output tuples. Arrangement values are lists of row weights; `WeightAdd/v1`
accumulates weights per join key.

**Arrangement layout** (per side):
```
Key: [join_key_bytes][row_key_bytes]
Value: [weight: i64 BE]
```

---

### Outer Join (Left / Right / Full)

**Crate**: `rockstream-ops::outer_join`  
**Struct**: `OuterJoinOp`

| Property | Value |
|---|---|
| Merge law | `WeightAdd/v1` per side |
| `not_merge_safe_reason` | `extremum_requires_rmw` (unmatched-row state) |
| Arrangement | Two sides + unmatched-row state |
| `EXPLAIN` indicator | ⚠ |

**Description**: Extends `HashJoinOp` with unmatched-row tracking. Unmatched
rows must be retracted when a match arrives, requiring a prior read.

---

### Window (ROW_NUMBER / RANK / LAG / LEAD / Sliding SUM/AVG)

**Crate**: `rockstream-ops::window`  
**Struct**: `WindowOp`

| Property | Value |
|---|---|
| Merge law (sliding SUM/AVG) | `SumCount/v1` (sub-component) |
| Merge law (ranking) | none — partition recomputation |
| `not_merge_safe_reason` | `partition_recomputation` |
| Arrangement | Per-partition row set |
| `EXPLAIN` indicator | ⚠ |

**Description**: Ranking functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`) recompute
the entire partition when any row changes — they are not merge-safe. Sliding
aggregates (`SUM`, `AVG`) reuse `SumCount/v1` where possible. `LAG`/`LEAD`
require positional access and use partition recomputation.

---

### Tumble Window

**Crate**: `rockstream-ops::tumble`  
**Struct**: `TumbleOp`

| Property | Value |
|---|---|
| Merge law | `MaxRegister/v1` (watermark) |
| Frontier policy | Watermark-driven |
| Arrangement | Per-window bucket |
| `EXPLAIN` indicator | ✓ (within window) |

**Description**: Partitions event-time rows into fixed-width tumbling windows.
Watermarks are tracked via `MaxRegister/v1` (semilattice, idempotent). A window
is closed and emitted exactly once when its watermark passes the window
boundary.

---

### Top-K

**Crate**: `rockstream-ops::top_k`  
**Struct**: `TopKOp`

| Property | Value |
|---|---|
| Merge law | `MaxRegister/v1` sub-component for cached extremum |
| `not_merge_safe_reason` | `extremum_requires_rmw` |
| Arrangement | Ranked multiset per partition |
| `EXPLAIN` indicator | ⚠ |

**Description**: Maintains a ranked multiset of size `K + epsilon` per
partition. Deletes from the current top-K require a prefix scan refill path.
`MaxRegister/v1` is used as a sub-component for the cached extremum boundary.

---

### Recursive

**Crate**: `rockstream-ops::recursive`  
**Struct**: `RecursiveOp`

| Property | Value |
|---|---|
| Merge law | `WeightAdd/v1` (monotone progress) |
| `not_merge_safe_reason` | `recursion_dred_required` (non-monotone) |
| Arrangement | Working set + delta accumulator |
| `EXPLAIN` indicator | ✓ (monotone) / ⚠ (non-monotone, rejected) |

**Description**: Implements semi-naive recursive evaluation. Monotone
(insert-only) recursive terms use `WeightAdd/v1` to publish partial progress
via `complete_through`. Non-monotone recursive terms are rejected at runtime
with `RS-1009` and annotated `not_merge_safe_reason=recursion_dred_required`.

---

### Snapshot (Bootstrap Source)

**Crate**: `rockstream-ops::snapshot`  
**Struct**: `SnapshotOp`

| Property | Value |
|---|---|
| Merge law | `none` |
| Arrangement | None (source-only) |
| `EXPLAIN` indicator | ✓ |

**Description**: Delivers a pre-loaded relation as insert-only batches during
bootstrap. `resume_from(N)` skips already-committed rows on restart. Signals
bootstrap completion via `is_complete()`.

---

### ViewRef

**Crate**: `rockstream-ops` / `rockstream-plan`  
**Kind**: `OpKind::ViewRef`

| Property | Value |
|---|---|
| Merge law | Inherited from upstream view |
| Arrangement | Inherited from upstream |
| `EXPLAIN` indicator | Inherits from upstream operator |

**Description**: References another materialized view's output as an input
source. CDC (change data capture) semantics: downstream operators receive
upstream deltas on each frontier advance.

---

## State Budget Enforcement

Every arrangement-backed operator accepts a `StateBudget` (from
`rockstream-types::state_budget`) that limits the maximum bytes of arrangement
state. When a `try_acquire(bytes)` call exceeds the budget, the operator
returns error `RS-5003` rather than growing unbounded in memory.

```rust
use rockstream_types::state_budget::StateBudget;

let budget = StateBudget::new("my_aggregate", 64 * 1024 * 1024); // 64 MiB
budget.try_acquire(key_size + value_size)?;  // RS-5003 if over budget
```

Budget utilisation is exposed via `StateBudget::utilization()` for metrics
and diagnostics.

---

## RMW-Avoidance Ratio

The `rockstream-types::metrics` module publishes per-law RMW-avoidance ratios
via `rmw_avoidance_ratio(key)` and `rmw_ratio_report()`. The v0.27 proof
requirement is:

> `SumCount/v1` and `WeightAdd/v1` must have avoidance ratio = 1.0 (100%
> RMW-free on the hot path).

| Law | Class | RMW Avoided | Expected Ratio |
|---|---|---|---|
| `WeightAdd/v1` | AbelianGroup | Yes | 1.00 |
| `SumCount/v1` | AbelianGroup | Yes | 1.00 |
| `MaxRegister/v1` | Semilattice | No | 0.00 |
| `MinRegister/v1` | Semilattice | No | 0.00 |
| `HyperLogLog/v1` | Semilattice | No | 0.00 |
| `BloomUnion/v1` | Semilattice | No | 0.00 |

The CI proof test `per_law_rmw_avoidance_ratio_proof` in
`crates/rockstream-ops/benches/ivm_vs_batch.rs` verifies these ratios.

---

## Merge-Read Fallback

When a merge law's operand is malformed, the storage layer increments
`merge_law_fallback_total` (via `rockstream_types::metrics::inc_fallback`) and
falls back to the identity element. This prevents data loss at the cost of
correctness for the affected key. The metric is exposed for diagnostics and
future Prometheus wiring.

---

## EXPLAIN INCREMENTAL Output Levels

| Level | Description |
|---|---|
| `Default` | ✓/⚠/✗ per operator, merge_law name, state_bytes estimate |
| `Verbose` | Adds law_class, idempotent, compaction, combiner, partial_pushdown, shard count, parallelism, frontier timestamps |
| `Analyze` | Adds live per-operator runtime statistics: rows/s, state_reads, RMW_ratio, p99_latency, DLQ_entries |

---

*Document generated for v0.27.0. Update on any operator surface change.*

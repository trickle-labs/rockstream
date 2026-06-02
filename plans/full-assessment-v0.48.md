# Rockstream v0.48.0 — Full Technical Assessment

**Auditor role:** Principal Database Architect / Cloud-Native Systems Engineer
**Scope:** Spec-to-code drift, IVM engine internals, cloud-native architecture, ergonomics, code/concurrency quality
**Inputs reviewed:** `DESIGN.md` (5,186 LOC), `IVM.md` (1,334 LOC), `IMPLEMENTATION_PLAN.md` (1,998 LOC), `ROADMAP.md`, `docs/*.md`, `sign-offs/v0.4*.md`, every `crates/*/src/**/*.rs` (~63.6k LOC across 15 crates, 205 source files), git history through commit `302b6bb` (v0.48.0).
**Build/test status at audit:** `cargo test --workspace` — 1,298 passed, 2 ignored, 0 failed (57 suites, 83s wall).

---

## 1. Executive Summary

Rockstream is, at v0.48.0, an **ambitious, structurally coherent IVM kernel** with a real Z‑set core, a properly factored `MergeLaw`/`LawBundle` catalog, an LSM-on-object-store storage contract (SlateDB), an explicit exchange path classifier, cluster checkpoints, recovery driver, a pgwire read+write gateway, and a freshly published Tier 2 connector contract. The compile/test bar is green; sign-off discipline is genuine; the law abstraction is the project's strongest architectural idea and it is actually plumbed through storage, operators, exchange, and (partly) gateway.

That said, the project at v0.48 has crossed a credibility threshold where the **claimed surface area is materially larger than the operational surface area**. The Integration Beta milestone has been signed off, but a non-trivial fraction of the user-visible features in versions v0.16, v0.17, v0.41, v0.44, v0.47, and v0.48 are **type definitions plus isolated tests with no production call site**. The Definition of Done — "no unbounded queues, every error has a code, every claim has a benchmark, every coordination path has a SimRuntime test" — is partially honored at the abstract level and is partially violated at the integration level.

Health by axis:

| Axis | Grade | Why |
|---|---|---|
| Spec rigor (design intent) | A− | Genuinely thorough specs, law-first architecture is sound. |
| Spec adherence (code vs spec) | C+ | Multiple checked-`Done` features are stubs; see §2. |
| IVM kernel correctness | B+ | Core algebra appears right; fallback/corruption paths leak. |
| Cloud-native topology | B | Storage/compute split is real; segment cache claimed, weakly evidenced. |
| Async / concurrency hygiene | B− | Several `unwrap()`s in hot paths; some unbounded vectors. |
| Public ergonomics | C+ | Connector trait too defaulted; gateway errors lack `next_steps`; storage `Result<Option<_>>` everywhere. |
| Test coverage breadth | B+ | 1,298 unit/integration tests; soak/chaos infra exists. |
| Test coverage depth (end-to-end) | C | Many tests verify a *type* not a *path*. |

**Headline risks blocking v0.49+:**

1. Several v0.16/v0.41/v0.47/v0.48 "Done" features are stub-only (workload SLO enforcement, `rockstream_catalog` virtual tables, DLQ persistence, `EXPLAIN TRANSACTION`, connector lifecycle wiring, `EXPLAIN INCREMENTAL ANALYZE` live worker round-trip).
2. The durable shuffle decode path panics on malformed objects (5× `try_into().unwrap()` in a hot recovery path).
3. The merge-read fallback returns raw bytes on corruption while only incrementing a metric — silent semantic drift.
4. Stateful operators have no `StateBudget` enforcement despite the explicit Definition of Done that bans unbounded in-memory accumulation.
5. Idempotency-key dedup for DML lives in gateway memory only; a gateway restart loses dedup history (v0.44 "1M concurrent counter increments land exact total" proof is single-process).

The above are correctable, and most fixes are local. None invalidate the architecture. They do invalidate roughly one to two versions' worth of sign-offs as currently scoped, and v0.49 should not proceed until the drift list is acknowledged in writing.

---

## 2. The Drift Log

Each entry: **Spec claim** (file:line) → **Code reality** (file:line) → **Severity** → **Recommendation**.

### 2.1 Critical drift

#### D-01 — `rockstream_catalog.*` virtual tables return fabricated data
- **Spec:** [ROADMAP.md](../ROADMAP.md) v0.41 row: "`SELECT * FROM rockstream_catalog.merge_laws` returns the registered catalog … `rockstream_catalog.epochs|pipelines|shards|audit_log`". Signed off.
- **Code:** [`crates/rockstream-gateway/src/rockstream_catalog.rs`](../crates/rockstream-gateway/src/rockstream_catalog.rs) — comment at L113 explicitly says *"In a live cluster these would be populated from the control-plane state; the stub allows catalog queries to succeed."* `catalog_epochs` synthesizes `committed_epoch: i * 100`. `catalog_dead_letter_queue` returns one hard-coded entry with `raw_bytes_hex = "DEADC0DE"`.
- **Severity:** Critical. Operators querying for cluster state get lies.
- **Fix:** Wire each function to its real source: `catalog_epochs` → control-plane `epochs` index; `catalog_shards` → topology service; `catalog_audit_log` → audit log store; `catalog_dead_letter_queue` → per-source DLQ sink. Until then, mark the schema as `_stub` or fail with `RS-XXXX experimental.catalog_unwired` so the data is never silently consumed.
- **Caveat:** `merge_laws` is the one entry that *is* real — it iterates the registered `LawBundle`s. So the contract is half-honored.

#### D-02 — Workload SLO is parsed and stored but never enforced
- **Spec:** [ROADMAP.md](../ROADMAP.md) v0.16: "`CREATE WORKLOAD` with `FRESHNESS_SLO`, `MEMORY_LIMIT`, `PRIORITY`; SHOW VIEW STATUS reports current state and SLO". DESIGN.md §14.2–14.3.
- **Code:** `FreshnessSlo` / `MemoryLimit` / `WorkloadPriority` are defined in [`crates/rockstream-types/src/workload.rs`](../crates/rockstream-types/src/workload.rs) and parsed by [`crates/rockstream-catalog/src/lib.rs`](../crates/rockstream-catalog/src/lib.rs) (L362–L510). `grep` for `FreshnessSlo|MemoryLimit` across `crates/rockstream-runtime` and `crates/rockstream-ops` returns **zero** matches. Scheduler/epoch-coordinator/state-budget paths do not consult workload SLOs.
- **Severity:** Critical. The auto-tuner v0.51 has no foundation; the v0.50 `SHOW RESOURCE USAGE` work will land on a non-enforcing substrate.
- **Fix:** Plumb `WorkloadId` through `OperatorContext`. The epoch coordinator should size epochs against `freshness_slo_ms`. The state budget enforcer must charge against `memory_limit`. Add an `RS-5018`-style proactive notice now (not in v0.50) so users at least see the budget exists.

#### D-03 — DLQ persistence and `REPLAY`/`DISMISS` semantics are catalog-only
- **Spec:** [ROADMAP.md](../ROADMAP.md) v0.47: "`rockstream_catalog.dead_letter_queue` exposes failed records … `ALTER SOURCE … REPLAY DEAD_LETTER_QUEUE`, `… DISMISS DEAD_LETTER_QUEUE WHERE …`". Signed off.
- **Code:** DDL recognition exists in [`crates/rockstream-gateway/src/inline_view.rs`](../crates/rockstream-gateway/src/inline_view.rs) L407–L529 (string-match on `REPLAY DEAD_LETTER_QUEUE` / `DISMISS DEAD_LETTER_QUEUE`), but the dispatch lands on in-memory catalog mutation. No connector source actually writes to a DLQ on decode failure; the Kafka/Postgres/HTTP sources have no DLQ code path. The proof test `proof_dlq_replay_increments_attempt` operates on the in-memory catalog.
- **Severity:** Critical. A v0.47 sign-off claim ("Postgres CDC → IVM → Kafka sustains 100k rows/s for 24h exactly once; DLQ surfaces failed records") is not supported by any wiring between a real source's decode error and the catalog.
- **Fix:** Define a per-source DLQ sink interface (an actual `ShardDb` table prefix is the cheapest), have every `Source::poll_batch` decode error funnel through it, and have `catalog_dead_letter_queue` scan that storage. Without this, `REPLAY` is theatre.

#### D-04 — `EXPLAIN TRANSACTION` type exists with zero generation path
- **Spec:** [ROADMAP.md](../ROADMAP.md) v0.48: "connector write-classification metadata surfaces in `EXPLAIN TRANSACTION`". Signed off.
- **Code:** [`crates/rockstream-types/src/connector.rs`](../crates/rockstream-types/src/connector.rs) L220+ defines `ExplainTransaction`, `ExplainTransactionColumn`, and a `from_schema_metadata()` helper. No call site in `rockstream-sql`, `rockstream-gateway`, `rockstream-runtime`, or `rockstream-plan` constructs one from a real query.
- **Severity:** Critical for the v0.48 milestone integrity (it's an explicit sign-off bullet).
- **Fix:** Either implement the SQL frontend path (parser recognizes `EXPLAIN TRANSACTION`, planner gathers `LawSchemaMetadata` from every source/sink in the plan, gateway renders the struct) or strike the bullet from the v0.48 sign-off and reopen it as v0.49 scope.

#### D-05 — Tier 2 sink `should_flush(bytes_buffered, epochs_buffered)` is never consulted
- **Spec:** v0.48 proof bullet: "Iceberg sink implementing Tier 2 `should_flush` with a 10ms epoch produces ≤ 2 files/minute (≥ 256 MB each)".
- **Code:** [`crates/rockstream-connectors/src/sink.rs`](../crates/rockstream-connectors/src/sink.rs) defines the method with a default that flushes every epoch; [`crates/rockstream-connectors/src/iceberg_sink.rs`](../crates/rockstream-connectors/src/iceberg_sink.rs) overrides it. The pipeline driver in [`crates/rockstream-runtime/src/pipeline.rs`](../crates/rockstream-runtime/src/pipeline.rs) calls `sink.write_batch` then `sink.commit(epoch)` unconditionally and **never asks `should_flush`**. The "≤ 2 files/minute" proof is a unit test against the sink in isolation, not an end-to-end runtime test.
- **Severity:** Critical. This is the headline v0.48 proof; in the running engine the Iceberg sink will still produce one file per epoch.
- **Fix:** In `pipeline.rs`, replace unconditional commit with: track `bytes_buffered` and `epochs_buffered`, call `sink.should_flush(...)`, only commit when true (with a hard ceiling of `max_epochs_buffered` and `max_bytes_buffered` for backpressure). Add a runtime-level proof test that wires source → operator → IcebergSink and counts artifact files.

#### D-06 — Partition filter pushdown traits exist with no planner derivation
- **Spec:** v0.48 proof bullet: "`partition_filter_support() -> bool` returns false on connectors that do not implement pushdown and operator-layer filtering is verified to produce identical output".
- **Code:** [`crates/rockstream-types/src/connector.rs`](../crates/rockstream-types/src/connector.rs) defines `PartitionFilter` / `PartitionPredicate` with `eq()` / `between()` builders. [`crates/rockstream-connectors/src/source.rs`](../crates/rockstream-connectors/src/source.rs) defines `start_snapshot(filter)` / `poll_delta(filter)` defaulting to `poll_batch`. The SQL frontend / planner never derives a `PartitionFilter` from a WHERE clause; the pipeline driver always calls `poll_batch`, never `start_snapshot(Some(f))` or `poll_delta(Some(f))`.
- **Severity:** Critical. The "operator-layer filtering produces identical output" guarantee is trivially satisfied because no push-down ever happens.
- **Fix:** Add a planner pass that recognizes column equality / range predicates against partition columns, builds a `PartitionFilter`, and the source operator dispatches to `start_snapshot(Some(filter))` when `source.partition_filter_support()` is true. Add a regression test asserting both branches return identical Z-set output.

#### D-07 — Connector lifecycle (pause/resume/delete) is example-only
- **Spec:** v0.48 row: "Connector pause/resume/delete; pause/resume/delete on all new connectors". Signed off.
- **Code:** `ConnectorLifecycleState` is defined in [`crates/rockstream-types/src/connector.rs`](../crates/rockstream-types/src/connector.rs). [`crates/rockstream-connectors/src/example_sdk.rs`](../crates/rockstream-connectors/src/example_sdk.rs) is the only connector with non-trivial pause/resume logic. Real connectors (Kafka, Postgres CDC, S3, HTTP, Iceberg) inherit defaults that return `false`. No DDL surface (`ALTER SOURCE … PAUSE` / `RESUME` / `DROP CONNECTOR`); no control-plane state-machine transitions; no audit events.
- **Severity:** Critical for the sign-off ("pause/resume/delete implemented on **all** new connectors"); the bullet is literally false.
- **Fix:** Either correct the sign-off to "lifecycle types defined; integration deferred to v0.49" or implement the DDL parsing path, control-plane transition handler, audit emission, and update Kafka/Postgres/S3/HTTP/Iceberg to honor it.

#### D-08 — Idempotency-key dedup is gateway-local (gateway restart loses history)
- **Spec:** v0.44 sign-off: "1M concurrent counter increments with idempotency keys land exact total". DESIGN.md §13.5.2 specifies a *per-shard, time-bounded* idempotency table that participates in the per-shard epoch commit.
- **Code:** [`crates/rockstream-gateway/src/dml.rs`](../crates/rockstream-gateway/src/dml.rs) L184–L343 stores idempotency keys on the in-memory `OptimisticTransaction` struct and checks against a `Vec<CommittedWrite>`. No persistent per-shard idempotency-key table; no participation in the per-shard epoch `WriteBatch`. Restarting the gateway loses dedup state.
- **Severity:** Critical for the "exactly once at the boundary" claim; not visible in the 1M proof test because that test runs in one process.
- **Fix:** Add a `0x02 0xIK shard(4) idem_key_hash(16) → epoch(8)` namespace in `crates/rockstream-storage/src/keys.rs`; have the per-shard epoch commit include idempotency-key inserts atomically with state writes; have the gateway look up the key in the writer shard's DbReader before assuming uniqueness; expire keys via compaction filter at `now - 24h`. Update the v0.44 proof to span a gateway restart.

#### D-09 — Durable shuffle decoder panics on malformed object footer
- **Spec:** DESIGN.md §1 fail-closed posture; v0.31 "object-store fallback path … receiver re-merges per-target operands".
- **Code:** [`crates/rockstream-runtime/src/exchange/durable.rs`](../crates/rockstream-runtime/src/exchange/durable.rs) L146, L152, L172, L174, L176 — `decode_object` does `data[…].try_into().unwrap()` five times in the hot durable-receive path, panicking on any truncation or corruption.
- **Severity:** Critical. A single corrupted shuffle object on object storage crashes the receiving worker; lease churn cascades.
- **Fix:** Replace `try_into().unwrap()` with `try_into().map_err(|_| DurableError::MalformedObject)?`; add fuzz cases (truncated to 0..N bytes, random byte flips) to `durable_tests`.

#### D-10 — Merge-read fallback silently returns raw bytes on corruption
- **Spec:** DESIGN.md §6.11 — invariant: "the value returned equals `merge_law(all visible operands)`".
- **Code:** [`crates/rockstream-storage/src/shard_db.rs`](../crates/rockstream-storage/src/shard_db.rs) L270–L295 (per the IVM subagent walkthrough): when a stored operand fails `is_valid_law_operand`, the code increments `merge_law_fallback_total` and **returns the raw bytes**. Downstream operators consume the corrupted value as if it were the true merged result; readers have no flag.
- **Severity:** Critical. A bumped fallback counter is the only signal of silent correctness drift.
- **Fix:** Either (a) hard-fail with `RS-5003 law.operand_corruption` on first detection (the safe default for a correctness-first system), or (b) wrap the returned value in a `MaybeFallback { value, was_fallback: bool }` and propagate the flag to query results (`pg_notice`, EXPLAIN, subscribe metadata). Option (a) is more consistent with project posture.

#### D-11 — Stateful operators have no `StateBudget` enforcement
- **Spec:** ROADMAP.md Definition of Done: *"Any new queue, buffer, or scan window has a named upper bound, a metric reporting current fill level, and a backpressure or error path when the bound is reached. Unbounded in-memory accumulation is never acceptable."* `crates/rockstream-types/src/state_budget.rs` (279 LOC) defines the budget type.
- **Code:** [`crates/rockstream-ops/src/aggregate.rs`](../crates/rockstream-ops/src/aggregate.rs) `AggregateMergeOp { agg_state: HashMap<Vec<u8>, Vec<u8>>, last_emitted: HashMap<…>, … }` — neither map is bound by any budget; neither consults `state_budget::try_acquire`. Same pattern in `min_max.rs`, `join.rs`, `top_k.rs`, `distinct.rs`.
- **Severity:** Critical. The single most explicit DoD rule is universally violated for stateful operators.
- **Fix:** Inject `Arc<StateBudgetMeter>` (already exists) into every stateful operator constructor; on every `merge`/`insert`, call `meter.charge(delta_bytes)` and on `Err(BudgetExceeded)` emit `RS-3604` (or the closest existing code) and surface `OVER_BUDGET_RELAXED` per docs/concepts.md L1294.

### 2.2 Major drift

#### D-12 — `EXPLAIN INCREMENTAL ANALYZE` lacks the live worker round-trip
- **Spec:** v0.17 proof: "`ANALYZE` adds live per-operator runtime statistics … requiring a live worker round-trip."
- **Code:** `OperatorStats { rows_processed, state_read_count, rmw_ratio, p99_latency_ms, dlq_entries }` exists in `rockstream-types/src/explain.rs`. No RPC method, no gateway → worker collector, no test of live values.
- **Severity:** Major; v0.17 signed off with this missing.
- **Fix:** Add `ControlService::collect_operator_stats(pipeline_id) -> Vec<OperatorStats>`; have the gateway's `EXPLAIN INCREMENTAL ANALYZE` handler call it and render. Add a SimRuntime test asserting non-zero `rows_processed` after a fed batch.

#### D-13 — Per-worker segment cache claim is weakly evidenced
- **Spec:** v0.41 proof: "segment cache hit ratio > 80% for hot-join workloads in benchmarks." DESIGN.md §5.4 / §3.14.
- **Code:** [`crates/rockstream-gateway/src/segment_cache.rs`](../crates/rockstream-gateway/src/segment_cache.rs) exists in the **gateway** crate but not in `rockstream-runtime` — yet cross-shard join lookups happen in workers, not in the gateway. No worker-side LRU keyed by `(shard_id, segment_id)`. No published benchmark of the 80% claim.
- **Severity:** Major.
- **Fix:** Move/duplicate the cache into worker-side `DbReader` access; publish a hit-ratio benchmark in `criterion/`.

#### D-14 — RMW-avoidance metric is not per-operator
- **Spec:** v0.27 proof: "per-law RMW-avoidance ratio published"; DESIGN.md §3.3.
- **Code:** `crates/rockstream-types/src/metrics.rs` keys RMW counters by `(law_id, law_name, law_version)` only; no `operator_id` / `instance_id` dimension.
- **Severity:** Major. A planner cannot diagnose which operator instance fails its RMW SLO.
- **Fix:** Extend `LawMetricKey` with `operator_id: Option<OperatorId>`; render in `EXPLAIN INCREMENTAL ANALYZE`.

#### D-15 — External gRPC connector protocol is absent
- **Spec:** v0.48 scope: "external gRPC connector protocol, SDK, examples".
- **Code:** No `.proto` file in tree; no gRPC server module; the only "SDK" is the in-process Rust trait shown in `example_sdk.rs`.
- **Severity:** Major; v0.48 scope bullet is unfulfilled.
- **Fix:** Either ship a minimal `proto/connector.proto` + `tonic` server stub or remove the bullet from the v0.48 sign-off.

#### D-16 — `discover_schema()` results never reach the planner
- **Spec:** v0.48 proof: "connector-declared CRDT columns round-trip through schema discovery and `EXPLAIN`".
- **Code:** `Source::discover_schema()` and `Sink::discover_schema()` exist with implementations in Iceberg/Example/Kafka connectors. `grep` for `discover_schema` in `rockstream-sql|runtime|plan|gateway` returns zero. The "round-trip" test calls `discover_schema` directly on the connector instance.
- **Severity:** Major.
- **Fix:** During SQL `CREATE SOURCE/SINK`, the planner should call `discover_schema()` and compare/merge with the user-declared schema, raising `RS-XXXX schema.connector_mismatch` on divergence; cache the result in the catalog.

#### D-17 — `WriteClassification` enum is defined and unused
- **Spec:** v0.48 scope: "extended `LawSchemaMetadata` includes write-classification fields … so external sources participate in the optimistic validation protocol without inventing a gateway-only path".
- **Code:** Defined in `connector.rs`; no gateway DML code reads it. Optimistic validation in `dml.rs` doesn't branch on classification.
- **Severity:** Major.
- **Fix:** Gateway DML path: if the affected column's source has `read_dependent_delta` classification, force a read-validation round; if `blind_delta`, skip the read check. Encode this in `OptimisticTransaction::validate`.

### 2.3 Minor drift / accidental complexity

- **D-18** — `rockstream.*` historical-alias deprecation path (v0.41 → remove in v0.50) is not visibly tested; add a CI assertion that aliasing emits the deprecation NOTICE.
- **D-19** — Sink `committed_epochs: Vec<Epoch>` / `aborted_epochs` grow unbounded in `iceberg_sink.rs`, `kafka_sink.rs`, `s3_sink.rs`, `postgres_sink.rs`, `http_sink.rs`. Should be ring buffers of bounded size.
- **D-20** — `crates/rockstream-connectors/src/generate_rows.rs` L203 `key.try_into().unwrap()` panics if a row key is not exactly 8 bytes — a real risk now that user-defined keys exist.
- **D-21** — Test/dev panic in `crates/rockstream-gateway/src/dml.rs` L518 (`panic!("expected OptimisticConflict, got {other:?}")`) is in test-mode but the surrounding module compiles into the non-test path; prefer `assert!`.
- **D-22** — `rockstream-sql` is a single 1-file crate; the SQL frontend's modularity will not survive v0.49 RBAC/auth integration without a deliberate restructure.
- **D-23** — `Cargo.lock` was last updated at v0.42.0 per commit history — confirm it is not stale w.r.t. v0.48 dep changes.
- **D-24** — `docs/connector-developer-guide.md` (v0.48 deliverable) does not document async/blocking-I/O expectations, watermark/clock-skew handling, partition-filter examples, or the external-gRPC protocol. Several Tier 2 features are documented as if they work end-to-end when in fact they don't (see D-04..D-07).

---

## 3. Deep-Dive Architectural Critique

### 3.1 IVM kernel mechanics

**What is good:**
- The `MergeLaw` / `LawBundle` abstraction is the right central primitive. It correctly factors algebraic structure out of operator code: storage gets `merge`/`get_merged`/`scan_merged`, operators get `identity`/`merge`/`combine`/`reduce`, exchange gets `combiner`, gateway gets `gateway_combiner`. This is the project's best architectural decision and the one feature most worth defending.
- Arrangement headers carry `(law_id, law_version)`, enabling rolling law upgrades.
- The `not_merge_safe_reason` closed enum makes the boundary between merge-safe and non-merge-safe operators explicit and machine-checkable.
- Z-set discipline (the `_weight` convention, `EpochOutput`, `DiffCtx`) is uniformly applied; differentiation passes appear correct.

**What is weak:**
- **Write amplification on cardinality-explosion aggregates** is unbounded because of D-11. A `GROUP BY` over a high-cardinality key will balloon `agg_state` until the host OOMs.
- **Merge-read fallback is silent** (D-10): metric-only signaling violates the "fail closed" posture.
- **Per-operator metrics are aggregated by law, not by instance** (D-14), making SLO attribution impossible.
- The `last_emitted` cache in `AggregateMergeOp` is a duplication of arrangement state; consider deriving from arrangement via a snapshot reader rather than maintaining a parallel HashMap.
- Min/Max via "indexed multiset" looks correct but the prefix-scan delete path will perform poorly under hot-key churn; consider a segment-tree implementation matching the `0x02 0xST` key encoded in the repo memory.
- DRed for recursion was deliberately removed (v0.22 escape hatch). Document this in `IVM.md` more prominently — currently it's a footnote.

**Arrangement layout** (DESIGN.md §3.15) — claim: "row-oriented at rest, columnar in flight". Storage keys (e.g. `0x01 0xJL op_id join_key row_id → row_bytes`) confirm row-oriented at rest. Columnar in flight is via Arrow IPC in `crates/rockstream-types/src/arrow_batch.rs`; that part is real. The hybrid is fine but the **cross-shard read path** still serializes one row at a time through `DbReader::get`, which is the wrong access pattern for hot-key joins. Add a `DbReader::scan_arrow(prefix)` that materializes an Arrow batch directly from one or more SSTs without per-row Rust-level boxing.

### 3.2 Cloud-native topology

- Compute/storage separation is real: SlateDB sits behind an `ObjectStore` facade, and a `SimRuntime` test runs the whole stack against an in-memory store. This is excellent.
- The exchange path classifier (`elided / loopback / direct / durable`) is the right abstraction; combiners are properly law-driven (v0.30 cleanup of the SUM/COUNT/AVG allowlist is genuine — `crates/rockstream-runtime/src/exchange/combiner.rs` reads `MergeLawId` from plan annotations).
- The segment-cache story (D-13) is the weakest cloud-native claim and should be the v0.49–v0.50 priority.
- WAL listing cache (`crates/rockstream-storage/src/wal_cache.rs`) genuinely avoids the documented expensive `list()` cost. Good.
- Worker-level multiplexing is honored; credit backpressure exists (`exchange/credit.rs`).

**Topology gaps:**
- No first-class cold-tier read path (Iceberg-as-source via gateway query). Scheduled v0.58 but the substrate doesn't exist yet.
- `DbReader` cross-shard reads pin to a "published checkpoint" but I see no test confirming the checkpoint is actually pinned (vs. read-latest). A determinism test is needed.

### 3.3 Memory, async, resource bounding

- `tokio` is used throughout; `async_trait` on `Operator`/`Source`/`Sink`. Acceptable.
- No grepped instances of `std::sync::Mutex` held across `.await` boundaries in operator code — clean on that axis.
- Frontier reporting uses bounded `tokio::sync::mpsc` — good.
- Loopback exchange path is bounded. Good.
- The `Vec` accumulation of `committed_epochs` / `aborted_epochs` in sinks (D-19) is unbounded.
- The `HashMap` operator state (D-11) is unbounded.
- `merge_law_fallback_total` increments but there's no rate-limit / circuit breaker — a corrupted segment will spam the metric every read until something else trips.

### 3.4 Concurrency correctness

- Per-shard writer exclusivity *is* documented to lean on SlateDB's manifest fencing (per repo memory and `crates/rockstream-storage/src/shard_db.rs`). Worth adding an in-tree integration test that opens the same shard from two `ShardDb` handles and asserts the second `write()` fences.
- The recovery driver (`crates/rockstream-runtime/src/recovery.rs`) and the lease-grant rate limiter (v0.35) appear to exist. A specific failure I'd write a SimRuntime test for: simultaneous control-plane partition + worker-led self-fence — verify no shard double-leases.
- Checkpoint barrier injection is in `crates/rockstream-runtime/src/checkpoint.rs`; alignment buffers are claimed bounded. Verify the bound is configurable and the metric exists.

---

## 4. Ergonomics & API Friction

### 4.1 `Source` / `Sink` trait surface (Tier 1 + Tier 2 in one trait)

Both traits now have 8–11 methods, most with silent default delegations (e.g. `start_snapshot → poll_batch`, `poll_delta → poll_batch`). This is **the single worst ergonomics regression of v0.48**. A connector author looking at the trait cannot tell which methods are *required* vs *delegated* vs *purely advisory*.

**Recommendation:** Split:

```rust
pub trait Source: Send {                 // Tier 1, ~3 methods
    fn name(&self) -> &str;
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch>;
    fn credits_available(&self) -> usize { usize::MAX }
}

pub trait PartitionPushdownSource: Source {     // Tier 2 opt-in
    async fn start_snapshot(&mut self, filter: Option<PartitionFilter>, epoch: Epoch)
        -> Option<SourceBatch>;
    async fn poll_delta(&mut self, filter: Option<PartitionFilter>, epoch: Epoch)
        -> Option<SourceBatch>;
}

pub trait SchemaAwareSource: Source {
    fn discover_schema(&self) -> LawSchemaMetadata;
}

pub trait LifecycleSource: Source {
    async fn pause(&mut self) -> bool;
    async fn resume(&mut self) -> bool;
    async fn delete(&mut self) -> bool;
    fn lifecycle_state(&self) -> ConnectorLifecycleState;
}
```

Wire the runtime via `&dyn Source` + `Option<&dyn PartitionPushdownSource>` downcast (or use an enum of capabilities at registration time). This forces the runtime to *explicitly handle each capability* rather than inheriting a silent default.

### 4.2 Storage `Result<Option<Bytes>>`

[`crates/rockstream-storage/src/shard_db.rs`](../crates/rockstream-storage/src/shard_db.rs) returns `Result<Option<Bytes>, StorageError>` everywhere. Every caller writes `match db.get(k).await? { Some(v) => …, None => … }`. Consider a `Lookup<T>` enum (`Found(T) | NotFound | Error(StorageError)`) or commit to `Result<Bytes, StorageError>` with a distinguished `StorageError::NotFound`. Pick one and apply uniformly.

### 4.3 Gateway errors lack `next_steps`

DESIGN.md §14.14 promises every error has a `next_steps` remediation string, enforced in CI. [`crates/rockstream-gateway/src/error.rs`](../crates/rockstream-gateway/src/error.rs) has none. Scheduled v0.50 but the trait should land *now* so v0.49 errors don't compound the debt:

```rust
pub trait UserFacingError {
    fn code(&self) -> ErrorCode;
    fn message(&self) -> String;
    fn next_steps(&self) -> &'static str;
}
```

CI assertion: every `RS-XXXX` registered code must have a non-empty `next_steps`.

### 4.4 No unified `ClusterConfig` / `WorkerConfig` / `ConnectorConfig`

Knobs like `min_epoch_ms`, `segment_cache_bytes`, `checkpoint_retention_count`, `state_budget_gb`, `max_rows_per_quantum`, etc. are scattered as per-module defaults. Auto-tuner (v0.51) and resource-usage visibility (v0.50) need a single config surface. **Define now**, even if mostly defaulted:

```rust
pub struct ClusterConfig {
    pub epoch: EpochConfig,           // min_ms, max_ms, min_bytes
    pub checkpoint: CheckpointConfig, // retention_count, retention_duration
    pub state_budget: StateBudgetConfig,
    pub exchange: ExchangeConfig,
    pub workload_defaults: WorkloadConfig,
}
```

Serialize from `rockstream.toml`; document in `docs/configuration.md` (does not yet exist).

### 4.5 CLI is split / scattered

The binary supports `start`, `sql`, `explain`. Spec mentions `rockstream debug arrangement`, `rockstream support-bundle`, `rockstream describe`, etc. Centralize via `clap` subcommand structure now (low cost) — the surface will grow rapidly in v0.49–v0.55.

### 4.6 `Operator` trait has no metric/config hook

[`crates/rockstream-ops/src/operator.rs`](../crates/rockstream-ops/src/operator.rs) — `async fn process(&mut self, input) -> Output;` and nothing else. v0.50 observability and v0.51 auto-tuner need:

```rust
fn snapshot_metrics(&self) -> OperatorMetrics;
fn reconfigure(&mut self, hints: OperatorHints) -> ReconfigOutcome;
fn state_bytes(&self) -> u64;
```

### 4.7 Connector developer guide overpromises

[`docs/connector-developer-guide.md`](../docs/connector-developer-guide.md) documents `should_flush`, `partition_filter`, lifecycle, schema discovery as production-ready. Until D-04…D-07 are fixed, the guide is misleading. Add a "Capability Status" matrix to the top of the doc.

---

## 5. Quality & Correctness Hotspots (module-by-module)

### `rockstream-runtime/src/exchange/durable.rs`
- **L146, 152, 172, 174, 176** — `try_into().unwrap()` in `decode_object`. **Replace immediately.** Add fuzz tests.
- L342, L358 — `unwrap()` inside test fixtures is fine; flag with `#[cfg(test)]` for clarity.

### `rockstream-runtime/src/pipeline.rs`
- The epoch driver does not respect `Sink::should_flush` (D-05) or `Source::partition_filter_support` (D-06).
- No `Source::lifecycle_state` check before `poll_batch` — a paused connector is still polled.

### `rockstream-storage/src/shard_db.rs`
- `validate_law_catalog` `try_into().unwrap()` on header bytes (per IVM subagent walkthrough at L350). Add a `const_assert!(ArrangementHeader::WIRE_SIZE == 4)` and a fallback `?`.
- Merge-read fallback (D-10) needs a posture decision: hard-fail or wrap-and-propagate. The current behavior is the worst of both.

### `rockstream-ops/src/aggregate.rs` (and `min_max.rs`, `join.rs`, `top_k.rs`, `distinct.rs`)
- `agg_state: HashMap<Vec<u8>, Vec<u8>>` is unbounded (D-11).
- `MapOp` in `map.rs` uses `.expect("DataFusion map expression evaluation failed")` — a malformed UDF crashes the whole operator. Should route to DLQ.
- Several operator `process` methods are large; consider splitting into `consume_delta` / `emit_changes` to localize state mutation.

### `rockstream-gateway/src/rockstream_catalog.rs`
- D-01: every catalog function (except `catalog_merge_laws`) is stubbed.

### `rockstream-gateway/src/dml.rs`
- D-08: gateway-local idempotency; missing per-shard durable table.
- L518: `panic!` in match arm — should use `assert!` or return a typed error.

### `rockstream-connectors/src/*`
- Iceberg/Kafka/S3/HTTP/Postgres: unbounded `committed_epochs` / `aborted_epochs` (D-19).
- `generate_rows.rs` L203 unwrap on key bytes (D-20).

### `rockstream-catalog/src/lib.rs`
- Workload DDL parses `FRESHNESS_SLO` / `MEMORY_LIMIT` and stores them, but no consumer (D-02).

### `rockstream-sql/src/lib.rs`
- Single 1-file crate; refactor into submodules (parser, binder, lowering, explain, ddl) before v0.49.

### `rockstream-types/src/metrics.rs`
- `LawMetricKey` needs `operator_id` dimension (D-14).

---

## 6. Strategic Horizon — Roadmap to v0.49 (prioritized checklist)

Before any v0.49 work begins, the team should write a *post-mortem* on the v0.48 sign-off, reopen the bullets that are stub-only, and execute the following in order. Each is sized as a concrete unit of work; numbers in parentheses are rough engineering days.

### Phase A — Correctness debts (must clear before v0.49 begins)

- [ ] **A-1 (1d).** Fix `decode_object` panics in `exchange/durable.rs` (D-09). Add fuzz tests for truncated/corrupt objects.
- [ ] **A-2 (1d).** Fix `validate_law_catalog` panic in `shard_db.rs`; add a `const_assert!` on header wire size.
- [ ] **A-3 (3d).** Decide merge-read fallback posture (D-10). Recommended: hard-fail with `RS-5003 law.operand_corruption`; add an explicit override flag for emergency recovery.
- [ ] **A-4 (5d).** Wire `StateBudget` into all stateful operators (D-11). Emit `RS-3604`/`OVER_BUDGET_RELAXED` on exhaustion. Add a property test that asserts no operator can exceed its budget.
- [ ] **A-5 (3d).** Bound `committed_epochs` / `aborted_epochs` in all sinks (D-19). Ring buffer of 1,024 entries; metric for fill level.
- [ ] **A-6 (1d).** Fix `generate_rows.rs` panic on non-8-byte keys (D-20).
- [ ] **A-7 (1d).** Remove the `panic!` in `dml.rs` L518; use `assert!`.

### Phase B — Sign-off reconciliation (rewrite v0.41/v0.44/v0.47/v0.48 sign-offs honestly)

- [ ] **B-1 (2d).** Mark `rockstream_catalog.*` stub functions with an `_unwired` suffix or have them return an `RS-XXXX experimental.catalog_unwired` warning. Update v0.41 sign-off to acknowledge.
- [ ] **B-2 (5d).** Implement real `catalog_epochs` and `catalog_shards` backed by control-plane state. (Audit log + DLQ defer to B-3 / B-5.)
- [ ] **B-3 (8d).** Implement persistent per-source DLQ sink (D-03): `0x0X DLQ source(16) seq(8) → record_bytes`. Wire every connector source's decode-error path. Implement `REPLAY` and `DISMISS` against this. Rewrite v0.47 sign-off proof.
- [ ] **B-4 (5d).** Implement per-shard idempotency-key table (D-08); add the v0.44 proof variant that survives a gateway restart.
- [ ] **B-5 (5d).** Implement `EXPLAIN TRANSACTION` SQL frontend + planner integration (D-04). Render `WriteClassification` from collected `LawSchemaMetadata`.
- [ ] **B-6 (5d).** Wire `Sink::should_flush` into pipeline driver (D-05); add a runtime-level Iceberg test that asserts file count over a 10-second window.
- [ ] **B-7 (5d).** Add planner derivation of `PartitionFilter` from WHERE predicates against partition columns (D-06); runtime dispatch to `start_snapshot(Some(f))`.
- [ ] **B-8 (5d).** Implement `ALTER SOURCE … PAUSE/RESUME` DDL, control-plane state-machine, audit emission; update Kafka/Postgres/S3/HTTP/Iceberg to honor lifecycle (D-07).

### Phase C — SLO & observability foundations (so v0.50 lands cleanly)

- [ ] **C-1 (8d).** Workload SLO enforcement (D-02): plumb `WorkloadId` → `OperatorContext`; epoch coordinator respects `freshness_slo_ms`; state budget enforcer charges against `memory_limit`. Emit `RS-5018` proactive notice.
- [ ] **C-2 (3d).** `LawMetricKey` adds `operator_id` (D-14); render in `EXPLAIN INCREMENTAL ANALYZE`.
- [ ] **C-3 (5d).** Implement `ControlService::collect_operator_stats` RPC and gateway-side `EXPLAIN INCREMENTAL ANALYZE` live round-trip (D-12).
- [ ] **C-4 (3d).** Move/duplicate segment cache into worker-side `DbReader` access path (D-13); publish hit-ratio benchmark.
- [ ] **C-5 (2d).** Introduce `UserFacingError` trait with `next_steps`; backfill across `GatewayError`, `StorageError`, `ConnectorError`. CI assertion: every `RS-XXXX` has non-empty `next_steps`.

### Phase D — Ergonomics restructure

- [ ] **D-1 (3d).** Split `Source`/`Sink` traits into Tier 1 + capability traits (`PartitionPushdownSource`, `SchemaAwareSource`, `LifecycleSource`, etc.). Runtime dispatches via capability detection.
- [ ] **D-2 (2d).** Introduce `ClusterConfig`/`WorkerConfig` and a `rockstream.toml` loader; document in `docs/configuration.md`.
- [ ] **D-3 (2d).** Refactor `rockstream-sql/src/lib.rs` into submodules (parser, binder, lowering, ddl, explain).
- [ ] **D-4 (2d).** Centralize CLI subcommand structure via `clap`; add `rockstream describe`, `rockstream debug arrangement` stubs.
- [ ] **D-5 (2d).** Add `Operator::snapshot_metrics()` / `reconfigure(hints)` / `state_bytes()`.

### Phase E — Documentation honesty

- [ ] **E-1 (1d).** Add a "Capability Status" matrix to `docs/connector-developer-guide.md` reflecting D-04…D-07 status.
- [ ] **E-2 (1d).** Rewrite v0.41, v0.44, v0.47, v0.48 sign-offs to reflect what is wired vs. defined.
- [ ] **E-3 (1d).** Add `docs/cli.md` and `docs/configuration.md`.

### Phase F — Decision: rename or fix the Integration Beta milestone

The v0.48 "Integration Beta" milestone is documented as "Postgres access, direct writes, and major external connectors work end to end". As of v0.48:
- Postgres read gateway works (pgwire, partial-agg pushdown).
- DML write path works in-process but does not survive gateway restart (D-08).
- Connectors are wired for the read/poll cycle but not for lifecycle, partition push-down, Tier 2 flush, schema discovery, or DLQ persistence (D-03..D-07).

**Two acceptable outcomes:**

1. **Rename v0.48 to "Integration Beta — Code Complete"** and add **v0.48.1 "Integration Beta — Wired"** as the actual milestone, with Phase A + Phase B as scope. Slip the Production Beta target.

2. **Roll the deltas above into v0.49** and rename v0.49 from "Auth, RBAC, secrets" to "Integration Beta hardening + Auth subset". Move the rest of auth/RBAC to v0.50.

I would recommend option (1) — it is more honest to the project's own stated philosophy ("evidence over dates", "correctness before scale", "a version is done only when its proof is done").

---

## 7. Closing Assessment

Rockstream at v0.48 is **structurally world-class and operationally early-beta**. The architectural ideas — law-driven IVM, SlateDB-on-object-store, planner-attached merge laws driving combiners, the exchange path classifier, the antichain frontier protocol, the explicit `not_merge_safe_reason` enum, the simulation discipline — are first-rate, and there is no reason this project cannot become *the* cloud-native IVM engine. The DESIGN.md is the most coherent IVM-system spec I have read outside of internal papers at the obvious vendors.

The blocker is not capability; it is integration discipline. The team has been writing more types and tests per version than the runtime can fully consume, and the sign-off process has not caught the gap between "the trait/type exists, with a test" and "the operational path uses it". The recommended remediation is small in absolute scope (roughly 50–60 engineering-days across Phases A–E) but requires the team to slow down on net-new scope for one version cycle and convert the existing spec surface into reality.

Do not proceed to v0.49 scope until Phase A (10 days) is clear and at least Phase B-1, B-3, B-6, B-8 are merged. Auth/RBAC on a substrate where the catalog returns synthetic data and the DLQ is a single hard-coded row will compound the credibility debt and make security review impossible.

The right next move is a v0.48.1 "wire what we shipped" release.

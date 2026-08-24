# RockStream Language Features

Canonical generated references: [functions](reference/functions.md) and [SQL support](reference/sql-support.md).

## Secret DDL — Tier: Maintain

```sql
CREATE SECRET kafka_auth (TYPE = 'sasl_plain', username = 'user', password = 'value');
ALTER SECRET kafka_auth SET (username = 'next-user', password = 'next-value');
SHOW SECRETS;
DROP SECRET kafka_auth;
```

Sources and sinks may use `secret = '<name>'`; inline credential values are
rejected and secret references are validated before catalog registration.

This inventory collects the SQL, DML, and DDL surface that appears across the current README, ROADMAP, concepts guide, connector guide, index-tuning guide, and the `rockstream-sql`/`rockstream-gateway` frontend crates.

Status legend:
- `implemented`: live and reachable via SQL over pgwire today.
- `documented/planned`: explicitly described in design or roadmap material, not yet reachable via SQL.
- `frontend gap`: the current lowering code still rejects the construct.

The v0.57 strategic tier is the compatibility contract:
- **Core** — release-gated and covered by a named proof test.
- **Maintain** — shipped, regression-tested, and secure, but not a growth area.
- **Experimental** — no continuity guarantee; future work must pass the admission rule.

The complete machine-readable source is [`capabilities.toml`](../capabilities.toml);
the generated [`capability matrix`](capability-matrix.md) is not hand-edited.

### Maintain compatibility and deprecation policy

Maintain features remain callable and preserve documented behavior. A removal or
incompatible change requires a roadmap entry, a migration note, an `RS-4017`
style failure contract where applicable, and one release of deprecation notice
before removal. Maintain features do not receive new syntax or semantics without
being admitted as a new Core or Experimental capability.

### Future-surface admission

Any new SQL family, connector, sink, protocol, or transactional semantic must
document user value for IVM, compatibility and security impact, boundedness and
failure behavior, a named proof plan, and an explicit tier before it enters a
roadmap version. The admission checklist in
[`ROCKSTREAM_PROJECT_FOCUS.md`](../ROCKSTREAM_PROJECT_FOCUS.md) §8 is the source
of that requirement.

> **Accuracy note (v0.45.5 full audit).** A 2026-07-11 usability review moved
> several previously-misclassified "Implemented Today" bullets to
> "Documented / Planned Surface". v0.45.5 completed the line-by-line audit
> that review scheduled: every remaining "Implemented Today" bullet was
> re-verified against `rockstream-sql`'s parser/lowering code
> (`crates/rockstream-sql/src`) and `rockstream-gateway`'s literal-prefix
> query dispatch (`crates/rockstream-gateway/src/server.rs`), which together
> are RockStream's entire SQL-reachable surface. This pass found substantially
> more drift than the prior sample check: a large number of bullets described
> Rust-API-level or data-model-level constructs (`rockstream-types`,
> `rockstream-connectors` trait methods, etc.) as if they were reachable SQL
> statements, when no corresponding literal-prefix dispatch or parser rule
> exists in `rockstream-gateway`/`rockstream-sql`. All such bullets have been
> moved to "Documented / Planned Surface" below, each annotated with the
> concrete evidence (or absence of evidence) found. Confirmed still-accurate:
> the multi-tenancy workload DDL/DML surface (`CREATE WORKLOAD`,
> `ALTER WORKLOAD`, `DROP WORKLOAD`, `SHOW WORKLOAD STATUS`, and the real
> `WITH WORKLOAD = '<name>'` view-assignment clause) and the
> `SHOW RESOURCE USAGE` family, both confirmed live in
> `crates/rockstream-gateway/src/server.rs`. The automated conformance test
> that locks this file's all-caps keyword claims against the real source
> going forward is `crates/rockstream-sql/tests/docs_conformance_tests.rs::
> test_language_features_doc_keywords_are_parseable` (mirrors the
> linked-proof-test pattern `docs/pgwire-conformance.md` already has); it is
> necessarily a heuristic substring check (it cannot execute every statement
> shape), so this accuracy note's human full-read audit remains the
> authoritative record for the corrections above.

## Implemented Today

- **Tier: Core** — Query and read surface: `SELECT` over base tables, materialized views, and inline views; `WHERE`, projection, aliases, table scans, subquery aliases, and view-on-view references. Proof: [`test_create_view_and_select`](../crates/rockstream-gateway/tests/gateway_dml_tests.rs).
- **Tier: Core** — Scalar expressions: column references, literals, arithmetic, comparisons, boolean operators, `CAST`, `TRY_CAST` (`crates/rockstream-sql/src/lower.rs` handles `DfExpr::Cast`/`DfExpr::TryCast`), `CASE`, `NOW()`, and interval arithmetic as shown in the concepts guide. Proof: [`tpch_q6_filter_aggregate_no_join`](../crates/rockstream-sql/tests/tpch_plans.rs).
- **Tier: Experimental** — Aggregation and algebraic maintenance: the supported integer-key/integer-value subset is proven by [`test_basic_int_sum_group_by`](../crates/rockstream-gateway/tests/serving_path_aggregate_matrix_tests.rs); the v0.57.1 type-matrix audit found unsupported floating-point and text-key combinations, so the broad aggregate claim is not release-gated.
- **Tier: Experimental** — Relational operators: inner, left, right, full outer, semi, anti, and cross joins; `UNION`, `INTERSECT`, `EXCEPT`, `DISTINCT`, and bag/set semantics. Floating-point join keys and text semi/anti joins remain unsupported in the v0.57.1 matrix.
- **Tier: Experimental** — Analytics and time: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, sliding `SUM`/`AVG`, `TUMBLE`, watermarks, event-time TTL, late-data policies (`drop`, `update`, `route_to_sink`), and watermark gating. Timestamp-key matrix cells remain unsupported; `NTILE` is *not* supported.
- **Tier: Core** — Recursion and graph-shaped plans: view-on-view DAGs with compile-time cycle detection. Proof: [`test_5_level_view_chain_convergence`](../crates/rockstream-sql/tests/lfs_catalog.rs).
- **Tier: Core** — Historical and streaming reads: `SUBSCRIBE <view> [AS OF NOW WITH SNAPSHOT | AS OF EPOCH <n>] [WHERE ...] [(<columns>)]` — see `crates/rockstream-gateway/src/subscribe_parser.rs`. `AS OF TIMESTAMP` and `AS OF MONOTONE PARTIAL` do **not** exist anywhere in `rockstream-gateway`; a prior revision of this doc incorrectly listed both alongside the two real `AS OF` forms. Proof: [`proof_subscribe_snapshot_then_deltas_lfs`](../crates/rockstream-gateway/tests/gateway_proof_tests.rs).
- **Tier: Core** — Session and freshness controls: read-your-writes, `SET rockstream.session_wait_for = off`, `SET rockstream.max_staleness = ...`, `rockstream.write_fence()`, and `rockstream.after_fence(...)`; the gateway also implements `READ COMMITTED` and `REPEATABLE READ` isolation levels (`crates/rockstream-gateway/src/session.rs`). Proof: [`write_fence_token_can_be_used_by_another_session_via_after_fence`](../crates/rockstream-gateway/tests/gateway_proof_tests.rs).
- **Tier: Core** — DML: `INSERT`, `UPDATE`, `DELETE`, and `INSERT ... RETURNING` over pgwire. Proof: [`test_update_accumulates_in_write_buffer`](../crates/rockstream-gateway/tests/gateway_dml_tests.rs).
- **Tier: Core** — Transaction semantics: client idempotency keys (`SET rockstream.idempotency_key = '<key>'`) and source-epoch write fencing (`SET rockstream.source_epoch = N`); missing either before `COMMIT` returns `RS-2007` (`crates/rockstream-gateway/src/error.rs`). Proof: [`explicit_idempotency_key_dedupes_multi_statement_transaction_replay`](../crates/rockstream-gateway/tests/gateway_proof_tests.rs).
- **Tier: Core** — Views: `CREATE VIEW` as an inline macro-expanded view, `CREATE MATERIALIZED VIEW`, `CREATE OR REPLACE VIEW`/`CREATE OR REPLACE MATERIALIZED VIEW`, and `REFRESH MATERIALIZED VIEW` (`crates/rockstream-gateway/src/server.rs`'s literal-prefix dispatch). Proof: [`test_refresh_materialized_view_roundtrip`](../crates/rockstream-gateway/tests/gateway_dml_tests.rs).
- **Tier: Maintain** — Multi-tenancy and quotas: `CREATE WORKLOAD <name> WITH (...)`, `ALTER WORKLOAD <name> SET (...)`, `DROP WORKLOAD <name>`, `SHOW WORKLOAD STATUS`, `SHOW WORKLOAD STATUS FOR <name>`, and assigning a view to a workload at creation time with `CREATE MATERIALIZED VIEW <name> WITH WORKLOAD = '<workload>' AS ...` (note: the assignment clause is `WITH WORKLOAD = '<name>'`, not a parenthesized `WITH (WORKLOAD = ...)`).
- **Tier: Maintain** — Resource visibility: `SHOW RESOURCE USAGE`, `SHOW RESOURCE USAGE FOR WORKLOAD <name>`, and `SHOW CLUSTER RESOURCE USAGE`, backed by `rockstream_catalog.view_resource_usage`/`workload_resource_usage` and reachable via the gateway's real query dispatch (`crates/rockstream-gateway/src/catalog_stubs.rs`, `crates/rockstream-gateway/src/server.rs`).
- **Tier: Maintain** — Removed connectors: Iceberg, Delta, object-store sinks, S3 sources, and HTTP webhook sources fail closed with `RS-4017`. Their replacements are listed in [connector migration](connector-migration.md).
- **Tier: Maintain** — Indexes: `CREATE INDEX`, `CREATE INDEX ... WHERE ...`, `DROP INDEX`, `REBUILD INDEX`, and `MARK INDEX` (`crates/rockstream-sql/src/frontend.rs`'s `DdlStatement`, dispatched by `crates/rockstream-gateway/src/server.rs`). Multi-column composite index point lookups and prefix scans are accelerated via `IndexArrangement`. There is no separate `EXPLAIN INDEX` statement — index state (`RS-2014`/`RS-2015`) is surfaced as an annotation inside the generic `EXPLAIN <query>` output instead; see "Current Frontend Gaps".
- **Tier: Maintain** — Query diagnostics: `EXPLAIN <query>` (with index-state and sink-target annotations) and `EXPLAIN INCREMENTAL [VERBOSE | ANALYZE | ESTIMATE] <query>` (`crates/rockstream-gateway/src/server.rs`).
- **Tier: Maintain** — System catalog SQL: `rockstream_catalog.view_resource_usage` and `rockstream_catalog.workload_resource_usage` (`crates/rockstream-gateway/src/catalog_stubs.rs`). No other `rockstream_catalog.*` table (`epochs`, `pipelines`, `shards`, `merge_laws`, `audit_log`, `dead_letter_queue`) and no `rockstream.*` alias are wired to SQL today; see "Documented / Planned Surface".

## Documented / Planned Surface

- **Tier: Experimental** — Time windows: `HOP` and `SESSION` were explicitly deferred after `TUMBLE`.
- **Tier: Experimental** — Custom algebra: `CREATE MERGE LAW` is planned behind a feature flag with a property-test gate, alongside the user-visible CRDT column types it would back (`CREATE TABLE ... (amount COUNTER)`, `MAX_REGISTER`, `MIN_REGISTER`, `LWW`, `OR_SET`, `MV_REGISTER`) — both are explicit post-1.0 goals per `README.md` and `NEW_IMPLEMENTATION_PLAN.md`'s "Out of Scope" list, not current-roadmap items.
- **Tier: Experimental** — Recursion: `WITH RECURSIVE`, monotone insert-only recursion with `complete_through`, semi-naive maintenance, and DRed rejection for non-monotone terms are `NEW_ROADMAP.md` v0.50 scope (Phase 15), not yet implemented — `rockstream-sql` has no `WITH RECURSIVE` support today.
- **Tier: Maintain** — Multi-tenancy and secrets: `CREATE SECRET`/envelope encryption/KEK configuration are v0.55.1 scope (`env` and `aws_kms` backends only; `gcp_kms`/`vault` are deferred by decision); `ALTER SCHEMA/NAMESPACE ... SET DEFAULT WORKLOAD` (schema/namespace-level default workload assignment, distinct from the per-view `WITH WORKLOAD = '<name>'` assignment clause, which is implemented) remains unimplemented; none exist in `rockstream-sql`/`rockstream-gateway` today.
- **Tier: Experimental** — Session hints: a `SELECT /*+ ALLOW_STALE */ ...` optimizer-hint comment syntax is described in earlier design notes but has no corresponding parsing anywhere in `crates/rockstream-gateway/src` — no `/*+ ... */` hint-comment handling exists at all today. Staleness control is only reachable via the real `SET rockstream.max_staleness = ...` session variable (see "Implemented Today").
- **Tier: Experimental** — Views — replacement workflow: `CREATE REPLACEMENT MATERIALIZED VIEW`/`CREATE REPLACEMENT VIEW`, `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`, `ALTER MATERIALIZED VIEW ... DISCARD REPLACEMENT`, and `SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW` do not exist anywhere in `crates/rockstream-gateway/src` or `crates/rockstream-sql/src` — no `replacement`-related dispatch of any kind was found. A prior revision of this doc listed these as implemented; corrected in v0.45.5.
- **Tier: Experimental** — Workload/view lifecycle visibility: `PAUSE`/`RESUME` at the materialized-view or schema level, `SHOW VIEW STATUS FOR NAMESPACE`, and `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW` do not exist anywhere in `crates/rockstream-gateway/src/server.rs`'s dispatch table. A prior revision of this doc listed these as implemented; corrected in v0.45.5.
- **Tier: Experimental** — Source lifecycle DDL: `CREATE SOURCE`, `PAUSE SOURCE`, `RESUME SOURCE`, `DROP SOURCE`, and `GENERATE ROWS` are not reachable via any SQL statement — `crates/rockstream-connectors`'s `SourceConnector` trait has `pause()`/`resume()` methods, but these are plain Rust APIs invoked directly by tests/operators, not dispatched from parsed SQL anywhere in `crates/rockstream-gateway/src`. A prior revision of this doc listed a `CREATE SOURCE`/`PAUSE SOURCE`/`RESUME SOURCE`/`DROP SOURCE`/`GENERATE ROWS` SQL surface as implemented; corrected in v0.45.5.
- **Tier: Experimental** — Dead-letter queue operations: `ALTER SOURCE ... REPLAY DEAD_LETTER_QUEUE [SINCE ... UNTIL ...]` and `ALTER SOURCE ... DISMISS DEAD_LETTER_QUEUE WHERE ...` do not exist anywhere in `crates/rockstream-gateway/src`/`crates/rockstream-sql/src`; neither does a `rockstream_catalog.dead_letter_queue` SQL-queryable table (the DLQ exists as a Rust data model in `rockstream-types::dlq`, not as SQL surface). A prior revision of this doc listed both as implemented; corrected in v0.45.5.
- **Tier: Maintain** — Connector schema contracts: `discover_schema()`, `LawSchemaMetadata`, `partition_filter` pushdown, `should_flush(bytes_buffered, epochs_buffered)`, and schema validation that rejects mismatched CRDT columns are real `rockstream-connectors` Rust trait/API surface, but are connector-development contracts, not SQL statements a client issues over pgwire; listed here rather than under "Implemented Today" to avoid implying SQL reachability.
- **Tier: Experimental** — Write classification: `EXPLAIN TRANSACTION` and the `blind_delta`/`read_dependent_delta`/`exact_key_guarded_delta`/`source_exactly_once_protected` classification labels do not exist anywhere in `crates/rockstream-gateway/src`. A prior revision of this doc listed this as implemented; corrected in v0.45.5.
- **Tier: Experimental** — Schema evolution introspection: `SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA` and `SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW` do not exist anywhere in `crates/rockstream-gateway/src`'s dispatch table (`rockstream_types::schema_evolution` is a data model only). A prior revision of this doc listed these as implemented; corrected in v0.45.5.
- **Tier: Experimental** — DDL coordination: `SET BACKGROUND_DDL = ON`, `WAIT FOR MATERIALIZED VIEW ... TO BE READY TIMEOUT ...`, and `WITHOUT CONFIRMATION` do not exist anywhere in `crates/rockstream-gateway/src`. A prior revision of this doc listed these as implemented; corrected in v0.45.5.
- **Tier: Experimental** — `LATERAL` subqueries/functions: `rockstream-plan`'s `PlanNode::Lateral` type exists and is threaded through dependency-collection/rewrite passes, but no `LogicalPlan` match arm in `crates/rockstream-sql/src/lower.rs`'s main `lower()` function ever constructs a fresh `PlanNode::Lateral` from parsed SQL — nothing was found wiring `LATERAL` SQL syntax to plan construction. A prior revision of this doc listed `LATERAL` as implemented; corrected in v0.45.5.
- **Tier: Maintain** — Resource visibility (historical note): `SHOW RESOURCE USAGE`, `SHOW RESOURCE USAGE FOR WORKLOAD`, and `SHOW CLUSTER RESOURCE USAGE` are implemented today — see "Implemented Today" above (moved out of this section in v0.45.5; previously listed here as "not yet reachable via SQL", which was stale).
- **Tier: Experimental** — Optimistic transactions: `SERIALIZABLE LOCAL`, exact-key guarded writes, and optimistic conflict detection (`RS-2008`) — validated against per-row versions, explicitly **non-CRDT** — are **deferred by decision** and no longer scheduled: the 2026-08-11 strategic rebaseline removed the version that carried them, because broader transactional semantics move RockStream toward being an OLTP database rather than an IVM system (see `NEW_ROADMAP.md`'s "Deferred by decision" table and [ROCKSTREAM_PROJECT_FOCUS.md](../ROCKSTREAM_PROJECT_FOCUS.md) §6). *(Accuracy correction, 2026-07-11 architecture review: an earlier revision of this bullet invented a "CRDT-only transaction envelope" behind an `--experimental-optimistic-crdt-transactions` flag that appears in neither `NEW_ROADMAP.md` nor `DESIGN.md` and directly contradicts `NEW_IMPLEMENTATION_PLAN.md`'s Out-of-Scope ban on user-visible CRDT surface; removed.)*
- **Tier: Experimental** — Broader production SQL surface: the v0.57 v1-contract work assigns every statement a strategic tier (`Core`/`Maintain`/`Experimental`), v0.57.1 documents each `Core` operator's incremental/backfill/recovery/state-growth/failure semantics, and names the explicit Postgres differences, transaction/isolation semantics, and connector/sink guarantees that v1 commits to; the v0.58.3 sweep and the v0.59 RC gate then verify them.
- **Tier: Experimental** — Connector expansion: Kafka and Postgres CDC remain supported. Removed connector migrations are documented in [connector migration](connector-migration.md).
- **Tier: Experimental** — View retention and historical behavior: the concepts guide also documents per-view `retention` and `CHANGE_RETENTION` knobs as part of the historical-read and subscribe story; neither is wired to any SQL statement in `crates/rockstream-gateway/src`/`crates/rockstream-sql/src` today.

## Current Frontend Gaps

- **Tier: Experimental** — The current `rockstream-sql` lowering code still rejects `LIMIT` and `SORT` / `ORDER BY` logical-plan nodes explicitly (`crates/rockstream-sql/src/lower.rs`'s `lower()` has no match arm for `LogicalPlan::Limit`/`LogicalPlan::Sort`, so both fall through to the catch-all `UnsupportedPlanNode` error). `DISTINCT` (`SELECT DISTINCT ...`, `LogicalPlan::Distinct(Distinct::All(...))`) *is* implemented — routed to `PlanNode::Distinct` — and was incorrectly listed as a gap in a prior revision of this doc; only `DISTINCT ON (...)` (a PostgreSQL extension) remains unsupported.
- **Tier: Experimental** — `EXPLAIN INDEX` as a distinct statement does not exist; index state is surfaced only as an annotation inside the generic `EXPLAIN <query>` output (`RS-2014`/`RS-2015` for building/lagging indexes). A prior revision of this doc listed `EXPLAIN INDEX` as its own implemented statement; corrected in v0.45.5.
- **Tier: Experimental** — Some uncommon expressions and operators still bubble out as `NotYetImplemented`; the current lowering path only covers the expression and aggregate forms exercised by the SQL Alpha and IVM test corpus.

<!-- BEGIN GENERATED CORE SEMANTICS -->
## Core semantic ledger

This block is generated from the five-behavior ledger in `capabilities.toml`.

| Capability | Behavior | Statement | Proof | Paired proof | Bound | Metric | Bound outcome |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `language.dml` | `incremental` | INSERT, UPDATE, and DELETE deltas change the materialized result exactly once. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_dml_incremental` | — | — | — | — |
| `language.dml` | `backfill` | DML backfill starts from the committed table snapshot before applying mutations. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_dml_backfill` | — | — | — | — |
| `language.dml` | `checkpoint_recovery` | Reopening the durable shard restores all committed DML effects. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_dml_checkpoint_recovery` | — | — | — | — |
| `language.dml` | `state_growth` | The write buffer is bounded, exposes fill level, and cannot accumulate indefinitely. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_dml_state_growth` | — | configured write-buffer budget | write_buffer_fill_ratio | the write is rejected with its RS-coded resource error |
| `language.dml` | `failure` | Invalid DML returns an actionable RS-coded error and preserves the prior committed result. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_dml_failure` | — | — | — | — |
| `language.historical-streaming-reads` | `incremental` | SUBSCRIBE emits the snapshot followed by each committed delta in order. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_subscribe_incremental` | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_subscribe_pgwire_dispatch` | — | — | — |
| `language.historical-streaming-reads` | `backfill` | A historical subscription starts from the requested committed snapshot before streaming deltas. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_subscribe_backfill` | — | — | — | — |
| `language.historical-streaming-reads` | `checkpoint_recovery` | Reconnecting a subscription resumes from durable committed state without duplicate rows. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_subscribe_checkpoint_recovery` | — | — | — | — |
| `language.historical-streaming-reads` | `state_growth` | Subscription output uses a bounded delivery window and reports fill level. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_subscribe_state_growth` | — | configured subscription delivery window | subscription_buffer_fill_ratio | delivery applies backpressure or returns its RS-coded resource error |
| `language.historical-streaming-reads` | `failure` | An invalid subscription target returns an actionable RS-coded error. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_subscribe_failure` | — | — | — | — |
| `language.query-read` | `incremental` | A committed row delta is reflected in the same SELECT transcript as the corresponding source change. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_query_read_incremental` | — | — | — | — |
| `language.query-read` | `backfill` | A snapshot read returns every committed source row before subsequent deltas are observed. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_query_read_backfill` | — | — | — | — |
| `language.query-read` | `checkpoint_recovery` | Reopening the local durable shard restores the committed read transcript. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_query_read_checkpoint_recovery` | — | — | — | — |
| `language.query-read` | `state_growth` | Read results are materialized per query and do not retain an unbounded process buffer. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_query_read_state_growth` | — | one query result set | query_result_rows | the query fails with its RS-coded resource error |
| `language.query-read` | `failure` | Invalid read input returns an actionable RS-coded error instead of an empty success. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_query_read_failure` | — | — | — | — |
| `language.scalar-expressions` | `incremental` | Scalar expressions are recomputed for each changed input row and preserve the exact projected transcript. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_scalar_incremental` | — | — | — | — |
| `language.scalar-expressions` | `backfill` | Backfill evaluates the scalar expression over the complete snapshot before deltas. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_scalar_backfill` | — | — | — | — |
| `language.scalar-expressions` | `checkpoint_recovery` | A reopened shard preserves the scalar result of committed rows. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_scalar_checkpoint_recovery` | — | — | — | — |
| `language.scalar-expressions` | `state_growth` | Scalar evaluation retains no input-sized queue between queries. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_scalar_state_growth` | — | one query result set | query_result_rows | the query fails with its RS-coded resource error |
| `language.scalar-expressions` | `failure` | Unsupported scalar input returns an actionable RS-coded error. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_scalar_failure` | — | — | — | — |
| `language.session-freshness` | `incremental` | Freshness controls make committed writes visible according to the session fence. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_freshness_incremental` | — | — | — | — |
| `language.session-freshness` | `backfill` | A session fence observes the complete committed snapshot at the requested frontier. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_freshness_backfill` | — | — | — | — |
| `language.session-freshness` | `checkpoint_recovery` | Freshness tokens remain valid after reopening the durable session state. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_freshness_checkpoint_recovery` | — | — | — | — |
| `language.session-freshness` | `state_growth` | Session freshness retains only bounded fence metadata and reports its fill level. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_freshness_state_growth` | — | configured fence-token budget | fence_token_fill_ratio | new fence creation returns its RS-coded resource error |
| `language.session-freshness` | `failure` | An invalid freshness setting returns an actionable RS-coded error. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_freshness_failure` | — | — | — | — |
| `language.transaction-semantics` | `incremental` | A committed transaction applies its complete statement set atomically to the result transcript. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_transaction_incremental` | — | — | — | — |
| `language.transaction-semantics` | `backfill` | Transaction backfill observes only the committed snapshot and excludes uncommitted statements. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_transaction_backfill` | — | — | — | — |
| `language.transaction-semantics` | `checkpoint_recovery` | A committed transaction remains durable and replay-safe after restart. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_transaction_checkpoint_recovery` | — | — | — | — |
| `language.transaction-semantics` | `state_growth` | Transaction statements are bounded by the configured write-buffer budget and expose fill level. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_transaction_state_growth` | — | configured transaction write-buffer budget | transaction_buffer_fill_ratio | the transaction is rejected with its RS-coded resource error |
| `language.transaction-semantics` | `failure` | A failed or replayed transaction returns an actionable RS-coded error or exact no-op. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_transaction_failure` | — | — | — | — |
| `language.view-on-view-dag` | `incremental` | View-on-view deltas propagate through the dependency DAG once and preserve the downstream transcript. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_view_dag_incremental` | — | — | — | — |
| `language.view-on-view-dag` | `backfill` | DAG backfill materializes dependencies in topological order before downstream deltas. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_view_dag_backfill` | — | — | — | — |
| `language.view-on-view-dag` | `checkpoint_recovery` | A reopened DAG restores committed upstream and downstream view state. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_view_dag_checkpoint_recovery` | — | — | — | — |
| `language.view-on-view-dag` | `state_growth` | DAG compilation and dependency state are bounded by the configured view budget. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_view_dag_state_growth` | — | configured view dependency budget | view_dependency_fill_ratio | the view operation is rejected with its RS-coded resource error |
| `language.view-on-view-dag` | `failure` | A cycle or invalid dependency returns an actionable RS-coded error. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_view_dag_failure` | — | — | — | — |
| `language.views` | `incremental` | View maintenance publishes each source delta to the materialized view transcript. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_views_incremental` | — | — | — | — |
| `language.views` | `backfill` | View creation backfills from the committed source snapshot before later deltas. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_views_backfill` | — | — | — | — |
| `language.views` | `checkpoint_recovery` | A reopened materialized view restores its committed output. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_views_checkpoint_recovery` | — | — | — | — |
| `language.views` | `state_growth` | View maintenance is bounded by the configured arrangement budget and exposes fill level. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_views_state_growth` | — | configured view arrangement budget | view_state_fill_ratio | the write is rejected with its RS-coded resource error |
| `language.views` | `failure` | Invalid view definitions return an actionable RS-coded error and do not publish partial state. | `crates/rockstream-gateway/tests/core_operator_semantics_e2e_tests.rs::core_views_failure` | — | — | — | — |

<!-- END GENERATED CORE SEMANTICS -->

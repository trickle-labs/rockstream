# RockStream Language Features

This inventory collects the SQL, DML, and DDL surface that appears across the current README, ROADMAP, concepts guide, connector guide, index-tuning guide, and the `rockstream-sql`/`rockstream-gateway` frontend crates.

Status legend:
- `implemented`: live and reachable via SQL over pgwire today.
- `documented/planned`: explicitly described in design or roadmap material, not yet reachable via SQL.
- `frontend gap`: the current lowering code still rejects the construct.

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

- Query and read surface: `SELECT` over base tables, materialized views, and inline views; `WHERE`, projection, aliases, table scans, subquery aliases, and view-on-view references.
- Scalar expressions: column references, literals, arithmetic, comparisons, boolean operators, `CAST`, `TRY_CAST` (`crates/rockstream-sql/src/lower.rs` handles `DfExpr::Cast`/`DfExpr::TryCast`), `CASE`, `NOW()`, and interval arithmetic as shown in the concepts guide.
- Aggregation and algebraic maintenance: `COUNT`, `COUNT(*)`, `SUM`, `AVG`/`MEAN`, `MIN`, `MAX`, algebraic merge-law propagation, and CRDT-backed aggregate state (`WeightAdd/v1`, `SumCount/v1`, `MaxRegister/v1`, `MinRegister/v1`).
- Relational operators: inner, left, right, full outer, semi, anti, and cross joins; `UNION`, `INTERSECT`, `EXCEPT`, `DISTINCT`, and bag/set semantics.
- Analytics and time: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, sliding `SUM`/`AVG`, `TUMBLE`, watermarks, event-time TTL, late-data policies (`drop`, `update`, `route_to_sink`), and watermark gating. `NTILE` is *not* supported — `crates/rockstream-sql/src/lower.rs` explicitly returns `RS-1016` (`UnsupportedWindowFunction`) for it; a prior revision of this doc incorrectly listed it as implemented.
- Recursion and graph-shaped plans: view-on-view DAGs with compile-time cycle detection.
- Historical and streaming reads: `SUBSCRIBE <view> [AS OF NOW WITH SNAPSHOT | AS OF EPOCH <n>] [WHERE ...] [(<columns>)]` — see `crates/rockstream-gateway/src/subscribe_parser.rs`. `AS OF TIMESTAMP` and `AS OF MONOTONE PARTIAL` do **not** exist anywhere in `rockstream-gateway`; a prior revision of this doc incorrectly listed both alongside the two real `AS OF` forms.
- Session and freshness controls: read-your-writes, `SET rockstream.session_wait_for = off`, `SET rockstream.max_staleness = ...`, `rockstream.write_fence()`, and `rockstream.after_fence(...)`; the gateway also implements `READ COMMITTED` and `REPEATABLE READ` isolation levels (`crates/rockstream-gateway/src/session.rs`).
- DML: `INSERT`, `UPDATE`, `DELETE`, and `INSERT ... RETURNING` over pgwire.
- Transaction semantics: client idempotency keys (`SET rockstream.idempotency_key = '<key>'`) and source-epoch write fencing (`SET rockstream.source_epoch = N`); missing either before `COMMIT` returns `RS-2007` (`crates/rockstream-gateway/src/error.rs`).
- Views: `CREATE VIEW` as an inline macro-expanded view, `CREATE MATERIALIZED VIEW`, `CREATE OR REPLACE VIEW`/`CREATE OR REPLACE MATERIALIZED VIEW`, and `REFRESH MATERIALIZED VIEW` (`crates/rockstream-gateway/src/server.rs`'s literal-prefix dispatch).
- Multi-tenancy and quotas: `CREATE WORKLOAD <name> WITH (...)`, `ALTER WORKLOAD <name> SET (...)`, `DROP WORKLOAD <name>`, `SHOW WORKLOAD STATUS`, `SHOW WORKLOAD STATUS FOR <name>`, and assigning a view to a workload at creation time with `CREATE MATERIALIZED VIEW <name> WITH WORKLOAD = '<workload>' AS ...` (note: the assignment clause is `WITH WORKLOAD = '<name>'`, not a parenthesized `WITH (WORKLOAD = ...)`).
- Resource visibility: `SHOW RESOURCE USAGE`, `SHOW RESOURCE USAGE FOR WORKLOAD <name>`, and `SHOW CLUSTER RESOURCE USAGE`, backed by `rockstream_catalog.view_resource_usage`/`workload_resource_usage` and reachable via the gateway's real query dispatch (`crates/rockstream-gateway/src/catalog_stubs.rs`, `crates/rockstream-gateway/src/server.rs`).
- Removed connectors: Iceberg, Delta, object-store sinks, S3 sources, and HTTP webhook sources fail closed with `RS-4017`. Their replacements are listed in [connector migration](connector-migration.md).
- Indexes: `CREATE INDEX`, `CREATE INDEX ... WHERE ...`, `DROP INDEX`, `REBUILD INDEX`, and `MARK INDEX` (`crates/rockstream-sql/src/frontend.rs`'s `DdlStatement`, dispatched by `crates/rockstream-gateway/src/server.rs`). Multi-column composite index point lookups and prefix scans are accelerated via `IndexArrangement`. There is no separate `EXPLAIN INDEX` statement — index state (`RS-2014`/`RS-2015`) is surfaced as an annotation inside the generic `EXPLAIN <query>` output instead; see "Current Frontend Gaps".
- Query diagnostics: `EXPLAIN <query>` (with index-state and sink-target annotations) and `EXPLAIN INCREMENTAL [VERBOSE | ANALYZE | ESTIMATE] <query>` (`crates/rockstream-gateway/src/server.rs`).
- System catalog SQL: `rockstream_catalog.view_resource_usage` and `rockstream_catalog.workload_resource_usage` (`crates/rockstream-gateway/src/catalog_stubs.rs`). No other `rockstream_catalog.*` table (`epochs`, `pipelines`, `shards`, `merge_laws`, `audit_log`, `dead_letter_queue`) and no `rockstream.*` alias are wired to SQL today; see "Documented / Planned Surface".

## Documented / Planned Surface

- Time windows: `HOP` and `SESSION` were explicitly deferred after `TUMBLE`.
- Custom algebra: `CREATE MERGE LAW` is planned behind a feature flag with a property-test gate, alongside the user-visible CRDT column types it would back (`CREATE TABLE ... (amount COUNTER)`, `MAX_REGISTER`, `MIN_REGISTER`, `LWW`, `OR_SET`, `MV_REGISTER`) — both are explicit post-1.0 goals per `README.md` and `NEW_IMPLEMENTATION_PLAN.md`'s "Out of Scope" list, not current-roadmap items.
- Recursion: `WITH RECURSIVE`, monotone insert-only recursion with `complete_through`, semi-naive maintenance, and DRed rejection for non-monotone terms are `NEW_ROADMAP.md` v0.50 scope (Phase 15), not yet implemented — `rockstream-sql` has no `WITH RECURSIVE` support today.
- Multi-tenancy and secrets: `CREATE SECRET`/envelope encryption/KEK configuration are v0.55.1 scope (`env` and `aws_kms` backends only; `gcp_kms`/`vault` are deferred by decision); `ALTER SCHEMA/NAMESPACE ... SET DEFAULT WORKLOAD` (schema/namespace-level default workload assignment, distinct from the per-view `WITH WORKLOAD = '<name>'` assignment clause, which is implemented) remains unimplemented; none exist in `rockstream-sql`/`rockstream-gateway` today.
- Session hints: a `SELECT /*+ ALLOW_STALE */ ...` optimizer-hint comment syntax is described in earlier design notes but has no corresponding parsing anywhere in `crates/rockstream-gateway/src` — no `/*+ ... */` hint-comment handling exists at all today. Staleness control is only reachable via the real `SET rockstream.max_staleness = ...` session variable (see "Implemented Today").
- Views — replacement workflow: `CREATE REPLACEMENT MATERIALIZED VIEW`/`CREATE REPLACEMENT VIEW`, `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`, `ALTER MATERIALIZED VIEW ... DISCARD REPLACEMENT`, and `SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW` do not exist anywhere in `crates/rockstream-gateway/src` or `crates/rockstream-sql/src` — no `replacement`-related dispatch of any kind was found. A prior revision of this doc listed these as implemented; corrected in v0.45.5.
- Workload/view lifecycle visibility: `PAUSE`/`RESUME` at the materialized-view or schema level, `SHOW VIEW STATUS FOR NAMESPACE`, and `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW` do not exist anywhere in `crates/rockstream-gateway/src/server.rs`'s dispatch table. A prior revision of this doc listed these as implemented; corrected in v0.45.5.
- Source lifecycle DDL: `CREATE SOURCE`, `PAUSE SOURCE`, `RESUME SOURCE`, `DROP SOURCE`, and `GENERATE ROWS` are not reachable via any SQL statement — `crates/rockstream-connectors`'s `SourceConnector` trait has `pause()`/`resume()` methods, but these are plain Rust APIs invoked directly by tests/operators, not dispatched from parsed SQL anywhere in `crates/rockstream-gateway/src`. A prior revision of this doc listed a `CREATE SOURCE`/`PAUSE SOURCE`/`RESUME SOURCE`/`DROP SOURCE`/`GENERATE ROWS` SQL surface as implemented; corrected in v0.45.5.
- Dead-letter queue operations: `ALTER SOURCE ... REPLAY DEAD_LETTER_QUEUE [SINCE ... UNTIL ...]` and `ALTER SOURCE ... DISMISS DEAD_LETTER_QUEUE WHERE ...` do not exist anywhere in `crates/rockstream-gateway/src`/`crates/rockstream-sql/src`; neither does a `rockstream_catalog.dead_letter_queue` SQL-queryable table (the DLQ exists as a Rust data model in `rockstream-types::dlq`, not as SQL surface). A prior revision of this doc listed both as implemented; corrected in v0.45.5.
- Connector schema contracts: `discover_schema()`, `LawSchemaMetadata`, `partition_filter` pushdown, `should_flush(bytes_buffered, epochs_buffered)`, and schema validation that rejects mismatched CRDT columns are real `rockstream-connectors` Rust trait/API surface, but are connector-development contracts, not SQL statements a client issues over pgwire; listed here rather than under "Implemented Today" to avoid implying SQL reachability.
- Write classification: `EXPLAIN TRANSACTION` and the `blind_delta`/`read_dependent_delta`/`exact_key_guarded_delta`/`source_exactly_once_protected` classification labels do not exist anywhere in `crates/rockstream-gateway/src`. A prior revision of this doc listed this as implemented; corrected in v0.45.5.
- Schema evolution introspection: `SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA` and `SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW` do not exist anywhere in `crates/rockstream-gateway/src`'s dispatch table (`rockstream_types::schema_evolution` is a data model only). A prior revision of this doc listed these as implemented; corrected in v0.45.5.
- DDL coordination: `SET BACKGROUND_DDL = ON`, `WAIT FOR MATERIALIZED VIEW ... TO BE READY TIMEOUT ...`, and `WITHOUT CONFIRMATION` do not exist anywhere in `crates/rockstream-gateway/src`. A prior revision of this doc listed these as implemented; corrected in v0.45.5.
- `LATERAL` subqueries/functions: `rockstream-plan`'s `PlanNode::Lateral` type exists and is threaded through dependency-collection/rewrite passes, but no `LogicalPlan` match arm in `crates/rockstream-sql/src/lower.rs`'s main `lower()` function ever constructs a fresh `PlanNode::Lateral` from parsed SQL — nothing was found wiring `LATERAL` SQL syntax to plan construction. A prior revision of this doc listed `LATERAL` as implemented; corrected in v0.45.5.
- Resource visibility (historical note): `SHOW RESOURCE USAGE`, `SHOW RESOURCE USAGE FOR WORKLOAD`, and `SHOW CLUSTER RESOURCE USAGE` are implemented today — see "Implemented Today" above (moved out of this section in v0.45.5; previously listed here as "not yet reachable via SQL", which was stale).
- Optimistic transactions: `SERIALIZABLE LOCAL`, exact-key guarded writes, and optimistic conflict detection (`RS-2008`) — validated against per-row versions, explicitly **non-CRDT** — are **deferred by decision** and no longer scheduled: the 2026-08-11 strategic rebaseline removed the version that carried them, because broader transactional semantics move RockStream toward being an OLTP database rather than an IVM system (see `NEW_ROADMAP.md`'s "Deferred by decision" table and [ROCKSTREAM_PROJECT_FOCUS.md](../ROCKSTREAM_PROJECT_FOCUS.md) §6). *(Accuracy correction, 2026-07-11 architecture review: an earlier revision of this bullet invented a "CRDT-only transaction envelope" behind an `--experimental-optimistic-crdt-transactions` flag that appears in neither `NEW_ROADMAP.md` nor `DESIGN.md` and directly contradicts `NEW_IMPLEMENTATION_PLAN.md`'s Out-of-Scope ban on user-visible CRDT surface; removed.)*
- Broader production SQL surface: the v0.57 v1-contract work assigns every statement a strategic tier (`Core`/`Maintain`/`Experimental`), v0.57.1 documents each `Core` operator's incremental/backfill/recovery/state-growth/failure semantics, and names the explicit Postgres differences, transaction/isolation semantics, and connector/sink guarantees that v1 commits to; the v0.58.3 sweep and the v0.59 RC gate then verify them.
- Connector expansion: Kafka and Postgres CDC remain supported. Removed connector migrations are documented in [connector migration](connector-migration.md).
- View retention and historical behavior: the concepts guide also documents per-view `retention` and `CHANGE_RETENTION` knobs as part of the historical-read and subscribe story; neither is wired to any SQL statement in `crates/rockstream-gateway/src`/`crates/rockstream-sql/src` today.

## Current Frontend Gaps

- The current `rockstream-sql` lowering code still rejects `LIMIT` and `SORT` / `ORDER BY` logical-plan nodes explicitly (`crates/rockstream-sql/src/lower.rs`'s `lower()` has no match arm for `LogicalPlan::Limit`/`LogicalPlan::Sort`, so both fall through to the catch-all `UnsupportedPlanNode` error). `DISTINCT` (`SELECT DISTINCT ...`, `LogicalPlan::Distinct(Distinct::All(...))`) *is* implemented — routed to `PlanNode::Distinct` — and was incorrectly listed as a gap in a prior revision of this doc; only `DISTINCT ON (...)` (a PostgreSQL extension) remains unsupported.
- `EXPLAIN INDEX` as a distinct statement does not exist; index state is surfaced only as an annotation inside the generic `EXPLAIN <query>` output (`RS-2014`/`RS-2015` for building/lagging indexes). A prior revision of this doc listed `EXPLAIN INDEX` as its own implemented statement; corrected in v0.45.5.
- Some uncommon expressions and operators still bubble out as `NotYetImplemented`; the current lowering path only covers the expression and aggregate forms exercised by the SQL Alpha and IVM test corpus.

@@
# RockStream Language Features

This inventory collects the SQL, DML, and DDL surface that appears across the current README, ROADMAP, concepts guide, connector guide, index-tuning guide, and the `rockstream-sql` frontend crate.

Status legend:
- `implemented`: live in the current code/docs path.
- `documented/planned`: explicitly described in design or roadmap material.
- `frontend gap`: the current lowering code still rejects the construct.

> **Accuracy note (2026-07-11 usability review).** Several bullets below
> previously claimed statements as "Implemented Today" that do not exist
> anywhere in `rockstream-sql`'s parser/lowering code or `rockstream-gateway`
> (verified by grepping the actual crates, which is the same standard
> `NEW_ROADMAP.md`'s own review passes hold every other document to). Those
> have been moved to "Documented / Planned Surface" below. A full line-by-line
> audit of this entire file, plus an automated test that checks every
> "Implemented Today" claim against the real parser (mirroring the linked-proof-
> test pattern `docs/pgwire-conformance.md` already has), is scheduled at
> `NEW_ROADMAP.md` v0.45.5.

## Implemented Today

- Query and read surface: `SELECT` over base tables, materialized views, inline views, and `rockstream_catalog.*` system tables; `WHERE`, projection, aliases, table scans, subquery aliases, and view-on-view references.
- Scalar expressions: column references, literals, arithmetic, comparisons, boolean operators, `CAST`, `TRY_CAST`, `CASE`, `NOW()`, and interval arithmetic as shown in the concepts guide.
- Aggregation and algebraic maintenance: `COUNT`, `COUNT(*)`, `SUM`, `AVG`/`MEAN`, `MIN`, `MAX`, algebraic merge-law propagation, and CRDT-backed aggregate state (`WeightAdd/v1`, `SumCount/v1`, `MaxRegister/v1`, `MinRegister/v1`).
- Relational operators: inner, left, right, full outer, semi, anti, and cross joins; `UNION`, `INTERSECT`, `EXCEPT`, `DISTINCT`, and bag/set semantics; `LATERAL` subqueries and functions.
- Analytics and time: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`, sliding `SUM`/`AVG`, `TUMBLE`, watermarks, event-time TTL, late-data policies (`drop`, `update`, `route_to_sink`), watermark gating, and top-K.
- Recursion and graph-shaped plans: view-on-view DAGs with compile-time cycle detection.
- Historical and streaming reads: `AS OF EPOCH`, `AS OF TIMESTAMP`, `AS OF NOW WITH SNAPSHOT`, `AS OF MONOTONE PARTIAL`, `SUBSCRIBE`, server-side `WHERE`, column projection, and `CHANGE_RETENTION`.
- Session and freshness controls: read-your-writes, `SET rockstream.session_wait_for = off`, `SELECT /*+ ALLOW_STALE */ ...`, `SET rockstream.max_staleness = ...`, `rockstream.write_fence()`, and `rockstream.after_fence(...)`; the gateway also documents `READ COMMITTED` and `REPEATABLE READ` read semantics.
- DML: `INSERT`, `UPDATE`, `DELETE`, and `INSERT ... RETURNING` over pgwire.
- Transaction semantics: client idempotency keys and write-fence tokens; missing idempotency keys return `RS-2007`.
- Write classification: `EXPLAIN TRANSACTION` surfaces `blind_delta`, `read_dependent_delta`, `exact_key_guarded_delta`, and `source_exactly_once_protected`.
- Views: `CREATE VIEW` as an inline macro-expanded view, `CREATE MATERIALIZED VIEW`, `DROP VIEW`, `CREATE REPLACEMENT MATERIALIZED VIEW` / `CREATE REPLACEMENT VIEW`, `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`, `ALTER MATERIALIZED VIEW ... DISCARD REPLACEMENT`, and `SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW`.
- Workloads and freshness: `PAUSE` / `RESUME` at the materialized-view or schema level; `SHOW VIEW STATUS FOR NAMESPACE`; `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW`.
- Source lifecycle: `CREATE SOURCE`, `PAUSE SOURCE`, `RESUME SOURCE`, `DROP SOURCE`, `GENERATE ROWS`, watermark gating, and connector-driven resumption from durable offsets.
- Sink lifecycle: `CREATE SINK`, file-format sinks, and cold-tier lakehouse sinks — `CREATE SINK <name> FOR VIEW <view> TO ICEBERG|DELTA '<path>' WITH (snapshot_interval_epochs=..., snapshot_interval_ms=..., parquet_row_group_bytes=..., format_version=..., partition_by=ARRAY[...], catalog=filesystem|glue|rest|hive|ducklake)`, e.g. `CREATE SINK orders_cold FOR VIEW orders_view TO ICEBERG 's3://warehouse/orders' WITH (snapshot_interval_epochs=100, partition_by=ARRAY['region'], catalog=filesystem)`. See `docs/cold-tier-sinks.md` for the full option surface, retention/GC defaults, and catalog-backend matrix.
- Dead-letter queue operations: `rockstream_catalog.dead_letter_queue`, `ALTER SOURCE ... REPLAY DEAD_LETTER_QUEUE [SINCE ... UNTIL ...]`, and `ALTER SOURCE ... DISMISS DEAD_LETTER_QUEUE WHERE ...`.
- Connector schema contracts: `discover_schema()`, `LawSchemaMetadata`, `partition_filter` pushdown, `should_flush(bytes_buffered, epochs_buffered)`, and schema validation that rejects mismatched CRDT columns such as `CREATE SOURCE ... (amount COUNTER)` when the connector expects a different type.
- Indexes: `CREATE INDEX`, `CREATE INDEX ... WHERE ...`, `DROP INDEX`, `REBUILD INDEX`, and `EXPLAIN INDEX`.
- Schema evolution introspection: `SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA` and `SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW`.
- DDL coordination: `SET BACKGROUND_DDL = ON`, `WAIT FOR MATERIALIZED VIEW ... TO BE READY TIMEOUT ...`, and `WITHOUT CONFIRMATION` for expensive `CREATE MATERIALIZED VIEW` backfills.
- Query diagnostics: `EXPLAIN INCREMENTAL`, `EXPLAIN INCREMENTAL ESTIMATE`, `EXPLAIN INCREMENTAL VERBOSE`, `EXPLAIN INCREMENTAL ANALYZE`, `EXPLAIN TRANSACTION`, and `EXPLAIN INDEX`.
- System catalog SQL: `rockstream_catalog.epochs`, `pipelines`, `shards`, `merge_laws`, `audit_log`, `dead_letter_queue`, plus the read-only `rockstream.*` alias through v0.49.

## Documented / Planned Surface

- Time windows: `HOP` and `SESSION` were explicitly deferred after `TUMBLE`.
- Custom algebra: `CREATE MERGE LAW` is planned behind a feature flag with a property-test gate, alongside the user-visible CRDT column types it would back (`CREATE TABLE ... (amount COUNTER)`, `MAX_REGISTER`, `MIN_REGISTER`, `LWW`, `OR_SET`, `MV_REGISTER`) — both are explicit post-1.0 goals per `README.md` and `NEW_IMPLEMENTATION_PLAN.md`'s "Out of Scope" list, not current-roadmap items.
- Recursion: `WITH RECURSIVE`, monotone insert-only recursion with `complete_through`, semi-naive maintenance, and DRed rejection for non-monotone terms are `NEW_ROADMAP.md` v0.50 scope (Phase 15), not yet implemented — `rockstream-sql` has no `WITH RECURSIVE` support today.
- Multi-tenancy and secrets: `CREATE WORKLOAD` (`FRESHNESS_SLO`/`MEMORY_LIMIT`/`PRIORITY`), `WITH (WORKLOAD = ...)`, `ALTER SCHEMA/NAMESPACE ... SET DEFAULT WORKLOAD` are v0.45.1 scope; `CREATE SECRET`/envelope encryption/KEK configuration are v0.56 scope; none exist in `rockstream-sql` today (the `rockstream-types::workload` data model exists but nothing parses or admits it yet).
- Resource visibility: `SHOW RESOURCE USAGE`, `SHOW RESOURCE USAGE FOR WORKLOAD`, and `SHOW CLUSTER RESOURCE USAGE`, backed by new `rockstream_catalog.view_resource_usage`/`workload_resource_usage` tables, are v0.45 scope, currently being wired into the gateway's real query dispatch — not yet reachable via SQL.
- Optimistic transactions: `SERIALIZABLE LOCAL`, exact-key guarded writes, and optimistic conflict detection (`RS-2008`) — validated against per-row versions, explicitly **non-CRDT** per `NEW_ROADMAP.md` v0.54's actual scope — are v0.54 scope. *(Accuracy correction, 2026-07-11 architecture review: an earlier revision of this bullet invented a "CRDT-only transaction envelope" behind an `--experimental-optimistic-crdt-transactions` flag that appears in neither `NEW_ROADMAP.md` nor `DESIGN.md` and directly contradicts `NEW_IMPLEMENTATION_PLAN.md`'s Out-of-Scope ban on user-visible CRDT surface; removed.)*
- Broader production SQL surface: the v0.59 compatibility work calls out a stable SQL subset, explicit Postgres differences, transaction/isolation semantics, and connector/sink guarantees.
- Connector expansion: Kafka, Postgres CDC, S3, HTTP push/webhook, Iceberg, and Delta Lake connectors and sinks remain part of the documented roadmap surface even where the current frontend only exposes the connector contract.
- View retention and historical behavior: the concepts guide also documents per-view `retention` and `CHANGE_RETENTION` knobs as part of the historical-read and subscribe story.

## Current Frontend Gaps

- The current `rockstream-sql` lowering code still rejects `LIMIT`, `SORT` / `ORDER BY`, and `DISTINCT` logical-plan nodes explicitly.
- Some uncommon expressions and operators still bubble out as `NotYetImplemented`; the current lowering path only covers the expression and aggregate forms exercised by the SQL Alpha and IVM test corpus.
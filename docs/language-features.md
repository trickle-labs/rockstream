@@
# RockStream Language Features

This inventory collects the SQL, DML, and DDL surface that appears across the current README, ROADMAP, concepts guide, connector guide, index-tuning guide, and the `rockstream-sql` frontend crate.

Status legend:
- `implemented`: live in the current code/docs path.
- `documented/planned`: explicitly described in design or roadmap material.
- `frontend gap`: the current lowering code still rejects the construct.

## Implemented Today

- Query and read surface: `SELECT` over base tables, materialized views, inline views, and `rockstream_catalog.*` system tables; `WHERE`, projection, aliases, table scans, subquery aliases, and view-on-view references.
- Scalar expressions: column references, literals, arithmetic, comparisons, boolean operators, `CAST`, `TRY_CAST`, `CASE`, `NOW()`, and interval arithmetic as shown in the concepts guide.
- Aggregation and algebraic maintenance: `COUNT`, `COUNT(*)`, `SUM`, `AVG`/`MEAN`, `MIN`, `MAX`, algebraic merge-law propagation, and CRDT-backed aggregate state (`WeightAdd/v1`, `SumCount/v1`, `MaxRegister/v1`, `MinRegister/v1`).
- Relational operators: inner, left, right, full outer, semi, anti, and cross joins; `UNION`, `INTERSECT`, `EXCEPT`, `DISTINCT`, and bag/set semantics; `LATERAL` subqueries and functions.
- Analytics and time: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `NTILE`, sliding `SUM`/`AVG`, `TUMBLE`, watermarks, event-time TTL, late-data policies (`drop`, `update`, `route_to_sink`), watermark gating, and top-K.
- Recursion and graph-shaped plans: recursive queries, monotone insert-only recursion with `complete_through`, semi-naive maintenance, DRed rejection for non-monotone terms, and view-on-view DAGs.
- Historical and streaming reads: `AS OF EPOCH`, `AS OF TIMESTAMP`, `AS OF NOW WITH SNAPSHOT`, `AS OF MONOTONE PARTIAL`, `SUBSCRIBE`, server-side `WHERE`, column projection, and `CHANGE_RETENTION`.
- Session and freshness controls: read-your-writes, `SET rockstream.session_wait_for = off`, `SELECT /*+ ALLOW_STALE */ ...`, `SET rockstream.max_staleness = ...`, `rockstream.write_fence()`, and `rockstream.after_fence(...)`; the gateway also documents `READ COMMITTED` and `REPEATABLE READ` read semantics.
- DML: `INSERT`, `UPDATE`, `DELETE`, and `INSERT ... RETURNING` over pgwire.
- Transaction semantics: optimistic conflict detection, client idempotency keys, and write-fence tokens; missing idempotency keys return `RS-2007`, and optimistic conflicts return `RS-2008`.
- Write classification: `EXPLAIN TRANSACTION` surfaces `blind_delta`, `read_dependent_delta`, `exact_key_guarded_delta`, and `source_exactly_once_protected`.
- Views: `CREATE VIEW` as an inline macro-expanded view, `CREATE MATERIALIZED VIEW`, `DROP VIEW`, `CREATE REPLACEMENT MATERIALIZED VIEW` / `CREATE REPLACEMENT VIEW`, `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`, `ALTER MATERIALIZED VIEW ... DISCARD REPLACEMENT`, and `SHOW REPLACEMENT STATUS FOR MATERIALIZED VIEW`.
- Workloads and freshness: `CREATE WORKLOAD` with `FRESHNESS_SLO`, `MEMORY_LIMIT`, and `PRIORITY`; `WITH (WORKLOAD = ...)` on materialized views; `ALTER SCHEMA ... SET DEFAULT WORKLOAD` / `ALTER NAMESPACE ... SET DEFAULT WORKLOAD`; `PAUSE` / `RESUME` at the materialized-view or schema level; `SHOW VIEW STATUS FOR NAMESPACE`; `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW`.
- Source lifecycle: `CREATE SOURCE`, `PAUSE SOURCE`, `RESUME SOURCE`, `DROP SOURCE`, `GENERATE ROWS`, watermark gating, and connector-driven resumption from durable offsets.
- Sink lifecycle: `CREATE SINK`, file-format sinks, and the documented path toward Iceberg and Delta Lake sinks.
- Dead-letter queue operations: `rockstream_catalog.dead_letter_queue`, `ALTER SOURCE ... REPLAY DEAD_LETTER_QUEUE [SINCE ... UNTIL ...]`, and `ALTER SOURCE ... DISMISS DEAD_LETTER_QUEUE WHERE ...`.
- Connector schema contracts: `discover_schema()`, `LawSchemaMetadata`, `partition_filter` pushdown, `should_flush(bytes_buffered, epochs_buffered)`, and schema validation that rejects mismatched CRDT columns such as `CREATE SOURCE ... (amount COUNTER)` when the connector expects a different type.
- CRDT table types: `CREATE TABLE ... (amount COUNTER)`, plus `MAX_REGISTER`, `MIN_REGISTER`, `LWW`, `OR_SET`, and `MV_REGISTER`.
- Indexes: `CREATE INDEX`, `CREATE INDEX ... WHERE ...`, `DROP INDEX`, `REBUILD INDEX`, and `EXPLAIN INDEX`.
- Secrets and security: `CREATE SECRET`, secret envelope encryption, KEK configuration, and worker-side token resolution.
- Schema and resource introspection: `SHOW RESOURCE USAGE`, `SHOW RESOURCE USAGE FOR WORKLOAD`, `SHOW CLUSTER RESOURCE USAGE`, `SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA`, and `SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW`.
- DDL coordination: `SET BACKGROUND_DDL = ON`, `WAIT FOR MATERIALIZED VIEW ... TO BE READY TIMEOUT ...`, and `WITHOUT CONFIRMATION` for expensive `CREATE MATERIALIZED VIEW` backfills.
- Query diagnostics: `EXPLAIN INCREMENTAL`, `EXPLAIN INCREMENTAL ESTIMATE`, `EXPLAIN INCREMENTAL VERBOSE`, `EXPLAIN INCREMENTAL ANALYZE`, `EXPLAIN TRANSACTION`, and `EXPLAIN INDEX`.
- System catalog SQL: `rockstream_catalog.epochs`, `pipelines`, `shards`, `merge_laws`, `audit_log`, `dead_letter_queue`, `view_resource_usage`, `workload_resource_usage`, plus the read-only `rockstream.*` alias through v0.48.

## Documented / Planned Surface

- Time windows: `HOP` and `SESSION` were explicitly deferred after `TUMBLE`.
- Custom algebra: `CREATE MERGE LAW` is planned behind a feature flag with a property-test gate.
- Experimental transactions: `SERIALIZABLE LOCAL`, exact-key guarded writes, and a CRDT-only transaction envelope are planned behind `--experimental-optimistic-crdt-transactions`.
- Cold tier and lakehouse sinks: `CREATE SINK ... TO ICEBERG`, Delta Lake output, Parquet data files, manifest/metadata commits, and finalized CRDT values in cold snapshots are roadmap items.
- Catalog backends: `filesystem`, `glue`, `rest`, `hive`, and `ducklake` registration backends, plus the native Iceberg REST catalog server.
- Broader production SQL surface: the v0.59 compatibility work calls out a stable SQL subset, explicit Postgres differences, transaction/isolation semantics, and connector/sink guarantees.
- Connector expansion: Kafka, Postgres CDC, S3, HTTP push/webhook, Iceberg, and Delta Lake connectors and sinks remain part of the documented roadmap surface even where the current frontend only exposes the connector contract.
- View retention and historical behavior: the concepts guide also documents per-view `retention` and `CHANGE_RETENTION` knobs as part of the historical-read and subscribe story.

## Current Frontend Gaps

- The current `rockstream-sql` lowering code still rejects `LIMIT`, `SORT` / `ORDER BY`, and `DISTINCT` logical-plan nodes explicitly.
- Some uncommon expressions and operators still bubble out as `NotYetImplemented`; the current lowering path only covers the expression and aggregate forms exercised by the SQL Alpha and IVM test corpus.
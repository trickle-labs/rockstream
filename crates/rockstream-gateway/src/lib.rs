//! Query gateway service for RockStream (v0.42).
//!
//! Serves the pgwire protocol for reading views with Postgres-compatible
//! clients (psql, SQLAlchemy, JDBC).
//!
//! # v0.40 deliverables
//!
//! - [`pgwire`] — pgwire startup/query/extended-query message types, Postgres
//!   OID constants, row descriptions, and isolation-level parsing.
//! - [`inline_view`] — `CREATE VIEW` inline view catalog with macro expansion;
//!   `DROP VIEW` rejects drops with dependent materialized views (RS-2004).
//! - [`pool`] — fixed-capacity connection pool with fail-fast exhaustion
//!   semantics.
//! - [`limits`] — query wall-clock timeouts (RS-2002) and per-connection
//!   rate limiting (RS-2005).
//! - [`pg_catalog`] — `pg_catalog.pg_type`, `pg_catalog.pg_namespace`,
//!   `information_schema.tables`, and `information_schema.columns` stubs for
//!   ORM schema reflection.
//!
//! # v0.41 deliverables
//!
//! - [`partial_agg`] — `LawBundle::gateway_combiner`-driven cross-shard
//!   partial aggregation pushdown (DESIGN.md §12.3.1, §6.11); `SumCount/v1`
//!   and `WeightAdd/v1` override `gateway_combiner` so GROUP BY aggregates
//!   receive O(groups) rows from shards rather than O(view rows).
//! - [`rockstream_catalog`] — `rockstream_catalog.*` system schema virtual
//!   tables: `merge_laws`, `epochs`, `pipelines`, `shards`, `audit_log`
//!   (DESIGN.md §12.6.1); legacy `rockstream.*` prefix alias accepted through
//!   the 0.45 release.
//!
//! # v0.42 deliverables
//!
//! - [`freshness`] — read-your-writes session (`ReadYourWritesSession`),
//!   snapshot isolation modes (`IsolationMode`), and `wait_for` semantics.
//! - [`subscribe`] — SUBSCRIBE cursor with `AS OF NOW WITH SNAPSHOT`,
//!   server-side row predicates, durable cursor resume after gateway restart.
//! - [`historical`] — `AS OF EPOCH`, `AS OF TIMESTAMP`, and
//!   `AS OF MONOTONE PARTIAL` query execution; retention window enforcement
//!   with RS-2006.
//!
//! # v0.43 deliverables
//!
//! - [`dml`] — INSERT, UPDATE, DELETE, `INSERT ... RETURNING` DML statement
//!   types; [`dml::OptimisticTransaction`] with epoch-based conflict detection
//!   returning RS-2008 on write-write conflicts.
//! - [`max_staleness`] — session max-staleness check and Postgres NOTICE
//!   formatting; emits a NOTICE when a session's snapshot epoch lags the
//!   committed frontier by more than the configured threshold.

pub mod dml;
pub mod error;
pub mod freshness;
pub mod historical;
pub mod index_scan;
pub mod inline_view;
pub mod limits;
pub mod max_staleness;
pub mod partial_agg;
pub mod pg_catalog;
pub mod pgwire;
pub mod pool;
pub mod rockstream_catalog;
pub mod subscribe;

pub use dml::{
    CommittedWrite, DmlResult, DmlStatement, OptimisticTransaction, WriteKind, WriteSetEntry,
};
pub use error::GatewayError;
pub use freshness::{
    FreshnessToken, IsolationMode, ReadYourWritesSession, WaitForConfig, WaitForOutcome,
};
pub use historical::{
    check_retention, execute_historical_query, execute_monotone_partial, find_epoch_for_timestamp,
    oldest_retained_epoch, EpochTimestampEntry, HistoricalAsOf, HistoricalQueryResult,
    HistoricalRow, MonotonePartialResult, RetentionConfig,
};
pub use index_scan::{charge_index_budget, check_index_status, select_scan_path, ScanPath};
pub use inline_view::InlineViewCatalog;
pub use limits::{check_timeout, QueryTimeoutConfig, RateLimitConfig, RateLimiter};
pub use max_staleness::{check_staleness, format_notice, MaxStalenessConfig, StalenessStatus};
pub use partial_agg::{
    build_partial_agg_plan, combine_partial_results, explain_partial_agg, CombinedAggRow,
    PartialAggExplainRow, PartialAggPlan, PartialAggRow,
};
pub use pg_catalog::{
    information_schema_columns, information_schema_tables, pg_namespaces, pg_types, ColumnSpec,
};
pub use pgwire::{
    map_to_postgres_oid, parse_isolation_level, IsolationLevel, PgColumn, PgExtendedQuery,
    PgQueryMessage, PgRowDescription, PgStartupMessage, PostgresOid,
};
pub use pool::{ConnectionPool, ConnectionPoolConfig, PooledConnection};
pub use rockstream_catalog::{
    catalog_audit_log, catalog_epochs, catalog_indexes, catalog_merge_laws, catalog_pipelines,
    catalog_shards, resolve_catalog_alias, resolve_catalog_table, set_live_indexes,
    CatalogAuditEntry, CatalogEpoch, CatalogIndex, CatalogMergeLaw, CatalogPipeline, CatalogShard,
    PipelineStatus, ShardHealth,
};
pub use subscribe::{
    simulate_subscribe_batch, ChangeRetentionConfig, SubscribeAsOf, SubscribeBatch,
    SubscribeCursor, SubscribeOptions, SubscribePredicate, SubscribeRow,
};

#[cfg(test)]
mod tests {
    #[test]
    fn gateway_crate_compiles() {}
}

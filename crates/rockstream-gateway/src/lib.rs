//! Query gateway service for RockStream (v0.41).
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
//!   v0.45.
//! - [`segment_cache`] — per-worker arrangement segment cache keyed by
//!   `(shard_id, segment_id)` with LRU eviction and hit-ratio tracking
//!   (DESIGN.md §5.4).

pub mod error;
pub mod inline_view;
pub mod limits;
pub mod partial_agg;
pub mod pg_catalog;
pub mod pgwire;
pub mod pool;
pub mod rockstream_catalog;
pub mod segment_cache;

pub use error::GatewayError;
pub use inline_view::InlineViewCatalog;
pub use limits::{check_timeout, QueryTimeoutConfig, RateLimitConfig, RateLimiter};
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
    catalog_audit_log, catalog_epochs, catalog_merge_laws, catalog_pipelines, catalog_shards,
    resolve_catalog_alias, resolve_catalog_table, CatalogAuditEntry, CatalogEpoch, CatalogMergeLaw,
    CatalogPipeline, CatalogShard, PipelineStatus, ShardHealth,
};
pub use segment_cache::{SegmentCache, SegmentCacheConfig, SegmentCacheStats, ShardSegmentKey};

#[cfg(test)]
mod tests {
    #[test]
    fn gateway_crate_compiles() {}
}

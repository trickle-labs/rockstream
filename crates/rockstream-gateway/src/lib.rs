//! Query gateway service for RockStream (v0.40).
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

pub mod error;
pub mod inline_view;
pub mod limits;
pub mod pg_catalog;
pub mod pgwire;
pub mod pool;

pub use error::GatewayError;
pub use inline_view::InlineViewCatalog;
pub use limits::{check_timeout, QueryTimeoutConfig, RateLimitConfig, RateLimiter};
pub use pg_catalog::{
    information_schema_columns, information_schema_tables, pg_namespaces, pg_types, ColumnSpec,
};
pub use pgwire::{
    map_to_postgres_oid, parse_isolation_level, IsolationLevel, PgColumn, PgExtendedQuery,
    PgQueryMessage, PgRowDescription, PgStartupMessage, PostgresOid,
};
pub use pool::{ConnectionPool, ConnectionPoolConfig, PooledConnection};

#[cfg(test)]
mod tests {
    #[test]
    fn gateway_crate_compiles() {}
}

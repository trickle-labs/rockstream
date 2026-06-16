//! PostgreSQL wire gateway service for RockStream (v0.23).
//!
//! Serves reads of maintained views to Postgres-compatible clients (`psql`,
//! SQLAlchemy, JDBC) via the pgwire protocol.
//!
//! # Modules
//!
//! - `server`         — `GatewayServer` and `GatewayHandler`
//! - `catalog_stubs`  — `pg_catalog` + `information_schema` stub responders
//! - `view_reader`    — `ViewReader` trait + `HotOnlyViewReader`
//! - `multi_shard_reader` — multi-shard scatter reader pinned to a frontier
//! - `session`        — per-connection session state and isolation levels
//! - `protocol`       — protocol type helpers
//! - `error`          — `GatewayError`

pub mod catalog_stubs;
pub mod error;
pub mod multi_shard_reader;
pub mod protocol;
pub mod server;
pub mod session;
pub mod view_reader;

pub use error::GatewayError;
pub use server::GatewayServer;
pub use view_reader::{HotOnlyViewReader, ViewReadStrategy, ViewReader};

#[cfg(test)]
mod tests {
    #[test]
    fn gateway_crate_compiles() {}
}

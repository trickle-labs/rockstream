//! PostgreSQL wire gateway service for RockStream (v0.24).
//!
//! Serves reads of maintained views and accepts direct-write DML (INSERT/UPDATE/DELETE)
//! from Postgres-compatible clients (`psql`, SQLAlchemy, JDBC) via the pgwire protocol.
//!
//! # Modules
//!
//! - `server`         — `GatewayServer` and `GatewayHandler`
//! - `catalog_stubs`  — `pg_catalog` + `information_schema` stub responders
//! - `view_reader`    — `ViewReader` trait + `HotOnlyViewReader`
//! - `multi_shard_reader` — multi-shard scatter reader pinned to a frontier
//! - `session`        — per-connection session state and isolation levels
//! - `write_buffer`   — bounded per-connection DML accumulator (`WriteBuffer`)
//! - `protocol`       — protocol type helpers
//! - `error`          — `GatewayError`

pub mod admission;
pub mod auth;
pub mod catalog_stubs;
pub mod change_log;
pub mod copy_state;
pub mod error;
pub mod multi_shard_reader;
pub mod notify_registry;
pub mod protocol;
pub mod role_catalog;
pub mod server;
pub mod session;
pub mod subscribe_handler;
pub mod subscribe_parser;
pub mod tls;
pub mod view_reader;
pub mod write_buffer;

pub use error::GatewayError;
pub use role_catalog::RoleCatalog;
pub use server::{
    query_time_scatter_fill_levels, query_time_scatter_peak_fill_levels, ConnectionStateTotals,
    GatewayServer, QueryTimeScatterBudget, QueryTimeScatterFillLevels, QueryTimeShardReaderSpec,
    QueryTimeShardTopology, QueryTimeShardTopologyProvider, MAX_CONNECTIONS,
    QUERY_TIME_SCATTER_MAX_CONCURRENT_SHARD_BATCHES, QUERY_TIME_SCATTER_MAX_IN_FLIGHT_BYTES,
    QUERY_TIME_SCATTER_MAX_IN_FLIGHT_ROWS,
};
pub use session::{CursorState, TxStatus, MAX_CURSORS_PER_CONNECTION};
pub use view_reader::{
    HotOnlyViewReader, ViewReadStrategy, ViewReader, ROWS_IN_FLIGHT_BATCH, STREAM_BATCH_BYTES,
};
pub use write_buffer::{DmlOp, WriteBuffer, WRITE_BUFFER_LIMIT_BYTES};

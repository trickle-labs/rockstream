//! Gateway error types.

use thiserror::Error;

/// Errors produced by the gateway.
#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("[RS-2003] isolation.serializable_not_supported: SERIALIZABLE isolation is not supported; use READ COMMITTED or REPEATABLE READ")]
    SerializableNotSupported,

    #[error("[RS-2004] isolation.repeatable_read_not_supported: REPEATABLE READ isolation is not supported; use READ COMMITTED")]
    RepeatableReadNotSupported,

    #[error("[RS-2600] limit.prepared_statements_exceeded: prepared statement limit of {limit} exceeded for this connection. next_steps: Deallocate unused prepared statements using DEALLOCATE.")]
    PreparedStatementsLimitExceeded { limit: usize },

    #[error("[RS-2601] limit.portals_exceeded: portal limit of {limit} exceeded for this connection. next_steps: Close unused portals.")]
    PortalsLimitExceeded { limit: usize },

    #[error("View not found: {0}")]
    ViewNotFound(String),

    /// Bound: result set exceeds `max_in_flight_rows`.
    #[error("[RS-2040] limit.result_set_too_large: result set exceeded max_in_flight_rows bound. next_steps: Add a LIMIT clause or paginate using cursors.")]
    ResultSetTooLarge,

    /// [RS-2019] write.shard_backpressure — per-connection write buffer exceeded WRITE_BUFFER_LIMIT_BYTES.
    /// next_steps: "Wait for downstream IVM processing to drain, then retry COMMIT."
    #[error("[RS-2019] write.shard_backpressure: per-connection write buffer full ({current_bytes} bytes, limit {limit_bytes} bytes). Wait for downstream IVM processing to drain, then retry COMMIT.")]
    ShardBackpressure {
        current_bytes: usize,
        limit_bytes: usize,
    },

    /// [RS-2007] write.idempotency_key_required.
    ///
    /// As of v0.51.1, a COMMIT with neither `SET rockstream.idempotency_key`
    /// nor `SET rockstream.source_epoch` no longer fails: the server mints a
    /// fresh CSPRNG-derived idempotency key for that commit instead. This
    /// variant is retained for protocol/documentation stability but is no
    /// longer reachable from the normal write-commit path.
    #[error("[RS-2007] write.idempotency_key_required: SET rockstream.idempotency_key = '<key>' or SET rockstream.source_epoch = N before COMMIT.")]
    IdempotencyKeyRequired,

    /// [RS-2500] copy.table_not_found — COPY target table does not exist in the catalog.
    /// next_steps: "Register the table with CREATE TABLE before using COPY FROM STDIN."
    #[error("[RS-2500] copy.table_not_found: COPY target table '{table}' does not exist in the catalog. next_steps: Register the table with CREATE TABLE before using COPY FROM STDIN.")]
    CopyTableNotFound { table: String },

    /// [RS-2501] copy.column_count_mismatch — Row field count does not match the declared column count.
    /// next_steps: "Check that the TSV row matches the column count declared in COPY or the catalog."
    #[error("[RS-2501] copy.column_count_mismatch: expected {expected} fields but got {got}. next_steps: Check that the TSV row matches the column count declared in COPY or the catalog.")]
    CopyColumnCountMismatch { expected: usize, got: usize },

    /// [RS-2050] query.cancelled — query was cancelled by a CancelRequest.
    /// SQLSTATE: 57014 (query_canceled)
    #[error("[RS-2050] query.cancelled: query was cancelled by a client CancelRequest. next_steps: Retry the query or adjust client timeout settings.")]
    QueryCancelled,

    /// [RS-2051] cursor.not_found — FETCH/MOVE/CLOSE referenced a cursor that does not exist.
    /// SQLSTATE: 34000 (invalid_cursor_name)
    #[error("[RS-2051] cursor.not_found: cursor '{name}' does not exist. next_steps: Use DECLARE to open a cursor before FETCH/MOVE/CLOSE.")]
    CursorNotFound { name: String },

    /// [RS-2052] cursor.already_exists — DECLARE attempted to open a cursor that is already open.
    /// SQLSTATE: 42P03 (duplicate_cursor)
    #[error("[RS-2052] cursor.already_exists: cursor '{name}' already exists. next_steps: CLOSE the existing cursor or use a different name.")]
    CursorAlreadyExists { name: String },

    /// [RS-2053] limit.memory_limit_exceeded — per-connection memory limit exceeded.
    /// SQLSTATE: 53200 (out_of_memory)
    #[error("[RS-2053] limit.memory_limit_exceeded: per-connection memory limit exceeded. next_steps: Close unused cursors, reduce result set sizes, or split the query.")]
    MemoryLimitExceeded,

    /// [RS-2054] query.statement_timeout — query exceeded the configured statement timeout.
    /// SQLSTATE: 57014 (query_canceled)
    #[error("[RS-2054] query.statement_timeout: query exceeded the configured statement timeout. next_steps: Increase statement_timeout or optimize the query.")]
    StatementTimeout,

    /// [RS-2055] limit.connection_limit_exceeded — server-wide connection limit reached.
    /// SQLSTATE: 53300 (too_many_connections)
    #[error("[RS-2055] limit.connection_limit_exceeded: server-wide connection limit of {limit} reached. next_steps: Close idle connections or increase max_connections.")]
    ConnectionLimitExceeded { limit: usize },

    /// [RS-2401] auth.invalid_password — password authentication failed for user.
    /// SQLSTATE: 28P01 (invalid_password) / severity: FATAL
    #[error("[RS-2401] auth.invalid_password: password authentication failed for user '{user}'. next_steps: Check password and retry")]
    InvalidPassword { user: String },

    /// [RS-2560] transaction.in_failed_sql_transaction — SQLSTATE 25P02
    #[error("[RS-2560] transaction.in_failed_sql_transaction: query cannot run inside a failed transaction block. next_steps: Issue ROLLBACK to exit the failed block, then retry.")]
    InFailedSqlTransaction,

    /// [RS-2561] transaction.savepoint_not_found — SQLSTATE 3B001
    #[error("[RS-2561] transaction.savepoint_not_found: savepoint '{name}' does not exist. next_steps: Use SAVEPOINT <name> to create one before ROLLBACK TO.")]
    SavepointNotFound { name: String },

    /// [RS-2562] transaction.two_phase_not_supported — SQLSTATE 0A000
    #[error("[RS-2562] transaction.two_phase_not_supported: PREPARE TRANSACTION / XA two-phase commit is not supported. next_steps: Use a single-phase COMMIT instead.")]
    TwoPhaseNotSupported,

    /// [RS-2563] transaction.savepoint_limit_exceeded — SQLSTATE 54000
    #[error("[RS-2563] transaction.savepoint_limit_exceeded: per-transaction savepoint limit of {limit} exceeded. next_steps: RELEASE earlier savepoints before creating new ones.")]
    SavepointLimitExceeded { limit: usize },

    /// [RS-2564] notify.channel_limit_exceeded — SQLSTATE 54000
    #[error("[RS-2564] notify.channel_limit_exceeded: notify channel limit of {limit} exceeded. next_steps: UNLISTEN unused channels.")]
    NotifyChannelLimitExceeded { limit: usize },

    /// [RS-2025] query.query_time_result_set_too_large — query-time DataFusion source scan exceeded its bound.
    #[error("[RS-2025] query.query_time_result_set_too_large: query result set too large for query-time execution while scanning '{relation}' (row limit {row_limit}). next_steps: Add a LIMIT clause, reduce source-table cardinality, or materialize the query into a view.")]
    QueryTimeResultSetTooLarge { relation: String, row_limit: usize },

    /// [RS-2028] query.query_time_scatter_topology_unavailable — a query-time
    /// relation cannot safely fall back to the gateway-local shard.
    #[error("[RS-2028] query.query_time_scatter_topology_unavailable: query-time execution has no complete pinned shard-reader topology. next_steps: Configure every owning shard reader at one cluster frontier, then retry the query.")]
    QueryTimeScatterTopologyUnavailable,

    /// [RS-2030] query.query_time_scatter_frontier_mismatch — configured
    /// query-time shards do not expose one common durable frontier.
    #[error("[RS-2030] query.query_time_scatter_frontier_mismatch: shard reader '{shard_path}' is at frontier {actual}, but the selected query frontier is {expected}. next_steps: Wait for every owning shard to reach the same frontier, then retry the query.")]
    QueryTimeScatterFrontierMismatch {
        shard_path: String,
        expected: u64,
        actual: u64,
    },

    /// [RS-2029] query.query_time_scatter_budget_exceeded — the explicit
    /// pathological scan budget was reached before a complete response existed.
    #[error("[RS-2029] query.query_time_scatter_budget_exceeded: query-time scatter scan for '{relation}' exceeded the pathological budget ({row_limit} rows or {byte_limit} bytes). next_steps: Narrow the predicate, add a LIMIT, or materialize the query into a view.")]
    QueryTimeScatterBudgetExceeded {
        relation: String,
        row_limit: usize,
        byte_limit: usize,
    },

    /// [RS-2026] query.query_time_execution_failed — query-time DataFusion planning/execution failed.
    #[error("[RS-2026] query.query_time_execution_failed: query-time execution failed: {detail}. next_steps: Simplify the query, validate referenced table/view schemas, or materialize the query into a view.")]
    QueryTimeExecutionFailed { detail: String },

    /// [RS-2027] index.backfill_row_limit_exceeded — `CREATE INDEX` automatic
    /// backfill scan exceeded its configured bounded row budget.
    #[error("[RS-2027] index.backfill_row_limit_exceeded: CREATE INDEX backfill for '{index_name}' on table '{table}' exceeded the row limit ({row_limit} rows). next_steps: Reduce table cardinality before indexing, or drop and recreate the index once the table is smaller.")]
    IndexBackfillRowLimitExceeded {
        index_name: String,
        table: String,
        row_limit: usize,
    },

    #[error("Not supported: {0}")]
    NotSupported(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Storage error: {0}")]
    Storage(#[from] rockstream_storage::StorageError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("PgWire error: {0}")]
    PgWire(String),
}

impl From<pgwire::error::PgWireError> for GatewayError {
    fn from(e: pgwire::error::PgWireError) -> Self {
        GatewayError::PgWire(e.to_string())
    }
}

/// Return the 5-char Postgres SQLSTATE code for a `GatewayError`.
pub fn sqlstate_for(e: &GatewayError) -> &'static str {
    match e {
        GatewayError::SerializableNotSupported => "25001",
        GatewayError::RepeatableReadNotSupported => "25001",
        GatewayError::PreparedStatementsLimitExceeded { .. } => "53200",
        GatewayError::PortalsLimitExceeded { .. } => "53200",
        GatewayError::ViewNotFound(_) => "42P01",
        GatewayError::ResultSetTooLarge => "54000",
        GatewayError::ShardBackpressure { .. } => "53100",
        GatewayError::IdempotencyKeyRequired => "XX000",
        GatewayError::CopyTableNotFound { .. } => "42P01",
        GatewayError::CopyColumnCountMismatch { .. } => "22000",
        GatewayError::QueryCancelled => "57014",
        GatewayError::CursorNotFound { .. } => "34000",
        GatewayError::CursorAlreadyExists { .. } => "42P03",
        GatewayError::MemoryLimitExceeded => "53200",
        GatewayError::StatementTimeout => "57014",
        GatewayError::ConnectionLimitExceeded { .. } => "53300",
        GatewayError::InvalidPassword { .. } => "28P01",
        GatewayError::InFailedSqlTransaction => "25P02",
        GatewayError::SavepointNotFound { .. } => "3B001",
        GatewayError::TwoPhaseNotSupported => "0A000",
        GatewayError::SavepointLimitExceeded { .. } => "54000",
        GatewayError::NotifyChannelLimitExceeded { .. } => "54000",
        GatewayError::QueryTimeResultSetTooLarge { .. } => "54000",
        GatewayError::QueryTimeScatterTopologyUnavailable => "55000",
        GatewayError::QueryTimeScatterFrontierMismatch { .. } => "55000",
        GatewayError::QueryTimeScatterBudgetExceeded { .. } => "54000",
        GatewayError::QueryTimeExecutionFailed { .. } => "0A000",
        GatewayError::IndexBackfillRowLimitExceeded { .. } => "54000",
        GatewayError::NotSupported(_) => "0A000",
        GatewayError::ParseError(_) => "42601",
        GatewayError::Storage(_) => "XX000",
        GatewayError::Io(_) => "XX000",
        GatewayError::PgWire(_) => "XX000",
    }
}

impl From<GatewayError> for pgwire::error::PgWireError {
    fn from(e: GatewayError) -> Self {
        let code = sqlstate_for(&e).to_string();
        let severity = match &e {
            GatewayError::InvalidPassword { .. } => "FATAL",
            _ => "ERROR",
        };
        let msg = e.to_string();
        pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
            severity.to_string(),
            code,
            msg,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// S1 green gate: each new variant's Display contains the expected RS code.
    #[test]
    fn copy_error_codes_display() {
        let e = GatewayError::CopyTableNotFound {
            table: "ghost_t".to_string(),
        };
        assert!(
            e.to_string().contains("RS-2500"),
            "expected RS-2500 in: {}",
            e
        );
        assert!(
            e.to_string().contains("ghost_t"),
            "expected table name in: {}",
            e
        );

        let e = GatewayError::CopyColumnCountMismatch {
            expected: 3,
            got: 2,
        };
        assert!(
            e.to_string().contains("RS-2501"),
            "expected RS-2501 in: {}",
            e
        );
        assert!(
            e.to_string().contains('3') && e.to_string().contains('2'),
            "expected counts in: {}",
            e
        );
    }
}

//! Gateway error types.

use thiserror::Error;

/// Errors produced by the gateway.
#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("[RS-2003] isolation.serializable_not_supported: SERIALIZABLE isolation is not supported; use READ COMMITTED or REPEATABLE READ")]
    SerializableNotSupported,

    #[error("[RS-2600] limit.prepared_statements_exceeded: prepared statement limit of {limit} exceeded for this connection. next_steps: Deallocate unused prepared statements using DEALLOCATE.")]
    PreparedStatementsLimitExceeded { limit: usize },

    #[error("[RS-2601] limit.portals_exceeded: portal limit of {limit} exceeded for this connection. next_steps: Close unused portals.")]
    PortalsLimitExceeded { limit: usize },

    #[error("View not found: {0}")]
    ViewNotFound(String),

    /// Bound: result set exceeds `max_in_flight_rows`.
    #[error("Result set too large: exceeded max_in_flight_rows bound")]
    ResultSetTooLarge,

    /// [RS-2019] write.shard_backpressure — per-connection write buffer exceeded WRITE_BUFFER_LIMIT_BYTES.
    /// next_steps: "Wait for downstream IVM processing to drain, then retry COMMIT."
    #[error("[RS-2019] write.shard_backpressure: per-connection write buffer full ({current_bytes} bytes, limit {limit_bytes} bytes). Wait for downstream IVM processing to drain, then retry COMMIT.")]
    ShardBackpressure {
        current_bytes: usize,
        limit_bytes: usize,
    },

    /// [RS-2007] write.idempotency_key_required.
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

impl From<GatewayError> for pgwire::error::PgWireError {
    fn from(e: GatewayError) -> Self {
        let code = match &e {
            GatewayError::PreparedStatementsLimitExceeded { .. } => "53200".to_string(),
            GatewayError::PortalsLimitExceeded { .. } => "53200".to_string(),
            GatewayError::SerializableNotSupported => "25001".to_string(),
            _ => "XX000".to_string(),
        };
        pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
            "ERROR".to_string(),
            code,
            e.to_string(),
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

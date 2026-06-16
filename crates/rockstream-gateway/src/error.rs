//! Gateway error types.

use thiserror::Error;

/// Errors produced by the gateway.
#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("[RS-2003] isolation.serializable_not_supported: SERIALIZABLE isolation is not supported; use READ COMMITTED or REPEATABLE READ")]
    SerializableNotSupported,

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

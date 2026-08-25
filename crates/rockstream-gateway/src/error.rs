//! Gateway error types.

use rockstream_types::diagnostic::{record_diagnostic, DiagnosticOccurrence};
use rockstream_types::error_code::{ErrorCode, ErrorDescriptor};
use uuid::Uuid;

/// Errors produced by the gateway.
#[derive(Debug)]
pub enum GatewayError {
    CommitEpochExhausted,
    SerializableNotSupported,

    RepeatableReadNotSupported,

    PreparedStatementsLimitExceeded {
        limit: usize,
    },

    PortalsLimitExceeded {
        limit: usize,
    },

    ViewNotFound(String),

    /// Bound: result set exceeds `max_in_flight_rows`.
    ResultSetTooLarge,

    /// [RS-2019] write.shard_backpressure — per-connection write buffer exceeded WRITE_BUFFER_LIMIT_BYTES.
    /// next_steps: "Wait for downstream IVM processing to drain, then retry COMMIT."
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
    IdempotencyKeyRequired,

    /// [RS-2500] copy.table_not_found — COPY target table does not exist in the catalog.
    /// next_steps: "Register the table with CREATE TABLE before using COPY FROM STDIN."
    CopyTableNotFound {
        table: String,
    },

    /// [RS-2501] copy.column_count_mismatch — Row field count does not match the declared column count.
    /// next_steps: "Check that the TSV row matches the column count declared in COPY or the catalog."
    CopyColumnCountMismatch {
        expected: usize,
        got: usize,
    },

    /// [RS-2050] query.cancelled — query was cancelled by a CancelRequest.
    /// SQLSTATE: 57014 (query_canceled)
    QueryCancelled,

    /// [RS-2051] cursor.not_found — FETCH/MOVE/CLOSE referenced a cursor that does not exist.
    /// SQLSTATE: 34000 (invalid_cursor_name)
    CursorNotFound {
        name: String,
    },

    /// [RS-2052] cursor.already_exists — DECLARE attempted to open a cursor that is already open.
    /// SQLSTATE: 42P03 (duplicate_cursor)
    CursorAlreadyExists {
        name: String,
    },

    /// [RS-2053] limit.memory_limit_exceeded — per-connection memory limit exceeded.
    /// SQLSTATE: 53200 (out_of_memory)
    MemoryLimitExceeded,

    /// [RS-2054] query.statement_timeout — query exceeded the configured statement timeout.
    /// SQLSTATE: 57014 (query_canceled)
    StatementTimeout,

    /// [RS-2055] limit.connection_limit_exceeded — server-wide connection limit reached.
    /// SQLSTATE: 53300 (too_many_connections)
    ConnectionLimitExceeded {
        limit: usize,
    },

    /// [RS-2401] auth.invalid_password — password authentication failed for user.
    /// SQLSTATE: 28P01 (invalid_password) / severity: FATAL
    InvalidPassword {
        user: String,
    },

    /// [RS-2560] transaction.in_failed_sql_transaction — SQLSTATE 25P02
    InFailedSqlTransaction,

    /// [RS-2561] transaction.savepoint_not_found — SQLSTATE 3B001
    SavepointNotFound {
        name: String,
    },

    /// [RS-2562] transaction.two_phase_not_supported — SQLSTATE 0A000
    TwoPhaseNotSupported,

    /// [RS-2563] transaction.savepoint_limit_exceeded — SQLSTATE 54000
    SavepointLimitExceeded {
        limit: usize,
    },

    /// [RS-2564] notify.channel_limit_exceeded — SQLSTATE 54000
    NotifyChannelLimitExceeded {
        limit: usize,
    },

    /// [RS-2025] query.query_time_result_set_too_large — query-time DataFusion source scan exceeded its bound.
    QueryTimeResultSetTooLarge {
        relation: String,
        row_limit: usize,
    },

    /// [RS-2028] query.query_time_scatter_topology_unavailable — a query-time
    /// relation cannot safely fall back to the gateway-local shard.
    QueryTimeScatterTopologyUnavailable,

    /// [RS-2030] query.query_time_scatter_frontier_mismatch — configured
    /// query-time shards do not expose one common durable frontier.
    QueryTimeScatterFrontierMismatch {
        shard_path: String,
        expected: u64,
        actual: u64,
    },

    /// [RS-2029] query.query_time_scatter_budget_exceeded — the explicit
    /// pathological scan budget was reached before a complete response existed.
    QueryTimeScatterBudgetExceeded {
        relation: String,
        row_limit: usize,
        byte_limit: usize,
    },

    /// [RS-2026] query.query_time_execution_failed — query-time DataFusion planning/execution failed.
    QueryTimeExecutionFailed {
        detail: String,
    },

    /// [RS-2027] index.backfill_row_limit_exceeded — `CREATE INDEX` automatic
    /// backfill scan exceeded its configured bounded row budget.
    IndexBackfillRowLimitExceeded {
        index_name: String,
        table: String,
        row_limit: usize,
    },

    NotSupported(String),

    ParseError(String),

    Storage(rockstream_storage::StorageError),

    Io(std::io::Error),

    PgWire(String),
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if matches!(self, Self::CommitEpochExhausted) {
            return f.write_str(
                "[RS-2060] write.epoch_exhausted: commit epoch reached u64::MAX. next_steps: create a new shard before retrying.",
            );
        }
        if let Self::QueryTimeExecutionFailed { detail } = self {
            return write!(
                f,
                "[RS-2026] query.query_time_execution_failed: query-time execution failed: {detail}. next_steps: {}",
                ErrorDescriptor::lookup(ErrorCode::new(2026))
                    .map(|descriptor| descriptor.default_next_steps.as_str())
                    .unwrap_or("Retry after simplifying the query."),
            );
        }
        f.write_str(&self.diagnostic_occurrence().render_text())
    }
}

impl std::error::Error for GatewayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rockstream_storage::StorageError> for GatewayError {
    fn from(error: rockstream_storage::StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<std::io::Error> for GatewayError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl GatewayError {
    pub fn diagnostic_code(&self) -> ErrorCode {
        match self {
            Self::CommitEpochExhausted => ErrorCode::new(2060),
            Self::SerializableNotSupported => ErrorCode::new(2003),
            Self::RepeatableReadNotSupported => ErrorCode::new(2004),
            Self::PreparedStatementsLimitExceeded { .. } => ErrorCode::new(2600),
            Self::PortalsLimitExceeded { .. } => ErrorCode::new(2601),
            Self::ViewNotFound(_) => ErrorCode::new(1001),
            Self::ResultSetTooLarge => ErrorCode::new(2040),
            Self::ShardBackpressure { .. } => ErrorCode::new(2019),
            Self::IdempotencyKeyRequired => ErrorCode::new(2007),
            Self::CopyTableNotFound { .. } => ErrorCode::new(2500),
            Self::CopyColumnCountMismatch { .. } => ErrorCode::new(2501),
            Self::QueryCancelled => ErrorCode::new(2050),
            Self::CursorNotFound { .. } => ErrorCode::new(2051),
            Self::CursorAlreadyExists { .. } => ErrorCode::new(2052),
            Self::MemoryLimitExceeded => ErrorCode::new(2053),
            Self::StatementTimeout => ErrorCode::new(2054),
            Self::ConnectionLimitExceeded { .. } => ErrorCode::new(2055),
            Self::InvalidPassword { .. } => ErrorCode::new(2401),
            Self::InFailedSqlTransaction => ErrorCode::new(2560),
            Self::SavepointNotFound { .. } => ErrorCode::new(2561),
            Self::TwoPhaseNotSupported => ErrorCode::new(2562),
            Self::SavepointLimitExceeded { .. } => ErrorCode::new(2563),
            Self::NotifyChannelLimitExceeded { .. } => ErrorCode::new(2564),
            Self::QueryTimeResultSetTooLarge { .. } => ErrorCode::new(2025),
            Self::QueryTimeScatterTopologyUnavailable => ErrorCode::new(2028),
            Self::QueryTimeScatterFrontierMismatch { .. } => ErrorCode::new(2030),
            Self::QueryTimeScatterBudgetExceeded { .. } => ErrorCode::new(2029),
            Self::QueryTimeExecutionFailed { .. } => ErrorCode::new(2026),
            Self::IndexBackfillRowLimitExceeded { .. } => ErrorCode::new(2027),
            Self::NotSupported(_) => ErrorCode::new(1),
            Self::ParseError(_) => ErrorCode::new(1012),
            Self::Storage(_) => ErrorCode::new(3),
            Self::Io(_) | Self::PgWire(_) => ErrorCode::new(1),
        }
    }

    pub fn diagnostic_occurrence(&self) -> DiagnosticOccurrence {
        let code = self.diagnostic_code();
        let correlation_id = Uuid::new_v4();
        let context = match self {
            Self::PreparedStatementsLimitExceeded { limit }
            | Self::PortalsLimitExceeded { limit }
            | Self::ConnectionLimitExceeded { limit } => {
                vec![("limit".to_string(), limit.to_string())]
            }
            Self::ViewNotFound(view) => vec![("view".to_string(), view.clone())],
            Self::ShardBackpressure {
                current_bytes,
                limit_bytes,
            } => vec![
                ("current_bytes".to_string(), current_bytes.to_string()),
                ("limit_bytes".to_string(), limit_bytes.to_string()),
            ],
            Self::CopyTableNotFound { table } => vec![("table".to_string(), table.clone())],
            Self::CopyColumnCountMismatch { expected, got } => vec![
                ("expected".to_string(), expected.to_string()),
                ("got".to_string(), got.to_string()),
            ],
            Self::CursorNotFound { name } | Self::CursorAlreadyExists { name } => {
                vec![("name".to_string(), name.clone())]
            }
            Self::InvalidPassword { user } => vec![("user".to_string(), user.clone())],
            Self::SavepointNotFound { name } => vec![("name".to_string(), name.clone())],
            Self::SavepointLimitExceeded { limit } => {
                vec![("limit".to_string(), limit.to_string())]
            }
            Self::NotifyChannelLimitExceeded { limit } => {
                vec![("limit".to_string(), limit.to_string())]
            }
            Self::QueryTimeResultSetTooLarge {
                relation,
                row_limit,
            } => vec![
                ("relation".to_string(), relation.clone()),
                ("row_limit".to_string(), row_limit.to_string()),
            ],
            Self::QueryTimeScatterFrontierMismatch {
                shard_path,
                expected,
                actual,
            } => vec![
                ("shard_path".to_string(), shard_path.clone()),
                ("expected".to_string(), expected.to_string()),
                ("actual".to_string(), actual.to_string()),
            ],
            Self::QueryTimeScatterBudgetExceeded {
                relation,
                row_limit,
                byte_limit,
            } => vec![
                ("relation".to_string(), relation.clone()),
                ("row_limit".to_string(), row_limit.to_string()),
                ("byte_limit".to_string(), byte_limit.to_string()),
            ],
            Self::QueryTimeExecutionFailed { detail }
            | Self::NotSupported(detail)
            | Self::ParseError(detail)
            | Self::PgWire(detail) => vec![("detail".to_string(), detail.clone())],
            Self::Storage(error) => vec![("detail".to_string(), error.to_string())],
            Self::Io(error) => vec![("detail".to_string(), error.to_string())],
            Self::IndexBackfillRowLimitExceeded {
                index_name,
                table,
                row_limit,
            } => vec![
                ("index_name".to_string(), index_name.clone()),
                ("table".to_string(), table.clone()),
                ("row_limit".to_string(), row_limit.to_string()),
            ],
            _ => Vec::new(),
        };
        match DiagnosticOccurrence::new(code, correlation_id, context, None, None) {
            Ok(occurrence) => occurrence,
            Err(_) => DiagnosticOccurrence {
                code,
                correlation_id,
                message: ErrorDescriptor::lookup(code)
                    .map(|descriptor| descriptor.title.clone())
                    .unwrap_or_else(|| "Unknown error".to_string()),
                context: std::collections::BTreeMap::new(),
                retry_after: None,
                cause: None,
            },
        }
    }

    pub fn record_structured_log(&self, occurrence: &DiagnosticOccurrence) {
        tracing::error!(
            code = %occurrence.code,
            correlation_id = %occurrence.correlation_id,
            diagnostic = %occurrence.render_json(),
            "gateway diagnostic"
        );
    }
}

impl From<pgwire::error::PgWireError> for GatewayError {
    fn from(e: pgwire::error::PgWireError) -> Self {
        GatewayError::PgWire(e.to_string())
    }
}

/// Return the 5-char Postgres SQLSTATE code for a `GatewayError`.
pub fn sqlstate_for(e: &GatewayError) -> &'static str {
    ErrorDescriptor::lookup(e.diagnostic_code())
        .map(|descriptor| descriptor.sqlstate.as_str())
        .unwrap_or("XX000")
}

impl From<GatewayError> for pgwire::error::PgWireError {
    fn from(e: GatewayError) -> Self {
        let occurrence = e.diagnostic_occurrence();
        let descriptor = occurrence.descriptor();
        record_diagnostic(occurrence.clone());
        e.record_structured_log(&occurrence);
        pgwire::error::PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
            descriptor
                .map(|value| value.severity.to_string())
                .unwrap_or_else(|| "ERROR".to_string()),
            descriptor
                .map(|value| value.sqlstate.clone())
                .unwrap_or_else(|| "XX000".to_string()),
            occurrence.render_text(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_epoch_exhausted_error_is_rs2060() {
        let error = GatewayError::CommitEpochExhausted;
        assert_eq!(
            error.to_string(),
            "[RS-2060] write.epoch_exhausted: commit epoch reached u64::MAX. next_steps: create a new shard before retrying."
        );
        assert_eq!(sqlstate_for(&error), "54000");
    }

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

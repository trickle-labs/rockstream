//! Gateway error types (v0.43).

use thiserror::Error;

use rockstream_types::error_code::{
    ErrorCode, RS_2001, RS_2002, RS_2003, RS_2004, RS_2005, RS_2006, RS_2008,
};

/// Errors produced by the RockStream Postgres gateway.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum GatewayError {
    /// The client requested `SERIALIZABLE` or another unsupported isolation
    /// level; RockStream supports only snapshot isolation.
    #[error(
        "unsupported transaction isolation level; only snapshot isolation is supported (RS-2003)"
    )]
    UnsupportedIsolationLevel,

    /// The named view was not found in the catalog.
    #[error("view not found: {0}")]
    ViewNotFound(String),

    /// Attempting to drop an inline view that still has dependent objects.
    #[error("cannot drop inline view '{0}': {1} dependent materialized view(s) still exist")]
    InlineViewHasDependents(String, usize),

    /// Query exceeded the configured wall-clock deadline.
    #[error("query timeout exceeded after {0} ms")]
    QueryTimeoutExceeded(u64),

    /// The per-connection or per-tenant query rate limit was exceeded.
    #[error("query rate limit exceeded: max {0} queries/second")]
    RateLimitExceeded(u32),

    /// The connection pool has no available slots.
    #[error("connection pool exhausted: {0} active connections")]
    PoolExhausted(usize),

    /// Partial aggregation combining failed (merge law returned an error).
    #[error("partial aggregation merge error: {0}")]
    PartialAggMergeError(String),

    /// Historical query references an epoch before the checkpoint retention
    /// window (RS-2006, v0.42).
    #[error(
        "historical query references epoch {requested} which is before the \
         retention window (oldest retained: {oldest_retained})"
    )]
    HistoricalQueryBeyondRetention {
        /// The epoch requested by the query.
        requested: u64,
        /// Oldest retained epoch.
        oldest_retained: u64,
    },

    /// A concurrent transaction committed to the same row before this
    /// transaction could commit — the client must retry (RS-2008, v0.43).
    #[error(
        "optimistic conflict on table '{table}': a concurrent transaction \
         committed at epoch {conflicting_epoch}"
    )]
    OptimisticConflict {
        /// The table on which the conflict was detected.
        table: String,
        /// The epoch at which the conflicting transaction committed.
        conflicting_epoch: u64,
    },
}

impl GatewayError {
    /// Return the canonical RockStream error code for this error.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::UnsupportedIsolationLevel => RS_2003,
            Self::ViewNotFound(_) => RS_2001,
            Self::InlineViewHasDependents(_, _) => RS_2004,
            Self::QueryTimeoutExceeded(_) => RS_2002,
            Self::RateLimitExceeded(_) => RS_2005,
            Self::PoolExhausted(_) => RS_2001,
            Self::PartialAggMergeError(_) => RS_2001,
            Self::HistoricalQueryBeyondRetention { .. } => RS_2006,
            Self::OptimisticConflict { .. } => RS_2008,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isolation_level_error_code_is_rs_2003() {
        assert_eq!(
            GatewayError::UnsupportedIsolationLevel.error_code(),
            RS_2003
        );
    }

    #[test]
    fn view_has_dependents_error_code_is_rs_2004() {
        assert_eq!(
            GatewayError::InlineViewHasDependents("v".to_string(), 1).error_code(),
            RS_2004
        );
    }
}

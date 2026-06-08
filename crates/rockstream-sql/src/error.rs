//! Error types for the SQL frontend (v0.7).
//!
//! Every user-visible failure from the SQL frontend carries an RS-XXXX code
//! with actionable next_steps text.

use rockstream_types::error_code::{ErrorCode, RS_1002, RS_1012, RS_1013};
use thiserror::Error;

/// Errors produced by the SQL frontend, lowering pass, and schema catalog.
#[derive(Debug, Error)]
pub enum SqlError {
    /// SQL statement could not be parsed.
    ///
    /// RS-1012: check SQL syntax; see docs/language-features.md for the
    /// supported SQL subset.
    #[error("[RS-1012] SQL parse error: {message}")]
    ParseError { message: String },

    /// The query contains a feature not yet supported by the incremental
    /// planner (e.g. a lateral join, a subquery, or an unsupported scalar
    /// expression type).
    ///
    /// RS-1013: simplify the query or consult docs/language-features.md.
    #[error("[RS-1013] Unsupported plan node in incremental lowering: {node_type}")]
    UnsupportedPlanNode { node_type: String },

    /// A view update would change columns in a way that requires re-encoding
    /// existing data (e.g. renaming, dropping, or retyping a column).
    ///
    /// RS-1002: use a new view name or follow the blue/green procedure.
    #[error("[RS-1002] Incompatible schema change: {reason}")]
    IncompatibleSchemaChange { reason: String },

    /// Storage layer error (e.g. ShardDb put/get failure).
    #[error("[RS-0003] Storage error: {0}")]
    Storage(#[from] rockstream_storage::StorageError),

    /// DataFusion internal error (propagated as RS-1012 at the API boundary).
    #[error("[RS-1012] DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// JSON serialization error (catalog encoding).
    #[error("[RS-0001] Catalog serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

impl SqlError {
    /// The RS-XXXX error code for this error.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::ParseError { .. } => RS_1012,
            Self::UnsupportedPlanNode { .. } => RS_1013,
            Self::IncompatibleSchemaChange { .. } => RS_1002,
            Self::Storage(_) => rockstream_types::error_code::RS_0003,
            Self::DataFusion(_) => RS_1012,
            Self::Serde(_) => rockstream_types::error_code::RS_0001,
        }
    }
}

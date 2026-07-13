//! Error types for the SQL frontend (v0.7).
//!
//! Every user-visible failure from the SQL frontend carries an RS-XXXX code
//! with actionable next_steps text.

use rockstream_types::error_code::{
    ErrorCode, RS_0001, RS_0003, RS_1002, RS_1011, RS_1012, RS_1013, RS_1016, RS_1731, RS_2016,
};
use thiserror::Error;

/// Errors produced by the SQL frontend, lowering pass, and schema catalog.
#[derive(Debug, Error)]
pub enum SqlError {
    /// View-on-view DAG contains a cycle.
    ///
    /// RS-1011: resolve cycle in view dependencies; view-on-view relations must form a DAG.
    #[error("[RS-1011] Cycle detected in view dependencies: view '{view_name}' forms a cycle via path: {cycle_path:?}")]
    CycleDetected {
        view_name: String,
        cycle_path: Vec<String>,
    },

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

    /// Unsupported window function in SQL lowering (v0.11).
    ///
    /// RS-1016: supported in v0.11: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD,
    /// SUM/AVG over ROWS frame.
    #[error(
        "[RS-1016] Unsupported window function '{fn_name}' in v0.11 — \
         ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, SUM/AVG over rows frame are supported."
    )]
    UnsupportedWindowFunction { fn_name: String },

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

    /// Index name conflict: same index name already exists for a different table.
    ///
    /// RS-2016: use a different index name or drop the existing index first.
    #[error("[RS-2016] Index name conflict: index '{index_name}' already exists for table '{existing_table}' (not '{requested_table}')")]
    IndexNameConflict {
        index_name: String,
        existing_table: String,
        requested_table: String,
    },

    /// DDL parse error: unrecognized or malformed DDL statement.
    #[error("[RS-1012] DDL parse error: {message}")]
    DdlParseError { message: String },

    /// A workload-catalog write (`CREATE WORKLOAD` / update / drop) was
    /// attempted on a control node that is not the current Raft-elected
    /// control leader (v0.45.2, M7-S2 leader-only write gating).
    ///
    /// RS-1731: retry against the current control leader; the caller
    /// should re-resolve leadership (e.g. via `cluster status`) before
    /// retrying.
    #[error(
        "[RS-1731] Workload-catalog write rejected: this node is not the control-plane leader"
    )]
    NotLeader,
}

impl SqlError {
    /// The RS-XXXX error code for this error.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::CycleDetected { .. } => RS_1011,
            Self::ParseError { .. } => RS_1012,
            Self::UnsupportedPlanNode { .. } => RS_1013,
            Self::UnsupportedWindowFunction { .. } => RS_1016,
            Self::IncompatibleSchemaChange { .. } => RS_1002,
            Self::Storage(_) => RS_0003,
            Self::DataFusion(_) => RS_1012,
            Self::Serde(_) => RS_0001,
            Self::IndexNameConflict { .. } => RS_2016,
            Self::DdlParseError { .. } => RS_1012,
            Self::NotLeader => RS_1731,
        }
    }
}

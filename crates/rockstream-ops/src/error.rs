//! Error types for the ops crate.
//!
//! Every user-visible failure uses an `RS-XXXX` code with actionable
//! `next_steps`. Internal programming errors use `RS-0001`.

use rockstream_types::error_code::{ErrorCode, RS_0001};
use thiserror::Error;

/// Operator-level errors.
#[derive(Debug, Error)]
pub enum OpError {
    /// Arrow computation error (internal).
    #[error("[{code}] Arrow error: {source}; next_steps: check schema and data types")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
        code: ErrorCode,
    },

    /// Expression type mismatch: operator received wrong column type.
    #[error("[{code}] Expression type error: {context}; next_steps: verify column types in schema")]
    ExprTypeMismatch { context: String, code: ErrorCode },

    /// A column index is out of bounds.
    #[error("[{code}] Column index {index} out of bounds (schema has {num_cols} columns); next_steps: check plan column references")]
    ColumnOutOfBounds {
        index: usize,
        num_cols: usize,
        code: ErrorCode,
    },

    /// A column is not of the expected type.
    #[error("[{code}] Column type mismatch: expected {expected}, got {got}; next_steps: ensure source schema matches plan")]
    ColumnTypeMismatch {
        expected: String,
        got: String,
        code: ErrorCode,
    },

    /// An invalid literal value (wrong byte length, etc.).
    #[error("[{code}] Invalid literal: {detail}; next_steps: {detail}")]
    InvalidLiteral { detail: String, code: ErrorCode },

    /// Feature not yet implemented in this version.
    #[error("[{code}] Not implemented in v0.4: {feature}; next_steps: this feature arrives in a later version")]
    Unimplemented { feature: String, code: ErrorCode },

    /// Storage error from the ShardDb layer.
    #[error("[{code}] Storage error: {source}; next_steps: check disk space and object store connectivity")]
    Storage {
        #[source]
        source: rockstream_storage::StorageError,
        code: ErrorCode,
    },
}

impl OpError {
    pub fn arrow(source: arrow::error::ArrowError) -> Self {
        Self::Arrow { source, code: RS_0001 }
    }

    pub fn column_out_of_bounds(index: usize, num_cols: usize) -> Self {
        use rockstream_types::error_code::ErrorCode;
        Self::ColumnOutOfBounds { index, num_cols, code: ErrorCode::new(1010) }
    }

    pub fn column_type_mismatch(expected: impl Into<String>, got: impl Into<String>) -> Self {
        use rockstream_types::error_code::ErrorCode;
        Self::ColumnTypeMismatch {
            expected: expected.into(),
            got: got.into(),
            code: ErrorCode::new(1011),
        }
    }

    pub fn expr_type_mismatch(context: impl Into<String>) -> Self {
        use rockstream_types::error_code::ErrorCode;
        Self::ExprTypeMismatch { context: context.into(), code: ErrorCode::new(1012) }
    }

    pub fn invalid_literal(detail: impl Into<String>) -> Self {
        use rockstream_types::error_code::ErrorCode;
        Self::InvalidLiteral { detail: detail.into(), code: ErrorCode::new(1013) }
    }

    pub fn unimplemented(feature: impl Into<String>) -> Self {
        use rockstream_types::error_code::ErrorCode;
        Self::Unimplemented { feature: feature.into(), code: ErrorCode::new(1014) }
    }

    pub fn storage(source: rockstream_storage::StorageError) -> Self {
        Self::Storage { source, code: RS_0001 }
    }
}

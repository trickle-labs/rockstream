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
    #[error(
        "[{code}] Expression type error: {context}; next_steps: verify column types in schema"
    )]
    ExprTypeMismatch { context: String, code: ErrorCode },

    /// A column index is out of bounds.
    #[error(
        "[{code}] Column index {index} out of bounds (schema has {num_cols} columns); next_steps: check plan column references"
    )]
    ColumnOutOfBounds {
        index: usize,
        num_cols: usize,
        code: ErrorCode,
    },

    /// A column is not of the expected type.
    #[error(
        "[{code}] Column type mismatch: expected {expected}, got {got}; next_steps: ensure source schema matches plan"
    )]
    ColumnTypeMismatch {
        expected: String,
        got: String,
        code: ErrorCode,
    },

    /// An invalid literal value (wrong byte length, etc.).
    #[error("[{code}] Invalid literal: {detail}; next_steps: {detail}")]
    InvalidLiteral { detail: String, code: ErrorCode },

    /// Feature not yet implemented in this version.
    #[error(
        "[{code}] Not implemented in v0.4: {feature}; next_steps: this feature arrives in a later version"
    )]
    Unimplemented { feature: String, code: ErrorCode },

    /// Storage error from the ShardDb layer.
    #[error(
        "[{code}] Storage error: {source}; next_steps: check disk space and object store connectivity"
    )]
    Storage {
        #[source]
        source: rockstream_storage::StorageError,
        code: ErrorCode,
    },

    /// Group-commit capacity exceeded; applying back-pressure.
    #[error(
        "[{code}] Group-commit queue full ({current}/{max} batches pending); next_steps: reduce epoch rate, increase GROUP_COMMIT_MAX_BATCHES, or add more shards"
    )]
    GroupCommitFull {
        current: usize,
        max: usize,
        code: ErrorCode,
    },

    /// Aggregate running sum overflowed i64.
    #[error(
        "[{code}] Aggregate sum overflow for group key {group_key}: next_steps: reduce value magnitudes or switch to a wider numeric type"
    )]
    AggregateOverflow { group_key: i64, code: ErrorCode },

    /// MIN/MAX multiset retraction underflow.
    #[error(
        "[{code}] MIN/MAX retraction underflow for group key {group_key}, value {value}: next_steps: ensure every retraction is matched by a prior insertion; check source event ordering"
    )]
    MinMaxRetractionUnderflow {
        group_key: i64,
        value: i64,
        code: ErrorCode,
    },

    /// TopK buffer overflow: too many unique rows in a single partition.
    #[error(
        "[{code}] TopK buffer overflow: {limit} unique positive-weight rows exceeded in one partition; next_steps: reduce partition cardinality, increase TOPK_BUFFER_LIMIT, or add partition columns"
    )]
    TopKBufferOverflow { limit: usize, code: ErrorCode },

    /// Hop-window state exceeded its configured overlap-aware bound.
    #[error(
        "[{code}] Hop window state bound exceeded ({current}/{limit} rows); next_steps: reduce hop overlap, increase HOP_WINDOW_STATE_LIMIT, or shard the windowed stream more finely"
    )]
    HopWindowStateOverflow {
        current: usize,
        limit: usize,
        code: ErrorCode,
    },

    /// Session-window state exceeded its configured bound.
    #[error(
        "[{code}] Session window state bound exceeded ({current}/{limit} sessions); next_steps: reduce session cardinality, increase SESSION_WINDOW_STATE_LIMIT, or shard the windowed stream more finely"
    )]
    SessionWindowStateOverflow {
        current: usize,
        limit: usize,
        code: ErrorCode,
    },

    /// Late-data side-channel queue exceeded its configured bound.
    #[error(
        "[{code}] Late-data side-channel queue full ({current}/{limit} rows); next_steps: drain the configured late-data sink, reduce late-event volume, or increase TUMBLE_WINDOW_LATE_ROUTE_LIMIT after verifying available capacity"
    )]
    LateRouteOverflow {
        current: usize,
        limit: usize,
        code: ErrorCode,
    },

    /// Factorized payload tree exceeded its row or byte bound.
    #[error(
        "[{code}] Factorized payload bound exceeded ({current_rows}/{max_rows} rows, {current_bytes}/{max_bytes} bytes); next_steps: reduce join fan-out, increase the factor payload bound after capacity review, or use the classic join path"
    )]
    FactorPayloadOverflow {
        current_rows: usize,
        max_rows: usize,
        current_bytes: usize,
        max_bytes: usize,
        code: ErrorCode,
    },

    /// A factorized epoch would exceed one of its immutable amplification budgets.
    #[error(
        "[{code}] Delta amplification budget exceeded for {dimension} ({current}/{limit}); next_steps: use the classic plan, reduce the input delta, or raise the reviewed operator budget"
    )]
    DeltaAmplificationExceeded {
        dimension: &'static str,
        current: u64,
        limit: u64,
        code: ErrorCode,
    },

    /// Monotone recursion received a negative delta.
    #[error(
        "[{code}] Non-monotone delta rejected in monotone recursion; next_steps: mark the recursive query non-monotone or remove retractions from the input stream"
    )]
    RecursionNonMonotoneDelta { code: ErrorCode },

    /// Recursion arrangement exceeded its configured bound.
    #[error(
        "[{code}] Recursion state bound exceeded ({current}/{limit} rows); next_steps: reduce recursive fan-out, increase RECURSION_STATE_LIMIT, or shard the recursive relation more finely"
    )]
    RecursionStateOverflow {
        current: usize,
        limit: usize,
        code: ErrorCode,
    },

    /// Distributed recursion stalled without advancing the inner frontier.
    #[error(
        "[{code}] Distributed recursion inner frontier stalled; next_steps: inspect slow shards, restart the stalled worker, or allow per-shard recompute fallback"
    )]
    RecursionInnerFrontierStalled { code: ErrorCode },

    /// Recursion hit its max-iteration safety cap.
    #[error(
        "[{code}] Recursion max-iteration cap exceeded after {max_iterations} iterations; next_steps: increase recursion_max_iterations or simplify the recursive step to converge faster"
    )]
    RecursionMaxIterations {
        max_iterations: usize,
        code: ErrorCode,
    },

    /// `compile_plan` encountered a `PlanNode` shape it does not support.
    #[error(
        "[{code}] Plan node not supported by the direct operator compiler: {kind}; next_steps: this query shape requires the DiffCtx/OpNode physical-plan path, not the v0.51.3 fast-path compiler"
    )]
    UnsupportedPlanNode { kind: String, code: ErrorCode },

    /// Operator not found in pipeline (v0.53.2 IVM arrangement debugger).
    #[error(
        "[{code}] Operator '{op_id}' not found in pipeline; next_steps: run rockstream explain <view> --op-ids to inspect available operator IDs for this view"
    )]
    OperatorNotFound { op_id: String, code: ErrorCode },

    /// Arrangement key decoding failed or unsupported family (v0.53.2 IVM arrangement debugger).
    #[error(
        "[{code}] Arrangement key decoding failed for family '{family}': {detail}; next_steps: check arrangement key syntax or verify if the operator family key codec is supported"
    )]
    ArrangementKeyDecodeFailed {
        family: String,
        detail: String,
        code: ErrorCode,
    },

    /// Internal error.
    #[error("[{code}] Internal error: {detail}; next_steps: report this issue")]
    Internal { detail: String, code: ErrorCode },
}

impl OpError {
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal {
            detail: detail.into(),
            code: RS_0001,
        }
    }

    pub fn arrow(source: arrow::error::ArrowError) -> Self {
        Self::Arrow {
            source,
            code: RS_0001,
        }
    }

    pub fn column_out_of_bounds(index: usize, num_cols: usize) -> Self {
        use rockstream_types::error_code::ErrorCode;
        Self::ColumnOutOfBounds {
            index,
            num_cols,
            code: ErrorCode::new(1010),
        }
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
        Self::ExprTypeMismatch {
            context: context.into(),
            code: ErrorCode::new(1012),
        }
    }

    pub fn invalid_literal(detail: impl Into<String>) -> Self {
        use rockstream_types::error_code::ErrorCode;
        Self::InvalidLiteral {
            detail: detail.into(),
            code: ErrorCode::new(1013),
        }
    }

    pub fn unimplemented(feature: impl Into<String>) -> Self {
        use rockstream_types::error_code::ErrorCode;
        Self::Unimplemented {
            feature: feature.into(),
            code: ErrorCode::new(1014),
        }
    }

    pub fn storage(source: rockstream_storage::StorageError) -> Self {
        Self::Storage {
            source,
            code: RS_0001,
        }
    }

    pub fn storage_error(msg: impl Into<String>) -> Self {
        Self::Storage {
            source: rockstream_storage::StorageError::KeyEncoding(msg.into()),
            code: RS_0001,
        }
    }

    pub fn group_commit_full(current: usize) -> Self {
        use rockstream_types::error_code::RS_1015;
        Self::GroupCommitFull {
            current,
            max: crate::group_commit::GROUP_COMMIT_MAX_BATCHES,
            code: RS_1015,
        }
    }

    pub fn aggregate_overflow(group_key: i64) -> Self {
        use rockstream_types::error_code::RS_1016;
        Self::AggregateOverflow {
            group_key,
            code: RS_1016,
        }
    }

    pub fn minmax_retraction_underflow(group_key: i64, value: i64) -> Self {
        use rockstream_types::error_code::RS_1017;
        Self::MinMaxRetractionUnderflow {
            group_key,
            value,
            code: RS_1017,
        }
    }

    pub fn topk_buffer_overflow(limit: usize) -> Self {
        use rockstream_types::error_code::RS_1018;
        Self::TopKBufferOverflow {
            limit,
            code: RS_1018,
        }
    }

    pub fn hop_window_state_overflow(current: usize, limit: usize) -> Self {
        use rockstream_types::error_code::RS_2023;
        Self::HopWindowStateOverflow {
            current,
            limit,
            code: RS_2023,
        }
    }

    pub fn session_window_state_overflow(current: usize, limit: usize) -> Self {
        use rockstream_types::error_code::RS_2024;
        Self::SessionWindowStateOverflow {
            current,
            limit,
            code: RS_2024,
        }
    }

    pub fn late_route_overflow(current: usize, limit: usize) -> Self {
        use rockstream_types::error_code::RS_2028;
        Self::LateRouteOverflow {
            current,
            limit,
            code: RS_2028,
        }
    }

    pub fn factor_payload_overflow(
        current_rows: usize,
        max_rows: usize,
        current_bytes: usize,
        max_bytes: usize,
    ) -> Self {
        use rockstream_types::error_code::RS_2030;
        Self::FactorPayloadOverflow {
            current_rows,
            max_rows,
            current_bytes,
            max_bytes,
            code: RS_2030,
        }
    }

    pub fn delta_amplification_exceeded(dimension: &'static str, current: u64, limit: u64) -> Self {
        use rockstream_types::error_code::RS_2030;
        Self::DeltaAmplificationExceeded {
            dimension,
            current,
            limit,
            code: RS_2030,
        }
    }

    pub fn recursion_non_monotone_delta() -> Self {
        use rockstream_types::error_code::RS_1009;
        Self::RecursionNonMonotoneDelta { code: RS_1009 }
    }

    pub fn recursion_state_overflow(current: usize, limit: usize) -> Self {
        use rockstream_types::error_code::RS_2019;
        Self::RecursionStateOverflow {
            current,
            limit,
            code: RS_2019,
        }
    }

    pub fn recursion_inner_frontier_stalled() -> Self {
        use rockstream_types::error_code::RS_1512;
        Self::RecursionInnerFrontierStalled { code: RS_1512 }
    }

    pub fn recursion_max_iterations(max_iterations: usize) -> Self {
        use rockstream_types::error_code::RS_1513;
        Self::RecursionMaxIterations {
            max_iterations,
            code: RS_1513,
        }
    }

    pub fn unsupported_plan_node(kind: impl Into<String>) -> Self {
        use rockstream_types::error_code::RS_1013;
        Self::UnsupportedPlanNode {
            kind: kind.into(),
            code: RS_1013,
        }
    }

    pub fn operator_not_found(op_id: impl Into<String>) -> Self {
        use rockstream_types::error_code::RS_1020;
        Self::OperatorNotFound {
            op_id: op_id.into(),
            code: RS_1020,
        }
    }

    pub fn arrangement_key_decode_failed(
        family: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        use rockstream_types::error_code::RS_1021;
        Self::ArrangementKeyDecodeFailed {
            family: family.into(),
            detail: detail.into(),
            code: RS_1021,
        }
    }
}

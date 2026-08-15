//! Storage error types.

use thiserror::Error;

/// Errors returned by the storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SlateDB error: {0}")]
    Slate(slatedb::Error),

    #[error("key encoding error: {0}")]
    KeyEncoding(String),

    #[error("merge operator not configured")]
    MergeOperatorNotConfigured,

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    /// RS-2002: partial-agg result too large — shard returned >
    /// MAX_PARTIAL_AGG_RESULT_ROWS groups.
    /// next_steps: "Reduce GROUP BY cardinality or increase
    /// MAX_PARTIAL_AGG_RESULT_ROWS."
    #[error("[RS-2002] view.state_budget_exceeded: partial-agg result exceeded MAX_PARTIAL_AGG_RESULT_ROWS ({limit} groups). Reduce GROUP BY cardinality.")]
    PartialAggResultTooLarge { limit: usize },

    /// RS-5002: arrangement header references a merge law that is not
    /// registered in the catalog. The shard refuses to attach until the law
    /// is either registered or the arrangement is migrated.
    #[non_exhaustive]
    #[error(
        "RS-5002: unknown merge law id={law_id} version={law_version} in shard arrangement header"
    )]
    UnknownMergeLaw { law_id: u16, law_version: u16 },

    /// RS-5001: the stored format is outside the binary's inclusive range.
    #[error(
        "RS-5001: incompatible storage format stored={stored}, supported={min}..={max}; run rockstream migrate --from=N --to=M --storage=<url>"
    )]
    IncompatibleFormat { stored: u8, min: u8, max: u8 },

    /// RS-5001: the fixed storage-format marker is not one byte.
    #[error(
        "RS-5001: malformed storage format marker length={length}, supported={min}..={max}; run rockstream migrate --from=N --to=M --storage=<url>"
    )]
    MalformedFormatMarker { length: usize, min: u8, max: u8 },

    /// Test/operation hook used to prove resumability without retaining a key list.
    #[error("format migration interrupted after {processed} objects")]
    MigrationInterrupted { processed: usize },

    /// A live writer attempted to use a shard while offline migration was pending.
    #[error(
        "RS-5001: offline format migration is in progress; stop shard writers and rerun the migration"
    )]
    MigrationInProgress,

    /// RS-5003: stored operand fails validation for the merge law.
    #[error("RS-5003: operand corruption for law {law_name} ({law_id}): invalid bytes")]
    OperandCorruption { law_id: u16, law_name: String },

    /// RS-3001: writer was fenced out by a newer writer.
    #[error("RS-3001: shard writer fenced out: lease lost")]
    Fenced,

    /// RS-2006: historical epoch outside checkpoint retention window.
    #[error("[RS-2006] Requested epoch {requested_epoch} is outside the retention window (minimum epoch: {min_retention_epoch})")]
    EpochPruned {
        requested_epoch: u64,
        min_retention_epoch: u64,
    },
}

impl From<slatedb::Error> for StorageError {
    fn from(err: slatedb::Error) -> Self {
        if matches!(
            err.kind(),
            slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced)
        ) {
            StorageError::Fenced
        } else {
            StorageError::Slate(err)
        }
    }
}

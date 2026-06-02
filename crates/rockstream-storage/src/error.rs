//! Storage error types.

use thiserror::Error;

/// Errors returned by the storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SlateDB error: {0}")]
    Slate(#[from] slatedb::Error),

    #[error("key encoding error: {0}")]
    KeyEncoding(String),

    #[error("merge operator not configured")]
    MergeOperatorNotConfigured,

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    /// RS-5002: arrangement header references a merge law that is not
    /// registered in the catalog. The shard refuses to attach until the law
    /// is either registered or the arrangement is migrated.
    #[non_exhaustive]
    #[error(
        "RS-5002: unknown merge law id={law_id} version={law_version} in shard arrangement header"
    )]
    UnknownMergeLaw { law_id: u16, law_version: u16 },

    /// RS-5003: stored operand fails validation for the merge law.
    #[error("RS-5003: operand corruption for law {law_name} ({law_id}): invalid bytes")]
    OperandCorruption { law_id: u16, law_name: String },
}

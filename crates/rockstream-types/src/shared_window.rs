use crate::merge_law::{MergeLawId, MergeLawVersion};
use serde::{Deserialize, Serialize};

/// Canonical identity for one physical time-slice arrangement.
///
/// Logical windows may use different widths while sharing this physical slice
/// store when all fields below match.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SharedWindowSpec {
    pub source_identity: String,
    pub partitioning: String,
    pub time_column: String,
    pub slice_width_ms: u64,
    pub predicate_digest: Option<[u8; 32]>,
    pub merge_law_id: MergeLawId,
    pub merge_law_version: MergeLawVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedWindowSpecError {
    ZeroSliceWidth,
}

impl std::fmt::Display for SharedWindowSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroSliceWidth => write!(f, "shared window slice width must be positive"),
        }
    }
}

impl std::error::Error for SharedWindowSpecError {}

impl SharedWindowSpec {
    pub fn new(
        source_identity: impl Into<String>,
        partitioning: impl Into<String>,
        time_column: impl Into<String>,
        slice_width_ms: u64,
        predicate_digest: Option<[u8; 32]>,
        merge_law_id: MergeLawId,
        merge_law_version: MergeLawVersion,
    ) -> Result<Self, SharedWindowSpecError> {
        if slice_width_ms == 0 {
            return Err(SharedWindowSpecError::ZeroSliceWidth);
        }
        Ok(Self {
            source_identity: source_identity.into(),
            partitioning: partitioning.into(),
            time_column: time_column.into(),
            slice_width_ms,
            predicate_digest,
            merge_law_id,
            merge_law_version,
        })
    }
}

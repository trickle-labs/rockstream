//! Merge law types for the IVM arrangement layer.
//!
//! Defines the `MergeLawId`, `MergeLawVersion`, `LawProperties`, and the
//! `LawBundle` trait that every registered law must implement. The trait
//! encodes algebraic properties (associativity, commutativity, idempotence)
//! and provides the merge/identity operations used by the storage layer,
//! exchange combiners, and compaction.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Identifies a merge law in the catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MergeLawId(pub u16);

impl fmt::Display for MergeLawId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "law-{:04}", self.0)
    }
}

/// Version of a merge law (for forward-compatible evolution).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MergeLawVersion(pub u16);

impl fmt::Display for MergeLawVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// Classification of merge law algebraic properties.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeLawClass {
    /// A commutative monoid (associative + commutative + identity).
    CommutativeMonoid,
    /// A semilattice (associative + commutative + idempotent).
    Semilattice,
    /// An abelian group (commutative monoid + inverse).
    AbelianGroup,
}

/// Algebraic properties declared by a merge law.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LawProperties {
    /// The merge function is associative: f(f(a, b), c) = f(a, f(b, c)).
    pub associative: bool,
    /// The merge function is commutative: f(a, b) = f(b, a).
    pub commutative: bool,
    /// The merge function is idempotent: f(a, a) = a.
    pub idempotent: bool,
    /// An inverse exists for every element.
    pub has_inverse: bool,
    /// An identity element exists.
    pub has_identity: bool,
}

/// Policy for handling duplicate merge operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DuplicatePolicy {
    /// Merge duplicates (default for associative ops).
    Merge,
    /// Reject duplicates with an error.
    Reject,
    /// Last writer wins (only for non-associative fallback).
    LastWriterWins,
}

/// Compaction policy for arrangement state managed by a law.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionPolicy {
    /// Standard merge-on-compaction (applies the law's merge function).
    MergeOnCompact,
    /// Tombstone GC: remove entries that reach the identity element.
    TombstoneGc,
    /// Retain all versions (no compaction reduction).
    RetainAll,
}

/// Frontier advancement policy: what progress guarantees are needed before
/// emitting output for this law.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrontierPolicy {
    /// Any frontier advancement may trigger output (suitable for abelian groups
    /// and commutative monoids where partial results are still correct).
    AnyAdvancement,
    /// Output may only be emitted when the frontier is exact (no in-flight
    /// retractions). Required for non-idempotent laws such as `SumCount/v1`.
    ExactOnly,
    /// The law produces no aggregate state — output is a direct delta passthrough
    /// (suitable for stateless linear operators: Filter, Project, Map).
    Stateless,
}

/// Descriptor returned by `LawBundle::gateway_combiner` for laws that support
/// cross-shard partial aggregation pushdown at the gateway layer (DESIGN.md
/// §12.3.1, §6.11).
///
/// When a law returns `Some(GatewayAggCombinerDesc)`, the gateway may push
/// partial aggregation down to individual shards and merge the O(groups) partial
/// results rather than streaming all O(view rows) to the coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayAggCombinerDesc {
    /// The law driving the partial aggregation.
    pub law_id: MergeLawId,
    /// Human-readable name for EXPLAIN output.
    pub law_name: &'static str,
    /// Whether the partial result is associative (required for safe pushdown).
    pub is_associative: bool,
    /// Whether the partial result is commutative (order of shards doesn't matter).
    pub is_commutative: bool,
}

/// The core trait that all registered merge laws must implement.
///
/// A `LawBundle` encapsulates the algebraic operations and metadata for a
/// merge law. It is the single source of truth consumed by the storage merge
/// operator, exchange combiners, the planner, `EXPLAIN INCREMENTAL`, and
/// compaction filters.
pub trait LawBundle: Send + Sync + 'static {
    /// Unique identifier for this law in the catalog.
    fn id(&self) -> MergeLawId;

    /// Current version of this law implementation.
    fn version(&self) -> MergeLawVersion;

    /// Human-readable name (e.g. "WeightAdd", "SumCount", "MaxRegister").
    fn name(&self) -> &'static str;

    /// Algebraic properties of the merge function.
    fn properties(&self) -> LawProperties;

    /// Classification of this law.
    fn class(&self) -> MergeLawClass;

    /// Duplicate handling policy.
    fn duplicate_policy(&self) -> DuplicatePolicy;

    /// Compaction policy.
    fn compaction_policy(&self) -> CompactionPolicy;

    /// Frontier advancement policy for this law.
    fn frontier_policy(&self) -> FrontierPolicy;

    /// The identity element serialized to bytes.
    /// Returns `None` if no identity exists (rare — most laws have one).
    fn identity(&self) -> Option<Vec<u8>>;

    /// Merge two operands. Both `left` and `right` are raw value bytes
    /// (without the arrangement header). Returns the merged result.
    ///
    /// # Errors
    /// Returns an error string if the operands are malformed.
    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String>;

    /// Returns true if the given value equals the identity element.
    /// Used by `TombstoneGc` compaction to reclaim space.
    fn is_identity(&self, value: &[u8]) -> bool;

    /// A reason if this law does NOT support merge-safe reads
    /// (i.e., read-modify-write cannot be avoided). Returns `None` if it does.
    fn not_merge_safe_reason(&self) -> Option<crate::explain::NotMergeSafeReason> {
        None
    }

    /// Returns a descriptor for cross-shard partial aggregation pushdown at
    /// the gateway layer (DESIGN.md §12.3.1, §6.11).
    ///
    /// Laws that support gateway-level combining return `Some(desc)`.  The
    /// gateway uses this to push partial aggregation to shards and merge the
    /// O(groups) results rather than streaming all O(view rows).
    ///
    /// Returns `None` for laws that cannot safely be partially combined (e.g.
    /// non-associative or stateless pass-through laws).
    fn gateway_combiner(&self) -> Option<GatewayAggCombinerDesc> {
        None
    }
}

/// Descriptor for a registered law (used in catalogs and `EXPLAIN` output).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LawDescriptor {
    pub id: MergeLawId,
    pub version: MergeLawVersion,
    pub name: String,
    pub class: MergeLawClass,
    pub properties: LawProperties,
    pub duplicate_policy: DuplicatePolicy,
    pub compaction_policy: CompactionPolicy,
    pub frontier_policy: FrontierPolicy,
    pub idempotent: bool,
}

impl LawDescriptor {
    /// Build a descriptor from a `LawBundle` implementation.
    pub fn from_bundle(bundle: &dyn LawBundle) -> Self {
        Self {
            id: bundle.id(),
            version: bundle.version(),
            name: bundle.name().to_owned(),
            class: bundle.class(),
            properties: bundle.properties(),
            duplicate_policy: bundle.duplicate_policy(),
            compaction_policy: bundle.compaction_policy(),
            frontier_policy: bundle.frontier_policy(),
            idempotent: bundle.properties().idempotent,
        }
    }
}

/// Header prepended to stored arrangement values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrangementHeader {
    /// The merge law used for this value.
    pub law_id: MergeLawId,
    /// The version of the merge law.
    pub law_version: MergeLawVersion,
}

impl ArrangementHeader {
    /// Byte size of the serialized header (4 bytes: 2 for id, 2 for version).
    pub const WIRE_SIZE: usize = 4;

    /// Encode the header into a 4-byte representation.
    pub fn encode(&self) -> [u8; 4] {
        let mut buf = [0u8; 4];
        buf[0..2].copy_from_slice(&self.law_id.0.to_be_bytes());
        buf[2..4].copy_from_slice(&self.law_version.0.to_be_bytes());
        buf
    }

    /// Decode a header from a 4-byte representation.
    pub fn decode(buf: &[u8; 4]) -> Self {
        Self {
            law_id: MergeLawId(u16::from_be_bytes([buf[0], buf[1]])),
            law_version: MergeLawVersion(u16::from_be_bytes([buf[2], buf[3]])),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrangement_header_round_trip() {
        let header = ArrangementHeader {
            law_id: MergeLawId(1),
            law_version: MergeLawVersion(2),
        };
        let encoded = header.encode();
        let decoded = ArrangementHeader::decode(&encoded);
        assert_eq!(header, decoded);
    }

    #[test]
    fn merge_law_id_display() {
        assert_eq!(MergeLawId(1).to_string(), "law-0001");
        assert_eq!(MergeLawId(99).to_string(), "law-0099");
    }

    #[test]
    fn merge_law_version_display() {
        assert_eq!(MergeLawVersion(1).to_string(), "v1");
    }
}

//! Canonical ArrangementSpec, ArrangementId and constituent types for RockStream (v0.59.6).
//!
//! Defines the canonical specification `ArrangementSpec` containing all 12+ mandatory
//! constituent fields, deterministic BLAKE3 / SHA-256 domain-separated hashing into
//! a stable `ArrangementId`, and multi-tenant isolation semantics.

use crate::ids::{ArrangementId, TenantId};
use crate::merge_law::{MergeLawId, MergeLawVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

/// Identity of the source relation or table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceIdentity(pub String);

impl SourceIdentity {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Canonical literal representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CanonicalLiteral {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64), // IEEE-754 bitwise representation for exact equality & hashing
    Utf8(String),
    Bytes(Vec<u8>),
}

/// Canonical binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CanonicalBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

impl CanonicalBinaryOp {
    /// Returns true if this binary operator is commutative (a op b == b op a).
    pub fn is_commutative(&self) -> bool {
        matches!(
            self,
            Self::Add | Self::Mul | Self::Eq | Self::NotEq | Self::And | Self::Or
        )
    }
}

/// Canonical unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CanonicalUnaryOp {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

/// Canonical data types for expressions and keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CanonicalType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Date,
    Timestamp,
    Decimal(u8, u8), // precision, scale
    Binary,
}

/// Deterministic, normalized canonical expression representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CanonicalExpr {
    Column(String),
    Literal(CanonicalLiteral),
    BinaryOp {
        op: CanonicalBinaryOp,
        left: Box<CanonicalExpr>,
        right: Box<CanonicalExpr>,
    },
    UnaryOp {
        op: CanonicalUnaryOp,
        expr: Box<CanonicalExpr>,
    },
    Cast {
        expr: Box<CanonicalExpr>,
        target_type: CanonicalType,
    },
    FunctionCall {
        name: String,
        args: Vec<CanonicalExpr>,
    },
}

impl CanonicalExpr {
    pub fn col(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    pub fn lit_int(v: i64) -> Self {
        Self::Literal(CanonicalLiteral::Int(v))
    }

    pub fn lit_str(s: impl Into<String>) -> Self {
        Self::Literal(CanonicalLiteral::Utf8(s.into()))
    }

    pub fn lit_bool(b: bool) -> Self {
        Self::Literal(CanonicalLiteral::Bool(b))
    }

    pub fn binary_op(op: CanonicalBinaryOp, left: CanonicalExpr, right: CanonicalExpr) -> Self {
        if op.is_commutative() && left > right {
            Self::BinaryOp {
                op,
                left: Box::new(right),
                right: Box::new(left),
            }
        } else {
            Self::BinaryOp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            }
        }
    }
}

/// Null sorting semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum NullSemantics {
    NullsFirst,
    NullsLast,
}

/// Collation identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CollationId(pub String);

impl CollationId {
    pub fn binary() -> Self {
        Self("binary".to_string())
    }

    pub fn utf8_default() -> Self {
        Self("utf8_default".to_string())
    }

    pub fn rockstream_binary_v1() -> Self {
        Self("rockstream_binary_v1".to_string())
    }

    /// Check if this collation identifier represents a supported binary/UTF-8 byte-wise collation.
    pub fn is_binary_supported(&self) -> bool {
        let s = self.0.trim().to_ascii_lowercase();
        matches!(
            s.as_str(),
            "rockstream_binary_v1"
                | "binary"
                | "c"
                | "posix"
                | "utf8_default"
                | "default"
                | "ucs_basic"
        )
    }
}

impl fmt::Display for CollationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Collation version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CollationVersion(pub u32);

impl fmt::Display for CollationVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// Time domain semantics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TimeDomainSemantics {
    Utc,
    Timezone(String),
    ProcessingTime,
    EventTimeWatermark,
}

/// Partitioning specification for arrangement sharding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PartitioningSpec {
    SingleShard(u64),
    Hash { num_shards: usize },
    Broadcast,
}

/// Canonical specification for a physical shared arrangement.
///
/// If two views have identical `ArrangementSpec`s, they share the exact same
/// physical trace, compaction horizon, and memory cache blocks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArrangementSpec {
    pub tenant_id: TenantId,
    pub security_policy_digest: [u8; 32],
    pub source_identity: SourceIdentity,
    pub source_schema_generation: u64,
    pub key_expressions: Vec<CanonicalExpr>,
    pub key_types: Vec<CanonicalType>,
    pub value_projection: Vec<CanonicalExpr>,
    pub predicate: Option<CanonicalExpr>,
    pub null_semantics: NullSemantics,
    pub decimal_scale: Option<u8>,
    pub collation_identifier: CollationId,
    pub collation_version: CollationVersion,
    pub time_domain: TimeDomainSemantics,
    pub merge_law_id: MergeLawId,
    pub merge_law_version: MergeLawVersion,
    pub partitioning: PartitioningSpec,
}

impl ArrangementSpec {
    /// Domain tag for deterministic domain separation.
    pub const DOMAIN_TAG: &'static str = "rockstream:arrangement:v1";

    /// Compute the deterministic, stable `ArrangementId` for this canonical spec.
    pub fn arrangement_id(&self) -> ArrangementId {
        let serialized =
            serde_json::to_vec(self).expect("ArrangementSpec serialization must never fail");

        let mut hasher = Sha256::new();
        hasher.update(Self::DOMAIN_TAG.as_bytes());
        hasher.update([0x00]);
        hasher.update(&serialized);
        let hash_bytes = hasher.finalize();

        let id_u64 = u64::from_be_bytes(
            hash_bytes[0..8]
                .try_into()
                .expect("SHA-256 slice must be 8 bytes"),
        );
        ArrangementId(id_u64)
    }

    /// Create a minimal default ArrangementSpec for a source.
    pub fn default_for_source(tenant_id: TenantId, source_name: impl Into<String>) -> Self {
        Self {
            tenant_id,
            security_policy_digest: [0u8; 32],
            source_identity: SourceIdentity::new(source_name),
            source_schema_generation: 1,
            key_expressions: vec![CanonicalExpr::col("id")],
            key_types: vec![CanonicalType::Int64],
            value_projection: vec![CanonicalExpr::col("id")],
            predicate: None,
            null_semantics: NullSemantics::NullsLast,
            decimal_scale: None,
            collation_identifier: CollationId::utf8_default(),
            collation_version: CollationVersion(1),
            time_domain: TimeDomainSemantics::Utc,
            merge_law_id: MergeLawId(1),
            merge_law_version: MergeLawVersion(1),
            partitioning: PartitioningSpec::SingleShard(0),
        }
    }
}

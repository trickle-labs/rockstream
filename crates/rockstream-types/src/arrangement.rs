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
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(u64),   // IEEE-754 bitwise representation for exact equality & hashing (f64)
    Float32(u32), // IEEE-754 bitwise representation (f32)
    Float64(u64),
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: u8,
    },
    Utf8(String),
    Bytes(Vec<u8>),
    Date(i32),        // days since 1970-01-01
    Timestamp(i64),   // microseconds since Unix epoch
    TimestampTz(i64), // UTC microseconds since Unix epoch
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
    Uuid([u8; 16]),               // 128-bit UUID octets
    Array(Vec<CanonicalLiteral>), // 1D typed array
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
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal(u8, u8), // precision, scale
    Utf8,
    Binary,
    Date,
    Timestamp,
    TimestampTz,
    Interval,
    Uuid,
    Array(Box<CanonicalType>),
}

impl CanonicalType {
    pub const INT2: Self = Self::Int16;
    pub const INT4: Self = Self::Int32;
    pub const INT8: Self = Self::Int64;
    pub const FLOAT4: Self = Self::Float32;
    pub const FLOAT8: Self = Self::Float64;

    /// Return standard SQL type name.
    pub fn sql_name(&self) -> String {
        match self {
            Self::Boolean => "BOOLEAN".to_string(),
            Self::Int16 => "INT2".to_string(),
            Self::Int32 => "INT4".to_string(),
            Self::Int64 => "INT8".to_string(),
            Self::Float32 => "FLOAT4".to_string(),
            Self::Float64 => "FLOAT8".to_string(),
            Self::Decimal(p, s) => format!("DECIMAL({},{})", p, s),
            Self::Utf8 => "TEXT".to_string(),
            Self::Binary => "BYTEA".to_string(),
            Self::Date => "DATE".to_string(),
            Self::Timestamp => "TIMESTAMP".to_string(),
            Self::TimestampTz => "TIMESTAMPTZ".to_string(),
            Self::Interval => "INTERVAL".to_string(),
            Self::Uuid => "UUID".to_string(),
            Self::Array(inner) => format!("{}[]", inner.sql_name()),
        }
    }

    /// Check if this type represents an exact integer.
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Int16 | Self::Int32 | Self::Int64)
    }

    /// Check if this type is numeric (integer, float, or decimal).
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::Float32
                | Self::Float64
                | Self::Decimal(_, _)
        )
    }

    /// Check if this type is temporal.
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Date | Self::Timestamp | Self::TimestampTz | Self::Interval
        )
    }
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

/// Deterministic arrangement key codec for all 14 admitted types under `rockstream_binary_v1` collation.
pub struct CanonicalKeyCodec;

impl CanonicalKeyCodec {
    /// Encode a slice of canonical literals into a single deterministic byte key for SlateDB storage.
    pub fn encode_key(literals: &[CanonicalLiteral]) -> Vec<u8> {
        let mut buf = Vec::new();
        for lit in literals {
            Self::encode_literal_into(lit, &mut buf);
        }
        buf
    }

    /// Encode a single canonical literal into a byte buffer.
    pub fn encode_literal_into(lit: &CanonicalLiteral, buf: &mut Vec<u8>) {
        match lit {
            CanonicalLiteral::Null => {
                buf.push(0x00);
            }
            CanonicalLiteral::Bool(b) => {
                buf.push(0x01);
                buf.push(if *b { 1 } else { 0 });
            }
            CanonicalLiteral::Int16(v) => {
                buf.push(0x02);
                let enc = (*v as u16) ^ (1 << 15);
                buf.extend_from_slice(&enc.to_be_bytes());
            }
            CanonicalLiteral::Int32(v) => {
                buf.push(0x03);
                let enc = (*v as u32) ^ (1 << 31);
                buf.extend_from_slice(&enc.to_be_bytes());
            }
            CanonicalLiteral::Int(v) | CanonicalLiteral::Int64(v) => {
                buf.push(0x04);
                let enc = (*v as u64) ^ (1 << 63);
                buf.extend_from_slice(&enc.to_be_bytes());
            }
            CanonicalLiteral::Float32(v) => {
                buf.push(0x05);
                buf.extend_from_slice(&v.to_be_bytes());
            }
            CanonicalLiteral::Float(v) | CanonicalLiteral::Float64(v) => {
                buf.push(0x06);
                buf.extend_from_slice(&v.to_be_bytes());
            }
            CanonicalLiteral::Decimal {
                unscaled,
                precision,
                scale,
            } => {
                buf.push(0x07);
                let enc = (*unscaled as u128) ^ (1 << 127);
                buf.extend_from_slice(&enc.to_be_bytes());
                buf.push(*precision);
                buf.push(*scale);
            }
            CanonicalLiteral::Utf8(s) => {
                buf.push(0x08);
                for b in s.as_bytes() {
                    if *b == 0 {
                        buf.push(0);
                        buf.push(0xFF);
                    } else {
                        buf.push(*b);
                    }
                }
                buf.push(0);
                buf.push(0);
            }
            CanonicalLiteral::Bytes(b) => {
                buf.push(0x09);
                for byte in b {
                    if *byte == 0 {
                        buf.push(0);
                        buf.push(0xFF);
                    } else {
                        buf.push(*byte);
                    }
                }
                buf.push(0);
                buf.push(0);
            }
            CanonicalLiteral::Date(d) => {
                buf.push(0x0A);
                let enc = (*d as u32) ^ (1 << 31);
                buf.extend_from_slice(&enc.to_be_bytes());
            }
            CanonicalLiteral::Timestamp(ts) => {
                buf.push(0x0B);
                let enc = (*ts as u64) ^ (1 << 63);
                buf.extend_from_slice(&enc.to_be_bytes());
            }
            CanonicalLiteral::TimestampTz(tstz) => {
                buf.push(0x0C);
                let enc = (*tstz as u64) ^ (1 << 63);
                buf.extend_from_slice(&enc.to_be_bytes());
            }
            CanonicalLiteral::Interval {
                months,
                days,
                micros,
            } => {
                buf.push(0x0D);
                let m_enc = (*months as u32) ^ (1 << 31);
                let d_enc = (*days as u32) ^ (1 << 31);
                let us_enc = (*micros as u64) ^ (1 << 63);
                buf.extend_from_slice(&m_enc.to_be_bytes());
                buf.extend_from_slice(&d_enc.to_be_bytes());
                buf.extend_from_slice(&us_enc.to_be_bytes());
            }
            CanonicalLiteral::Uuid(octets) => {
                buf.push(0x0E);
                buf.extend_from_slice(octets);
            }
            CanonicalLiteral::Array(elems) => {
                buf.push(0x0F);
                let len = elems.len() as u32;
                buf.extend_from_slice(&len.to_be_bytes());
                for elem in elems {
                    Self::encode_literal_into(elem, buf);
                }
            }
        }
    }

    /// Decode a single literal from slice, returning (literal, remaining_slice).
    pub fn decode_literal(data: &[u8]) -> Result<(CanonicalLiteral, &[u8]), String> {
        if data.is_empty() {
            return Err("Unexpected EOF while decoding CanonicalLiteral".to_string());
        }
        let tag = data[0];
        let rest = &data[1..];
        match tag {
            0x00 => Ok((CanonicalLiteral::Null, rest)),
            0x01 => {
                if rest.is_empty() {
                    return Err("Unexpected EOF decoding Bool".to_string());
                }
                Ok((CanonicalLiteral::Bool(rest[0] != 0), &rest[1..]))
            }
            0x02 => {
                if rest.len() < 2 {
                    return Err("Unexpected EOF decoding Int16".to_string());
                }
                let raw = u16::from_be_bytes(rest[..2].try_into().unwrap());
                let val = (raw ^ (1 << 15)) as i16;
                Ok((CanonicalLiteral::Int16(val), &rest[2..]))
            }
            0x03 => {
                if rest.len() < 4 {
                    return Err("Unexpected EOF decoding Int32".to_string());
                }
                let raw = u32::from_be_bytes(rest[..4].try_into().unwrap());
                let val = (raw ^ (1 << 31)) as i32;
                Ok((CanonicalLiteral::Int32(val), &rest[4..]))
            }
            0x04 => {
                if rest.len() < 8 {
                    return Err("Unexpected EOF decoding Int64".to_string());
                }
                let raw = u64::from_be_bytes(rest[..8].try_into().unwrap());
                let val = (raw ^ (1 << 63)) as i64;
                Ok((CanonicalLiteral::Int64(val), &rest[8..]))
            }
            0x05 => {
                if rest.len() < 4 {
                    return Err("Unexpected EOF decoding Float32".to_string());
                }
                let raw = u32::from_be_bytes(rest[..4].try_into().unwrap());
                Ok((CanonicalLiteral::Float32(raw), &rest[4..]))
            }
            0x06 => {
                if rest.len() < 8 {
                    return Err("Unexpected EOF decoding Float64".to_string());
                }
                let raw = u64::from_be_bytes(rest[..8].try_into().unwrap());
                Ok((CanonicalLiteral::Float64(raw), &rest[8..]))
            }
            0x07 => {
                if rest.len() < 18 {
                    return Err("Unexpected EOF decoding Decimal".to_string());
                }
                let raw = u128::from_be_bytes(rest[..16].try_into().unwrap());
                let precision = rest[16];
                let scale = rest[17];
                let unscaled = (raw ^ (1 << 127)) as i128;
                Ok((
                    CanonicalLiteral::Decimal {
                        unscaled,
                        precision,
                        scale,
                    },
                    &rest[18..],
                ))
            }
            0x08 => {
                let mut bytes = Vec::new();
                let mut i = 0;
                while i < rest.len() {
                    if rest[i] == 0 {
                        if i + 1 >= rest.len() {
                            return Err("Truncated null-escape sequence in Utf8".to_string());
                        }
                        if rest[i + 1] == 0 {
                            let s = std::str::from_utf8(&bytes)
                                .map_err(|e| format!("Invalid UTF-8 string: {}", e))?
                                .to_string();
                            return Ok((CanonicalLiteral::Utf8(s), &rest[i + 2..]));
                        } else if rest[i + 1] == 0xFF {
                            bytes.push(0);
                            i += 2;
                        } else {
                            return Err("Invalid escape byte in Utf8".to_string());
                        }
                    } else {
                        bytes.push(rest[i]);
                        i += 1;
                    }
                }
                Err("Unterminated Utf8 string in CanonicalLiteral".to_string())
            }
            0x09 => {
                let mut bytes = Vec::new();
                let mut i = 0;
                while i < rest.len() {
                    if rest[i] == 0 {
                        if i + 1 >= rest.len() {
                            return Err("Truncated null-escape sequence in Bytes".to_string());
                        }
                        if rest[i + 1] == 0 {
                            return Ok((CanonicalLiteral::Bytes(bytes), &rest[i + 2..]));
                        } else if rest[i + 1] == 0xFF {
                            bytes.push(0);
                            i += 2;
                        } else {
                            return Err("Invalid escape byte in Bytes".to_string());
                        }
                    } else {
                        bytes.push(rest[i]);
                        i += 1;
                    }
                }
                Err("Unterminated Bytes in CanonicalLiteral".to_string())
            }
            0x0A => {
                if rest.len() < 4 {
                    return Err("Unexpected EOF decoding Date".to_string());
                }
                let raw = u32::from_be_bytes(rest[..4].try_into().unwrap());
                let val = (raw ^ (1 << 31)) as i32;
                Ok((CanonicalLiteral::Date(val), &rest[4..]))
            }
            0x0B => {
                if rest.len() < 8 {
                    return Err("Unexpected EOF decoding Timestamp".to_string());
                }
                let raw = u64::from_be_bytes(rest[..8].try_into().unwrap());
                let val = (raw ^ (1 << 63)) as i64;
                Ok((CanonicalLiteral::Timestamp(val), &rest[8..]))
            }
            0x0C => {
                if rest.len() < 8 {
                    return Err("Unexpected EOF decoding TimestampTz".to_string());
                }
                let raw = u64::from_be_bytes(rest[..8].try_into().unwrap());
                let val = (raw ^ (1 << 63)) as i64;
                Ok((CanonicalLiteral::TimestampTz(val), &rest[8..]))
            }
            0x0D => {
                if rest.len() < 16 {
                    return Err("Unexpected EOF decoding Interval".to_string());
                }
                let m_raw = u32::from_be_bytes(rest[..4].try_into().unwrap());
                let d_raw = u32::from_be_bytes(rest[4..8].try_into().unwrap());
                let us_raw = u64::from_be_bytes(rest[8..16].try_into().unwrap());
                let months = (m_raw ^ (1 << 31)) as i32;
                let days = (d_raw ^ (1 << 31)) as i32;
                let micros = (us_raw ^ (1 << 63)) as i64;
                Ok((
                    CanonicalLiteral::Interval {
                        months,
                        days,
                        micros,
                    },
                    &rest[16..],
                ))
            }
            0x0E => {
                if rest.len() < 16 {
                    return Err("Unexpected EOF decoding Uuid".to_string());
                }
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&rest[..16]);
                Ok((CanonicalLiteral::Uuid(octets), &rest[16..]))
            }
            0x0F => {
                if rest.len() < 4 {
                    return Err("Unexpected EOF decoding Array count".to_string());
                }
                let count = u32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
                let mut cur = &rest[4..];
                let mut elems = Vec::with_capacity(count);
                for _ in 0..count {
                    let (elem, next_cur) = Self::decode_literal(cur)?;
                    elems.push(elem);
                    cur = next_cur;
                }
                Ok((CanonicalLiteral::Array(elems), cur))
            }
            other => Err(format!("Unknown CanonicalLiteral tag: 0x{:02X}", other)),
        }
    }

    /// Decode a sequence of literals from bytes.
    pub fn decode_key(mut data: &[u8]) -> Result<Vec<CanonicalLiteral>, String> {
        let mut res = Vec::new();
        while !data.is_empty() {
            let (lit, rest) = Self::decode_literal(data)?;
            res.push(lit);
            data = rest;
        }
        Ok(res)
    }
}

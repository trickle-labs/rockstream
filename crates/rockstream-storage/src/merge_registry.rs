//! Merge operator registry for associative aggregates.
//!
//! Provides a `MergeOperatorRegistry` that dispatches to the correct
//! merge function based on a tag byte at the start of values.

use bytes::Bytes;
use rockstream_types::laws::arithmetic::{
    checked_add_i64, checked_add_u64, decode_i64, decode_u64, encode_i64, encode_u64,
};
use rockstream_types::merge_law::LawBundle;
use slatedb::{MergeOperator, MergeOperatorError};

/// Tag byte prepended to values indicating the merge strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MergeTag {
    /// Associative sum: values are i64 (big-endian).
    Sum = 0x01,
    /// Associative count: values are u64 (big-endian).
    Count = 0x02,
    /// Semilattice MAX: values are i64 (big-endian), merged by `max(a, b)`.
    MaxRegister = 0x03,
    /// Semilattice MIN: values are i64 (big-endian), merged by `min(a, b)`.
    MinRegister = 0x04,
    /// Semilattice LWWRegister: timestamp (u64 BE) + value (i64 BE).
    LWWRegister = 0x22,
    /// PNCounter: positive-negative counter, values are i64 (big-endian).
    PNCounter = 0x30,
}

impl MergeTag {
    /// Decode a byte into a known `MergeTag`.
    pub const fn from_u8(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Sum),
            0x02 => Some(Self::Count),
            0x03 => Some(Self::MaxRegister),
            0x04 => Some(Self::MinRegister),
            0x22 => Some(Self::LWWRegister),
            0x30 => Some(Self::PNCounter),
            _ => None,
        }
    }

    /// Expected total wire length (1 byte tag + payload).
    pub const fn expected_len(self) -> usize {
        1 + self.payload_len()
    }

    /// Expected payload length without the tag byte.
    pub const fn payload_len(self) -> usize {
        match self {
            Self::Sum | Self::Count | Self::MaxRegister | Self::MinRegister | Self::PNCounter => 8,
            Self::LWWRegister => 16,
        }
    }

    /// Check whether this storage merge tag is compatible with the given merge law.
    pub fn is_compatible_with_law(self, law: &dyn LawBundle) -> bool {
        let id = law.id().0;
        let name = law.name();
        let base_name = name.split('/').next().unwrap_or(name);
        match self {
            Self::Sum => {
                id == 0x0001
                    || base_name.eq_ignore_ascii_case("WeightAdd")
                    || base_name.eq_ignore_ascii_case("Sum")
            }
            Self::PNCounter => {
                id == 0x0001
                    || id == 10
                    || id == 0x0030
                    || base_name.eq_ignore_ascii_case("WeightAdd")
                    || base_name.eq_ignore_ascii_case("Sum")
                    || base_name.eq_ignore_ascii_case("PNCounter")
                    || base_name.eq_ignore_ascii_case("PN-Counter")
            }
            Self::Count => {
                id == 0x0002
                    || base_name.eq_ignore_ascii_case("Count")
                    || base_name.eq_ignore_ascii_case("SumCount")
            }
            Self::MaxRegister => {
                id == 0x0003
                    || id == 11
                    || id == 0x1001
                    || base_name.eq_ignore_ascii_case("MaxRegister")
                    || base_name.eq_ignore_ascii_case("Max")
            }
            Self::MinRegister => {
                id == 0x0004
                    || base_name.eq_ignore_ascii_case("MinRegister")
                    || base_name.eq_ignore_ascii_case("Min")
            }
            Self::LWWRegister => {
                id == 0x0022
                    || base_name.eq_ignore_ascii_case("LWWRegister")
                    || base_name.eq_ignore_ascii_case("LWW")
            }
        }
    }
}

/// A view of a law operand, either raw or extracted from a tagged storage value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LawOperandView<'a> {
    /// Raw un-tagged operand bytes.
    Raw(&'a [u8]),
    /// Tagged storage merge operand with the tag stripped.
    Tagged {
        tag: MergeTag,
        payload: &'a [u8],
    },
}

/// Error returned when resolving a storage law operand fails.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LawOperandError {
    #[error("RS-3009: incompatible merge tag {tag:?} for law {law_name} ({law_id})")]
    IncompatibleTag {
        tag: MergeTag,
        law_name: String,
        law_id: u16,
    },
    #[error("RS-3009: malformed tagged {tag:?} payload length: expected {expected}, got {actual}")]
    MalformedTaggedLength {
        tag: MergeTag,
        expected: usize,
        actual: usize,
    },
}

/// Resolves a stored byte slice against a merge law into either a raw operand view
/// or a validated tagged operand view with the tag stripped.
pub fn resolve_law_operand<'a>(
    law: &dyn LawBundle,
    bytes: &'a [u8],
) -> Result<LawOperandView<'a>, LawOperandError> {
    let Some(&first) = bytes.first() else {
        return Ok(LawOperandView::Raw(bytes));
    };

    let Some(tag) = MergeTag::from_u8(first) else {
        return Ok(LawOperandView::Raw(bytes));
    };

    if bytes.len() == tag.expected_len() {
        if tag.is_compatible_with_law(law) {
            Ok(LawOperandView::Tagged {
                tag,
                payload: &bytes[1..],
            })
        } else {
            Err(LawOperandError::IncompatibleTag {
                tag,
                law_name: law.name().to_string(),
                law_id: law.id().0,
            })
        }
    } else if tag.is_compatible_with_law(law) {
        if bytes.len() == tag.payload_len() {
            Ok(LawOperandView::Raw(bytes))
        } else {
            Err(LawOperandError::MalformedTaggedLength {
                tag,
                expected: tag.expected_len(),
                actual: bytes.len(),
            })
        }
    } else {
        let expected_raw = law.identity().map(|id| id.len());
        if expected_raw == Some(bytes.len()) {
            Ok(LawOperandView::Raw(bytes))
        } else {
            Err(LawOperandError::IncompatibleTag {
                tag,
                law_name: law.name().to_string(),
                law_id: law.id().0,
            })
        }
    }
}

/// A merge operator that performs associative sum and count operations.
///
/// Value format: `[tag:1][payload:8]` (or 16 for LWWRegister)
/// - Sum tag: payload is i64 big-endian, merged by addition
/// - Count tag: payload is u64 big-endian, merged by addition
///
/// Fail-closed: malformed inputs or tag mismatches return an error
/// (RS-3009 merge.malformed_operand) rather than silently overwriting.
#[derive(Debug)]
pub struct SumCountMergeOperator;

impl MergeOperator for SumCountMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let value_tag = validate_operand(&value)?;
        let Some(existing) = existing_value else {
            return Ok(value);
        };
        let tag = validate_operand(&existing)?;
        if tag != value_tag {
            // Fail-closed: tag mismatch (RS-3009).
            return Err(MergeOperatorError::Callback {
                message: "RS-3009: merge tag mismatch between existing and incoming value".into(),
            });
        }

        match tag {
            t if t == MergeTag::Sum as u8 => {
                let a = decode_i64(&existing[1..]).map_err(malformed_arithmetic)?;
                let b = decode_i64(&value[1..]).map_err(malformed_arithmetic)?;
                let result = checked_add_i64(a, b).map_err(|_| arithmetic_overflow())?;
                let mut out = Vec::with_capacity(9);
                out.push(MergeTag::Sum as u8);
                out.extend_from_slice(&encode_i64(result));
                Ok(Bytes::from(out))
            }
            t if t == MergeTag::Count as u8 => {
                let a = decode_u64(&existing[1..]).map_err(malformed_arithmetic)?;
                let b = decode_u64(&value[1..]).map_err(malformed_arithmetic)?;
                let result = checked_add_u64(a, b).map_err(|_| arithmetic_overflow())?;
                let mut out = Vec::with_capacity(9);
                out.push(MergeTag::Count as u8);
                out.extend_from_slice(&encode_u64(result));
                Ok(Bytes::from(out))
            }
            t if t == MergeTag::MaxRegister as u8 => {
                let a = decode_i64(&existing[1..]).map_err(malformed_arithmetic)?;
                let b = decode_i64(&value[1..]).map_err(malformed_arithmetic)?;
                let result = a.max(b);
                let mut out = Vec::with_capacity(9);
                out.push(MergeTag::MaxRegister as u8);
                out.extend_from_slice(&encode_i64(result));
                Ok(Bytes::from(out))
            }
            t if t == MergeTag::MinRegister as u8 => {
                let a = decode_i64(&existing[1..]).map_err(malformed_arithmetic)?;
                let b = decode_i64(&value[1..]).map_err(malformed_arithmetic)?;
                let result = a.min(b);
                let mut out = Vec::with_capacity(9);
                out.push(MergeTag::MinRegister as u8);
                out.extend_from_slice(&encode_i64(result));
                Ok(Bytes::from(out))
            }
            t if t == MergeTag::PNCounter as u8 => {
                let a = decode_i64(&existing[1..]).map_err(malformed_arithmetic)?;
                let b = decode_i64(&value[1..]).map_err(malformed_arithmetic)?;
                let result = checked_add_i64(a, b).map_err(|_| arithmetic_overflow())?;
                let mut out = Vec::with_capacity(9);
                out.push(MergeTag::PNCounter as u8);
                out.extend_from_slice(&encode_i64(result));
                Ok(Bytes::from(out))
            }
            t if t == MergeTag::LWWRegister as u8 => {
                let ts_a = decode_u64(&existing[1..9]).map_err(malformed_arithmetic)?;
                let val_a = decode_i64(&existing[9..]).map_err(malformed_arithmetic)?;
                let ts_b = decode_u64(&value[1..9]).map_err(malformed_arithmetic)?;
                let val_b = decode_i64(&value[9..]).map_err(malformed_arithmetic)?;

                let (ts_res, val_res) = if ts_b > ts_a {
                    (ts_b, val_b)
                } else if ts_a > ts_b {
                    (ts_a, val_a)
                } else {
                    (ts_a, val_a.max(val_b))
                };

                let mut out = Vec::with_capacity(17);
                out.push(MergeTag::LWWRegister as u8);
                out.extend_from_slice(&encode_u64(ts_res));
                out.extend_from_slice(&encode_i64(val_res));
                Ok(Bytes::from(out))
            }
            _ => {
                // Fail-closed: unknown tag (RS-5002 / RS-3009).
                Err(MergeOperatorError::Callback {
                    message: "RS-5002: unknown merge law tag byte".into(),
                })
            }
        }
    }
}

fn expected_len(tag: u8) -> Option<usize> {
    MergeTag::from_u8(tag).map(|t| t.expected_len())
}

fn validate_operand(value: &[u8]) -> Result<u8, MergeOperatorError> {
    let Some(&tag) = value.first() else {
        return Err(MergeOperatorError::Callback {
            message: "RS-3009: merge operand is empty".into(),
        });
    };
    let Some(expected) = expected_len(tag) else {
        return Err(MergeOperatorError::Callback {
            message: "RS-5002: unknown merge law tag byte".into(),
        });
    };
    if value.len() != expected {
        return Err(MergeOperatorError::Callback {
            message: format!(
                "RS-3009: merge operand width mismatch: expected {expected}, got {}",
                value.len()
            ),
        });
    }
    Ok(tag)
}

fn malformed_arithmetic(
    _: rockstream_types::laws::arithmetic::ArithmeticError,
) -> MergeOperatorError {
    MergeOperatorError::Callback {
        message: "RS-3009: merge operand malformed".into(),
    }
}

fn arithmetic_overflow() -> MergeOperatorError {
    MergeOperatorError::Callback {
        message: "RS-1002: arithmetic overflow during merge".into(),
    }
}

/// Registry for merge operators.
///
/// Currently uses a single `SumCountMergeOperator` that dispatches based on
/// the tag byte. Additional operators can be added by extending the tag space.
pub struct MergeOperatorRegistry;

impl MergeOperatorRegistry {
    /// Encode a sum value for merge operations.
    pub fn encode_sum(value: i64) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push(MergeTag::Sum as u8);
        out.extend_from_slice(&encode_i64(value));
        out
    }

    /// Decode a sum value from merged bytes.
    pub fn decode_sum(data: &[u8]) -> Option<i64> {
        if data.len() != 9 || data[0] != MergeTag::Sum as u8 {
            return None;
        }
        decode_i64(&data[1..]).ok()
    }

    /// Encode a count value for merge operations.
    pub fn encode_count(value: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push(MergeTag::Count as u8);
        out.extend_from_slice(&encode_u64(value));
        out
    }

    /// Decode a count value from merged bytes.
    pub fn decode_count(data: &[u8]) -> Option<u64> {
        if data.len() != 9 || data[0] != MergeTag::Count as u8 {
            return None;
        }
        decode_u64(&data[1..]).ok()
    }

    /// Encode a max-register value for merge operations.
    pub fn encode_max(value: i64) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push(MergeTag::MaxRegister as u8);
        out.extend_from_slice(&encode_i64(value));
        out
    }

    /// Decode a max-register value from merged bytes.
    pub fn decode_max(data: &[u8]) -> Option<i64> {
        if data.len() != 9 || data[0] != MergeTag::MaxRegister as u8 {
            return None;
        }
        decode_i64(&data[1..]).ok()
    }

    /// Encode a min-register value for merge operations.
    pub fn encode_min(value: i64) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push(MergeTag::MinRegister as u8);
        out.extend_from_slice(&encode_i64(value));
        out
    }

    /// Decode a min-register value from merged bytes.
    pub fn decode_min(data: &[u8]) -> Option<i64> {
        if data.len() != 9 || data[0] != MergeTag::MinRegister as u8 {
            return None;
        }
        decode_i64(&data[1..]).ok()
    }

    /// Encode a PNCounter value for merge operations.
    pub fn encode_pn_counter(value: i64) -> Vec<u8> {
        let mut out = Vec::with_capacity(9);
        out.push(MergeTag::PNCounter as u8);
        out.extend_from_slice(&encode_i64(value));
        out
    }

    /// Decode a PNCounter value from merged bytes.
    pub fn decode_pn_counter(data: &[u8]) -> Option<i64> {
        if data.len() != 9 || data[0] != MergeTag::PNCounter as u8 {
            return None;
        }
        decode_i64(&data[1..]).ok()
    }

    /// Encode a LWWRegister value for merge operations.
    pub fn encode_lww_register(ts: u64, value: i64) -> Vec<u8> {
        let mut out = Vec::with_capacity(17);
        out.push(MergeTag::LWWRegister as u8);
        out.extend_from_slice(&encode_u64(ts));
        out.extend_from_slice(&encode_i64(value));
        out
    }

    /// Decode a LWWRegister value from merged bytes.
    pub fn decode_lww_register(data: &[u8]) -> Option<(u64, i64)> {
        if data.len() != 17 || data[0] != MergeTag::LWWRegister as u8 {
            return None;
        }
        let ts = decode_u64(&data[1..9]).ok()?;
        let val = decode_i64(&data[9..]).ok()?;
        Some((ts, val))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> Bytes {
        Bytes::from_static(b"key")
    }

    #[test]
    fn sum_merge_no_existing() {
        let op = SumCountMergeOperator;
        let value = Bytes::from(MergeOperatorRegistry::encode_sum(42));
        let result = op.merge(&key(), None, value).unwrap();
        assert_eq!(MergeOperatorRegistry::decode_sum(&result), Some(42));
    }

    #[test]
    fn sum_merge_addition() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from(MergeOperatorRegistry::encode_sum(10));
        let value = Bytes::from(MergeOperatorRegistry::encode_sum(32));
        let result = op.merge(&key(), Some(existing), value).unwrap();
        assert_eq!(MergeOperatorRegistry::decode_sum(&result), Some(42));
    }

    #[test]
    fn sum_merge_negative() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from(MergeOperatorRegistry::encode_sum(100));
        let value = Bytes::from(MergeOperatorRegistry::encode_sum(-30));
        let result = op.merge(&key(), Some(existing), value).unwrap();
        assert_eq!(MergeOperatorRegistry::decode_sum(&result), Some(70));
    }

    #[test]
    fn count_merge_addition() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from(MergeOperatorRegistry::encode_count(5));
        let value = Bytes::from(MergeOperatorRegistry::encode_count(3));
        let result = op.merge(&key(), Some(existing), value).unwrap();
        assert_eq!(MergeOperatorRegistry::decode_count(&result), Some(8));
    }

    #[test]
    fn tag_mismatch_returns_error() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from(MergeOperatorRegistry::encode_sum(100));
        let value = Bytes::from(MergeOperatorRegistry::encode_count(1));
        let result = op.merge(&key(), Some(existing), value);
        // Fail-closed: tag mismatch is an error (RS-3009).
        assert!(result.is_err());
    }

    #[test]
    fn malformed_existing_returns_error() {
        let op = SumCountMergeOperator;
        let existing = Bytes::from_static(b"short");
        let value = Bytes::from(MergeOperatorRegistry::encode_sum(99));
        let result = op.merge(&key(), Some(existing), value);
        // Fail-closed: malformed operand is an error (RS-3009).
        assert!(result.is_err());
    }

    #[test]
    fn malformed_first_operand_returns_error() {
        let op = SumCountMergeOperator;
        let result = op.merge(&key(), None, Bytes::from_static(&[MergeTag::Sum as u8]));
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "RS-3009: merge operand width mismatch: expected 9, got 1"
        );
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let op = SumCountMergeOperator;
        let mut value = MergeOperatorRegistry::encode_sum(42);
        value.push(0);
        let result = op.merge(&key(), None, Bytes::from(value));
        let error = result.unwrap_err();
        assert_eq!(
            error.to_string(),
            "RS-3009: merge operand width mismatch: expected 9, got 10"
        );
    }

    #[test]
    fn sum_is_associative() {
        let op = SumCountMergeOperator;
        let k = Bytes::from_static(b"k");
        let a = Bytes::from(MergeOperatorRegistry::encode_sum(1));
        let b = Bytes::from(MergeOperatorRegistry::encode_sum(2));
        let c = Bytes::from(MergeOperatorRegistry::encode_sum(3));

        // (a + b) + c
        let ab = op.merge(&k, Some(a.clone()), b.clone()).unwrap();
        let abc_left = op.merge(&k, Some(ab), c.clone()).unwrap();

        // a + (b + c)
        let bc = op.merge(&k, Some(b), c).unwrap();
        let abc_right = op.merge(&k, Some(a), bc).unwrap();

        assert_eq!(abc_left, abc_right);
        assert_eq!(MergeOperatorRegistry::decode_sum(&abc_left), Some(6));
    }

    #[test]
    fn count_is_associative() {
        let op = SumCountMergeOperator;
        let k = Bytes::from_static(b"k");
        let a = Bytes::from(MergeOperatorRegistry::encode_count(10));
        let b = Bytes::from(MergeOperatorRegistry::encode_count(20));
        let c = Bytes::from(MergeOperatorRegistry::encode_count(30));

        let ab = op.merge(&k, Some(a.clone()), b.clone()).unwrap();
        let abc_left = op.merge(&k, Some(ab), c.clone()).unwrap();

        let bc = op.merge(&k, Some(b), c).unwrap();
        let abc_right = op.merge(&k, Some(a), bc).unwrap();

        assert_eq!(abc_left, abc_right);
        assert_eq!(MergeOperatorRegistry::decode_count(&abc_left), Some(60));
    }

    #[test]
    fn merge_tag_from_u8_and_lengths() {
        assert_eq!(MergeTag::from_u8(0x01), Some(MergeTag::Sum));
        assert_eq!(MergeTag::from_u8(0x02), Some(MergeTag::Count));
        assert_eq!(MergeTag::from_u8(0x03), Some(MergeTag::MaxRegister));
        assert_eq!(MergeTag::from_u8(0x04), Some(MergeTag::MinRegister));
        assert_eq!(MergeTag::from_u8(0x22), Some(MergeTag::LWWRegister));
        assert_eq!(MergeTag::from_u8(0x30), Some(MergeTag::PNCounter));
        assert_eq!(MergeTag::from_u8(0x00), None);
        assert_eq!(MergeTag::from_u8(0x99), None);

        for tag in [
            MergeTag::Sum,
            MergeTag::Count,
            MergeTag::MaxRegister,
            MergeTag::MinRegister,
            MergeTag::PNCounter,
        ] {
            assert_eq!(tag.expected_len(), 9);
            assert_eq!(tag.payload_len(), 8);
        }

        assert_eq!(MergeTag::LWWRegister.expected_len(), 17);
        assert_eq!(MergeTag::LWWRegister.payload_len(), 16);
    }

    #[test]
    fn merge_tag_law_compatibility() {
        use rockstream_types::laws::weight_add::WeightAddV1;
        use rockstream_types::laws::sum_count::SumCountV1;

        let weight_law = WeightAddV1;
        let sum_count_law = SumCountV1;

        // Sum and PNCounter are compatible with WeightAdd
        assert!(MergeTag::Sum.is_compatible_with_law(&weight_law));
        assert!(MergeTag::PNCounter.is_compatible_with_law(&weight_law));

        // Count is compatible with SumCount
        assert!(MergeTag::Count.is_compatible_with_law(&sum_count_law));

        // Incompatible mappings
        assert!(!MergeTag::Sum.is_compatible_with_law(&sum_count_law));
        assert!(!MergeTag::Count.is_compatible_with_law(&weight_law));
        assert!(!MergeTag::MaxRegister.is_compatible_with_law(&weight_law));
        assert!(!MergeTag::MinRegister.is_compatible_with_law(&weight_law));
        assert!(!MergeTag::LWWRegister.is_compatible_with_law(&weight_law));
    }

    #[test]
    fn resolve_law_operand_tagged_sum() {
        use rockstream_types::laws::weight_add::WeightAddV1;
        let law = WeightAddV1;

        let tagged = [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07];
        let view = resolve_law_operand(&law, &tagged).unwrap();
        match view {
            LawOperandView::Tagged { tag, payload } => {
                assert_eq!(tag, MergeTag::Sum);
                assert_eq!(payload, &[0, 0, 0, 0, 0, 0, 0, 7]);
            }
            LawOperandView::Raw(_) => panic!("expected tagged view"),
        }
    }

    #[test]
    fn resolve_law_operand_raw_starting_with_tag_byte() {
        use rockstream_types::laws::weight_add::WeightAddV1;
        let law = WeightAddV1;

        // Raw 8-byte operand beginning with 0x01
        let raw = [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05];
        let view = resolve_law_operand(&law, &raw).unwrap();
        match view {
            LawOperandView::Raw(bytes) => {
                assert_eq!(bytes, &raw);
            }
            LawOperandView::Tagged { .. } => panic!("expected raw view"),
        }

        // Raw 8-byte operand beginning with 0x02 (Count tag)
        let raw2 = [0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05];
        let view2 = resolve_law_operand(&law, &raw2).unwrap();
        match view2 {
            LawOperandView::Raw(bytes) => {
                assert_eq!(bytes, &raw2);
            }
            LawOperandView::Tagged { .. } => panic!("expected raw view"),
        }
    }

    #[test]
    fn resolve_law_operand_malformed_tagged_length() {
        use rockstream_types::laws::weight_add::WeightAddV1;
        let law = WeightAddV1;

        // Too short for tagged (and not 8 bytes)
        let short = [0x01, 0x00, 0x00];
        let err = resolve_law_operand(&law, &short).unwrap_err();
        assert_eq!(
            err,
            LawOperandError::MalformedTaggedLength {
                tag: MergeTag::Sum,
                expected: 9,
                actual: 3,
            }
        );

        // Too long (10 bytes)
        let long = [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x99];
        let err_long = resolve_law_operand(&law, &long).unwrap_err();
        assert_eq!(
            err_long,
            LawOperandError::MalformedTaggedLength {
                tag: MergeTag::Sum,
                expected: 9,
                actual: 10,
            }
        );
    }

    #[test]
    fn resolve_law_operand_incompatible_tag() {
        use rockstream_types::laws::weight_add::WeightAddV1;
        let law = WeightAddV1;

        // Tagged Count (9 bytes) passed to WeightAdd
        let tagged_count = [0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05];
        let err = resolve_law_operand(&law, &tagged_count).unwrap_err();
        assert_eq!(
            err,
            LawOperandError::IncompatibleTag {
                tag: MergeTag::Count,
                law_name: "WeightAdd".into(),
                law_id: 1,
            }
        );
    }
}

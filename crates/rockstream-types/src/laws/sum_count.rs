//! `SumCount/v1` — the aggregate merge law for SUM, COUNT, and AVG.
//!
//! This law merges two partial aggregate accumulators by adding both the sum
//! component and the count component. It supports:
//! - `SUM(val)` — the sum field
//! - `COUNT(*)` — the count field
//! - `AVG(val)` — sum / count (computed from the merged accumulator)
//!
//! Because it supports negative weights (Z-set deletions), it is an abelian
//! group: associative, commutative, has inverse, has identity.
//!
//! Wire format: 16 bytes
//! - bytes [0..8]  — i64 sum value, big-endian
//! - bytes [8..16] — i64 count value, big-endian

use super::arithmetic::{
    admit_i64_reassociation, checked_add_i64, checked_neg_i64, decode_i64, encode_i64,
};
use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, LawBundle, LawProperties, MergeLawClass,
    MergeLawId, MergeLawVersion,
};

/// Well-known ID for `SumCount/v1`.
pub const SUM_COUNT_ID: MergeLawId = MergeLawId(0x0002);

/// Well-known version.
pub const SUM_COUNT_VERSION: MergeLawVersion = MergeLawVersion(1);

/// Wire size in bytes for `SumCount/v1`.
pub const SUM_COUNT_WIRE_SIZE: usize = 16;

/// The `SumCount/v1` merge law.
///
/// Stores a `(sum, count)` pair and merges by component-wise addition.
#[derive(Debug, Clone, Copy)]
pub struct SumCountV1;

impl LawBundle for SumCountV1 {
    fn id(&self) -> MergeLawId {
        SUM_COUNT_ID
    }

    fn version(&self) -> MergeLawVersion {
        SUM_COUNT_VERSION
    }

    fn name(&self) -> &'static str {
        "SumCount"
    }

    fn properties(&self) -> LawProperties {
        LawProperties {
            associative: true,
            commutative: true,
            idempotent: false,
            has_inverse: true,
            has_identity: true,
            composable: true,
        }
    }

    fn class(&self) -> MergeLawClass {
        MergeLawClass::AbelianGroup
    }

    fn duplicate_policy(&self) -> DuplicatePolicy {
        DuplicatePolicy::Merge
    }

    fn compaction_policy(&self) -> CompactionPolicy {
        CompactionPolicy::TombstoneGc
    }

    fn frontier_policy(&self) -> FrontierPolicy {
        FrontierPolicy::ExactOnly
    }

    fn identity(&self) -> Option<Vec<u8>> {
        Some(encode_sum_count(0, 0))
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        let (ls, lc) = parse_sum_count(left)?;
        let (rs, rc) = parse_sum_count(right)?;
        let sum = checked_add_i64(ls, rs).map_err(|_| {
            format!(
                "[{}] SumCount sum arithmetic overflow",
                crate::error_code::RS_1016
            )
        })?;
        let count = checked_add_i64(lc, rc).map_err(|_| {
            format!(
                "[{}] SumCount count arithmetic overflow",
                crate::error_code::RS_1016
            )
        })?;
        Ok(encode_sum_count(sum, count))
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        parse_sum_count(value)
            .map(|(s, c)| s == 0 && c == 0)
            .unwrap_or(false)
    }

    fn inverse(&self, value: &[u8]) -> Result<Vec<u8>, String> {
        let (sum, count) = parse_sum_count(value)?;
        let inverse_sum = checked_neg_i64(sum).map_err(|_| {
            format!(
                "[{}] SumCount inverse sum overflow",
                crate::error_code::RS_1016
            )
        })?;
        let inverse_count = checked_neg_i64(count).map_err(|_| {
            format!(
                "[{}] SumCount inverse count overflow",
                crate::error_code::RS_1016
            )
        })?;
        Ok(encode_sum_count(inverse_sum, inverse_count))
    }

    fn admits_reassociation(&self, operands: &[i64]) -> bool {
        admit_i64_reassociation(operands)
    }

    fn domain(&self) -> &'static str {
        "checked i64 sum/count partials; materialized count > 0; regrouping requires an absolute-sum budget"
    }

    fn evidence_ref(&self) -> &'static str {
        "VS1-02/VS1-03/VS1-06: arithmetic kernel, law tests, and VS1 ADR"
    }
}

/// Encode `(sum, count)` to the `SumCount/v1` wire format (16 bytes).
pub fn encode_sum_count(sum: i64, count: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&encode_i64(sum));
    out.extend_from_slice(&encode_i64(count));
    out
}

/// Decode `(sum, count)` from the `SumCount/v1` wire format.
pub fn decode_sum_count(bytes: &[u8]) -> Result<(i64, i64), String> {
    parse_sum_count(bytes)
}

/// Compute the average from an in-memory `(sum, count)` pair.
///
/// Returns `None` if the count is zero (undefined average). Shared by the
/// `SumCount/v1` wire-format helper below and by `AggregateOp`'s live
/// `avg` computation so both paths use identical floating-point semantics.
pub fn avg_from_sum_count(sum: i64, count: i64) -> Option<f64> {
    if count == 0 {
        None
    } else {
        Some(sum as f64 / count as f64)
    }
}

/// Compute the average from a `SumCount/v1` accumulator.
///
/// Returns `None` if the count is zero (undefined average).
pub fn aggregate_avg(bytes: &[u8]) -> Option<f64> {
    let (sum, count) = parse_sum_count(bytes).ok()?;
    avg_from_sum_count(sum, count)
}

fn parse_sum_count(bytes: &[u8]) -> Result<(i64, i64), String> {
    if bytes.len() != SUM_COUNT_WIRE_SIZE {
        return Err(format!(
            "[{}] SumCount: expected {} bytes, got {}. Next steps: inspect the stored merge-law accumulator bytes; possible data corruption or an accumulator wire-format version mismatch.",
            crate::error_code::RS_3501,
            SUM_COUNT_WIRE_SIZE,
            bytes.len()
        ));
    }
    let sum = decode_i64(&bytes[0..8]).map_err(|error| format!("SumCount: {error}"))?;
    let count = decode_i64(&bytes[8..16]).map_err(|error| format!("SumCount: {error}"))?;
    Ok((sum, count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_law::MergeLawClass;

    #[test]
    fn merge_adds_sum_and_count() {
        let law = SumCountV1;
        let a = encode_sum_count(10, 2);
        let b = encode_sum_count(5, 3);
        let result = law.merge(&a, &b).unwrap();
        let (sum, count) = decode_sum_count(&result).unwrap();
        assert_eq!(sum, 15);
        assert_eq!(count, 5);
    }

    #[test]
    fn merge_handles_negative_weights() {
        let law = SumCountV1;
        // Insert 3 rows with val=10, then delete 1 row with val=10
        let insert = encode_sum_count(30, 3);
        let delete = encode_sum_count(-10, -1);
        let result = law.merge(&insert, &delete).unwrap();
        let (sum, count) = decode_sum_count(&result).unwrap();
        assert_eq!(sum, 20);
        assert_eq!(count, 2);
    }

    #[test]
    fn identity_is_zero_zero() {
        let law = SumCountV1;
        let id = law.identity().unwrap();
        assert_eq!(id.len(), 16);
        assert!(law.is_identity(&id));
        assert!(!law.is_identity(&encode_sum_count(1, 0)));
        assert!(!law.is_identity(&encode_sum_count(0, 1)));
    }

    #[test]
    fn merge_with_identity() {
        let law = SumCountV1;
        let a = encode_sum_count(42, 7);
        let id = law.identity().unwrap();
        let result = law.merge(&a, &id).unwrap();
        assert_eq!(result, a);
        let result2 = law.merge(&id, &a).unwrap();
        assert_eq!(result2, a);
    }

    #[test]
    fn avg_computed_correctly() {
        let acc = encode_sum_count(30, 3);
        let avg = aggregate_avg(&acc).unwrap();
        assert!((avg - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn avg_undefined_when_count_zero() {
        let acc = encode_sum_count(0, 0);
        assert!(aggregate_avg(&acc).is_none());
    }

    #[test]
    fn malformed_input_rejected() {
        let law = SumCountV1;
        let bad = vec![0u8; 3];
        assert!(law.merge(&bad, &bad).is_err());
    }

    #[test]
    fn overflow_detected() {
        let law = SumCountV1;
        let max_sum = encode_sum_count(i64::MAX, 0);
        let one = encode_sum_count(1, 0);
        assert!(law.merge(&max_sum, &one).is_err());
    }

    #[test]
    fn inverse_rejects_unrepresentable_minimum() {
        let law = SumCountV1;
        assert!(law.inverse(&encode_sum_count(i64::MIN, 0)).is_err());
        assert_eq!(
            law.inverse(&encode_sum_count(i64::MAX, 1)).unwrap(),
            encode_sum_count(i64::MIN + 1, -1)
        );
    }

    #[test]
    fn regrouping_is_not_claimed_without_an_admitted_domain() {
        assert!(!SumCountV1.admits_reassociation(&[i64::MAX, 1, -1]));
        assert!(SumCountV1.admits_reassociation(&[10, -3, 2]));
    }

    #[test]
    fn properties_are_abelian_group() {
        let law = SumCountV1;
        let props = law.properties();
        assert!(props.associative);
        assert!(props.commutative);
        assert!(!props.idempotent);
        assert!(props.has_inverse);
        assert!(props.has_identity);
        assert_eq!(law.frontier_policy(), FrontierPolicy::ExactOnly);
        assert_eq!(law.class(), MergeLawClass::AbelianGroup);
    }

    #[test]
    fn id_and_name() {
        let law = SumCountV1;
        assert_eq!(law.id(), SUM_COUNT_ID);
        assert_eq!(law.version(), SUM_COUNT_VERSION);
        assert_eq!(law.name(), "SumCount");
    }
}

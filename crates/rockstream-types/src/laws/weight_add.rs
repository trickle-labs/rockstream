//! `WeightAdd/v1` — the fundamental Z-set weight addition law.
//!
//! This is the default law for all Z-set arrangements. It merges operands by
//! adding their i64 weights. The identity element is 0. The law forms an
//! abelian group (associative, commutative, has inverse, has identity).
//!
//! Wire format: 8 bytes, big-endian i64.

use super::arithmetic::{
    admit_i64_reassociation, checked_add_i64, checked_neg_i64, decode_i64, encode_i64,
};
use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, GatewayAggCombinerDesc, LawBundle,
    LawProperties, MergeLawClass, MergeLawId, MergeLawVersion,
};

/// Well-known ID for `WeightAdd/v1`.
pub const WEIGHT_ADD_ID: MergeLawId = MergeLawId(0x0001);

/// Well-known version.
pub const WEIGHT_ADD_VERSION: MergeLawVersion = MergeLawVersion(1);

/// The `WeightAdd/v1` merge law.
#[derive(Debug, Clone, Copy)]
pub struct WeightAddV1;

impl LawBundle for WeightAddV1 {
    fn id(&self) -> MergeLawId {
        WEIGHT_ADD_ID
    }

    fn version(&self) -> MergeLawVersion {
        WEIGHT_ADD_VERSION
    }

    fn name(&self) -> &'static str {
        "WeightAdd"
    }

    fn properties(&self) -> LawProperties {
        LawProperties {
            associative: true,
            commutative: true,
            idempotent: false,
            has_inverse: true,
            has_identity: true,
            composable: false,
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
        // WeightAdd is an abelian group: partial results are always valid,
        // so any frontier advancement may trigger output.
        FrontierPolicy::AnyAdvancement
    }

    fn identity(&self) -> Option<Vec<u8>> {
        Some(encode_i64(0).to_vec())
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        let l = parse_weight(left)?;
        let r = parse_weight(right)?;
        let sum = checked_add_i64(l, r).map_err(|_| {
            format!(
                "[{}] WeightAdd arithmetic overflow",
                crate::error_code::RS_1016
            )
        })?;
        Ok(encode_i64(sum).to_vec())
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        parse_weight(value).map(|w| w == 0).unwrap_or(false)
    }

    fn gateway_combiner(&self) -> Option<GatewayAggCombinerDesc> {
        Some(GatewayAggCombinerDesc {
            law_id: WEIGHT_ADD_ID,
            law_name: "WeightAdd",
            is_associative: true,
            is_commutative: true,
        })
    }

    fn inverse(&self, value: &[u8]) -> Result<Vec<u8>, String> {
        let weight = parse_weight(value)?;
        checked_neg_i64(weight)
            .map(|inverse| encode_i64(inverse).to_vec())
            .map_err(|_| {
                format!(
                    "[{}] WeightAdd inverse overflow",
                    crate::error_code::RS_1016
                )
            })
    }

    fn admits_reassociation(&self, operands: &[i64]) -> bool {
        admit_i64_reassociation(operands)
    }

    fn domain(&self) -> &'static str {
        "checked i64 weights; regrouping requires absolute operand sum <= i64::MAX"
    }

    fn evidence_ref(&self) -> &'static str {
        "VS1-02/VS1-03: arithmetic kernel, law tests, and VS1 ADR"
    }
}

/// Encode a weight as the `WeightAdd/v1` wire format.
pub fn encode_weight(w: i64) -> Vec<u8> {
    encode_i64(w).to_vec()
}

/// Decode a weight from the `WeightAdd/v1` wire format.
pub fn decode_weight(bytes: &[u8]) -> Result<i64, String> {
    parse_weight(bytes)
}

fn parse_weight(bytes: &[u8]) -> Result<i64, String> {
    if bytes.len() != 8 {
        return Err(format!(
            "[{}] WeightAdd: expected 8 bytes, got {}. Next steps: inspect the stored merge-law accumulator bytes; possible data corruption or an accumulator wire-format version mismatch.",
            crate::error_code::RS_3501,
            bytes.len()
        ));
    }
    decode_i64(bytes).map_err(|error| format!("WeightAdd: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_adds_weights() {
        let law = WeightAddV1;
        let a = encode_weight(3);
        let b = encode_weight(5);
        let result = law.merge(&a, &b).unwrap();
        assert_eq!(decode_weight(&result).unwrap(), 8);
    }

    #[test]
    fn merge_handles_negatives() {
        let law = WeightAddV1;
        let a = encode_weight(3);
        let b = encode_weight(-5);
        let result = law.merge(&a, &b).unwrap();
        assert_eq!(decode_weight(&result).unwrap(), -2);
    }

    #[test]
    fn identity_is_zero() {
        let law = WeightAddV1;
        let id = law.identity().unwrap();
        assert!(law.is_identity(&id));
        assert!(!law.is_identity(&encode_weight(1)));
    }

    #[test]
    fn merge_with_identity() {
        let law = WeightAddV1;
        let a = encode_weight(42);
        let id = law.identity().unwrap();
        let result = law.merge(&a, &id).unwrap();
        assert_eq!(decode_weight(&result).unwrap(), 42);
    }

    #[test]
    fn malformed_input_rejected() {
        let law = WeightAddV1;
        let bad = vec![0u8; 3];
        assert!(law.merge(&bad, &bad).is_err());
    }

    #[test]
    fn overflow_detected() {
        let law = WeightAddV1;
        let max = encode_weight(i64::MAX);
        let one = encode_weight(1);
        assert!(law.merge(&max, &one).is_err());
    }

    #[test]
    fn checked_addition_does_not_claim_unrestricted_associativity() {
        let law = WeightAddV1;
        let max = encode_weight(i64::MAX);
        let one = encode_weight(1);
        let minus_one = encode_weight(-1);

        assert!(law.merge(&max, &one).is_err());
        let one_minus_one = law.merge(&one, &minus_one).unwrap();
        assert_eq!(law.merge(&max, &one_minus_one).unwrap(), max);
        assert!(!law.admits_reassociation(&[i64::MAX, 1, -1]));
    }

    #[test]
    fn inverse_rejects_unrepresentable_minimum() {
        let law = WeightAddV1;
        assert_eq!(
            law.inverse(&encode_weight(i64::MAX)).unwrap(),
            encode_weight(i64::MIN + 1)
        );
        assert!(law.inverse(&encode_weight(i64::MIN)).is_err());
    }

    #[test]
    fn properties_are_abelian_group() {
        let law = WeightAddV1;
        let props = law.properties();
        assert!(props.associative);
        assert!(props.commutative);
        assert!(!props.idempotent);
        assert!(props.has_inverse);
        assert!(props.has_identity);
        assert!(!props.composable);
        assert_eq!(law.class(), MergeLawClass::AbelianGroup);
    }
}

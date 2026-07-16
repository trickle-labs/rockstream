//! `WeightAdd/v1` — the fundamental Z-set weight addition law.
//!
//! This is the default law for all Z-set arrangements. It merges operands by
//! adding their i64 weights. The identity element is 0. The law forms an
//! abelian group (associative, commutative, has inverse, has identity).
//!
//! Wire format: 8 bytes, big-endian i64.

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
        Some(0i64.to_be_bytes().to_vec())
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        let l = parse_weight(left)?;
        let r = parse_weight(right)?;
        let sum = l.checked_add(r).ok_or("WeightAdd overflow")?;
        Ok(sum.to_be_bytes().to_vec())
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        parse_weight(value).map(|w| w == 0).unwrap_or(false)
    }

    fn gateway_combiner(&self) -> Option<GatewayAggCombinerDesc> {
        // WeightAdd is an abelian group: associative + commutative, so partial
        // weight sums can be safely pushed to individual shards.
        Some(GatewayAggCombinerDesc {
            law_id: WEIGHT_ADD_ID,
            law_name: "WeightAdd",
            is_associative: true,
            is_commutative: true,
        })
    }
}

/// Encode a weight as the `WeightAdd/v1` wire format.
pub fn encode_weight(w: i64) -> Vec<u8> {
    w.to_be_bytes().to_vec()
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
    let arr: [u8; 8] = bytes.try_into().unwrap();
    Ok(i64::from_be_bytes(arr))
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

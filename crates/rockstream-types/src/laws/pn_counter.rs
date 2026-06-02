//! `PNCounter/v1` — the positive-negative counter merge law.
//!
//! A PNCounter is a non-idempotent operation-CRDT. It merges count updates
//! by adding their signed integer increments.
//!
//! Because it supports signed weights/increments, it forms an abelian group:
//! associative, commutative, has inverse, has identity.
//!
//! Wire format: 8-byte big-endian i64.

use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, GatewayAggCombinerDesc, LawBundle,
    LawProperties, MergeLawClass, MergeLawId, MergeLawVersion,
};

/// Well-known ID for `PNCounter/v1`.
pub const PN_COUNTER_ID: MergeLawId = MergeLawId(0x0006);

/// Well-known version.
pub const PN_COUNTER_VERSION: MergeLawVersion = MergeLawVersion(1);

/// Wire size in bytes for `PNCounter/v1`.
pub const PN_COUNTER_WIRE_SIZE: usize = 8;

/// The `PNCounter/v1` merge law.
#[derive(Debug, Clone, Copy)]
pub struct PNCounterV1;

impl LawBundle for PNCounterV1 {
    fn id(&self) -> MergeLawId {
        PN_COUNTER_ID
    }

    fn version(&self) -> MergeLawVersion {
        PN_COUNTER_VERSION
    }

    fn name(&self) -> &'static str {
        "PNCounter"
    }

    fn properties(&self) -> LawProperties {
        LawProperties {
            associative: true,
            commutative: true,
            idempotent: false,
            has_inverse: true,
            has_identity: true,
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
        FrontierPolicy::AnyAdvancement
    }

    fn identity(&self) -> Option<Vec<u8>> {
        Some(0i64.to_be_bytes().to_vec())
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        let l = parse_i64(left)?;
        let r = parse_i64(right)?;
        let sum = l.checked_add(r).ok_or("PNCounter: overflow")?;
        Ok(sum.to_be_bytes().to_vec())
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        parse_i64(value).map(|v| v == 0).unwrap_or(false)
    }

    fn gateway_combiner(&self) -> Option<GatewayAggCombinerDesc> {
        Some(GatewayAggCombinerDesc {
            law_id: PN_COUNTER_ID,
            law_name: "PNCounter",
            is_associative: true,
            is_commutative: true,
        })
    }
}

/// Encode a counter value as `PNCounter/v1` wire format.
pub fn encode_pn_counter(w: i64) -> Vec<u8> {
    w.to_be_bytes().to_vec()
}

/// Decode a counter value from `PNCounter/v1` wire format.
pub fn decode_pn_counter(bytes: &[u8]) -> Result<i64, String> {
    parse_i64(bytes)
}

fn parse_i64(bytes: &[u8]) -> Result<i64, String> {
    if bytes.len() != PN_COUNTER_WIRE_SIZE {
        return Err(format!("PNCounter: expected 8 bytes, got {}", bytes.len()));
    }
    let arr: [u8; 8] = bytes.try_into().unwrap();
    Ok(i64::from_be_bytes(arr))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_adds_values() {
        let law = PNCounterV1;
        let a = encode_pn_counter(5);
        let b = encode_pn_counter(7);
        let result = law.merge(&a, &b).unwrap();
        assert_eq!(decode_pn_counter(&result).unwrap(), 12);
    }

    #[test]
    fn merge_handles_negatives() {
        let law = PNCounterV1;
        let a = encode_pn_counter(10);
        let b = encode_pn_counter(-3);
        let result = law.merge(&a, &b).unwrap();
        assert_eq!(decode_pn_counter(&result).unwrap(), 7);
    }

    #[test]
    fn identity_is_zero() {
        let law = PNCounterV1;
        let id = law.identity().unwrap();
        assert!(law.is_identity(&id));
        assert!(!law.is_identity(&encode_pn_counter(1)));
    }
}

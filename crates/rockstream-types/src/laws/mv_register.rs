//! `MVRegister/v1` — the Multi-Value Register merge law.
//!
//! An MV-Register maintains a set of concurrent values. Merges keep
//! all values that share the highest timestamp.
//!
//! Wire format:
//! [count: u32 BE] [timestamp: u64 BE, value: i64 BE] × count
//!
//! Total size: 4 + 16 × count bytes.

use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, GatewayAggCombinerDesc, LawBundle,
    LawProperties, MergeLawClass, MergeLawId, MergeLawVersion,
};

/// Well-known ID for `MVRegister/v1`.
pub const MV_REGISTER_ID: MergeLawId = MergeLawId(0x0008);

/// Well-known version.
pub const MV_REGISTER_VERSION: MergeLawVersion = MergeLawVersion(1);

/// A (timestamp, value) pair stored in an MV-Register arrangement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MVRegisterPair {
    pub timestamp: u64,
    pub value: i64,
}

/// The `MVRegister/v1` merge law.
#[derive(Debug, Clone, Copy)]
pub struct MVRegisterV1;

impl LawBundle for MVRegisterV1 {
    fn id(&self) -> MergeLawId {
        MV_REGISTER_ID
    }

    fn version(&self) -> MergeLawVersion {
        MV_REGISTER_VERSION
    }

    fn name(&self) -> &'static str {
        "MVRegister"
    }

    fn properties(&self) -> LawProperties {
        LawProperties {
            associative: true,
            commutative: true,
            idempotent: true,
            has_inverse: false,
            has_identity: true,
        }
    }

    fn class(&self) -> MergeLawClass {
        MergeLawClass::Semilattice
    }

    fn duplicate_policy(&self) -> DuplicatePolicy {
        DuplicatePolicy::Merge
    }

    fn compaction_policy(&self) -> CompactionPolicy {
        CompactionPolicy::MergeOnCompact
    }

    fn frontier_policy(&self) -> FrontierPolicy {
        FrontierPolicy::AnyAdvancement
    }

    fn identity(&self) -> Option<Vec<u8>> {
        Some(encode_mv_register(&[]))
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        let mut left_pairs = decode_mv_register(left)?;
        let right_pairs = decode_mv_register(right)?;

        left_pairs.extend_from_slice(&right_pairs);

        if left_pairs.is_empty() {
            return Ok(encode_mv_register(&[]));
        }

        // Find the maximum timestamp.
        let max_ts = left_pairs.iter().map(|p| p.timestamp).max().unwrap();

        // Keep only pairs that match the maximum timestamp.
        left_pairs.retain(|p| p.timestamp == max_ts);
        left_pairs.sort_unstable();
        left_pairs.dedup();

        Ok(encode_mv_register(&left_pairs))
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        decode_mv_register(value)
            .map(|pairs| pairs.is_empty())
            .unwrap_or(false)
    }

    fn gateway_combiner(&self) -> Option<GatewayAggCombinerDesc> {
        Some(GatewayAggCombinerDesc {
            law_id: MV_REGISTER_ID,
            law_name: "MVRegister",
            is_associative: true,
            is_commutative: true,
        })
    }
}

/// Encode MV-Register pairs into wire format.
pub fn encode_mv_register(pairs: &[MVRegisterPair]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + 16 * pairs.len());
    buf.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
    for p in pairs {
        buf.extend_from_slice(&p.timestamp.to_be_bytes());
        buf.extend_from_slice(&p.value.to_be_bytes());
    }
    buf
}

/// Decode wire bytes into MV-Register pairs.
pub fn decode_mv_register(bytes: &[u8]) -> Result<Vec<MVRegisterPair>, String> {
    if bytes.len() < 4 {
        return Err(format!("MVRegister: need ≥ 4 bytes, got {}", bytes.len()));
    }
    let count = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let expected = 4 + 16 * count;
    if bytes.len() != expected {
        return Err(format!(
            "MVRegister: expected {} bytes for count={}, got {}",
            expected,
            count,
            bytes.len()
        ));
    }
    let mut pairs = Vec::with_capacity(count);
    for i in 0..count {
        let off = 4 + 16 * i;
        let timestamp = u64::from_be_bytes(bytes[off..off + 8].try_into().unwrap());
        let value = i64::from_be_bytes(bytes[off + 8..off + 16].try_into().unwrap());
        pairs.push(MVRegisterPair { timestamp, value });
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(ts: u64, val: i64) -> MVRegisterPair {
        MVRegisterPair { timestamp: ts, value: val }
    }

    #[test]
    fn identity_is_empty() {
        let law = MVRegisterV1;
        let id = law.identity().unwrap();
        assert!(law.is_identity(&id));
        assert_eq!(decode_mv_register(&id).unwrap(), vec![]);
    }

    #[test]
    fn merge_keeps_latest_timestamp_only() {
        let law = MVRegisterV1;
        let a = encode_mv_register(&[pair(10, 42)]);
        let b = encode_mv_register(&[pair(20, 99)]);
        let merged = law.merge(&a, &b).unwrap();
        let result = decode_mv_register(&merged).unwrap();
        assert_eq!(result, vec![pair(20, 99)]);
    }

    #[test]
    fn merge_keeps_concurrent_on_tie() {
        let law = MVRegisterV1;
        let a = encode_mv_register(&[pair(20, 42)]);
        let b = encode_mv_register(&[pair(20, 99)]);
        let merged = law.merge(&a, &b).unwrap();
        let result = decode_mv_register(&merged).unwrap();
        assert_eq!(result, vec![pair(20, 42), pair(20, 99)]);
    }

    #[test]
    fn merge_idempotent() {
        let law = MVRegisterV1;
        let a = encode_mv_register(&[pair(20, 42), pair(20, 99)]);
        let merged = law.merge(&a, &a).unwrap();
        assert_eq!(merged, a);
    }
}

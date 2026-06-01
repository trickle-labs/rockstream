//! `LWWRegister/v1` — the Last-Writer-Wins register merge law.
//!
//! A Last-Writer-Wins register is a semilattice CRDT where each write carries
//! a timestamp. Merges keep the value with the higher timestamp.
//!
//! Wire format: 16 bytes:
//! - bytes [0..8]  — u64 timestamp, big-endian
//! - bytes [8..16] — i64 value, big-endian

use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, GatewayAggCombinerDesc, LawBundle,
    LawProperties, MergeLawClass, MergeLawId, MergeLawVersion,
};

/// Well-known ID for `LWWRegister/v1`.
pub const LWW_REGISTER_ID: MergeLawId = MergeLawId(0x0005);

/// Well-known version.
pub const LWW_REGISTER_VERSION: MergeLawVersion = MergeLawVersion(1);

/// Wire size in bytes for `LWWRegister/v1`.
pub const LWW_REGISTER_WIRE_SIZE: usize = 16;

/// The `LWWRegister/v1` merge law.
#[derive(Debug, Clone, Copy)]
pub struct LWWRegisterV1;

impl LawBundle for LWWRegisterV1 {
    fn id(&self) -> MergeLawId {
        LWW_REGISTER_ID
    }

    fn version(&self) -> MergeLawVersion {
        LWW_REGISTER_VERSION
    }

    fn name(&self) -> &'static str {
        "LWWRegister"
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
        Some(encode_lww_register(0, 0))
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        let (ts_l, val_l) = parse_lww_register(left)?;
        let (ts_r, val_r) = parse_lww_register(right)?;

        let (ts_res, val_res) = if ts_r > ts_l {
            (ts_r, val_r)
        } else if ts_l > ts_r {
            (ts_l, val_l)
        } else {
            (ts_l, val_l.max(val_r))
        };

        Ok(encode_lww_register(ts_res, val_res))
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        parse_lww_register(value)
            .map(|(ts, val)| ts == 0 && val == 0)
            .unwrap_or(false)
    }

    fn gateway_combiner(&self) -> Option<GatewayAggCombinerDesc> {
        Some(GatewayAggCombinerDesc {
            law_id: LWW_REGISTER_ID,
            law_name: "LWWRegister",
            is_associative: true,
            is_commutative: true,
        })
    }
}

/// Encode `(timestamp, value)` to `LWWRegister/v1` wire format.
pub fn encode_lww_register(timestamp: u64, value: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&timestamp.to_be_bytes());
    out.extend_from_slice(&value.to_be_bytes());
    out
}

/// Decode `(timestamp, value)` from `LWWRegister/v1` wire format.
pub fn decode_lww_register(bytes: &[u8]) -> Result<(u64, i64), String> {
    parse_lww_register(bytes)
}

fn parse_lww_register(bytes: &[u8]) -> Result<(u64, i64), String> {
    if bytes.len() != LWW_REGISTER_WIRE_SIZE {
        return Err(format!(
            "LWWRegister: expected {} bytes, got {}",
            LWW_REGISTER_WIRE_SIZE,
            bytes.len()
        ));
    }
    let timestamp = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let value = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
    Ok((timestamp, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_keeps_latest_timestamp() {
        let law = LWWRegisterV1;
        let a = encode_lww_register(10, 42);
        let b = encode_lww_register(20, 99);
        let result = law.merge(&a, &b).unwrap();
        let (ts, val) = decode_lww_register(&result).unwrap();
        assert_eq!(ts, 20);
        assert_eq!(val, 99);
    }

    #[test]
    fn merge_resolves_ties_deterministically() {
        let law = LWWRegisterV1;
        let a = encode_lww_register(10, 100);
        let b = encode_lww_register(10, 200);
        let result = law.merge(&a, &b).unwrap();
        let (ts, val) = decode_lww_register(&result).unwrap();
        assert_eq!(ts, 10);
        assert_eq!(val, 200);
    }

    #[test]
    fn identity_is_zero_zero() {
        let law = LWWRegisterV1;
        let id = law.identity().unwrap();
        assert!(law.is_identity(&id));
    }
}

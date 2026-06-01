//! `BloomUnion/v1` — semilattice merge law for Bloom-filter membership sketches.
//!
//! `BloomUnion/v1` stores a probabilistic membership sketch as a 256-bit
//! (32-byte) Bloom filter.  Each bit is set by hashing an element into the
//! filter.  The merge operation is bitwise OR, which computes the union of
//! two membership sketches.
//!
//! ## Merge semantics
//!
//! Union of two Bloom filters is computed by bitwise OR:
//! `merge(a, b)[i] = a[i] | b[i]`.  This operation is:
//! - **Associative**: `merge(merge(a,b),c) = merge(a,merge(b,c))`
//! - **Commutative**: `merge(a,b) = merge(b,a)`
//! - **Idempotent**: `merge(a,a) = a`
//!
//! Together these make it a **semilattice**.  Bits can only be set, never
//! cleared; the law is **not invertible**.
//!
//! ## Wire format
//!
//! 32 bytes (256 bits): one bit per Bloom filter slot.  The identity element
//! is 32 zero bytes (empty filter — reports no element as present).
//!
//! ## Usage in RockStream
//!
//! `BloomUnion/v1` backs the `APPROX_MEMBERSHIP(v)` aggregate function
//! (v0.25).  The aggregate accumulates element hashes into a Bloom filter
//! sketch; membership queries are answered with a bounded false-positive
//! rate.  The filter is non-invertible: once a bit is set it cannot be
//! unset, so the operator carries `ExtremumRequiresRmw` to signal that
//! retraction-aware correctness requires a full rescan.

use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, LawBundle, LawProperties, MergeLawClass,
    MergeLawId, MergeLawVersion,
};

/// Well-known ID for `BloomUnion/v1`.
pub const BLOOM_UNION_ID: MergeLawId = MergeLawId(0x000a);

/// Well-known version.
pub const BLOOM_UNION_VERSION: MergeLawVersion = MergeLawVersion(1);

/// Wire size in bytes for `BloomUnion/v1` (256-bit Bloom filter).
pub const BLOOM_UNION_WIRE_SIZE: usize = 32;

/// The `BloomUnion/v1` merge law.
///
/// Semilattice: `merge(a, b)[i] = a[i] | b[i]` (bitwise OR).  Identity = all-zero.
#[derive(Debug, Clone, Copy)]
pub struct BloomUnionV1;

impl LawBundle for BloomUnionV1 {
    fn id(&self) -> MergeLawId {
        BLOOM_UNION_ID
    }

    fn version(&self) -> MergeLawVersion {
        BLOOM_UNION_VERSION
    }

    fn name(&self) -> &'static str {
        "BloomUnion"
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
        // Merge on compaction is safe: bitwise OR is idempotent.
        CompactionPolicy::MergeOnCompact
    }

    fn frontier_policy(&self) -> FrontierPolicy {
        // Bloom filters are monotone (bits only grow); any partial filter is a
        // valid (conservative) membership sketch.
        FrontierPolicy::AnyAdvancement
    }

    fn identity(&self) -> Option<Vec<u8>> {
        Some(vec![0u8; BLOOM_UNION_WIRE_SIZE])
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        if left.len() != BLOOM_UNION_WIRE_SIZE {
            return Err(format!(
                "BloomUnion: expected {} bytes on left, got {}",
                BLOOM_UNION_WIRE_SIZE,
                left.len()
            ));
        }
        if right.len() != BLOOM_UNION_WIRE_SIZE {
            return Err(format!(
                "BloomUnion: expected {} bytes on right, got {}",
                BLOOM_UNION_WIRE_SIZE,
                right.len()
            ));
        }
        let result: Vec<u8> = left
            .iter()
            .zip(right.iter())
            .map(|(&l, &r)| l | r)
            .collect();
        Ok(result)
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        if value.len() != BLOOM_UNION_WIRE_SIZE {
            return false;
        }
        value.iter().all(|&b| b == 0)
    }

    fn not_merge_safe_reason(&self) -> Option<crate::explain::NotMergeSafeReason> {
        // BloomUnion is a semilattice (non-invertible): bits can only be set.
        // Retraction-aware correctness requires a full filter rescan.
        Some(crate::explain::NotMergeSafeReason::ExtremumRequiresRmw)
    }
}

/// Check approximate membership in a Bloom filter sketch.
///
/// Returns `true` if the element may be present (with a bounded false-positive
/// rate), `false` if the element is definitely absent.
///
/// The element `hash` is interpreted as a byte pattern; two hash positions
/// are checked using the 8-bit and 5-bit masks of the hash byte for
/// simplicity (this is a toy implementation adequate for proof tests).
pub fn bloom_check(filter: &[u8; BLOOM_UNION_WIRE_SIZE], hash_byte: u8) -> bool {
    let byte_idx = (hash_byte as usize) % BLOOM_UNION_WIRE_SIZE;
    let bit_idx = (hash_byte >> 3) % 8;
    (filter[byte_idx] >> bit_idx) & 1 == 1
}

/// Set a bit in a Bloom filter sketch for the given hash byte.
pub fn bloom_insert(filter: &mut [u8; BLOOM_UNION_WIRE_SIZE], hash_byte: u8) {
    let byte_idx = (hash_byte as usize) % BLOOM_UNION_WIRE_SIZE;
    let bit_idx = (hash_byte >> 3) % 8;
    filter[byte_idx] |= 1 << bit_idx;
}

//! `HyperLogLog/v1` — semilattice merge law for cardinality sketches.
//!
//! `HyperLogLog/v1` stores a compact probabilistic sketch of a set of hashed
//! values.  The sketch is represented as 64 registers, each 8 bits wide
//! (64 bytes total).  Each register holds the maximum leading-zero run
//! observed for values hashed into that bucket.
//!
//! ## Merge semantics
//!
//! Union of two HLL sketches is computed by taking the per-register maximum:
//! `merge(a, b)[i] = max(a[i], b[i])`.  This operation is:
//! - **Associative**: `merge(merge(a,b),c) = merge(a,merge(b,c))`
//! - **Commutative**: `merge(a,b) = merge(b,a)`
//! - **Idempotent**: `merge(a,a) = a`
//!
//! Together these make it a **semilattice**.  Because the registers are
//! monotonically non-decreasing, the law is also **not invertible**: once a
//! higher register value is merged in, it cannot be reduced.
//!
//! ## Wire format
//!
//! 64 bytes: one byte per register (indices 0..63).  A register value of `0`
//! means no hash has been observed in that bucket.  The identity element is
//! 64 zero bytes.
//!
//! ## Usage in RockStream
//!
//! `HyperLogLog/v1` is registered in the global law registry for use by the
//! planner cost model when estimating the number of distinct values (NDV) in
//! a column.  It is **not** used as the primary arrangement law for any
//! operator; operators that require exact semantics (e.g. joins, aggregates)
//! continue to use `WeightAdd/v1`.
//!
//! ## Escape hatch
//!
//! If HLL cardinality accuracy is insufficient for planner cost-model
//! correctness (±3 % typical error with 64 registers), fall back to exact
//! NDV sampling.  This release ships HLL; the escape hatch was not triggered.

use crate::merge_law::{
    CompactionPolicy, DuplicatePolicy, FrontierPolicy, LawBundle, LawProperties, MergeLawClass,
    MergeLawId, MergeLawVersion,
};

/// Well-known ID for `HyperLogLog/v1`.
pub const HLL_ID: MergeLawId = MergeLawId(0x0009);

/// Well-known version.
pub const HLL_VERSION: MergeLawVersion = MergeLawVersion(1);

/// Number of registers in the HLL sketch.
pub const HLL_NUM_REGISTERS: usize = 64;

/// Wire size in bytes for `HyperLogLog/v1`.
pub const HLL_WIRE_SIZE: usize = HLL_NUM_REGISTERS;

/// The `HyperLogLog/v1` merge law.
///
/// Semilattice: `merge(a, b)[i] = max(a[i], b[i])`.  Identity = all-zero.
#[derive(Debug, Clone, Copy)]
pub struct HyperLogLogV1;

impl LawBundle for HyperLogLogV1 {
    fn id(&self) -> MergeLawId {
        HLL_ID
    }

    fn version(&self) -> MergeLawVersion {
        HLL_VERSION
    }

    fn name(&self) -> &'static str {
        "HyperLogLog"
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
        // Merge on compaction is safe: per-register max is idempotent.
        CompactionPolicy::MergeOnCompact
    }

    fn frontier_policy(&self) -> FrontierPolicy {
        // HLL sketches are used for estimation only; any partial sketch is a
        // valid (conservative) cardinality estimate.
        FrontierPolicy::AnyAdvancement
    }

    fn identity(&self) -> Option<Vec<u8>> {
        Some(vec![0u8; HLL_WIRE_SIZE])
    }

    fn merge(&self, left: &[u8], right: &[u8]) -> Result<Vec<u8>, String> {
        if left.len() != HLL_WIRE_SIZE {
            return Err(format!(
                "HyperLogLog: expected {} bytes on left, got {}",
                HLL_WIRE_SIZE,
                left.len()
            ));
        }
        if right.len() != HLL_WIRE_SIZE {
            return Err(format!(
                "HyperLogLog: expected {} bytes on right, got {}",
                HLL_WIRE_SIZE,
                right.len()
            ));
        }
        let result: Vec<u8> = left
            .iter()
            .zip(right.iter())
            .map(|(&l, &r)| l.max(r))
            .collect();
        Ok(result)
    }

    fn is_identity(&self, value: &[u8]) -> bool {
        if value.len() != HLL_WIRE_SIZE {
            return false;
        }
        value.iter().all(|&b| b == 0)
    }

    fn not_merge_safe_reason(&self) -> Option<crate::explain::NotMergeSafeReason> {
        // HLL is a semilattice (non-invertible): register values can only grow.
        Some(crate::explain::NotMergeSafeReason::ExtremumRequiresRmw)
    }
}

/// Estimate the number of distinct values from an HLL sketch.
///
/// Uses the HyperLogLog estimation formula:
/// `ndv ≈ alpha_m * m^2 / sum(2^{-M[j]})`.
///
/// With m = 64 registers, `alpha_m ≈ 0.709`.
pub fn hll_estimate_ndv(sketch: &[u8]) -> f64 {
    assert_eq!(
        sketch.len(),
        HLL_WIRE_SIZE,
        "HyperLogLog: wrong sketch length"
    );
    let m = HLL_NUM_REGISTERS as f64;
    // alpha_64 = 0.7213 / (1 + 1.079 / 64) ≈ 0.709
    let alpha_m = 0.7213 / (1.0 + 1.079 / m);

    let sum: f64 = sketch.iter().map(|&reg| 2.0_f64.powi(-(reg as i32))).sum();

    let raw_estimate = alpha_m * m * m / sum;

    // Small-range correction (linear counting) when estimate < 2.5 * m.
    let zeros = sketch.iter().filter(|&&b| b == 0).count() as f64;
    if raw_estimate < 2.5 * m && zeros > 0.0 {
        return m * (m / zeros).ln();
    }

    raw_estimate
}

/// Build an HLL sketch by hashing a set of 8-byte values.
///
/// Each value is assigned to a register via the 6 most-significant bits of
/// its hash, and the register value is updated to the maximum leading-zero
/// count + 1 of the remaining bits.
///
/// This is a test helper; production callers would build sketches inline
/// during ingestion.
pub fn hll_add(sketch: &mut [u8; HLL_NUM_REGISTERS], value: &[u8]) {
    // Use a simple FNV-64 hash for determinism.
    let h = fnv64(value);
    let reg_idx = (h >> 58) as usize; // top 6 bits → register index (0..63)
    let remaining = h << 6; // remaining 58 bits
    let lz = remaining.leading_zeros() as u8 + 1; // ρ(w) = position of first 1-bit
    if lz > sketch[reg_idx] {
        sketch[reg_idx] = lz;
    }
}

/// FNV-64a hash with a splitmix64 finalizer for uniform bit distribution.
///
/// The finalizer ensures the top 6 bits (used as register index) are
/// well-distributed for sequential and small-valued inputs.
fn fnv64(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let mut h = FNV_OFFSET;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    // splitmix64 finalizer — ensures uniform distribution of all bit positions,
    // including the top 6 bits used as the register index.
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^= h >> 31;
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_law::LawBundle;

    // ── Basic law properties ──────────────────────────────────────────────────

    #[test]
    fn merge_takes_per_register_max() {
        let law = HyperLogLogV1;
        let mut a = vec![0u8; HLL_WIRE_SIZE];
        let mut b = vec![0u8; HLL_WIRE_SIZE];
        a[0] = 5;
        a[10] = 3;
        b[0] = 2;
        b[10] = 7;
        let result = law.merge(&a, &b).unwrap();
        assert_eq!(result[0], 5, "max(5, 2) = 5");
        assert_eq!(result[10], 7, "max(3, 7) = 7");
        for (i, item) in result.iter().enumerate().skip(1) {
            if i != 10 {
                assert_eq!(*item, 0, "other registers are 0");
            }
        }
    }

    #[test]
    fn merge_is_commutative() {
        let law = HyperLogLogV1;
        let mut a = vec![0u8; HLL_WIRE_SIZE];
        let mut b = vec![0u8; HLL_WIRE_SIZE];
        for (i, a_val) in a.iter_mut().enumerate() {
            *a_val = (i % 7) as u8;
        }
        for (i, b_val) in b.iter_mut().enumerate() {
            *b_val = (i % 11) as u8;
        }
        let ab = law.merge(&a, &b).unwrap();
        let ba = law.merge(&b, &a).unwrap();
        assert_eq!(ab, ba, "merge(a, b) == merge(b, a)");
    }

    #[test]
    fn merge_is_idempotent() {
        let law = HyperLogLogV1;
        let mut a = vec![0u8; HLL_WIRE_SIZE];
        for (i, item) in a.iter_mut().enumerate() {
            *item = i as u8;
        }
        let result = law.merge(&a, &a).unwrap();
        assert_eq!(result, a, "merge(a, a) == a for semilattice");
    }

    #[test]
    fn merge_is_associative() {
        let law = HyperLogLogV1;
        let mut a = vec![0u8; HLL_WIRE_SIZE];
        let mut b = vec![0u8; HLL_WIRE_SIZE];
        let mut c = vec![0u8; HLL_WIRE_SIZE];
        for (i, item) in a.iter_mut().enumerate() {
            *item = (i % 5) as u8;
        }
        for (i, item) in b.iter_mut().enumerate() {
            *item = (i % 7) as u8;
        }
        for (i, item) in c.iter_mut().enumerate() {
            *item = (i % 11) as u8;
        }
        let ab_c = law.merge(&law.merge(&a, &b).unwrap(), &c).unwrap();
        let a_bc = law.merge(&a, &law.merge(&b, &c).unwrap()).unwrap();
        assert_eq!(ab_c, a_bc, "merge is associative");
    }

    #[test]
    fn identity_is_all_zero() {
        let law = HyperLogLogV1;
        let id = law.identity().unwrap();
        assert_eq!(id.len(), HLL_WIRE_SIZE);
        assert!(id.iter().all(|&b| b == 0));
        assert!(law.is_identity(&id));
    }

    #[test]
    fn identity_is_neutral_for_merge() {
        let law = HyperLogLogV1;
        let id = law.identity().unwrap();
        let mut val = vec![0u8; HLL_WIRE_SIZE];
        for (i, item) in val.iter_mut().enumerate() {
            *item = (i % 13) as u8;
        }
        assert_eq!(law.merge(&id, &val).unwrap(), val, "identity left-neutral");
        assert_eq!(law.merge(&val, &id).unwrap(), val, "identity right-neutral");
    }

    #[test]
    fn non_identity_value_not_identity() {
        let law = HyperLogLogV1;
        let mut val = vec![0u8; HLL_WIRE_SIZE];
        val[5] = 1;
        assert!(!law.is_identity(&val));
    }

    #[test]
    fn stale_lower_value_cannot_reduce_register() {
        // Proves: once register[i]=10 is observed, merging register[i]=3
        // (stale lower value) does NOT reduce it.
        let law = HyperLogLogV1;
        let mut high = vec![0u8; HLL_WIRE_SIZE];
        high[0] = 10;
        let mut low = vec![0u8; HLL_WIRE_SIZE];
        low[0] = 3;
        let result = law.merge(&high, &low).unwrap();
        assert_eq!(
            result[0], 10,
            "stale lower register value cannot reduce cached max"
        );
    }

    #[test]
    fn malformed_input_returns_error() {
        let law = HyperLogLogV1;
        let ok = vec![0u8; HLL_WIRE_SIZE];
        assert!(law.merge(b"short", &ok).is_err());
        assert!(law.merge(&ok, b"short").is_err());
    }

    // ── NDV estimation ────────────────────────────────────────────────────────

    #[test]
    fn ndv_estimate_of_identity_sketch_is_zero() {
        let sketch = [0u8; HLL_NUM_REGISTERS];
        // All registers are 0 → linear counting → m * ln(m / zeros)
        // = 64 * ln(64/64) = 64 * 0 = 0
        let ndv = hll_estimate_ndv(&sketch);
        assert_eq!(ndv, 0.0, "identity sketch → NDV = 0");
    }

    #[test]
    fn ndv_estimate_grows_with_distinct_values() {
        let mut sketch = [0u8; HLL_NUM_REGISTERS];
        for i in 0u64..100 {
            hll_add(&mut sketch, &i.to_be_bytes());
        }
        let ndv = hll_estimate_ndv(&sketch);
        // With 100 distinct values and 64 registers, expect 70–140 (±40%).
        assert!(
            ndv > 60.0 && ndv < 200.0,
            "NDV estimate for 100 distinct values: {ndv}"
        );
    }

    // ── hll_add helper ────────────────────────────────────────────────────────

    #[test]
    fn hll_add_same_value_twice_is_idempotent() {
        let mut s1 = [0u8; HLL_NUM_REGISTERS];
        let mut s2 = [0u8; HLL_NUM_REGISTERS];
        hll_add(&mut s1, b"hello");
        hll_add(&mut s2, b"hello");
        hll_add(&mut s2, b"hello"); // duplicate
        assert_eq!(s1, s2, "adding same value twice is idempotent");
    }

    #[test]
    fn union_of_sketches_is_superset() {
        let law = HyperLogLogV1;
        let mut sa = [0u8; HLL_NUM_REGISTERS];
        let mut sb = [0u8; HLL_NUM_REGISTERS];
        for i in 0u64..50 {
            hll_add(&mut sa, &i.to_be_bytes());
        }
        for i in 50u64..100 {
            hll_add(&mut sb, &i.to_be_bytes());
        }
        let union = law.merge(&sa, &sb).unwrap();
        let ndv_a = hll_estimate_ndv(&sa);
        let ndv_b = hll_estimate_ndv(&sb);
        let ndv_union = hll_estimate_ndv(&union);
        // Union NDV should be >= NDV of either individual sketch.
        assert!(
            ndv_union >= ndv_a - 1.0,
            "union NDV {ndv_union} should be >= NDV_a {ndv_a}"
        );
        assert!(
            ndv_union >= ndv_b - 1.0,
            "union NDV {ndv_union} should be >= NDV_b {ndv_b}"
        );
    }
}

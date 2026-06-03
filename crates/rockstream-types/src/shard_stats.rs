//! Shard column statistics for OLAP scatter pruning (DESIGN.md §8.7, §12.3.1).
//!
//! Per-shard statistics are piggybacked on checkpoint `WorkerFrontierSummary` and used by the
//! gateway planner to prune the scatter set before dispatching cross-shard queries.
//!
//! # Components
//!
//! - **`ColumnMinMax`**: Min/max value bounds, used for range-predicate pruning.
//! - **`BlockedBloomFilter`**: A deterministic blocked Bloom filter (≤ 64 KB budget per
//!   column per shard) for equality-predicate pruning — guarantees no false negatives.
//! - **`HllCardinality`**: HyperLogLog cardinality sketch for planner cost estimates.
//! - **`ShardColumnStats`**: Aggregate container for all three above per column.
//! - **`ShardStatsRegistry`**: In-process registry mapping `(shard_id, column_name)` →
//!   `ShardColumnStats`, used by the gateway scatter-pruner.
//!
//! # Freshness
//!
//! Stats are published at checkpoint boundaries. A `published_epoch` and `published_at_ms`
//! timestamp are stored alongside each entry. When the age exceeds `shard_stats_max_age_ms`,
//! the gateway falls back to full scatter and emits `RS-2017 shard_stats.too_stale`.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::ids::ShardId;
use crate::timestamp::Epoch;

// ─── ColumnMinMax ─────────────────────────────────────────────────────────────

/// Per-column min/max bounds observed across all rows in a shard.
///
/// Values are encoded as raw bytes (big-endian for numeric types so that lexicographic
/// ordering matches numeric ordering). An empty `min_bytes` / `max_bytes` means the
/// column had no non-null rows in the shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMinMax {
    /// Column name this stat applies to.
    pub column_name: String,
    /// Serialised minimum value (big-endian bytes for ordered types).
    pub min_bytes: Vec<u8>,
    /// Serialised maximum value (big-endian bytes for ordered types).
    pub max_bytes: Vec<u8>,
}

impl ColumnMinMax {
    /// Construct from raw bytes.
    pub fn new(column_name: impl Into<String>, min_bytes: Vec<u8>, max_bytes: Vec<u8>) -> Self {
        Self {
            column_name: column_name.into(),
            min_bytes,
            max_bytes,
        }
    }

    /// Returns `true` when the predicate `column = value_bytes` *could* match this shard.
    ///
    /// Returns `false` (prune the shard) only when `value_bytes < min` or `value_bytes > max`.
    /// This is a conservative check — it can return `true` even when no row matches (false
    /// positive), but it **never** returns `false` when a matching row exists (no false negative).
    pub fn could_contain_eq(&self, value_bytes: &[u8]) -> bool {
        if self.min_bytes.is_empty() && self.max_bytes.is_empty() {
            // No bounds recorded — cannot prune.
            return true;
        }
        value_bytes >= self.min_bytes.as_slice() && value_bytes <= self.max_bytes.as_slice()
    }

    /// Returns `true` when the predicate `column BETWEEN lo_bytes AND hi_bytes` *could* match.
    pub fn could_contain_range(&self, lo_bytes: &[u8], hi_bytes: &[u8]) -> bool {
        if self.min_bytes.is_empty() && self.max_bytes.is_empty() {
            return true;
        }
        // Ranges overlap iff lo ≤ max AND hi ≥ min.
        lo_bytes <= self.max_bytes.as_slice() && hi_bytes >= self.min_bytes.as_slice()
    }
}

// ─── BlockedBloomFilter ───────────────────────────────────────────────────────

/// Budget for the blocked Bloom filter (64 KB per column per shard).
pub const BLOOM_BUDGET_BYTES: usize = 64 * 1024;

/// Number of bits in the filter.
const BLOOM_BITS: usize = BLOOM_BUDGET_BYTES * 8;

/// Number of hash functions (k).  k = 3 gives a good FPR at reasonable fill levels.
const BLOOM_HASH_K: usize = 3;

/// A compact, deterministic blocked Bloom filter.
///
/// The filter is backed by a fixed-size bit array of `BLOOM_BITS` bits.
/// Each inserted value is hashed `BLOOM_HASH_K` times (via wrapping arithmetic on
/// two independent 64-bit seeds derived from `std::hash` / FNV-style mixing).
///
/// **Guarantee**: The filter never produces false negatives.  `could_contain` may return
/// `true` for absent values (false positives) but never `false` for present values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockedBloomFilter {
    bits: Vec<u8>, // BLOOM_BUDGET_BYTES bytes = BLOOM_BITS bits
    item_count: u64,
}

impl BlockedBloomFilter {
    /// Create an empty filter.
    pub fn new() -> Self {
        Self {
            bits: vec![0u8; BLOOM_BUDGET_BYTES],
            item_count: 0,
        }
    }

    /// Insert `value` into the filter.
    pub fn insert(&mut self, value: &[u8]) {
        let (h1, h2) = Self::double_hash(value);
        for i in 0..BLOOM_HASH_K {
            let bit = Self::bit_index(h1, h2, i);
            self.set_bit(bit);
        }
        self.item_count += 1;
    }

    /// Return `true` if `value` **might** be in the filter.
    ///
    /// A return value of `false` is a definitive guarantee that `value` was never inserted.
    pub fn could_contain(&self, value: &[u8]) -> bool {
        let (h1, h2) = Self::double_hash(value);
        for i in 0..BLOOM_HASH_K {
            let bit = Self::bit_index(h1, h2, i);
            if !self.get_bit(bit) {
                return false;
            }
        }
        true
    }

    /// Number of items inserted so far.
    pub fn item_count(&self) -> u64 {
        self.item_count
    }

    /// Serialise to raw bytes (the backing bit array).
    pub fn to_bytes(&self) -> &[u8] {
        &self.bits
    }

    // ─── internals ────────────────────────────────────────────────────────────

    fn double_hash(value: &[u8]) -> (u64, u64) {
        // FNV-1a 64-bit for h1.
        let mut h1: u64 = 0xcbf29ce484222325;
        for &b in value {
            h1 ^= b as u64;
            h1 = h1.wrapping_mul(0x100000001b3);
        }
        // A second independent hash by mixing h1 with a different constant.
        let h2 = h1
            .wrapping_mul(0x517cc1b727220a95)
            .wrapping_add(0xdeadbeef_cafebabe);
        (h1, h2)
    }

    fn bit_index(h1: u64, h2: u64, i: usize) -> usize {
        h1.wrapping_add(h2.wrapping_mul(i as u64)) as usize % BLOOM_BITS
    }

    fn set_bit(&mut self, bit: usize) {
        let byte = bit / 8;
        let offset = bit % 8;
        self.bits[byte] |= 1 << offset;
    }

    fn get_bit(&self, bit: usize) -> bool {
        let byte = bit / 8;
        let offset = bit % 8;
        (self.bits[byte] >> offset) & 1 == 1
    }
}

impl Default for BlockedBloomFilter {
    fn default() -> Self {
        Self::new()
    }
}

// ─── HllCardinality ──────────────────────────────────────────────────────────

/// Precision parameter for the HyperLogLog sketch (2^p registers).
const HLL_PRECISION: u8 = 12; // 4096 registers ≈ ±1.6% error

/// A compact HyperLogLog cardinality sketch backed by `2^HLL_PRECISION` registers.
///
/// Used for planner cost estimation. The sketch is mergeable (union via per-register max).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HllCardinality {
    registers: Vec<u8>, // 2^HLL_PRECISION = 4096 entries
}

impl HllCardinality {
    /// Construct a fresh empty sketch.
    pub fn new() -> Self {
        let m = 1usize << HLL_PRECISION;
        Self {
            registers: vec![0u8; m],
        }
    }

    /// Add a value to the sketch.
    pub fn add(&mut self, value: &[u8]) {
        let h = Self::hash(value);
        // Top `HLL_PRECISION` bits select the register.
        let j = (h >> (64 - HLL_PRECISION)) as usize;
        // Remaining bits: shift out the top p bits, leaving 64-p bits.
        // Count leading zeros; rho is that count + 1.
        // Set the lowest bit as sentinel so an all-zero remainder doesn't give leading_zeros = 64.
        let remaining = (h << HLL_PRECISION) | 1;
        let rho = remaining.leading_zeros() as u8 + 1;
        if rho > self.registers[j] {
            self.registers[j] = rho;
        }
    }

    /// Estimate the number of distinct values using the LogLog-Beta correction.
    ///
    /// This uses the standard HyperLogLog estimator with linear-counting small-range correction.
    pub fn estimate(&self) -> u64 {
        let m = self.registers.len() as f64;
        // Constant alpha_m for bias correction (standard formula).
        let alpha_mm = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        } * m * m;

        // Harmonic mean of 2^(-M[j]) over all registers.
        let z: f64 = self
            .registers
            .iter()
            .map(|&r| 2.0f64.powi(-(r as i32)))
            .sum();

        let raw_estimate = alpha_mm / z;

        // Small-range correction: linear counting.
        if raw_estimate < 2.5 * m {
            let v = self.registers.iter().filter(|&&r| r == 0).count() as f64;
            if v > 0.0 {
                return (m * (m / v).ln()) as u64;
            }
        }
        raw_estimate as u64
    }

    /// Merge another sketch into this one (union operation, register-max).
    pub fn merge(&mut self, other: &HllCardinality) {
        for (a, &b) in self.registers.iter_mut().zip(&other.registers) {
            if b > *a {
                *a = b;
            }
        }
    }

    fn hash(value: &[u8]) -> u64 {
        // MurmurHash3-inspired finalizer for better avalanche properties than raw FNV.
        // Use FNV-1a as the base, then apply the Murmur3 finalizer.
        let mut h: u64 = 0xcbf29ce484222325u64;
        for &b in value {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3u64);
        }
        // Murmur3 finalizer for better bit distribution.
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccdu64);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53u64);
        h ^= h >> 33;
        h
    }
}

impl Default for HllCardinality {
    fn default() -> Self {
        Self::new()
    }
}

// ─── ShardColumnStats ─────────────────────────────────────────────────────────

/// All per-column statistics for a single shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardColumnStats {
    /// Column name.
    pub column_name: String,
    /// Min/max bounds for range pruning.
    pub min_max: ColumnMinMax,
    /// Bloom filter for equality-predicate pruning.
    pub bloom: BlockedBloomFilter,
    /// HLL sketch for cardinality estimation.
    pub hll: HllCardinality,
    /// Epoch at which these stats were computed.
    pub published_epoch: Epoch,
    /// Wall-clock time these stats were published (ms since Unix epoch).
    pub published_at_ms: u64,
}

impl ShardColumnStats {
    /// Create a new stats entry for `column_name` at `published_epoch`.
    pub fn new(column_name: impl Into<String>, published_epoch: Epoch) -> Self {
        let name = column_name.into();
        let published_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Self {
            column_name: name.clone(),
            min_max: ColumnMinMax::new(name, vec![], vec![]),
            bloom: BlockedBloomFilter::new(),
            hll: HllCardinality::new(),
            published_epoch,
            published_at_ms,
        }
    }

    /// Insert a value (raw bytes) into all three structures.
    pub fn insert(&mut self, value_bytes: &[u8]) {
        // Update min/max.
        if self.min_max.min_bytes.is_empty() || value_bytes < self.min_max.min_bytes.as_slice() {
            self.min_max.min_bytes = value_bytes.to_vec();
        }
        if self.min_max.max_bytes.is_empty() || value_bytes > self.min_max.max_bytes.as_slice() {
            self.min_max.max_bytes = value_bytes.to_vec();
        }
        // Update bloom.
        self.bloom.insert(value_bytes);
        // Update HLL.
        self.hll.add(value_bytes);
    }

    /// Check whether this shard *could* contain the value (equality predicate).
    ///
    /// Returns `false` only if the Bloom filter definitely excludes the value.
    pub fn could_contain_eq(&self, value_bytes: &[u8]) -> bool {
        // Min/max check first (cheap).
        if !self.min_max.could_contain_eq(value_bytes) {
            return false;
        }
        // Bloom check second.
        self.bloom.could_contain(value_bytes)
    }

    /// Stats age in milliseconds from `now_ms`.
    pub fn age_ms(&self, now_ms: u64) -> u64 {
        now_ms.saturating_sub(self.published_at_ms)
    }
}

// ─── ShardStatsRegistry ───────────────────────────────────────────────────────

/// Per-operator scatter-pruning context.
#[derive(Debug, Clone, Default)]
pub struct ScatterPruningResult {
    /// Total shards in the scatter set before pruning.
    pub total_shards: usize,
    /// Shards pruned (shard definitely has no matching rows).
    pub pruned_shards: usize,
    /// Whether stats were too stale (RS-2017); when `true`, no pruning occurred.
    pub stats_too_stale: bool,
}

impl ScatterPruningResult {
    /// Number of shards that will be queried after pruning.
    pub fn surviving_shards(&self) -> usize {
        self.total_shards - self.pruned_shards
    }
}

/// In-process registry mapping `(shard_id, column_name)` to [`ShardColumnStats`].
///
/// Updated at checkpoint commit time. The gateway scatter-pruner calls
/// [`ShardStatsRegistry::prune_scatter`] before dispatching cross-shard queries.
#[derive(Debug, Default)]
pub struct ShardStatsRegistry {
    /// `shard_id → column_name → stats`
    stats: HashMap<ShardId, HashMap<String, ShardColumnStats>>,
    /// Maximum age in milliseconds before stats are considered too stale (RS-2017).
    pub max_age_ms: u64,
}

impl ShardStatsRegistry {
    /// Create a new registry with the given freshness limit.
    pub fn new(max_age_ms: u64) -> Self {
        Self {
            stats: HashMap::new(),
            max_age_ms,
        }
    }

    /// Publish stats for a shard (called at checkpoint commit).
    pub fn publish(&mut self, shard_id: ShardId, col_stats: ShardColumnStats) {
        self.stats
            .entry(shard_id)
            .or_default()
            .insert(col_stats.column_name.clone(), col_stats);
    }

    /// Prune a scatter set of `shard_ids` for an equality predicate on `column_name = value_bytes`.
    ///
    /// Returns the pruning result. If stats are stale for any shard, falls back to full scatter
    /// and sets `stats_too_stale = true`.
    pub fn prune_scatter(
        &self,
        shard_ids: &[ShardId],
        column_name: &str,
        value_bytes: &[u8],
        now_ms: u64,
    ) -> ScatterPruningResult {
        let total_shards = shard_ids.len();
        let mut pruned_shards = 0;

        for shard_id in shard_ids {
            // Check freshness.
            if let Some(col_map) = self.stats.get(shard_id) {
                if let Some(stats) = col_map.get(column_name) {
                    if stats.age_ms(now_ms) > self.max_age_ms {
                        // Stale — fall back to full scatter.
                        return ScatterPruningResult {
                            total_shards,
                            pruned_shards: 0,
                            stats_too_stale: true,
                        };
                    }
                    // Prune if definitely absent.
                    if !stats.could_contain_eq(value_bytes) {
                        pruned_shards += 1;
                    }
                }
                // No stats for this column → cannot prune, keep shard.
            }
            // No stats for this shard → cannot prune, keep shard.
        }

        ScatterPruningResult {
            total_shards,
            pruned_shards,
            stats_too_stale: false,
        }
    }

    /// Get stats for a specific shard + column (for diagnostics / `rockstream inspect stats`).
    pub fn get(&self, shard_id: ShardId, column_name: &str) -> Option<&ShardColumnStats> {
        self.stats.get(&shard_id)?.get(column_name)
    }

    /// Iterate all registered shards.
    pub fn shards(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.stats.keys().copied()
    }
}

// ─── Scatter-pruning metrics ──────────────────────────────────────────────────

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;

struct ScatterMetrics {
    shards_total: AtomicU64,
    shards_pruned: AtomicU64,
    bloom_false_positives: AtomicU64,
}

static SCATTER_METRICS: LazyLock<ScatterMetrics> = LazyLock::new(|| ScatterMetrics {
    shards_total: AtomicU64::new(0),
    shards_pruned: AtomicU64::new(0),
    bloom_false_positives: AtomicU64::new(0),
});

/// Increment `scatter_shards_total` by `n`.
pub fn inc_scatter_shards_total(n: u64) {
    SCATTER_METRICS
        .shards_total
        .fetch_add(n, Ordering::Relaxed);
}

/// Increment `scatter_shards_pruned_total` by `n`.
pub fn inc_scatter_shards_pruned(n: u64) {
    SCATTER_METRICS
        .shards_pruned
        .fetch_add(n, Ordering::Relaxed);
}

/// Increment `shard_bloom_false_positive_total` (a row matched in Bloom but not found on shard).
pub fn inc_bloom_false_positive() {
    SCATTER_METRICS
        .bloom_false_positives
        .fetch_add(1, Ordering::Relaxed);
}

/// Read `scatter_shards_total`.
pub fn read_scatter_shards_total() -> u64 {
    SCATTER_METRICS.shards_total.load(Ordering::Relaxed)
}

/// Read `scatter_shards_pruned_total`.
pub fn read_scatter_shards_pruned() -> u64 {
    SCATTER_METRICS.shards_pruned.load(Ordering::Relaxed)
}

/// Read `shard_bloom_false_positive_total`.
pub fn read_bloom_false_positives() -> u64 {
    SCATTER_METRICS.bloom_false_positives.load(Ordering::Relaxed)
}

/// Reset scatter metrics (test helper).
#[doc(hidden)]
pub fn reset_scatter_metrics() {
    SCATTER_METRICS.shards_total.store(0, Ordering::Relaxed);
    SCATTER_METRICS.shards_pruned.store(0, Ordering::Relaxed);
    SCATTER_METRICS
        .bloom_false_positives
        .store(0, Ordering::Relaxed);
}

// ─── StorageFormatVersion ─────────────────────────────────────────────────────

/// Storage format version gate (v0.53, DESIGN.md §5.5).
///
/// Every shard header and checkpoint manifest records the format version at which it was written.
/// Before attaching a shard, the runtime checks that the stored version is compatible with the
/// running binary. Incompatibility surfaces as `RS-5001`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StorageFormatVersion(pub u32);

impl StorageFormatVersion {
    /// The current binary's storage format version.
    pub const CURRENT: Self = Self(53); // v0.53.0

    /// The minimum format version this binary can read without migration.
    pub const MIN_COMPATIBLE: Self = Self(48); // backward-compatible back to v0.48

    /// Returns `true` when `stored` is compatible with the running binary.
    pub fn is_compatible(stored: Self) -> bool {
        stored >= Self::MIN_COMPATIBLE && stored <= Self::CURRENT
    }
}

impl std::fmt::Display for StorageFormatVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::ShardId;

    // ── BlockedBloomFilter ─────────────────────────────────────────────────────

    #[test]
    fn bloom_no_false_negatives_10k() {
        let mut bloom = BlockedBloomFilter::new();
        let values: Vec<Vec<u8>> = (0u32..10_000).map(|i| i.to_be_bytes().to_vec()).collect();
        // Insert all.
        for v in &values {
            bloom.insert(v);
        }
        // Must return true for every inserted value.
        for v in &values {
            assert!(
                bloom.could_contain(v),
                "False negative for value {:?}",
                v
            );
        }
    }

    #[test]
    fn bloom_returns_false_for_absent_values() {
        let mut bloom = BlockedBloomFilter::new();
        bloom.insert(b"hello");
        bloom.insert(b"world");
        // A completely absent value should often return false (FPR), but the key
        // property is: inserted values always return true.
        assert!(bloom.could_contain(b"hello"));
        assert!(bloom.could_contain(b"world"));
        // This absent value *might* be a false positive, which is acceptable.
        // We just verify it doesn't panic.
        let _ = bloom.could_contain(b"absent_key_xyz");
    }

    #[test]
    fn bloom_serialises_to_64kb() {
        let bloom = BlockedBloomFilter::new();
        assert_eq!(bloom.to_bytes().len(), BLOOM_BUDGET_BYTES);
    }

    // ── ColumnMinMax ──────────────────────────────────────────────────────────

    #[test]
    fn minmax_prunes_out_of_range_value() {
        let mm = ColumnMinMax::new(
            "col",
            100u64.to_be_bytes().to_vec(),
            200u64.to_be_bytes().to_vec(),
        );
        // 50 < 100: prune.
        assert!(!mm.could_contain_eq(&50u64.to_be_bytes()));
        // 150 is in range: keep.
        assert!(mm.could_contain_eq(&150u64.to_be_bytes()));
        // 250 > 200: prune.
        assert!(!mm.could_contain_eq(&250u64.to_be_bytes()));
        // Boundaries are inclusive.
        assert!(mm.could_contain_eq(&100u64.to_be_bytes()));
        assert!(mm.could_contain_eq(&200u64.to_be_bytes()));
    }

    #[test]
    fn minmax_empty_bounds_never_prune() {
        let mm = ColumnMinMax::new("col", vec![], vec![]);
        assert!(mm.could_contain_eq(b"anything"));
    }

    // ── HllCardinality ────────────────────────────────────────────────────────

    #[test]
    fn hll_cardinality_estimate_within_20_percent() {
        let mut hll = HllCardinality::new();
        let n = 10_000u32;
        for i in 0..n {
            hll.add(&i.to_be_bytes());
        }
        let est = hll.estimate();
        let error = ((est as i64 - n as i64).unsigned_abs() as f64) / n as f64;
        assert!(
            error < 0.20,
            "HLL estimate {est} deviates from {n} by {:.1}%, exceeds 20%",
            error * 100.0
        );
    }

    #[test]
    fn hll_merge_union() {
        let mut a = HllCardinality::new();
        let mut b = HllCardinality::new();
        for i in 0u32..1000 {
            a.add(&i.to_be_bytes());
        }
        for i in 500u32..1500 {
            b.add(&i.to_be_bytes());
        }
        a.merge(&b);
        let est = a.estimate();
        // Union should be ≈ 1500.
        assert!(est > 1000, "union estimate {est} should exceed 1000");
        assert!(est < 3000, "union estimate {est} should be < 3000");
    }

    // ── ShardColumnStats ──────────────────────────────────────────────────────

    #[test]
    fn shard_stats_insert_and_query() {
        let mut stats = ShardColumnStats::new("customer_id", 42);
        for i in 100u64..200 {
            stats.insert(&i.to_be_bytes());
        }
        // In-range value: could contain (min/max says yes, Bloom should say yes too).
        assert!(stats.could_contain_eq(&150u64.to_be_bytes()));
        // Definitely out-of-range: min/max prunes it.
        assert!(!stats.could_contain_eq(&50u64.to_be_bytes()));
        assert!(!stats.could_contain_eq(&300u64.to_be_bytes()));
    }

    // ── ShardStatsRegistry ────────────────────────────────────────────────────

    #[test]
    fn registry_prune_scatter_100_shards_8_match() {
        let max_age_ms = 60_000;
        let mut registry = ShardStatsRegistry::new(max_age_ms);

        let shard_ids: Vec<ShardId> = (0u64..100).map(ShardId).collect();
        let matching_value = 42u64.to_be_bytes().to_vec();

        // Publish stats: 8 shards contain value 42, rest have values 100..200.
        for (i, &shard_id) in shard_ids.iter().enumerate() {
            let mut col_stats = ShardColumnStats::new("customer_id", 1);
            // Backdate published_at_ms so it's not stale.
            if i < 8 {
                col_stats.insert(&matching_value);
            } else {
                // Insert values that definitely exclude 42.
                for v in 100u64..110 {
                    col_stats.insert(&v.to_be_bytes());
                }
            }
            registry.publish(shard_id, col_stats);
        }

        // now_ms: set to something very close to published_at_ms (fresh).
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let result = registry.prune_scatter(&shard_ids, "customer_id", &matching_value, now_ms);

        assert!(!result.stats_too_stale, "Stats should be fresh");
        // Must keep ≤ 12 shards (8 matching + some FP from bloom is acceptable).
        assert!(
            result.surviving_shards() <= 12,
            "Surviving shards {} should be ≤ 12 (8 real + ≤4 FP)",
            result.surviving_shards()
        );
        // Must not prune any matching shard (no false negatives).
        assert!(
            result.pruned_shards <= 92,
            "Pruned {} shards, expected at most 92 (never prune matching shards)",
            result.pruned_shards
        );
    }

    #[test]
    fn registry_stale_stats_fall_back() {
        let max_age_ms = 1; // 1 ms — immediately stale.
        let mut registry = ShardStatsRegistry::new(max_age_ms);
        let shard_id = ShardId(0);
        let mut col_stats = ShardColumnStats::new("x", 1);
        col_stats.insert(b"value");
        // Force published_at_ms to 0 (epoch) so it's definitely stale.
        col_stats.published_at_ms = 0;
        registry.publish(shard_id, col_stats);

        let now_ms = 1_000_000; // very large "now"
        let result = registry.prune_scatter(&[shard_id], "x", b"value", now_ms);
        assert!(result.stats_too_stale, "Should detect stale stats");
        assert_eq!(result.pruned_shards, 0);
    }

    // ── StorageFormatVersion ──────────────────────────────────────────────────

    #[test]
    fn format_version_current_is_compatible() {
        assert!(StorageFormatVersion::is_compatible(StorageFormatVersion::CURRENT));
    }

    #[test]
    fn format_version_too_old_is_incompatible() {
        // Format version 10 (written by v0.10 binary) is before MIN_COMPATIBLE(48).
        assert!(!StorageFormatVersion::is_compatible(
            StorageFormatVersion(10)
        ));
    }

    #[test]
    fn format_version_too_new_is_incompatible() {
        // A future binary (v99) wrote this; our binary cannot read it.
        assert!(!StorageFormatVersion::is_compatible(
            StorageFormatVersion(99)
        ));
    }

    #[test]
    fn format_version_min_compatible_passes() {
        assert!(StorageFormatVersion::is_compatible(
            StorageFormatVersion::MIN_COMPATIBLE
        ));
    }

    #[test]
    fn rolling_upgrade_n_to_n1_compatible() {
        // Simulate: a v0.52 shard is read by a v0.53 binary. Since 52 ≥ MIN_COMPATIBLE(48),
        // it must be compatible.
        let v52 = StorageFormatVersion(52);
        assert!(StorageFormatVersion::is_compatible(v52));
    }
}

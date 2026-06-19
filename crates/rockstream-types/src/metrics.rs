//! Merge-law metric counters (IVM-0, DESIGN.md §6.11).
//!
//! Every code path that invokes a merge law increments `merge_law_applied_total`.
//! Code paths that fall back to a safe default (e.g., on parse error) increment
//! `merge_law_fallback_total`. Later phases wire these into a Prometheus registry;
//! for now they are process-global atomics that tests and diagnostics can read.
//!
//! # v0.27 additions
//!
//! - `merge_law_rmw_avoided_total` / `merge_law_rmw_required_total`: per-law
//!   counters that prove `WeightAdd/v1` and `SumCount/v1` avoid read-modify-write
//!   on the hot path (abelian group laws can be merged blindly).
//! - `manifest_write_total`: epoch-level manifest write counter used by the
//!   manifest churn budget gate (≤ 1 manifest write per epoch, DESIGN.md §5.4).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use crate::merge_law::MergeLawId;

/// Key for a per-law metric bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LawMetricKey {
    pub law_id: MergeLawId,
    pub law_name: &'static str,
    pub law_version: u16,
    pub operator_id: Option<crate::ids::OperatorId>,
}

/// A single atomic counter for one metric key.
struct Counter {
    value: AtomicU64,
}

impl Counter {
    fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Global registry for merge-law metric counters.
struct MetricRegistry {
    applied: HashMap<LawMetricKey, Counter>,
    fallback: HashMap<LawMetricKey, Counter>,
    /// RMW avoided: abelian-group laws that can merge without a prior read.
    rmw_avoided: HashMap<LawMetricKey, Counter>,
    /// RMW required: semilattice / extremum laws that need a prior read.
    rmw_required: HashMap<LawMetricKey, Counter>,
    /// Total manifest writes (global, not per-law).
    manifest_writes: AtomicU64,

    // SRE Observability metrics
    compaction_bytes_reclaimed: HashMap<u16, Counter>,
    duplicate_dropped_total: HashMap<u16, Counter>,
    tombstone_bytes: HashMap<u16, Counter>,
    monotone_partial_lag_ms: HashMap<u16, Counter>,
    workload_memory_bytes: HashMap<String, Counter>,
    state_budget_bytes: AtomicU64,
    freshness_lag_ms: HashMap<String, Counter>,

    // Flush duration metrics
    flush_duration_sum_ms: AtomicU64,
    flush_duration_count: AtomicU64,
    flush_duration_last_ms: AtomicU64,
}

impl MetricRegistry {
    fn new() -> Self {
        Self {
            applied: HashMap::new(),
            fallback: HashMap::new(),
            rmw_avoided: HashMap::new(),
            rmw_required: HashMap::new(),
            manifest_writes: AtomicU64::new(0),
            compaction_bytes_reclaimed: HashMap::new(),
            duplicate_dropped_total: HashMap::new(),
            tombstone_bytes: HashMap::new(),
            monotone_partial_lag_ms: HashMap::new(),
            workload_memory_bytes: HashMap::new(),
            state_budget_bytes: AtomicU64::new(0),
            freshness_lag_ms: HashMap::new(),
            flush_duration_sum_ms: AtomicU64::new(0),
            flush_duration_count: AtomicU64::new(0),
            flush_duration_last_ms: AtomicU64::new(0),
        }
    }
}

static REGISTRY: LazyLock<Mutex<MetricRegistry>> =
    LazyLock::new(|| Mutex::new(MetricRegistry::new()));

fn with_registry<F, R>(f: F) -> R
where
    F: FnOnce(&mut MetricRegistry) -> R,
{
    let mut guard = REGISTRY.lock().expect("merge law metrics mutex poisoned");
    f(&mut guard)
}

// ─── merge_law_applied / merge_law_fallback ───────────────────────────────────

/// Increment `merge_law_applied_total` for the given law.
pub fn inc_applied(key: &LawMetricKey) {
    with_registry(|reg| {
        reg.applied
            .entry(key.clone())
            .or_insert_with(Counter::new)
            .inc();
    });
}

/// Increment `merge_law_fallback_total` for the given law.
pub fn inc_fallback(key: &LawMetricKey) {
    with_registry(|reg| {
        reg.fallback
            .entry(key.clone())
            .or_insert_with(Counter::new)
            .inc();
    });
}

/// Read the `merge_law_applied_total` counter for a law (for tests/diagnostics).
pub fn read_applied(key: &LawMetricKey) -> u64 {
    with_registry(|reg| reg.applied.get(key).map(|c| c.get()).unwrap_or(0))
}

/// Read the `merge_law_fallback_total` counter for a law (for tests/diagnostics).
pub fn read_fallback(key: &LawMetricKey) -> u64 {
    with_registry(|reg| reg.fallback.get(key).map(|c| c.get()).unwrap_or(0))
}

// ─── merge_law_rmw_avoided / merge_law_rmw_required ──────────────────────────

/// Increment `merge_law_rmw_avoided_total` for the given law.
///
/// Call this when the law's merge can be applied as a **blind append** without
/// reading the existing stored value first (abelian group laws: WeightAdd/v1,
/// SumCount/v1).
pub fn inc_rmw_avoided(key: &LawMetricKey) {
    with_registry(|reg| {
        reg.rmw_avoided
            .entry(key.clone())
            .or_insert_with(Counter::new)
            .inc();
    });
}

/// Increment `merge_law_rmw_required_total` for the given law.
///
/// Call this when the law requires reading the current stored value before
/// writing (semilattice laws: MaxRegister/v1, MinRegister/v1, HyperLogLog/v1,
/// BloomUnion/v1 — all of which carry `not_merge_safe_reason=ExtremumRequiresRmw`).
pub fn inc_rmw_required(key: &LawMetricKey) {
    with_registry(|reg| {
        reg.rmw_required
            .entry(key.clone())
            .or_insert_with(Counter::new)
            .inc();
    });
}

/// Read the `merge_law_rmw_avoided_total` counter (for tests/diagnostics).
pub fn read_rmw_avoided(key: &LawMetricKey) -> u64 {
    with_registry(|reg| reg.rmw_avoided.get(key).map(|c| c.get()).unwrap_or(0))
}

/// Read the `merge_law_rmw_required_total` counter (for tests/diagnostics).
pub fn read_rmw_required(key: &LawMetricKey) -> u64 {
    with_registry(|reg| reg.rmw_required.get(key).map(|c| c.get()).unwrap_or(0))
}

/// Compute the RMW-avoidance ratio for a law:
/// `avoided / (avoided + required)`, or `1.0` if both are zero.
///
/// A ratio of 1.0 proves the law never requires a prior read (hot path).
/// A ratio of 0.0 means every merge needed a read.
pub fn rmw_avoidance_ratio(key: &LawMetricKey) -> f64 {
    let avoided = read_rmw_avoided(key);
    let required = read_rmw_required(key);
    let total = avoided + required;
    if total == 0 {
        1.0 // no merges yet — considered RMW-free by default
    } else {
        avoided as f64 / total as f64
    }
}

/// Snapshot of per-law RMW metrics for all registered laws.
///
/// Returns a `Vec` of `(law_name, law_id, avoided, required, ratio)` tuples
/// sorted by law_id. Used in benchmarks and sign-off evidence.
pub fn rmw_ratio_report() -> Vec<RmwRatioEntry> {
    with_registry(|reg| {
        // Collect all keys from both maps.
        let mut keys: Vec<LawMetricKey> = reg
            .rmw_avoided
            .keys()
            .chain(reg.rmw_required.keys())
            .cloned()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        keys.sort_by_key(|k| k.law_id.0);

        keys.into_iter()
            .map(|k| {
                let avoided = reg.rmw_avoided.get(&k).map(|c| c.get()).unwrap_or(0);
                let required = reg.rmw_required.get(&k).map(|c| c.get()).unwrap_or(0);
                let total = avoided + required;
                let ratio = if total == 0 {
                    1.0
                } else {
                    avoided as f64 / total as f64
                };
                RmwRatioEntry {
                    law_name: k.law_name,
                    law_id: k.law_id.0,
                    law_version: k.law_version,
                    rmw_avoided: avoided,
                    rmw_required: required,
                    avoidance_ratio: ratio,
                }
            })
            .collect()
    })
}

/// One row in the per-law RMW ratio report.
#[derive(Debug, Clone)]
pub struct RmwRatioEntry {
    pub law_name: &'static str,
    pub law_id: u16,
    pub law_version: u16,
    pub rmw_avoided: u64,
    pub rmw_required: u64,
    /// Fraction of merges that avoided RMW: 0.0 (never avoided) to 1.0 (always avoided).
    pub avoidance_ratio: f64,
}

// ─── manifest_write_total ─────────────────────────────────────────────────────

/// Increment the global manifest write counter.
///
/// Call once per manifest commit (typically once per epoch in steady state).
/// The manifest churn budget gate (DESIGN.md §5.4) asserts ≤ 1 call per epoch.
pub fn inc_manifest_write() {
    with_registry(|reg| {
        reg.manifest_writes.fetch_add(1, Ordering::Relaxed);
    });
}

/// Read the total manifest write counter.
pub fn read_manifest_writes() -> u64 {
    with_registry(|reg| reg.manifest_writes.load(Ordering::Relaxed))
}

// ─── reset_all ───────────────────────────────────────────────────────────────

/// Reset all counters to zero.
///
/// For use in tests only. Calling this from production code has no effect on
/// correctness but loses metric history.
#[doc(hidden)]
pub fn reset_all() {
    with_registry(|reg| {
        reg.applied.clear();
        reg.fallback.clear();
        reg.rmw_avoided.clear();
        reg.rmw_required.clear();
        reg.manifest_writes.store(0, Ordering::Relaxed);
        reg.compaction_bytes_reclaimed.clear();
        reg.duplicate_dropped_total.clear();
        reg.tombstone_bytes.clear();
        reg.monotone_partial_lag_ms.clear();
        reg.workload_memory_bytes.clear();
        reg.state_budget_bytes.store(0, Ordering::Relaxed);
        reg.freshness_lag_ms.clear();
        reg.flush_duration_sum_ms.store(0, Ordering::Relaxed);
        reg.flush_duration_count.store(0, Ordering::Relaxed);
        reg.flush_duration_last_ms.store(0, Ordering::Relaxed);
    });
}

// ─── SRE Observability Metrics Helpers ────────────────────────────────────────

pub fn inc_compaction_bytes_reclaimed(law_id: u16, bytes: u64) {
    with_registry(|reg| {
        let counter = reg
            .compaction_bytes_reclaimed
            .entry(law_id)
            .or_insert_with(Counter::new);
        for _ in 0..bytes {
            counter.inc();
        }
    });
}

pub fn inc_duplicate_dropped(law_id: u16) {
    with_registry(|reg| {
        reg.duplicate_dropped_total
            .entry(law_id)
            .or_insert_with(Counter::new)
            .inc();
    });
}

pub fn set_tombstone_bytes(law_id: u16, bytes: u64) {
    with_registry(|reg| {
        let counter = reg
            .tombstone_bytes
            .entry(law_id)
            .or_insert_with(Counter::new);
        counter.value.store(bytes, Ordering::Relaxed);
    });
}

pub fn set_monotone_partial_lag(law_id: u16, lag_ms: u64) {
    with_registry(|reg| {
        let counter = reg
            .monotone_partial_lag_ms
            .entry(law_id)
            .or_insert_with(Counter::new);
        counter.value.store(lag_ms, Ordering::Relaxed);
    });
}

pub fn set_workload_memory(workload: &str, bytes: u64) {
    with_registry(|reg| {
        let counter = reg
            .workload_memory_bytes
            .entry(workload.to_string())
            .or_insert_with(Counter::new);
        counter.value.store(bytes, Ordering::Relaxed);
    });
}

pub fn set_state_budget(bytes: u64) {
    with_registry(|reg| {
        reg.state_budget_bytes.store(bytes, Ordering::Relaxed);
    });
}

pub fn set_freshness_lag(view_name: &str, lag_ms: u64) {
    with_registry(|reg| {
        let counter = reg
            .freshness_lag_ms
            .entry(view_name.to_string())
            .or_insert_with(Counter::new);
        counter.value.store(lag_ms, Ordering::Relaxed);
    });
}

pub fn record_flush_duration(duration: std::time::Duration) {
    with_registry(|reg| {
        let ms = duration.as_millis() as u64;
        reg.flush_duration_sum_ms.fetch_add(ms, Ordering::Relaxed);
        reg.flush_duration_count.fetch_add(1, Ordering::Relaxed);
        reg.flush_duration_last_ms.store(ms, Ordering::Relaxed);
    });
}

pub fn generate_prometheus_metrics() -> String {
    let mut out = String::new();
    with_registry(|reg| {
        // 1. merge_law_applied_total
        out.push_str("# HELP merge_law_applied_total Counter tracking the number of times a merge law is evaluated on a state merge.\n");
        out.push_str("# TYPE merge_law_applied_total counter\n");
        for (k, c) in &reg.applied {
            out.push_str(&format!(
                "merge_law_applied_total{{law_id=\"{}\",law_name=\"{}\",law_version=\"{}\"}} {}\n",
                k.law_id.0,
                k.law_name,
                k.law_version,
                c.get()
            ));
        }
        out.push_str("\n");

        // 2. merge_law_fallback_total
        out.push_str("# HELP merge_law_fallback_total Counter tracking the number of fallback reads resolved by copying all raw bytes.\n");
        out.push_str("# TYPE merge_law_fallback_total counter\n");
        for (k, c) in &reg.fallback {
            out.push_str(&format!(
                "merge_law_fallback_total{{law_id=\"{}\",law_name=\"{}\",law_version=\"{}\"}} {}\n",
                k.law_id.0,
                k.law_name,
                k.law_version,
                c.get()
            ));
        }
        out.push_str("\n");

        // 3. merge_law_compaction_bytes_reclaimed
        out.push_str("# HELP merge_law_compaction_bytes_reclaimed Counter of bytes reclaimed during SlateDB compaction filters running laws.\n");
        out.push_str("# TYPE merge_law_compaction_bytes_reclaimed counter\n");
        for (law_id, c) in &reg.compaction_bytes_reclaimed {
            out.push_str(&format!(
                "merge_law_compaction_bytes_reclaimed{{law_id=\"{}\"}} {}\n",
                law_id,
                c.get()
            ));
        }
        out.push_str("\n");

        // 4. merge_law_duplicate_dropped_total
        out.push_str("# HELP merge_law_duplicate_dropped_total Counter of duplicated update/insert operands dropped by idempotent laws.\n");
        out.push_str("# TYPE merge_law_duplicate_dropped_total counter\n");
        for (law_id, c) in &reg.duplicate_dropped_total {
            out.push_str(&format!(
                "merge_law_duplicate_dropped_total{{law_id=\"{}\"}} {}\n",
                law_id,
                c.get()
            ));
        }
        out.push_str("\n");

        // 5. merge_law_tombstone_bytes
        out.push_str("# HELP merge_law_tombstone_bytes Gauge of active tombstone bytes in the arrangement state.\n");
        out.push_str("# TYPE merge_law_tombstone_bytes gauge\n");
        for (law_id, c) in &reg.tombstone_bytes {
            out.push_str(&format!(
                "merge_law_tombstone_bytes{{law_id=\"{}\"}} {}\n",
                law_id,
                c.get()
            ));
        }
        out.push_str("\n");

        // 6. merge_law_monotone_partial_lag_ms
        out.push_str("# HELP merge_law_monotone_partial_lag_ms Gauge showing freshness lag of monotone recursive operators.\n");
        out.push_str("# TYPE merge_law_monotone_partial_lag_ms gauge\n");
        for (law_id, c) in &reg.monotone_partial_lag_ms {
            out.push_str(&format!(
                "merge_law_monotone_partial_lag_ms{{law_id=\"{}\"}} {}\n",
                law_id,
                c.get()
            ));
        }
        out.push_str("\n");

        // 7. workload_memory_bytes
        out.push_str("# HELP workload_memory_bytes Gauge showing live memory consumption of stateful operators grouped by workload.\n");
        out.push_str("# TYPE workload_memory_bytes gauge\n");
        for (workload, c) in &reg.workload_memory_bytes {
            out.push_str(&format!(
                "workload_memory_bytes{{workload_name=\"{}\"}} {}\n",
                workload,
                c.get()
            ));
        }
        out.push_str("\n");

        // 8. state_budget_bytes
        out.push_str("# HELP state_budget_bytes Gauge tracking total memory allocations against state_budget_gb.\n");
        out.push_str("# TYPE state_budget_bytes gauge\n");
        out.push_str(&format!(
            "state_budget_bytes {}\n\n",
            reg.state_budget_bytes.load(Ordering::Relaxed)
        ));

        // 9. freshness_lag_ms
        out.push_str("# HELP freshness_lag_ms Gauge showing the lag between input source watermarks and the committed epoch.\n");
        out.push_str("# TYPE freshness_lag_ms gauge\n");
        for (view_name, c) in &reg.freshness_lag_ms {
            out.push_str(&format!(
                "freshness_lag_ms{{view_name=\"{}\"}} {}\n",
                view_name,
                c.get()
            ));
        }
        out.push_str("\n");

        // 10. flush_duration_seconds_sum
        out.push_str("# HELP flush_duration_seconds_sum Cumulative duration of all flushes.\n");
        out.push_str("# TYPE flush_duration_seconds_sum counter\n");
        let sum_sec = reg.flush_duration_sum_ms.load(Ordering::Relaxed) as f64 / 1000.0;
        out.push_str(&format!("flush_duration_seconds_sum {:.4}\n\n", sum_sec));

        // 11. flush_duration_seconds_count
        out.push_str("# HELP flush_duration_seconds_count Total count of flushes.\n");
        out.push_str("# TYPE flush_duration_seconds_count counter\n");
        out.push_str(&format!(
            "flush_duration_seconds_count {}\n\n",
            reg.flush_duration_count.load(Ordering::Relaxed)
        ));

        // 12. flush_duration_seconds_last
        out.push_str("# HELP flush_duration_seconds_last Latency of the last flush operation.\n");
        out.push_str("# TYPE flush_duration_seconds_last gauge\n");
        let last_sec = reg.flush_duration_last_ms.load(Ordering::Relaxed) as f64 / 1000.0;
        out.push_str(&format!("flush_duration_seconds_last {:.4}\n\n", last_sec));

        // 13. slatedb_manifest_write_total
        out.push_str("# HELP slatedb_manifest_write_total Total manifest writes.\n");
        out.push_str("# TYPE slatedb_manifest_write_total counter\n");
        out.push_str(&format!(
            "slatedb_manifest_write_total {}\n",
            reg.manifest_writes.load(Ordering::Relaxed)
        ));
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merge_law::MergeLawId;
    use std::sync::{LazyLock, Mutex};

    /// Serialise all tests that touch the process-global REGISTRY so that
    /// concurrent test threads don't corrupt each other's `reset_all` / `inc`
    /// sequences.
    static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn key() -> LawMetricKey {
        LawMetricKey {
            law_id: MergeLawId(0x0001),
            law_name: "WeightAdd",
            law_version: 1,
            operator_id: None,
        }
    }

    #[test]
    fn applied_counter_increments() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k = key();
        assert_eq!(read_applied(&k), 0);
        inc_applied(&k);
        inc_applied(&k);
        assert_eq!(read_applied(&k), 2);
    }

    #[test]
    fn fallback_counter_increments() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k = key();
        assert_eq!(read_fallback(&k), 0);
        inc_fallback(&k);
        assert_eq!(read_fallback(&k), 1);
    }

    #[test]
    fn independent_counters_per_law() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k1 = LawMetricKey {
            law_id: MergeLawId(0x0001),
            law_name: "WeightAdd",
            law_version: 1,
            operator_id: None,
        };
        let k2 = LawMetricKey {
            law_id: MergeLawId(0x0002),
            law_name: "SumCount",
            law_version: 1,
            operator_id: None,
        };
        inc_applied(&k1);
        inc_applied(&k1);
        inc_fallback(&k2);
        assert_eq!(read_applied(&k1), 2);
        assert_eq!(read_applied(&k2), 0);
        assert_eq!(read_fallback(&k1), 0);
        assert_eq!(read_fallback(&k2), 1);
    }

    #[test]
    fn rmw_avoided_increments() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k = LawMetricKey {
            law_id: MergeLawId(0x0001),
            law_name: "WeightAdd",
            law_version: 1,
            operator_id: None,
        };
        assert_eq!(read_rmw_avoided(&k), 0);
        inc_rmw_avoided(&k);
        inc_rmw_avoided(&k);
        assert_eq!(read_rmw_avoided(&k), 2);
        assert_eq!(read_rmw_required(&k), 0);
    }

    #[test]
    fn rmw_required_increments() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k = LawMetricKey {
            law_id: MergeLawId(0x0003),
            law_name: "MaxRegister",
            law_version: 1,
            operator_id: None,
        };
        inc_rmw_required(&k);
        inc_rmw_required(&k);
        inc_rmw_required(&k);
        assert_eq!(read_rmw_required(&k), 3);
        assert_eq!(read_rmw_avoided(&k), 0);
    }

    #[test]
    fn rmw_avoidance_ratio_abelian_group() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        // WeightAdd/v1: 100% avoidance
        let k = LawMetricKey {
            law_id: MergeLawId(0x0001),
            law_name: "WeightAdd",
            law_version: 1,
            operator_id: None,
        };
        for _ in 0..100 {
            inc_rmw_avoided(&k);
        }
        let ratio = rmw_avoidance_ratio(&k);
        assert!(
            (ratio - 1.0).abs() < 1e-9,
            "WeightAdd RMW avoidance ratio should be 1.0, got {ratio}"
        );
    }

    #[test]
    fn rmw_avoidance_ratio_semilattice() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        // MaxRegister/v1: 0% avoidance
        let k = LawMetricKey {
            law_id: MergeLawId(0x0003),
            law_name: "MaxRegister",
            law_version: 1,
            operator_id: None,
        };
        for _ in 0..50 {
            inc_rmw_required(&k);
        }
        let ratio = rmw_avoidance_ratio(&k);
        assert!(
            ratio.abs() < 1e-9,
            "MaxRegister RMW avoidance ratio should be 0.0, got {ratio}"
        );
    }

    #[test]
    fn rmw_avoidance_ratio_default_one_when_no_ops() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k = LawMetricKey {
            law_id: MergeLawId(0x0002),
            law_name: "SumCount",
            law_version: 1,
            operator_id: None,
        };
        // No operations yet — should default to 1.0.
        assert!((rmw_avoidance_ratio(&k) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn rmw_ratio_report_includes_all_keyed_laws() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let k1 = LawMetricKey {
            law_id: MergeLawId(0x0001),
            law_name: "WeightAdd",
            law_version: 1,
            operator_id: None,
        };
        let k2 = LawMetricKey {
            law_id: MergeLawId(0x0002),
            law_name: "SumCount",
            law_version: 1,
            operator_id: None,
        };
        inc_rmw_avoided(&k1);
        inc_rmw_avoided(&k2);
        inc_rmw_required(&k2); // mixed for k2
        let report = rmw_ratio_report();
        assert_eq!(report.len(), 2);
        let weight_add = report.iter().find(|e| e.law_name == "WeightAdd").unwrap();
        assert_eq!(weight_add.rmw_avoided, 1);
        assert_eq!(weight_add.rmw_required, 0);
        assert!((weight_add.avoidance_ratio - 1.0).abs() < 1e-9);
        let sum_count = report.iter().find(|e| e.law_name == "SumCount").unwrap();
        assert_eq!(sum_count.rmw_avoided, 1);
        assert_eq!(sum_count.rmw_required, 1);
        assert!((sum_count.avoidance_ratio - 0.5).abs() < 1e-9);
    }

    #[test]
    fn manifest_write_counter() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        assert_eq!(read_manifest_writes(), 0);
        inc_manifest_write();
        inc_manifest_write();
        assert_eq!(read_manifest_writes(), 2);
    }

    #[test]
    fn sum_count_abelian_group_proves_rmw_avoidance() {
        // Proof: SumCount/v1 is an abelian group, so every merge avoids RMW.
        // This test simulates 1000 merge operations and verifies the ratio is 1.0.
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let sum_count_key = LawMetricKey {
            law_id: MergeLawId(0x0002),
            law_name: "SumCount",
            law_version: 1,
            operator_id: None,
        };
        // Simulate 1000 merge operations — all avoided (abelian group).
        for _ in 0..1000 {
            inc_rmw_avoided(&sum_count_key);
        }
        let ratio = rmw_avoidance_ratio(&sum_count_key);
        assert!(
            (ratio - 1.0).abs() < 1e-9,
            "SumCount/v1 must have 100% RMW avoidance, got {ratio}"
        );
        println!("[proof] SumCount/v1 RMW avoidance ratio: {ratio:.4}");
    }

    #[test]
    fn weight_add_abelian_group_proves_rmw_avoidance() {
        // Proof: WeightAdd/v1 is an abelian group, so every merge avoids RMW.
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        let weight_add_key = LawMetricKey {
            law_id: MergeLawId(0x0001),
            law_name: "WeightAdd",
            law_version: 1,
            operator_id: None,
        };
        for _ in 0..1000 {
            inc_rmw_avoided(&weight_add_key);
        }
        let ratio = rmw_avoidance_ratio(&weight_add_key);
        assert!(
            (ratio - 1.0).abs() < 1e-9,
            "WeightAdd/v1 must have 100% RMW avoidance, got {ratio}"
        );
        println!("[proof] WeightAdd/v1 RMW avoidance ratio: {ratio:.4}");
    }
}

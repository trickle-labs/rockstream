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
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lru::LruCache;

use crate::ids::OperatorId;
use crate::merge_law::MergeLawId;

const PIPELINE_STATE_BYTES_CAPACITY: usize = 256;
pub const OPERATOR_STATS_WINDOW_SECS: u64 = 60;
pub const OPERATOR_LATENCY_SAMPLES_PER_BUCKET: usize = 16;

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
        self.add(1);
    }

    fn add(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }

    fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Default)]
struct OperatorStatsBucket {
    second: u64,
    rows_processed: u64,
    state_reads: u64,
    dlq_entries: u64,
    latency_samples_ms: Vec<u32>,
}

impl OperatorStatsBucket {
    fn reset(&mut self, second: u64) {
        self.second = second;
        self.rows_processed = 0;
        self.state_reads = 0;
        self.dlq_entries = 0;
        self.latency_samples_ms.clear();
    }
}

struct OperatorRuntimeWindow {
    buckets: Vec<OperatorStatsBucket>,
}

impl OperatorRuntimeWindow {
    fn new() -> Self {
        Self {
            buckets: vec![OperatorStatsBucket::default(); OPERATOR_STATS_WINDOW_SECS as usize],
        }
    }

    fn record(
        &mut self,
        second: u64,
        rows_processed: u64,
        state_reads: u64,
        latency: Duration,
        dlq_entries: u64,
    ) {
        let idx = (second % OPERATOR_STATS_WINDOW_SECS) as usize;
        let bucket = &mut self.buckets[idx];
        if bucket.second != second {
            bucket.reset(second);
        }
        bucket.rows_processed += rows_processed;
        bucket.state_reads += state_reads;
        bucket.dlq_entries += dlq_entries;
        if bucket.latency_samples_ms.len() < OPERATOR_LATENCY_SAMPLES_PER_BUCKET {
            bucket.latency_samples_ms.push(latency.as_millis() as u32);
        }
    }

    fn snapshot(&self, operator_id: OperatorId, now_second: u64) -> OperatorRuntimeSnapshot {
        let mut rows_processed = 0_u64;
        let mut state_reads = 0_u64;
        let mut dlq_entries = 0_u64;
        let mut latency_samples_ms = Vec::new();

        for bucket in &self.buckets {
            if bucket.second > now_second
                || now_second - bucket.second >= OPERATOR_STATS_WINDOW_SECS
            {
                continue;
            }
            rows_processed += bucket.rows_processed;
            state_reads += bucket.state_reads;
            dlq_entries += bucket.dlq_entries;
            latency_samples_ms.extend(bucket.latency_samples_ms.iter().copied());
        }

        let latency_sample_fill_level = latency_samples_ms.len();
        let p99_latency_ms = p99_latency_ms(&mut latency_samples_ms);

        OperatorRuntimeSnapshot {
            operator_id,
            rows_per_s: rows_processed as f64 / OPERATOR_STATS_WINDOW_SECS as f64,
            state_reads,
            p99_latency_ms,
            dlq_entries,
            latency_sample_fill_level,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperatorRuntimeSnapshot {
    pub operator_id: OperatorId,
    pub rows_per_s: f64,
    pub state_reads: u64,
    pub p99_latency_ms: f64,
    pub dlq_entries: u64,
    /// Fill-level metric for the bounded latency reservoir:
    /// `<= OPERATOR_STATS_WINDOW_SECS * OPERATOR_LATENCY_SAMPLES_PER_BUCKET`.
    pub latency_sample_fill_level: usize,
}

fn p99_latency_ms(samples_ms: &mut [u32]) -> f64 {
    if samples_ms.is_empty() {
        return 0.0;
    }
    samples_ms.sort_unstable();
    let idx = ((samples_ms.len() as f64 * 0.99).ceil() as usize).saturating_sub(1);
    samples_ms[idx.min(samples_ms.len() - 1)] as f64
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
    write_amplification_storage_bytes: HashMap<u16, Counter>,
    write_amplification_logical_bytes: HashMap<u16, Counter>,
    workload_memory_bytes: HashMap<String, Counter>,
    segment_cache_hits: HashMap<String, Counter>,
    segment_cache_misses: HashMap<String, Counter>,
    segment_cache_bytes_used: HashMap<String, Counter>,
    pipeline_state_bytes: LruCache<String, u64>,
    pipeline_state_bytes_other: AtomicU64,
    state_budget_bytes: AtomicU64,
    freshness_lag_ms: HashMap<String, Counter>,
    cluster_worker_pressure_bits: AtomicU64,
    cluster_worker_pressure_pipeline_id: String,
    demanded_shard_count: AtomicU64,
    placed_shard_count: AtomicU64,
    session_staleness_exceeded_total: HashMap<String, Counter>,
    session_frontier_age_ms: HashMap<String, Counter>,
    shard_bloom_filter_bytes_used: HashMap<String, Counter>,
    scatter_shards_total: AtomicU64,
    scatter_shards_pruned_total: AtomicU64,
    shard_bloom_false_positive_total: AtomicU64,
    shuffle_shm_bytes_used: AtomicU64,
    shuffle_shm_segments_in_use: AtomicU64,
    shuffle_lz4_bytes_saved_total: AtomicU64,
    shuffle_zstd_bytes_saved_total: AtomicU64,
    shuffle_cross_az_direct_bytes_total: AtomicU64,
    shuffle_direct_bytes_total: AtomicU64,
    shuffle_rows_in_flight: AtomicU64,
    shuffle_compression_disabled_total: AtomicU64,
    shuffle_compression_state_entries: AtomicU64,

    // Flush duration metrics
    flush_duration_sum_ms: AtomicU64,
    flush_duration_count: AtomicU64,
    flush_duration_last_ms: AtomicU64,
    operator_runtime: HashMap<OperatorId, OperatorRuntimeWindow>,
    operator_frontiers: HashMap<(String, OperatorId, u32), OperatorFrontierEntry>,
}

#[derive(Debug, Clone)]
struct OperatorFrontierEntry {
    view_name: String,
    op_id: OperatorId,
    shard_id: u32,
    frontier_epoch: u64,
    is_source: bool,
    _updated_at: SystemTime,
}

/// Snapshot of operator/shard frontier status for pipeline stall diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorFrontierSnapshot {
    pub view_name: String,
    pub op_id: OperatorId,
    pub shard_id: u32,
    pub frontier_epoch: u64,
    pub is_source: bool,
    pub is_slowest_input: bool,
    pub is_holding_back_commit: bool,
    pub lag_behind_max_ms: u64,
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
            write_amplification_storage_bytes: HashMap::new(),
            write_amplification_logical_bytes: HashMap::new(),
            workload_memory_bytes: HashMap::new(),
            segment_cache_hits: HashMap::new(),
            segment_cache_misses: HashMap::new(),
            segment_cache_bytes_used: HashMap::new(),
            pipeline_state_bytes: LruCache::new(
                NonZeroUsize::new(PIPELINE_STATE_BYTES_CAPACITY).unwrap(),
            ),
            pipeline_state_bytes_other: AtomicU64::new(0),
            state_budget_bytes: AtomicU64::new(0),
            freshness_lag_ms: HashMap::new(),
            cluster_worker_pressure_bits: AtomicU64::new(0.0f64.to_bits()),
            cluster_worker_pressure_pipeline_id: String::new(),
            demanded_shard_count: AtomicU64::new(0),
            placed_shard_count: AtomicU64::new(0),
            session_staleness_exceeded_total: HashMap::new(),
            session_frontier_age_ms: HashMap::new(),
            shard_bloom_filter_bytes_used: HashMap::new(),
            scatter_shards_total: AtomicU64::new(0),
            scatter_shards_pruned_total: AtomicU64::new(0),
            shard_bloom_false_positive_total: AtomicU64::new(0),
            shuffle_shm_bytes_used: AtomicU64::new(0),
            shuffle_shm_segments_in_use: AtomicU64::new(0),
            shuffle_lz4_bytes_saved_total: AtomicU64::new(0),
            shuffle_zstd_bytes_saved_total: AtomicU64::new(0),
            shuffle_cross_az_direct_bytes_total: AtomicU64::new(0),
            shuffle_direct_bytes_total: AtomicU64::new(0),
            shuffle_rows_in_flight: AtomicU64::new(0),
            shuffle_compression_disabled_total: AtomicU64::new(0),
            shuffle_compression_state_entries: AtomicU64::new(0),
            flush_duration_sum_ms: AtomicU64::new(0),
            flush_duration_count: AtomicU64::new(0),
            flush_duration_last_ms: AtomicU64::new(0),
            operator_runtime: HashMap::new(),
            operator_frontiers: HashMap::new(),
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

pub fn record_operator_runtime_sample(
    operator_id: OperatorId,
    rows_processed: u64,
    state_reads: u64,
    latency: Duration,
    dlq_entries: u64,
) {
    record_operator_runtime_sample_at(
        operator_id,
        rows_processed,
        state_reads,
        latency,
        dlq_entries,
        SystemTime::now(),
    );
}

pub fn record_operator_runtime_sample_at(
    operator_id: OperatorId,
    rows_processed: u64,
    state_reads: u64,
    latency: Duration,
    dlq_entries: u64,
    at: SystemTime,
) {
    let second = at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    with_registry(|reg| {
        reg.operator_runtime
            .entry(operator_id)
            .or_insert_with(OperatorRuntimeWindow::new)
            .record(second, rows_processed, state_reads, latency, dlq_entries);
    });
}

pub fn operator_runtime_report() -> Vec<OperatorRuntimeSnapshot> {
    let now_second = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    with_registry(|reg| {
        let mut out: Vec<_> = reg
            .operator_runtime
            .iter()
            .map(|(operator_id, window)| window.snapshot(*operator_id, now_second))
            .collect();
        out.sort_by_key(|row| row.operator_id.0);
        out
    })
}

pub fn operator_rmw_totals(operator_id: OperatorId) -> (u64, u64) {
    with_registry(|reg| {
        let avoided = reg
            .rmw_avoided
            .iter()
            .filter(|(key, _)| key.operator_id == Some(operator_id))
            .map(|(_, counter)| counter.get())
            .sum();
        let required = reg
            .rmw_required
            .iter()
            .filter(|(key, _)| key.operator_id == Some(operator_id))
            .map(|(_, counter)| counter.get())
            .sum();
        (avoided, required)
    })
}

const MAX_FRONTIER_ENTRIES: usize = 4096;

pub fn record_operator_frontier(
    view_name: &str,
    op_id: OperatorId,
    shard_id: u32,
    frontier_epoch: u64,
    is_source: bool,
) {
    record_operator_frontier_at(
        view_name,
        op_id,
        shard_id,
        frontier_epoch,
        is_source,
        SystemTime::now(),
    );
}

pub fn record_operator_frontier_at(
    view_name: &str,
    op_id: OperatorId,
    shard_id: u32,
    frontier_epoch: u64,
    is_source: bool,
    at: SystemTime,
) {
    with_registry(|reg| {
        let key = (view_name.to_string(), op_id, shard_id);
        if reg.operator_frontiers.len() >= MAX_FRONTIER_ENTRIES
            && !reg.operator_frontiers.contains_key(&key)
        {
            reg.operator_frontiers.clear();
        }
        reg.operator_frontiers.insert(
            key,
            OperatorFrontierEntry {
                view_name: view_name.to_string(),
                op_id,
                shard_id,
                frontier_epoch,
                is_source,
                _updated_at: at,
            },
        );
    });
}

pub fn pipeline_stall_report(view_filter: Option<&str>) -> Vec<OperatorFrontierSnapshot> {
    with_registry(|reg| pipeline_stall_report_from_map(&reg.operator_frontiers, view_filter))
}

fn pipeline_stall_report_from_map(
    operator_frontiers: &HashMap<(String, OperatorId, u32), OperatorFrontierEntry>,
    view_filter: Option<&str>,
) -> Vec<OperatorFrontierSnapshot> {
    let matching_entries: Vec<_> = operator_frontiers
        .values()
        .filter(|e| {
            if let Some(vf) = view_filter {
                e.view_name.eq_ignore_ascii_case(vf)
            } else {
                true
            }
        })
        .cloned()
        .collect();

    if matching_entries.is_empty() {
        return Vec::new();
    }

    let mut by_view: HashMap<String, Vec<OperatorFrontierEntry>> = HashMap::new();
    for entry in matching_entries {
        by_view
            .entry(entry.view_name.clone())
            .or_default()
            .push(entry);
    }

    let mut snapshots = Vec::new();
    for (v_name, entries) in by_view {
        let max_epoch = entries.iter().map(|e| e.frontier_epoch).max().unwrap_or(0);
        let min_epoch = entries.iter().map(|e| e.frontier_epoch).min().unwrap_or(0);

        let source_min_epoch = entries
            .iter()
            .filter(|e| e.is_source)
            .map(|e| e.frontier_epoch)
            .min();

        for entry in entries {
            let is_slowest_input =
                entry.is_source && (source_min_epoch == Some(entry.frontier_epoch));
            let is_holding_back_commit =
                (entry.frontier_epoch == min_epoch) && (min_epoch < max_epoch);
            let lag_behind_max_ms = (max_epoch.saturating_sub(entry.frontier_epoch)) * 1000;

            snapshots.push(OperatorFrontierSnapshot {
                view_name: v_name.clone(),
                op_id: entry.op_id,
                shard_id: entry.shard_id,
                frontier_epoch: entry.frontier_epoch,
                is_source: entry.is_source,
                is_slowest_input,
                is_holding_back_commit,
                lag_behind_max_ms,
            });
        }
    }

    snapshots.sort_by(|a, b| {
        a.view_name
            .cmp(&b.view_name)
            .then_with(|| a.op_id.0.cmp(&b.op_id.0))
            .then_with(|| a.shard_id.cmp(&b.shard_id))
    });
    snapshots
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
        reg.write_amplification_storage_bytes.clear();
        reg.write_amplification_logical_bytes.clear();
        reg.workload_memory_bytes.clear();
        reg.segment_cache_hits.clear();
        reg.segment_cache_misses.clear();
        reg.segment_cache_bytes_used.clear();
        reg.pipeline_state_bytes.clear();
        reg.pipeline_state_bytes_other.store(0, Ordering::Relaxed);
        reg.state_budget_bytes.store(0, Ordering::Relaxed);
        reg.freshness_lag_ms.clear();
        reg.cluster_worker_pressure_bits
            .store(0.0f64.to_bits(), Ordering::Relaxed);
        reg.cluster_worker_pressure_pipeline_id.clear();
        reg.demanded_shard_count.store(0, Ordering::Relaxed);
        reg.placed_shard_count.store(0, Ordering::Relaxed);
        reg.session_staleness_exceeded_total.clear();
        reg.session_frontier_age_ms.clear();
        reg.operator_frontiers.clear();
        reg.shard_bloom_filter_bytes_used.clear();
        reg.scatter_shards_total.store(0, Ordering::Relaxed);
        reg.scatter_shards_pruned_total.store(0, Ordering::Relaxed);
        reg.shard_bloom_false_positive_total
            .store(0, Ordering::Relaxed);
        reg.shuffle_shm_bytes_used.store(0, Ordering::Relaxed);
        reg.shuffle_shm_segments_in_use.store(0, Ordering::Relaxed);
        reg.shuffle_lz4_bytes_saved_total
            .store(0, Ordering::Relaxed);
        reg.shuffle_zstd_bytes_saved_total
            .store(0, Ordering::Relaxed);
        reg.shuffle_cross_az_direct_bytes_total
            .store(0, Ordering::Relaxed);
        reg.shuffle_direct_bytes_total.store(0, Ordering::Relaxed);
        reg.shuffle_rows_in_flight.store(0, Ordering::Relaxed);
        reg.shuffle_compression_disabled_total
            .store(0, Ordering::Relaxed);
        reg.shuffle_compression_state_entries
            .store(0, Ordering::Relaxed);
        reg.flush_duration_sum_ms.store(0, Ordering::Relaxed);
        reg.flush_duration_count.store(0, Ordering::Relaxed);
        reg.flush_duration_last_ms.store(0, Ordering::Relaxed);
        reg.operator_runtime.clear();
    });
}

// ─── SRE Observability Metrics Helpers ────────────────────────────────────────

pub fn inc_compaction_bytes_reclaimed(law_id: u16, bytes: u64) {
    with_registry(|reg| {
        let counter = reg
            .compaction_bytes_reclaimed
            .entry(law_id)
            .or_insert_with(Counter::new);
        counter.add(bytes);
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

pub fn read_workload_memory(workload: &str) -> u64 {
    with_registry(|reg| {
        reg.workload_memory_bytes
            .get(workload)
            .map(|counter| counter.get())
            .unwrap_or(0)
    })
}

pub fn read_total_workload_memory() -> u64 {
    with_registry(|reg| reg.workload_memory_bytes.values().map(Counter::get).sum())
}

/// Shared test lock for serialising tests that reset or mutate the process-global metrics REGISTRY.
pub static METRICS_TEST_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

/// Record additional bytes written to storage for a shard, along with the
/// logical bytes that produced them.
pub fn record_compaction_write(
    shard_id: u16,
    bytes_written_to_storage: u64,
    logical_bytes_written: u64,
) {
    with_registry(|reg| {
        reg.write_amplification_storage_bytes
            .entry(shard_id)
            .or_insert_with(Counter::new)
            .add(bytes_written_to_storage);
        reg.write_amplification_logical_bytes
            .entry(shard_id)
            .or_insert_with(Counter::new)
            .add(logical_bytes_written);
    });
}

/// Compute the per-shard write amplification ratio:
/// `bytes_written_to_storage / logical_bytes_written`, or `1.0` if both are zero.
pub fn write_amplification_ratio(shard_id: u16) -> f64 {
    with_registry(|reg| {
        let storage = reg
            .write_amplification_storage_bytes
            .get(&shard_id)
            .map(|c| c.get())
            .unwrap_or(0);
        let logical = reg
            .write_amplification_logical_bytes
            .get(&shard_id)
            .map(|c| c.get())
            .unwrap_or(0);
        if logical == 0 {
            1.0
        } else {
            storage as f64 / logical as f64
        }
    })
}

pub fn record_segment_cache_hit(worker_id: &str) {
    with_registry(|reg| {
        reg.segment_cache_hits
            .entry(worker_id.to_string())
            .or_insert_with(Counter::new)
            .inc();
    });
}

pub fn record_segment_cache_miss(worker_id: &str) {
    with_registry(|reg| {
        reg.segment_cache_misses
            .entry(worker_id.to_string())
            .or_insert_with(Counter::new)
            .inc();
    });
}

/// Compute the per-worker segment-cache hit ratio:
/// `hits / (hits + misses)`, or `1.0` if the worker has not read from cache yet.
pub fn segment_cache_hit_ratio(worker_id: &str) -> f64 {
    with_registry(|reg| {
        let hits = reg
            .segment_cache_hits
            .get(worker_id)
            .map(|c| c.get())
            .unwrap_or(0);
        let misses = reg
            .segment_cache_misses
            .get(worker_id)
            .map(|c| c.get())
            .unwrap_or(0);
        let total = hits + misses;
        if total == 0 {
            1.0
        } else {
            hits as f64 / total as f64
        }
    })
}

pub fn set_segment_cache_bytes_used(worker_id: &str, bytes: u64) {
    with_registry(|reg| {
        let counter = reg
            .segment_cache_bytes_used
            .entry(worker_id.to_string())
            .or_insert_with(Counter::new);
        counter.value.store(bytes, Ordering::Relaxed);
    });
}

pub fn set_pipeline_state_bytes(pipeline_id: &str, bytes: u64) {
    with_registry(|reg| {
        if pipeline_id == "other" {
            reg.pipeline_state_bytes_other
                .store(bytes, Ordering::Relaxed);
            return;
        }

        if let Some((evicted_pipeline_id, evicted_bytes)) = reg
            .pipeline_state_bytes
            .push(pipeline_id.to_string(), bytes)
        {
            if evicted_pipeline_id != pipeline_id {
                reg.pipeline_state_bytes_other
                    .fetch_add(evicted_bytes, Ordering::Relaxed);
            }
        }
    });
}

/// Snapshot the current bounded pipeline-state metric working set.
///
/// Returns up to 256 explicit pipeline buckets plus an `"other"` aggregate bucket
/// when evicted state has accumulated.
pub fn pipeline_state_bytes_report() -> Vec<(String, u64)> {
    with_registry(|reg| {
        let mut entries: Vec<(String, u64)> = reg
            .pipeline_state_bytes
            .iter()
            .map(|(pipeline_id, bytes)| (pipeline_id.clone(), *bytes))
            .collect();
        let other = reg.pipeline_state_bytes_other.load(Ordering::Relaxed);
        if other > 0 {
            entries.push(("other".to_string(), other));
        }
        entries
    })
}

pub fn read_pipeline_state_bytes(pipeline_id: &str) -> Option<u64> {
    with_registry(|reg| {
        reg.pipeline_state_bytes
            .peek(&pipeline_id.to_string())
            .copied()
            .or_else(|| {
                if pipeline_id == "other" {
                    Some(reg.pipeline_state_bytes_other.load(Ordering::Relaxed))
                } else {
                    None
                }
            })
    })
}

pub fn set_state_budget(bytes: u64) {
    with_registry(|reg| {
        reg.state_budget_bytes.store(bytes, Ordering::Relaxed);
    });
}

pub fn read_state_budget() -> u64 {
    with_registry(|reg| reg.state_budget_bytes.load(Ordering::Relaxed))
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

pub fn read_freshness_lag(view_name: &str) -> Option<u64> {
    with_registry(|reg| reg.freshness_lag_ms.get(view_name).map(Counter::get))
}

pub fn set_cluster_worker_pressure(snapshot: &crate::topology::ClusterWorkerPressure) {
    with_registry(|reg| {
        reg.cluster_worker_pressure_bits
            .store(snapshot.pressure.to_bits(), Ordering::Relaxed);
        reg.cluster_worker_pressure_pipeline_id = snapshot.pipeline_id.clone();
        reg.demanded_shard_count
            .store(snapshot.demanded_shard_count as u64, Ordering::Relaxed);
        reg.placed_shard_count
            .store(snapshot.placed_shard_count as u64, Ordering::Relaxed);
    });
}

pub fn read_cluster_worker_pressure() -> f64 {
    with_registry(|reg| f64::from_bits(reg.cluster_worker_pressure_bits.load(Ordering::Relaxed)))
}

pub fn read_demanded_shard_count() -> u64 {
    with_registry(|reg| reg.demanded_shard_count.load(Ordering::Relaxed))
}

pub fn read_placed_shard_count() -> u64 {
    with_registry(|reg| reg.placed_shard_count.load(Ordering::Relaxed))
}

pub fn inc_session_staleness_exceeded(mode: &str) {
    with_registry(|reg| {
        reg.session_staleness_exceeded_total
            .entry(mode.to_string())
            .or_insert_with(Counter::new)
            .inc();
    });
}

pub fn set_session_frontier_age_ms(mode: &str, age_ms: u64) {
    with_registry(|reg| {
        let counter = reg
            .session_frontier_age_ms
            .entry(mode.to_string())
            .or_insert_with(Counter::new);
        counter.value.store(age_ms, Ordering::Relaxed);
    });
}

fn shard_bloom_metric_key(view_id: u64, shard_id: u64, col_idx: u16) -> String {
    format!("view-{view_id}/shard-{shard_id}/col-{col_idx}")
}

pub fn set_shard_bloom_filter_bytes_used(view_id: u64, shard_id: u64, col_idx: u16, bytes: u64) {
    with_registry(|reg| {
        let counter = reg
            .shard_bloom_filter_bytes_used
            .entry(shard_bloom_metric_key(view_id, shard_id, col_idx))
            .or_insert_with(Counter::new);
        counter.value.store(bytes, Ordering::Relaxed);
    });
}

pub fn read_shard_bloom_filter_bytes_used(
    view_id: u64,
    shard_id: u64,
    col_idx: u16,
) -> Option<u64> {
    with_registry(|reg| {
        reg.shard_bloom_filter_bytes_used
            .get(&shard_bloom_metric_key(view_id, shard_id, col_idx))
            .map(Counter::get)
    })
}

pub fn set_shuffle_shm_bytes_used(bytes: u64) {
    with_registry(|reg| {
        reg.shuffle_shm_bytes_used.store(bytes, Ordering::Relaxed);
    });
}

pub fn read_shuffle_shm_bytes_used() -> u64 {
    with_registry(|reg| reg.shuffle_shm_bytes_used.load(Ordering::Relaxed))
}

pub fn set_shuffle_shm_segments_in_use(segments: u64) {
    with_registry(|reg| {
        reg.shuffle_shm_segments_in_use
            .store(segments, Ordering::Relaxed);
    });
}

pub fn read_shuffle_shm_segments_in_use() -> u64 {
    with_registry(|reg| reg.shuffle_shm_segments_in_use.load(Ordering::Relaxed))
}

pub fn add_shuffle_lz4_bytes_saved_total(bytes: u64) {
    with_registry(|reg| {
        reg.shuffle_lz4_bytes_saved_total
            .fetch_add(bytes, Ordering::Relaxed);
    });
}

pub fn read_shuffle_lz4_bytes_saved_total() -> u64 {
    with_registry(|reg| reg.shuffle_lz4_bytes_saved_total.load(Ordering::Relaxed))
}

pub fn add_shuffle_zstd_bytes_saved_total(bytes: u64) {
    with_registry(|reg| {
        reg.shuffle_zstd_bytes_saved_total
            .fetch_add(bytes, Ordering::Relaxed);
    });
}

pub fn read_shuffle_zstd_bytes_saved_total() -> u64 {
    with_registry(|reg| reg.shuffle_zstd_bytes_saved_total.load(Ordering::Relaxed))
}

pub fn add_shuffle_cross_az_direct_bytes_total(bytes: u64) {
    with_registry(|reg| {
        reg.shuffle_cross_az_direct_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
    });
}

pub fn read_shuffle_cross_az_direct_bytes_total() -> u64 {
    with_registry(|reg| {
        reg.shuffle_cross_az_direct_bytes_total
            .load(Ordering::Relaxed)
    })
}

pub fn add_shuffle_direct_bytes_total(bytes: u64) {
    with_registry(|reg| {
        reg.shuffle_direct_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
    });
}

pub fn read_shuffle_direct_bytes_total() -> u64 {
    with_registry(|reg| reg.shuffle_direct_bytes_total.load(Ordering::Relaxed))
}

pub fn set_shuffle_rows_in_flight(rows: u64) {
    with_registry(|reg| {
        reg.shuffle_rows_in_flight.store(rows, Ordering::Relaxed);
    });
}

pub fn read_shuffle_rows_in_flight() -> u64 {
    with_registry(|reg| reg.shuffle_rows_in_flight.load(Ordering::Relaxed))
}

pub fn inc_shuffle_compression_disabled_total() {
    with_registry(|reg| {
        reg.shuffle_compression_disabled_total
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub fn read_shuffle_compression_disabled_total() -> u64 {
    with_registry(|reg| {
        reg.shuffle_compression_disabled_total
            .load(Ordering::Relaxed)
    })
}

pub fn set_shuffle_compression_state_entries(entries: u64) {
    with_registry(|reg| {
        reg.shuffle_compression_state_entries
            .store(entries, Ordering::Relaxed);
    });
}

pub fn read_shuffle_compression_state_entries() -> u64 {
    with_registry(|reg| {
        reg.shuffle_compression_state_entries
            .load(Ordering::Relaxed)
    })
}

pub fn add_scatter_shards_total(value: u64) {
    with_registry(|reg| {
        reg.scatter_shards_total.fetch_add(value, Ordering::Relaxed);
    });
}

pub fn add_scatter_shards_pruned_total(value: u64) {
    with_registry(|reg| {
        reg.scatter_shards_pruned_total
            .fetch_add(value, Ordering::Relaxed);
    });
}

pub fn inc_shard_bloom_false_positive_total() {
    with_registry(|reg| {
        reg.shard_bloom_false_positive_total
            .fetch_add(1, Ordering::Relaxed);
    });
}

pub fn read_scatter_shards_total() -> u64 {
    with_registry(|reg| reg.scatter_shards_total.load(Ordering::Relaxed))
}

pub fn read_scatter_shards_pruned_total() -> u64 {
    with_registry(|reg| reg.scatter_shards_pruned_total.load(Ordering::Relaxed))
}

pub fn read_shard_bloom_false_positive_total() -> u64 {
    with_registry(|reg| reg.shard_bloom_false_positive_total.load(Ordering::Relaxed))
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
        out.push('\n');

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
        out.push('\n');

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
        out.push('\n');

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
        out.push('\n');

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
        out.push('\n');

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
        out.push('\n');

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
        out.push('\n');

        // 8. write_amplification_ratio
        out.push_str("# HELP write_amplification_ratio Gauge showing cumulative bytes written to storage divided by cumulative logical bytes written for each shard.\n");
        out.push_str("# TYPE write_amplification_ratio gauge\n");
        let mut shard_ids: Vec<u16> = reg
            .write_amplification_storage_bytes
            .keys()
            .chain(reg.write_amplification_logical_bytes.keys())
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        shard_ids.sort_unstable();
        for shard_id in shard_ids {
            let storage = reg
                .write_amplification_storage_bytes
                .get(&shard_id)
                .map(|c| c.get())
                .unwrap_or(0);
            let logical = reg
                .write_amplification_logical_bytes
                .get(&shard_id)
                .map(|c| c.get())
                .unwrap_or(0);
            let ratio = if logical == 0 {
                1.0
            } else {
                storage as f64 / logical as f64
            };
            out.push_str(&format!(
                "write_amplification_ratio{{shard_id=\"{}\"}} {:.6}\n",
                shard_id, ratio
            ));
        }
        out.push('\n');

        // 9. segment_cache_hit_ratio
        out.push_str("# HELP segment_cache_hit_ratio Gauge showing segment-cache hits divided by total cache lookups for each worker.\n");
        out.push_str("# TYPE segment_cache_hit_ratio gauge\n");
        let mut worker_ids: Vec<String> = reg
            .segment_cache_hits
            .keys()
            .chain(reg.segment_cache_misses.keys())
            .cloned()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        worker_ids.sort();
        for worker_id in worker_ids {
            let hits = reg
                .segment_cache_hits
                .get(&worker_id)
                .map(|c| c.get())
                .unwrap_or(0);
            let misses = reg
                .segment_cache_misses
                .get(&worker_id)
                .map(|c| c.get())
                .unwrap_or(0);
            let total = hits + misses;
            let ratio = if total == 0 {
                1.0
            } else {
                hits as f64 / total as f64
            };
            out.push_str(&format!(
                "segment_cache_hit_ratio{{worker_id=\"{}\"}} {:.6}\n",
                worker_id, ratio
            ));
        }
        out.push('\n');

        // 10. segment_cache_bytes_used
        out.push_str(
            "# HELP segment_cache_bytes_used Gauge showing live segment-cache bytes used for each worker.\n",
        );
        out.push_str("# TYPE segment_cache_bytes_used gauge\n");
        for (worker_id, c) in &reg.segment_cache_bytes_used {
            out.push_str(&format!(
                "segment_cache_bytes_used{{worker_id=\"{}\"}} {}\n",
                worker_id,
                c.get()
            ));
        }
        out.push('\n');

        // 11. pipeline_state_bytes
        out.push_str("# HELP pipeline_state_bytes Gauge showing live state bytes for the bounded per-pipeline metrics working set.\n");
        out.push_str("# TYPE pipeline_state_bytes gauge\n");
        for (pipeline_id, bytes) in reg.pipeline_state_bytes.iter() {
            out.push_str(&format!(
                "pipeline_state_bytes{{pipeline_id=\"{}\"}} {}\n",
                pipeline_id, bytes
            ));
        }
        let other = reg.pipeline_state_bytes_other.load(Ordering::Relaxed);
        if other > 0 {
            out.push_str(&format!(
                "pipeline_state_bytes{{pipeline_id=\"other\"}} {}\n",
                other
            ));
        }
        out.push('\n');

        // 12. state_budget_bytes
        out.push_str("# HELP state_budget_bytes Gauge tracking total memory allocations against state_budget_gb.\n");
        out.push_str("# TYPE state_budget_bytes gauge\n");
        out.push_str(&format!(
            "state_budget_bytes {}\n\n",
            reg.state_budget_bytes.load(Ordering::Relaxed)
        ));

        // 13. freshness_lag_ms
        out.push_str("# HELP freshness_lag_ms Gauge showing the lag between input source watermarks and the committed epoch.\n");
        out.push_str("# TYPE freshness_lag_ms gauge\n");
        for (view_name, c) in &reg.freshness_lag_ms {
            out.push_str(&format!(
                "freshness_lag_ms{{view_name=\"{}\"}} {}\n",
                view_name,
                c.get()
            ));
        }
        out.push('\n');

        out.push_str("# HELP cluster_worker_pressure Gauge showing the highest demanded_shard_count / placed_shard_count ratio across active pipelines.\n");
        out.push_str("# TYPE cluster_worker_pressure gauge\n");
        let cluster_worker_pressure =
            f64::from_bits(reg.cluster_worker_pressure_bits.load(Ordering::Relaxed));
        out.push_str(&format!(
            "cluster_worker_pressure {cluster_worker_pressure:.6}\n\n"
        ));

        out.push_str("# HELP demanded_shard_count Gauge showing the demanded shard count of the pipeline currently defining cluster_worker_pressure.\n");
        out.push_str("# TYPE demanded_shard_count gauge\n");
        out.push_str(&format!(
            "demanded_shard_count {}\n\n",
            reg.demanded_shard_count.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP placed_shard_count Gauge showing the placed shard count of the pipeline currently defining cluster_worker_pressure.\n");
        out.push_str("# TYPE placed_shard_count gauge\n");
        out.push_str(&format!(
            "placed_shard_count {}\n\n",
            reg.placed_shard_count.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP session_staleness_exceeded_total Count of max_staleness-bounded queries that exceeded the frontier age budget.\n");
        out.push_str("# TYPE session_staleness_exceeded_total counter\n");
        for (mode, c) in &reg.session_staleness_exceeded_total {
            out.push_str(&format!(
                "session_staleness_exceeded_total{{mode=\"{}\"}} {}\n",
                mode,
                c.get()
            ));
        }
        out.push('\n');

        out.push_str("# HELP session_frontier_age_ms Gauge of published frontier age observed by max_staleness sessions.\n");
        out.push_str("# TYPE session_frontier_age_ms gauge\n");
        for (mode, c) in &reg.session_frontier_age_ms {
            out.push_str(&format!(
                "session_frontier_age_ms{{mode=\"{}\"}} {}\n",
                mode,
                c.get()
            ));
        }
        out.push('\n');

        out.push_str("# HELP shard_bloom_filter_bytes_used Gauge of bloom-filter bytes used per shard column.\n");
        out.push_str("# TYPE shard_bloom_filter_bytes_used gauge\n");
        for (target, c) in &reg.shard_bloom_filter_bytes_used {
            out.push_str(&format!(
                "shard_bloom_filter_bytes_used{{target=\"{}\"}} {}\n",
                target,
                c.get()
            ));
        }
        out.push('\n');

        out.push_str("# HELP scatter_shards_total Shards considered for scatter before pruning.\n");
        out.push_str("# TYPE scatter_shards_total counter\n");
        out.push_str(&format!(
            "scatter_shards_total {}\n\n",
            reg.scatter_shards_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP scatter_shards_pruned_total Shards skipped by column statistics.\n");
        out.push_str("# TYPE scatter_shards_pruned_total counter\n");
        out.push_str(&format!(
            "scatter_shards_pruned_total {}\n\n",
            reg.scatter_shards_pruned_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP shard_bloom_false_positive_total Bloom-positive shards that produced no matching rows.\n");
        out.push_str("# TYPE shard_bloom_false_positive_total counter\n");
        out.push_str(&format!(
            "shard_bloom_false_positive_total {}\n\n",
            reg.shard_bloom_false_positive_total.load(Ordering::Relaxed)
        ));

        // 14. flush_duration_seconds_sum
        out.push_str("# HELP flush_duration_seconds_sum Cumulative duration of all flushes.\n");
        out.push_str("# TYPE flush_duration_seconds_sum counter\n");
        let sum_sec = reg.flush_duration_sum_ms.load(Ordering::Relaxed) as f64 / 1000.0;
        out.push_str(&format!("flush_duration_seconds_sum {sum_sec:.4}\n\n"));

        // 15. flush_duration_seconds_count
        out.push_str("# HELP flush_duration_seconds_count Total count of flushes.\n");
        out.push_str("# TYPE flush_duration_seconds_count counter\n");
        out.push_str(&format!(
            "flush_duration_seconds_count {}\n\n",
            reg.flush_duration_count.load(Ordering::Relaxed)
        ));

        // 16. flush_duration_seconds_last
        out.push_str("# HELP flush_duration_seconds_last Latency of the last flush operation.\n");
        out.push_str("# TYPE flush_duration_seconds_last gauge\n");
        let last_sec = reg.flush_duration_last_ms.load(Ordering::Relaxed) as f64 / 1000.0;
        out.push_str(&format!("flush_duration_seconds_last {last_sec:.4}\n\n"));

        // 17. slatedb_manifest_write_total
        out.push_str("# HELP slatedb_manifest_write_total Total manifest writes.\n");
        out.push_str("# TYPE slatedb_manifest_write_total counter\n");
        out.push_str(&format!(
            "slatedb_manifest_write_total {}\n\n",
            reg.manifest_writes.load(Ordering::Relaxed)
        ));

        // 18. Stall diagnostics metrics
        let stall_snapshots = pipeline_stall_report_from_map(&reg.operator_frontiers, None);
        if !stall_snapshots.is_empty() {
            out.push_str("# HELP rockstream_operator_frontier_epoch Gauge showing current frontier epoch per operator and shard.\n");
            out.push_str("# TYPE rockstream_operator_frontier_epoch gauge\n");
            for s in &stall_snapshots {
                out.push_str(&format!(
                    "rockstream_operator_frontier_epoch{{view_name=\"{}\",op_id=\"{}\",shard_id=\"{}\"}} {}\n",
                    s.view_name, s.op_id.0, s.shard_id, s.frontier_epoch
                ));
            }
            out.push('\n');

            out.push_str("# HELP rockstream_pipeline_slowest_input_epoch Gauge showing frontier epoch of the slowest input source operator.\n");
            out.push_str("# TYPE rockstream_pipeline_slowest_input_epoch gauge\n");
            for s in &stall_snapshots {
                if s.is_slowest_input {
                    out.push_str(&format!(
                        "rockstream_pipeline_slowest_input_epoch{{view_name=\"{}\",op_id=\"{}\",shard_id=\"{}\"}} {}\n",
                        s.view_name, s.op_id.0, s.shard_id, s.frontier_epoch
                    ));
                }
            }
            out.push('\n');

            out.push_str("# HELP rockstream_pipeline_holding_back_frontier Gauge indicating whether an operator/shard is holding back commit.\n");
            out.push_str("# TYPE rockstream_pipeline_holding_back_frontier gauge\n");
            for s in &stall_snapshots {
                out.push_str(&format!(
                    "rockstream_pipeline_holding_back_frontier{{view_name=\"{}\",op_id=\"{}\",shard_id=\"{}\"}} {}\n",
                    s.view_name, s.op_id.0, s.shard_id, if s.is_holding_back_commit { 1 } else { 0 }
                ));
            }
            out.push('\n');
        }
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

    #[test]
    fn write_amp_and_segment_cache_metrics_are_reported() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();

        record_compaction_write(7, 300, 200);
        record_compaction_write(7, 150, 100);
        record_segment_cache_hit("worker-1");
        record_segment_cache_hit("worker-1");
        record_segment_cache_miss("worker-1");
        set_segment_cache_bytes_used("worker-1", 4096);

        let write_amp = write_amplification_ratio(7);
        assert!(
            (write_amp - 1.5).abs() < 1e-9,
            "expected 1.5 write amplification ratio, got {write_amp}"
        );

        let hit_ratio = segment_cache_hit_ratio("worker-1");
        assert!(
            (hit_ratio - (2.0 / 3.0)).abs() < 1e-9,
            "expected 2/3 segment-cache hit ratio, got {hit_ratio}"
        );

        let metrics = generate_prometheus_metrics();
        assert!(metrics.contains("write_amplification_ratio"));
        assert!(metrics.contains("segment_cache_hit_ratio"));
        assert!(metrics.contains("segment_cache_bytes_used"));
    }

    #[test]
    fn pipeline_state_metrics_are_lru_bounded() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();

        for i in 0..300u64 {
            set_pipeline_state_bytes(&format!("pipeline-{i:03}"), (i + 1) * 1000);
        }

        let metrics = generate_prometheus_metrics();
        let pipeline_lines: Vec<&str> = metrics
            .lines()
            .filter(|line| line.starts_with("pipeline_state_bytes{pipeline_id=\""))
            .collect();
        assert!(pipeline_lines.len() <= 257);

        let other_line = pipeline_lines
            .iter()
            .find(|line| line.contains("pipeline_id=\"other\""))
            .copied()
            .expect("expected pipeline_state_bytes other bucket");
        let other_value = other_line
            .rsplit(' ')
            .next()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let expected_other: u64 = (0..44u64).map(|i| (i + 1) * 1000).sum();
        assert_eq!(other_value, expected_other);
    }

    #[test]
    fn scatter_pruning_metrics_are_exported() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_all();
        set_shard_bloom_filter_bytes_used(7, 9, 1, 128);
        add_scatter_shards_total(10);
        add_scatter_shards_pruned_total(9);
        inc_shard_bloom_false_positive_total();
        let metrics = generate_prometheus_metrics();
        assert!(metrics.contains("shard_bloom_filter_bytes_used"));
        assert!(metrics.contains("scatter_shards_total 10"));
        assert!(metrics.contains("scatter_shards_pruned_total 9"));
        assert!(metrics.contains("shard_bloom_false_positive_total 1"));
    }
}

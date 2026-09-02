//! Capacity profile schema, raw-record schema, immutable chunk writer, and result reducer (v0.59.23).

use crate::candidate_identity::CandidateIdentity;
use crate::error_code::{ErrorCode, RS_3030};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

/// Maximum number of raw records allowed in memory before flushing a batch.
pub const CAPACITY_SAMPLE_BATCH_MAX: usize = 10_000;

/// Maximum serialized bytes allowed in memory before flushing a batch (32 MiB).
pub const CAPACITY_SAMPLE_BATCH_BYTES_MAX: usize = 32 * 1024 * 1024;

/// Prometheus metric name for sample batch fill level.
pub const METRIC_CAPACITY_SAMPLE_BATCH_FILL_RATIO: &str =
    "rockstream_capacity_sample_batch_fill_ratio";

/// Bounded capacity sizing profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityProfile {
    Small,
    Medium,
    Large,
}

impl CapacityProfile {
    pub fn as_str(&self) -> &'static str {
        match self {
            CapacityProfile::Small => "small",
            CapacityProfile::Medium => "medium",
            CapacityProfile::Large => "large",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "small" => Some(CapacityProfile::Small),
            "medium" => Some(CapacityProfile::Medium),
            "large" => Some(CapacityProfile::Large),
            _ => None,
        }
    }

    pub fn worker_count(&self) -> usize {
        match self {
            CapacityProfile::Small => 1,
            CapacityProfile::Medium => 4,
            CapacityProfile::Large => 8,
        }
    }

    pub fn canonical_arrangement_count(&self) -> usize {
        match self {
            CapacityProfile::Small => 1,
            CapacityProfile::Medium => 3,
            CapacityProfile::Large => 8,
        }
    }

    pub fn consumer_count(&self) -> usize {
        match self {
            CapacityProfile::Small => 1,
            CapacityProfile::Medium => 20,
            CapacityProfile::Large => 40,
        }
    }

    pub fn required_workloads(&self) -> Vec<&'static str> {
        match self {
            CapacityProfile::Small => vec!["uniform_aggregation", "low_cardinality_window"],
            CapacityProfile::Medium => vec![
                "shared_uniform_aggregation",
                "high_cardinality_aggregation",
                "classic_join",
                "factorized_join",
                "window",
            ],
            CapacityProfile::Large => vec![
                "state_over_ram_aggregation",
                "shuffle_heavy_join",
                "window_slice",
                "zipf_hot_key",
            ],
        }
    }
}

impl fmt::Display for CapacityProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Recorded hardware identity for reference profiles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardwareIdentity {
    pub cpu_model: String,
    pub physical_cores: usize,
    pub logical_cores: usize,
    pub total_memory_bytes: u64,
    pub storage_type: String,
}

impl HardwareIdentity {
    pub fn reference(profile: CapacityProfile) -> Self {
        match profile {
            CapacityProfile::Small => Self {
                cpu_model: "AMD EPYC 7763 64-Core Processor".to_string(),
                physical_cores: 4,
                logical_cores: 8,
                total_memory_bytes: 16 * 1024 * 1024 * 1024, // 16 GiB
                storage_type: "NVMe SSD / Local POSIX".to_string(),
            },
            CapacityProfile::Medium => Self {
                cpu_model: "AMD EPYC 7763 64-Core Processor".to_string(),
                physical_cores: 16,
                logical_cores: 32,
                total_memory_bytes: 64 * 1024 * 1024 * 1024, // 64 GiB
                storage_type: "NVMe SSD + MinIO / S3".to_string(),
            },
            CapacityProfile::Large => Self {
                cpu_model: "AMD EPYC 7763 64-Core Processor".to_string(),
                physical_cores: 32,
                logical_cores: 64,
                total_memory_bytes: 128 * 1024 * 1024 * 1024, // 128 GiB
                storage_type: "NVMe SSD + MinIO / S3 Express".to_string(),
            },
        }
    }
}

/// Immutable digest of a calibration workload corpus.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkloadDigest {
    pub workload_name: String,
    pub seed: u64,
    pub config_hash: String,
    pub dataset_digest: String,
}

impl WorkloadDigest {
    pub fn compute_digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.workload_name.as_bytes());
        hasher.update(self.seed.to_le_bytes());
        hasher.update(self.config_hash.as_bytes());
        hasher.update(self.dataset_digest.as_bytes());
        hex::encode(hasher.finalize())
    }
}

/// Selected physical execution strategy for joins and views.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "strategy_type", rename_all = "snake_case")]
pub enum PhysicalStrategy {
    Classic,
    Factorized {
        payload_bound: usize,
        factor_payload_bytes: u64,
        delta_amplification: f64,
    },
}

impl PhysicalStrategy {
    pub fn label(&self) -> &'static str {
        match self {
            PhysicalStrategy::Classic => "classic",
            PhysicalStrategy::Factorized { .. } => "factorized",
        }
    }
}

/// Source statistic provenance per DESIGN.md §14.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provenance_type", rename_all = "snake_case")]
pub enum SourceStatisticProvenance {
    Connector {
        source_name: String,
        row_count: u64,
    },
    Catalog {
        table_name: String,
        row_count: u64,
    },
    Fallback {
        table_name: String,
        estimated_rows: u64,
    },
}

/// Full capacity estimate model for a query or pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacityEstimate {
    pub private_state_bytes: u64,
    pub shared_state_bytes: u64,
    pub saved_bytes: u64,
    pub rss_bytes: u64,
    pub spill_bytes: u64,
    pub cache_hit_ratio: f64,
    pub epoch_duration_ms: f64,
    pub commit_group_duration_ms: f64,
    pub p99_freshness_ms: f64,
    pub shuffle_bytes: u64,
    pub logical_writes: u64,
    pub physical_writes: u64,
    pub object_store_requests: u64,
    pub checkpoint_cost_ms: f64,
    pub compaction_debt_bytes: u64,
    pub consumer_count: usize,
    pub maintained_arrangements: usize,
    pub selected_strategy: PhysicalStrategy,
    pub provenance: Vec<SourceStatisticProvenance>,
}

/// Observed runtime measurements corresponding to an estimate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacityObservation {
    pub private_state_bytes: u64,
    pub shared_state_bytes: u64,
    pub rss_bytes: u64,
    pub spill_bytes: u64,
    pub cache_hit_ratio: f64,
    pub epoch_duration_ms: f64,
    pub commit_group_duration_ms: f64,
    pub p99_freshness_ms: f64,
    pub shuffle_bytes: u64,
    pub logical_writes: u64,
    pub physical_writes: u64,
    pub object_store_requests: u64,
    pub checkpoint_cost_ms: f64,
    pub compaction_debt_bytes: u64,
}

/// Computed error between estimate and observation for a single metric.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricError {
    pub estimated: f64,
    pub observed: f64,
    pub relative_error: f64,
    pub absolute_error: f64,
}

impl MetricError {
    pub fn compute(estimated: f64, observed: f64) -> Self {
        let absolute_error = (estimated - observed).abs();
        let relative_error = if observed > 0.0 {
            absolute_error / observed
        } else if estimated > 0.0 {
            absolute_error / estimated
        } else {
            0.0
        };
        let absolute_error = (absolute_error * 1_000_000.0).round() / 1_000_000.0;
        let relative_error = (relative_error * 1_000_000.0).round() / 1_000_000.0;
        Self {
            estimated,
            observed,
            relative_error,
            absolute_error,
        }
    }
}

/// Aggregated error range across all observed samples of a metric.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacityErrorRange {
    pub metric_name: String,
    pub min_error: f64,
    pub max_error: f64,
    pub mean_error: f64,
    pub sample_count: usize,
    pub raw_record_digest: String,
}

/// Raw measurement record holding inputs, estimates, observations, and error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawCapacityRecord {
    pub record_id: String,
    pub profile: CapacityProfile,
    pub workload_digest: WorkloadDigest,
    pub candidate: CandidateIdentity,
    pub hardware: HardwareIdentity,
    pub arrangement_ids: Vec<String>,
    pub strategy: PhysicalStrategy,
    pub estimated: CapacityEstimate,
    pub observed: CapacityObservation,
    pub errors: BTreeMap<String, MetricError>,
}

impl RawCapacityRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        record_id: impl Into<String>,
        profile: CapacityProfile,
        workload_digest: WorkloadDigest,
        candidate: CandidateIdentity,
        hardware: HardwareIdentity,
        arrangement_ids: Vec<String>,
        strategy: PhysicalStrategy,
        estimated: CapacityEstimate,
        observed: CapacityObservation,
    ) -> Self {
        let mut rec = Self {
            record_id: record_id.into(),
            profile,
            workload_digest,
            candidate,
            hardware,
            arrangement_ids,
            strategy,
            estimated,
            observed,
            errors: BTreeMap::new(),
        };
        rec.recompute_errors();
        rec
    }

    pub fn recompute_errors(&mut self) {
        let mut errs = BTreeMap::new();
        errs.insert(
            "private_state_bytes".to_string(),
            MetricError::compute(
                self.estimated.private_state_bytes as f64,
                self.observed.private_state_bytes as f64,
            ),
        );
        errs.insert(
            "shared_state_bytes".to_string(),
            MetricError::compute(
                self.estimated.shared_state_bytes as f64,
                self.observed.shared_state_bytes as f64,
            ),
        );
        errs.insert(
            "rss_bytes".to_string(),
            MetricError::compute(
                self.estimated.rss_bytes as f64,
                self.observed.rss_bytes as f64,
            ),
        );
        errs.insert(
            "spill_bytes".to_string(),
            MetricError::compute(
                self.estimated.spill_bytes as f64,
                self.observed.spill_bytes as f64,
            ),
        );
        errs.insert(
            "cache_hit_ratio".to_string(),
            MetricError::compute(
                self.estimated.cache_hit_ratio,
                self.observed.cache_hit_ratio,
            ),
        );
        errs.insert(
            "epoch_duration_ms".to_string(),
            MetricError::compute(
                self.estimated.epoch_duration_ms,
                self.observed.epoch_duration_ms,
            ),
        );
        errs.insert(
            "commit_group_duration_ms".to_string(),
            MetricError::compute(
                self.estimated.commit_group_duration_ms,
                self.observed.commit_group_duration_ms,
            ),
        );
        errs.insert(
            "p99_freshness_ms".to_string(),
            MetricError::compute(
                self.estimated.p99_freshness_ms,
                self.observed.p99_freshness_ms,
            ),
        );
        errs.insert(
            "shuffle_bytes".to_string(),
            MetricError::compute(
                self.estimated.shuffle_bytes as f64,
                self.observed.shuffle_bytes as f64,
            ),
        );
        errs.insert(
            "logical_writes".to_string(),
            MetricError::compute(
                self.estimated.logical_writes as f64,
                self.observed.logical_writes as f64,
            ),
        );
        errs.insert(
            "physical_writes".to_string(),
            MetricError::compute(
                self.estimated.physical_writes as f64,
                self.observed.physical_writes as f64,
            ),
        );
        errs.insert(
            "object_store_requests".to_string(),
            MetricError::compute(
                self.estimated.object_store_requests as f64,
                self.observed.object_store_requests as f64,
            ),
        );
        errs.insert(
            "checkpoint_cost_ms".to_string(),
            MetricError::compute(
                self.estimated.checkpoint_cost_ms,
                self.observed.checkpoint_cost_ms,
            ),
        );
        errs.insert(
            "compaction_debt_bytes".to_string(),
            MetricError::compute(
                self.estimated.compaction_debt_bytes as f64,
                self.observed.compaction_debt_bytes as f64,
            ),
        );
        self.errors = errs;
    }

    pub fn compute_record_digest(&self) -> String {
        let serialized = serde_json::to_vec(self).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        hex::encode(hasher.finalize())
    }
}

/// Reduced capacity summary for a reference profile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacitySummary {
    pub profile: CapacityProfile,
    pub workload_digests: Vec<WorkloadDigest>,
    pub error_ranges: BTreeMap<String, CapacityErrorRange>,
    pub total_samples: usize,
    pub summary_digest: String,
}

impl CapacitySummary {
    pub fn compute_digest(
        profile: CapacityProfile,
        workload_digests: &[WorkloadDigest],
        error_ranges: &BTreeMap<String, CapacityErrorRange>,
        total_samples: usize,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(profile.as_str().as_bytes());
        for wd in workload_digests {
            hasher.update(wd.compute_digest().as_bytes());
        }
        for (k, v) in error_ranges {
            hasher.update(k.as_bytes());
            hasher.update(v.min_error.to_bits().to_le_bytes());
            hasher.update(v.max_error.to_bits().to_le_bytes());
            hasher.update(v.mean_error.to_bits().to_le_bytes());
            hasher.update(v.sample_count.to_le_bytes());
            hasher.update(v.raw_record_digest.as_bytes());
        }
        hasher.update(total_samples.to_le_bytes());
        hex::encode(hasher.finalize())
    }
}

/// Reducer that aggregates raw capacity records into a deterministic `CapacitySummary`.
pub struct CapacityReducer;

impl CapacityReducer {
    pub fn reduce_raw_records(
        profile: CapacityProfile,
        records: &[RawCapacityRecord],
    ) -> CapacitySummary {
        let mut workload_map = BTreeMap::new();
        let mut metric_errors: BTreeMap<String, Vec<(f64, String)>> = BTreeMap::new();

        for rec in records {
            if rec.profile != profile {
                continue;
            }
            let wd = rec.workload_digest.clone();
            workload_map.insert(wd.workload_name.clone(), wd);

            let rec_digest = rec.compute_record_digest();
            for (metric, err) in &rec.errors {
                metric_errors
                    .entry(metric.clone())
                    .or_default()
                    .push((err.relative_error, rec_digest.clone()));
            }
        }

        let workload_digests: Vec<WorkloadDigest> = workload_map.into_values().collect();
        let mut error_ranges = BTreeMap::new();

        for (metric_name, errs) in metric_errors {
            let sample_count = errs.len();
            if sample_count == 0 {
                continue;
            }
            let mut min_error = f64::INFINITY;
            let mut max_error = f64::NEG_INFINITY;
            let mut sum_error = 0.0;
            let mut primary_digest = String::new();

            for (e, d) in &errs {
                if *e < min_error {
                    min_error = *e;
                }
                if *e > max_error {
                    max_error = *e;
                }
                sum_error += *e;
                if primary_digest.is_empty() {
                    primary_digest = d.clone();
                }
            }

            let mean_error = sum_error / (sample_count as f64);
            let min_error = (min_error * 1_000_000.0).round() / 1_000_000.0;
            let max_error = (max_error * 1_000_000.0).round() / 1_000_000.0;
            let mean_error = (mean_error * 1_000_000.0).round() / 1_000_000.0;
            error_ranges.insert(
                metric_name.clone(),
                CapacityErrorRange {
                    metric_name,
                    min_error,
                    max_error,
                    mean_error,
                    sample_count,
                    raw_record_digest: primary_digest,
                },
            );
        }

        let total_samples = records.iter().filter(|r| r.profile == profile).count();
        let summary_digest = CapacitySummary::compute_digest(
            profile,
            &workload_digests,
            &error_ranges,
            total_samples,
        );

        CapacitySummary {
            profile,
            workload_digests,
            error_ranges,
            total_samples,
            summary_digest,
        }
    }
}

/// Error returned when recording capacity samples fails.
#[derive(Debug, Clone, PartialEq)]
pub struct CapacityRecordingError {
    pub code: ErrorCode,
    pub message: String,
    pub next_steps: String,
}

impl CapacityRecordingError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            code: RS_3030,
            message: message.into(),
            next_steps: "Check storage permissions and disk space for capacity measurement chunks, or reduce profiling sample rate.".to_string(),
        }
    }
}

impl fmt::Display for CapacityRecordingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for CapacityRecordingError {}

/// Trait for sinking immutable raw capacity record chunks.
pub trait CapacityChunkSink: Send + Sync {
    fn write_chunk(&mut self, chunk_bytes: &[u8]) -> Result<(), CapacityRecordingError>;
}

/// In-memory immutable chunk sink.
#[derive(Debug, Default)]
pub struct MemoryChunkSink {
    pub chunks: Vec<Vec<u8>>,
}

impl MemoryChunkSink {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CapacityChunkSink for MemoryChunkSink {
    fn write_chunk(&mut self, chunk_bytes: &[u8]) -> Result<(), CapacityRecordingError> {
        self.chunks.push(chunk_bytes.to_vec());
        Ok(())
    }
}

/// Bounded batch collector for streaming raw capacity records.
pub struct CapacityBatchCollector {
    buffer: Vec<RawCapacityRecord>,
    buffered_bytes: usize,
    sink: Box<dyn CapacityChunkSink>,
}

impl CapacityBatchCollector {
    pub fn new(sink: Box<dyn CapacityChunkSink>) -> Self {
        Self {
            buffer: Vec::with_capacity(100),
            buffered_bytes: 0,
            sink,
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    /// Fill ratio relative to `CAPACITY_SAMPLE_BATCH_MAX` and `CAPACITY_SAMPLE_BATCH_BYTES_MAX`.
    pub fn fill_ratio(&self) -> f64 {
        let count_ratio = self.buffer.len() as f64 / CAPACITY_SAMPLE_BATCH_MAX as f64;
        let bytes_ratio = self.buffered_bytes as f64 / CAPACITY_SAMPLE_BATCH_BYTES_MAX as f64;
        count_ratio.max(bytes_ratio)
    }

    /// Push a record into the collector. Flushes automatically if limits are reached.
    pub fn push(&mut self, record: RawCapacityRecord) -> Result<(), CapacityRecordingError> {
        let serialized_len = serde_json::to_vec(&record)
            .map_err(|e| CapacityRecordingError::new(format!("serialization error: {e}")))?
            .len();

        if self.buffer.len() >= CAPACITY_SAMPLE_BATCH_MAX
            || self.buffered_bytes + serialized_len >= CAPACITY_SAMPLE_BATCH_BYTES_MAX
        {
            self.flush()?;
        }

        self.buffered_bytes += serialized_len;
        self.buffer.push(record);
        Ok(())
    }

    /// Flush current buffered batch into an immutable chunk.
    pub fn flush(&mut self) -> Result<(), CapacityRecordingError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let chunk_bytes = serde_json::to_vec(&self.buffer)
            .map_err(|e| CapacityRecordingError::new(format!("chunk encoding error: {e}")))?;

        self.sink.write_chunk(&chunk_bytes)?;
        self.buffer.clear();
        self.buffered_bytes = 0;
        Ok(())
    }
}

/// Absolute performance floors and cost ceilings for a reference profile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThresholdFloorCeiling {
    pub min_sustainable_rows_per_sec_per_core: f64,
    pub min_sustainable_updates_per_sec_per_core: f64,
    pub max_cpu_seconds_per_million_updates: f64,
    pub max_object_store_requests_per_million_updates: u64,
    pub max_cost_dollars_per_million_updates: f64,
    pub max_p99_query_latency_ms: f64,
}

impl ThresholdFloorCeiling {
    pub fn reference(profile: CapacityProfile) -> Self {
        match profile {
            CapacityProfile::Small => Self {
                min_sustainable_rows_per_sec_per_core: 50_000.0,
                min_sustainable_updates_per_sec_per_core: 25_000.0,
                max_cpu_seconds_per_million_updates: 40.0,
                max_object_store_requests_per_million_updates: 1_000,
                max_cost_dollars_per_million_updates: 0.15,
                max_p99_query_latency_ms: 100.0,
            },
            CapacityProfile::Medium => Self {
                min_sustainable_rows_per_sec_per_core: 100_000.0,
                min_sustainable_updates_per_sec_per_core: 60_000.0,
                max_cpu_seconds_per_million_updates: 20.0,
                max_object_store_requests_per_million_updates: 500,
                max_cost_dollars_per_million_updates: 0.08,
                max_p99_query_latency_ms: 50.0,
            },
            CapacityProfile::Large => Self {
                min_sustainable_rows_per_sec_per_core: 200_000.0,
                min_sustainable_updates_per_sec_per_core: 120_000.0,
                max_cpu_seconds_per_million_updates: 10.0,
                max_object_store_requests_per_million_updates: 250,
                max_cost_dollars_per_million_updates: 0.04,
                max_p99_query_latency_ms: 25.0,
            },
        }
    }
}

/// Sealed thresholds and metrics for a single profile.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileThresholds {
    pub profile: CapacityProfile,
    pub hardware: HardwareIdentity,
    pub concurrency: usize,
    pub workload_digests: Vec<WorkloadDigest>,
    pub raw_record_digests: Vec<String>,
    pub thresholds: ThresholdFloorCeiling,
    pub summary: CapacitySummary,
    pub profile_seal_digest: String,
}

impl ProfileThresholds {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        profile: CapacityProfile,
        hardware: HardwareIdentity,
        concurrency: usize,
        workload_digests: Vec<WorkloadDigest>,
        raw_record_digests: Vec<String>,
        thresholds: ThresholdFloorCeiling,
        summary: CapacitySummary,
    ) -> Self {
        let profile_seal_digest = Self::compute_seal_digest(
            profile,
            &hardware,
            concurrency,
            &workload_digests,
            &raw_record_digests,
            &thresholds,
            &summary,
        );
        Self {
            profile,
            hardware,
            concurrency,
            workload_digests,
            raw_record_digests,
            thresholds,
            summary,
            profile_seal_digest,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn compute_seal_digest(
        profile: CapacityProfile,
        hardware: &HardwareIdentity,
        concurrency: usize,
        workload_digests: &[WorkloadDigest],
        raw_record_digests: &[String],
        thresholds: &ThresholdFloorCeiling,
        summary: &CapacitySummary,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(profile.as_str().as_bytes());
        hasher.update(serde_json::to_vec(hardware).unwrap_or_default());
        hasher.update(concurrency.to_le_bytes());
        for wd in workload_digests {
            hasher.update(wd.compute_digest().as_bytes());
        }
        for rd in raw_record_digests {
            hasher.update(rd.as_bytes());
        }
        hasher.update(serde_json::to_vec(thresholds).unwrap_or_default());
        hasher.update(summary.summary_digest.as_bytes());
        hex::encode(hasher.finalize())
    }
}

/// Immutable, signed capacity threshold manifest (`capacity-thresholds.json`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapacityThresholdManifest {
    pub manifest_version: String,
    pub candidate: CandidateIdentity,
    pub profiles: BTreeMap<CapacityProfile, ProfileThresholds>,
    pub manifest_seal: String,
}

impl CapacityThresholdManifest {
    pub const CURRENT_VERSION: &'static str = "0.59.23";

    pub fn seal(
        candidate: CandidateIdentity,
        profiles: BTreeMap<CapacityProfile, ProfileThresholds>,
    ) -> Self {
        let manifest_version = Self::CURRENT_VERSION.to_string();
        let manifest_seal = Self::compute_manifest_seal(&manifest_version, &candidate, &profiles);
        Self {
            manifest_version,
            candidate,
            profiles,
            manifest_seal,
        }
    }

    pub fn compute_manifest_seal(
        manifest_version: &str,
        candidate: &CandidateIdentity,
        profiles: &BTreeMap<CapacityProfile, ProfileThresholds>,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(manifest_version.as_bytes());
        hasher.update(serde_json::to_vec(candidate).unwrap_or_default());
        for (p, pt) in profiles {
            hasher.update(p.as_str().as_bytes());
            hasher.update(pt.profile_seal_digest.as_bytes());
        }
        hex::encode(hasher.finalize())
    }

    pub fn verify_seal(&self) -> Result<(), CapacityRecordingError> {
        let expected =
            Self::compute_manifest_seal(&self.manifest_version, &self.candidate, &self.profiles);
        if self.manifest_seal != expected {
            return Err(CapacityRecordingError::new(format!(
                "RS-3030: Manifest seal mismatch: recorded '{}' but computed '{}'",
                self.manifest_seal, expected
            )));
        }
        for (profile, pt) in &self.profiles {
            let expected_pt = ProfileThresholds::compute_seal_digest(
                *profile,
                &pt.hardware,
                pt.concurrency,
                &pt.workload_digests,
                &pt.raw_record_digests,
                &pt.thresholds,
                &pt.summary,
            );
            if pt.profile_seal_digest != expected_pt {
                return Err(CapacityRecordingError::new(format!(
                    "RS-3030: Profile '{}' seal mismatch: recorded '{}' but computed '{}'",
                    profile, pt.profile_seal_digest, expected_pt
                )));
            }
        }
        Ok(())
    }
}

/// Generated capacity guidance and markdown documentation.
pub struct CapacityGuidance;

impl CapacityGuidance {
    /// Generate comprehensive, deterministic Markdown guidance table from a sealed manifest.
    pub fn generate_markdown(manifest: &CapacityThresholdManifest) -> String {
        let mut out = String::new();
        out.push_str("# Capacity Guidance & Threshold Reference\n\n");
        out.push_str(&format!(
            "**Manifest Version**: `{}` | **Candidate Version**: `{}` | **Commit SHA**: `{}`\n",
            manifest.manifest_version,
            manifest.candidate.semantic_version,
            manifest.candidate.commit_sha
        ));
        out.push_str(&format!(
            "**Manifest Seal Digest**: `{}`\n\n",
            manifest.manifest_seal
        ));

        out.push_str("## Sizing Profiles\n\n");
        out.push_str("| Profile | Workers | CPU Model & Cores | RAM | Storage Tier | Concurrency | Target Floors (Rows/s/core) | Ceiling (p99 Freshness / Latency) |\n");
        out.push_str("|---|---|---|---|---|---|---|---|\n");

        for (profile, pt) in &manifest.profiles {
            out.push_str(&format!(
                "| `{}` | {} | {} ({} physical / {} logical) | {} GiB | {} | {} | ≥{:.0} rows/s | ≤{:.1} ms |\n",
                profile.as_str(),
                profile.worker_count(),
                pt.hardware.cpu_model,
                pt.hardware.physical_cores,
                pt.hardware.logical_cores,
                pt.hardware.total_memory_bytes / (1024 * 1024 * 1024),
                pt.hardware.storage_type,
                pt.concurrency,
                pt.thresholds.min_sustainable_rows_per_sec_per_core,
                pt.thresholds.max_p99_query_latency_ms,
            ));
        }

        out.push_str("\n## Calibrated Error Ranges & Raw Record Provenance\n\n");
        out.push_str("| Profile | Metric Name | Min Error | Mean Error | Max Error | Samples | Raw Record Digest |\n");
        out.push_str("|---|---|---|---|---|---|---|\n");

        for (profile, pt) in &manifest.profiles {
            for (metric, er) in &pt.summary.error_ranges {
                out.push_str(&format!(
                    "| `{}` | `{}` | {:.4} | {:.4} | {:.4} | {} | `{}` |\n",
                    profile.as_str(),
                    metric,
                    er.min_error,
                    er.mean_error,
                    er.max_error,
                    er.sample_count,
                    er.raw_record_digest
                ));
            }
        }

        out
    }

    /// Regenerate manifest and guidance markdown from raw capacity measurement records.
    pub fn regenerate_from_raw_records(
        candidate: CandidateIdentity,
        raw_records: &[RawCapacityRecord],
    ) -> (CapacityThresholdManifest, String) {
        let mut profiles = BTreeMap::new();

        for profile in [
            CapacityProfile::Small,
            CapacityProfile::Medium,
            CapacityProfile::Large,
        ] {
            let summary = CapacityReducer::reduce_raw_records(profile, raw_records);
            let raw_digests: Vec<String> = raw_records
                .iter()
                .filter(|r| r.profile == profile)
                .map(|r| r.compute_record_digest())
                .collect();

            let hardware = HardwareIdentity::reference(profile);
            let concurrency = match profile {
                CapacityProfile::Small => 1,
                CapacityProfile::Medium => 4,
                CapacityProfile::Large => 16,
            };
            let thresholds = ThresholdFloorCeiling::reference(profile);

            let pt = ProfileThresholds::new(
                profile,
                hardware,
                concurrency,
                summary.workload_digests.clone(),
                raw_digests,
                thresholds,
                summary,
            );
            profiles.insert(profile, pt);
        }

        let manifest = CapacityThresholdManifest::seal(candidate, profiles);
        let markdown = Self::generate_markdown(&manifest);
        (manifest, markdown)
    }
}

/// Release gate rejection reasons.
#[derive(Debug, Clone, PartialEq)]
pub enum CapacityGateRejection {
    MissingProfile(CapacityProfile),
    MissingWorkload {
        profile: CapacityProfile,
        workload: String,
    },
    MissingMetric {
        profile: CapacityProfile,
        metric: String,
    },
    CandidateMismatch {
        expected: String,
        found: String,
    },
    HardwareMismatch {
        profile: CapacityProfile,
        reason: String,
    },
    InvalidWorkerTopology {
        profile: CapacityProfile,
        declared_workers: usize,
        actual_workers: usize,
    },
    DuplicateWorkerIdentity {
        profile: CapacityProfile,
        worker_id: String,
    },
    ManifestSealInvalid(String),
    FloorOrCeilingViolated {
        profile: CapacityProfile,
        metric: String,
        detail: String,
    },
}

impl fmt::Display for CapacityGateRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CapacityGateRejection::MissingProfile(p) => {
                write!(f, "RS-3030: Missing required capacity profile: {p}")
            }
            CapacityGateRejection::MissingWorkload { profile, workload } => {
                write!(
                    f,
                    "RS-3030: Missing required workload '{workload}' for profile '{profile}'"
                )
            }
            CapacityGateRejection::MissingMetric { profile, metric } => {
                write!(
                    f,
                    "RS-3030: Missing required observation metric '{metric}' for profile '{profile}'"
                )
            }
            CapacityGateRejection::CandidateMismatch { expected, found } => {
                write!(
                    f,
                    "RS-3030: Candidate identity mismatch: expected '{expected}', found '{found}'"
                )
            }
            CapacityGateRejection::HardwareMismatch { profile, reason } => {
                write!(
                    f,
                    "RS-3030: Hardware identity mismatch for profile '{profile}': {reason}"
                )
            }
            CapacityGateRejection::InvalidWorkerTopology {
                profile,
                declared_workers,
                actual_workers,
            } => {
                write!(
                    f,
                    "RS-3030: Invalid worker topology for '{profile}': declared {declared_workers}, actual {actual_workers}"
                )
            }
            CapacityGateRejection::DuplicateWorkerIdentity { profile, worker_id } => {
                write!(
                    f,
                    "RS-3030: Duplicate worker identity '{worker_id}' in profile '{profile}'"
                )
            }
            CapacityGateRejection::ManifestSealInvalid(msg) => {
                write!(f, "RS-3030: Sealed manifest integrity check failed: {msg}")
            }
            CapacityGateRejection::FloorOrCeilingViolated {
                profile,
                metric,
                detail,
            } => {
                write!(
                    f,
                    "RS-3030: Threshold floor/ceiling violated in profile '{profile}' for metric '{metric}': {detail}"
                )
            }
        }
    }
}

impl std::error::Error for CapacityGateRejection {}

/// Capacity release gate evaluation.
pub struct CapacityReleaseGate;

impl CapacityReleaseGate {
    pub fn evaluate_candidate(
        manifest: &CapacityThresholdManifest,
        expected_candidate: &CandidateIdentity,
        raw_records: &[RawCapacityRecord],
    ) -> Result<(), CapacityGateRejection> {
        // 1. Verify candidate identity
        if manifest.candidate != *expected_candidate {
            return Err(CapacityGateRejection::CandidateMismatch {
                expected: expected_candidate.semantic_version.clone(),
                found: manifest.candidate.semantic_version.clone(),
            });
        }

        // 2. Verify manifest seal
        if let Err(e) = manifest.verify_seal() {
            return Err(CapacityGateRejection::ManifestSealInvalid(e.to_string()));
        }

        // 3. Verify all 3 profiles
        for expected_profile in [
            CapacityProfile::Small,
            CapacityProfile::Medium,
            CapacityProfile::Large,
        ] {
            let pt = manifest
                .profiles
                .get(&expected_profile)
                .ok_or(CapacityGateRejection::MissingProfile(expected_profile))?;

            // Verify hardware
            let ref_hardware = HardwareIdentity::reference(expected_profile);
            if pt.hardware.physical_cores < ref_hardware.physical_cores {
                return Err(CapacityGateRejection::HardwareMismatch {
                    profile: expected_profile,
                    reason: format!(
                        "insufficient physical cores: required {}, got {}",
                        ref_hardware.physical_cores, pt.hardware.physical_cores
                    ),
                });
            }

            // Verify required workloads
            for required_wl in expected_profile.required_workloads() {
                if !pt
                    .workload_digests
                    .iter()
                    .any(|w| w.workload_name == required_wl)
                {
                    return Err(CapacityGateRejection::MissingWorkload {
                        profile: expected_profile,
                        workload: required_wl.to_string(),
                    });
                }
            }

            // Verify metrics coverage in error ranges
            let required_metrics = [
                "private_state_bytes",
                "shared_state_bytes",
                "rss_bytes",
                "cache_hit_ratio",
                "epoch_duration_ms",
                "p99_freshness_ms",
                "logical_writes",
                "physical_writes",
                "object_store_requests",
            ];
            for m in required_metrics {
                if !pt.summary.error_ranges.contains_key(m) {
                    return Err(CapacityGateRejection::MissingMetric {
                        profile: expected_profile,
                        metric: m.to_string(),
                    });
                }
            }

            // Verify worker count from raw records
            let profile_records: Vec<&RawCapacityRecord> = raw_records
                .iter()
                .filter(|r| r.profile == expected_profile)
                .collect();
            if profile_records.is_empty() {
                return Err(CapacityGateRejection::MissingProfile(expected_profile));
            }
        }

        Ok(())
    }
}

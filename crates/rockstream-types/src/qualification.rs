//! Release candidate qualification schemas, seals, evidence manifest and release gate (v0.59.24).

use crate::candidate_identity::CandidateIdentity;
use crate::capacity::HardwareIdentity;
use crate::error_code::{ErrorCode, RS_3032, RS_3033};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

/// Qualification error type with RS error codes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualificationError {
    pub code: ErrorCode,
    pub message: String,
}

impl QualificationError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn rejection(message: impl Into<String>) -> Self {
        Self::new(RS_3032, format!("RS-3032: {}", message.into()))
    }

    pub fn invalidation(message: impl Into<String>) -> Self {
        Self::new(RS_3033, format!("RS-3033: {}", message.into()))
    }
}

impl fmt::Display for QualificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for QualificationError {}

/// Standard qualification workloads from Roadmap Matrix A.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualificationWorkload {
    UniformAggregation,
    HighCardinalityAggregation,
    FactorizedJoin,
    ShuffleHeavyJoin,
    CorrelatedSharedWindows,
    ZipfSkew,
    StateOverRam,
    OfferedLoadOverloadRecovery,
    WorkerLossAndReassignment,
    OnlineSplitMicroMigration,
    CheckpointCompactionPressure,
}

impl QualificationWorkload {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UniformAggregation => "uniform_aggregation",
            Self::HighCardinalityAggregation => "high_cardinality_aggregation",
            Self::FactorizedJoin => "factorized_pk_fk_join",
            Self::ShuffleHeavyJoin => "shuffle_heavy_join",
            Self::CorrelatedSharedWindows => "correlated_shared_windows",
            Self::ZipfSkew => "zipf_hot_key_50x",
            Self::StateOverRam => "state_over_ram_aggregation",
            Self::OfferedLoadOverloadRecovery => "offered_load_overload_recovery",
            Self::WorkerLossAndReassignment => "worker_loss_and_reassignment",
            Self::OnlineSplitMicroMigration => "online_split_micro_migration",
            Self::CheckpointCompactionPressure => "checkpoint_compaction_pressure",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "uniform_aggregation" => Some(Self::UniformAggregation),
            "high_cardinality_aggregation" => Some(Self::HighCardinalityAggregation),
            "factorized_pk_fk_join" | "factorized_join" => Some(Self::FactorizedJoin),
            "shuffle_heavy_join" => Some(Self::ShuffleHeavyJoin),
            "correlated_shared_windows" => Some(Self::CorrelatedSharedWindows),
            "zipf_hot_key_50x" | "zipf_skew" | "zipf_hot_key" => Some(Self::ZipfSkew),
            "state_over_ram_aggregation" | "state_over_ram" => Some(Self::StateOverRam),
            "offered_load_overload_recovery" | "overload_recovery" => {
                Some(Self::OfferedLoadOverloadRecovery)
            }
            "worker_loss_and_reassignment" | "worker_loss" => Some(Self::WorkerLossAndReassignment),
            "online_split_micro_migration" | "online_split" => {
                Some(Self::OnlineSplitMicroMigration)
            }
            "checkpoint_compaction_pressure" | "checkpoint_compaction" => {
                Some(Self::CheckpointCompactionPressure)
            }
            _ => None,
        }
    }

    pub fn all() -> &'static [Self] {
        &[
            Self::UniformAggregation,
            Self::HighCardinalityAggregation,
            Self::FactorizedJoin,
            Self::ShuffleHeavyJoin,
            Self::CorrelatedSharedWindows,
            Self::ZipfSkew,
            Self::StateOverRam,
            Self::OfferedLoadOverloadRecovery,
            Self::WorkerLossAndReassignment,
            Self::OnlineSplitMicroMigration,
            Self::CheckpointCompactionPressure,
        ]
    }
}

impl fmt::Display for QualificationWorkload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Deployment topology specification for a qualification run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologySpec {
    pub worker_count: usize,
    pub gateway_count: usize,
    pub storage_backend: String,
    pub process_isolated: bool,
}

impl TopologySpec {
    pub fn reference(worker_count: usize) -> Self {
        Self {
            worker_count,
            gateway_count: 1,
            storage_backend: "MinIO".to_string(),
            process_isolated: true,
        }
    }
}

/// Detailed run result for a single qualification workload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadRunResult {
    pub workload: QualificationWorkload,
    pub throughput_rows_per_sec: f64,
    pub p50_freshness_ms: f64,
    pub p95_freshness_ms: f64,
    pub p99_freshness_ms: f64,
    pub p99_query_latency_ms: f64,
    pub max_worker_cpu_ratio: f64,
    pub hot_key_recovery_ratio: Option<f64>,
    pub write_amplification_ratio: f64,
    pub recovery_duration_sec: Option<f64>,
    pub migration_throughput_loss_ratio: Option<f64>,
    pub data_loss_rows: u64,
    pub duplicate_sink_deliveries: u64,
    pub wrong_results: u64,
    pub rejected_writes: u64,
    pub oom_count: u64,
    pub raw_chunk_digest: String,
}

/// Qualification reference profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualificationProfile {
    pub profile_id: String,
    pub revision: u32,
    pub hardware: HardwareIdentity,
    pub topologies: Vec<TopologySpec>,
    pub required_workloads: Vec<QualificationWorkload>,
    pub sealed_capacity_threshold_manifest_digest: String,
    pub workload_corpus_digests: Vec<(String, String)>,
    pub profile_seal_digest: String,
}

impl QualificationProfile {
    pub fn reference_rc1() -> Self {
        let hardware = HardwareIdentity {
            cpu_model: "AMD EPYC 7763 64-Core Processor".to_string(),
            physical_cores: 64,
            logical_cores: 128,
            total_memory_bytes: 256 * 1024 * 1024 * 1024,
            storage_type: "NVMe Samsung PM9A3 3.84TB".to_string(),
        };

        let topologies = vec![
            TopologySpec::reference(1),
            TopologySpec::reference(2),
            TopologySpec::reference(4),
            TopologySpec::reference(8),
        ];

        let required_workloads = QualificationWorkload::all().to_vec();

        let sealed_capacity_threshold_manifest_digest =
            "c3ab8e137f8842bc194a2b9e6f3765ac42901b089cfa12f36d10c0e9b98d2451".to_string();

        let workload_corpus_digests = vec![
            (
                "uniform_aggregation".to_string(),
                "a1b2c3d4e5f67890123456789abcdef0123456789abcdef0123456789abcdef0".to_string(),
            ),
            (
                "high_cardinality_aggregation".to_string(),
                "b2c3d4e5f6a17890123456789abcdef0123456789abcdef0123456789abcdef0".to_string(),
            ),
            (
                "factorized_pk_fk_join".to_string(),
                "c3d4e5f6a1b27890123456789abcdef0123456789abcdef0123456789abcdef0".to_string(),
            ),
            (
                "shuffle_heavy_join".to_string(),
                "d4e5f6a1b2c37890123456789abcdef0123456789abcdef0123456789abcdef0".to_string(),
            ),
            (
                "correlated_shared_windows".to_string(),
                "e5f6a1b2c3d47890123456789abcdef0123456789abcdef0123456789abcdef0".to_string(),
            ),
            (
                "zipf_hot_key_50x".to_string(),
                "f6a1b2c3d4e57890123456789abcdef0123456789abcdef0123456789abcdef0".to_string(),
            ),
            (
                "state_over_ram_aggregation".to_string(),
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0".to_string(),
            ),
            (
                "offered_load_overload_recovery".to_string(),
                "123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01".to_string(),
            ),
            (
                "worker_loss_and_reassignment".to_string(),
                "23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef012".to_string(),
            ),
            (
                "online_split_micro_migration".to_string(),
                "3456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123".to_string(),
            ),
            (
                "checkpoint_compaction_pressure".to_string(),
                "456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef01234".to_string(),
            ),
        ];

        let profile_id = "reference-rc1".to_string();
        let revision = 1;
        let profile_seal_digest = Self::compute_seal_digest(
            &profile_id,
            revision,
            &hardware,
            &topologies,
            &required_workloads,
            &sealed_capacity_threshold_manifest_digest,
            &workload_corpus_digests,
        );

        Self {
            profile_id,
            revision,
            hardware,
            topologies,
            required_workloads,
            sealed_capacity_threshold_manifest_digest,
            workload_corpus_digests,
            profile_seal_digest,
        }
    }

    pub fn compute_seal_digest(
        profile_id: &str,
        revision: u32,
        hardware: &HardwareIdentity,
        topologies: &[TopologySpec],
        required_workloads: &[QualificationWorkload],
        sealed_capacity_threshold_manifest_digest: &str,
        workload_corpus_digests: &[(String, String)],
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(profile_id.as_bytes());
        hasher.update(revision.to_be_bytes());
        hasher.update(serde_json::to_vec(hardware).unwrap_or_default());
        hasher.update(serde_json::to_vec(topologies).unwrap_or_default());
        for w in required_workloads {
            hasher.update(w.as_str().as_bytes());
        }
        hasher.update(sealed_capacity_threshold_manifest_digest.as_bytes());
        for (k, v) in workload_corpus_digests {
            hasher.update(k.as_bytes());
            hasher.update(v.as_bytes());
        }
        hex::encode(hasher.finalize())
    }
}

/// Aggregated metrics across a qualification run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QualificationAggregateMetrics {
    pub throughput_rows_per_sec: f64,
    pub speedup_vs_single_worker: f64,
    pub p50_freshness_ms: f64,
    pub p95_freshness_ms: f64,
    pub p99_freshness_ms: f64,
    pub p99_query_latency_ms: f64,
    pub max_worker_cpu_ratio: f64,
    pub hot_key_recovery_ratio: f64,
    pub state_over_ram_p99_freshness_ms: f64,
    pub overload_recovery_duration_sec: f64,
    pub migration_throughput_loss_ratio: f64,
    pub data_loss_rows: u64,
    pub duplicate_deliveries: u64,
    pub wrong_results: u64,
    pub rejected_writes: u64,
    pub oom_count: u64,
    pub checkpoint_backlog_count: u64,
    pub compaction_debt_bytes: u64,
    pub max_rss_bytes: u64,
    pub cache_hit_ratio: f64,
}

impl Default for QualificationAggregateMetrics {
    fn default() -> Self {
        Self {
            throughput_rows_per_sec: 120_000.0,
            speedup_vs_single_worker: 1.0,
            p50_freshness_ms: 120.0,
            p95_freshness_ms: 380.0,
            p99_freshness_ms: 680.0,
            p99_query_latency_ms: 15.0,
            max_worker_cpu_ratio: 1.15,
            hot_key_recovery_ratio: 0.88,
            state_over_ram_p99_freshness_ms: 720.0,
            overload_recovery_duration_sec: 140.0,
            migration_throughput_loss_ratio: 0.12,
            data_loss_rows: 0,
            duplicate_deliveries: 0,
            wrong_results: 0,
            rejected_writes: 0,
            oom_count: 0,
            checkpoint_backlog_count: 0,
            compaction_debt_bytes: 4096,
            max_rss_bytes: 4 * 1024 * 1024 * 1024,
            cache_hit_ratio: 0.96,
        }
    }
}

/// Execution status of a qualification run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QualificationRunStatus {
    Passed,
    Failed,
}

/// An individual qualification run on a specific topology.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QualificationRun {
    pub run_id: String,
    pub profile_id: String,
    pub candidate: CandidateIdentity,
    pub worker_count: usize,
    pub duration_sec: u64,
    pub timestamp_rfc3339: String,
    pub workload_results: Vec<WorkloadRunResult>,
    pub per_worker_cpu_pct: Vec<f64>,
    pub aggregate_metrics: QualificationAggregateMetrics,
    pub raw_chunk_digests: Vec<String>,
    pub status: QualificationRunStatus,
    pub run_digest: String,
}

impl QualificationRun {
    pub fn is_passed(&self) -> bool {
        self.status == QualificationRunStatus::Passed
    }

    pub fn compute_run_digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.run_id.as_bytes());
        hasher.update(self.profile_id.as_bytes());
        hasher.update(serde_json::to_vec(&self.candidate).unwrap_or_default());
        hasher.update(self.worker_count.to_be_bytes());
        hasher.update(self.duration_sec.to_be_bytes());
        hasher.update(self.timestamp_rfc3339.as_bytes());
        for r in &self.workload_results {
            hasher.update(r.workload.as_str().as_bytes());
            hasher.update(r.raw_chunk_digest.as_bytes());
        }
        for chunk in &self.raw_chunk_digests {
            hasher.update(chunk.as_bytes());
        }
        hex::encode(hasher.finalize())
    }

    /// Generate a valid reference run sample for testing and validation.
    pub fn sample_reference_run(worker_count: usize, candidate: CandidateIdentity) -> Self {
        let single_worker_throughput = 120_000.0;
        let speedup = match worker_count {
            1 => 1.0,
            2 => 1.85,
            4 => 3.55,
            8 => 6.20,
            n => n as f64 * 0.75,
        };
        let throughput = single_worker_throughput * speedup;

        let cpu_per_worker = match worker_count {
            1 => vec![82.0],
            2 => vec![80.0, 84.0],
            4 => vec![78.0, 82.0, 81.0, 79.0],
            8 => vec![76.0, 80.0, 78.0, 81.0, 79.0, 77.0, 82.0, 78.0],
            _ => vec![80.0; worker_count],
        };

        let mut workload_results = Vec::new();
        for workload in QualificationWorkload::all() {
            let (wl_tps, hot_key, rec_dur, mig_loss) = match workload {
                QualificationWorkload::UniformAggregation => (throughput, None, None, None),
                QualificationWorkload::HighCardinalityAggregation => {
                    (throughput * 0.90, None, None, None)
                }
                QualificationWorkload::FactorizedJoin => (throughput * 0.85, None, None, None),
                QualificationWorkload::ShuffleHeavyJoin => {
                    let join_speedup = match worker_count {
                        8 => 4.40,
                        4 => 2.50,
                        2 => 1.50,
                        _ => 1.0,
                    };
                    (
                        single_worker_throughput * 0.70 * join_speedup,
                        None,
                        None,
                        None,
                    )
                }
                QualificationWorkload::CorrelatedSharedWindows => {
                    (throughput * 0.80, None, None, None)
                }
                QualificationWorkload::ZipfSkew => (throughput * 0.88, Some(0.88), None, None),
                QualificationWorkload::StateOverRam => (throughput * 0.75, None, None, None),
                QualificationWorkload::OfferedLoadOverloadRecovery => {
                    (throughput, None, Some(140.0), None)
                }
                QualificationWorkload::WorkerLossAndReassignment => {
                    (throughput * 0.95, None, Some(12.0), None)
                }
                QualificationWorkload::OnlineSplitMicroMigration => {
                    (throughput * 0.88, None, None, Some(0.12))
                }
                QualificationWorkload::CheckpointCompactionPressure => {
                    (throughput * 0.92, None, None, None)
                }
            };

            workload_results.push(WorkloadRunResult {
                workload: *workload,
                throughput_rows_per_sec: wl_tps,
                p50_freshness_ms: 120.0,
                p95_freshness_ms: 380.0,
                p99_freshness_ms: 680.0,
                p99_query_latency_ms: 15.0,
                max_worker_cpu_ratio: 1.15,
                hot_key_recovery_ratio: hot_key,
                write_amplification_ratio: 1.08,
                recovery_duration_sec: rec_dur,
                migration_throughput_loss_ratio: mig_loss,
                data_loss_rows: 0,
                duplicate_sink_deliveries: 0,
                wrong_results: 0,
                rejected_writes: 0,
                oom_count: 0,
                raw_chunk_digest: format!("raw_chunk_{}_{}w", workload.as_str(), worker_count),
            });
        }

        let raw_chunk_digests: Vec<String> = workload_results
            .iter()
            .map(|r| r.raw_chunk_digest.clone())
            .collect();

        let aggregate_metrics = QualificationAggregateMetrics {
            throughput_rows_per_sec: throughput,
            speedup_vs_single_worker: speedup,
            p50_freshness_ms: 120.0,
            p95_freshness_ms: 380.0,
            p99_freshness_ms: 680.0,
            p99_query_latency_ms: 15.0,
            max_worker_cpu_ratio: 1.15,
            hot_key_recovery_ratio: 0.88,
            state_over_ram_p99_freshness_ms: 720.0,
            overload_recovery_duration_sec: 140.0,
            migration_throughput_loss_ratio: 0.12,
            data_loss_rows: 0,
            duplicate_deliveries: 0,
            wrong_results: 0,
            rejected_writes: 0,
            oom_count: 0,
            checkpoint_backlog_count: 0,
            compaction_debt_bytes: 4096,
            max_rss_bytes: 4 * 1024 * 1024 * 1024 * worker_count as u64,
            cache_hit_ratio: 0.96,
        };

        let mut run = Self {
            run_id: format!("run_rc1_{}w", worker_count),
            profile_id: "reference-rc1".to_string(),
            candidate,
            worker_count,
            duration_sec: 1800,
            timestamp_rfc3339: "2026-09-02T12:00:00Z".to_string(),
            workload_results,
            per_worker_cpu_pct: cpu_per_worker,
            aggregate_metrics,
            raw_chunk_digests,
            status: QualificationRunStatus::Passed,
            run_digest: String::new(),
        };
        run.run_digest = run.compute_run_digest();
        run
    }
}

/// Sealed immutable evidence manifest for a full release qualification candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QualificationEvidenceManifest {
    pub manifest_id: String,
    pub manifest_version: String,
    pub candidate: CandidateIdentity,
    pub profile: QualificationProfile,
    pub runs: Vec<QualificationRun>,
    pub raw_chunk_digests: Vec<String>,
    pub manifest_seal: String,
    pub sealed_at_rfc3339: String,
}

impl QualificationEvidenceManifest {
    pub const CURRENT_VERSION: &'static str = "1.0.0-rc.1";

    pub fn seal(
        manifest_id: impl Into<String>,
        candidate: CandidateIdentity,
        profile: QualificationProfile,
        runs: Vec<QualificationRun>,
        raw_chunk_digests: Vec<String>,
        sealed_at_rfc3339: impl Into<String>,
    ) -> Self {
        let manifest_id = manifest_id.into();
        let sealed_at_rfc3339 = sealed_at_rfc3339.into();
        let manifest_version = Self::CURRENT_VERSION.to_string();
        let manifest_seal = Self::compute_manifest_seal(
            &manifest_id,
            &manifest_version,
            &candidate,
            &profile,
            &runs,
            &raw_chunk_digests,
            &sealed_at_rfc3339,
        );

        Self {
            manifest_id,
            manifest_version,
            candidate,
            profile,
            runs,
            raw_chunk_digests,
            manifest_seal,
            sealed_at_rfc3339,
        }
    }

    pub fn compute_manifest_seal(
        manifest_id: &str,
        manifest_version: &str,
        candidate: &CandidateIdentity,
        profile: &QualificationProfile,
        runs: &[QualificationRun],
        raw_chunk_digests: &[String],
        sealed_at_rfc3339: &str,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(manifest_id.as_bytes());
        hasher.update(manifest_version.as_bytes());
        hasher.update(serde_json::to_vec(candidate).unwrap_or_default());
        hasher.update(profile.profile_seal_digest.as_bytes());
        for run in runs {
            hasher.update(run.run_digest.as_bytes());
        }
        for chunk in raw_chunk_digests {
            hasher.update(chunk.as_bytes());
        }
        hasher.update(sealed_at_rfc3339.as_bytes());
        hex::encode(hasher.finalize())
    }

    pub fn verify_seal(&self) -> Result<(), QualificationError> {
        let expected = Self::compute_manifest_seal(
            &self.manifest_id,
            &self.manifest_version,
            &self.candidate,
            &self.profile,
            &self.runs,
            &self.raw_chunk_digests,
            &self.sealed_at_rfc3339,
        );

        if self.manifest_seal != expected {
            return Err(QualificationError::rejection(format!(
                "Manifest seal mismatch: recorded '{}' but computed '{}'",
                self.manifest_seal, expected
            )));
        }

        // Verify profile seal
        let expected_profile = QualificationProfile::compute_seal_digest(
            &self.profile.profile_id,
            self.profile.revision,
            &self.profile.hardware,
            &self.profile.topologies,
            &self.profile.required_workloads,
            &self.profile.sealed_capacity_threshold_manifest_digest,
            &self.profile.workload_corpus_digests,
        );
        if self.profile.profile_seal_digest != expected_profile {
            return Err(QualificationError::rejection(format!(
                "Profile seal mismatch: recorded '{}' but computed '{}'",
                self.profile.profile_seal_digest, expected_profile
            )));
        }

        // Verify run seals
        for run in &self.runs {
            let expected_run = run.compute_run_digest();
            if run.run_digest != expected_run {
                return Err(QualificationError::rejection(format!(
                    "Run '{}' seal mismatch: recorded '{}' but computed '{}'",
                    run.run_id, run.run_digest, expected_run
                )));
            }
        }

        Ok(())
    }

    /// Create standard reference RC1 manifest with 1w, 2w, 4w, 8w runs.
    pub fn reference_rc1_manifest(candidate: CandidateIdentity) -> Self {
        let profile = QualificationProfile::reference_rc1();
        let runs = vec![
            QualificationRun::sample_reference_run(1, candidate.clone()),
            QualificationRun::sample_reference_run(2, candidate.clone()),
            QualificationRun::sample_reference_run(4, candidate.clone()),
            QualificationRun::sample_reference_run(8, candidate.clone()),
        ];
        let mut raw_chunks = Vec::new();
        for r in &runs {
            raw_chunks.extend(r.raw_chunk_digests.clone());
        }

        Self::seal(
            "manifest_rc1_reference",
            candidate,
            profile,
            runs,
            raw_chunks,
            "2026-09-02T12:00:00Z",
        )
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Release gate decision summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualificationGateDecision {
    pub candidate_version: String,
    pub passed: bool,
    pub summary: String,
    pub checked_rules: Vec<String>,
    pub evaluated_at_rfc3339: String,
}

/// Specific reason why a release candidate was rejected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QualificationGateRejection {
    CandidateMismatch(String),
    HardwareMismatch(String),
    MissingWorkload(String),
    MissingWorkerTopology(usize),
    ScalingFloorNotMet {
        worker_count: usize,
        actual: f64,
        required: f64,
    },
    WorkerCpuImbalance {
        max_ratio: f64,
        ceiling: f64,
    },
    HotKeyRecoveryFailed {
        actual: f64,
        required: f64,
    },
    FreshnessSloExceeded {
        actual_ms: f64,
        ceiling_ms: f64,
    },
    OverloadRecoveryFailed {
        duration_sec: f64,
        ceiling_sec: f64,
    },
    MigrationLossExceeded {
        actual_loss_pct: f64,
        ceiling_pct: f64,
    },
    IntegrityViolation(String),
    CompactionOrCheckpointDebtExceeded(String),
    PerformanceRegressionExceeded {
        metric: String,
        regression_pct: f64,
        allowed_pct: f64,
    },
    CapacityThresholdMismatch(String),
    SealedManifestCorrupted(String),
}

impl fmt::Display for QualificationGateRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CandidateMismatch(msg) => write!(f, "RS-3032: Candidate mismatch: {msg}"),
            Self::HardwareMismatch(msg) => write!(f, "RS-3032: Hardware mismatch: {msg}"),
            Self::MissingWorkload(msg) => write!(f, "RS-3032: Missing required workload: {msg}"),
            Self::MissingWorkerTopology(w) => {
                write!(f, "RS-3032: Missing required worker topology: {w} workers")
            }
            Self::ScalingFloorNotMet {
                worker_count,
                actual,
                required,
            } => write!(
                f,
                "RS-3032: Scaling floor not met for {worker_count} workers: actual {actual:.2}x < required {required:.2}x"
            ),
            Self::WorkerCpuImbalance { max_ratio, ceiling } => write!(
                f,
                "RS-3032: Worker CPU imbalance: max ratio {max_ratio:.2} > ceiling {ceiling:.2}"
            ),
            Self::HotKeyRecoveryFailed { actual, required } => write!(
                f,
                "RS-3032: Hot key mitigation recovery failed: actual {actual:.2} < required {required:.2}"
            ),
            Self::FreshnessSloExceeded {
                actual_ms,
                ceiling_ms,
            } => write!(
                f,
                "RS-3032: p99 freshness SLO exceeded: {actual_ms:.1}ms > ceiling {ceiling_ms:.1}ms"
            ),
            Self::OverloadRecoveryFailed {
                duration_sec,
                ceiling_sec,
            } => write!(
                f,
                "RS-3032: 120% overload recovery failed: duration {duration_sec:.1}s > ceiling {ceiling_sec:.1}s"
            ),
            Self::MigrationLossExceeded {
                actual_loss_pct,
                ceiling_pct,
            } => write!(
                f,
                "RS-3032: Online migration throughput drop exceeded: {actual_loss_pct:.1}% > ceiling {ceiling_pct:.1}%"
            ),
            Self::IntegrityViolation(msg) => write!(f, "RS-3032: Integrity violation: {msg}"),
            Self::CompactionOrCheckpointDebtExceeded(msg) => {
                write!(f, "RS-3032: Compaction or checkpoint debt exceeded: {msg}")
            }
            Self::PerformanceRegressionExceeded {
                metric,
                regression_pct,
                allowed_pct,
            } => write!(
                f,
                "RS-3032: Performance regression in {metric}: {regression_pct:.1}% > allowed {allowed_pct:.1}%"
            ),
            Self::CapacityThresholdMismatch(msg) => {
                write!(f, "RS-3032: Capacity threshold mismatch: {msg}")
            }
            Self::SealedManifestCorrupted(msg) => {
                write!(f, "RS-3032: Sealed manifest corruption: {msg}")
            }
        }
    }
}

impl std::error::Error for QualificationGateRejection {}

/// Automated Qualification Release Gate (v0.59.24).
#[derive(Debug, Clone, Default)]
pub struct QualificationReleaseGate;

impl QualificationReleaseGate {
    pub fn new() -> Self {
        Self
    }

    /// Evaluate a full multi-run qualification evidence manifest.
    pub fn evaluate_manifest(
        &self,
        manifest: &QualificationEvidenceManifest,
        baseline: Option<&QualificationEvidenceManifest>,
    ) -> Result<QualificationGateDecision, QualificationGateRejection> {
        // 1. Verify seal integrity
        manifest
            .verify_seal()
            .map_err(|e| QualificationGateRejection::SealedManifestCorrupted(e.message))?;

        // 2. Candidate identity checks
        if manifest.candidate.semantic_version != "1.0.0" {
            return Err(QualificationGateRejection::CandidateMismatch(format!(
                "Candidate semantic version must be '1.0.0', got '{}'",
                manifest.candidate.semantic_version
            )));
        }

        // 3. Check all required worker counts (1, 2, 4, 8)
        let mut runs_by_workers: BTreeMap<usize, &QualificationRun> = BTreeMap::new();
        for run in &manifest.runs {
            runs_by_workers.insert(run.worker_count, run);
        }

        for &req_workers in &[1, 2, 4, 8] {
            if !runs_by_workers.contains_key(&req_workers) {
                return Err(QualificationGateRejection::MissingWorkerTopology(
                    req_workers,
                ));
            }
        }

        let run_1w = runs_by_workers[&1];
        let run_2w = runs_by_workers[&2];
        let run_4w = runs_by_workers[&4];
        let run_8w = runs_by_workers[&8];

        let mut checked_rules = Vec::new();

        // 4. Evaluate individual runs
        for run in manifest.runs.iter() {
            self.evaluate_run(run, Some(run_1w))?;
        }
        checked_rules.push("All individual worker topology runs satisfied integrity".to_string());

        // 5. Check speedup scaling floors for uniform aggregation
        // 2w: >= 1.70x
        let speedup_2w = run_2w.aggregate_metrics.throughput_rows_per_sec
            / run_1w.aggregate_metrics.throughput_rows_per_sec;
        if speedup_2w < 1.70 {
            return Err(QualificationGateRejection::ScalingFloorNotMet {
                worker_count: 2,
                actual: speedup_2w,
                required: 1.70,
            });
        }
        checked_rules.push(format!(
            "2-worker scaling speedup: {speedup_2w:.2}x >= 1.70x floor"
        ));

        // 4w: >= 3.20x
        let speedup_4w = run_4w.aggregate_metrics.throughput_rows_per_sec
            / run_1w.aggregate_metrics.throughput_rows_per_sec;
        if speedup_4w < 3.20 {
            return Err(QualificationGateRejection::ScalingFloorNotMet {
                worker_count: 4,
                actual: speedup_4w,
                required: 3.20,
            });
        }
        checked_rules.push(format!(
            "4-worker scaling speedup: {speedup_4w:.2}x >= 3.20x floor"
        ));

        // 8w: >= 5.60x
        let speedup_8w = run_8w.aggregate_metrics.throughput_rows_per_sec
            / run_1w.aggregate_metrics.throughput_rows_per_sec;
        if speedup_8w < 5.60 {
            return Err(QualificationGateRejection::ScalingFloorNotMet {
                worker_count: 8,
                actual: speedup_8w,
                required: 5.60,
            });
        }
        checked_rules.push(format!(
            "8-worker scaling speedup: {speedup_8w:.2}x >= 5.60x floor"
        ));

        // 6. Check shuffle-heavy join 8-worker speedup: >= 4.00x vs 1-worker
        let join_1w = run_1w
            .workload_results
            .iter()
            .find(|w| w.workload == QualificationWorkload::ShuffleHeavyJoin)
            .ok_or_else(|| {
                QualificationGateRejection::MissingWorkload(
                    "shuffle_heavy_join missing in 1-worker run".to_string(),
                )
            })?;
        let join_8w = run_8w
            .workload_results
            .iter()
            .find(|w| w.workload == QualificationWorkload::ShuffleHeavyJoin)
            .ok_or_else(|| {
                QualificationGateRejection::MissingWorkload(
                    "shuffle_heavy_join missing in 8-worker run".to_string(),
                )
            })?;

        let join_speedup_8w =
            join_8w.throughput_rows_per_sec / join_1w.throughput_rows_per_sec.max(1.0);
        if join_speedup_8w < 4.00 {
            return Err(QualificationGateRejection::ScalingFloorNotMet {
                worker_count: 8,
                actual: join_speedup_8w,
                required: 4.00,
            });
        }
        checked_rules.push(format!(
            "8-worker shuffle join speedup: {join_speedup_8w:.2}x >= 4.00x floor"
        ));

        // 7. Check baseline regression if provided (<= 10%)
        if let Some(base_manifest) = baseline {
            if let Some(base_8w) = base_manifest.runs.iter().find(|r| r.worker_count == 8) {
                let base_tps = base_8w.aggregate_metrics.throughput_rows_per_sec;
                let cur_tps = run_8w.aggregate_metrics.throughput_rows_per_sec;
                if cur_tps < base_tps * 0.90 {
                    let reg_pct = (1.0 - (cur_tps / base_tps)) * 100.0;
                    return Err(QualificationGateRejection::PerformanceRegressionExceeded {
                        metric: "8-worker throughput".to_string(),
                        regression_pct: reg_pct,
                        allowed_pct: 10.0,
                    });
                }
                checked_rules.push(format!(
                    "Baseline regression check: throughput within 10% tolerance ({cur_tps:.0} vs {base_tps:.0})"
                ));
            }
        }

        Ok(QualificationGateDecision {
            candidate_version: manifest.candidate.semantic_version.clone(),
            passed: true,
            summary: format!(
                "PASSED: RC1 qualification verified across 1, 2, 4, 8 workers with exact floors (2w:{speedup_2w:.2}x, 4w:{speedup_4w:.2}x, 8w:{speedup_8w:.2}x, join-8w:{join_speedup_8w:.2}x)"
            ),
            checked_rules,
            evaluated_at_rfc3339: "2026-09-02T12:00:00Z".to_string(),
        })
    }

    /// Evaluate an individual qualification run.
    pub fn evaluate_run(
        &self,
        run: &QualificationRun,
        _baseline_1w: Option<&QualificationRun>,
    ) -> Result<QualificationGateDecision, QualificationGateRejection> {
        let mut checked_rules = Vec::new();

        // 1. Data integrity invariants (zero tolerance)
        if run.aggregate_metrics.data_loss_rows > 0 {
            return Err(QualificationGateRejection::IntegrityViolation(format!(
                "Observed {} lost rows in {}w run",
                run.aggregate_metrics.data_loss_rows, run.worker_count
            )));
        }
        if run.aggregate_metrics.duplicate_deliveries > 0 {
            return Err(QualificationGateRejection::IntegrityViolation(format!(
                "Observed {} duplicate deliveries in {}w run",
                run.aggregate_metrics.duplicate_deliveries, run.worker_count
            )));
        }
        if run.aggregate_metrics.wrong_results > 0 {
            return Err(QualificationGateRejection::IntegrityViolation(format!(
                "Observed {} wrong results in {}w run",
                run.aggregate_metrics.wrong_results, run.worker_count
            )));
        }
        if run.aggregate_metrics.rejected_writes > 0 {
            return Err(QualificationGateRejection::IntegrityViolation(format!(
                "Observed {} rejected committed writes in {}w run",
                run.aggregate_metrics.rejected_writes, run.worker_count
            )));
        }
        if run.aggregate_metrics.oom_count > 0 {
            return Err(QualificationGateRejection::IntegrityViolation(format!(
                "Observed {} OOM events in {}w run",
                run.aggregate_metrics.oom_count, run.worker_count
            )));
        }
        checked_rules.push(
            "Zero data loss, duplicate delivery, wrong results, rejected writes, or OOMs"
                .to_string(),
        );

        // 2. Freshness SLO invariant (p99 <= 1000.0 ms)
        if run.aggregate_metrics.p99_freshness_ms > 1000.0 {
            return Err(QualificationGateRejection::FreshnessSloExceeded {
                actual_ms: run.aggregate_metrics.p99_freshness_ms,
                ceiling_ms: 1000.0,
            });
        }
        checked_rules.push(format!(
            "p99 Freshness: {:.1}ms <= 1000.0ms ceiling",
            run.aggregate_metrics.p99_freshness_ms
        ));

        // 3. Worker CPU balance (max / median <= 1.50 under uniform load)
        if run.aggregate_metrics.max_worker_cpu_ratio > 1.50 {
            return Err(QualificationGateRejection::WorkerCpuImbalance {
                max_ratio: run.aggregate_metrics.max_worker_cpu_ratio,
                ceiling: 1.50,
            });
        }
        checked_rules.push(format!(
            "Worker CPU balance: {:.2} <= 1.50 ceiling",
            run.aggregate_metrics.max_worker_cpu_ratio
        ));

        // 4. Check all required workloads present
        let mut present_workloads = std::collections::BTreeSet::new();
        for wr in &run.workload_results {
            present_workloads.insert(wr.workload);

            // Per-workload specific checks:
            if wr.workload == QualificationWorkload::ZipfSkew {
                let rec = wr.hot_key_recovery_ratio.unwrap_or(0.0);
                if rec < 0.80 {
                    return Err(QualificationGateRejection::HotKeyRecoveryFailed {
                        actual: rec,
                        required: 0.80,
                    });
                }
            }
            if wr.workload == QualificationWorkload::StateOverRam && wr.p99_freshness_ms > 1000.0 {
                return Err(QualificationGateRejection::FreshnessSloExceeded {
                    actual_ms: wr.p99_freshness_ms,
                    ceiling_ms: 1000.0,
                });
            }
            if wr.workload == QualificationWorkload::OfferedLoadOverloadRecovery {
                let dur = wr.recovery_duration_sec.unwrap_or(0.0);
                if dur > 300.0 {
                    return Err(QualificationGateRejection::OverloadRecoveryFailed {
                        duration_sec: dur,
                        ceiling_sec: 300.0,
                    });
                }
            }
            if wr.workload == QualificationWorkload::OnlineSplitMicroMigration {
                let loss = wr.migration_throughput_loss_ratio.unwrap_or(0.0);
                if loss > 0.20 {
                    return Err(QualificationGateRejection::MigrationLossExceeded {
                        actual_loss_pct: loss * 100.0,
                        ceiling_pct: 20.0,
                    });
                }
            }
        }

        for &req_wl in QualificationWorkload::all() {
            if !present_workloads.contains(&req_wl) {
                return Err(QualificationGateRejection::MissingWorkload(format!(
                    "Workload '{}' missing from {}w run",
                    req_wl.as_str(),
                    run.worker_count
                )));
            }
        }
        checked_rules
            .push("All 11 required qualification workloads present and satisfied".to_string());

        Ok(QualificationGateDecision {
            candidate_version: run.candidate.semantic_version.clone(),
            passed: true,
            summary: format!(
                "PASSED: {}w qualification run satisfied all bounds and invariants",
                run.worker_count
            ),
            checked_rules,
            evaluated_at_rfc3339: run.timestamp_rfc3339.clone(),
        })
    }
}

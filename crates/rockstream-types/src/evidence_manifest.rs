//! Machine-readable evidence manifest for RockStream release candidates.
//!
//! Replaces checked-in self-attestation with an immutable, machine-verifiable
//! manifest (`evidence-manifest.json`). Binds candidate SHA, artifact digests,
//! workflow run ID, environment and workload digests, test pass/fail/skip counts,
//! raw metrics, and regenerated summaries.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::candidate_identity::CandidateIdentity;

/// Runner environment metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunnerEnvironment {
    pub os: String,
    pub arch: String,
    pub cpu_cores: usize,
    pub memory_gb: f64,
}

/// Workflow run execution context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRunInfo {
    pub id: String,
    pub run_url: String,
    pub trigger_event: String,
    pub runner_environment: RunnerEnvironment,
}

/// Test execution results for a test suite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TestSuiteResult {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub mandatory_skipped: usize,
}

/// Aggregated summary metrics with statistical percentiles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SummaryMetric {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub mean: f64,
    pub min: f64,
    pub max: f64,
    pub sample_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_per_sec: Option<f64>,
}

impl SummaryMetric {
    /// Calculate summary metrics from raw observation samples.
    pub fn calculate_from_raw(samples: &[f64], throughput_per_sec: Option<f64>) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }

        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let sample_count = sorted.len();
        let min = sorted[0];
        let max = sorted[sample_count - 1];
        let sum: f64 = sorted.iter().sum();
        let mean = sum / sample_count as f64;

        let p50 = Self::percentile(&sorted, 0.50);
        let p95 = Self::percentile(&sorted, 0.95);
        let p99 = Self::percentile(&sorted, 0.99);

        Some(Self {
            p50,
            p95,
            p99,
            mean,
            min,
            max,
            sample_count,
            throughput_per_sec,
        })
    }

    /// Calculate percentile via linear interpolation (R-7 / numpy default).
    pub fn percentile(sorted: &[f64], p: f64) -> f64 {
        if sorted.is_empty() {
            return 0.0;
        }
        if sorted.len() == 1 {
            return sorted[0];
        }
        let rank = p * (sorted.len() - 1) as f64;
        let lower = rank.floor() as usize;
        let upper = rank.ceil() as usize;
        let weight = rank - lower as f64;
        if lower == upper || upper >= sorted.len() {
            sorted[lower]
        } else {
            sorted[lower] * (1.0 - weight) + sorted[upper] * weight
        }
    }
}

/// The machine-readable evidence manifest for a release candidate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceManifest {
    pub candidate: CandidateIdentity,
    pub workflow_run: WorkflowRunInfo,
    pub artifacts: BTreeMap<String, String>,
    pub workloads: BTreeMap<String, String>,
    pub test_results: BTreeMap<String, TestSuiteResult>,
    pub raw_metrics: BTreeMap<String, Vec<f64>>,
    pub summary_metrics: BTreeMap<String, SummaryMetric>,
    pub targets: BTreeMap<String, f64>,
}

/// Error type for evidence manifest validation failures.
#[derive(Debug, Clone, PartialEq)]
pub enum EvidenceIntegrityError {
    InvalidCandidateIdentity(String),
    MandatoryTestsSkipped {
        suite: String,
        count: usize,
    },
    TestFailed {
        suite: String,
        count: usize,
    },
    InvalidArtifactDigest {
        path: String,
        digest: String,
    },
    ArtifactDigestMismatch {
        path: String,
        expected: String,
        actual: String,
    },
    ArtifactFileNotFound(String),
    MissingRawData {
        metric: String,
    },
    SummaryRegenerationMismatch {
        metric: String,
        field: String,
        expected: f64,
        actual: f64,
    },
    TargetCannotSatisfyMeasured {
        metric: String,
        reason: String,
    },
    IoError(String),
    SerializationError(String),
}

impl fmt::Display for EvidenceIntegrityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCandidateIdentity(msg) => {
                write!(f, "RS-0001: Invalid candidate identity: {msg}")
            }
            Self::MandatoryTestsSkipped { suite, count } => {
                write!(
                    f,
                    "RS-0001: Mandatory test suite '{suite}' has {count} skipped tests (must be 0)"
                )
            }
            Self::TestFailed { suite, count } => {
                write!(f, "RS-0001: Test suite '{suite}' has {count} failed tests")
            }
            Self::InvalidArtifactDigest { path, digest } => {
                write!(
                    f,
                    "RS-0001: Invalid SHA-256 digest '{digest}' for artifact '{path}'"
                )
            }
            Self::ArtifactDigestMismatch {
                path,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "RS-0001: SHA-256 digest mismatch for artifact '{path}': expected {expected}, actual {actual}"
                )
            }
            Self::ArtifactFileNotFound(path) => {
                write!(f, "RS-0001: Artifact file not found: '{path}'")
            }
            Self::MissingRawData { metric } => {
                write!(
                    f,
                    "RS-0001: Missing raw observation data for summary metric '{metric}'"
                )
            }
            Self::SummaryRegenerationMismatch {
                metric,
                field,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "RS-0001: Summary metric '{metric}.{field}' regeneration mismatch: mathematically expected {expected:.4}, manifest recorded {actual:.4}"
                )
            }
            Self::TargetCannotSatisfyMeasured { metric, reason } => {
                write!(
                    f,
                    "RS-0001: Checked-in baseline target cannot satisfy measured result for '{metric}': {reason}"
                )
            }
            Self::IoError(msg) => write!(f, "RS-0003: IO error: {msg}"),
            Self::SerializationError(msg) => write!(f, "RS-0001: Serialization error: {msg}"),
        }
    }
}

impl std::error::Error for EvidenceIntegrityError {}

impl EvidenceManifest {
    /// Validate manifest integrity.
    pub fn validate(&self) -> Result<(), EvidenceIntegrityError> {
        if self.candidate.semantic_version.is_empty() {
            return Err(EvidenceIntegrityError::InvalidCandidateIdentity(
                "semantic_version is empty".to_string(),
            ));
        }
        if self.candidate.commit_sha.is_empty() {
            return Err(EvidenceIntegrityError::InvalidCandidateIdentity(
                "commit_sha is empty".to_string(),
            ));
        }
        if self.candidate.lockfile_digest.len() != 64 {
            return Err(EvidenceIntegrityError::InvalidCandidateIdentity(
                "lockfile_digest is not a valid 64-char SHA-256 hex digest".to_string(),
            ));
        }

        // Check artifacts digests
        for (path, digest) in &self.artifacts {
            if digest.len() != 64 || !digest.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(EvidenceIntegrityError::InvalidArtifactDigest {
                    path: path.clone(),
                    digest: digest.clone(),
                });
            }
        }

        // Check test results: zero mandatory skipped, zero failed
        for (suite, res) in &self.test_results {
            if res.mandatory_skipped > 0 {
                return Err(EvidenceIntegrityError::MandatoryTestsSkipped {
                    suite: suite.clone(),
                    count: res.mandatory_skipped,
                });
            }
            if res.failed > 0 {
                return Err(EvidenceIntegrityError::TestFailed {
                    suite: suite.clone(),
                    count: res.failed,
                });
            }
        }

        // Check raw metrics and summary metrics mathematical regeneration
        const EPSILON: f64 = 1e-3;
        for (name, summary) in &self.summary_metrics {
            let raw = self.raw_metrics.get(name).ok_or_else(|| {
                EvidenceIntegrityError::MissingRawData {
                    metric: name.clone(),
                }
            })?;

            if raw.is_empty() {
                return Err(EvidenceIntegrityError::MissingRawData {
                    metric: name.clone(),
                });
            }

            let expected_summary =
                SummaryMetric::calculate_from_raw(raw, summary.throughput_per_sec).ok_or_else(
                    || EvidenceIntegrityError::MissingRawData {
                        metric: name.clone(),
                    },
                )?;

            if (summary.p50 - expected_summary.p50).abs() > EPSILON {
                return Err(EvidenceIntegrityError::SummaryRegenerationMismatch {
                    metric: name.clone(),
                    field: "p50".to_string(),
                    expected: expected_summary.p50,
                    actual: summary.p50,
                });
            }
            if (summary.p95 - expected_summary.p95).abs() > EPSILON {
                return Err(EvidenceIntegrityError::SummaryRegenerationMismatch {
                    metric: name.clone(),
                    field: "p95".to_string(),
                    expected: expected_summary.p95,
                    actual: summary.p95,
                });
            }
            if (summary.p99 - expected_summary.p99).abs() > EPSILON {
                return Err(EvidenceIntegrityError::SummaryRegenerationMismatch {
                    metric: name.clone(),
                    field: "p99".to_string(),
                    expected: expected_summary.p99,
                    actual: summary.p99,
                });
            }
            if (summary.mean - expected_summary.mean).abs() > EPSILON {
                return Err(EvidenceIntegrityError::SummaryRegenerationMismatch {
                    metric: name.clone(),
                    field: "mean".to_string(),
                    expected: expected_summary.mean,
                    actual: summary.mean,
                });
            }
            if (summary.min - expected_summary.min).abs() > EPSILON {
                return Err(EvidenceIntegrityError::SummaryRegenerationMismatch {
                    metric: name.clone(),
                    field: "min".to_string(),
                    expected: expected_summary.min,
                    actual: summary.min,
                });
            }
            if (summary.max - expected_summary.max).abs() > EPSILON {
                return Err(EvidenceIntegrityError::SummaryRegenerationMismatch {
                    metric: name.clone(),
                    field: "max".to_string(),
                    expected: expected_summary.max,
                    actual: summary.max,
                });
            }
            if summary.sample_count != raw.len() {
                return Err(EvidenceIntegrityError::SummaryRegenerationMismatch {
                    metric: name.clone(),
                    field: "sample_count".to_string(),
                    expected: raw.len() as f64,
                    actual: summary.sample_count as f64,
                });
            }

            // Target separation check:
            // If targets contain this metric, check if the measured summary was purely copied from target
            if let Some(&target_val) = self.targets.get(name) {
                if raw.len() == 1 && (raw[0] - target_val).abs() < f64::EPSILON && target_val > 0.0
                {
                    return Err(EvidenceIntegrityError::TargetCannotSatisfyMeasured {
                        metric: name.clone(),
                        reason:
                            "raw samples contain only a single static value identical to target threshold"
                                .to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Verify artifact digests against files on disk.
    pub fn verify_files_on_disk(&self, base_dir: &Path) -> Result<(), EvidenceIntegrityError> {
        for (rel_path, expected_digest) in &self.artifacts {
            let full_path = base_dir.join(rel_path);
            if !full_path.is_file() {
                return Err(EvidenceIntegrityError::ArtifactFileNotFound(
                    full_path.display().to_string(),
                ));
            }
            let bytes = std::fs::read(&full_path).map_err(|e| {
                EvidenceIntegrityError::IoError(format!("{}: {e}", full_path.display()))
            })?;
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            let actual_digest = hex::encode(hasher.finalize());
            if !actual_digest.eq_ignore_ascii_case(expected_digest) {
                return Err(EvidenceIntegrityError::ArtifactDigestMismatch {
                    path: rel_path.clone(),
                    expected: expected_digest.clone(),
                    actual: actual_digest,
                });
            }
        }
        Ok(())
    }

    /// Serialize to JSON.
    pub fn to_json(&self) -> Result<String, EvidenceIntegrityError> {
        serde_json::to_string_pretty(self)
            .map_err(|e| EvidenceIntegrityError::SerializationError(e.to_string()))
    }

    /// Deserialize from JSON string.
    pub fn from_json(json_str: &str) -> Result<Self, EvidenceIntegrityError> {
        serde_json::from_str(json_str)
            .map_err(|e| EvidenceIntegrityError::SerializationError(e.to_string()))
    }
}

//! Independent IVM Scale Qualification Program (Issue #405: R1, R2, R3, R4).
//!
//! Provides:
//! - Strict separation of synthetic fixtures vs. measured runs (R1)
//! - Expanding qualification ladder for qualified throughput `Q_n` and
//!   `Scaling efficiency(n) = Q_n / (n * Q_1)` (R3)
//! - Complete workload cost accounting ("cost per million correct durable
//!   changes at freshness F") (R4)
//! - Negative-controlled independent oracle/checker proving intended faults
//!   actually fired and rejecting 1 dropped row, 1 duplicate row, stale-owner
//!   writes, torn visibility, fault-never-triggered teardowns, and synthetic
//!   `sample_reference_run` fixtures (R4 + Verification #1 & #2).

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Strict provenance tag separating unit-test fixtures from measured runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceProvenance {
    SyntheticFixture,
    MeasuredExecution,
}

/// Scaling mode in the R3 qualification ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalingMode {
    FixedSizeSpeedup,
    ProportionalScaleOut,
}

/// Key distribution profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyDistribution {
    Uniform,
    Skewed50xHotKey,
}

/// Workload shape in the R3 qualification ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadShape {
    Aggregation,
    CoPartitionedJoin,
    RepartitionedJoin,
    DependentViewsSingle,
    DependentViews20SharedArrangements,
    BoundedWindow,
    ConcurrentHighQpsReadsAndSubscribe,
}

/// Duration tier for qualification runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurationProfile {
    ShortDiagnostic,
    Soak72HourMixedLoad,
}

/// Retained state size relative to configured worker memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetainedStateMultiplier {
    Ram1x,
    Ram4x,
    Ram10x,
}

/// Status of a cell in the expanding qualification ladder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LadderCellStatus {
    Measured,
    Blocked {
        owning_issue: String,
        blocker: String,
    },
}

/// A single cell in the R3 expanding qualification ladder.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationLadderCell {
    pub cell_id: String,
    pub worker_count: usize,
    pub scaling_mode: ScalingMode,
    pub retained_state: RetainedStateMultiplier,
    pub distribution: KeyDistribution,
    pub workload_shape: WorkloadShape,
    pub duration_profile: DurationProfile,
    pub status: LadderCellStatus,
    pub qualified_throughput_q_n: Option<f64>,
    pub freshness_slo_ms: f64,
}

/// Canonical R3 expanding qualification ladder covering 1..64 workers,
/// 1x/4x/10x RAM, uniform vs 50x skew, all workload shapes, and diagnostic + 72h soak.
pub fn canonical_r3_ladder() -> Vec<QualificationLadderCell> {
    let mut cells = Vec::new();

    // 1. Maintenance worker ladder: 1, 2, 4, 8, 16, 32, 64
    for &workers in &[1usize, 2, 4, 8, 16, 32, 64] {
        for mode in [
            ScalingMode::FixedSizeSpeedup,
            ScalingMode::ProportionalScaleOut,
        ] {
            let status = if workers <= 4 {
                LadderCellStatus::Measured
            } else {
                LadderCellStatus::Blocked {
                    owning_issue: "#400/#402/#407".to_string(),
                    blocker: format!(
                        "{workers}-worker fleet cell requires direct distributed data plane (#400), O(affected) worker efficiency (#402), and regional cell (#407)"
                    ),
                }
            };
            cells.push(QualificationLadderCell {
                cell_id: format!(
                    "workers-{workers}-{}",
                    match mode {
                        ScalingMode::FixedSizeSpeedup => "fixed-speedup",
                        ScalingMode::ProportionalScaleOut => "proportional-scaleout",
                    }
                ),
                worker_count: workers,
                scaling_mode: mode,
                retained_state: RetainedStateMultiplier::Ram1x,
                distribution: KeyDistribution::Uniform,
                workload_shape: WorkloadShape::Aggregation,
                duration_profile: DurationProfile::ShortDiagnostic,
                status,
                qualified_throughput_q_n: None,
                freshness_slo_ms: 1000.0,
            });
        }
    }

    // 2. Retained state beyond RAM: 1x, 4x, 10x through compaction and failover
    for (mult, id, blocked) in [
        (RetainedStateMultiplier::Ram1x, "state-1x-ram", false),
        (RetainedStateMultiplier::Ram4x, "state-4x-ram", true),
        (RetainedStateMultiplier::Ram10x, "state-10x-ram", true),
    ] {
        cells.push(QualificationLadderCell {
            cell_id: id.to_string(),
            worker_count: 4,
            scaling_mode: ScalingMode::FixedSizeSpeedup,
            retained_state: mult,
            distribution: KeyDistribution::Uniform,
            workload_shape: WorkloadShape::Aggregation,
            duration_profile: DurationProfile::ShortDiagnostic,
            status: if blocked {
                LadderCellStatus::Blocked {
                    owning_issue: "#402/#406".to_string(),
                    blocker: "Beyond-RAM spillable arrangements and online failover/compaction qualification".to_string(),
                }
            } else {
                LadderCellStatus::Measured
            },
            qualified_throughput_q_n: None,
            freshness_slo_ms: 1000.0,
        });
    }

    // 3. Distributions & shapes
    for (id, dist, shape, blocked, issue) in [
        (
            "skew-50x-hot-key",
            KeyDistribution::Skewed50xHotKey,
            WorkloadShape::Aggregation,
            true,
            "#403",
        ),
        (
            "shape-co-partitioned-join",
            KeyDistribution::Uniform,
            WorkloadShape::CoPartitionedJoin,
            false,
            "",
        ),
        (
            "shape-repartitioned-join",
            KeyDistribution::Uniform,
            WorkloadShape::RepartitionedJoin,
            true,
            "#400",
        ),
        (
            "shape-dependent-views-1",
            KeyDistribution::Uniform,
            WorkloadShape::DependentViewsSingle,
            false,
            "",
        ),
        (
            "shape-dependent-views-20-shared",
            KeyDistribution::Uniform,
            WorkloadShape::DependentViews20SharedArrangements,
            true,
            "#400/#402",
        ),
        (
            "shape-bounded-window",
            KeyDistribution::Uniform,
            WorkloadShape::BoundedWindow,
            false,
            "",
        ),
        (
            "shape-high-qps-reads-subscribe",
            KeyDistribution::Uniform,
            WorkloadShape::ConcurrentHighQpsReadsAndSubscribe,
            true,
            "#401",
        ),
    ] {
        cells.push(QualificationLadderCell {
            cell_id: id.to_string(),
            worker_count: 4,
            scaling_mode: ScalingMode::FixedSizeSpeedup,
            retained_state: RetainedStateMultiplier::Ram1x,
            distribution: dist,
            workload_shape: shape,
            duration_profile: DurationProfile::ShortDiagnostic,
            status: if blocked {
                LadderCellStatus::Blocked {
                    owning_issue: issue.to_string(),
                    blocker: format!("Cell '{id}' gated on {issue}"),
                }
            } else {
                LadderCellStatus::Measured
            },
            qualified_throughput_q_n: None,
            freshness_slo_ms: 1000.0,
        });
    }

    // 4. Long-duration 72-hour mixed-load soak
    cells.push(QualificationLadderCell {
        cell_id: "soak-72h-mixed-load".to_string(),
        worker_count: 4,
        scaling_mode: ScalingMode::ProportionalScaleOut,
        retained_state: RetainedStateMultiplier::Ram4x,
        distribution: KeyDistribution::Skewed50xHotKey,
        workload_shape: WorkloadShape::ConcurrentHighQpsReadsAndSubscribe,
        duration_profile: DurationProfile::Soak72HourMixedLoad,
        status: LadderCellStatus::Blocked {
            owning_issue: "#400/#401/#403/#406".to_string(),
            blocker: "72-hour mixed-load soak requires full cloud-scale infrastructure".to_string(),
        },
        qualified_throughput_q_n: None,
        freshness_slo_ms: 1000.0,
    });

    cells
}

/// Compute `Scaling efficiency(n) = Q_n / (n * Q_1)`.
pub fn scaling_efficiency(q_n: f64, q_1: f64, worker_count: usize) -> Result<f64> {
    if worker_count == 0 {
        bail!("worker_count must be >= 1");
    }
    if !q_n.is_finite() || q_n <= 0.0 {
        bail!("qualified throughput Q_n must be finite and positive, got {q_n}");
    }
    if !q_1.is_finite() || q_1 <= 0.0 {
        bail!("single-worker qualified throughput Q_1 must be finite and positive, got {q_1}");
    }
    let eff = q_n / (worker_count as f64 * q_1);
    if !eff.is_finite() {
        bail!("scaling efficiency computation overflowed");
    }
    Ok(eff)
}

/// Complete R4 workload cost accounting ("cost per million correct durable changes at freshness F").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteCostReport {
    pub serving_gateways_hourly_usd: f64,
    pub control_plane_hourly_usd: f64,
    pub maintenance_workers_hourly_usd: f64,
    pub compaction_hourly_usd: f64,
    pub retained_history_storage_hourly_usd: f64,
    pub object_store_requests_hourly_usd: f64,
    pub backups_hourly_usd: f64,
    pub failover_headroom_hourly_usd: f64,
    pub cross_az_traffic_hourly_usd: f64,
    pub total_hourly_usd: f64,
    pub freshness_slo_ms: f64,
    pub qualified_changes_per_sec: f64,
    pub cost_per_million_correct_durable_changes_usd: f64,
    pub cloud_instance_class: String,
    pub region: String,
    pub pricing_date_iso: String,
    pub pricing_scope: String,
}

impl CompleteCostReport {
    pub fn itemized_total_hourly_usd(&self) -> f64 {
        self.serving_gateways_hourly_usd
            + self.control_plane_hourly_usd
            + self.maintenance_workers_hourly_usd
            + self.compaction_hourly_usd
            + self.retained_history_storage_hourly_usd
            + self.object_store_requests_hourly_usd
            + self.backups_hourly_usd
            + self.failover_headroom_hourly_usd
            + self.cross_az_traffic_hourly_usd
    }

    pub fn validate(&self) -> Result<()> {
        let components = [
            (
                "serving_gateways_hourly_usd",
                self.serving_gateways_hourly_usd,
            ),
            ("control_plane_hourly_usd", self.control_plane_hourly_usd),
            (
                "maintenance_workers_hourly_usd",
                self.maintenance_workers_hourly_usd,
            ),
            ("compaction_hourly_usd", self.compaction_hourly_usd),
            (
                "retained_history_storage_hourly_usd",
                self.retained_history_storage_hourly_usd,
            ),
            (
                "object_store_requests_hourly_usd",
                self.object_store_requests_hourly_usd,
            ),
            ("backups_hourly_usd", self.backups_hourly_usd),
            (
                "failover_headroom_hourly_usd",
                self.failover_headroom_hourly_usd,
            ),
            (
                "cross_az_traffic_hourly_usd",
                self.cross_az_traffic_hourly_usd,
            ),
        ];
        for (name, val) in components {
            if !val.is_finite() || val < 0.0 {
                bail!("cost component {name} must be finite and non-negative, got {val}");
            }
        }
        let sum = self.itemized_total_hourly_usd();
        let tol = 1e-6_f64.max(sum.abs() * 1e-9);
        if !self.total_hourly_usd.is_finite() || (sum - self.total_hourly_usd).abs() > tol {
            bail!(
                "total_hourly_usd ({:.6}) does not match sum of itemized components ({:.6})",
                self.total_hourly_usd,
                sum
            );
        }
        if !self.freshness_slo_ms.is_finite() || self.freshness_slo_ms <= 0.0 {
            bail!("freshness_slo_ms must be finite and positive");
        }
        if !self.qualified_changes_per_sec.is_finite() || self.qualified_changes_per_sec <= 0.0 {
            bail!("qualified_changes_per_sec must be finite and positive");
        }
        let expected_cpm =
            (self.total_hourly_usd * 1_000_000.0) / (self.qualified_changes_per_sec * 3_600.0);
        let cpm_tol = 1e-6_f64.max(expected_cpm.abs() * 1e-9);
        if !self
            .cost_per_million_correct_durable_changes_usd
            .is_finite()
            || (expected_cpm - self.cost_per_million_correct_durable_changes_usd).abs() > cpm_tol
        {
            bail!(
                "cost_per_million_correct_durable_changes_usd ({:.6}) != expected ({:.6})",
                self.cost_per_million_correct_durable_changes_usd,
                expected_cpm
            );
        }
        if self.cloud_instance_class.trim().is_empty()
            || self.region.trim().is_empty()
            || self.pricing_date_iso.len() != 10
        {
            bail!("cloud_instance_class, region, and YYYY-MM-DD pricing_date_iso are required");
        }
        match self.pricing_scope.as_str() {
            "local-observed" | "cloud-measured-dated" => Ok(()),
            other => bail!("unsupported pricing_scope '{other}'"),
        }
    }
}

/// Ownership write event recorded during a qualification run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipWriteRecord {
    pub shard_id: u32,
    pub worker_id: String,
    pub writer_lease_epoch: u64,
    pub authoritative_lease_epoch: u64,
    pub committed_epoch: u64,
}

/// Cross-shard / cross-view visibility observation recorded during reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VisibilitySnapshotRecord {
    pub read_id: u64,
    pub view_or_shard_epochs: BTreeMap<String, u64>,
}

/// Planned and observed fault event proving intended faults actually fired.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultInjectionEvent {
    pub fault_name: String,
    pub target_process_or_pod: String,
    pub fired_at_rfc3339: Option<String>,
    pub recovered_at_rfc3339: Option<String>,
}

/// Complete archived qualification run bundle checked by `IndependentQualificationChecker`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScaleQualificationBundle {
    pub run_id: String,
    pub provenance: EvidenceProvenance,
    pub commit_sha: String,
    pub binary_sha256: String,
    pub hardware_spec: String,
    pub worker_count: usize,
    pub worker_pids: Vec<u32>,
    pub offered_rate_rows_per_sec: f64,
    pub achieved_rate_rows_per_sec: f64,
    pub freshness_p50_ms: f64,
    pub freshness_p95_ms: f64,
    pub freshness_p99_ms: f64,
    pub read_p99_ms: f64,
    pub errors_and_timeouts: u64,
    pub max_rss_bytes: u64,
    pub network_bytes: u64,
    pub storage_requests: u64,
    pub compaction_debt_bytes: u64,
    pub expected_oracle_rows: Vec<Vec<String>>,
    pub observed_rockstream_rows: Vec<Vec<String>>,
    pub ownership_writes: Vec<OwnershipWriteRecord>,
    pub visibility_snapshots: Vec<VisibilitySnapshotRecord>,
    pub planned_faults: Vec<FaultInjectionEvent>,
    pub cost_report: CompleteCostReport,
}

/// Typed rejection reasons from the independent qualification checker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckerViolation {
    SyntheticFixtureDisallowed {
        reason: String,
    },
    InvalidProvenance {
        reason: String,
    },
    DroppedRowDetected {
        row: Vec<String>,
        expected_count: usize,
        observed_count: usize,
    },
    DuplicateRowDetected {
        row: Vec<String>,
        expected_count: usize,
        observed_count: usize,
    },
    StaleOwnerWriteDetected {
        shard_id: u32,
        worker_id: String,
        writer_lease_epoch: u64,
        authoritative_lease_epoch: u64,
    },
    TornVisibilityDetected {
        read_id: u64,
        observed_epochs: BTreeMap<String, u64>,
    },
    FaultNeverTriggered {
        fault_name: String,
    },
    SloOrIntegrityViolation {
        reason: String,
    },
    InvalidCostAccounting {
        reason: String,
    },
}

/// Summary of running the mandatory negative-control suite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NegativeControlReport {
    pub all_negative_controls_rejected: bool,
    pub verified_controls: Vec<String>,
}

/// Independent qualification checker enforcing R1 + R4 invariants.
pub struct IndependentQualificationChecker;

impl IndependentQualificationChecker {
    /// Verify a `ScaleQualificationBundle` and fail closed on any integrity,
    /// provenance, multiset, ownership, visibility, fault, or cost violation.
    pub fn verify(bundle: &ScaleQualificationBundle) -> std::result::Result<(), CheckerViolation> {
        // 1. Strict separation of synthetic fixtures vs. measured runs (R1)
        if bundle.provenance != EvidenceProvenance::MeasuredExecution
            || bundle.run_id.contains("sample_reference_run")
        {
            return Err(CheckerViolation::SyntheticFixtureDisallowed {
                reason: format!(
                    "run '{}' has provenance {:?}; synthetic fixtures cannot pass qualification",
                    bundle.run_id, bundle.provenance
                ),
            });
        }

        // 2. Provenance digests and distinct non-zero OS worker PIDs
        if !is_hex(&bundle.commit_sha, 40) {
            return Err(CheckerViolation::InvalidProvenance {
                reason: format!(
                    "commit_sha must be 40 hex chars, got '{}'",
                    bundle.commit_sha
                ),
            });
        }
        if !is_hex(&bundle.binary_sha256, 64) {
            return Err(CheckerViolation::InvalidProvenance {
                reason: format!(
                    "binary_sha256 must be 64 hex chars, got '{}'",
                    bundle.binary_sha256
                ),
            });
        }
        if bundle.hardware_spec.trim().is_empty() {
            return Err(CheckerViolation::InvalidProvenance {
                reason: "hardware_spec must be non-empty".to_string(),
            });
        }
        if bundle.worker_count == 0 || bundle.worker_pids.len() != bundle.worker_count {
            return Err(CheckerViolation::InvalidProvenance {
                reason: format!(
                    "expected {} distinct worker PIDs, got {}",
                    bundle.worker_count,
                    bundle.worker_pids.len()
                ),
            });
        }
        let unique_pids: BTreeSet<u32> = bundle.worker_pids.iter().copied().collect();
        if unique_pids.len() != bundle.worker_count || unique_pids.contains(&0) {
            return Err(CheckerViolation::InvalidProvenance {
                reason: "worker_pids must be non-zero and distinct".to_string(),
            });
        }

        // 3. Exact multiset comparison (catches 1 dropped row and 1 duplicate row)
        let mut expected_counts: BTreeMap<&Vec<String>, usize> = BTreeMap::new();
        for row in &bundle.expected_oracle_rows {
            *expected_counts.entry(row).or_insert(0) += 1;
        }
        let mut observed_counts: BTreeMap<&Vec<String>, usize> = BTreeMap::new();
        for row in &bundle.observed_rockstream_rows {
            *observed_counts.entry(row).or_insert(0) += 1;
        }

        for (row, &exp_cnt) in &expected_counts {
            let obs_cnt = observed_counts.get(row).copied().unwrap_or(0);
            if obs_cnt < exp_cnt {
                return Err(CheckerViolation::DroppedRowDetected {
                    row: (*row).clone(),
                    expected_count: exp_cnt,
                    observed_count: obs_cnt,
                });
            }
            if obs_cnt > exp_cnt {
                return Err(CheckerViolation::DuplicateRowDetected {
                    row: (*row).clone(),
                    expected_count: exp_cnt,
                    observed_count: obs_cnt,
                });
            }
        }
        for (row, &obs_cnt) in &observed_counts {
            let exp_cnt = expected_counts.get(row).copied().unwrap_or(0);
            if obs_cnt > exp_cnt {
                return Err(CheckerViolation::DuplicateRowDetected {
                    row: (*row).clone(),
                    expected_count: exp_cnt,
                    observed_count: obs_cnt,
                });
            }
        }

        // 4. Stale-owner write fencing check
        for w in &bundle.ownership_writes {
            if w.writer_lease_epoch < w.authoritative_lease_epoch {
                return Err(CheckerViolation::StaleOwnerWriteDetected {
                    shard_id: w.shard_id,
                    worker_id: w.worker_id.clone(),
                    writer_lease_epoch: w.writer_lease_epoch,
                    authoritative_lease_epoch: w.authoritative_lease_epoch,
                });
            }
        }

        // 5. Atomic visibility frontier check (no torn visibility across views/shards)
        for snap in &bundle.visibility_snapshots {
            let unique_epochs: BTreeSet<u64> =
                snap.view_or_shard_epochs.values().copied().collect();
            if unique_epochs.len() > 1 {
                return Err(CheckerViolation::TornVisibilityDetected {
                    read_id: snap.read_id,
                    observed_epochs: snap.view_or_shard_epochs.clone(),
                });
            }
        }

        // 6. Prove intended faults actually fired and recovered
        for fault in &bundle.planned_faults {
            let fired = fault
                .fired_at_rfc3339
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            let recovered = fault
                .recovered_at_rfc3339
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            if !fired || !recovered {
                return Err(CheckerViolation::FaultNeverTriggered {
                    fault_name: fault.fault_name.clone(),
                });
            }
        }

        // 7. Rate, freshness, error, and resource accounting
        if bundle.errors_and_timeouts > 0 {
            return Err(CheckerViolation::SloOrIntegrityViolation {
                reason: format!("recorded {} errors/timeouts", bundle.errors_and_timeouts),
            });
        }
        if bundle.freshness_p99_ms > bundle.cost_report.freshness_slo_ms {
            return Err(CheckerViolation::SloOrIntegrityViolation {
                reason: format!(
                    "freshness_p99_ms ({:.2}) exceeded SLO ({:.2})",
                    bundle.freshness_p99_ms, bundle.cost_report.freshness_slo_ms
                ),
            });
        }
        if bundle.max_rss_bytes == 0 {
            return Err(CheckerViolation::SloOrIntegrityViolation {
                reason: "max_rss_bytes must be measured (> 0)".to_string(),
            });
        }

        // 8. Complete cost accounting validation
        bundle
            .cost_report
            .validate()
            .map_err(|e| CheckerViolation::InvalidCostAccounting {
                reason: e.to_string(),
            })?;

        Ok(())
    }

    /// Execute the full Verification #1 & #2 negative-control suite against a
    /// valid baseline bundle, proving the checker rejects:
    /// 1. 1 dropped row
    /// 2. 1 duplicate row
    /// 3. Stale-owner write
    /// 4. Torn visibility
    /// 5. Fault-never-triggered teardown
    /// 6. Synthetic `sample_reference_run` fixture
    pub fn run_negative_control_suite(
        valid_bundle: &ScaleQualificationBundle,
    ) -> Result<NegativeControlReport> {
        // Baseline must pass first so negative controls are non-vacuous.
        if let Err(err) = Self::verify(valid_bundle) {
            bail!("baseline bundle must pass before running negative controls: {err:?}");
        }
        if valid_bundle.expected_oracle_rows.is_empty() {
            bail!("baseline bundle must contain at least 1 oracle row for negative controls");
        }

        let mut verified = Vec::new();

        // Control 1: 1 dropped row
        let mut dropped = valid_bundle.clone();
        dropped.observed_rockstream_rows.pop();
        match Self::verify(&dropped) {
            Err(CheckerViolation::DroppedRowDetected { .. }) => {
                verified.push("dropped_row_rejected".to_string());
            }
            other => bail!("negative control 'dropped_row' failed: got {other:?}"),
        }

        // Control 2: 1 duplicate row
        let mut duplicate = valid_bundle.clone();
        let dup_row = duplicate.observed_rockstream_rows[0].clone();
        duplicate.observed_rockstream_rows.push(dup_row);
        match Self::verify(&duplicate) {
            Err(CheckerViolation::DuplicateRowDetected { .. }) => {
                verified.push("duplicate_row_rejected".to_string());
            }
            other => bail!("negative control 'duplicate_row' failed: got {other:?}"),
        }

        // Control 3: Stale-owner write
        let mut stale_owner = valid_bundle.clone();
        stale_owner.ownership_writes.push(OwnershipWriteRecord {
            shard_id: 0,
            worker_id: "worker-stale".to_string(),
            writer_lease_epoch: 3,
            authoritative_lease_epoch: 4,
            committed_epoch: 100,
        });
        match Self::verify(&stale_owner) {
            Err(CheckerViolation::StaleOwnerWriteDetected { .. }) => {
                verified.push("stale_owner_write_rejected".to_string());
            }
            other => bail!("negative control 'stale_owner_write' failed: got {other:?}"),
        }

        // Control 4: Torn visibility
        let mut torn = valid_bundle.clone();
        let mut torn_epochs = BTreeMap::new();
        torn_epochs.insert("sales_by_region".to_string(), 42);
        torn_epochs.insert("top_customers".to_string(), 41);
        torn.visibility_snapshots.push(VisibilitySnapshotRecord {
            read_id: 9999,
            view_or_shard_epochs: torn_epochs,
        });
        match Self::verify(&torn) {
            Err(CheckerViolation::TornVisibilityDetected { .. }) => {
                verified.push("torn_visibility_rejected".to_string());
            }
            other => bail!("negative control 'torn_visibility' failed: got {other:?}"),
        }

        // Control 5: Fault-never-triggered teardown
        let mut unfired = valid_bundle.clone();
        unfired.planned_faults.push(FaultInjectionEvent {
            fault_name: "worker_kill_pod_1".to_string(),
            target_process_or_pod: "rockstream-worker-1".to_string(),
            fired_at_rfc3339: None,
            recovered_at_rfc3339: None,
        });
        match Self::verify(&unfired) {
            Err(CheckerViolation::FaultNeverTriggered { .. }) => {
                verified.push("fault_never_triggered_rejected".to_string());
            }
            other => bail!("negative control 'fault_never_triggered' failed: got {other:?}"),
        }

        // Control 6: Synthetic sample_reference_run fixture
        let mut synthetic = valid_bundle.clone();
        synthetic.provenance = EvidenceProvenance::SyntheticFixture;
        synthetic.run_id = "sample_reference_run_4w".to_string();
        match Self::verify(&synthetic) {
            Err(CheckerViolation::SyntheticFixtureDisallowed { .. }) => {
                verified.push("synthetic_fixture_rejected".to_string());
            }
            other => bail!("negative control 'synthetic_fixture' failed: got {other:?}"),
        }

        Ok(NegativeControlReport {
            all_negative_controls_rejected: verified.len() == 6,
            verified_controls: verified,
        })
    }
}

fn is_hex(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| b.is_ascii_hexdigit())
}

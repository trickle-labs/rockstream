//! Production failure matrix registry and programmatic definitions (v0.58).
//!
//! Provides structured programmatic access to all 11 production failure modes
//! specified in `ROCKSTREAM_PROJECT_FOCUS.md` §5.5, `NEW_ROADMAP.md` v0.58,
//! and `docs/failure-matrix.md`.

use std::fmt;

/// Production failure mode identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FailureModeId {
    /// FM-001: Worker loss during active epoch / shard processing.
    Fm001,
    /// FM-002: Control-node loss during active epoch coordination.
    Fm002,
    /// FM-003: Exchange interruption & retry-budget exhaustion.
    Fm003,
    /// FM-004: Source disconnect with offset/LSN recovery.
    Fm004,
    /// FM-005: Object-store brownout and throttling (HTTP 429 / latency spike).
    Fm005,
    /// FM-006: Spill and compaction pressure.
    Fm006,
    /// FM-007: Checkpoint interruption during manifest write / 2PC.
    Fm007,
    /// FM-008: Sink failure during 2PC commit and recovery.
    Fm008,
    /// FM-009: Shard-migration interruption mid-copy / handoff.
    Fm009,
    /// FM-010: Rolling upgrade with mixed versions (N and N+1).
    Fm010,
    /// FM-011: Resource exhaustion with recovery (memory quota / queue saturation).
    Fm011,
}

impl FailureModeId {
    /// Return the canonical string identifier (e.g. `"FM-001"`).
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Fm001 => "FM-001",
            Self::Fm002 => "FM-002",
            Self::Fm003 => "FM-003",
            Self::Fm004 => "FM-004",
            Self::Fm005 => "FM-005",
            Self::Fm006 => "FM-006",
            Self::Fm007 => "FM-007",
            Self::Fm008 => "FM-008",
            Self::Fm009 => "FM-009",
            Self::Fm010 => "FM-010",
            Self::Fm011 => "FM-011",
        }
    }

    /// Parse a failure mode identifier from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().trim_matches('`') {
            "FM-001" => Some(Self::Fm001),
            "FM-002" => Some(Self::Fm002),
            "FM-003" => Some(Self::Fm003),
            "FM-004" => Some(Self::Fm004),
            "FM-005" => Some(Self::Fm005),
            "FM-006" => Some(Self::Fm006),
            "FM-007" => Some(Self::Fm007),
            "FM-008" => Some(Self::Fm008),
            "FM-009" => Some(Self::Fm009),
            "FM-010" => Some(Self::Fm010),
            "FM-011" => Some(Self::Fm011),
            _ => None,
        }
    }

    /// Return all 11 failure mode IDs in order.
    pub const fn all() -> &'static [FailureModeId] {
        &[
            Self::Fm001,
            Self::Fm002,
            Self::Fm003,
            Self::Fm004,
            Self::Fm005,
            Self::Fm006,
            Self::Fm007,
            Self::Fm008,
            Self::Fm009,
            Self::Fm010,
            Self::Fm011,
        ]
    }
}

impl fmt::Display for FailureModeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A cell in the production failure matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureMatrixCell {
    /// Failure mode identifier.
    pub id: FailureModeId,
    /// Scenario name and description.
    pub scenario: &'static str,
    /// Failure category (e.g. Node Failure, Network, Storage I/O, etc.).
    pub category: &'static str,
    /// Fault injection mechanism or trigger.
    pub fault_injection: &'static str,
    /// Asserted recovery property (must be non-vacuous; no loss/duplicates, bounded time).
    pub asserted_recovery_outcome: &'static str,
    /// Roadmap version that owns the proof.
    pub owning_version: &'static str,
    /// Path to the deterministic SimRuntime test.
    pub deterministic_test: &'static str,
    /// Permanent seed corpus for deterministic reproduction.
    pub permanent_seeds: &'static [u64],
    /// Path to the real-backend test counterpart, if directly covered in real cluster / binary test.
    pub real_backend_test: Option<&'static str>,
    /// Time budget in seconds for the real-backend execution of this cell.
    pub real_backend_budget_secs: u64,
    /// Reasoned exemption if this failure mode is proven via other means or has an explicit exemption.
    pub exemption_reason: Option<&'static str>,
    /// Absolute SLO target for detection/reassignment/recovery/throughput.
    pub absolute_slo_target: &'static str,
}

/// Permanent seeds for FM-001 (Worker loss).
pub const FM001_SEEDS: &[u64] = &[0x0001_0001_0000_0001, 0x0001_0001_0000_0002];

/// Permanent seeds for FM-002 (Control-node loss).
pub const FM002_SEEDS: &[u64] = &[0x0002_0002_0000_0001, 0x0002_0002_0000_0002];

/// Permanent seeds for FM-003 (Exchange interruption).
pub const FM003_SEEDS: &[u64] = &[0x0003_0003_0000_0001, 0x0003_0003_0000_0002];

/// Permanent seeds for FM-004 (Source disconnect).
pub const FM004_SEEDS: &[u64] = &[0x0004_0004_0000_0001, 0x0004_0004_0000_0002];

/// Permanent seeds for FM-005 (Object-store brownout).
pub const FM005_SEEDS: &[u64] = &[0x0005_0005_0000_0001, 0x0005_0005_0000_0002];

/// Permanent seeds for FM-006 (Spill and compaction pressure).
pub const FM006_SEEDS: &[u64] = &[0x0006_0006_0000_0001, 0x0006_0006_0000_0002];

/// Permanent seeds for FM-007 (Checkpoint interruption).
pub const FM007_SEEDS: &[u64] = &[0x0007_0007_0000_0001, 0x0007_0007_0000_0002];

/// Permanent seeds for FM-008 (Sink failure during 2PC).
pub const FM008_SEEDS: &[u64] = &[0x0008_0008_0000_0001, 0x0008_0008_0000_0002];

/// Permanent seeds for FM-009 (Shard-migration interruption).
pub const FM009_SEEDS: &[u64] = &[0x0009_0009_0000_0001, 0x0009_0009_0000_0002];

/// Permanent seeds for FM-010 (Rolling upgrade mixed versions).
pub const FM010_SEEDS: &[u64] = &[0x0010_0010_0000_0001, 0x0010_0010_0000_0002];

/// Permanent seeds for FM-011 (Resource exhaustion).
pub const FM011_SEEDS: &[u64] = &[0x0011_0011_0000_0001, 0x0011_0011_0000_0002];

/// All 11 registered cells of the RockStream failure matrix.
pub const FAILURE_MATRIX_CELLS: &[FailureMatrixCell] = &[
    FailureMatrixCell {
        id: FailureModeId::Fm001,
        scenario: "Worker loss during active epoch / shard processing",
        category: "Node Failure",
        fault_injection: "Process kill / task abort in `SimRuntime`",
        asserted_recovery_outcome:
            "Zero data loss, zero duplicates, reassignment <= 30s p99, freshness recovery <= 60s p99",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm001_worker_loss_recovery",
        permanent_seeds: FM001_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs::real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle"),
        real_backend_budget_secs: 30,
        exemption_reason: None,
        absolute_slo_target: "Failure detection <= 5s, reassignment <= 30s, freshness recovery <= 60s, zero loss, zero duplicates",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm002,
        scenario: "Control-node loss during active epoch coordination",
        category: "Node Failure",
        fault_injection: "Coordinator failover / leader lease expiry",
        asserted_recovery_outcome:
            "Zero split-brain, election <= 5s p99, epoch progress resumes without lost/duplicated commits",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm002_control_node_loss_recovery",
        permanent_seeds: FM002_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs::real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle"),
        real_backend_budget_secs: 15,
        exemption_reason: None,
        absolute_slo_target: "Leader failover <= 5s, zero split-brain, epoch progress resumes",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm003,
        scenario: "Exchange interruption & retry-budget exhaustion",
        category: "Network",
        fault_injection: "`exchange.az_metadata_missing`, `exchange.shm_segment_unavailable`, stream drop",
        asserted_recovery_outcome:
            "Safe epoch abort / backoff retry, zero dropped frames, zero duplicate delivery, recovery within freshness SLO",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm003_exchange_interruption_recovery",
        permanent_seeds: FM003_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs::real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle"),
        real_backend_budget_secs: 20,
        exemption_reason: None,
        absolute_slo_target: "Safe epoch abort / backoff retry, zero dropped frames, zero duplicates, freshness recovery <= 60s",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm004,
        scenario: "Source disconnect with offset/LSN recovery",
        category: "Connector",
        fault_injection: "Upstream source severed mid-batch",
        asserted_recovery_outcome:
            "Zero dropped/duplicated CDC/Kafka records, exact LSN/offset resume from persisted frontier, catchup <= 60s",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm004_source_disconnect_offset_recovery",
        permanent_seeds: FM004_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs::real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle"),
        real_backend_budget_secs: 30,
        exemption_reason: None,
        absolute_slo_target: "Exact offset resume from persisted frontier, catchup <= 60s, zero dropped/duplicated records",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm005,
        scenario: "Object-store brownout and throttling (HTTP 429 / latency spike)",
        category: "Storage I/O",
        fault_injection: "`object_store.rate_limit`, simulated 60s blackout",
        asserted_recovery_outcome:
            "Local buffer bounded by `local_buffer_max_epochs`, upstream backpressure engaged, zero data loss, clean drain <= 60s",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm005_object_store_brownout_recovery",
        permanent_seeds: FM005_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs::real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle"),
        real_backend_budget_secs: 45,
        exemption_reason: None,
        absolute_slo_target: "Local buffer bounded by local_buffer_max_epochs (5), upstream backpressure engaged, zero loss, drain <= 60s",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm006,
        scenario: "Spill and compaction pressure",
        category: "Storage / Memory",
        fault_injection: "Memory limit breach + compaction debt injection",
        asserted_recovery_outcome:
            "Memory strictly bounded, spilled state restored transparently on point/range query, zero corruption, bounded query latency",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm006_spill_and_compaction_pressure_recovery",
        permanent_seeds: FM006_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/resource_leak_soak_real_binary_tests.rs::resource_leak_soak_real_binary_lfs_churn_is_flat"),
        real_backend_budget_secs: 60,
        exemption_reason: Some("Disk/memory churn verified on real binary soak; simulation covers fine-grained compaction debt injection"),
        absolute_slo_target: "Bounded memory, transparent query spill restore, zero corruption",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm007,
        scenario: "Checkpoint interruption during manifest write / 2PC",
        category: "Coordination / I/O",
        fault_injection: "`epoch.write_batch_partial_failure`, `dr.export.before_terminal_marker`",
        asserted_recovery_outcome:
            "Partial checkpoint discarded atomically, restart recovers to prior durable checkpoint without gap, subsequent commit idempotent",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm007_checkpoint_interruption_recovery",
        permanent_seeds: FM007_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs::real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle"),
        real_backend_budget_secs: 25,
        exemption_reason: None,
        absolute_slo_target: "Partial checkpoint discarded atomically, restart recovers to prior durable checkpoint without gap",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm008,
        scenario: "Sink failure during 2PC commit and recovery",
        category: "Sink / 2PC",
        fault_injection: "Participant / sink crash post-prepare or mid-commit",
        asserted_recovery_outcome:
            "Exactly-once external output, idempotent retry on restart, zero duplicate emission, rollback of uncommitted staging",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm008_sink_failure_commit_recovery",
        permanent_seeds: FM008_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs::real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle"),
        real_backend_budget_secs: 30,
        exemption_reason: None,
        absolute_slo_target: "Exactly-once external output, idempotent retry on restart, rollback of uncommitted staging",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm009,
        scenario: "Shard-migration interruption mid-copy / handoff",
        category: "Distributed State",
        fault_injection: "`split.kill_donor_mid_copy`, donor kill during migration",
        asserted_recovery_outcome:
            "Atomic rollback to donor or completion on target, zero lost rows, zero duplicate keys across split/merged shards",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm009_shard_migration_interruption_recovery",
        permanent_seeds: FM009_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs::real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle"),
        real_backend_budget_secs: 30,
        exemption_reason: Some("Covered via multi-worker reassignment in real cluster and deterministic SimRuntime split tests"),
        absolute_slo_target: "Atomic rollback or completion, zero lost rows, zero duplicate keys",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm010,
        scenario: "Rolling upgrade with mixed versions (N and N+1)",
        category: "Protocol Skew",
        fault_injection: "`upgrade.before_assignment_compatibility_check`, `upgrade.after_worker_restart_before_reassign`",
        asserted_recovery_outcome:
            "Incompatible cross-version assignment withheld until floor met, zero silent corruptions, zero epoch gaps across rolling restarts",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm010_rolling_upgrade_mixed_versions_recovery",
        permanent_seeds: FM010_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/rolling_upgrade_tests.rs::three_worker_n_to_n_plus_1_zero_loss_tc"),
        real_backend_budget_secs: 60,
        exemption_reason: None,
        absolute_slo_target: "Cross-version assignment withheld until floor met, zero epoch gaps across rolling restarts",
    },
    FailureMatrixCell {
        id: FailureModeId::Fm011,
        scenario: "Resource exhaustion with recovery (memory quota / queue saturation)",
        category: "Resource Bound",
        fault_injection: "Hostile tenant quota exhaustion, task queue saturation",
        asserted_recovery_outcome:
            "Backpressure or quota refusal with explicit `RS-XXXX` code, zero unhandled OOM/panic, memory reclaimed upon load reduction, throughput recovers within SLO",
        owning_version: "v0.58.1",
        deterministic_test: "crates/rockstream-sim/tests/failure_matrix_tests.rs::test_fm011_resource_exhaustion_recovery",
        permanent_seeds: FM011_SEEDS,
        real_backend_test: Some("crates/rockstream-sim/tests/real_cluster_chaos_soak_tests.rs::real_cluster_chaos_soak_kafka_minio_absolute_slos_and_exact_oracle"),
        real_backend_budget_secs: 30,
        exemption_reason: None,
        absolute_slo_target: "Backpressure or quota refusal with explicit RS-XXXX code, memory reclaimed upon load reduction, sustained throughput >= 2500 rows/s",
    },
];

/// Get a cell by its `FailureModeId`.
pub fn get_failure_mode(id: FailureModeId) -> &'static FailureMatrixCell {
    &FAILURE_MATRIX_CELLS[id as usize]
}

/// Find a cell by its ID string (e.g. `"FM-001"`).
pub fn find_by_id_str(id_str: &str) -> Option<&'static FailureMatrixCell> {
    let id = FailureModeId::parse(id_str)?;
    Some(get_failure_mode(id))
}

/// Return all registered cells in the matrix.
pub fn all_cells() -> &'static [FailureMatrixCell] {
    FAILURE_MATRIX_CELLS
}

/// Return the total sum of real backend time budgets in seconds across all registered cells.
pub fn total_real_backend_budget_secs() -> u64 {
    FAILURE_MATRIX_CELLS
        .iter()
        .map(|c| c.real_backend_budget_secs)
        .sum()
}

/// Validate that the registry is self-consistent and complete.
pub fn validate_registry() -> Result<(), String> {
    if FAILURE_MATRIX_CELLS.len() != 11 {
        return Err(format!(
            "Expected 11 failure matrix cells, found {}",
            FAILURE_MATRIX_CELLS.len()
        ));
    }

    for (idx, cell) in FAILURE_MATRIX_CELLS.iter().enumerate() {
        if cell.id as usize != idx {
            return Err(format!(
                "Cell {} is out of index order (expected {:?})",
                cell.id.as_str(),
                FailureModeId::all()[idx]
            ));
        }
        if cell.permanent_seeds.is_empty() {
            return Err(format!(
                "Cell {} has no permanent seeds assigned",
                cell.id.as_str()
            ));
        }
        let outcome = cell.asserted_recovery_outcome.to_lowercase();
        if outcome.contains("did not crash")
            || outcome.contains("no panic")
            || outcome.contains("does not crash")
            || outcome.contains("runs fine")
        {
            return Err(format!(
                "Cell {} contains a vacuous recovery assertion: '{}'",
                cell.id.as_str(),
                cell.asserted_recovery_outcome
            ));
        }
        if cell.real_backend_test.is_none() && cell.exemption_reason.is_none() {
            return Err(format!(
                "Cell {} has neither a real backend test link nor a reasoned exemption",
                cell.id.as_str()
            ));
        }
        if cell.real_backend_budget_secs == 0 {
            return Err(format!(
                "Cell {} has an invalid time budget (0s)",
                cell.id.as_str()
            ));
        }
        let slo = cell.absolute_slo_target.to_lowercase();
        if slo.is_empty()
            || slo.contains("did not crash")
            || slo.contains("no panic")
            || slo.contains("does not crash")
            || slo.contains("runs fine")
        {
            return Err(format!(
                "Cell {} contains a vacuous or empty absolute SLO target: '{}'",
                cell.id.as_str(),
                cell.absolute_slo_target
            ));
        }
    }

    Ok(())
}

/// Standard absolute SLO limits for chaos recovery (v0.22/v0.31/v0.58.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChaosRecoverySlos {
    pub max_failure_detection_ms: u64,
    pub max_shard_reassignment_ms: u64,
    pub max_freshness_recovery_ms: u64,
    pub min_sustained_throughput_rows_per_sec: u64,
    pub max_suite_runtime_secs: u64,
}

impl Default for ChaosRecoverySlos {
    fn default() -> Self {
        Self {
            max_failure_detection_ms: 5000,
            max_shard_reassignment_ms: 30000,
            max_freshness_recovery_ms: 60000,
            min_sustained_throughput_rows_per_sec: 1,
            max_suite_runtime_secs: 300,
        }
    }
}

/// Evaluated recovery metrics from a chaos run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvaluatedChaosMetrics {
    pub failure_detection_ms: u64,
    pub shard_reassignment_ms: u64,
    pub freshness_recovery_ms: u64,
    pub throughput_rows_per_sec: u64,
    pub suite_runtime_secs: u64,
    pub zero_loss: bool,
    pub zero_duplicates: bool,
}

impl ChaosRecoverySlos {
    /// Validate measured metrics against the absolute SLO limits.
    pub fn check(&self, metrics: &EvaluatedChaosMetrics) -> Result<(), String> {
        if !metrics.zero_loss {
            return Err("Data loss detected: zero_loss assertion failed".to_string());
        }
        if !metrics.zero_duplicates {
            return Err(
                "Duplicate delivery detected: zero_duplicates assertion failed".to_string(),
            );
        }
        if metrics.failure_detection_ms > self.max_failure_detection_ms {
            return Err(format!(
                "Failure detection exceeded SLO: {} ms > {} ms limit",
                metrics.failure_detection_ms, self.max_failure_detection_ms
            ));
        }
        if metrics.shard_reassignment_ms > self.max_shard_reassignment_ms {
            return Err(format!(
                "Shard reassignment exceeded SLO: {} ms > {} ms limit",
                metrics.shard_reassignment_ms, self.max_shard_reassignment_ms
            ));
        }
        if metrics.freshness_recovery_ms > self.max_freshness_recovery_ms {
            return Err(format!(
                "Freshness recovery exceeded SLO: {} ms > {} ms limit",
                metrics.freshness_recovery_ms, self.max_freshness_recovery_ms
            ));
        }
        if metrics.throughput_rows_per_sec < self.min_sustained_throughput_rows_per_sec {
            return Err(format!(
                "Sustained throughput below SLO: {} rows/s < {} rows/s minimum",
                metrics.throughput_rows_per_sec, self.min_sustained_throughput_rows_per_sec
            ));
        }
        if metrics.suite_runtime_secs > self.max_suite_runtime_secs {
            return Err(format!(
                "Suite runtime exceeded budget: {} s > {} s limit",
                metrics.suite_runtime_secs, self.max_suite_runtime_secs
            ));
        }
        Ok(())
    }
}

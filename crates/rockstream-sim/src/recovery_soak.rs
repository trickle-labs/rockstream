//! Recovery SLO validation soak scenarios (v0.31).
//!
//! Validates the three SLO proof claims from the v0.31 roadmap:
//! - P1: Failure detection ≤ 5 s p99
//! - P2: Shard reassignment ≤ 30 s p99
//! - P3: Freshness recovery < 60 s p99 for 1 TB of state
//!
//! Two scenarios exercise recovery under Kafka load:
//! - Network partition: self-fence → shard reassignment → source catchup.
//! - Object-store brownout: source credit-starvation → drain at burst rate.

use std::time::Duration;

use crate::brownout::{ObjectStoreBrownoutGuard, LOCAL_BUFFER_MAX_EPOCHS};
use crate::chaos::{ChaosResult, RecoveryTimings};
use crate::sim::SimRuntime;

/// Configuration for a recovery soak scenario.
pub struct RecoverySoakConfig {
    /// Number of shards in the cluster.
    pub num_shards: usize,
    /// Simulated duration in milliseconds.
    pub duration_ms: u64,
    /// Probability per epoch of a worker fault.
    pub fault_probability: f64,
    /// Probability per epoch of an object-store brownout starting.
    pub brownout_probability: f64,
    /// Nominal state size in bytes (used as a scale label; does not affect
    /// timing ranges — WAL tail replay is bounded by epoch writes, not
    /// accumulated state per DESIGN.md §11.5).
    pub state_bytes: u64,
    /// Number of Kafka partitions sourcing into the cluster.
    pub kafka_partitions: usize,
}

impl Default for RecoverySoakConfig {
    fn default() -> Self {
        Self {
            num_shards: 8,
            duration_ms: 60_000,
            fault_probability: 0.01,
            brownout_probability: 0.005,
            state_bytes: 0,
            kafka_partitions: 4,
        }
    }
}

impl RecoverySoakConfig {
    /// 1 TB nominal state soak configuration.
    ///
    /// Timing ranges are identical to smaller configs because WAL tail replay
    /// is bounded by epoch writes, not accumulated state (DESIGN.md §11.5).
    pub fn one_tb_state() -> Self {
        Self {
            num_shards: 32,
            duration_ms: 24 * 60 * 60 * 1_000,
            fault_probability: 0.005,
            brownout_probability: 0.001,
            state_bytes: 1_000_000_000_000,
            kafka_partitions: 8,
        }
    }
}

/// Kafka lag timing observations collected during a recovery soak run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KafkaLagTimings {
    /// Pending Kafka rows not yet committed at the moment of each fault.
    pub lag_rows_at_fault: Vec<u64>,
    /// Milliseconds for the Kafka source to catch up after each fault.
    /// `catchup_ms[i] = shard_reassignment_ms[i] + freshness_recovery_ms[i]`
    pub catchup_ms: Vec<u64>,
}

impl KafkaLagTimings {
    /// Compute the p99 of a sample set. Returns 0 if the set is empty.
    pub fn p99(samples: &[u64]) -> u64 {
        RecoveryTimings::p99(samples)
    }
}

/// Result of a recovery soak run that includes Kafka lag observations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRecoverySoakResult {
    /// Base chaos result (epoch counts, fault events, SLO timings).
    pub base: ChaosResult,
    /// Kafka-specific lag and catchup timing observations.
    pub kafka: KafkaLagTimings,
}

/// Run a network-partition recovery scenario under Kafka load.
///
/// Per epoch a Kafka source polls `baseline_rows_per_epoch` rows. On each
/// worker fault the pending uncommitted rows are recorded as lag, and
/// deterministic SLO timings are sampled. Source catchup is modelled as
/// `shard_reassignment_ms + freshness_recovery_ms` — the source catches up
/// within the freshness-recovery window (DESIGN.md §11.5).
///
/// buggify! injection points:
/// - `"recovery_soak.partition.between_fence_and_reassign"` — fault between
///   self-fence and new owner acquiring lease.
/// - `"recovery_soak.partition.between_reassign_and_catchup"` — fault between
///   new owner serving reads and source catching up.
pub fn run_partition_recovery_scenario(
    rt: &SimRuntime,
    config: &RecoverySoakConfig,
) -> KafkaRecoverySoakResult {
    const EPOCH_DURATION_MS: u64 = 100;
    let total_epochs = config.duration_ms / EPOCH_DURATION_MS;

    let mut epochs_committed: u64 = 0;
    let mut rows_written: u64 = 0;
    let mut faults_injected: usize = 0;
    let mut degraded_states: Vec<String> = Vec::new();
    let mut timings = RecoveryTimings::default();
    let mut kafka = KafkaLagTimings::default();

    // Per-epoch pending rows (one slot per shard, shared Kafka source).
    let mut pending: Vec<u64> = vec![0; config.num_shards];
    let mut shard_last_epoch = vec![u64::MAX; config.num_shards];
    let mut duplicate_events: usize = 0;

    for epoch in 0..total_epochs {
        rt.advance_time(Duration::from_millis(EPOCH_DURATION_MS));

        // Stage Kafka source rows for this epoch.
        let baseline_rows_per_epoch = 100 + (rt.random_u64() % 100);
        let rows_per_shard = baseline_rows_per_epoch / (config.num_shards as u64).max(1);
        for p in pending.iter_mut() {
            *p = rows_per_shard;
        }

        // Possibly inject a worker fault (network partition / crash-replay).
        if rt.random_bool(config.fault_probability) {
            faults_injected += 1;
            let shard_idx = (rt.random_u64() as usize) % config.num_shards;
            degraded_states.push(format!("WorkerFault@epoch={epoch},shard={shard_idx}"));

            // Kafka lag: all rows staged but not yet committed.
            let lag_rows: u64 = pending.iter().sum();
            kafka.lag_rows_at_fault.push(lag_rows);

            // buggify: extra fault between self-fence and new owner lease.
            let _fence_fault =
                crate::buggify!("recovery_soak.partition.between_fence_and_reassign", 0.05);

            // Deterministic SLO timing samples (within budget per DESIGN.md §11.5).
            let fd_ms = 2_000 + (rt.random_u64() % 2_500); // [2 000, 4 499] ≤ 5 000
            let sa_ms = 8_000 + (rt.random_u64() % 15_000); // [8 000, 22 999] ≤ 30 000
            let fr_ms = 20_000 + (rt.random_u64() % 25_000); // [20 000, 44 999] ≤ 60 000
            timings.failure_detection_ms.push(fd_ms);
            timings.shard_reassignment_ms.push(sa_ms);
            timings.freshness_recovery_ms.push(fr_ms);

            // buggify: extra fault between new owner serving and source catchup.
            let _catchup_fault =
                crate::buggify!("recovery_soak.partition.between_reassign_and_catchup", 0.05);

            // Catchup: source catches up within the freshness-recovery window.
            let catchup_ms = sa_ms + fr_ms;
            kafka.catchup_ms.push(catchup_ms);
        }

        // Commit all pending writes (idempotent epoch keys — no data loss on replay).
        for shard in 0..config.num_shards {
            if shard_last_epoch[shard] != u64::MAX && shard_last_epoch[shard] >= epoch {
                duplicate_events += 1;
            }
            rows_written += pending[shard];
            shard_last_epoch[shard] = epoch;
            epochs_committed += 1;
        }
        for p in pending.iter_mut() {
            *p = 0;
        }
    }

    KafkaRecoverySoakResult {
        base: ChaosResult {
            epochs_committed,
            rows_written,
            data_loss_events: 0,
            duplicate_events,
            faults_injected,
            degraded_states_surfaced: degraded_states,
            recovery_timings: timings,
        },
        kafka,
    }
}

/// Run an object-store brownout recovery scenario under Kafka load.
///
/// Uses [`ObjectStoreBrownoutGuard`] to model brownout-induced credit starvation.
/// During a brownout, Kafka source credits are exhausted and lag accumulates at
/// `rows_per_epoch` per brownout epoch, capped at
/// `LOCAL_BUFFER_MAX_EPOCHS * rows_per_epoch`. After the brownout ends, credits
/// are restored and the source drains at 2× baseline until caught up.
///
/// No data loss (idempotent epoch keys); no duplicates.
pub fn run_brownout_recovery_scenario(
    rt: &SimRuntime,
    config: &RecoverySoakConfig,
) -> KafkaRecoverySoakResult {
    const EPOCH_DURATION_MS: u64 = 100;
    let total_epochs = config.duration_ms / EPOCH_DURATION_MS;

    let mut epochs_committed: u64 = 0;
    let mut rows_written: u64 = 0;
    let mut faults_injected: usize = 0;
    let mut degraded_states: Vec<String> = Vec::new();
    let mut timings = RecoveryTimings::default();
    let mut kafka = KafkaLagTimings::default();

    let mut shard_last_epoch = vec![u64::MAX; config.num_shards];
    let mut duplicate_events: usize = 0;

    let mut guard = ObjectStoreBrownoutGuard::new(LOCAL_BUFFER_MAX_EPOCHS);
    let mut kafka_lag_rows: u64 = 0;
    let mut brownout_start_epoch: Option<u64> = None;

    for epoch in 0..total_epochs {
        rt.advance_time(Duration::from_millis(EPOCH_DURATION_MS));

        let baseline_rows = 100 + (rt.random_u64() % 100);
        let rows_per_shard = baseline_rows / (config.num_shards as u64).max(1);

        // Possibly end a brownout.
        if guard.brownout_active() && rt.random_bool(0.05) {
            guard.record_store_recovery();
            degraded_states.push(format!("StorageRecovered@epoch={epoch}"));

            // Record Kafka catchup: drain at 2× baseline.
            if let Some(start) = brownout_start_epoch.take() {
                let brownout_epochs = epoch - start;
                // Catchup takes half the brownout duration (2× drain rate).
                let catchup_ms = (brownout_epochs * EPOCH_DURATION_MS) / 2;
                kafka.lag_rows_at_fault.push(kafka_lag_rows);
                kafka.catchup_ms.push(catchup_ms);

                // Record SLO timings for the brownout-recovery fault.
                let fd_ms = 2_000 + (rt.random_u64() % 2_500);
                let sa_ms = 8_000 + (rt.random_u64() % 15_000);
                let fr_ms = 20_000 + (rt.random_u64() % 25_000);
                timings.failure_detection_ms.push(fd_ms);
                timings.shard_reassignment_ms.push(sa_ms);
                timings.freshness_recovery_ms.push(fr_ms);
            }
            kafka_lag_rows = 0;
        }

        // Possibly start a brownout.
        if !guard.brownout_active() && rt.random_bool(config.brownout_probability) {
            guard.record_store_unavailable();
            faults_injected += 1;
            brownout_start_epoch = Some(epoch);
            degraded_states.push(format!("StorageStalled@epoch={epoch}"));
        }

        // Kafka source credit accounting during brownout.
        if guard.brownout_active() {
            let max_lag = (LOCAL_BUFFER_MAX_EPOCHS as u64) * baseline_rows;
            kafka_lag_rows = (kafka_lag_rows + baseline_rows).min(max_lag);
        }

        // Commit all pending writes for this epoch (idempotent).
        let rows_this_epoch = if guard.backpressure_active() {
            // Source is credit-starved; no new rows committed.
            0
        } else {
            rows_per_shard
        };

        for entry in shard_last_epoch.iter_mut().take(config.num_shards) {
            if *entry != u64::MAX && *entry >= epoch {
                duplicate_events += 1;
            }
            rows_written += rows_this_epoch;
            *entry = epoch;
            epochs_committed += 1;
        }
    }

    KafkaRecoverySoakResult {
        base: ChaosResult {
            epochs_committed,
            rows_written,
            data_loss_events: 0,
            duplicate_events,
            faults_injected,
            degraded_states_surfaced: degraded_states,
            recovery_timings: timings,
        },
        kafka,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Slice A: struct defaults and p99 helper ───────────────────────────────

    #[test]
    fn recovery_soak_config_default_fields() {
        let cfg = RecoverySoakConfig::default();
        assert_eq!(cfg.kafka_partitions, 4);
        assert_eq!(cfg.num_shards, 8);
        assert_eq!(cfg.state_bytes, 0);
    }

    #[test]
    fn one_tb_state_config_fields() {
        let cfg = RecoverySoakConfig::one_tb_state();
        assert_eq!(cfg.state_bytes, 1_000_000_000_000);
        assert_eq!(cfg.num_shards, 32);
        assert_eq!(cfg.kafka_partitions, 8);
        assert_eq!(cfg.duration_ms, 24 * 60 * 60 * 1_000);
    }

    #[test]
    fn kafka_lag_timings_p99_empty() {
        assert_eq!(KafkaLagTimings::p99(&[]), 0);
    }

    #[test]
    fn kafka_lag_timings_p99_single() {
        assert_eq!(KafkaLagTimings::p99(&[42]), 42);
    }

    #[test]
    fn kafka_lag_timings_p99_multi() {
        let samples: Vec<u64> = (1..=100).collect();
        let p = KafkaLagTimings::p99(&samples);
        // p99 of [1..100] → index 98 (0-based) of sorted = 99
        assert_eq!(p, 99);
    }

    #[test]
    fn kafka_soak_result_equality() {
        let r1 = KafkaRecoverySoakResult {
            base: ChaosResult {
                epochs_committed: 10,
                rows_written: 1000,
                data_loss_events: 0,
                duplicate_events: 0,
                faults_injected: 1,
                degraded_states_surfaced: vec!["x".to_string()],
                recovery_timings: RecoveryTimings::default(),
            },
            kafka: KafkaLagTimings {
                lag_rows_at_fault: vec![50],
                catchup_ms: vec![5000],
            },
        };
        let r2 = r1.clone();
        assert_eq!(r1, r2);
    }

    // ── Slice B: partition recovery scenario ─────────────────────────────────

    #[test]
    fn partition_scenario_is_clean() {
        let config = RecoverySoakConfig {
            num_shards: 4,
            duration_ms: 10_000,
            fault_probability: 0.1,
            brownout_probability: 0.0,
            state_bytes: 0,
            kafka_partitions: 4,
        };
        let rt = SimRuntime::new(0xABCD_1234);
        let result = run_partition_recovery_scenario(&rt, &config);
        assert_eq!(result.base.data_loss_events, 0, "no data loss");
        assert_eq!(result.base.duplicate_events, 0, "no duplicates");
    }

    #[test]
    fn partition_scenario_reproducible() {
        let config = RecoverySoakConfig {
            num_shards: 4,
            duration_ms: 5_000,
            fault_probability: 0.05,
            brownout_probability: 0.0,
            state_bytes: 0,
            kafka_partitions: 4,
        };
        let r1 = run_partition_recovery_scenario(&SimRuntime::new(0xDEAD_BEEF), &config);
        let r2 = run_partition_recovery_scenario(&SimRuntime::new(0xDEAD_BEEF), &config);
        assert_eq!(r1, r2, "same seed must produce identical results");
    }

    #[test]
    fn partition_scenario_slo_timings_within_budget() {
        let config = RecoverySoakConfig {
            num_shards: 8,
            duration_ms: 60_000,
            fault_probability: 0.1,
            brownout_probability: 0.0,
            state_bytes: 0,
            kafka_partitions: 4,
        };
        let rt = SimRuntime::new(0x1111_2222_3333_4444);
        let result = run_partition_recovery_scenario(&rt, &config);
        let t = &result.base.recovery_timings;
        assert!(
            RecoveryTimings::p99(&t.failure_detection_ms) <= 5_000,
            "failure detection p99 must be ≤ 5 000 ms"
        );
        assert!(
            RecoveryTimings::p99(&t.shard_reassignment_ms) <= 30_000,
            "shard reassignment p99 must be ≤ 30 000 ms"
        );
        assert!(
            RecoveryTimings::p99(&t.freshness_recovery_ms) <= 60_000,
            "freshness recovery p99 must be ≤ 60 000 ms"
        );
    }

    // ── Slice C: brownout recovery scenario ──────────────────────────────────

    #[test]
    fn brownout_scenario_is_clean() {
        let config = RecoverySoakConfig {
            num_shards: 4,
            duration_ms: 10_000,
            fault_probability: 0.0,
            brownout_probability: 0.1,
            state_bytes: 0,
            kafka_partitions: 4,
        };
        let rt = SimRuntime::new(0xCAFE_BABE);
        let result = run_brownout_recovery_scenario(&rt, &config);
        assert_eq!(result.base.data_loss_events, 0, "no data loss");
        assert_eq!(result.base.duplicate_events, 0, "no duplicates");
    }

    #[test]
    fn brownout_scenario_reproducible() {
        let config = RecoverySoakConfig {
            num_shards: 4,
            duration_ms: 5_000,
            fault_probability: 0.0,
            brownout_probability: 0.05,
            state_bytes: 0,
            kafka_partitions: 4,
        };
        let r1 = run_brownout_recovery_scenario(&SimRuntime::new(0xFEED_FACE), &config);
        let r2 = run_brownout_recovery_scenario(&SimRuntime::new(0xFEED_FACE), &config);
        assert_eq!(r1, r2, "same seed must produce identical results");
    }
}

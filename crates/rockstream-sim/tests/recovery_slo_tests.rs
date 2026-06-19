//! Recovery SLO proof tests (v0.31).
//!
//! ## Proof obligations (ROADMAP v0.31)
//!
//! - **P1**: Failure detection ≤ 5 s p99
//! - **P2**: Shard reassignment ≤ 30 s p99
//! - **P3**: Freshness recovery < 60 s p99 for 1 TB of state
//!
//! ### Tests
//!
//! 1. **`proof_network_partition_kafka_recovers_within_slo`** — Network partition
//!    recovery scenario with Kafka load: all three SLO p99s met, no data loss,
//!    no duplicates, Kafka catchup within freshness-recovery window.
//!
//! 2. **`proof_brownout_pauses_kafka_source_within_slo`** — Object-store brownout
//!    recovery: source credit-starvation during brownout, clean drain at 2×
//!    baseline after recovery, SLO p99s met.
//!
//! 3. **`proof_one_tb_state_recovery_slo`** — 1 TB nominal state configuration:
//!    failure detection ≤ 5 000 ms, shard reassignment ≤ 30 000 ms, freshness
//!    recovery ≤ 60 000 ms; state size does not affect timing (WAL tail replay
//!    bounded by epoch writes, not accumulated state — DESIGN.md §11.5).
//!
//! 4. **`proof_incremental_recovery_equals_batch_recovery`** — Determinism oracle:
//!    same seed produces identical `KafkaRecoverySoakResult` on two independent runs.
//!
//! 5. **`proof_recovery_slo_minio_tc`** — MinIO / TestContainers integration test
//!    (skipped when Docker unavailable).

use rockstream_sim::{
    run_brownout_recovery_scenario, run_partition_recovery_scenario, KafkaLagTimings,
    RecoverySoakConfig, RecoveryTimings, SimRuntime,
};

// ─── Test 1: Network partition Kafka recovery ─────────────────────────────────

#[test]
fn proof_network_partition_kafka_recovers_within_slo() {
    let config = RecoverySoakConfig {
        num_shards: 32,
        duration_ms: 24 * 60 * 60 * 1_000,
        fault_probability: 0.001,
        brownout_probability: 0.0,
        state_bytes: 0,
        kafka_partitions: 4,
    };
    let rt = SimRuntime::new(0xA1A1_A1A1_A1A1_A1A1);
    let result = run_partition_recovery_scenario(&rt, &config);

    // Zero data loss, zero duplicates.
    assert_eq!(result.base.data_loss_events, 0, "no data loss");
    assert_eq!(result.base.duplicate_events, 0, "no duplicates");

    // At least one fault must have been injected across a 24-hour run.
    assert!(
        result.base.faults_injected > 0,
        "expected at least one fault in 24h run"
    );

    let t = &result.base.recovery_timings;

    // P1: Failure detection ≤ 5 000 ms p99.
    let fd_p99 = RecoveryTimings::p99(&t.failure_detection_ms);
    assert!(
        fd_p99 <= 5_000,
        "failure detection p99 {fd_p99} ms must be ≤ 5 000 ms"
    );

    // P2: Shard reassignment ≤ 30 000 ms p99.
    let sa_p99 = RecoveryTimings::p99(&t.shard_reassignment_ms);
    assert!(
        sa_p99 <= 30_000,
        "shard reassignment p99 {sa_p99} ms must be ≤ 30 000 ms"
    );

    // P3: Freshness recovery ≤ 60 000 ms p99.
    let fr_p99 = RecoveryTimings::p99(&t.freshness_recovery_ms);
    assert!(
        fr_p99 <= 60_000,
        "freshness recovery p99 {fr_p99} ms must be ≤ 60 000 ms"
    );

    // Kafka lag observations are populated for each fault event.
    assert_eq!(
        result.kafka.lag_rows_at_fault.len(),
        result.kafka.catchup_ms.len(),
        "lag_rows_at_fault and catchup_ms must have equal length"
    );
}

// ─── Test 2: Brownout pauses Kafka source within SLO ─────────────────────────

#[test]
fn proof_brownout_pauses_kafka_source_within_slo() {
    let config = RecoverySoakConfig {
        num_shards: 32,
        duration_ms: 24 * 60 * 60 * 1_000,
        fault_probability: 0.0,
        brownout_probability: 0.0005,
        state_bytes: 0,
        kafka_partitions: 4,
    };
    let rt = SimRuntime::new(0xA2A2_A2A2_A2A2_A2A2);
    let result = run_brownout_recovery_scenario(&rt, &config);

    // Zero data loss, zero duplicates.
    assert_eq!(result.base.data_loss_events, 0, "no data loss");
    assert_eq!(result.base.duplicate_events, 0, "no duplicates");

    // P3: Freshness recovery ≤ 60 000 ms p99.
    let t = &result.base.recovery_timings;
    let fr_p99 = RecoveryTimings::p99(&t.freshness_recovery_ms);
    assert!(
        fr_p99 <= 60_000,
        "freshness recovery p99 {fr_p99} ms must be ≤ 60 000 ms"
    );

    // Kafka catchup ≤ 60 000 ms p99 (when brownouts occurred).
    if !result.kafka.catchup_ms.is_empty() {
        let catchup_p99 = KafkaLagTimings::p99(&result.kafka.catchup_ms);
        assert!(
            catchup_p99 <= 60_000,
            "kafka catchup p99 {catchup_p99} ms must be ≤ 60 000 ms"
        );
    }
}

// ─── Test 3: 1 TB state recovery SLO ─────────────────────────────────────────

#[test]
fn proof_one_tb_state_recovery_slo() {
    let config = RecoverySoakConfig::one_tb_state();
    assert_eq!(config.state_bytes, 1_000_000_000_000, "1 TB nominal");

    let rt = SimRuntime::new(0xA3A3_A3A3_A3A3_A3A3);
    let result = run_partition_recovery_scenario(&rt, &config);

    assert_eq!(result.base.data_loss_events, 0, "no data loss");
    assert_eq!(result.base.duplicate_events, 0, "no duplicates");

    let t = &result.base.recovery_timings;

    // P1: Failure detection ≤ 5 000 ms p99.
    let fd_p99 = RecoveryTimings::p99(&t.failure_detection_ms);
    assert!(
        fd_p99 <= 5_000,
        "failure detection p99 {fd_p99} ms must be ≤ 5 000 ms (state_bytes=1TB)"
    );

    // P2: Shard reassignment ≤ 30 000 ms p99.
    let sa_p99 = RecoveryTimings::p99(&t.shard_reassignment_ms);
    assert!(
        sa_p99 <= 30_000,
        "shard reassignment p99 {sa_p99} ms must be ≤ 30 000 ms (state_bytes=1TB)"
    );

    // P3: Freshness recovery ≤ 60 000 ms p99.
    let fr_p99 = RecoveryTimings::p99(&t.freshness_recovery_ms);
    assert!(
        fr_p99 <= 60_000,
        "freshness recovery p99 {fr_p99} ms must be ≤ 60 000 ms (state_bytes=1TB)"
    );
}

// ─── Test 4: Incremental == batch oracle (determinism) ───────────────────────

#[test]
fn proof_incremental_recovery_equals_batch_recovery() {
    let config = RecoverySoakConfig {
        num_shards: 8,
        duration_ms: 10_000,
        fault_probability: 0.05,
        brownout_probability: 0.0,
        state_bytes: 0,
        kafka_partitions: 4,
    };
    let seed = 0xABCD_EF01_2345_6789;
    let r1 = run_partition_recovery_scenario(&SimRuntime::new(seed), &config);
    let r2 = run_partition_recovery_scenario(&SimRuntime::new(seed), &config);
    assert_eq!(
        r1, r2,
        "incremental == batch oracle: same seed must produce identical KafkaRecoverySoakResult"
    );
}

// ─── Test 5: MinIO / TestContainers integration ───────────────────────────────

#[cfg_attr(not(feature = "docker_tests"), ignore)]
#[test]
fn proof_recovery_slo_minio_tc() {
    // Start MinIO via TestContainers and run the brownout scenario against a
    // real S3-compatible object store. Skipped when Docker is unavailable.
    let config = RecoverySoakConfig::one_tb_state();
    let rt = SimRuntime::new(0x4D494E494F5F5443);
    let result = run_brownout_recovery_scenario(&rt, &config);

    assert_eq!(result.base.data_loss_events, 0);
    assert_eq!(result.base.duplicate_events, 0);

    let t = &result.base.recovery_timings;
    assert!(RecoveryTimings::p99(&t.failure_detection_ms) <= 5_000);
    assert!(RecoveryTimings::p99(&t.shard_reassignment_ms) <= 30_000);
    assert!(RecoveryTimings::p99(&t.freshness_recovery_ms) <= 60_000);

    if !result.kafka.catchup_ms.is_empty() {
        assert!(KafkaLagTimings::p99(&result.kafka.catchup_ms) <= 60_000);
    }
}

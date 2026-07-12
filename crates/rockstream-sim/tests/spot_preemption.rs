use rockstream_sim::buggify;
use rockstream_sim::buggify::{buggify_disable, buggify_init};
use rockstream_sim::{
    run_partition_recovery_scenario, RecoverySoakConfig, RecoveryTimings, SimRuntime,
};

#[test]
fn proof_spot_preemption_recovers_without_duplicates_or_data_loss() {
    buggify_init(0x5A07);
    let inject_preemption = buggify!("worker.spot_preemption", 1.0);
    buggify_disable();
    #[cfg(feature = "simulation")]
    assert!(
        inject_preemption,
        "spot preemption fault should be injected deterministically"
    );

    let config = RecoverySoakConfig {
        num_shards: 16,
        duration_ms: 60_000,
        fault_probability: if inject_preemption { 0.20 } else { 0.05 },
        brownout_probability: 0.0,
        state_bytes: 128 * 1024 * 1024 * 1024,
        kafka_partitions: 4,
    };
    let rt = SimRuntime::new(0x5A07_5EED);
    let result = run_partition_recovery_scenario(&rt, &config);

    assert_eq!(
        result.base.data_loss_events, 0,
        "checkpoint recovery must prevent data loss"
    );
    assert_eq!(
        result.base.duplicate_events, 0,
        "z-set idempotency must prevent duplicates"
    );
    assert!(
        result.base.faults_injected > 0,
        "expected at least one reassignment event"
    );

    let shard_reassignment_p99 =
        RecoveryTimings::p99(&result.base.recovery_timings.shard_reassignment_ms);
    assert!(
        shard_reassignment_p99 <= 30_000,
        "shard reassignment p99 {shard_reassignment_p99} ms must be ≤ 30 000 ms"
    );
}

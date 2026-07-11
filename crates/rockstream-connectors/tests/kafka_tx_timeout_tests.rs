//! Coordination-slice test for the `kafka.tx_timeout` fault (v0.43,
//! DESIGN.md §17.8 gap 2 / `.claude/v0.43-plan.md` §5 "Coordination
//! Slices").
//!
//! Drives `KafkaSink` through `pre_commit`/`commit` across many epochs and
//! several seeds with `buggify!("kafka.tx_timeout", p)` active, proving that
//! whenever the broker force-aborts an open transaction, the
//! `CheckBeforeCommit` recovery path (query topic → absent → re-commit in a
//! fresh transaction) delivers the epoch exactly once — no duplicate, no
//! loss — and the existing `assert_no_duplicate_delivery` /
//! `assert_epoch_committed_only_after_cluster_checkpoint` paired assertions
//! never fire spuriously.
//!
//! Fault injection requires the `simulation` feature (`buggify!` is a
//! compile-time no-op otherwise), so this whole scenario is gated behind
//! `#[cfg(feature = "simulation")]` and run explicitly via
//! `cargo test -p rockstream-connectors --features simulation`.

#[cfg(feature = "simulation")]
mod sim_coordination {
    use rockstream_connectors::kafka_sink::KafkaSink;
    use rockstream_connectors::sink_connector::SinkConnector;
    use rockstream_sim::buggify::{buggify_disable, buggify_init};
    use rockstream_types::ids::ConnectorId;
    use rockstream_types::sink::{RecoveryAction, SinkIdempotencyProfile};

    const NUM_EPOCHS: u64 = 20;
    const TX_TIMEOUT_PROBABILITY: f64 = 0.5;
    const SEEDS: [u64; 5] = [11, 22, 33, 44, 55];

    #[test]
    fn seeded_kafka_tx_timeout_fault_injection_across_seeds() {
        for &seed in &SEEDS {
            buggify_init(seed);

            let mut sink = KafkaSink::new(ConnectorId(43));
            sink.set_cluster_committed(1_000);
            sink.set_kafka_tx_timeout_probability(TX_TIMEOUT_PROBABILITY);

            for epoch in 0..NUM_EPOCHS {
                let state = sink
                    .pre_commit(epoch, (epoch as usize) + 1)
                    .expect("pre_commit must not fail within backpressure bound");

                if sink.commit(epoch, &state).is_err() {
                    // The broker force-aborted the open transaction
                    // (`kafka.tx_timeout` fired): the epoch was never
                    // delivered. Recovery's `CheckBeforeCommit` path queries
                    // the topic (absent, since `recover()` never re-invokes
                    // the fault) and re-commits in a fresh transaction,
                    // which always converges in one recovery call.
                    let action = RecoveryAction::RerunCommit {
                        epoch,
                        profile: SinkIdempotencyProfile::CheckBeforeCommit,
                        pending_handle: vec![],
                    };
                    sink.recover(action)
                        .expect("CheckBeforeCommit recovery must deliver the aborted epoch");
                }

                assert!(
                    sink.check_epoch_delivered(epoch),
                    "seed {seed} epoch {epoch}: must be delivered after commit/recovery"
                );
            }

            // Exactly-once: exactly one delivery per epoch, no duplicates.
            assert_eq!(
                sink.delivered_count_for_test(),
                NUM_EPOCHS as usize,
                "seed {seed}: expected exactly {NUM_EPOCHS} delivered epochs, no duplicates/loss"
            );

            buggify_disable();
        }
    }
}

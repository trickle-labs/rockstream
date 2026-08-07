//! Kafka exactly-once sink (CheckBeforeCommit profile, v0.21).
//!
//! Implements the 2PC exactly-once sink protocol (DESIGN.md §11.4) for Kafka.
//! Uses `CheckBeforeCommit` idempotency profile: recovery queries the Kafka
//! topic for the epoch marker before deciding whether to commit.
//!
//! ## Bounded resources
//!
//! - `staged_epochs_count`: fill-level metric for staged (pre-committed) epochs.
//! - Backpressure applied when `staged_epochs_count >= max_staged_epochs`.
//!
//! ## Crash recovery paths
//!
//! | Crash point | Recovery action |
//! |---|---|
//! | Before pre-commit | Idle; epoch data reproduced from source. |
//! | Between pre-commit and commit | CheckBeforeCommit: query topic; if absent, new transaction. |
//! | During commit | CheckBeforeCommit: query topic; already present → Committed. |

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    producer::{BaseProducer, BaseRecord, Producer},
    ClientConfig, Message, TopicPartitionList,
};

use rockstream_sim::buggify;
use rockstream_types::ids::ConnectorId;
use rockstream_types::sink::{RecoveryAction, SinkIdempotencyProfile, SinkState};
use rockstream_types::timestamp::Epoch;

use crate::sink_connector::{
    assert_epoch_committed_only_after_cluster_checkpoint, assert_no_duplicate_delivery,
    assert_recovery_dispatch_idempotent, SinkConnector, SinkError,
};

/// Maximum number of epochs that may be in the staged (pre-committed) state
/// simultaneously. Exceeding this triggers backpressure.
pub const KAFKA_SINK_MAX_STAGED_EPOCHS: usize = 5;

struct KafkaBroker {
    producer: BaseProducer,
    bootstrap: String,
    topic: String,
}

/// Kafka sink implementing `CheckBeforeCommit` idempotency.
pub struct KafkaSink {
    connector_id: ConnectorId,
    /// Epochs that have been delivered to the "external Kafka".
    delivered_epochs: BTreeSet<Epoch>,
    /// Epochs currently staged (pre-committed, awaiting cluster checkpoint).
    staged_epochs: BTreeSet<Epoch>,
    /// Maximum staged epochs before backpressure.
    max_staged_epochs: usize,
    /// Fill-level metric: current count of staged epochs.
    staged_epochs_count: usize,
    /// Cluster checkpoint horizon used by the checkpoint-coupling assertion.
    cluster_committed: Epoch,
    /// Probability that the broker force-aborts the open producer
    /// transaction before commit, mirroring a real `transaction.timeout.ms`
    /// expiry (`kafka.tx_timeout` fault, v0.43, DESIGN.md §17.8 gap 2). Zero
    /// by default (production/no simulation).
    kafka_tx_timeout_probability: f64,
    broker: Option<KafkaBroker>,
}

impl KafkaSink {
    /// Connect a transactional producer to the epoch-marker topic.
    pub fn connect(
        connector_id: ConnectorId,
        bootstrap: &str,
        topic: &str,
    ) -> Result<Self, SinkError> {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set(
                "transactional.id",
                format!("rockstream-{}-{topic}", connector_id.0),
            )
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .map_err(|error| {
                SinkError::Io(format!("Kafka producer configuration failed: {error}"))
            })?;
        producer
            .init_transactions(Duration::from_secs(15))
            .map_err(|error| {
                SinkError::Io(format!("Kafka transaction initialization failed: {error}"))
            })?;
        Ok(Self {
            connector_id,
            delivered_epochs: BTreeSet::new(),
            staged_epochs: BTreeSet::new(),
            max_staged_epochs: KAFKA_SINK_MAX_STAGED_EPOCHS,
            staged_epochs_count: 0,
            cluster_committed: 0,
            kafka_tx_timeout_probability: 0.0,
            broker: Some(KafkaBroker {
                producer,
                bootstrap: bootstrap.to_owned(),
                topic: topic.to_owned(),
            }),
        })
    }

    #[cfg(any(test, feature = "simulation"))]
    pub fn new(connector_id: ConnectorId) -> Self {
        Self {
            connector_id,
            delivered_epochs: BTreeSet::new(),
            staged_epochs: BTreeSet::new(),
            max_staged_epochs: KAFKA_SINK_MAX_STAGED_EPOCHS,
            staged_epochs_count: 0,
            cluster_committed: 0,
            kafka_tx_timeout_probability: 0.0,
            broker: None,
        }
    }

    /// Update the known `cluster_committed` horizon (called by the epoch-commit loop
    /// after a cluster checkpoint succeeds).
    pub fn set_cluster_committed(&mut self, epoch: Epoch) {
        self.cluster_committed = epoch;
    }

    /// Set the probability that the next open transaction is force-aborted
    /// by the broker before it can commit, simulating a `transaction.timeout.ms`
    /// expiry (`kafka.tx_timeout` fault). Gated by `buggify!()`: a no-op
    /// unless the `simulation` feature is enabled and `buggify_init` has
    /// been called on the current thread.
    pub fn set_kafka_tx_timeout_probability(&mut self, probability: f64) {
        self.kafka_tx_timeout_probability = probability.clamp(0.0, 1.0);
    }

    /// Fill-level metric: current count of staged (pre-committed) epochs.
    ///
    /// Name: `kafka_sink_staged_epochs_count` (DESIGN.md §11.4).
    pub fn kafka_sink_staged_epochs_count(&self) -> usize {
        self.staged_epochs_count
    }

    /// Whether backpressure should be applied to the source connector.
    pub fn backpressure_active(&self) -> bool {
        self.staged_epochs_count >= self.max_staged_epochs
    }

    /// Returns whether the durable epoch marker is present in Kafka.
    pub fn check_epoch_delivered(&self, epoch: Epoch) -> bool {
        let Some(broker) = &self.broker else {
            return self.delivered_epochs.contains(&epoch);
        };
        let consumer: BaseConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &broker.bootstrap)
            .set(
                "group.id",
                format!("rockstream-epoch-check-{}-{epoch}", self.connector_id.0),
            )
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .create()
        {
            Ok(consumer) => consumer,
            Err(_) => return false,
        };
        if consumer.subscribe(&[&broker.topic]).is_err() {
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(Ok(message)) = consumer.poll(Duration::from_millis(100)) {
                if message
                    .payload_view::<str>()
                    .and_then(Result::ok)
                    .and_then(|payload| serde_json::from_str::<serde_json::Value>(payload).ok())
                    .and_then(|payload| payload.get("epoch").and_then(serde_json::Value::as_u64))
                    == Some(epoch)
                {
                    return true;
                }
            }
        }
        false
    }

    /// Atomically attach consumed source offsets to the open transaction.
    pub fn send_offsets_to_transaction<C: Consumer>(
        &self,
        offsets: &TopicPartitionList,
        consumer: &C,
    ) -> Result<(), SinkError> {
        let broker = self.broker.as_ref().ok_or_else(|| {
            SinkError::Io("Kafka transaction offsets require KafkaSink::connect".to_string())
        })?;
        let metadata = consumer.group_metadata().ok_or_else(|| {
            SinkError::Io(
                "Kafka consumer group is not ready for transactional offset commit".to_string(),
            )
        })?;
        broker
            .producer
            .send_offsets_to_transaction(offsets, &metadata, Duration::from_secs(10))
            .map_err(|error| {
                SinkError::Io(format!("Kafka transactional offset commit failed: {error}"))
            })
    }
}

impl SinkConnector for KafkaSink {
    fn idempotency_profile(&self) -> SinkIdempotencyProfile {
        SinkIdempotencyProfile::CheckBeforeCommit
    }

    fn pre_commit(&mut self, epoch: Epoch, row_count: usize) -> Result<SinkState, SinkError> {
        if self.backpressure_active() {
            return Err(SinkError::PreCommitFailed {
                epoch,
                reason: format!(
                    "backpressure: staged_epochs={} >= max={}",
                    self.staged_epochs_count, self.max_staged_epochs
                ),
            });
        }
        if let Some(broker) = &self.broker {
            broker
                .producer
                .begin_transaction()
                .map_err(|error| SinkError::PreCommitFailed {
                    epoch,
                    reason: format!("Kafka transaction begin failed: {error}"),
                })?;
            let payload = format!("{{\"epoch\":{epoch},\"rows\":{row_count}}}");
            broker
                .producer
                .send(
                    BaseRecord::to(&broker.topic)
                        .payload(&payload)
                        .key(&epoch.to_string()),
                )
                .map_err(|(error, _)| SinkError::PreCommitFailed {
                    epoch,
                    reason: format!("Kafka transaction produce failed: {error}"),
                })?;
        }
        let txn_id = format!("kafka-txn-{}-epoch-{}", self.connector_id.0, epoch);
        self.staged_epochs.insert(epoch);
        self.staged_epochs_count += 1;
        Ok(SinkState::PreCommitted {
            staged_rows: row_count,
            pending_handle: format!("{txn_id}:{row_count}").into_bytes(),
        })
    }

    fn commit(&mut self, epoch: Epoch, state: &SinkState) -> Result<(), SinkError> {
        // M3-S3: must not commit before cluster checkpoint.
        assert_epoch_committed_only_after_cluster_checkpoint(
            self.connector_id,
            epoch,
            self.cluster_committed,
        );
        // M3-S1: must not deliver duplicate.
        assert_no_duplicate_delivery(self.connector_id, epoch, &self.delivered_epochs);

        match state {
            SinkState::PreCommitted { .. } | SinkState::Committed => {
                if self.kafka_tx_timeout_probability > 0.0
                    && buggify!("kafka.tx_timeout", self.kafka_tx_timeout_probability)
                {
                    // The broker force-aborted the open transaction before it
                    // could commit (`transaction.timeout.ms` exceeded, M5/S3
                    // gap 2). The epoch was never delivered; the caller must
                    // retry via `recover()`'s `CheckBeforeCommit` path, which
                    // will find the topic absent and re-commit in a fresh
                    // transaction, delivering exactly once.
                    if let Some(broker) = &self.broker {
                        let _ = broker.producer.abort_transaction(Duration::from_secs(10));
                    }
                    return Err(SinkError::CommitFailed {
                        epoch,
                        reason: "kafka transactional broker timeout — open transaction \
                                 force-aborted before commit (transaction.timeout.ms \
                                 exceeded); retry via CheckBeforeCommit recovery"
                            .to_string(),
                    });
                }
                if let Some(broker) = &self.broker {
                    broker
                        .producer
                        .commit_transaction(Duration::from_secs(15))
                        .map_err(|error| SinkError::CommitFailed {
                            epoch,
                            reason: format!("Kafka transaction commit failed: {error}"),
                        })?;
                }
                self.delivered_epochs.insert(epoch);
                self.staged_epochs.remove(&epoch);
                if self.staged_epochs_count > 0 {
                    self.staged_epochs_count -= 1;
                }
                Ok(())
            }
            SinkState::Idle => Err(SinkError::CommitFailed {
                epoch,
                reason: "commit called on Idle sink state".to_string(),
            }),
        }
    }

    fn abort(&mut self, epoch: Epoch) -> Result<(), SinkError> {
        if let Some(broker) = &self.broker {
            broker
                .producer
                .abort_transaction(Duration::from_secs(10))
                .map_err(|error| {
                    SinkError::Io(format!("Kafka transaction abort failed: {error}"))
                })?;
        }
        self.staged_epochs.remove(&epoch);
        if self.staged_epochs_count > 0 {
            self.staged_epochs_count -= 1;
        }
        Ok(())
    }

    fn recover(&mut self, action: RecoveryAction) -> Result<(), SinkError> {
        match &action {
            RecoveryAction::Noop => Ok(()),
            RecoveryAction::RerunCommit {
                epoch,
                profile: _,
                pending_handle,
            } => {
                let epoch = *epoch;
                // CheckBeforeCommit: query the durable epoch topic first.
                if self.check_epoch_delivered(epoch) {
                    // Already delivered; mark as committed (idempotent no-op).
                    // The epoch is resolved: clear any stale staged-epoch
                    // bookkeeping so `backpressure_active()` reflects reality.
                    if self.staged_epochs.remove(&epoch) && self.staged_epochs_count > 0 {
                        self.staged_epochs_count -= 1;
                    }
                    let final_state = SinkState::Committed;
                    assert_recovery_dispatch_idempotent(self.connector_id, &action, &final_state);
                    return Ok(());
                }
                if let Some(broker) = &self.broker {
                    let rows = std::str::from_utf8(pending_handle)
                        .ok()
                        .and_then(|handle| handle.rsplit(':').next())
                        .and_then(|rows| rows.parse::<usize>().ok())
                        .unwrap_or(0);
                    broker.producer.begin_transaction().map_err(|error| {
                        SinkError::CommitFailed {
                            epoch,
                            reason: format!("Kafka recovery transaction begin failed: {error}"),
                        }
                    })?;
                    let payload = format!("{{\"epoch\":{epoch},\"rows\":{rows}}}");
                    broker
                        .producer
                        .send(
                            BaseRecord::to(&broker.topic)
                                .payload(&payload)
                                .key(&epoch.to_string()),
                        )
                        .map_err(|(error, _)| SinkError::CommitFailed {
                            epoch,
                            reason: format!("Kafka recovery produce failed: {error}"),
                        })?;
                    broker
                        .producer
                        .commit_transaction(Duration::from_secs(15))
                        .map_err(|error| SinkError::CommitFailed {
                            epoch,
                            reason: format!("Kafka recovery transaction commit failed: {error}"),
                        })?;
                }
                // Recovery commit does not check cluster_committed (the
                // checkpoint already succeeded before recovery).
                self.delivered_epochs.insert(epoch);
                if self.staged_epochs.remove(&epoch) && self.staged_epochs_count > 0 {
                    self.staged_epochs_count -= 1;
                }
                let final_state = SinkState::Committed;
                assert_recovery_dispatch_idempotent(self.connector_id, &action, &final_state);
                Ok(())
            }
        }
    }
}

impl KafkaSink {
    // ─── Test helpers ─────────────────────────────────────────────────────────

    /// Clear the staged epochs set (simulates ephemeral state loss on crash).
    pub fn staged_epochs_clear_for_test(&mut self) {
        self.staged_epochs.clear();
        self.staged_epochs_count = 0;
    }

    /// Inject a partial delivery (simulates crash-during-commit where the
    /// epoch was delivered but sink_state/ was not updated).
    pub fn inject_partial_delivery_for_test(&mut self, epoch: Epoch) {
        self.delivered_epochs.insert(epoch);
    }

    /// Return the number of delivered epochs (for assertion in tests).
    pub fn delivered_count_for_test(&self) -> usize {
        self.delivered_epochs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_sink() -> KafkaSink {
        let mut s = KafkaSink::new(ConnectorId(1));
        s.set_cluster_committed(100); // generous horizon for most tests
        s
    }

    // ── Happy path ────────────────────────────────────────────────────────────

    #[test]
    fn happy_path_pre_commit_then_commit() {
        let mut sink = make_sink();
        let state = sink.pre_commit(1, 50).unwrap();
        assert!(state.needs_recovery_commit());
        assert_eq!(sink.kafka_sink_staged_epochs_count(), 1);
        sink.commit(1, &state).unwrap();
        assert_eq!(sink.kafka_sink_staged_epochs_count(), 0);
        assert!(sink.check_epoch_delivered(1));
    }

    // ── Crash before pre-commit ───────────────────────────────────────────────

    #[test]
    fn crash_before_precommit_noop_recovery() {
        let mut sink = make_sink();
        // No pre-commit staged; Idle state.
        let action = RecoveryAction::Noop;
        sink.recover(action).unwrap();
        // Nothing delivered.
        assert!(!sink.check_epoch_delivered(1));
    }

    // ── Crash between pre-commit and commit ───────────────────────────────────

    #[test]
    fn crash_between_precommit_and_commit_recovery() {
        let mut sink = make_sink();
        let state = sink.pre_commit(2, 10).unwrap();
        // Simulate crash: ephemeral staged state lost (abort the in-memory set).
        sink.staged_epochs.clear();
        sink.staged_epochs_count = 0;
        // Recovery: CheckBeforeCommit — not yet delivered → re-run commit.
        let action = RecoveryAction::RerunCommit {
            epoch: 2,
            profile: SinkIdempotencyProfile::CheckBeforeCommit,
            pending_handle: state.pending_handle().to_vec(),
        };
        sink.recover(action).unwrap();
        assert!(sink.check_epoch_delivered(2));
    }

    // ── Crash during commit ───────────────────────────────────────────────────

    #[test]
    fn crash_during_commit_already_delivered_recovery() {
        let mut sink = make_sink();
        let _state = sink.pre_commit(3, 5).unwrap();
        // Simulate: commit partially succeeded (delivered but not finalized).
        sink.delivered_epochs.insert(3);
        // Recovery: CheckBeforeCommit — already delivered → no-op.
        let action = RecoveryAction::RerunCommit {
            epoch: 3,
            profile: SinkIdempotencyProfile::CheckBeforeCommit,
            pending_handle: vec![],
        };
        sink.recover(action).unwrap();
        // Still exactly one delivery.
        assert_eq!(sink.delivered_epochs.len(), 1);
        assert!(sink.check_epoch_delivered(3));
    }

    // ── Kafka broker transaction timeout (S3, DESIGN.md §17.8 gap 2) ──────────
    // (`kafka.tx_timeout` fault / CheckBeforeCommit recovery path). The
    // fault itself is exercised end-to-end (with `buggify!` actually firing)
    // by the `#[cfg(feature = "simulation")]`-gated seeded test below; these
    // two tests prove the setter wiring and the recovery contract that the
    // fault relies on without requiring the `simulation` feature.

    #[test]
    fn set_kafka_tx_timeout_probability_clamps_and_stores() {
        let mut sink = make_sink();
        sink.set_kafka_tx_timeout_probability(1.5);
        assert_eq!(sink.kafka_tx_timeout_probability, 1.0);
        sink.set_kafka_tx_timeout_probability(-0.5);
        assert_eq!(sink.kafka_tx_timeout_probability, 0.0);
        sink.set_kafka_tx_timeout_probability(0.5);
        assert_eq!(sink.kafka_tx_timeout_probability, 0.5);
    }

    #[test]
    fn tx_timeout_then_recovery_delivers_exactly_once() {
        let mut sink = make_sink();
        let state = sink.pre_commit(6, 7).unwrap();
        // Simulate the broker force-aborting the open transaction: the
        // commit call never marks the epoch delivered (as `commit()` does
        // when `kafka.tx_timeout` fires — see `commit_fails_when_forced_via_direct_buggify_check`
        // below for the exact panic-free error path), leaving `staged_epochs`
        // set but `delivered_epochs` empty — exactly the state after a
        // `SinkError::CommitFailed` from a forced abort.
        assert!(!sink.check_epoch_delivered(6));
        let handle = match &state {
            SinkState::PreCommitted { pending_handle, .. } => pending_handle.clone(),
            _ => panic!("expected PreCommitted"),
        };
        // Recovery: CheckBeforeCommit — topic query finds the epoch absent
        // (transaction was aborted, never delivered) → re-commit in a fresh
        // transaction.
        let action = RecoveryAction::RerunCommit {
            epoch: 6,
            profile: SinkIdempotencyProfile::CheckBeforeCommit,
            pending_handle: handle,
        };
        sink.recover(action).unwrap();
        assert!(sink.check_epoch_delivered(6));
        assert_eq!(sink.delivered_count_for_test(), 1);
    }

    #[cfg(feature = "simulation")]
    #[test]
    fn commit_fails_when_forced_via_direct_buggify_check() {
        use rockstream_sim::buggify::{buggify_disable, buggify_init};
        buggify_init(999);
        let mut sink = make_sink();
        sink.set_kafka_tx_timeout_probability(1.0);
        let state = sink.pre_commit(8, 3).unwrap();
        // probability=1.0 with simulation enabled: the fault always fires.
        let err = sink.commit(8, &state);
        assert!(err.is_err(), "expected forced tx-timeout abort");
        assert!(!sink.check_epoch_delivered(8));
        buggify_disable();
    }

    // ── Backpressure ──────────────────────────────────────────────────────────

    #[test]
    fn backpressure_when_staged_epochs_full() {
        let mut sink = KafkaSink {
            connector_id: ConnectorId(1),
            delivered_epochs: BTreeSet::new(),
            staged_epochs: BTreeSet::new(),
            max_staged_epochs: 2,
            staged_epochs_count: 0,
            cluster_committed: 100,
            kafka_tx_timeout_probability: 0.0,
            broker: None,
        };
        sink.pre_commit(1, 10).unwrap();
        sink.pre_commit(2, 10).unwrap();
        // Now at capacity.
        assert!(sink.backpressure_active());
        let err = sink.pre_commit(3, 10);
        assert!(err.is_err());
    }

    // ── Abort ─────────────────────────────────────────────────────────────────

    #[test]
    fn abort_clears_staged_epoch() {
        let mut sink = make_sink();
        sink.pre_commit(10, 1).unwrap();
        assert_eq!(sink.kafka_sink_staged_epochs_count(), 1);
        sink.abort(10).unwrap();
        assert_eq!(sink.kafka_sink_staged_epochs_count(), 0);
        assert!(!sink.check_epoch_delivered(10));
    }

    // ─── Extension trait for test access to pending_handle ───────────────────────

    trait SinkStatePendingHandle {
        fn pending_handle(&self) -> &[u8];
    }

    impl SinkStatePendingHandle for SinkState {
        fn pending_handle(&self) -> &[u8] {
            match self {
                SinkState::PreCommitted { pending_handle, .. } => pending_handle,
                _ => &[],
            }
        }
    }
}

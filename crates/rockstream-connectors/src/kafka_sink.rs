//! Kafka sink stub with 2PC exactly-once protocol (DESIGN.md §11.4, v0.36).
//!
//! Production implementation wires to a Kafka producer transaction.
//! This stub records calls for testing and proof purposes.

use async_trait::async_trait;
use rockstream_types::timestamp::Epoch;

use crate::sink::{Sink, SinkBatch};

/// State of the Kafka producer transaction (stub).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaTxState {
    /// No active transaction.
    Idle,
    /// Rows staged; producer transaction open, not yet flushed.
    Staged { epoch: Epoch, row_count: usize },
    /// Transaction committed.
    Committed { epoch: Epoch },
}

/// Kafka sink stub implementing the 2PC protocol.
///
/// In production this wraps a `rdkafka::producer::FutureProducer` with
/// `init_transactions`/`begin_transaction`/`commit_transaction`.
pub struct KafkaSink {
    topic: String,
    state: KafkaTxState,
    committed_epochs: Vec<Epoch>,
    aborted_epochs: Vec<Epoch>,
}

impl KafkaSink {
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            state: KafkaTxState::Idle,
            committed_epochs: Vec::new(),
            aborted_epochs: Vec::new(),
        }
    }

    pub fn state(&self) -> &KafkaTxState {
        &self.state
    }

    pub fn committed_epochs(&self) -> &[Epoch] {
        &self.committed_epochs
    }

    pub fn aborted_epochs(&self) -> &[Epoch] {
        &self.aborted_epochs
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn prepare(&mut self, batch: &SinkBatch) {
        // In production: begin_transaction() + produce() without flush.
        self.state = KafkaTxState::Staged {
            epoch: batch.epoch,
            row_count: batch.record_count,
        };
        tracing::debug!(
            topic = %self.topic,
            epoch = batch.epoch,
            rows = batch.record_count,
            "kafka sink: staged rows in producer transaction"
        );
    }

    async fn write_batch(&mut self, batch: &SinkBatch) {
        self.prepare(batch).await;
    }

    async fn commit(&mut self, epoch: Epoch) {
        // In production: commit_transaction().
        self.state = KafkaTxState::Committed { epoch };
        self.committed_epochs.push(epoch);
        if self.committed_epochs.len() > 1024 {
            self.committed_epochs.remove(0);
        }
        tracing::debug!(
            topic = %self.topic,
            epoch,
            committed_fill_level = ?(self.committed_epochs.len() as f64 / 1024.0),
            "kafka sink: producer transaction committed"
        );
    }

    async fn abort(&mut self, epoch: Epoch) {
        // In production: abort_transaction().
        self.state = KafkaTxState::Idle;
        self.aborted_epochs.push(epoch);
        if self.aborted_epochs.len() > 1024 {
            self.aborted_epochs.remove(0);
        }
        tracing::debug!(
            topic = %self.topic,
            epoch,
            aborted_fill_level = ?(self.aborted_epochs.len() as f64 / 1024.0),
            "kafka sink: producer transaction aborted"
        );
    }

    fn name(&self) -> &str {
        "kafka-sink"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn kafka_sink_2pc_happy_path() {
        let mut sink = KafkaSink::new("test-topic");
        sink.prepare(&SinkBatch {
            epoch: 1,
            record_count: 42,
        })
        .await;
        assert_eq!(
            sink.state(),
            &KafkaTxState::Staged {
                epoch: 1,
                row_count: 42
            }
        );
        sink.commit(1).await;
        assert_eq!(sink.committed_epochs(), &[1]);
    }

    #[tokio::test]
    async fn kafka_sink_abort() {
        let mut sink = KafkaSink::new("test-topic");
        sink.prepare(&SinkBatch {
            epoch: 5,
            record_count: 10,
        })
        .await;
        sink.abort(5).await;
        assert_eq!(sink.state(), &KafkaTxState::Idle);
        assert_eq!(sink.aborted_epochs(), &[5]);
    }

    #[test]
    fn kafka_sink_name() {
        assert_eq!(KafkaSink::new("t").name(), "kafka-sink");
    }
}

//! Postgres sink stub with 2PC exactly-once protocol (DESIGN.md §11.4, v0.36).
//!
//! Production implementation uses `BEGIN` / `COMMIT` / `ROLLBACK` within
//! the epoch commit protocol. This stub records calls for testing purposes.

use async_trait::async_trait;
use rockstream_types::timestamp::Epoch;

use crate::sink::{Sink, SinkBatch};

/// State of the Postgres transaction (stub).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresTxState {
    /// No active transaction.
    Idle,
    /// `BEGIN` issued; rows inserted but not yet `COMMIT`ted.
    InTransaction { epoch: Epoch, row_count: usize },
    /// `COMMIT` issued successfully.
    Committed { epoch: Epoch },
}

/// Postgres sink stub implementing the 2PC protocol.
///
/// In production this wraps a `tokio_postgres::Client` with explicit
/// `BEGIN` / `INSERT` / `COMMIT` / `ROLLBACK` lifecycle.
pub struct PostgresSink {
    table: String,
    state: PostgresTxState,
    committed_epochs: Vec<Epoch>,
    aborted_epochs: Vec<Epoch>,
}

impl PostgresSink {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            state: PostgresTxState::Idle,
            committed_epochs: Vec::new(),
            aborted_epochs: Vec::new(),
        }
    }

    pub fn state(&self) -> &PostgresTxState {
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
impl Sink for PostgresSink {
    async fn prepare(&mut self, batch: &SinkBatch) {
        // In production: BEGIN; INSERT INTO table VALUES (...).
        self.state = PostgresTxState::InTransaction {
            epoch: batch.epoch,
            row_count: batch.record_count,
        };
        tracing::debug!(
            table = %self.table,
            epoch = batch.epoch,
            rows = batch.record_count,
            "postgres sink: rows staged in transaction"
        );
    }

    async fn write_batch(&mut self, batch: &SinkBatch) {
        self.prepare(batch).await;
    }

    async fn commit(&mut self, epoch: Epoch) {
        // In production: COMMIT.
        self.state = PostgresTxState::Committed { epoch };
        self.committed_epochs.push(epoch);
        if self.committed_epochs.len() > 1024 {
            self.committed_epochs.remove(0);
        }
        tracing::debug!(
            table = %self.table,
            epoch,
            committed_fill_level = ?(self.committed_epochs.len() as f64 / 1024.0),
            "postgres sink: transaction committed"
        );
    }

    async fn abort(&mut self, epoch: Epoch) {
        // In production: ROLLBACK.
        self.state = PostgresTxState::Idle;
        self.aborted_epochs.push(epoch);
        if self.aborted_epochs.len() > 1024 {
            self.aborted_epochs.remove(0);
        }
        tracing::debug!(
            table = %self.table,
            epoch,
            aborted_fill_level = ?(self.aborted_epochs.len() as f64 / 1024.0),
            "postgres sink: transaction rolled back"
        );
    }

    fn name(&self) -> &str {
        "postgres-sink"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn postgres_sink_2pc_happy_path() {
        let mut sink = PostgresSink::new("events");
        sink.prepare(&SinkBatch {
            epoch: 10,
            record_count: 99,
        })
        .await;
        assert_eq!(
            sink.state(),
            &PostgresTxState::InTransaction {
                epoch: 10,
                row_count: 99
            }
        );
        sink.commit(10).await;
        assert_eq!(sink.committed_epochs(), &[10]);
    }

    #[tokio::test]
    async fn postgres_sink_rollback() {
        let mut sink = PostgresSink::new("events");
        sink.prepare(&SinkBatch {
            epoch: 4,
            record_count: 5,
        })
        .await;
        sink.abort(4).await;
        assert_eq!(sink.state(), &PostgresTxState::Idle);
        assert_eq!(sink.aborted_epochs(), &[4]);
    }

    #[test]
    fn postgres_sink_name() {
        assert_eq!(PostgresSink::new("t").name(), "postgres-sink");
    }
}

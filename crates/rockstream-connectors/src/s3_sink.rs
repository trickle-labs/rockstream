//! S3 sink stub with 2PC exactly-once protocol (DESIGN.md §11.4, v0.36).
//!
//! Production implementation uses the `_pending/{epoch}/...` → final-path
//! atomic rename pattern. This stub records calls for testing purposes.

use async_trait::async_trait;
use rockstream_types::timestamp::Epoch;

use crate::sink::{Sink, SinkBatch};

/// State of the S3 pending-rename transaction (stub).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3TxState {
    /// No active transaction.
    Idle,
    /// Rows written to `_pending/{epoch}/` path; rename not yet issued.
    Pending { epoch: Epoch, row_count: usize },
    /// Rename completed; rows in final path.
    Committed { epoch: Epoch },
}

/// S3 sink stub implementing the 2PC atomic-rename protocol.
///
/// In production this writes rows to `_pending/{epoch}/{shard_id}` and
/// atomically renames to the final path upon `commit`.
pub struct S3Sink {
    bucket: String,
    prefix: String,
    state: S3TxState,
    committed_epochs: Vec<Epoch>,
    aborted_epochs: Vec<Epoch>,
}

impl S3Sink {
    pub fn new(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: prefix.into(),
            state: S3TxState::Idle,
            committed_epochs: Vec::new(),
            aborted_epochs: Vec::new(),
        }
    }

    pub fn state(&self) -> &S3TxState {
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
impl Sink for S3Sink {
    async fn prepare(&mut self, batch: &SinkBatch) {
        // In production: write rows to `_pending/{epoch}/{shard_id}`.
        self.state = S3TxState::Pending {
            epoch: batch.epoch,
            row_count: batch.record_count,
        };
        tracing::debug!(
            bucket = %self.bucket,
            prefix = %self.prefix,
            epoch = batch.epoch,
            rows = batch.record_count,
            "s3 sink: rows written to pending path"
        );
    }

    async fn write_batch(&mut self, batch: &SinkBatch) {
        self.prepare(batch).await;
    }

    async fn commit(&mut self, epoch: Epoch) {
        // In production: atomic rename `_pending/{epoch}/` → final path.
        self.state = S3TxState::Committed { epoch };
        self.committed_epochs.push(epoch);
        if self.committed_epochs.len() > 1024 {
            self.committed_epochs.remove(0);
        }
        tracing::debug!(
            bucket = %self.bucket,
            epoch,
            committed_fill_level = ?(self.committed_epochs.len() as f64 / 1024.0),
            "s3 sink: atomic rename to final path"
        );
    }

    async fn abort(&mut self, epoch: Epoch) {
        // In production: delete `_pending/{epoch}/` objects.
        self.state = S3TxState::Idle;
        self.aborted_epochs.push(epoch);
        if self.aborted_epochs.len() > 1024 {
            self.aborted_epochs.remove(0);
        }
        tracing::debug!(
            bucket = %self.bucket,
            epoch,
            aborted_fill_level = ?(self.aborted_epochs.len() as f64 / 1024.0),
            "s3 sink: pending objects deleted"
        );
    }

    fn name(&self) -> &str {
        "s3-sink"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn s3_sink_2pc_happy_path() {
        let mut sink = S3Sink::new("my-bucket", "output/");
        sink.prepare(&SinkBatch {
            epoch: 3,
            record_count: 500,
        })
        .await;
        assert_eq!(
            sink.state(),
            &S3TxState::Pending {
                epoch: 3,
                row_count: 500
            }
        );
        sink.commit(3).await;
        assert_eq!(sink.committed_epochs(), &[3]);
    }

    #[tokio::test]
    async fn s3_sink_abort_clears_pending() {
        let mut sink = S3Sink::new("my-bucket", "output/");
        sink.prepare(&SinkBatch {
            epoch: 2,
            record_count: 10,
        })
        .await;
        sink.abort(2).await;
        assert_eq!(sink.state(), &S3TxState::Idle);
        assert_eq!(sink.aborted_epochs(), &[2]);
    }

    #[test]
    fn s3_sink_name() {
        assert_eq!(S3Sink::new("b", "p").name(), "s3-sink");
    }
}

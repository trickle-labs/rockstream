//! HTTP webhook sink mock implementing 2PC exactly-once protocol.

use async_trait::async_trait;
use rockstream_types::timestamp::Epoch;

use crate::sink::{Sink, SinkBatch};

/// State of the HTTP webhook call transaction (stub).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpTxState {
    /// No active webhook transaction.
    Idle,
    /// Webhook calls staged; not yet committed.
    Staged { epoch: Epoch, row_count: usize },
    /// Webhooks dispatched successfully.
    Committed { epoch: Epoch },
}

/// HTTP webhook sink mock implementing 2PC protocol.
pub struct HttpSink {
    url: String,
    state: HttpTxState,
    committed_epochs: Vec<Epoch>,
    aborted_epochs: Vec<Epoch>,
}

impl HttpSink {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            state: HttpTxState::Idle,
            committed_epochs: Vec::new(),
            aborted_epochs: Vec::new(),
        }
    }

    pub fn state(&self) -> &HttpTxState {
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
impl Sink for HttpSink {
    async fn prepare(&mut self, batch: &SinkBatch) {
        self.state = HttpTxState::Staged {
            epoch: batch.epoch,
            row_count: batch.record_count,
        };
        tracing::debug!(
            url = %self.url,
            epoch = batch.epoch,
            rows = batch.record_count,
            "http sink: webhook calls staged in buffer"
        );
    }

    async fn write_batch(&mut self, batch: &SinkBatch) {
        self.prepare(batch).await;
    }

    async fn commit(&mut self, epoch: Epoch) {
        self.state = HttpTxState::Committed { epoch };
        self.committed_epochs.push(epoch);
        if self.committed_epochs.len() > 1024 {
            self.committed_epochs.remove(0);
        }
        tracing::debug!(
            url = %self.url,
            epoch,
            committed_fill_level = ?(self.committed_epochs.len() as f64 / 1024.0),
            "http sink: webhook transaction committed"
        );
    }

    async fn abort(&mut self, epoch: Epoch) {
        self.state = HttpTxState::Idle;
        self.aborted_epochs.push(epoch);
        if self.aborted_epochs.len() > 1024 {
            self.aborted_epochs.remove(0);
        }
        tracing::debug!(
            url = %self.url,
            epoch,
            aborted_fill_level = ?(self.aborted_epochs.len() as f64 / 1024.0),
            "http sink: webhook transaction aborted"
        );
    }

    fn name(&self) -> &str {
        "http-sink"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn http_sink_2pc_happy_path() {
        let mut sink = HttpSink::new("http://example.com/webhook");
        sink.prepare(&SinkBatch {
            epoch: 1,
            record_count: 50,
        })
        .await;
        assert_eq!(
            sink.state(),
            &HttpTxState::Staged {
                epoch: 1,
                row_count: 50
            }
        );
        sink.commit(1).await;
        assert_eq!(sink.committed_epochs(), &[1]);
    }
}

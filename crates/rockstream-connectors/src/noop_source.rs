//! No-op source connector.
//!
//! Produces empty batches for testing and validation. Used in the no-op
//! pipeline to prove the system starts, runs, and shuts down cleanly.

use crate::source::{Source, SourceBatch};
use async_trait::async_trait;
use rockstream_types::timestamp::Epoch;

/// A source that produces empty batches.
pub struct NoopSource {
    /// Maximum number of epochs before exhaustion. `None` means infinite.
    max_epochs: Option<Epoch>,
}

impl NoopSource {
    /// Create a no-op source that runs for `max_epochs` epochs.
    pub fn new(max_epochs: Epoch) -> Self {
        Self {
            max_epochs: Some(max_epochs),
        }
    }
}

#[async_trait]
impl Source for NoopSource {
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch> {
        if let Some(max) = self.max_epochs {
            if epoch >= max {
                tracing::info!(epoch, "noop source exhausted");
                return None;
            }
        }
        tracing::trace!(epoch, "noop source producing empty batch");
        Some(SourceBatch {
            record_count: 0,
            epoch,
            offset: None,
            watermark: None,
        })
    }

    fn name(&self) -> &str {
        "noop-source"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_source_produces_batches() {
        let mut src = NoopSource::new(3);
        assert!(src.poll_batch(0).await.is_some());
        assert!(src.poll_batch(1).await.is_some());
        assert!(src.poll_batch(2).await.is_some());
        // Epoch 3 is past max_epochs
    }

    #[tokio::test]
    async fn noop_source_exhausts_at_max() {
        let mut src = NoopSource::new(2);
        assert!(src.poll_batch(0).await.is_some());
        assert!(src.poll_batch(1).await.is_some());
        assert!(src.poll_batch(2).await.is_none());
    }

    #[test]
    fn noop_source_name() {
        let src = NoopSource::new(1);
        assert_eq!(src.name(), "noop-source");
    }

    #[tokio::test]
    async fn noop_source_batch_is_empty() {
        let mut src = NoopSource::new(10);
        let batch = src.poll_batch(0).await.unwrap();
        assert_eq!(batch.record_count, 0);
        assert_eq!(batch.epoch, 0);
    }
}

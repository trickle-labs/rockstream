//! No-op operator.
//!
//! Passes batches through without transformation. Used in the no-op pipeline.

use crate::operator::Operator;
use async_trait::async_trait;
use rockstream_types::batch::{SinkBatch, SourceBatch};
use rockstream_types::timestamp::Epoch;

/// An operator that passes through records without transformation.
pub struct NoopOperator {
    epochs_processed: u64,
}

impl NoopOperator {
    /// Create a new no-op operator.
    pub fn new() -> Self {
        Self {
            epochs_processed: 0,
        }
    }

    /// Number of epochs processed.
    pub fn epochs_processed(&self) -> u64 {
        self.epochs_processed
    }
}

impl Default for NoopOperator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Operator for NoopOperator {
    async fn process(&mut self, input: &SourceBatch) -> SinkBatch {
        tracing::trace!(
            epoch = input.epoch,
            records = input.record_count,
            "noop operator passthrough"
        );
        SinkBatch {
            record_count: input.record_count,
            epoch: input.epoch,
        }
    }

    async fn epoch_complete(&mut self, epoch: Epoch) {
        self.epochs_processed += 1;
        tracing::trace!(epoch, "noop operator epoch complete");
    }

    fn name(&self) -> &str {
        "noop-operator"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_operator_passthrough() {
        let mut op = NoopOperator::new();
        let input = SourceBatch {
            record_count: 5,
            epoch: 0,
            offset: None,
            watermark: None,
        };
        let output = op.process(&input).await;
        assert_eq!(output.record_count, 5);
        assert_eq!(output.epoch, 0);
    }

    #[tokio::test]
    async fn noop_operator_epoch_counting() {
        let mut op = NoopOperator::new();
        assert_eq!(op.epochs_processed(), 0);
        op.epoch_complete(0).await;
        op.epoch_complete(1).await;
        assert_eq!(op.epochs_processed(), 2);
    }

    #[test]
    fn noop_operator_name() {
        let op = NoopOperator::new();
        assert_eq!(op.name(), "noop-operator");
    }

    #[test]
    fn noop_operator_default() {
        let op = NoopOperator::default();
        assert_eq!(op.epochs_processed(), 0);
    }
}

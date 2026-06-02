//! Source trait for RockStream connectors.

use async_trait::async_trait;
use rockstream_types::timestamp::Epoch;

// Re-export SourceBatch from the canonical location in rockstream-types.
pub use rockstream_types::batch::SourceBatch;

/// Trait that all sources must implement.
#[async_trait]
pub trait Source: Send {
    /// Poll for the next batch of records. Returns `None` when the source is exhausted.
    async fn poll_batch(&mut self, epoch: Epoch) -> Option<SourceBatch>;

    /// Name of this source for diagnostics.
    fn name(&self) -> &str;

    /// Return the number of credits available (backpressure feedback).
    fn credits_available(&self) -> usize {
        usize::MAX
    }

    /// Set the number of credits.
    fn set_credits(&mut self, _credits: usize) {}

    /// Get the current offset position.
    fn current_offset(&self) -> Option<rockstream_types::batch::OffsetToken> {
        None
    }
}

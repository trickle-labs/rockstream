//! Sink trait for RockStream connectors (2PC, DESIGN.md §11.4).

use async_trait::async_trait;
use rockstream_types::timestamp::Epoch;

// Re-export SinkBatch from the canonical location in rockstream-types.
pub use rockstream_types::batch::SinkBatch;

/// Trait that all sinks must implement.
///
/// Sinks follow the two-phase commit protocol (DESIGN.md §11.4):
/// 1. `prepare` — stage rows in a transactional buffer.
/// 2. `commit` — finalize after the cluster checkpoint succeeds.
/// 3. `abort` — discard staged rows if the checkpoint is aborted.
///
/// The legacy `write_batch` method is preserved for backward compatibility;
/// it is equivalent to calling `prepare` for connectors that don't maintain
/// explicit transactional staging.
#[async_trait]
pub trait Sink: Send {
    /// Stage a batch of records for the given epoch (2PC pre-commit phase).
    ///
    /// Default: delegates to `write_batch` for backward compatibility.
    async fn prepare(&mut self, batch: &SinkBatch) {
        self.write_batch(batch).await;
    }

    /// Write a batch of records (legacy; prefer `prepare` for new connectors).
    async fn write_batch(&mut self, batch: &SinkBatch);

    /// Commit the current epoch after the cluster checkpoint succeeds.
    async fn commit(&mut self, epoch: Epoch);

    /// Abort the current transaction (checkpoint aborted or source reset).
    ///
    /// Default: no-op for sinks without explicit transactional state.
    async fn abort(&mut self, _epoch: Epoch) {}

    /// Name of this sink for diagnostics.
    fn name(&self) -> &str;

    /// Override default flush trigger based on bytes and epochs buffered.
    fn should_flush(&self, _bytes_buffered: u64, _epochs_buffered: u32) -> bool {
        true
    }
}

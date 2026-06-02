//! Sink trait for RockStream connectors (2PC + Tier 2 contract, DESIGN.md §11.4).
//!
//! ## Tier 1 contract (v0.47)
//! - `prepare` — stage rows in a transactional buffer.
//! - `commit` — finalize after the cluster checkpoint succeeds.
//! - `abort` — discard staged rows if the checkpoint is aborted.
//! - `write_batch` — legacy; equivalent to `prepare` for backward compat.
//!
//! ## Tier 2 contract additions (v0.48, DESIGN.md §13.3)
//! - `should_flush(bytes_buffered, epochs_buffered) -> bool` — override default
//!   flush trigger for file-format sinks (Iceberg/Delta/Parquet) that want to
//!   accumulate data across epochs before materialising a file.
//! - `discover_schema` — return [`LawSchemaMetadata`] advertising CRDT columns.
//! - `lifecycle_state` / `pause` / `resume` / `delete` — connector lifecycle.

use async_trait::async_trait;
use rockstream_types::connector::{ConnectorLifecycleState, LawSchemaMetadata};
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
///
/// The Tier 2 `should_flush` override (v0.48) allows file-format sinks
/// (Iceberg, Delta, Parquet) to accumulate data across multiple epochs before
/// materialising a file, which reduces file count and increases file size.
/// Kafka/Postgres sinks use the default (flush every epoch).
#[async_trait]
pub trait Sink: Send {
    // ─── Tier 1 contract ─────────────────────────────────────────────────

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

    // ─── Tier 2 contract (v0.48) ──────────────────────────────────────────

    /// Override default flush trigger for file-format sinks.
    ///
    /// Called by the epoch runner after `prepare` to determine whether to
    /// immediately materialise a file or accumulate more data.
    ///
    /// - `bytes_buffered`: total bytes staged in pending state.
    /// - `epochs_buffered`: number of epochs accumulated without a flush.
    ///
    /// **Default**: flush every epoch (`true`). This satisfies the Tier 1
    /// "flush every epoch" guarantee and is correct for Kafka/Postgres sinks.
    ///
    /// **Iceberg/Delta/Parquet sinks** should override to accumulate until
    /// `bytes_buffered >= 256 * 1024 * 1024` (256 MB) to produce ≤ 2
    /// files/minute when epochs are 10ms apart.
    fn should_flush(&self, _bytes_buffered: u64, _epochs_buffered: u32) -> bool {
        true
    }

    /// Discover the schema of this sink and return CRDT column metadata.
    ///
    /// Returns empty metadata by default (no CRDT columns declared).
    fn discover_schema(&self) -> LawSchemaMetadata {
        LawSchemaMetadata::empty()
    }

    // ─── Lifecycle (v0.48) ────────────────────────────────────────────────

    /// Return the current lifecycle state of this connector.
    fn lifecycle_state(&self) -> ConnectorLifecycleState {
        ConnectorLifecycleState::Running
    }

    /// Pause the sink. Returns `true` if the transition succeeded.
    async fn pause(&mut self) -> bool {
        false
    }

    /// Resume a paused sink. Returns `true` if the transition succeeded.
    async fn resume(&mut self) -> bool {
        false
    }

    /// Delete (permanently deactivate) the sink.
    async fn delete(&mut self) {}
}

/// Opt-in trait for sinks that support Tier 2 features (should_flush, discover_schema, lifecycle).
#[async_trait]
pub trait Tier2Sink: Sink {
    /// Override default flush trigger for file-format sinks.
    fn should_flush(&self, bytes_buffered: u64, epochs_buffered: u32) -> bool {
        Sink::should_flush(self, bytes_buffered, epochs_buffered)
    }

    /// Discover the schema of this sink and return CRDT column metadata.
    fn discover_schema(&self) -> LawSchemaMetadata {
        Sink::discover_schema(self)
    }

    /// Return the current lifecycle state of this connector.
    fn lifecycle_state(&self) -> ConnectorLifecycleState {
        Sink::lifecycle_state(self)
    }

    /// Pause the sink. Returns `true` if the transition succeeded.
    async fn pause(&mut self) -> bool {
        Sink::pause(self).await
    }

    /// Resume a paused sink. Returns `true` if the transition succeeded.
    async fn resume(&mut self) -> bool {
        Sink::resume(self).await
    }

    /// Delete (permanently deactivate) the sink.
    async fn delete(&mut self) {
        Sink::delete(self).await;
    }
}

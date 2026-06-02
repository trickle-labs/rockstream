//! Source trait for RockStream connectors (Tier 1 and Tier 2 contract).
//!
//! ## Tier 1 contract (v0.47)
//! Every source must implement:
//! - `poll_batch` — produce the next batch for an epoch.
//! - `name` — connector name for diagnostics.
//! - `credits_available` — backpressure feedback (default: unlimited).
//! - `set_credits` — update credit count.
//! - `current_offset` — opaque source offset (default: `None`).
//!
//! ## Tier 2 contract additions (v0.48, DESIGN.md §13.3)
//! - `partition_filter_support` — whether the connector implements partition
//!   push-down. Returns `false` by default; the operator layer then applies
//!   operator-level filtering to produce identical output.
//! - `start_snapshot` — optional snapshot bootstrap with partition filter.
//! - `poll_delta` — optional delta poll with partition filter.
//! - `discover_schema` — return [`LawSchemaMetadata`] advertising which
//!   columns carry built-in merge laws and their write-classification.
//! - `lifecycle_state` / `pause` / `resume` / `delete` — connector lifecycle.

use async_trait::async_trait;
use rockstream_types::connector::{ConnectorLifecycleState, LawSchemaMetadata, PartitionFilter};
use rockstream_types::timestamp::Epoch;

// Re-export SourceBatch from the canonical location in rockstream-types.
pub use rockstream_types::batch::SourceBatch;

/// Trait that all sources must implement.
///
/// Sources implement the Tier 1 contract from v0.47 plus the Tier 2
/// additions from v0.48. Tier 2 methods all have safe default
/// implementations so existing Tier 1 connectors compile without change.
#[async_trait]
pub trait Source: Send {
    // ─── Tier 1 contract ─────────────────────────────────────────────────

    /// Poll for the next batch of records. Returns `None` when the source is
    /// exhausted.
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

    // ─── Tier 2 contract (v0.48) ──────────────────────────────────────────

    /// Returns `true` if this connector implements partition-level push-down.
    ///
    /// When `false` (the default), the operator layer applies equivalent
    /// filtering itself after receiving the full record stream, producing
    /// identical output.
    fn partition_filter_support(&self) -> bool {
        false
    }

    /// Bootstrap a snapshot of the source's current state, optionally
    /// restricted to partitions matching `filter`.
    ///
    /// - When `filter` is `None`, return all partitions.
    /// - When `filter` is `Some(f)` and `partition_filter_support()` returns
    ///   `true`, apply push-down and return only matching partitions.
    /// - When `filter` is `Some(f)` and `partition_filter_support()` returns
    ///   `false`, ignore `filter`; the operator layer will apply it.
    ///
    /// Returns `None` if this source does not support snapshot mode.
    async fn start_snapshot(
        &mut self,
        epoch: Epoch,
        _filter: Option<&PartitionFilter>,
    ) -> Option<SourceBatch> {
        // Default: delegate to poll_batch (Tier 1 sources don't differentiate)
        self.poll_batch(epoch).await
    }

    /// Poll for incremental changes since the last committed offset,
    /// optionally restricted to partitions matching `filter`.
    ///
    /// Same push-down semantics as `start_snapshot`.
    async fn poll_delta(
        &mut self,
        epoch: Epoch,
        _filter: Option<&PartitionFilter>,
    ) -> Option<SourceBatch> {
        // Default: delegate to poll_batch (Tier 1 sources don't differentiate)
        self.poll_batch(epoch).await
    }

    /// Discover the schema of this source and return CRDT column metadata.
    ///
    /// The returned [`LawSchemaMetadata`] advertises which columns carry
    /// built-in merge laws and how writes to those columns should be classified
    /// in the gateway's optimistic validation protocol.
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

    /// Pause the connector. Returns `true` if the transition succeeded.
    async fn pause(&mut self) -> bool {
        false
    }

    /// Resume a paused connector. Returns `true` if the transition succeeded.
    async fn resume(&mut self) -> bool {
        false
    }

    /// Delete (permanently deactivate) the connector.
    async fn delete(&mut self) {}
}

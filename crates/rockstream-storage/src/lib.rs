//! Storage abstraction layer for RockStream.
//!
//! Wraps SlateDB and provides:
//! - Key encoders/decoders with `namespace_id` in all catalog keys
//! - `ShardDb` for per-shard database access
//! - `WriteBatch` builders for atomic epoch commits
//! - `DbReader` for cross-shard snapshot reads
//! - Merge operator registry for associative aggregates
//! - WAL reader utilities
//! - WAL listing cache (hot-path LIST avoidance, DESIGN.md §9.1)
//!
//! No code path depends on range deletion. Cleanup uses
//! scan-and-delete or compaction-filter patterns.

pub mod arrangement_catalog;
pub mod error;
pub mod format_migration;
pub mod keys;
pub mod merge_registry;
pub mod reader;
pub mod shard_db;
pub mod storage_context;
pub mod tiered_store;
pub mod trace;
pub mod wal;
pub mod wal_cache;

pub use arrangement_catalog::{ArrangementCatalog, ArrangementEntry};
pub use error::StorageError;
pub use keys::{
    minmax_sort_key, minmax_sort_key_decode, CatalogKeyEncoder, JoinSide, ShardKeyEncoder,
    ShardPrefix, DISTINCT_DISCRIMINATOR, FORMAT_VERSION_DISCRIMINATOR, MINMAX_DISCRIMINATOR,
};
pub use merge_registry::{MergeOperatorRegistry, SumCountMergeOperator};
pub use reader::ShardReader;
pub use rockstream_types::{EpochStateDelta, OperatorEpochMetrics, StateMutation};
pub use shard_db::{
    is_allow_law_operand_fallback, set_allow_law_operand_fallback, BatchOp, CheckpointHandle,
    PartialAggSpec, ShardDb, WriteBatch,
};
pub use storage_context::{
    BlockCacheKey, SharedStorageContext, StorageCacheStats, WorkerStorageContext,
};
pub use tiered_store::{
    build_migration_object_store, build_runtime_object_store, build_s3_backend_from_config,
    s3_express_build_config, tier_aged_ssts, TieredObjectStore, MAX_TIERING_SCAN_OBJECTS,
};
pub use trace::{SharedArrangementTrace, TraceBatch, TraceManifestHeader, TraceSegmentDescriptor};
pub use wal_cache::WalListingCache;

mod slatedb_metrics;
#[cfg(test)]
mod tests;

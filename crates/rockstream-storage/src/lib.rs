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

pub mod error;
pub mod keys;
pub mod merge_registry;
pub mod reader;
pub mod shard_db;
pub mod wal;
pub mod wal_cache;

pub use error::StorageError;
pub use keys::{
    minmax_sort_key, minmax_sort_key_decode, CatalogKeyEncoder, JoinSide, ShardKeyEncoder,
    ShardPrefix, DISTINCT_DISCRIMINATOR, MINMAX_DISCRIMINATOR,
};
pub use merge_registry::{MergeOperatorRegistry, SumCountMergeOperator};
pub use reader::ShardReader;
pub use shard_db::{
    is_allow_law_operand_fallback, set_allow_law_operand_fallback, BatchOp, CheckpointHandle,
    PartialAggSpec, ShardDb, WriteBatch,
};
pub use wal_cache::WalListingCache;

#[cfg(test)]
mod tests;

//! Worker-side arrangement segment cache keyed by `(shard_id, segment_id)`.
//!
//! Implements the worker-side LRU arrangement segment cache in `rockstream-runtime`,
//! keyed by `(shard_id, segment_id)` with TTL tied to checkpoint epoch.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use rockstream_storage::error::StorageError;
use rockstream_storage::ShardReader;

/// Opaque key identifying a specific arrangement segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardSegmentKey {
    /// Worker-local shard identifier.
    pub shard_id: u32,
    /// Segment identifier within the shard (monotonically increasing).
    pub segment_id: u64,
}

impl ShardSegmentKey {
    pub fn new(shard_id: u32, segment_id: u64) -> Self {
        Self {
            shard_id,
            segment_id,
        }
    }
}

/// Configuration for the per-worker segment cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentCacheConfig {
    /// Maximum number of segments held in the cache at one time.
    pub max_segments: usize,
    /// TTL in epochs.
    pub epoch_ttl: u64,
}

impl Default for SegmentCacheConfig {
    fn default() -> Self {
        Self {
            max_segments: 256,
            epoch_ttl: 5,
        }
    }
}

/// Counters tracking cache efficiency.
#[derive(Debug, Clone, Copy, Default)]
pub struct SegmentCacheStats {
    pub hits: u64,
    pub misses: u64,
}

impl SegmentCacheStats {
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

struct CacheEntry {
    data: Vec<u8>,
    last_accessed: u64,
    created_epoch: u64,
}

/// Per-worker arrangement segment cache with LRU eviction and epoch-based TTL.
pub struct SegmentCache {
    config: SegmentCacheConfig,
    entries: HashMap<ShardSegmentKey, CacheEntry>,
    seq: u64,
    current_epoch: u64,
    pub stats: SegmentCacheStats,
}

impl SegmentCache {
    pub fn new(config: SegmentCacheConfig) -> Self {
        Self {
            config,
            entries: HashMap::with_capacity(config.max_segments),
            seq: 0,
            current_epoch: 0,
            stats: SegmentCacheStats::default(),
        }
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch;
        self.evict_expired();
    }

    pub fn get(&mut self, key: &ShardSegmentKey) -> Option<&[u8]> {
        self.seq += 1;
        let seq = self.seq;

        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_accessed = seq;
            self.stats.hits += 1;
            // Since we return immutable slice reference, let's get it safely.
            Some(entry.data.as_slice())
        } else {
            self.stats.misses += 1;
            None
        }
    }

    pub fn insert(&mut self, key: ShardSegmentKey, data: Vec<u8>) {
        self.seq += 1;
        let seq = self.seq;

        if self.entries.contains_key(&key) {
            let entry = self.entries.get_mut(&key).unwrap();
            entry.data = data;
            entry.last_accessed = seq;
            entry.created_epoch = self.current_epoch;
            return;
        }

        if self.entries.len() >= self.config.max_segments {
            self.evict_lru();
        }

        self.entries.insert(
            key,
            CacheEntry {
                data,
                last_accessed: seq,
                created_epoch: self.current_epoch,
            },
        );
    }

    pub fn invalidate(&mut self, key: &ShardSegmentKey) {
        self.entries.remove(key);
    }

    pub fn invalidate_shard(&mut self, shard_id: u32) {
        self.entries.retain(|k, _| k.shard_id != shard_id);
    }

    fn evict_expired(&mut self) {
        let current = self.current_epoch;
        let ttl = self.config.epoch_ttl;
        self.entries
            .retain(|_, entry| current.saturating_sub(entry.created_epoch) <= ttl);
    }

    fn evict_lru(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        let lru_key = self
            .entries
            .iter()
            .min_by_key(|(_, e)| e.last_accessed)
            .map(|(k, _)| *k)
            .unwrap();
        self.entries.remove(&lru_key);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// A wrapper around `ShardReader` that integrates the `SegmentCache` directly
/// into the read path.
pub struct CachedShardReader {
    pub reader: ShardReader,
    pub cache: Arc<Mutex<SegmentCache>>,
    pub shard_id: u32,
}

impl CachedShardReader {
    pub fn new(reader: ShardReader, cache: Arc<Mutex<SegmentCache>>, shard_id: u32) -> Self {
        Self {
            reader,
            cache,
            shard_id,
        }
    }

    /// Read value for key/segment from the cache, falling back to the reader.
    pub async fn get_cached(
        &self,
        key: &[u8],
        segment_id: u64,
    ) -> Result<Option<Bytes>, StorageError> {
        let cache_key = ShardSegmentKey::new(self.shard_id, segment_id);

        // 1. Try cache lookup
        {
            let mut cache = self.cache.lock().await;
            if let Some(cached_data) = cache.get(&cache_key) {
                return Ok(Some(Bytes::copy_from_slice(cached_data)));
            }
        }

        // 2. Cache miss: read from reader
        let val_opt = self.reader.get(key).await?;
        if let Some(ref val) = val_opt {
            let mut cache = self.cache.lock().await;
            cache.insert(cache_key, val.to_vec());
        }

        Ok(val_opt)
    }
}

//! Worker-Wide Shared Storage Context & Cache Memory Budgets (v0.59.6).
//!
//! Provides a unified, worker-wide storage context (`WorkerStorageContext` / `SharedStorageContext`)
//! managing decoded-block and index caches across shards and views under explicit memory budgets,
//! with strict multi-tenant isolation.

use rockstream_types::ids::{ArrangementId, TenantId};
use rockstream_types::state_budget::{StateBudgetError, WorkerBudgetLedger, WorkerCacheBudget};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Partitioned cache key guaranteeing tenant and security policy isolation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockCacheKey {
    pub tenant_id: TenantId,
    pub security_policy_digest: [u8; 32],
    pub arrangement_id: ArrangementId,
    pub block_id: u64,
}

impl BlockCacheKey {
    pub fn new(
        tenant_id: TenantId,
        security_policy_digest: [u8; 32],
        arrangement_id: ArrangementId,
        block_id: u64,
    ) -> Self {
        Self {
            tenant_id,
            security_policy_digest,
            arrangement_id,
            block_id,
        }
    }
}

/// Statistics and counters for cache operations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub current_bytes: usize,
    pub capacity_bytes: usize,
}

#[derive(Debug)]
struct CacheEntry {
    data: Vec<u8>,
    access_seq: u64,
}

#[derive(Debug)]
struct LruStore {
    entries: HashMap<BlockCacheKey, CacheEntry>,
    current_bytes: usize,
    capacity_bytes: usize,
    access_counter: u64,
}

impl LruStore {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            current_bytes: 0,
            capacity_bytes,
            access_counter: 0,
        }
    }

    fn get(&mut self, key: &BlockCacheKey) -> Option<Vec<u8>> {
        self.access_counter += 1;
        let counter = self.access_counter;
        if let Some(entry) = self.entries.get_mut(key) {
            entry.access_seq = counter;
            Some(entry.data.clone())
        } else {
            None
        }
    }

    fn put(&mut self, key: BlockCacheKey, data: Vec<u8>) -> usize {
        let entry_size = data.len() + std::mem::size_of::<BlockCacheKey>() + 16;
        self.access_counter += 1;

        // If key already exists, subtract old size
        if let Some(old) = self.entries.remove(&key) {
            let old_size = old.data.len() + std::mem::size_of::<BlockCacheKey>() + 16;
            self.current_bytes = self.current_bytes.saturating_sub(old_size);
        }

        let mut evicted_count = 0;
        // Evict LRU entries until under capacity
        while self.current_bytes + entry_size > self.capacity_bytes && !self.entries.is_empty() {
            // Find key with smallest access_seq
            if let Some((oldest_key, _)) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.access_seq)
                .map(|(k, e)| (k.clone(), e.access_seq))
            {
                if let Some(removed) = self.entries.remove(&oldest_key) {
                    let rem_size = removed.data.len() + std::mem::size_of::<BlockCacheKey>() + 16;
                    self.current_bytes = self.current_bytes.saturating_sub(rem_size);
                    evicted_count += 1;
                }
            }
        }

        self.current_bytes += entry_size;
        self.entries.insert(
            key,
            CacheEntry {
                data,
                access_seq: self.access_counter,
            },
        );

        evicted_count
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.current_bytes = 0;
    }
}

use std::path::PathBuf;
use std::sync::Arc;

/// Configuration for a tiered local NVMe block caching layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NvmeCacheConfig {
    pub root_folder: PathBuf,
    pub max_cache_size_bytes: usize,
    pub part_size_bytes: usize,
    pub cache_puts: bool,
}

/// Worker-wide shared storage context managing unified block & index caches.
pub struct WorkerStorageContext {
    heat_cache: Arc<crate::heat_cache::HeatCache>,
    budget_bytes: usize,
    cache_budget: WorkerCacheBudget,
    blocks: Mutex<LruStore>,
    indexes: Mutex<LruStore>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    registered_shards: AtomicUsize,
    db_cache: Arc<dyn slatedb::db_cache::DbCache>,
    nvme_config: Option<NvmeCacheConfig>,
    filter_bits_per_key: Option<u32>,
}

impl std::fmt::Debug for WorkerStorageContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerStorageContext")
            .field("budget_bytes", &self.budget_bytes)
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .field("evictions", &self.evictions.load(Ordering::Relaxed))
            .finish()
    }
}

pub type SharedStorageContext = WorkerStorageContext;

impl WorkerStorageContext {
    /// Create a new worker storage context with the specified memory budget in bytes.
    pub fn new(budget_bytes: usize) -> Self {
        Self::new_with_worker_id("worker-default", budget_bytes)
    }

    /// Create a new worker storage context with explicit worker ID and memory budget in bytes.
    pub fn new_with_worker_id(worker_id: &str, budget_bytes: usize) -> Self {
        // Allocate 70% budget for decoded blocks, 30% for indexes
        let block_budget = (budget_bytes * 7) / 10;
        let index_budget = budget_bytes.saturating_sub(block_budget);
        Self::from_cache_budget(
            worker_id,
            WorkerCacheBudget::new(block_budget, index_budget, 0),
        )
    }

    /// Create a worker storage context with explicitly separated RAM decoded block/meta
    /// caches vs. worker-wide raw object-store disk cache budget.
    pub fn from_cache_budget(worker_id: &str, cache_budget: WorkerCacheBudget) -> Self {
        let heat_budget = (cache_budget.decoded_block_cache_bytes / 8).min(4 * 1024 * 1024);
        let block_budget = cache_budget
            .decoded_block_cache_bytes
            .saturating_sub(heat_budget);
        let index_budget = cache_budget.metadata_index_cache_bytes;
        let budget_bytes = cache_budget.total_ram_bytes();

        let db_cache = crate::slatedb_metrics::instrumented_db_cache_with_capacities(
            worker_id,
            block_budget as u64,
            index_budget as u64,
        );

        let nvme_config = std::env::var_os("ROCKSTREAM_NVME_CACHE_DIR")
            .or_else(|| std::env::var_os("ROCKSTREAM_DISK_CACHE_DIR"))
            .map(|dir| NvmeCacheConfig {
                root_folder: PathBuf::from(dir),
                max_cache_size_bytes: if cache_budget.raw_object_cache_bytes > 0 {
                    cache_budget.raw_object_cache_bytes
                } else {
                    16 * 1024 * 1024 * 1024
                },
                part_size_bytes: 4 * 1024 * 1024,
                cache_puts: true,
            });

        Self {
            budget_bytes,
            heat_cache: Arc::new(crate::heat_cache::HeatCache::new(heat_budget)),
            cache_budget,
            blocks: Mutex::new(LruStore::new(block_budget)),
            indexes: Mutex::new(LruStore::new(index_budget)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            registered_shards: AtomicUsize::new(0),
            db_cache,
            nvme_config,
            filter_bits_per_key: None,
        }
    }

    /// Charge this worker-wide cache budget ONCE to `ledger` (`MemoryCategory::BlockAndMetadataCaches`),
    /// ensuring opening N shards never multiplies cache accounting by N.
    pub fn charge_to_budget_ledger(
        &self,
        ledger: &Arc<WorkerBudgetLedger>,
    ) -> Result<bool, StateBudgetError> {
        ledger.charge_shared_cache_once(self.budget_bytes as u64)
    }

    /// Return the configured split between decoded block, metadata/index, and raw object-store caches.
    pub fn cache_budget(&self) -> WorkerCacheBudget {
        let raw_bytes = self
            .nvme_config
            .as_ref()
            .map(|cfg| cfg.max_cache_size_bytes)
            .unwrap_or(self.cache_budget.raw_object_cache_bytes);
        WorkerCacheBudget {
            decoded_block_cache_bytes: self.cache_budget.decoded_block_cache_bytes,
            metadata_index_cache_bytes: self.cache_budget.metadata_index_cache_bytes,
            raw_object_cache_bytes: raw_bytes,
        }
    }

    /// Register a shard sharing this worker storage context and return the new active shard count.
    pub fn register_shard(&self) -> usize {
        self.registered_shards.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Number of shards currently registered against this shared worker storage context.
    pub fn registered_shards(&self) -> usize {
        self.registered_shards.load(Ordering::Relaxed)
    }

    /// Worker-wide raw object-store cache cap divided across `expected_shards` (at least 1)
    /// so per-shard disk caches never exceed the worker-wide raw cache budget.
    pub fn per_shard_raw_cache_bytes(&self, expected_shards: usize) -> Option<usize> {
        let cfg = self.nvme_config.as_ref()?;
        let divisor = expected_shards.max(1);
        Some(
            (cfg.max_cache_size_bytes / divisor)
                .max(cfg.part_size_bytes.min(cfg.max_cache_size_bytes)),
        )
    }

    /// Configure local NVMe block caching tier for hot SSTable blocks.
    pub fn with_nvme_cache(mut self, dir: impl Into<PathBuf>, max_bytes: usize) -> Self {
        self.nvme_config = Some(NvmeCacheConfig {
            root_folder: dir.into(),
            max_cache_size_bytes: max_bytes,
            part_size_bytes: 4 * 1024 * 1024,
            cache_puts: true,
        });
        self
    }

    /// Configure local NVMe block caching tier with explicit part/block size.
    pub fn with_nvme_block_cache(
        mut self,
        dir: impl Into<PathBuf>,
        max_bytes: usize,
        part_size_bytes: usize,
    ) -> Self {
        self.nvme_config = Some(NvmeCacheConfig {
            root_folder: dir.into(),
            max_cache_size_bytes: max_bytes,
            part_size_bytes,
            cache_puts: true,
        });
        self
    }

    /// Configure default Bloom filter bits per key for arrangements managed under this context.
    pub fn with_filter_bits_per_key(mut self, bits_per_key: u32) -> Self {
        self.filter_bits_per_key = Some(bits_per_key);
        self
    }

    /// Retrieve the configured NVMe block cache config, if any.
    pub fn nvme_config(&self) -> Option<&NvmeCacheConfig> {
        self.nvme_config.as_ref()
    }

    /// Retrieve the configured Bloom filter bits per key, if any.
    pub fn filter_bits_per_key(&self) -> Option<u32> {
        self.filter_bits_per_key
    }

    /// Retrieve the shared SlateDB database cache.
    pub fn db_cache(&self) -> Arc<dyn slatedb::db_cache::DbCache> {
        self.db_cache.clone()
    }

    pub fn heat_cache(&self) -> Arc<crate::heat_cache::HeatCache> {
        self.heat_cache.clone()
    }

    /// Retrieve the total configured budget in bytes.
    pub fn budget_bytes(&self) -> usize {
        self.budget_bytes
    }

    /// Retrieve a decoded block from cache.
    pub fn get_block(&self, key: &BlockCacheKey) -> Option<Vec<u8>> {
        let mut guard = self.blocks.lock().unwrap();
        if let Some(data) = guard.get(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(data)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Put a decoded block into cache under memory budget constraints.
    pub fn put_block(&self, key: BlockCacheKey, block: Vec<u8>) {
        let mut guard = self.blocks.lock().unwrap();
        let evicted = guard.put(key, block);
        if evicted > 0 {
            self.evictions.fetch_add(evicted as u64, Ordering::Relaxed);
        }
    }

    /// Retrieve an index block from cache.
    pub fn get_index(&self, key: &BlockCacheKey) -> Option<Vec<u8>> {
        let mut guard = self.indexes.lock().unwrap();
        if let Some(data) = guard.get(key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(data)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Put an index block into cache.
    pub fn put_index(&self, key: BlockCacheKey, index_data: Vec<u8>) {
        let mut guard = self.indexes.lock().unwrap();
        let evicted = guard.put(key, index_data);
        if evicted > 0 {
            self.evictions.fetch_add(evicted as u64, Ordering::Relaxed);
        }
    }

    /// Return aggregated cache metrics.
    pub fn stats(&self) -> StorageCacheStats {
        let blocks_bytes = self.blocks.lock().unwrap().current_bytes;
        let indexes_bytes = self.indexes.lock().unwrap().current_bytes;

        StorageCacheStats {
            hits: self
                .hits
                .load(Ordering::Relaxed)
                .saturating_add(self.heat_cache.hits()),
            misses: self
                .misses
                .load(Ordering::Relaxed)
                .saturating_add(self.heat_cache.misses()),
            evictions: self.evictions.load(Ordering::Relaxed),
            current_bytes: blocks_bytes + indexes_bytes + self.heat_cache.resident_bytes(),
            capacity_bytes: self.budget_bytes,
        }
    }

    /// Hit ratio helper (0.0 .. 1.0).
    pub fn hit_ratio(&self) -> f64 {
        let h = self
            .hits
            .load(Ordering::Relaxed)
            .saturating_add(self.heat_cache.hits());
        let m = self
            .misses
            .load(Ordering::Relaxed)
            .saturating_add(self.heat_cache.misses());
        let total = h + m;
        if total == 0 {
            0.0
        } else {
            h as f64 / total as f64
        }
    }

    /// Clear all cached blocks and indexes.
    pub fn clear(&self) {
        self.heat_cache.clear();
        self.blocks.lock().unwrap().clear();
        self.indexes.lock().unwrap().clear();
    }
}

//! Worker-Wide Shared Storage Context & Cache Memory Budgets (v0.59.6).
//!
//! Provides a unified, worker-wide storage context (`WorkerStorageContext` / `SharedStorageContext`)
//! managing decoded-block and index caches across shards and views under explicit memory budgets,
//! with strict multi-tenant isolation.

use rockstream_types::ids::{ArrangementId, TenantId};
use rockstream_types::state_budget::{
    MemoryCategory, MemoryOwner, MemoryPermit, StateBudgetError, WorkerBudgetLedger,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

pub const WORKER_DISK_CACHE_CAPACITY_BYTES: usize = 32 * 1024 * 1024 * 1024;
pub const WORKER_SLATEDB_WRITE_BUFFER_CAPACITY_BYTES: usize = 64 * 1024 * 1024;

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

/// Filesystem usage for the local object-store cache, separate from RAM cache stats.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiskCacheStats {
    pub used_bytes: u64,
    pub capacity_bytes: u64,
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
    worker_id: String,
    budget_bytes: usize,
    blocks: Mutex<LruStore>,
    indexes: Mutex<LruStore>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    db_cache: Arc<dyn slatedb::db_cache::DbCache>,
    nvme_config: Option<NvmeCacheConfig>,
    filter_bits_per_key: Option<u32>,
    _block_cache_permit: Option<MemoryPermit>,
    _metadata_cache_permit: Option<MemoryPermit>,
    budget_ledger: Option<std::sync::Arc<WorkerBudgetLedger>>,
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

        let db_cache = crate::slatedb_metrics::instrumented_db_cache_with_capacities(
            worker_id,
            block_budget as u64,
            index_budget as u64,
        );

        let nvme_config = std::env::var_os("ROCKSTREAM_NVME_CACHE_DIR")
            .or_else(|| std::env::var_os("ROCKSTREAM_DISK_CACHE_DIR"))
            .map(|dir| NvmeCacheConfig {
                root_folder: PathBuf::from(dir),
                max_cache_size_bytes: WORKER_DISK_CACHE_CAPACITY_BYTES,
                part_size_bytes: 4 * 1024 * 1024,
                cache_puts: true,
            });

        Self {
            worker_id: worker_id.to_string(),
            budget_bytes,
            blocks: Mutex::new(LruStore::new(block_budget)),
            indexes: Mutex::new(LruStore::new(index_budget)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            db_cache,
            nvme_config,
            filter_bits_per_key: None,
            _block_cache_permit: None,
            _metadata_cache_permit: None,
            budget_ledger: None,
        }
    }

    /// Create the worker cache and prospectively reserve its bounded RAM capacity.
    pub fn new_with_worker_id_and_budget(
        worker_id: &str,
        budget_bytes: usize,
        ledger: &std::sync::Arc<WorkerBudgetLedger>,
    ) -> Result<Self, StateBudgetError> {
        let block_budget = (budget_bytes * 7) / 10;
        let index_budget = budget_bytes.saturating_sub(block_budget);
        let owner = MemoryOwner::worker(worker_id);
        let block_permit = ledger.try_acquire_for_owner(
            MemoryCategory::SlateDbBlockCache,
            owner.clone(),
            block_budget as u64,
            false,
        )?;
        let metadata_permit = ledger.try_acquire_for_owner(
            MemoryCategory::SlateDbMetadataCache,
            owner,
            index_budget as u64,
            false,
        )?;
        let mut context = Self::new_with_worker_id(worker_id, budget_bytes);
        context._block_cache_permit = Some(block_permit);
        context._metadata_cache_permit = Some(metadata_permit);
        context.budget_ledger = Some(ledger.clone());
        Ok(context)
    }

    /// Prospectively reserve one SlateDB instance's bounded unflushed write memory.
    pub fn reserve_slate_db_write_buffers(
        &self,
        bytes: u64,
    ) -> Result<Option<MemoryPermit>, StateBudgetError> {
        self.budget_ledger
            .as_ref()
            .map(|ledger| {
                ledger.try_acquire_for_owner(
                    MemoryCategory::SlateDbWriteBuffers,
                    MemoryOwner::worker(self.worker_id.clone()),
                    bytes,
                    false,
                )
            })
            .transpose()
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

    /// Independently sum regular file bytes under the configured local cache root.
    pub fn disk_cache_stats(&self) -> io::Result<Option<DiskCacheStats>> {
        let Some(config) = &self.nvme_config else {
            return Ok(None);
        };
        Ok(Some(DiskCacheStats {
            used_bytes: directory_file_bytes(&config.root_folder, true)?,
            capacity_bytes: config.max_cache_size_bytes as u64,
        }))
    }

    /// Retrieve the configured Bloom filter bits per key, if any.
    pub fn filter_bits_per_key(&self) -> Option<u32> {
        self.filter_bits_per_key
    }

    /// Retrieve the shared SlateDB database cache.
    pub fn db_cache(&self) -> Arc<dyn slatedb::db_cache::DbCache> {
        self.db_cache.clone()
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
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            current_bytes: blocks_bytes + indexes_bytes,
            capacity_bytes: self.budget_bytes,
        }
    }

    /// Hit ratio helper (0.0 .. 1.0).
    pub fn hit_ratio(&self) -> f64 {
        let h = self.hits.load(Ordering::Relaxed);
        let m = self.misses.load(Ordering::Relaxed);
        let total = h + m;
        if total == 0 {
            0.0
        } else {
            h as f64 / total as f64
        }
    }

    /// Clear all cached blocks and indexes.
    pub fn clear(&self) {
        self.blocks.lock().unwrap().clear();
        self.indexes.lock().unwrap().clear();
    }
}

fn directory_file_bytes(root: &std::path::Path, missing_root_is_empty: bool) -> io::Result<u64> {
    let mut entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if missing_root_is_empty && error.kind() == io::ErrorKind::NotFound => {
            return Ok(0);
        }
        Err(error) => return Err(error),
    };
    entries.try_fold(0_u64, |total, entry| {
        let entry = entry?;
        let kind = entry.file_type()?;
        let bytes = if kind.is_dir() {
            directory_file_bytes(&entry.path(), false)?
        } else if kind.is_file() {
            entry.metadata()?.len()
        } else {
            0
        };
        total.checked_add(bytes).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "disk cache byte total overflow")
        })
    })
}

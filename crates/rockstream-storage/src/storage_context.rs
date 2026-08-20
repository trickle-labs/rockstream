//! Worker-Wide Shared Storage Context & Cache Memory Budgets (v0.59.6).
//!
//! Provides a unified, worker-wide storage context (`WorkerStorageContext` / `SharedStorageContext`)
//! managing decoded-block and index caches across shards and views under explicit memory budgets,
//! with strict multi-tenant isolation.

use rockstream_types::ids::{ArrangementId, TenantId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Worker-wide shared storage context managing unified block & index caches.
#[derive(Debug)]
pub struct WorkerStorageContext {
    budget_bytes: usize,
    blocks: Mutex<LruStore>,
    indexes: Mutex<LruStore>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

pub type SharedStorageContext = WorkerStorageContext;

impl WorkerStorageContext {
    /// Create a new worker storage context with the specified memory budget in bytes.
    pub fn new(budget_bytes: usize) -> Self {
        // Allocate 70% budget for decoded blocks, 30% for indexes
        let block_budget = (budget_bytes * 7) / 10;
        let index_budget = budget_bytes.saturating_sub(block_budget);

        Self {
            budget_bytes,
            blocks: Mutex::new(LruStore::new(block_budget)),
            indexes: Mutex::new(LruStore::new(index_budget)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
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

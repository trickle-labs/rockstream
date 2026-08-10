//! Bounded IVM arrangement with transparent SlateDB (`ShardDb`) spill-to-disk.
//!
//! Exceeding `memory_limit_bytes` evicts cold arrangement entries to SlateDB (`ShardDb`)
//! and transparently faults them back into memory on lookup.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

use rockstream_storage::ShardDb;
use rockstream_types::metrics::{inc_spill_faults_total, inc_spilled_bytes};

use crate::error::OpError;

pub(crate) fn block_on_future<F>(fut: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => std::thread::scope(|s| {
                s.spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(fut)
                })
                .join()
                .unwrap()
            }),
            _ => tokio::task::block_in_place(|| handle.block_on(fut)),
        },
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut),
    }
}

/// Trait for arrangement keys that can be spilled to disk.
pub trait SpillKey: Clone + Eq + Hash + Send + Sync + 'static {
    fn to_spill_bytes(&self) -> Vec<u8>;
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, OpError>;
    fn byte_size(&self) -> usize {
        self.to_spill_bytes().len()
    }
}

/// Trait for arrangement values that can be spilled to disk.
pub trait SpillValue: Clone + Send + Sync + 'static {
    fn to_spill_bytes(&self) -> Vec<u8>;
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, OpError>;
    fn byte_size(&self) -> usize {
        self.to_spill_bytes().len()
    }
}

impl SpillKey for Vec<u8> {
    fn to_spill_bytes(&self) -> Vec<u8> {
        self.clone()
    }
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, OpError> {
        Ok(bytes.to_vec())
    }
    fn byte_size(&self) -> usize {
        self.len()
    }
}

impl SpillValue for Vec<u8> {
    fn to_spill_bytes(&self) -> Vec<u8> {
        self.clone()
    }
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, OpError> {
        Ok(bytes.to_vec())
    }
    fn byte_size(&self) -> usize {
        self.len()
    }
}

impl SpillKey for i64 {
    fn to_spill_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, OpError> {
        if bytes.len() == 8 {
            Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
        } else {
            Err(OpError::storage_error(format!(
                "invalid i64 bytes len {}",
                bytes.len()
            )))
        }
    }
    fn byte_size(&self) -> usize {
        8
    }
}

impl SpillValue for i64 {
    fn to_spill_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, OpError> {
        if bytes.len() == 8 {
            Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
        } else {
            Err(OpError::storage_error(format!(
                "invalid i64 bytes len {}",
                bytes.len()
            )))
        }
    }
    fn byte_size(&self) -> usize {
        8
    }
}

impl SpillKey for u128 {
    fn to_spill_bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, OpError> {
        if bytes.len() == 16 {
            Ok(u128::from_be_bytes(bytes.try_into().unwrap()))
        } else {
            Err(OpError::storage_error(format!(
                "invalid u128 bytes len {}",
                bytes.len()
            )))
        }
    }
    fn byte_size(&self) -> usize {
        16
    }
}

/// A wrapper for types that use `serde_json` for spill serialization.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SerdeSpill<T>(pub T);

impl<
        T: serde::Serialize + serde::de::DeserializeOwned + Clone + Eq + Hash + Send + Sync + 'static,
    > SpillKey for SerdeSpill<T>
{
    fn to_spill_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.0).unwrap()
    }
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, OpError> {
        serde_json::from_slice(bytes)
            .map(SerdeSpill)
            .map_err(|e| OpError::storage_error(format!("serde spill key decode err: {e}")))
    }
}

impl<T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static> SpillValue
    for SerdeSpill<T>
{
    fn to_spill_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.0).unwrap()
    }
    fn from_spill_bytes(bytes: &[u8]) -> Result<Self, OpError> {
        serde_json::from_slice(bytes)
            .map(SerdeSpill)
            .map_err(|e| OpError::storage_error(format!("serde spill val decode err: {e}")))
    }
}

/// A generic bounded in-memory arrangement backed by SlateDB (`ShardDb`) cold storage.
pub struct SpillableArrangement<K: SpillKey, V: SpillValue> {
    db: Option<Arc<ShardDb>>,
    prefix: Vec<u8>,
    memory_limit_bytes: usize,
    in_memory_bytes: usize,
    spilled_bytes: u64,
    in_memory: HashMap<K, V>,
    access_queue: VecDeque<K>,
    spilled_keys: HashSet<K>,
}

impl<K: SpillKey + std::fmt::Debug, V: SpillValue + std::fmt::Debug> std::fmt::Debug
    for SpillableArrangement<K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillableArrangement")
            .field("has_db", &self.db.is_some())
            .field("prefix", &self.prefix)
            .field("memory_limit_bytes", &self.memory_limit_bytes)
            .field("in_memory_bytes", &self.in_memory_bytes)
            .field("spilled_bytes", &self.spilled_bytes)
            .field("in_memory", &self.in_memory)
            .field("spilled_keys", &self.spilled_keys)
            .finish()
    }
}

impl<K: SpillKey, V: SpillValue> SpillableArrangement<K, V> {
    pub fn new(db: Option<Arc<ShardDb>>, prefix: Vec<u8>, memory_limit_bytes: usize) -> Self {
        let memory_limit_bytes = if memory_limit_bytes == 0 {
            usize::MAX
        } else {
            memory_limit_bytes
        };
        Self {
            db,
            prefix,
            memory_limit_bytes,
            in_memory_bytes: 0,
            spilled_bytes: 0,
            in_memory: HashMap::new(),
            access_queue: VecDeque::new(),
            spilled_keys: HashSet::new(),
        }
    }

    pub fn db(&self) -> Option<&Arc<ShardDb>> {
        self.db.as_ref()
    }

    pub fn set_db(&mut self, db: Arc<ShardDb>) {
        self.db = Some(db);
    }

    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    pub fn set_memory_limit(&mut self, limit_bytes: usize) {
        self.memory_limit_bytes = if limit_bytes == 0 {
            usize::MAX
        } else {
            limit_bytes
        };
        self.evict_if_needed().unwrap();
    }

    pub fn state_bytes(&self) -> u64 {
        self.in_memory_bytes as u64
    }

    pub fn spilled_bytes(&self) -> u64 {
        self.spilled_bytes
    }

    pub fn in_memory_entry_count(&self) -> usize {
        self.in_memory.len()
    }

    pub fn spilled_entry_count(&self) -> usize {
        self.spilled_keys.len()
    }

    pub fn total_entry_count(&self) -> usize {
        self.in_memory.len() + self.spilled_keys.len()
    }

    fn entry_bytes(key: &K, val: &V) -> usize {
        key.byte_size() + val.byte_size()
    }

    fn make_db_key(&self, key: &K) -> Vec<u8> {
        let mut k_bytes = self.prefix.clone();
        k_bytes.extend_from_slice(&key.to_spill_bytes());
        k_bytes
    }

    pub fn insert(&mut self, key: K, val: V) -> Result<Option<V>, OpError> {
        let entry_sz = Self::entry_bytes(&key, &val);

        let mut old_val = None;
        if let Some(old) = self.in_memory.insert(key.clone(), val) {
            let old_sz = Self::entry_bytes(&key, &old);
            self.in_memory_bytes = self.in_memory_bytes.saturating_sub(old_sz);
            self.in_memory_bytes += entry_sz;
            old_val = Some(old);
        } else {
            self.in_memory_bytes += entry_sz;
        }

        self.access_queue.push_back(key.clone());

        if self.spilled_keys.remove(&key) {
            if let Some(db) = &self.db {
                let db_key = self.make_db_key(&key);
                block_on_future(db.delete(&db_key))
                    .map_err(|e| OpError::storage_error(format!("spill delete err: {e}")))?;
            }
        }

        self.evict_if_needed()?;
        Ok(old_val)
    }

    pub fn get(&mut self, key: &K) -> Result<Option<V>, OpError> {
        if let Some(val) = self.in_memory.get(key) {
            let val = val.clone();
            self.access_queue.push_back(key.clone());
            return Ok(Some(val));
        }

        let is_spilled = self.spilled_keys.contains(key);
        if is_spilled || self.db.is_some() {
            if let Some(db) = &self.db {
                let db_key = self.make_db_key(key);
                let res = block_on_future(db.get(&db_key))
                    .map_err(|e| OpError::storage_error(format!("spill get err: {e}")))?;
                if let Some(v_bytes) = res {
                    let val = V::from_spill_bytes(&v_bytes)?;
                    block_on_future(db.delete(&db_key))
                        .map_err(|e| OpError::storage_error(format!("spill delete err: {e}")))?;
                    self.spilled_keys.remove(key);
                    inc_spill_faults_total();

                    let entry_sz = Self::entry_bytes(key, &val);
                    self.in_memory.insert(key.clone(), val.clone());
                    self.in_memory_bytes += entry_sz;
                    self.access_queue.push_back(key.clone());
                    self.evict_if_needed()?;
                    return Ok(Some(val));
                }
            }
        }

        Ok(None)
    }

    pub fn remove(&mut self, key: &K) -> Result<Option<V>, OpError> {
        if let Some(old) = self.in_memory.remove(key) {
            let old_sz = Self::entry_bytes(key, &old);
            self.in_memory_bytes = self.in_memory_bytes.saturating_sub(old_sz);
            if self.spilled_keys.remove(key) {
                if let Some(db) = &self.db {
                    let db_key = self.make_db_key(key);
                    block_on_future(db.delete(&db_key))
                        .map_err(|e| OpError::storage_error(format!("spill delete err: {e}")))?;
                }
            }
            return Ok(Some(old));
        }

        if self.spilled_keys.contains(key) || self.db.is_some() {
            if let Some(db) = &self.db {
                let db_key = self.make_db_key(key);
                let res = block_on_future(db.get(&db_key))
                    .map_err(|e| OpError::storage_error(format!("spill get err: {e}")))?;
                if let Some(v_bytes) = res {
                    let val = V::from_spill_bytes(&v_bytes)?;
                    block_on_future(db.delete(&db_key))
                        .map_err(|e| OpError::storage_error(format!("spill delete err: {e}")))?;
                    self.spilled_keys.remove(key);
                    return Ok(Some(val));
                }
            }
        }

        Ok(None)
    }

    pub fn contains_key(&mut self, key: &K) -> Result<bool, OpError> {
        if self.in_memory.contains_key(key) || self.spilled_keys.contains(key) {
            return Ok(true);
        }
        if let Some(db) = &self.db {
            let db_key = self.make_db_key(key);
            let res = block_on_future(db.get(&db_key))
                .map_err(|e| OpError::storage_error(format!("spill get err: {e}")))?;
            if res.is_some() {
                self.spilled_keys.insert(key.clone());
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn scan_all(&mut self) -> Result<Vec<(K, V)>, OpError> {
        let mut results = Vec::new();
        let mut seen_keys = HashSet::new();

        for (k, v) in &self.in_memory {
            results.push((k.clone(), v.clone()));
            seen_keys.insert(k.clone());
        }

        if let Some(db) = &self.db {
            let raw_pairs = block_on_future(db.scan_prefix(&self.prefix))
                .map_err(|e| OpError::storage_error(format!("spill scan err: {e}")))?;
            let prefix_len = self.prefix.len();
            for (k_buf, v_buf) in raw_pairs {
                if k_buf.len() < prefix_len {
                    continue;
                }
                let k_bytes = &k_buf[prefix_len..];
                if let Ok(key) = K::from_spill_bytes(k_bytes) {
                    if !seen_keys.contains(&key) {
                        if let Ok(val) = V::from_spill_bytes(&v_buf) {
                            seen_keys.insert(key.clone());
                            results.push((key, val));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn populate_spilled_keys_from_db(&mut self) -> Result<(), OpError> {
        if let Some(db) = &self.db {
            let raw_pairs = block_on_future(db.scan_prefix(&self.prefix))
                .map_err(|e| OpError::storage_error(format!("spill scan err: {e}")))?;
            let prefix_len = self.prefix.len();
            for (k_buf, _) in raw_pairs {
                if k_buf.len() >= prefix_len {
                    let k_bytes = &k_buf[prefix_len..];
                    if let Ok(key) = K::from_spill_bytes(k_bytes) {
                        if !self.in_memory.contains_key(&key) {
                            self.spilled_keys.insert(key);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn evict_if_needed(&mut self) -> Result<(), OpError> {
        while self.in_memory_bytes > self.memory_limit_bytes && self.in_memory.len() > 1 {
            let cold_key = match self.access_queue.pop_front() {
                Some(k) => k,
                None => break,
            };

            if let Some(cold_val) = self.in_memory.remove(&cold_key) {
                let sz = Self::entry_bytes(&cold_key, &cold_val);
                self.in_memory_bytes = self.in_memory_bytes.saturating_sub(sz);

                if let Some(db) = &self.db {
                    let k_bytes = cold_key.to_spill_bytes();
                    let v_bytes = cold_val.to_spill_bytes();
                    let db_key = self.make_db_key(&cold_key);
                    block_on_future(db.put(&db_key, &v_bytes))
                        .map_err(|e| OpError::storage_error(format!("spill put err: {e}")))?;
                    self.spilled_keys.insert(cold_key);
                    let spilled_sz = (k_bytes.len() + v_bytes.len()) as u64;
                    self.spilled_bytes += spilled_sz;
                    inc_spilled_bytes(spilled_sz);
                }
            }
        }
        Ok(())
    }
}

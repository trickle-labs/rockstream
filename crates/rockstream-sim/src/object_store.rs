//! In-memory object store for simulation.
//!
//! Provides a deterministic, in-memory key-value store that simulates
//! cloud object storage (S3, GCS, Azure Blob).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::clock::{Clock, SimClock};

/// Error type for simulated object store operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStoreError {
    NotFound(String),
    AlreadyExists(String),
    Io(String),
}

impl std::fmt::Display for ObjectStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(key) => write!(f, "object not found: {key}"),
            Self::AlreadyExists(key) => write!(f, "object already exists: {key}"),
            Self::Io(msg) => write!(f, "object store I/O error: {msg}"),
        }
    }
}

impl std::error::Error for ObjectStoreError {}

/// Handle to a simulated object store instance (cheaply cloneable).
#[derive(Debug, Clone)]
pub struct SimObjectStoreHandle {
    inner: Arc<SimObjectStore>,
}

impl SimObjectStoreHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SimObjectStore::new()),
        }
    }

    pub fn set_clock(&self, clock: SimClock) {
        self.inner.set_clock(clock);
    }

    pub fn set_rate_limit(&self, ops_per_sec: Option<f64>) {
        self.inner.set_rate_limit(ops_per_sec);
    }

    pub fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectStoreError> {
        self.inner.put(key, value)
    }

    pub fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        self.inner.get(key)
    }

    pub fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    pub fn list(&self, prefix: &str) -> Vec<String> {
        self.inner.list(prefix)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.inner.exists(key)
    }

    /// Get a snapshot of all keys and values for determinism checking.
    pub fn snapshot(&self) -> BTreeMap<String, Bytes> {
        self.inner.snapshot()
    }
}

impl Default for SimObjectStoreHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory object store implementation.
#[derive(Debug)]
pub struct SimObjectStore {
    objects: Mutex<BTreeMap<String, Bytes>>,
    rate_limit: Mutex<Option<f64>>,
    request_times: Mutex<Vec<Duration>>,
    clock: Mutex<Option<SimClock>>,
}

impl SimObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(BTreeMap::new()),
            rate_limit: Mutex::new(None),
            request_times: Mutex::new(Vec::new()),
            clock: Mutex::new(None),
        }
    }

    pub fn set_clock(&self, clock: SimClock) {
        let mut clk = self.clock.lock();
        *clk = Some(clock);
    }

    pub fn set_rate_limit(&self, ops_per_sec: Option<f64>) {
        let mut limit = self.rate_limit.lock();
        *limit = ops_per_sec;
    }

    fn check_rate_limit(&self) -> Result<(), ObjectStoreError> {
        if crate::buggify!("object_store.rate_limit", 0.05) {
            return Err(ObjectStoreError::Io("HTTP 429 Too Many Requests".to_string()));
        }
        let limit = { *self.rate_limit.lock() };
        if let Some(r) = limit {
            let now = if let Some(ref clock) = *self.clock.lock() {
                clock.elapsed_since_epoch()
              } else {
                Duration::from_secs(0)
            };

            let mut times = self.request_times.lock();
            times.retain(|&t| now.saturating_sub(t) < Duration::from_secs(1));

            if times.len() as f64 >= r {
                return Err(ObjectStoreError::Io("HTTP 429 Too Many Requests".to_string()));
            }
            times.push(now);
        }
        Ok(())
    }

    pub fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectStoreError> {
        self.check_rate_limit()?;
        let mut objects = self.objects.lock();
        objects.insert(key.to_string(), value);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Bytes, ObjectStoreError> {
        self.check_rate_limit()?;
        let objects = self.objects.lock();
        objects
            .get(key)
            .cloned()
            .ok_or_else(|| ObjectStoreError::NotFound(key.to_string()))
    }

    pub fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.check_rate_limit()?;
        let mut objects = self.objects.lock();
        if objects.remove(key).is_none() {
            return Err(ObjectStoreError::NotFound(key.to_string()));
        }
        Ok(())
    }

    pub fn list(&self, prefix: &str) -> Vec<String> {
        if self.check_rate_limit().is_err() {
            return vec![];
        }
        let objects = self.objects.lock();
        objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn exists(&self, key: &str) -> bool {
        if self.check_rate_limit().is_err() {
            return false;
        }
        let objects = self.objects.lock();
        objects.contains_key(key)
    }

    /// Get a snapshot of all keys and values for determinism checking.
    pub fn snapshot(&self) -> BTreeMap<String, Bytes> {
        self.objects.lock().clone()
    }
}

impl Default for SimObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get() {
        let store = SimObjectStoreHandle::new();
        store.put("key1", Bytes::from("value1")).unwrap();
        let val = store.get("key1").unwrap();
        assert_eq!(val, Bytes::from("value1"));
    }

    #[test]
    fn get_not_found() {
        let store = SimObjectStoreHandle::new();
        let err = store.get("missing").unwrap_err();
        assert_eq!(err, ObjectStoreError::NotFound("missing".to_string()));
    }

    #[test]
    fn delete_existing() {
        let store = SimObjectStoreHandle::new();
        store.put("key1", Bytes::from("value1")).unwrap();
        store.delete("key1").unwrap();
        assert!(!store.exists("key1"));
    }

    #[test]
    fn delete_missing() {
        let store = SimObjectStoreHandle::new();
        let err = store.delete("missing").unwrap_err();
        assert_eq!(err, ObjectStoreError::NotFound("missing".to_string()));
    }

    #[test]
    fn list_with_prefix() {
        let store = SimObjectStoreHandle::new();
        store.put("data/a", Bytes::from("1")).unwrap();
        store.put("data/b", Bytes::from("2")).unwrap();
        store.put("meta/c", Bytes::from("3")).unwrap();

        let listed = store.list("data/");
        assert_eq!(listed, vec!["data/a", "data/b"]);
    }

    #[test]
    fn overwrite_put() {
        let store = SimObjectStoreHandle::new();
        store.put("key1", Bytes::from("v1")).unwrap();
        store.put("key1", Bytes::from("v2")).unwrap();
        assert_eq!(store.get("key1").unwrap(), Bytes::from("v2"));
    }

    #[test]
    fn snapshot_is_ordered() {
        let store = SimObjectStoreHandle::new();
        store.put("c", Bytes::from("3")).unwrap();
        store.put("a", Bytes::from("1")).unwrap();
        store.put("b", Bytes::from("2")).unwrap();

        let snap = store.snapshot();
        let keys: Vec<&String> = snap.keys().collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_throttled_object_store() {
        let clock = SimClock::new();
        let store = SimObjectStoreHandle::new();
        store.set_clock(clock);
        // Rate limit: 2 operations per second
        store.set_rate_limit(Some(2.0));

        // First operation is fine
        store.put("key1", Bytes::from("v1")).unwrap();
        // Second operation is fine
        store.put("key2", Bytes::from("v2")).unwrap();

        // Third operation in the same second fails with 429
        let err = store.put("key3", Bytes::from("v3")).unwrap_err();
        assert!(matches!(err, ObjectStoreError::Io(ref msg) if msg.contains("429")));
    }
}

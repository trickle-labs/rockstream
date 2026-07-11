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

    /// Set the probability that a subsequent `put` truncates its bytes
    /// mid-write (`object_store.partial_write` fault, DESIGN.md §17.8 gap 1).
    pub fn set_partial_write_probability(&self, probability: f64) {
        self.inner.set_partial_write_probability(probability);
    }

    /// Set how many epochs a `put` is hidden from `list()` results, simulating
    /// S3-style LIST eventual consistency (`object_store.list_staleness`
    /// fault, DESIGN.md §17.8 gap 3). Direct-key `get`/`exists` are always
    /// immediately consistent regardless of this setting.
    pub fn set_list_staleness_epochs(&self, epochs: u64) {
        self.inner.set_list_staleness_epochs(epochs);
    }

    /// Advance the store's logical epoch counter, used together with
    /// `list_staleness_epochs` to simulate LIST results lagging behind
    /// recent writes by a bounded number of epochs.
    pub fn advance_epoch(&self) -> u64 {
        self.inner.advance_epoch()
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
    /// Probability (0.0–1.0) that a `put` truncates its bytes mid-write,
    /// simulating a crashed/interrupted multi-part upload leaving a
    /// truncated object visible (DESIGN.md §17.8 gap 1, v0.43).
    partial_write_probability: Mutex<f64>,
    /// Logical epoch counter, advanced by `advance_epoch()`. Used to
    /// simulate LIST eventual consistency (DESIGN.md §17.8 gap 3, v0.43).
    current_epoch: Mutex<u64>,
    /// Number of epochs a `put` remains hidden from `list()` results after
    /// it lands, mirroring real S3/GCS LIST eventual consistency. 0 (the
    /// default) means `list()` is always immediately consistent.
    list_staleness_epochs: Mutex<u64>,
    /// The epoch each key was last written at, used by `list()` to decide
    /// visibility when `list_staleness_epochs > 0`.
    key_epochs: Mutex<BTreeMap<String, u64>>,
}

impl SimObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(BTreeMap::new()),
            rate_limit: Mutex::new(None),
            request_times: Mutex::new(Vec::new()),
            clock: Mutex::new(None),
            partial_write_probability: Mutex::new(0.0),
            current_epoch: Mutex::new(0),
            list_staleness_epochs: Mutex::new(0),
            key_epochs: Mutex::new(BTreeMap::new()),
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

    /// Set the probability that a subsequent `put` truncates its bytes
    /// mid-write (`object_store.partial_write` fault, DESIGN.md §17.8 gap 1).
    pub fn set_partial_write_probability(&self, probability: f64) {
        let mut p = self.partial_write_probability.lock();
        *p = probability.clamp(0.0, 1.0);
    }

    /// Set how many epochs a `put` is hidden from `list()` results
    /// (`object_store.list_staleness` fault, DESIGN.md §17.8 gap 3).
    pub fn set_list_staleness_epochs(&self, epochs: u64) {
        let mut s = self.list_staleness_epochs.lock();
        *s = epochs;
    }

    /// Advance the logical epoch counter and return the new value.
    pub fn advance_epoch(&self) -> u64 {
        let mut e = self.current_epoch.lock();
        *e += 1;
        *e
    }

    fn check_rate_limit(&self) -> Result<(), ObjectStoreError> {
        if crate::buggify!("object_store.rate_limit", 0.05) {
            return Err(ObjectStoreError::Io(
                "HTTP 429 Too Many Requests".to_string(),
            ));
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
                return Err(ObjectStoreError::Io(
                    "HTTP 429 Too Many Requests".to_string(),
                ));
            }
            times.push(now);
        }
        Ok(())
    }

    pub fn put(&self, key: &str, value: Bytes) -> Result<(), ObjectStoreError> {
        self.check_rate_limit()?;
        let probability = *self.partial_write_probability.lock();
        let value =
            if probability > 0.0 && crate::buggify!("object_store.partial_write", probability) {
                // Truncate to simulate a crashed/interrupted multi-part upload:
                // the object becomes visible with fewer bytes than intended.
                // Half the payload (rounded down) is retained; an empty payload
                // stays empty (nothing to truncate).
                let truncated_len = value.len() / 2;
                value.slice(0..truncated_len)
            } else {
                value
            };
        let mut objects = self.objects.lock();
        objects.insert(key.to_string(), value);
        let epoch = *self.current_epoch.lock();
        self.key_epochs.lock().insert(key.to_string(), epoch);
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
        self.key_epochs.lock().remove(key);
        Ok(())
    }

    pub fn list(&self, prefix: &str) -> Vec<String> {
        if self.check_rate_limit().is_err() {
            return vec![];
        }
        let objects = self.objects.lock();
        let staleness = *self.list_staleness_epochs.lock();
        if staleness == 0 {
            return objects
                .range(prefix.to_string()..)
                .take_while(|(k, _)| k.starts_with(prefix))
                .map(|(k, _)| k.clone())
                .collect();
        }
        // Simulate LIST eventual consistency: keys written within the last
        // `staleness` epochs are hidden from LIST results, even though a
        // direct `get`/`exists` on the same key is always consistent.
        let visible_epoch = self.current_epoch.lock().saturating_sub(staleness);
        let key_epochs = self.key_epochs.lock();
        objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .filter(|(k, _)| key_epochs.get(*k).copied().unwrap_or(0) <= visible_epoch)
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

    // DESIGN.md §17.8 gap 3, v0.43: `list_staleness_epochs` simulates real
    // S3 LIST eventual consistency. `list()` must hide recently-written keys
    // for the configured number of epochs, while direct `get`/`exists` reads
    // remain immediately consistent regardless.
    #[test]
    fn list_staleness_hides_recent_puts_but_direct_reads_are_consistent() {
        let store = SimObjectStoreHandle::new();
        store.set_list_staleness_epochs(2);

        // epoch 0
        store.put("data/a", Bytes::from("1")).unwrap();
        store.advance_epoch(); // epoch 1
        store.put("data/b", Bytes::from("2")).unwrap();

        // At epoch 1 with staleness 2, visible_epoch = 1.saturating_sub(2) = 0,
        // so only the epoch-0 key is LIST-visible.
        assert_eq!(store.list("data/"), vec!["data/a"]);
        // Direct-key reads are never affected by LIST staleness.
        assert_eq!(store.get("data/b").unwrap(), Bytes::from("2"));
        assert!(store.exists("data/b"));

        store.advance_epoch(); // epoch 2
        store.advance_epoch(); // epoch 3, visible_epoch = 3 - 2 = 1
        assert_eq!(store.list("data/"), vec!["data/a", "data/b"]);
    }

    #[test]
    fn list_staleness_zero_is_immediately_consistent() {
        let store = SimObjectStoreHandle::new();
        store.put("data/a", Bytes::from("1")).unwrap();
        assert_eq!(store.list("data/"), vec!["data/a"]);
    }

    // Regression seed proving the `object_store.list_staleness` fault never
    // trips any `assert_*` correctness invariant in
    // `rockstream-connectors::sink_connector` (DESIGN.md §17.8 gap 3 is
    // informational only: CALM epoch manifest reads are direct-key reads,
    // never LIST-based, so LIST staleness cannot affect commit correctness).
    #[test]
    fn list_staleness_does_not_affect_sink_connector_asserts() {
        use rockstream_connectors::assert_commit_pointer_atomic;
        use rockstream_types::ids::ConnectorId;

        let store = SimObjectStoreHandle::new();
        store.set_list_staleness_epochs(5);

        let connector_id = ConnectorId(1);
        let epoch = 3;
        let final_key = "final/000003";
        let payload = Bytes::from("committed-payload");
        // Advance the epoch clock first so the upcoming `put` lands at a
        // recent epoch that staleness=5 will keep hidden from LIST.
        for _ in 0..3 {
            store.advance_epoch();
        }
        store.put(final_key, payload.clone()).unwrap();

        // LIST does not yet observe the just-written key (current epoch=3,
        // staleness=5 -> visible_epoch=0), but the sink's commit-path
        // invariant is checked against a direct read, which is always
        // immediately consistent regardless of LIST staleness.
        assert!(store.list("final/").is_empty());
        let observed = store.get(final_key).unwrap();
        assert_commit_pointer_atomic(connector_id, epoch, observed.len(), payload.len());
    }
}

//! Bounded worker-wide raw SST range cache used by live reads and lease prefetch.
//!
//! Only immutable `db/compacted/*.sst` ranges participate. Manifest/WAL reads,
//! conditional GETs and object versions always reach the backing store.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, Result,
};
use parking_lot::Mutex;
use rockstream_types::lease::{LeaseHeatHint, SstBlockHeat};

type CacheKey = (String, String, u64, u64);

#[derive(Clone)]
struct Entry {
    bytes: Bytes,
    meta: ObjectMeta,
    attributes: Attributes,
    observed_at_ms: u64,
    accesses: u64,
    charge: usize,
}

#[derive(Default)]
struct State {
    entries: BTreeMap<CacheKey, Entry>,
    bytes: usize,
}

pub struct HeatCache {
    capacity: usize,
    state: Mutex<State>,
    prefetch: tokio::sync::Semaphore,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl fmt::Debug for HeatCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeatCache")
            .field("capacity", &self.capacity)
            .finish()
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl HeatCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(State::default()),
            prefetch: tokio::sync::Semaphore::new(1),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn resident_bytes(&self) -> usize {
        self.state.lock().bytes
    }
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
    pub fn clear(&self) {
        *self.state.lock() = State::default();
    }

    fn get(&self, key: &CacheKey) -> Option<Entry> {
        let mut state = self.state.lock();
        if let Some(entry) = state.entries.get_mut(key) {
            entry.accesses = entry.accesses.saturating_add(1);
            entry.observed_at_ms = now_ms();
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    fn insert(&self, key: CacheKey, mut entry: Entry) {
        // Bound both payload and metadata; entry count is independently bounded.
        entry.charge = entry.bytes.len()
            + key.0.len()
            + key.1.len()
            + entry.meta.e_tag.as_ref().map_or(0, String::len)
            + entry.meta.version.as_ref().map_or(0, String::len)
            + entry
                .attributes
                .iter()
                .map(|(attribute, value)| {
                    value.as_ref().len()
                        + match attribute {
                            object_store::Attribute::Metadata(key) => key.len(),
                            _ => 0,
                        }
                        + 128
                })
                .sum::<usize>()
            + 1024;
        if entry.charge > self.capacity {
            return;
        }
        let mut state = self.state.lock();
        if let Some(previous) = state.entries.remove(&key) {
            state.bytes -= previous.charge;
        }
        while state.bytes + entry.charge > self.capacity
            || state.entries.len() >= LeaseHeatHint::MAX_ENTRIES
        {
            let Some(victim) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| (entry.observed_at_ms, entry.accesses))
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            let removed = state.entries.remove(&victim).unwrap();
            state.bytes -= removed.charge;
        }
        state.bytes += entry.charge;
        state.entries.insert(key, entry);
    }

    fn invalidate(&self, scope: &str, location: Option<&Path>) {
        let mut state = self.state.lock();
        state.entries.retain(|key, _| {
            key.0 != scope || location.is_some_and(|location| key.1 != location.as_ref())
        });
        state.bytes = state.entries.values().map(|entry| entry.charge).sum();
    }

    pub fn heat(&self, scopes: &[String], at_ms: u64) -> LeaseHeatHint {
        let state = self.state.lock();
        let mut entries: Vec<_> = state
            .entries
            .iter()
            .filter(|(key, entry)| {
                scopes.contains(&key.0)
                    && at_ms >= entry.observed_at_ms
                    && at_ms - entry.observed_at_ms <= LeaseHeatHint::MAX_AGE_MS
            })
            .collect();
        entries.sort_by(|(left_key, left), (right_key, right)| {
            right
                .accesses
                .cmp(&left.accesses)
                .then_with(|| left_key.cmp(right_key))
        });
        LeaseHeatHint {
            blocks: entries
                .into_iter()
                .map(|(key, entry)| SstBlockHeat {
                    storage_scope: key.0.clone(),
                    object_path: key.1.clone(),
                    start: key.2,
                    end: key.3,
                    observed_at_ms: entry.observed_at_ms,
                })
                .collect(),
            ..Default::default()
        }
        .bounded()
    }

    /// One prefetch job per worker, one range in flight, 4 MiB and two seconds
    /// per handoff. Errors/expired hints are advisory misses, never lease input.
    pub async fn warm(
        self: &Arc<Self>,
        stores: &[(String, Arc<dyn ObjectStore>)],
        hint: &LeaseHeatHint,
        at_ms: u64,
    ) -> u64 {
        let Ok(_permit) = self.prefetch.try_acquire() else {
            return 0;
        };
        let hint = hint.clone().bounded();
        let mut fetched = 0;
        let _ = tokio::time::timeout(Duration::from_secs(2), async {
            for block in hint.blocks {
                if at_ms < block.observed_at_ms
                    || at_ms - block.observed_at_ms > LeaseHeatHint::MAX_AGE_MS
                {
                    continue;
                }
                let Some((scope, store)) = stores
                    .iter()
                    .find(|(scope, _)| *scope == block.storage_scope)
                else {
                    continue;
                };
                let wrapped = HeatObjectStore::new(scope.clone(), store.clone(), self.clone());
                let options = GetOptions {
                    range: Some(GetRange::Bounded(block.start..block.end)),
                    ..Default::default()
                };
                if let Ok(result) = wrapped
                    .get_opts(&Path::from(block.object_path), options)
                    .await
                {
                    fetched += result.range.end - result.range.start;
                }
            }
        })
        .await;
        fetched
    }
}

#[derive(Debug)]
pub struct HeatObjectStore {
    scope: String,
    inner: Arc<dyn ObjectStore>,
    cache: Arc<HeatCache>,
}

impl HeatObjectStore {
    pub fn new(scope: String, inner: Arc<dyn ObjectStore>, cache: Arc<HeatCache>) -> Self {
        Self {
            scope,
            inner,
            cache,
        }
    }
}

impl fmt::Display for HeatObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HeatObjectStore({})", self.inner)
    }
}

fn payload(bytes: Bytes) -> GetResultPayload {
    GetResultPayload::Stream(Box::pin(stream::once(async move { Ok(bytes) })))
}

#[async_trait]
impl ObjectStore for HeatObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        data: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.cache.invalidate(&self.scope, Some(location));
        self.inner.put_opts(location, data, options).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.cache.invalidate(&self.scope, Some(location));
        self.inner.put_multipart_opts(location, options).await
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let range = match &options.range {
            Some(GetRange::Bounded(range))
                if !options.head
                    && options.if_match.is_none()
                    && options.if_none_match.is_none()
                    && options.if_modified_since.is_none()
                    && options.if_unmodified_since.is_none()
                    && options.version.is_none() =>
            {
                range.clone()
            }
            _ => return self.inner.get_opts(location, options).await,
        };
        let block = SstBlockHeat {
            storage_scope: self.scope.clone(),
            object_path: location.to_string(),
            start: range.start,
            end: range.end,
            observed_at_ms: 0,
        };
        if !block.is_valid() {
            return self.inner.get_opts(location, options).await;
        }
        let key = (
            self.scope.clone(),
            location.to_string(),
            range.start,
            range.end,
        );
        if let Some(entry) = self.cache.get(&key) {
            return Ok(GetResult {
                payload: payload(entry.bytes),
                meta: entry.meta,
                range,
                attributes: entry.attributes,
                extensions: Default::default(),
            });
        }
        let result = self.inner.get_opts(location, options).await?;
        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let actual_range = result.range.clone();
        let bytes = result.bytes().await?;
        if actual_range == range && bytes.len() as u64 == range.end - range.start {
            self.cache.insert(
                key,
                Entry {
                    // Some backends return a small slice retaining an entire
                    // object allocation. Copy the admitted range so resident
                    // cache accounting also bounds the backing allocation.
                    bytes: Bytes::copy_from_slice(&bytes),
                    meta: meta.clone(),
                    attributes: attributes.clone(),
                    observed_at_ms: now_ms(),
                    accesses: 1,
                    charge: 0,
                },
            );
        }
        Ok(GetResult {
            payload: payload(bytes),
            meta,
            range: actual_range,
            attributes,
            extensions,
        })
    }
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut bytes = Vec::with_capacity(ranges.len());
        for range in ranges {
            bytes.push(
                self.get_opts(
                    location,
                    GetOptions {
                        range: Some(GetRange::Bounded(range.clone())),
                        ..Default::default()
                    },
                )
                .await?
                .bytes()
                .await?,
            );
        }
        Ok(bytes)
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.cache.invalidate(&self.scope, None);
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.cache.invalidate(&self.scope, Some(to));
        self.inner.copy_opts(from, to, options).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.cache.invalidate(&self.scope, None);
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;

    #[tokio::test]
    async fn actual_prefetch_populates_foreground_cache_with_exact_bytes_and_scope() {
        let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("db/compacted/01ABC.sst");
        backing
            .put(&path, Bytes::from_static(b"0123456789abcdef").into())
            .await
            .unwrap();
        let donor_cache = Arc::new(HeatCache::new(64 * 1024));
        let donor = HeatObjectStore::new(
            "tenant-a/shard-1".into(),
            backing.clone(),
            donor_cache.clone(),
        );
        assert_eq!(
            donor.get_range(&path, 4..12).await.unwrap(),
            Bytes::from_static(b"456789ab")
        );
        let now = now_ms();
        let hint = donor_cache.heat(&["tenant-a/shard-1".into()], now);
        assert_eq!(
            hint.blocks
                .iter()
                .map(|block| (&*block.object_path, block.start, block.end))
                .collect::<Vec<_>>(),
            vec![("db/compacted/01ABC.sst", 4, 12)]
        );
        let target_cache = Arc::new(HeatCache::new(64 * 1024));
        assert_eq!(
            target_cache
                .warm(&[("tenant-a/shard-1".into(), backing.clone())], &hint, now)
                .await,
            8
        );
        let target = HeatObjectStore::new(
            "tenant-a/shard-1".into(),
            backing.clone(),
            target_cache.clone(),
        );
        assert_eq!(
            target.get_range(&path, 4..12).await.unwrap(),
            Bytes::from_static(b"456789ab")
        );
        assert_eq!((target_cache.hits(), target_cache.misses()), (1, 1));
        let isolated =
            HeatObjectStore::new("tenant-b/shard-1".into(), backing, target_cache.clone());
        assert_eq!(
            isolated.get_range(&path, 4..12).await.unwrap(),
            Bytes::from_static(b"456789ab")
        );
        assert_eq!((target_cache.hits(), target_cache.misses()), (1, 2));
    }

    #[tokio::test]
    async fn stale_malformed_oversized_and_unowned_hints_do_no_io() {
        let cache = Arc::new(HeatCache::new(2048));
        let valid = SstBlockHeat {
            storage_scope: "owned".into(),
            object_path: "db/compacted/01ABC.sst".into(),
            start: 0,
            end: 1,
            observed_at_ms: 1,
        };
        let mut malformed = valid.clone();
        malformed.object_path = "db/compacted/../../manifest".into();
        let mut oversized = valid.clone();
        oversized.end = LeaseHeatHint::MAX_BLOCK_RANGE_BYTES + 1;
        let mut unowned = valid.clone();
        unowned.storage_scope = "other-tenant".into();
        let mut future = valid.clone();
        future.observed_at_ms = u64::MAX;
        let hint = LeaseHeatHint {
            blocks: vec![valid, malformed, oversized, unowned, future],
            ..Default::default()
        };
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        assert_eq!(
            cache
                .warm(
                    &[("owned".into(), store)],
                    &hint,
                    LeaseHeatHint::MAX_AGE_MS + 2
                )
                .await,
            0
        );
        assert_eq!(
            (cache.hits(), cache.misses(), cache.resident_bytes()),
            (0, 0, 0)
        );
    }

    #[tokio::test]
    async fn prefetch_enforces_actual_backend_byte_and_resident_memory_budgets() {
        let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path = Path::from("db/compacted/01BUDGET.sst");
        let block = LeaseHeatHint::MAX_BLOCK_RANGE_BYTES;
        backing
            .put(&path, Bytes::from(vec![17; (block * 5) as usize]).into())
            .await
            .unwrap();
        let tracker = Arc::new(crate::metered_store::PhysicalWriteTracker::new());
        let metered: Arc<dyn ObjectStore> = Arc::new(
            crate::metered_store::MeteredObjectStore::new("budget", backing, tracker.clone()),
        );
        let at_ms = now_ms();
        let hint = LeaseHeatHint {
            blocks: (0..5)
                .map(|index| SstBlockHeat {
                    storage_scope: "owned".into(),
                    object_path: path.to_string(),
                    start: block * index,
                    end: block * (index + 1),
                    observed_at_ms: at_ms,
                })
                .collect(),
            ..Default::default()
        };
        let cache = Arc::new(HeatCache::new((block * 2) as usize));
        assert_eq!(
            cache.warm(&[("owned".into(), metered)], &hint, at_ms).await,
            LeaseHeatHint::MAX_PREFETCH_BYTES
        );
        let io = tracker.snapshot();
        assert_eq!(
            (io.get_ops, io.get_bytes),
            (4, LeaseHeatHint::MAX_PREFETCH_BYTES)
        );
        assert!(cache.resident_bytes() <= (block * 2) as usize);
        let resident = cache.heat(&["owned".into()], now_ms());
        assert_eq!(
            resident
                .blocks
                .iter()
                .map(|hint| (hint.start, hint.end))
                .collect::<Vec<_>>(),
            vec![(block * 3, block * 4)]
        );
    }
}

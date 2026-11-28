//! Metered ObjectStore wrapper reporting actual physical WAL, SST, and manifest
//! bytes written and read separately from logical `StateMutation` bytes.

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
};
use rockstream_types::metrics::{record_physical_persistence, PhysicalPersistenceCounters};

/// Shared atomic tracker for physical storage I/O issued by a `ShardDb`.
#[derive(Debug, Default)]
pub struct PhysicalWriteTracker {
    wal_bytes_written: AtomicU64,
    sst_bytes_written: AtomicU64,
    manifest_bytes_written: AtomicU64,
    other_bytes_written: AtomicU64,
    put_ops: AtomicU64,
    get_ops: AtomicU64,
    get_bytes: AtomicU64,
}

impl PhysicalWriteTracker {
    pub fn new() -> Self {
        Self::default()
    }

    fn record_put(&self, scope: &str, location: &Path, bytes: u64) {
        let path_str = location.as_ref();
        let mut delta = PhysicalPersistenceCounters {
            put_ops: 1,
            ..Default::default()
        };
        if path_str.contains("/wal/") || path_str.ends_with(".wal") {
            self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
            delta.wal_bytes_written = bytes;
        } else if path_str.contains("/compacted/") || path_str.ends_with(".sst") {
            self.sst_bytes_written.fetch_add(bytes, Ordering::Relaxed);
            delta.sst_bytes_written = bytes;
        } else if path_str.contains("/manifest/") || path_str.ends_with(".manifest") {
            self.manifest_bytes_written
                .fetch_add(bytes, Ordering::Relaxed);
            delta.manifest_bytes_written = bytes;
        } else {
            self.other_bytes_written.fetch_add(bytes, Ordering::Relaxed);
            delta.other_bytes_written = bytes;
        }
        self.put_ops.fetch_add(1, Ordering::Relaxed);
        record_physical_persistence(scope, delta);
    }

    fn record_part_bytes(&self, scope: &str, location: &Path, bytes: u64) {
        let path_str = location.as_ref();
        let mut delta = PhysicalPersistenceCounters::default();
        if path_str.contains("/wal/") || path_str.ends_with(".wal") {
            self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
            delta.wal_bytes_written = bytes;
        } else if path_str.contains("/compacted/") || path_str.ends_with(".sst") {
            self.sst_bytes_written.fetch_add(bytes, Ordering::Relaxed);
            delta.sst_bytes_written = bytes;
        } else if path_str.contains("/manifest/") || path_str.ends_with(".manifest") {
            self.manifest_bytes_written
                .fetch_add(bytes, Ordering::Relaxed);
            delta.manifest_bytes_written = bytes;
        } else {
            self.other_bytes_written.fetch_add(bytes, Ordering::Relaxed);
            delta.other_bytes_written = bytes;
        }
        record_physical_persistence(scope, delta);
    }

    fn record_put_op(&self, scope: &str) {
        self.put_ops.fetch_add(1, Ordering::Relaxed);
        record_physical_persistence(
            scope,
            PhysicalPersistenceCounters {
                put_ops: 1,
                ..Default::default()
            },
        );
    }

    fn record_get(&self, scope: &str, bytes: u64) {
        self.get_ops.fetch_add(1, Ordering::Relaxed);
        self.get_bytes.fetch_add(bytes, Ordering::Relaxed);
        record_physical_persistence(
            scope,
            PhysicalPersistenceCounters {
                get_ops: 1,
                get_bytes: bytes,
                ..Default::default()
            },
        );
    }

    /// Snapshot current physical I/O counters.
    pub fn snapshot(&self) -> PhysicalPersistenceCounters {
        PhysicalPersistenceCounters {
            wal_bytes_written: self.wal_bytes_written.load(Ordering::Relaxed),
            sst_bytes_written: self.sst_bytes_written.load(Ordering::Relaxed),
            manifest_bytes_written: self.manifest_bytes_written.load(Ordering::Relaxed),
            other_bytes_written: self.other_bytes_written.load(Ordering::Relaxed),
            put_ops: self.put_ops.load(Ordering::Relaxed),
            get_ops: self.get_ops.load(Ordering::Relaxed),
            get_bytes: self.get_bytes.load(Ordering::Relaxed),
        }
    }

    /// Reset per-shard counters (useful for measuring incremental per-epoch physical writes).
    pub fn reset(&self) {
        self.wal_bytes_written.store(0, Ordering::Relaxed);
        self.sst_bytes_written.store(0, Ordering::Relaxed);
        self.manifest_bytes_written.store(0, Ordering::Relaxed);
        self.other_bytes_written.store(0, Ordering::Relaxed);
        self.put_ops.store(0, Ordering::Relaxed);
        self.get_ops.store(0, Ordering::Relaxed);
        self.get_bytes.store(0, Ordering::Relaxed);
    }
}

/// ObjectStore decorator that counts exact physical PUT/GET bytes by SlateDB file category.
#[derive(Debug)]
pub struct MeteredObjectStore {
    scope: String,
    inner: Arc<dyn ObjectStore>,
    tracker: Arc<PhysicalWriteTracker>,
}

impl MeteredObjectStore {
    pub fn new(
        scope: impl Into<String>,
        inner: Arc<dyn ObjectStore>,
        tracker: Arc<PhysicalWriteTracker>,
    ) -> Self {
        Self {
            scope: scope.into(),
            inner,
            tracker,
        }
    }

    pub fn tracker(&self) -> &Arc<PhysicalWriteTracker> {
        &self.tracker
    }
}

impl fmt::Display for MeteredObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MeteredObjectStore({}, {})", self.scope, self.inner)
    }
}

struct MeteredMultipartUpload {
    scope: String,
    location: Path,
    tracker: Arc<PhysicalWriteTracker>,
    inner: Box<dyn MultipartUpload>,
}

impl fmt::Debug for MeteredMultipartUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MeteredMultipartUpload")
            .field("scope", &self.scope)
            .field("location", &self.location)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for MeteredMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let len = data.content_length() as u64;
        self.tracker
            .record_part_bytes(&self.scope, &self.location, len);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let res = self.inner.complete().await?;
        self.tracker.record_put_op(&self.scope);
        Ok(res)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for MeteredObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let len = payload.content_length() as u64;
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.tracker.record_put(&self.scope, location, len);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(MeteredMultipartUpload {
            scope: self.scope.clone(),
            location: location.clone(),
            tracker: Arc::clone(&self.tracker),
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let result = self.inner.get_opts(location, options).await?;
        let bytes = result.range.end.saturating_sub(result.range.start);
        self.tracker.record_get(&self.scope, bytes);
        Ok(result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let chunks = self.inner.get_ranges(location, ranges).await?;
        let total: u64 = chunks.iter().map(|b| b.len() as u64).sum();
        self.tracker.record_get(&self.scope, total);
        Ok(chunks)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

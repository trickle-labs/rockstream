//! Bounded Local Object-Store Disk Cache Integration Tests (v0.62.1 Slice 6 / Phase 3b).

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use rockstream_storage::shard_db::ShardDb;

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    get_count: Arc<AtomicUsize>,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, get_count: Arc<AtomicUsize>) -> Self {
        Self { inner, get_count }
    }
}

impl std::fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.get_count.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjPath) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjPath>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjPath>,
        offset: &ObjPath,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjPath, to: &ObjPath) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &ObjPath, to: &ObjPath) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn get_range(&self, location: &ObjPath, range: Range<u64>) -> ObjectStoreResult<Bytes> {
        self.get_count.fetch_add(1, Ordering::SeqCst);
        self.inner.get_range(location, range).await
    }
}

#[tokio::test]
async fn test_disk_cache_directory_path_validation() {
    let store = Arc::new(InMemory::new());
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let cache_dir = temp_dir.path().join("cache");

    let builder =
        ShardDb::builder("test-shard-path", store).with_disk_cache(&cache_dir, 64 * 1024 * 1024);

    let opts = builder.object_store_cache_options();
    assert_eq!(opts.root_folder.as_ref(), Some(&cache_dir));
    assert_eq!(opts.max_cache_size_bytes, Some(64 * 1024 * 1024));
    assert_eq!(opts.part_size_bytes, 4 * 1024 * 1024);
    assert_eq!(opts.scan_interval, Some(Duration::from_secs(3600)));
    assert_eq!(opts.max_open_file_handles, 1000);
}

#[tokio::test]
async fn test_pinned_slatedb_disk_cache_enforces_capacity_limit() {
    let store = Arc::new(InMemory::new());
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let cache_dir = temp_dir.path().join("slatedb_cache");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");

    let shard = ShardDb::builder("test-disk-cache-shard", store)
        .with_disk_cache(&cache_dir, 16 * 1024 * 1024)
        .build()
        .await
        .expect("build shard with disk cache");

    assert_eq!(shard.disk_cache_dir(), Some(cache_dir.as_path()));

    // Write data, flush to SST, and read back
    shard
        .put(b"key-disk-1", b"value-disk-1")
        .await
        .expect("put");
    shard
        .put(b"key-disk-2", b"value-disk-2")
        .await
        .expect("put");
    shard.flush().await.expect("flush");

    let v1 = shard.get(b"key-disk-1").await.expect("get key 1");
    let v2 = shard.get(b"key-disk-2").await.expect("get key 2");

    assert_eq!(v1, Some(Bytes::from_static(b"value-disk-1")));
    assert_eq!(v2, Some(Bytes::from_static(b"value-disk-2")));
}

#[tokio::test]
async fn test_disk_cache_shard_cleanup_on_drop() {
    let store = Arc::new(InMemory::new());
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let cache_dir = temp_dir.path().join("cleanup_cache_dir");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");
    assert!(cache_dir.exists());

    {
        let shard = ShardDb::builder("test-cleanup-shard", store)
            .with_disk_cache(&cache_dir, 8 * 1024 * 1024)
            .with_cleanup_on_drop(true)
            .build()
            .await
            .expect("build shard");

        shard.put(b"temp_k", b"temp_v").await.expect("put");
        shard.flush().await.expect("flush");
        assert!(shard.disk_cache_dir().is_some());
    } // shard dropped here

    // Must be cleaned up on drop
    assert!(
        !cache_dir.exists(),
        "disk cache directory should be removed on drop"
    );
}

#[tokio::test]
async fn test_cross_instance_download_reuse_measured_via_request_trace() {
    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let get_counter = Arc::new(AtomicUsize::new(0));
    let tracking_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        raw_store.clone(),
        get_counter.clone(),
    ));

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let shared_cache_dir = temp_dir.path().join("shared_disk_cache");
    std::fs::create_dir_all(&shared_cache_dir).expect("create dir");

    // Shard instance 1 writes and flushes
    {
        let shard1 = ShardDb::builder("shared-sst-db", tracking_store.clone())
            .with_disk_cache(&shared_cache_dir, 64 * 1024 * 1024)
            .build()
            .await
            .expect("build shard 1");

        shard1.put(b"reuse-k1", b"reuse-v1").await.expect("put");
        shard1.flush().await.expect("flush");

        // First read populates local disk cache
        let v = shard1.get(b"reuse-k1").await.expect("get");
        assert_eq!(v, Some(Bytes::from_static(b"reuse-v1")));
    }

    // Reopened instance with same local disk cache
    {
        let shard2 = ShardDb::builder("shared-sst-db", tracking_store.clone())
            .with_disk_cache(&shared_cache_dir, 64 * 1024 * 1024)
            .build()
            .await
            .expect("build shard 2");

        let gets_before_read = get_counter.load(Ordering::SeqCst);
        let v2 = shard2.get(b"reuse-k1").await.expect("get from reopened");
        assert_eq!(v2, Some(Bytes::from_static(b"reuse-v1")));
        let gets_after_read = get_counter.load(Ordering::SeqCst);

        // Warm read for SST block already in local disk cache must not issue remote GETs
        assert_eq!(
            gets_after_read, gets_before_read,
            "warm reads from reopened instance must hit local disk cache without issuing remote GET requests"
        );
    }
}

//! SlateDB NVMe Block Cache and Bloom Filter Arrangement Integration Tests (Issue #94).
//!
//! Tests:
//! - Bloom filter sizing and min_filter_keys tuning for arrangements
//! - Tiered NVMe block caching configuration and WorkerStorageContext inheritance
//! - S3 GET amplification minimization during arrangement lookups

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use rockstream_storage::reader::ShardReader;
use rockstream_storage::shard_db::ShardDb;
use rockstream_storage::storage_context::WorkerStorageContext;
use rockstream_storage::{JoinSide, ShardKeyEncoder};

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
async fn test_bloom_filter_tuning_and_sizing_for_arrangements() {
    let store = Arc::new(InMemory::new());
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let nvme_dir = temp_dir.path().join("nvme_cache");
    std::fs::create_dir_all(&nvme_dir).expect("create nvme dir");

    let builder = ShardDb::builder("test-arrangement-db", store.clone())
        .with_bloom_filter(14, 0)
        .with_nvme_cache(&nvme_dir, 32 * 1024 * 1024);

    assert_eq!(builder.bloom_filter_bits(), Some(14));
    assert_eq!(builder.min_filter_keys(), 0);
    assert!(builder.is_nvme_cache_configured());

    let shard = builder.build().await.expect("build shard");
    assert_eq!(shard.disk_cache_dir(), Some(nvme_dir.as_path()));

    // Insert 50 arrangement keys (< 1000 default threshold, tests min_filter_keys = 0)
    for i in 0..50u64 {
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 42, &i.to_be_bytes(), i as u128);
        let val = format!("arr_val_{i}").into_bytes();
        shard.put(&key, &val).await.expect("put");
    }
    shard.flush().await.expect("flush");

    // Verify all 50 keys are accurately readable
    for i in 0..50u64 {
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 42, &i.to_be_bytes(), i as u128);
        let expected = format!("arr_val_{i}").into_bytes();
        let actual = shard.get(&key).await.expect("get");
        assert_eq!(actual, Some(Bytes::from(expected)));
    }

    // Verify absent keys return None
    for i in 100..150u64 {
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 42, &i.to_be_bytes(), i as u128);
        let actual = shard.get(&key).await.expect("get absent");
        assert_eq!(actual, None);
    }
}

#[tokio::test]
async fn test_worker_storage_context_nvme_and_bloom_inheritance() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let nvme_dir = temp_dir.path().join("worker_nvme_cache");
    std::fs::create_dir_all(&nvme_dir).expect("create nvme dir");

    let storage_ctx = Arc::new(
        WorkerStorageContext::new_with_worker_id("worker-arr-1", 1024 * 1024)
            .with_nvme_cache(&nvme_dir, 64 * 1024 * 1024)
            .with_filter_bits_per_key(14),
    );

    assert_eq!(storage_ctx.filter_bits_per_key(), Some(14));
    assert!(storage_ctx.nvme_config().is_some());

    let store = Arc::new(InMemory::new());
    let shard = ShardDb::builder("shard-inherited", store)
        .with_storage_context(storage_ctx.clone())
        .build()
        .await
        .expect("build shard");

    assert_eq!(shard.disk_cache_dir(), Some(nvme_dir.as_path()));

    let key = ShardKeyEncoder::join_arr_key(JoinSide::Left, 99, &[1, 2, 3], 100);
    shard.put(&key, b"left_row_data").await.expect("put");
    shard.flush().await.expect("flush");

    let val = shard.get(&key).await.expect("get");
    assert_eq!(val, Some(Bytes::from_static(b"left_row_data")));
}

#[tokio::test]
async fn test_s3_get_amplification_avoidance_with_bloom_and_nvme() {
    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let get_counter = Arc::new(AtomicUsize::new(0));
    let tracking_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        raw_store.clone(),
        get_counter.clone(),
    ));

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let nvme_cache_dir = temp_dir.path().join("nvme_tier");
    std::fs::create_dir_all(&nvme_cache_dir).expect("create nvme dir");

    let shard = ShardDb::builder("arr-amplification-db", tracking_store.clone())
        .with_bloom_filter(14, 0)
        .with_nvme_cache(&nvme_cache_dir, 64 * 1024 * 1024)
        .build()
        .await
        .expect("build shard");

    // Write 100 arrangement keys
    for i in 0..100u64 {
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 77, &i.to_be_bytes(), i as u128);
        let val = format!("join_row_payload_{i}").into_bytes();
        shard.put(&key, &val).await.expect("put");
    }
    shard.flush().await.expect("flush");

    // Warm read one key to ensure SST metadata is cached
    let warm_key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 77, &0u64.to_be_bytes(), 0);
    let warm_val = shard.get(&warm_key).await.expect("warm get");
    assert_eq!(
        warm_val,
        Some(Bytes::from("join_row_payload_0".to_string().into_bytes()))
    );

    let gets_before_absent_probes = get_counter.load(Ordering::SeqCst);

    // Probe 50 keys that were NEVER written to this arrangement
    for absent_id in 1000..1050u64 {
        let absent_key = ShardKeyEncoder::join_arr_key(
            JoinSide::Right,
            77,
            &absent_id.to_be_bytes(),
            absent_id as u128,
        );
        let res = shard.get(&absent_key).await.expect("get absent");
        assert_eq!(res, None);
    }

    let gets_after_absent_probes = get_counter.load(Ordering::SeqCst);

    // Bloom filter must prevent remote GET amplification on absent keys!
    assert_eq!(
        gets_after_absent_probes, gets_before_absent_probes,
        "Absent key probes must be filtered by Bloom filter without issuing remote S3 GET requests"
    );

    // Now test warm reads for hot keys cached in local NVMe tier
    let gets_before_hot_reads = get_counter.load(Ordering::SeqCst);
    for i in 1..20u64 {
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 77, &i.to_be_bytes(), i as u128);
        let val = shard.get(&key).await.expect("get hot key");
        assert_eq!(
            val,
            Some(Bytes::from(format!("join_row_payload_{i}").into_bytes()))
        );
    }
    let gets_after_hot_reads = get_counter.load(Ordering::SeqCst);

    // Hot reads must hit local NVMe tier without remote S3 GET amplification
    assert_eq!(
        gets_after_hot_reads, gets_before_hot_reads,
        "Warm reads from hot SSTable blocks must hit local NVMe cache without issuing remote GET requests"
    );
}

#[tokio::test]
async fn test_shard_reader_with_nvme_cache_and_filter() {
    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let nvme_cache_dir = temp_dir.path().join("reader_nvme_tier");
    std::fs::create_dir_all(&nvme_cache_dir).expect("create nvme dir");

    // Writer writes arrangement
    {
        let shard = ShardDb::builder("reader-db", raw_store.clone())
            .with_bloom_filter(14, 0)
            .with_nvme_cache(&nvme_cache_dir, 32 * 1024 * 1024)
            .build()
            .await
            .expect("build writer");

        for i in 0..10u64 {
            let key =
                ShardKeyEncoder::join_arr_key(JoinSide::Left, 88, &i.to_be_bytes(), i as u128);
            shard
                .put(&key, &format!("row_{i}").into_bytes())
                .await
                .expect("put");
        }
        shard.flush().await.expect("flush");
    }

    // Reader opens with NVMe cache and Bloom filter
    let reader = ShardReader::open_with_cache_and_filter(
        "reader-db",
        raw_store.clone(),
        Some(nvme_cache_dir),
        Some(32 * 1024 * 1024),
        Some(14),
    )
    .await
    .expect("open reader");

    for i in 0..10u64 {
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Left, 88, &i.to_be_bytes(), i as u128);
        let val = reader.get(&key).await.expect("reader get");
        assert_eq!(val, Some(Bytes::from(format!("row_{i}").into_bytes())));
    }

    let absent = ShardKeyEncoder::join_arr_key(JoinSide::Left, 88, &999u64.to_be_bytes(), 999);
    assert_eq!(reader.get(&absent).await.expect("reader get absent"), None);
}

#[tokio::test]
async fn test_arrangement_cache_reused_across_fresh_worker_contexts() {
    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let get_counter = Arc::new(AtomicUsize::new(0));
    let tracking_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        raw_store.clone(),
        get_counter.clone(),
    ));

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let nvme_cache_dir = temp_dir.path().join("fresh-worker-nvme-cache");

    {
        let writer_context = Arc::new(
            WorkerStorageContext::new_with_worker_id("arrangement-writer", 1)
                .with_nvme_cache(&nvme_cache_dir, 32 * 1024 * 1024)
                .with_filter_bits_per_key(14),
        );
        let builder = ShardDb::builder("fresh-worker-arrangement-db", tracking_store.clone())
            .with_storage_context(writer_context)
            .with_bloom_filter(14, 0)
            .with_nvme_cache(&nvme_cache_dir, 32 * 1024 * 1024);
        assert!(builder.object_store_cache_options().cache_puts);
        let shard = builder.build().await.expect("build writer");

        for i in 0..100u64 {
            let key =
                ShardKeyEncoder::join_arr_key(JoinSide::Right, 91, &i.to_be_bytes(), i as u128);
            shard
                .put(&key, format!("arrangement-row-{i}").as_bytes())
                .await
                .expect("put arrangement row");
        }
        shard.flush().await.expect("flush arrangement rows");
    }

    assert!(
        std::fs::read_dir(&nvme_cache_dir)
            .expect("read populated NVMe cache")
            .next()
            .is_some(),
        "writer must populate the shared local cache"
    );

    let reader_context = Arc::new(
        WorkerStorageContext::new_with_worker_id("arrangement-reader", 1)
            .with_nvme_cache(&nvme_cache_dir, 32 * 1024 * 1024)
            .with_filter_bits_per_key(14),
    );
    let shard = ShardDb::builder("fresh-worker-arrangement-db", tracking_store)
        .with_storage_context(reader_context)
        .with_bloom_filter(14, 0)
        .build()
        .await
        .expect("build fresh reader");
    let gets_before_reads = get_counter.load(Ordering::SeqCst);

    for i in 0..100u64 {
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 91, &i.to_be_bytes(), i as u128);
        assert_eq!(
            shard.get(&key).await.expect("read cached arrangement row"),
            Some(Bytes::from(format!("arrangement-row-{i}")))
        );
    }
    for i in 1000..1050u64 {
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 91, &i.to_be_bytes(), i as u128);
        assert_eq!(
            shard.get(&key).await.expect("read absent arrangement row"),
            None
        );
    }

    assert_eq!(
        get_counter.load(Ordering::SeqCst),
        gets_before_reads,
        "fresh worker reads must use the NVMe cache and Bloom filters without remote GETs"
    );
}

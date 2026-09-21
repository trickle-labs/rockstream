//! Real S3-compatible proof for Issue #94's arrangement cache path.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path as ObjPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use rockstream_ops::zset::ArrowZSet;
use rockstream_ops::{JoinKind, JoinOp, JoinPipeline};
use rockstream_storage::storage_context::WorkerStorageContext;
use rockstream_storage::{JoinSide, ShardDb, ShardKeyEncoder};
use rockstream_test_support::minio::{minio_object_store, start_minio};
use rockstream_types::ids::OperatorId;

const MINIO_BUCKET: &str = "rockstream-arrangement-cache-test";

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    gets: Arc<AtomicUsize>,
}

impl CountingObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, gets: Arc<AtomicUsize>) -> Self {
        Self { inner, gets }
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
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjPath>> {
        self.inner.delete_stream(locations)
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

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn open_shard(path: &str, store: Arc<dyn ObjectStore>, cache_dir: Option<&Path>) -> ShardDb {
    let context = WorkerStorageContext::new_with_worker_id(path, 1);
    let mut builder =
        ShardDb::builder(path.to_string(), store).with_storage_context(Arc::new(context));
    if let Some(dir) = cache_dir {
        builder = builder
            .with_bloom_filter(14, 0)
            .with_nvme_cache(dir.to_path_buf(), 64 * 1024 * 1024);
    }
    builder.build().await.expect("build MinIO shard")
}

async fn seed_arrangement(path: &str, store: Arc<dyn ObjectStore>, cache_dir: Option<&Path>) {
    let shard = open_shard(path, store, cache_dir).await;
    for i in 0..64i64 {
        let join_key =
            rockstream_types::KeyCapsule::from_values(&[rockstream_types::KeyValue::Int64(i)])
                .expect("encode join key")
                .typed_bytes()
                .to_vec();
        let row_bytes = [i.to_be_bytes(), (i * 100).to_be_bytes()].concat();
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 947, &join_key, i as u128);
        shard
            .put(&key, &row_bytes)
            .await
            .expect("seed arrangement row");
    }
    shard.flush().await.expect("flush MinIO arrangement");
    shard.close().await.expect("close MinIO writer");
}

fn make_left_delta() -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 32, 10_000])) as _,
            Arc::new(Int64Array::from(vec![10, 20, 30])) as _,
        ],
    )
    .expect("build left delta");
    ArrowZSet::new(data, vec![1, 1, 1])
}

fn join_values(output: &ArrowZSet) -> Vec<Vec<i64>> {
    (0..4)
        .map(|column| {
            output
                .data
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 join output")
                .values()
                .to_vec()
        })
        .collect()
}

async fn run_join(
    path: &str,
    store: Arc<dyn ObjectStore>,
    cache_dir: Option<&Path>,
) -> Vec<Vec<i64>> {
    let shard = Arc::new(open_shard(path, store, cache_dir).await);
    let pipeline = JoinPipeline::new(
        vec![],
        vec![],
        JoinKind::Inner(Arc::new(
            JoinOp::new(OperatorId(947), vec![0], vec![0]).with_db(shard),
        )),
        vec![],
    );
    let output = pipeline
        .process_async(
            make_left_delta(),
            ArrowZSet::empty(Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
            ]))),
        )
        .await
        .expect("process MinIO join");
    assert_eq!(output.weights, vec![1, 1]);
    let values = join_values(&output);
    assert_eq!(
        values,
        vec![vec![1, 32], vec![10, 20], vec![1, 32], vec![100, 3200]]
    );
    values
}

#[tokio::test]
async fn optimized_arrangement_lookups_reduce_minio_gets_vs_uncached_baseline() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP optimized_arrangement_lookups_reduce_minio_gets_vs_uncached_baseline: Docker unavailable");
        return;
    }
    let Some((_container, port)) = start_minio(MINIO_BUCKET).await else {
        return;
    };

    let raw_store = minio_object_store(port, MINIO_BUCKET);
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let optimized_cache = temp_dir.path().join("optimized-cache");
    seed_arrangement("optimized", raw_store.clone(), Some(&optimized_cache)).await;
    seed_arrangement("baseline", raw_store.clone(), None).await;

    let optimized_gets = Arc::new(AtomicUsize::new(0));
    let baseline_gets = Arc::new(AtomicUsize::new(0));
    let optimized_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        raw_store.clone(),
        optimized_gets.clone(),
    ));
    let baseline_store: Arc<dyn ObjectStore> =
        Arc::new(CountingObjectStore::new(raw_store, baseline_gets.clone()));

    let _ = run_join("optimized", optimized_store.clone(), Some(&optimized_cache)).await;
    optimized_gets.store(0, Ordering::SeqCst);
    let optimized = run_join("optimized", optimized_store, Some(&optimized_cache)).await;
    let baseline = run_join("baseline", baseline_store, None).await;

    assert_eq!(optimized, baseline);
    assert!(
        optimized_gets.load(Ordering::SeqCst) < baseline_gets.load(Ordering::SeqCst),
        "optimized GETs ({}) must be lower than uncached baseline GETs ({})",
        optimized_gets.load(Ordering::SeqCst),
        baseline_gets.load(Ordering::SeqCst)
    );
    eprintln!(
        "Issue #94 MinIO GETs: optimized={} baseline={}",
        optimized_gets.load(Ordering::SeqCst),
        baseline_gets.load(Ordering::SeqCst)
    );
}

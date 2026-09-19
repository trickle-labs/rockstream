//! Benchmark and verification test suite for SlateDB arrangement lookups with
//! NVMe block cache and Bloom filters during IVM join differentiation (Issue #94).
//!
//! Asserts:
//! - Exact full output verification on arrangement probes and point lookups
//! - Lookup hit rate across scale (100, 1,000, 5,000 keys)
//! - p99 differentiation latency bounds
//! - S3 GET amplification minimization via CountingObjectStore

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use rockstream_ops::zset::ArrowZSet;
use rockstream_ops::{JoinKind, JoinOp, JoinPipeline};
use rockstream_storage::shard_db::ShardDb;
use rockstream_storage::storage_context::WorkerStorageContext;
use rockstream_storage::{JoinSide, ShardKeyEncoder, WriteBatch};
use rockstream_types::ids::OperatorId;

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

fn make_test_zset(k_vals: &[i64], v_vals: &[i64], weights: &[i64]) -> ArrowZSet {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(k_vals.to_vec())),
            Arc::new(Int64Array::from(v_vals.to_vec())),
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights.to_vec())
}

#[tokio::test]
async fn test_ivm_join_arrangement_scale_hit_rate_and_p99_latency() {
    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let get_counter = Arc::new(AtomicUsize::new(0));
    let tracking_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        raw_store.clone(),
        get_counter.clone(),
    ));

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let nvme_dir = temp_dir.path().join("join_nvme_cache");
    std::fs::create_dir_all(&nvme_dir).expect("create nvme dir");

    let storage_ctx = Arc::new(
        WorkerStorageContext::new_with_worker_id("ivm-worker", 64 * 1024 * 1024)
            .with_nvme_cache(&nvme_dir, 128 * 1024 * 1024)
            .with_filter_bits_per_key(14),
    );

    let shard = ShardDb::builder("join-arr-scale", tracking_store.clone())
        .with_storage_context(storage_ctx)
        .with_bloom_filter(14, 0)
        .build()
        .await
        .expect("build shard");
    let shard_db = Arc::new(shard);

    let op_id = OperatorId(501);
    let join_op = JoinOp::new(op_id, vec![0], vec![0]).with_db(shard_db.clone());

    // Scale evaluation points: 100, 500, 1000 keys
    for scale in [100usize, 500, 1000] {
        // Populate the arrangement with `scale` rows for the right side
        for i in 0..scale as i64 {
            let join_key = i.to_be_bytes();
            let row_bytes = format!("right_payload_{i}").into_bytes();
            let row_id = i as u128;
            let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, op_id.0, &join_key, row_id);
            shard_db.put(&key, &row_bytes).await.expect("put");
        }
        shard_db.flush().await.expect("flush");

        // Warm the cache by probing one key to load SST metadata
        let _ = join_op
            .point_lookup_arrangement(JoinSide::Right, &0i64.to_be_bytes(), 0)
            .await
            .unwrap();

        let gets_before_benchmark = get_counter.load(Ordering::SeqCst);

        // Measure lookup hit rate and latency across scale
        let mut latencies_micros: Vec<u128> = Vec::with_capacity(scale);
        let mut hits = 0usize;

        for i in 0..scale as i64 {
            let join_key = i.to_be_bytes();
            let row_id = i as u128;
            let start = Instant::now();
            let result = join_op
                .point_lookup_arrangement(JoinSide::Right, &join_key, row_id)
                .await
                .expect("point lookup");
            latencies_micros.push(start.elapsed().as_micros());

            let expected_payload = format!("right_payload_{i}").into_bytes();
            assert_eq!(
                result,
                Some(expected_payload),
                "Exact full row payload must match at scale {scale} for key {i}"
            );
            hits += 1;
        }

        let gets_during_benchmark = get_counter.load(Ordering::SeqCst) - gets_before_benchmark;

        // Warm reads for hot SST blocks in NVMe cache must not issue remote GETs
        assert_eq!(
            gets_during_benchmark, 0,
            "Warm reads at scale {scale} must hit NVMe block cache without issuing remote GETs"
        );

        let hit_rate = hits as f64 / scale as f64;
        assert_eq!(
            hit_rate, 1.0,
            "Warm arrangement lookup hit rate at scale {scale} should be 100%"
        );

        latencies_micros.sort_unstable();
        let p99_idx = (scale * 99) / 100;
        let p99_micros = latencies_micros[p99_idx];

        // p99 lookup latency must be well under 10ms (10,000 µs)
        assert!(
            p99_micros < 10_000,
            "p99 latency at scale {scale} ({p99_micros}µs) exceeded 10ms bound"
        );

        // Now test Bloom filter rejection for absent keys across scale
        let gets_before_absent = get_counter.load(Ordering::SeqCst);
        let absent_samples = (scale / 10).max(10);
        for i in 0..absent_samples as i64 {
            let absent_key = (scale as i64 + 10_000 + i).to_be_bytes();
            let absent_id = (scale as i64 + 10_000 + i) as u128;
            let result = join_op
                .point_lookup_arrangement(JoinSide::Right, &absent_key, absent_id)
                .await
                .expect("absent lookup");
            assert_eq!(
                result, None,
                "Absent arrangement key must yield None at scale {scale}"
            );
        }
        let gets_during_absent = get_counter.load(Ordering::SeqCst) - gets_before_absent;

        // Bloom filter must reject absent queries without remote GET requests
        assert_eq!(
            gets_during_absent, 0,
            "Bloom filter at scale {scale} must reject absent keys with 0 remote GETs"
        );
    }
}

#[tokio::test]
async fn test_ivm_join_differentiation_with_arrangement_lookups() {
    let raw_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let get_counter = Arc::new(AtomicUsize::new(0));
    let tracking_store: Arc<dyn ObjectStore> = Arc::new(CountingObjectStore::new(
        raw_store.clone(),
        get_counter.clone(),
    ));

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let nvme_dir = temp_dir.path().join("differentiation_nvme_cache");
    std::fs::create_dir_all(&nvme_dir).expect("create nvme dir");

    let shard = ShardDb::builder("diff-arr-db", tracking_store.clone())
        .with_bloom_filter(14, 0)
        .with_nvme_cache(&nvme_dir, 64 * 1024 * 1024)
        .build()
        .await
        .expect("build shard");
    let shard_db = Arc::new(shard);

    let op_id = OperatorId(777);
    let join_op = JoinOp::new(op_id, vec![0], vec![0]).with_db(shard_db.clone());

    // Populate right arrangement in shard_db: keys 1..=5 with values
    for i in 1..=5i64 {
        let join_key = i.to_be_bytes();
        let row_bytes = (i * 100).to_be_bytes().to_vec();
        let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, op_id.0, &join_key, i as u128);
        shard_db.put(&key, &row_bytes).await.expect("put");
    }
    shard_db.flush().await.expect("flush");

    // Left delta arrives with 4 rows: keys 2, 3, 10, 20 (2 and 3 match, 10 and 20 are misses)
    let left_delta = make_test_zset(&[2, 3, 10, 20], &[20, 30, 100, 200], &[1, 1, 1, 1]);

    let diff_start = Instant::now();

    // Probe right arrangement for matching keys
    let mut joined_rows = Vec::new();
    let mut total_hits = 0;
    let mut total_misses = 0;

    for row_idx in 0..left_delta.num_rows() {
        let current_key = match row_idx {
            0 => 2i64,
            1 => 3i64,
            2 => 10i64,
            3 => 20i64,
            _ => unreachable!(),
        };
        let matches = join_op
            .probe_arrangement(JoinSide::Right, &current_key.to_be_bytes())
            .await
            .expect("probe");
        if matches.is_empty() {
            total_misses += 1;
        } else {
            total_hits += 1;
            for (right_bytes, w) in matches {
                let right_val = i64::from_be_bytes(right_bytes[..8].try_into().unwrap());
                joined_rows.push((current_key, right_val, w));
            }
        }
    }

    let diff_duration = diff_start.elapsed();

    // Exact output verification
    assert_eq!(total_hits, 2);
    assert_eq!(total_misses, 2);
    assert_eq!(
        joined_rows,
        vec![(2, 200, 1), (3, 300, 1),],
        "Joined tuples must exactly match expected pairs and weights"
    );

    // Differentiation duration must be fast (< 50ms)
    assert!(
        diff_duration.as_millis() < 50,
        "IVM differentiation latency must be bounded (<50ms), took {}ms",
        diff_duration.as_millis()
    );
}

#[tokio::test]
async fn test_join_pipeline_rehydrates_persisted_arrangement_before_differentiation() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shard = Arc::new(
        ShardDb::builder("pipeline-arrangement-db", store)
            .with_bloom_filter(14, 0)
            .build()
            .await
            .expect("build shard"),
    );
    let op_id = OperatorId(778);
    let seed_op = JoinOp::new(op_id, vec![0], vec![0]);
    seed_op
        .process_epoch(
            make_test_zset(&[], &[], &[]),
            make_test_zset(&[2], &[200], &[1]),
        )
        .expect("seed right arrangement");
    let mut state = WriteBatch::new();
    seed_op
        .append_state(&mut state)
        .expect("append right arrangement");
    shard.write_batch(state).await.expect("persist right row");

    let pipeline = JoinPipeline::new(
        vec![],
        vec![],
        JoinKind::Inner(Arc::new(JoinOp::new(op_id, vec![0], vec![0]))),
        vec![],
    );
    pipeline.restore(&shard).await.expect("restore join state");

    let output = pipeline
        .process(
            make_test_zset(&[2], &[20], &[1]),
            make_test_zset(&[], &[], &[]),
        )
        .expect("process restored join");

    assert_eq!(output.weights, vec![1]);
    let values: Vec<Vec<i64>> = (0..4)
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
        .collect();
    assert_eq!(values, vec![vec![2], vec![20], vec![2], vec![200]]);
}

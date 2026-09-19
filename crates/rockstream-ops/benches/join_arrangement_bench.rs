use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use object_store::memory::InMemory;
use rockstream_ops::join::JoinOp;
use rockstream_storage::shard_db::ShardDb;
use rockstream_storage::storage_context::WorkerStorageContext;
use rockstream_storage::{JoinSide, ShardKeyEncoder};
use rockstream_types::ids::OperatorId;
use std::sync::Arc;

fn bench_join_arrangement_lookups(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let temp_dir = tempfile::tempdir().expect("tempdir");
    let nvme_dir = temp_dir.path().join("nvme_cache");
    std::fs::create_dir_all(&nvme_dir).expect("create nvme dir");

    let storage_ctx = Arc::new(
        WorkerStorageContext::new_with_worker_id("bench-worker", 64 * 1024 * 1024)
            .with_nvme_cache(&nvme_dir, 128 * 1024 * 1024)
            .with_filter_bits_per_key(14),
    );

    let store = Arc::new(InMemory::new());
    let db = rt.block_on(async {
        let shard = ShardDb::builder("bench-arr-db", store)
            .with_storage_context(storage_ctx)
            .with_bloom_filter(14, 0)
            .build()
            .await
            .unwrap();

        // Seed 1,000 arrangement keys
        for i in 0..1000u64 {
            let key = ShardKeyEncoder::join_arr_key(JoinSide::Right, 10, &i.to_be_bytes(), i as u128);
            let val = format!("val_{i}").into_bytes();
            shard.put(&key, &val).await.unwrap();
        }
        shard.flush().await.unwrap();
        Arc::new(shard)
    });

    let join_op = JoinOp::new(OperatorId(10), vec![0], vec![0]).with_db(db);

    let mut group = c.benchmark_group("join_arrangement_lookups");
    for scale in [10usize, 100, 500].iter() {
        group.throughput(Throughput::Elements(*scale as u64));
        group.bench_with_input(BenchmarkId::new("point_lookup", scale), scale, |b, &s| {
            b.to_async(&rt).iter(|| async {
                for i in 0..s as u64 {
                    let key = i.to_be_bytes();
                    let _ = join_op
                        .point_lookup_arrangement(JoinSide::Right, &key, i as u128)
                        .await
                        .unwrap();
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("absent_key_bloom_filtered", scale), scale, |b, &s| {
            b.to_async(&rt).iter(|| async {
                for i in 0..s as u64 {
                    let absent_id = 10_000 + i;
                    let key = absent_id.to_be_bytes();
                    let res = join_op
                        .point_lookup_arrangement(JoinSide::Right, &key, absent_id as u128)
                        .await
                        .unwrap();
                    assert!(res.is_none());
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_join_arrangement_lookups);
criterion_main!(benches);

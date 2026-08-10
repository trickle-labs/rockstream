use criterion::{criterion_group, criterion_main, Criterion};
use object_store::memory::InMemory;
use rockstream_ops::spill::SpillableArrangement;
use rockstream_storage::ShardDb;
use std::sync::Arc;

fn bench_spill_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let db = rt.block_on(async {
        let store = Arc::new(InMemory::new());
        Arc::new(ShardDb::builder("bench-spill", store).build().await.unwrap())
    });

    c.bench_function("spillable_arrangement_insert_spill", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut arr: SpillableArrangement<Vec<u8>, Vec<u8>> =
                    SpillableArrangement::new(Some(db.clone()), b"bench:".to_vec(), 100);
                for i in 0..10 {
                    let key = format!("k_{}", i).into_bytes();
                    let val = vec![0u8; 30];
                    let _ = arr.insert(key, val);
                }
            });
        });
    });
}

criterion_group!(benches, bench_spill_latency);
criterion_main!(benches);

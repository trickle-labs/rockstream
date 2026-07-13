//! Storage performance regression benchmark suite (v0.45.4, S3).
//!
//! Benches `ShardDb::get`/`put`/`merge` over `object_store::memory::InMemory`
//! — the existing, already LFS/MinIO-tested durability primitives — to guard
//! against latency regressions. No new durability path is introduced; this
//! measures existing code only.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use criterion::{BatchSize, Criterion, Throughput};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rockstream_storage::{MergeOperatorRegistry, ShardDb};
use tokio::runtime::Runtime;

fn rt() -> Runtime {
    Runtime::new().expect("build tokio runtime")
}

async fn open_warm_db(seed: u64) -> ShardDb {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = ShardDb::builder(format!("bench-storage-{seed}"), store)
        .build()
        .await
        .expect("build ShardDb");
    // Warm a fixed key so `get` measures a hit, not a miss.
    db.put(b"bench/warm-key", &[7u8; 64])
        .await
        .expect("warm put");
    db
}

fn bench_get(c: &mut Criterion) {
    let rt = rt();
    let db = rt.block_on(open_warm_db(1));

    let mut group = c.benchmark_group("storage_get");
    group.throughput(Throughput::Elements(1));
    group.bench_function("point_read_warm_key", |b| {
        b.to_async(&rt)
            .iter(|| async { db.get(b"bench/warm-key").await.unwrap() });
    });
    group.finish();
}

fn bench_put(c: &mut Criterion) {
    let rt = rt();
    let db = rt.block_on(open_warm_db(2));

    let mut group = c.benchmark_group("storage_put");
    group.throughput(Throughput::Elements(1));
    group.bench_function("point_write_fixed_size", |b| {
        b.to_async(&rt)
            .iter(|| async { db.put(b"bench/write-key", &[9u8; 64]).await.unwrap() });
    });
    group.finish();
}

fn bench_merge(c: &mut Criterion) {
    let rt = rt();
    let db = rt.block_on(open_warm_db(3));

    let mut group = c.benchmark_group("storage_merge");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sum_merge_operator", |b| {
        b.to_async(&rt).iter_batched(
            || (),
            |()| async {
                db.merge(b"bench/merge-key", &MergeOperatorRegistry::encode_sum(1))
                    .await
                    .unwrap()
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// See `rockstream-ops/src/bench_regression.rs` for the shared comparator
/// this feeds into via the `bench_regression_gate` binary. Kept as a small
/// local helper (not a cross-crate dependency) to avoid a
/// `rockstream-storage -> rockstream-ops` dev-dependency cycle, since
/// `rockstream-ops` already depends on `rockstream-storage`.
fn collect_criterion_summary(criterion_dir: &Path, groups: &[&str]) -> BTreeMap<String, f64> {
    fn walk(dir: &Path, id_prefix: &str, out: &mut BTreeMap<String, f64>) {
        let estimates_path = dir.join("new").join("estimates.json");
        if estimates_path.is_file() {
            if let Ok(text) = std::fs::read_to_string(&estimates_path) {
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                    if let Some(mean) = value
                        .get("mean")
                        .and_then(|m| m.get("point_estimate"))
                        .and_then(|p| p.as_f64())
                    {
                        out.insert(id_prefix.to_string(), mean);
                    }
                }
            }
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == "new" || name == "base" || name == "report" {
                continue;
            }
            walk(&path, &format!("{id_prefix}/{name}"), out);
        }
    }

    let mut out = BTreeMap::new();
    for group in groups {
        walk(&criterion_dir.join(group), group, &mut out);
    }
    out
}

fn default_criterion_dir() -> PathBuf {
    std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"))
        .join("criterion")
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    bench_get(&mut criterion);
    bench_put(&mut criterion);
    bench_merge(&mut criterion);
    criterion.final_summary();

    let summary = collect_criterion_summary(
        &default_criterion_dir(),
        &["storage_get", "storage_put", "storage_merge"],
    );
    println!(
        "[bench_summary:storage] {}",
        serde_json::to_string(&summary).unwrap()
    );
}

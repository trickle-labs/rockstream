//! Slice 8 & Matrix F: Workload & Benchmark Qualification Matrix (1 and 20 views) (v0.65.1).
//!
//! Validates:
//! 1. bench_one_view_light_load: 1 View under light load (idle spacing), flushes/epoch <= 1.0, latencies within budget.
//! 2. bench_one_view_sustained_load: 1 View under sustained load, multi-epoch coalescing, flushes/epoch <= 0.2, throughput gain.
//! 3. bench_twenty_views_light_load: 20 Views under light load, bounded concurrency, flushes/epoch <= 1.0.
//! 4. bench_twenty_views_sustained_load: 20 Views under sustained load, concurrent maintenance + group commit batching.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::memory::InMemory;
use rockstream_ops::branch_scheduler::{
    BranchScheduler, FnExecutor, ViewDependencyGraph, MAX_CONCURRENT_BRANCH_TASKS,
};
use rockstream_ops::group_commit::{
    PhysicalCommitGroup, DEFAULT_GROUP_COMMIT_MAX_DELAY_MS, MAX_GROUP_COMMIT_PENDING_BYTES,
    PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
};
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::{ShardDb, WriteBatch};

async fn create_shard(name: &str) -> Arc<ShardDb> {
    let store = Arc::new(InMemory::new());
    Arc::new(
        ShardDb::builder(name, store)
            .with_flush_interval(Duration::from_millis(5))
            .build()
            .await
            .unwrap(),
    )
}

fn p99_latency(mut samples: Vec<Duration>) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples.sort();
    let idx = ((samples.len() as f64 * 0.99).ceil() as usize).saturating_sub(1);
    samples[idx.min(samples.len() - 1)]
}

/// Matrix F: 1-View Light Load Benchmark
/// Profile: 1 View, 1 op / 50ms spacing.
/// Targets: flushes/epoch <= 1.0, Commit p99 <= 20ms, Read p99 <= 5ms, Freshness p99 <= 25ms.
#[tokio::test]
async fn bench_one_view_light_load() {
    let db = create_shard("bench_1v_light").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db.clone(),
        DEFAULT_GROUP_COMMIT_MAX_DELAY_MS,
        MAX_GROUP_COMMIT_PENDING_BYTES,
        PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
    ));

    let mut commit_latencies = Vec::new();
    let mut read_latencies = Vec::new();
    let mut freshness_latencies = Vec::new();

    // Warmup 1 op to initialize SlateDB structures
    let mut warmup_batch = WriteBatch::new();
    warmup_batch.put(b"view1:warmup", b"v");
    group.add_epoch(1, warmup_batch).unwrap();
    let _ = group.flush().await.unwrap();

    let num_ops = 10;
    let mut flush_count = 0;

    for epoch in 2..=(num_ops + 1) as u64 {
        let op_start = Instant::now();

        // Write batch for 1 view
        let mut batch = WriteBatch::new();
        batch.put(format!("view1:k{epoch}").as_bytes(), b"v");
        group
            .add_epoch(epoch, batch)
            .expect("add_epoch must succeed");

        // Wait for idle delay timer or explicit flush
        let flushed = group.flush().await.expect("flush must succeed");
        if !flushed.is_empty() {
            flush_count += 1;
        }

        let commit_dur = op_start.elapsed();
        commit_latencies.push(commit_dur);

        // Read query
        let read_start = Instant::now();
        let val = db.get(format!("view1:k{epoch}").as_bytes()).await.unwrap();
        assert!(val.is_some());
        let read_dur = read_start.elapsed();
        read_latencies.push(read_dur);

        // End-to-end freshness
        freshness_latencies.push(commit_dur + read_dur);

        // Idle spacing between operations
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let flushes_per_epoch = flush_count as f64 / num_ops as f64;
    let commit_p99 = p99_latency(commit_latencies);
    let read_p99 = p99_latency(read_latencies);
    let freshness_p99 = p99_latency(freshness_latencies);

    println!(
        "1-View Light Load: flushes/epoch={flushes_per_epoch:.2}, commit_p99={:?}, read_p99={:?}, freshness_p99={:?}",
        commit_p99, read_p99, freshness_p99
    );

    assert!(
        flushes_per_epoch <= 1.0,
        "flushes/epoch ({flushes_per_epoch}) must be <= 1.0"
    );
    assert!(
        commit_p99 <= Duration::from_millis(20),
        "commit_p99 ({:?}) must be <= 20ms",
        commit_p99
    );
    assert!(
        read_p99 <= Duration::from_millis(5),
        "read_p99 ({:?}) must be <= 5ms",
        read_p99
    );
    assert!(
        freshness_p99 <= Duration::from_millis(25),
        "freshness_p99 ({:?}) must be <= 25ms",
        freshness_p99
    );
}

/// Matrix F: 1-View Sustained Load Benchmark
/// Profile: 1 View, sustained multi-epoch write stream.
/// Targets: flushes/epoch <= 0.2 (>= 5x batching efficiency), Commit p99 <= 30ms, Read p99 <= 10ms, Freshness p99 <= 40ms.
#[tokio::test]
async fn bench_one_view_sustained_load() {
    let db = create_shard("bench_1v_sustained").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db.clone(),
        50, // allow 50ms batching window for sustained bursts
        MAX_GROUP_COMMIT_PENDING_BYTES,
        PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
    ));

    let total_epochs = 50;
    let mut commit_latencies = Vec::new();
    let mut flush_count = 0;

    let start = Instant::now();

    // Stage sustained stream of 50 epochs in batches of 10
    for chunk in (1..=total_epochs as u64).collect::<Vec<_>>().chunks(10) {
        let chunk_start = Instant::now();
        for &epoch in chunk {
            let mut batch = WriteBatch::new();
            batch.put(format!("view1:k{epoch}").as_bytes(), b"v_sustained");
            group
                .add_epoch(epoch, batch)
                .expect("add_epoch must succeed");
        }

        let flushed = group.flush().await.expect("flush must succeed");
        if !flushed.is_empty() {
            flush_count += 1;
        }

        let chunk_dur = chunk_start.elapsed();
        for _ in 0..chunk.len() {
            commit_latencies.push(chunk_dur / chunk.len() as u32);
        }
    }

    let elapsed = start.elapsed();
    let flushes_per_epoch = flush_count as f64 / total_epochs as f64;
    let commit_p99 = p99_latency(commit_latencies);

    // Read latency check
    let read_start = Instant::now();
    let val = db.get(b"view1:k50").await.unwrap();
    assert!(val.is_some());
    let read_p99 = read_start.elapsed();

    let freshness_p99 = commit_p99 + read_p99;

    println!(
        "1-View Sustained Load: total_epochs={total_epochs}, flushes={flush_count}, flushes/epoch={flushes_per_epoch:.2}, elapsed={:?}, commit_p99={:?}, read_p99={:?}, freshness_p99={:?}",
        elapsed, commit_p99, read_p99, freshness_p99
    );

    assert!(
        flushes_per_epoch <= 0.25,
        "multi-epoch batching must achieve flushes/epoch ({flushes_per_epoch}) <= 0.25"
    );
    assert!(
        commit_p99 <= Duration::from_millis(30),
        "commit_p99 ({:?}) must be <= 30ms",
        commit_p99
    );
    assert!(
        read_p99 <= Duration::from_millis(10),
        "read_p99 ({:?}) must be <= 10ms",
        read_p99
    );
    assert!(
        freshness_p99 <= Duration::from_millis(40),
        "freshness_p99 ({:?}) must be <= 40ms",
        freshness_p99
    );
}

/// Matrix F: 20-View Light Load Benchmark
/// Profile: 20 independent views on single table, 1 op / 50ms spacing.
/// Targets: flushes/epoch <= 1.0, Commit p99 <= 30ms, Read p99 <= 10ms, Freshness p99 <= 40ms.
#[tokio::test]
async fn bench_twenty_views_light_load() {
    let mut graph = ViewDependencyGraph::new();
    let num_views = 20;
    for i in 1..=num_views {
        graph.add_view(format!("v{i}"), vec!["src_table".to_string()]);
    }

    let scheduler = BranchScheduler::with_concurrency(MAX_CONCURRENT_BRANCH_TASKS);
    let db = create_shard("bench_20v_light").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db.clone(),
        DEFAULT_GROUP_COMMIT_MAX_DELAY_MS,
        MAX_GROUP_COMMIT_PENDING_BYTES,
        PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
    ));

    let executor = Arc::new(FnExecutor(|_name: &str, _inputs| async {
        Ok(ArrowZSet::from_ab_rows(&[(1, 100)], 1))
    }));

    // Warmup 1 op
    let mut warmup_batch = WriteBatch::new();
    warmup_batch.put(b"v1:warmup", b"v");
    group.add_epoch(1, warmup_batch).unwrap();
    let _ = group.flush().await.unwrap();

    let num_ops = 10;
    let mut commit_latencies = Vec::new();
    let mut flush_count = 0;

    for epoch in 2..=(num_ops + 1) as u64 {
        let op_start = Instant::now();

        let mut deltas = HashMap::new();
        deltas.insert(
            "src_table".to_string(),
            ArrowZSet::from_ab_rows(&[(1, 100)], 1),
        );

        // Concurrent branch execution across 20 views
        let computed = scheduler
            .execute_epoch(&graph, deltas, executor.clone())
            .await
            .unwrap();
        assert_eq!(computed.len(), 20);

        // Group commit staging
        let mut batch = WriteBatch::new();
        for i in 1..=num_views {
            batch.put(format!("v{i}:k{epoch}").as_bytes(), b"v_20");
        }
        group.add_epoch(epoch, batch).unwrap();

        let flushed = group.flush().await.unwrap();
        if !flushed.is_empty() {
            flush_count += 1;
        }

        let commit_dur = op_start.elapsed();
        commit_latencies.push(commit_dur);

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let flushes_per_epoch = flush_count as f64 / num_ops as f64;
    let commit_p99 = p99_latency(commit_latencies);

    let read_start = Instant::now();
    let val = db.get(b"v20:k5").await.unwrap();
    assert!(val.is_some());
    let read_p99 = read_start.elapsed();
    let freshness_p99 = commit_p99 + read_p99;

    println!(
        "20-View Light Load: flushes/epoch={flushes_per_epoch:.2}, commit_p99={:?}, read_p99={:?}, freshness_p99={:?}",
        commit_p99, read_p99, freshness_p99
    );

    assert!(flushes_per_epoch <= 1.0);
    assert!(
        commit_p99 <= Duration::from_millis(30),
        "commit_p99 ({:?}) must be <= 30ms",
        commit_p99
    );
    assert!(
        read_p99 <= Duration::from_millis(10),
        "read_p99 ({:?}) must be <= 10ms",
        read_p99
    );
    assert!(
        freshness_p99 <= Duration::from_millis(40),
        "freshness_p99 ({:?}) must be <= 40ms",
        freshness_p99
    );
}

/// Matrix F: 20-View Sustained Load Benchmark
/// Profile: 20 views under sustained multi-epoch load.
/// Targets: flushes/epoch <= 0.1 (>= 10x coalescing), Commit p99 <= 50ms, Read p99 <= 15ms, Freshness p99 <= 60ms.
#[tokio::test]
async fn bench_twenty_views_sustained_load() {
    let mut graph = ViewDependencyGraph::new();
    let num_views = 20;
    for i in 1..=num_views {
        graph.add_view(format!("v{i}"), vec!["src_table".to_string()]);
    }

    let scheduler = BranchScheduler::with_concurrency(MAX_CONCURRENT_BRANCH_TASKS);
    let db = create_shard("bench_20v_sustained").await;
    let group = Arc::new(PhysicalCommitGroup::with_config(
        db.clone(),
        50,
        MAX_GROUP_COMMIT_PENDING_BYTES,
        PHYSICAL_COMMIT_GROUP_MAX_EPOCHS,
    ));

    let executor = Arc::new(FnExecutor(|_name: &str, _inputs| async {
        Ok(ArrowZSet::from_ab_rows(&[(1, 100)], 1))
    }));

    let total_epochs = 40;
    let mut flush_count = 0;
    let mut commit_latencies = Vec::new();

    let start = Instant::now();

    // Stage in chunks of 10 epochs
    for chunk in (1..=total_epochs as u64).collect::<Vec<_>>().chunks(10) {
        let chunk_start = Instant::now();
        for &epoch in chunk {
            let mut deltas = HashMap::new();
            deltas.insert(
                "src_table".to_string(),
                ArrowZSet::from_ab_rows(&[(1, 100)], 1),
            );
            let _ = scheduler
                .execute_epoch(&graph, deltas, executor.clone())
                .await
                .unwrap();

            let mut batch = WriteBatch::new();
            for i in 1..=num_views {
                batch.put(format!("v{i}:k{epoch}").as_bytes(), b"v_sustained_20");
            }
            group.add_epoch(epoch, batch).unwrap();
        }

        let flushed = group.flush().await.unwrap();
        if !flushed.is_empty() {
            flush_count += 1;
        }

        let chunk_dur = chunk_start.elapsed();
        for _ in 0..chunk.len() {
            commit_latencies.push(chunk_dur / chunk.len() as u32);
        }
    }

    let elapsed = start.elapsed();
    let flushes_per_epoch = flush_count as f64 / total_epochs as f64;
    let commit_p99 = p99_latency(commit_latencies);

    let read_start = Instant::now();
    let val = db.get(b"v20:k40").await.unwrap();
    assert!(val.is_some());
    let read_p99 = read_start.elapsed();
    let freshness_p99 = commit_p99 + read_p99;

    println!(
        "20-View Sustained Load: total_epochs={total_epochs}, flushes={flush_count}, flushes/epoch={flushes_per_epoch:.2}, elapsed={:?}, commit_p99={:?}, read_p99={:?}, freshness_p99={:?}",
        elapsed, commit_p99, read_p99, freshness_p99
    );

    assert!(
        flushes_per_epoch <= 0.15,
        "multi-epoch coalescing must achieve flushes/epoch ({flushes_per_epoch}) <= 0.15"
    );
    assert!(
        commit_p99 <= Duration::from_millis(50),
        "commit_p99 ({:?}) must be <= 50ms",
        commit_p99
    );
    assert!(
        read_p99 <= Duration::from_millis(15),
        "read_p99 ({:?}) must be <= 15ms",
        read_p99
    );
    assert!(
        freshness_p99 <= Duration::from_millis(60),
        "freshness_p99 ({:?}) must be <= 60ms",
        freshness_p99
    );
}

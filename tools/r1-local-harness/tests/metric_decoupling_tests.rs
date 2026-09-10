use r1_local_harness::load::{calculate_p99, DecoupledLoadConfig, GeneratorQueue, LatencyRecord};
use std::time::Duration;

#[tokio::test]
async fn test_independent_load_schedule_not_completion_throttled() {
    // Independent schedule: generator pushes events at a fixed rate,
    // while the consumer is artificially delayed.
    // The generator should continue generating on its own clock.
    let config = DecoupledLoadConfig {
        target_rate_per_second: 500,
        max_queue_capacity: 10_000,
        measurement_duration: Duration::from_millis(200),
        warm_up_duration: Duration::from_millis(0),
        query_interval: Duration::from_millis(50),
    };

    let mut queue = GeneratorQueue::new(config.max_queue_capacity);
    let start = std::time::Instant::now();

    // Schedule 50 items independently with scheduled timestamps
    for i in 0..50 {
        let scheduled = start + Duration::from_millis(i * 2);
        let accepted = queue.try_enqueue(scheduled, format!("event_{i}"));
        assert!(accepted, "Queue with capacity 10000 should accept 50 items");
    }

    // Generator dispatched all 50 items regardless of consumption
    assert_eq!(queue.enqueued_count(), 50);
    assert_eq!(queue.overflow_drops(), 0);

    // Consumer is delayed, draining only 5 items
    tokio::time::sleep(Duration::from_millis(10)).await;
    let drained: Vec<_> = (0..5).filter_map(|_| queue.try_dequeue()).collect();
    assert_eq!(drained.len(), 5);

    // Remaining items are still in queue, not discarded or throttled
    assert_eq!(queue.current_len(), 45);
}

#[test]
fn test_read_commit_freshness_latency_separation() {
    let read_latencies = vec![1.0, 2.0, 3.0, 4.0, 5.0, 9.0, 10.0];
    let commit_latencies = vec![12.0, 15.0, 18.0, 20.0, 22.0, 24.0, 25.0];
    let freshness_latencies = vec![40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0];

    let read_p99 = calculate_p99(read_latencies.clone());
    let commit_p99 = calculate_p99(commit_latencies.clone());
    let freshness_p99 = calculate_p99(freshness_latencies.clone());

    let record = LatencyRecord {
        read_p99_ms: read_p99,
        commit_p99_ms: commit_p99,
        freshness_p99_ms: freshness_p99,
        read_latencies_ms: read_latencies,
        commit_latencies_ms: commit_latencies,
        freshness_latencies_ms: freshness_latencies,
    };

    // All three latencies must be distinct and adhere to their frozen targets
    assert!(
        record.read_p99_ms <= 10.0,
        "Read p99 must be <= 10.0ms: {}",
        record.read_p99_ms
    );
    assert!(
        record.commit_p99_ms <= 25.0,
        "Commit p99 must be <= 25.0ms: {}",
        record.commit_p99_ms
    );
    assert!(
        record.freshness_p99_ms <= 100.0,
        "Freshness p99 must be <= 100.0ms: {}",
        record.freshness_p99_ms
    );

    // Verify separation: read p99 != commit p99 != freshness p99
    assert_ne!(record.read_p99_ms, record.commit_p99_ms);
    assert_ne!(record.commit_p99_ms, record.freshness_p99_ms);
}

#[test]
fn test_generator_queue_bounds_and_drop_accounting() {
    let bounded_capacity = 100; // test with a smaller bound to trigger drops
    let mut queue = GeneratorQueue::new(bounded_capacity);
    let now = std::time::Instant::now();

    // Enqueue up to capacity
    for i in 0..bounded_capacity {
        let accepted = queue.try_enqueue(now, format!("event_{i}"));
        assert!(accepted, "Should accept up to bounded capacity");
    }
    assert_eq!(queue.current_len(), bounded_capacity);
    assert_eq!(queue.overflow_drops(), 0);

    // Overflow by 25 items
    for i in 0..25 {
        let accepted = queue.try_enqueue(now, format!("overflow_{i}"));
        assert!(!accepted, "Should reject when capacity is exceeded");
    }

    // Drops must be strictly accounted for
    assert_eq!(queue.current_len(), bounded_capacity);
    assert_eq!(queue.overflow_drops(), 25);
    assert_eq!(queue.total_offered(), bounded_capacity as u64 + 25);
}

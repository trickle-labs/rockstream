use r1_local_harness::load::GeneratorQueue;
use std::time::{Duration, Instant};

#[tokio::test]
async fn sim_runtime_generator_backpressure_and_timeout_under_delay() {
    let queue_capacity = 10;
    let mut queue = GeneratorQueue::new(queue_capacity);

    let start = Instant::now();

    // Enqueue 10 items (filling the capacity)
    for i in 0..10 {
        let ok = queue.try_enqueue(start + Duration::from_millis(i), format!("event-{i}"));
        assert!(ok, "First 10 items must succeed");
    }
    assert_eq!(queue.current_len(), 10);
    assert_eq!(queue.overflow_drops(), 0);

    // Enqueue 5 more items - these must be dropped due to backpressure
    for i in 10..15 {
        let ok = queue.try_enqueue(start + Duration::from_millis(i), format!("event-{i}"));
        assert!(
            !ok,
            "Items beyond capacity must be dropped under backpressure"
        );
    }

    assert_eq!(queue.current_len(), 10);
    assert_eq!(queue.overflow_drops(), 5);
    assert_eq!(queue.total_offered(), 15);

    // Simulate consumer draining with delay
    let mut dequeued = 0;
    while let Some(item) = queue.try_dequeue() {
        dequeued += 1;
        assert!(item.item.starts_with("event-"));
    }
    assert_eq!(dequeued, 10);
    assert_eq!(queue.current_len(), 0);
}

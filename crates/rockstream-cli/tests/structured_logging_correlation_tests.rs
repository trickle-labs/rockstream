//! Structured logging correlation tests (v0.71 V071-05, Slice 5, §4.5, §4.8).

use rockstream_types::logging::{
    current_log_context, enter_log_context, LogContext, LogRingBuffer, StructuredLogEvent,
    CURRENT_TASK_LOG_CONTEXT, MAX_STRUCTURED_LOG_EVENTS,
};

#[test]
fn test_structured_logging_seven_identifiers_propagation() {
    let ctx = LogContext::new()
        .with_request_id("req-12345")
        .with_operation_id("op-backfill-01")
        .with_workload_id("wl-default")
        .with_view_id("view-orders")
        .with_shard_id(7)
        .with_worker_id("worker-node-2")
        .with_epoch(42);

    assert_eq!(ctx.request_id.as_deref(), Some("req-12345"));
    assert_eq!(ctx.operation_id.as_deref(), Some("op-backfill-01"));
    assert_eq!(ctx.workload_id.as_deref(), Some("wl-default"));
    assert_eq!(ctx.view_id.as_deref(), Some("view-orders"));
    assert_eq!(ctx.shard_id, Some(7));
    assert_eq!(ctx.worker_id.as_deref(), Some("worker-node-2"));
    assert_eq!(ctx.epoch, Some(42));
    assert!(!ctx.is_empty());

    let map = ctx.as_map();
    assert_eq!(map.get("request_id").map(String::as_str), Some("req-12345"));
    assert_eq!(
        map.get("operation_id").map(String::as_str),
        Some("op-backfill-01")
    );
    assert_eq!(
        map.get("workload_id").map(String::as_str),
        Some("wl-default")
    );
    assert_eq!(map.get("view_id").map(String::as_str), Some("view-orders"));
    assert_eq!(map.get("shard_id").map(String::as_str), Some("7"));
    assert_eq!(
        map.get("worker_id").map(String::as_str),
        Some("worker-node-2")
    );
    assert_eq!(map.get("epoch").map(String::as_str), Some("42"));

    // Enter thread-local context scope
    {
        let _guard = enter_log_context(ctx.clone());
        let current = current_log_context();
        assert_eq!(current, ctx);

        let ring = LogRingBuffer::new(100);
        ring.log("INFO", "Executing shard IVM maintenance");

        let events = ring.events();
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.level, "INFO");
        assert_eq!(ev.message, "Executing shard IVM maintenance");
        assert_eq!(ev.context.request_id.as_deref(), Some("req-12345"));
        assert_eq!(ev.context.operation_id.as_deref(), Some("op-backfill-01"));
        assert_eq!(ev.context.workload_id.as_deref(), Some("wl-default"));
        assert_eq!(ev.context.view_id.as_deref(), Some("view-orders"));
        assert_eq!(ev.context.shard_id, Some(7));
        assert_eq!(ev.context.worker_id.as_deref(), Some("worker-node-2"));
        assert_eq!(ev.context.epoch, Some(42));
    }

    // Exited scope: thread-local context restored
    let restored = current_log_context();
    assert!(restored.is_empty());
}

#[tokio::test]
async fn test_structured_logging_async_task_propagation() {
    let ctx = LogContext::new()
        .with_request_id("req-async-999")
        .with_operation_id("op-stream-02")
        .with_workload_id("wl-analytics")
        .with_view_id("view-metrics")
        .with_shard_id(12)
        .with_worker_id("worker-compute-1")
        .with_epoch(101);

    let handle = CURRENT_TASK_LOG_CONTEXT.scope(ctx.clone(), async move {
        tokio::task::yield_now().await;
        let observed = current_log_context();
        assert_eq!(observed.request_id.as_deref(), Some("req-async-999"));
        assert_eq!(observed.operation_id.as_deref(), Some("op-stream-02"));
        assert_eq!(observed.workload_id.as_deref(), Some("wl-analytics"));
        assert_eq!(observed.view_id.as_deref(), Some("view-metrics"));
        assert_eq!(observed.shard_id, Some(12));
        assert_eq!(observed.worker_id.as_deref(), Some("worker-compute-1"));
        assert_eq!(observed.epoch, Some(101));
    });

    handle.await;
}

#[test]
fn test_log_correlation_ingest_delta() {
    let ctx = LogContext::new()
        .with_request_id("client-req-001")
        .with_workload_id("wl-ingest")
        .with_view_id("view-events")
        .with_shard_id(3)
        .with_worker_id("worker-ingest-0")
        .with_epoch(10);

    assert_eq!(ctx.request_id.as_deref(), Some("client-req-001"));
    assert_eq!(ctx.operation_id, None);
    assert_eq!(ctx.workload_id.as_deref(), Some("wl-ingest"));
    assert_eq!(ctx.view_id.as_deref(), Some("view-events"));
    assert_eq!(ctx.shard_id, Some(3));
    assert_eq!(ctx.worker_id.as_deref(), Some("worker-ingest-0"));
    assert_eq!(ctx.epoch, Some(10));
}

#[test]
fn test_log_correlation_epoch_assembly() {
    let ctx = LogContext::new()
        .with_operation_id("op-batch-assembly-55")
        .with_workload_id("wl-ingest")
        .with_view_id("view-events")
        .with_shard_id(3)
        .with_worker_id("worker-ingest-0")
        .with_epoch(11);

    assert_eq!(ctx.request_id, None);
    assert_eq!(ctx.operation_id.as_deref(), Some("op-batch-assembly-55"));
    assert_eq!(ctx.workload_id.as_deref(), Some("wl-ingest"));
    assert_eq!(ctx.view_id.as_deref(), Some("view-events"));
    assert_eq!(ctx.shard_id, Some(3));
    assert_eq!(ctx.worker_id.as_deref(), Some("worker-ingest-0"));
    assert_eq!(ctx.epoch, Some(11));
}

#[test]
fn test_log_correlation_ivm_maintenance() {
    let ctx = LogContext::new()
        .with_workload_id("wl-compute")
        .with_view_id("view-orders-agg")
        .with_shard_id(5)
        .with_worker_id("worker-compute-2")
        .with_epoch(20);

    assert_eq!(ctx.request_id, None);
    assert_eq!(ctx.operation_id, None);
    assert_eq!(ctx.workload_id.as_deref(), Some("wl-compute"));
    assert_eq!(ctx.view_id.as_deref(), Some("view-orders-agg"));
    assert_eq!(ctx.shard_id, Some(5));
    assert_eq!(ctx.worker_id.as_deref(), Some("worker-compute-2"));
    assert_eq!(ctx.epoch, Some(20));
}

#[test]
fn test_log_correlation_slatedb_commit() {
    let ctx = LogContext::new()
        .with_operation_id("op-commit-777")
        .with_workload_id("wl-storage")
        .with_view_id("view-orders-agg")
        .with_shard_id(5)
        .with_worker_id("worker-storage-1")
        .with_epoch(20);

    assert_eq!(ctx.request_id, None);
    assert_eq!(ctx.operation_id.as_deref(), Some("op-commit-777"));
    assert_eq!(ctx.workload_id.as_deref(), Some("wl-storage"));
    assert_eq!(ctx.view_id.as_deref(), Some("view-orders-agg"));
    assert_eq!(ctx.shard_id, Some(5));
    assert_eq!(ctx.worker_id.as_deref(), Some("worker-storage-1"));
    assert_eq!(ctx.epoch, Some(20));
}

#[test]
fn test_log_correlation_shard_migration() {
    let ctx = LogContext::new()
        .with_request_id("admin-req-migrate-1")
        .with_operation_id("op-migration-42")
        .with_workload_id("wl-critical")
        .with_view_id("view-orders-agg")
        .with_shard_id(5)
        .with_worker_id("worker-target-node")
        .with_epoch(25);

    assert_eq!(ctx.request_id.as_deref(), Some("admin-req-migrate-1"));
    assert_eq!(ctx.operation_id.as_deref(), Some("op-migration-42"));
    assert_eq!(ctx.workload_id.as_deref(), Some("wl-critical"));
    assert_eq!(ctx.view_id.as_deref(), Some("view-orders-agg"));
    assert_eq!(ctx.shard_id, Some(5));
    assert_eq!(ctx.worker_id.as_deref(), Some("worker-target-node"));
    assert_eq!(ctx.epoch, Some(25));
}

#[test]
fn test_log_correlation_view_recovery() {
    let ctx = LogContext::new()
        .with_request_id("recovery-req-009")
        .with_operation_id("op-restore-12")
        .with_workload_id("wl-critical")
        .with_view_id("view-orders-agg")
        .with_shard_id(5)
        .with_worker_id("worker-replay-0")
        .with_epoch(15);

    assert_eq!(ctx.request_id.as_deref(), Some("recovery-req-009"));
    assert_eq!(ctx.operation_id.as_deref(), Some("op-restore-12"));
    assert_eq!(ctx.workload_id.as_deref(), Some("wl-critical"));
    assert_eq!(ctx.view_id.as_deref(), Some("view-orders-agg"));
    assert_eq!(ctx.shard_id, Some(5));
    assert_eq!(ctx.worker_id.as_deref(), Some("worker-replay-0"));
    assert_eq!(ctx.epoch, Some(15));
}

#[test]
fn test_log_correlation_pgwire_query() {
    let ctx = LogContext::new()
        .with_request_id("client-query-101")
        .with_workload_id("wl-user-session")
        .with_view_id("view-summary")
        .with_worker_id("gateway-node-1")
        .with_epoch(30);

    assert_eq!(ctx.request_id.as_deref(), Some("client-query-101"));
    assert_eq!(ctx.operation_id, None);
    assert_eq!(ctx.workload_id.as_deref(), Some("wl-user-session"));
    assert_eq!(ctx.view_id.as_deref(), Some("view-summary"));
    assert_eq!(ctx.shard_id, None);
    assert_eq!(ctx.worker_id.as_deref(), Some("gateway-node-1"));
    assert_eq!(ctx.epoch, Some(30));
}

#[test]
fn test_bounds_log_ring_buffer_drop_oldest() {
    assert_eq!(MAX_STRUCTURED_LOG_EVENTS, 4096);
    let buffer = LogRingBuffer::new(MAX_STRUCTURED_LOG_EVENTS);
    assert_eq!(buffer.capacity(), 4096);
    assert_eq!(buffer.len(), 0);
    assert_eq!(buffer.dropped_count(), 0);
    assert_eq!(buffer.fill_ratio(), 0.0);

    // Push 4096 events
    for i in 0..4096 {
        let ctx = LogContext::new().with_request_id(format!("req-{i}"));
        buffer.push(StructuredLogEvent::new("DEBUG", format!("event {i}"), ctx));
    }
    assert_eq!(buffer.len(), 4096);
    assert_eq!(buffer.dropped_count(), 0);
    assert!((buffer.fill_ratio() - 1.0).abs() < 1e-6);

    // Push 10 additional events -> should drop the oldest 10
    for i in 4096..4106 {
        let ctx = LogContext::new().with_request_id(format!("req-{i}"));
        buffer.push(StructuredLogEvent::new("DEBUG", format!("event {i}"), ctx));
    }
    assert_eq!(buffer.len(), 4096);
    assert_eq!(buffer.dropped_count(), 10);

    let events = buffer.events();
    assert_eq!(events.len(), 4096);
    // Oldest remaining should be req-10
    assert_eq!(events[0].context.request_id.as_deref(), Some("req-10"));
    // Newest should be req-4105
    assert_eq!(events[4095].context.request_id.as_deref(), Some("req-4105"));
}

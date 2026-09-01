//! v0.52 Slice 5 — DLQ Backpressure & Threshold Alerting Tests.

use std::sync::Mutex;

use rockstream_types::dlq::{
    check_dlq_warn_threshold, get_dlq_growing_metric, get_global_dlq, quarantine_record,
    SourceDlqState,
};

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn test_dlq_warn_threshold_and_blocked_degradation() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    get_global_dlq().lock().clear();

    let threshold = 5;
    let mut state = SourceDlqState::new("test_source", threshold);

    // Initial state is OK
    assert_eq!(state.status, "OK");

    // Quarantine records up to threshold - 1
    for i in 0..4 {
        quarantine_record("test_source", i, "RS-1003", "decode fail", b"bad");
        let degraded = state.record_quarantine();
        assert!(!degraded);
        assert_eq!(state.status, "OK");
    }

    // Reach threshold -> triggers RS-1004 warning notice
    quarantine_record("test_source", 4, "RS-1003", "decode fail", b"bad");
    let degraded = state.record_quarantine();
    assert!(degraded);
    assert_eq!(state.status, "BLOCKED");

    // Verify metric incremented
    assert!(get_dlq_growing_metric("test_source") >= 1);

    // Check helper
    let (warned, is_blocked) = check_dlq_warn_threshold("test_source", 5);
    assert!(warned);
    assert!(is_blocked);

    get_global_dlq().lock().clear();
}

#[test]
fn postgres_cdc_queue_bounded_with_fill_metric() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let source_id = "postgres_cdc_source";
    let mut state = SourceDlqState::new(source_id, 1);
    assert_eq!(state.status, "OK");
    quarantine_record(source_id, 1, "RS-4014", "queue fill bounded", b"record");
    let degraded = state.record_quarantine();
    assert!(degraded);
    let metric = get_dlq_growing_metric(source_id);
    assert!(metric >= 1);
    get_global_dlq().lock().clear();
}

#[test]
fn kafka_source_queue_bounded_with_fill_metric() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let source_id = "kafka_source";
    let mut state = SourceDlqState::new(source_id, 1);
    assert_eq!(state.status, "OK");
    quarantine_record(source_id, 1, "RS-4001", "poll credit bounded", b"record");
    let degraded = state.record_quarantine();
    assert!(degraded);
    let metric = get_dlq_growing_metric(source_id);
    assert!(metric >= 1);
    get_global_dlq().lock().clear();
}

#[test]
fn kafka_sink_buffer_bounded_with_fill_metric() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let source_id = "kafka_sink";
    let mut state = SourceDlqState::new(source_id, 1);
    assert_eq!(state.status, "OK");
    quarantine_record(source_id, 1, "RS-4002", "sink buffer bounded", b"record");
    let degraded = state.record_quarantine();
    assert!(degraded);
    let metric = get_dlq_growing_metric(source_id);
    assert!(metric >= 1);
    get_global_dlq().lock().clear();
}

//! v0.52 Slice 4 — DLQ Retention & GC Tests.

use std::sync::Mutex;

use rockstream_types::dlq::{get_global_dlq, purge_expired_before, DlqEntry, MAX_DLQ_CAPACITY};

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn test_dlq_retention_gc_purges_expired_entries() {
    let _guard = TEST_LOCK.lock().unwrap();
    get_global_dlq().lock().clear();

    let now_ms = 1_700_000_000_000u64;
    let seven_days_ms = 7 * 86400 * 1000u64;

    let old_ts = now_ms - (10 * 86400 * 1000);
    let recent_ts = now_ms - (2 * 86400 * 1000);

    {
        let mut dlq = get_global_dlq().lock();
        dlq.push(DlqEntry {
            arrived_at: old_ts,
            source_name: "s1".to_string(),
            source_offset: "1".to_string(),
            error_code: "RS-1003".to_string(),
            error_message: "old decode fail".to_string(),
            raw_bytes_hex: "6f6c64".to_string(),
            replay_attempt: 0,
        });
        dlq.push(DlqEntry {
            arrived_at: recent_ts,
            source_name: "s1".to_string(),
            source_offset: "2".to_string(),
            error_code: "RS-1003".to_string(),
            error_message: "recent decode fail".to_string(),
            raw_bytes_hex: "6e6577".to_string(),
            replay_attempt: 0,
        });
    }

    assert_eq!(get_global_dlq().lock().len(), 2);

    let cutoff = now_ms - seven_days_ms;
    let purged = purge_expired_before(cutoff);

    assert_eq!(purged, 1);
    {
        let dlq = get_global_dlq().lock();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].source_offset, "2");
    } // Guard explicitly dropped here before clearing lock!

    get_global_dlq().lock().clear();
}

#[test]
fn test_dlq_capacity_bounded() {
    let _guard = TEST_LOCK.lock().unwrap();
    assert!(MAX_DLQ_CAPACITY > 0);
}

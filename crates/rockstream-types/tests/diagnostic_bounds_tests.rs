use rockstream_types::diagnostic::{
    diagnostic_metrics, global_diagnostic_journal, record_diagnostic, DiagnosticOccurrence,
    MAX_DIAGNOSTIC_CONTEXT_ENTRIES, MAX_DIAGNOSTIC_OCCURRENCES,
};
use rockstream_types::error_code::RS_2018;
use std::sync::Mutex;
use uuid::Uuid;

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn journal_and_context_bounds_have_exact_output() {
    let _guard = TEST_LOCK.lock().unwrap();
    global_diagnostic_journal().lock().clear();
    let before = diagnostic_metrics();
    let context = (0..=MAX_DIAGNOSTIC_CONTEXT_ENTRIES)
        .map(|index| (format!("key{index}"), "value".to_string()))
        .collect::<Vec<_>>();
    assert!(DiagnosticOccurrence::new(RS_2018, Uuid::nil(), context, None, None).is_err());

    for index in 0..=MAX_DIAGNOSTIC_OCCURRENCES {
        record_diagnostic(
            DiagnosticOccurrence::new(RS_2018, Uuid::from_u128(index as u128 + 1), [], None, None)
                .unwrap(),
        );
    }

    let after = diagnostic_metrics();
    assert_eq!(
        after.diagnostic_context_rejected_total - before.diagnostic_context_rejected_total,
        1
    );
    assert_eq!(
        after.diagnostic_occurrences_evicted_total - before.diagnostic_occurrences_evicted_total,
        1
    );
    assert_eq!(
        after.rockstream_diagnostic_occurrences_retained,
        MAX_DIAGNOSTIC_OCCURRENCES
    );
    assert_eq!(
        serde_json::to_string(&global_diagnostic_journal().lock().recent(1)).unwrap(),
        format!(
            "[{{\"code\":\"RS-2018\",\"correlation_id\":\"{}\",\"message\":\"Published frontier exceeded the session max_staleness bound; query proceeded\",\"context\":{{}},\"retry_after\":null}}]",
            Uuid::from_u128((MAX_DIAGNOSTIC_OCCURRENCES + 1) as u128)
        )
    );
}

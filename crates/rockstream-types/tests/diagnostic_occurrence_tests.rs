use rockstream_types::diagnostic::{DiagnosticOccurrence, MAX_DIAGNOSTIC_CONTEXT_ENTRIES};
use rockstream_types::error_code::{RS_2004, RS_2018};
use std::time::Duration;
use uuid::Uuid;

#[test]
fn descriptor_reference_resolves_exact_catalog_record() {
    let occurrence = DiagnosticOccurrence::new(
        RS_2018,
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        [],
        None,
        None,
    )
    .unwrap();
    let descriptor = occurrence.descriptor().unwrap();
    assert_eq!(descriptor.code, RS_2018);
    assert_eq!(descriptor.key, "session.max_staleness_exceeded");
    assert_eq!(
        descriptor.title,
        "Published frontier exceeded the session max_staleness bound; query proceeded"
    );
    assert_eq!(descriptor.sqlstate, "01000");
}

#[test]
fn preserves_direct_causal_occurrence_exactly() {
    let cause = DiagnosticOccurrence::new(
        RS_2018,
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        [("view".to_string(), "orders_mv".to_string())],
        None,
        None,
    )
    .unwrap();
    let occurrence = DiagnosticOccurrence::new(
        RS_2004,
        Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap(),
        [],
        Some(Duration::from_secs(2)),
        Some(cause),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_string(&occurrence).unwrap(),
        r#"{"code":"RS-2004","correlation_id":"22222222-2222-4222-8222-222222222222","message":"Cannot drop inline view: dependent materialized views still exist","context":{},"retry_after":2000,"cause":{"code":"RS-2018","correlation_id":"11111111-1111-4111-8111-111111111111","message":"Published frontier exceeded the session max_staleness bound; query proceeded (view=orders_mv)","context":{"view":"orders_mv"},"retry_after":null}}"#
    );
}

#[test]
fn context_bounds_reject_extra_entries() {
    let context = (0..=MAX_DIAGNOSTIC_CONTEXT_ENTRIES)
        .map(|index| (format!("key{index}"), "value".to_string()))
        .collect::<Vec<_>>();
    assert!(DiagnosticOccurrence::new(RS_2018, Uuid::nil(), context, None, None,).is_err());
}

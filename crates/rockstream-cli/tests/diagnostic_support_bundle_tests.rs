use rockstream_cli::output::{DiagnosticSupportInfo, OutputFormat, SupportBundleInfo};
use rockstream_cli::transport::StorageClient;
use rockstream_cli::{run_support_bundle, run_support_diagnose};
use rockstream_types::audit::AuditEvent;
use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::diagnostic::{
    global_diagnostic_journal, record_diagnostic, DiagnosticOccurrence,
};
use rockstream_types::error_code::{RS_0003, RS_2018};
use serde_json::json;
use std::sync::Mutex;
use uuid::Uuid;

static TEST_LOCK: Mutex<()> = Mutex::new(());
const BUNDLE_TIME_MS: u64 = 1_724_000_000_000;

fn occurrence() -> DiagnosticOccurrence {
    let cause = DiagnosticOccurrence::new(
        RS_2018,
        Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap(),
        [(
            "query".to_string(),
            "SELECT * FROM accounts WHERE id = 'support-fixture-cause-literal'".to_string(),
        )],
        None,
        None,
    )
    .unwrap();
    DiagnosticOccurrence::new(
        RS_2018,
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        [
            ("view".to_string(), "orders_mv".to_string()),
            (
                "sql".to_string(),
                "SELECT * FROM accounts WHERE email = 'support-fixture-sql-literal'".to_string(),
            ),
            (
                "connector_payload".to_string(),
                r#"{"customer":"support-fixture-connector-payload"}"#.to_string(),
            ),
        ],
        None,
        Some(cause),
    )
    .unwrap()
}

fn expected_bundle(occurrence: &DiagnosticOccurrence) -> serde_json::Value {
    json!({
        "generated_at_ms": BUNDLE_TIME_MS,
        "candidate_identity": CandidateIdentity::current(),
        "view": null,
        "audit_events": [],
        "diagnostic_occurrences": [occurrence],
        "redaction": "secret values are never included; only metadata and audit events are exported"
    })
}

#[test]
fn support_diagnose_exact_bundle() {
    let _guard = TEST_LOCK.lock().unwrap();
    global_diagnostic_journal().lock().clear();
    let mut raw_occurrence = occurrence();
    raw_occurrence.context.insert(
        "detail".to_string(),
        "password=support-fixture-secret".to_string(),
    );
    record_diagnostic(raw_occurrence);
    let expected_occurrence = DiagnosticOccurrence::new(
        RS_2018,
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        [],
        None,
        Some(
            DiagnosticOccurrence::new(
                RS_2018,
                Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap(),
                [],
                None,
                None,
            )
            .unwrap(),
        ),
    )
    .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let output_path = temp.path().join("diagnostic.json");
    let expected_bundle = expected_bundle(&expected_occurrence);
    let expected_bytes = serde_json::to_vec_pretty(&expected_bundle).unwrap();
    let expected_output = serde_json::to_string_pretty(&DiagnosticSupportInfo {
        occurrence: expected_occurrence.clone(),
        bundle: SupportBundleInfo {
            bundle_path: output_path.to_string_lossy().into_owned(),
            view: None,
            size_bytes: expected_bytes.len() as u64,
            redacted_secrets_count: 0,
            generated_at_ms: BUNDLE_TIME_MS,
        },
    })
    .unwrap();

    let actual_output = run_support_diagnose(
        OutputFormat::Json,
        &StorageClient::new().with_support_bundle_time_ms(BUNDLE_TIME_MS),
        temp.path(),
        Some("RS-2018"),
        None,
        Some(&output_path),
    )
    .unwrap();
    let actual_bytes = std::fs::read(&output_path).unwrap();
    assert_eq!(actual_output, expected_output);
    assert_eq!(actual_bytes, expected_bytes);
    for forbidden in [
        "support-fixture-sql-literal",
        "support-fixture-connector-payload",
    ] {
        assert!(!actual_output.contains(forbidden));
        assert!(!String::from_utf8_lossy(&actual_bytes).contains(forbidden));
    }

    assert_eq!(
        run_support_diagnose(
            OutputFormat::Text,
            &StorageClient::new().with_support_bundle_time_ms(BUNDLE_TIME_MS),
            temp.path(),
            None,
            Some("11111111-1111-4111-8111-111111111111"),
            Some(&output_path),
        )
        .unwrap(),
        format!(
            "{}\nSupport bundle generated at {}",
            expected_occurrence.render_text(),
            output_path.display()
        )
    );
}

#[test]
fn failed_support_bundle_write_has_exact_failure_audit() {
    let _guard = TEST_LOCK.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let storage = StorageClient::new().with_support_bundle_time_ms(BUNDLE_TIME_MS);

    let error = storage
        .generate_support_bundle(temp.path(), None, None, Some(temp.path()))
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!(
            "{RS_0003} failed to write support bundle\n  next steps: Check that the output path is writable and retry."
        )
    );
    let events = storage.audit_tail(temp.path(), 10).unwrap();
    let expected_event = AuditEvent {
        timestamp_ms: events[0].timestamp_ms,
        actor: "admin".to_string(),
        action: "support.bundle".to_string(),
        resource: "all".to_string(),
        error_code: Some(RS_0003.to_string()),
        detail: Some("support bundle generation failed".to_string()),
    };
    assert_eq!(events, vec![expected_event]);
}

#[test]
fn support_bundle_returns_error_when_its_audit_cannot_be_persisted() {
    let _guard = TEST_LOCK.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let output_path = temp.path().join("support.json");
    std::fs::create_dir(temp.path().join("audit.jsonl")).unwrap();
    let storage = StorageClient::new().with_support_bundle_time_ms(BUNDLE_TIME_MS);
    let expected_bytes = serde_json::to_vec_pretty(&json!({
        "generated_at_ms": BUNDLE_TIME_MS,
        "candidate_identity": CandidateIdentity::current(),
        "view": null,
        "audit_events": [],
        "diagnostic_occurrences": [],
        "redaction": "secret values are never included; only metadata and audit events are exported"
    }))
    .unwrap();

    let error = storage
        .generate_support_bundle(temp.path(), None, None, Some(&output_path))
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        format!(
            "{RS_0003} support bundle audit could not be persisted\n  next steps: Check durable audit storage before retrying."
        )
    );
    assert_eq!(std::fs::read(&output_path).unwrap(), expected_bytes);
}

#[test]
fn support_bundle_sanitizes_diagnostics_in_exact_artifact() {
    let _guard = TEST_LOCK.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let output_path = temp.path().join("diagnostic.json");
    let mut raw_occurrence = occurrence();
    raw_occurrence.context.insert(
        "detail".to_string(),
        "password=support-fixture-secret".to_string(),
    );
    let expected_occurrence = DiagnosticOccurrence::new(
        RS_2018,
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        [],
        None,
        Some(
            DiagnosticOccurrence::new(
                RS_2018,
                Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap(),
                [],
                None,
                None,
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let expected_bytes = serde_json::to_vec_pretty(&expected_bundle(&expected_occurrence)).unwrap();
    let storage = StorageClient::new().with_support_bundle_time_ms(BUNDLE_TIME_MS);

    assert_eq!(
        storage
            .generate_support_bundle_with_diagnostics(
                temp.path(),
                None,
                None,
                Some(&output_path),
                &[raw_occurrence],
            )
            .unwrap(),
        SupportBundleInfo {
            bundle_path: output_path.to_string_lossy().into_owned(),
            view: None,
            size_bytes: expected_bytes.len() as u64,
            redacted_secrets_count: 1,
            generated_at_ms: BUNDLE_TIME_MS,
        }
    );
    assert_eq!(std::fs::read(&output_path).unwrap(), expected_bytes);
}

#[test]
fn support_bundle_redacts_view_metadata_in_exact_output_artifact_and_audit() {
    let _guard = TEST_LOCK.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let output_path = temp.path().join("support.json");
    let storage = StorageClient::new().with_support_bundle_time_ms(BUNDLE_TIME_MS);
    let safe_view = "selected";
    let expected_bundle = json!({
        "generated_at_ms": BUNDLE_TIME_MS,
        "candidate_identity": CandidateIdentity::current(),
        "view": safe_view,
        "audit_events": [],
        "diagnostic_occurrences": [],
        "redaction": "secret values are never included; only metadata and audit events are exported"
    });
    let expected_bytes = serde_json::to_vec_pretty(&expected_bundle).unwrap();
    let expected_output = serde_json::to_string_pretty(&SupportBundleInfo {
        bundle_path: output_path.to_string_lossy().into_owned(),
        view: Some(safe_view.to_string()),
        size_bytes: expected_bytes.len() as u64,
        redacted_secrets_count: 1,
        generated_at_ms: BUNDLE_TIME_MS,
    })
    .unwrap();

    assert_eq!(
        run_support_bundle(
            OutputFormat::Json,
            &storage,
            temp.path(),
            Some("Authorization: Basic dXNlcjpwYXNz"),
            None,
            Some(&output_path),
        )
        .unwrap(),
        expected_output
    );
    assert_eq!(std::fs::read(&output_path).unwrap(), expected_bytes);

    let events = storage.audit_tail(temp.path(), 10).unwrap();
    let expected_event = AuditEvent {
        timestamp_ms: events[0].timestamp_ms,
        actor: "admin".to_string(),
        action: "support.bundle".to_string(),
        resource: safe_view.to_string(),
        error_code: None,
        detail: Some("support bundle generated".to_string()),
    };
    assert_eq!(events, vec![expected_event]);
}

#[test]
fn support_bundle_redacts_nested_json_secrets_in_exact_output_artifact_and_audit() {
    let _guard = TEST_LOCK.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let output_path = temp.path().join("support.json");
    let storage = StorageClient::new().with_support_bundle_time_ms(BUNDLE_TIME_MS);
    let raw_view = r#"{"token":{"value":"fixture-secret-value","backup":"another-secret"}}"#;
    let safe_view = "selected";
    let expected_bundle = json!({
        "generated_at_ms": BUNDLE_TIME_MS,
        "candidate_identity": CandidateIdentity::current(),
        "view": safe_view,
        "audit_events": [],
        "diagnostic_occurrences": [],
        "redaction": "secret values are never included; only metadata and audit events are exported"
    });
    let expected_bytes = serde_json::to_vec_pretty(&expected_bundle).unwrap();
    let expected_output = serde_json::to_string_pretty(&SupportBundleInfo {
        bundle_path: output_path.to_string_lossy().into_owned(),
        view: Some(safe_view.to_string()),
        size_bytes: expected_bytes.len() as u64,
        redacted_secrets_count: 1,
        generated_at_ms: BUNDLE_TIME_MS,
    })
    .unwrap();

    assert_eq!(
        run_support_bundle(
            OutputFormat::Json,
            &storage,
            temp.path(),
            Some(raw_view),
            None,
            Some(&output_path),
        )
        .unwrap(),
        expected_output
    );
    assert_eq!(std::fs::read(&output_path).unwrap(), expected_bytes);

    let events = storage.audit_tail(temp.path(), 10).unwrap();
    let expected_event = AuditEvent {
        timestamp_ms: events[0].timestamp_ms,
        actor: "admin".to_string(),
        action: "support.bundle".to_string(),
        resource: safe_view.to_string(),
        error_code: None,
        detail: Some("support bundle generated".to_string()),
    };
    assert_eq!(events, vec![expected_event]);
}

#[test]
fn support_bundle_redacts_private_key_material_in_exact_output_artifact_and_audit() {
    let _guard = TEST_LOCK.lock().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let output_path = temp.path().join("support.json");
    let storage = StorageClient::new().with_support_bundle_time_ms(BUNDLE_TIME_MS);
    let raw_view =
        "private_key=-----BEGIN PRIVATE KEY-----\nfixture-key-material\n-----END PRIVATE KEY-----";
    let safe_view = "selected";
    let expected_bundle = json!({
        "generated_at_ms": BUNDLE_TIME_MS,
        "candidate_identity": CandidateIdentity::current(),
        "view": safe_view,
        "audit_events": [],
        "diagnostic_occurrences": [],
        "redaction": "secret values are never included; only metadata and audit events are exported"
    });
    let expected_bytes = serde_json::to_vec_pretty(&expected_bundle).unwrap();
    let expected_output = serde_json::to_string_pretty(&SupportBundleInfo {
        bundle_path: output_path.to_string_lossy().into_owned(),
        view: Some(safe_view.to_string()),
        size_bytes: expected_bytes.len() as u64,
        redacted_secrets_count: 1,
        generated_at_ms: BUNDLE_TIME_MS,
    })
    .unwrap();

    let actual_output = run_support_bundle(
        OutputFormat::Json,
        &storage,
        temp.path(),
        Some(raw_view),
        None,
        Some(&output_path),
    )
    .unwrap();
    let actual_bytes = std::fs::read(&output_path).unwrap();
    assert_eq!(actual_output, expected_output);
    assert_eq!(actual_bytes, expected_bytes);
    assert!(!actual_output.contains("fixture-key-material"));
    assert!(!String::from_utf8_lossy(&actual_bytes).contains("fixture-key-material"));

    let events = storage.audit_tail(temp.path(), 10).unwrap();
    let expected_event = AuditEvent {
        timestamp_ms: events[0].timestamp_ms,
        actor: "admin".to_string(),
        action: "support.bundle".to_string(),
        resource: safe_view.to_string(),
        error_code: None,
        detail: Some("support bundle generated".to_string()),
    };
    assert_eq!(events, vec![expected_event]);
}

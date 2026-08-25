use rockstream_cli::output::{DiagnosticSupportInfo, OutputFormat, SupportBundleInfo};
use rockstream_cli::run_support_diagnose;
use rockstream_cli::transport::StorageClient;
use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::diagnostic::{
    global_diagnostic_journal, record_diagnostic, DiagnosticOccurrence,
};
use rockstream_types::error_code::RS_2018;
use serde_json::json;
use std::sync::Mutex;
use uuid::Uuid;

static TEST_LOCK: Mutex<()> = Mutex::new(());
const BUNDLE_TIME_MS: u64 = 1_724_000_000_000;

fn occurrence() -> DiagnosticOccurrence {
    DiagnosticOccurrence::new(
        RS_2018,
        Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        [("view".to_string(), "orders_mv".to_string())],
        None,
        None,
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
    let expected_occurrence = occurrence();
    record_diagnostic(expected_occurrence.clone());

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
            redacted_secrets_count: 1,
            generated_at_ms: BUNDLE_TIME_MS,
        },
    })
    .unwrap();

    assert_eq!(
        run_support_diagnose(
            OutputFormat::Json,
            &StorageClient::new().with_support_bundle_time_ms(BUNDLE_TIME_MS),
            temp.path(),
            Some("RS-2018"),
            None,
            Some(&output_path),
        )
        .unwrap(),
        expected_output
    );
    assert_eq!(std::fs::read(&output_path).unwrap(), expected_bytes);

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

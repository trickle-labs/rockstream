//! v0.59.4 Slice 1 — Uniform CLI Output Contract Tests (CLI-01)

use rockstream_cli::output::{
    render_error, render_json_lines, render_output, CliErrorEnvelope, OutputFormat, SubscribeEvent,
    ViewSummary,
};
use rockstream_cli::CliError;
use rockstream_types::audit::AuditEvent;
use rockstream_types::error_code::RS_0002;

#[test]
fn test_view_commands_json_output() {
    let views = vec![ViewSummary {
        name: "active_users".to_string(),
        state: "RUNNING".to_string(),
        workload: Some("analytics".to_string()),
        freshness_slo_ms: Some(500),
        memory_limit_bytes: Some(1048576),
        depends_on: vec!["users".to_string()],
    }];

    let text_out = render_output(&views, OutputFormat::Text);
    assert!(text_out.contains("active_users"));
    assert!(text_out.contains("analytics"));

    let json_out = render_output(&views, OutputFormat::Json);
    let parsed: Vec<ViewSummary> = serde_json::from_str(&json_out).expect("Valid JSON");
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].name, "active_users");
}

#[test]
fn test_structured_error_rendering_json_and_text() {
    let err = CliError::new(
        RS_0002,
        "invalid configuration option",
        "Check rockstream.toml syntax.",
    );

    let text_err = render_error(&err, OutputFormat::Text);
    assert!(text_err.contains("RS-0002"));
    assert!(text_err.contains("invalid configuration option"));
    assert!(text_err.contains("Check rockstream.toml syntax."));

    let json_err = render_error(&err, OutputFormat::Json);
    let parsed: CliErrorEnvelope =
        serde_json::from_str(&json_err).expect("Valid JSON error envelope");
    assert_eq!(parsed.code, "RS-0002");
    assert_eq!(parsed.message, "invalid configuration option");
    assert_eq!(parsed.next_steps, "Check rockstream.toml syntax.");
    assert!(!parsed.retryable);
    assert_eq!(
        parsed.documentation_url.as_deref(),
        Some("https://rockstream.dev/docs/errors#RS-0002")
    );
}

#[test]
fn test_audit_tail_json_lines_streaming() {
    let event1 = AuditEvent::now("alice", "view.create", "orders_mv");
    let event2 = AuditEvent::now("bob", "workload.create", "etl_high");
    let events = vec![event1.clone(), event2.clone()];

    let json_lines = render_json_lines(&events);
    let lines: Vec<&str> = json_lines.lines().collect();
    assert_eq!(lines.len(), 2);

    let parsed1: AuditEvent = serde_json::from_str(lines[0]).expect("Line 1 valid JSON");
    let parsed2: AuditEvent = serde_json::from_str(lines[1]).expect("Line 2 valid JSON");

    assert_eq!(parsed1.actor, "alice");
    assert_eq!(parsed1.resource, "orders_mv");
    assert_eq!(parsed2.actor, "bob");
    assert_eq!(parsed2.resource, "etl_high");
}

#[test]
fn test_view_subscribe_json_lines_streaming() {
    let event1 = SubscribeEvent {
        epoch: 10,
        view_name: "sales_by_store".to_string(),
        diff_type: "INSERT".to_string(),
        key: "store_100".to_string(),
        row: serde_json::json!({"store_id": 100, "total": 120}),
    };
    let event2 = SubscribeEvent {
        epoch: 11,
        view_name: "sales_by_store".to_string(),
        diff_type: "UPDATE".to_string(),
        key: "store_100".to_string(),
        row: serde_json::json!({"store_id": 100, "total": 170}),
    };
    let events = vec![event1, event2];

    let json_lines = render_json_lines(&events);
    let lines: Vec<&str> = json_lines.lines().collect();
    assert_eq!(lines.len(), 2);

    let parsed1: SubscribeEvent = serde_json::from_str(lines[0]).expect("Line 1 valid JSON");
    let parsed2: SubscribeEvent = serde_json::from_str(lines[1]).expect("Line 2 valid JSON");

    assert_eq!(parsed1.epoch, 10);
    assert_eq!(parsed1.diff_type, "INSERT");
    assert_eq!(parsed2.epoch, 11);
    assert_eq!(parsed2.diff_type, "UPDATE");
}

//! Integration tests for `rockstream doctor` diagnostic command (`UX-02`).

use std::path::PathBuf;
use std::time::Duration;

use rockstream_cli::doctor::{
    redact_secrets, run_doctor, run_doctor_checks, DiagnosticStatus, DoctorOptions, DoctorReport,
};
use rockstream_cli::output::OutputFormat;
use tempfile::TempDir;

#[tokio::test]
async fn test_check_candidate_identity() {
    let opts = DoctorOptions::default();
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "binary.candidate_identity")
        .expect("binary.candidate_identity check must exist");

    assert_eq!(check.status, DiagnosticStatus::Pass);
    assert_eq!(check.category, "binary");
    assert!(check.summary.contains("Version"));
    assert!(check.summary.contains("sha:"));
}

#[tokio::test]
async fn test_check_config_diagnostics() {
    let opts = DoctorOptions::default();
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "config.parse_and_semantic")
        .expect("config.parse_and_semantic check must exist");

    assert_eq!(check.status, DiagnosticStatus::Pass);
    assert_eq!(check.category, "config");
}

#[tokio::test]
async fn test_check_system_monotonic_clock() {
    let opts = DoctorOptions::default();
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "system.monotonic_clock")
        .expect("system.monotonic_clock check must exist");

    assert_eq!(check.status, DiagnosticStatus::Pass);
}

#[tokio::test]
async fn test_check_system_temp_directory() {
    let opts = DoctorOptions::default();
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "system.temp_directory")
        .expect("system.temp_directory check must exist");

    assert_eq!(check.status, DiagnosticStatus::Pass);
}

#[tokio::test]
async fn test_check_system_file_descriptors() {
    let opts = DoctorOptions::default();
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "system.file_descriptor_limit")
        .expect("system.file_descriptor_limit check must exist");

    assert!(check.status == DiagnosticStatus::Pass || check.status == DiagnosticStatus::Warn);
}

#[tokio::test]
async fn test_check_system_memory() {
    let opts = DoctorOptions::default();
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "system.memory_available")
        .expect("system.memory_available check must exist");

    assert!(check.status == DiagnosticStatus::Pass || check.status == DiagnosticStatus::Warn);
}

#[tokio::test]
async fn test_check_system_os_arch() {
    let opts = DoctorOptions::default();
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "system.os_arch_supported")
        .expect("system.os_arch_supported check must exist");

    assert_eq!(check.status, DiagnosticStatus::Pass);
}

#[tokio::test]
async fn test_check_storage_path() {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap().to_string();

    let opts = DoctorOptions {
        storage: Some(storage_path),
        ..DoctorOptions::default()
    };
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "storage.path_or_url_valid")
        .expect("storage.path_or_url_valid check must exist");

    assert_eq!(check.status, DiagnosticStatus::Pass);
}

#[tokio::test]
async fn test_check_storage_permissions() {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap().to_string();

    let opts = DoctorOptions {
        storage: Some(storage_path),
        ..DoctorOptions::default()
    };
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "storage.readable_and_writable")
        .expect("storage.readable_and_writable check must exist");

    assert_eq!(check.status, DiagnosticStatus::Pass);
}

#[tokio::test]
async fn test_check_storage_deep_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap().to_string();

    let opts = DoctorOptions {
        storage: Some(storage_path.clone()),
        deep: true,
        ..DoctorOptions::default()
    };
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "storage.deep_roundtrip")
        .expect("storage.deep_roundtrip check must exist");

    assert_eq!(check.status, DiagnosticStatus::Pass);
    assert!(check.summary.contains("cleanup succeeded"));

    // Verify probe directory was deleted
    let doctor_probe_dir = PathBuf::from(storage_path).join("_rockstream_doctor");
    assert!(
        !doctor_probe_dir.exists(),
        "doctor probe directory must be cleaned up"
    );
}

#[tokio::test]
async fn test_check_control_reachability_failure() {
    let opts = DoctorOptions {
        control: Some("http://127.0.0.1:59999".to_string()),
        timeout: Duration::from_millis(500),
        ..DoctorOptions::default()
    };
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "control.tcp_connect_and_status")
        .expect("control.tcp_connect_and_status check must exist");

    assert_eq!(check.status, DiagnosticStatus::Fail);
    assert_eq!(check.code.as_deref(), Some("RS-0003"));
    assert!(check.next_steps.is_some());
}

#[tokio::test]
async fn test_check_gateway_reachability_failure() {
    let opts = DoctorOptions {
        gateway: Some("127.0.0.1:59998".to_string()),
        timeout: Duration::from_millis(500),
        ..DoctorOptions::default()
    };
    let report = run_doctor_checks(&opts).await;

    let check = report
        .checks
        .iter()
        .find(|c| c.id == "gateway.tcp_connect_and_version")
        .expect("gateway.tcp_connect_and_version check must exist");

    assert_eq!(check.status, DiagnosticStatus::Fail);
    assert_eq!(check.code.as_deref(), Some("RS-0003"));
}

#[tokio::test]
async fn test_check_docker_daemon() {
    // Skipped by default
    let opts_skip = DoctorOptions::default();
    let report_skip = run_doctor_checks(&opts_skip).await;
    let check_skip = report_skip
        .checks
        .iter()
        .find(|c| c.id == "docker.daemon_available")
        .expect("docker check must exist");
    assert_eq!(check_skip.status, DiagnosticStatus::Skip);

    // Executed when --include-docker is specified
    let opts_include = DoctorOptions {
        include_docker: true,
        ..DoctorOptions::default()
    };
    let report_include = run_doctor_checks(&opts_include).await;
    let check_include = report_include
        .checks
        .iter()
        .find(|c| c.id == "docker.daemon_available")
        .expect("docker check must exist");
    assert!(
        check_include.status == DiagnosticStatus::Pass
            || check_include.status == DiagnosticStatus::Warn
    );
}

#[test]
fn test_doctor_secret_redaction() {
    let raw = "Connect with s3://user:secret=super_secret_token123&password=my_secret_password@bucket/prefix";
    let redacted = redact_secrets(raw);
    assert!(!redacted.contains("super_secret_token123"));
    assert!(!redacted.contains("my_secret_password"));
    assert!(redacted.contains("[REDACTED]"));

    let cert_key =
        "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA0...\n-----END RSA PRIVATE KEY-----";
    let key_redacted = redact_secrets(cert_key);
    assert!(!key_redacted.contains("MIIEowIBAAKCAQEA0"));
    assert!(key_redacted.contains("[REDACTED_PRIVATE_KEY_MATERIAL]"));
}

#[tokio::test]
async fn test_doctor_output_formats() {
    let opts = DoctorOptions::default();

    // Text format
    let text_output = run_doctor(OutputFormat::Text, &opts).expect("doctor run failed");
    assert!(text_output.contains("Doctor Diagnostics:"));
    assert!(text_output.contains("binary.candidate_identity"));
    assert!(text_output.contains("config.parse_and_semantic"));

    // JSON format
    let json_output = run_doctor(OutputFormat::Json, &opts).expect("doctor run failed");
    let report: DoctorReport = serde_json::from_str(&json_output).expect("valid JSON DoctorReport");
    assert!(report.passed_count >= 5);
    assert_eq!(report.failed_count, 0);
    assert!(report
        .checks
        .iter()
        .any(|c| c.id == "binary.candidate_identity"));
}

//! Comprehensive Diagnostic Inspection Tests in `rockstream doctor` (v0.59.22 Slice 5 / Phase 3a).

use rockstream_cli::doctor::{run_doctor_checks, DiagnosticStatus};
use rockstream_cli::DoctorOptions;
use tempfile::TempDir;

#[tokio::test]
async fn test_doctor_platform_libc_and_backend_classification() {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().to_str().unwrap().to_string();

    let opts = DoctorOptions {
        storage: Some(storage_path),
        ..DoctorOptions::default()
    };
    let report = run_doctor_checks(&opts).await;

    // 1. Platform Classification Check
    let platform_check = report
        .checks
        .iter()
        .find(|c| c.id == "platform.classification")
        .expect("platform.classification check must exist");
    assert_eq!(platform_check.status, DiagnosticStatus::Pass);

    // 2. Libc Compatibility Check
    let libc_check = report
        .checks
        .iter()
        .find(|c| c.id == "platform.libc_compatibility")
        .expect("platform.libc_compatibility check must exist");
    assert_eq!(libc_check.status, DiagnosticStatus::Pass);

    // 3. Container Security Check
    let container_check = report
        .checks
        .iter()
        .find(|c| c.id == "platform.container_security")
        .expect("platform.container_security check must exist");
    assert!(
        container_check.status == DiagnosticStatus::Pass
            || container_check.status == DiagnosticStatus::Warn
    );

    // 4. Port Availability Check
    let port_check = report
        .checks
        .iter()
        .find(|c| c.id == "platform.port_availability")
        .expect("platform.port_availability check must exist");
    assert!(
        port_check.status == DiagnosticStatus::Pass || port_check.status == DiagnosticStatus::Warn
    );

    // 5. Storage Filesystem Check
    let fs_check = report
        .checks
        .iter()
        .find(|c| c.id == "platform.storage_filesystem")
        .expect("platform.storage_filesystem check must exist");
    assert_eq!(fs_check.status, DiagnosticStatus::Pass);

    // 6. Backend Compatibility Check
    let backend_check = report
        .checks
        .iter()
        .find(|c| c.id == "platform.backend_compatibility")
        .expect("platform.backend_compatibility check must exist");
    assert_eq!(backend_check.status, DiagnosticStatus::Pass);
}

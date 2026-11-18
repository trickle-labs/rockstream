use rockstream_types::lifecycle::{HealthDimensionStatus, LifecycleState, LifecycleTracker};
use rockstream_types::metrics;

#[test]
fn audit_storage_degradation_is_visible_without_failing_ready_health() {
    metrics::reset_all();
    let tracker = LifecycleTracker::new_with_state("control", LifecycleState::Ready);
    metrics::set_security_audit_degraded(true);

    let (status_code, report) = tracker.generate_health_report();
    assert_eq!(status_code, 200);
    assert_eq!(report.status, "degraded");
    let degradation = report.dimensions.unwrap().degradation;
    assert_eq!(degradation.status, HealthDimensionStatus::Warn);
    assert_eq!(
        degradation.reason.as_deref(),
        Some("security audit storage is degraded")
    );
    assert_eq!(degradation.producer, "control:security-audit");
    assert!(degradation.sampled_at > 0);

    metrics::set_security_audit_degraded(false);
    let (status_code, report) = tracker.generate_health_report();
    assert_eq!(status_code, 200);
    assert_eq!(report.status, "healthy");
    assert_eq!(
        report.dimensions.unwrap().degradation.status,
        HealthDimensionStatus::Pass
    );

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    tracker.set_degradation_status(
        HealthDimensionStatus::Fail,
        Some("catalog write failed".to_owned()),
        now_secs,
    );
    metrics::set_security_audit_degraded(true);

    let (status_code, report) = tracker.generate_health_report();
    assert_eq!(status_code, 503);
    assert_eq!(report.status, "unhealthy");
    let degradation = report.dimensions.unwrap().degradation;
    assert_eq!(degradation.status, HealthDimensionStatus::Fail);
    assert_eq!(
        degradation.reason.as_deref(),
        Some("catalog write failed; security audit storage is degraded")
    );
    assert_eq!(degradation.producer, "control:diagnostic");
    metrics::reset_all();
}

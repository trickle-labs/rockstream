use object_store::memory::InMemory;
use rockstream_storage::catalog::{CatalogMutation, CatalogStore, CatalogTxn, DurableCatalogStore};
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, SecurityAuditClass, SecurityAuditOutcome,
    SecurityAuditRecord, SECURITY_AUDIT_FORMAT_VERSION,
};
use rockstream_types::metrics;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn metric_lines() -> Vec<String> {
    metrics::generate_prometheus_metrics()
        .lines()
        .filter(|line| {
            line.starts_with("# HELP security_audit_")
                || line.starts_with("# TYPE security_audit_")
                || line.starts_with("security_audit_")
        })
        .map(str::to_owned)
        .collect()
}

fn expected_metric_lines(records: u64, bytes: u64) -> Vec<String> {
    vec![
        "# HELP security_audit_retained_records Number of retained security audit records."
            .to_owned(),
        "# TYPE security_audit_retained_records gauge".to_owned(),
        format!("security_audit_retained_records {records}"),
        "# HELP security_audit_retained_bytes JSON-encoded size of retained security audit records."
            .to_owned(),
        "# TYPE security_audit_retained_bytes gauge".to_owned(),
        format!("security_audit_retained_bytes {bytes}"),
        "# HELP security_audit_capacity_bytes Configured security audit retention byte limit."
            .to_owned(),
        "# TYPE security_audit_capacity_bytes gauge".to_owned(),
        "security_audit_capacity_bytes 1073741824".to_owned(),
        "# HELP security_audit_degraded Whether required security audit writes are failing."
            .to_owned(),
        "# TYPE security_audit_degraded gauge".to_owned(),
        "security_audit_degraded 0".to_owned(),
        "# HELP security_audit_transport_capacity Maximum in-flight worker security audit acknowledgements."
            .to_owned(),
        "# TYPE security_audit_transport_capacity gauge".to_owned(),
        "security_audit_transport_capacity 0".to_owned(),
        "# HELP security_audit_transport_pending Worker security audit requests awaiting durable acknowledgement."
            .to_owned(),
        "# TYPE security_audit_transport_pending gauge".to_owned(),
        "security_audit_transport_pending 0".to_owned(),
    ]
}

#[tokio::test]
async fn durable_audit_metrics_follow_commit_and_restart() {
    metrics::reset_all();
    let object_store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::recover(object_store.clone(), "audit-metrics")
        .await
        .unwrap();
    assert_eq!(metric_lines(), expected_metric_lines(0, 2));

    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let record = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "metric-event".into(),
        timestamp_ms,
        actor: AuditIdentity::Unknown,
        target: AuditField::Unknown,
        scope: AuditField::NotApplicable,
        action: SecurityAuditClass::AuthenticationFailure,
        outcome: SecurityAuditOutcome::Failure,
        correlation: AuditCorrelation::NotApplicable,
        operation_id: AuditField::NotApplicable,
        policy_revision: AuditField::NotApplicable,
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    let encoded_bytes = serde_json::to_vec(std::slice::from_ref(&record))
        .unwrap()
        .len() as u64;
    metrics::set_security_audit_degraded(true);
    catalog
        .commit_txn(
            CatalogTxn::new(1, 1, vec![CatalogMutation::AppendSecurityAudit(record)]).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(metric_lines(), expected_metric_lines(1, encoded_bytes));

    let _recovered = DurableCatalogStore::recover(object_store, "audit-metrics")
        .await
        .unwrap();
    assert_eq!(metric_lines(), expected_metric_lines(1, encoded_bytes));
    metrics::reset_all();
}

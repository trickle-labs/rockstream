use std::io::BufReader;
use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_cli::transport::ManagementCliClient;
use rockstream_control::{
    ManagementOperationStore, ManagementService, ShardManager, TopologyCatalog,
};
use rockstream_storage::catalog::{CatalogMutation, CatalogStore, CatalogTxn, DurableCatalogStore};
use rockstream_test_support::pki::TestPki;
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ScopedPresetGrant, SecurityAuditClass, SecurityAuditOutcome, SecurityAuditRecord,
    SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION, SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::config::NodeConfig;
use rockstream_types::error_code::RS_0003;
use rockstream_types::ids::PrincipalId;
use sha2::Digest;

#[tokio::test]
async fn management_cli_connects_to_mtls_service_and_reads_exact_status() {
    let pki = TestPki::generate();
    let certs = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(&pki.cli_cert_path).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let certificate_sha256 = sha2::Sha256::digest(certs[0].as_ref()).into();
    let worker_cert = pki.worker_certs.get(&1).unwrap().0.clone();
    let worker_certs = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(worker_cert).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let worker_certificate_sha256 = sha2::Sha256::digest(worker_certs[0].as_ref()).into();
    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "management CLI test administrator".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![
            LoginMethod::Mtls { certificate_sha256 },
            LoginMethod::Mtls {
                certificate_sha256: worker_certificate_sha256,
            },
        ],
        preset_grants: [PermissionPreset::Administrator, PermissionPreset::Operator]
            .into_iter()
            .map(|preset| ScopedPresetGrant {
                preset,
                scope: PolicyScope::Cluster,
            })
            .collect(),
        scoped_grants: Vec::new(),
    };
    let catalog = Arc::new(DurableCatalogStore::new(
        Arc::new(InMemory::new()),
        "management-cli-test",
    ));
    catalog
        .commit_txn(
            CatalogTxn::new(
                1,
                1,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: principal.clone(),
                    expected_revision: None,
                    grantor_revision: None,
                    audit: SecurityAuditRecord {
                        format_version: SECURITY_AUDIT_FORMAT_VERSION,
                        event_id: "management-cli-bootstrap".to_owned(),
                        timestamp_ms: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64,
                        actor: AuditIdentity::Unknown,
                        target: AuditField::Known("principal:1".to_owned()),
                        scope: AuditField::Known("cluster".to_owned()),
                        action: SecurityAuditClass::GrantChange,
                        outcome: SecurityAuditOutcome::Success,
                        correlation: AuditCorrelation::NotApplicable,
                        operation_id: AuditField::Known("1".to_owned()),
                        policy_revision: AuditField::Known(1),
                        before_state: AuditField::NotApplicable,
                        after_state: AuditField::Known(format!(
                            "revision=1;policy={}",
                            serde_json::to_string(&(
                                true,
                                &principal.preset_grants,
                                &principal.scoped_grants
                            ))
                            .unwrap()
                        )),
                    },
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        ManagementOperationStore::new(Arc::new(InMemory::new())),
        NodeConfig::default(),
    )
    .with_security_catalog(catalog.clone() as Arc<dyn CatalogStore>);
    let reloader = Arc::new(
        rockstream_control::tls::TlsCertificateReloader::new(pki.control_tls_config()).unwrap(),
    );
    let server = service.start("127.0.0.1:0", reloader).await.unwrap();

    let addr = server.addr.to_string();
    let tls = pki.cli_tls_config();
    let status = tokio::task::spawn_blocking(move || {
        let mut client = ManagementCliClient::connect(&addr, tls).unwrap();
        client.cluster_status().unwrap()
    })
    .await
    .unwrap();

    assert_eq!(status.source_version, "topology:0");
    assert_eq!(status.state, "unknown");
    assert!(status.nodes.is_empty());
    assert_eq!(status.active_operations, 0);
    assert_eq!(status.retained_operations, 0);
    assert_eq!(status.request_fill, 1);
    assert_eq!(status.request_capacity, 64);
    assert_eq!(status.ack_waiter_fill, 0);
    assert_eq!(status.ack_waiter_capacity, 0);
    assert!(status
        .observed_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());

    let worker_addr = server.addr.to_string();
    let worker_tls = pki.worker_tls_config(1);
    let worker_attempt = tokio::task::spawn_blocking(move || {
        ManagementCliClient::connect(&worker_addr, worker_tls)
            .and_then(|mut client| client.cluster_status())
    })
    .await
    .unwrap();
    let worker_error =
        worker_attempt.expect_err("a node certificate cannot authenticate as a client");
    assert_eq!(worker_error.code, RS_0003);
    let records = catalog.list_security_audit_records().await.unwrap();
    let rejection = records
        .iter()
        .find(|record| {
            record.action == SecurityAuditClass::AuthenticationFailure
                && record.actor == AuditIdentity::Unverified("worker:worker-1".to_owned())
        })
        .expect("rejected node identity is audited as unverified");
    assert_eq!(rejection.outcome, SecurityAuditOutcome::Failure);
    assert_eq!(
        rejection.target,
        AuditField::Known("management-api".to_owned())
    );
    assert_eq!(rejection.format_version, SECURITY_AUDIT_FORMAT_VERSION);
    assert!(rejection.event_id.starts_with("management-authn-"));
    assert!(rejection.timestamp_ms > 0);
    assert_eq!(rejection.scope, AuditField::NotApplicable);
    assert_eq!(rejection.correlation, AuditCorrelation::Unknown);
    assert_eq!(rejection.operation_id, AuditField::NotApplicable);
    assert_eq!(rejection.policy_revision, AuditField::Unknown);
    assert_eq!(rejection.before_state, AuditField::NotApplicable);
    assert_eq!(rejection.after_state, AuditField::NotApplicable);
    server.shutdown();
}

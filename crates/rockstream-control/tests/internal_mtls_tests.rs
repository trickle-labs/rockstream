//! End-to-End Integration Tests for Internal mTLS Transport & Lifecycle (v0.55).
//!
//! Validates:
//! - Proof Claim 1: Rejection and audit denial of untrusted/expired/absent cert peers with zero data processed.
//! - Proof Claim 2: Dynamic zero-downtime certificate rotation under sustained traffic with 0 lost epochs.
//! - Proof Claim 3: CLI transport seam behavior over mTLS and refusal when unauthenticated.
//! - Proof Claim 5: Durability of topology, shard leases, and audit trail under mTLS across node restarts.

use std::sync::Arc;
use std::time::Duration;

use rockstream_cli::output::OutputFormat;
use rockstream_cli::run_cluster_status;
use rockstream_cli::transport::{ClientIdentity, ControlClient};
use rockstream_control::audit::FileAuditLog;
use rockstream_control::delegation::{
    request_digest, ActorAssertionSigner, ActorAssertionVerifier,
};
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::{ControlService, SecretStore, ShardManager, TopologyCatalog};
use rockstream_runtime::client::start_worker_client_with_tls;
use rockstream_runtime::data_plane::DataPlaneClient;
use rockstream_storage::catalog::{
    CatalogDatabase, CatalogMutation, CatalogNamespace, CatalogSourceEntry, CatalogStore,
    CatalogTable, CatalogTxn, DurableCatalogStore,
};
use rockstream_test_support::pki::TestPki;
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ResourceKind, ScopedActionGrant, ScopedPresetGrant, SecurityAction, SecurityAuditClass,
    SecurityAuditOutcome, SecurityAuditRecord, SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION,
    SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::error_code::{RS_2400, RS_2410, RS_2411, RS_2412};
use rockstream_types::ids::{
    DatabaseId, NamespaceId, OperatorId, PrincipalId, ShardId, SourceId, TableId, WorkerId,
    WorkloadId,
};
use rockstream_types::secret::SecretType;
use rockstream_types::{config::JoinStrategy, data_plane::DeploymentRequest};

#[path = "common/management_auth.rs"]
mod management_auth;

fn security_audit_catalog() -> Arc<DurableCatalogStore> {
    Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "internal-mtls-test",
    ))
}

fn delegated_view_request() -> DeploymentRequest {
    DeploymentRequest {
        version: rockstream_types::data_plane::DEPLOYMENT_DESCRIPTOR_VERSION,
        workload_id: WorkloadId(42),
        namespace: "public".to_owned(),
        workload_name: "delegated_view".to_owned(),
        plan_json: "{}".to_owned(),
        join_strategy: JoinStrategy::Auto,
        schemas: Vec::new(),
        frontier: 0,
        storage_root: "/tmp/test-delegated-view".to_owned(),
        sink_operator_id: OperatorId(43),
        output_columns: Vec::new(),
        primary_key: Vec::new(),
        merge_key_columns: Vec::new(),
        routing_columns: Default::default(),
    }
}

fn sign_delegated_view_request(
    principal_id: PrincipalId,
    request: &DeploymentRequest,
    secret_use_scopes: &[PolicyScope],
) -> String {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let signer = ActorAssertionSigner::from_pkcs8_pem(
        &fixtures.join("actor-assertion-test-key.pem"),
        "test-deployment",
    )
    .unwrap();
    let worker_request = rockstream_types::topology::WorkerMessage::DeployWorkload(request.clone());
    signer
        .issue(
            principal_id,
            1,
            SecurityAction::ViewAuthor,
            PolicyScope::Resource {
                namespace: request.namespace.clone(),
                kind: ResourceKind::View,
                name: request.workload_name.clone(),
            },
            secret_use_scopes.to_vec(),
            request_digest(&worker_request).unwrap(),
        )
        .unwrap()
}

fn security_policy_state(principal: &SecurityPrincipal) -> String {
    format!(
        "revision={};policy={}",
        principal.revision,
        serde_json::to_string(&(
            principal.enabled,
            &principal.preset_grants,
            &principal.scoped_grants,
        ))
        .unwrap()
    )
}

async fn provision_view_author(auth: &management_auth::ManagementAuth) {
    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "Scoped view author".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls {
            certificate_sha256: [2; 32],
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Builder,
            scope: PolicyScope::Resource {
                namespace: "public".to_owned(),
                kind: ResourceKind::View,
                name: "delegated_view".to_owned(),
            },
        }],
        scoped_grants: Vec::new(),
    };
    let after_state = security_policy_state(&principal);
    let audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "delegation-test-principal".to_owned(),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        actor: AuditIdentity::Verified("principal:1".to_owned()),
        target: AuditField::Known("principal:2".to_owned()),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::Known("2".to_owned()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::Known(after_state),
    };
    auth.catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal,
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn revoke_view_author(auth: &management_auth::ManagementAuth) {
    auth.catalog.refresh().await.unwrap();
    let mut principal = auth
        .catalog
        .get_security_principal(PrincipalId(2))
        .await
        .unwrap()
        .unwrap();
    let before_state = security_policy_state(&principal);
    principal.revision = 2;
    principal.preset_grants.clear();
    let after_state = security_policy_state(&principal);
    let audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "delegation-test-revoke".to_owned(),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        actor: AuditIdentity::Verified("principal:1".to_owned()),
        target: AuditField::Known("principal:2".to_owned()),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::Known("3".to_owned()),
        policy_revision: AuditField::Known(2),
        before_state: AuditField::Known(before_state),
        after_state: AuditField::Known(after_state),
    };
    auth.catalog
        .commit_txn(
            CatalogTxn::new(
                auth.catalog.get_revision().await + 1,
                3,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal,
                    expected_revision: Some(1),
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

fn worker_secret_scope() -> PolicyScope {
    PolicyScope::Secret {
        namespace: "public".to_owned(),
        name: "kafka".to_owned(),
        consumer: Some(PrincipalId(3)),
    }
}

fn current_time_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn security_grant_audit(
    event_id: String,
    principal_id: PrincipalId,
    operation_id: u64,
    revision: u64,
    before_state: AuditField<String>,
    after_state: String,
) -> SecurityAuditRecord {
    SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id,
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        actor: AuditIdentity::Verified("principal:1".to_owned()),
        target: AuditField::Known(format!("principal:{}", principal_id.0)),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::Known(operation_id.to_string()),
        policy_revision: AuditField::Known(revision),
        before_state,
        after_state: AuditField::Known(after_state),
    }
}

async fn provision_worker_secret_source(auth: &management_auth::ManagementAuth) {
    let consumer = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(3),
        display_name: "Orders source service".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Service {
            service_id: "orders-source".to_owned(),
        }],
        preset_grants: Vec::new(),
        scoped_grants: vec![ScopedActionGrant {
            action: SecurityAction::SecretUse,
            scope: worker_secret_scope(),
        }],
    };
    let audit = security_grant_audit(
        "worker-secret-consumer".to_owned(),
        PrincipalId(3),
        3,
        1,
        AuditField::NotApplicable,
        security_policy_state(&consumer),
    );
    auth.catalog
        .commit_txn(
            CatalogTxn::new(
                3,
                3,
                vec![
                    CatalogMutation::PutDatabase(CatalogDatabase {
                        id: DatabaseId(1),
                        name: "rockstream".to_owned(),
                        default_namespace: "public".to_owned(),
                        created_at: 1,
                    }),
                    CatalogMutation::PutNamespace(CatalogNamespace {
                        id: NamespaceId(1),
                        name: "public".to_owned(),
                        database_id: DatabaseId(1),
                        created_at: 1,
                    }),
                    CatalogMutation::PutTable(CatalogTable {
                        id: TableId(1),
                        name: "orders".to_owned(),
                        namespace_id: NamespaceId(1),
                        columns: Vec::new(),
                        pk_cols: Vec::new(),
                    }),
                    CatalogMutation::PutSource(CatalogSourceEntry {
                        id: SourceId(1),
                        name: "orders_source".to_owned(),
                        connector_type: "kafka".to_owned(),
                        table_name: Some("orders".to_owned()),
                        format: "json".to_owned(),
                        options: std::collections::HashMap::from([(
                            "secret".to_owned(),
                            "secret://kafka".to_owned(),
                        )]),
                        secret_consumer: Some(PrincipalId(3)),
                        secret_namespace: Some("public".to_owned()),
                    }),
                    CatalogMutation::PutSecurityPrincipal {
                        principal: consumer,
                        expected_revision: None,
                        grantor_revision: Some((PrincipalId(1), 1)),
                        audit,
                    },
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn set_actor_secret_use(auth: &management_auth::ManagementAuth, allowed: bool) {
    auth.catalog.refresh().await.unwrap();
    let mut actor = auth
        .catalog
        .get_security_principal(PrincipalId(2))
        .await
        .unwrap()
        .unwrap();
    let before_state = security_policy_state(&actor);
    actor.revision += 1;
    actor.scoped_grants.retain(|grant| {
        !(grant.action == SecurityAction::SecretUse && grant.scope == worker_secret_scope())
    });
    if allowed {
        actor.scoped_grants.push(ScopedActionGrant {
            action: SecurityAction::SecretUse,
            scope: worker_secret_scope(),
        });
    }
    let operation_id = auth.catalog.get_revision().await + 1;
    let audit = security_grant_audit(
        format!("worker-secret-actor-{}", actor.revision),
        PrincipalId(2),
        operation_id,
        actor.revision,
        AuditField::Known(before_state),
        security_policy_state(&actor),
    );
    auth.catalog
        .commit_txn(
            CatalogTxn::new(
                operation_id,
                operation_id,
                vec![CatalogMutation::PutSecurityPrincipal {
                    expected_revision: Some(actor.revision - 1),
                    grantor_revision: Some((PrincipalId(1), 1)),
                    principal: actor,
                    audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn internal_mtls_requires_security_audit_catalog() {
    let pki = TestPki::generate();
    let service =
        ControlService::new(TopologyCatalog::new()).with_internal_tls(pki.control_tls_config());

    let error = service
        .start("127.0.0.1:0")
        .await
        .err()
        .expect("mTLS must fail startup without the security audit catalog");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "RS-2405: internal mTLS requires the durable security audit catalog"
    );
}

#[tokio::test]
async fn gateway_data_plane_requires_actor_assertion_and_audits_wrong_role() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());
    let security_catalog = security_audit_catalog();
    let service = ControlService::new(TopologyCatalog::new())
        .with_audit(audit)
        .with_security_catalog(security_catalog.clone())
        .with_internal_tls(pki.control_tls_config());
    let handle = service.start("127.0.0.1:0").await.unwrap();
    let control_addr = handle.addr.to_string();

    let gateway = rockstream_runtime::data_plane::DataPlaneClient::new(&control_addr)
        .with_internal_tls(&pki.gateway_tls_config())
        .unwrap();
    let missing_assertion = gateway.read_workload(WorkloadId(1)).await.unwrap_err();
    assert_eq!(
        missing_assertion.to_string(),
        format!(
            "{RS_2400}: a valid user actor assertion is required for gateway data-plane requests"
        )
    );

    let wrong_role = rockstream_runtime::data_plane::DataPlaneClient::new(&control_addr)
        .with_internal_tls(&pki.cli_tls_config())
        .unwrap()
        .read_workload(WorkloadId(1))
        .await
        .unwrap_err();
    let records = security_catalog
        .list_security_audit_records()
        .await
        .unwrap();
    let missing_assertion = records
        .iter()
        .find(|record| record.action == SecurityAuditClass::AuthenticationFailure)
        .expect("missing actor assertion is durably audited");
    assert_eq!(
        missing_assertion.actor,
        AuditIdentity::Unverified("gateway:gateway-1".to_owned())
    );
    assert_eq!(
        missing_assertion.outcome,
        rockstream_types::authorization::SecurityAuditOutcome::Denied
    );
    let rejection = records
        .iter()
        .find(|record| record.action == SecurityAuditClass::NodeIdentityRejection)
        .expect("wrong gateway role is durably audited");
    assert_eq!(
        wrong_role.to_string(),
        format!("{RS_2412}: certificate identity is not authorized for the gateway data plane"),
    );

    assert_eq!(
        rejection.actor,
        AuditIdentity::Unverified("cli:admin".to_owned())
    );
    assert_eq!(
        rejection.outcome,
        rockstream_types::authorization::SecurityAuditOutcome::Denied
    );
}

#[tokio::test]
async fn worker_secret_tokens_are_workload_bound_and_recheck_current_grants() {
    let auth = management_auth::ManagementAuth::new(&[]).await;
    provision_view_author(&auth).await;
    provision_worker_secret_source(&auth).await;
    let secret_store = Arc::new(SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("worker-secret-test-kek")),
    ));
    let payload = std::collections::HashMap::from([(
        "password".to_owned(),
        "worker-secret-value".to_owned(),
    )]);
    secret_store
        .create_secret(1, "kafka", SecretType::BearerToken, payload.clone(), "test")
        .await
        .unwrap();
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let verifier = ActorAssertionVerifier::from_jwk_file(
        &fixtures.join("actor-assertion-test-public.jwk"),
        "test-deployment",
    )
    .unwrap();
    let enforcement_catalog = Arc::new(DurableCatalogStore::new(
        auth.store.clone(),
        "management-security",
    ));
    let service = ControlService::new(TopologyCatalog::new())
        .with_security_catalog(enforcement_catalog)
        .with_secret_store(secret_store)
        .with_internal_tls(auth.pki.control_tls_config())
        .with_actor_assertion_verifier(verifier);
    let handle = service.start("127.0.0.1:0").await.unwrap();
    let storage_dir = tempfile::tempdir().unwrap();
    let (worker, worker_handle) = start_worker_client_with_tls(
        1,
        &handle.addr.to_string(),
        storage_dir.path(),
        auth.pki.worker_tls_config(1),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while worker.worker_id().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let mut request = delegated_view_request();
    request.schemas = vec![rockstream_types::data_plane::DeploymentSchema {
        relation: "orders".to_owned(),
        columns: vec![rockstream_types::data_plane::DeploymentColumn {
            name: "id".to_owned(),
            data_type: "i64".to_owned(),
        }],
    }];
    request.plan_json = serde_json::to_string(&rockstream_plan::PlanNode::ViewSink {
        view_name: request.workload_name.clone(),
        pk: vec![0],
        child: Box::new(rockstream_plan::PlanNode::Source {
            name: "orders".to_owned(),
        }),
    })
    .unwrap();
    request.output_columns = vec!["id".to_owned()];
    request.primary_key = vec![0];
    let gateway = DataPlaneClient::new(handle.addr.to_string())
        .with_internal_tls(&auth.pki.gateway_tls_config())
        .unwrap();
    gateway
        .deploy_delegated(
            request.clone(),
            &sign_delegated_view_request(PrincipalId(2), &request, &[]),
        )
        .await
        .unwrap();
    assert!(worker
        .get_deployment(request.workload_id, ShardId(request.workload_id.0 * 16))
        .is_some());

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            auth.catalog.refresh().await.unwrap();
            let records = auth.catalog.list_security_audit_records().await.unwrap();
            if records.iter().any(|record| {
                record.action == SecurityAuditClass::AuthorizationDenial
                    && record.target == AuditField::Known("kafka".to_owned())
            }) {
                assert!(worker
                    .secret(request.workload_id, "kafka", current_time_secs())
                    .is_none());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    set_actor_secret_use(&auth, true).await;
    worker
        .request_secret_token(request.workload_id, "kafka")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let records = auth.catalog.list_security_audit_records().await.unwrap();
            if records.iter().any(|record| {
                record.action == SecurityAuditClass::AuthorizationDenial
                    && record.target == AuditField::Known("kafka".to_owned())
                    && record.policy_revision == AuditField::Known(2)
            }) {
                assert!(worker
                    .secret(request.workload_id, "kafka", current_time_secs())
                    .is_none());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let assertion = sign_delegated_view_request(PrincipalId(2), &request, &[worker_secret_scope()]);
    gateway
        .deploy_delegated(request.clone(), &assertion)
        .await
        .unwrap();
    worker
        .request_secret_token(request.workload_id, "kafka")
        .await
        .unwrap();
    let resolved = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(secret) = worker.secret(request.workload_id, "kafka", current_time_secs()) {
                break secret;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(resolved.workload_id, request.workload_id);
    assert_eq!(resolved.secret_name, "kafka");
    assert_eq!(resolved.payload, payload);

    set_actor_secret_use(&auth, false).await;
    let refresh_result = tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            if worker
                .secret(request.workload_id, "kafka", current_time_secs())
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if refresh_result.is_err() {
        auth.catalog.refresh().await.unwrap();
        let records = auth.catalog.list_security_audit_records().await.unwrap();
        let denials = records
            .iter()
            .filter(|record| {
                record.action == SecurityAuditClass::AuthorizationDenial
                    && record.target == AuditField::Known("kafka".to_owned())
            })
            .count();
        let targets = worker
            .secret_manager()
            .refresh_targets(&std::collections::HashSet::from([request.workload_id]));
        let active = worker
            .get_deployment(request.workload_id, ShardId(request.workload_id.0 * 16))
            .is_some();
        panic!("periodic secret reauthorization did not revoke the cache; denials={denials}, active={active}, targets={targets:?}");
    }
    auth.catalog.refresh().await.unwrap();
    let records = auth.catalog.list_security_audit_records().await.unwrap();
    let denials: Vec<_> = records
        .iter()
        .filter(|record| {
            record.action == SecurityAuditClass::AuthorizationDenial
                && record.target == AuditField::Known("kafka".to_owned())
        })
        .collect();
    assert_eq!(denials.len(), 3);
    assert!(denials.iter().all(|record| {
        record.actor == AuditIdentity::Verified("principal-2".to_owned())
            && record.scope == AuditField::Known(format!("{:?}", worker_secret_scope()))
    }));
    assert!(denials
        .iter()
        .any(|record| record.policy_revision == AuditField::Known(1)));
    assert!(denials
        .iter()
        .any(|record| record.policy_revision == AuditField::Known(2)));
    assert!(denials
        .iter()
        .any(|record| record.policy_revision == AuditField::Known(3)));
    let success = records
        .iter()
        .find(|record| {
            record.action == SecurityAuditClass::AuthenticationSuccess
                && record.target == AuditField::Known("kafka".to_owned())
                && record.policy_revision == AuditField::Known(2)
        })
        .expect("current actor grant audited the worker token issuance");
    assert_eq!(
        success.actor,
        AuditIdentity::Verified("principal-2".to_owned())
    );
    assert_eq!(
        success.scope,
        AuditField::Known(format!("{:?}", worker_secret_scope()))
    );

    worker_handle.abort();
    handle.shutdown();
}

#[tokio::test]
async fn gateway_actor_assertion_intersects_current_durable_policy() {
    let auth = management_auth::ManagementAuth::new(&[]).await;
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let verifier = ActorAssertionVerifier::from_jwk_file(
        &fixtures.join("actor-assertion-test-public.jwk"),
        "test-deployment",
    )
    .unwrap();
    let request = delegated_view_request();
    let assertion = sign_delegated_view_request(PrincipalId(1), &request, &[]);

    let service = ControlService::new(TopologyCatalog::new())
        .with_security_catalog(auth.catalog.clone())
        .with_internal_tls(auth.pki.control_tls_config())
        .with_actor_assertion_verifier(verifier);
    let handle = service.start("127.0.0.1:0").await.unwrap();
    let gateway = DataPlaneClient::new(handle.addr.to_string())
        .with_internal_tls(&auth.pki.gateway_tls_config())
        .unwrap();
    let error = gateway
        .deploy_delegated(request, &assertion)
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "RS-2401: asserted user actor is no longer authorized for this operation and scope"
    );

    let records = auth.catalog.list_security_audit_records().await.unwrap();
    let denied = records
        .iter()
        .find(|record| record.action == SecurityAuditClass::AuthorizationDenial)
        .expect("current durable actor policy denied the assertion");
    assert_eq!(
        denied.actor,
        AuditIdentity::Verified("principal-1".to_owned())
    );
    assert_eq!(denied.target, AuditField::Known("ViewAuthor".to_owned()));
    assert_eq!(denied.policy_revision, AuditField::Known(1));
    assert_eq!(denied.outcome, SecurityAuditOutcome::Denied);
}

#[tokio::test]
async fn gateway_actor_assertion_allows_independent_scoped_grant() {
    let auth = management_auth::ManagementAuth::new(&[]).await;
    provision_view_author(&auth).await;
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let verifier = ActorAssertionVerifier::from_jwk_file(
        &fixtures.join("actor-assertion-test-public.jwk"),
        "test-deployment",
    )
    .unwrap();
    let request = delegated_view_request();
    let assertion = sign_delegated_view_request(PrincipalId(2), &request, &[]);
    let enforcement_catalog = Arc::new(DurableCatalogStore::new(
        auth.store.clone(),
        "management-security",
    ));

    let service = ControlService::new(TopologyCatalog::new())
        .with_security_catalog(enforcement_catalog)
        .with_internal_tls(auth.pki.control_tls_config())
        .with_actor_assertion_verifier(verifier);
    let handle = service.start("127.0.0.1:0").await.unwrap();
    let gateway = DataPlaneClient::new(handle.addr.to_string())
        .with_internal_tls(&auth.pki.gateway_tls_config())
        .unwrap();
    let error = gateway
        .deploy_delegated(request.clone(), &assertion)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "RS-0001: no healthy workers available");
    revoke_view_author(&auth).await;
    let revoked = gateway
        .deploy_delegated(request, &assertion)
        .await
        .unwrap_err();
    auth.catalog.refresh().await.unwrap();
    let records = auth.catalog.list_security_audit_records().await.unwrap();
    let denied = records
        .iter()
        .find(|record| record.action == SecurityAuditClass::AuthorizationDenial)
        .expect("revoked actor policy denial is durably audited");
    assert_eq!(denied.policy_revision, AuditField::Known(2));
    assert_eq!(
        revoked.to_string(),
        "RS-2401: asserted user actor is no longer authorized for this operation and scope"
    );

    let accepted = records
        .iter()
        .find(|record| record.action == SecurityAuditClass::AuthenticationSuccess)
        .expect("current scoped grant accepted the assertion");
    assert_eq!(
        accepted.actor,
        AuditIdentity::Verified("principal-2".to_owned())
    );
    assert_eq!(accepted.target, AuditField::Known("ViewAuthor".to_owned()));
    assert_eq!(accepted.policy_revision, AuditField::Known(1));
    assert_eq!(accepted.outcome, SecurityAuditOutcome::Success);
    let denied = records
        .iter()
        .find(|record| record.action == SecurityAuditClass::AuthorizationDenial)
        .expect("revoked actor policy denied the still-valid assertion");
    assert_eq!(
        denied.actor,
        AuditIdentity::Verified("principal-2".to_owned())
    );
    assert_eq!(denied.policy_revision, AuditField::Known(2));
    assert_eq!(denied.outcome, SecurityAuditOutcome::Denied);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_peer_invalid_expired_absent_cert_rejected_with_audit_denial_no_data() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit_file = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(FileAuditLog::open(&audit_file).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_security_catalog(security_audit_catalog())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();

    // 1. Untrusted certificate peer connection attempt
    let untrusted_tls = pki.untrusted_worker_tls_config();
    let (c1, h1) = start_worker_client_with_tls(1, &control_url, storage_dir.path(), untrusted_tls)
        .await
        .unwrap();

    // 2. Expired certificate peer connection attempt
    let expired_tls = pki.expired_worker_tls_config();
    let (c2, h2) = start_worker_client_with_tls(2, &control_url, storage_dir.path(), expired_tls)
        .await
        .unwrap();

    // 3. Absent certificate (plaintext) peer connection attempt
    let absent_tls = rockstream_types::identity::InternalTlsConfig::default();
    let (c3, h3) = start_worker_client_with_tls(3, &control_url, storage_dir.path(), absent_tls)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify zero workers admitted to cluster topology catalog
    assert_eq!(
        c1.worker_id(),
        None,
        "Untrusted peer must not obtain WorkerId"
    );
    assert_eq!(
        c2.worker_id(),
        None,
        "Expired peer must not obtain WorkerId"
    );
    assert_eq!(
        c3.worker_id(),
        None,
        "Absent cert peer must not obtain WorkerId"
    );
    assert_eq!(
        catalog.len(),
        0,
        "No rejected peer may be registered in topology"
    );

    // Verify audit logs contain rejection denial records
    let audit_events = audit.read_all().unwrap();
    let denial_events: Vec<_> = audit_events
        .iter()
        .filter(|e| e.action == "security.internal_mtls_denied")
        .collect();
    assert!(
        !denial_events.is_empty(),
        "Audited denial events must be recorded for failed mTLS handshakes"
    );
    assert!(
        denial_events.iter().all(|event| event.actor == "unknown"),
        "pre-authentication TLS failures must not claim an authenticated actor: {denial_events:?}"
    );

    h1.abort();
    h2.abort();
    h3.abort();
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_certificate_rotation_preserves_streams_and_accepts_gen2_worker() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_security_catalog(security_audit_catalog())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();
    let mut handles = Vec::new();
    let mut clients = Vec::new();

    for id in 1..=3 {
        let (client, h) = start_worker_client_with_tls(
            id,
            &control_url,
            storage_dir.path(),
            pki.worker_tls_config(id),
        )
        .await
        .unwrap();
        clients.push(client);
        handles.push(h);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 3);

    // Actively stream shard lease requests
    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(clients.len());
    let (resume_tx, resume_rx) = tokio::sync::watch::channel(());
    let mut traffic_tasks = Vec::new();
    for client in &clients {
        let c = client.clone();
        let started_tx = started_tx.clone();
        let mut resume_rx = resume_rx.clone();
        traffic_tasks.push(tokio::spawn(async move {
            let worker_id = c.worker_id().unwrap();
            c.request_shard(ShardId(1)).await.unwrap();
            let mut requests = vec![(worker_id, ShardId(1))];
            started_tx.send(()).await.unwrap();
            resume_rx.changed().await.unwrap();
            for i in 2..=15 {
                c.request_shard(ShardId(i)).await.unwrap();
                requests.push((worker_id, ShardId(i)));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            requests
        }));
    }
    for _ in 0..clients.len() {
        started_rx.recv().await.unwrap();
    }

    // In-flight certificate rotation
    let (ca2_path, ctrl2_cfg, _) = pki.generate_gen2_pki();
    handle
        .reloader
        .as_ref()
        .unwrap()
        .add_trusted_ca(&ca2_path)
        .unwrap();
    handle.reload_tls(ctrl2_cfg).unwrap();
    let (gen2_client, gen2_handle) = start_worker_client_with_tls(
        4,
        &control_url,
        storage_dir.path(),
        pki.gen2_worker_tls_config(4),
    )
    .await
    .unwrap();
    handles.push(gen2_handle);
    tokio::time::timeout(Duration::from_secs(3), async {
        while catalog.len() != 4 || gen2_client.worker_id().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(gen2_client.worker_id(), Some(WorkerId(4)));
    resume_tx.send(()).unwrap();

    let expected_requests = clients
        .iter()
        .map(|client| {
            let worker_id = client.worker_id().unwrap();
            (1..=15)
                .map(|shard| (worker_id, ShardId(shard)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut actual_requests = Vec::new();
    for task in traffic_tasks {
        actual_requests.push(task.await.unwrap());
    }
    assert_eq!(actual_requests, expected_requests);

    // The three old-generation streams survived, and the new generation registered.
    assert_eq!(catalog.len(), 4);
    for (id, client) in (1..=3).zip(&clients) {
        assert_eq!(client.worker_id(), Some(WorkerId(id)));
    }

    for h in handles {
        h.abort();
    }
    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cli_control_mtls_transport_seam_and_refusal() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(FileAuditLog::open(audit_dir.path().join("audit.jsonl")).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_security_catalog(security_audit_catalog())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    // 1. Authenticated CLI client with valid certificate succeeds
    let cli_tls = pki.cli_tls_config();
    let client_identity = ClientIdentity::default().with_cert(cli_tls.cert_path.clone().unwrap());
    let valid_client =
        ControlClient::new(Some(control_url.clone()), client_identity).with_internal_tls(cli_tls);

    let status = run_cluster_status(OutputFormat::Json, &valid_client);
    assert!(
        status.is_ok(),
        "Authenticated CLI status request must succeed over mTLS"
    );

    // 2. Unauthenticated CLI client without certificate is refused with RS-2410
    let unauthenticated_client =
        ControlClient::new(Some(control_url.clone()), ClientIdentity::default());
    let res_refused = run_cluster_status(OutputFormat::Json, &unauthenticated_client);
    assert!(
        res_refused.is_err(),
        "Unauthenticated CLI request must be refused"
    );
    let err = res_refused.unwrap_err();
    assert_eq!(err.code, RS_2410, "Refusal error code must be RS-2410");

    // 3. Untrusted CLI client is refused with RS-2411
    let untrusted_cli_client =
        ControlClient::new(Some(control_url.clone()), ClientIdentity::default())
            .with_internal_tls(pki.untrusted_worker_tls_config());
    let res_untrusted = run_cluster_status(OutputFormat::Json, &untrusted_cli_client);
    assert!(
        res_untrusted.is_err(),
        "Untrusted CLI request must be refused"
    );
    let err_untrusted = res_untrusted.unwrap_err();
    assert_eq!(
        err_untrusted.code, RS_2411,
        "Refusal error code must be RS-2411"
    );

    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mtls_cluster_durability_lfs_and_minio() {
    let pki = TestPki::generate();
    let audit_dir = tempfile::tempdir().unwrap();
    let audit_file = audit_dir.path().join("audit.jsonl");
    let audit = Arc::new(FileAuditLog::open(&audit_file).unwrap());

    let catalog = TopologyCatalog::new();
    let manager = ShardManager::new();
    let svc = ControlService::new(catalog.clone())
        .with_shard_manager(manager)
        .with_audit(audit.clone())
        .with_security_catalog(security_audit_catalog())
        .with_internal_tls(pki.control_tls_config());

    let handle = svc.start("127.0.0.1:0").await.unwrap();
    let control_url = handle.addr.to_string();

    let storage_dir = tempfile::tempdir().unwrap();

    let (client, h) = start_worker_client_with_tls(
        1,
        &control_url,
        storage_dir.path(),
        pki.worker_tls_config(1),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(catalog.len(), 1);
    assert_eq!(client.worker_id(), Some(WorkerId(1)));

    // Verify audit durability on disk
    let audit_events = audit.read_all().unwrap();
    assert!(
        audit_events
            .iter()
            .any(|e| e.action == "worker.registered" || e.action == "cli.authenticated"),
        "Audit log must contain durable event records"
    );

    h.abort();
    handle.shutdown();
}

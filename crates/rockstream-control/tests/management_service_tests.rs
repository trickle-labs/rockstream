use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_control::{
    ManagementOperationStore, ManagementService, OperationKind, OperationStatus, OperationUpdate,
    ShardManager, TopologyCatalog,
};
use rockstream_management_proto::v1::{
    CancelOperationRequest, CreateBackupRequest, DrainWorkerRequest, GetCapabilitiesRequest,
    GetClusterStatusRequest, GetConfigSummaryRequest, GetHealthRequest, GetNodeRequest,
    GetOperationRequest, GetShardRequest, ListNodesRequest, ListOperationsRequest,
    ListSecurityPrincipalsRequest, ListShardsRequest, MigrateShardRequest, Operation,
    SetSecurityPrincipalGrantsRequest,
};
use rockstream_storage::catalog::{CatalogMutation, CatalogStore, CatalogTxn, DurableCatalogStore};
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ScopedActionGrant, ScopedPresetGrant, SecurityAction, SecurityAuditClass, SecurityAuditOutcome,
    SecurityAuditRecord, SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION,
    SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::config::NodeConfig;
use rockstream_types::ids::{PrincipalId, ShardId, WorkerId};
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerMessage,
    WorkerRegistration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[path = "common/management_auth.rs"]
mod management_auth;
use management_auth::ManagementAuth;

async fn register_worker(addr: std::net::SocketAddr, worker_id: u64) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let registration = WorkerRegistration::new(
        WorkerId(worker_id),
        NodeRole::Worker,
        format!("127.0.0.1:{}", 7000 + worker_id),
        CapacityHeadroom::FULL,
    )
    .with_capabilities(WorkerCapabilities {
        shared_shard_store_id: Some([9; 32]),
        ..Default::default()
    });
    let request = serde_json::to_string(&WorkerMessage::Register(registration)).unwrap() + "\n";
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut response = String::new();
    reader.read_line(&mut response).await.unwrap();
    assert!(matches!(
        serde_json::from_str::<ControlMessage>(response.trim()).unwrap(),
        ControlMessage::Registered { worker_id: registered } if registered == WorkerId(worker_id)
    ));
    drop(reader);
    stream
}

async fn read_until(
    stream: &mut TcpStream,
    matches: impl Fn(&ControlMessage) -> bool,
) -> ControlMessage {
    let mut reader = BufReader::new(stream);
    loop {
        let mut response = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            reader.read_line(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        let message = serde_json::from_str(response.trim()).unwrap();
        if matches(&message) {
            return message;
        }
    }
}

#[tokio::test]
async fn management_reads_return_exact_authoritative_empty_state() {
    let auth = ManagementAuth::new(&[]).await;
    let catalog = TopologyCatalog::new();
    let service = ManagementService::new(
        catalog,
        ShardManager::new(),
        ManagementOperationStore::new(Arc::new(InMemory::new())),
        NodeConfig::default(),
    );
    let server = auth.start_management(service).await;
    let mut client = auth.client(server.addr).await;

    let nodes = client
        .list_nodes(ListNodesRequest {
            protocol_version: 1,
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(nodes.protocol_version, 1);
    assert_eq!(nodes.source_version, "topology:0");
    assert!(nodes
        .observed_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    assert!(nodes.nodes.is_empty());
    assert!(nodes.next_page_token.is_empty());

    let health = client
        .get_health(GetHealthRequest {
            protocol_version: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health.protocol_version, 1);
    assert_eq!(health.source_version, "topology:0");
    assert!(health
        .observed_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    assert_eq!(health.state, "unknown");
    assert_eq!(
        health.reason,
        "authoritative process health telemetry is not registered"
    );

    let missing = client
        .get_node(GetNodeRequest {
            protocol_version: 1,
            node_id: "404".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);
    assert_eq!(missing.message(), "node 404 not found");
    server.shutdown();
}

#[tokio::test]
async fn management_denial_is_audited_without_tearing_down_session_or_mutating_state() {
    let auth = ManagementAuth::with_presets(
        &[],
        &[PermissionPreset::Administrator, PermissionPreset::Operator],
    )
    .await;
    let operations = ManagementOperationStore::new(Arc::new(InMemory::new()));
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        operations.clone(),
        NodeConfig::default(),
    );
    let server = auth.start_management(service).await;
    let mut client = auth.client(server.addr).await;

    let error = client
        .create_backup(CreateBackupRequest {
            protocol_version: 1,
            destination: "file:///tmp/unauthorized-backup".to_owned(),
            idempotency_key: "unauthorized-backup".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    let (records, next_page_token) = operations.list(50, "").await.unwrap();
    assert!(records.is_empty());
    assert!(next_page_token.is_empty());

    let nodes = client
        .list_nodes(ListNodesRequest {
            protocol_version: 1,
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(nodes.protocol_version, 1);
    assert_eq!(nodes.source_version, "topology:0");
    assert!(nodes.nodes.is_empty());
    assert!(nodes.next_page_token.is_empty());
    assert!(nodes
        .observed_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());

    let audit = auth.catalog.list_security_audit_records().await.unwrap();
    let denial = audit
        .iter()
        .find(|record| record.action == SecurityAuditClass::AuthorizationDenial)
        .unwrap();
    assert_eq!(
        denial.actor,
        rockstream_types::authorization::AuditIdentity::Verified("principal:1".to_owned())
    );
    assert_eq!(denial.target, AuditField::Known("BackupCreate".to_owned()));
    assert_eq!(denial.outcome, SecurityAuditOutcome::Denied);
    server.shutdown();
}

#[tokio::test]
async fn management_principal_listing_pages_grants_without_exposing_login_credentials() {
    let auth = ManagementAuth::with_presets(&[], &[PermissionPreset::Administrator]).await;
    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "SCRAM reader".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: "private-scram-user".to_owned(),
            salt: vec![0x11; 16],
            iterations: 4096,
            stored_key: vec![0x22; 32],
            server_key: vec![0x33; 32],
        }],
        preset_grants: vec![
            ScopedPresetGrant {
                preset: PermissionPreset::DataReader,
                scope: PolicyScope::Namespace {
                    namespace: "sales".to_owned(),
                },
            },
            ScopedPresetGrant {
                preset: PermissionPreset::Builder,
                scope: PolicyScope::Namespace {
                    namespace: "sales".to_owned(),
                },
            },
        ],
        scoped_grants: vec![ScopedActionGrant {
            action: SecurityAction::SecretUse,
            scope: PolicyScope::Secret {
                namespace: "sales".to_owned(),
                name: "warehouse".to_owned(),
                consumer: Some(PrincipalId(2)),
            },
        }],
    };
    let audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "management-test-second-principal".to_owned(),
        timestamp_ms: chrono::Utc::now().timestamp_millis().max(1) as u64,
        actor: AuditIdentity::Verified("principal:1".to_owned()),
        target: AuditField::Known("principal:2".to_owned()),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::NotApplicable,
        operation_id: AuditField::Known("2".to_owned()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::Known(format!(
            "revision=1;policy={}",
            serde_json::to_string(&(
                principal.enabled,
                &principal.preset_grants,
                &principal.scoped_grants
            ))
            .unwrap()
        )),
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

    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        ManagementOperationStore::new(Arc::new(InMemory::new())),
        NodeConfig::default(),
    );
    let server = auth.start_management(service).await;
    let mut client = auth.client(server.addr).await;
    let first_page = client
        .list_security_principals(ListSecurityPrincipalsRequest {
            protocol_version: 1,
            page_size: 1,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        first_page,
        rockstream_management_proto::v1::ListSecurityPrincipalsResponse {
            protocol_version: 1,
            principals: vec![rockstream_management_proto::v1::SecurityPrincipal {
                principal_id: 1,
                display_name: "Management test administrator".to_owned(),
                enabled: true,
                revision: 1,
                preset_grants: vec![rockstream_management_proto::v1::SecurityPresetGrant {
                    preset: "administrator".to_owned(),
                    scope_json: "\"cluster\"".to_owned(),
                }],
                scoped_grants: Vec::new(),
                effective_permissions: vec![
                    management_action("principal_manage", "\"cluster\""),
                    management_action("grant_manage", "\"cluster\""),
                    management_action("auth_config_manage", "\"cluster\""),
                    management_action("secret_metadata_read", "\"cluster\""),
                    management_action("secret_manage", "\"cluster\""),
                    management_action("secret_rotate", "\"cluster\""),
                    management_action("security_audit_read", "\"cluster\""),
                    management_action("schema_drop", "\"cluster\""),
                    management_action("namespace_create", "\"cluster\""),
                    management_action("index_manage", "\"cluster\""),
                ],
            }],
            next_page_token: "1".to_owned(),
        }
    );

    let second_page = client
        .list_security_principals(ListSecurityPrincipalsRequest {
            protocol_version: 1,
            page_size: 1,
            page_token: first_page.next_page_token,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        second_page,
        rockstream_management_proto::v1::ListSecurityPrincipalsResponse {
            protocol_version: 1,
            principals: vec![rockstream_management_proto::v1::SecurityPrincipal {
                principal_id: 2,
                display_name: "SCRAM reader".to_owned(),
                enabled: true,
                revision: 1,
                preset_grants: vec![
                    rockstream_management_proto::v1::SecurityPresetGrant {
                        preset: "data_reader".to_owned(),
                        scope_json:
                            "{\"namespace\":{\"namespace\":\"sales\"}}".to_owned(),
                    },
                    rockstream_management_proto::v1::SecurityPresetGrant {
                        preset: "builder".to_owned(),
                        scope_json:
                            "{\"namespace\":{\"namespace\":\"sales\"}}".to_owned(),
                    },
                ],
                scoped_grants: vec![rockstream_management_proto::v1::SecurityActionGrant {
                    action: "secret_use".to_owned(),
                    scope_json: "{\"secret\":{\"namespace\":\"sales\",\"name\":\"warehouse\",\"consumer\":2}}"
                        .to_owned(),
                }],
                effective_permissions: vec![
                    management_action(
                        "metadata_inspect",
                        "{\"namespace\":{\"namespace\":\"sales\"}}",
                    ),
                    management_action(
                        "data_read",
                        "{\"namespace\":{\"namespace\":\"sales\"}}",
                    ),
                    management_action(
                        "data_subscribe",
                        "{\"namespace\":{\"namespace\":\"sales\"}}",
                    ),
                    management_action(
                        "data_write",
                        "{\"namespace\":{\"namespace\":\"sales\"}}",
                    ),
                    management_action(
                        "view_author",
                        "{\"namespace\":{\"namespace\":\"sales\"}}",
                    ),
                    management_action(
                        "table_create",
                        "{\"namespace\":{\"namespace\":\"sales\"}}",
                    ),
                    management_action(
                        "source_lifecycle",
                        "{\"namespace\":{\"namespace\":\"sales\"}}",
                    ),
                    management_action(
                        "workload_change",
                        "{\"namespace\":{\"namespace\":\"sales\"}}",
                    ),
                    management_action(
                        "schema_create",
                        "{\"namespace\":{\"namespace\":\"sales\"}}",
                    ),
                    management_action(
                        "secret_use",
                        "{\"secret\":{\"namespace\":\"sales\",\"name\":\"warehouse\",\"consumer\":2}}",
                    ),
                ],
            }],
            next_page_token: String::new(),
        }
    );
    let response_text = format!("{second_page:?}");
    assert!(!response_text.contains("private-scram-user"));
    assert!(!response_text.contains("stored_key"));
    assert!(!response_text.contains("server_key"));
    server.shutdown();
}

#[tokio::test]
async fn management_grant_mutation_is_idempotent_and_audits_the_complete_result() {
    let auth = ManagementAuth::with_presets(
        &[],
        &[PermissionPreset::Administrator, PermissionPreset::Operator],
    )
    .await;
    let target = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "SCRAM target".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: "private-user".to_owned(),
            salt: vec![0x11; 16],
            iterations: 4096,
            stored_key: vec![0x22; 32],
            server_key: vec![0x33; 32],
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::DataReader,
            scope: PolicyScope::Namespace {
                namespace: "sales".to_owned(),
            },
        }],
        scoped_grants: Vec::new(),
    };
    let initial_audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "management-grant-mutation-target".to_owned(),
        timestamp_ms: chrono::Utc::now().timestamp_millis().max(1) as u64,
        actor: AuditIdentity::Verified("principal:1".to_owned()),
        target: AuditField::Known("principal:2".to_owned()),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::NotApplicable,
        operation_id: AuditField::Known("2".to_owned()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::Known(format!(
            "revision=1;policy={}",
            serde_json::to_string(&(target.enabled, &target.preset_grants, &target.scoped_grants))
                .unwrap()
        )),
    };
    auth.catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: target.clone(),
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: initial_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let operation_store = Arc::new(InMemory::new());
    let operations = ManagementOperationStore::new(operation_store.clone());
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        operations.clone(),
        NodeConfig::default(),
    );
    let server = auth.start_management(service).await;
    let mut client = auth.client(server.addr).await;
    let operation_id = "00000000-0000-4000-8000-000000000123";
    let request = SetSecurityPrincipalGrantsRequest {
        protocol_version: 1,
        principal_id: 2,
        expected_revision: 1,
        expected_grantor_revision: 1,
        idempotency_key: operation_id.to_owned(),
        preset_grants: vec![rockstream_management_proto::v1::SecurityPresetGrant {
            preset: "data_reader".to_owned(),
            scope_json: "{\"namespace\":{\"namespace\":\"sales\"}}".to_owned(),
        }],
        scoped_grants: vec![management_action(
            "secret_use",
            "{\"secret\":{\"namespace\":\"sales\",\"name\":\"warehouse\",\"consumer\":2}}",
        )],
    };
    let first = client
        .set_security_principal_grants(request.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(first.operation_id, operation_id);
    assert_eq!(first.kind, "security_principal_grants");
    assert_eq!(first.state, "succeeded");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&first.result_json).unwrap(),
        serde_json::json!({
            "principal_id": 2,
            "revision": 2,
            "preset_grants": [{
                "preset": "data_reader",
                "scope": {"namespace": {"namespace": "sales"}}
            }],
            "scoped_grants": [{
                "action": "secret_use",
                "scope": {"secret": {
                    "namespace": "sales",
                    "name": "warehouse",
                    "consumer": 2
                }}
            }],
            "effective_permissions": [
                {"action": "metadata_inspect", "scope": {"namespace": {"namespace": "sales"}}},
                {"action": "data_read", "scope": {"namespace": {"namespace": "sales"}}},
                {"action": "data_subscribe", "scope": {"namespace": {"namespace": "sales"}}},
                {"action": "secret_use", "scope": {"secret": {
                    "namespace": "sales",
                    "name": "warehouse",
                    "consumer": 2
                }}}
            ]
        })
    );
    assert!(!first.result_json.contains("private-user"));
    assert!(!first.result_json.contains("stored_key"));
    let retry = client
        .set_security_principal_grants(request.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(retry, first);
    let recovered_operation = ManagementOperationStore::new(operation_store)
        .get(operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recovered_operation.result(),
        Some(&serde_json::from_str(&first.result_json).unwrap())
    );
    let recovered_catalog = DurableCatalogStore::recover(auth.store.clone(), "management-security")
        .await
        .unwrap();
    let recovered_principal = recovered_catalog
        .get_security_principal(PrincipalId(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered_principal.revision, 2);
    assert_eq!(recovered_principal.preset_grants, target.preset_grants);
    assert_eq!(recovered_principal.scoped_grants.len(), 1);
    let looked_up = client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: operation_id.to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(looked_up.result_json, first.result_json);

    let mut conflicting = request;
    conflicting.scoped_grants.clear();
    let conflict = client
        .set_security_principal_grants(conflicting)
        .await
        .unwrap_err();
    assert_eq!(conflict.code(), tonic::Code::AlreadyExists);
    assert_eq!(
        auth.catalog
            .get_security_principal(PrincipalId(2))
            .await
            .unwrap()
            .unwrap()
            .revision,
        2
    );
    let records = auth.catalog.list_security_audit_records().await.unwrap();
    let audit = records
        .iter()
        .find(|record| record.event_id == format!("management-security-grants:{operation_id}"))
        .unwrap();
    assert_eq!(
        audit.actor,
        AuditIdentity::Verified("principal:1".to_owned())
    );
    assert_eq!(audit.target, AuditField::Known("principal:2".to_owned()));
    assert_eq!(audit.scope, AuditField::Known("cluster".to_owned()));
    assert_eq!(audit.action, SecurityAuditClass::GrantChange);
    assert_eq!(audit.outcome, SecurityAuditOutcome::Success);
    assert_eq!(
        audit.correlation,
        AuditCorrelation::Observed(operation_id.to_owned())
    );
    assert_eq!(
        audit.operation_id,
        AuditField::Known(operation_id.to_owned())
    );
    assert_eq!(audit.policy_revision, AuditField::Known(2));
    assert_eq!(
        audit.before_state,
        AuditField::Known(format!(
            "revision=1;policy={}",
            serde_json::to_string(&(target.enabled, &target.preset_grants, &target.scoped_grants))
                .unwrap()
        ))
    );
    assert!(
        matches!(&audit.after_state, AuditField::Known(value) if value.starts_with("revision=2;policy="))
    );
    let started = records
        .iter()
        .find(|record| {
            record.event_id == format!("management-security-grants-start:{operation_id}")
        })
        .unwrap();
    assert_eq!(
        started.actor,
        AuditIdentity::Verified("principal:1".to_owned())
    );
    assert_eq!(started.target, AuditField::Known("principal:2".to_owned()));
    assert_eq!(started.scope, AuditField::Known("cluster".to_owned()));
    assert_eq!(started.action, SecurityAuditClass::AdminOperationStart);
    assert_eq!(started.outcome, SecurityAuditOutcome::Success);
    assert_eq!(
        started.correlation,
        AuditCorrelation::Observed(operation_id.to_owned())
    );
    assert_eq!(
        started.operation_id,
        AuditField::Known(operation_id.to_owned())
    );
    assert_eq!(started.policy_revision, AuditField::Known(1));
    assert_eq!(started.before_state, AuditField::NotApplicable);
    assert_eq!(started.after_state, AuditField::NotApplicable);
    let completion = records
        .iter()
        .find(|record| {
            record.event_id == format!("management-security-grants-completion:{operation_id}")
        })
        .unwrap();
    assert_eq!(
        completion.actor,
        AuditIdentity::Verified("principal:1".to_owned())
    );
    assert_eq!(
        completion.target,
        AuditField::Known("principal:2".to_owned())
    );
    assert_eq!(completion.scope, AuditField::Known("cluster".to_owned()));
    assert_eq!(
        completion.action,
        SecurityAuditClass::AdminOperationCompletion
    );
    assert_eq!(completion.outcome, SecurityAuditOutcome::Success);
    assert_eq!(
        completion.correlation,
        AuditCorrelation::Observed(operation_id.to_owned())
    );
    assert_eq!(
        completion.operation_id,
        AuditField::Known(operation_id.to_owned())
    );
    assert_eq!(completion.policy_revision, AuditField::Known(2));
    assert_eq!(completion.before_state, audit.before_state);
    assert_eq!(completion.after_state, audit.after_state);
    let (listed, _) = operations.list(10, "").await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(
        client
            .list_operations(ListOperationsRequest {
                protocol_version: 1,
                page_size: 10,
                page_token: String::new(),
            })
            .await
            .unwrap()
            .into_inner()
            .operations[0]
            .result_json,
        ""
    );
    server.shutdown();
}

#[tokio::test]
async fn grantor_can_reconcile_its_self_revocation_without_removing_last_admin() {
    let auth = ManagementAuth::with_presets(
        &[],
        &[PermissionPreset::Administrator, PermissionPreset::Operator],
    )
    .await;
    let second_admin = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "Second administrator".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls {
            certificate_sha256: [8; 32],
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: Vec::new(),
    };
    let initial_audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "management-self-revoke-second-admin".to_owned(),
        timestamp_ms: chrono::Utc::now().timestamp_millis().max(1) as u64,
        actor: AuditIdentity::Verified("principal:1".to_owned()),
        target: AuditField::Known("principal:2".to_owned()),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::NotApplicable,
        operation_id: AuditField::Known("2".to_owned()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::Known(format!(
            "revision=1;policy={}",
            serde_json::to_string(&(
                second_admin.enabled,
                &second_admin.preset_grants,
                &second_admin.scoped_grants
            ))
            .unwrap()
        )),
    };
    auth.catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: second_admin,
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: initial_audit,
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
    );
    let server = auth.start_management(service).await;
    let mut client = auth.client(server.addr).await;
    let operation_id = "00000000-0000-4000-8000-000000000124";
    let mutation = SetSecurityPrincipalGrantsRequest {
        protocol_version: 1,
        principal_id: 1,
        expected_revision: 1,
        expected_grantor_revision: 1,
        idempotency_key: operation_id.to_owned(),
        preset_grants: vec![rockstream_management_proto::v1::SecurityPresetGrant {
            preset: "operator".to_owned(),
            scope_json: "\"cluster\"".to_owned(),
        }],
        scoped_grants: Vec::new(),
    };
    let operation = client
        .set_security_principal_grants(mutation)
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(operation.state, "succeeded");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&operation.result_json).unwrap(),
        serde_json::json!({
            "principal_id": 1,
            "revision": 2,
            "preset_grants": [{"preset": "operator", "scope": "cluster"}],
            "scoped_grants": [],
            "effective_permissions": [
                {"action": "cluster_observe", "scope": "cluster"},
                {"action": "node_drain", "scope": "cluster"},
                {"action": "shard_migrate", "scope": "cluster"},
                {"action": "operation_cancel", "scope": "cluster"},
                {"action": "operation_history_read", "scope": "cluster"},
                {"action": "diagnostics_read", "scope": "cluster"},
                {"action": "support_generate", "scope": "cluster"},
                {"action": "support_download", "scope": "cluster"}
            ]
        })
    );
    assert_eq!(
        auth.catalog
            .get_security_principal(PrincipalId(1))
            .await
            .unwrap()
            .unwrap()
            .preset_grants,
        vec![ScopedPresetGrant {
            preset: PermissionPreset::Operator,
            scope: PolicyScope::Cluster,
        }]
    );

    let listing = client
        .list_security_principals(ListSecurityPrincipalsRequest {
            protocol_version: 1,
            page_size: 10,
            page_token: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(listing.code(), tonic::Code::PermissionDenied);
    let reconciled = client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: operation_id.to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(reconciled.result_json, operation.result_json);
    assert_eq!(
        auth.catalog
            .get_security_principal(PrincipalId(2))
            .await
            .unwrap()
            .unwrap()
            .revision,
        1
    );
    server.shutdown();
}

fn management_action(
    action: &str,
    scope_json: &str,
) -> rockstream_management_proto::v1::SecurityActionGrant {
    rockstream_management_proto::v1::SecurityActionGrant {
        action: action.to_owned(),
        scope_json: scope_json.to_owned(),
    }
}

#[tokio::test]
async fn management_rejects_unmapped_node_certificate_before_application_session() {
    let auth = ManagementAuth::new(&[]).await;
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        ManagementOperationStore::new(Arc::new(InMemory::new())),
        NodeConfig::default(),
    );
    let server = auth.start_management(service).await;

    let mut node_client = auth
        .try_client_with_identity(
            server.addr,
            &auth.pki.mismatched_cert_path,
            &auth.pki.mismatched_key_path,
        )
        .await
        .unwrap();
    let rejected = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        node_client.list_nodes(ListNodesRequest {
            protocol_version: 1,
            page_size: 0,
            page_token: String::new(),
        }),
    )
    .await
    .unwrap();
    assert!(rejected.is_err());

    let mut valid_client = auth.client(server.addr).await;
    let nodes = valid_client
        .list_nodes(ListNodesRequest {
            protocol_version: 1,
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(nodes.nodes.is_empty());

    let audit = auth.catalog.list_security_audit_records().await.unwrap();
    assert_eq!(
        audit.iter().map(|record| record.action).collect::<Vec<_>>(),
        vec![
            SecurityAuditClass::GrantChange,
            SecurityAuditClass::AuthenticationFailure,
            SecurityAuditClass::AuthenticationSuccess,
        ]
    );
    let failures: Vec<_> = audit
        .iter()
        .filter(|record| record.action == SecurityAuditClass::AuthenticationFailure)
        .collect();
    assert_eq!(failures.len(), 1);
    let failure = failures[0];
    assert!(failure.event_id.starts_with("management-authn-"));
    assert!(failure.timestamp_ms > 0);
    let mut actual = failure.clone();
    actual.event_id = "management-authn-normalized".to_owned();
    actual.timestamp_ms = 1;
    assert_eq!(
        actual,
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "management-authn-normalized".to_owned(),
            timestamp_ms: 1,
            actor: AuditIdentity::Unverified("worker:worker-999".to_owned()),
            target: AuditField::Known("management-api".to_owned()),
            scope: AuditField::NotApplicable,
            action: SecurityAuditClass::AuthenticationFailure,
            outcome: SecurityAuditOutcome::Failure,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Unknown,
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
    server.shutdown();
}

#[tokio::test]
async fn management_rejects_incompatible_versions_before_reading_state() {
    let auth = ManagementAuth::new(&[]).await;
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        ManagementOperationStore::new(Arc::new(InMemory::new())),
        NodeConfig::default(),
    );
    let server = auth.start_management(service).await;
    let mut client = auth.client(server.addr).await;
    macro_rules! assert_incompatible {
        ($method:ident, $request:expr) => {{
            let error = client.$method($request).await.unwrap_err();
            assert_eq!(
                error.code(),
                tonic::Code::FailedPrecondition,
                stringify!($method)
            );
            assert_eq!(
                error.message(),
                "unsupported protocol version 2; supported range is 1..=1",
                stringify!($method)
            );
        }};
    }
    assert_incompatible!(
        get_cluster_status,
        GetClusterStatusRequest {
            protocol_version: 2
        }
    );
    assert_incompatible!(
        list_nodes,
        ListNodesRequest {
            protocol_version: 2,
            page_size: 0,
            page_token: String::new()
        }
    );
    assert_incompatible!(
        get_node,
        GetNodeRequest {
            protocol_version: 2,
            node_id: String::new()
        }
    );
    assert_incompatible!(
        list_shards,
        ListShardsRequest {
            protocol_version: 2,
            page_size: 0,
            page_token: String::new()
        }
    );
    assert_incompatible!(
        get_shard,
        GetShardRequest {
            protocol_version: 2,
            shard_id: String::new()
        }
    );
    assert_incompatible!(
        list_operations,
        ListOperationsRequest {
            protocol_version: 2,
            page_size: 0,
            page_token: String::new()
        }
    );
    assert_incompatible!(
        get_operation,
        GetOperationRequest {
            protocol_version: 2,
            operation_id: String::new()
        }
    );
    assert_incompatible!(
        get_config_summary,
        GetConfigSummaryRequest {
            protocol_version: 2
        }
    );
    assert_incompatible!(
        get_capabilities,
        GetCapabilitiesRequest {
            protocol_version: 2
        }
    );
    assert_incompatible!(
        get_health,
        GetHealthRequest {
            protocol_version: 2
        }
    );
    assert_incompatible!(
        drain_worker,
        DrainWorkerRequest {
            protocol_version: 2,
            worker_id: String::new(),
            idempotency_key: String::new()
        }
    );
    assert_incompatible!(
        migrate_shard,
        MigrateShardRequest {
            protocol_version: 2,
            shard_id: String::new(),
            target_node_id: String::new(),
            idempotency_key: String::new()
        }
    );
    assert_incompatible!(
        create_backup,
        CreateBackupRequest {
            protocol_version: 2,
            destination: String::new(),
            idempotency_key: String::new()
        }
    );
    assert_incompatible!(
        cancel_operation,
        CancelOperationRequest {
            protocol_version: 2,
            operation_id: String::new()
        }
    );
    server.shutdown();
}

#[tokio::test]
async fn management_start_recovers_running_operation_with_exact_phase() {
    let auth = ManagementAuth::new(&[]).await;
    let store = Arc::new(InMemory::new());
    let operations = ManagementOperationStore::new(store);
    operations
        .accept_idempotent(
            "drain-recovery-key",
            1,
            &serde_json::json!({"worker_id": 9}),
            "op_recovered",
            OperationKind::DrainWorker,
            1_000,
        )
        .await
        .unwrap();
    operations
        .transition(
            "op_recovered",
            OperationUpdate {
                status: OperationStatus::Running,
                updated_at_ms: 2_000,
                progress: Some(5),
                phase: Some("worker_handoff_started".to_owned()),
                error_code: None,
                next_steps: Vec::new(),
            },
        )
        .await
        .unwrap();
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        operations,
        NodeConfig::default(),
    );
    let server = auth.start_management(service).await;
    let mut client = auth.client(server.addr).await;
    let operation = client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: "op_recovered".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(
        operation,
        Operation {
            operation_id: "op_recovered".to_owned(),
            kind: "drain_worker".to_owned(),
            state: "waiting".to_owned(),
            started_at: "1970-01-01T00:00:01.000Z".to_owned(),
            updated_at: operation.updated_at.clone(),
            progress: "5%".to_owned(),
            phase: "worker_handoff_started".to_owned(),
            error_code: String::new(),
            next_steps: vec![
                "Management restarted; reconcile this operation before retrying.".to_owned(),
            ],
            source_version: "operation-record:4".to_owned(),
            result_json: String::new(),
        }
    );
    assert!(operation
        .updated_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    server.shutdown();
}

#[tokio::test]
async fn management_cancellation_obeys_the_safe_boundary() {
    let auth = ManagementAuth::new(&[]).await;
    let operations = ManagementOperationStore::new(Arc::new(InMemory::new()));
    operations
        .accept_idempotent(
            "cancel-pending-key",
            1,
            &serde_json::json!({"worker_id": 11}),
            "op_cancel_pending",
            OperationKind::DrainWorker,
            1_000,
        )
        .await
        .unwrap();
    operations
        .accept_idempotent(
            "cancel-late-key",
            1,
            &serde_json::json!({"worker_id": 12}),
            "op_cancel_late",
            OperationKind::DrainWorker,
            1_000,
        )
        .await
        .unwrap();
    operations
        .transition(
            "op_cancel_late",
            OperationUpdate {
                status: OperationStatus::Waiting,
                updated_at_ms: 2_000,
                progress: Some(5),
                phase: Some("worker_handoff_started".to_owned()),
                error_code: None,
                next_steps: Vec::new(),
            },
        )
        .await
        .unwrap();
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        operations.clone(),
        NodeConfig::default(),
    );
    let server = auth.start_management(service).await;
    let mut client = auth.client(server.addr).await;

    let cancelled = client
        .cancel_operation(CancelOperationRequest {
            protocol_version: 1,
            operation_id: "op_cancel_pending".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    let updated_at = cancelled.updated_at.clone();
    assert_eq!(
        cancelled,
        Operation {
            operation_id: "op_cancel_pending".to_owned(),
            kind: "drain_worker".to_owned(),
            state: "cancelled".to_owned(),
            started_at: "1970-01-01T00:00:01.000Z".to_owned(),
            updated_at,
            progress: String::new(),
            phase: "cancelled_before_start".to_owned(),
            error_code: String::new(),
            next_steps: Vec::new(),
            source_version: "operation-record:4".to_owned(),
            result_json: String::new(),
        }
    );

    let rejected = client
        .cancel_operation(CancelOperationRequest {
            protocol_version: 1,
            operation_id: "op_cancel_late".to_owned(),
        })
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        rejected.message(),
        "operation cancellation is past its safe boundary"
    );
    let still_running = client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: "op_cancel_late".to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(
        still_running,
        Operation {
            operation_id: "op_cancel_late".to_owned(),
            kind: "drain_worker".to_owned(),
            state: "waiting".to_owned(),
            started_at: "1970-01-01T00:00:01.000Z".to_owned(),
            updated_at: "1970-01-01T00:00:02.000Z".to_owned(),
            progress: "5%".to_owned(),
            phase: "worker_handoff_started".to_owned(),
            error_code: String::new(),
            next_steps: Vec::new(),
            source_version: "operation-record:4".to_owned(),
            result_json: String::new(),
        }
    );
    server.shutdown();
}

#[tokio::test]
async fn management_start_replays_pending_operation_audit_before_listening() {
    let auth = ManagementAuth::new(&[]).await;
    let object_store = Arc::new(InMemory::new());
    let operations = ManagementOperationStore::new(object_store.clone());
    let operation_id = "op_cancel_audit_recovery";
    let now_ms = chrono::Utc::now().timestamp_millis().max(1);
    operations
        .accept_idempotent(
            operation_id,
            1,
            &serde_json::json!({"worker_id": 11}),
            operation_id,
            OperationKind::DrainWorker,
            now_ms,
        )
        .await
        .unwrap();
    let audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: format!("management-operation-cancel:{operation_id}"),
        timestamp_ms: (now_ms + 1) as u64,
        actor: AuditIdentity::Verified("principal:1".to_owned()),
        target: AuditField::Known(format!("operation:{operation_id}")),
        scope: AuditField::Known(
            serde_json::to_string(&PolicyScope::Operation {
                operation_id: operation_id.to_owned(),
            })
            .unwrap(),
        ),
        action: SecurityAuditClass::AdminOperationCompletion,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Observed(operation_id.to_owned()),
        operation_id: AuditField::Known(operation_id.to_owned()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::Known("pending".to_owned()),
        after_state: AuditField::Known("cancelled_before_start".to_owned()),
    };
    operations
        .transition_if_with_audit(
            operation_id,
            OperationStatus::Pending,
            None,
            OperationUpdate {
                status: OperationStatus::Cancelled,
                updated_at_ms: now_ms + 1,
                progress: None,
                phase: Some("cancelled_before_start".to_owned()),
                error_code: None,
                next_steps: Vec::new(),
            },
            audit.clone(),
        )
        .await
        .unwrap();

    let recovered_operations = ManagementOperationStore::new(object_store);
    assert_eq!(
        recovered_operations
            .pending_audit_records()
            .await
            .unwrap()
            .len(),
        1
    );
    let service = ManagementService::new(
        TopologyCatalog::new(),
        ShardManager::new(),
        recovered_operations.clone(),
        NodeConfig::default(),
    );
    let server = auth.start_management(service).await;

    assert!(recovered_operations
        .get(operation_id)
        .await
        .unwrap()
        .unwrap()
        .pending_audit()
        .is_none());
    assert!(auth
        .catalog
        .list_security_audit_records()
        .await
        .unwrap()
        .contains(&audit));
    server.shutdown();
}

#[tokio::test]
async fn management_drain_is_idempotent_and_transfers_the_owned_shard() {
    let auth = ManagementAuth::new(&[]).await;
    let catalog = TopologyCatalog::new();
    let shard_manager = ShardManager::new();
    let operations = Arc::new(InMemory::new());
    let management_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management_addr = management_listener.local_addr().unwrap();
    drop(management_listener);
    let service = auth.protect_control(
        rockstream_control::ControlService::new(catalog.clone())
            .with_shard_manager(shard_manager.clone())
            .with_management(
                management_addr.to_string(),
                operations,
                NodeConfig::default(),
            ),
    );
    let control = service
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let mut donor = register_worker(control.addr, 1).await;
    let mut recipient = register_worker(control.addr, 2).await;
    shard_manager.acquire(ShardId(77), WorkerId(1)).unwrap();
    let mut client = auth.client(management_addr).await;
    let request = DrainWorkerRequest {
        protocol_version: 1,
        worker_id: "1".to_owned(),
        idempotency_key: "drain-worker-1".to_owned(),
    };
    let accepted = client
        .drain_worker(request.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    let repeated = client
        .drain_worker(request.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(accepted.operation_id, request.idempotency_key);
    assert_eq!(repeated.operation_id, accepted.operation_id);
    let reconciled = client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: request.idempotency_key,
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(reconciled.operation_id, accepted.operation_id);
    assert_eq!(reconciled.kind, accepted.kind);
    assert_eq!(accepted.kind, "drain_worker");
    assert_eq!(accepted.state, "pending");
    assert!(accepted.phase.is_empty());
    assert!(accepted.error_code.is_empty());
    assert!(accepted.next_steps.is_empty());
    assert_eq!(
        shard_manager.get(ShardId(77)).unwrap().worker_id,
        WorkerId(1)
    );

    let _ = read_until(&mut donor, |message| {
        matches!(message, ControlMessage::BeginDrain(_))
    })
    .await;
    let acknowledgement = serde_json::to_string(&WorkerMessage::DrainAck {
        worker_id: WorkerId(1),
        shards_remaining: 0,
    })
    .unwrap()
        + "\n";
    donor.write_all(acknowledgement.as_bytes()).await.unwrap();
    let assignment = read_until(&mut recipient, |message| {
        matches!(message, ControlMessage::ShardAssigned { lease, .. } if lease.shard_id == ShardId(77))
    })
    .await;
    let (lease, transfer_id) = match assignment {
        ControlMessage::ShardAssigned {
            lease,
            operation_id: Some(transfer_id),
        } => (lease, transfer_id),
        other => panic!("expected recipient shard assignment, got {other:?}"),
    };
    assert_eq!(lease.worker_id, WorkerId(2));
    assert_eq!(lease.shard_id, ShardId(77));
    assert!(transfer_id.starts_with(&format!("{}:shard:", accepted.operation_id)));

    let awaiting_recipient = client
        .get_operation(GetOperationRequest {
            protocol_version: 1,
            operation_id: accepted.operation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert!(matches!(
        awaiting_recipient.state.as_str(),
        "pending" | "running" | "waiting"
    ));
    assert!(matches!(
        catalog.get(WorkerId(1)).unwrap().lifecycle,
        rockstream_types::topology::WorkerLifecycleState::Draining { .. }
    ));

    recipient
        .write_all(
            (serde_json::to_string(&WorkerMessage::ShardTransferAck {
                operation_id: transfer_id,
                stage: "recipient".to_owned(),
                worker_id: lease.worker_id,
                shard_id: lease.shard_id,
                lease_token: lease.lease_token,
                success: true,
                error: None,
            })
            .unwrap()
                + "\n")
                .as_bytes(),
        )
        .await
        .unwrap();

    let mut completed = None;
    let mut last_state = None;
    for _ in 0..50 {
        let response = client
            .get_operation(GetOperationRequest {
                protocol_version: 1,
                operation_id: accepted.operation_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .operation
            .unwrap();
        last_state = Some((
            response.state.clone(),
            response.phase.clone(),
            response.error_code.clone(),
        ));
        if response.state == "succeeded" {
            completed = Some(response);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let completed = completed.unwrap_or_else(|| {
        panic!("drain operation did not reach its durable terminal state: {last_state:?}")
    });
    assert_eq!(completed.operation_id, accepted.operation_id);
    assert_eq!(completed.kind, "drain_worker");
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");
    assert!(completed.error_code.is_empty());
    assert!(completed.next_steps.is_empty());
    assert_eq!(completed.source_version, "operation-record:4");
    assert_eq!(
        shard_manager.get(ShardId(77)).unwrap().worker_id,
        WorkerId(2)
    );
    control.shutdown();
}

#[tokio::test]
async fn management_migrate_moves_a_real_shard_once() {
    let auth = ManagementAuth::new(&[]).await;
    let catalog = TopologyCatalog::new();
    let shard_manager = ShardManager::new();
    let operations = Arc::new(InMemory::new());
    let management_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management_addr = management_listener.local_addr().unwrap();
    drop(management_listener);
    let service = auth.protect_control(
        rockstream_control::ControlService::new(catalog)
            .with_shard_manager(shard_manager.clone())
            .with_management(
                management_addr.to_string(),
                operations,
                NodeConfig::default(),
            ),
    );
    let control = service
        .with_insecure_dev_mode()
        .start("127.0.0.1:0")
        .await
        .unwrap();
    let mut donor = register_worker(control.addr, 11).await;
    let mut recipient = register_worker(control.addr, 12).await;
    let old_lease = shard_manager.acquire(ShardId(77), WorkerId(11)).unwrap();
    let mut client = auth.client(management_addr).await;
    let request = MigrateShardRequest {
        protocol_version: 1,
        shard_id: "77".to_owned(),
        target_node_id: "12".to_owned(),
        idempotency_key: "migrate-shard-77-to-12".to_owned(),
    };
    let accepted = client
        .migrate_shard(request.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    let retried = client
        .migrate_shard(request.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(accepted.operation_id, request.idempotency_key);
    assert_eq!(retried.operation_id, accepted.operation_id);
    assert_eq!(accepted.kind, "migrate_shard");
    assert!(matches!(accepted.state.as_str(), "pending" | "running"));

    let prepare = read_until(&mut donor, |message| {
        matches!(message, ControlMessage::PrepareShardTransfer { operation_id, lease }
            if operation_id == &accepted.operation_id && lease.shard_id == ShardId(77))
    })
    .await;
    let lease = match prepare {
        ControlMessage::PrepareShardTransfer { lease, .. } => lease,
        other => panic!("expected donor prepare, got {other:?}"),
    };
    assert_eq!(lease, old_lease);
    donor
        .write_all(
            (serde_json::to_string(&WorkerMessage::ShardTransferAck {
                operation_id: accepted.operation_id.clone(),
                stage: "donor".to_owned(),
                worker_id: WorkerId(11),
                shard_id: ShardId(77),
                lease_token: lease.lease_token,
                success: true,
                error: None,
            })
            .unwrap()
                + "\n")
                .as_bytes(),
        )
        .await
        .unwrap();

    let assignment = read_until(&mut recipient, |message| {
        matches!(message, ControlMessage::ShardAssigned { lease, operation_id: Some(id) }
            if id == &accepted.operation_id && lease.shard_id == ShardId(77))
    })
    .await;
    let new_lease = match assignment {
        ControlMessage::ShardAssigned { lease, .. } => lease,
        other => panic!("expected recipient lease, got {other:?}"),
    };
    assert_eq!(new_lease.worker_id, WorkerId(12));
    assert!(new_lease.lease_token.0 > old_lease.lease_token.0);
    recipient
        .write_all(
            (serde_json::to_string(&WorkerMessage::ShardTransferAck {
                operation_id: accepted.operation_id.clone(),
                stage: "recipient".to_owned(),
                worker_id: WorkerId(12),
                shard_id: ShardId(77),
                lease_token: new_lease.lease_token,
                success: true,
                error: None,
            })
            .unwrap()
                + "\n")
                .as_bytes(),
        )
        .await
        .unwrap();

    let mut completed = None;
    for _ in 0..100 {
        let operation = client
            .get_operation(GetOperationRequest {
                protocol_version: 1,
                operation_id: accepted.operation_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .operation
            .unwrap();
        if operation.state == "succeeded" {
            completed = Some(operation);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let completed = completed.expect("migration did not reach its durable terminal state");
    assert_eq!(completed.operation_id, accepted.operation_id);
    assert_eq!(completed.kind, "migrate_shard");
    assert_eq!(completed.state, "succeeded");
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");
    assert_eq!(completed.error_code, "");
    assert_eq!(completed.next_steps, Vec::<String>::new());
    assert_eq!(completed.source_version, "operation-record:4");
    assert_eq!(shard_manager.get(ShardId(77)), Some(new_lease));
    control.shutdown();
}

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rockstream_control::audit::FileAuditLog;
use rockstream_gateway::auth::Principal;
use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable, CatalogView};
use rockstream_gateway::role_catalog::{gen_scram_verifiers, RoleEntry};
use rockstream_gateway::server::GatewayHandler;
use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_gateway::GatewayError;
use rockstream_storage::catalog::{CatalogMutation, CatalogStore, CatalogTxn, DurableCatalogStore};
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ScopedPresetGrant, SecurityAuditClass, SecurityAuditOutcome, SecurityAuditRecord,
    SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION, SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::ids::PrincipalId;
use rockstream_types::mutation_policy::{pgwire_mutation_policy, PGWIRE_MUTATION_POLICY};

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_millis() as u64
}

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(Vec::new())
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

fn mutation_cases() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("CREATE VIEW", "CREATE VIEW blocked AS SELECT 1", "blocked"),
        (
            "REFRESH MATERIALIZED VIEW",
            "REFRESH MATERIALIZED VIEW blocked",
            "blocked",
        ),
        (
            "CREATE TABLE",
            "CREATE TABLE blocked (id BIGINT)",
            "blocked",
        ),
        (
            "CREATE SINK",
            "CREATE SINK blocked FOR VIEW source TO ICEBERG 'x'",
            "blocked",
        ),
        (
            "CREATE SOURCE",
            "CREATE SOURCE blocked FROM KAFKA",
            "blocked",
        ),
        (
            "ALTER SOURCE PAUSE",
            "ALTER SOURCE blocked PAUSE",
            "blocked",
        ),
        (
            "ALTER SOURCE RESUME",
            "ALTER SOURCE blocked RESUME",
            "blocked",
        ),
        ("ALTER SOURCE", "ALTER SOURCE blocked REPLAY DLQ", "blocked"),
        ("DROP SOURCE", "DROP SOURCE blocked", "blocked"),
        ("CREATE SECRET", "CREATE SECRET blocked TYPE 'x'", "blocked"),
        ("ALTER SECRET", "ALTER SECRET blocked SET 'x'", "blocked"),
        ("DROP SECRET", "DROP SECRET blocked", "blocked"),
        (
            "CREATE INDEX",
            "CREATE INDEX blocked ON source (id)",
            "blocked",
        ),
        ("DROP INDEX", "DROP INDEX blocked", "blocked"),
        ("REBUILD INDEX", "REBUILD INDEX blocked", "blocked"),
        ("MARK INDEX", "MARK INDEX blocked READY op_id=1", "blocked"),
        ("CREATE WORKLOAD", "CREATE WORKLOAD blocked", "blocked"),
        (
            "ALTER WORKLOAD",
            "ALTER WORKLOAD blocked SET (MEMORY_LIMIT = 1)",
            "blocked",
        ),
        ("DROP WORKLOAD", "DROP WORKLOAD blocked", "blocked"),
        ("INSERT", "INSERT INTO blocked (id) VALUES (1)", "blocked"),
        ("UPDATE", "UPDATE blocked SET id = 2", "blocked"),
        ("DELETE", "DELETE FROM blocked", "blocked"),
        ("COPY FROM STDIN", "COPY blocked FROM STDIN", "blocked"),
        ("CREATE NAMESPACE", "CREATE NAMESPACE blocked", "blocked"),
    ]
}

async fn denied_matrix(handler: &GatewayHandler, conn_id: &str) -> Vec<(String, String, String)> {
    let mut results = Vec::new();
    for (operation, query, resource) in mutation_cases() {
        let spec = pgwire_mutation_policy(query).expect("matrix query must be policy-covered");
        assert_eq!(spec.operation, operation);
        let responses = handler
            .dispatch_async_with_conn(query, Some(conn_id))
            .await
            .expect("authorization denial must be a pgwire response");
        assert_eq!(responses.len(), 1, "exact denial response for {query}");
        let error = match &responses[0] {
            pgwire::api::results::Response::Error(error) => error,
            _ => panic!("expected denial for {query}"),
        };
        assert_eq!(error.severity, "ERROR");
        assert_eq!(error.code, "42501");
        assert_eq!(
            error.message,
            format!(
                "[RS-2401] auth.permission_denied: principal 'viewer' lacks required role '{:?}' on {resource}. next_steps: Request an ACL grant for the required role, then retry.",
                spec.minimum_role
            ),
            "operation={operation} query={query}"
        );
        results.push((
            spec.operation.to_string(),
            spec.audit_action.to_string(),
            resource.to_string(),
        ));
    }
    results
}

fn handler_with_audit(catalog: Arc<CatalogStubs>, path: &std::path::Path) -> GatewayHandler {
    let log = Arc::new(FileAuditLog::open(path).expect("audit log"));
    GatewayHandler::new(catalog, Arc::new(NoopViewReader)).with_audit_log(log)
}

fn set_viewer(handler: &GatewayHandler, conn_id: &str) {
    let mut session = handler.sessions.entry(conn_id.to_string()).or_default();
    session.principal = Principal::Jwt {
        sub: "viewer".to_string(),
    };
}

#[tokio::test]
async fn pgwire_mutation_policy_simple_and_extended() {
    assert_eq!(PGWIRE_MUTATION_POLICY.len(), mutation_cases().len());
    let temp = tempfile::tempdir().unwrap();
    let catalog = Arc::new(CatalogStubs::new());
    let handler = handler_with_audit(catalog.clone(), &temp.path().join("simple.jsonl"));
    set_viewer(&handler, "simple");
    let simple = denied_matrix(&handler, "simple").await;
    assert!(catalog.get_table("blocked").is_none());

    let extended_handler = handler_with_audit(
        Arc::new(CatalogStubs::new()),
        &temp.path().join("extended.jsonl"),
    );
    set_viewer(&extended_handler, "extended");
    let extended = denied_matrix(&extended_handler, "extended").await;
    assert_eq!(simple, extended);
}

#[tokio::test]
async fn viewer_cannot_bypass_mutation_policy_with_multi_statement_sql() {
    let temp = tempfile::tempdir().unwrap();
    let audit_path = temp.path().join("multi.jsonl");
    let catalog = Arc::new(CatalogStubs::new());
    let handler = handler_with_audit(catalog.clone(), &audit_path);
    set_viewer(&handler, "multi");

    for query in [
        "CREATE TABLE first (id BIGINT)",
        "CREATE TABLE second (id BIGINT)",
    ] {
        let responses = handler
            .dispatch_async_with_conn(query, Some("multi"))
            .await
            .unwrap();
        assert!(matches!(
            responses.as_slice(),
            [pgwire::api::results::Response::Error(_)]
        ));
    }

    assert!(catalog.get_table("first").is_none());
    assert!(catalog.get_table("second").is_none());
    let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| (
                event.actor.as_str(),
                event.action.as_str(),
                event.resource.as_str(),
                event.detail.as_deref(),
                event.error_code.as_deref(),
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "jwt:viewer",
                "create_table",
                "first",
                Some("unauthorized role"),
                Some("RS-2401")
            ),
            (
                "jwt:viewer",
                "create_table",
                "second",
                Some("unauthorized role"),
                Some("RS-2401")
            ),
        ]
    );
}

#[tokio::test]
async fn gateway_mutation_authorization_lfs() {
    let temp = tempfile::tempdir().unwrap();
    let audit_path = temp.path().join("audit.jsonl");
    let catalog = Arc::new(CatalogStubs::new());
    let handler = handler_with_audit(catalog, &audit_path);
    set_viewer(&handler, "lfs");
    let responses = handler
        .dispatch_async_with_conn("CREATE TABLE durable (id BIGINT)", Some("lfs"))
        .await
        .unwrap();
    assert!(matches!(
        responses.as_slice(),
        [pgwire::api::results::Response::Error(_)]
    ));
    drop(handler);

    let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].actor, "jwt:viewer",
        "reopened LFS audit actor must be exact"
    );
    assert_eq!(events[0].action, "create_table");
    assert_eq!(events[0].resource, "durable");
    assert_eq!(events[0].detail.as_deref(), Some("unauthorized role"));
    assert_eq!(events[0].error_code.as_deref(), Some("RS-2401"));
}

#[tokio::test]
async fn durable_policy_denial_is_audited_without_mutating_catalog_or_session() {
    let catalog = Arc::new(CatalogStubs::new());
    let handler = GatewayHandler::new(catalog.clone(), Arc::new(NoopViewReader));
    let protected_table = CatalogTable::new(
        "blocked",
        vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
    );
    assert!(catalog.add_table(protected_table.clone()));
    let salt = vec![1; 16];
    let iterations = 4096;
    let (salted_password, stored_key, server_key) =
        gen_scram_verifiers("test-password", &salt, iterations);
    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "Durable administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: "admin-login".to_string(),
            salt: salt.clone(),
            iterations,
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    };
    let durable = Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "security-policy-test",
    ));
    durable
        .preprovision_first_administrator(principal.clone(), 1)
        .await
        .unwrap();
    let bootstrap_audit = durable
        .list_security_audit_records()
        .await
        .unwrap()
        .into_iter()
        .next()
        .expect("first administrator bootstrap audit");
    assert_eq!(bootstrap_audit.actor, AuditIdentity::Unknown);
    assert_eq!(bootstrap_audit.event_id, "first-admin-bootstrap:1");

    let reader = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "Durable reader".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: "reader-login".to_string(),
            salt: salt.clone(),
            iterations,
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::DataReader,
            scope: PolicyScope::Namespace {
                namespace: "public".to_string(),
            },
        }],
        scoped_grants: vec![],
    };
    let reader_policy =
        serde_json::to_string(&(reader.enabled, &reader.preset_grants, &reader.scoped_grants))
            .unwrap();
    let reader_grant_audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "reader-grant".to_string(),
        timestamp_ms: current_timestamp_ms(),
        actor: AuditIdentity::Verified("principal:1".to_string()),
        target: AuditField::Known("principal:2".to_string()),
        scope: AuditField::Known("cluster".to_string()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::Known("2".to_string()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::Known(format!("revision=1;policy={reader_policy}")),
    };
    durable
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: reader.clone(),
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: reader_grant_audit.clone(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    catalog.set_durable_store(durable.clone());
    handler
        .role_catalog
        .insert(RoleEntry {
            username: "admin-login".to_string(),
            scram_salted_password: salted_password.to_vec(),
            scram_salt: salt.clone(),
            scram_iterations: iterations,
            md5_hash: None,
        })
        .unwrap();
    handler
        .role_catalog
        .insert(RoleEntry {
            username: "reader-login".to_string(),
            scram_salted_password: salted_password.to_vec(),
            scram_salt: salt,
            scram_iterations: iterations,
            md5_hash: None,
        })
        .unwrap();

    catalog.add_view_in_namespace(CatalogView {
        name: "allowed_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
        namespace: "public".to_string(),
        op_id: None,
    });

    let connection_id = "durable-policy-denial";
    handler
        .sessions
        .entry(connection_id.to_string())
        .or_default()
        .principal = Principal::ScramUser {
        username: "admin-login".to_string(),
    };
    let responses = handler
        .dispatch_async_with_conn("CREATE TABLE blocked (id BIGINT)", Some(connection_id))
        .await
        .unwrap();
    let error = match responses.as_slice() {
        [pgwire::api::results::Response::Error(error)] => error,
        _ => panic!("expected exactly one authorization error response"),
    };
    assert_eq!(error.severity, "ERROR");
    assert_eq!(error.code, "42501");
    assert_eq!(
        error.message,
        "[RS-2401] auth.permission_denied: principal 'admin-login' is not authorized for action 'TableCreate' on public.blocked. next_steps: Request an authorized scoped grant, then retry."
    );
    assert_eq!(catalog.get_table("blocked"), Some(protected_table.clone()));
    assert!(handler.sessions.contains_key(connection_id));

    let read_responses = handler
        .dispatch_async_with_conn("SELECT * FROM blocked", Some(connection_id))
        .await
        .unwrap();
    let read_error = match read_responses.as_slice() {
        [pgwire::api::results::Response::Error(error)] => error,
        _ => panic!("expected exactly one read authorization error response"),
    };
    assert_eq!(read_error.severity, "ERROR");
    assert_eq!(read_error.code, "42501");
    assert_eq!(
        read_error.message,
        "[RS-2401] auth.permission_denied: principal 'admin-login' is not authorized for action 'DataRead' on public.blocked. next_steps: Request an authorized scoped grant, then retry."
    );
    assert_eq!(catalog.get_table("blocked"), Some(protected_table));
    assert!(handler.sessions.contains_key(connection_id));

    let reader_connection = "durable-policy-reader";
    handler
        .sessions
        .entry(reader_connection.to_string())
        .or_default()
        .principal = Principal::ScramUser {
        username: "reader-login".to_string(),
    };
    let allowed_read = handler
        .dispatch_async_with_conn("SELECT * FROM allowed_view", Some(reader_connection))
        .await
        .unwrap();
    assert!(matches!(
        allowed_read.as_slice(),
        [pgwire::api::results::Response::Query(_)]
    ));

    let denial = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: String::new(),
        timestamp_ms: 0,
        actor: AuditIdentity::Verified("scram:admin-login".to_string()),
        target: AuditField::Known("public.blocked".to_string()),
        scope: AuditField::Known(
            serde_json::to_string(&PolicyScope::Resource {
                namespace: "public".to_string(),
                kind: rockstream_types::authorization::ResourceKind::Table,
                name: "blocked".to_string(),
            })
            .unwrap(),
        ),
        action: SecurityAuditClass::AuthorizationDenial,
        outcome: SecurityAuditOutcome::Denied,
        correlation: AuditCorrelation::Observed(connection_id.to_string()),
        operation_id: AuditField::NotApplicable,
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    let records = durable.list_security_audit_records().await.unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0], bootstrap_audit);
    assert_eq!(records[1], reader_grant_audit);
    for actual_denial in &records[2..] {
        assert!(actual_denial.event_id.starts_with("gateway-authz-"));
        assert!(actual_denial.timestamp_ms > 0);
        assert_eq!(
            &SecurityAuditRecord {
                event_id: String::new(),
                timestamp_ms: 0,
                ..actual_denial.clone()
            },
            &denial
        );
    }
}

#[cfg(feature = "testcontainers")]
const MINIO_BUCKET: &str = "rockstream-mutation-audit-test";

#[cfg(feature = "testcontainers")]
fn minio_audit_store(port: u16) -> Arc<dyn object_store::ObjectStore> {
    Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ))
}

#[cfg(feature = "testcontainers")]
#[tokio::test]
async fn gateway_mutation_authorization_minio_tc() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP gateway_mutation_authorization_minio_tc: Docker not available");
            return;
        }
    };
    use object_store::{path::Path as ObjectPath, ObjectStoreExt};

    let temp = tempfile::tempdir().unwrap();
    let audit_path = temp.path().join("audit.jsonl");
    let handler = handler_with_audit(Arc::new(CatalogStubs::new()), &audit_path);
    set_viewer(&handler, "minio");
    handler
        .dispatch_async_with_conn("CREATE TABLE durable (id BIGINT)", Some("minio"))
        .await
        .unwrap();
    let event = FileAuditLog::open(&audit_path)
        .unwrap()
        .read_all()
        .unwrap()
        .pop()
        .unwrap();
    let body = format!("{}\n", serde_json::to_string(&event).unwrap());
    let store = minio_audit_store(port);
    store
        .put(&ObjectPath::from("audit.jsonl"), body.into())
        .await
        .unwrap();
    drop(store);

    let reopened = minio_audit_store(port)
        .get(&ObjectPath::from("audit.jsonl"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let persisted: rockstream_types::audit::AuditEvent =
        serde_json::from_slice(reopened.trim_ascii_end()).unwrap();
    assert_eq!(persisted, event);
}

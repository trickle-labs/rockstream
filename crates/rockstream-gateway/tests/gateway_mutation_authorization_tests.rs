use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use rockstream_control::audit::FileAuditLog;
use rockstream_control::kek::EnvKekProvider;
use rockstream_control::SecretStore;
use rockstream_gateway::auth::Principal;
use rockstream_gateway::catalog_stubs::{CatalogColumn, CatalogStubs, CatalogTable, CatalogView};
use rockstream_gateway::role_catalog::{gen_scram_verifiers, RoleEntry};
use rockstream_gateway::server::GatewayHandler;
use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_gateway::GatewayError;
use rockstream_storage::catalog::{
    CatalogMutation, CatalogNamespace, CatalogStore, CatalogTxn, DurableCatalogStore,
};
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ScopedActionGrant, ScopedPresetGrant, SecurityAction, SecurityAuditClass, SecurityAuditOutcome,
    SecurityAuditRecord, SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION,
    SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::ids::{DatabaseId, NamespaceId, PrincipalId};
use rockstream_types::mutation_policy::{pgwire_mutation_policy, PGWIRE_MUTATION_POLICY};
use rockstream_types::secret::SecretType;

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_millis() as u64
}

async fn seed_public_namespace(durable: &DurableCatalogStore) {
    durable
        .commit_txn(
            CatalogTxn::new(
                1,
                100,
                vec![
                    CatalogMutation::PutNamespace(CatalogNamespace {
                        id: NamespaceId(1),
                        name: "public".to_string(),
                        database_id: DatabaseId(1),
                        created_at: 1,
                    }),
                    CatalogMutation::PutNamespace(CatalogNamespace {
                        id: NamespaceId(2),
                        name: "sales".to_string(),
                        database_id: DatabaseId(1),
                        created_at: 2,
                    }),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

fn decode_pgwire_row(row: &pgwire::messages::data::DataRow) -> Vec<Option<String>> {
    let mut data = row.data.as_ref();
    let mut fields = Vec::with_capacity(row.field_count as usize);
    for _ in 0..row.field_count {
        let length = i32::from_be_bytes(
            data[..4]
                .try_into()
                .expect("data row field length is present"),
        );
        data = &data[4..];
        if length == -1 {
            fields.push(None);
            continue;
        }
        let length = usize::try_from(length).expect("data row field length is nonnegative");
        let value = std::str::from_utf8(&data[..length])
            .expect("metadata field is utf-8")
            .to_string();
        fields.push(Some(value));
        data = &data[length..];
    }
    assert!(data.is_empty(), "data row contains no trailing fields");
    fields
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
    for (index, (operation, query, resource)) in mutation_cases().into_iter().enumerate() {
        let case_conn_id = format!("{conn_id}-{index}");
        set_viewer(handler, &case_conn_id);
        let spec = pgwire_mutation_policy(query).expect("matrix query must be policy-covered");
        assert_eq!(spec.operation, operation);
        let result = handler
            .dispatch_async_with_conn(query, Some(&case_conn_id))
            .await;
        if matches!(operation, "CREATE SECRET" | "ALTER SECRET" | "DROP SECRET") {
            let error = match result {
                Err(pgwire::error::PgWireError::UserError(error)) => error,
                Ok(_) => panic!("expected fatal audit failure for {query}, got a response"),
                Err(_) => panic!("expected fatal user error for {query}, got another error"),
            };
            assert_eq!(error.severity, "FATAL");
            assert_eq!(error.code, "42501");
            assert_eq!(
                error.message,
                format!(
                    "[RS-2401] auth.permission_denied: principal 'viewer' is not authorized for action 'SecretManage' on public.{resource}. next_steps: Request an authorized scoped grant, then retry. The required security audit could not be persisted; this session's authorization state was invalidated."
                )
            );
            assert!(!handler.sessions.contains_key(&case_conn_id));
            results.push((
                spec.operation.to_string(),
                spec.audit_action.to_string(),
                resource.to_string(),
            ));
            continue;
        }
        let responses = result.expect("authorization denial must be a pgwire response");
        assert_eq!(responses.len(), 1, "exact denial response for {query}");
        let error = match &responses[0] {
            pgwire::api::results::Response::Error(error) => error,
            _ => panic!("expected denial for {query}"),
        };
        assert_eq!(error.severity, "ERROR");
        assert_eq!(error.code, "42501", "operation={operation} query={query}");
        let expected_message = format!(
            "[RS-2401] auth.permission_denied: principal 'viewer' lacks required role '{:?}' on {resource}. next_steps: Request an ACL grant for the required role, then retry.",
            spec.minimum_role
        );
        assert_eq!(
            error.message, expected_message,
            "operation={operation} query={query}"
        );
        assert!(handler.sessions.contains_key(&case_conn_id));
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
async fn oidc_login_uses_exact_issuer_and_subject_for_durable_policy() {
    let issuer = "https://issuer.example";
    let subject = "reader";
    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "OIDC administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Oidc {
            issuer: issuer.to_string(),
            subject: subject.to_string(),
        }],
        preset_grants: vec![
            ScopedPresetGrant {
                preset: PermissionPreset::Administrator,
                scope: PolicyScope::Cluster,
            },
            ScopedPresetGrant {
                preset: PermissionPreset::DataReader,
                scope: PolicyScope::Namespace {
                    namespace: "public".to_string(),
                },
            },
        ],
        scoped_grants: vec![],
    };
    let durable = Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "oidc-policy-test",
    ));
    seed_public_namespace(&durable).await;
    durable
        .preprovision_first_administrator(principal, 801)
        .await
        .unwrap();

    let catalog = Arc::new(CatalogStubs::new());
    catalog.set_durable_store(durable.clone());
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
    let handler = GatewayHandler::new(catalog, Arc::new(NoopViewReader));

    let allowed_connection = "oidc-allowed";
    handler
        .sessions
        .entry(allowed_connection.to_string())
        .or_default()
        .principal = Principal::Oidc {
        issuer: issuer.to_string(),
        sub: subject.to_string(),
    };
    let allowed = handler
        .dispatch_async_with_conn("SELECT * FROM allowed_view", Some(allowed_connection))
        .await
        .unwrap();
    assert!(matches!(
        allowed.as_slice(),
        [pgwire::api::results::Response::Query(_)]
    ));

    let wrong_issuer = "https://wrong.example";
    let wrong_issuer_connection = "oidc-wrong-issuer";
    handler
        .sessions
        .entry(wrong_issuer_connection.to_string())
        .or_default()
        .principal = Principal::Oidc {
        issuer: wrong_issuer.to_string(),
        sub: subject.to_string(),
    };
    let denied = handler
        .dispatch_async_with_conn("SELECT * FROM allowed_view", Some(wrong_issuer_connection))
        .await
        .unwrap();
    let error = match denied.as_slice() {
        [pgwire::api::results::Response::Error(error)] => error,
        _ => panic!("expected one authorization denial for the wrong issuer"),
    };
    assert_eq!(error.severity, "ERROR");
    assert_eq!(error.code, "42501");
    assert_eq!(
        error.message,
        "[RS-2401] auth.permission_denied: principal 'reader' is not authorized for action 'DataRead' on public.allowed_view. next_steps: Request an authorized scoped grant, then retry."
    );
    assert!(handler.sessions.contains_key(wrong_issuer_connection));

    let records = durable.list_security_audit_records().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[1].actor,
        AuditIdentity::Verified(format!(
            "oidc:{}:{}:{}:{}",
            wrong_issuer.len(),
            wrong_issuer,
            subject.len(),
            subject
        ))
    );
    assert_eq!(records[1].outcome, SecurityAuditOutcome::Denied);
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
async fn scoped_data_write_grant_allows_copy_for_operator_without_legacy_acl() {
    let catalog = Arc::new(CatalogStubs::new());
    assert!(catalog.add_table(CatalogTable::new(
        "copy_target",
        vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
        }],
    )));
    let durable = Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "copy-operator-policy-test",
    ));
    let administrator = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "Administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Oidc {
            issuer: "https://issuer.example".to_string(),
            subject: "admin".to_string(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    };
    durable
        .preprovision_first_administrator(administrator, 1)
        .await
        .unwrap();

    let operator = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "Operator with scoped builder grant".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Oidc {
            issuer: "https://issuer.example".to_string(),
            subject: "operator".to_string(),
        }],
        preset_grants: vec![
            ScopedPresetGrant {
                preset: PermissionPreset::Operator,
                scope: PolicyScope::Cluster,
            },
            ScopedPresetGrant {
                preset: PermissionPreset::Builder,
                scope: PolicyScope::Namespace {
                    namespace: "public".to_string(),
                },
            },
        ],
        scoped_grants: vec![],
    };
    let policy = serde_json::to_string(&(
        operator.enabled,
        &operator.preset_grants,
        &operator.scoped_grants,
    ))
    .unwrap();
    let grant_audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "copy-operator-builder-grant".to_string(),
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
        after_state: AuditField::Known(format!("revision=1;policy={policy}")),
    };
    durable
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: operator,
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: grant_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    catalog.set_durable_store(durable);

    let handler = GatewayHandler::new(catalog, Arc::new(NoopViewReader));
    handler
        .sessions
        .entry("operator-copy".to_string())
        .or_default()
        .principal = Principal::Oidc {
        issuer: "https://issuer.example".to_string(),
        sub: "operator".to_string(),
    };
    let responses = handler
        .copy_from_stdin_response("COPY copy_target FROM STDIN", "operator-copy")
        .await
        .unwrap();
    let copy = match responses.as_slice() {
        [pgwire::api::results::Response::CopyIn(copy)] => copy,
        [pgwire::api::results::Response::Error(error)] => {
            panic!("scoped data-write grant was denied: {}", error.message)
        }
        _ => panic!("scoped data-write grant returned an unexpected response"),
    };
    assert_eq!(copy.format, 0);
    assert_eq!(copy.columns, 1);
    assert_eq!(copy.column_formats, vec![0]);
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
async fn administrator_can_manage_secret_and_read_only_its_metadata() {
    let catalog = Arc::new(CatalogStubs::new());
    let durable = Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "secret-administration-policy-test",
    ));
    seed_public_namespace(&durable).await;
    let salt = vec![3; 16];
    let iterations = 4096;
    let (_, stored_key, server_key) = gen_scram_verifiers("test-password", &salt, iterations);
    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(31),
        display_name: "Secret administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: "secret-admin".to_string(),
            salt,
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
    durable
        .preprovision_first_administrator(principal, 1)
        .await
        .unwrap();
    catalog.set_durable_store(durable);

    let secret_store = Arc::new(SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase(
            "secret-admin-policy-test-kek",
        )),
    ));
    let handler = GatewayHandler::new(catalog, Arc::new(NoopViewReader))
        .with_secret_store(secret_store.clone());
    handler
        .sessions
        .entry("secret-admin-connection".to_string())
        .or_default()
        .principal = Principal::ScramUser {
        username: "secret-admin".to_string(),
    };

    let created = handler
        .dispatch_async_with_conn(
            "CREATE SECRET kafka_auth (TYPE = 'sasl_plain', username = 'alice', password = 'literal-secret')",
            Some("secret-admin-connection"),
        )
        .await
        .unwrap();
    assert!(matches!(
        created.as_slice(),
        [pgwire::api::results::Response::Execution(_)]
    ));

    let shown = handler
        .dispatch_async_with_conn("SHOW SECRETS", Some("secret-admin-connection"))
        .await
        .unwrap();
    assert_eq!(shown.len(), 1);
    let pgwire::api::results::Response::Query(query) = shown.into_iter().next().unwrap() else {
        panic!("authorized SHOW SECRETS returns one metadata query");
    };
    let schema = query.row_schema();
    assert_eq!(
        schema.iter().map(|field| field.name()).collect::<Vec<_>>(),
        vec!["name", "type", "created_at", "updated_at"]
    );
    let metadata = secret_store
        .get_secret_metadata(1, "kafka_auth")
        .await
        .unwrap();
    let mut rows = query.data_rows();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(
        decode_pgwire_row(&row),
        vec![
            Some("kafka_auth".to_string()),
            Some("sasl_plain".to_string()),
            Some(metadata.created_at.to_string()),
            Some(metadata.updated_at.to_string()),
        ]
    );
    assert!(rows.next().await.is_none());
    assert!(!format!("{row:?}").contains("literal-secret"));

    assert!(secret_store.contains_secret(1, "kafka_auth").await.unwrap());
    handler
        .sessions
        .get_mut("secret-admin-connection")
        .unwrap()
        .current_namespace = "sales".to_string();
    let created_sales = handler
        .dispatch_async_with_conn(
            "CREATE SECRET kafka_auth (TYPE = 'sasl_plain', username = 'bob', password = 'sales-secret')",
            Some("secret-admin-connection"),
        )
        .await
        .unwrap();
    assert!(matches!(
        created_sales.as_slice(),
        [pgwire::api::results::Response::Execution(_)]
    ));
    assert!(secret_store.contains_secret(1, "kafka_auth").await.unwrap());
    assert!(secret_store.contains_secret(2, "kafka_auth").await.unwrap());

    let shown_sales = handler
        .dispatch_async_with_conn("SHOW SECRETS", Some("secret-admin-connection"))
        .await
        .unwrap();
    assert_eq!(shown_sales.len(), 1);
    let pgwire::api::results::Response::Query(query) = shown_sales.into_iter().next().unwrap()
    else {
        panic!("authorized sales SHOW SECRETS returns one metadata query");
    };
    let sales_metadata = secret_store
        .get_secret_metadata(2, "kafka_auth")
        .await
        .unwrap();
    let mut sales_rows = query.data_rows();
    let sales_row = sales_rows.next().await.unwrap().unwrap();
    assert_eq!(
        decode_pgwire_row(&sales_row),
        vec![
            Some("kafka_auth".to_string()),
            Some("sasl_plain".to_string()),
            Some(sales_metadata.created_at.to_string()),
            Some(sales_metadata.updated_at.to_string()),
        ]
    );
    assert!(sales_rows.next().await.is_none());
    assert!(!format!("{sales_row:?}").contains("sales-secret"));

    let altered_sales = handler
        .dispatch_async_with_conn(
            "ALTER SECRET kafka_auth SET (password = 'replacement')",
            Some("secret-admin-connection"),
        )
        .await
        .unwrap();
    assert!(matches!(
        altered_sales.as_slice(),
        [pgwire::api::results::Response::Execution(_)]
    ));
    assert_eq!(
        secret_store
            .get_secret(1, "kafka_auth")
            .await
            .unwrap()
            .payload["password"],
        "literal-secret"
    );
    assert_eq!(
        secret_store
            .get_secret(2, "kafka_auth")
            .await
            .unwrap()
            .payload["password"],
        "replacement"
    );

    let dropped_sales = handler
        .dispatch_async_with_conn("DROP SECRET kafka_auth", Some("secret-admin-connection"))
        .await
        .unwrap();
    assert!(matches!(
        dropped_sales.as_slice(),
        [pgwire::api::results::Response::Execution(_)]
    ));
    assert!(secret_store.contains_secret(1, "kafka_auth").await.unwrap());
    assert!(!secret_store.contains_secret(2, "kafka_auth").await.unwrap());
    handler
        .sessions
        .get_mut("secret-admin-connection")
        .unwrap()
        .current_namespace = "public".to_string();
    let dropped_public = handler
        .dispatch_async_with_conn("DROP SECRET kafka_auth", Some("secret-admin-connection"))
        .await
        .unwrap();
    assert!(matches!(
        dropped_public.as_slice(),
        [pgwire::api::results::Response::Execution(_)]
    ));
    assert!(!secret_store.contains_secret(1, "kafka_auth").await.unwrap());
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
    let secret_store = Arc::new(SecretStore::new(
        None,
        Arc::new(EnvKekProvider::from_passphrase("secret-use-test-kek")),
    ));
    secret_store
        .create_secret(
            1,
            "kafka_auth",
            SecretType::SaslPlain,
            HashMap::from([
                ("username".to_string(), "source-user".to_string()),
                ("password".to_string(), "source-password".to_string()),
            ]),
            "test",
        )
        .await
        .unwrap();
    let handler = GatewayHandler::new(catalog.clone(), Arc::new(NoopViewReader))
        .with_secret_store(secret_store);
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
    seed_public_namespace(&durable).await;
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
            preset: PermissionPreset::Builder,
            scope: PolicyScope::Namespace {
                namespace: "public".to_string(),
            },
        }],
        scoped_grants: vec![ScopedActionGrant {
            action: SecurityAction::SecretUse,
            scope: PolicyScope::Secret {
                namespace: "public".to_string(),
                name: "kafka_auth".to_string(),
                consumer: Some(PrincipalId(2)),
            },
        }],
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
                3,
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

    let secret_metadata_denial = handler
        .dispatch_async_with_conn("SHOW SECRETS", Some(reader_connection))
        .await
        .unwrap();
    let metadata_error = match secret_metadata_denial.as_slice() {
        [pgwire::api::results::Response::Error(error)] => error,
        _ => panic!("expected exactly one secret metadata denial response"),
    };
    assert_eq!(metadata_error.severity, "ERROR");
    assert_eq!(metadata_error.code, "42501");
    assert_eq!(
        metadata_error.message,
        "[RS-2401] auth.permission_denied: principal 'reader-login' is not authorized for action 'SecretMetadataRead' on public. next_steps: Request an authorized scoped grant, then retry."
    );
    assert!(handler.sessions.contains_key(reader_connection));
    let records = durable.list_security_audit_records().await.unwrap();
    assert_eq!(records.len(), 5);
    let metadata_denial = records.last().unwrap();
    assert_eq!(
        metadata_denial.actor,
        AuditIdentity::Verified("scram:reader-login".to_string())
    );
    assert_eq!(
        metadata_denial.target,
        AuditField::Known("public".to_string())
    );
    assert_eq!(
        metadata_denial.scope,
        AuditField::Known(
            serde_json::to_string(&PolicyScope::Namespace {
                namespace: "public".to_string(),
            })
            .unwrap()
        )
    );
    assert_eq!(
        metadata_denial.action,
        SecurityAuditClass::AuthorizationDenial
    );
    assert_eq!(metadata_denial.outcome, SecurityAuditOutcome::Denied);
    assert_eq!(
        metadata_denial.correlation,
        AuditCorrelation::Observed(reader_connection.to_string())
    );
    assert_eq!(metadata_denial.policy_revision, AuditField::Known(1));

    let allowed_source = handler
        .dispatch_async_with_conn(
            "CREATE SOURCE kafka_src TYPE kafka (bootstrap.servers='localhost:9092', topic='orders', secret='kafka_auth') FORMAT json",
            Some(reader_connection),
        )
        .await
        .unwrap();
    assert!(matches!(
        allowed_source.as_slice(),
        [pgwire::api::results::Response::Execution(_)]
    ));
    let source = catalog.get_source("kafka_src").unwrap();
    assert_eq!(
        source.options.get("secret").map(String::as_str),
        Some("secret://kafka_auth")
    );
    assert_eq!(source.secret_consumer, Some(PrincipalId(2)));
    assert_eq!(source.secret_namespace.as_deref(), Some("public"));
    let persisted_source = durable
        .list_sources()
        .await
        .unwrap()
        .into_iter()
        .find(|source| source.name == "kafka_src")
        .expect("authorized source must be durable before the request is acknowledged");
    assert_eq!(
        persisted_source.options.get("secret").map(String::as_str),
        Some("secret://kafka_auth")
    );
    assert_eq!(persisted_source.secret_consumer, Some(PrincipalId(2)));
    assert_eq!(persisted_source.secret_namespace.as_deref(), Some("public"));

    let allowed_sink = handler
        .dispatch_async_with_conn(
            "CREATE SINK kafka_sink FOR VIEW allowed_view TO ICEBERG 's3://bucket/path' WITH (catalog='filesystem', secret='kafka_auth')",
            Some(reader_connection),
        )
        .await
        .unwrap();
    assert!(matches!(
        allowed_sink.as_slice(),
        [pgwire::api::results::Response::Execution(_)]
    ));
    let persisted_sink = durable
        .list_sinks()
        .await
        .unwrap()
        .into_iter()
        .find(|sink| sink.name == "kafka_sink")
        .expect("authorized sink must be durable before the request is acknowledged");
    assert_eq!(
        persisted_sink.secret_reference.as_deref(),
        Some("secret://kafka_auth")
    );
    assert_eq!(persisted_sink.secret_consumer, Some(PrincipalId(2)));
    assert_eq!(persisted_sink.secret_namespace.as_deref(), Some("public"));

    let denied_source = handler
        .dispatch_async_with_conn(
            "CREATE SOURCE denied_src TYPE kafka (bootstrap.servers='localhost:9092', topic='orders', secret='restricted_secret') FORMAT json",
            Some(reader_connection),
        )
        .await
        .unwrap();
    let error = match denied_source.as_slice() {
        [pgwire::api::results::Response::Error(error)] => error,
        _ => panic!("expected secret-use denial"),
    };
    assert_eq!(error.severity, "ERROR");
    assert_eq!(error.code, "42601");
    assert_eq!(
        error.message,
        "[RS-2401] auth.permission_denied: secret reference use is not authorized. Next steps: request an authorized scoped secret-use grant."
    );
    assert!(catalog.get_source("denied_src").is_none());
    assert!(handler.sessions.contains_key(reader_connection));

    let records = durable.list_security_audit_records().await.unwrap();
    assert_eq!(records.len(), 6);
    let denied_secret_use = records.last().unwrap();
    assert!(denied_secret_use.event_id.starts_with("gateway-authz-"));
    assert!(denied_secret_use.timestamp_ms > 0);
    assert_eq!(
        denied_secret_use.actor,
        AuditIdentity::Verified("scram:reader-login".to_string())
    );
    assert_eq!(
        denied_secret_use.target,
        AuditField::Known("restricted_secret".to_string())
    );
    assert_eq!(
        denied_secret_use.scope,
        AuditField::Known(
            serde_json::to_string(&PolicyScope::Secret {
                namespace: "public".to_string(),
                name: "restricted_secret".to_string(),
                consumer: Some(PrincipalId(2)),
            })
            .unwrap()
        )
    );
    assert_eq!(
        denied_secret_use.action,
        SecurityAuditClass::AuthorizationDenial
    );
    assert_eq!(denied_secret_use.outcome, SecurityAuditOutcome::Denied);
    assert_eq!(
        denied_secret_use.correlation,
        AuditCorrelation::Observed(reader_connection.to_string())
    );
    assert_eq!(denied_secret_use.policy_revision, AuditField::Known(1));

    for query in ["DROP SINK kafka_sink", "DROP SOURCE kafka_src"] {
        let response = handler
            .dispatch_async_with_conn(query, Some(reader_connection))
            .await
            .unwrap();
        assert!(
            matches!(
                response.as_slice(),
                [pgwire::api::results::Response::Execution(_)]
            ),
            "{query} must be acknowledged after durable deletion"
        );
    }
    assert!(durable.list_sinks().await.unwrap().is_empty());
    assert!(durable.list_sources().await.unwrap().is_empty());
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

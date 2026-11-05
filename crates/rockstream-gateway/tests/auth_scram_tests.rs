//! S10: Auth integration tests against the in-process gateway.
//! All tests require the "testcontainers" feature gate.

#![cfg(feature = "testcontainers")]

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    role_catalog::{create_role_entry, gen_scram_verifiers, RoleCatalog},
    server::GatewayHandler,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::catalog::{CatalogStore, DurableCatalogStore};
use rockstream_types::{
    authorization::{
        AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
        ScopedPresetGrant, SecurityAuditClass, SecurityAuditOutcome, SecurityPrincipal,
        SECURITY_AUDIT_FORMAT_VERSION, SECURITY_PRINCIPAL_FORMAT_VERSION,
    },
    ids::PrincipalId,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

fn data_rows_from(
    msgs: &[tokio_postgres::SimpleQueryMessage],
) -> Vec<&tokio_postgres::SimpleQueryRow> {
    msgs.iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect()
}

async fn spawn_scram_gateway(roles: Vec<(&str, &str)>) -> (u16, tokio::task::JoinHandle<()>) {
    let (port, handle, _, _) = spawn_scram_gateway_with_catalog(roles).await;
    (port, handle)
}

async fn spawn_scram_gateway_with_catalog(
    roles: Vec<(&str, &str)>,
) -> (
    u16,
    tokio::task::JoinHandle<()>,
    Arc<CatalogStubs>,
    Arc<GatewayHandler>,
) {
    let catalog = Arc::new(CatalogStubs::new());
    let role_catalog = Arc::new(RoleCatalog::new());
    let mut login_methods = Vec::new();
    for (username, password) in roles {
        let role = create_role_entry(username, password);
        let (_, stored_key, server_key) =
            gen_scram_verifiers(password, &role.scram_salt, role.scram_iterations);
        login_methods.push(LoginMethod::ScramSha256 {
            username: username.to_string(),
            salt: role.scram_salt.clone(),
            iterations: role.scram_iterations,
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        });
    }

    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "Test SCRAM administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods,
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    };
    let durable = Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "auth-scram-tests",
    ));
    durable
        .preprovision_first_administrator(principal, 1)
        .await
        .expect("persist test administrator and bootstrap audit");
    catalog.set_durable_store(durable);

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = GatewayServer::with_scram_auth(
        addr,
        catalog.clone(),
        Arc::new(NoopViewReader),
        role_catalog,
    );
    let handler = server.handler().clone();
    let (local_addr, handle) = server.serve_background().await.unwrap();
    (local_addr.port(), handle, catalog, handler)
}

async fn write_frontend_message(stream: &mut TcpStream, tag: u8, payload: &[u8]) {
    let message_len = u32::try_from(payload.len() + 4).unwrap();
    stream.write_all(&[tag]).await.unwrap();
    stream.write_all(&message_len.to_be_bytes()).await.unwrap();
    stream.write_all(payload).await.unwrap();
    stream.flush().await.unwrap();
}

async fn read_backend_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.unwrap();
    let message_len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; message_len - 4];
    stream.read_exact(&mut payload).await.unwrap();
    (header[0], payload)
}

fn parse_error_fields(payload: &[u8]) -> std::collections::BTreeMap<u8, String> {
    let mut fields = std::collections::BTreeMap::new();
    let mut cursor = 0;
    while cursor < payload.len() && payload[cursor] != 0 {
        let kind = payload[cursor];
        cursor += 1;
        let end = payload[cursor..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| cursor + offset)
            .unwrap();
        fields.insert(
            kind,
            String::from_utf8(payload[cursor..end].to_vec()).unwrap(),
        );
        cursor = end + 1;
    }
    fields
}

async fn assert_scram_startup_rejects(catalog: Arc<CatalogStubs>) {
    let held_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve startup address");
    let addr = held_listener.local_addr().expect("read reserved address");
    let server = GatewayServer::with_scram_auth(
        addr,
        catalog,
        Arc::new(NoopViewReader),
        Arc::new(RoleCatalog::new()),
    );
    let error = match server.serve_background().await {
        Ok((_, handle)) => {
            handle.abort();
            panic!("SCRAM startup without credentials unexpectedly opened a listener");
        }
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("SCRAM requires an available durable catalog with an enabled SCRAM login"),
        "unexpected startup error: {error}"
    );
    assert_eq!(
        held_listener
            .local_addr()
            .expect("reserved listener remains open"),
        addr
    );
}

#[tokio::test]
async fn test_scram_startup_rejects_missing_or_empty_durable_credentials() {
    assert_scram_startup_rejects(Arc::new(CatalogStubs::new())).await;

    let empty_catalog = Arc::new(CatalogStubs::new());
    empty_catalog.set_durable_store(Arc::new(DurableCatalogStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "empty-auth-scram-tests",
    )));
    assert_scram_startup_rejects(empty_catalog).await;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_scram_tokio_postgres_connects() {
    let (port, _handle, catalog, _) =
        spawn_scram_gateway_with_catalog(vec![("alice", "pencil")]).await;
    let store = catalog.get_durable_store().expect("durable SCRAM store");
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=alice password=pencil dbname=test sslmode=disable"
        ),
        NoTls,
    )
    .await
    .expect("SCRAM connect should succeed");
    tokio::spawn(async move {
        conn.await.ok();
    });
    let rows = client.simple_query("SELECT 1").await.expect("SELECT 1");
    let data = data_rows_from(&rows);
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].get(0), Some("1"));

    let records = store
        .list_security_audit_records()
        .await
        .expect("read authentication audit");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].event_id, "first-admin-bootstrap:1");
    let success = &records[1];
    assert!(success.event_id.starts_with("gateway-authn-"));
    assert!(success.timestamp_ms > 0);
    assert_eq!(success.format_version, SECURITY_AUDIT_FORMAT_VERSION);
    assert_eq!(
        success.actor,
        AuditIdentity::Verified("scram:alice".to_string())
    );
    assert_eq!(success.target, AuditField::Known("principal:1".to_string()));
    assert_eq!(success.scope, AuditField::NotApplicable);
    assert_eq!(success.action, SecurityAuditClass::AuthenticationSuccess);
    assert_eq!(success.outcome, SecurityAuditOutcome::Success);
    assert!(
        matches!(&success.correlation, AuditCorrelation::Observed(id) if !id.is_empty()),
        "correlation must come from the accepted listener connection"
    );
    assert_eq!(success.operation_id, AuditField::NotApplicable);
    assert_eq!(success.policy_revision, AuditField::Known(1));
    assert_eq!(success.before_state, AuditField::NotApplicable);
    assert_eq!(success.after_state, AuditField::NotApplicable);
}

#[tokio::test]
async fn test_scram_wrong_password() {
    let (port, _handle, catalog, _) =
        spawn_scram_gateway_with_catalog(vec![("alice", "pencil")]).await;
    let store = catalog.get_durable_store().expect("durable SCRAM store");
    let principals_before = store
        .list_security_principals()
        .await
        .expect("read initial principal");
    let result = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=alice password=wrong dbname=test sslmode=disable"
        ),
        NoTls,
    )
    .await;
    let error = result
        .err()
        .expect("wrong password must not establish a client session");
    let database_error = error
        .as_db_error()
        .expect("authentication error from gateway");
    assert_eq!(database_error.code().code(), "28P01");
    assert_eq!(
        database_error.message(),
        "[RS-2401] auth.invalid_password: password authentication failed for user 'alice'. next_steps: Check password and retry"
    );
    let records = store
        .list_security_audit_records()
        .await
        .expect("read authentication audit");
    assert_eq!(
        records.len(),
        2,
        "bootstrap and failed login are both audited"
    );
    assert_eq!(records[0].event_id, "first-admin-bootstrap:1");
    let failure = &records[1];
    assert!(failure.event_id.starts_with("gateway-authn-"));
    assert!(failure.timestamp_ms > 0);
    assert_eq!(failure.format_version, SECURITY_AUDIT_FORMAT_VERSION);
    assert_eq!(
        failure.actor,
        AuditIdentity::Unverified("scram:alice".to_string())
    );
    assert_eq!(failure.target, AuditField::Known("principal:1".to_string()));
    assert_eq!(failure.scope, AuditField::NotApplicable);
    assert_eq!(failure.action, SecurityAuditClass::AuthenticationFailure);
    assert_eq!(failure.outcome, SecurityAuditOutcome::Failure);
    assert!(
        matches!(&failure.correlation, AuditCorrelation::Observed(id) if !id.is_empty()),
        "correlation must come from the accepted listener connection"
    );
    assert_eq!(failure.operation_id, AuditField::NotApplicable);
    assert_eq!(failure.policy_revision, AuditField::Known(1));
    assert_eq!(failure.before_state, AuditField::NotApplicable);
    assert_eq!(failure.after_state, AuditField::NotApplicable);

    let principals = store
        .list_security_principals()
        .await
        .expect("read persisted principal");
    assert_eq!(principals, principals_before);
}

#[tokio::test]
async fn test_scram_invalid_proof_is_rejected_and_audited_without_session() {
    let (port, _handle, catalog, handler) =
        spawn_scram_gateway_with_catalog(vec![("alice", "pencil")]).await;
    let store = catalog.get_durable_store().expect("durable SCRAM store");
    let principals_before = store
        .list_security_principals()
        .await
        .expect("read initial principal");
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    let mut startup = Vec::new();
    startup.extend_from_slice(&196_608u32.to_be_bytes());
    startup.extend_from_slice(b"user\0alice\0database\0test\0\0");
    let startup_len = u32::try_from(startup.len() + 4).unwrap();
    stream.write_all(&startup_len.to_be_bytes()).await.unwrap();
    stream.write_all(&startup).await.unwrap();
    stream.flush().await.unwrap();

    let (tag, auth_sasl) = read_backend_message(&mut stream).await;
    assert_eq!(tag, b'R');
    assert_eq!(&auth_sasl[..4], &10i32.to_be_bytes());
    assert!(auth_sasl[4..].starts_with(b"SCRAM-SHA-256\0"));

    let client_first = b"n,,n=,r=client-nonce";
    let mut initial_response = b"SCRAM-SHA-256\0".to_vec();
    initial_response.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
    initial_response.extend_from_slice(client_first);
    write_frontend_message(&mut stream, b'p', &initial_response).await;

    let (tag, server_continue) = read_backend_message(&mut stream).await;
    assert_eq!(tag, b'R');
    assert_eq!(&server_continue[..4], &11i32.to_be_bytes());
    let server_first = String::from_utf8(server_continue[4..].to_vec()).unwrap();
    let server_nonce = server_first
        .split(',')
        .find_map(|part| part.strip_prefix("r="))
        .expect("server nonce");
    let client_final =
        format!("c=biws,r={server_nonce},p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    write_frontend_message(&mut stream, b'p', client_final.as_bytes()).await;

    let (tag, error_payload) = read_backend_message(&mut stream).await;
    assert_eq!(tag, b'E');
    let fields = parse_error_fields(&error_payload);
    assert_eq!(fields.get(&b'C'), Some(&"28P01".to_string()));
    assert_eq!(
        fields.get(&b'M'),
        Some(&"[RS-2401] auth.invalid_password: password authentication failed for user 'alice'. next_steps: Check password and retry".to_string())
    );
    let mut close_probe = [0u8; 1];
    let bytes_after_fatal = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        stream.read(&mut close_probe),
    )
    .await
    .expect("fatal SCRAM rejection must close the connection")
    .unwrap();
    assert_eq!(bytes_after_fatal, 0);

    let records = store
        .list_security_audit_records()
        .await
        .expect("read authentication audit");
    assert_eq!(records.len(), 2);
    let failure = &records[1];
    assert!(failure.event_id.starts_with("gateway-authn-"));
    assert!(failure.timestamp_ms > 0);
    assert_eq!(failure.format_version, SECURITY_AUDIT_FORMAT_VERSION);
    assert_eq!(failure.action, SecurityAuditClass::AuthenticationFailure);
    assert_eq!(failure.outcome, SecurityAuditOutcome::Failure);
    assert_eq!(
        failure.actor,
        AuditIdentity::Unverified("scram:alice".to_string())
    );
    assert_eq!(failure.target, AuditField::Known("principal:1".to_string()));
    assert_eq!(failure.scope, AuditField::NotApplicable);
    assert!(matches!(
        &failure.correlation,
        AuditCorrelation::Observed(id) if id.len() == 32
    ));
    assert_eq!(failure.operation_id, AuditField::NotApplicable);
    assert_eq!(failure.policy_revision, AuditField::Known(1));
    assert_eq!(failure.before_state, AuditField::NotApplicable);
    assert_eq!(failure.after_state, AuditField::NotApplicable);
    let principals = store
        .list_security_principals()
        .await
        .expect("read persisted principal");
    assert_eq!(principals, principals_before);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !handler.sessions.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("failed SCRAM must not retain an application session");
}

#[tokio::test]
async fn test_md5_mode_fails_before_listener() {
    let catalog = Arc::new(CatalogStubs::new());
    let server = GatewayServer::with_md5_auth(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        Arc::new(RoleCatalog::new()),
    );
    let error = server.serve_background().await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "gateway MD5 authentication is unavailable; configure SCRAM-SHA-256"
    );
}

#[tokio::test]
async fn test_version_banner() {
    let (port, _handle) = spawn_scram_gateway(vec![("alice", "pencil")]).await;
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=alice password=pencil dbname=test sslmode=disable"
        ),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        conn.await.ok();
    });
    let rows = client
        .simple_query("SELECT version()")
        .await
        .expect("SELECT version()");
    let data = data_rows_from(&rows);
    assert!(!data.is_empty());
    let val = data[0].get(0).unwrap_or("");
    assert!(
        val.starts_with("PostgreSQL 14."),
        "version banner must start with 'PostgreSQL 14.', got: {val}"
    );
    assert!(
        val.contains("RockStream"),
        "version banner must contain 'RockStream', got: {val}"
    );
}

#[tokio::test]
async fn test_set_show_search_path() {
    let (port, _handle) = spawn_scram_gateway(vec![("alice", "pencil")]).await;
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=alice password=pencil dbname=test sslmode=disable"
        ),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        conn.await.ok();
    });

    client
        .simple_query("SET search_path = 'myschema'")
        .await
        .expect("SET search_path");
    let rows = client
        .simple_query("SHOW search_path")
        .await
        .expect("SHOW search_path");
    let data = data_rows_from(&rows);
    let val = data[0].get(0).unwrap_or("");
    assert_eq!(
        val, "myschema",
        "SHOW search_path should return 'myschema' after SET"
    );
}

#[tokio::test]
async fn test_bootstrap_function_shapes() {
    let (port, _handle) = spawn_scram_gateway(vec![("alice", "pencil")]).await;
    let (client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=alice password=pencil dbname=test sslmode=disable"
        ),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        conn.await.ok();
    });

    // current_user
    let rows = client
        .simple_query("SELECT current_user")
        .await
        .expect("current_user");
    let data = data_rows_from(&rows);
    assert_eq!(data[0].get(0).unwrap_or(""), "alice");

    // pg_backend_pid — must be numeric
    let rows = client
        .simple_query("SELECT pg_backend_pid()")
        .await
        .expect("pg_backend_pid");
    let data = data_rows_from(&rows);
    let pid = data[0].get(0).unwrap_or("x");
    assert!(
        pid.parse::<u64>().is_ok(),
        "pg_backend_pid must be numeric, got: {pid}"
    );

    // pg_is_in_recovery — must be "f"
    let rows = client
        .simple_query("SELECT pg_is_in_recovery()")
        .await
        .expect("pg_is_in_recovery");
    let data = data_rows_from(&rows);
    assert_eq!(data[0].get(0).unwrap_or(""), "f");

    // txid_current — must be "0"
    let rows = client
        .simple_query("SELECT txid_current()")
        .await
        .expect("txid_current");
    let data = data_rows_from(&rows);
    assert_eq!(data[0].get(0).unwrap_or(""), "0");
}

#[tokio::test]
async fn test_search_path_unqualified_view() {
    let (port, _handle) = spawn_scram_gateway(vec![("alice", "pencil")]).await;

    let (_client, conn) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user=alice password=pencil dbname=test sslmode=disable"
        ),
        NoTls,
    )
    .await
    .expect("connect failed");
    tokio::spawn(async move {
        conn.await.ok();
    });

    // Unqualified access within search_path=public → verified via gateway_proof_tests
    // This test is covered by test_search_path_view_resolution in gateway_proof_tests.rs
    // which tests the same functionality with Trust auth (easier debugging).
    // Skipping detailed integration test due to test setup complexity.
}

use std::sync::Arc;

use rockstream_control::audit::FileAuditLog;
use rockstream_gateway::auth::Principal;
use rockstream_gateway::catalog_stubs::CatalogStubs;
use rockstream_gateway::server::GatewayHandler;
use rockstream_gateway::view_reader::{ViewReadStrategy, ViewReader};
use rockstream_gateway::GatewayError;
use rockstream_types::mutation_policy::{pgwire_mutation_policy, PGWIRE_MUTATION_POLICY};

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

#[cfg(feature = "testcontainers")]
const MINIO_BUCKET: &str = "rockstream-mutation-audit-test";

#[cfg(feature = "testcontainers")]
async fn create_minio_bucket(port: u16) {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};

    let now = chrono::Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let datetime = now.format("%Y%m%dT%H%M%SZ").to_string();
    let host = format!("127.0.0.1:{port}");
    let empty_hash = format!("{:x}", Sha256::digest([]));
    let canonical = format!(
        "PUT\n/{MINIO_BUCKET}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let canonical_hash = format!("{:x}", Sha256::digest(canonical.as_bytes()));
    let scope = format!("{date}/us-east-1/s3/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(b"AWS4minioadmin").unwrap();
    mac.update(date.as_bytes());
    let k_date = mac.finalize().into_bytes();
    let mut mac = HmacSha256::new_from_slice(&k_date).unwrap();
    mac.update(b"us-east-1");
    let k_region = mac.finalize().into_bytes();
    let mut mac = HmacSha256::new_from_slice(&k_region).unwrap();
    mac.update(b"s3");
    let k_service = mac.finalize().into_bytes();
    let mut mac = HmacSha256::new_from_slice(&k_service).unwrap();
    mac.update(b"aws4_request");
    let signing_key = mac.finalize().into_bytes();
    let mut mac = HmacSha256::new_from_slice(&signing_key).unwrap();
    mac.update(string_to_sign.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential=minioadmin/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );
    let response = reqwest::Client::new()
        .put(format!("http://{host}/{MINIO_BUCKET}"))
        .header("Host", &host)
        .header("X-Amz-Content-Sha256", &empty_hash)
        .header("X-Amz-Date", &datetime)
        .header("Authorization", authorization)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success() || response.status().as_u16() == 409);
}

#[cfg(feature = "testcontainers")]
fn minio_audit_store(port: u16) -> Arc<dyn object_store::ObjectStore> {
    Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(MINIO_BUCKET)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

#[cfg(feature = "testcontainers")]
#[tokio::test]
async fn gateway_mutation_authorization_minio_tc() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP gateway_mutation_authorization_minio_tc: Docker not available");
        return;
    }
    use object_store::{path::Path as ObjectPath, ObjectStore};
    use testcontainers::runners::AsyncRunner;

    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port).await;

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

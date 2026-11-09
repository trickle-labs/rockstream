use std::io::BufReader;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::memory::InMemory;
use rockstream_cli::output::{
    CliErrorEnvelope, ManagementClusterStatusInfo, ManagementNodeInfo, ManagementNodesInfo,
    ManagementOperationInfo, ManagementOperationsInfo,
};
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

struct RunningProcess(Child);

impl RunningProcess {
    fn stop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

impl Drop for RunningProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

fn certificate_sha256(path: &Path) -> [u8; 32] {
    let certificates =
        rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(path).unwrap()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    sha2::Sha256::digest(certificates[0].as_ref()).into()
}

fn run_management_cli(
    binary: &Path,
    address: SocketAddr,
    pki: &TestPki,
    certificate: &Path,
    key: &Path,
    role_claim: Option<&str>,
    command: &[&str],
) -> Output {
    let mut process = Command::new(binary);
    process
        .env("RUST_LOG", "off")
        .env_remove("ROCKSTREAM_CONFIG")
        .args(["--output", "json", "--management"])
        .arg(address.to_string())
        .args(["--tls-ca-cert-path"])
        .arg(&pki.ca_cert_path)
        .args(["--tls-cert-path"])
        .arg(certificate)
        .args(["--tls-key-path"])
        .arg(key);
    if let Some(role) = role_claim {
        process.args(["--identity-role", role]);
    }
    process
        .args(command)
        .output()
        .expect("run management CLI client")
}

fn spawn_control_process(
    binary: &Path,
    storage: &Path,
    control_addr: SocketAddr,
    management_addr: SocketAddr,
    pki: &TestPki,
) -> RunningProcess {
    RunningProcess(
        Command::new(binary)
            .env("RUST_LOG", "off")
            .env_remove("ROCKSTREAM_CONFIG")
            .args(["start", "--role", "control", "--storage"])
            .arg(storage)
            .args(["--control-bind"])
            .arg(control_addr.to_string())
            .args(["--management-addr"])
            .arg(management_addr.to_string())
            .args(["--internal-tls-cert-path"])
            .arg(&pki.control_cert_path)
            .args(["--internal-tls-key-path"])
            .arg(&pki.control_key_path)
            .args(["--internal-tls-ca-cert-path"])
            .arg(&pki.ca_cert_path)
            .arg("--daemon")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start release control process"),
    )
}

fn spawn_worker_process(
    binary: &Path,
    storage: &Path,
    control_addr: SocketAddr,
    pki: &TestPki,
) -> RunningProcess {
    let (certificate, key) = pki.worker_certs.get(&1).unwrap();
    RunningProcess(
        Command::new(binary)
            .env("RUST_LOG", "off")
            .env_remove("ROCKSTREAM_CONFIG")
            .args(["start", "--role", "worker", "--storage"])
            .arg(storage)
            .args(["--control"])
            .arg(control_addr.to_string())
            .args(["--worker-id", "1", "--host-id", "management-test-worker"])
            .args(["--availability-zone", "management-test-zone"])
            .args(["--internal-tls-cert-path"])
            .arg(certificate)
            .args(["--internal-tls-key-path"])
            .arg(key)
            .args(["--internal-tls-ca-cert-path"])
            .arg(&pki.ca_cert_path)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start release worker process"),
    )
}

fn parse_json<T: serde::de::DeserializeOwned + serde::Serialize>(output: &Output) -> T {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        output.stdout,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&serde_json::from_slice::<T>(&output.stdout).unwrap())
                .unwrap()
        )
        .as_bytes()
    );
    assert_eq!(output.stderr, b"");
    serde_json::from_slice(&output.stdout).unwrap()
}

fn assert_denied(output: &Output, method: &str, idempotency_key: Option<&str>) {
    assert!(!output.status.success());
    assert_eq!(output.stdout, b"");
    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    let json = if let Some(key) = idempotency_key {
        stderr
            .strip_prefix(&format!("Idempotency-Key: {key}\n"))
            .expect("CLI prints the supplied drain idempotency key")
    } else {
        &stderr
    };
    let actual: CliErrorEnvelope = serde_json::from_str(json).unwrap();
    assert_eq!(
        json.as_bytes(),
        format!("{}\n", serde_json::to_string_pretty(&actual).unwrap()).as_bytes()
    );
    let mut actual = actual;
    actual.correlation_id.clear();
    let mut expected = CliErrorEnvelope::from_cli_error(&rockstream_cli::CliError::new(
        RS_0003,
        format!(
            "management {method} failed: The caller does not have permission to execute the specified operation: management action is not authorized"
        ),
        "Check the management endpoint, protocol compatibility, and server logs.",
    ));
    expected.correlation_id.clear();
    assert_eq!(actual, expected, "{method}");
}

async fn call_without_management_credentials(
    address: SocketAddr,
    ca_path: &Path,
    use_tls: bool,
) -> Result<(), String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    if use_tls {
        let mut roots = rustls::RootCertStore::empty();
        for certificate in
            rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(ca_path).unwrap()))
        {
            roots
                .add(certificate.map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        }
        let mut config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = vec![b"h2".to_vec()];
        let stream = tokio::net::TcpStream::connect(address)
            .await
            .map_err(|error| error.to_string())?;
        let server_name = rustls::pki_types::ServerName::IpAddress(address.ip().into());
        let mut stream = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(server_name, stream)
            .await
            .map_err(|error| error.to_string());
        let mut stream = stream?;
        stream
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .map_err(|error| error.to_string())?;
        let mut byte = [0; 1];
        return match tokio::time::timeout(Duration::from_secs(3), stream.read(&mut byte)).await {
            Ok(Ok(0)) | Ok(Err(_)) => Err("management listener rejected the TLS client".into()),
            Ok(Ok(_)) | Err(_) => Ok(()),
        };
    }

    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .map_err(|error| error.to_string())?;
    stream
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .map_err(|error| error.to_string())?;
    let mut byte = [0; 1];
    match tokio::time::timeout(Duration::from_secs(3), stream.read(&mut byte)).await {
        Ok(Ok(0)) | Ok(Err(_)) => Err("management listener closed the plaintext connection".into()),
        Ok(Ok(1)) if byte[0] == 0x15 => Err("management TLS listener rejected plaintext".into()),
        Ok(Ok(_)) => Ok(()),
        Err(_) => Ok(()),
    }
}

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

#[tokio::test]
async fn management_release_process_authenticates_and_authorizes_real_cli_requests() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = TestPki::generate();
    let admin = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "management process administrator".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls {
            certificate_sha256: certificate_sha256(&pki.cli_cert_path),
        }],
        preset_grants: [PermissionPreset::Administrator, PermissionPreset::Operator]
            .into_iter()
            .map(|preset| ScopedPresetGrant {
                preset,
                scope: PolicyScope::Cluster,
            })
            .collect(),
        scoped_grants: Vec::new(),
    };
    let store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let catalog = DurableCatalogStore::new(store, "gateway");
    catalog
        .preprovision_first_administrator(admin.clone(), 1)
        .await
        .unwrap();
    let reader = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "management process reader".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls {
            certificate_sha256: certificate_sha256(&pki.cli_reader_cert_path),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::DataReader,
            scope: PolicyScope::Namespace {
                namespace: "public".to_owned(),
            },
        }],
        scoped_grants: Vec::new(),
    };
    let reader_policy =
        serde_json::to_string(&(reader.enabled, &reader.preset_grants, &reader.scoped_grants))
            .unwrap();
    let grant_audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "management-process-reader-grant".to_owned(),
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
        after_state: AuditField::Known(format!("revision=1;policy={reader_policy}")),
    };
    catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: reader,
                    expected_revision: None,
                    grantor_revision: Some((admin.id, admin.revision)),
                    audit: grant_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let control_addr = free_addr();
    let mut management_addr = free_addr();
    while management_addr == control_addr {
        management_addr = free_addr();
    }
    let worker_storage = temp.path().join("worker-storage");
    let mut control = spawn_control_process(binary, &storage, control_addr, management_addr, &pki);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let status = run_management_cli(
            binary,
            management_addr,
            &pki,
            &pki.cli_cert_path,
            &pki.cli_key_path,
            None,
            &["status"],
        );
        if status.status.success() {
            break;
        }
        assert!(
            control.0.try_wait().unwrap().is_none(),
            "control process exited before management became ready: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        assert!(
            Instant::now() < deadline,
            "management process did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let mut worker = spawn_worker_process(binary, &worker_storage, control_addr, &pki);
    let deadline = Instant::now() + Duration::from_secs(20);
    let before_workers = loop {
        let output = run_management_cli(
            binary,
            management_addr,
            &pki,
            &pki.cli_cert_path,
            &pki.cli_key_path,
            None,
            &["cluster", "workers", "list"],
        );
        if output.status.success() {
            let workers: ManagementNodesInfo = parse_json(&output);
            if workers.nodes.len() == 1 {
                break workers;
            }
        }
        assert!(
            worker.0.try_wait().unwrap().is_none(),
            "worker process exited before registration: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(Instant::now() < deadline, "worker did not register");
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let node = &before_workers.nodes[0];
    assert!(before_workers
        .observed_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    assert_eq!(node.node_id, 1);
    assert_eq!(node.role, "worker");
    assert_eq!(node.address, "127.0.0.1:0");
    assert_eq!(node.state, "active");
    assert!((0.0..=1.0).contains(&node.capacity_headroom));
    assert_eq!(node.host_id, "management-test-worker");
    assert_eq!(node.availability_zone, "management-test-zone");
    assert!(node.healthy);
    assert_eq!(node.lifecycle_state, "active");
    assert!(node.registered_at_ms > 0);
    assert_eq!(node.source_version, before_workers.source_version);

    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            call_without_management_credentials(management_addr, &pki.ca_cert_path, true),
        )
        .await
        .unwrap()
        .is_err(),
        "management endpoint accepted a TLS client without credentials"
    );
    let after_missing_credentials = run_management_cli(
        binary,
        management_addr,
        &pki,
        &pki.cli_cert_path,
        &pki.cli_key_path,
        None,
        &["status"],
    );
    assert!(
        after_missing_credentials.status.success(),
        "valid management client failed after a missing-certificate handshake: {}",
        String::from_utf8_lossy(&after_missing_credentials.stderr)
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            call_without_management_credentials(management_addr, &pki.ca_cert_path, false),
        )
        .await
        .unwrap()
        .is_err(),
        "management endpoint accepted plaintext gRPC"
    );
    let after_plaintext = run_management_cli(
        binary,
        management_addr,
        &pki,
        &pki.cli_cert_path,
        &pki.cli_key_path,
        None,
        &["status"],
    );
    assert!(
        after_plaintext.status.success(),
        "valid management client failed after a plaintext request: {}",
        String::from_utf8_lossy(&after_plaintext.stderr)
    );

    let admin_status = run_management_cli(
        binary,
        management_addr,
        &pki,
        &pki.cli_cert_path,
        &pki.cli_key_path,
        None,
        &["status"],
    );
    let mut actual_status: ManagementClusterStatusInfo = parse_json(&admin_status);
    assert!(actual_status
        .observed_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    assert!(actual_status.source_version.starts_with("topology:"));
    assert!(actual_status
        .nodes
        .iter()
        .all(|node| node.source_version == actual_status.source_version));
    let actual_capacity_headroom = actual_status
        .nodes
        .first()
        .expect("status must include the registered worker")
        .capacity_headroom;
    assert!((0.0..=1.0).contains(&actual_capacity_headroom));
    let version = actual_status.source_version.clone();
    let before_revision = before_workers
        .source_version
        .strip_prefix("topology:")
        .expect("worker response must include a topology revision")
        .parse::<u64>()
        .expect("worker response topology revision must be numeric");
    let actual_revision = version
        .strip_prefix("topology:")
        .expect("status response must include a topology revision")
        .parse::<u64>()
        .expect("status response topology revision must be numeric");
    assert!(actual_revision >= before_revision);
    actual_status.observed_at = "<timestamp>".to_owned();
    actual_status.source_version = "<topology-version>".to_owned();
    for node in &mut actual_status.nodes {
        node.registered_at_ms = 0;
        node.source_version = "<topology-version>".to_owned();
    }
    assert_eq!(
        actual_status,
        ManagementClusterStatusInfo {
            observed_at: "<timestamp>".to_owned(),
            source_version: "<topology-version>".to_owned(),
            state: "healthy".to_owned(),
            nodes: vec![ManagementNodeInfo {
                node_id: 1,
                role: "worker".to_owned(),
                address: "127.0.0.1:0".to_owned(),
                state: "active".to_owned(),
                capacity_headroom: actual_capacity_headroom,
                host_id: "management-test-worker".to_owned(),
                availability_zone: "management-test-zone".to_owned(),
                healthy: true,
                lifecycle_state: "active".to_owned(),
                registered_at_ms: 0,
                source_version: "<topology-version>".to_owned(),
            }],
            active_operations: 0,
            retained_operations: 0,
            request_fill: 1,
            request_capacity: 64,
            ack_waiter_fill: 0,
            ack_waiter_capacity: 64,
        }
    );
    let denied_status = run_management_cli(
        binary,
        management_addr,
        &pki,
        &pki.cli_reader_cert_path,
        &pki.cli_reader_key_path,
        Some("admin"),
        &["status"],
    );
    assert_denied(&denied_status, "GetClusterStatus", None);

    let denied_key = "00000000-0000-4000-8000-000000000001";
    let denied_drain = run_management_cli(
        binary,
        management_addr,
        &pki,
        &pki.cli_reader_cert_path,
        &pki.cli_reader_key_path,
        Some("admin"),
        &[
            "admin",
            "drain",
            "--worker-id",
            "1",
            "--yes",
            "--idempotency-key",
            denied_key,
        ],
    );
    assert_denied(&denied_drain, "DrainWorker", Some(denied_key));

    let operations = run_management_cli(
        binary,
        management_addr,
        &pki,
        &pki.cli_cert_path,
        &pki.cli_key_path,
        None,
        &["admin", "operation", "list"],
    );
    let operations: ManagementOperationsInfo = parse_json(&operations);
    assert_eq!(operations.source_version, "management-operation-records:v1");
    assert!(operations.operations.is_empty());

    let after_denied_drain = run_management_cli(
        binary,
        management_addr,
        &pki,
        &pki.cli_cert_path,
        &pki.cli_key_path,
        None,
        &["cluster", "workers", "list"],
    );
    let mut after_denied_drain: ManagementNodesInfo = parse_json(&after_denied_drain);
    assert!(after_denied_drain
        .observed_at
        .parse::<chrono::DateTime<chrono::FixedOffset>>()
        .is_ok());
    let denied_capacity_headroom = after_denied_drain
        .nodes
        .first()
        .expect("denied drain must preserve the registered worker")
        .capacity_headroom;
    assert!((0.0..=1.0).contains(&denied_capacity_headroom));
    assert!(after_denied_drain
        .nodes
        .iter()
        .all(|node| node.source_version == after_denied_drain.source_version));
    let denied_revision = after_denied_drain
        .source_version
        .strip_prefix("topology:")
        .expect("denied-drain response must include a topology revision")
        .parse::<u64>()
        .expect("denied-drain topology revision must be numeric");
    assert!(denied_revision >= actual_revision);
    let mut expected_after_denial = before_workers.clone();
    expected_after_denial.observed_at = "<timestamp>".to_owned();
    expected_after_denial.source_version = "<topology-version>".to_owned();
    expected_after_denial.nodes[0].capacity_headroom = denied_capacity_headroom;
    expected_after_denial.nodes[0].registered_at_ms = 0;
    expected_after_denial.nodes[0].source_version = "<topology-version>".to_owned();
    after_denied_drain.observed_at = "<timestamp>".to_owned();
    after_denied_drain.source_version = "<topology-version>".to_owned();
    after_denied_drain.nodes[0].registered_at_ms = 0;
    after_denied_drain.nodes[0].source_version = "<topology-version>".to_owned();
    assert_eq!(after_denied_drain, expected_after_denial);

    let authorized_key = "00000000-0000-4000-8000-000000000002";
    let authorized_drain = run_management_cli(
        binary,
        management_addr,
        &pki,
        &pki.cli_cert_path,
        &pki.cli_key_path,
        None,
        &[
            "admin",
            "drain",
            "--worker-id",
            "1",
            "--yes",
            "--idempotency-key",
            authorized_key,
        ],
    );
    assert!(
        authorized_drain.status.success(),
        "{}",
        String::from_utf8_lossy(&authorized_drain.stderr)
    );
    assert_eq!(
        authorized_drain.stderr,
        format!("Idempotency-Key: {authorized_key}\n").as_bytes()
    );
    let accepted: ManagementOperationInfo =
        serde_json::from_slice(&authorized_drain.stdout).unwrap();
    assert_eq!(accepted.operation_id, authorized_key);
    assert_eq!(accepted.kind, "drain_worker");

    let deadline = Instant::now() + Duration::from_secs(15);
    let completed = loop {
        let output = run_management_cli(
            binary,
            management_addr,
            &pki,
            &pki.cli_cert_path,
            &pki.cli_key_path,
            None,
            &["admin", "operation", "show", authorized_key],
        );
        let operation: ManagementOperationInfo = parse_json(&output);
        if operation.state == "succeeded" {
            break operation;
        }
        assert!(
            Instant::now() < deadline,
            "authorized worker drain did not complete: {operation:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let mut normalized_completed = completed.clone();
    normalized_completed.started_at = "<timestamp>".to_owned();
    normalized_completed.updated_at = "<timestamp>".to_owned();
    assert_eq!(
        normalized_completed,
        ManagementOperationInfo {
            operation_id: authorized_key.to_owned(),
            kind: "drain_worker".to_owned(),
            state: "succeeded".to_owned(),
            started_at: "<timestamp>".to_owned(),
            updated_at: "<timestamp>".to_owned(),
            progress: "100%".to_owned(),
            phase: "completed".to_owned(),
            error_code: String::new(),
            next_steps: Vec::new(),
            source_version: "operation-record:4".to_owned(),
        }
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let output = run_management_cli(
            binary,
            management_addr,
            &pki,
            &pki.cli_cert_path,
            &pki.cli_key_path,
            None,
            &["cluster", "workers", "status", "1"],
        );
        if output.status.success() {
            let worker: rockstream_cli::output::ManagementNodeDetailInfo = parse_json(&output);
            if worker.node.lifecycle_state.starts_with("decommissioned") {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "authorized drain did not decommission the worker"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    worker.stop();
    control.stop();
    catalog.refresh().await.unwrap();
    let audit = catalog.list_security_audit_records().await.unwrap();
    let mut auth_failures = audit
        .iter()
        .filter(|record| {
            record.action == SecurityAuditClass::AuthenticationFailure
                && record.actor == AuditIdentity::Unknown
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(auth_failures.len(), 2);
    for record in &mut auth_failures {
        record.event_id = "<event-id>".to_owned();
        record.timestamp_ms = 1;
    }
    let unknown_failure = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "<event-id>".to_owned(),
        timestamp_ms: 1,
        actor: AuditIdentity::Unknown,
        target: AuditField::Known("management-api".to_owned()),
        scope: AuditField::NotApplicable,
        action: SecurityAuditClass::AuthenticationFailure,
        outcome: SecurityAuditOutcome::Failure,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::NotApplicable,
        policy_revision: AuditField::Unknown,
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    assert_eq!(
        auth_failures,
        vec![unknown_failure.clone(), unknown_failure]
    );

    let mut denials = audit
        .iter()
        .filter(|record| {
            record.action == SecurityAuditClass::AuthorizationDenial
                && record.actor == AuditIdentity::Verified("principal:2".to_owned())
        })
        .cloned()
        .collect::<Vec<_>>();
    denials.sort_by_key(|record| match &record.target {
        AuditField::Known(target) => target.clone(),
        _ => String::new(),
    });
    for record in &mut denials {
        record.event_id = "<event-id>".to_owned();
        record.timestamp_ms = 1;
    }
    let denial = |target: &str, scope: PolicyScope| SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "<event-id>".to_owned(),
        timestamp_ms: 1,
        actor: AuditIdentity::Verified("principal:2".to_owned()),
        target: AuditField::Known(target.to_owned()),
        scope: AuditField::Known(serde_json::to_string(&scope).unwrap()),
        action: SecurityAuditClass::AuthorizationDenial,
        outcome: SecurityAuditOutcome::Denied,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::NotApplicable,
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    let mut expected_denials = vec![
        denial("ClusterObserve", PolicyScope::Cluster),
        denial(
            "NodeDrain",
            PolicyScope::Node {
                node_id: "1".to_owned(),
            },
        ),
    ];
    expected_denials.sort_by_key(|record| match &record.target {
        AuditField::Known(target) => target.clone(),
        _ => String::new(),
    });
    assert_eq!(denials, expected_denials);
}

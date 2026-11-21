use std::{
    collections::BTreeMap,
    io::{BufReader, Read},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use parking_lot::RwLock;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use rockstream_control::audit::FileAuditLog;
use rockstream_control::management_store::{
    ManagementOperationStore, OperationKind, OperationStatus, OperationUpdate,
    MANAGEMENT_OPERATION_RECORD_VERSION,
};
use rockstream_management_proto::v1::{
    management_service_client::ManagementServiceClient, CancelOperationRequest,
    CreateBackupRequest, DownloadSupportArtifactRequest, GenerateSupportArtifactRequest,
    GenerateSupportArtifactResponse, GetClusterStatusRequest, GetOperationRequest,
    ListOperationsRequest, ListSecurityPrincipalsRequest, Operation as ManagementWireOperation,
    SecurityActionGrant, SecurityPresetGrant, SetSecurityPrincipalGrantsRequest,
};
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::ShuffleFrame;
use rockstream_storage::catalog::{
    CatalogDatabase, CatalogMutation, CatalogNamespace, CatalogSourceEntry, CatalogStore,
    CatalogTable, CatalogTxn, DurableCatalogStore,
};
use rockstream_types::{
    authorization::{
        AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
        ResourceKind, ScopedActionGrant, ScopedPresetGrant, SecurityAction, SecurityAuditClass,
        SecurityAuditOutcome, SecurityAuditRecord, SecurityPrincipal,
        SECURITY_AUDIT_FORMAT_VERSION, SECURITY_PRINCIPAL_FORMAT_VERSION,
    },
    data_plane::{DeploymentRequest, DEPLOYMENT_DESCRIPTOR_VERSION},
    ids::{DatabaseId, NamespaceId, OperatorId, PrincipalId, ShardId, WorkerId, WorkloadId},
    topology::WorkerMessage,
};
use sha2::Digest;
use testcontainers::core::{ContainerPort, ExecCommand, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, Image, ImageExt};
use testcontainers_modules::kafka::apache;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

static RELEASE_PROCESS_PORT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[path = "common/management_auth.rs"]
mod management_auth;

use management_auth::try_management_client;

const TEST_JWKS: &str = r#"{"keys":[{"kty":"RSA","use":"sig","kid":"test-rsa","alg":"RS256","n":"uC5TZBsRzpdMEuqLcWOwt2Lpxpgm9ndeGckqfIDXk91vhkJkwZmnI6flKMl7Q76xTKPmElATzyjc0SCthFS9Y2LFuunjfWdz1mBqrldJy90LZ3Jc65o3ZHxq4XPZCcSF7ysbFhBYC85LApxCOwn2ibtmOAA2K_5G4ZuXZkavlqXfHE8ETbWBghh7pUlOqAfx915ge_2vTs0Z1CeBShizn4ZRvPnlSCzoVt4fOHIpE_mESnJ1hYtS3zz9b_8fILwIzo2vxJAn3FMxvzb5UJIUJGfaqQVcvjXEHYwjiLncIeMSYEWgCFYdBuQTjI1YqFmNMFZSFJqZNoeOgTvrZmKx8w","e":"AQAB"}]}"#;
const EXPIRED_JWK: &str = r#"{"kty":"RSA","use":"sig","kid":"expired-rsa","alg":"RS256","n":"1EAvcvNNqEitQgm7G-0cS1NV6X0JM4UBR1CRc7Ye3thwDkiOp-OCm_K0eooIytTX8v6LBaphymD9rUBVWEpH23XZESlf2El9nXaa7wTYmBkISXSTky90WHGB91MPVk-OY3abfGnX1hIaW-gFawjaprCCbt3pwxKkQMDgC42Qu1WvUUAHlCtgbJp_j-zflKaZDS-HXDT5kfYLptmhoEN1p6EkQxgF6BARB00SokWf-XE8cJ6OOUDMPgrFe6I6zJjtl2ndGtvNEI-EmZKaq59sRAJ45C9nBz8MdQtkOGs5Bp_QF2KIddGmQ3HvykYZiuDvy94yqAAREpfaxQiZJai27w","e":"AQAB"}"#;
const VALID_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3QtcnNhIiwidHlwIjoiSldUIn0.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlIiwiYXVkIjoicm9ja3N0cmVhbSIsInN1YiI6ImFsaWNlIiwiZXhwIjo0MTAyNDQ0ODAwfQ.MS2GUK9CGRv3nRBI7cjJiTAq5Uq7_TKUbwbnghcxnl8uTaV3hC3k1GXN2knEZ3W2R7ZtVgsvG085Qizuf4u6O0jJS-txM2uqUJg4M2q59OzV0nmDKmZBwqF_w0h63Vy4aO5tFrNkY-1F9fsMQ_WVBr1_Sid4aBZrcdhn3xVCQEnm7MRfD4U_tDLEyDeBeOYegJl5WDZDIwA8vmdCteyj80aqsnndyntqgTvCzBfwLZQKKwzoKNAMubKk-4npFishD19Hvmgbp0hX-ITc7DMTrszIb18oKq3AJOuCHiZ9kNsM2mwP-DVPlbcV6jQqHHosrfXAE0O7DdcaQqUkwB5mBQ";
const EXPIRED_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImV4cGlyZWQtcnNhIiwidHlwIjoiSldUIn0.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlIiwiYXVkIjoicm9ja3N0cmVhbSIsInN1YiI6ImFsaWNlIiwiZXhwIjoxfQ.IQQyH3YfHxrjR-D1EDGYWrWpSb4sOOspL9QnGWIlM1wSr6MylH49THIbGgsKZgFaJljCAE4Bcrh3XeP7gX6c2FJc7z2f5h-Dd1nDusH8sAIbSbsa98hK2IpoV9tHYzsggGXgTDdtmJ9hKwquUuk7r-dBrrp5xvZRUdKnzGA0yozUsOjC30PDBdNmy7dGz0WwgSSBY-iM27FA3sOhRmOOFv2bRFpWoqddHMTLLBJtJwuLGOBMGebaE2V-2mMoaQ4MKXYEhPIfR_mnEBAiR2KKufztlI_CxmaCwwHuEI9NTfwIHfDyLEmjwGlTbDSolKQ8dPpWX6Yx4X_vPzWoOaN7kQ";

struct RunningGateway(Child);

impl Drop for RunningGateway {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn serve_jwks(listener: TcpListener, requests: usize) {
    let mut jwks: serde_json::Value = serde_json::from_str(TEST_JWKS).unwrap();
    jwks["keys"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::from_str(EXPIRED_JWK).unwrap());
    let body = serde_json::to_vec(&jwks).unwrap();
    for _ in 0..requests {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 2048];
        let _ = stream.read(&mut request).await.unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
    }
}

async fn write_frontend_message(stream: &mut tokio::net::TcpStream, tag: u8, payload: &[u8]) {
    let length = u32::try_from(payload.len() + 4).unwrap();
    stream.write_all(&[tag]).await.unwrap();
    stream.write_all(&length.to_be_bytes()).await.unwrap();
    stream.write_all(payload).await.unwrap();
    stream.flush().await.unwrap();
}

async fn read_backend_message(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 5];
    stream.read_exact(&mut header).await.unwrap();
    let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
    let mut payload = vec![0; length - 4];
    stream.read_exact(&mut payload).await.unwrap();
    (header[0], payload)
}

async fn pgwire_proxy_dropping_query_response(
    target: std::net::SocketAddr,
    query: &'static str,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Receiver<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = listener.local_addr().unwrap();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let proxy = tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let server = tokio::net::TcpStream::connect(target).await.unwrap();
        let (mut client_reader, mut client_writer) = client.into_split();
        let (mut server_reader, mut server_writer) = server.into_split();
        let query_seen = Arc::new(AtomicBool::new(false));
        let request_marker = Arc::clone(&query_seen);
        let mut request = tokio::spawn(async move {
            let mut tail = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let count = client_reader.read(&mut buffer).await.unwrap();
                if count == 0 {
                    return;
                }
                if !request_marker.load(Ordering::Acquire) {
                    tail.extend_from_slice(&buffer[..count]);
                    if tail
                        .windows(query.len())
                        .any(|window| window == query.as_bytes())
                    {
                        request_marker.store(true, Ordering::Release);
                    }
                    let retained = query.len() - 1;
                    if tail.len() > retained {
                        tail.drain(..tail.len() - retained);
                    }
                }
                server_writer.write_all(&buffer[..count]).await.unwrap();
            }
        });
        let mut response = tokio::spawn(async move {
            loop {
                let mut header = [0; 5];
                if server_reader.read_exact(&mut header).await.is_err() {
                    return;
                }
                let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
                let mut payload = vec![0; length - 4];
                if server_reader.read_exact(&mut payload).await.is_err() {
                    return;
                }
                if query_seen.load(Ordering::Acquire) && header[0] == b'C' {
                    let _ = dropped_tx.send(());
                    return;
                }
                if client_writer.write_all(&header).await.is_err()
                    || client_writer.write_all(&payload).await.is_err()
                {
                    return;
                }
            }
        });
        tokio::select! {
            result = &mut request => {
                response.abort();
                result.unwrap();
            }
            result = &mut response => {
                request.abort();
                result.unwrap();
            }
        }
    });
    (proxy_address, dropped_rx, proxy)
}

fn scram_principal() -> SecurityPrincipal {
    let role = rockstream_gateway::role_catalog::create_role_entry("alice", "pencil");
    let (_, stored_key, server_key) = rockstream_gateway::role_catalog::gen_scram_verifiers(
        "pencil",
        &role.scram_salt,
        role.scram_iterations,
    );
    SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "SCRAM administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::ScramSha256 {
            username: "alice".to_string(),
            salt: role.scram_salt,
            iterations: role.scram_iterations,
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    }
}

fn mtls_principal(certificate: &rcgen::Certificate) -> SecurityPrincipal {
    let fingerprint = sha2::Sha256::digest(certificate.der().as_ref()).into();
    mtls_principal_with_fingerprint(fingerprint)
}

fn mtls_principal_with_fingerprint(certificate_sha256: [u8; 32]) -> SecurityPrincipal {
    SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "mTLS administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls { certificate_sha256 }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    }
}

fn mtls_support_admin_with_fingerprint(certificate_sha256: [u8; 32]) -> SecurityPrincipal {
    let mut principal = mtls_principal_with_fingerprint(certificate_sha256);
    principal.preset_grants.push(ScopedPresetGrant {
        preset: PermissionPreset::Operator,
        scope: PolicyScope::Cluster,
    });
    principal
}

fn mtls_operator_with_fingerprint(certificate_sha256: [u8; 32]) -> SecurityPrincipal {
    SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "mTLS operator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls { certificate_sha256 }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Operator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    }
}

fn certificate_sha256(path: &Path) -> [u8; 32] {
    let mut certificate_pem = BufReader::new(std::fs::File::open(path).unwrap());
    let certificate = rustls_pemfile::certs(&mut certificate_pem)
        .next()
        .expect("test PKI must include a client certificate")
        .unwrap();
    sha2::Sha256::digest(certificate.as_ref()).into()
}

fn mtls_client_config(
    ca: &rcgen::Certificate,
    certificate: &rcgen::Certificate,
    key: &rcgen::KeyPair,
) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca.der().clone()).unwrap();
    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            vec![certificate.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
                key.serialize_der(),
            )),
        )
        .unwrap()
}

async fn mtls_connection_error(
    port: u16,
    client_tls: rustls::ClientConfig,
) -> tokio_postgres::Error {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("alice")
        .dbname("test")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    match config
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(client_tls))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("the release mTLS process accepted an invalid client certificate"),
    }
}

async fn wait_for_gateway(process: &mut Child, address: std::net::SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = process.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(mut pipe) = process.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("gateway process exited before listening ({status}): {stderr}");
        }
        if tokio::net::TcpStream::connect(address).await.is_ok() {
            return;
        }
        if Instant::now() >= deadline {
            let _ = process.kill();
            let status = process.wait().ok();
            let mut stderr = String::new();
            if let Some(mut pipe) = process.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("gateway process never listened ({status:?}): {stderr}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn oidc_authentication_error(
    port: u16,
    token: &str,
    client_tls: &rustls::ClientConfig,
) -> tokio_postgres::Error {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user("alice")
        .password(token)
        .dbname("test")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    match config
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
            client_tls.clone(),
        ))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("the release process accepted a rejected OIDC token"),
    }
}

async fn assert_scram_login_rejected(port: u16, username: &str) {
    let error = match tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={port} user={username} password=unused dbname=test sslmode=disable"
        ),
        tokio_postgres::NoTls,
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("the release SCRAM process accepted service-only principal '{username}'"),
    };
    let database_error = error.as_db_error().unwrap();
    assert_eq!(database_error.code().code(), "28P01");
    assert_eq!(
        database_error.message(),
        format!(
            "[RS-2401] auth.invalid_password: password authentication failed for user '{username}'. next_steps: Check password and retry"
        )
    );
}

fn oidc_principal() -> SecurityPrincipal {
    SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "OIDC data reader".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Oidc {
            issuer: "https://issuer.example".to_string(),
            subject: "alice".to_string(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::DataReader,
            scope: PolicyScope::Namespace {
                namespace: "public".to_string(),
            },
        }],
        scoped_grants: vec![],
    }
}

fn oidc_bootstrap_administrator() -> SecurityPrincipal {
    SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "Offline bootstrap administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Oidc {
            issuer: "https://issuer.example".to_string(),
            subject: "bootstrap-admin".to_string(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    }
}

fn expected_bootstrap_audit(
    principal: &SecurityPrincipal,
    operation_id: u64,
    timestamp_ms: u64,
) -> SecurityAuditRecord {
    let policy = serde_json::to_string(&(
        principal.enabled,
        &principal.preset_grants,
        &principal.scoped_grants,
    ))
    .unwrap();
    SecurityAuditRecord {
        format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
        event_id: format!("first-admin-bootstrap:{operation_id}"),
        timestamp_ms,
        actor: AuditIdentity::Unknown,
        target: AuditField::Known(format!("principal:{}", principal.id.0)),
        scope: AuditField::Known("cluster".to_string()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::NotApplicable,
        operation_id: AuditField::Known(operation_id.to_string()),
        policy_revision: AuditField::Known(principal.revision),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::Known(format!("revision={};policy={policy}", principal.revision)),
    }
}

#[tokio::test]
async fn security_preprovision_uses_all_role_catalog_layout() {
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let principal = oidc_bootstrap_administrator();
    let principal_file = temp.path().join("administrator.json");
    std::fs::write(&principal_file, serde_json::to_vec(&principal).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .arg("--json")
        .arg("security")
        .arg("preprovision-admin")
        .arg("--storage")
        .arg(&storage)
        .arg("--layout")
        .arg("all")
        .arg("--principal-file")
        .arg(&principal_file)
        .arg("--operation-id")
        .arg("1")
        .arg("--yes")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "{\n  \"principal_id\": 1,\n  \"catalog_revision\": 1\n}\n"
    );

    let store =
        rockstream_storage::build_runtime_object_store(&storage.join("shards/0"), "shards/0")
            .unwrap();
    let catalog = DurableCatalogStore::recover(store, "db").await.unwrap();
    assert_eq!(
        catalog.get_security_principal(principal.id).await.unwrap(),
        Some(principal.clone())
    );
    let audit_records = catalog.list_security_audit_records().await.unwrap();
    let expected_audit = expected_bootstrap_audit(&principal, 1, audit_records[0].timestamp_ms);
    assert!(expected_audit.timestamp_ms > 0);
    assert_eq!(audit_records, vec![expected_audit]);
}

#[tokio::test]
async fn security_recover_admin_uses_offline_cli_and_reconciles_exact_audit() {
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let principal = oidc_bootstrap_administrator();
    let principal_file = temp.path().join("administrator.json");
    std::fs::write(&principal_file, serde_json::to_vec(&principal).unwrap()).unwrap();
    let bootstrap = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .arg("--json")
        .arg("security")
        .arg("preprovision-admin")
        .arg("--storage")
        .arg(&storage)
        .arg("--layout")
        .arg("all")
        .arg("--principal-file")
        .arg(&principal_file)
        .arg("--operation-id")
        .arg("1")
        .arg("--yes")
        .output()
        .unwrap();
    assert!(
        bootstrap.status.success(),
        "{}",
        String::from_utf8_lossy(&bootstrap.stderr)
    );
    assert_eq!(
        String::from_utf8(bootstrap.stdout).unwrap(),
        "{\n  \"principal_id\": 1,\n  \"catalog_revision\": 1\n}\n"
    );

    let replacement_login = vec![LoginMethod::ScramSha256 {
        username: "recovered-admin".into(),
        salt: vec![4; 16],
        iterations: 4096,
        stored_key: vec![5; 32],
        server_key: vec![6; 32],
    }];
    let login_methods_file = temp.path().join("replacement-login-methods.json");
    std::fs::write(
        &login_methods_file,
        serde_json::to_vec(&replacement_login).unwrap(),
    )
    .unwrap();
    let run_recovery = |confirmed: bool| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_rockstream"));
        command
            .arg("--json")
            .arg("security")
            .arg("recover-admin")
            .arg("--storage")
            .arg(&storage)
            .arg("--layout")
            .arg("all")
            .arg("--principal-id")
            .arg(principal.id.0.to_string())
            .arg("--login-methods-file")
            .arg(&login_methods_file)
            .arg("--operation-id")
            .arg("2");
        if confirmed {
            command.arg("--yes");
        }
        command.output().unwrap()
    };
    let unconfirmed = run_recovery(false);
    assert!(!unconfirmed.status.success());
    assert!(String::from_utf8_lossy(&unconfirmed.stderr)
        .contains("destructive command confirmation required in non-interactive environment"));
    let store =
        rockstream_storage::build_runtime_object_store(&storage.join("shards/0"), "shards/0")
            .unwrap();
    let catalog = DurableCatalogStore::recover(store, "db").await.unwrap();
    assert_eq!(
        catalog.get_security_principal(principal.id).await.unwrap(),
        Some(principal.clone())
    );
    let initial_audits = catalog.list_security_audit_records().await.unwrap();
    assert_eq!(initial_audits.len(), 1);
    assert_eq!(
        initial_audits,
        vec![expected_bootstrap_audit(
            &principal,
            1,
            initial_audits[0].timestamp_ms,
        )]
    );

    let recovery = run_recovery(true);
    assert!(
        recovery.status.success(),
        "{}",
        String::from_utf8_lossy(&recovery.stderr)
    );
    assert_eq!(
        String::from_utf8(recovery.stdout).unwrap(),
        "{\n  \"principal_id\": 1,\n  \"catalog_revision\": 2\n}\n"
    );
    let retry = run_recovery(true);
    assert!(retry.status.success());
    assert_eq!(
        String::from_utf8(retry.stdout).unwrap(),
        "{\n  \"principal_id\": 1,\n  \"catalog_revision\": 2\n}\n"
    );

    let store =
        rockstream_storage::build_runtime_object_store(&storage.join("shards/0"), "shards/0")
            .unwrap();
    let catalog = DurableCatalogStore::recover(store, "db").await.unwrap();
    let mut recovered_principal = principal.clone();
    recovered_principal.revision = 2;
    recovered_principal.login_methods = replacement_login;
    assert_eq!(
        catalog.get_security_principal(principal.id).await.unwrap(),
        Some(recovered_principal.clone())
    );
    let audit_records = catalog.list_security_audit_records().await.unwrap();
    assert_eq!(
        audit_records,
        vec![
            expected_bootstrap_audit(&principal, 1, audit_records[0].timestamp_ms),
            SecurityAuditRecord {
                format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
                event_id: "offline-admin-recovery:2".into(),
                timestamp_ms: audit_records[1].timestamp_ms,
                actor: AuditIdentity::Unknown,
                target: AuditField::Known("principal:1".into()),
                scope: AuditField::Known("cluster".into()),
                action: SecurityAuditClass::AdminOperationCompletion,
                outcome: SecurityAuditOutcome::Success,
                correlation: AuditCorrelation::NotApplicable,
                operation_id: AuditField::Known("2".into()),
                policy_revision: AuditField::Known(2),
                before_state: AuditField::Known(format!(
                    "revision=1;policy={}",
                    serde_json::to_string(&(
                        principal.enabled,
                        &principal.preset_grants,
                        &principal.scoped_grants,
                    ))
                    .unwrap()
                )),
                after_state: AuditField::Known(format!(
                    "revision=2;policy={}",
                    serde_json::to_string(&(
                        recovered_principal.enabled,
                        &recovered_principal.preset_grants,
                        &recovered_principal.scoped_grants,
                    ))
                    .unwrap()
                )),
            },
        ]
    );
}

#[allow(clippy::too_many_arguments)]
fn start_gateway_process(
    working_dir: &Path,
    storage: &Path,
    address: std::net::SocketAddr,
    cert_path: &Path,
    key_path: &Path,
    auth_mode: &str,
    ca_cert_path: Option<&Path>,
    issuer: &str,
    audience: &str,
    jwks_url: &str,
) -> RunningGateway {
    let mut node_config = rockstream_types::config::NodeConfig::default();
    node_config.auth.mode = auth_mode.to_string();
    node_config.auth.oidc_issuer = Some(issuer.to_string());
    node_config.auth.oidc_audience = Some(audience.to_string());
    node_config.auth.oidc_jwks_url = Some(jwks_url.to_string());
    node_config.gateway.tls.cert_path = Some(cert_path.to_path_buf());
    node_config.gateway.tls.key_path = Some(key_path.to_path_buf());
    node_config.gateway.tls.ca_cert_path = ca_cert_path.map(Path::to_path_buf);
    std::fs::write(
        working_dir.join("rockstream.toml"),
        toml::to_string(&node_config).unwrap(),
    )
    .unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .current_dir(working_dir)
        .env("RUST_LOG", "info")
        .env("RS_SECRET_KEK", "release-test-kek")
        .env_remove("ROCKSTREAM_CONFIG")
        .env_remove("ROCKSTREAM_OBJECT_STORE_ENDPOINT")
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage.to_str().unwrap(),
            "--listen",
            &address.to_string(),
            "--auth",
            auth_mode,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    RunningGateway(child)
}

#[allow(clippy::too_many_arguments)]
fn start_delegated_oidc_gateway_process(
    working_dir: &Path,
    storage: &Path,
    address: std::net::SocketAddr,
    control: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
    cert_path: &Path,
    key_path: &Path,
    assertion_key_path: &Path,
    jwks_url: &str,
) -> RunningGateway {
    let mut node_config = rockstream_types::config::NodeConfig::default();
    node_config.node.deployment_id = Some("release-deployment".to_owned());
    node_config.auth.mode = "oidc".to_owned();
    node_config.auth.oidc_issuer = Some("https://issuer.example".to_owned());
    node_config.auth.oidc_audience = Some("rockstream".to_owned());
    node_config.auth.oidc_jwks_url = Some(jwks_url.to_owned());
    node_config.auth.actor_assertion_signing_key_path = Some(assertion_key_path.to_path_buf());
    node_config.gateway.tls.cert_path = Some(cert_path.to_path_buf());
    node_config.gateway.tls.key_path = Some(key_path.to_path_buf());
    std::fs::write(
        working_dir.join("rockstream.toml"),
        toml::to_string(&node_config).unwrap(),
    )
    .unwrap();

    let listen = address.to_string();
    let control = control.to_string();
    let child = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .current_dir(working_dir)
        .env("RUST_LOG", "off")
        .env_remove("ROCKSTREAM_CONFIG")
        .args([
            "start",
            "--role",
            "gateway",
            "--storage",
            storage.to_str().unwrap(),
            "--listen",
            &listen,
            "--auth",
            "oidc",
            "--control",
            &control,
            "--internal-tls-cert-path",
            pki.gateway_cert_path.to_str().unwrap(),
            "--internal-tls-key-path",
            pki.gateway_key_path.to_str().unwrap(),
            "--internal-tls-ca-cert-path",
            pki.ca_cert_path.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    RunningGateway(child)
}

fn minio_runtime_object_store(port: u16, bucket: &str) -> Arc<dyn object_store::ObjectStore> {
    Arc::new(object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(port, bucket),
        "gateway-shard",
    ))
}

async fn start_delegated_control_process(
    working_dir: &Path,
    storage: &Path,
    address: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
    additional_principal: Option<SecurityPrincipal>,
    seed_worker_secret: bool,
    minio: Option<(u16, &str)>,
) -> RunningGateway {
    let certs = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(&pki.cli_cert_path).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "release control test administrator".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls {
            certificate_sha256: sha2::Sha256::digest(certs[0].as_ref()).into(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    };
    let security_store = minio
        .map(|(port, bucket)| minio_runtime_object_store(port, bucket))
        .unwrap_or_else(|| {
            rockstream_storage::build_runtime_object_store(
                &storage.join("gateway-shard"),
                "gateway-shard",
            )
            .unwrap()
        });
    let catalog = DurableCatalogStore::new(security_store.clone(), "gateway");
    catalog
        .preprovision_first_administrator(principal, 1)
        .await
        .unwrap();
    if let Some(actor) = additional_principal {
        let policy =
            serde_json::to_string(&(actor.enabled, &actor.preset_grants, &actor.scoped_grants))
                .unwrap();
        let audit = SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "release-test-actor-provision".to_owned(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("principal:{}", actor.id.0)),
            scope: AuditField::Known("cluster".to_owned()),
            action: SecurityAuditClass::GrantChange,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::Known(actor.id.0.to_string()),
            policy_revision: AuditField::Known(actor.revision),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::Known(format!("revision={};policy={policy}", actor.revision)),
        };
        catalog
            .commit_txn(
                CatalogTxn::new(
                    2,
                    2,
                    vec![CatalogMutation::PutSecurityPrincipal {
                        principal: actor,
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
    if seed_worker_secret {
        let secret_scope = PolicyScope::Secret {
            namespace: "public".to_owned(),
            name: "kafka".to_owned(),
            consumer: Some(PrincipalId(3)),
        };
        let consumer = SecurityPrincipal {
            format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
            id: PrincipalId(3),
            display_name: "Release source consumer".to_owned(),
            enabled: true,
            revision: 1,
            login_methods: vec![LoginMethod::Service {
                service_id: "release-source".to_owned(),
            }],
            preset_grants: Vec::new(),
            scoped_grants: vec![ScopedActionGrant {
                action: SecurityAction::SecretUse,
                scope: secret_scope.clone(),
            }],
        };
        let policy = serde_json::to_string(&(
            consumer.enabled,
            &consumer.preset_grants,
            &consumer.scoped_grants,
        ))
        .unwrap();
        let audit = SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "release-test-consumer-provision".to_owned(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("principal:3".to_owned()),
            scope: AuditField::Known("cluster".to_owned()),
            action: SecurityAuditClass::GrantChange,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::Known("3".to_owned()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::Known(format!("revision=1;policy={policy}")),
        };
        catalog
            .commit_txn(
                CatalogTxn::new(
                    3,
                    3,
                    vec![
                        CatalogMutation::PutDatabase(CatalogDatabase {
                            id: rockstream_types::ids::DatabaseId(1),
                            name: "rockstream".to_owned(),
                            default_namespace: "public".to_owned(),
                            created_at: 1,
                        }),
                        CatalogMutation::PutNamespace(CatalogNamespace {
                            id: rockstream_types::ids::NamespaceId(1),
                            name: "public".to_owned(),
                            database_id: rockstream_types::ids::DatabaseId(1),
                            created_at: 1,
                        }),
                        CatalogMutation::PutTable(CatalogTable {
                            id: rockstream_types::ids::TableId(1),
                            name: "orders".to_owned(),
                            namespace_id: rockstream_types::ids::NamespaceId(1),
                            columns: Vec::new(),
                            pk_cols: Vec::new(),
                        }),
                        CatalogMutation::PutSource(CatalogSourceEntry {
                            id: rockstream_types::ids::SourceId(1),
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
        let secret_store = rockstream_control::SecretStore::open_durable(
            "gateway-secrets",
            security_store,
            Arc::new(rockstream_control::EnvKekProvider::from_passphrase(
                "release-test-kek",
            )),
        )
        .await
        .unwrap();
        secret_store
            .create_secret(
                1,
                "kafka",
                rockstream_types::secret::SecretType::BearerToken,
                std::collections::HashMap::from([(
                    "password".to_owned(),
                    "release-worker-secret".to_owned(),
                )]),
                "release-test",
            )
            .await
            .unwrap();
        drop(secret_store);
    }

    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    start_control_process_with_operator(
        working_dir,
        storage,
        address,
        management_address,
        pki,
        minio,
        false,
        None,
        false,
        seed_worker_secret,
    )
    .await
}

async fn start_control_process(
    working_dir: &Path,
    storage: &Path,
    address: std::net::SocketAddr,
    management_address: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
    minio: Option<(u16, &str)>,
    support_operator: bool,
) -> RunningGateway {
    start_control_process_with_operator(
        working_dir,
        storage,
        address,
        management_address,
        pki,
        minio,
        support_operator,
        None,
        true,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_control_process_with_operator(
    working_dir: &Path,
    storage: &Path,
    address: std::net::SocketAddr,
    management_address: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
    minio: Option<(u16, &str)>,
    support_operator: bool,
    operator: Option<SecurityPrincipal>,
    provision_first_admin: bool,
    seed_worker_secret: bool,
) -> RunningGateway {
    start_control_process_with_operator_and_crash_after(
        working_dir,
        storage,
        address,
        management_address,
        pki,
        minio,
        support_operator,
        operator,
        provision_first_admin,
        seed_worker_secret,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_control_process_with_operator_and_crash_after(
    working_dir: &Path,
    storage: &Path,
    address: std::net::SocketAddr,
    management_address: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
    minio: Option<(u16, &str)>,
    support_operator: bool,
    operator: Option<SecurityPrincipal>,
    provision_first_admin: bool,
    seed_worker_secret: bool,
    crash_after: Option<&str>,
) -> RunningGateway {
    let fingerprint = certificate_sha256(&pki.cli_cert_path);
    let principal = if support_operator {
        mtls_support_admin_with_fingerprint(fingerprint)
    } else {
        mtls_principal_with_fingerprint(fingerprint)
    };
    let security_store: Arc<dyn ObjectStore> = if let Some((port, bucket)) = minio {
        Arc::new(object_store::prefix::PrefixStore::new(
            rockstream_test_support::minio::minio_object_store(port, bucket),
            "gateway-shard",
        ))
    } else {
        rockstream_storage::build_runtime_object_store(
            &storage.join("gateway-shard"),
            "gateway-shard",
        )
        .unwrap()
    };
    let durable = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    if provision_first_admin {
        match durable.get_security_principal(principal.id).await.unwrap() {
            Some(existing) => assert_eq!(existing, principal),
            None => {
                durable
                    .preprovision_first_administrator(principal, 1)
                    .await
                    .unwrap();
            }
        }
    }
    if let Some(operator) = operator {
        let policy = serde_json::to_string(&(
            operator.enabled,
            &operator.preset_grants,
            &operator.scoped_grants,
        ))
        .unwrap();
        let audit = SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "release-operator-grant".to_owned(),
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("principal:{}", operator.id.0)),
            scope: AuditField::Known("cluster".to_owned()),
            action: SecurityAuditClass::GrantChange,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::NotApplicable,
            operation_id: AuditField::Known("2".to_owned()),
            policy_revision: AuditField::Known(operator.revision),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::Known(format!(
                "revision={};policy={policy}",
                operator.revision
            )),
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
                        audit,
                    }],
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }

    let control_address = address.to_string();
    let management_address = management_address.to_string();
    let mut command = Command::new(env!("CARGO_BIN_EXE_rockstream"));
    command
        .current_dir(working_dir)
        .env("RUST_LOG", "off")
        .env_remove("ROCKSTREAM_CONFIG")
        .env_remove("ROCKSTREAM_TEST_SUPPORT_ARTIFACT_CRASH_AFTER")
        .args([
            "start",
            "--role",
            "control",
            "--daemon",
            "--storage",
            storage.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
            "--control-bind",
            &control_address,
            "--management-addr",
            &management_address,
            "--internal-tls-cert-path",
            pki.control_cert_path.to_str().unwrap(),
            "--internal-tls-key-path",
            pki.control_key_path.to_str().unwrap(),
            "--internal-tls-ca-cert-path",
            pki.ca_cert_path.to_str().unwrap(),
        ]);
    if let Some((port, bucket)) = minio {
        command
            .env(
                "ROCKSTREAM_OBJECT_STORE_ENDPOINT",
                format!("http://127.0.0.1:{port}"),
            )
            .env("ROCKSTREAM_OBJECT_STORE_BUCKET", bucket)
            .env("ROCKSTREAM_OBJECT_STORE_REGION", "us-east-1")
            .env(
                "ROCKSTREAM_OBJECT_STORE_ACCESS_KEY",
                rockstream_test_support::minio::MINIO_USER,
            )
            .env(
                "ROCKSTREAM_OBJECT_STORE_SECRET_KEY",
                rockstream_test_support::minio::MINIO_PASS,
            );
    }
    if seed_worker_secret {
        command.env("RS_SECRET_KEK", "release-test-kek");
    }
    if let Some(boundary) = crash_after {
        command.env("ROCKSTREAM_TEST_SUPPORT_ARTIFACT_CRASH_AFTER", boundary);
    }
    let child = command
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    RunningGateway(child)
}

async fn management_client(
    address: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
) -> Result<ManagementServiceClient<tonic::transport::Channel>, String> {
    management_client_with_identity(address, pki, &pki.cli_cert_path, &pki.cli_key_path).await
}

async fn management_client_with_identity(
    address: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
    cert_path: &Path,
    key_path: &Path,
) -> Result<ManagementServiceClient<tonic::transport::Channel>, String> {
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(
            std::fs::read(&pki.ca_cert_path).unwrap(),
        ))
        .identity(Identity::from_pem(
            std::fs::read(cert_path).unwrap(),
            std::fs::read(key_path).unwrap(),
        ));
    let channel = Endpoint::from_shared(format!("https://{address}"))
        .map_err(|error| error.to_string())?
        .tls_config(tls)
        .map_err(|error| error.to_string())?
        .connect()
        .await
        .map_err(|error| error.to_string())?;
    Ok(ManagementServiceClient::new(channel))
}

async fn wait_for_management_client(
    address: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
) -> ManagementServiceClient<tonic::transport::Channel> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match management_client(address, pki).await {
            Ok(client) => return client,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("management service did not become ready: {error}"),
        }
    }
}

async fn download_support_artifact(
    client: &mut ManagementServiceClient<tonic::transport::Channel>,
    opaque_id: &str,
) -> (Vec<u8>, u64) {
    let mut stream = client
        .download_support_artifact(DownloadSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            opaque_id: opaque_id.to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut content = Vec::new();
    let mut expires_at_ms = None;
    let mut expected_offset = 0;
    let mut final_chunk = false;
    while let Some(chunk) = stream.message().await.unwrap() {
        assert_eq!(chunk.offset, expected_offset);
        expected_offset += chunk.content.len() as u64;
        expires_at_ms = Some(chunk.expires_at_ms);
        content.extend_from_slice(&chunk.content);
        final_chunk = chunk.final_chunk;
        if final_chunk {
            break;
        }
    }
    assert!(final_chunk);
    assert!(stream.message().await.unwrap().is_none());
    (
        content,
        expires_at_ms.expect("artifact must contain a chunk"),
    )
}

fn support_artifact_object_path(id: &str) -> object_store::path::Path {
    let encoded_id = id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    object_store::path::Path::from("control/support-artifacts")
        .join(format!("artifacts/{encoded_id}.json"))
}

#[tokio::test]
async fn delegated_actor_policy_is_rechecked_by_release_control_process() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../rockstream-control/tests/fixtures");
    let assertion_public_key = fixtures.join("actor-assertion-test-public.jwk");
    let assertion_private_key = fixtures.join("actor-assertion-test-key.pem");
    let actor = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "Release delegated actor".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Service {
            service_id: "release-test-actor".to_owned(),
        }],
        preset_grants: Vec::new(),
        scoped_grants: Vec::new(),
    };
    let mut config = rockstream_types::config::NodeConfig::default();
    config.node.deployment_id = Some("release-deployment".to_owned());
    config.auth.actor_assertion_public_key_path = Some(assertion_public_key);
    std::fs::write(
        temp.path().join("rockstream.toml"),
        toml::to_string(&config).unwrap(),
    )
    .unwrap();

    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_delegated_control_process(
        temp.path(),
        &storage,
        address,
        &pki,
        Some(actor.clone()),
        false,
        None,
    )
    .await;
    wait_for_gateway(&mut process.0, address).await;

    let object_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let catalog = DurableCatalogStore::recover(object_store, "gateway")
        .await
        .unwrap();
    let mut principals_before = catalog.list_security_principals().await.unwrap();
    principals_before.sort_by_key(|principal| principal.id);
    let audit_before = catalog.list_security_audit_records().await.unwrap();

    let request = DeploymentRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        workload_id: WorkloadId(42),
        namespace: "public".to_owned(),
        workload_name: "delegated_view".to_owned(),
        plan_json: "{}".to_owned(),
        join_strategy: rockstream_types::config::JoinStrategy::Auto,
        schemas: Vec::new(),
        frontier: 0,
        storage_root: temp.path().join("workload").to_string_lossy().into_owned(),
        sink_operator_id: OperatorId(43),
        output_columns: Vec::new(),
        primary_key: Vec::new(),
        merge_key_columns: Vec::new(),
        routing_columns: Default::default(),
    };
    let message = WorkerMessage::DeployWorkload(request.clone());
    let action = SecurityAction::ViewAuthor;
    let scope = PolicyScope::Resource {
        namespace: "public".to_owned(),
        kind: ResourceKind::View,
        name: "delegated_view".to_owned(),
    };
    let secret_allowance = PolicyScope::Secret {
        namespace: "public".to_owned(),
        name: "kafka".to_owned(),
        consumer: Some(PrincipalId(3)),
    };
    let digest = rockstream_control::delegation::request_digest(&message).unwrap();
    let signer = rockstream_control::delegation::ActorAssertionSigner::from_pkcs8_pem(
        &assertion_private_key,
        "release-deployment",
    )
    .unwrap();
    let assertion = signer
        .issue(
            actor.id,
            actor.revision,
            action,
            scope.clone(),
            vec![secret_allowance],
            digest,
        )
        .unwrap();
    let wrong_deployment = rockstream_control::delegation::ActorAssertionSigner::from_pkcs8_pem(
        &assertion_private_key,
        "another-deployment",
    )
    .unwrap()
    .issue(
        actor.id,
        actor.revision,
        action,
        scope.clone(),
        Vec::new(),
        digest,
    )
    .unwrap();
    let client = rockstream_runtime::data_plane::DataPlaneClient::new(address.to_string())
        .with_internal_tls(&pki.gateway_tls_config())
        .unwrap();
    let policy_denial = client
        .deploy_delegated(request.clone(), &assertion)
        .await
        .unwrap_err();
    assert_eq!(
        policy_denial.to_string(),
        "RS-2401: asserted user actor is no longer authorized for this operation and scope"
    );
    let identity_denial = client
        .deploy_delegated(request, &wrong_deployment)
        .await
        .unwrap_err();
    assert_eq!(
        identity_denial.to_string(),
        "RS-2400: gateway actor assertion is invalid, expired, or bound to another deployment/request"
    );

    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let recovered_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let recovered = DurableCatalogStore::recover(recovered_store, "gateway")
        .await
        .unwrap();
    let mut principals_after = recovered.list_security_principals().await.unwrap();
    principals_after.sort_by_key(|principal| principal.id);
    assert_eq!(principals_after, principals_before);
    let records = recovered.list_security_audit_records().await.unwrap();
    assert_eq!(records.len(), audit_before.len() + 2);
    assert_eq!(&records[..audit_before.len()], audit_before.as_slice());
    let denial = &records[audit_before.len()];
    assert!(denial.event_id.starts_with("gateway-delegation:"));
    assert!(denial.timestamp_ms > 0);
    assert_eq!(
        denial,
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: denial.event_id.clone(),
            timestamp_ms: denial.timestamp_ms,
            actor: AuditIdentity::Verified(actor.id.to_string()),
            target: AuditField::Known(format!("{action:?}")),
            scope: AuditField::Known(format!("{scope:?}")),
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(actor.revision),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
    let rejected_identity = &records[audit_before.len() + 1];
    assert!(rejected_identity
        .event_id
        .starts_with("gateway-delegation:"));
    assert!(rejected_identity.timestamp_ms > 0);
    assert!(matches!(
        rejected_identity.actor,
        AuditIdentity::Unverified(_)
    ));
    assert_eq!(
        rejected_identity,
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: rejected_identity.event_id.clone(),
            timestamp_ms: rejected_identity.timestamp_ms,
            actor: rejected_identity.actor.clone(),
            target: AuditField::Known("gateway_data_plane".to_owned()),
            scope: AuditField::Unknown,
            action: SecurityAuditClass::AuthenticationFailure,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::NotApplicable,
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
}

#[tokio::test]
async fn delegated_gateway_sql_rechecks_control_policy_in_release_process() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let pki = rockstream_test_support::pki::TestPki::generate();
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../rockstream-control/tests/fixtures");
    let reader_certificates = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(&pki.cli_reader_cert_path).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let gateway_actor = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "Release delegated SQL actor".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![
            LoginMethod::Oidc {
                issuer: "https://issuer.example".to_owned(),
                subject: "alice".to_owned(),
            },
            LoginMethod::Mtls {
                certificate_sha256: sha2::Sha256::digest(reader_certificates[0].as_ref()).into(),
            },
        ],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Builder,
            scope: PolicyScope::Namespace {
                namespace: "public".to_owned(),
            },
        }],
        scoped_grants: Vec::new(),
    };
    let control_actor = gateway_actor.clone();

    let gateway_working_dir = temp.path().join("gateway-process");
    std::fs::create_dir_all(&gateway_working_dir).unwrap();
    let gateway_storage = temp.path().join("gateway-storage");
    let seed_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut seed_process = start_delegated_control_process(
        &gateway_working_dir,
        &gateway_storage,
        seed_address,
        &pki,
        Some(gateway_actor.clone()),
        false,
        None,
    )
    .await;
    wait_for_gateway(&mut seed_process.0, seed_address).await;
    seed_process.0.kill().unwrap();
    seed_process.0.wait().unwrap();

    let control_working_dir = temp.path().join("control-process");
    std::fs::create_dir_all(&control_working_dir).unwrap();
    let mut control_config = rockstream_types::config::NodeConfig::default();
    control_config.node.deployment_id = Some("release-deployment".to_owned());
    control_config.auth.actor_assertion_public_key_path =
        Some(fixtures.join("actor-assertion-test-public.jwk"));
    std::fs::write(
        control_working_dir.join("rockstream.toml"),
        toml::to_string(&control_config).unwrap(),
    )
    .unwrap();
    let control_storage = gateway_storage.clone();
    let control_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let control_store = rockstream_storage::build_runtime_object_store(
        &control_storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let control_catalog = DurableCatalogStore::recover(control_store, "gateway")
        .await
        .unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut control_process = start_control_process_with_operator(
        &control_working_dir,
        &control_storage,
        control_address,
        management_address,
        &pki,
        None,
        false,
        None,
        false,
        false,
    )
    .await;
    wait_for_gateway(&mut control_process.0, control_address).await;
    control_catalog.refresh().await.unwrap();
    let mut principals_before = control_catalog.list_security_principals().await.unwrap();
    principals_before.sort_by_key(|principal| principal.id);

    let mut tls_params = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    tls_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let tls_key = rcgen::KeyPair::generate().unwrap();
    let tls_cert = tls_params.self_signed(&tls_key).unwrap();
    let cert_path = temp.path().join("gateway.pem");
    let key_path = temp.path().join("gateway-key.pem");
    std::fs::write(&cert_path, tls_cert.pem()).unwrap();
    std::fs::write(&key_path, tls_key.serialize_pem()).unwrap();

    let jwks_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let jwks_addr = jwks_listener.local_addr().unwrap();
    let jwks_url = format!("http://{jwks_addr}/jwks");
    let jwks_task = tokio::spawn(serve_jwks(jwks_listener, 1));
    let gateway_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut gateway_process = start_delegated_oidc_gateway_process(
        &gateway_working_dir,
        &gateway_storage,
        gateway_address,
        control_address,
        &pki,
        &cert_path,
        &key_path,
        &fixtures.join("actor-assertion-test-key.pem"),
        &jwks_url,
    );
    wait_for_gateway(&mut gateway_process.0, gateway_address).await;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(tls_cert.der().clone()).unwrap();
    let client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(gateway_address.port())
        .user("alice")
        .password(VALID_TOKEN)
        .dbname("test")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    let (client, connection) = config
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(client_tls))
        .await
        .expect("normal OIDC PostgreSQL client should connect to the delegated gateway");
    let connection_task = tokio::spawn(connection);
    control_catalog.refresh().await.unwrap();
    let audit_before = control_catalog.list_security_audit_records().await.unwrap();
    client
        .execute("CREATE TABLE orders (id BIGINT, amount BIGINT)", &[])
        .await
        .unwrap();
    client
        .batch_execute("INSERT INTO orders VALUES (7, 70), (7, 70), (9, NULL)")
        .await
        .unwrap();
    let initial_rows = client
        .query(
            "SELECT id, amount FROM orders ORDER BY id, amount NULLS LAST",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
        .collect::<Vec<_>>();
    assert_eq!(initial_rows, [(7, Some(70)), (7, Some(70)), (9, None)]);

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut management = loop {
        match try_management_client(management_address, &pki).await {
            Ok(client) => break client,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("management endpoint did not start: {error}"),
        }
    };
    let operation_id = uuid::Uuid::new_v4().to_string();
    let response = management
        .set_security_principal_grants(SetSecurityPrincipalGrantsRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            principal_id: 2,
            expected_revision: 1,
            expected_grantor_revision: 1,
            idempotency_key: operation_id.clone(),
            preset_grants: vec![SecurityPresetGrant {
                preset: "operator".to_owned(),
                scope_json: serde_json::to_string(&PolicyScope::Cluster).unwrap(),
            }],
            scoped_grants: Vec::new(),
        })
        .await
        .expect("authenticated management request should leave only operator grants")
        .into_inner();
    assert_eq!(
        response.protocol_version,
        rockstream_management_proto::PROTOCOL_VERSION
    );
    let mut operation = response.operation.expect("grant mutation operation");
    assert_eq!(operation.operation_id, operation_id);
    assert!(chrono::DateTime::parse_from_rfc3339(&operation.started_at).is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(&operation.updated_at).is_ok());
    operation.started_at = "<timestamp>".to_owned();
    operation.updated_at = "<timestamp>".to_owned();
    assert_eq!(
        operation,
        ManagementWireOperation {
            operation_id: operation_id.clone(),
            kind: "security_principal_grants".to_owned(),
            state: "succeeded".to_owned(),
            started_at: "<timestamp>".to_owned(),
            updated_at: "<timestamp>".to_owned(),
            progress: "100%".to_owned(),
            phase: "security_policy_committed".to_owned(),
            error_code: String::new(),
            next_steps: Vec::new(),
            source_version: format!("operation-record:{MANAGEMENT_OPERATION_RECORD_VERSION}"),
            result_json: r#"{"effective_permissions":[{"action":"cluster_observe","scope":"cluster"},{"action":"node_drain","scope":"cluster"},{"action":"shard_migrate","scope":"cluster"},{"action":"operation_cancel","scope":"cluster"},{"action":"operation_history_read","scope":"cluster"},{"action":"diagnostics_read","scope":"cluster"},{"action":"support_generate","scope":"cluster"},{"action":"support_download","scope":"cluster"}],"preset_grants":[{"preset":"operator","scope":"cluster"}],"principal_id":2,"revision":2,"scoped_grants":[]}"#.to_owned(),
        }
    );

    let mut operator_only_actor = control_actor.clone();
    operator_only_actor.revision = 2;
    operator_only_actor.preset_grants = vec![ScopedPresetGrant {
        preset: PermissionPreset::Operator,
        scope: PolicyScope::Cluster,
    }];
    let mut principals_after_grant = principals_before.clone();
    *principals_after_grant
        .iter_mut()
        .find(|principal| principal.id == PrincipalId(2))
        .expect("delegated actor is provisioned") = operator_only_actor.clone();
    control_catalog.refresh().await.unwrap();
    let mut current_principals = control_catalog.list_security_principals().await.unwrap();
    current_principals.sort_by_key(|principal| principal.id);
    assert_eq!(current_principals, principals_after_grant);

    let audit_after_grant = control_catalog.list_security_audit_records().await.unwrap();
    assert_eq!(audit_after_grant.len(), audit_before.len() + 4);
    assert_eq!(
        &audit_after_grant[..audit_before.len()],
        audit_before.as_slice()
    );
    let authentication = &audit_after_grant[audit_before.len()];
    let authentication_id = authentication
        .event_id
        .strip_prefix("management-authn-")
        .expect("management authentication audit event");
    assert!(uuid::Uuid::parse_str(authentication_id).is_ok());
    assert!(authentication.timestamp_ms > 0);
    assert_eq!(
        authentication,
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: authentication.event_id.clone(),
            timestamp_ms: authentication.timestamp_ms,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("management-api".to_owned()),
            scope: AuditField::NotApplicable,
            action: SecurityAuditClass::AuthenticationSuccess,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
    let start = &audit_after_grant[audit_before.len() + 1];
    assert!(start.timestamp_ms > 0);
    assert_eq!(
        start,
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: format!("management-security-grants-start:{operation_id}"),
            timestamp_ms: start.timestamp_ms,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("principal:2".to_owned()),
            scope: AuditField::Known("cluster".to_owned()),
            action: SecurityAuditClass::AdminOperationStart,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Observed(operation_id.clone()),
            operation_id: AuditField::Known(operation_id.clone()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
    let before_policy = serde_json::to_string(&(
        control_actor.enabled,
        &control_actor.preset_grants,
        &control_actor.scoped_grants,
    ))
    .unwrap();
    let after_policy = serde_json::to_string(&(
        operator_only_actor.enabled,
        &operator_only_actor.preset_grants,
        &operator_only_actor.scoped_grants,
    ))
    .unwrap();
    let grant = &audit_after_grant[audit_before.len() + 2];
    assert!(grant.timestamp_ms > 0);
    let expected_grant = SecurityAuditRecord {
        format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
        event_id: format!("management-security-grants:{operation_id}"),
        timestamp_ms: grant.timestamp_ms,
        actor: AuditIdentity::Verified("principal:1".to_owned()),
        target: AuditField::Known("principal:2".to_owned()),
        scope: AuditField::Known("cluster".to_owned()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Observed(operation_id.clone()),
        operation_id: AuditField::Known(operation_id.clone()),
        policy_revision: AuditField::Known(2),
        before_state: AuditField::Known(format!("revision=1;policy={before_policy}")),
        after_state: AuditField::Known(format!("revision=2;policy={after_policy}")),
    };
    assert_eq!(grant, &expected_grant);
    let completion = &audit_after_grant[audit_before.len() + 3];
    assert_eq!(completion.timestamp_ms, grant.timestamp_ms);
    let mut expected_completion = expected_grant;
    expected_completion.event_id = format!("management-security-grants-completion:{operation_id}");
    expected_completion.action = SecurityAuditClass::AdminOperationCompletion;
    assert_eq!(completion, &expected_completion);

    let error = client
        .execute(
            "CREATE MATERIALIZED VIEW stale_view AS SELECT id, SUM(amount) AS total FROM orders GROUP BY id",
            &[],
        )
        .await
        .expect_err("control must reject a gateway assertion outside current actor policy");
    let db_error = error
        .as_db_error()
        .expect("delegation denial is a pgwire error");
    let descriptor = rockstream_types::error_code::ErrorDescriptor::lookup(
        rockstream_types::error_code::RS_2026,
    )
    .unwrap();
    let (prefix, with_correlation) = db_error
        .message()
        .split_once("(correlation_id=")
        .expect("registered error includes its correlation ID");
    let (correlation_id, suffix) = with_correlation
        .split_once(" context=")
        .expect("registered error includes diagnostic context");
    assert!(uuid::Uuid::parse_str(correlation_id).is_ok());
    let expected_message = format!(
        "[RS-2026] query.query_time_execution_failed: Query-time DataFusion planning or execution failed for an ad hoc query (detail=deploy distributed view stale_view: RS-2401: asserted user actor is no longer authorized for this operation and scope) (correlation_id=<uuid> context=detail=deploy distributed view stale_view: RS-2401: asserted user actor is no longer authorized for this operation and scope) next_steps: {}",
        descriptor.default_next_steps
    );
    assert_eq!(
        format!("{prefix}(correlation_id=<uuid> context={suffix}"),
        expected_message
    );
    assert_eq!(db_error.severity(), descriptor.severity.to_string());
    assert_eq!(db_error.code().code(), descriptor.sqlstate);
    assert_eq!(
        client
            .query_one("SELECT 1 AS value", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1_i64
    );

    tokio::time::sleep(Duration::from_secs(2)).await;
    let data_denial = client
        .query(
            "SELECT id, amount FROM orders ORDER BY id, amount NULLS LAST",
            &[],
        )
        .await
        .expect_err("the operator preset must not authorize business-data reads");
    assert_eq!(data_denial.as_db_error().unwrap().code().code(), "42501");
    assert_eq!(
        data_denial.as_db_error().unwrap().message(),
        "[RS-2401] auth.permission_denied: principal 'alice' is not authorized for action 'DataRead' on public.orders. next_steps: Request an authorized scoped grant, then retry."
    );
    assert_eq!(
        client
            .query_one("SELECT 1 AS value", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1_i64,
        "a denied read must preserve the authenticated session"
    );

    let operator_tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(
            std::fs::read(&pki.ca_cert_path).unwrap(),
        ))
        .identity(tonic::transport::Identity::from_pem(
            std::fs::read(&pki.cli_reader_cert_path).unwrap(),
            std::fs::read(&pki.cli_reader_key_path).unwrap(),
        ));
    let operator_channel =
        tonic::transport::Endpoint::from_shared(format!("https://{management_address}"))
            .unwrap()
            .tls_config(operator_tls)
            .unwrap()
            .connect()
            .await
            .unwrap();
    let mut operator_management =
        rockstream_management_proto::v1::management_service_client::ManagementServiceClient::new(
            operator_channel,
        );
    let self_grant_error = operator_management
        .set_security_principal_grants(SetSecurityPrincipalGrantsRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            principal_id: 2,
            expected_revision: 2,
            expected_grantor_revision: 2,
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            preset_grants: vec![SecurityPresetGrant {
                preset: "administrator".to_owned(),
                scope_json: serde_json::to_string(&PolicyScope::Cluster).unwrap(),
            }],
            scoped_grants: Vec::new(),
        })
        .await
        .expect_err("the operator preset must not authorize grant administration");
    assert_eq!(self_grant_error.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        self_grant_error.message(),
        "management action is not authorized"
    );

    let backup_destination = temp.path().join("operator-backup");
    let backup_scope = PolicyScope::BackupDestination {
        destination: backup_destination.display().to_string(),
    };
    let backup_operation_id = uuid::Uuid::new_v4().to_string();
    let operator_backup_denial = operator_management
        .create_backup(CreateBackupRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            destination: backup_destination.display().to_string(),
            idempotency_key: backup_operation_id.clone(),
        })
        .await
        .expect_err("the operator preset must not authorize backup creation");
    assert_eq!(operator_backup_denial.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        operator_backup_denial.message(),
        "management action is not authorized"
    );
    assert!(!backup_destination.exists());
    control_catalog.refresh().await.unwrap();
    let mut principals_after_operator_denials =
        control_catalog.list_security_principals().await.unwrap();
    principals_after_operator_denials.sort_by_key(|principal| principal.id);
    assert_eq!(principals_after_operator_denials, principals_after_grant);
    let audit_before_scoped_grant = control_catalog.list_security_audit_records().await.unwrap();
    let self_grant_denial_record = &audit_before_scoped_grant[audit_before_scoped_grant.len() - 2];
    assert!(self_grant_denial_record
        .event_id
        .starts_with("management-denial-"));
    assert!(self_grant_denial_record.timestamp_ms > 0);
    assert_eq!(
        self_grant_denial_record,
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: self_grant_denial_record.event_id.clone(),
            timestamp_ms: self_grant_denial_record.timestamp_ms,
            actor: AuditIdentity::Verified("principal:2".to_owned()),
            target: AuditField::Known("GrantManage".to_owned()),
            scope: AuditField::Known(serde_json::to_string(&PolicyScope::Cluster).unwrap()),
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(2),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
    let backup_denial_record = audit_before_scoped_grant
        .last()
        .expect("operator backup denial is durably audited");
    assert!(backup_denial_record
        .event_id
        .starts_with("management-denial-"));
    assert!(backup_denial_record.timestamp_ms > 0);
    assert_eq!(
        backup_denial_record,
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: backup_denial_record.event_id.clone(),
            timestamp_ms: backup_denial_record.timestamp_ms,
            actor: AuditIdentity::Verified("principal:2".to_owned()),
            target: AuditField::Known("BackupCreate".to_owned()),
            scope: AuditField::Known(serde_json::to_string(&backup_scope).unwrap()),
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(2),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );

    let scoped_operation_id = uuid::Uuid::new_v4().to_string();
    let scoped_response = management
        .set_security_principal_grants(SetSecurityPrincipalGrantsRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            principal_id: 2,
            expected_revision: 2,
            expected_grantor_revision: 1,
            idempotency_key: scoped_operation_id.clone(),
            preset_grants: vec![SecurityPresetGrant {
                preset: "operator".to_owned(),
                scope_json: serde_json::to_string(&PolicyScope::Cluster).unwrap(),
            }],
            scoped_grants: vec![SecurityActionGrant {
                action: "backup_create".to_owned(),
                scope_json: serde_json::to_string(&backup_scope).unwrap(),
            }],
        })
        .await
        .expect("the administrator should add a scoped backup-create grant")
        .into_inner();
    assert_eq!(
        scoped_response.protocol_version,
        rockstream_management_proto::PROTOCOL_VERSION
    );
    let mut scoped_operation = scoped_response.operation.expect("scoped grant operation");
    assert_eq!(scoped_operation.operation_id, scoped_operation_id);
    assert!(chrono::DateTime::parse_from_rfc3339(&scoped_operation.started_at).is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(&scoped_operation.updated_at).is_ok());
    scoped_operation.started_at = "<timestamp>".to_owned();
    scoped_operation.updated_at = "<timestamp>".to_owned();
    assert_eq!(
        scoped_operation,
        ManagementWireOperation {
            operation_id: scoped_operation_id.clone(),
            kind: "security_principal_grants".to_owned(),
            state: "succeeded".to_owned(),
            started_at: "<timestamp>".to_owned(),
            updated_at: "<timestamp>".to_owned(),
            progress: "100%".to_owned(),
            phase: "security_policy_committed".to_owned(),
            error_code: String::new(),
            next_steps: Vec::new(),
            source_version: format!("operation-record:{MANAGEMENT_OPERATION_RECORD_VERSION}"),
            result_json: serde_json::json!({
                "principal_id": 2,
                "revision": 3,
                "preset_grants": [{"preset": "operator", "scope": "cluster"}],
                "scoped_grants": [{
                    "action": "backup_create",
                    "scope": {"backup_destination": {"destination": backup_destination.display().to_string()}}
                }],
                "effective_permissions": [
                    {"action": "cluster_observe", "scope": "cluster"},
                    {"action": "node_drain", "scope": "cluster"},
                    {"action": "shard_migrate", "scope": "cluster"},
                    {"action": "operation_cancel", "scope": "cluster"},
                    {"action": "operation_history_read", "scope": "cluster"},
                    {"action": "diagnostics_read", "scope": "cluster"},
                    {"action": "support_generate", "scope": "cluster"},
                    {"action": "support_download", "scope": "cluster"},
                    {"action": "backup_create", "scope": {"backup_destination": {"destination": backup_destination.display().to_string()}}}
                ]
            })
            .to_string(),
        }
    );

    let mut scoped_actor = operator_only_actor.clone();
    scoped_actor.revision = 3;
    scoped_actor.scoped_grants = vec![ScopedActionGrant {
        action: SecurityAction::BackupCreate,
        scope: backup_scope,
    }];
    let mut principals_after_scoped_grant = principals_after_grant.clone();
    *principals_after_scoped_grant
        .iter_mut()
        .find(|principal| principal.id == PrincipalId(2))
        .expect("delegated actor is provisioned") = scoped_actor.clone();
    control_catalog.refresh().await.unwrap();
    let mut current_principals = control_catalog.list_security_principals().await.unwrap();
    current_principals.sort_by_key(|principal| principal.id);
    assert_eq!(current_principals, principals_after_scoped_grant);
    let audit_after_scoped_grant = control_catalog.list_security_audit_records().await.unwrap();
    assert_eq!(
        audit_after_scoped_grant.len(),
        audit_before_scoped_grant.len() + 3
    );
    assert_eq!(
        &audit_after_scoped_grant[..audit_before_scoped_grant.len()],
        audit_before_scoped_grant.as_slice()
    );
    let scoped_grant_record = &audit_after_scoped_grant[audit_before_scoped_grant.len() + 1];
    assert_eq!(scoped_grant_record.action, SecurityAuditClass::GrantChange);
    assert_eq!(scoped_grant_record.outcome, SecurityAuditOutcome::Success);
    assert_eq!(
        scoped_grant_record.correlation,
        AuditCorrelation::Observed(scoped_operation_id.clone())
    );
    assert_eq!(scoped_grant_record.policy_revision, AuditField::Known(3));

    tokio::time::sleep(Duration::from_secs(2)).await;
    let backup_response = operator_management
        .create_backup(CreateBackupRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            destination: backup_destination.display().to_string(),
            idempotency_key: backup_operation_id.clone(),
        })
        .await
        .expect_err("the control-only fixture has no backup executor");
    assert_eq!(
        backup_response.code(),
        tonic::Code::Unavailable,
        "the granted action passed authorization before executor availability was checked"
    );
    assert_eq!(
        backup_response.message(),
        "CreateBackup executor is not attached"
    );
    assert!(!backup_destination.exists());

    tokio::time::sleep(Duration::from_secs(2)).await;
    let scoped_data_denial = client
        .query(
            "SELECT id, amount FROM orders ORDER BY id, amount NULLS LAST",
            &[],
        )
        .await
        .expect_err("the scoped backup grant must not authorize business-data reads");
    assert_eq!(
        scoped_data_denial.as_db_error().unwrap().code().code(),
        "42501"
    );
    assert_eq!(
        scoped_data_denial.as_db_error().unwrap().message(),
        "[RS-2401] auth.permission_denied: principal 'alice' is not authorized for action 'DataRead' on public.orders. next_steps: Request an authorized scoped grant, then retry."
    );
    assert_eq!(
        client
            .query_one("SELECT 1 AS value", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1_i64,
        "a denied read must preserve the authenticated session after the separate grant"
    );

    control_catalog.refresh().await.unwrap();
    let audit_before_shutdown = control_catalog.list_security_audit_records().await.unwrap();
    drop(client);
    connection_task.abort();
    jwks_task.await.unwrap();
    gateway_process.0.kill().unwrap();
    gateway_process.0.wait().unwrap();
    control_process.0.kill().unwrap();
    control_process.0.wait().unwrap();

    let gateway_store = rockstream_storage::build_runtime_object_store(
        &gateway_storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let gateway_catalog = DurableCatalogStore::recover(gateway_store, "gateway")
        .await
        .unwrap();
    assert_eq!(
        gateway_catalog
            .get_view_by_name("stale_view")
            .await
            .unwrap(),
        None
    );

    let control_store = rockstream_storage::build_runtime_object_store(
        &control_storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let recovered = DurableCatalogStore::recover(control_store, "gateway")
        .await
        .unwrap();
    let mut principals_after = recovered.list_security_principals().await.unwrap();
    principals_after.sort_by_key(|principal| principal.id);
    assert_eq!(principals_after, principals_after_scoped_grant);
    let records = recovered.list_security_audit_records().await.unwrap();
    assert_eq!(records, audit_before_shutdown);
    let data_denials = records
        .iter()
        .filter(|record| {
            record.target == AuditField::Known("public.orders".to_owned())
                && record.action == SecurityAuditClass::AuthorizationDenial
                && record.outcome == SecurityAuditOutcome::Denied
        })
        .collect::<Vec<_>>();
    assert_eq!(data_denials.len(), 2);
    let data_scope = AuditField::Known(
        serde_json::to_string(&PolicyScope::Resource {
            namespace: "public".to_owned(),
            kind: ResourceKind::Table,
            name: "orders".to_owned(),
        })
        .unwrap(),
    );
    let actor = AuditIdentity::Verified(format!(
        "oidc:{}:{}:{}:{}",
        "https://issuer.example".len(),
        "https://issuer.example",
        "alice".len(),
        "alice"
    ));
    for (record, policy_revision) in data_denials.into_iter().zip([2, 3]) {
        assert!(record.event_id.starts_with("gateway-authz-"));
        assert!(record.timestamp_ms > 0);
        assert!(
            matches!(&record.correlation, AuditCorrelation::Observed(value) if !value.is_empty())
        );
        assert_eq!(
            record,
            &SecurityAuditRecord {
                format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
                event_id: record.event_id.clone(),
                timestamp_ms: record.timestamp_ms,
                actor: actor.clone(),
                target: AuditField::Known("public.orders".to_owned()),
                scope: data_scope.clone(),
                action: SecurityAuditClass::AuthorizationDenial,
                outcome: SecurityAuditOutcome::Denied,
                correlation: record.correlation.clone(),
                operation_id: AuditField::NotApplicable,
                policy_revision: AuditField::Known(policy_revision),
                before_state: AuditField::NotApplicable,
                after_state: AuditField::NotApplicable,
            }
        );
    }
    let denial = records
        .iter()
        .find(|record| record.target == AuditField::Known("ViewAuthor".to_owned()))
        .expect("stale-view denial is durably audited");
    assert!(denial.event_id.starts_with("gateway-delegation:"));
    assert!(denial.timestamp_ms > 0);
    assert_eq!(
        denial,
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: denial.event_id.clone(),
            timestamp_ms: denial.timestamp_ms,
            actor: AuditIdentity::Verified(PrincipalId(2).to_string()),
            target: AuditField::Known("ViewAuthor".to_owned()),
            scope: AuditField::Known(format!(
                "{:?}",
                PolicyScope::Resource {
                    namespace: "public".to_owned(),
                    kind: ResourceKind::View,
                    name: "stale_view".to_owned(),
                }
            )),
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(2),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
}

#[tokio::test]
async fn delegated_worker_secret_uses_durable_store_in_release_control_process() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../rockstream-control/tests/fixtures");
    let secret_scope = PolicyScope::Secret {
        namespace: "public".to_owned(),
        name: "kafka".to_owned(),
        consumer: Some(PrincipalId(3)),
    };
    let actor = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "Release secret actor".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Service {
            service_id: "release-test-actor".to_owned(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Builder,
            scope: PolicyScope::Resource {
                namespace: "public".to_owned(),
                kind: ResourceKind::View,
                name: "delegated_view".to_owned(),
            },
        }],
        scoped_grants: vec![ScopedActionGrant {
            action: SecurityAction::SecretUse,
            scope: secret_scope.clone(),
        }],
    };
    let mut config = rockstream_types::config::NodeConfig::default();
    config.node.deployment_id = Some("release-deployment".to_owned());
    config.auth.actor_assertion_public_key_path =
        Some(fixtures.join("actor-assertion-test-public.jwk"));
    std::fs::write(
        temp.path().join("rockstream.toml"),
        toml::to_string(&config).unwrap(),
    )
    .unwrap();

    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_delegated_control_process(
        temp.path(),
        &storage,
        address,
        &pki,
        Some(actor.clone()),
        true,
        None,
    )
    .await;
    wait_for_gateway(&mut process.0, address).await;

    let object_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let catalog = DurableCatalogStore::recover(object_store, "gateway")
        .await
        .unwrap();
    let audit_before = catalog.list_security_audit_records().await.unwrap();
    let gateway_address: std::net::SocketAddr =
        format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut gateway_process = start_gateway_process(
        temp.path(),
        &storage,
        gateway_address,
        &pki.gateway_cert_path,
        &pki.gateway_key_path,
        "off",
        None,
        "",
        "",
        "",
    );
    wait_for_gateway(&mut gateway_process.0, gateway_address).await;
    let worker_storage = temp.path().join("worker-storage");
    let (worker, worker_handle) = rockstream_runtime::client::start_worker_client_with_tls(
        1,
        &address.to_string(),
        &worker_storage,
        pki.worker_tls_config(1),
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

    let mut request = DeploymentRequest {
        version: DEPLOYMENT_DESCRIPTOR_VERSION,
        workload_id: WorkloadId(42),
        namespace: "public".to_owned(),
        workload_name: "delegated_view".to_owned(),
        plan_json: "{}".to_owned(),
        join_strategy: rockstream_types::config::JoinStrategy::Auto,
        schemas: vec![rockstream_types::data_plane::DeploymentSchema {
            relation: "orders".to_owned(),
            columns: vec![rockstream_types::data_plane::DeploymentColumn {
                name: "id".to_owned(),
                data_type: "i64".to_owned(),
            }],
        }],
        frontier: 0,
        storage_root: temp.path().join("workload").to_string_lossy().into_owned(),
        sink_operator_id: OperatorId(43),
        output_columns: vec!["id".to_owned()],
        primary_key: vec![0],
        merge_key_columns: Vec::new(),
        routing_columns: Default::default(),
    };
    request.plan_json = serde_json::to_string(&rockstream_plan::PlanNode::ViewSink {
        view_name: request.workload_name.clone(),
        pk: vec![0],
        child: Box::new(rockstream_plan::PlanNode::Source {
            name: "orders".to_owned(),
        }),
    })
    .unwrap();
    let message = WorkerMessage::DeployWorkload(request.clone());
    let action = SecurityAction::ViewAuthor;
    let view_scope = PolicyScope::Resource {
        namespace: "public".to_owned(),
        kind: ResourceKind::View,
        name: "delegated_view".to_owned(),
    };
    let assertion = rockstream_control::delegation::ActorAssertionSigner::from_pkcs8_pem(
        &fixtures.join("actor-assertion-test-key.pem"),
        "release-deployment",
    )
    .unwrap()
    .issue(
        actor.id,
        actor.revision,
        action,
        view_scope,
        vec![secret_scope.clone()],
        rockstream_control::delegation::request_digest(&message).unwrap(),
    )
    .unwrap();
    let gateway = rockstream_runtime::data_plane::DataPlaneClient::new(address.to_string())
        .with_internal_tls(&pki.gateway_tls_config())
        .unwrap();
    gateway
        .deploy_delegated(request.clone(), &assertion)
        .await
        .unwrap();
    assert!(worker
        .get_deployment(request.workload_id, ShardId(request.workload_id.0 * 16))
        .is_some());

    let resolved = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(secret) = worker.secret(
                request.workload_id,
                "kafka",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ) {
                break secret;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if resolved.is_err() {
        worker_handle.abort();
        gateway_process.0.kill().unwrap();
        gateway_process.0.wait().unwrap();
        process.0.kill().unwrap();
        process.0.wait().unwrap();
        let recovered_store = rockstream_storage::build_runtime_object_store(
            &storage.join("gateway-shard"),
            "gateway-shard",
        )
        .unwrap();
        let recovered = DurableCatalogStore::recover(recovered_store, "gateway")
            .await
            .unwrap();
        let records = recovered.list_security_audit_records().await.unwrap();
        panic!(
            "worker secret refresh timed out; new records: {:?}",
            &records[audit_before.len()..]
        );
    }
    let resolved = resolved.unwrap();
    assert_eq!(resolved.workload_id, request.workload_id);
    assert_eq!(resolved.secret_name, "kafka");
    assert_eq!(
        resolved.payload,
        std::collections::HashMap::from([(
            "password".to_owned(),
            "release-worker-secret".to_owned(),
        )])
    );
    worker_handle.abort();
    gateway_process.0.kill().unwrap();
    gateway_process.0.wait().unwrap();
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let recovered_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let recovered = DurableCatalogStore::recover(recovered_store, "gateway")
        .await
        .unwrap();
    let records = recovered.list_security_audit_records().await.unwrap();
    let secret_audits: Vec<_> = records[audit_before.len()..]
        .iter()
        .filter(|record| {
            record.action == SecurityAuditClass::AuthenticationSuccess
                && record.target == AuditField::Known("kafka".to_owned())
        })
        .collect();
    assert_eq!(secret_audits.len(), 1);
    assert_eq!(
        secret_audits[0].actor,
        AuditIdentity::Verified(actor.id.to_string())
    );
    assert_eq!(
        secret_audits[0].scope,
        AuditField::Known(format!("{secret_scope:?}"))
    );
    assert_eq!(secret_audits[0].outcome, SecurityAuditOutcome::Success);
    assert_eq!(
        secret_audits[0].policy_revision,
        AuditField::Known(actor.revision)
    );
}

fn parse_error_fields(payload: &[u8]) -> std::collections::BTreeMap<u8, String> {
    let mut fields = std::collections::BTreeMap::new();
    let mut cursor = 0;
    while cursor < payload.len() && payload[cursor] != 0 {
        let kind = payload[cursor];
        cursor += 1;
        let end = cursor
            + payload[cursor..]
                .iter()
                .position(|byte| *byte == 0)
                .unwrap();
        fields.insert(
            kind,
            String::from_utf8(payload[cursor..end].to_vec()).unwrap(),
        );
        cursor = end + 1;
    }
    fields
}

async fn wait_for_exact_query_rows(
    broker: &ContainerAsync<apache::Kafka>,
    client: &tokio_postgres::Client,
    process: &mut Child,
    query: &str,
    expected: &[Vec<Option<String>>],
    deadline: Instant,
    label: &str,
) {
    loop {
        let observed = client.simple_query(query).await.ok().map(|messages| {
            let mut rows = simple_query_rows(&messages);
            rows.sort();
            rows
        });
        if observed
            .as_ref()
            .is_some_and(|rows| rows.as_slice() == expected)
        {
            return;
        }
        if Instant::now() >= deadline {
            let sources = client
                .simple_query("SHOW SOURCES")
                .await
                .ok()
                .map(|messages| simple_query_rows(&messages));
            let source_rows = client
                .simple_query("SELECT * FROM rotating_orders")
                .await
                .ok()
                .map(|messages| simple_query_rows(&messages));
            let backfill = client
                .simple_query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW rotating_a")
                .await
                .ok()
                .map(|messages| simple_query_rows(&messages));
            let mut view_diagnostics = Vec::new();
            for view in ["rotating_a", "rotating_b", "steady_rows"] {
                let rows = client
                    .simple_query(&format!("SELECT * FROM {view}"))
                    .await
                    .ok()
                    .map(|messages| simple_query_rows(&messages));
                let status = client
                    .simple_query(&format!(
                        "SHOW BACKFILL STATUS FOR MATERIALIZED VIEW {view}"
                    ))
                    .await
                    .ok()
                    .map(|messages| simple_query_rows(&messages));
                view_diagnostics.push((view, rows, status));
            }
            let consumer_checkpoints =
                kafka_source_group_checkpoints(broker, "rotating-orders", "rotation-group-").await;
            let _ = process.kill();
            let _ = process.wait();
            let mut stderr = String::new();
            if let Some(stderr_pipe) = process.stderr.as_mut() {
                let _ = stderr_pipe.read_to_string(&mut stderr);
            }
            panic!(
                "{label}: expected {expected:?}, observed {observed:?}; sources={sources:?}; source_rows={source_rows:?}; backfill={backfill:?}; view_diagnostics={view_diagnostics:?}; consumer_checkpoints={consumer_checkpoints:?}; stderr={stderr}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn kafka_source_rows(
    producer: &FutureProducer,
    topic: &str,
    rows: &[(i64, Option<i64>, i32)],
) {
    let mut deliveries = Vec::with_capacity(rows.len());
    for (id, amount, partition) in rows {
        let payload = serde_json::json!({
            "timestamp": id,
            "values": [id, amount],
            "weight": 1,
        })
        .to_string();
        deliveries.push(
            producer
                .send_result(
                    FutureRecord::<(), _>::to(topic)
                        .partition(*partition)
                        .payload(&payload),
                )
                .unwrap(),
        );
    }
    for delivery in deliveries {
        delivery.await.unwrap().unwrap();
    }
}

async fn kafka_source_group_checkpoints<I: Image>(
    broker: &ContainerAsync<I>,
    topic: &str,
    group_prefix: &str,
) -> BTreeMap<String, BTreeMap<i32, (i64, i64)>> {
    let mut command = broker
        .exec(ExecCommand::new([
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:9093",
            "--describe",
            "--all-groups",
        ]))
        .await
        .unwrap();
    let output = String::from_utf8(command.stdout_to_vec().await.unwrap()).unwrap();
    assert_eq!(command.exit_code().await.unwrap(), Some(0), "{output}");
    let mut checkpoints = BTreeMap::new();
    for line in output.lines() {
        let columns = line.split_whitespace().collect::<Vec<_>>();
        if columns.len() < 6 || !columns[0].starts_with(group_prefix) || columns[1] != topic {
            continue;
        }
        let (Ok(partition), Ok(current), Ok(log_end)) = (
            columns[2].parse::<i32>(),
            columns[3].parse::<i64>(),
            columns[4].parse::<i64>(),
        ) else {
            continue;
        };
        checkpoints
            .entry(columns[0].to_string())
            .or_insert_with(BTreeMap::new)
            .insert(partition, (current, log_end));
    }
    checkpoints
}

async fn wait_for_kafka_group_checkpoints<I: Image>(
    broker: &ContainerAsync<I>,
    topic: &str,
    group_prefix: &str,
    expected_groups: usize,
    expected_offsets: &BTreeMap<i32, i64>,
    deadline: Instant,
    label: &str,
) -> BTreeMap<String, BTreeMap<i32, (i64, i64)>> {
    let expected = expected_offsets
        .iter()
        .map(|(partition, offset)| (*partition, (*offset, *offset)))
        .collect::<BTreeMap<_, _>>();
    loop {
        let observed = kafka_source_group_checkpoints(broker, topic, group_prefix).await;
        if observed.len() == expected_groups
            && observed.values().all(|partitions| partitions == &expected)
        {
            return observed;
        }
        assert!(
            Instant::now() < deadline,
            "{label}: expected {expected_groups} groups at {expected:?}, observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn kafka_sasl_client(bootstrap: &str, username: &str, password: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanisms", "PLAIN")
        .set("sasl.username", username)
        .set("sasl.password", password);
    config
}

async fn kafka_acl<I: Image>(broker: &ContainerAsync<I>, args: &[&str]) {
    let mut command_args = vec![
        "/opt/kafka/bin/kafka-acls.sh",
        "--bootstrap-server",
        "localhost:9093",
    ];
    command_args.extend_from_slice(args);
    let mut command = broker.exec(ExecCommand::new(command_args)).await.unwrap();
    let stdout = String::from_utf8(command.stdout_to_vec().await.unwrap()).unwrap();
    let stderr = String::from_utf8(command.stderr_to_vec().await.unwrap()).unwrap();
    assert_eq!(
        command.exit_code().await.unwrap(),
        Some(0),
        "stdout={stdout}; stderr={stderr}"
    );
    assert_eq!(stderr, "");
}

fn simple_query_rows(messages: &[tokio_postgres::SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|column| row.get(column).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn security_audit_history_uses_release_pgwire_pages_after_denial() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let shard_dir = storage.join("gateway-shard");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let catalog = DurableCatalogStore::new(store, "gateway");
    let mut administrator = scram_principal();
    administrator.preset_grants.push(ScopedPresetGrant {
        preset: PermissionPreset::Builder,
        scope: PolicyScope::Resource {
            namespace: "public".to_string(),
            kind: ResourceKind::Table,
            name: "blocked".to_string(),
        },
    });
    catalog
        .preprovision_first_administrator(administrator, 1)
        .await
        .unwrap();

    let base_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 1;
    let audit_window_end_ms = base_ms + 300_000;
    let audit_scope = PolicyScope::AuditRange {
        from_ms: Some(base_ms),
        to_ms: Some(audit_window_end_ms),
    };
    let mut auditor = scram_principal();
    auditor.id = PrincipalId(2);
    auditor.display_name = "Scoped audit reader".to_string();
    auditor.preset_grants = vec![ScopedPresetGrant {
        preset: PermissionPreset::Administrator,
        scope: audit_scope.clone(),
    }];
    let LoginMethod::ScramSha256 { username, .. } = &mut auditor.login_methods[0] else {
        unreachable!("scram_principal creates a SCRAM login")
    };
    *username = "auditor".to_string();

    let audit_policy = serde_json::to_string(&(
        auditor.enabled,
        &auditor.preset_grants,
        &auditor.scoped_grants,
    ))
    .unwrap();
    let grant_audit = SecurityAuditRecord {
        format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "audit-reader-grant".to_string(),
        timestamp_ms: base_ms - 1,
        actor: AuditIdentity::Verified("principal:1".to_string()),
        target: AuditField::Known("principal:2".to_string()),
        scope: AuditField::Known("cluster".to_string()),
        action: SecurityAuditClass::GrantChange,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::NotApplicable,
        operation_id: AuditField::Known("2".to_string()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::Known(format!("revision=1;policy={audit_policy}")),
    };
    catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: auditor,
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: grant_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let event_a = SecurityAuditRecord {
        format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "a".to_string(),
        timestamp_ms: base_ms,
        actor: AuditIdentity::Unverified("presented:alice".to_string()),
        target: AuditField::Unknown,
        scope: AuditField::Known("listener".to_string()),
        action: SecurityAuditClass::AuthenticationFailure,
        outcome: SecurityAuditOutcome::Failure,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::NotApplicable,
        policy_revision: AuditField::Unknown,
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    let event_z = SecurityAuditRecord {
        format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "z".to_string(),
        timestamp_ms: base_ms,
        actor: AuditIdentity::Unknown,
        target: AuditField::NotApplicable,
        scope: AuditField::Known("node:7".to_string()),
        action: SecurityAuditClass::NodeIdentityRejection,
        outcome: SecurityAuditOutcome::Denied,
        correlation: AuditCorrelation::Unknown,
        operation_id: AuditField::NotApplicable,
        policy_revision: AuditField::NotApplicable,
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    let excluded_event = SecurityAuditRecord {
        format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "outside-range".to_string(),
        timestamp_ms: audit_window_end_ms + 1,
        actor: AuditIdentity::Verified("principal:1".to_string()),
        target: AuditField::Known("principal:2".to_string()),
        scope: AuditField::Known("cluster".to_string()),
        action: SecurityAuditClass::AdminOperationCompletion,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::NotApplicable,
        operation_id: AuditField::Known("3".to_string()),
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    catalog
        .commit_txn(
            CatalogTxn::new(
                3,
                3,
                vec![
                    CatalogMutation::AppendSecurityAudit(event_z.clone()),
                    CatalogMutation::AppendSecurityAudit(event_a.clone()),
                    CatalogMutation::AppendSecurityAudit(excluded_event.clone()),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let initial_audit = catalog.list_security_audit_records().await.unwrap();

    let tls_key = rcgen::KeyPair::generate().unwrap();
    let tls_cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&tls_key)
        .unwrap();
    let cert_path = temp.path().join("gateway.pem");
    let key_path = temp.path().join("gateway-key.pem");
    std::fs::write(&cert_path, tls_cert.pem()).unwrap();
    std::fs::write(&key_path, tls_key.serialize_pem()).unwrap();

    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "scram",
        None,
        "",
        "",
        "",
    );
    wait_for_gateway(&mut process.0, address).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=auditor password=pencil dbname=test sslmode=disable",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("the normal PostgreSQL client should authenticate to the release process");
    let connection_task = tokio::spawn(connection);

    let page_one = simple_query_rows(
        &client
            .simple_query(&format!(
                "SHOW SECURITY AUDIT FROM {base_ms} TO {audit_window_end_ms} LIMIT 1"
            ))
            .await
            .unwrap(),
    );
    assert_eq!(
        page_one,
        vec![vec![
            Some("1".to_string()),
            Some("a".to_string()),
            Some(base_ms.to_string()),
            Some("unverified:presented:alice".to_string()),
            Some("unknown".to_string()),
            Some("known:listener".to_string()),
            Some("authentication_failure".to_string()),
            Some("failure".to_string()),
            Some("unknown".to_string()),
            Some("not_applicable".to_string()),
            Some("unknown".to_string()),
            Some("not_applicable".to_string()),
            Some("not_applicable".to_string()),
            Some(format!("{base_ms}:YQ")),
        ]]
    );

    let denied = client
        .simple_query("CREATE TABLE blocked (id BIGINT)")
        .await
        .unwrap_err();
    assert_eq!(denied.as_db_error().unwrap().code().code(), "42501");
    assert_eq!(
        denied.as_db_error().unwrap().message(),
        "[RS-2401] auth.permission_denied: principal 'auditor' is not authorized for action 'TableCreate' on public.blocked. next_steps: Request an authorized scoped grant, then retry."
    );
    let (admin_client, admin_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=pencil dbname=test sslmode=disable",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("the preprovisioned administrator should authenticate to the release process");
    let admin_connection_task = tokio::spawn(admin_connection);
    let authorized_create = admin_client
        .simple_query("CREATE TABLE blocked (id BIGINT)")
        .await
        .unwrap();
    assert!(matches!(
        authorized_create.as_slice(),
        [tokio_postgres::SimpleQueryMessage::CommandComplete(0)]
    ));
    drop(admin_client);
    admin_connection_task.abort();

    let page_two = simple_query_rows(
        &client
            .simple_query(&format!(
                "SHOW SECURITY AUDIT FROM {base_ms} TO {audit_window_end_ms} AFTER {base_ms}:YQ LIMIT 1"
            ))
            .await
            .unwrap(),
    );
    assert_eq!(
        page_two,
        vec![vec![
            Some("1".to_string()),
            Some("z".to_string()),
            Some(base_ms.to_string()),
            Some("unknown".to_string()),
            Some("not_applicable".to_string()),
            Some("known:node:7".to_string()),
            Some("node_identity_rejection".to_string()),
            Some("denied".to_string()),
            Some("unknown".to_string()),
            Some("not_applicable".to_string()),
            Some("not_applicable".to_string()),
            Some("not_applicable".to_string()),
            Some("not_applicable".to_string()),
            Some(format!("{base_ms}:eg")),
        ]]
    );

    let page_three = simple_query_rows(
        &client
            .simple_query(&format!(
                "SHOW SECURITY AUDIT FROM {base_ms} TO {audit_window_end_ms} AFTER {base_ms}:eg LIMIT 10"
            ))
            .await
            .unwrap(),
    );
    assert_eq!(page_three.len(), 3);
    assert!(page_three
        .iter()
        .all(|row| row.len() == 14 && row[0].as_deref() == Some("1")));
    let mut page_three_actions: Vec<_> = page_three
        .iter()
        .map(|row| row[6].as_deref().unwrap())
        .collect();
    page_three_actions.sort_unstable();
    assert_eq!(
        page_three_actions,
        vec![
            "authentication_success",
            "authentication_success",
            "authorization_denial"
        ]
    );
    let live_denial = page_three
        .iter()
        .find(|row| row[4].as_deref() == Some("known:public.blocked"))
        .unwrap();
    let reader_login = page_three
        .iter()
        .find(|row| {
            row[3].as_deref() == Some("verified:scram:auditor")
                && row[6].as_deref() == Some("authentication_success")
        })
        .unwrap();
    assert_eq!(live_denial[3].as_deref(), Some("verified:scram:auditor"));
    assert_eq!(
        live_denial[5].as_deref(),
        Some(
            format!(
                "known:{}",
                serde_json::to_string(&PolicyScope::Resource {
                    namespace: "public".to_string(),
                    kind: ResourceKind::Table,
                    name: "blocked".to_string(),
                })
                .unwrap()
            )
            .as_str()
        )
    );
    assert_eq!(live_denial[6].as_deref(), Some("authorization_denial"));
    assert_eq!(live_denial[7].as_deref(), Some("denied"));
    assert_eq!(live_denial[9].as_deref(), Some("not_applicable"));
    assert_eq!(live_denial[10].as_deref(), Some("known:1"));
    assert_eq!(live_denial[11].as_deref(), Some("not_applicable"));
    assert_eq!(live_denial[12].as_deref(), Some("not_applicable"));
    assert!(matches!(
        live_denial[8].as_deref(),
        Some(correlation) if correlation.starts_with("observed:") && correlation.len() > "observed:".len()
    ));
    assert_eq!(live_denial[8], reader_login[8]);
    let denial_event_id = live_denial[1].as_deref().unwrap();
    assert_eq!(denial_event_id.len(), "gateway-authz-".len() + 32);
    assert!(denial_event_id["gateway-authz-".len()..]
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit()));
    let denial_timestamp = live_denial[2].as_deref().unwrap();
    assert!((base_ms..=audit_window_end_ms).contains(&denial_timestamp.parse().unwrap()));
    assert!(live_denial[13]
        .as_deref()
        .unwrap()
        .starts_with(&format!("{denial_timestamp}:")));
    let page_three_keys: Vec<_> = page_three
        .iter()
        .map(|row| (row[2].as_deref().unwrap(), row[1].as_deref().unwrap()))
        .collect();
    assert!(page_three_keys
        .windows(2)
        .all(|window| window[0] <= window[1]));
    let last_cursor = page_three.last().unwrap()[13].as_deref().unwrap();
    let page_four = simple_query_rows(
        &client
            .simple_query(&format!(
                "SHOW SECURITY AUDIT FROM {base_ms} TO {audit_window_end_ms} AFTER {last_cursor} LIMIT 1"
            ))
            .await
            .unwrap(),
    );
    assert!(page_four.is_empty());
    let out_of_scope = client
        .simple_query("SHOW SECURITY AUDIT LIMIT 1")
        .await
        .unwrap_err();
    assert_eq!(out_of_scope.as_db_error().unwrap().code().code(), "42501");
    assert_eq!(
        out_of_scope.as_db_error().unwrap().message(),
        "[RS-2401] auth.permission_denied: principal 'auditor' is not authorized for action 'SecurityAuditRead' on the requested security audit range. next_steps: Request an authorized scoped grant, then retry."
    );
    assert_eq!(
        simple_query_rows(
            &client
                .simple_query(&format!(
                    "SHOW SECURITY AUDIT FROM {base_ms} TO {audit_window_end_ms} LIMIT 1"
                ))
                .await
                .unwrap()
        ),
        page_one
    );

    drop(client);
    connection_task.abort();
    process.0.kill().unwrap();
    process.0.wait().unwrap();

    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let recovered = DurableCatalogStore::recover(store, "gateway")
        .await
        .unwrap();
    let records = recovered.list_security_audit_records().await.unwrap();
    assert_eq!(&records[..initial_audit.len()], initial_audit.as_slice());
    assert_eq!(records.len(), initial_audit.len() + 4);
    let login = &records[initial_audit.len()];
    assert_eq!(login.action, SecurityAuditClass::AuthenticationSuccess);
    assert_eq!(login.outcome, SecurityAuditOutcome::Success);
    assert_eq!(
        login.actor,
        AuditIdentity::Verified("scram:auditor".to_string())
    );
    assert_eq!(login.target, AuditField::Known("principal:2".to_string()));
    let table_denial = &records[initial_audit.len() + 1];
    assert!(table_denial.event_id.starts_with("gateway-authz-"));
    assert!(table_denial.timestamp_ms > 0);
    assert_eq!(
        table_denial.actor,
        AuditIdentity::Verified("scram:auditor".to_string())
    );
    assert_eq!(
        table_denial.target,
        AuditField::Known("public.blocked".to_string())
    );
    assert_eq!(
        table_denial.scope,
        AuditField::Known(
            serde_json::to_string(&PolicyScope::Resource {
                namespace: "public".to_string(),
                kind: ResourceKind::Table,
                name: "blocked".to_string(),
            })
            .unwrap()
        )
    );
    assert_eq!(table_denial.action, SecurityAuditClass::AuthorizationDenial);
    assert_eq!(table_denial.outcome, SecurityAuditOutcome::Denied);
    assert!(matches!(
        &table_denial.correlation,
        AuditCorrelation::Observed(value) if !value.is_empty()
    ));
    assert_eq!(table_denial.policy_revision, AuditField::Known(1));
    assert_eq!(table_denial.before_state, AuditField::NotApplicable);
    assert_eq!(table_denial.after_state, AuditField::NotApplicable);

    let admin_login = &records[initial_audit.len() + 2];
    assert_eq!(
        admin_login.action,
        SecurityAuditClass::AuthenticationSuccess
    );
    assert_eq!(admin_login.outcome, SecurityAuditOutcome::Success);
    assert_eq!(
        admin_login.actor,
        AuditIdentity::Verified("scram:alice".to_string())
    );
    assert_eq!(
        admin_login.target,
        AuditField::Known("principal:1".to_string())
    );

    let audit_denial = &records[initial_audit.len() + 3];
    assert!(audit_denial.event_id.starts_with("gateway-authz-"));
    assert!(audit_denial.timestamp_ms > 0);
    assert_eq!(
        audit_denial.actor,
        AuditIdentity::Verified("scram:auditor".to_string())
    );
    assert_eq!(
        audit_denial.target,
        AuditField::Known("security_audit".to_string())
    );
    assert_eq!(
        audit_denial.scope,
        AuditField::Known(
            serde_json::to_string(&PolicyScope::AuditRange {
                from_ms: None,
                to_ms: None,
            })
            .unwrap()
        )
    );
    assert_eq!(audit_denial.action, SecurityAuditClass::AuthorizationDenial);
    assert_eq!(audit_denial.outcome, SecurityAuditOutcome::Denied);
    assert!(matches!(
        &audit_denial.correlation,
        AuditCorrelation::Observed(value) if !value.is_empty()
    ));
    assert_eq!(audit_denial.operation_id, AuditField::NotApplicable);
    assert_eq!(audit_denial.policy_revision, AuditField::Known(1));
    assert_eq!(audit_denial.before_state, AuditField::NotApplicable);
    assert_eq!(audit_denial.after_state, AuditField::NotApplicable);
    assert!(!serde_json::to_string(&records).unwrap().contains("pencil"));
}

#[tokio::test]
async fn management_storage_outage_rejects_grant_change_across_process_restart() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let bucket = "release-audit-outage";
    let (minio_container, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO is required for release-process audit-storage qualification");
    let minio_config = Some((minio_port, bucket));
    let temp = tempfile::tempdir().unwrap();
    let pki = rockstream_test_support::pki::TestPki::generate();
    let actor = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "Release audit outage target".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Service {
            service_id: "release-audit-outage-target".to_owned(),
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Builder,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: Vec::new(),
    };
    let seed_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let storage = temp.path().join("control-storage");
    let mut seed_process = start_delegated_control_process(
        temp.path(),
        &storage,
        seed_address,
        &pki,
        Some(actor),
        false,
        minio_config,
    )
    .await;
    wait_for_gateway(&mut seed_process.0, seed_address).await;
    seed_process.0.kill().unwrap();
    seed_process.0.wait().unwrap();

    let catalog =
        DurableCatalogStore::recover(minio_runtime_object_store(minio_port, bucket), "gateway")
            .await
            .unwrap();
    let mut principals_before = catalog.list_security_principals().await.unwrap();
    principals_before.sort_by_key(|principal| principal.id);
    let audit_before = catalog.list_security_audit_records().await.unwrap();
    let revision_before = catalog.get_revision().await;

    let address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_control_process_with_operator(
        temp.path(),
        &storage,
        address,
        management_address,
        &pki,
        minio_config,
        false,
        None,
        false,
        false,
    )
    .await;
    wait_for_gateway(&mut process.0, address).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut management = loop {
        match try_management_client(management_address, &pki).await {
            Ok(client) => break client,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("management endpoint did not start: {error}"),
        }
    };
    let listed = management
        .list_security_principals(ListSecurityPrincipalsRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            page_size: 10,
            page_token: String::new(),
        })
        .await
        .expect("authenticated management client should list principals")
        .into_inner();
    assert_eq!(
        listed,
        rockstream_management_proto::v1::ListSecurityPrincipalsResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            principals: [
                (
                    1,
                    "release control test administrator",
                    "administrator",
                    PermissionPreset::Administrator,
                ),
                (
                    2,
                    "Release audit outage target",
                    "builder",
                    PermissionPreset::Builder,
                ),
            ]
            .into_iter()
            .map(|(principal_id, display_name, preset_name, preset)| {
                let scope_json = "\"cluster\"".to_owned();
                rockstream_management_proto::v1::SecurityPrincipal {
                    principal_id,
                    display_name: display_name.to_owned(),
                    enabled: true,
                    revision: 1,
                    preset_grants: vec![rockstream_management_proto::v1::SecurityPresetGrant {
                        preset: preset_name.to_owned(),
                        scope_json: scope_json.clone(),
                    }],
                    scoped_grants: Vec::new(),
                    effective_permissions: preset
                        .actions()
                        .iter()
                        .map(
                            |action| rockstream_management_proto::v1::SecurityActionGrant {
                                action: serde_json::to_value(action)
                                    .unwrap()
                                    .as_str()
                                    .unwrap()
                                    .to_owned(),
                                scope_json: scope_json.clone(),
                            },
                        )
                        .collect(),
                }
            })
            .collect(),
            next_page_token: String::new(),
        }
    );

    catalog.refresh().await.unwrap();
    let mut principals_authenticated = catalog.list_security_principals().await.unwrap();
    principals_authenticated.sort_by_key(|principal| principal.id);
    let audit_authenticated = catalog.list_security_audit_records().await.unwrap();
    let revision_authenticated = catalog.get_revision().await;
    assert_eq!(principals_authenticated, principals_before);
    assert_eq!(audit_authenticated.len(), audit_before.len() + 1);
    assert_eq!(
        &audit_authenticated[..audit_before.len()],
        audit_before.as_slice()
    );
    assert_eq!(revision_authenticated, revision_before + 1);

    let operation_id = uuid::Uuid::new_v4().to_string();
    minio_container.stop().await.unwrap();
    let error = management
        .set_security_principal_grants(SetSecurityPrincipalGrantsRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            principal_id: 2,
            expected_revision: 1,
            expected_grantor_revision: 1,
            idempotency_key: operation_id.clone(),
            preset_grants: Vec::new(),
            scoped_grants: Vec::new(),
        })
        .await
        .expect_err("grant mutation must fail while durable policy storage is unavailable");
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "management security policy is unavailable");
    process.0.kill().unwrap();
    process.0.wait().unwrap();

    minio_container.start().await.unwrap();
    let minio_port = minio_container.get_host_port_ipv4(9000).await.unwrap();
    let minio_config = Some((minio_port, bucket));
    let deadline = Instant::now() + Duration::from_secs(30);
    let recovered = loop {
        match DurableCatalogStore::recover(
            minio_runtime_object_store(minio_port, bucket),
            "gateway",
        )
        .await
        {
            Ok(catalog) => break catalog,
            Err(_error) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("MinIO did not recover its durable catalog: {error}"),
        }
    };
    let mut principals_after = recovered.list_security_principals().await.unwrap();
    principals_after.sort_by_key(|principal| principal.id);
    assert_eq!(principals_after, principals_authenticated);
    assert_eq!(
        recovered.list_security_audit_records().await.unwrap(),
        audit_authenticated
    );
    assert_eq!(recovered.get_revision().await, revision_authenticated);

    let mut recovered_process = start_control_process_with_operator(
        temp.path(),
        &storage,
        address,
        management_address,
        &pki,
        minio_config,
        false,
        None,
        false,
        false,
    )
    .await;
    wait_for_gateway(&mut recovered_process.0, address).await;
    let recovery_deadline = Instant::now() + Duration::from_secs(30);
    let mut recovered_management = loop {
        match try_management_client(management_address, &pki).await {
            Ok(client) => break client,
            Err(_) if Instant::now() < recovery_deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => {
                panic!("management endpoint did not recover after MinIO returns: {error}")
            }
        }
    };
    let missing = recovered_management
        .get_operation(GetOperationRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation_id: operation_id.clone(),
        })
        .await
        .expect_err("failed grant mutation must not leave an accepted operation");
    assert_eq!(missing.code(), tonic::Code::NotFound);
    assert_eq!(
        missing.message(),
        format!("operation {operation_id} not found")
    );
    recovered.refresh().await.unwrap();
    let mut principals_after_restart = recovered.list_security_principals().await.unwrap();
    principals_after_restart.sort_by_key(|principal| principal.id);
    assert_eq!(principals_after_restart, principals_authenticated);
    let audit_after_restart = recovered.list_security_audit_records().await.unwrap();
    assert_eq!(
        &audit_after_restart[..audit_authenticated.len()],
        audit_authenticated.as_slice()
    );
    assert_eq!(audit_after_restart.len(), audit_authenticated.len() + 1);
    let authentication = audit_after_restart.last().unwrap();
    let authentication_id = authentication
        .event_id
        .strip_prefix("management-authn-")
        .expect("recovered management authentication audit event");
    assert!(uuid::Uuid::parse_str(authentication_id).is_ok());
    assert!(authentication.timestamp_ms > 0);
    assert_eq!(
        &SecurityAuditRecord {
            event_id: String::new(),
            timestamp_ms: 0,
            ..authentication.clone()
        },
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: String::new(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("management-api".to_owned()),
            scope: AuditField::NotApplicable,
            action: SecurityAuditClass::AuthenticationSuccess,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
    assert_eq!(recovered.get_revision().await, revision_authenticated + 1);
    recovered_process.0.kill().unwrap();
    recovered_process.0.wait().unwrap();
}

#[tokio::test]
async fn oidc_password_auth_runs_in_release_process_and_audits_after_restart() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let shard_dir = storage.join("gateway-shard");
    let administrator = oidc_bootstrap_administrator();
    let reader = oidc_principal();
    let principal_file = temp.path().join("administrator.json");
    std::fs::write(&principal_file, serde_json::to_vec(&administrator).unwrap()).unwrap();
    let run_bootstrap = || {
        Command::new(env!("CARGO_BIN_EXE_rockstream"))
            .arg("--json")
            .arg("security")
            .arg("preprovision-admin")
            .arg("--storage")
            .arg(&storage)
            .arg("--layout")
            .arg("gateway")
            .arg("--principal-file")
            .arg(&principal_file)
            .arg("--operation-id")
            .arg("1")
            .arg("--yes")
            .output()
            .unwrap()
    };
    let expected_output = "{\n  \"principal_id\": 1,\n  \"catalog_revision\": 1\n}\n";
    let first_bootstrap = run_bootstrap();
    assert!(
        first_bootstrap.status.success(),
        "{}",
        String::from_utf8_lossy(&first_bootstrap.stderr)
    );
    assert_eq!(
        String::from_utf8(first_bootstrap.stdout).unwrap(),
        expected_output
    );
    let retry_bootstrap = run_bootstrap();
    assert!(
        retry_bootstrap.status.success(),
        "{}",
        String::from_utf8_lossy(&retry_bootstrap.stderr)
    );
    assert_eq!(
        String::from_utf8(retry_bootstrap.stdout).unwrap(),
        expected_output
    );

    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let durable = DurableCatalogStore::recover(store, "gateway")
        .await
        .unwrap();
    assert_eq!(
        durable
            .get_security_principal(administrator.id)
            .await
            .unwrap(),
        Some(administrator.clone())
    );
    let audit_records = durable.list_security_audit_records().await.unwrap();
    let bootstrap_audit =
        expected_bootstrap_audit(&administrator, 1, audit_records[0].timestamp_ms);
    assert!(bootstrap_audit.timestamp_ms > 0);
    assert_eq!(audit_records, vec![bootstrap_audit]);
    let reader_policy =
        serde_json::to_string(&(reader.enabled, &reader.preset_grants, &reader.scoped_grants))
            .unwrap();
    let grant_audit = SecurityAuditRecord {
        format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "oidc-reader-grant".to_string(),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
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
                    audit: grant_audit,
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    drop(durable);

    let mut tls_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    tls_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let tls_key = rcgen::KeyPair::generate().unwrap();
    let tls_cert = tls_params.self_signed(&tls_key).unwrap();
    let cert_path = temp.path().join("gateway.pem");
    let key_path = temp.path().join("gateway-key.pem");
    std::fs::write(&cert_path, tls_cert.pem()).unwrap();
    std::fs::write(&key_path, tls_key.serialize_pem()).unwrap();

    let jwks_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let jwks_addr = jwks_listener.local_addr().unwrap();
    let jwks_url = format!("http://{jwks_addr}/jwks");
    let jwks_task = tokio::spawn(serve_jwks(jwks_listener, 3));

    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "oidc",
        None,
        "https://issuer.example",
        "rockstream",
        &jwks_url,
    );
    wait_for_gateway(&mut process.0, address).await;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(tls_cert.der().clone()).unwrap();
    let client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut valid = tokio_postgres::Config::new();
    valid
        .host("127.0.0.1")
        .port(address.port())
        .user("alice")
        .password(VALID_TOKEN)
        .dbname("test")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    let (client, connection) = valid
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
            client_tls.clone(),
        ))
        .await
        .expect("the normal PostgreSQL client should authenticate through the release process");
    let connection_task = tokio::spawn(connection);

    let denial = match client
        .execute("CREATE TABLE blocked (id BIGINT)", &[])
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("the data-reader OIDC principal created a table"),
    };
    assert_eq!(denial.as_db_error().unwrap().code().code(), "42501");
    assert_eq!(
        denial.as_db_error().unwrap().message(),
        "[RS-2401] auth.permission_denied: principal 'alice' is not authorized for action 'TableCreate' on public.blocked. next_steps: Request an authorized scoped grant, then retry."
    );
    let rows = client.query("SELECT 1 AS value", &[]).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>(),
        vec![1i64]
    );
    let absent_table = match client.query("SELECT * FROM blocked", &[]).await {
        Err(error) => error,
        Ok(_) => panic!("the denied table creation left a visible table"),
    };
    assert_eq!(absent_table.as_db_error().unwrap().code().code(), "42P01");

    let mut expired = tokio_postgres::Config::new();
    expired
        .host("127.0.0.1")
        .port(address.port())
        .user("alice")
        .password(EXPIRED_TOKEN)
        .dbname("test")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    let expired_error = match expired
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
            client_tls.clone(),
        ))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("the release process accepted an expired OIDC token"),
    };
    assert_eq!(expired_error.as_db_error().unwrap().code().code(), "28000");
    assert_eq!(
        expired_error.as_db_error().unwrap().message(),
        "[RS-2400] auth.unauthenticated: JWT token expired. next_steps: Provide valid credentials (Bearer token or mTLS certificate)"
    );

    let mut plaintext = tokio_postgres::Config::new();
    plaintext
        .host("127.0.0.1")
        .port(address.port())
        .user("alice")
        .password(VALID_TOKEN)
        .dbname("test")
        .ssl_mode(tokio_postgres::config::SslMode::Disable);
    let plaintext_error = match plaintext.connect(tokio_postgres::NoTls).await {
        Err(error) => error,
        Ok(_) => panic!("the release process accepted OIDC credentials without TLS"),
    };
    assert_eq!(
        plaintext_error.as_db_error().unwrap().code().code(),
        "28000"
    );
    assert_eq!(
        plaintext_error.as_db_error().unwrap().message(),
        "[RS-2400] auth.unauthenticated: OIDC requires a TLS connection. next_steps: Connect to the gateway with TLS enabled"
    );

    let mut invalid = tokio_postgres::Config::new();
    invalid
        .host("127.0.0.1")
        .port(address.port())
        .user("alice")
        .password("bad-token")
        .dbname("test")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    let error = match invalid
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
            client_tls.clone(),
        ))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("the release process accepted an invalid OIDC token"),
    };
    assert_eq!(error.as_db_error().unwrap().code().code(), "28000");
    assert_eq!(
        error.as_db_error().unwrap().message(),
        "[RS-2400] auth.unauthenticated: malformed JWT: expected 3 parts. next_steps: Provide valid credentials (Bearer token or mTLS certificate)"
    );
    drop(client);
    connection_task.abort();
    process.0.kill().unwrap();
    process.0.wait().unwrap();

    let wrong_issuer_address: std::net::SocketAddr =
        format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut wrong_issuer_process = start_gateway_process(
        temp.path(),
        &storage,
        wrong_issuer_address,
        &cert_path,
        &key_path,
        "oidc",
        None,
        "https://wrong-issuer.example",
        "rockstream",
        &jwks_url,
    );
    wait_for_gateway(&mut wrong_issuer_process.0, wrong_issuer_address).await;
    let wrong_issuer_error =
        oidc_authentication_error(wrong_issuer_address.port(), VALID_TOKEN, &client_tls).await;
    assert_eq!(
        wrong_issuer_error.as_db_error().unwrap().code().code(),
        "28000"
    );
    assert_eq!(
        wrong_issuer_error.as_db_error().unwrap().message(),
        "[RS-2400] auth.unauthenticated: OIDC issuer claim does not match configuration. next_steps: Provide valid credentials (Bearer token or mTLS certificate)"
    );
    wrong_issuer_process.0.kill().unwrap();
    wrong_issuer_process.0.wait().unwrap();

    let wrong_audience_address: std::net::SocketAddr =
        format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut wrong_audience_process = start_gateway_process(
        temp.path(),
        &storage,
        wrong_audience_address,
        &cert_path,
        &key_path,
        "oidc",
        None,
        "https://issuer.example",
        "wrong-audience",
        &jwks_url,
    );
    wait_for_gateway(&mut wrong_audience_process.0, wrong_audience_address).await;
    let wrong_audience_error =
        oidc_authentication_error(wrong_audience_address.port(), VALID_TOKEN, &client_tls).await;
    assert_eq!(
        wrong_audience_error.as_db_error().unwrap().code().code(),
        "28000"
    );
    assert_eq!(
        wrong_audience_error.as_db_error().unwrap().message(),
        "[RS-2400] auth.unauthenticated: OIDC audience claim does not match configuration. next_steps: Provide valid credentials (Bearer token or mTLS certificate)"
    );
    wrong_audience_process.0.kill().unwrap();
    wrong_audience_process.0.wait().unwrap();
    jwks_task.await.unwrap();

    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let recovered = DurableCatalogStore::recover(store, "gateway")
        .await
        .unwrap();
    let records = recovered.list_security_audit_records().await.unwrap();
    assert_eq!(records.len(), 9);
    assert_eq!(records[0].action, SecurityAuditClass::GrantChange);
    assert_eq!(records[0].outcome, SecurityAuditOutcome::Success);
    assert_eq!(records[1].action, SecurityAuditClass::GrantChange);
    assert_eq!(records[1].outcome, SecurityAuditOutcome::Success);
    assert_eq!(records[2].action, SecurityAuditClass::AuthenticationSuccess);
    assert_eq!(records[3].action, SecurityAuditClass::AuthorizationDenial);
    assert_eq!(records[3].outcome, SecurityAuditOutcome::Denied);
    assert_eq!(
        records[3].actor,
        AuditIdentity::Verified(
            rockstream_gateway::auth::Principal::Oidc {
                issuer: "https://issuer.example".to_string(),
                sub: "alice".to_string(),
            }
            .actor()
        )
    );
    assert_eq!(
        records[2].actor,
        AuditIdentity::Verified(
            rockstream_gateway::auth::Principal::Oidc {
                issuer: "https://issuer.example".to_string(),
                sub: "alice".to_string(),
            }
            .actor()
        )
    );
    assert_eq!(
        records[2].target,
        AuditField::Known("principal:2".to_string())
    );
    for record in &records[4..] {
        assert_eq!(record.action, SecurityAuditClass::AuthenticationFailure);
        assert_eq!(record.outcome, SecurityAuditOutcome::Failure);
        assert_eq!(record.actor, AuditIdentity::Unknown);
        assert_eq!(record.target, AuditField::Unknown);
    }
    assert!(!serde_json::to_string(&records)
        .unwrap()
        .contains("bad-token"));
}

#[tokio::test]
async fn oidc_startup_rejects_unavailable_jwks_before_listening() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("oidc-startup-storage");

    let mut tls_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    tls_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let tls_key = rcgen::KeyPair::generate().unwrap();
    let tls_cert = tls_params.self_signed(&tls_key).unwrap();
    let cert_path = temp.path().join("gateway.pem");
    let key_path = temp.path().join("gateway-key.pem");
    std::fs::write(&cert_path, tls_cert.pem()).unwrap();
    std::fs::write(&key_path, tls_key.serialize_pem()).unwrap();

    let jwks_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let jwks_address = jwks_listener.local_addr().unwrap();
    drop(jwks_listener);
    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    assert_ne!(address.port(), jwks_address.port());

    let jwks_url = format!("http://{jwks_address}/jwks");
    let mut process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "oidc",
        None,
        "https://issuer.example",
        "rockstream",
        &jwks_url,
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            break status;
        }
        if tokio::net::TcpStream::connect(address).await.is_ok() || Instant::now() >= deadline {
            process.0.kill().unwrap();
            let status = process.0.wait().unwrap();
            let mut stderr = String::new();
            process
                .0
                .stderr
                .as_mut()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!("gateway listened or did not reject unavailable JWKS ({status}): {stderr}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut stderr = String::new();
    process
        .0
        .stderr
        .as_mut()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert_eq!(status.code(), Some(1), "unexpected release process status");
    let correlation_id = stderr
        .split("correlation_id=")
        .nth(1)
        .and_then(|value| value.split_whitespace().next())
        .expect("startup diagnostic correlation id");
    let normalized = stderr.replace(correlation_id, "<correlation_id>");
    assert_eq!(
        normalized.trim_end(),
        "[RS-0003] storage.unavailable: Storage unavailable (detail=OIDC verifier initialization failed: [RS-2400] auth.unauthenticated: OIDC JWKS request failed) (correlation_id=<correlation_id> context=detail=OIDC verifier initialization failed: [RS-2400] auth.unauthenticated: OIDC JWKS request failed) next_steps: Verify storage directory permissions and disk space.\n  next steps: Restore access to the configured JWKS endpoint before starting the gateway."
    );
    assert!(
        !stderr.contains(&jwks_url),
        "diagnostic exposed the JWKS URL"
    );
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn release_process_rejects_scram_with_empty_durable_credentials_before_listening() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("empty-scram-storage");

    let mut tls_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    tls_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let tls_key = rcgen::KeyPair::generate().unwrap();
    let tls_cert = tls_params.self_signed(&tls_key).unwrap();
    let cert_path = temp.path().join("gateway.pem");
    let key_path = temp.path().join("gateway-key.pem");
    std::fs::write(&cert_path, tls_cert.pem()).unwrap();
    std::fs::write(&key_path, tls_key.serialize_pem()).unwrap();

    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "scram",
        None,
        "",
        "",
        "",
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = process.0.try_wait().unwrap() {
            break status;
        }
        if tokio::net::TcpStream::connect(address).await.is_ok() || Instant::now() >= deadline {
            process.0.kill().unwrap();
            let status = process.0.wait().unwrap();
            let mut stderr = String::new();
            process
                .0
                .stderr
                .as_mut()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            panic!(
                "release process opened a listener or failed to reject empty SCRAM credentials ({status}): {stderr}"
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut stderr = String::new();
    process
        .0
        .stderr
        .as_mut()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert_eq!(
        status.code(),
        Some(1),
        "unexpected release process status: {stderr}"
    );
    let correlation_id = stderr
        .split("correlation_id=")
        .nth(1)
        .and_then(|value| value.split_whitespace().next())
        .expect("startup diagnostic correlation id");
    let normalized = stderr.replace(correlation_id, "<correlation_id>");
    assert_eq!(
        normalized.trim_end(),
        "[RS-0002] config.invalid: Configuration error (detail=gateway startup rejected: SCRAM requires an available durable catalog with an enabled SCRAM login) (correlation_id=<correlation_id> context=detail=gateway startup rejected: SCRAM requires an available durable catalog with an enabled SCRAM login) next_steps: Check configuration file and CLI flags.\n  next steps: Provision an enabled SCRAM login in the recovered durable catalog before starting the gateway."
    );
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

#[tokio::test]
async fn scram_failures_and_service_only_login_rejection_run_in_release_process() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let shard_dir = storage.join("gateway-shard");
    std::fs::create_dir_all(&shard_dir).unwrap();
    let principal = scram_principal();
    let service_scope = PolicyScope::Secret {
        namespace: "public".to_owned(),
        name: "kafka".to_owned(),
        consumer: Some(PrincipalId(2)),
    };
    let service_principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(2),
        display_name: "Release service-only consumer".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Service {
            service_id: "release-service-only".to_owned(),
        }],
        preset_grants: vec![],
        scoped_grants: vec![ScopedActionGrant {
            action: SecurityAction::SecretUse,
            scope: service_scope,
        }],
    };
    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let catalog = DurableCatalogStore::new(store, "gateway");
    catalog
        .preprovision_first_administrator(principal.clone(), 1)
        .await
        .unwrap();
    let service_policy = serde_json::to_string(&(
        service_principal.enabled,
        &service_principal.preset_grants,
        &service_principal.scoped_grants,
    ))
    .unwrap();
    let grant_audit = SecurityAuditRecord {
        format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
        event_id: "release-service-only-grant".to_owned(),
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
        after_state: AuditField::Known(format!("revision=1;policy={service_policy}")),
    };
    catalog
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![CatalogMutation::PutSecurityPrincipal {
                    principal: service_principal.clone(),
                    expected_revision: None,
                    grantor_revision: Some((PrincipalId(1), 1)),
                    audit: grant_audit.clone(),
                }],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    drop(catalog);

    let tls_key = rcgen::KeyPair::generate().unwrap();
    let tls_cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&tls_key)
        .unwrap();
    let cert_path = temp.path().join("gateway.pem");
    let key_path = temp.path().join("gateway-key.pem");
    std::fs::write(&cert_path, tls_cert.pem()).unwrap();
    std::fs::write(&key_path, tls_key.serialize_pem()).unwrap();

    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "scram",
        None,
        "",
        "",
        "",
    );
    wait_for_gateway(&mut process.0, address).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=pencil dbname=test sslmode=disable",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("the release SCRAM process should accept a valid login");
    let connection_task = tokio::spawn(connection);
    let rows = client.query("SELECT 1 AS value", &[]).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>(),
        vec![1i64]
    );
    drop(client);
    connection_task.abort();

    let wrong_password = match tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=wrong dbname=test sslmode=disable",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("the release SCRAM process accepted a wrong password"),
    };
    assert_eq!(wrong_password.as_db_error().unwrap().code().code(), "28P01");
    assert_eq!(
        wrong_password.as_db_error().unwrap().message(),
        "[RS-2401] auth.invalid_password: password authentication failed for user 'alice'. next_steps: Check password and retry"
    );
    assert_scram_login_rejected(address.port(), "release-service-only").await;

    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut startup = Vec::new();
    startup.extend_from_slice(&196_608u32.to_be_bytes());
    startup.extend_from_slice(b"user\0alice\0database\0test\0\0");
    let startup_length = u32::try_from(startup.len() + 4).unwrap();
    stream
        .write_all(&startup_length.to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&startup).await.unwrap();
    let (tag, sasl) = read_backend_message(&mut stream).await;
    assert_eq!(tag, b'R');
    assert_eq!(&sasl[..4], &10i32.to_be_bytes());
    assert!(sasl[4..].starts_with(b"SCRAM-SHA-256\0"));
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
        .unwrap();
    let client_final =
        format!("c=biws,r={server_nonce},p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
    write_frontend_message(&mut stream, b'p', client_final.as_bytes()).await;
    let (tag, error_payload) = read_backend_message(&mut stream).await;
    assert_eq!(tag, b'E');
    let fields = parse_error_fields(&error_payload);
    assert_eq!(fields.get(&b'C').map(String::as_str), Some("28P01"));
    assert_eq!(
        fields.get(&b'M').map(String::as_str),
        Some(
            "[RS-2401] auth.invalid_password: password authentication failed for user 'alice'. next_steps: Check password and retry"
        )
    );
    let mut byte = [0];
    assert_eq!(stream.read(&mut byte).await.unwrap(), 0);

    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let recovered = DurableCatalogStore::recover(store, "gateway")
        .await
        .unwrap();
    assert_eq!(recovered.list_security_principals().await.unwrap().len(), 2);
    assert_eq!(
        recovered
            .get_security_principal(principal.id)
            .await
            .unwrap(),
        Some(principal.clone())
    );
    assert_eq!(
        recovered
            .get_security_principal(service_principal.id)
            .await
            .unwrap(),
        Some(service_principal.clone())
    );
    let records = recovered.list_security_audit_records().await.unwrap();
    assert_eq!(records.len(), 6);
    assert_eq!(
        records[0],
        expected_bootstrap_audit(&principal, 1, records[0].timestamp_ms)
    );
    assert_eq!(records[1], grant_audit);
    assert_eq!(records[2].action, SecurityAuditClass::AuthenticationSuccess);
    assert_eq!(records[2].outcome, SecurityAuditOutcome::Success);
    assert_eq!(
        records[2].actor,
        AuditIdentity::Verified("scram:alice".to_owned())
    );
    assert_eq!(
        records[2].target,
        AuditField::Known("principal:1".to_owned())
    );
    assert_eq!(records[2].policy_revision, AuditField::Known(1));
    for record in &records[3..] {
        assert_eq!(record.action, SecurityAuditClass::AuthenticationFailure);
        assert_eq!(record.outcome, SecurityAuditOutcome::Failure);
        assert_eq!(record.scope, AuditField::NotApplicable);
        assert!(matches!(
            &record.correlation,
            AuditCorrelation::Observed(id)
                if id.len() == 32 && id.chars().all(|character| character.is_ascii_hexdigit())
        ));
        assert_eq!(record.operation_id, AuditField::NotApplicable);
        assert_eq!(record.before_state, AuditField::NotApplicable);
        assert_eq!(record.after_state, AuditField::NotApplicable);
        assert!(record.event_id.starts_with("gateway-authn-"));
        assert_eq!(record.event_id.len(), "gateway-authn-".len() + 32);
        assert!(record.timestamp_ms > 0);
    }
    for record in [&records[3], &records[5]] {
        assert_eq!(
            record.actor,
            AuditIdentity::Unverified("scram:alice".to_owned())
        );
        assert_eq!(record.target, AuditField::Known("principal:1".to_owned()));
        assert_eq!(record.policy_revision, AuditField::Known(1));
    }
    assert_eq!(
        records[4].actor,
        AuditIdentity::Unverified("scram:release-service-only".to_owned())
    );
    assert_eq!(records[4].target, AuditField::Unknown);
    assert_eq!(records[4].policy_revision, AuditField::Unknown);

    let records_before_restart = records;
    let restarted_address: std::net::SocketAddr =
        format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut restarted_process = start_gateway_process(
        temp.path(),
        &storage,
        restarted_address,
        &cert_path,
        &key_path,
        "scram",
        None,
        "",
        "",
        "",
    );
    wait_for_gateway(&mut restarted_process.0, restarted_address).await;
    assert_scram_login_rejected(restarted_address.port(), "release-service-only").await;
    restarted_process.0.kill().unwrap();
    restarted_process.0.wait().unwrap();

    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let recovered = DurableCatalogStore::recover(store, "gateway")
        .await
        .unwrap();
    assert_eq!(
        recovered
            .get_security_principal(service_principal.id)
            .await
            .unwrap(),
        Some(service_principal)
    );
    let records = recovered.list_security_audit_records().await.unwrap();
    assert_eq!(records.len(), 7);
    assert_eq!(&records[..6], records_before_restart.as_slice());
    let failure = &records[6];
    assert_eq!(failure.action, SecurityAuditClass::AuthenticationFailure);
    assert_eq!(failure.outcome, SecurityAuditOutcome::Failure);
    assert_eq!(
        failure.actor,
        AuditIdentity::Unverified("scram:release-service-only".to_owned())
    );
    assert_eq!(failure.target, AuditField::Unknown);
    assert_eq!(failure.policy_revision, AuditField::Unknown);
    assert_eq!(failure.scope, AuditField::NotApplicable);
    assert_eq!(failure.operation_id, AuditField::NotApplicable);
    assert_eq!(failure.before_state, AuditField::NotApplicable);
    assert_eq!(failure.after_state, AuditField::NotApplicable);
    assert!(matches!(
        &failure.correlation,
        AuditCorrelation::Observed(id)
            if id.len() == 32 && id.chars().all(|character| character.is_ascii_hexdigit())
    ));
    assert!(!serde_json::to_string(&records).unwrap().contains("pencil"));
}

#[tokio::test]
async fn pgoutput_secret_rotation_keeps_release_worker_alive_and_recovers_results() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the PostgreSQL secret-rotation release proof"
    );
    let postgres = GenericImage::new("postgres", "16-alpine")
        .with_exposed_port(ContainerPort::Tcp(5432))
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "rotation-fixture-value")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .unwrap();
    let postgres_host = postgres.get_host().await.unwrap();
    let postgres_port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let (upstream, upstream_connection) = tokio_postgres::connect(
        &format!(
            "host={postgres_host} port={postgres_port} user=postgres password=rotation-fixture-value dbname=postgres"
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = upstream_connection.await;
    });
    upstream
        .batch_execute(concat!(
            "CREATE TABLE public.orders (id BIGINT, amount BIGINT); ",
            "ALTER TABLE public.orders REPLICA IDENTITY FULL; ",
            "INSERT INTO public.orders VALUES (1, 10), (1, 10), (2, NULL); ",
            "CREATE PUBLICATION orders_pub FOR TABLE public.orders;",
        ))
        .await
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let shard_dir = storage.join("gateway-shard");
    std::fs::create_dir_all(&shard_dir).unwrap();
    let mut principal = scram_principal();
    principal.preset_grants.push(ScopedPresetGrant {
        preset: PermissionPreset::Builder,
        scope: PolicyScope::Namespace {
            namespace: "public".to_string(),
        },
    });
    principal.scoped_grants.push(ScopedActionGrant {
        action: SecurityAction::SecretUse,
        scope: PolicyScope::Secret {
            namespace: "public".to_string(),
            name: "pg_auth".to_string(),
            consumer: Some(PrincipalId(1)),
        },
    });
    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let durable = DurableCatalogStore::new(store.clone(), "gateway");
    durable
        .preprovision_first_administrator(principal.clone(), 1)
        .await
        .unwrap();
    durable
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![
                    CatalogMutation::PutDatabase(CatalogDatabase {
                        id: DatabaseId(1),
                        name: "default".to_string(),
                        default_namespace: "public".to_string(),
                        created_at: 1,
                    }),
                    CatalogMutation::PutNamespace(CatalogNamespace {
                        id: NamespaceId(1),
                        name: "public".to_string(),
                        database_id: DatabaseId(1),
                        created_at: 1,
                    }),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let audit_before_client = durable.list_security_audit_records().await.unwrap();
    assert_eq!(audit_before_client.len(), 1);
    let bootstrap = &audit_before_client[0];
    assert!(bootstrap.timestamp_ms > 0);
    assert_eq!(
        bootstrap,
        &expected_bootstrap_audit(&principal, 1, bootstrap.timestamp_ms)
    );

    let tls_key = rcgen::KeyPair::generate().unwrap();
    let tls_cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&tls_key)
        .unwrap();
    let cert_path = temp.path().join("gateway.pem");
    let key_path = temp.path().join("gateway-key.pem");
    std::fs::write(&cert_path, tls_cert.pem()).unwrap();
    std::fs::write(&key_path, tls_key.serialize_pem()).unwrap();

    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "scram",
        None,
        "",
        "",
        "",
    );
    wait_for_gateway(&mut process.0, address).await;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=pencil dbname=test sslmode=disable",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(connection);
    client
        .simple_query(
            "CREATE SECRET pg_auth (TYPE = 'postgres_role', PASSWORD = 'rotation-fixture-value')",
        )
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE orders (id BIGINT, amount BIGINT)")
        .await
        .unwrap();
    let slot_name = format!("rotation_{}", address.port());
    client
        .simple_query(&format!(
            "CREATE SOURCE orders TYPE postgres_cdc (host='{postgres_host}', port='{postgres_port}', database='postgres', user='postgres', publication='orders_pub', slot='{slot_name}', table='public.orders', secret='pg_auth') FORMAT pgoutput"
        ))
        .await
        .unwrap();
    client
        .simple_query("CREATE MATERIALIZED VIEW order_rows AS SELECT id, amount FROM orders")
        .await
        .unwrap();

    let row =
        |id: &str, amount: Option<&str>| vec![Some(id.to_string()), amount.map(str::to_string)];
    let mut expected_rows = vec![row("1", Some("10")), row("1", Some("10")), row("2", None)];
    expected_rows.sort();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let observed = client
            .simple_query("SELECT * FROM order_rows")
            .await
            .ok()
            .map(|messages| {
                let mut rows = simple_query_rows(&messages);
                rows.sort();
                rows
            });
        if observed.as_ref() == Some(&expected_rows) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "release PostgreSQL source did not publish its exact initial rows"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    upstream
        .batch_execute("BEGIN; INSERT INTO public.orders VALUES (3, 30), (3, 30), (4, NULL);")
        .await
        .unwrap();
    let (rotation_admin, rotation_admin_connection) = tokio_postgres::connect(
        &format!(
            "host={postgres_host} port={postgres_port} user=postgres password=rotation-fixture-value dbname=postgres"
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let rotation_admin_task = tokio::spawn(async move {
        let _ = rotation_admin_connection.await;
    });
    rotation_admin
        .simple_query("ALTER ROLE postgres PASSWORD 'rotated-fixture-value'")
        .await
        .unwrap();
    drop(rotation_admin);
    rotation_admin_task.await.unwrap();
    let old_credential_error = match tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={postgres_port} user=postgres password=rotation-fixture-value dbname=postgres"
        ),
        tokio_postgres::NoTls,
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("PostgreSQL accepted the old credential after rotation"),
    };
    let old_credential_db_error = old_credential_error.as_db_error().unwrap_or_else(|| {
        panic!("old credential failed outside PostgreSQL: {old_credential_error:?}")
    });
    assert_eq!(old_credential_db_error.code().code(), "28P01");
    assert_eq!(
        old_credential_db_error.message(),
        "password authentication failed for user \"postgres\""
    );
    let rotation_id = "d931137e-9e28-4a29-96f2-0cb10e5acb21";
    let rotation_query = "ALTER SECRET pg_auth SET (PASSWORD = 'rotated-fixture-value')";
    let missing_key = client.simple_query(rotation_query).await.unwrap_err();
    let missing_key = missing_key.as_db_error().unwrap();
    assert_eq!(missing_key.code().code(), "XX000");
    assert!(missing_key
        .message()
        .starts_with("[RS-2007] write.idempotency_key_required:"));
    let (proxy_address, response_dropped, proxy) =
        pgwire_proxy_dropping_query_response(address, rotation_query).await;
    let (lost_response_client, lost_response_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=pencil dbname=test sslmode=disable",
            proxy_address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let lost_response_connection_task = tokio::spawn(lost_response_connection);
    let set_response = lost_response_client
        .simple_query(&format!("SET rockstream.idempotency_key = '{rotation_id}'"))
        .await
        .unwrap();
    assert!(
        matches!(
            set_response.as_slice(),
            [tokio_postgres::SimpleQueryMessage::CommandComplete(0)]
        ),
        "expected exactly one successful SET response, got {set_response:?}"
    );
    let lost_response = tokio::time::timeout(
        Duration::from_secs(10),
        lost_response_client.simple_query(rotation_query),
    )
    .await
    .unwrap();
    assert!(
        lost_response.is_err(),
        "the proxy should drop the committed ALTER SECRET response"
    );
    tokio::time::timeout(Duration::from_secs(10), response_dropped)
        .await
        .unwrap()
        .unwrap();
    lost_response_connection_task.abort();
    drop(lost_response_client);
    proxy.await.unwrap();
    let set_response = client
        .simple_query(&format!("SET rockstream.idempotency_key = '{rotation_id}'"))
        .await
        .unwrap();
    assert!(matches!(
        set_response.as_slice(),
        [tokio_postgres::SimpleQueryMessage::CommandComplete(0)]
    ));
    let conflict = client
        .simple_query("ALTER SECRET pg_auth SET (PASSWORD = 'different-fixture-value')")
        .await
        .unwrap_err();
    assert_eq!(
        conflict.as_db_error().unwrap().message(),
        format!(
            "idempotency key is already bound to operation {rotation_id} with a different request"
        )
    );
    let retry_response = client.simple_query(rotation_query).await.unwrap();
    assert!(
        matches!(
            retry_response.as_slice(),
            [tokio_postgres::SimpleQueryMessage::CommandComplete(0)]
        ),
        "expected exactly one successful retry response, got {retry_response:?}"
    );
    upstream.batch_execute("COMMIT").await.unwrap();
    expected_rows.extend([row("3", Some("30")), row("3", Some("30")), row("4", None)]);
    expected_rows.sort();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let observed = client
            .simple_query("SELECT * FROM order_rows")
            .await
            .ok()
            .map(|messages| {
                let mut rows = simple_query_rows(&messages);
                rows.sort();
                rows
            });
        if observed.as_ref() == Some(&expected_rows) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "release PostgreSQL source did not preserve rows across secret rotation"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(process.0.try_wait().unwrap().is_none());
    upstream
        .batch_execute("INSERT INTO public.orders VALUES (5, 50)")
        .await
        .unwrap();
    let committed_wal_lsn: String = upstream
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    expected_rows.push(row("5", Some("50")));
    expected_rows.sort();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let observed = client
            .simple_query("SELECT * FROM order_rows")
            .await
            .ok()
            .map(|messages| {
                let mut rows = simple_query_rows(&messages);
                rows.sort();
                rows
            });
        let slot_checkpointed = upstream
            .query_opt(
                "SELECT COALESCE(confirmed_flush_lsn >= ($2::text)::pg_lsn, false) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name, &committed_wal_lsn],
            )
            .await
            .unwrap()
            .is_some_and(|row| row.get::<_, bool>(0));
        if observed.as_ref() == Some(&expected_rows) && slot_checkpointed {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "release PostgreSQL source did not durably checkpoint after secret rotation; observed: {observed:?}, slot_checkpointed: {slot_checkpointed}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let before_restart = simple_query_rows(&client.simple_query("SHOW SECRETS").await.unwrap());
    assert_eq!(before_restart.len(), 1);
    assert_eq!(before_restart[0][0].as_deref(), Some("pg_auth"));
    assert_eq!(before_restart[0][1].as_deref(), Some("postgres_role"));
    assert!(!serde_json::to_string(&before_restart)
        .unwrap()
        .contains("rotated-fixture-value"));
    drop(client);
    connection_task.abort();
    process.0.kill().unwrap();
    process.0.wait().unwrap();

    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let mut recovered = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "scram",
        None,
        "",
        "",
        "",
    );
    wait_for_gateway(&mut recovered.0, address).await;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=pencil dbname=test sslmode=disable",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(connection);
    let after_restart = simple_query_rows(&client.simple_query("SHOW SECRETS").await.unwrap());
    assert_eq!(after_restart, before_restart);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let observed = match client.simple_query("SELECT * FROM order_rows").await {
            Ok(messages) => {
                let mut rows = simple_query_rows(&messages);
                rows.sort();
                Ok(rows)
            }
            Err(error) => Err(format!("[RS-0001] {error:?}")),
        };
        if observed.as_ref().is_ok_and(|rows| rows == &expected_rows) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "release PostgreSQL source did not recover the complete committed rows: {observed:?}; backfill status: {:?}",
            simple_query_rows(
                &client
                    .simple_query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows")
                    .await
                    .unwrap()
            )
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (rotated_upstream, rotated_upstream_connection) = tokio_postgres::connect(
        &format!(
            "host={postgres_host} port={postgres_port} user=postgres password=rotated-fixture-value dbname=postgres"
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("the rotated credential should authenticate after gateway restart");
    let rotated_upstream_connection_task = tokio::spawn(rotated_upstream_connection);
    rotated_upstream
        .batch_execute("INSERT INTO public.orders VALUES (6, 60)")
        .await
        .unwrap();
    let committed_wal_lsn: String = rotated_upstream
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .unwrap()
        .get(0);
    expected_rows.push(row("6", Some("60")));
    expected_rows.sort();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let observed = client
            .simple_query("SELECT * FROM order_rows")
            .await
            .ok()
            .map(|messages| {
                let mut rows = simple_query_rows(&messages);
                rows.sort();
                rows
            });
        let slot_checkpointed = upstream
            .query_opt(
                "SELECT COALESCE(confirmed_flush_lsn >= ($2::text)::pg_lsn, false) FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name, &committed_wal_lsn],
            )
            .await
            .unwrap()
            .is_some_and(|row| row.get::<_, bool>(0));
        if observed.as_ref() == Some(&expected_rows) && slot_checkpointed {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "release PostgreSQL source did not consume the post-restart row with the rotated credential; observed: {observed:?}, slot_checkpointed: {slot_checkpointed}; backfill status: {:?}; source status: {:?}; pending slot messages: {}",
            simple_query_rows(
                &client
                    .simple_query("SHOW BACKFILL STATUS FOR MATERIALIZED VIEW order_rows")
                    .await
                    .unwrap()
            ),
            simple_query_rows(
                &client
                    .simple_query("SHOW SOURCE STATUS FOR orders")
                    .await
                    .unwrap()
            ),
            upstream
                .query_one(
                    "SELECT count(*)::bigint FROM pg_logical_slot_peek_binary_changes($1, NULL, NULL, 'proto_version', '1', 'publication_names', 'orders_pub')",
                    &[&slot_name],
                )
                .await
                .unwrap()
                .get::<_, i64>(0)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(rotated_upstream);
    rotated_upstream_connection_task.abort();
    drop(client);
    connection_task.abort();
    recovered.0.kill().unwrap();
    recovered.0.wait().unwrap();

    let durable_catalog = DurableCatalogStore::recover(store, "gateway")
        .await
        .unwrap();
    let namespace = durable_catalog
        .get_namespace_by_name("public")
        .await
        .unwrap()
        .unwrap();
    let secret_store = rockstream_control::SecretStore::open_durable(
        "gateway-secrets",
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap(),
        Arc::new(rockstream_control::EnvKekProvider::from_passphrase(
            "release-test-kek",
        )),
    )
    .await
    .unwrap();
    let secret = secret_store
        .get_secret(u128::from(namespace.id.0), "pg_auth")
        .await
        .unwrap();
    assert_eq!(secret.metadata.version, 2);
    assert!(secret
        .payload
        .get("password")
        .is_some_and(|value| value == "rotated-fixture-value"));
    let records = durable_catalog.list_security_audit_records().await.unwrap();
    assert_eq!(
        records.len(),
        audit_before_client.len() + 4,
        "unexpected release audit sequence: {records:#?}"
    );
    assert_eq!(
        &records[..audit_before_client.len()],
        audit_before_client.as_slice()
    );
    let events = &records[audit_before_client.len()..];
    let authentication_events = events
        .iter()
        .filter(|record| record.action == SecurityAuditClass::AuthenticationSuccess)
        .collect::<Vec<_>>();
    let rotation_audit = events
        .iter()
        .find(|record| record.action == SecurityAuditClass::SecretRotation)
        .unwrap();
    assert_eq!(events.len(), 4);
    assert_eq!(authentication_events.len(), 3);
    let rotation_correlation = rotation_audit.correlation.clone();
    assert!(matches!(
        &rotation_correlation,
        AuditCorrelation::Observed(_)
    ));
    let rotation_authentication = authentication_events
        .iter()
        .copied()
        .find(|record| record.correlation == rotation_correlation)
        .unwrap();
    let initial_authentication = authentication_events
        .iter()
        .copied()
        .find(|record| {
            record.correlation != rotation_correlation
                && record.timestamp_ms < rotation_audit.timestamp_ms
        })
        .unwrap();
    assert_ne!(initial_authentication.correlation, rotation_correlation);
    let restart_authentication = authentication_events
        .iter()
        .copied()
        .find(|record| record.timestamp_ms > rotation_audit.timestamp_ms)
        .unwrap();
    assert!(matches!(
        &restart_authentication.correlation,
        AuditCorrelation::Observed(_)
    ));
    assert_ne!(
        rotation_authentication.correlation,
        restart_authentication.correlation
    );
    for record in &authentication_events {
        let suffix = record.event_id.strip_prefix("gateway-authn-").unwrap();
        assert_eq!(suffix.len(), 32);
        assert!(u128::from_str_radix(suffix, 16).is_ok());
        assert!(record.timestamp_ms > 0);
    }
    assert!(rotation_audit.timestamp_ms > 0);
    let expected_authentication = |record: &SecurityAuditRecord| SecurityAuditRecord {
        format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
        event_id: record.event_id.clone(),
        timestamp_ms: record.timestamp_ms,
        actor: AuditIdentity::Verified("scram:alice".to_string()),
        target: AuditField::Known("principal:1".to_string()),
        scope: AuditField::NotApplicable,
        action: SecurityAuditClass::AuthenticationSuccess,
        outcome: SecurityAuditOutcome::Success,
        correlation: record.correlation.clone(),
        operation_id: AuditField::NotApplicable,
        policy_revision: AuditField::Known(1),
        before_state: AuditField::NotApplicable,
        after_state: AuditField::NotApplicable,
    };
    assert_eq!(
        rotation_authentication,
        &expected_authentication(rotation_authentication)
    );
    assert_eq!(
        restart_authentication,
        &expected_authentication(restart_authentication)
    );
    for record in &authentication_events {
        assert_eq!(*record, &expected_authentication(record));
    }
    assert_eq!(
        rotation_audit.event_id,
        format!("gateway-secret-rotation-{rotation_id}")
    );
    assert_eq!(
        rotation_audit.format_version,
        rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION
    );
    assert_eq!(
        rotation_audit.actor,
        AuditIdentity::Verified("principal:1".to_string())
    );
    assert_eq!(
        rotation_audit.target,
        AuditField::Known("secret:public/pg_auth".to_string())
    );
    assert_eq!(
        rotation_audit.scope,
        AuditField::Known(
            serde_json::to_string(&PolicyScope::Secret {
                namespace: "public".to_string(),
                name: "pg_auth".to_string(),
                consumer: None,
            })
            .unwrap()
        )
    );
    assert_eq!(rotation_audit.outcome, SecurityAuditOutcome::Success);
    assert!(matches!(
        &rotation_audit.correlation,
        AuditCorrelation::Observed(id) if !id.is_empty()
    ));
    assert_eq!(
        rotation_audit.operation_id,
        AuditField::Known(rotation_id.to_owned())
    );
    assert_eq!(rotation_audit.policy_revision, AuditField::Known(1));
    assert_eq!(
        rotation_audit.before_state,
        AuditField::Known("version=1".to_string())
    );
    assert_eq!(
        rotation_audit.after_state,
        AuditField::Known("version=2".to_string())
    );
    let mut expected_events = authentication_events
        .iter()
        .map(|record| expected_authentication(record))
        .collect::<Vec<_>>();
    expected_events.push(rotation_audit.clone());
    expected_events.sort_by_key(|record| (record.timestamp_ms, record.event_id.clone()));
    let mut observed_events = events.to_vec();
    observed_events.sort_by_key(|record| (record.timestamp_ms, record.event_id.clone()));
    assert_eq!(observed_events, expected_events);
    let operation = rockstream_control::ManagementOperationStore::new(
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap(),
    )
    .get(rotation_id)
    .await
    .unwrap()
    .unwrap();
    assert_eq!(operation.operation_id(), rotation_id);
    assert_eq!(
        operation.kind(),
        rockstream_control::OperationKind::SecretRotation
    );
    assert_eq!(
        operation.status(),
        rockstream_control::OperationStatus::Succeeded
    );
    assert_eq!(
        operation.result(),
        Some(&serde_json::json!({"command_tag": "ALTER SECRET", "rows": 0}))
    );
    assert!(!serde_json::to_string(operation.request().unwrap())
        .unwrap()
        .contains("rotated-fixture-value"));
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sasl_secret_rotation_preserves_multiview_checkpoints_and_other_consumers() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the Kafka secret-rotation release proof"
    );
    let initial_jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-pass\" user_admin=\"admin-pass\" user_rotating=\"old-pass\" user_live=\"live-pass\" user_rotated=\"new-pass\" user_steady=\"steady-pass\";";
    let broker = apache::Kafka::default()
        .with_jvm_image()
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "PLAINTEXT:SASL_PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
        )
        .with_env_var("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
        .with_env_var(
            "KAFKA_AUTHORIZER_CLASS_NAME",
            "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
        )
        .with_env_var("KAFKA_SUPER_USERS", "User:admin;User:ANONYMOUS")
        .with_env_var("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")
        .with_env_var(
            "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
            initial_jaas,
        )
        .with_env_var(
            "KAFKA_LISTENER_NAME_PLAINTEXT_CONNECTIONS_MAX_REAUTH_MS",
            "2000",
        )
        .with_env_var(
            "KAFKA_PASSWORD_ENCODER_SECRET",
            "release-test-password-encoder",
        )
        .start()
        .await
        .unwrap();
    let bootstrap = format!(
        "127.0.0.1:{}",
        broker.get_host_port_ipv4(apache::KAFKA_PORT).await.unwrap()
    );
    let admin: AdminClient<_> = kafka_sasl_client(&bootstrap, "admin", "admin-pass")
        .create()
        .unwrap();
    let created_topics = admin
        .create_topics(
            &[
                NewTopic::new("rotating-orders", 3, TopicReplication::Fixed(1)),
                NewTopic::new("steady-orders", 1, TopicReplication::Fixed(1)),
            ],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        created_topics,
        [
            Ok("rotating-orders".to_string()),
            Ok("steady-orders".to_string())
        ]
    );
    for (principal, topic, group) in [
        ("User:rotating", "rotating-orders", "rotation-group-"),
        ("User:live", "rotating-orders", "rotation-group-"),
        ("User:steady", "steady-orders", "steady-group-"),
    ] {
        kafka_acl(
            &broker,
            &[
                "--add",
                "--allow-principal",
                principal,
                "--operation",
                "Read",
                "--operation",
                "Describe",
                "--topic",
                topic,
            ],
        )
        .await;
        kafka_acl(
            &broker,
            &[
                "--add",
                "--allow-principal",
                principal,
                "--operation",
                "Read",
                "--operation",
                "Describe",
                "--group",
                group,
                "--resource-pattern-type",
                "prefixed",
            ],
        )
        .await;
    }
    let producer: FutureProducer = kafka_sasl_client(&bootstrap, "admin", "admin-pass")
        .create()
        .unwrap();
    let initial_rotating = [(1, Some(10), 0), (1, Some(10), 1), (2, None, 2)];
    let initial_steady = [(10, Some(100), 0)];
    kafka_source_rows(&producer, "rotating-orders", &initial_rotating).await;
    kafka_source_rows(&producer, "steady-orders", &initial_steady).await;

    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let shard_dir = storage.join("gateway-shard");
    std::fs::create_dir_all(&shard_dir).unwrap();
    let mut principal = scram_principal();
    principal.preset_grants.push(ScopedPresetGrant {
        preset: PermissionPreset::Builder,
        scope: PolicyScope::Namespace {
            namespace: "public".to_string(),
        },
    });
    for name in ["kafka_auth", "steady_auth"] {
        principal.scoped_grants.push(ScopedActionGrant {
            action: SecurityAction::SecretUse,
            scope: PolicyScope::Secret {
                namespace: "public".to_string(),
                name: name.to_string(),
                consumer: Some(PrincipalId(1)),
            },
        });
    }
    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let durable = DurableCatalogStore::new(store.clone(), "gateway");
    durable
        .preprovision_first_administrator(principal, 1)
        .await
        .unwrap();
    durable
        .commit_txn(
            CatalogTxn::new(
                2,
                2,
                vec![
                    CatalogMutation::PutDatabase(CatalogDatabase {
                        id: DatabaseId(1),
                        name: "default".to_string(),
                        default_namespace: "public".to_string(),
                        created_at: 1,
                    }),
                    CatalogMutation::PutNamespace(CatalogNamespace {
                        id: NamespaceId(1),
                        name: "public".to_string(),
                        database_id: DatabaseId(1),
                        created_at: 1,
                    }),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let tls_key = rcgen::KeyPair::generate().unwrap();
    let tls_cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&tls_key)
        .unwrap();
    let cert_path = temp.path().join("gateway.pem");
    let key_path = temp.path().join("gateway-key.pem");
    std::fs::write(&cert_path, tls_cert.pem()).unwrap();
    std::fs::write(&key_path, tls_key.serialize_pem()).unwrap();
    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "scram",
        None,
        "",
        "",
        "",
    );
    wait_for_gateway(&mut process.0, address).await;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=pencil dbname=test sslmode=disable",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(connection);
    for sql in [
        "CREATE SECRET kafka_auth (TYPE = 'sasl_plain', USERNAME = 'rotating', PASSWORD = 'old-pass')",
        "CREATE SECRET steady_auth (TYPE = 'sasl_plain', USERNAME = 'steady', PASSWORD = 'steady-pass')",
        "CREATE TABLE rotating_orders (id BIGINT, amount BIGINT)",
        "CREATE TABLE steady_orders (id BIGINT, amount BIGINT)",
        &format!(
            "CREATE SOURCE rotating_orders TYPE kafka (bootstrap.servers='{bootstrap}', topic='rotating-orders', group.id='rotation-group', security.protocol='SASL_PLAINTEXT', secret='kafka_auth') FORMAT json"
        ),
        &format!(
            "CREATE SOURCE steady_orders TYPE kafka (bootstrap.servers='{bootstrap}', topic='steady-orders', group.id='steady-group', security.protocol='SASL_PLAINTEXT', secret='steady_auth') FORMAT json"
        ),
        "CREATE MATERIALIZED VIEW rotating_a AS SELECT id, amount FROM rotating_orders",
        "CREATE MATERIALIZED VIEW rotating_b AS SELECT id, amount FROM rotating_orders",
        "CREATE MATERIALIZED VIEW steady_rows AS SELECT id, amount FROM steady_orders",
    ] {
        client.simple_query(sql).await.unwrap();
    }

    let row = |id: i64, amount: Option<i64>| {
        vec![Some(id.to_string()), amount.map(|value| value.to_string())]
    };
    let mut expected_rotating = initial_rotating
        .iter()
        .map(|(id, amount, _)| row(*id, *amount))
        .collect::<Vec<_>>();
    expected_rotating.sort();
    let mut expected_steady = initial_steady
        .iter()
        .map(|(id, amount, _)| row(*id, *amount))
        .collect::<Vec<_>>();
    expected_steady.sort();
    let initial_deadline = Instant::now() + Duration::from_secs(30);
    for view in ["rotating_a", "rotating_b"] {
        wait_for_exact_query_rows(
            &broker,
            &client,
            &mut process.0,
            &format!("SELECT * FROM {view}"),
            &expected_rotating,
            initial_deadline,
            "initial Kafka materialized view backfill",
        )
        .await;
    }
    wait_for_exact_query_rows(
        &broker,
        &client,
        &mut process.0,
        "SELECT * FROM steady_rows",
        &expected_steady,
        initial_deadline,
        "initial unaffected Kafka consumer backfill",
    )
    .await;

    kafka_acl(
        &broker,
        &[
            "--add",
            "--allow-principal",
            "User:rotated",
            "--operation",
            "Read",
            "--operation",
            "Write",
            "--operation",
            "Describe",
            "--topic",
            "rotating-orders",
        ],
    )
    .await;
    kafka_acl(
        &broker,
        &[
            "--add",
            "--allow-principal",
            "User:rotated",
            "--operation",
            "Read",
            "--operation",
            "Describe",
            "--group",
            "rotation-group-",
            "--resource-pattern-type",
            "prefixed",
        ],
    )
    .await;
    for (resource, name) in [
        ("--topic", "rotating-orders"),
        ("--group", "rotation-group-"),
    ] {
        let mut args = vec![
            "--add",
            "--deny-principal",
            "User:rotating",
            "--operation",
            "Read",
            "--operation",
            "Describe",
            resource,
            name,
        ];
        if resource == "--topic" {
            args.extend(["--operation", "Write"]);
        } else {
            args.extend(["--resource-pattern-type", "prefixed"]);
        }
        kafka_acl(&broker, &args).await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let pending_rows = (1000..11_000)
        .map(|id| (id, (id % 7 != 0).then_some(id * 10), (id % 3) as i32))
        .collect::<Vec<_>>();
    let mut expected_rotating_offsets = BTreeMap::from([(0, 1), (1, 1), (2, 1)]);
    for (_, _, partition) in &pending_rows {
        *expected_rotating_offsets.get_mut(partition).unwrap() += 1;
    }
    kafka_source_rows(&producer, "rotating-orders", &pending_rows).await;
    expected_rotating.extend(pending_rows.iter().map(|(id, amount, _)| row(*id, *amount)));
    expected_rotating.sort();
    let pending_checkpoints =
        kafka_source_group_checkpoints(&broker, "rotating-orders", "rotation-group-").await;
    assert_eq!(
        pending_checkpoints.len(),
        2,
        "expected one Kafka group per view"
    );
    assert!(
        pending_checkpoints.values().all(|partitions| partitions
            .values()
            .any(|(current, log_end)| log_end > current)),
        "rotation began without pending work in each view: {pending_checkpoints:?}"
    );

    let rotation_operation_id = "00000000-0000-4000-8000-000000000304";
    let live_rotation_operation_id = "00000000-0000-4000-8000-000000000305";
    let live_process_id = process.0.id();
    client
        .simple_query(&format!(
            "SET rockstream.idempotency_key = '{live_rotation_operation_id}'"
        ))
        .await
        .unwrap();
    let immediately_before_rotation =
        kafka_source_group_checkpoints(&broker, "rotating-orders", "rotation-group-").await;
    assert_eq!(immediately_before_rotation.len(), 2);
    assert!(
        immediately_before_rotation
            .values()
            .all(|partitions| partitions
                .values()
                .any(|(current, log_end)| log_end > current)),
        "live rotation requires pending work in each view: {immediately_before_rotation:?}"
    );
    let live_rotation = client
        .simple_query("ALTER SECRET kafka_auth SET (USERNAME = 'live', PASSWORD = 'live-pass')")
        .await
        .unwrap();
    assert!(matches!(
        live_rotation.as_slice(),
        [tokio_postgres::SimpleQueryMessage::CommandComplete(0)]
    ));
    let live_deadline = Instant::now() + Duration::from_secs(90);
    for view in ["rotating_a", "rotating_b"] {
        wait_for_exact_query_rows(
            &broker,
            &client,
            &mut process.0,
            &format!("SELECT * FROM {view}"),
            &expected_rotating,
            live_deadline,
            "pending Kafka records after live secret rotation",
        )
        .await;
    }
    wait_for_kafka_group_checkpoints(
        &broker,
        "rotating-orders",
        "rotation-group-",
        2,
        &expected_rotating_offsets,
        live_deadline,
        "rotating view checkpoints after live rotation",
    )
    .await;
    wait_for_exact_query_rows(
        &broker,
        &client,
        &mut process.0,
        "SELECT * FROM steady_rows",
        &expected_steady,
        live_deadline,
        "unaffected Kafka view after live rotation",
    )
    .await;
    wait_for_kafka_group_checkpoints(
        &broker,
        "steady-orders",
        "steady-group-",
        1,
        &BTreeMap::from([(0, 1)]),
        live_deadline,
        "unaffected consumer checkpoint after live rotation",
    )
    .await;
    assert!(process.0.try_wait().unwrap().is_none());
    assert_eq!(process.0.id(), live_process_id);

    let rotation_query =
        "ALTER SECRET kafka_auth SET (USERNAME = 'rotated', PASSWORD = 'new-pass')";
    let (proxy_address, dropped_response, response_proxy) =
        pgwire_proxy_dropping_query_response(address, rotation_query).await;
    let (rotation_client, rotation_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=pencil dbname=test sslmode=disable",
            proxy_address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let rotation_connection_task = tokio::spawn(rotation_connection);
    rotation_client
        .simple_query(&format!(
            "SET rockstream.idempotency_key = '{rotation_operation_id}'"
        ))
        .await
        .unwrap();
    let lost_response = rotation_client
        .simple_query(rotation_query)
        .await
        .unwrap_err();
    assert!(lost_response.as_db_error().is_none());
    dropped_response.await.unwrap();
    response_proxy.await.unwrap();
    drop(rotation_client);
    rotation_connection_task.abort();
    connection_task.abort();
    drop(client);
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    drop(process);
    let mut process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "scram",
        None,
        "",
        "",
        "",
    );
    wait_for_gateway(&mut process.0, address).await;
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=pencil dbname=test sslmode=disable",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection_task = tokio::spawn(connection);
    let reconciled_process_id = process.0.id();
    assert!(process.0.try_wait().unwrap().is_none());
    client
        .simple_query(&format!(
            "SET rockstream.idempotency_key = '{rotation_operation_id}'"
        ))
        .await
        .unwrap();
    let rotation = client.simple_query(rotation_query).await.unwrap();
    assert!(matches!(
        rotation.as_slice(),
        [tokio_postgres::SimpleQueryMessage::CommandComplete(0)]
    ));
    let drain_deadline = Instant::now() + Duration::from_secs(90);
    for view in ["rotating_a", "rotating_b"] {
        wait_for_exact_query_rows(
            &broker,
            &client,
            &mut process.0,
            &format!("SELECT * FROM {view}"),
            &expected_rotating,
            drain_deadline,
            "pending Kafka records across secret rotation",
        )
        .await;
    }

    for (resource, name) in [
        ("--topic", "rotating-orders"),
        ("--group", "rotation-group-"),
    ] {
        let mut args = vec![
            "--add",
            "--deny-principal",
            "User:live",
            "--operation",
            "Read",
            "--operation",
            "Describe",
            resource,
            name,
        ];
        if resource == "--group" {
            args.extend(["--resource-pattern-type", "prefixed"]);
        }
        kafka_acl(&broker, &args).await;
    }
    let mut rotated_client = kafka_sasl_client(&bootstrap, "rotated", "new-pass");
    rotated_client.set("message.timeout.ms", "10000");
    let rotated_producer: FutureProducer = rotated_client.create().unwrap();
    let after_rotation = [(50_000, None, 1)];
    kafka_source_rows(&rotated_producer, "rotating-orders", &after_rotation).await;
    *expected_rotating_offsets.get_mut(&1).unwrap() += 1;
    expected_rotating.push(row(50_000, None));
    expected_rotating.sort();

    let rejected_old_credentials = kafka_sasl_client(&bootstrap, "rotating", "old-pass")
        .create::<FutureProducer>()
        .unwrap()
        .send(
            FutureRecord::<(), _>::to("rotating-orders").payload("invalid old credential probe"),
            Duration::from_secs(10),
        )
        .await;
    assert!(
        rejected_old_credentials.is_err(),
        "Kafka continued to accept the rotated credential"
    );
    kafka_source_rows(&producer, "steady-orders", &[(50_001, Some(500_010), 0)]).await;
    expected_steady.push(row(50_001, Some(500_010)));
    expected_steady.sort();

    let final_deadline = Instant::now() + Duration::from_secs(90);
    for view in ["rotating_a", "rotating_b"] {
        wait_for_exact_query_rows(
            &broker,
            &client,
            &mut process.0,
            &format!("SELECT * FROM {view}"),
            &expected_rotating,
            final_deadline,
            "Kafka source after broker credential revocation",
        )
        .await;
    }
    wait_for_kafka_group_checkpoints(
        &broker,
        "rotating-orders",
        "rotation-group-",
        2,
        &expected_rotating_offsets,
        final_deadline,
        "rotating view checkpoints after credential replacement",
    )
    .await;
    wait_for_exact_query_rows(
        &broker,
        &client,
        &mut process.0,
        "SELECT * FROM steady_rows",
        &expected_steady,
        final_deadline,
        "unaffected Kafka consumer after credential rotation",
    )
    .await;
    let expected_steady_offsets = BTreeMap::from([(0, 2)]);
    wait_for_kafka_group_checkpoints(
        &broker,
        "steady-orders",
        "steady-group-",
        1,
        &expected_steady_offsets,
        final_deadline,
        "unaffected consumer checkpoint after rotation",
    )
    .await;
    assert!(process.0.try_wait().unwrap().is_none());
    assert_eq!(process.0.id(), reconciled_process_id);

    connection_task.abort();
    drop(client);
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let envelope_db = rockstream_storage::ShardDb::builder("gateway-secrets", store.clone())
        .build()
        .await
        .unwrap();
    let envelope_bytes = envelope_db
        .get(&rockstream_control::secret_store::secret_key(
            1,
            "kafka_auth",
        ))
        .await
        .unwrap()
        .expect("the released secret must have a durable envelope");
    let envelope: rockstream_types::secret::EncryptedSecret =
        serde_json::from_slice(&envelope_bytes).unwrap();
    assert_eq!(envelope.name, "kafka_auth");
    assert_eq!(envelope.metadata.version, 3);
    assert!(!envelope.ciphertext.is_empty());
    assert_eq!(envelope.nonce.len(), 12);
    for value in [
        b"old-pass".as_slice(),
        b"live-pass".as_slice(),
        b"new-pass".as_slice(),
    ] {
        assert!(
            !envelope_bytes
                .windows(value.len())
                .any(|window| window == value),
            "durable secret envelope contains plaintext credentials"
        );
    }
    drop(envelope_db);
    drop(process);
    let mut recovered = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "scram",
        None,
        "",
        "",
        "",
    );
    wait_for_gateway(&mut recovered.0, address).await;
    let (recovered_client, recovered_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=alice password=pencil dbname=test sslmode=disable",
            address.port()
        ),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let recovered_connection_task = tokio::spawn(recovered_connection);
    let recovery_deadline = Instant::now() + Duration::from_secs(90);
    for view in ["rotating_a", "rotating_b"] {
        wait_for_exact_query_rows(
            &broker,
            &recovered_client,
            &mut recovered.0,
            &format!("SELECT * FROM {view}"),
            &expected_rotating,
            recovery_deadline,
            "Kafka view after gateway restart",
        )
        .await;
    }
    wait_for_kafka_group_checkpoints(
        &broker,
        "rotating-orders",
        "rotation-group-",
        2,
        &expected_rotating_offsets,
        recovery_deadline,
        "rotating view checkpoints after gateway restart",
    )
    .await;
    wait_for_exact_query_rows(
        &broker,
        &recovered_client,
        &mut recovered.0,
        "SELECT * FROM steady_rows",
        &expected_steady,
        recovery_deadline,
        "unaffected Kafka consumer after gateway restart",
    )
    .await;
    wait_for_kafka_group_checkpoints(
        &broker,
        "steady-orders",
        "steady-group-",
        1,
        &expected_steady_offsets,
        recovery_deadline,
        "unaffected consumer checkpoint after gateway restart",
    )
    .await;
    let audit = FileAuditLog::open(storage.join("audit.jsonl"))
        .unwrap()
        .read_all()
        .unwrap();
    let audit_json = serde_json::to_string(&audit).unwrap();
    assert!(!audit_json.is_empty());
    for value in ["old-pass", "live-pass", "new-pass", "steady-pass"] {
        assert!(
            !audit_json.contains(value),
            "secret value leaked into audit"
        );
    }
    recovered_connection_task.abort();
    recovered.0.kill().unwrap();
    recovered.0.wait().unwrap();
    let durable_catalog = DurableCatalogStore::recover(store, "gateway")
        .await
        .unwrap();
    let sources = durable_catalog.list_sources().await.unwrap();
    for (source_name, expected_reference) in [
        ("rotating_orders", "secret://kafka_auth"),
        ("steady_orders", "secret://steady_auth"),
    ] {
        let source = sources
            .iter()
            .find(|source| source.name == source_name)
            .unwrap_or_else(|| panic!("missing durable source {source_name}"));
        assert_eq!(
            source.options.get("secret").map(String::as_str),
            Some(expected_reference),
            "durable source {source_name} must retain a canonical secret reference"
        );
    }
    let rotation_audits = durable_catalog
        .list_security_audit_records()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.action == SecurityAuditClass::SecretRotation)
        .collect::<Vec<_>>();
    assert_eq!(rotation_audits.len(), 2);
    let live_audit = rotation_audits
        .iter()
        .find(|record| {
            record.operation_id == AuditField::Known(live_rotation_operation_id.to_string())
        })
        .unwrap();
    assert!(live_audit.timestamp_ms > 0);
    assert!(matches!(&live_audit.correlation, AuditCorrelation::Observed(id) if !id.is_empty()));
    assert_eq!(
        live_audit,
        &SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: format!("gateway-secret-rotation-{live_rotation_operation_id}"),
            timestamp_ms: live_audit.timestamp_ms,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("secret:public/kafka_auth".to_owned()),
            scope: AuditField::Known(
                serde_json::to_string(&PolicyScope::Secret {
                    namespace: "public".to_owned(),
                    name: "kafka_auth".to_owned(),
                    consumer: None,
                })
                .unwrap()
            ),
            action: SecurityAuditClass::SecretRotation,
            outcome: SecurityAuditOutcome::Success,
            correlation: live_audit.correlation.clone(),
            operation_id: AuditField::Known(live_rotation_operation_id.to_owned()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::Known("version=1".to_owned()),
            after_state: AuditField::Known("version=2".to_owned()),
        }
    );
    let rotation_audit = rotation_audits
        .iter()
        .find(|record| record.operation_id == AuditField::Known(rotation_operation_id.to_string()))
        .unwrap();
    assert_eq!(
        rotation_audit.event_id,
        format!("gateway-secret-rotation-{rotation_operation_id}")
    );
    assert!(rotation_audit.timestamp_ms > 0);
    assert_eq!(
        rotation_audit.format_version,
        rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION
    );
    assert_eq!(
        rotation_audit.actor,
        AuditIdentity::Verified("principal:1".to_string())
    );
    assert_eq!(
        rotation_audit.target,
        AuditField::Known("secret:public/kafka_auth".to_string())
    );
    assert_eq!(
        rotation_audit.scope,
        AuditField::Known(
            serde_json::to_string(&PolicyScope::Secret {
                namespace: "public".to_string(),
                name: "kafka_auth".to_string(),
                consumer: None,
            })
            .unwrap()
        )
    );
    assert_eq!(rotation_audit.outcome, SecurityAuditOutcome::Success);
    assert!(matches!(
        &rotation_audit.correlation,
        AuditCorrelation::Observed(id) if !id.is_empty()
    ));
    assert_eq!(
        rotation_audit.operation_id,
        AuditField::Known(rotation_operation_id.to_string())
    );
    assert_eq!(rotation_audit.policy_revision, AuditField::Known(1));
    assert_eq!(
        rotation_audit.before_state,
        AuditField::Known("version=2".to_string())
    );
    assert_eq!(
        rotation_audit.after_state,
        AuditField::Known("version=3".to_string())
    );
    let operation_store = rockstream_control::ManagementOperationStore::new(
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap(),
    );
    let live_operation = operation_store
        .get(live_rotation_operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_operation.operation_id(), live_rotation_operation_id);
    assert_eq!(
        live_operation.kind(),
        rockstream_control::OperationKind::SecretRotation
    );
    assert_eq!(
        live_operation.status(),
        rockstream_control::OperationStatus::Succeeded
    );
    assert_eq!(
        live_operation.result(),
        Some(&serde_json::json!({"command_tag": "ALTER SECRET", "rows": 0}))
    );
    let operation = operation_store
        .get(rotation_operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.operation_id(), rotation_operation_id);
    assert_eq!(
        operation.kind(),
        rockstream_control::OperationKind::SecretRotation
    );
    assert_eq!(
        operation.status(),
        rockstream_control::OperationStatus::Succeeded
    );
    assert_eq!(
        operation.result(),
        Some(&serde_json::json!({"command_tag": "ALTER SECRET", "rows": 0}))
    );
    assert!(!serde_json::to_string(operation.request().unwrap())
        .unwrap()
        .contains("rotated-fixture-value"));
}

#[tokio::test]
async fn mtls_certificate_rejections_run_in_release_process_and_audit_after_restart() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let shard_dir = storage.join("gateway-shard");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let mut ca_params = rcgen::CertificateParams::default();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::CrlSign,
        rcgen::KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let mut server_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let server_key = rcgen::KeyPair::generate().unwrap();
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();
    let cert_path = temp.path().join("gateway.pem");
    let key_path = temp.path().join("gateway-key.pem");
    let ca_path = temp.path().join("gateway-ca.pem");
    std::fs::write(&cert_path, server_cert.pem()).unwrap();
    std::fs::write(&key_path, server_key.serialize_pem()).unwrap();
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();

    let mut client_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    client_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "alice");
    let client_key = rcgen::KeyPair::generate().unwrap();
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .unwrap();

    let mut expired_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    expired_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "alice");
    expired_params.not_before = rcgen::date_time_ymd(2020, 1, 1);
    expired_params.not_after = rcgen::date_time_ymd(2020, 1, 2);
    let expired_key = rcgen::KeyPair::generate().unwrap();
    let expired_cert = expired_params
        .signed_by(&expired_key, &ca_cert, &ca_key)
        .unwrap();

    let mut rogue_ca_params = rcgen::CertificateParams::default();
    rogue_ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let rogue_ca_key = rcgen::KeyPair::generate().unwrap();
    let rogue_ca_cert = rogue_ca_params.self_signed(&rogue_ca_key).unwrap();
    let mut wrong_ca_params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    wrong_ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "alice");
    let wrong_ca_key = rcgen::KeyPair::generate().unwrap();
    let wrong_ca_cert = wrong_ca_params
        .signed_by(&wrong_ca_key, &rogue_ca_cert, &rogue_ca_key)
        .unwrap();

    let principal = mtls_principal(&client_cert);
    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    DurableCatalogStore::new(store, "gateway")
        .preprovision_first_administrator(principal, 1)
        .await
        .unwrap();

    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mismatched_key_path = temp.path().join("gateway-mismatched-key.pem");
    let mismatched_key_pem = rcgen::KeyPair::generate().unwrap().serialize_pem();
    std::fs::write(&mismatched_key_path, &mismatched_key_pem).unwrap();
    let mut mismatched_process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &mismatched_key_path,
        "mtls",
        Some(&ca_path),
        "",
        "",
        "",
    );
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = mismatched_process.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("release gateway did not reject the mismatched TLS key");
    assert_eq!(status.code(), Some(1));
    let mut stderr = String::new();
    mismatched_process
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(
        stderr.contains("[RS-0002]")
            && stderr.contains("gateway mTLS configuration is invalid")
            && stderr.contains("Check the gateway certificate, key, and client CA files."),
        "unexpected startup error: {stderr}"
    );
    assert!(
        !stderr.contains(&mismatched_key_pem),
        "startup error exposed key material"
    );
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("invalid TLS configuration left the gateway listener bound");
    drop(listener);

    let mut process = start_gateway_process(
        temp.path(),
        &storage,
        address,
        &cert_path,
        &key_path,
        "mtls",
        Some(&ca_path),
        "",
        "",
        "",
    );
    wait_for_gateway(&mut process.0, address).await;

    let valid_tls = mtls_client_config(&ca_cert, &client_cert, &client_key);
    let mut valid = tokio_postgres::Config::new();
    valid
        .host("127.0.0.1")
        .port(address.port())
        .user("alice")
        .dbname("test")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    let (client, connection) = valid
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(valid_tls))
        .await
        .expect("the release mTLS process should accept its mapped certificate");
    let connection_task = tokio::spawn(connection);
    let rows = client.query("SELECT 1 AS value", &[]).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>(),
        vec![1i64]
    );
    drop(client);
    connection_task.abort();

    let expired_error = mtls_connection_error(
        address.port(),
        mtls_client_config(&ca_cert, &expired_cert, &expired_key),
    )
    .await;
    assert!(expired_error.as_db_error().is_none());
    let wrong_ca_error = mtls_connection_error(
        address.port(),
        mtls_client_config(&ca_cert, &wrong_ca_cert, &wrong_ca_key),
    )
    .await;
    assert!(wrong_ca_error.as_db_error().is_none());
    tokio::time::sleep(Duration::from_millis(100)).await;

    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let recovered = DurableCatalogStore::recover(store, "gateway")
        .await
        .unwrap();
    let records = recovered.list_security_audit_records().await.unwrap();
    assert_eq!(
        records.len(),
        4,
        "expired TLS error: {expired_error}; wrong-CA TLS error: {wrong_ca_error}; records: {records:?}"
    );
    assert_eq!(records[0].action, SecurityAuditClass::GrantChange);
    assert_eq!(records[0].outcome, SecurityAuditOutcome::Success);
    assert_eq!(records[1].action, SecurityAuditClass::AuthenticationSuccess);
    assert!(matches!(records[1].actor, AuditIdentity::Verified(_)));
    for record in &records[2..] {
        assert_eq!(record.action, SecurityAuditClass::AuthenticationFailure);
        assert_eq!(record.outcome, SecurityAuditOutcome::Failure);
        assert_eq!(record.actor, AuditIdentity::Unknown);
        assert_eq!(record.target, AuditField::Unknown);
    }
}

#[tokio::test]
async fn expired_and_wrong_ca_worker_certificates_are_rejected_by_release_control_process() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process =
        start_delegated_control_process(temp.path(), &storage, address, &pki, None, false, None)
            .await;
    wait_for_gateway(&mut process.0, address).await;

    let audit_path = storage.join("audit.jsonl");
    let deadline = Instant::now() + Duration::from_secs(5);
    let baseline = loop {
        let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
        let denials = events
            .iter()
            .filter(|event| event.action == "security.internal_mtls_denied")
            .cloned()
            .collect::<Vec<_>>();
        if !denials.is_empty() {
            break (events, denials);
        }
        assert!(
            Instant::now() < deadline,
            "release process did not audit its readiness probe"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    for (index, (name, tls)) in [
        ("expired", pki.expired_worker_tls_config()),
        ("wrong-ca", pki.untrusted_worker_tls_config()),
    ]
    .into_iter()
    .enumerate()
    {
        match rockstream_runtime::start_worker_client_with_tls(
            1,
            &address.to_string(),
            &temp.path().join(format!("worker-{name}")),
            tls,
        )
        .await
        {
            Err(error) => assert!(
                error.to_string().contains("RS-2411"),
                "unexpected {name} certificate rejection: {error}"
            ),
            Ok((client, handle)) => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                assert_eq!(
                    client.worker_id(),
                    None,
                    "release control process assigned a worker identity to the rejected {name} certificate"
                );
                handle.abort();
            }
        }
        if let Some(status) = process.0.try_wait().unwrap() {
            let mut stderr = String::new();
            if let Some(pipe) = process.0.stderr.as_mut() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("release control process exited after {name} certificate ({status}): {stderr}");
        }

        let expected_denials = baseline.1.len() + index + 1;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
            if events
                .iter()
                .filter(|event| event.action == "security.internal_mtls_denied")
                .count()
                == expected_denials
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "release process did not audit the rejected {name} certificate: {events:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let events = loop {
        let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
        let denials = events
            .iter()
            .filter(|event| event.action == "security.internal_mtls_denied")
            .count();
        if denials == baseline.1.len() + 2 {
            break events;
        }
        assert!(
            Instant::now() < deadline,
            "release process audit did not record both certificate denials: {events:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let denials = events
        .iter()
        .filter(|event| event.action == "security.internal_mtls_denied")
        .collect::<Vec<_>>();
    assert_eq!(denials.len(), baseline.1.len() + 2);
    for denial in &denials[baseline.1.len()..] {
        assert_eq!(denial.actor, "unknown");
        assert!(denial.resource.starts_with("peer=127.0.0.1:"));
        assert_eq!(denial.error_code.as_deref(), Some("RS-2411"));
        assert!(denial
            .detail
            .as_deref()
            .is_some_and(|detail| detail.starts_with("TLS handshake failed: ")
                && detail.ends_with(", error_code=RS-2411")));
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action == "worker.registered")
            .count(),
        baseline
            .0
            .iter()
            .filter(|event| event.action == "worker.registered")
            .count(),
        "rejected certificates must not register a worker"
    );

    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let recovered = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
    let recovered_denials = recovered
        .iter()
        .filter(|event| event.action == "security.internal_mtls_denied")
        .collect::<Vec<_>>();
    assert_eq!(recovered_denials.len(), denials.len());
    assert_eq!(recovered_denials, denials);
}

#[tokio::test]
async fn wrong_node_certificate_is_rejected_by_release_control_process() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let worker_storage = temp.path().join("worker-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process =
        start_delegated_control_process(temp.path(), &storage, address, &pki, None, false, None)
            .await;
    wait_for_gateway(&mut process.0, address).await;

    let security_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let security_catalog = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    let mut principals_before = security_catalog.list_security_principals().await.unwrap();
    principals_before.sort_by_key(|principal| principal.id);
    let audit_before = security_catalog
        .list_security_audit_records()
        .await
        .unwrap();

    let (client, worker_handle) = rockstream_runtime::start_worker_client_with_tls(
        1,
        &address.to_string(),
        &worker_storage,
        pki.mismatched_worker_tls_config(),
    )
    .await
    .expect("the mTLS worker client should reach the release control endpoint");
    assert_eq!(client.worker_id(), None);

    let audit_path = storage.join("audit.jsonl");
    let expected_detail = "node identity mismatch: cert=worker:worker-999, requested_worker_id=worker-1, error_code=RS-2412";
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
        security_catalog.refresh().await.unwrap();
        let security_audit = security_catalog
            .list_security_audit_records()
            .await
            .unwrap();
        if events.iter().any(|event| {
            event.action == "security.internal_mtls_denied"
                && event.detail.as_deref() == Some(expected_detail)
        }) && security_audit
            .iter()
            .any(|record| record.action == SecurityAuditClass::NodeIdentityRejection)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "release control process did not audit the wrong-node rejection; records: {events:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(client.worker_id(), None);
    worker_handle.abort();
    process.0.kill().unwrap();
    process.0.wait().unwrap();

    let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
    let denial = events
        .iter()
        .find(|event| {
            event.action == "security.internal_mtls_denied"
                && event.detail.as_deref() == Some(expected_detail)
        })
        .expect("durable wrong-node identity rejection audit");
    assert_eq!(denial.actor, "worker:worker-999");
    assert!(denial.resource.starts_with("worker_id=worker-1, peer="));
    assert_eq!(denial.error_code.as_deref(), Some("RS-2412"));

    let recovered_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let recovered = DurableCatalogStore::recover(recovered_store, "gateway")
        .await
        .unwrap();
    let mut principals_after = recovered.list_security_principals().await.unwrap();
    principals_after.sort_by_key(|principal| principal.id);
    assert_eq!(principals_after, principals_before);
    let security_audit = recovered.list_security_audit_records().await.unwrap();
    let rejections = &security_audit[audit_before.len()..];
    assert!(!rejections.is_empty());
    for rejection in rejections {
        assert!(rejection.event_id.starts_with("node-identity-rejection:"));
        assert!(rejection.timestamp_ms > 0);
        assert_eq!(
            rejection,
            &SecurityAuditRecord {
                format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
                event_id: rejection.event_id.clone(),
                timestamp_ms: rejection.timestamp_ms,
                actor: AuditIdentity::Unverified("worker:worker-999".to_owned()),
                target: AuditField::Unverified("worker-1".to_owned()),
                scope: AuditField::Known("cluster".to_owned()),
                action: SecurityAuditClass::NodeIdentityRejection,
                outcome: SecurityAuditOutcome::Denied,
                correlation: AuditCorrelation::Unknown,
                operation_id: AuditField::NotApplicable,
                policy_revision: AuditField::NotApplicable,
                before_state: AuditField::NotApplicable,
                after_state: AuditField::NotApplicable,
            }
        );
    }
}

#[tokio::test]
async fn operator_only_management_access_keeps_artifacts_and_denies_backup_creation() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(rockstream_test_support::docker_available());
    let bucket = "rockstream-release-operator-policy";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO is required for the operator-only artifact release proof");
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let operator = mtls_operator_with_fingerprint(certificate_sha256(&pki.secondary_cli_cert_path));
    let mut process = start_control_process_with_operator(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        Some((minio_port, bucket)),
        false,
        Some(operator),
        true,
        false,
    )
    .await;
    wait_for_gateway(&mut process.0, control_address).await;

    let mut admin = wait_for_management_client(management_address, &pki).await;
    let mut client = management_client_with_identity(
        management_address,
        &pki,
        &pki.secondary_cli_cert_path,
        &pki.secondary_cli_key_path,
    )
    .await
    .unwrap();
    assert_eq!(
        client
            .get_cluster_status(GetClusterStatusRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            })
            .await
            .unwrap()
            .into_inner()
            .protocol_version,
        rockstream_management_proto::PROTOCOL_VERSION
    );

    let idempotency_key = uuid::Uuid::new_v4().to_string();
    let artifact = client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: idempotency_key.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(artifact.opaque_id, idempotency_key);
    let generated_at_ms = artifact.expires_at_ms - 600_000;
    let expected_content = format!(
        "{{\"format_version\":1,\"generated_at_ms\":{generated_at_ms},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":1,\"active_operations\":1,\"retained_operations\":1,\"node\":null}}",
        env!("CARGO_PKG_VERSION")
    );
    let (content, expires_at_ms) =
        download_support_artifact(&mut client, &artifact.opaque_id).await;
    assert_eq!(String::from_utf8(content).unwrap(), expected_content);
    assert_eq!(expires_at_ms, artifact.expires_at_ms);

    let principal_request = rockstream_management_proto::v1::ListSecurityPrincipalsRequest {
        protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
        page_size: 100,
        page_token: String::new(),
    };
    let before_principals = admin
        .list_security_principals(principal_request.clone())
        .await
        .unwrap()
        .into_inner()
        .principals;
    let operator_before = before_principals
        .iter()
        .find(|principal| principal.principal_id == 2)
        .unwrap()
        .clone();

    let operations_request = ListOperationsRequest {
        protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
        page_size: 100,
        page_token: String::new(),
    };
    let before_denial = client
        .list_operations(operations_request.clone())
        .await
        .unwrap()
        .into_inner();
    let denied_grant_key = uuid::Uuid::new_v4().to_string();
    let grant_denial = client
        .set_security_principal_grants(SetSecurityPrincipalGrantsRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            principal_id: 2,
            expected_revision: 1,
            expected_grantor_revision: 1,
            idempotency_key: denied_grant_key.clone(),
            preset_grants: ["data_reader", "administrator"]
                .into_iter()
                .map(
                    |preset| rockstream_management_proto::v1::SecurityPresetGrant {
                        preset: preset.to_owned(),
                        scope_json: serde_json::to_string(&PolicyScope::Cluster).unwrap(),
                    },
                )
                .collect(),
            scoped_grants: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(grant_denial.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        grant_denial.message(),
        "management action is not authorized"
    );

    let backup_destination = temp.path().join("operator-only-backup");
    let denied_key = uuid::Uuid::new_v4().to_string();
    let denial = client
        .create_backup(CreateBackupRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            destination: backup_destination.to_string_lossy().into_owned(),
            idempotency_key: denied_key.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(denial.code(), tonic::Code::PermissionDenied);
    assert_eq!(denial.message(), "management action is not authorized");
    let after_denial = client
        .list_operations(operations_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(after_denial.operations, before_denial.operations);
    assert_eq!(after_denial.next_page_token, before_denial.next_page_token);
    assert!(!after_denial
        .operations
        .iter()
        .any(|operation| operation.operation_id == denied_grant_key));
    assert!(!after_denial
        .operations
        .iter()
        .any(|operation| operation.operation_id == denied_key));
    assert!(!backup_destination.exists());
    let after_principals = admin
        .list_security_principals(principal_request)
        .await
        .unwrap()
        .into_inner()
        .principals;
    let operator_after = after_principals
        .iter()
        .find(|principal| principal.principal_id == 2)
        .unwrap();
    assert_eq!(operator_after, &operator_before);

    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let security_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "gateway-shard",
    ));
    let catalog = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    catalog.refresh().await.unwrap();
    let audits = catalog.list_security_audit_records().await.unwrap();
    let denials = audits
        .iter()
        .filter(|record| record.action == SecurityAuditClass::AuthorizationDenial)
        .collect::<Vec<_>>();
    assert_eq!(denials.len(), 2);
    for (target, scope) in [
        ("GrantManage", PolicyScope::Cluster),
        (
            "BackupCreate",
            PolicyScope::BackupDestination {
                destination: backup_destination.to_string_lossy().into_owned(),
            },
        ),
    ] {
        let matching = denials
            .iter()
            .copied()
            .filter(|record| record.target == AuditField::Known(target.to_owned()))
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        let denial = matching[0];
        assert!(denial.event_id.starts_with("management-denial-"));
        assert!(denial.timestamp_ms > 0);
        assert_eq!(
            denial,
            &SecurityAuditRecord {
                format_version: SECURITY_AUDIT_FORMAT_VERSION,
                event_id: denial.event_id.clone(),
                timestamp_ms: denial.timestamp_ms,
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
            }
        );
    }
}

#[tokio::test]
async fn support_artifact_download_is_bound_to_authenticated_principal() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(rockstream_test_support::docker_available());
    let bucket = "rockstream-release-scoped-artifact-download";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO is required for scoped support-artifact qualification");
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let artifact_id = uuid::Uuid::new_v4().to_string();
    let operator = mtls_operator_with_fingerprint(certificate_sha256(&pki.secondary_cli_cert_path));
    let mut process = start_control_process_with_operator(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        Some((minio_port, bucket)),
        false,
        Some(operator),
        true,
        false,
    )
    .await;
    wait_for_gateway(&mut process.0, control_address).await;
    let mut admin = wait_for_management_client(management_address, &pki).await;
    let mut client = management_client_with_identity(
        management_address,
        &pki,
        &pki.secondary_cli_cert_path,
        &pki.secondary_cli_key_path,
    )
    .await
    .unwrap();

    let artifact = client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: artifact_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(artifact.opaque_id, artifact_id);
    let generated_at_ms = artifact.expires_at_ms - 600_000;
    let expected_content = format!(
        "{{\"format_version\":1,\"generated_at_ms\":{generated_at_ms},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":1,\"active_operations\":1,\"retained_operations\":1,\"node\":null}}",
        env!("CARGO_PKG_VERSION")
    );
    let (content, expires_at_ms) = download_support_artifact(&mut client, &artifact_id).await;
    assert_eq!(String::from_utf8(content).unwrap(), expected_content);
    assert_eq!(expires_at_ms, artifact.expires_at_ms);

    let denial = admin
        .download_support_artifact(DownloadSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            opaque_id: artifact_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(denial.code(), tonic::Code::PermissionDenied);
    assert_eq!(denial.message(), "management action is not authorized");

    process.0.kill().unwrap();
    process.0.wait().unwrap();
    let security_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "gateway-shard",
    ));
    let catalog = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    catalog.refresh().await.unwrap();
    let audits = catalog.list_security_audit_records().await.unwrap();
    let denials = audits
        .iter()
        .filter(|record| {
            record.action == SecurityAuditClass::AuthorizationDenial
                && record.target == AuditField::Known("SupportDownload".to_owned())
        })
        .collect::<Vec<_>>();
    assert_eq!(denials.len(), 1);
    let denial = denials[0];
    assert!(denial.event_id.starts_with("management-denial-"));
    assert!(denial.timestamp_ms > 0);
    assert_eq!(
        denial,
        &SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: denial.event_id.clone(),
            timestamp_ms: denial.timestamp_ms,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("SupportDownload".to_owned()),
            scope: AuditField::Known(
                serde_json::to_string(&PolicyScope::Artifact {
                    opaque_id: artifact_id,
                })
                .unwrap()
            ),
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
}

#[tokio::test]
async fn operator_with_explicit_backup_scope_can_create_backup_over_release_management_api() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(rockstream_test_support::docker_available());
    let bucket = "rockstream-release-scoped-backup";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO is required for scoped-backup release qualification");
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let backup_destination = temp.path().join("scoped-backup");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut operator =
        mtls_operator_with_fingerprint(certificate_sha256(&pki.secondary_cli_cert_path));
    operator.scoped_grants.push(ScopedActionGrant {
        action: SecurityAction::BackupCreate,
        scope: PolicyScope::BackupDestination {
            destination: backup_destination.to_string_lossy().into_owned(),
        },
    });
    let mut process = start_control_process_with_operator(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        Some((minio_port, bucket)),
        false,
        Some(operator),
        true,
        false,
    )
    .await;
    wait_for_gateway(&mut process.0, control_address).await;
    let _admin = wait_for_management_client(management_address, &pki).await;
    let mut client = management_client_with_identity(
        management_address,
        &pki,
        &pki.secondary_cli_cert_path,
        &pki.secondary_cli_key_path,
    )
    .await
    .unwrap();

    let idempotency_key = uuid::Uuid::new_v4().to_string();
    let accepted = client
        .create_backup(CreateBackupRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            destination: backup_destination.to_string_lossy().into_owned(),
            idempotency_key: idempotency_key.clone(),
        })
        .await
        .expect("separate destination-scoped grant authorizes backup creation")
        .into_inner()
        .operation
        .expect("backup operation response");
    assert_eq!(accepted.operation_id, idempotency_key);
    assert_eq!(accepted.kind, "create_backup");

    let deadline = Instant::now() + Duration::from_secs(10);
    let completed = loop {
        let operation = client
            .get_operation(GetOperationRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                operation_id: idempotency_key.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .operation
            .expect("accepted backup operation remains queryable");
        if operation.state == "succeeded" {
            break operation;
        }
        assert_ne!(
            operation.state, "failed",
            "backup operation failed: {operation:?}"
        );
        assert!(
            Instant::now() < deadline,
            "backup did not complete: {operation:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(completed.progress, "100%");
    assert_eq!(completed.phase, "completed");

    process.0.kill().unwrap();
    process.0.wait().unwrap();
}

#[tokio::test]
async fn operator_can_cancel_pending_support_artifact_over_release_management_api() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(rockstream_test_support::docker_available());
    let bucket = "rockstream-release-support-cancellation";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO is required for support-artifact cancellation qualification");
    let management_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "management",
    ));
    let security_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "gateway-shard",
    ));
    let security_catalog = DurableCatalogStore::recover(Arc::clone(&security_store), "gateway")
        .await
        .unwrap();
    let operations = ManagementOperationStore::new(Arc::clone(&management_store));
    let operation_id = uuid::Uuid::new_v4().to_string();
    let pending = operations
        .accept_idempotent(
            &operation_id,
            rockstream_management_proto::PROTOCOL_VERSION as u16,
            &serde_json::json!({ "principal_id": 2, "node_id": null }),
            operation_id.clone(),
            OperationKind::GenerateSupportArtifact,
            chrono::Utc::now().timestamp_millis().max(1),
        )
        .await
        .unwrap();
    assert_eq!(pending.status(), OperationStatus::Pending);

    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let operator = mtls_operator_with_fingerprint(certificate_sha256(&pki.secondary_cli_cert_path));
    let operator_revision = operator.revision;
    let mut process = start_control_process_with_operator(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        Some((minio_port, bucket)),
        false,
        Some(operator),
        true,
        false,
    )
    .await;
    wait_for_gateway(&mut process.0, control_address).await;
    let _admin = wait_for_management_client(management_address, &pki).await;
    let mut client = management_client_with_identity(
        management_address,
        &pki,
        &pki.secondary_cli_cert_path,
        &pki.secondary_cli_key_path,
    )
    .await
    .unwrap();

    let cancelled = client
        .cancel_operation(CancelOperationRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation_id: operation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(cancelled.operation_id, operation_id);
    assert_eq!(cancelled.kind, "generate_support_artifact");
    assert_eq!(cancelled.state, "cancelled");
    assert_eq!(cancelled.phase, "cancelled_before_start");
    security_catalog.refresh().await.unwrap();
    let audit = security_catalog
        .list_security_audit_records()
        .await
        .unwrap();
    let cancellation = audit
        .iter()
        .find(|record| record.event_id == format!("management-operation-cancel:{operation_id}"))
        .unwrap();
    assert_eq!(
        cancellation,
        &SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: format!("management-operation-cancel:{operation_id}"),
            timestamp_ms: cancellation.timestamp_ms,
            actor: AuditIdentity::Verified("principal:2".to_owned()),
            target: AuditField::Known(format!("operation:{operation_id}")),
            scope: AuditField::Known(
                serde_json::to_string(&PolicyScope::Operation {
                    operation_id: operation_id.clone(),
                })
                .unwrap(),
            ),
            action: SecurityAuditClass::AdminOperationCompletion,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Observed(operation_id.clone()),
            operation_id: AuditField::Known(operation_id.clone()),
            policy_revision: AuditField::Known(operator_revision),
            before_state: AuditField::Known("pending".to_owned()),
            after_state: AuditField::Known("cancelled_before_start".to_owned()),
        }
    );
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    process = start_control_process(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        Some((minio_port, bucket)),
        false,
    )
    .await;
    wait_for_gateway(&mut process.0, control_address).await;
    let _admin = wait_for_management_client(management_address, &pki).await;
    let mut client = management_client_with_identity(
        management_address,
        &pki,
        &pki.secondary_cli_cert_path,
        &pki.secondary_cli_key_path,
    )
    .await
    .unwrap();
    let recovered_security_catalog = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    let recovered_audit = recovered_security_catalog
        .list_security_audit_records()
        .await
        .unwrap();
    assert!(recovered_audit.contains(cancellation));
    let recovered = client
        .get_operation(GetOperationRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation_id: cancelled.operation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(recovered, cancelled);

    let retry = client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: operation_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(retry.code(), tonic::Code::FailedPrecondition);
    assert!(matches!(
        management_store
            .get(&support_artifact_object_path(&operation_id))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    process.0.kill().unwrap();
    process.0.wait().unwrap();
}

#[tokio::test]
async fn support_artifact_generation_and_cancellation_race_has_one_durable_winner() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(rockstream_test_support::docker_available());
    let bucket = "rockstream-release-support-cancel-race";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO is required for support-artifact cancellation race qualification");
    let management_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "management",
    ));
    let operations = ManagementOperationStore::new(Arc::clone(&management_store));
    let operation_id = uuid::Uuid::new_v4().to_string();
    operations
        .accept_idempotent(
            &operation_id,
            rockstream_management_proto::PROTOCOL_VERSION as u16,
            &serde_json::json!({ "principal_id": 2, "node_id": null }),
            operation_id.clone(),
            OperationKind::GenerateSupportArtifact,
            chrono::Utc::now().timestamp_millis().max(1),
        )
        .await
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let operator = mtls_operator_with_fingerprint(certificate_sha256(&pki.secondary_cli_cert_path));
    let mut process = start_control_process_with_operator(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        Some((minio_port, bucket)),
        false,
        Some(operator),
        true,
        false,
    )
    .await;
    wait_for_gateway(&mut process.0, control_address).await;
    let _admin = wait_for_management_client(management_address, &pki).await;
    let mut generator = management_client_with_identity(
        management_address,
        &pki,
        &pki.secondary_cli_cert_path,
        &pki.secondary_cli_key_path,
    )
    .await
    .unwrap();
    let mut canceller = management_client_with_identity(
        management_address,
        &pki,
        &pki.secondary_cli_cert_path,
        &pki.secondary_cli_key_path,
    )
    .await
    .unwrap();

    let (generated, cancelled) = tokio::join!(
        generator.generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: operation_id.clone(),
        }),
        canceller.cancel_operation(CancelOperationRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation_id: operation_id.clone(),
        }),
    );
    let operation = generator
        .get_operation(GetOperationRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation_id: operation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    let expected_audit = match (generated, cancelled) {
        (Ok(response), Err(error)) => {
            let response = response.into_inner();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert_eq!(operation.state, "succeeded");
            assert_eq!(
                operation.phase,
                format!("available_until_ms:{}", response.expires_at_ms)
            );
            let (content, expiry) =
                download_support_artifact(&mut generator, &response.opaque_id).await;
            let generated_at_ms = response.expires_at_ms - 600_000;
            let expected_content = format!(
                "{{\"format_version\":1,\"generated_at_ms\":{generated_at_ms},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":1,\"active_operations\":1,\"retained_operations\":1,\"node\":null}}",
                env!("CARGO_PKG_VERSION")
            );
            assert_eq!(content, expected_content.as_bytes());
            assert_eq!(expiry, response.expires_at_ms);
            vec![
                (
                    SecurityAuditClass::AdminOperationStart,
                    "pending",
                    "generating",
                ),
                (
                    SecurityAuditClass::AdminOperationCompletion,
                    "generating",
                    &operation.phase,
                ),
            ]
        }
        (Err(error), Ok(response)) => {
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            let cancelled = response.into_inner().operation.unwrap();
            assert_eq!(cancelled.state, "cancelled");
            assert_eq!(cancelled.phase, "cancelled_before_start");
            assert_eq!(operation, cancelled);
            assert!(matches!(
                management_store
                    .get(&support_artifact_object_path(&operation_id))
                    .await,
                Err(object_store::Error::NotFound { .. })
            ));
            vec![(
                SecurityAuditClass::AdminOperationCompletion,
                "pending",
                "cancelled_before_start",
            )]
        }
        (generated, cancelled) => panic!(
            "generation and cancellation returned an invalid pair: generated={}, cancelled={}",
            generated.is_ok(),
            cancelled.is_ok()
        ),
    };

    let security_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "gateway-shard",
    ));
    let security_catalog = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    let operation_audit = security_catalog
        .list_security_audit_records()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.operation_id == AuditField::Known(operation_id.clone()))
        .collect::<Vec<_>>();
    let actual_audit = operation_audit
        .iter()
        .filter(|record| record.outcome == SecurityAuditOutcome::Success)
        .map(|record| {
            (
                record.action,
                record.before_state.clone(),
                record.after_state.clone(),
            )
        })
        .collect::<Vec<_>>();
    let expected_audit = expected_audit
        .into_iter()
        .map(|(action, before, after)| {
            (
                action,
                AuditField::Known(before.to_owned()),
                AuditField::Known(after.to_owned()),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_audit, expected_audit);
    let cancellation_denials = operation_audit
        .iter()
        .filter(|record| record.outcome == SecurityAuditOutcome::Failure)
        .collect::<Vec<_>>();
    if operation.state == "succeeded" {
        assert_eq!(cancellation_denials.len(), 1);
        let denial = cancellation_denials[0];
        assert!(denial
            .event_id
            .strip_prefix("management-operation-cancel-denied:")
            .and_then(|suffix| uuid::Uuid::parse_str(suffix).ok())
            .is_some());
        assert_eq!(
            denial.actor,
            AuditIdentity::Verified("principal:2".to_owned())
        );
        assert_eq!(
            denial.target,
            AuditField::Known(format!("operation:{operation_id}"))
        );
        assert_eq!(
            denial.scope,
            AuditField::Known(
                serde_json::to_string(&PolicyScope::Operation {
                    operation_id: operation_id.clone(),
                })
                .unwrap()
            )
        );
        assert_eq!(denial.action, SecurityAuditClass::AdminOperationCompletion);
        assert_eq!(
            denial.correlation,
            AuditCorrelation::Observed(operation_id.clone())
        );
        assert_eq!(denial.operation_id, AuditField::Known(operation_id.clone()));
        assert_eq!(denial.policy_revision, AuditField::Known(1));
        assert!(matches!(
            &denial.before_state,
            AuditField::Known(state) if state.starts_with("running:") || state.starts_with("succeeded:")
        ));
        assert_eq!(denial.before_state, denial.after_state);
    } else {
        assert!(cancellation_denials.is_empty());
    }
    process.0.kill().unwrap();
    process.0.wait().unwrap();
}

#[tokio::test]
async fn support_artifact_reconciles_durable_boundaries_after_process_exit() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for support-artifact durable-boundary recovery"
    );
    let bucket = "rockstream-release-support-artifact-boundaries";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO must be available for support-artifact durable-boundary recovery");
    let scenarios = [
        "operation-accepted",
        "operation-running",
        "start-audit",
        "ledger-reserved",
        "artifact-written",
        "completion-audit",
        "operation-succeeded",
    ];
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let principal = mtls_support_admin_with_fingerprint(certificate_sha256(&pki.cli_cert_path));
    let security_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "gateway-shard",
    ));
    let security_catalog = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    let management_store = object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "management",
    );
    let ledger_path = object_store::path::Path::from("control/support-artifacts/ledger.json");
    let mut operation_ids = Vec::new();

    for (index, boundary) in scenarios.iter().enumerate() {
        let expected_actions = match *boundary {
            "start-audit" | "ledger-reserved" | "artifact-written" => vec![
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditClass::AdminOperationCompletion,
            ],
            "completion-audit" => vec![
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditClass::AdminOperationCompletion,
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditClass::AdminOperationCompletion,
            ],
            _ => vec![
                SecurityAuditClass::AdminOperationStart,
                SecurityAuditClass::AdminOperationCompletion,
            ],
        };
        let operation_id = uuid::Uuid::new_v4().to_string();
        operation_ids.push(operation_id.clone());
        let mut crashed = start_control_process_with_operator_and_crash_after(
            temp.path(),
            &storage,
            control_address,
            management_address,
            &pki,
            Some((minio_port, bucket)),
            true,
            None,
            true,
            false,
            Some(*boundary),
        )
        .await;
        wait_for_gateway(&mut crashed.0, control_address).await;
        let mut client = wait_for_management_client(management_address, &pki).await;
        assert!(
            client
                .generate_support_artifact(GenerateSupportArtifactRequest {
                    protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                    node_id: String::new(),
                    idempotency_key: operation_id.clone(),
                })
                .await
                .is_err(),
            "configured boundary {boundary} did not terminate the process"
        );
        drop(client);
        let exit_status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = crashed.0.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("support-artifact process did not exit at its durable boundary");
        assert_eq!(exit_status.code(), Some(86), "boundary {boundary}");

        let mut recovered = start_control_process(
            temp.path(),
            &storage,
            control_address,
            management_address,
            &pki,
            Some((minio_port, bucket)),
            true,
        )
        .await;
        wait_for_gateway(&mut recovered.0, control_address).await;
        let mut client = wait_for_management_client(management_address, &pki).await;
        let response = client
            .generate_support_artifact(GenerateSupportArtifactRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                node_id: String::new(),
                idempotency_key: operation_id.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        let operation = client
            .get_operation(GetOperationRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                operation_id: operation_id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .operation
            .expect("reconciled operation remains queryable");
        let started_at = chrono::DateTime::parse_from_rfc3339(&operation.started_at).unwrap();
        let updated_at = chrono::DateTime::parse_from_rfc3339(&operation.updated_at).unwrap();
        assert_eq!(
            response.expires_at_ms,
            started_at.timestamp_millis() as u64 + 600_000
        );
        assert!(updated_at >= started_at);
        assert_eq!(
            response,
            GenerateSupportArtifactResponse {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                opaque_id: operation_id.clone(),
                expires_at_ms: response.expires_at_ms,
            }
        );
        assert_eq!(
            operation,
            rockstream_management_proto::v1::Operation {
                operation_id: operation_id.clone(),
                kind: "generate_support_artifact".to_owned(),
                state: "succeeded".to_owned(),
                started_at: operation.started_at.clone(),
                updated_at: operation.updated_at.clone(),
                progress: "100%".to_owned(),
                phase: format!("available_until_ms:{}", response.expires_at_ms),
                error_code: String::new(),
                next_steps: Vec::new(),
                source_version: format!("operation-record:{MANAGEMENT_OPERATION_RECORD_VERSION}"),
                result_json: String::new(),
            }
        );

        let expected_content = format!(
            "{{\"format_version\":1,\"generated_at_ms\":{},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":1,\"active_operations\":1,\"retained_operations\":{},\"node\":null}}",
            response.expires_at_ms - 600_000,
            env!("CARGO_PKG_VERSION"),
            index + 1
        );
        let (content, expires_at_ms) =
            download_support_artifact(&mut client, &response.opaque_id).await;
        assert_eq!(String::from_utf8(content).unwrap(), expected_content);
        assert_eq!(expires_at_ms, response.expires_at_ms);

        security_catalog.refresh().await.unwrap();
        let mut audit = security_catalog
            .list_security_audit_records()
            .await
            .unwrap()
            .into_iter()
            .filter(|record| record.operation_id == AuditField::Known(operation_id.clone()))
            .collect::<Vec<_>>();
        audit.sort_by_key(|record| record.timestamp_ms);
        assert_eq!(
            audit.iter().map(|record| record.action).collect::<Vec<_>>(),
            expected_actions
        );
        for record in audit {
            let event_id = record
                .event_id
                .strip_prefix("support-artifact-")
                .expect("support-artifact audit event prefix");
            assert!(uuid::Uuid::parse_str(event_id).is_ok());
            let (before_state, after_state) = match record.action {
                SecurityAuditClass::AdminOperationStart => (
                    AuditField::Known("pending".to_owned()),
                    AuditField::Known("generating".to_owned()),
                ),
                SecurityAuditClass::AdminOperationCompletion => (
                    AuditField::Known("generating".to_owned()),
                    AuditField::Known(format!("available_until_ms:{}", response.expires_at_ms)),
                ),
                action => panic!("unexpected support-artifact audit action: {action:?}"),
            };
            assert_eq!(
                record,
                SecurityAuditRecord {
                    format_version: SECURITY_AUDIT_FORMAT_VERSION,
                    event_id: record.event_id.clone(),
                    timestamp_ms: record.timestamp_ms,
                    actor: AuditIdentity::Verified(format!("principal:{}", principal.id.0)),
                    target: AuditField::Known(format!("support-artifact:{operation_id}")),
                    scope: AuditField::Known(serde_json::to_string(&PolicyScope::Cluster).unwrap()),
                    action: record.action,
                    outcome: SecurityAuditOutcome::Success,
                    correlation: AuditCorrelation::Observed(operation_id.clone()),
                    operation_id: AuditField::Known(operation_id.clone()),
                    policy_revision: AuditField::Known(principal.revision),
                    before_state,
                    after_state,
                }
            );
        }
        recovered.0.kill().unwrap();
        recovered.0.wait().unwrap();
    }

    let ledger = serde_json::from_slice::<serde_json::Value>(
        &management_store
            .get(&ledger_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    let mut actual_ids = ledger["artifacts"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    actual_ids.sort();
    operation_ids.sort();
    assert_eq!(actual_ids, operation_ids);

    let expired_id = operation_ids[0].clone();
    let expired_path = support_artifact_object_path(&expired_id);
    let survivor_id = operation_ids[1].clone();
    let survivor_path = support_artifact_object_path(&survivor_id);
    let survivor_bytes = management_store
        .get(&survivor_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let ledger_bytes = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
    ledger["artifacts"][&expired_id]["expires_at_ms"] = serde_json::json!(1);
    management_store
        .put(&ledger_path, serde_json::to_vec(&ledger).unwrap().into())
        .await
        .unwrap();

    let mut cleanup_crashed = start_control_process_with_operator_and_crash_after(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        Some((minio_port, bucket)),
        true,
        None,
        true,
        true,
        Some("cleanup-after-delete"),
    )
    .await;
    let exit_status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = cleanup_crashed.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("support-artifact cleanup process did not exit at its delete boundary");
    assert_eq!(exit_status.code(), Some(86));

    let mut recovered = start_control_process(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        Some((minio_port, bucket)),
        true,
    )
    .await;
    wait_for_gateway(&mut recovered.0, control_address).await;
    let mut client = wait_for_management_client(management_address, &pki).await;
    let expired_download = client
        .download_support_artifact(DownloadSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            opaque_id: expired_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(expired_download.code(), tonic::Code::NotFound);
    assert_eq!(
        expired_download.message(),
        "support artifact not found or expired"
    );
    assert!(matches!(
        management_store.get(&expired_path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    assert_eq!(
        management_store
            .get(&survivor_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        survivor_bytes
    );
    let ledger = serde_json::from_slice::<serde_json::Value>(
        &management_store
            .get(&ledger_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    let mut recovered_ids = ledger["artifacts"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    recovered_ids.sort();
    assert_eq!(
        recovered_ids,
        operation_ids
            .into_iter()
            .filter(|id| id != &expired_id)
            .collect::<Vec<_>>()
    );
    recovered.0.kill().unwrap();
    recovered.0.wait().unwrap();
}

#[tokio::test]
async fn concurrent_processes_reconcile_same_support_artifact_request() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(rockstream_test_support::docker_available());
    let bucket = "rockstream-release-support-contention";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO is required for multi-process support-artifact contention");
    let temp = tempfile::tempdir().unwrap();
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_a = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_a = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let control_b = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_b = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let minio = Some((minio_port, bucket));
    let mut process_a = start_control_process(
        temp.path(),
        &temp.path().join("control-a"),
        control_a,
        management_a,
        &pki,
        minio,
        true,
    )
    .await;
    wait_for_gateway(&mut process_a.0, control_a).await;
    let mut process_b = start_control_process(
        temp.path(),
        &temp.path().join("control-b"),
        control_b,
        management_b,
        &pki,
        minio,
        true,
    )
    .await;
    wait_for_gateway(&mut process_b.0, control_b).await;
    let mut client_a = wait_for_management_client(management_a, &pki).await;
    let mut client_b = wait_for_management_client(management_b, &pki).await;
    let idempotency_key = uuid::Uuid::new_v4().to_string();
    let request = || GenerateSupportArtifactRequest {
        protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
        node_id: String::new(),
        idempotency_key: idempotency_key.clone(),
    };

    let (first, retry) = tokio::join!(
        client_a.generate_support_artifact(request()),
        client_b.generate_support_artifact(request()),
    );
    let first = first.unwrap().into_inner();
    let retry = retry.unwrap().into_inner();
    assert_eq!(first, retry);
    assert_eq!(first.opaque_id, idempotency_key);

    let get_operation = || GetOperationRequest {
        protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
        operation_id: first.opaque_id.clone(),
    };
    let (operation_a, operation_b) = tokio::join!(
        client_a.get_operation(get_operation()),
        client_b.get_operation(get_operation()),
    );
    let operation_a = operation_a.unwrap().into_inner().operation.unwrap();
    let operation_b = operation_b.unwrap().into_inner().operation.unwrap();
    assert_eq!(operation_a, operation_b);
    assert_eq!(operation_a.operation_id, idempotency_key);
    assert_eq!(operation_a.kind, "generate_support_artifact");
    assert_eq!(operation_a.state, "succeeded");
    assert_eq!(operation_a.progress, "100%");
    assert_eq!(
        operation_a.phase,
        format!("available_until_ms:{}", first.expires_at_ms)
    );

    let (content_a, expiry_a) = download_support_artifact(&mut client_a, &first.opaque_id).await;
    let (content_b, expiry_b) = download_support_artifact(&mut client_b, &first.opaque_id).await;
    let generated_at_ms = first.expires_at_ms - 600_000;
    let expected_content = format!(
        "{{\"format_version\":1,\"generated_at_ms\":{generated_at_ms},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":1,\"active_operations\":1,\"retained_operations\":1,\"node\":null}}",
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(content_a, expected_content.as_bytes());
    assert_eq!(content_b, expected_content.as_bytes());
    assert_eq!(expiry_a, first.expires_at_ms);
    assert_eq!(expiry_b, first.expires_at_ms);

    process_a.0.kill().unwrap();
    process_a.0.wait().unwrap();
    process_b.0.kill().unwrap();
    process_b.0.wait().unwrap();
}

#[tokio::test]
async fn support_artifact_runs_in_release_process_and_recovers_after_restart() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(
        rockstream_test_support::docker_available(),
        "Docker is required for the MinIO-backed support-artifact release proof"
    );
    let bucket = "rockstream-release-support-artifacts";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO must be available for the support-artifact release proof");
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let minio = Some((minio_port, bucket));
    let mut process = start_control_process(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        minio,
        true,
    )
    .await;
    wait_for_gateway(&mut process.0, control_address).await;

    let idempotency_key = uuid::Uuid::new_v4().to_string();
    let mut client = wait_for_management_client(management_address, &pki).await;
    let first = client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: idempotency_key.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.opaque_id, idempotency_key);
    let generated_at_ms = first.expires_at_ms - 600_000;
    let expected_content = format!(
        "{{\"format_version\":1,\"generated_at_ms\":{generated_at_ms},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":1,\"active_operations\":1,\"retained_operations\":1,\"node\":null}}",
        env!("CARGO_PKG_VERSION")
    );
    let (downloaded, download_expiry) =
        download_support_artifact(&mut client, &first.opaque_id).await;
    assert_eq!(String::from_utf8(downloaded).unwrap(), expected_content);
    assert_eq!(download_expiry, first.expires_at_ms);

    let management_store = object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "management",
    );
    let ledger_path = object_store::path::Path::from("control/support-artifacts/ledger.json");
    let pressure_id = uuid::Uuid::new_v4().to_string();
    let pressure_blob_path = support_artifact_object_path(&pressure_id);
    let pressure_size = 10 * 1024 * 1024;
    let pressure_payload = vec![0u8; pressure_size];
    let ledger_bytes = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
    ledger["artifacts"][&pressure_id] = serde_json::json!({
        "owner": PrincipalId(1),
        "scope": PolicyScope::Cluster,
        "size": pressure_payload.len(),
        "expires_at_ms": first.expires_at_ms,
    });
    management_store
        .put(&pressure_blob_path, pressure_payload.into())
        .await
        .unwrap();
    management_store
        .put(&ledger_path, serde_json::to_vec(&ledger).unwrap().into())
        .await
        .unwrap();

    let download_request = || DownloadSupportArtifactRequest {
        protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
        opaque_id: pressure_id.clone(),
    };
    let mut open_downloads = Vec::new();
    for _ in 0..8 {
        open_downloads.push(
            client
                .download_support_artifact(download_request())
                .await
                .unwrap()
                .into_inner(),
        );
    }
    let over_limit = match client.download_support_artifact(download_request()).await {
        Ok(_) => panic!("ninth concurrent support-artifact download was admitted"),
        Err(error) => error,
    };
    assert_eq!(over_limit.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        over_limit.message(),
        "support artifact download limit reached"
    );
    let mut full_stream = open_downloads
        .pop()
        .expect("one admitted stream remains open");
    drop(open_downloads);
    let deadline = Instant::now() + Duration::from_secs(5);
    let recovered_stream = loop {
        match client.download_support_artifact(download_request()).await {
            Ok(response) => break response.into_inner(),
            Err(error)
                if error.code() == tonic::Code::ResourceExhausted && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("download capacity did not recover after stream close: {error}"),
        }
    };
    drop(recovered_stream);

    let mut downloaded = Vec::new();
    let mut expected_offset = 0;
    let mut expires_at_ms = None;
    let mut final_chunk = false;
    while let Some(chunk) = full_stream.message().await.unwrap() {
        assert_eq!(chunk.offset, expected_offset);
        expected_offset += chunk.content.len() as u64;
        expires_at_ms = Some(chunk.expires_at_ms);
        downloaded.extend_from_slice(&chunk.content);
        final_chunk = chunk.final_chunk;
        if final_chunk {
            break;
        }
    }
    assert!(final_chunk);
    assert!(full_stream.message().await.unwrap().is_none());
    assert_eq!(downloaded.len(), pressure_size);
    assert!(downloaded.iter().all(|byte| *byte == 0));
    assert_eq!(expires_at_ms, Some(first.expires_at_ms));
    let completed_stream = client
        .download_support_artifact(download_request())
        .await
        .expect("completed download should release its permit")
        .into_inner();
    drop(completed_stream);

    management_store.delete(&pressure_blob_path).await.unwrap();
    let ledger_bytes = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
    ledger["artifacts"]
        .as_object_mut()
        .unwrap()
        .remove(&pressure_id);
    management_store
        .put(&ledger_path, serde_json::to_vec(&ledger).unwrap().into())
        .await
        .unwrap();

    let expired_id = uuid::Uuid::new_v4().to_string();
    let expired_content = b"{\"format_version\":1,\"safe\":\"expired fixture\"}";
    let expired_blob_path = support_artifact_object_path(&expired_id);
    let foreign_id = uuid::Uuid::new_v4().to_string();
    let foreign_content = b"{\"format_version\":1,\"safe\":\"foreign fixture\"}";
    let foreign_blob_path = support_artifact_object_path(&foreign_id);
    let first_blob_path = support_artifact_object_path(&first.opaque_id);
    let stored_artifacts = management_store
        .list_with_delimiter(Some(&object_store::path::Path::from(
            "control/support-artifacts",
        )))
        .await
        .unwrap()
        .objects
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect::<Vec<_>>();
    assert!(
        stored_artifacts
            .iter()
            .any(|path| path == &first_blob_path.to_string()),
        "support artifact object paths: {stored_artifacts:?}"
    );
    assert_eq!(
        management_store
            .get(&first_blob_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        expected_content.as_bytes()
    );
    let ledger_bytes = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
    ledger["artifacts"][&foreign_id] = serde_json::json!({
        "owner": PrincipalId(2),
        "scope": PolicyScope::Cluster,
        "size": foreign_content.len(),
        "expires_at_ms": first.expires_at_ms,
    });
    management_store
        .put(&foreign_blob_path, foreign_content.to_vec().into())
        .await
        .unwrap();
    let ledger_with_foreign_artifact = serde_json::to_vec(&ledger).unwrap();
    management_store
        .put(&ledger_path, ledger_with_foreign_artifact.clone().into())
        .await
        .unwrap();
    let foreign_denial = client
        .download_support_artifact(DownloadSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            opaque_id: foreign_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(foreign_denial.code(), tonic::Code::NotFound);
    assert_eq!(
        foreign_denial.message(),
        "support artifact not found or expired"
    );
    assert_eq!(
        management_store
            .get(&ledger_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        ledger_with_foreign_artifact
    );
    assert_eq!(
        management_store
            .get(&foreign_blob_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        foreign_content
    );
    let live_expiry_id = uuid::Uuid::new_v4().to_string();
    let live_expiry_content = b"{\"format_version\":1,\"safe\":\"live expiry fixture\"}";
    let live_expiry_blob_path = support_artifact_object_path(&live_expiry_id);
    let live_expiry_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 3_000;
    let mut ledger: serde_json::Value = serde_json::from_slice(
        &management_store
            .get(&ledger_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    management_store
        .put(&live_expiry_blob_path, live_expiry_content.to_vec().into())
        .await
        .unwrap();
    ledger["artifacts"][&live_expiry_id] = serde_json::json!({
        "owner": PrincipalId(1),
        "scope": PolicyScope::Cluster,
        "size": live_expiry_content.len(),
        "expires_at_ms": live_expiry_ms,
    });
    management_store
        .put(&ledger_path, serde_json::to_vec(&ledger).unwrap().into())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(
        live_expiry_ms.saturating_sub(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        ),
    ))
    .await;
    let expired_download = client
        .download_support_artifact(DownloadSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            opaque_id: live_expiry_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(expired_download.code(), tonic::Code::NotFound);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blob_deleted = match management_store.get(&live_expiry_blob_path).await {
                Err(object_store::Error::NotFound { .. }) => true,
                Err(error) => panic!("live expiry blob lookup failed: {error:?}"),
                Ok(_) => false,
            };
            let ledger_bytes = management_store
                .get(&ledger_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
            if blob_deleted
                && !ledger["artifacts"]
                    .as_object()
                    .unwrap()
                    .contains_key(&live_expiry_id)
            {
                break ledger;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("live artifact cleanup must finish within 10 seconds");
    assert_eq!(
        management_store
            .get(&first_blob_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        expected_content.as_bytes()
    );
    process.0.kill().unwrap();
    process.0.wait().unwrap();
    // Seed expiry only after stopping the cleaner so startup recovery owns this fixture.
    let expired_at_ms = generated_at_ms.saturating_sub(60_000);
    let mut ledger: serde_json::Value = serde_json::from_slice(
        &management_store
            .get(&ledger_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap();
    management_store
        .put(&expired_blob_path, expired_content.to_vec().into())
        .await
        .unwrap();
    ledger["artifacts"][&expired_id] = serde_json::json!({
        "owner": PrincipalId(1),
        "scope": PolicyScope::Cluster,
        "size": expired_content.len(),
        "expires_at_ms": expired_at_ms,
    });
    management_store
        .put(&ledger_path, serde_json::to_vec(&ledger).unwrap().into())
        .await
        .unwrap();
    assert_eq!(
        management_store
            .get(&expired_blob_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        expired_content
    );
    let mut restarted = start_control_process(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        minio,
        true,
    )
    .await;
    wait_for_gateway(&mut restarted.0, control_address).await;
    let mut client = wait_for_management_client(management_address, &pki).await;
    let retry = client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: idempotency_key.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(retry, first);
    let (recovered_content, recovered_expiry) =
        download_support_artifact(&mut client, &first.opaque_id).await;
    assert_eq!(
        String::from_utf8(recovered_content).unwrap(),
        expected_content
    );
    assert_eq!(recovered_expiry, first.expires_at_ms);
    let recovered_ledger = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let recovered_ids = serde_json::from_slice::<serde_json::Value>(&recovered_ledger).unwrap()
        ["artifacts"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let mut expected_recovered_ids = vec![idempotency_key.clone(), foreign_id.clone()];
    expected_recovered_ids.sort();
    assert_eq!(recovered_ids, expected_recovered_ids);
    assert_eq!(
        management_store
            .get(&foreign_blob_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        foreign_content
    );
    match management_store.get(&expired_blob_path).await {
        Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => panic!("expired artifact lookup failed unexpectedly: {error:?}"),
        Ok(_) => panic!("expired artifact bytes remained after startup cleanup"),
    }
    assert_eq!(
        client
            .download_support_artifact(DownloadSupportArtifactRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                opaque_id: expired_id.clone(),
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );

    let fingerprint = {
        let mut certificate_pem =
            std::io::BufReader::new(std::fs::File::open(&pki.cli_cert_path).unwrap());
        let certificate = rustls_pemfile::certs(&mut certificate_pem)
            .next()
            .unwrap()
            .unwrap();
        sha2::Sha256::digest(certificate.as_ref()).into()
    };
    let principal = mtls_support_admin_with_fingerprint(fingerprint);
    let policy = serde_json::to_string(&(
        principal.enabled,
        &principal.preset_grants,
        &principal.scoped_grants,
    ))
    .unwrap();
    let bootstrap_state = format!("revision={};policy={policy}", principal.revision);
    let cluster_scope = serde_json::to_string(&PolicyScope::Cluster).unwrap();
    let artifact_scope = serde_json::to_string(&PolicyScope::Artifact {
        opaque_id: idempotency_key.clone(),
    })
    .unwrap();
    let pressure_scope = serde_json::to_string(&PolicyScope::Artifact {
        opaque_id: pressure_id.clone(),
    })
    .unwrap();
    let live_expiry_scope = serde_json::to_string(&PolicyScope::Artifact {
        opaque_id: live_expiry_id.clone(),
    })
    .unwrap();
    let mut expected_audit = vec![
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "first-admin-bootstrap:1".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Unknown,
            target: AuditField::Known("principal:1".to_owned()),
            scope: AuditField::Known("cluster".to_owned()),
            action: SecurityAuditClass::GrantChange,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::NotApplicable,
            operation_id: AuditField::Known("1".to_owned()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::Known(bootstrap_state),
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "management-authn".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("management-api".to_owned()),
            scope: AuditField::NotApplicable,
            action: SecurityAuditClass::AuthenticationSuccess,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "support-artifact".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("support-artifact:{idempotency_key}")),
            scope: AuditField::Known(cluster_scope.clone()),
            action: SecurityAuditClass::AdminOperationStart,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Observed(idempotency_key.clone()),
            operation_id: AuditField::Known(idempotency_key.clone()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::Known("pending".to_owned()),
            after_state: AuditField::Known("generating".to_owned()),
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "support-artifact".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("support-artifact:{idempotency_key}")),
            scope: AuditField::Known(cluster_scope),
            action: SecurityAuditClass::AdminOperationCompletion,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Observed(idempotency_key.clone()),
            operation_id: AuditField::Known(idempotency_key.clone()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::Known("generating".to_owned()),
            after_state: AuditField::Known(format!("available_until_ms:{}", first.expires_at_ms)),
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "support-artifact".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("support-artifact:{idempotency_key}")),
            scope: AuditField::Known(artifact_scope.clone()),
            action: SecurityAuditClass::AdminOperationStart,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "support-artifact".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("support-artifact:{foreign_id}")),
            scope: AuditField::Known(
                serde_json::to_string(&PolicyScope::Artifact {
                    opaque_id: foreign_id.clone(),
                })
                .unwrap(),
            ),
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "support-artifact".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("support-artifact:{live_expiry_id}")),
            scope: AuditField::Known(live_expiry_scope),
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "management-authn".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("management-api".to_owned()),
            scope: AuditField::NotApplicable,
            action: SecurityAuditClass::AuthenticationSuccess,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "support-artifact".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("support-artifact:{idempotency_key}")),
            scope: AuditField::Known(artifact_scope),
            action: SecurityAuditClass::AdminOperationStart,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "support-artifact".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("support-artifact:{expired_id}")),
            scope: AuditField::Known(
                serde_json::to_string(&PolicyScope::Artifact {
                    opaque_id: expired_id.clone(),
                })
                .unwrap(),
            ),
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        },
    ];
    expected_audit.splice(
        5..5,
        std::iter::repeat_n(
            SecurityAuditRecord {
                format_version: SECURITY_AUDIT_FORMAT_VERSION,
                event_id: "support-artifact".to_owned(),
                timestamp_ms: 0,
                actor: AuditIdentity::Verified("principal:1".to_owned()),
                target: AuditField::Known(format!("support-artifact:{pressure_id}")),
                scope: AuditField::Known(pressure_scope),
                action: SecurityAuditClass::AdminOperationStart,
                outcome: SecurityAuditOutcome::Success,
                correlation: AuditCorrelation::Unknown,
                operation_id: AuditField::NotApplicable,
                policy_revision: AuditField::Known(1),
                before_state: AuditField::NotApplicable,
                after_state: AuditField::NotApplicable,
            },
            10,
        ),
    );
    let security_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::prefix::PrefixStore::new(
            rockstream_test_support::minio::minio_object_store(minio_port, bucket),
            "gateway-shard",
        ));
    let recovered = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    let mut actual_audit = recovered.list_security_audit_records().await.unwrap();
    for record in &mut actual_audit {
        record.timestamp_ms = 0;
        if record.event_id.starts_with("support-artifact-") {
            record.event_id = "support-artifact".to_owned();
        } else if record.event_id.starts_with("management-authn-") {
            record.event_id = "management-authn".to_owned();
        }
    }
    assert_eq!(actual_audit.len(), expected_audit.len());
    for (index, (actual, expected)) in actual_audit.iter().zip(&expected_audit).enumerate() {
        assert_eq!(actual, expected, "audit record {index}");
    }

    restarted.0.kill().unwrap();
    restarted.0.wait().unwrap();
    let recovery_id = uuid::Uuid::new_v4().to_string();
    let started_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let expires_at_ms = (started_at_ms + 600_000) as u64;
    let operations =
        ManagementOperationStore::new(Arc::new(object_store::prefix::PrefixStore::new(
            rockstream_test_support::minio::minio_object_store(minio_port, bucket),
            "management",
        )));
    let pending = operations
        .accept_idempotent(
            &recovery_id,
            rockstream_management_proto::PROTOCOL_VERSION as u16,
            &serde_json::json!({ "principal_id": 1, "node_id": null }),
            recovery_id.clone(),
            OperationKind::GenerateSupportArtifact,
            started_at_ms,
        )
        .await
        .unwrap();
    assert_eq!(pending.status(), OperationStatus::Pending);
    operations
        .transition_if(
            &recovery_id,
            OperationStatus::Pending,
            None,
            OperationUpdate {
                status: OperationStatus::Running,
                updated_at_ms: started_at_ms,
                progress: Some(0),
                phase: Some("generating_support_artifact".to_owned()),
                error_code: None,
                next_steps: Vec::new(),
            },
        )
        .await
        .unwrap();

    let recovery_content = format!(
        "{{\"format_version\":1,\"generated_at_ms\":{started_at_ms},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":1,\"active_operations\":1,\"retained_operations\":2,\"node\":null}}",
        env!("CARGO_PKG_VERSION")
    );
    let recovery_start_audit = SecurityAuditRecord {
        format_version: SECURITY_AUDIT_FORMAT_VERSION,
        event_id: format!("test-seeded-start:{recovery_id}"),
        timestamp_ms: started_at_ms as u64,
        actor: AuditIdentity::Verified("principal:1".to_owned()),
        target: AuditField::Known(format!("support-artifact:{recovery_id}")),
        scope: AuditField::Known(serde_json::to_string(&PolicyScope::Cluster).unwrap()),
        action: SecurityAuditClass::AdminOperationStart,
        outcome: SecurityAuditOutcome::Success,
        correlation: AuditCorrelation::Observed(recovery_id.clone()),
        operation_id: AuditField::Known(recovery_id.clone()),
        policy_revision: AuditField::Known(principal.revision),
        before_state: AuditField::Known("pending".to_owned()),
        after_state: AuditField::Known("generating".to_owned()),
    };
    let recovery_security_store: Arc<dyn ObjectStore> =
        Arc::new(object_store::prefix::PrefixStore::new(
            rockstream_test_support::minio::minio_object_store(minio_port, bucket),
            "gateway-shard",
        ));
    let recovery_catalog = DurableCatalogStore::recover(recovery_security_store, "gateway")
        .await
        .unwrap();
    let revision = recovery_catalog.get_revision().await + 1;
    recovery_catalog
        .commit_txn(
            CatalogTxn::new(
                revision,
                uuid::Uuid::new_v4().as_u128() as u64,
                vec![CatalogMutation::AppendSecurityAudit(
                    recovery_start_audit.clone(),
                )],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let recovery_blob_path = support_artifact_object_path(&recovery_id);
    let ledger_bytes = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
    ledger["artifacts"][&recovery_id] = serde_json::json!({
        "owner": PrincipalId(1),
        "scope": PolicyScope::Cluster,
        "size": recovery_content.len(),
        "expires_at_ms": expires_at_ms,
    });
    management_store
        .put(&ledger_path, serde_json::to_vec(&ledger).unwrap().into())
        .await
        .unwrap();
    assert!(matches!(
        management_store.get(&recovery_blob_path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    let mut recovered_process = start_control_process(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        minio,
        true,
    )
    .await;
    wait_for_gateway(&mut recovered_process.0, control_address).await;
    let mut recovery_client = wait_for_management_client(management_address, &pki).await;
    let recovered = recovery_client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: recovery_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        recovered,
        rockstream_management_proto::v1::GenerateSupportArtifactResponse {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            opaque_id: recovery_id.clone(),
            expires_at_ms,
        }
    );
    let (recovered_content, download_expiry) =
        download_support_artifact(&mut recovery_client, &recovery_id).await;
    assert_eq!(recovered_content, recovery_content.as_bytes());
    assert_eq!(download_expiry, expires_at_ms);
    assert_eq!(
        management_store
            .get(&recovery_blob_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        recovery_content.as_bytes()
    );
    let operation = recovery_client
        .get_operation(GetOperationRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation_id: recovery_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(operation.operation_id, recovery_id);
    assert_eq!(operation.kind, "generate_support_artifact");
    assert_eq!(operation.state, "succeeded");
    assert_eq!(operation.progress, "100%");
    assert_eq!(
        operation.phase,
        format!("available_until_ms:{expires_at_ms}")
    );
    assert!(operation.error_code.is_empty());
    assert!(operation.next_steps.is_empty());
    assert!(operation.result_json.is_empty());

    recovery_catalog.refresh().await.unwrap();
    let mut recovery_audit = recovery_catalog
        .list_security_audit_records()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.operation_id == AuditField::Known(recovery_id.clone()))
        .collect::<Vec<_>>();
    let retry_start_audit = SecurityAuditRecord {
        event_id: "support-artifact".to_owned(),
        timestamp_ms: 0,
        ..recovery_start_audit.clone()
    };
    let completion_audit = SecurityAuditRecord {
        event_id: "support-artifact".to_owned(),
        timestamp_ms: 0,
        action: SecurityAuditClass::AdminOperationCompletion,
        before_state: AuditField::Known("generating".to_owned()),
        after_state: AuditField::Known(format!("available_until_ms:{expires_at_ms}")),
        ..recovery_start_audit.clone()
    };
    for record in &mut recovery_audit {
        record.timestamp_ms = 0;
        if record.event_id.starts_with("support-artifact-") {
            record.event_id = "support-artifact".to_owned();
        }
    }
    let mut expected_recovery_audit = vec![
        SecurityAuditRecord {
            timestamp_ms: 0,
            ..recovery_start_audit
        },
        retry_start_audit,
        completion_audit,
    ];
    let sort_audits = |records: &mut Vec<SecurityAuditRecord>| {
        records.sort_by_key(|record| serde_json::to_string(record).unwrap());
    };
    sort_audits(&mut recovery_audit);
    sort_audits(&mut expected_recovery_audit);
    assert_eq!(recovery_audit, expected_recovery_audit);
    recovered_process.0.kill().unwrap();
    recovered_process.0.wait().unwrap();
}

#[tokio::test]
async fn support_artifact_generation_rejects_full_deployment_budget() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(rockstream_test_support::docker_available());
    let bucket = "rockstream-release-support-budget";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO must be available for the support-artifact budget proof");
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_control_process(
        temp.path(),
        &storage,
        control_address,
        management_address,
        &pki,
        Some((minio_port, bucket)),
        true,
    )
    .await;
    wait_for_gateway(&mut process.0, control_address).await;
    let mut client = wait_for_management_client(management_address, &pki).await;

    let management_store = object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "management",
    );
    let ledger_path = object_store::path::Path::from("control/support-artifacts/ledger.json");
    let ledger_bytes = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
    let expiry_ms = chrono::Utc::now().timestamp_millis().max(1) as u64 + 600_000;
    let mut expected_ids = (0..10)
        .map(|_| uuid::Uuid::new_v4().to_string())
        .collect::<Vec<_>>();
    expected_ids.sort();
    for id in &expected_ids {
        ledger["artifacts"][id] = serde_json::json!({
            "owner": PrincipalId(1),
            "scope": PolicyScope::Cluster,
            "size": 10 * 1024 * 1024,
            "expires_at_ms": expiry_ms,
        });
    }
    management_store
        .put(&ledger_path, serde_json::to_vec(&ledger).unwrap().into())
        .await
        .unwrap();

    let idempotency_key = uuid::Uuid::new_v4().to_string();
    let error = client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: idempotency_key.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(error.message(), "support artifact storage budget is full");

    let recovered_ledger = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let actual_ids = serde_json::from_slice::<serde_json::Value>(&recovered_ledger).unwrap()
        ["artifacts"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(actual_ids, expected_ids);
    assert!(matches!(
        management_store
            .get(&support_artifact_object_path(&idempotency_key))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));

    let mut count_ledger = ledger.clone();
    let mut count_artifacts = serde_json::Map::new();
    for index in 0..2_048 {
        count_artifacts.insert(
            format!("count-seed-{index:04}"),
            serde_json::json!({
                "owner": PrincipalId(1),
                "scope": PolicyScope::Cluster,
                "size": 0,
                "expires_at_ms": expiry_ms,
            }),
        );
    }
    count_ledger["artifacts"] = serde_json::Value::Object(count_artifacts);
    let expected_count_ledger = serde_json::to_vec(&count_ledger).unwrap();
    management_store
        .put(&ledger_path, expected_count_ledger.clone().into())
        .await
        .unwrap();

    let count_key = uuid::Uuid::new_v4().to_string();
    let count_error = client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: count_key.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(count_error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        count_error.message(),
        "support artifact storage budget is full"
    );
    assert_eq!(
        management_store
            .get(&ledger_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        expected_count_ledger.as_slice()
    );
    assert!(matches!(
        management_store
            .get(&support_artifact_object_path(&count_key))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
    let count_operation = client
        .get_operation(GetOperationRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            operation_id: count_key.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(count_operation.operation_id, count_key);
    assert_eq!(count_operation.kind, "generate_support_artifact");
    assert_eq!(count_operation.state, "failed");
    assert_eq!(count_operation.progress, "100%");
    assert_eq!(count_operation.phase, "generation_failed");
    assert!(count_operation.error_code.is_empty());
    assert!(count_operation.next_steps.is_empty());
    assert!(count_operation.result_json.is_empty());

    let fingerprint = {
        let mut certificate_pem =
            std::io::BufReader::new(std::fs::File::open(&pki.cli_cert_path).unwrap());
        let certificate = rustls_pemfile::certs(&mut certificate_pem)
            .next()
            .unwrap()
            .unwrap();
        sha2::Sha256::digest(certificate.as_ref()).into()
    };
    let principal = mtls_support_admin_with_fingerprint(fingerprint);
    let policy = serde_json::to_string(&(
        principal.enabled,
        &principal.preset_grants,
        &principal.scoped_grants,
    ))
    .unwrap();
    let bootstrap_state = format!("revision={};policy={policy}", principal.revision);
    let cluster_scope = serde_json::to_string(&PolicyScope::Cluster).unwrap();
    let mut expected_audit = vec![
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "first-admin-bootstrap:1".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Unknown,
            target: AuditField::Known("principal:1".to_owned()),
            scope: AuditField::Known("cluster".to_owned()),
            action: SecurityAuditClass::GrantChange,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::NotApplicable,
            operation_id: AuditField::Known("1".to_owned()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::Known(bootstrap_state),
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "management-authn".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known("management-api".to_owned()),
            scope: AuditField::NotApplicable,
            action: SecurityAuditClass::AuthenticationSuccess,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::Known(1),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "support-artifact".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("support-artifact:{idempotency_key}")),
            scope: AuditField::Known(cluster_scope.clone()),
            action: SecurityAuditClass::AdminOperationStart,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::Observed(idempotency_key.clone()),
            operation_id: AuditField::Known(idempotency_key.clone()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::Known("pending".to_owned()),
            after_state: AuditField::Known("generating".to_owned()),
        },
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "support-artifact".to_owned(),
            timestamp_ms: 0,
            actor: AuditIdentity::Verified("principal:1".to_owned()),
            target: AuditField::Known(format!("support-artifact:{idempotency_key}")),
            scope: AuditField::Known(cluster_scope),
            action: SecurityAuditClass::AdminOperationCompletion,
            outcome: SecurityAuditOutcome::Failure,
            correlation: AuditCorrelation::Observed(idempotency_key.clone()),
            operation_id: AuditField::Known(idempotency_key.clone()),
            policy_revision: AuditField::Known(1),
            before_state: AuditField::Known("generating".to_owned()),
            after_state: AuditField::Known("generation_failed".to_owned()),
        },
    ];
    let mut count_start_audit = expected_audit[2].clone();
    count_start_audit.target = AuditField::Known(format!("support-artifact:{count_key}"));
    count_start_audit.correlation = AuditCorrelation::Observed(count_key.clone());
    count_start_audit.operation_id = AuditField::Known(count_key.clone());
    let mut count_completion_audit = expected_audit[3].clone();
    count_completion_audit.target = AuditField::Known(format!("support-artifact:{count_key}"));
    count_completion_audit.correlation = AuditCorrelation::Observed(count_key.clone());
    count_completion_audit.operation_id = AuditField::Known(count_key);
    expected_audit.extend([count_start_audit, count_completion_audit]);
    let security_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::prefix::PrefixStore::new(
            rockstream_test_support::minio::minio_object_store(minio_port, bucket),
            "gateway-shard",
        ));
    let recovered = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    let mut actual_audit = recovered.list_security_audit_records().await.unwrap();
    for record in &mut actual_audit {
        record.timestamp_ms = 0;
        if record.event_id.starts_with("support-artifact-") {
            record.event_id = "support-artifact".to_owned();
        } else if record.event_id.starts_with("management-authn-") {
            record.event_id = "management-authn".to_owned();
        }
    }
    assert_eq!(actual_audit, expected_audit);
}

#[tokio::test]
async fn support_artifact_budget_is_shared_between_release_processes() {
    for _ in 0..3 {
        support_artifact_budget_is_shared_between_release_processes_once().await;
    }
}

async fn support_artifact_budget_is_shared_between_release_processes_once() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    assert!(rockstream_test_support::docker_available());
    let bucket = "rockstream-release-support-cross-process";
    let (_minio, minio_port) = rockstream_test_support::minio::start_minio(bucket)
        .await
        .expect("MinIO is required for cross-process support-artifact qualification");
    let temp = tempfile::tempdir().unwrap();
    let pki = rockstream_test_support::pki::TestPki::generate();
    let first_control = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let first_management = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut first_process = start_control_process(
        temp.path(),
        &temp.path().join("control-one"),
        first_control,
        first_management,
        &pki,
        Some((minio_port, bucket)),
        true,
    )
    .await;
    wait_for_gateway(&mut first_process.0, first_control).await;
    let mut first_client = wait_for_management_client(first_management, &pki).await;
    let management_store = object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "management",
    );
    let ledger_path = object_store::path::Path::from("control/support-artifacts/ledger.json");

    let seed = first_client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: uuid::Uuid::new_v4().to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let (seed_content, _) = download_support_artifact(&mut first_client, &seed.opaque_id).await;
    let seed_size = seed_content.len();
    let deployment_budget = 100 * 1024 * 1024;
    assert!(seed_size > 0 && seed_size * 3 < deployment_budget);
    let ledger_bytes = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let mut ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
    let artifacts = ledger["artifacts"].as_object_mut().unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(
        artifacts.get(&seed.opaque_id).unwrap()["size"].as_u64(),
        Some(seed_size as u64)
    );
    let filler_id = uuid::Uuid::new_v4().to_string();
    artifacts.insert(
        filler_id.clone(),
        serde_json::json!({
            "owner": PrincipalId(1),
            "scope": PolicyScope::Cluster,
            "size": deployment_budget - 2 * seed_size,
            "expires_at_ms": seed.expires_at_ms,
        }),
    );
    management_store
        .put(&ledger_path, serde_json::to_vec(&ledger).unwrap().into())
        .await
        .unwrap();

    let second_control = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let second_management = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut second_process = start_control_process_with_operator(
        temp.path(),
        &temp.path().join("control-two"),
        second_control,
        second_management,
        &pki,
        Some((minio_port, bucket)),
        true,
        None,
        false,
        false,
    )
    .await;
    wait_for_gateway(&mut second_process.0, second_control).await;
    let mut second_client = wait_for_management_client(second_management, &pki).await;
    assert_eq!(
        second_client
            .get_cluster_status(GetClusterStatusRequest {
                protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            })
            .await
            .unwrap()
            .into_inner()
            .protocol_version,
        rockstream_management_proto::PROTOCOL_VERSION
    );

    let first_attempt_id = uuid::Uuid::new_v4().to_string();
    let second_attempt_id = uuid::Uuid::new_v4().to_string();
    let (first_attempt, second_attempt) = tokio::join!(
        first_client.generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: first_attempt_id.clone(),
        }),
        second_client.generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: second_attempt_id.clone(),
        }),
    );
    let (winner_id, winner, loser_id, loser_error) = match (first_attempt, second_attempt) {
        (Ok(winner), Err(error)) if error.code() == tonic::Code::ResourceExhausted => (
            first_attempt_id,
            winner.into_inner(),
            second_attempt_id,
            error,
        ),
        (Err(error), Ok(winner)) if error.code() == tonic::Code::ResourceExhausted => (
            second_attempt_id,
            winner.into_inner(),
            first_attempt_id,
            error,
        ),
        (first, second) => panic!(
            "expected one successful artifact and one quota rejection; first={first:?}, second={second:?}"
        ),
    };
    assert_eq!(
        winner.protocol_version,
        rockstream_management_proto::PROTOCOL_VERSION
    );
    assert_eq!(winner.opaque_id, winner_id);
    assert_eq!(loser_error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        loser_error.message(),
        "support artifact storage budget is full"
    );

    let (downloaded, downloaded_expiry_ms) =
        download_support_artifact(&mut first_client, &winner_id).await;
    assert_eq!(downloaded_expiry_ms, winner.expires_at_ms);
    let generated_at_ms = winner.expires_at_ms - 600_000;
    // The concurrent loser may be recorded before or after the winner's snapshot.
    let expected_contents = [(1, 2), (1, 3), (2, 3)].map(|(active, retained)| {
        format!(
            "{{\"format_version\":1,\"generated_at_ms\":{generated_at_ms},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":1,\"active_operations\":{active},\"retained_operations\":{retained},\"node\":null}}",
            env!("CARGO_PKG_VERSION")
        )
    });
    let actual_content = String::from_utf8(downloaded.clone()).unwrap();
    assert!(
        expected_contents.contains(&actual_content),
        "unexpected complete artifact: {actual_content}"
    );
    let stored_winner = management_store
        .get(&support_artifact_object_path(&winner_id))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(stored_winner.as_ref(), downloaded.as_slice());
    assert!(matches!(
        management_store
            .get(&support_artifact_object_path(&loser_id))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));

    let ledger_bytes = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
    let artifacts = ledger["artifacts"].as_object().unwrap();
    let mut actual_ids = artifacts.keys().cloned().collect::<Vec<_>>();
    actual_ids.sort();
    let mut expected_ids = vec![seed.opaque_id.clone(), filler_id.clone(), winner_id.clone()];
    expected_ids.sort();
    assert_eq!(actual_ids, expected_ids);
    assert_eq!(
        artifacts.get(&seed.opaque_id).unwrap()["size"].as_u64(),
        Some(seed_size as u64)
    );
    assert_eq!(
        artifacts.get(&filler_id).unwrap()["size"].as_u64(),
        Some((deployment_budget - 2 * seed_size) as u64)
    );
    assert_eq!(
        artifacts.get(&winner_id).unwrap()["size"].as_u64(),
        Some(seed_size as u64)
    );
    assert_eq!(
        artifacts
            .values()
            .map(|artifact| artifact["size"].as_u64().unwrap())
            .sum::<u64>(),
        deployment_budget as u64
    );

    let pressure_ids = (0..16)
        .map(|_| uuid::Uuid::new_v4().to_string())
        .collect::<Vec<_>>();
    let mut pressure_operation_ids = Vec::new();
    let mut pressure_busy_ids = Vec::new();
    let mut pressure = tokio::task::JoinSet::new();
    for (index, id) in pressure_ids.iter().cloned().enumerate() {
        let mut client = if index % 2 == 0 {
            first_client.clone()
        } else {
            second_client.clone()
        };
        pressure.spawn(async move {
            let result = client
                .generate_support_artifact(GenerateSupportArtifactRequest {
                    protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
                    node_id: String::new(),
                    idempotency_key: id.clone(),
                })
                .await;
            (id, result)
        });
    }
    while let Some(joined) = pressure.join_next().await {
        let (id, result) = joined.expect("support-artifact pressure request task should finish");
        let error = result.expect_err("full deployment budget must reject each pressure request");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted, "request {id}");
        match error.message() {
            "support artifact generation is busy" => pressure_busy_ids.push(id.clone()),
            "support artifact storage budget is full" => pressure_operation_ids.push(id.clone()),
            message => panic!("unexpected pressure response for {id}: {message}"),
        }
        assert!(matches!(
            management_store
                .get(&support_artifact_object_path(&id))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
    }
    assert!(!pressure_busy_ids.is_empty());
    assert!(!pressure_operation_ids.is_empty());
    assert_eq!(
        pressure_busy_ids.len() + pressure_operation_ids.len(),
        pressure_ids.len()
    );
    let ledger_after_pressure = management_store
        .get(&ledger_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(ledger_after_pressure.as_ref(), ledger_bytes.as_ref());

    let operations = first_client
        .list_operations(ListOperationsRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            page_size: 100,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        operations.protocol_version,
        rockstream_management_proto::PROTOCOL_VERSION
    );
    assert_eq!(operations.source_version, "management-operation-records:v1");
    assert!(operations.next_page_token.is_empty());
    let mut operation_ids = operations
        .operations
        .iter()
        .map(|operation| operation.operation_id.clone())
        .collect::<Vec<_>>();
    operation_ids.sort();
    let mut expected_operation_ids =
        vec![seed.opaque_id.clone(), winner_id.clone(), loser_id.clone()];
    expected_operation_ids.extend(pressure_operation_ids.iter().cloned());
    expected_operation_ids.sort();
    assert_eq!(operation_ids, expected_operation_ids);
    for id in &pressure_busy_ids {
        assert!(operation_ids.binary_search(id).is_err());
    }
    let mut expected_operations = vec![
        (
            seed.opaque_id.as_str(),
            "succeeded",
            format!("available_until_ms:{}", seed.expires_at_ms),
        ),
        (
            winner_id.as_str(),
            "succeeded",
            format!("available_until_ms:{}", winner.expires_at_ms),
        ),
        (loser_id.as_str(), "failed", "generation_failed".to_owned()),
    ];
    expected_operations.extend(
        pressure_operation_ids
            .iter()
            .map(|id| (id.as_str(), "failed", "generation_failed".to_owned())),
    );
    for (id, state, phase) in expected_operations {
        let matching = operations
            .operations
            .iter()
            .filter(|operation| operation.operation_id == id)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        let operation = matching[0];
        assert_eq!(operation.kind, "generate_support_artifact");
        assert_eq!(operation.state, state);
        assert_eq!(operation.phase, phase);
        assert_eq!(operation.progress, "100%");
        assert!(operation.error_code.is_empty());
        assert!(operation.next_steps.is_empty());
        assert_eq!(operation.source_version, "operation-record:4");
    }

    first_process.0.kill().unwrap();
    first_process.0.wait().unwrap();
    second_process.0.kill().unwrap();
    second_process.0.wait().unwrap();
    let security_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "gateway-shard",
    ));
    let catalog = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    let mut actual_audit = catalog.list_security_audit_records().await.unwrap();
    for record in &mut actual_audit {
        record.timestamp_ms = 0;
        if record.event_id.starts_with("support-artifact-") {
            record.event_id = "support-artifact".to_owned();
        }
    }
    for id in &pressure_busy_ids {
        assert!(!actual_audit
            .iter()
            .any(|record| record.operation_id == AuditField::Known(id.clone())));
    }
    let cluster_scope = serde_json::to_string(&PolicyScope::Cluster).unwrap();
    let mut audit_operations = vec![
        (winner_id.as_str(), true, winner.expires_at_ms),
        (loser_id.as_str(), false, 0),
    ];
    audit_operations.extend(
        pressure_operation_ids
            .iter()
            .map(|id| (id.as_str(), false, 0)),
    );
    for (id, successful, expiry_ms) in audit_operations {
        let expected_audit = vec![
            SecurityAuditRecord {
                format_version: SECURITY_AUDIT_FORMAT_VERSION,
                event_id: "support-artifact".to_owned(),
                timestamp_ms: 0,
                actor: AuditIdentity::Verified("principal:1".to_owned()),
                target: AuditField::Known(format!("support-artifact:{id}")),
                scope: AuditField::Known(cluster_scope.clone()),
                action: SecurityAuditClass::AdminOperationStart,
                outcome: SecurityAuditOutcome::Success,
                correlation: AuditCorrelation::Observed(id.to_owned()),
                operation_id: AuditField::Known(id.to_owned()),
                policy_revision: AuditField::Known(1),
                before_state: AuditField::Known("pending".to_owned()),
                after_state: AuditField::Known("generating".to_owned()),
            },
            SecurityAuditRecord {
                format_version: SECURITY_AUDIT_FORMAT_VERSION,
                event_id: "support-artifact".to_owned(),
                timestamp_ms: 0,
                actor: AuditIdentity::Verified("principal:1".to_owned()),
                target: AuditField::Known(format!("support-artifact:{id}")),
                scope: AuditField::Known(cluster_scope.clone()),
                action: SecurityAuditClass::AdminOperationCompletion,
                outcome: if successful {
                    SecurityAuditOutcome::Success
                } else {
                    SecurityAuditOutcome::Failure
                },
                correlation: AuditCorrelation::Observed(id.to_owned()),
                operation_id: AuditField::Known(id.to_owned()),
                policy_revision: AuditField::Known(1),
                before_state: AuditField::Known("generating".to_owned()),
                after_state: AuditField::Known(if successful {
                    format!("available_until_ms:{expiry_ms}")
                } else {
                    "generation_failed".to_owned()
                }),
            },
        ];
        let actual = actual_audit
            .iter()
            .filter(|record| record.operation_id == AuditField::Known(id.to_owned()))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected_audit);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn worker_peer_certificate_rejections_are_audited_by_release_process() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let control_addr: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut control = start_delegated_control_process(
        temp.path(),
        &storage,
        control_addr,
        &pki,
        None,
        false,
        None,
    )
    .await;
    wait_for_gateway(&mut control.0, control_addr).await;

    let security_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let security_catalog = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    let principals_before = security_catalog.list_security_principals().await.unwrap();

    let exchange_addr: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let worker_dir = temp.path().join("worker-process");
    std::fs::create_dir_all(&worker_dir).unwrap();
    let worker_storage = temp.path().join("worker-process-storage");
    let mut worker = start_worker_shuffle_process(
        &worker_dir,
        &worker_storage,
        control_addr,
        exchange_addr,
        &pki,
        3,
    );
    wait_for_gateway(&mut worker.0, exchange_addr).await;
    let audit_path = storage.join("audit.jsonl");
    let registration_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
        if events
            .iter()
            .any(|event| event.action == "worker.registered" && event.resource == "worker-3")
        {
            break;
        }
        assert!(
            Instant::now() < registration_deadline,
            "release worker did not register before peer qualification: {events:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    wait_for_worker_shuffle_peer(exchange_addr, WorkerId(3), pki.worker_tls_config(3)).await;

    security_catalog.refresh().await.unwrap();
    let registrations_before = FileAuditLog::open(&audit_path)
        .unwrap()
        .read_all()
        .unwrap()
        .iter()
        .filter(|event| event.action == "worker.registered")
        .count();
    let audit_before = security_catalog
        .list_security_audit_records()
        .await
        .unwrap();

    let peers = Arc::new(RwLock::new(std::collections::HashMap::from([(
        WorkerId(3),
        exchange_addr.to_string(),
    )])));
    let mut handshake_rejections = Vec::new();
    for tls in [
        pki.expired_worker_tls_config(),
        pki.untrusted_worker_tls_config(),
    ] {
        let config = rockstream_types::config::ExchangeConfig {
            max_retries: 1,
            ..Default::default()
        };
        let pool = ShuffleClientPool::new(peers.clone())
            .with_config(config)
            .with_internal_tls(tls);
        let mut client = pool.get_client(WorkerId(3)).await.unwrap();
        let mut request = tonic::Request::new(futures::stream::iter(Vec::<ShuffleFrame>::new()));
        request.metadata_mut().insert(
            "protocol_version",
            tonic::metadata::MetadataValue::from_static("1"),
        );
        assert!(client.shuffle_stream(request).await.is_err());

        let prior_count = audit_before.len() + handshake_rejections.len();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut observed_count = prior_count;
        let mut stable_since = None;
        let audit = loop {
            security_catalog.refresh().await.unwrap();
            let records = security_catalog
                .list_security_audit_records()
                .await
                .unwrap();
            if records.len() > prior_count {
                if records.len() != observed_count {
                    observed_count = records.len();
                    stable_since = Some(Instant::now());
                } else if stable_since
                    .is_some_and(|since: Instant| since.elapsed() >= Duration::from_millis(200))
                {
                    break records;
                }
            }
            assert!(
                Instant::now() < deadline,
                "worker listener did not durably audit the rejected TLS handshake: {records:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        let new_rejections = &audit[prior_count..];
        assert!(!new_rejections.is_empty());
        for rejection in new_rejections {
            assert!(
                rejection.event_id.starts_with("shuffle-tls-handshake:"),
                "unexpected security audit during rejected TLS connect: {rejection:?}"
            );
            let AuditCorrelation::Observed(peer) = &rejection.correlation else {
                panic!("handshake audit must use the listener-observed peer address");
            };
            assert!(peer
                .strip_prefix("peer=")
                .unwrap()
                .parse::<std::net::SocketAddr>()
                .is_ok());
            assert_eq!(
                rejection,
                &SecurityAuditRecord {
                    format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
                    event_id: rejection.event_id.clone(),
                    timestamp_ms: rejection.timestamp_ms,
                    actor: AuditIdentity::Unknown,
                    target: AuditField::Unknown,
                    scope: AuditField::Known("worker_shuffle".to_owned()),
                    action: SecurityAuditClass::NodeIdentityRejection,
                    outcome: SecurityAuditOutcome::Denied,
                    correlation: AuditCorrelation::Observed(peer.clone()),
                    operation_id: AuditField::NotApplicable,
                    policy_revision: AuditField::NotApplicable,
                    before_state: AuditField::NotApplicable,
                    after_state: AuditField::NotApplicable,
                }
            );
            handshake_rejections.push(rejection.clone());
        }
    }

    let pool = ShuffleClientPool::new(peers).with_internal_tls(pki.mismatched_worker_tls_config());
    let mut client = pool.get_client(WorkerId(3)).await.unwrap();
    let mut request = tonic::Request::new(futures::stream::iter(Vec::<ShuffleFrame>::new()));
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );
    let denial = client.shuffle_stream(request).await.unwrap_err();
    assert_eq!(denial.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        denial.message(),
        "RS-2401: worker node identity is not currently registered"
    );

    security_catalog.refresh().await.unwrap();
    let audit_after = security_catalog
        .list_security_audit_records()
        .await
        .unwrap();
    assert_eq!(
        audit_after.len(),
        audit_before.len() + handshake_rejections.len() + 1
    );
    assert_eq!(
        &audit_after[audit_before.len()..audit_before.len() + handshake_rejections.len()],
        handshake_rejections
    );
    let rejection = audit_after.last().unwrap();
    assert!(rejection.event_id.starts_with("shuffle-node-identity:"));
    assert_eq!(
        rejection,
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: rejection.event_id.clone(),
            timestamp_ms: rejection.timestamp_ms,
            actor: AuditIdentity::Unverified("worker:worker-999".to_owned()),
            target: AuditField::Unverified("worker:worker-999".to_owned()),
            scope: AuditField::Known("worker_shuffle".to_owned()),
            action: SecurityAuditClass::NodeIdentityRejection,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Unknown,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::NotApplicable,
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );
    assert_eq!(
        security_catalog.list_security_principals().await.unwrap(),
        principals_before
    );
    assert_eq!(
        FileAuditLog::open(&audit_path)
            .unwrap()
            .read_all()
            .unwrap()
            .iter()
            .filter(|event| event.action == "worker.registered")
            .count(),
        registrations_before
    );

    wait_for_worker_shuffle_peer(exchange_addr, WorkerId(3), pki.worker_tls_config(3)).await;
    assert!(worker.0.try_wait().unwrap().is_none());
    worker.0.kill().unwrap();
    worker.0.wait().unwrap();
    control.0.kill().unwrap();
    control.0.wait().unwrap();
    let recovered_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let recovered = DurableCatalogStore::recover(recovered_store, "gateway")
        .await
        .unwrap();
    assert_eq!(
        recovered.list_security_audit_records().await.unwrap(),
        audit_after
    );
}

#[cfg(unix)]
#[tokio::test]
async fn worker_shuffle_mtls_rollover_preserves_release_process_stream() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let (gen2_ca_path, _, _) = pki.generate_gen2_pki();
    let gen1_server_tls = pki.worker_tls_config(3);
    let gen2_server_tls = pki.gen2_worker_tls_config(3);
    let gen1_client_tls =
        pki.worker_tls_config_with_ca(4, gen2_server_tls.ca_cert_path.clone().unwrap());
    let mut gen2_client_tls = pki.gen2_worker_tls_config(4);
    gen2_client_tls.ca_cert_path = Some(gen2_ca_path);
    let control_addr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut control = start_delegated_control_process(
        temp.path(),
        &storage,
        control_addr,
        &pki,
        None,
        false,
        None,
    )
    .await;
    wait_for_gateway(&mut control.0, control_addr).await;

    let server_addr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let client_addr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let server_dir = temp.path().join("worker-3");
    let client_dir = temp.path().join("worker-4");
    std::fs::create_dir_all(&server_dir).unwrap();
    std::fs::create_dir_all(&client_dir).unwrap();
    let mut server = start_worker_shuffle_process(
        &server_dir,
        &temp.path().join("worker-3-storage"),
        control_addr,
        server_addr,
        &pki,
        3,
    );
    let mut client_worker = start_worker_shuffle_process(
        &client_dir,
        &temp.path().join("worker-4-storage"),
        control_addr,
        client_addr,
        &pki,
        4,
    );
    wait_for_gateway(&mut server.0, server_addr).await;
    wait_for_gateway(&mut client_worker.0, client_addr).await;
    wait_for_worker_shuffle_peer(server_addr, WorkerId(3), gen1_client_tls.clone()).await;
    wait_for_worker_shuffle_peer(client_addr, WorkerId(4), pki.worker_tls_config(3)).await;

    let peers = Arc::new(RwLock::new(std::collections::HashMap::from([(
        WorkerId(3),
        server_addr.to_string(),
    )])));
    let pool = ShuffleClientPool::new(peers).with_internal_tls(gen1_client_tls.clone());
    let mut client = pool.get_client(WorkerId(3)).await.unwrap();
    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(1);
    let stream = futures::stream::unfold(frame_rx, |mut receiver| async move {
        receiver.recv().await.map(|frame| (frame, receiver))
    });
    let mut request = tonic::Request::new(stream);
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );
    let mut response = client.shuffle_stream(request).await.unwrap().into_inner();

    for (source, target) in [
        (
            gen2_server_tls.cert_path.as_ref().unwrap(),
            gen1_server_tls.cert_path.as_ref().unwrap(),
        ),
        (
            gen2_server_tls.key_path.as_ref().unwrap(),
            gen1_server_tls.key_path.as_ref().unwrap(),
        ),
        (
            gen2_server_tls.ca_cert_path.as_ref().unwrap(),
            gen1_server_tls.ca_cert_path.as_ref().unwrap(),
        ),
    ] {
        std::fs::copy(source, target).unwrap();
    }
    assert_eq!(
        unsafe { libc::kill(server.0.id() as libc::pid_t, libc::SIGHUP) },
        0,
        "worker listener must accept the certificate reload signal"
    );
    wait_for_worker_shuffle_peer(server_addr, WorkerId(3), gen1_client_tls).await;
    wait_for_worker_shuffle_peer(server_addr, WorkerId(3), gen2_client_tls).await;

    let security_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let security_catalog = DurableCatalogStore::recover(security_store, "gateway")
        .await
        .unwrap();
    security_catalog.refresh().await.unwrap();
    let audit_before = security_catalog
        .list_security_audit_records()
        .await
        .unwrap();

    frame_tx
        .send(ShuffleFrame {
            exchange_id: 100,
            src_shard: 10,
            target_shard: 20,
            epoch: 1,
            seq: 1,
            payload: Vec::new(),
            row_count: 0,
        })
        .await
        .unwrap();
    let denial = response.message().await.unwrap_err();
    assert_eq!(denial.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        denial.message(),
        "RS-2401: authenticated worker worker-4 does not own source shard 10"
    );
    assert!(server.0.try_wait().unwrap().is_none());
    assert!(client_worker.0.try_wait().unwrap().is_none());

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut observed_count = audit_before.len();
    let mut stable_since = None;
    let audit_after = loop {
        security_catalog.refresh().await.unwrap();
        let records = security_catalog
            .list_security_audit_records()
            .await
            .unwrap();
        let has_denial = records
            .iter()
            .skip(audit_before.len())
            .any(|record| record.event_id.starts_with("shuffle-authorization:"));
        if has_denial {
            if records.len() == observed_count {
                if stable_since
                    .is_some_and(|since: Instant| since.elapsed() >= Duration::from_millis(200))
                {
                    break records;
                }
            } else {
                observed_count = records.len();
                stable_since = Some(Instant::now());
            }
        } else {
            observed_count = records.len();
            stable_since = None;
        }
        assert!(
            Instant::now() < deadline,
            "shuffle denial was not audited: records grew from {} to {} ({:?})",
            audit_before.len(),
            records.len(),
            records
                .iter()
                .skip(audit_before.len())
                .map(|record| &record.event_id)
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let new_records = &audit_after[audit_before.len()..];
    let denials = new_records
        .iter()
        .filter(|record| record.event_id.starts_with("shuffle-authorization:"))
        .collect::<Vec<_>>();
    assert_eq!(denials.len(), 1, "expected one audited shuffle denial");
    let handshake_rejections = new_records
        .iter()
        .filter(|record| record.event_id.starts_with("shuffle-tls-handshake:"))
        .collect::<Vec<_>>();
    for handshake in &handshake_rejections {
        let AuditCorrelation::Observed(peer) = &handshake.correlation else {
            panic!("handshake audit must use the listener-observed peer address");
        };
        assert!(peer
            .strip_prefix("peer=")
            .unwrap()
            .parse::<std::net::SocketAddr>()
            .is_ok());
        assert_eq!(
            *handshake,
            &SecurityAuditRecord {
                format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
                event_id: handshake.event_id.clone(),
                timestamp_ms: handshake.timestamp_ms,
                actor: AuditIdentity::Unknown,
                target: AuditField::Unknown,
                scope: AuditField::Known("worker_shuffle".to_owned()),
                action: SecurityAuditClass::NodeIdentityRejection,
                outcome: SecurityAuditOutcome::Denied,
                correlation: AuditCorrelation::Observed(peer.clone()),
                operation_id: AuditField::NotApplicable,
                policy_revision: AuditField::NotApplicable,
                before_state: AuditField::NotApplicable,
                after_state: AuditField::NotApplicable,
            }
        );
    }
    assert_eq!(
        new_records.len(),
        denials.len() + handshake_rejections.len(),
        "unexpected security audit during worker certificate rollover: {new_records:?}"
    );
    let rejection = denials[0];
    assert!(rejection.event_id.starts_with("shuffle-authorization:"));
    assert_eq!(
        rejection,
        &SecurityAuditRecord {
            format_version: rockstream_types::authorization::SECURITY_AUDIT_FORMAT_VERSION,
            event_id: rejection.event_id.clone(),
            timestamp_ms: rejection.timestamp_ms,
            actor: AuditIdentity::Verified("worker-4".to_owned()),
            target: AuditField::Known("source_shard:10".to_owned()),
            scope: AuditField::Known("worker_shuffle".to_owned()),
            action: SecurityAuditClass::AuthorizationDenial,
            outcome: SecurityAuditOutcome::Denied,
            correlation: AuditCorrelation::Observed(
                "exchange:100:source:10:target:20:epoch:1:sequence:1".to_owned(),
            ),
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::NotApplicable,
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    );

    server.0.kill().unwrap();
    server.0.wait().unwrap();
    client_worker.0.kill().unwrap();
    client_worker.0.wait().unwrap();
    control.0.kill().unwrap();
    control.0.wait().unwrap();
    let recovered_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
    let recovered = DurableCatalogStore::recover(recovered_store, "gateway")
        .await
        .unwrap();
    assert_eq!(
        recovered.list_security_audit_records().await.unwrap(),
        audit_after
    );
}

#[cfg(unix)]
async fn wait_for_release_worker_lease(
    client: &rockstream_runtime::WorkerClientHandle,
    shard_id: ShardId,
) -> rockstream_types::lease::ShardLease {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(lease) = client
                .leases()
                .into_iter()
                .find(|lease| lease.shard_id == shard_id)
            {
                return lease;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("release worker did not receive lease for {shard_id:?}"))
}

#[cfg(unix)]
fn start_worker_shuffle_process(
    working_dir: &Path,
    storage: &Path,
    control_addr: std::net::SocketAddr,
    exchange_addr: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
    worker_id: u64,
) -> RunningGateway {
    let mut config = rockstream_types::config::NodeConfig::default();
    config.worker.exchange_bind_addr = Some(exchange_addr.to_string());
    config.worker.exchange_advertise_addr = Some(exchange_addr.to_string());
    std::fs::write(
        working_dir.join("rockstream.toml"),
        toml::to_string(&config).unwrap(),
    )
    .unwrap();

    let tls = pki.worker_tls_config(worker_id);
    let child = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .current_dir(working_dir)
        .env("RUST_LOG", "off")
        .env_remove("ROCKSTREAM_CONFIG")
        .args([
            "start",
            "--role",
            "worker",
            "--daemon",
            "--storage",
            storage.to_str().unwrap(),
            "--control",
            &control_addr.to_string(),
            "--worker-id",
            &worker_id.to_string(),
            "--internal-tls-cert-path",
            tls.cert_path.as_ref().unwrap().to_str().unwrap(),
            "--internal-tls-key-path",
            tls.key_path.as_ref().unwrap().to_str().unwrap(),
            "--internal-tls-ca-cert-path",
            tls.ca_cert_path.as_ref().unwrap().to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    RunningGateway(child)
}

#[cfg(unix)]
async fn wait_for_worker_shuffle_peer(
    address: std::net::SocketAddr,
    worker_id: WorkerId,
    tls: rockstream_types::identity::InternalTlsConfig,
) {
    let mut last_error = String::from("no connection attempt completed");
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let peers = Arc::new(RwLock::new(std::collections::HashMap::from([(
                worker_id,
                address.to_string(),
            )])));
            let pool = ShuffleClientPool::new(peers).with_internal_tls(tls.clone());
            match pool.get_client(worker_id).await {
                Ok(mut client) => {
                    let mut request =
                        tonic::Request::new(futures::stream::iter(Vec::<ShuffleFrame>::new()));
                    request.metadata_mut().insert(
                        "protocol_version",
                        tonic::metadata::MetadataValue::from_static("1"),
                    );
                    match client.shuffle_stream(request).await {
                        Ok(_) => return,
                        Err(error) => last_error = error.to_string(),
                    }
                }
                Err(error) => last_error = error.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "worker shuffle listener did not accept the qualified peer certificate: {last_error}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn in_flight_internal_mtls_rotation_preserves_release_worker_stream() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process =
        start_delegated_control_process(temp.path(), &storage, address, &pki, None, false, None)
            .await;
    wait_for_gateway(&mut process.0, address).await;
    let process_id = process.0.id();

    let old_worker_storage = temp.path().join("worker-gen1");
    let (old_worker, old_worker_task) = rockstream_runtime::start_worker_client_with_tls(
        1,
        &address.to_string(),
        &old_worker_storage,
        pki.worker_tls_config(1),
    )
    .await
    .expect("Gen1 worker should connect to the release control process");
    tokio::time::timeout(Duration::from_secs(5), async {
        while old_worker.worker_id() != Some(WorkerId(1)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("control did not register the Gen1 worker");
    let first_shard = ShardId(101);
    old_worker.request_shard(first_shard).await.unwrap();
    let first_lease = wait_for_release_worker_lease(&old_worker, first_shard).await;

    let audit_path = storage.join("audit.jsonl");
    let denials_before = FileAuditLog::open(&audit_path)
        .unwrap()
        .read_all()
        .unwrap()
        .iter()
        .filter(|event| event.action == "security.internal_mtls_denied")
        .count();

    let (_, gen2_control_tls, _) = pki.generate_gen2_pki();
    std::fs::copy(
        gen2_control_tls.cert_path.as_ref().unwrap(),
        &pki.control_cert_path,
    )
    .unwrap();
    std::fs::copy(
        gen2_control_tls.key_path.as_ref().unwrap(),
        &pki.control_key_path,
    )
    .unwrap();
    std::fs::copy(
        gen2_control_tls.ca_cert_path.as_ref().unwrap(),
        &pki.ca_cert_path,
    )
    .unwrap();
    assert_eq!(unsafe { libc::kill(process_id as i32, libc::SIGHUP) }, 0);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(process.0.try_wait().unwrap().is_none());
    assert_eq!(old_worker.worker_id(), Some(WorkerId(1)));
    assert!(!old_worker_task.is_finished());
    let second_shard = ShardId(102);
    old_worker.request_shard(second_shard).await.unwrap();
    let second_lease = wait_for_release_worker_lease(&old_worker, second_shard).await;
    assert!(old_worker.leases().contains(&first_lease));
    assert!(old_worker.leases().contains(&second_lease));

    let new_worker_storage = temp.path().join("worker-gen2");
    let (new_worker, new_worker_task) = rockstream_runtime::start_worker_client_with_tls(
        2,
        &address.to_string(),
        &new_worker_storage,
        pki.gen2_worker_tls_config(2),
    )
    .await
    .expect("dual-generation trust should admit a Gen2 worker after reload");
    tokio::time::timeout(Duration::from_secs(5), async {
        while new_worker.worker_id() != Some(WorkerId(2)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("control did not register the Gen2 worker");
    let gen2_shard = ShardId(201);
    new_worker.request_shard(gen2_shard).await.unwrap();
    let gen2_lease = wait_for_release_worker_lease(&new_worker, gen2_shard).await;
    assert_eq!(gen2_lease.worker_id, WorkerId(2));
    assert!(process.0.try_wait().unwrap().is_none());

    let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
    let registered = events
        .iter()
        .filter(|event| event.action == "worker.registered")
        .map(|event| event.resource.as_str())
        .collect::<Vec<_>>();
    assert_eq!(registered, ["worker-1", "worker-2"]);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action == "security.internal_mtls_denied")
            .count(),
        denials_before,
        "valid old and new workers must not be rejected during rollover"
    );

    let worker_exchange_addr: std::net::SocketAddr =
        format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let worker_dir = temp.path().join("worker-process");
    std::fs::create_dir_all(&worker_dir).unwrap();
    let worker_storage = temp.path().join("worker-process-storage");
    let mut worker_process = start_worker_shuffle_process(
        &worker_dir,
        &worker_storage,
        address,
        worker_exchange_addr,
        &pki,
        3,
    );
    wait_for_gateway(&mut worker_process.0, worker_exchange_addr).await;
    let gen1_client_tls = pki.worker_tls_config_with_ca(1, pki.ca_cert_path.clone());
    wait_for_worker_shuffle_peer(worker_exchange_addr, WorkerId(3), gen1_client_tls.clone()).await;

    let peers = Arc::new(RwLock::new(std::collections::HashMap::from([(
        WorkerId(3),
        worker_exchange_addr.to_string(),
    )])));
    let pool = ShuffleClientPool::new(peers.clone()).with_internal_tls(gen1_client_tls);
    let mut client = pool.get_client(WorkerId(3)).await.unwrap();
    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(1);
    let stream = futures::stream::unfold(frame_rx, |mut receiver| async move {
        receiver.recv().await.map(|frame| (frame, receiver))
    });
    let mut request = tonic::Request::new(stream);
    request.metadata_mut().insert(
        "protocol_version",
        tonic::metadata::MetadataValue::from_static("1"),
    );
    let mut active_stream = client.shuffle_stream(request).await.unwrap().into_inner();

    let gen2_worker_tls = pki.gen2_worker_tls_config(3);
    let (worker_cert, worker_key) = pki.worker_certs.get(&3).unwrap();
    std::fs::copy(gen2_worker_tls.cert_path.as_ref().unwrap(), worker_cert).unwrap();
    std::fs::copy(gen2_worker_tls.key_path.as_ref().unwrap(), worker_key).unwrap();
    let worker_process_id = worker_process.0.id();
    assert_eq!(
        unsafe { libc::kill(worker_process_id as i32, libc::SIGHUP) },
        0
    );

    frame_tx
        .send(ShuffleFrame {
            exchange_id: 100,
            src_shard: first_shard.0 as u32,
            target_shard: u32::MAX,
            epoch: 1,
            seq: 1,
            payload: Vec::new(),
            row_count: 0,
        })
        .await
        .unwrap();
    let denial = active_stream.next().await.unwrap().unwrap_err();
    assert_eq!(denial.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        denial.message(),
        "RS-2401: target shard is not assigned to this worker"
    );

    wait_for_worker_shuffle_peer(
        worker_exchange_addr,
        WorkerId(3),
        pki.worker_tls_config_with_ca(1, pki.ca_cert_path.clone()),
    )
    .await;
    wait_for_worker_shuffle_peer(
        worker_exchange_addr,
        WorkerId(3),
        pki.gen2_worker_tls_config(1),
    )
    .await;
    assert!(worker_process.0.try_wait().unwrap().is_none());

    old_worker_task.abort();
    new_worker_task.abort();
    process.0.kill().unwrap();
    process.0.wait().unwrap();
}

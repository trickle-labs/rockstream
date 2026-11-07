use std::{
    io::{BufReader, Read},
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use object_store::{ObjectStore, ObjectStoreExt};
use rockstream_control::audit::FileAuditLog;
use rockstream_management_proto::v1::{
    management_service_client::ManagementServiceClient, CreateBackupRequest,
    DownloadSupportArtifactRequest, GenerateSupportArtifactRequest, GetClusterStatusRequest,
    ListOperationsRequest,
};
use rockstream_storage::catalog::{
    CatalogDatabase, CatalogMutation, CatalogNamespace, CatalogStore, CatalogTxn,
    DurableCatalogStore,
};
use rockstream_types::{
    authorization::{
        AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
        ScopedActionGrant, ScopedPresetGrant, SecurityAction, SecurityAuditClass,
        SecurityAuditOutcome, SecurityAuditRecord, SecurityPrincipal,
        SECURITY_AUDIT_FORMAT_VERSION, SECURITY_PRINCIPAL_FORMAT_VERSION,
    },
    ids::{DatabaseId, NamespaceId, PrincipalId, ShardId, WorkerId},
};
use sha2::Digest;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

static RELEASE_PROCESS_PORT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    let durable = DurableCatalogStore::new(security_store, "gateway");
    if provision_first_admin {
        durable
            .preprovision_first_administrator(principal, 1)
            .await
            .unwrap();
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
async fn oidc_password_auth_runs_in_release_process_and_audits_after_restart() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let shard_dir = storage.join("gateway-shard");
    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    let durable = DurableCatalogStore::new(store.clone(), "gateway");
    let administrator = oidc_bootstrap_administrator();
    let reader = oidc_principal();
    durable
        .preprovision_first_administrator(administrator.clone(), 1)
        .await
        .unwrap();
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
async fn scram_failures_run_in_release_process_and_audit_after_restart() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("node-storage");
    let shard_dir = storage.join("gateway-shard");
    std::fs::create_dir_all(&shard_dir).unwrap();
    let principal = scram_principal();
    let store =
        rockstream_storage::build_runtime_object_store(&shard_dir, "gateway-shard").unwrap();
    DurableCatalogStore::new(store, "gateway")
        .preprovision_first_administrator(principal.clone(), 1)
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
        Some("[RS-2401] auth.invalid_password: password authentication failed for user 'alice'. next_steps: Check password and retry")
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
    assert_eq!(
        recovered.list_security_principals().await.unwrap(),
        vec![principal]
    );
    let records = recovered.list_security_audit_records().await.unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(records[0].action, SecurityAuditClass::GrantChange);
    assert_eq!(records[0].outcome, SecurityAuditOutcome::Success);
    assert_eq!(records[1].action, SecurityAuditClass::AuthenticationSuccess);
    for record in &records[2..] {
        assert_eq!(record.action, SecurityAuditClass::AuthenticationFailure);
        assert_eq!(record.outcome, SecurityAuditOutcome::Failure);
        assert_eq!(
            record.actor,
            AuditIdentity::Unverified("scram:alice".to_string())
        );
        assert_eq!(record.target, AuditField::Known("principal:1".to_string()));
    }
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
    upstream
        .simple_query("ALTER ROLE postgres PASSWORD 'rotated-fixture-value'")
        .await
        .unwrap();
    client
        .simple_query("ALTER SECRET pg_auth SET (PASSWORD = 'rotated-fixture-value')")
        .await
        .unwrap();
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
    let rotation_audits = durable_catalog
        .list_security_audit_records()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.action == SecurityAuditClass::SecretRotation)
        .collect::<Vec<_>>();
    assert_eq!(rotation_audits.len(), 1);
    let rotation_audit = &rotation_audits[0];
    assert!(rotation_audit
        .event_id
        .starts_with("gateway-secret-rotation-"));
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
    assert_eq!(rotation_audit.operation_id, AuditField::Unknown);
    assert_eq!(rotation_audit.policy_revision, AuditField::Known(1));
    assert_eq!(
        rotation_audit.before_state,
        AuditField::Known("version=1".to_string())
    );
    assert_eq!(
        rotation_audit.after_state,
        AuditField::Known("version=2".to_string())
    );
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
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_control_process(
        temp.path(),
        &storage,
        address,
        management_address,
        &pki,
        None,
        false,
    )
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
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_control_process(
        temp.path(),
        &storage,
        address,
        management_address,
        &pki,
        None,
        false,
    )
    .await;
    wait_for_gateway(&mut process.0, address).await;

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
    let expected_detail =
        "node identity mismatch: cert=worker:worker-999, requested_worker_id=worker-1, error_code=RS-2412";
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let events = FileAuditLog::open(&audit_path).unwrap().read_all().unwrap();
        if events.iter().any(|event| {
            event.action == "security.internal_mtls_denied"
                && event.detail.as_deref() == Some(expected_detail)
        }) {
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
        .any(|operation| operation.operation_id == denied_key));
    assert!(!backup_destination.exists());

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
                && record.target == AuditField::Known("BackupCreate".to_owned())
        })
        .collect::<Vec<_>>();
    assert_eq!(denials.len(), 1);
    let denial = denials[0];
    assert_eq!(
        denial.actor,
        AuditIdentity::Verified("principal:2".to_owned())
    );
    assert_eq!(
        denial.scope,
        AuditField::Known(
            serde_json::to_string(&PolicyScope::BackupDestination {
                destination: backup_destination.to_string_lossy().into_owned(),
            })
            .unwrap()
        )
    );
    assert_eq!(denial.outcome, SecurityAuditOutcome::Denied);
    assert_eq!(denial.correlation, AuditCorrelation::Unknown);
    assert_eq!(denial.operation_id, AuditField::NotApplicable);
    assert_eq!(denial.policy_revision, AuditField::Known(1));
    assert_eq!(denial.before_state, AuditField::NotApplicable);
    assert_eq!(denial.after_state, AuditField::NotApplicable);
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

    let expired_id = uuid::Uuid::new_v4().to_string();
    let expired_content = b"{\"format_version\":1,\"safe\":\"expired fixture\"}";
    let expired_blob_path = support_artifact_object_path(&expired_id);
    let foreign_id = uuid::Uuid::new_v4().to_string();
    let foreign_content = b"{\"format_version\":1,\"safe\":\"foreign fixture\"}";
    let foreign_blob_path = support_artifact_object_path(&foreign_id);
    let first_blob_path = support_artifact_object_path(&first.opaque_id);
    let management_store = object_store::prefix::PrefixStore::new(
        rockstream_test_support::minio::minio_object_store(minio_port, bucket),
        "management",
    );
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
    let ledger_path = object_store::path::Path::from("control/support-artifacts/ledger.json");
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
    let expected_audit = vec![
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
    let expected_audit = vec![
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

    let winner_id = uuid::Uuid::new_v4().to_string();
    let winner = second_client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: winner_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        winner.protocol_version,
        rockstream_management_proto::PROTOCOL_VERSION
    );
    assert_eq!(winner.opaque_id, winner_id);
    let loser_id = uuid::Uuid::new_v4().to_string();
    let loser_error = first_client
        .generate_support_artifact(GenerateSupportArtifactRequest {
            protocol_version: rockstream_management_proto::PROTOCOL_VERSION,
            node_id: String::new(),
            idempotency_key: loser_id.clone(),
        })
        .await
        .unwrap_err();
    assert_eq!(loser_error.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        loser_error.message(),
        "support artifact storage budget is full"
    );

    let (downloaded, downloaded_expiry_ms) =
        download_support_artifact(&mut first_client, &winner_id).await;
    assert_eq!(downloaded_expiry_ms, winner.expires_at_ms);
    let generated_at_ms = winner.expires_at_ms - 600_000;
    let expected_content = format!(
        "{{\"format_version\":1,\"generated_at_ms\":{generated_at_ms},\"scope\":\"cluster\",\"source_version\":\"rockstream:{}\",\"topology_revision\":1,\"active_operations\":1,\"retained_operations\":2,\"node\":null}}",
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(
        String::from_utf8(downloaded.clone()).unwrap(),
        expected_content
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
    expected_operation_ids.sort();
    assert_eq!(operation_ids, expected_operation_ids);
    for (id, state, phase) in [
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
    ] {
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
        assert_eq!(operation.source_version, "operation-record:3");
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
    let cluster_scope = serde_json::to_string(&PolicyScope::Cluster).unwrap();
    for (id, successful, expiry_ms) in [
        (winner_id.as_str(), true, winner.expires_at_ms),
        (loser_id.as_str(), false, 0),
    ] {
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
#[tokio::test]
async fn in_flight_internal_mtls_rotation_preserves_release_worker_stream() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let management_address = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_control_process(
        temp.path(),
        &storage,
        address,
        management_address,
        &pki,
        None,
        false,
    )
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

    old_worker_task.abort();
    new_worker_task.abort();
    process.0.kill().unwrap();
    process.0.wait().unwrap();
}

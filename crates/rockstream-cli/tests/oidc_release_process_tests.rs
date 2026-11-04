use std::{
    io::Read,
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use rockstream_control::audit::FileAuditLog;
use rockstream_storage::catalog::{
    CatalogDatabase, CatalogMutation, CatalogNamespace, CatalogStore, CatalogTxn,
    DurableCatalogStore,
};
use rockstream_types::{
    authorization::{
        AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
        ScopedActionGrant, ScopedPresetGrant, SecurityAction, SecurityAuditClass,
        SecurityAuditOutcome, SecurityAuditRecord, SecurityPrincipal,
        SECURITY_PRINCIPAL_FORMAT_VERSION,
    },
    ids::{DatabaseId, NamespaceId, PrincipalId},
};
use sha2::Digest;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

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
    SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "mTLS administrator".to_string(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls {
            certificate_sha256: fingerprint,
        }],
        preset_grants: vec![ScopedPresetGrant {
            preset: PermissionPreset::Administrator,
            scope: PolicyScope::Cluster,
        }],
        scoped_grants: vec![],
    }
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

fn start_control_process(
    working_dir: &Path,
    storage: &Path,
    address: std::net::SocketAddr,
    pki: &rockstream_test_support::pki::TestPki,
) -> RunningGateway {
    let child = Command::new(env!("CARGO_BIN_EXE_rockstream"))
        .current_dir(working_dir)
        .env("RUST_LOG", "off")
        .env_remove("ROCKSTREAM_CONFIG")
        .args([
            "start",
            "--role",
            "control",
            "--storage",
            storage.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
            "--control-bind",
            &address.to_string(),
            "--internal-tls-cert-path",
            pki.control_cert_path.to_str().unwrap(),
            "--internal-tls-key-path",
            pki.control_key_path.to_str().unwrap(),
            "--internal-tls-ca-cert-path",
            pki.ca_cert_path.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    RunningGateway(child)
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
async fn scram_failures_run_in_release_process_and_audit_after_restart() {
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
}

#[tokio::test]
async fn mtls_certificate_rejections_run_in_release_process_and_audit_after_restart() {
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
async fn wrong_node_certificate_is_rejected_by_release_control_process() {
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let worker_storage = temp.path().join("worker-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process = start_control_process(temp.path(), &storage, address, &pki);
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
}

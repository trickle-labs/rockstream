use std::{
    io::{BufReader, Read},
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use rockstream_control::audit::FileAuditLog;
use rockstream_storage::catalog::{
    CatalogDatabase, CatalogMutation, CatalogNamespace, CatalogSourceEntry, CatalogStore,
    CatalogTable, CatalogTxn, DurableCatalogStore,
};
use rockstream_types::{
    authorization::{
        AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
        ResourceKind, ScopedActionGrant, ScopedPresetGrant, SecurityAction, SecurityAuditClass,
        SecurityAuditOutcome, SecurityAuditRecord, SecurityPrincipal,
        SECURITY_PRINCIPAL_FORMAT_VERSION,
    },
    data_plane::{DeploymentRequest, DEPLOYMENT_DESCRIPTOR_VERSION},
    ids::{DatabaseId, NamespaceId, OperatorId, PrincipalId, ShardId, WorkerId, WorkloadId},
    topology::WorkerMessage,
};
use sha2::Digest;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

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
    pki: &rockstream_test_support::pki::TestPki,
    additional_principal: Option<SecurityPrincipal>,
    seed_worker_secret: bool,
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
    let security_store = rockstream_storage::build_runtime_object_store(
        &storage.join("gateway-shard"),
        "gateway-shard",
    )
    .unwrap();
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
            &address.to_string(),
            "--internal-tls-cert-path",
            pki.control_cert_path.to_str().unwrap(),
            "--internal-tls-key-path",
            pki.control_key_path.to_str().unwrap(),
            "--internal-tls-ca-cert-path",
            pki.ca_cert_path.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    if seed_worker_secret {
        command.env("RS_SECRET_KEK", "release-test-kek");
    }
    let child = command.spawn().unwrap();
    RunningGateway(child)
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
    let mut process = start_control_process(
        temp.path(),
        &storage,
        address,
        &pki,
        Some(actor.clone()),
        false,
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
    let mut process = start_control_process(
        temp.path(),
        &storage,
        address,
        &pki,
        Some(actor.clone()),
        true,
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

    worker
        .request_secret_token(request.workload_id, "kafka")
        .await
        .unwrap();
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

#[tokio::test]
async fn release_control_rejects_valid_wrong_node_certificate_without_registration() {
    let _port_lock = RELEASE_PROCESS_PORT_LOCK.lock().await;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let temp = tempfile::tempdir().unwrap();
    let storage = temp.path().join("control-storage");
    let pki = rockstream_test_support::pki::TestPki::generate();
    let address: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let mut process =
        start_control_process(temp.path(), &storage, address, &pki, None, false).await;
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

    let worker_storage = temp.path().join("worker-storage");
    let (worker, worker_handle) = rockstream_runtime::client::start_worker_client_with_tls(
        2,
        &address.to_string(),
        &worker_storage,
        pki.mismatched_worker_tls_config(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            catalog.refresh().await.unwrap();
            if catalog
                .list_security_audit_records()
                .await
                .unwrap()
                .iter()
                .any(|record| record.action == SecurityAuditClass::NodeIdentityRejection)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(worker.worker_id(), None);
    worker_handle.abort();
    let _ = worker_handle.await;
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
    let rejections = &records[audit_before.len()..];
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
                target: AuditField::Unverified("worker-2".to_owned()),
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
    let mut process =
        start_control_process(temp.path(), &storage, address, &pki, None, false).await;
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
        start_control_process(temp.path(), &storage, address, &pki, None, false).await;
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
    let mut process =
        start_control_process(temp.path(), &storage, address, &pki, None, false).await;
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

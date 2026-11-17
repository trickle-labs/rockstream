use std::net::SocketAddr;
use std::time::Duration;

use rockstream_cli::transport::ManagementCliClient;
use rockstream_test_support::pki::TestPki;
use rockstream_types::authorization::{
    LoginMethod, PermissionPreset, SecurityAuditClass, SecurityAuditOutcome,
};
use rockstream_types::identity::InternalTlsConfig;
use rockstream_types::ids::{ShardId, WorkerId};
use rockstream_types::lease::ShardLease;
use rockstream_types::topology::{
    CapacityHeadroom, ControlMessage, NodeRole, WorkerCapabilities, WorkerMessage,
    WorkerRegistration,
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[path = "management_security.rs"]
mod management_security;
pub use management_security::preprovision_management_catalog;

pub fn run_drain_cli(
    management_addr: SocketAddr,
    worker_id: u64,
    pki: &TestPki,
) -> serde_json::Value {
    let binary = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target/debug/rockstream");
    let output = std::process::Command::new(binary)
        .args([
            "--tls-ca-cert-path",
            pki.ca_cert_path.to_str().unwrap(),
            "--tls-cert-path",
            pki.cli_cert_path.to_str().unwrap(),
            "--tls-key-path",
            pki.cli_key_path.to_str().unwrap(),
            "--identity-role",
            "admin",
            "--management",
            &management_addr.to_string(),
            "--output",
            "json",
            "cluster",
            "workers",
            "drain",
            &worker_id.to_string(),
            "--yes",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "drain CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("drain CLI should return operation JSON")
}

pub async fn wait_for_operation_state(
    management_addr: SocketAddr,
    operation_id: &str,
    expected_state: &str,
    pki: &TestPki,
) -> serde_json::Value {
    let tls = pki.cli_tls_config();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let operation_id = operation_id.to_owned();
            let tls = tls.clone();
            let current = tokio::task::spawn_blocking(move || {
                let mut client =
                    ManagementCliClient::connect(&management_addr.to_string(), tls).unwrap();
                serde_json::to_value(client.get_operation(&operation_id).unwrap()).unwrap()
            })
            .await
            .unwrap();
            if current["state"] == expected_state {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("operation {operation_id} did not reach {expected_state}"))
}

#[tokio::test]
async fn preprovisioned_management_identity_recovers_with_audit() {
    use rockstream_storage::catalog::{CatalogStore, DurableCatalogStore};

    let pki = TestPki::generate();
    let storage_dir = tempfile::tempdir().unwrap();
    preprovision_management_catalog(storage_dir.path(), &pki).await;

    let catalog_dir = storage_dir.path().join("gateway-shard");
    let store = std::sync::Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(catalog_dir).unwrap(),
    );
    let catalog = DurableCatalogStore::recover(store, "gateway")
        .await
        .unwrap();
    let principals = catalog.list_security_principals().await.unwrap();
    assert_eq!(principals.len(), 1);
    assert!(principals[0].validate().is_ok());
    assert!(principals[0]
        .preset_grants
        .iter()
        .any(|grant| grant.preset == PermissionPreset::Administrator));
    assert!(matches!(
        principals[0].login_methods.as_slice(),
        [LoginMethod::Mtls { .. }]
    ));
    let audit = catalog.list_security_audit_records().await.unwrap();
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].action, SecurityAuditClass::GrantChange);
    assert_eq!(audit[0].outcome, SecurityAuditOutcome::Success);
}

pub struct WorkerPeer {
    reader: BufReader<tokio_rustls::client::TlsStream<TcpStream>>,
    worker_id: WorkerId,
}

impl WorkerPeer {
    pub async fn register(addr: SocketAddr, worker_id: u64, pki: &TestPki) -> Self {
        let tls = pki.worker_tls_config(worker_id);
        let connector = tls_connector(tls);
        let stream = TcpStream::connect(addr).await.unwrap();
        let server_name = rustls::pki_types::ServerName::IpAddress(addr.ip().into());
        let stream = connector.connect(server_name, stream).await.unwrap();
        let mut reader = BufReader::new(stream);
        let registration = WorkerRegistration::new(
            WorkerId(worker_id),
            NodeRole::Worker,
            format!("host-{worker_id}:7000"),
            CapacityHeadroom::FULL,
        )
        .with_capabilities(WorkerCapabilities {
            shared_shard_store_id: Some([6; 32]),
            ..Default::default()
        });
        Self::send_on(&mut reader, &WorkerMessage::Register(registration)).await;
        let worker_id = WorkerId(worker_id);
        loop {
            if matches!(Self::next_from(&mut reader).await, ControlMessage::Registered { worker_id: id } if id == worker_id)
            {
                return Self { reader, worker_id };
            }
        }
    }

    pub async fn request_shard(&mut self, shard_id: u64) -> ShardLease {
        self.request_shard_result(shard_id)
            .await
            .unwrap_or_else(|message| {
                panic!(
                    "worker {} could not request shard {shard_id}: {message}",
                    self.worker_id
                )
            })
    }

    pub async fn request_shard_result(&mut self, shard_id: u64) -> Result<ShardLease, String> {
        self.send(&WorkerMessage::RequestShard {
            worker_id: self.worker_id,
            shard_id: ShardId(shard_id),
        })
        .await;
        loop {
            match self.next().await {
                ControlMessage::ShardAssigned { lease, .. }
                    if lease.shard_id == ShardId(shard_id) =>
                {
                    return Ok(lease);
                }
                ControlMessage::OperationFailed { message, .. } => {
                    return Err(message);
                }
                _ => {}
            }
        }
    }

    pub async fn wait_for_drain(&mut self) -> ControlMessage {
        loop {
            let message = self.next().await;
            if matches!(message, ControlMessage::BeginDrain(_)) {
                return message;
            }
        }
    }

    pub async fn acknowledge_drain(&mut self) {
        self.send(&WorkerMessage::DrainAck {
            worker_id: self.worker_id,
            shards_remaining: 0,
        })
        .await;
    }

    pub async fn acknowledge_recipient(&mut self, shard_id: u64) -> ShardLease {
        let (lease, operation_id) = self.wait_for_assignment(shard_id).await;
        self.acknowledge_assignment(&lease, operation_id).await;
        lease
    }

    pub async fn wait_for_assignment(&mut self, shard_id: u64) -> (ShardLease, String) {
        loop {
            match self.next().await {
                ControlMessage::ShardAssigned {
                    lease,
                    operation_id: Some(operation_id),
                } if lease.shard_id == ShardId(shard_id) => {
                    return (lease, operation_id);
                }
                _ => {}
            }
        }
    }

    pub async fn acknowledge_assignment(&mut self, lease: &ShardLease, operation_id: String) {
        self.send(&WorkerMessage::ShardTransferAck {
            operation_id,
            stage: "recipient".to_owned(),
            worker_id: self.worker_id,
            shard_id: lease.shard_id,
            lease_token: lease.lease_token,
            success: true,
            error: None,
        })
        .await;
    }

    pub async fn next(&mut self) -> ControlMessage {
        Self::next_from(&mut self.reader).await
    }

    pub async fn send(&mut self, message: &WorkerMessage) {
        Self::send_on(&mut self.reader, message).await;
    }

    async fn send_on<S: AsyncRead + AsyncWrite + Unpin>(
        reader: &mut BufReader<S>,
        message: &WorkerMessage,
    ) {
        let line = serde_json::to_string(message).unwrap() + "\n";
        reader.get_mut().write_all(line.as_bytes()).await.unwrap();
    }

    async fn next_from<S: AsyncRead + AsyncWrite + Unpin>(
        reader: &mut BufReader<S>,
    ) -> ControlMessage {
        let mut line = String::new();
        let bytes = tokio::time::timeout(Duration::from_secs(30), reader.read_line(&mut line))
            .await
            .expect("timed out waiting for control message")
            .unwrap();
        assert_ne!(
            bytes, 0,
            "control connection closed before the expected message"
        );
        serde_json::from_str(line.trim()).unwrap()
    }
}

fn tls_connector(tls: InternalTlsConfig) -> tokio_rustls::TlsConnector {
    use std::io::BufReader;

    use rustls::pki_types::CertificateDer;
    use rustls::{ClientConfig, RootCertStore};

    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(tls.ca_cert_path.expect("test PKI CA path")).unwrap(),
    )) {
        roots.add(cert.unwrap()).unwrap();
    }
    let certificates: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(tls.cert_path.expect("worker certificate path")).unwrap(),
    ))
    .collect::<Result<_, _>>()
    .unwrap();
    let key = rustls_pemfile::private_key(&mut BufReader::new(
        std::fs::File::open(tls.key_path.expect("worker private key path")).unwrap(),
    ))
    .unwrap()
    .expect("worker private key");
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certificates, key)
        .unwrap();
    tokio_rustls::TlsConnector::from(std::sync::Arc::new(config))
}

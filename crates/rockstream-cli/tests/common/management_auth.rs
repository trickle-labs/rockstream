#![allow(dead_code)]

use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Command, Stdio};

use rockstream_management_proto::v1::management_service_client::ManagementServiceClient;
use rockstream_test_support::pki::TestPki;
use rockstream_types::authorization::{
    LoginMethod, PermissionPreset, PolicyScope, ScopedActionGrant, ScopedPresetGrant,
    SecurityAction, SecurityPrincipal, SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::ids::PrincipalId;
use sha2::{Digest, Sha256};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

fn administrator(pki: &TestPki) -> SecurityPrincipal {
    let certificates = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(&pki.cli_cert_path).expect("open administrator certificate"),
    ))
    .collect::<Result<Vec<_>, _>>()
    .expect("parse administrator certificate");
    SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "process test administrator".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls {
            certificate_sha256: Sha256::digest(certificates[0].as_ref()).into(),
        }],
        preset_grants: [PermissionPreset::Administrator, PermissionPreset::Operator]
            .into_iter()
            .map(|preset| ScopedPresetGrant {
                preset,
                scope: PolicyScope::Cluster,
            })
            .collect(),
        scoped_grants: Vec::new(),
    }
}

fn preprovision(binary: &Path, storage: &Path, layout: &str, principal: SecurityPrincipal) {
    let principal_path = storage.with_extension("admin-principal.json");
    std::fs::write(
        &principal_path,
        serde_json::to_vec(&principal).expect("serialize test administrator"),
    )
    .expect("write test administrator");
    let output = Command::new(binary)
        .args(["--json", "security", "preprovision-admin", "--storage"])
        .arg(storage)
        .args(["--layout", layout, "--principal-file"])
        .arg(&principal_path)
        .args(["--operation-id", "1", "--yes"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run supported administrator preprovisioning command");
    assert!(
        output.status.success(),
        "administrator preprovisioning failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("UTF-8 preprovision output"),
        "{\n  \"principal_id\": 1,\n  \"catalog_revision\": 1\n}\n"
    );
}

pub fn preprovision_administrator(binary: &Path, storage: &Path, pki: &TestPki, layout: &str) {
    preprovision(binary, storage, layout, administrator(pki));
}

pub fn preprovision_backup_administrator(
    binary: &Path,
    storage: &Path,
    pki: &TestPki,
    layout: &str,
    destinations: &[&Path],
) {
    let mut principal = administrator(pki);
    principal
        .scoped_grants
        .extend(destinations.iter().map(|destination| ScopedActionGrant {
            action: SecurityAction::BackupCreate,
            scope: PolicyScope::BackupDestination {
                destination: destination.display().to_string(),
            },
        }));
    preprovision(binary, storage, layout, principal);
}

pub fn add_server_tls_args(command: &mut Command, pki: &TestPki) {
    add_identity_tls_args(command, &pki.control_cert_path, &pki.control_key_path, pki);
}

pub fn add_identity_tls_args(command: &mut Command, certificate: &Path, key: &Path, pki: &TestPki) {
    command
        .args(["--internal-tls-cert-path"])
        .arg(certificate)
        .args(["--internal-tls-key-path"])
        .arg(key)
        .args(["--internal-tls-ca-cert-path"])
        .arg(&pki.ca_cert_path);
}

pub fn add_client_tls_args(command: &mut Command, pki: &TestPki) {
    command
        .args(["--tls-ca-cert-path"])
        .arg(&pki.ca_cert_path)
        .args(["--tls-cert-path"])
        .arg(&pki.cli_cert_path)
        .args(["--tls-key-path"])
        .arg(&pki.cli_key_path);
}

pub async fn management_client(
    address: SocketAddr,
    pki: &TestPki,
) -> ManagementServiceClient<Channel> {
    try_management_client(address, pki)
        .await
        .expect("connect authenticated management client")
}

pub async fn try_management_client(
    address: SocketAddr,
    pki: &TestPki,
) -> Result<ManagementServiceClient<Channel>, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(
            std::fs::read(&pki.ca_cert_path).expect("read test CA"),
        ))
        .identity(Identity::from_pem(
            std::fs::read(&pki.cli_cert_path).expect("read administrator certificate"),
            std::fs::read(&pki.cli_key_path).expect("read administrator private key"),
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

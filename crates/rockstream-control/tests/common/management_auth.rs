use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;

use object_store::memory::InMemory;
use rockstream_control::{ControlService, ManagementService, ManagementServiceHandle};
use rockstream_management_proto::v1::management_service_client::ManagementServiceClient;
use rockstream_storage::catalog::{CatalogMutation, CatalogStore, CatalogTxn, DurableCatalogStore};
use rockstream_test_support::pki::TestPki;
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, LoginMethod, PermissionPreset, PolicyScope,
    ScopedActionGrant, ScopedPresetGrant, SecurityAction, SecurityAuditClass, SecurityAuditOutcome,
    SecurityAuditRecord, SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION,
    SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::identity::InternalTlsConfig;
use rockstream_types::ids::PrincipalId;
use sha2::{Digest, Sha256};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

pub struct ManagementAuth {
    pub pki: TestPki,
    pub catalog: Arc<DurableCatalogStore>,
}

#[allow(dead_code)]
impl ManagementAuth {
    pub async fn new(backup_destinations: &[String]) -> Self {
        Self::with_presets(
            backup_destinations,
            &[PermissionPreset::Administrator, PermissionPreset::Operator],
        )
        .await
    }

    pub async fn with_presets(
        backup_destinations: &[String],
        presets: &[PermissionPreset],
    ) -> Self {
        let pki = TestPki::generate();
        let certificates =
            rustls_pemfile::certs(&mut BufReader::new(File::open(&pki.cli_cert_path).unwrap()))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        let certificate_sha256: [u8; 32] = Sha256::digest(certificates[0].as_ref()).into();
        let catalog = Arc::new(DurableCatalogStore::new(
            Arc::new(InMemory::new()),
            "management-security",
        ));
        let principal = SecurityPrincipal {
            format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
            id: PrincipalId(1),
            display_name: "Management test administrator".to_owned(),
            enabled: true,
            revision: 1,
            login_methods: vec![LoginMethod::Mtls { certificate_sha256 }],
            preset_grants: presets
                .iter()
                .map(|preset| ScopedPresetGrant {
                    preset: *preset,
                    scope: PolicyScope::Cluster,
                })
                .collect(),
            scoped_grants: backup_destinations
                .iter()
                .map(|destination| ScopedActionGrant {
                    action: SecurityAction::BackupCreate,
                    scope: PolicyScope::BackupDestination {
                        destination: destination.clone(),
                    },
                })
                .collect(),
        };
        let audit = SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: "management-test-bootstrap".to_owned(),
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
                "revision={};policy={}",
                1,
                serde_json::to_string(&(true, &principal.preset_grants, &principal.scoped_grants))
                    .unwrap()
            )),
        };
        catalog
            .commit_txn(
                CatalogTxn::new(
                    1,
                    1,
                    vec![CatalogMutation::PutSecurityPrincipal {
                        principal,
                        expected_revision: None,
                        grantor_revision: None,
                        audit,
                    }],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        Self { pki, catalog }
    }

    #[allow(dead_code)]
    pub fn reloader(&self) -> Arc<rockstream_control::tls::TlsCertificateReloader> {
        Arc::new(
            rockstream_control::tls::TlsCertificateReloader::new(self.pki.control_tls_config())
                .unwrap(),
        )
    }

    pub fn management_tls(&self) -> InternalTlsConfig {
        InternalTlsConfig {
            cert_path: Some(self.pki.control_cert_path.clone()),
            key_path: Some(self.pki.control_key_path.clone()),
            ca_cert_path: Some(self.pki.ca_cert_path.clone()),
            client_auth_required: true,
            reload_enabled: true,
        }
    }

    pub fn protect_control(&self, service: ControlService) -> ControlService {
        service
            .with_management_tls(self.management_tls())
            .with_security_catalog(self.catalog.clone() as Arc<dyn CatalogStore>)
    }

    #[allow(dead_code)]
    pub async fn start_management(&self, service: ManagementService) -> ManagementServiceHandle {
        service
            .with_security_catalog(self.catalog.clone() as Arc<dyn CatalogStore>)
            .start("127.0.0.1:0", self.reloader())
            .await
            .unwrap()
    }

    pub async fn client(&self, addr: SocketAddr) -> ManagementServiceClient<Channel> {
        self.client_with_identity(addr, &self.pki.cli_cert_path, &self.pki.cli_key_path)
            .await
    }

    pub async fn client_with_identity(
        &self,
        addr: SocketAddr,
        cert_path: &std::path::Path,
        key_path: &std::path::Path,
    ) -> ManagementServiceClient<Channel> {
        self.try_client_with_identity(addr, cert_path, key_path)
            .await
            .unwrap()
    }

    pub async fn try_client_with_identity(
        &self,
        addr: SocketAddr,
        cert_path: &std::path::Path,
        key_path: &std::path::Path,
    ) -> Result<ManagementServiceClient<Channel>, String> {
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(
                std::fs::read(&self.pki.ca_cert_path).unwrap(),
            ))
            .identity(Identity::from_pem(
                std::fs::read(cert_path).unwrap(),
                std::fs::read(key_path).unwrap(),
            ));
        let channel = Endpoint::from_shared(format!("https://{addr}"))
            .map_err(|error| error.to_string())?
            .tls_config(tls)
            .map_err(|error| error.to_string())?
            .connect()
            .await
            .map_err(|error| error.to_string())?;
        Ok(ManagementServiceClient::new(channel))
    }
}

//! TLS Helpers for Control Service mTLS Handshake (v0.55).

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rockstream_types::identity::{InternalTlsConfig, NodeIdentity};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

/// Alias for `InternalTlsConfig` for backward compatibility.
pub type TlsConfig = InternalTlsConfig;

/// Build a `TlsAcceptor` configured for internal mutual TLS (mTLS).
pub fn build_server_tls_acceptor(config: &InternalTlsConfig) -> Result<TlsAcceptor, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert_path = config
        .cert_path
        .as_ref()
        .ok_or_else(|| "RS-2410: cert_path is required for internal TLS".to_string())?;
    let key_path = config
        .key_path
        .as_ref()
        .ok_or_else(|| "RS-2410: key_path is required for internal TLS".to_string())?;

    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let mut roots = RootCertStore::empty();
    if let Some(ca_path) = &config.ca_cert_path {
        let ca_certs = load_certs(ca_path)?;
        for ca in ca_certs {
            roots
                .add(ca)
                .map_err(|e| format!("RS-2411: failed to add CA root certificate: {e}"))?;
        }
    } else if config.client_auth_required {
        return Err(
            "RS-2410: ca_cert_path is required when client_auth_required is true".to_string(),
        );
    }

    let verifier = if config.client_auth_required {
        WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| format!("RS-2411: failed to build client cert verifier: {e}"))?
    } else {
        WebPkiClientVerifier::builder(Arc::new(roots))
            .allow_unauthenticated()
            .build()
            .map_err(|e| format!("RS-2411: failed to build client cert verifier: {e}"))?
    };

    let server_config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|e| format!("RS-2411: failed to build TLS server config: {e}"))?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

use std::path::PathBuf;
use std::sync::RwLock;

/// Dynamic reloadable TLS acceptor supporting in-flight certificate swapping and dual-generation CA trust.
pub struct TlsCertificateReloader {
    config: RwLock<InternalTlsConfig>,
    acceptor: RwLock<TlsAcceptor>,
    extra_ca_roots: RwLock<Vec<PathBuf>>,
}

impl std::fmt::Debug for TlsCertificateReloader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsCertificateReloader")
            .field("config", &self.config)
            .field("extra_ca_roots", &self.extra_ca_roots)
            .finish_non_exhaustive()
    }
}

impl TlsCertificateReloader {
    /// Create a new `TlsCertificateReloader` with initial configuration.
    pub fn new(initial_config: InternalTlsConfig) -> Result<Self, String> {
        let acceptor = build_server_tls_acceptor(&initial_config)?;
        Ok(Self {
            config: RwLock::new(initial_config),
            acceptor: RwLock::new(acceptor),
            extra_ca_roots: RwLock::new(Vec::new()),
        })
    }

    /// Get a clone of the current `TlsAcceptor`.
    pub fn current_acceptor(&self) -> TlsAcceptor {
        self.acceptor.read().unwrap().clone()
    }

    /// Add an additional CA root certificate path to support dual-generation rollover.
    pub fn add_trusted_ca(&self, ca_path: impl Into<PathBuf>) -> Result<(), String> {
        let ca_path = ca_path.into();
        if !ca_path.exists() {
            return Err(format!(
                "RS-2411: CA certificate path does not exist: {}",
                ca_path.display()
            ));
        }
        self.extra_ca_roots.write().unwrap().push(ca_path);
        let cfg = self.config.read().unwrap().clone();
        self.rebuild_acceptor(&cfg)
    }

    /// Reload the server certificate, private key, and/or CA certificate in-place.
    pub fn reload(&self, new_config: InternalTlsConfig) -> Result<(), String> {
        self.rebuild_acceptor(&new_config)?;
        *self.config.write().unwrap() = new_config;
        Ok(())
    }

    fn rebuild_acceptor(&self, config: &InternalTlsConfig) -> Result<(), String> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert_path = config
            .cert_path
            .as_ref()
            .ok_or_else(|| "RS-2410: cert_path is required for internal TLS".to_string())?;
        let key_path = config
            .key_path
            .as_ref()
            .ok_or_else(|| "RS-2410: key_path is required for internal TLS".to_string())?;

        let certs = load_certs(cert_path)?;
        let key = load_key(key_path)?;

        let mut roots = RootCertStore::empty();
        if let Some(ca_path) = &config.ca_cert_path {
            let ca_certs = load_certs(ca_path)?;
            for ca in ca_certs {
                roots
                    .add(ca)
                    .map_err(|e| format!("RS-2411: failed to add CA root certificate: {e}"))?;
            }
        } else if config.client_auth_required {
            return Err(
                "RS-2410: ca_cert_path is required when client_auth_required is true".to_string(),
            );
        }

        let extra_roots = self.extra_ca_roots.read().unwrap();
        for extra_ca in extra_roots.iter() {
            if extra_ca.exists() {
                if let Ok(ca_certs) = load_certs(extra_ca) {
                    for ca in ca_certs {
                        let _ = roots.add(ca);
                    }
                }
            }
        }

        let verifier = if config.client_auth_required {
            WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| format!("RS-2411: failed to build client cert verifier: {e}"))?
        } else {
            WebPkiClientVerifier::builder(Arc::new(roots))
                .allow_unauthenticated()
                .build()
                .map_err(|e| format!("RS-2411: failed to build client cert verifier: {e}"))?
        };

        let server_config = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| format!("RS-2411: failed to build TLS server config: {e}"))?;

        let new_acceptor = TlsAcceptor::from(Arc::new(server_config));
        *self.acceptor.write().unwrap() = new_acceptor;
        Ok(())
    }
}

/// Extract and parse `NodeIdentity` from the peer certificates of a TLS stream.
pub fn extract_peer_identity(
    tls_stream: &tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
) -> Result<NodeIdentity, String> {
    let (_, session) = tls_stream.get_ref();
    let certs = session
        .peer_certificates()
        .ok_or_else(|| "RS-2410: no peer certificate presented during TLS handshake".to_string())?;
    let leaf_cert = certs
        .first()
        .ok_or_else(|| "RS-2411: peer certificate chain is empty".to_string())?;
    NodeIdentity::from_certificate_der(leaf_cert.as_ref())
}

pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = File::open(path)
        .map_err(|e| format!("RS-2411: failed to open cert file {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            format!(
                "RS-2411: failed to parse certs from {}: {e}",
                path.display()
            )
        })?;
    if certs.is_empty() {
        return Err(format!(
            "RS-2411: no certificates found in {}",
            path.display()
        ));
    }
    Ok(certs)
}

pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file = File::open(path)
        .map_err(|e| format!("RS-2411: failed to open key file {}: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    loop {
        match rustls_pemfile::read_one(&mut reader)
            .map_err(|e| format!("RS-2411: failed to read key from {}: {e}", path.display()))?
        {
            Some(rustls_pemfile::Item::Pkcs1Key(k)) => return Ok(PrivateKeyDer::Pkcs1(k)),
            Some(rustls_pemfile::Item::Pkcs8Key(k)) => return Ok(PrivateKeyDer::Pkcs8(k)),
            Some(rustls_pemfile::Item::Sec1Key(k)) => return Ok(PrivateKeyDer::Sec1(k)),
            None => break,
            _ => continue,
        }
    }
    Err(format!(
        "RS-2411: no valid private key found in {}",
        path.display()
    ))
}

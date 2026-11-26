//! Shared TLS certificate loading and live server credential reload.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::identity::InternalTlsConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;
use x509_parser::prelude::{FromDer, X509Certificate};

/// Validate the current validity window of a server's leaf certificate.
pub fn validate_server_certificate(certificate: &CertificateDer<'_>) -> Result<(), String> {
    let (_, certificate) = X509Certificate::from_der(certificate.as_ref())
        .map_err(|error| format!("failed to parse server certificate: {error}"))?;
    if !certificate.validity().is_valid() {
        return Err("RS-2411: server certificate is expired or not yet valid".to_string());
    }
    Ok(())
}

/// Build a `TlsAcceptor` configured for internal mutual TLS (mTLS).
pub fn build_server_tls_acceptor(config: &InternalTlsConfig) -> Result<TlsAcceptor, String> {
    build_server_tls_acceptor_with_extra(config, &[])
}

fn build_server_tls_acceptor_with_extra(
    config: &InternalTlsConfig,
    extra_ca_roots: &[PathBuf],
) -> Result<TlsAcceptor, String> {
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
    validate_server_certificate(&certs[0])?;

    let mut roots = RootCertStore::empty();
    if let Some(ca_path) = &config.ca_cert_path {
        for ca in load_certs(ca_path)? {
            roots
                .add(ca)
                .map_err(|e| format!("RS-2411: failed to add CA root certificate: {e}"))?;
        }
    } else if config.client_auth_required {
        return Err(
            "RS-2410: ca_cert_path is required when client_auth_required is true".to_string(),
        );
    }
    for extra_ca in extra_ca_roots {
        if extra_ca.exists() {
            if let Ok(certs) = load_certs(extra_ca) {
                for ca in certs {
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

    let mut server_config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|e| format!("RS-2411: failed to build TLS server config: {e}"))?;
    server_config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// Dynamic server credentials for in-flight certificate and CA rollover.
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
    /// Create a reloader with an initial validated server configuration.
    pub fn new(initial_config: InternalTlsConfig) -> Result<Self, String> {
        let acceptor = build_server_tls_acceptor(&initial_config)?;
        Ok(Self {
            config: RwLock::new(initial_config),
            acceptor: RwLock::new(acceptor),
            extra_ca_roots: RwLock::new(Vec::new()),
        })
    }

    /// Clone the currently active acceptor for a new connection.
    pub fn current_acceptor(&self) -> TlsAcceptor {
        self.acceptor.read().unwrap().clone()
    }

    /// Add a CA root for a dual-generation rollover window.
    pub fn add_trusted_ca(&self, ca_path: impl Into<PathBuf>) -> Result<(), String> {
        let ca_path = ca_path.into();
        if !ca_path.exists() {
            return Err(format!(
                "RS-2411: CA certificate path does not exist: {}",
                ca_path.display()
            ));
        }
        self.extra_ca_roots.write().unwrap().push(ca_path);
        let config = self.config.read().unwrap().clone();
        self.rebuild_acceptor(&config)
    }

    /// Reload server certificate, private key, and/or client CA roots in-place.
    pub fn reload(&self, new_config: InternalTlsConfig) -> Result<(), String> {
        self.rebuild_acceptor(&new_config)?;
        *self.config.write().unwrap() = new_config;
        Ok(())
    }

    fn rebuild_acceptor(&self, config: &InternalTlsConfig) -> Result<(), String> {
        let extra_roots = self.extra_ca_roots.read().unwrap();
        let acceptor = build_server_tls_acceptor_with_extra(config, &extra_roots)?;
        *self.acceptor.write().unwrap() = acceptor;
        Ok(())
    }
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
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => return Ok(PrivateKeyDer::Pkcs1(key)),
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => return Ok(PrivateKeyDer::Pkcs8(key)),
            Some(rustls_pemfile::Item::Sec1Key(key)) => return Ok(PrivateKeyDer::Sec1(key)),
            None => break,
            _ => continue,
        }
    }
    Err(format!(
        "RS-2411: no valid private key found in {}",
        path.display()
    ))
}

//! TLS Client Helpers for Worker mTLS Connections (v0.55).

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rockstream_types::identity::InternalTlsConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

/// Build a `TlsConnector` for worker client connections to the control plane.
pub fn build_client_tls_connector(config: &InternalTlsConfig) -> Result<TlsConnector, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert_path = config
        .cert_path
        .as_ref()
        .ok_or_else(|| "RS-2410: cert_path is required for internal TLS".to_string())?;
    let key_path = config
        .key_path
        .as_ref()
        .ok_or_else(|| "RS-2410: key_path is required for internal TLS".to_string())?;
    let ca_path = config
        .ca_cert_path
        .as_ref()
        .ok_or_else(|| "RS-2410: ca_cert_path is required for internal TLS".to_string())?;

    let certs = load_certs(cert_path)?;
    let key = load_key(key_path)?;

    let mut roots = RootCertStore::empty();
    let ca_certs = load_certs(ca_path)?;
    for ca in ca_certs {
        roots
            .add(ca)
            .map_err(|e| format!("RS-2411: failed to add CA root certificate: {e}"))?;
    }

    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| format!("RS-2411: failed to build TLS client config: {e}"))?;

    Ok(TlsConnector::from(Arc::new(client_config)))
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

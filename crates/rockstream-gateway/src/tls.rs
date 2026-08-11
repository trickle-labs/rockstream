//! v0.51.5: gateway-facing (client SQL-port) TLS termination and mTLS client
//! certificate CN extraction.
//!
//! Distinct from v0.56's *internal* control<->worker/worker<->worker mTLS —
//! this module is only the client-facing pgwire TLS handshake.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use dashmap::DashMap;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::WebPkiClientVerifier;
use rustls::{DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme};
use tokio_rustls::TlsAcceptor;

use crate::server::MAX_CONNECTIONS;
use rockstream_types::error_code::RS_2406;

/// Error building the gateway's TLS configuration from configured
/// cert/key/CA paths, or a startup misconfiguration (mTLS without a CA).
#[derive(Debug, thiserror::Error)]
pub enum GatewayTlsError {
    /// `[RS-2405] auth.tls_config_invalid`: failed to read a configured path.
    #[error(
        "[RS-2405] auth.tls_config_invalid: failed to read `{path}`: {source}. \
         next_steps: Verify the configured paths point to valid PEM-encoded \
         certificate/key files readable by the gateway process."
    )]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    /// `[RS-2405] auth.tls_config_invalid`: PEM parsing or rustls config
    /// construction failed.
    #[error(
        "[RS-2405] auth.tls_config_invalid: {0}. next_steps: Verify the \
         configured paths point to valid PEM-encoded certificate/key files \
         readable by the gateway process."
    )]
    Parse(String),
    /// `[RS-2403] auth.mtls_requires_ca_cert`: `--auth=mtls` was set without
    /// `tls_ca_cert_path`.
    #[error(
        "[RS-2403] auth.mtls_requires_ca_cert: --auth=mtls requires \
         --tls-ca-cert-path (or gateway.tls_ca_cert_path in rockstream.toml). \
         next_steps: Set --tls-ca-cert-path to the CA that signs client \
         certificates."
    )]
    MtlsRequiresCaCert,
}

impl From<rustls::Error> for GatewayTlsError {
    fn from(e: rustls::Error) -> Self {
        GatewayTlsError::Parse(e.to_string())
    }
}

fn read_path(path: &Path) -> Result<Vec<u8>, GatewayTlsError> {
    std::fs::read(path).map_err(|source| GatewayTlsError::Io {
        path: path.display().to_string(),
        source,
    })
}

/// Load a PEM-encoded certificate chain from `path`.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, GatewayTlsError> {
    let bytes = read_path(path)?;
    rustls_pemfile::certs(&mut bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| GatewayTlsError::Parse(format!("invalid certificate PEM in {path:?}: {e}")))
}

/// Load a single PEM-encoded private key from `path`.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, GatewayTlsError> {
    let bytes = read_path(path)?;
    rustls_pemfile::private_key(&mut bytes.as_slice())
        .map_err(|e| GatewayTlsError::Parse(format!("invalid private key PEM in {path:?}: {e}")))?
        .ok_or_else(|| GatewayTlsError::Parse(format!("no private key found in {path:?}")))
}

/// Fail-fast check: `--auth=mtls` without a configured CA is a startup
/// misconfiguration, not a runtime fallback to an unauthenticated identity.
pub fn require_ca_cert_for_mtls(
    auth_mode_is_mtls: bool,
    ca_cert_path: Option<&Path>,
) -> Result<(), GatewayTlsError> {
    if auth_mode_is_mtls && ca_cert_path.is_none() {
        return Err(GatewayTlsError::MtlsRequiresCaCert);
    }
    Ok(())
}

/// Build a `TlsAcceptor` from configured PEM paths. When `ca_cert_path` is
/// `Some`, client certificate authentication is *required* (mTLS); when
/// `None`, no client auth is requested.
pub fn build_tls_acceptor(
    cert_path: &Path,
    key_path: &Path,
    ca_cert_path: Option<&Path>,
) -> Result<Arc<TlsAcceptor>, GatewayTlsError> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    let config = if let Some(ca_path) = ca_cert_path {
        let ca_certs = load_certs(ca_path)?;
        let mut roots = RootCertStore::empty();
        for cert in ca_certs {
            roots
                .add(cert)
                .map_err(|e| GatewayTlsError::Parse(format!("invalid CA certificate: {e}")))?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| GatewayTlsError::Parse(format!("failed to build client verifier: {e}")))?;
        let cn_extracting_verifier: Arc<dyn ClientCertVerifier> =
            Arc::new(MtlsCnExtractingVerifier { inner: verifier });
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(cn_extracting_verifier)
            .with_single_cert(certs, key)?
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?
    };

    Ok(Arc::new(TlsAcceptor::from(Arc::new(config))))
}

// ── mTLS client-certificate CN extraction ─────────────────────────────────
//
// pgwire's `ClientInfo`/`DefaultClient` exposes no post-handshake peer
// certificate data (no API to retrieve the negotiated `ServerConnection`'s
// verified peer certificate from inside `StartupHandler::on_startup`). CN
// extraction is therefore wired through a bounded, connection-keyed side
// channel populated synchronously right after the TLS handshake completes,
// in the same task that accepted the connection.

/// Bound: `MAX_CONNECTIONS` (10_000), the same cap the existing
/// `cancellation_registry` already enforces — one entry per live connection.
/// Fill-level metric: `gateway_mtls_cn_cache_size()`.
pub static MTLS_CN_BY_PEER_ADDR: LazyLock<DashMap<SocketAddr, String>> =
    LazyLock::new(DashMap::new);

/// Current fill level of `MTLS_CN_BY_PEER_ADDR` (gauge metric
/// `gateway_mtls_cn_cache_size`).
pub fn mtls_cn_cache_size() -> usize {
    MTLS_CN_BY_PEER_ADDR.len()
}

/// Number of times a peer's CN was successfully extracted and recorded.
/// Diagnostic counter, not itself the bounded fill-level metric.
pub static MTLS_CN_RECORDED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Parse the Subject CN out of a leaf certificate's DER bytes.
fn parse_leaf_cn(cert_der: &CertificateDer<'_>) -> Option<String> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert_der.as_ref()).ok()?;
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(|s| s.to_string());
    cn
}

/// After a successful mTLS handshake, extract the verified leaf certificate's
/// Subject CN and record it keyed by the connection's peer address, read
/// from the `crate::server::PEER_ADDR` task-local (set once per accepted
/// connection, before the TLS handshake begins, in the same task that runs
/// the handshake — so it's synchronously available here even though this
/// runs deep inside rustls's handshake state machine). Bounded by
/// `MAX_CONNECTIONS`: never grows past the connection cap; full is silently
/// skipped (fails open to "no verified CN", never crashes the handshake),
/// which the `AuthMode::Mtls` startup branch then treats as a hard
/// authentication failure (`RS-2404`), not a silent fallback identity.
#[derive(Debug)]
struct MtlsCnExtractingVerifier {
    inner: Arc<dyn ClientCertVerifier>,
}

impl ClientCertVerifier for MtlsCnExtractingVerifier {
    fn offer_client_auth(&self) -> bool {
        self.inner.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.inner.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        if let Some(cn) = parse_leaf_cn(end_entity) {
            if let Ok(peer_addr) = crate::server::PEER_ADDR.try_with(|a| *a) {
                if MTLS_CN_BY_PEER_ADDR.len() >= MAX_CONNECTIONS {
                    tracing::error!(
                        code = %RS_2406,
                        peer_addr = %peer_addr,
                        max_connections = MAX_CONNECTIONS,
                        "[RS-2406] mTLS handshake rejected: identity map at capacity. next_steps: Reduce concurrent connections or raise MAX_CONNECTIONS."
                    );
                    return Err(rustls::Error::General(format!(
                        "[RS-2406] mTLS connection cap ({MAX_CONNECTIONS}) reached; handshake rejected. next_steps: Reduce concurrent connections or raise MAX_CONNECTIONS."
                    )));
                }
                MTLS_CN_BY_PEER_ADDR.insert(peer_addr, cn);
                rockstream_types::metrics::set_mtls_cn_cache_size(MTLS_CN_BY_PEER_ADDR.len() as u64);
                MTLS_CN_RECORDED_TOTAL.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Remove `peer_addr`'s recorded CN on disconnect so the map never grows
/// unbounded across reconnects.
pub fn remove_mtls_cn(peer_addr: &SocketAddr) {
    MTLS_CN_BY_PEER_ADDR.remove(peer_addr);
    rockstream_types::metrics::set_mtls_cn_cache_size(MTLS_CN_BY_PEER_ADDR.len() as u64);
}

/// Look up the CN recorded for `peer_addr` during the mTLS handshake (called
/// from `AuthMode::Mtls`'s `on_startup` branch via `client.socket_addr()`).
pub fn lookup_mtls_cn(peer_addr: &SocketAddr) -> Option<String> {
    MTLS_CN_BY_PEER_ADDR.get(peer_addr).map(|v| v.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn require_ca_cert_for_mtls_fails_fast_without_ca() {
        let err = require_ca_cert_for_mtls(true, None).unwrap_err();
        assert!(matches!(err, GatewayTlsError::MtlsRequiresCaCert));
        assert!(err.to_string().contains("RS-2403"));
    }

    #[test]
    fn require_ca_cert_for_mtls_ok_with_ca() {
        let path = Path::new("/tmp/ca.pem");
        assert!(require_ca_cert_for_mtls(true, Some(path)).is_ok());
    }

    #[test]
    fn require_ca_cert_for_mtls_ok_when_not_mtls() {
        assert!(require_ca_cert_for_mtls(false, None).is_ok());
    }

    #[test]
    fn build_tls_acceptor_reports_rs_2405_for_missing_cert_file() {
        let result = build_tls_acceptor(
            Path::new("/nonexistent/cert.pem"),
            Path::new("/nonexistent/key.pem"),
            None,
        );
        let err = match result {
            Ok(_) => panic!("expected an error for a nonexistent cert path"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("RS-2405"));
    }
}

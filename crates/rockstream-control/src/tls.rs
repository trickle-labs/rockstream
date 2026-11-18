//! TLS helpers for control service mTLS handshakes.

use rockstream_types::identity::NodeIdentity;

/// Alias for `InternalTlsConfig` for backward compatibility.
pub type TlsConfig = rockstream_types::identity::InternalTlsConfig;

pub use rockstream_types::tls::{
    build_server_tls_acceptor, load_certs, load_key, TlsCertificateReloader,
};

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

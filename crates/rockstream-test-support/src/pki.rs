//! PKI Test Utilities for Internal mTLS Testing (v0.55).
//!
//! Generates valid, expired, untrusted, and identity-mismatched X.509 certificates
//! for integration and unit testing.

use rcgen::{CertificateParams, DnType, IsCa, KeyPair};
use rockstream_types::identity::{InternalTlsConfig, NodeIdentity};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Complete test PKI harness with CA and role-specific certificates.
pub struct TestPki {
    _dir: TempDir,
    pub ca_cert_pem: String,
    pub ca_key_pem: String,
    pub ca_cert_path: PathBuf,
    pub control_cert_path: PathBuf,
    pub control_key_path: PathBuf,
    pub worker_certs: HashMap<u64, (PathBuf, PathBuf)>,
    pub cli_cert_path: PathBuf,
    pub cli_key_path: PathBuf,
    pub untrusted_ca_cert_path: PathBuf,
    pub untrusted_cert_path: PathBuf,
    pub untrusted_key_path: PathBuf,
    pub expired_cert_path: PathBuf,
    pub expired_key_path: PathBuf,
    pub mismatched_cert_path: PathBuf,
    pub mismatched_key_path: PathBuf,
}

impl TestPki {
    /// Generate a fresh test PKI fixture with all certificates configured.
    pub fn generate() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let (ca_cert, ca_key) = make_ca("RockStream Test Cluster CA");
        let ca_cert_pem = ca_cert.pem();
        let ca_key_pem = ca_key.serialize_pem();
        let ca_cert_path = write_file(dir.path(), "cluster-ca.pem", &ca_cert_pem);

        // Control node cert
        let (ctrl_cert_pem, ctrl_key_pem) = make_leaf(
            &ca_cert,
            &ca_key,
            &NodeIdentity::control("control-1").to_cn(),
            vec![
                "localhost".to_string(),
                "127.0.0.1".to_string(),
                "control".to_string(),
            ],
        );
        let control_cert_path = write_file(dir.path(), "control.pem", &ctrl_cert_pem);
        let control_key_path = write_file(dir.path(), "control-key.pem", &ctrl_key_pem);

        // Worker certs (workers 1..=5)
        let mut worker_certs = HashMap::new();
        for id in 1..=5 {
            let (w_cert_pem, w_key_pem) = make_leaf(
                &ca_cert,
                &ca_key,
                &NodeIdentity::worker(format!("worker-{id}")).to_cn(),
                vec![
                    "localhost".to_string(),
                    "127.0.0.1".to_string(),
                    format!("worker-{id}"),
                ],
            );
            let cert_path = write_file(dir.path(), &format!("worker-{id}.pem"), &w_cert_pem);
            let key_path = write_file(dir.path(), &format!("worker-{id}-key.pem"), &w_key_pem);
            worker_certs.insert(id, (cert_path, key_path));
        }

        // CLI admin cert
        let (cli_cert_pem, cli_key_pem) = make_leaf(
            &ca_cert,
            &ca_key,
            &NodeIdentity::cli("admin").to_cn(),
            vec!["localhost".to_string(), "127.0.0.1".to_string()],
        );
        let cli_cert_path = write_file(dir.path(), "cli.pem", &cli_cert_pem);
        let cli_key_path = write_file(dir.path(), "cli-key.pem", &cli_key_pem);

        // Untrusted CA and leaf
        let (untrusted_ca, untrusted_ca_key) = make_ca("Rogue Untrusted CA");
        let untrusted_ca_pem = untrusted_ca.pem();
        let untrusted_ca_cert_path = write_file(dir.path(), "untrusted-ca.pem", &untrusted_ca_pem);

        let (untrusted_cert_pem, untrusted_key_pem) = make_leaf(
            &untrusted_ca,
            &untrusted_ca_key,
            &NodeIdentity::worker("worker-1").to_cn(),
            vec!["localhost".to_string(), "127.0.0.1".to_string()],
        );
        let untrusted_cert_path =
            write_file(dir.path(), "untrusted-worker.pem", &untrusted_cert_pem);
        let untrusted_key_path =
            write_file(dir.path(), "untrusted-worker-key.pem", &untrusted_key_pem);

        // Expired certificate (simulated: not_after before not_before)
        let (expired_cert_pem, expired_key_pem) =
            make_expired_leaf(&ca_cert, &ca_key, &NodeIdentity::worker("worker-1").to_cn());
        let expired_cert_path = write_file(dir.path(), "expired-worker.pem", &expired_cert_pem);
        let expired_key_path = write_file(dir.path(), "expired-worker-key.pem", &expired_key_pem);

        // Mismatched worker identity cert (cert says worker-999)
        let (mismatched_cert_pem, mismatched_key_pem) = make_leaf(
            &ca_cert,
            &ca_key,
            &NodeIdentity::worker("worker-999").to_cn(),
            vec!["localhost".to_string(), "127.0.0.1".to_string()],
        );
        let mismatched_cert_path =
            write_file(dir.path(), "mismatched-worker.pem", &mismatched_cert_pem);
        let mismatched_key_path =
            write_file(dir.path(), "mismatched-worker-key.pem", &mismatched_key_pem);

        Self {
            _dir: dir,
            ca_cert_pem,
            ca_key_pem,
            ca_cert_path,
            control_cert_path,
            control_key_path,
            worker_certs,
            cli_cert_path,
            cli_key_path,
            untrusted_ca_cert_path,
            untrusted_cert_path,
            untrusted_key_path,
            expired_cert_path,
            expired_key_path,
            mismatched_cert_path,
            mismatched_key_path,
        }
    }

    /// Internal TLS configuration for the control service.
    pub fn control_tls_config(&self) -> InternalTlsConfig {
        InternalTlsConfig {
            cert_path: Some(self.control_cert_path.clone()),
            key_path: Some(self.control_key_path.clone()),
            ca_cert_path: Some(self.ca_cert_path.clone()),
            client_auth_required: true,
            reload_enabled: true,
        }
    }

    /// Internal TLS configuration for a specific worker.
    pub fn worker_tls_config(&self, worker_id: u64) -> InternalTlsConfig {
        let (cert, key) = self
            .worker_certs
            .get(&worker_id)
            .cloned()
            .unwrap_or_else(|| {
                panic!("Worker {} cert not found in test PKI", worker_id);
            });
        InternalTlsConfig {
            cert_path: Some(cert),
            key_path: Some(key),
            ca_cert_path: Some(self.ca_cert_path.clone()),
            client_auth_required: true,
            reload_enabled: true,
        }
    }

    /// Internal TLS configuration for CLI operations.
    pub fn cli_tls_config(&self) -> InternalTlsConfig {
        InternalTlsConfig {
            cert_path: Some(self.cli_cert_path.clone()),
            key_path: Some(self.cli_key_path.clone()),
            ca_cert_path: Some(self.ca_cert_path.clone()),
            client_auth_required: true,
            reload_enabled: true,
        }
    }

    /// Internal TLS configuration with an untrusted client cert.
    pub fn untrusted_worker_tls_config(&self) -> InternalTlsConfig {
        InternalTlsConfig {
            cert_path: Some(self.untrusted_cert_path.clone()),
            key_path: Some(self.untrusted_key_path.clone()),
            ca_cert_path: Some(self.ca_cert_path.clone()),
            client_auth_required: true,
            reload_enabled: true,
        }
    }

    /// Internal TLS configuration with an expired client cert.
    pub fn expired_worker_tls_config(&self) -> InternalTlsConfig {
        InternalTlsConfig {
            cert_path: Some(self.expired_cert_path.clone()),
            key_path: Some(self.expired_key_path.clone()),
            ca_cert_path: Some(self.ca_cert_path.clone()),
            client_auth_required: true,
            reload_enabled: true,
        }
    }

    /// Internal TLS configuration with an identity mismatch cert.
    pub fn mismatched_worker_tls_config(&self) -> InternalTlsConfig {
        InternalTlsConfig {
            cert_path: Some(self.mismatched_cert_path.clone()),
            key_path: Some(self.mismatched_key_path.clone()),
            ca_cert_path: Some(self.ca_cert_path.clone()),
            client_auth_required: true,
            reload_enabled: true,
        }
    }

    /// Generate a fresh Generation-2 CA and new certificates for dual-generation CA rollover tests.
    /// Generate a fresh Generation-2 CA and new certificates for dual-generation CA rollover tests.
    pub fn generate_gen2_pki(&self) -> (PathBuf, InternalTlsConfig, InternalTlsConfig) {
        let (ca2_cert, ca2_key) = make_ca("RockStream Test Cluster CA Gen2");
        let ca2_pem = ca2_cert.pem();
        let ca2_path = write_file(self._dir.path(), "cluster-ca-gen2.pem", &ca2_pem);

        let both_ca_pem = format!("{}\n{}", self.ca_cert_pem, ca2_pem);
        let both_ca_path = write_file(self._dir.path(), "cluster-ca-bundle.pem", &both_ca_pem);

        let (ctrl2_cert_pem, ctrl2_key_pem) = make_leaf(
            &ca2_cert,
            &ca2_key,
            &NodeIdentity::control("control-1").to_cn(),
            vec![
                "localhost".to_string(),
                "127.0.0.1".to_string(),
                "control".to_string(),
            ],
        );
        let ctrl2_cert_path = write_file(self._dir.path(), "control-gen2.pem", &ctrl2_cert_pem);
        let ctrl2_key_path = write_file(self._dir.path(), "control-gen2-key.pem", &ctrl2_key_pem);

        for id in 1..=5 {
            let (w_cert_pem, w_key_pem) = make_leaf(
                &ca2_cert,
                &ca2_key,
                &NodeIdentity::worker(format!("worker-{id}")).to_cn(),
                vec![
                    "localhost".to_string(),
                    "127.0.0.1".to_string(),
                    format!("worker-{id}"),
                ],
            );
            let cert_path = write_file(
                self._dir.path(),
                &format!("worker-{id}-gen2.pem"),
                &w_cert_pem,
            );
            let key_path = write_file(
                self._dir.path(),
                &format!("worker-{id}-gen2-key.pem"),
                &w_key_pem,
            );
            let _ = (cert_path, key_path);
        }

        let w1_cert_path = self._dir.path().join("worker-1-gen2.pem");
        let w1_key_path = self._dir.path().join("worker-1-gen2-key.pem");

        let ctrl_config = InternalTlsConfig {
            cert_path: Some(ctrl2_cert_path),
            key_path: Some(ctrl2_key_path),
            ca_cert_path: Some(both_ca_path.clone()),
            client_auth_required: true,
            reload_enabled: true,
        };

        let worker_config = InternalTlsConfig {
            cert_path: Some(w1_cert_path),
            key_path: Some(w1_key_path),
            ca_cert_path: Some(both_ca_path.clone()),
            client_auth_required: true,
            reload_enabled: true,
        };

        (ca2_path, ctrl_config, worker_config)
    }

    /// Return a worker TLS config for a Gen2 worker.
    pub fn gen2_worker_tls_config(&self, worker_id: u64) -> InternalTlsConfig {
        let cert_path = self
            ._dir
            .path()
            .join(format!("worker-{worker_id}-gen2.pem"));
        let key_path = self
            ._dir
            .path()
            .join(format!("worker-{worker_id}-gen2-key.pem"));
        let ca_path = self._dir.path().join("cluster-ca-bundle.pem");
        InternalTlsConfig {
            cert_path: Some(cert_path),
            key_path: Some(key_path),
            ca_cert_path: Some(ca_path),
            client_auth_required: true,
            reload_enabled: true,
        }
    }

    /// Return a worker TLS config for a Gen1 worker with dual-generation CA bundle.
    pub fn worker_tls_config_with_ca(&self, worker_id: u64, ca_path: PathBuf) -> InternalTlsConfig {
        let (cert, key) = self
            .worker_certs
            .get(&worker_id)
            .cloned()
            .unwrap_or_else(|| {
                panic!("Worker {} cert not found in test PKI", worker_id);
            });
        InternalTlsConfig {
            cert_path: Some(cert),
            key_path: Some(key),
            ca_cert_path: Some(ca_path),
            client_auth_required: true,
            reload_enabled: true,
        }
    }
}

fn write_file(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    let mut f = File::create(&path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    path
}

fn make_ca(common_name: &str) -> (rcgen::Certificate, KeyPair) {
    let mut params = CertificateParams::new(vec![]).unwrap();
    params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    (cert, key_pair)
}

fn make_leaf(
    ca_cert: &rcgen::Certificate,
    ca_key: &KeyPair,
    cn: &str,
    sans: Vec<String>,
) -> (String, String) {
    let mut params = CertificateParams::new(sans).unwrap();
    params.distinguished_name.push(DnType::CommonName, cn);
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key_pair, ca_cert, ca_key).unwrap();
    (cert.pem(), key_pair.serialize_pem())
}

fn make_expired_leaf(ca_cert: &rcgen::Certificate, ca_key: &KeyPair, cn: &str) -> (String, String) {
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.distinguished_name.push(DnType::CommonName, cn);
    // Set validity to a past timestamp
    params.not_before = rcgen::date_time_ymd(2020, 1, 1);
    params.not_after = rcgen::date_time_ymd(2020, 1, 2);
    let key_pair = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key_pair, ca_cert, ca_key).unwrap();
    (cert.pem(), key_pair.serialize_pem())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::identity::NodeRole;

    #[test]
    fn test_pki_generation_and_validation() {
        let pki = TestPki::generate();
        let ctrl_cfg = pki.control_tls_config();
        assert!(ctrl_cfg.is_enabled());
        assert!(ctrl_cfg.validate().is_ok());

        let w1_cfg = pki.worker_tls_config(1);
        assert!(w1_cfg.is_enabled());
        assert!(w1_cfg.validate().is_ok());

        let cli_cfg = pki.cli_tls_config();
        assert!(cli_cfg.is_enabled());
        assert!(cli_cfg.validate().is_ok());
    }

    #[test]
    fn test_pki_der_identity_extraction() {
        let pki = TestPki::generate();
        let cert_pem = std::fs::read_to_string(&pki.control_cert_path).unwrap();
        let mut reader = std::io::BufReader::new(cert_pem.as_bytes());
        let item = rustls_pemfile::certs(&mut reader).next().unwrap().unwrap();
        let identity = NodeIdentity::from_certificate_der(&item).unwrap();
        assert_eq!(identity.role, NodeRole::Control);
        assert_eq!(identity.node_id, "control-1");
    }
}

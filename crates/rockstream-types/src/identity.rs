//! Internal Node Identity and mTLS Configuration Types (v0.55).
//!
//! Provides `NodeIdentity` (extracted from client X.509 certificates)
//! and `InternalTlsConfig` for mutual TLS across internal cluster channels.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use x509_parser::prelude::*;

/// Node role in the RockStream cluster topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Control,
    Worker,
    Cli,
    Gateway,
    Aggregator,
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control => write!(f, "control"),
            Self::Worker => write!(f, "worker"),
            Self::Cli => write!(f, "cli"),
            Self::Gateway => write!(f, "gateway"),
            Self::Aggregator => write!(f, "aggregator"),
        }
    }
}

impl FromStr for NodeRole {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "control" => Ok(Self::Control),
            "worker" => Ok(Self::Worker),
            "cli" | "client" => Ok(Self::Cli),
            "gateway" => Ok(Self::Gateway),
            "aggregator" | "frontier" => Ok(Self::Aggregator),
            other => Err(format!("RS-2411: unknown node role: {other}")),
        }
    }
}

/// Extracted cryptographic identity for an authenticated node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeIdentity {
    /// Role of the node.
    pub role: NodeRole,
    /// Unique identifier for the node (e.g. "worker-1", "control-1", "cli-admin").
    pub node_id: String,
    /// Optional cluster identifier for multi-cluster segregation.
    pub cluster_id: Option<String>,
}

impl NodeIdentity {
    /// Create a new `NodeIdentity`.
    pub fn new(
        role: NodeRole,
        node_id: impl Into<String>,
        cluster_id: Option<impl Into<String>>,
    ) -> Self {
        Self {
            role,
            node_id: node_id.into(),
            cluster_id: cluster_id.map(Into::into),
        }
    }

    /// Create a worker identity.
    pub fn worker(node_id: impl Into<String>) -> Self {
        Self::new(NodeRole::Worker, node_id, None::<String>)
    }

    /// Create a control-plane identity.
    pub fn control(node_id: impl Into<String>) -> Self {
        Self::new(NodeRole::Control, node_id, None::<String>)
    }

    /// Create a CLI identity.
    pub fn cli(node_id: impl Into<String>) -> Self {
        Self::new(NodeRole::Cli, node_id, None::<String>)
    }

    /// Format this identity as an X.509 Common Name (CN).
    ///
    /// Formatted as `role:node_id` or `role:node_id:cluster_id`.
    pub fn to_cn(&self) -> String {
        if let Some(cluster) = &self.cluster_id {
            format!("{}:{}:{}", self.role, self.node_id, cluster)
        } else {
            format!("{}:{}", self.role, self.node_id)
        }
    }

    /// Parse a `NodeIdentity` from an X.509 Common Name (CN) string.
    ///
    /// Supports:
    /// - Colon-separated: `role:node_id` or `role:node_id:cluster_id`
    /// - Slash-separated: `role/node_id` or `role/node_id/cluster_id`
    /// - Hyphen-prefixed fallback: `worker-1` -> role=Worker, id="worker-1" or "1"
    /// - Role-only fallback: `control` -> role=Control, id="control"
    pub fn from_cn(cn: &str) -> Result<Self, String> {
        let trimmed = cn.trim();
        if trimmed.is_empty() {
            return Err("RS-2411: empty Common Name".to_string());
        }

        // Try colon-separated: "worker:worker-1" or "worker:worker-1:cluster-a"
        if trimmed.contains(':') {
            let parts: Vec<&str> = trimmed.split(':').collect();
            let role = parts[0].parse::<NodeRole>()?;
            let node_id = parts
                .get(1)
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "RS-2411: missing node_id in CN".to_string())?;
            let cluster_id = parts
                .get(2)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            return Ok(Self {
                role,
                node_id: node_id.to_string(),
                cluster_id,
            });
        }

        // Try slash-separated: "worker/worker-1" or "worker/worker-1/cluster-a"
        if trimmed.contains('/') {
            let parts: Vec<&str> = trimmed.split('/').collect();
            let role = parts[0].parse::<NodeRole>()?;
            let node_id = parts
                .get(1)
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "RS-2411: missing node_id in CN".to_string())?;
            let cluster_id = parts
                .get(2)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            return Ok(Self {
                role,
                node_id: node_id.to_string(),
                cluster_id,
            });
        }

        // Try hyphen prefix: "worker-1", "control-1", "cli-admin"
        if let Some((role_prefix, _rest)) = trimmed.split_once('-') {
            if let Ok(role) = role_prefix.parse::<NodeRole>() {
                return Ok(Self {
                    role,
                    node_id: trimmed.to_string(),
                    cluster_id: None,
                });
            }
        }

        // Try role name directly: "worker", "control", "cli"
        if let Ok(role) = trimmed.parse::<NodeRole>() {
            return Ok(Self {
                role,
                node_id: trimmed.to_string(),
                cluster_id: None,
            });
        }

        // Default fallback for client/admin certs
        Ok(Self {
            role: NodeRole::Cli,
            node_id: trimmed.to_string(),
            cluster_id: None,
        })
    }

    /// Extract and parse `NodeIdentity` from a DER-encoded X.509 certificate.
    pub fn from_certificate_der(der: &[u8]) -> Result<Self, String> {
        let (_, cert) = parse_x509_certificate(der)
            .map_err(|e| format!("RS-2411: failed to parse X.509 certificate DER: {e}"))?;

        // Extract Common Name from Subject
        for rdn in cert.subject().iter() {
            for attr in rdn.iter() {
                if attr.attr_type() == &oid_registry::OID_X509_COMMON_NAME {
                    if let Ok(cn_str) = attr.as_str() {
                        return Self::from_cn(cn_str);
                    }
                }
            }
        }

        // If no CN, check SAN DNS names
        if let Ok(Some(san_ext)) = cert.subject_alternative_name() {
            for name in &san_ext.value.general_names {
                if let GeneralName::DNSName(dns) = name {
                    return Self::from_cn(dns);
                }
            }
        }

        Err("RS-2411: certificate does not contain a Common Name (CN) or SAN DNS entry".to_string())
    }

    /// Check if this identity matches a given numeric worker ID or string worker ID.
    pub fn matches_worker_id(&self, worker_id: u64) -> bool {
        if self.role != NodeRole::Worker {
            return false;
        }
        let id_str = worker_id.to_string();
        let prefixed = format!("worker-{worker_id}");
        self.node_id == id_str || self.node_id == prefixed || self.node_id == "worker"
    }

    /// Check if this identity matches a given string worker ID or name.
    pub fn matches_worker_str(&self, id: &str) -> bool {
        if self.role != NodeRole::Worker {
            return false;
        }
        let trimmed = id.trim();
        self.node_id == trimmed
            || self.node_id == format!("worker-{}", trimmed)
            || trimmed == format!("worker-{}", self.node_id)
            || self.node_id == "worker"
    }
}

/// Configuration for internal mutual TLS (mTLS) between cluster nodes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct InternalTlsConfig {
    /// Path to the PEM-encoded certificate chain presented by this node.
    pub cert_path: Option<PathBuf>,
    /// Path to the PEM-encoded private key for `cert_path`.
    pub key_path: Option<PathBuf>,
    /// Path to the PEM-encoded CA certificate root used to verify peer certificates.
    pub ca_cert_path: Option<PathBuf>,
    /// Whether peer certificate verification is strictly required (mTLS). Default: true.
    pub client_auth_required: bool,
    /// Whether atomic live reloading of certificates on disk is enabled. Default: true.
    pub reload_enabled: bool,
}

impl Default for InternalTlsConfig {
    fn default() -> Self {
        Self {
            cert_path: None,
            key_path: None,
            ca_cert_path: None,
            client_auth_required: true,
            reload_enabled: true,
        }
    }
}

impl InternalTlsConfig {
    /// Returns `true` if internal TLS is configured on this node.
    pub fn is_enabled(&self) -> bool {
        self.cert_path.is_some() && self.key_path.is_some()
    }

    /// Validate the internal TLS configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.is_enabled() {
            if let Some(cert) = &self.cert_path {
                if !cert.exists() {
                    return Err(format!(
                        "RS-2410: internal TLS cert path does not exist: {}",
                        cert.display()
                    ));
                }
            }
            if let Some(key) = &self.key_path {
                if !key.exists() {
                    return Err(format!(
                        "RS-2410: internal TLS key path does not exist: {}",
                        key.display()
                    ));
                }
            }
            if self.client_auth_required {
                if let Some(ca) = &self.ca_cert_path {
                    if !ca.exists() {
                        return Err(format!(
                            "RS-2410: internal TLS CA cert path does not exist: {}",
                            ca.display()
                        ));
                    }
                } else {
                    return Err(
                        "RS-2410: client_auth_required is true but ca_cert_path is not set"
                            .to_string(),
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_identity_to_from_cn() {
        let id1 = NodeIdentity::new(NodeRole::Worker, "worker-1", None::<String>);
        assert_eq!(id1.to_cn(), "worker:worker-1");
        let parsed1 = NodeIdentity::from_cn("worker:worker-1").unwrap();
        assert_eq!(parsed1, id1);

        let id2 = NodeIdentity::new(NodeRole::Control, "control-1", Some("cluster-a"));
        assert_eq!(id2.to_cn(), "control:control-1:cluster-a");
        let parsed2 = NodeIdentity::from_cn("control:control-1:cluster-a").unwrap();
        assert_eq!(parsed2, id2);

        let id3 = NodeIdentity::from_cn("worker/worker-42").unwrap();
        assert_eq!(id3.role, NodeRole::Worker);
        assert_eq!(id3.node_id, "worker-42");

        let id4 = NodeIdentity::from_cn("worker-5").unwrap();
        assert_eq!(id4.role, NodeRole::Worker);
        assert_eq!(id4.node_id, "worker-5");
    }

    #[test]
    fn node_identity_worker_matching() {
        let id = NodeIdentity::new(NodeRole::Worker, "worker-1", None::<String>);
        assert!(id.matches_worker_id(1));
        assert!(id.matches_worker_str("1"));
        assert!(id.matches_worker_str("worker-1"));
        assert!(!id.matches_worker_id(2));

        let control_id = NodeIdentity::new(NodeRole::Control, "control-1", None::<String>);
        assert!(!control_id.matches_worker_id(1));
    }

    #[test]
    fn internal_tls_config_defaults() {
        let cfg = InternalTlsConfig::default();
        assert!(!cfg.is_enabled());
        assert!(cfg.client_auth_required);
        assert!(cfg.reload_enabled);
    }
}

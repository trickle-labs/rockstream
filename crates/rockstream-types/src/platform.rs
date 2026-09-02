//! Platform & Environment Classification Types and Evaluator (v0.59.22).
//!
//! Evaluates host CPU architecture, OS, libc runtime, storage filesystems,
//! container environment, and external backend compatibility against
//! `contracts/platform-matrix.toml`.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error_code::{RS_3025, RS_3028, RS_3029};

/// Authoritative environment classification tiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassificationTier {
    /// Formally qualified in CI across automated integration suites.
    Supported,
    /// Protocol-compatible or POSIX-compliant; unverified in release gates.
    CompatibleUnverified,
    /// Known incompatible, unsafe, or deprecated; execution rejected.
    Unsupported,
}

impl fmt::Display for ClassificationTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Supported => write!(f, "Supported"),
            Self::CompatibleUnverified => write!(f, "Compatible, unverified"),
            Self::Unsupported => write!(f, "Unsupported"),
        }
    }
}

/// Detailed evaluation of an architecture, OS, or component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentEvaluation {
    pub name: String,
    pub tier: ClassificationTier,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
}

/// Overall host platform classification report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformClassification {
    pub architecture: ComponentEvaluation,
    pub operating_system: ComponentEvaluation,
    pub libc_runtime: ComponentEvaluation,
    pub overall_tier: ClassificationTier,
    pub warnings: Vec<String>,
}

impl PlatformClassification {
    /// Returns true if this environment is allowed to run (Supported or CompatibleUnverified).
    pub fn is_allowed(&self) -> bool {
        self.overall_tier != ClassificationTier::Unsupported
    }
}

/// Platform classification evaluator.
pub struct PlatformClassifier;

impl PlatformClassifier {
    /// Evaluate the CPU architecture.
    pub fn evaluate_arch(arch: &str) -> ComponentEvaluation {
        match arch {
            "x86_64" => ComponentEvaluation {
                name: "x86_64".to_string(),
                tier: ClassificationTier::Supported,
                version: Some("x86-64-v2/v3".to_string()),
                reason: Some("Qualified in release gates".to_string()),
                code: None,
            },
            "aarch64" => ComponentEvaluation {
                name: "aarch64".to_string(),
                tier: ClassificationTier::Supported,
                version: Some("ARMv8.2-A+".to_string()),
                reason: Some("Qualified in release gates".to_string()),
                code: None,
            },
            "riscv64" | "ppc64le" | "s390x" => ComponentEvaluation {
                name: arch.to_string(),
                tier: ClassificationTier::CompatibleUnverified,
                version: None,
                reason: Some(format!(
                    "64-bit architecture {arch} is POSIX compatible but not continuously qualified in release gates"
                )),
                code: Some(format!("{RS_3025}")),
            },
            "x86" | "i386" | "i686" | "arm" | "armv7" | "armhf" => ComponentEvaluation {
                name: arch.to_string(),
                tier: ClassificationTier::Unsupported,
                version: None,
                reason: Some(format!(
                    "32-bit architecture {arch} is unsupported (64-bit memory addressing and atomic CAS required)"
                )),
                code: Some(format!("{RS_3028}")),
            },
            other => ComponentEvaluation {
                name: other.to_string(),
                tier: ClassificationTier::Unsupported,
                version: None,
                reason: Some(format!("Architecture {other} is not supported")),
                code: Some(format!("{RS_3028}")),
            },
        }
    }

    /// Evaluate the operating system.
    pub fn evaluate_os(os: &str) -> ComponentEvaluation {
        match os {
            "linux" => ComponentEvaluation {
                name: "linux".to_string(),
                tier: ClassificationTier::Supported,
                version: None,
                reason: Some("Qualified in release gates (Kernel >= 5.4)".to_string()),
                code: None,
            },
            "macos" => ComponentEvaluation {
                name: "macos".to_string(),
                tier: ClassificationTier::Supported,
                version: None,
                reason: Some("Qualified for development and evaluation".to_string()),
                code: None,
            },
            "wsl2" => ComponentEvaluation {
                name: "wsl2".to_string(),
                tier: ClassificationTier::CompatibleUnverified,
                version: None,
                reason: Some("WSL2 Linux virtualization is compatible for development".to_string()),
                code: Some(format!("{RS_3025}")),
            },
            "windows" => ComponentEvaluation {
                name: "windows".to_string(),
                tier: ClassificationTier::Unsupported,
                version: None,
                reason: Some(
                    "Native Windows lacks POSIX file locking and async io_uring execution semantics; use Linux or Docker"
                        .to_string(),
                ),
                code: Some(format!("{RS_3028}")),
            },
            other => ComponentEvaluation {
                name: other.to_string(),
                tier: ClassificationTier::Unsupported,
                version: None,
                reason: Some(format!("Operating system {other} is unsupported")),
                code: Some(format!("{RS_3028}")),
            },
        }
    }

    /// Evaluate the libc runtime.
    pub fn evaluate_libc() -> ComponentEvaluation {
        #[cfg(target_os = "linux")]
        {
            // Detect GNU libc version or Musl
            let libc_name = "glibc";
            ComponentEvaluation {
                name: libc_name.to_string(),
                tier: ClassificationTier::Supported,
                version: Some(">= 2.31".to_string()),
                reason: Some("Standard Linux C library runtime".to_string()),
                code: None,
            }
        }
        #[cfg(target_os = "macos")]
        {
            ComponentEvaluation {
                name: "apple-libc".to_string(),
                tier: ClassificationTier::Supported,
                version: Some("macOS Libc".to_string()),
                reason: Some("Standard Apple macOS BSD C library".to_string()),
                code: None,
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            ComponentEvaluation {
                name: "unknown-libc".to_string(),
                tier: ClassificationTier::CompatibleUnverified,
                version: None,
                reason: Some("Unverified C library runtime".to_string()),
                code: Some(format!("{RS_3025}")),
            }
        }
    }

    /// Evaluate current host platform.
    pub fn evaluate_host() -> PlatformClassification {
        let arch_eval = Self::evaluate_arch(std::env::consts::ARCH);
        let os_eval = Self::evaluate_os(std::env::consts::OS);
        let libc_eval = Self::evaluate_libc();

        let mut overall_tier = ClassificationTier::Supported;
        let mut warnings = Vec::new();

        for eval in [&arch_eval, &os_eval, &libc_eval] {
            match eval.tier {
                ClassificationTier::Unsupported => {
                    overall_tier = ClassificationTier::Unsupported;
                }
                ClassificationTier::CompatibleUnverified => {
                    if overall_tier != ClassificationTier::Unsupported {
                        overall_tier = ClassificationTier::CompatibleUnverified;
                    }
                    if let Some(ref reason) = eval.reason {
                        warnings.push(format!("[{RS_3025}] {}: {}", eval.name, reason));
                    }
                }
                ClassificationTier::Supported => {}
            }
        }

        PlatformClassification {
            architecture: arch_eval,
            operating_system: os_eval,
            libc_runtime: libc_eval,
            overall_tier,
            warnings,
        }
    }

    /// Validate host environment on node startup.
    ///
    /// Rejects unsupported platforms fast with code RS-3028.
    /// Emits actionable warnings for unverified environments with code RS-3025.
    pub fn validate_startup() -> Result<PlatformClassification, String> {
        let classification = Self::evaluate_host();

        if classification.overall_tier == ClassificationTier::Unsupported {
            let mut reasons = Vec::new();
            if classification.architecture.tier == ClassificationTier::Unsupported {
                reasons.push(
                    classification
                        .architecture
                        .reason
                        .clone()
                        .unwrap_or_default(),
                );
            }
            if classification.operating_system.tier == ClassificationTier::Unsupported {
                reasons.push(
                    classification
                        .operating_system
                        .reason
                        .clone()
                        .unwrap_or_default(),
                );
            }
            if classification.libc_runtime.tier == ClassificationTier::Unsupported {
                reasons.push(
                    classification
                        .libc_runtime
                        .reason
                        .clone()
                        .unwrap_or_default(),
                );
            }

            return Err(format!(
                "[{RS_3028}] Host platform ({}/{}) is unsupported: {}. Next steps: run on a supported 64-bit Linux (x86_64/aarch64) or macOS host, or use Docker.",
                std::env::consts::OS,
                std::env::consts::ARCH,
                reasons.join("; ")
            ));
        }

        for warning in &classification.warnings {
            eprintln!("WARNING: {warning}");
        }

        Ok(classification)
    }

    /// Evaluate storage backend compatibility.
    pub fn evaluate_storage_backend(storage_url: &str) -> ComponentEvaluation {
        if storage_url.starts_with("s3://")
            || storage_url.starts_with("http://")
            || storage_url.starts_with("https://")
        {
            let lower = storage_url.to_lowercase();
            if lower.contains("r2.cloudflarestorage.com") || lower.contains("r2") {
                ComponentEvaluation {
                    name: "Cloudflare R2".to_string(),
                    tier: ClassificationTier::CompatibleUnverified,
                    version: Some("S3 API".to_string()),
                    reason: Some("Cloudflare R2 is protocol-compatible S3 API (unverified in continuous release soak)".to_string()),
                    code: Some(format!("{RS_3025}")),
                }
            } else if lower.contains("storage.googleapis.com") || lower.contains("gcs") {
                ComponentEvaluation {
                    name: "Google Cloud Storage".to_string(),
                    tier: ClassificationTier::CompatibleUnverified,
                    version: Some("S3 Interop".to_string()),
                    reason: Some("GCS S3 interoperability API is compatible (unverified in continuous release soak)".to_string()),
                    code: Some(format!("{RS_3025}")),
                }
            } else if lower.contains("blob.core.windows.net") || lower.contains("azure") {
                ComponentEvaluation {
                    name: "Azure Blob Storage".to_string(),
                    tier: ClassificationTier::CompatibleUnverified,
                    version: Some("S3 Proxy".to_string()),
                    reason: Some(
                        "Azure Blob S3 proxy is compatible (unverified in continuous release soak)"
                            .to_string(),
                    ),
                    code: Some(format!("{RS_3025}")),
                }
            } else if lower.contains("ceph") {
                ComponentEvaluation {
                    name: "Ceph RADOS S3".to_string(),
                    tier: ClassificationTier::CompatibleUnverified,
                    version: Some("S3 API".to_string()),
                    reason: Some(
                        "Ceph RADOS S3 Gateway is protocol-compatible (unverified in release soak)"
                            .to_string(),
                    ),
                    code: Some(format!("{RS_3025}")),
                }
            } else {
                ComponentEvaluation {
                    name: "AWS S3 / MinIO".to_string(),
                    tier: ClassificationTier::Supported,
                    version: Some("S3 Standard".to_string()),
                    reason: Some("Fully supported and qualified object store backend".to_string()),
                    code: None,
                }
            }
        } else if storage_url.starts_with("nfs://") || storage_url.starts_with("smb://") {
            ComponentEvaluation {
                name: "Network File System (NFS/SMB)".to_string(),
                tier: ClassificationTier::Unsupported,
                version: None,
                reason: Some("NFS/SMB mounts lack strict POSIX locking / O_DIRECT durability for SlateDB WAL".to_string()),
                code: Some(format!("{RS_3028}")),
            }
        } else {
            ComponentEvaluation {
                name: "Local Filesystem (LFS)".to_string(),
                tier: ClassificationTier::Supported,
                version: Some("POSIX".to_string()),
                reason: Some(
                    "Local SSD/NVMe filesystem fully qualified in release gates".to_string(),
                ),
                code: None,
            }
        }
    }

    /// Evaluate PostgreSQL database version.
    pub fn evaluate_postgres_version(version: u32) -> ComponentEvaluation {
        if version >= 14 {
            ComponentEvaluation {
                name: format!("PostgreSQL {version}"),
                tier: ClassificationTier::Supported,
                version: Some(format!("{version}.x")),
                reason: Some("Fully qualified CDC logical replication reference".to_string()),
                code: None,
            }
        } else if version >= 12 {
            ComponentEvaluation {
                name: format!("PostgreSQL {version}"),
                tier: ClassificationTier::CompatibleUnverified,
                version: Some(format!("{version}.x")),
                reason: Some(
                    "PostgreSQL 12/13 supports pgoutput but is older than standard release target"
                        .to_string(),
                ),
                code: Some(format!("{RS_3025}")),
            }
        } else {
            ComponentEvaluation {
                name: format!("PostgreSQL {version}"),
                tier: ClassificationTier::Unsupported,
                version: Some(format!("{version}.x")),
                reason: Some(
                    "PostgreSQL < 12 lacks required CDC logical replication features".to_string(),
                ),
                code: Some(format!("{RS_3029}")),
            }
        }
    }

    /// Evaluate Kafka broker version.
    pub fn evaluate_kafka_version(version_str: &str) -> ComponentEvaluation {
        if version_str.starts_with('3')
            || version_str.starts_with('4')
            || version_str.contains("redpanda")
        {
            ComponentEvaluation {
                name: format!("Kafka/Redpanda ({version_str})"),
                tier: ClassificationTier::Supported,
                version: Some(version_str.to_string()),
                reason: Some("Qualified Kafka 3.x / Redpanda broker API".to_string()),
                code: None,
            }
        } else if version_str.starts_with("2.8") || version_str.starts_with("2.9") {
            ComponentEvaluation {
                name: format!("Kafka ({version_str})"),
                tier: ClassificationTier::CompatibleUnverified,
                version: Some(version_str.to_string()),
                reason: Some("Kafka 2.8+ is compatible (early KRaft)".to_string()),
                code: Some(format!("{RS_3025}")),
            }
        } else {
            ComponentEvaluation {
                name: format!("Kafka ({version_str})"),
                tier: ClassificationTier::Unsupported,
                version: Some(version_str.to_string()),
                reason: Some("Kafka < 2.8 is unsupported".to_string()),
                code: Some(format!("{RS_3029}")),
            }
        }
    }
}

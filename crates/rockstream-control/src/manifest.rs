//! Backup Manifest specification and cryptographic verification (v0.65 Slice 3).
//!
//! Implements canonical point-in-time backup manifest serialization, SHA-256
//! checksum calculation and verification, and compatibility checks.

use rockstream_types::error_code::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const CURRENT_BACKUP_MANIFEST_VERSION: u32 = 1;
pub const CURRENT_STORAGE_FORMAT: u32 = 3;
pub const BACKUP_MANIFEST_FILENAME: &str = "manifest.json";

/// Named operational bounds for backup and restore operations (Matrix G).
pub const MAX_BACKUP_SCAN_WINDOW_OBJECTS: usize = 1024;
pub const MAX_BACKUP_COPY_CONCURRENCY: usize = 4;
pub const MAX_BACKUP_PENDING_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_BACKUP_RETRY_COUNT: usize = 3;

/// Entry for a single payload file in a backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupFileEntry {
    pub path: String,
    pub byte_len: u64,
    pub sha256: String,
}

/// Unified logical point-in-time capture point (v0.65 Slice 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupPoint {
    pub catalog_revision: u64,
    pub checkpoint_id: u64,
    pub frontier: u64,
}

impl BackupPoint {
    pub fn new(catalog_revision: u64, checkpoint_id: u64, frontier: u64) -> Self {
        Self {
            catalog_revision,
            checkpoint_id,
            frontier,
        }
    }

    pub fn matches_manifest(&self, manifest: &BackupManifest) -> bool {
        self.catalog_revision == manifest.catalog_revision
            && self.checkpoint_id == manifest.checkpoint_id
            && self.frontier == manifest.frontier
    }
}

/// Canonical point-in-time backup manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub format_version: u32,
    pub catalog_revision: u64,
    pub checkpoint_id: u64,
    pub frontier: u64,
    pub storage_format: u32,
    pub files: Vec<BackupFileEntry>,
    #[serde(default)]
    pub checksum: String,
}

impl BackupManifest {
    /// Create a new backup manifest, automatically computing the canonical SHA-256 checksum.
    pub fn new(
        catalog_revision: u64,
        checkpoint_id: u64,
        frontier: u64,
        storage_format: u32,
        mut files: Vec<BackupFileEntry>,
    ) -> Self {
        files.sort_by(|a, b| a.path.cmp(&b.path));
        let mut manifest = Self {
            format_version: CURRENT_BACKUP_MANIFEST_VERSION,
            catalog_revision,
            checkpoint_id,
            frontier,
            storage_format,
            files,
            checksum: String::new(),
        };
        manifest.checksum = manifest.compute_checksum();
        manifest
    }

    /// Compute canonical SHA-256 checksum of manifest contents.
    pub fn compute_checksum(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.format_version.to_be_bytes());
        hasher.update(self.catalog_revision.to_be_bytes());
        hasher.update(self.checkpoint_id.to_be_bytes());
        hasher.update(self.frontier.to_be_bytes());
        hasher.update(self.storage_format.to_be_bytes());
        for f in &self.files {
            hasher.update(f.path.as_bytes());
            hasher.update(b":");
            hasher.update(f.byte_len.to_be_bytes());
            hasher.update(b":");
            hasher.update(f.sha256.as_bytes());
            hasher.update(b";");
        }
        format!("{:x}", hasher.finalize())
    }

    /// Validate manifest fields, version compatibility, and digest.
    pub fn validate(&self) -> Result<(), (ErrorCode, String)> {
        if self.format_version != CURRENT_BACKUP_MANIFEST_VERSION {
            return Err((
                RS_3617,
                format!(
                    "RS-3617: incompatible backup manifest format_version {}; supported version is {}",
                    self.format_version, CURRENT_BACKUP_MANIFEST_VERSION
                ),
            ));
        }

        if self.storage_format != CURRENT_STORAGE_FORMAT {
            return Err((
                RS_3617,
                format!(
                    "RS-3617: unsupported storage format {}; supported format is {}",
                    self.storage_format, CURRENT_STORAGE_FORMAT
                ),
            ));
        }

        if self.checksum.is_empty() {
            return Err((
                RS_3615,
                "RS-3615: manifest is unfinalized or missing checksum".to_string(),
            ));
        }

        let expected_checksum = self.compute_checksum();
        if self.checksum != expected_checksum {
            return Err((
                RS_3616,
                format!(
                    "RS-3616: manifest checksum mismatch; recorded={}, computed={}",
                    self.checksum, expected_checksum
                ),
            ));
        }

        if self.files.is_empty() {
            return Err((
                RS_3615,
                "RS-3615: backup manifest contains no payload files".to_string(),
            ));
        }

        Ok(())
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn from_json(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

/// Helper to compute SHA-256 hex digest for arbitrary byte slices.
pub fn compute_file_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// Validate that all payload files listed in the manifest exist in `backup_dir`
/// and match their declared byte lengths and SHA-256 digests.
pub fn verify_backup_payload_files(
    backup_dir: &std::path::Path,
    manifest: &BackupManifest,
) -> Result<usize, (ErrorCode, String)> {
    let mut verified = 0;
    for file in &manifest.files {
        let file_path = backup_dir.join(&file.path);
        if !file_path.exists() {
            return Err((
                RS_3615,
                format!("RS-3615: payload file '{}' missing from backup", file.path),
            ));
        }
        let bytes = std::fs::read(&file_path).map_err(|e| {
            (
                RS_3615,
                format!("RS-3615: failed to read payload file '{}': {e}", file.path),
            )
        })?;
        if bytes.len() as u64 != file.byte_len {
            return Err((
                RS_3616,
                format!(
                    "RS-3616: payload file '{}' length mismatch: expected {}, got {}",
                    file.path,
                    file.byte_len,
                    bytes.len()
                ),
            ));
        }
        let digest = compute_file_sha256(&bytes);
        if digest != file.sha256 {
            return Err((
                RS_3616,
                format!(
                    "RS-3616: payload file '{}' checksum mismatch: expected {}, got {}",
                    file.path, file.sha256, digest
                ),
            ));
        }
        verified += 1;
    }
    Ok(verified)
}

/// Validate that the backup manifest's catalog revision is consistent with
/// available catalog snapshot revisions.
pub fn validate_catalog_reference(
    manifest: &BackupManifest,
    available_catalog_revisions: &[u64],
) -> Result<(), (ErrorCode, String)> {
    if !available_catalog_revisions.contains(&manifest.catalog_revision) {
        return Err((
            RS_3618,
            format!(
                "RS-3618: broken catalog reference: backup catalog_revision {} not found in catalog revisions: {:?}",
                manifest.catalog_revision, available_catalog_revisions
            ),
        ));
    }
    Ok(())
}

/// Bounded copy and memory governor for backup operations (Matrix G).
#[derive(Clone, Debug)]
pub struct BackupConcurrencyGovernor {
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
    pending_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    max_pending_bytes: u64,
}

impl Default for BackupConcurrencyGovernor {
    fn default() -> Self {
        Self::new()
    }
}

impl BackupConcurrencyGovernor {
    pub fn new() -> Self {
        Self {
            semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(
                MAX_BACKUP_COPY_CONCURRENCY,
            )),
            pending_bytes: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            max_pending_bytes: MAX_BACKUP_PENDING_BYTES,
        }
    }

    pub fn with_limits(max_concurrency: usize, max_pending_bytes: u64) -> Self {
        Self {
            semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(max_concurrency)),
            pending_bytes: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            max_pending_bytes,
        }
    }

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn acquire_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore valid")
    }

    pub fn try_reserve_bytes(&self, bytes: u64) -> Result<(), (ErrorCode, String)> {
        let mut current = self
            .pending_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        loop {
            if current + bytes > self.max_pending_bytes {
                return Err((
                    RS_2002,
                    format!(
                        "RS-2002: backup pending bytes limit exceeded: {} + {} > {}",
                        current, bytes, self.max_pending_bytes
                    ),
                ));
            }
            match self.pending_bytes.compare_exchange_weak(
                current,
                current + bytes,
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    pub fn release_bytes(&self, bytes: u64) {
        self.pending_bytes
            .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
    }
}

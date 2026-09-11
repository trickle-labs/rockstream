//! Versioned Record Envelope for RockStream Catalog (Slice 2).
//!
//! Every catalog record is serialized in a canonical versioned envelope containing:
//! - `catalog_format_version: u32` (current format = 1).
//! - `record_version: u32` (per-entity schema version = 1).
//! - `object_id: u128` (stable 128-bit entity ID).
//! - `catalog_revision: u64` (monotonic sequence revision).
//! - `checksum: u32` (CRC32 checksum of serialized payload).
//! - `payload: Vec<u8>`.

use serde::{Deserialize, Serialize};

use super::CatalogError;

/// The current authoritative catalog format version.
pub const CURRENT_CATALOG_FORMAT_VERSION: u32 = 1;

/// A versioned envelope wrapping a serialized catalog entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogEnvelope {
    /// Format version of the catalog envelope itself.
    pub catalog_format_version: u32,
    /// Schema layout version of the inner entity.
    pub record_version: u32,
    /// Stable entity ID across renames, restarts, and compactions.
    pub object_id: u128,
    /// Monotonic catalog revision when this record was written.
    pub catalog_revision: u64,
    /// CRC32 checksum of the payload bytes.
    pub checksum: u32,
    /// Serialized entity payload.
    pub payload: Vec<u8>,
}

impl CatalogEnvelope {
    /// Create a new envelope for a given entity payload.
    pub fn new(
        record_version: u32,
        object_id: u128,
        catalog_revision: u64,
        payload: Vec<u8>,
    ) -> Self {
        let checksum = compute_checksum(&payload);
        Self {
            catalog_format_version: CURRENT_CATALOG_FORMAT_VERSION,
            record_version,
            object_id,
            catalog_revision,
            checksum,
            payload,
        }
    }

    /// Serialize the envelope to bytes.
    pub fn encode(&self) -> Result<Vec<u8>, CatalogError> {
        serde_json::to_vec(self).map_err(|e| {
            CatalogError::Internal(format!("[RS-0001] failed to encode catalog envelope: {e}"))
        })
    }

    /// Deserialize and validate an envelope from bytes.
    ///
    /// Validates:
    /// 1. `catalog_format_version <= CURRENT_CATALOG_FORMAT_VERSION`. Future versions fail with `RS-1002`.
    /// 2. `object_id != 0`. Zero IDs fail with `RS-1003`.
    /// 3. `checksum` matches CRC32 of `payload`. Corrupted payloads fail with `RS-1003`.
    pub fn decode(bytes: &[u8]) -> Result<Self, CatalogError> {
        let env: Self = serde_json::from_slice(bytes).map_err(|e| {
            CatalogError::DecodeError(format!(
                "[RS-1003] malformed catalog envelope: {e}; next_steps: inspect storage for corruption"
            ))
        })?;

        // 1. Format version validation
        if env.catalog_format_version > CURRENT_CATALOG_FORMAT_VERSION {
            return Err(CatalogError::IncompatibleVersion(format!(
                "[RS-1002] unsupported catalog format version {}; supported format is {}; next_steps: upgrade RockStream binary",
                env.catalog_format_version, CURRENT_CATALOG_FORMAT_VERSION
            )));
        }

        // 2. Object ID validation
        if env.object_id == 0 {
            return Err(CatalogError::InvalidObjectId(
                "[RS-1003] invalid object_id 0 in catalog envelope; next_steps: ensure all catalog objects have non-zero IDs".to_string(),
            ));
        }

        // 3. CRC32 checksum verification
        let expected_checksum = compute_checksum(&env.payload);
        if env.checksum != expected_checksum {
            return Err(CatalogError::ChecksumCorruption(format!(
                "[RS-1003] checksum mismatch in catalog envelope: stored {:#x}, computed {:#x}; next_steps: check disk integrity or restore from backup",
                env.checksum, expected_checksum
            )));
        }

        Ok(env)
    }

    /// Upgrade record layout version.
    ///
    /// Compatible forward upgrades are allowed; backward downgrades are rejected with `RS-1002`.
    pub fn upgrade_record(&self, target_version: u32) -> Result<Self, CatalogError> {
        if target_version < self.record_version {
            return Err(CatalogError::IncompatibleVersion(format!(
                "[RS-1002] cannot downgrade record version from {} to {}; next_steps: schema downgrades are not supported",
                self.record_version, target_version
            )));
        }
        let mut upgraded = self.clone();
        upgraded.record_version = target_version;
        Ok(upgraded)
    }
}

/// Compute CRC32 checksum of bytes using crc32fast.
pub fn compute_checksum(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

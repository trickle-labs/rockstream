//! Shared storage and wire compatibility contracts.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A gRPC wire protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProtocolVersion(pub u32);

impl ProtocolVersion {
    pub const V1: Self = Self(1);
    pub const V2: Self = Self(2);
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// Inclusive wire protocol versions accepted by a binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportedVersionRange {
    pub min: ProtocolVersion,
    pub max: ProtocolVersion,
}

impl SupportedVersionRange {
    pub const fn new(min: ProtocolVersion, max: ProtocolVersion) -> Self {
        Self { min, max }
    }

    pub const fn v1_only() -> Self {
        Self::new(ProtocolVersion::V1, ProtocolVersion::V1)
    }

    pub const fn v2_with_v1_compat() -> Self {
        Self::new(ProtocolVersion::V1, ProtocolVersion::V2)
    }

    pub const fn v2_only() -> Self {
        Self::new(ProtocolVersion::V2, ProtocolVersion::V2)
    }

    pub const fn v1_through_v2() -> Self {
        Self::v2_with_v1_compat()
    }

    pub fn contains(self, version: ProtocolVersion) -> bool {
        version >= self.min && version <= self.max
    }

    pub fn overlaps(self, other: Self) -> bool {
        self.min <= other.max && other.min <= self.max
    }

    pub fn highest_common(self, other: Self) -> Option<ProtocolVersion> {
        if !self.overlaps(other) {
            None
        } else if self.max <= other.max {
            Some(self.max)
        } else {
            Some(other.max)
        }
    }
}

impl Default for SupportedVersionRange {
    fn default() -> Self {
        Self::v1_only()
    }
}

/// A persisted shard storage format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StorageFormatVersion(pub u8);

impl StorageFormatVersion {
    pub const V1: Self = Self(1);
    pub const V2: Self = Self(2);
    pub const V3: Self = Self(3);
}

impl From<u8> for StorageFormatVersion {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl fmt::Display for StorageFormatVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Inclusive storage formats accepted by a binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SupportedStorageFormatRange {
    pub min: StorageFormatVersion,
    pub max: StorageFormatVersion,
}

impl SupportedStorageFormatRange {
    pub const fn new(min: StorageFormatVersion, max: StorageFormatVersion) -> Self {
        Self { min, max }
    }

    pub const fn v1_only() -> Self {
        Self::new(StorageFormatVersion::V1, StorageFormatVersion::V1)
    }

    pub const fn v1_through_v2() -> Self {
        Self::new(StorageFormatVersion::V1, StorageFormatVersion::V2)
    }

    pub const fn v2_only() -> Self {
        Self::new(StorageFormatVersion::V2, StorageFormatVersion::V2)
    }

    pub const fn v2_through_v3() -> Self {
        Self::new(StorageFormatVersion::V2, StorageFormatVersion::V3)
    }

    pub const fn v3_only() -> Self {
        Self::new(StorageFormatVersion::V3, StorageFormatVersion::V3)
    }

    pub const fn v1_through_v3() -> Self {
        Self::new(StorageFormatVersion::V1, StorageFormatVersion::V3)
    }

    pub fn contains(self, version: StorageFormatVersion) -> bool {
        version >= self.min && version <= self.max
    }

    pub fn overlaps(self, other: Self) -> bool {
        self.min <= other.max && other.min <= self.max
    }
}

impl Default for SupportedStorageFormatRange {
    fn default() -> Self {
        Self::v1_only()
    }
}

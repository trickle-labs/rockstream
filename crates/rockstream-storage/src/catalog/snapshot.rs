//! Catalog Snapshot Definition and Recovery (Slice 4).
//!
//! Stores full catalog state at a specific revision `R`:
//! `catalog/snapshots/<snapshot_revision:020>.snap`
//!
//! Recovery formula:
//! `latest valid snapshot + subsequent valid log entries = current catalog state`.

use serde::{Deserialize, Serialize};

use super::envelope::compute_checksum;
use super::{
    CatalogDatabase, CatalogError, CatalogIndexEntry, CatalogInlineView, CatalogNamespace,
    CatalogRoleEntry, CatalogSinkEntry, CatalogSourceEntry, CatalogTable, CatalogView,
    CompiledPlanRecord, ViewDependency,
};
use rockstream_types::workload::WorkloadDef;

/// Full state snapshot at revision `R`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub revision: u64,
    pub high_water_mark: u64,
    pub databases: Vec<CatalogDatabase>,
    pub namespaces: Vec<CatalogNamespace>,
    pub tables: Vec<CatalogTable>,
    pub views: Vec<CatalogView>,
    pub inline_views: Vec<CatalogInlineView>,
    pub view_dependencies: Vec<ViewDependency>,
    pub indexes: Vec<CatalogIndexEntry>,
    pub workloads: Vec<WorkloadDef>,
    pub sources: Vec<CatalogSourceEntry>,
    pub sinks: Vec<CatalogSinkEntry>,
    pub roles: Vec<CatalogRoleEntry>,
    pub compiled_plans: Vec<CompiledPlanRecord>,
    pub checksum: u32,
}

impl CatalogSnapshot {
    /// Create a new snapshot computing its checksum.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        revision: u64,
        high_water_mark: u64,
        databases: Vec<CatalogDatabase>,
        namespaces: Vec<CatalogNamespace>,
        tables: Vec<CatalogTable>,
        views: Vec<CatalogView>,
        inline_views: Vec<CatalogInlineView>,
        view_dependencies: Vec<ViewDependency>,
        indexes: Vec<CatalogIndexEntry>,
        workloads: Vec<WorkloadDef>,
        sources: Vec<CatalogSourceEntry>,
        sinks: Vec<CatalogSinkEntry>,
        roles: Vec<CatalogRoleEntry>,
        compiled_plans: Vec<CompiledPlanRecord>,
    ) -> Result<Self, CatalogError> {
        let mut snap = Self {
            revision,
            high_water_mark,
            databases,
            namespaces,
            tables,
            views,
            inline_views,
            view_dependencies,
            indexes,
            workloads,
            sources,
            sinks,
            roles,
            compiled_plans,
            checksum: 0,
        };

        let serialized = serde_json::to_vec(&snap).map_err(|e| {
            CatalogError::Internal(format!("[RS-0001] failed to serialize snapshot: {e}"))
        })?;
        snap.checksum = compute_checksum(&serialized);

        Ok(snap)
    }

    /// Serialize snapshot to bytes.
    pub fn encode(&self) -> Result<Vec<u8>, CatalogError> {
        serde_json::to_vec(self).map_err(|e| {
            CatalogError::Internal(format!(
                "[RS-0001] failed to serialize catalog snapshot: {e}"
            ))
        })
    }

    /// Deserialize and validate snapshot from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, CatalogError> {
        let mut snap: Self = serde_json::from_slice(bytes).map_err(|e| {
            CatalogError::DecodeError(format!(
                "[RS-1003] malformed catalog snapshot: {e}; next_steps: inspect snapshot file"
            ))
        })?;

        let stored_checksum = snap.checksum;
        snap.checksum = 0;
        let serialized = serde_json::to_vec(&snap).map_err(|e| {
            CatalogError::Internal(format!(
                "[RS-0001] failed to serialize snapshot for verification: {e}"
            ))
        })?;
        let expected = compute_checksum(&serialized);
        if stored_checksum != expected {
            return Err(CatalogError::ChecksumCorruption(format!(
                "[RS-1003] snapshot checksum mismatch: stored {:#x}, computed {:#x}; next_steps: check disk integrity",
                stored_checksum, expected
            )));
        }
        snap.checksum = stored_checksum;

        Ok(snap)
    }
}

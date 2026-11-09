//! Atomic Logical Transactions (CatalogTxn) for RockStream Catalog (Slice 3).
//!
//! DDL changes execute as atomic logical catalog transactions:
//! - `revision: u64`
//! - `operation_id: u64`
//! - `mutations: Vec<CatalogMutation>`
//! - `checksum: u32`
//!
//! Max mutations per transaction is strictly bounded to 1,000.

use serde::{Deserialize, Serialize};

use super::envelope::compute_checksum;
use super::{
    CatalogDatabase, CatalogError, CatalogIndexEntry, CatalogInlineView, CatalogNamespace,
    CatalogRoleEntry, CatalogSinkEntry, CatalogSourceEntry, CatalogTable, CatalogView,
    CatalogWorkloadEntry, CompiledPlanRecord, ViewDependency,
};
use rockstream_types::authorization::{SecurityAuditRecord, SecurityPrincipal};
use rockstream_types::ids::{
    CompiledPlanId, IndexId, PrincipalId, SinkId, SourceId, TableId, ViewId, WorkloadId,
};
use rockstream_types::workload::WorkloadDef;

/// Upper bound on mutations per transaction.
pub const MAX_MUTATIONS_PER_TRANSACTION: usize = 1000;

/// Atomic mutation variants supported by the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogMutation {
    PutDatabase(CatalogDatabase),
    PutNamespace(CatalogNamespace),
    PutTable(CatalogTable),
    DeleteTable(TableId),
    PutView(CatalogView),
    DeleteView(ViewId),
    PutInlineView(CatalogInlineView),
    DeleteInlineView(ViewId),
    PutViewDependency(ViewDependency),
    DeleteViewDependency {
        parent_id: u64,
        child_id: u64,
    },
    PutIndex(CatalogIndexEntry),
    DeleteIndex(IndexId),
    PutWorkload(WorkloadDef),
    /// Explicit identity recorded on commit; used for replay without allocation.
    PutWorkloadWithId(CatalogWorkloadEntry),
    DeleteWorkload(WorkloadId),
    PutSource(CatalogSourceEntry),
    DeleteSource(SourceId),
    PutSink(CatalogSinkEntry),
    DeleteSink(SinkId),
    PutRole(CatalogRoleEntry),
    DeleteRole(PrincipalId),
    PutSecurityPrincipal {
        principal: SecurityPrincipal,
        expected_revision: Option<u64>,
        grantor_revision: Option<(PrincipalId, u64)>,
        audit: SecurityAuditRecord,
    },
    PutSecurityPrincipalForOperation {
        principal: SecurityPrincipal,
        expected_revision: Option<u64>,
        grantor_revision: Option<(PrincipalId, u64)>,
        operation_id: String,
        audit: SecurityAuditRecord,
    },
    DeleteSecurityPrincipal {
        principal_id: PrincipalId,
        expected_revision: u64,
        grantor_revision: Option<(PrincipalId, u64)>,
        audit: SecurityAuditRecord,
    },
    AppendSecurityAudit(SecurityAuditRecord),
    PutCompiledPlan(CompiledPlanRecord),
    DeleteCompiledPlan(CompiledPlanId),
}

/// An atomic logical catalog transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogTxn {
    /// Version of the serialized transaction contract. Legacy transactions omit this field.
    #[serde(
        default = "legacy_txn_format_version",
        skip_serializing_if = "is_legacy_txn_format"
    )]
    pub format_version: u16,
    /// Monotonic revision sequence number.
    pub revision: u64,
    /// Operation identifier for deduplication and retry safety.
    pub operation_id: u64,
    /// Ordered list of mutations committed atomically.
    pub mutations: Vec<CatalogMutation>,
    /// CRC32 checksum over the serialized mutations.
    pub checksum: u32,
}

impl CatalogTxn {
    /// Create a new transaction, validating bounds and computing its checksum.
    pub fn new(
        revision: u64,
        operation_id: u64,
        mutations: Vec<CatalogMutation>,
    ) -> Result<Self, CatalogError> {
        if mutations.len() > MAX_MUTATIONS_PER_TRANSACTION {
            return Err(CatalogError::TransactionTooLarge(format!(
                "[RS-1002] transaction contains {} mutations, exceeding maximum allowed limit of {}; next_steps: split mutations across multiple transactions",
                mutations.len(),
                MAX_MUTATIONS_PER_TRANSACTION
            )));
        }

        let format_version = if mutations.iter().any(|mutation| {
            matches!(
                mutation,
                CatalogMutation::PutSecurityPrincipalForOperation { .. }
            )
        }) {
            3
        } else if mutations.iter().any(is_security_mutation) {
            2
        } else {
            1
        };
        let serialized = canonical_serialize_mutations(&mutations)?;
        let checksum = compute_checksum(&serialized);

        Ok(Self {
            format_version,
            revision,
            operation_id,
            mutations,
            checksum,
        })
    }

    /// Serialize transaction to bytes.
    pub fn encode(&self) -> Result<Vec<u8>, CatalogError> {
        serde_json::to_vec(self).map_err(|e| {
            CatalogError::Internal(format!("[RS-0001] failed to serialize catalog txn: {e}"))
        })
    }

    /// Deserialize and validate transaction from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, CatalogError> {
        let txn: Self = serde_json::from_slice(bytes).map_err(|e| {
            CatalogError::DecodeError(format!(
                "[RS-1003] malformed catalog transaction: {e}; next_steps: inspect catalog log files"
            ))
        })?;

        if txn.format_version > 3 {
            return Err(CatalogError::IncompatibleVersion(format!(
                "[RS-1002] unsupported catalog transaction format version {}; supported format is 3; next_steps: upgrade RockStream binary",
                txn.format_version
            )));
        }
        if txn.format_version < 2 && txn.mutations.iter().any(is_security_mutation) {
            return Err(CatalogError::IncompatibleVersion(
                "[RS-1002] security catalog mutations require transaction format version 2; next_steps: upgrade RockStream binary".to_string(),
            ));
        }
        if txn.format_version < 3
            && txn.mutations.iter().any(|mutation| {
                matches!(
                    mutation,
                    CatalogMutation::PutSecurityPrincipalForOperation { .. }
                )
            })
        {
            return Err(CatalogError::IncompatibleVersion(
                "[RS-1002] operation-correlated security mutations require transaction format version 3; next_steps: upgrade RockStream binary".to_string(),
            ));
        }

        if txn.mutations.len() > MAX_MUTATIONS_PER_TRANSACTION {
            return Err(CatalogError::TransactionTooLarge(format!(
                "[RS-1002] transaction contains {} mutations, exceeding limit of {}",
                txn.mutations.len(),
                MAX_MUTATIONS_PER_TRANSACTION
            )));
        }

        let serialized = canonical_serialize_mutations(&txn.mutations)?;
        let expected_checksum = compute_checksum(&serialized);
        if txn.checksum != expected_checksum {
            return Err(CatalogError::ChecksumCorruption(format!(
                "[RS-1003] transaction checksum mismatch: stored {:#x}, computed {:#x}; next_steps: check storage integrity",
                txn.checksum, expected_checksum
            )));
        }

        Ok(txn)
    }
}

fn legacy_txn_format_version() -> u16 {
    1
}

fn is_legacy_txn_format(version: &u16) -> bool {
    *version == 1
}

fn is_security_mutation(mutation: &CatalogMutation) -> bool {
    matches!(
        mutation,
        CatalogMutation::PutSecurityPrincipal { .. }
            | CatalogMutation::PutSecurityPrincipalForOperation { .. }
            | CatalogMutation::DeleteSecurityPrincipal { .. }
            | CatalogMutation::AppendSecurityAudit(_)
    )
}

fn canonical_serialize_mutations(mutations: &[CatalogMutation]) -> Result<Vec<u8>, CatalogError> {
    let value = serde_json::to_value(mutations).map_err(|e| {
        CatalogError::Internal(format!("[RS-0001] failed to serialize mutations: {e}"))
    })?;
    serde_json::to_vec(&value).map_err(|e| {
        CatalogError::Internal(format!(
            "[RS-0001] failed to serialize canonical mutations: {e}"
        ))
    })
}

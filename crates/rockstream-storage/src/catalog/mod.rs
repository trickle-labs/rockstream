//! Durable Catalog Store and Metadata Architecture (v0.63).
//!
//! Provides a single authoritative, durable, versioned, transaction-logged catalog store
//! backed by `ObjectStore` (`file://` or `s3://`).
//!
//! Eliminates ephemeral in-memory catalog storage in favor of:
//! - Versioned record envelope with CRC32 checksums (`envelope.rs`)
//! - Monotonic durable ID allocation with collision avoidance (`identity.rs`)
//! - Atomic logical catalog transactions with replay deduplication (`txn.rs`)
//! - Snapshot-plus-log metadata retention, compaction, and bounded scans (`log.rs`, `snapshot.rs`)
//! - `CatalogStore` trait and durable implementation (`store.rs`)

pub mod envelope;
pub mod identity;
pub mod log;
pub mod snapshot;
pub mod store;
pub mod txn;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

use rockstream_types::ids::{
    CompiledPlanId, DatabaseId, IndexId, NamespaceId, PrincipalId, SinkId, SourceId, TableId,
    ViewId,
};

pub use envelope::{CatalogEnvelope, CURRENT_CATALOG_FORMAT_VERSION};
pub use identity::{stable_name_id, IdAllocator};
pub use log::{CatalogLogManager, CatalogScanPage, MAX_REPLAY_BUFFER_BYTES, MAX_SCAN_PAGE_SIZE};
pub use snapshot::CatalogSnapshot;
pub use store::{CatalogStore, DurableCatalogStore};
pub use txn::{CatalogMutation, CatalogTxn, MAX_MUTATIONS_PER_TRANSACTION};

/// Catalog errors with standard RS-XXXX error codes.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CatalogError {
    #[error("{0}")]
    Internal(String),

    #[error("{0}")]
    Storage(String),

    #[error("{0}")]
    DecodeError(String),

    #[error("{0}")]
    ChecksumCorruption(String),

    #[error("{0}")]
    IncompatibleVersion(String),

    #[error("{0}")]
    InvalidObjectId(String),

    #[error("{0}")]
    TransactionTooLarge(String),

    #[error("{0}")]
    DependencyCycle(String),

    #[error("{0}")]
    ReferencedObjectExists(String),

    #[error("{0}")]
    ObjectNotFound(String),

    #[error("{0}")]
    UnsupportedDdl(String),

    #[error("{0}")]
    ReplayBufferExceeded(String),
}

/// A database entry in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDatabase {
    pub id: DatabaseId,
    pub name: String,
    pub default_namespace: String,
    pub created_at: u64,
}

/// A namespace (schema) entry in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogNamespace {
    pub id: NamespaceId,
    pub name: String,
    pub database_id: DatabaseId,
    pub created_at: u64,
}

/// A column in a table or view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub ordinal: u32,
}

/// A base table entry in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogTable {
    pub id: TableId,
    pub name: String,
    pub namespace_id: NamespaceId,
    pub columns: Vec<CatalogColumn>,
    pub pk_cols: Vec<String>,
}

/// A materialized view entry in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogView {
    pub id: ViewId,
    pub name: String,
    pub namespace_id: NamespaceId,
    pub sql: String,
    pub compiled_plan_id: Option<CompiledPlanId>,
    pub op_id: Option<u64>,
    pub columns: Vec<CatalogColumn>,
}

/// An inline view entry in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogInlineView {
    pub id: ViewId,
    pub name: String,
    pub namespace_id: NamespaceId,
    pub sql: String,
    pub ast_json: String,
    pub referenced_objects: Vec<String>,
}

/// Kind of object being depended upon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DependencyKind {
    Table,
    View,
}

/// A view dependency edge in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewDependency {
    pub parent_id: u64,
    pub child_id: u64,
    pub dependency_kind: DependencyKind,
}

/// Secondary index build state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogIndexState {
    Building,
    Ready,
}

/// A secondary index entry in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogIndexEntry {
    pub id: IndexId,
    pub name: String,
    pub table_id: TableId,
    pub index_cols: Vec<String>,
    pub pk_cols: Vec<String>,
    pub state: CatalogIndexState,
    pub op_id: Option<u64>,
}

/// A source connector entry in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSourceEntry {
    pub id: SourceId,
    pub name: String,
    pub connector_type: String,
    pub table_name: Option<String>,
    pub options: HashMap<String, String>,
}

/// A sink connector entry in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSinkEntry {
    pub id: SinkId,
    pub name: String,
    pub sink_type: String,
    pub target: String,
    pub options: HashMap<String, String>,
    pub status: String,
}

/// A role / principal permission entry in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogRoleEntry {
    pub id: PrincipalId,
    pub role_name: String,
    pub permissions: Vec<String>,
    pub member_of: Vec<String>,
}

/// Seven compiled plan identity fields for safe gateway view reconstruction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledPlanRecord {
    pub id: CompiledPlanId,
    pub sql: String,
    pub ast_hash: [u8; 32],
    pub logical_plan_hash: [u8; 32],
    pub compiler_version: String,
    pub state_layout_version: u32,
    pub output_schema: Vec<u8>,
    pub dependency_ids: Vec<u128>,
}

/// Validate DDL statements and reject unsupported/inapplicable operations fast with RS-1001/RS-2001.
pub fn validate_unsupported_ddl(sql: &str) -> Result<(), CatalogError> {
    let upper = sql.to_uppercase();
    let normalized = upper.split_whitespace().collect::<Vec<_>>().join(" ");

    let unsupported_patterns = [
        "CREATE TRIGGER",
        "CREATE OR REPLACE TRIGGER",
        "CREATE PROCEDURE",
        "CREATE OR REPLACE PROCEDURE",
        "CREATE FOREIGN TABLE",
        "CREATE DOMAIN",
        "CREATE SEQUENCE",
    ];

    for pattern in unsupported_patterns {
        if normalized.contains(pattern) {
            return Err(CatalogError::UnsupportedDdl(format!(
                "[RS-1001] unsupported DDL statement '{pattern}'; next_steps: triggers, stored procedures, sequences, domains, and foreign tables are not supported by the RockStream catalog"
            )));
        }
    }

    Ok(())
}

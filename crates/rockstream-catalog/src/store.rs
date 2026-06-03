//! Catalog entry types and the in-memory `CatalogStore`.
//!
//! The `CatalogStore` is the primary v0.12 persistence facade. It stores
//! source and view entries keyed by `(namespace_id, name)` and supports:
//!
//! - `register_source` / `register_view` — insert a new entry
//! - `get` — fetch an entry
//! - `update_schema` — evolve a schema (compatibility-checked)
//! - `store_plan` / `load_plan` — persist and recall a view's plan bytes
//!
//! In v0.12 the backing store is in-memory (HashMap). The interface is
//! designed to be backed by `ShardDb` in a later version without changing
//! the public API.
//!
//! ## v0.16 additions
//!
//! - `CatalogEntry` gains `state`, `workload_name`, `depends_on`, and
//!   `backfill_started_epoch` fields.
//! - `CatalogStore` gains workload-management methods, `pause_view`,
//!   `resume_view`, `show_view_status`, and `show_backfill_status`.

use crate::compat::check_schema_change;
use crate::error::CatalogError;
use crate::schema::SchemaVersion;
use crate::workload_store::WorkloadStore;
use rockstream_plan::PlanNode;
use rockstream_types::ids::NamespaceId;
use rockstream_types::laws::registry::LawRegistry;
use rockstream_types::view_lifecycle::{BackfillStatus, ViewState, ViewStatus};
use rockstream_types::workload::WorkloadDef;
use std::collections::HashMap;

/// The kind of catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    /// An external data source.
    Source,
    /// A materialized view.
    View,
}

/// A single entry in the catalog.
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    /// Entry name (unique within the namespace).
    pub name: String,
    /// Namespace this entry belongs to.
    pub namespace_id: NamespaceId,
    /// Whether this is a source or a view.
    pub kind: EntryKind,
    /// Current schema version.
    pub schema: SchemaVersion,
    /// Raw persisted plan bytes (present only for views).
    pub plan_bytes: Option<Vec<u8>>,
    /// Lifecycle state (only meaningful for views).
    pub state: ViewState,
    /// Name of the assigned workload (only meaningful for views).
    pub workload_name: Option<String>,
    /// Names of sources/views this view directly depends on.
    pub depends_on: Vec<String>,
    /// Epoch from which the current backfill started (only set during backfill).
    pub backfill_started_epoch: Option<u64>,
}

/// A secondary index entry in the catalog.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub name: String,
    pub namespace_id: NamespaceId,
    pub table: String,
    pub columns: Vec<String>,
    pub predicate: Option<String>,
    pub state: ViewState, // BUILDING or READY
    pub state_bytes: u64,
    pub lag_ms: u64,
}

/// Key type for catalog lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CatalogKey {
    namespace_id: NamespaceId,
    name: String,
}

/// In-memory catalog store.
///
/// Thread-safety: This type is not `Send`/`Sync` intentionally — the v0.12
/// interface is designed for single-threaded tests and the CLI. A real
/// production store will wrap this in `Arc<Mutex<_>>` or replace it with a
/// `ShardDb`-backed implementation.
#[derive(Debug, Default)]
pub struct CatalogStore {
    entries: HashMap<CatalogKey, CatalogEntry>,
    /// Workload registry and namespace defaults (v0.16).
    workloads: WorkloadStore,
    /// Secondary indexes (v0.52).
    pub indexes: HashMap<CatalogKey, IndexEntry>,
}

impl CatalogStore {
    /// Create an empty catalog store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new source in the catalog.
    ///
    /// Returns `CatalogError::AlreadyExists` if a source or view with the
    /// same name already exists in the namespace.
    pub fn register_source(
        &mut self,
        namespace_id: NamespaceId,
        name: impl Into<String>,
        schema: SchemaVersion,
    ) -> Result<(), CatalogError> {
        let name = name.into();
        let key = CatalogKey {
            namespace_id,
            name: name.clone(),
        };
        if self.entries.contains_key(&key) {
            return Err(CatalogError::AlreadyExists { name });
        }
        self.entries.insert(
            key,
            CatalogEntry {
                name,
                namespace_id,
                kind: EntryKind::Source,
                schema,
                plan_bytes: None,
                state: ViewState::Running,
                workload_name: None,
                depends_on: Vec::new(),
                backfill_started_epoch: None,
            },
        );
        Ok(())
    }

    /// Register a new view in the catalog with an optional persisted plan.
    ///
    /// Returns `CatalogError::AlreadyExists` if a source or view with the
    /// same name already exists in the namespace.
    ///
    /// `depends_on` lists the names of sources/views this view reads from.
    /// `workload_name` optionally assigns the view to a named workload
    /// (falls back to the namespace default if `None`).
    pub fn register_view(
        &mut self,
        namespace_id: NamespaceId,
        name: impl Into<String>,
        schema: SchemaVersion,
        plan_bytes: Option<Vec<u8>>,
    ) -> Result<(), CatalogError> {
        self.register_view_with_options(namespace_id, name, schema, plan_bytes, vec![], None)
    }

    /// Register a new view with explicit dependency and workload metadata.
    ///
    /// This is the full-featured variant used by v0.16 DDL. `register_view`
    /// delegates here with empty `depends_on` and `None` workload.
    pub fn register_view_with_options(
        &mut self,
        namespace_id: NamespaceId,
        name: impl Into<String>,
        schema: SchemaVersion,
        plan_bytes: Option<Vec<u8>>,
        depends_on: Vec<String>,
        workload_name: Option<String>,
    ) -> Result<(), CatalogError> {
        let name = name.into();
        // Validate workload if provided, otherwise fall back to namespace default.
        let resolved_workload = match &workload_name {
            Some(wl) => {
                if self.workloads.get_workload(namespace_id, wl).is_none() {
                    return Err(CatalogError::WorkloadNotFound { name: wl.clone() });
                }
                Some(wl.clone())
            }
            None => self
                .workloads
                .get_namespace_default(namespace_id)
                .map(str::to_owned),
        };
        let key = CatalogKey {
            namespace_id,
            name: name.clone(),
        };
        if self.entries.contains_key(&key) {
            return Err(CatalogError::AlreadyExists { name });
        }
        self.entries.insert(
            key,
            CatalogEntry {
                name,
                namespace_id,
                kind: EntryKind::View,
                schema,
                plan_bytes,
                state: ViewState::Running,
                workload_name: resolved_workload,
                depends_on,
                backfill_started_epoch: None,
            },
        );
        Ok(())
    }

    /// Get a catalog entry by namespace and name.
    pub fn get(&self, namespace_id: NamespaceId, name: &str) -> Option<&CatalogEntry> {
        self.entries.get(&CatalogKey {
            namespace_id,
            name: name.to_owned(),
        })
    }

    /// Update the schema for an existing entry.
    ///
    /// Applies the compatibility rules from `compat::check_schema_change`.
    /// Returns `RS-1002` if the new schema is incompatible with the old one.
    pub fn update_schema(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
        new_schema: SchemaVersion,
    ) -> Result<(), CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: name.to_owned(),
        };
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: name.to_owned(),
            })?;
        check_schema_change(&entry.schema, &new_schema).into_result()?;
        entry.schema = new_schema;
        Ok(())
    }

    /// Store (overwrite) the raw plan bytes for a view.
    ///
    /// Returns `RS-0001` (NotFound) if no entry with that name exists.
    pub fn store_plan(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
        plan_bytes: Vec<u8>,
    ) -> Result<(), CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: name.to_owned(),
        };
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: name.to_owned(),
            })?;
        entry.plan_bytes = Some(plan_bytes);
        Ok(())
    }

    /// Load and decode the persisted plan for a view, checking every law
    /// annotation against the provided registry.
    ///
    /// Returns:
    /// - `Ok(PlanNode)` if the plan decodes successfully.
    /// - `RS-5002` if any law in the plan is not in the registry.
    /// - `CatalogError::NotFound` if no entry exists.
    /// - `CatalogError::Codec` if no plan has been stored yet or the bytes
    ///   are malformed.
    pub fn load_plan(
        &self,
        namespace_id: NamespaceId,
        name: &str,
        registry: &LawRegistry,
    ) -> Result<PlanNode, CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: name.to_owned(),
        };
        let entry = self
            .entries
            .get(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: name.to_owned(),
            })?;
        let bytes = entry
            .plan_bytes
            .as_deref()
            .ok_or_else(|| CatalogError::Codec(format!("no plan stored for '{name}'")))?;
        crate::codec::decode(bytes, registry)
    }

    /// Number of entries in the catalog.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    // ─── v0.16: Workload DDL ─────────────────────────────────────────────────

    /// Create a workload in the namespace — `CREATE WORKLOAD`.
    ///
    /// Returns `RS-1006` if a workload with that name already exists.
    pub fn create_workload(
        &mut self,
        namespace_id: NamespaceId,
        def: WorkloadDef,
    ) -> Result<(), CatalogError> {
        self.workloads.create_workload(namespace_id, def)
    }

    /// Retrieve a workload definition by name.
    pub fn get_workload(&self, namespace_id: NamespaceId, name: &str) -> Option<&WorkloadDef> {
        self.workloads.get_workload(namespace_id, name)
    }

    /// Drop a workload — validates that no views are still assigned to it.
    ///
    /// Returns `RS-1005` if the workload does not exist.
    /// Returns `CatalogError::AlreadyExists` if views still reference it
    /// (caller should treat this as a dependency error).
    pub fn drop_workload(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<WorkloadDef, CatalogError> {
        // Check for dependent views.
        let in_use = self.entries.values().any(|e| {
            e.namespace_id == namespace_id
                && e.kind == EntryKind::View
                && e.workload_name.as_deref() == Some(name)
        });
        if in_use {
            return Err(CatalogError::AlreadyExists {
                name: format!("workload '{name}' is still referenced by one or more views"),
            });
        }
        self.workloads.drop_workload(namespace_id, name)
    }

    /// Set the namespace-level default workload —
    /// `ALTER NAMESPACE ... SET DEFAULT WORKLOAD`.
    ///
    /// Returns `RS-1005` if the named workload does not exist.
    pub fn set_namespace_default_workload(
        &mut self,
        namespace_id: NamespaceId,
        workload_name: &str,
    ) -> Result<(), CatalogError> {
        self.workloads
            .set_namespace_default(namespace_id, workload_name)
    }

    /// Get the current namespace-level default workload name.
    pub fn get_namespace_default_workload(&self, namespace_id: NamespaceId) -> Option<&str> {
        self.workloads.get_namespace_default(namespace_id)
    }

    /// Assign a workload to an existing view —
    /// `ALTER MATERIALIZED VIEW ... SET WORKLOAD = <name>`.
    ///
    /// Returns `RS-1005` if the workload does not exist.
    /// Returns `CatalogError::NotFound` if the view does not exist.
    pub fn assign_workload_to_view(
        &mut self,
        namespace_id: NamespaceId,
        view_name: &str,
        workload_name: &str,
    ) -> Result<(), CatalogError> {
        if self
            .workloads
            .get_workload(namespace_id, workload_name)
            .is_none()
        {
            return Err(CatalogError::WorkloadNotFound {
                name: workload_name.to_owned(),
            });
        }
        let key = CatalogKey {
            namespace_id,
            name: view_name.to_owned(),
        };
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: view_name.to_owned(),
            })?;
        entry.workload_name = Some(workload_name.to_owned());
        Ok(())
    }

    // ─── v0.16: View Lifecycle ────────────────────────────────────────────────

    /// Pause a materialized view — `PAUSE MATERIALIZED VIEW <name>`.
    ///
    /// Returns `RS-1007` if the view is already paused.
    /// Returns `CatalogError::NotFound` if the view does not exist.
    pub fn pause_view(
        &mut self,
        namespace_id: NamespaceId,
        view_name: &str,
    ) -> Result<(), CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: view_name.to_owned(),
        };
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: view_name.to_owned(),
            })?;
        if entry.state == ViewState::Paused {
            return Err(CatalogError::ViewAlreadyPaused {
                name: view_name.to_owned(),
            });
        }
        entry.state = ViewState::Paused;
        Ok(())
    }

    /// Resume a paused materialized view — `RESUME MATERIALIZED VIEW <name>`.
    ///
    /// Returns `RS-1008` if the view is not paused.
    /// Returns `CatalogError::NotFound` if the view does not exist.
    pub fn resume_view(
        &mut self,
        namespace_id: NamespaceId,
        view_name: &str,
    ) -> Result<(), CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: view_name.to_owned(),
        };
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: view_name.to_owned(),
            })?;
        if entry.state != ViewState::Paused {
            return Err(CatalogError::ViewNotPaused {
                name: view_name.to_owned(),
            });
        }
        entry.state = ViewState::Running;
        Ok(())
    }

    // ─── v0.16: SHOW commands ─────────────────────────────────────────────────

    /// Return a status row for every view in the namespace —
    /// `SHOW VIEW STATUS FOR NAMESPACE`.
    pub fn show_view_status(&self, namespace_id: NamespaceId) -> Vec<ViewStatus> {
        self.entries
            .values()
            .filter(|e| e.namespace_id == namespace_id && e.kind == EntryKind::View)
            .map(|e| {
                let workload = e
                    .workload_name
                    .as_deref()
                    .and_then(|wn| self.workloads.get_workload(namespace_id, wn));
                ViewStatus::new(
                    namespace_id,
                    &e.name,
                    e.state.clone(),
                    workload,
                    e.depends_on.clone(),
                )
            })
            .collect()
    }

    /// Return backfill progress for a single view —
    /// `SHOW BACKFILL STATUS FOR MATERIALIZED VIEW <name>`.
    ///
    /// Returns `CatalogError::NotFound` if the view does not exist.
    pub fn show_backfill_status(
        &self,
        namespace_id: NamespaceId,
        view_name: &str,
    ) -> Result<BackfillStatus, CatalogError> {
        let entry = self
            .entries
            .get(&CatalogKey {
                namespace_id,
                name: view_name.to_owned(),
            })
            .ok_or_else(|| CatalogError::NotFound {
                name: view_name.to_owned(),
            })?;
        Ok(BackfillStatus {
            view_name: entry.name.clone(),
            state: entry.state.clone(),
            backfill_started_epoch: entry.backfill_started_epoch,
        })
    }

    // ─── v0.52: Secondary Indexes ─────────────────────────────────────────────

    /// Register a secondary index — `CREATE INDEX`.
    pub fn register_index(
        &mut self,
        namespace_id: NamespaceId,
        name: impl Into<String>,
        table: impl Into<String>,
        columns: Vec<String>,
        predicate: Option<String>,
    ) -> Result<(), CatalogError> {
        let name = name.into();
        let table = table.into();
        let key = CatalogKey {
            namespace_id,
            name: name.clone(),
        };
        // Check conflict with existing entries or indexes
        if self.entries.contains_key(&key) || self.indexes.contains_key(&key) {
            return Err(CatalogError::IndexNameConflict { name });
        }
        self.indexes.insert(
            key,
            IndexEntry {
                name,
                namespace_id,
                table,
                columns,
                predicate,
                state: ViewState::BackfillingFromEpoch(0), // BUILDING
                state_bytes: 0,
                lag_ms: 0,
            },
        );
        Ok(())
    }

    /// Set an index to READY state (`ViewState::Running`).
    pub fn set_index_ready(
        &mut self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<(), CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: name.to_owned(),
        };
        let index = self
            .indexes
            .get_mut(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: name.to_owned(),
            })?;
        index.state = ViewState::Running;
        Ok(())
    }

    /// Retrieve an index entry.
    pub fn get_index(&self, namespace_id: NamespaceId, name: &str) -> Option<&IndexEntry> {
        self.indexes.get(&CatalogKey {
            namespace_id,
            name: name.to_owned(),
        })
    }

    /// Drop a secondary index — `DROP INDEX`.
    pub fn drop_index(&mut self, namespace_id: NamespaceId, name: &str) -> Result<(), CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: name.to_owned(),
        };
        if self.indexes.remove(&key).is_none() {
            return Err(CatalogError::NotFound {
                name: name.to_owned(),
            });
        }
        // Frontier-aware arrangement GC will handle physical deletion
        Ok(())
    }

    /// Rebuild an index — resets state to BUILDING.
    pub fn rebuild_index(&mut self, namespace_id: NamespaceId, name: &str) -> Result<(), CatalogError> {
        let key = CatalogKey {
            namespace_id,
            name: name.to_owned(),
        };
        let index = self
            .indexes
            .get_mut(&key)
            .ok_or_else(|| CatalogError::NotFound {
                name: name.to_owned(),
            })?;
        index.state = ViewState::BackfillingFromEpoch(0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType, SchemaVersion};
    use rockstream_types::error_code::{RS_1002, RS_5002};
    use rockstream_types::ids::NamespaceId;

    fn ns() -> NamespaceId {
        NamespaceId(1)
    }

    fn orders_schema() -> SchemaVersion {
        SchemaVersion::new(vec![
            ColumnDef::required("order_id", DataType::Int64),
            ColumnDef::required("amount", DataType::Float64),
        ])
    }

    #[test]
    fn register_source_and_get() {
        let mut store = CatalogStore::new();
        store
            .register_source(ns(), "orders", orders_schema())
            .unwrap();
        let entry = store.get(ns(), "orders").unwrap();
        assert_eq!(entry.name, "orders");
        assert_eq!(entry.kind, EntryKind::Source);
    }

    #[test]
    fn register_duplicate_returns_already_exists() {
        let mut store = CatalogStore::new();
        store
            .register_source(ns(), "orders", orders_schema())
            .unwrap();
        let err = store
            .register_source(ns(), "orders", orders_schema())
            .unwrap_err();
        assert!(matches!(err, CatalogError::AlreadyExists { .. }));
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = CatalogStore::new();
        assert!(store.get(ns(), "missing").is_none());
    }

    #[test]
    fn update_schema_compatible_succeeds() {
        let mut store = CatalogStore::new();
        store
            .register_source(ns(), "orders", orders_schema())
            .unwrap();
        let mut new_cols = orders_schema().columns;
        new_cols.push(ColumnDef::nullable("note", DataType::Utf8));
        let new_schema = SchemaVersion {
            version: 2,
            columns: new_cols,
        };
        store.update_schema(ns(), "orders", new_schema).unwrap();
        let entry = store.get(ns(), "orders").unwrap();
        assert_eq!(entry.schema.columns.len(), 3);
    }

    #[test]
    fn update_schema_incompatible_returns_rs_1002() {
        let mut store = CatalogStore::new();
        store
            .register_source(ns(), "orders", orders_schema())
            .unwrap();
        // Remove a column — incompatible.
        let new_schema = SchemaVersion::new(vec![ColumnDef::required("order_id", DataType::Int64)]);
        let err = store.update_schema(ns(), "orders", new_schema).unwrap_err();
        assert_eq!(err.error_code(), RS_1002);
    }

    #[test]
    fn different_namespaces_are_isolated() {
        let mut store = CatalogStore::new();
        store
            .register_source(NamespaceId(1), "orders", orders_schema())
            .unwrap();
        // Same name in a different namespace — should succeed.
        store
            .register_source(NamespaceId(2), "orders", orders_schema())
            .unwrap();
        assert_eq!(store.len(), 2);
        assert!(store.get(NamespaceId(1), "orders").is_some());
        assert!(store.get(NamespaceId(3), "orders").is_none());
    }

    #[test]
    fn load_plan_unknown_law_returns_rs_5002() {
        use rockstream_plan::{AggregateExpr, AggregateFunc, Expr, PlanNode};
        use rockstream_types::laws::registry::LawRegistry;
        use rockstream_types::merge_law::{MergeLawId, MergeLawVersion};

        let mut store = CatalogStore::new();
        store
            .register_view(ns(), "my_view", orders_schema(), None)
            .unwrap();

        // Encode a plan with an unknown law (0xABCD).
        let plan = PlanNode::Aggregate {
            input: Box::new(PlanNode::Source {
                name: "orders".into(),
            }),
            group_by: vec![Expr::Column(0)],
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Sum,
                input: Expr::Column(1),
                distinct: false,
            }],
        };
        let unknown_law = |_: &PlanNode| -> Option<(MergeLawId, MergeLawVersion)> {
            Some((MergeLawId(0xABCD), MergeLawVersion(1)))
        };
        let bytes = crate::codec::encode(&plan, &unknown_law).unwrap();
        store.store_plan(ns(), "my_view", bytes).unwrap();

        // Load with empty registry → RS-5002.
        let empty_registry = LawRegistry::new();
        let err = store
            .load_plan(ns(), "my_view", &empty_registry)
            .unwrap_err();
        assert_eq!(err.error_code(), RS_5002);
    }
}

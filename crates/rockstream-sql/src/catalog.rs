//! Schema-version catalog for RockStream views (v0.7).
//!
//! Stores view definitions (SQL text + lowered `PlanNode` + output schema) in
//! a `ShardDb` under the `CatalogType::View` namespace.  Each view entry
//! carries a schema version number.
//!
//! # Key format
//! `CatalogKeyEncoder::encode(CatalogType::View, namespace_id=0, object_id=<hash(name)>)`
//!
//! # Schema-change classification
//!
//! Compatible changes (applied in-place):
//! - View definition replaces with the identical schema
//! - New nullable columns appended at the end
//!
//! Incompatible changes (return `RS-1002`):
//! - Any existing column renamed or removed
//! - Any existing column type changed
//! - Existing columns reordered
//!
//! # Proof obligation
//!
//! `lfs_view_plan_roundtrips_through_catalog` (in `tests/lfs_catalog.rs`) proves
//! that a `PlanNode` serializes to bytes, survives a `ShardDb` close/reopen
//! cycle, and deserializes to an equal `PlanNode`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use rockstream_plan::PlanNode;
use rockstream_storage::{keys::CatalogType, CatalogKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::schema_evolution::SchemaChangeKind;

use crate::error::SqlError;

// Helper to recursively collect dependencies of a plan.
fn collect_dependencies(plan: &PlanNode, registered: &HashSet<String>, out: &mut Vec<String>) {
    match plan {
        PlanNode::ViewRef { view_name } => {
            out.push(view_name.clone());
        }
        PlanNode::Source { name } => {
            if registered.contains(name) {
                out.push(name.clone());
            }
        }
        PlanNode::Snapshot { source_name, .. } => {
            if registered.contains(source_name) {
                out.push(source_name.clone());
            }
        }
        PlanNode::Filter { input, .. } => collect_dependencies(input, registered, out),
        PlanNode::Project { input, .. } => collect_dependencies(input, registered, out),
        PlanNode::Map { input, .. } => collect_dependencies(input, registered, out),
        PlanNode::Aggregate { input, .. } => collect_dependencies(input, registered, out),
        PlanNode::Window { input, .. } => collect_dependencies(input, registered, out),
        PlanNode::TumbleWindow { input, .. }
        | PlanNode::HopWindow { input, .. }
        | PlanNode::SessionWindow { input, .. } => collect_dependencies(input, registered, out),
        PlanNode::TopK { input, .. } => collect_dependencies(input, registered, out),
        PlanNode::Join { left, right, .. }
        | PlanNode::InnerJoin { left, right, .. }
        | PlanNode::OuterJoin { left, right, .. } => {
            collect_dependencies(left, registered, out);
            collect_dependencies(right, registered, out);
        }
        PlanNode::Union { left, right } => {
            collect_dependencies(left, registered, out);
            collect_dependencies(right, registered, out);
        }
        PlanNode::Distinct { input, .. } => collect_dependencies(input, registered, out),
        PlanNode::Intersect { left, right, .. } | PlanNode::Except { left, right, .. } => {
            collect_dependencies(left, registered, out);
            collect_dependencies(right, registered, out);
        }
        PlanNode::Recursion { base, step, .. } => {
            collect_dependencies(base, registered, out);
            collect_dependencies(step, registered, out);
        }
        PlanNode::Lateral { input, .. } => collect_dependencies(input, registered, out),
        PlanNode::ViewSink { child, .. } => collect_dependencies(child, registered, out),
        PlanNode::Exchange { child, .. } => collect_dependencies(child, registered, out),
        PlanNode::IndexArrange { input, .. } => collect_dependencies(input, registered, out),
    }
}

// ─── Column definition ───────────────────────────────────────────────────────

/// A column in a view's output schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Arrow data type name (e.g. `"Int64"`, `"Utf8"`).
    pub data_type: String,
    /// Whether the column can contain NULL.
    pub nullable: bool,
}

// ─── View entry ──────────────────────────────────────────────────────────────

/// A complete view definition stored in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewEntry {
    /// View name.
    pub name: String,
    /// The original SQL text used to create the view.
    pub sql: String,
    /// The lowered `PlanNode` tree (the physical plan).
    pub plan: PlanNode,
    /// Monotonically increasing schema version (starts at 1).
    pub schema_version: u32,
    /// Output columns in order.
    pub columns: Vec<ColumnDef>,
}

// ─── Index entry ─────────────────────────────────────────────────────────────

/// The build state of a secondary index (v0.32).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexState {
    /// Index is being backfilled from existing data.
    Building,
    /// Index is fully built and ready for queries.
    Ready,
}

/// A complete secondary index definition stored in the catalog (v0.32).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    /// Index name (unique within the namespace).
    pub name: String,
    /// The base table this index covers.
    pub table: String,
    /// Column names forming the index key.
    pub index_cols: Vec<String>,
    /// Column names forming the primary key of the base table.
    pub pk_cols: Vec<String>,
    /// SQL text of optional filter predicate (partial index).
    pub where_pred: Option<String>,
    /// Current build state.
    pub state: IndexState,
}

// ─── Schema-change classification ────────────────────────────────────────────

/// Classify a schema change from `old` to `new` column list.
///
/// Returns:
/// - `Compatible` if `new` equals `old` or extends it with new columns at the end.
/// - `Incompatible` if any existing column is renamed, removed, reordered,
///   or has its type changed.
pub fn classify_schema_change(old: &[ColumnDef], new: &[ColumnDef]) -> SchemaChangeKind {
    // If fewer columns than before, or any existing column changed → incompatible.
    if new.len() < old.len() {
        return SchemaChangeKind::Incompatible;
    }
    for (o, n) in old.iter().zip(new.iter()) {
        if o.name != n.name || o.data_type != n.data_type {
            return SchemaChangeKind::Incompatible;
        }
        // Widening nullability (not_null → nullable) is compatible.
    }
    SchemaChangeKind::Compatible
}

// ─── Catalog key helpers ─────────────────────────────────────────────────────

pub(crate) const DEFAULT_NAMESPACE: u128 = 0;

/// Compute the object_id (stable hash of view name) for catalog key construction.
fn view_object_id(name: &str) -> u128 {
    // Simple FNV-1a hash of the name bytes, extended to 128 bits.
    let mut hash: u128 = 0x6c62272e07bb0142_62b821756295c58d_u128;
    for byte in name.as_bytes() {
        hash ^= *byte as u128;
        hash = hash.wrapping_mul(0x0000_0000_0001_0000_0000_0000_0000_013B_u128);
    }
    hash
}

fn view_key(name: &str) -> Vec<u8> {
    CatalogKeyEncoder::encode(CatalogType::View, DEFAULT_NAMESPACE, view_object_id(name))
}

/// Compute the object_id (stable FNV-1a hash of index name) for catalog key construction (v0.32).
fn index_object_id(name: &str) -> u128 {
    // FNV-1a 128-bit hash (separate from view_object_id even though algorithm is identical,
    // ensuring Index and View namespaces are independent).
    let mut hash: u128 = 0x6c62272e07bb0142_62b821756295c58d_u128;
    for byte in name.as_bytes() {
        hash ^= *byte as u128;
        hash = hash.wrapping_mul(0x0000_0000_0001_0000_0000_0000_0000_013B_u128);
    }
    hash
}

fn index_key(name: &str) -> Vec<u8> {
    CatalogKeyEncoder::encode(CatalogType::Index, DEFAULT_NAMESPACE, index_object_id(name))
}

// ─── SchemaCatalog ───────────────────────────────────────────────────────────

/// Schema-version catalog backed by a `ShardDb`.
///
/// Stores view definitions under `CatalogType::View` keys.  Compatible schema
/// changes are applied in-place by incrementing the `schema_version`.
/// Incompatible changes are rejected with `RS-1002`.
pub struct SchemaCatalog {
    db: Arc<ShardDb>,
}

impl SchemaCatalog {
    /// Create a catalog backed by the given `ShardDb`.
    pub fn new(db: Arc<ShardDb>) -> Self {
        Self { db }
    }

    /// Load all registered views from the catalog.
    pub async fn load_all_views(&self) -> Result<HashMap<String, PlanNode>, SqlError> {
        let prefix = CatalogKeyEncoder::namespace_prefix(CatalogType::View, DEFAULT_NAMESPACE);
        let entries = self.db.scan_prefix(&prefix).await?;
        let mut views = HashMap::new();
        for (_key, value) in entries {
            let entry: ViewEntry = serde_json::from_slice(&value)?;
            views.insert(entry.name, entry.plan);
        }
        Ok(views)
    }

    /// Register a new view or update an existing one.
    ///
    /// - If no view with `name` exists: insert it with `schema_version = 1`.
    /// - If it exists and the schema change is compatible: update in-place,
    ///   incrementing `schema_version`.
    /// - If it exists and the schema change is incompatible: return
    ///   `RS-1002` (`SqlError::IncompatibleSchemaChange`).
    pub async fn register_view(
        &self,
        name: &str,
        sql: &str,
        plan: &PlanNode,
        columns: Vec<ColumnDef>,
    ) -> Result<(), SqlError> {
        // Build the dependency graph of all views, including the new one.
        let mut all_views = self.load_all_views().await?;
        all_views.insert(name.to_string(), plan.clone());

        let view_names: HashSet<String> = all_views.keys().cloned().collect();
        let mut adj = HashMap::new();
        for (vname, vplan) in &all_views {
            let mut deps = Vec::new();
            collect_dependencies(vplan, &view_names, &mut deps);
            adj.insert(vname.clone(), deps);
        }

        // DFS to detect cycles
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut cycle_path = Vec::new();

        fn dfs(
            node: &str,
            adj: &HashMap<String, Vec<String>>,
            visited: &mut HashSet<String>,
            rec_stack: &mut HashSet<String>,
            path: &mut Vec<String>,
        ) -> bool {
            visited.insert(node.to_string());
            rec_stack.insert(node.to_string());
            path.push(node.to_string());

            if let Some(neighbors) = adj.get(node) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        if dfs(neighbor, adj, visited, rec_stack, path) {
                            return true;
                        }
                    } else if rec_stack.contains(neighbor) {
                        path.push(neighbor.clone());
                        return true;
                    }
                }
            }

            rec_stack.remove(node);
            path.pop();
            false
        }

        for vname in all_views.keys() {
            if !visited.contains(vname)
                && dfs(vname, &adj, &mut visited, &mut rec_stack, &mut cycle_path)
            {
                return Err(SqlError::CycleDetected {
                    view_name: name.to_string(),
                    cycle_path,
                });
            }
        }

        let key = view_key(name);

        if let Some(existing_bytes) = self.db.get(&key).await? {
            // View already exists — classify the change.
            let existing: ViewEntry = serde_json::from_slice(&existing_bytes)?;
            match classify_schema_change(&existing.columns, &columns) {
                SchemaChangeKind::Compatible => {
                    let updated = ViewEntry {
                        name: name.to_string(),
                        sql: sql.to_string(),
                        plan: plan.clone(),
                        schema_version: existing.schema_version + 1,
                        columns,
                    };
                    let value = serde_json::to_vec(&updated)?;
                    let mut batch = WriteBatch::new();
                    batch.put(&key, &value);
                    self.db.write_batch(batch).await?;
                }
                SchemaChangeKind::Incompatible => {
                    return Err(SqlError::IncompatibleSchemaChange {
                        reason: format!(
                            "view '{}' schema changed incompatibly (version {}→{}); \
                             existing columns: {:?}, new columns: {:?}",
                            name,
                            existing.schema_version,
                            existing.schema_version + 1,
                            existing.columns,
                            columns,
                        ),
                    });
                }
            }
        } else {
            // New view.
            let entry = ViewEntry {
                name: name.to_string(),
                sql: sql.to_string(),
                plan: plan.clone(),
                schema_version: 1,
                columns,
            };
            let value = serde_json::to_vec(&entry)?;
            let mut batch = WriteBatch::new();
            batch.put(&key, &value);
            self.db.write_batch(batch).await?;
        }

        Ok(())
    }

    /// Load a view entry by name.  Returns `None` if no view with that name
    /// exists.
    pub async fn load_view(&self, name: &str) -> Result<Option<ViewEntry>, SqlError> {
        let key = view_key(name);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Return all view names stored in the catalog (for listing).
    ///
    /// Scans the full `CatalogType::View` prefix in the default namespace.
    /// Names starting with `__idx_` (internal index views) are excluded.
    pub async fn list_view_names(&self) -> Result<Vec<String>, SqlError> {
        let prefix = CatalogKeyEncoder::namespace_prefix(CatalogType::View, DEFAULT_NAMESPACE);
        let entries = self.db.scan_prefix(&prefix).await?;
        let mut names = Vec::new();
        for (_key, value) in entries {
            let entry: ViewEntry = serde_json::from_slice(&value)?;
            if !entry.name.starts_with("__idx_") {
                names.push(entry.name);
            }
        }
        Ok(names)
    }

    // ── v0.32 Index catalog methods ──────────────────────────────────────────

    /// Register a secondary index entry in the catalog (v0.32).
    ///
    /// Returns `RS-2016` if an index with the same name already exists for a
    /// **different** table. If it exists for the same table, the entry is
    /// updated (idempotent re-register).
    pub async fn register_index(&self, entry: &IndexEntry) -> Result<(), SqlError> {
        let key = index_key(&entry.name);
        if let Some(existing_bytes) = self.db.get(&key).await? {
            let existing: IndexEntry = serde_json::from_slice(&existing_bytes)?;
            if existing.table != entry.table {
                return Err(SqlError::IndexNameConflict {
                    index_name: entry.name.clone(),
                    existing_table: existing.table.clone(),
                    requested_table: entry.table.clone(),
                });
            }
        }
        let value = serde_json::to_vec(entry)?;
        let mut batch = WriteBatch::new();
        batch.put(&key, &value);
        self.db.write_batch(batch).await?;
        Ok(())
    }

    /// Load an index entry by name. Returns `None` if not found.
    pub async fn load_index(&self, name: &str) -> Result<Option<IndexEntry>, SqlError> {
        let key = index_key(name);
        match self.db.get(&key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Return all index names stored in the catalog.
    pub async fn list_index_names(&self) -> Result<Vec<String>, SqlError> {
        let prefix = CatalogKeyEncoder::namespace_prefix(CatalogType::Index, DEFAULT_NAMESPACE);
        let entries = self.db.scan_prefix(&prefix).await?;
        let mut names = Vec::new();
        for (_key, value) in entries {
            let entry: IndexEntry = serde_json::from_slice(&value)?;
            names.push(entry.name);
        }
        Ok(names)
    }

    /// Remove an index entry by name (for DROP INDEX).
    pub async fn remove_index(&self, name: &str) -> Result<(), SqlError> {
        let key = index_key(name);
        let mut batch = WriteBatch::new();
        batch.delete(&key);
        self.db.write_batch(batch).await?;
        Ok(())
    }

    /// Remove a view entry by name (for DROP INDEX internal view cleanup).
    pub async fn remove_view(&self, name: &str) -> Result<(), SqlError> {
        let key = view_key(name);
        let mut batch = WriteBatch::new();
        batch.delete(&key);
        self.db.write_batch(batch).await?;
        Ok(())
    }
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, dt: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: dt.to_string(),
            nullable: false,
        }
    }

    #[test]
    fn compatible_no_change() {
        let schema = vec![col("a", "Int64"), col("b", "Int64")];
        assert_eq!(
            classify_schema_change(&schema, &schema),
            SchemaChangeKind::Compatible
        );
    }

    #[test]
    fn compatible_new_column_appended() {
        let old = vec![col("a", "Int64")];
        let new = vec![col("a", "Int64"), col("b", "Utf8")];
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Compatible
        );
    }

    #[test]
    fn incompatible_column_renamed() {
        let old = vec![col("a", "Int64"), col("b", "Int64")];
        let new = vec![col("a", "Int64"), col("c", "Int64")];
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    #[test]
    fn incompatible_column_removed() {
        let old = vec![col("a", "Int64"), col("b", "Int64")];
        let new = vec![col("a", "Int64")];
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    #[test]
    fn incompatible_type_changed() {
        let old = vec![col("a", "Int64")];
        let new = vec![col("a", "Utf8")];
        assert_eq!(
            classify_schema_change(&old, &new),
            SchemaChangeKind::Incompatible
        );
    }

    #[test]
    fn view_object_id_is_stable() {
        assert_eq!(view_object_id("orders"), view_object_id("orders"));
        assert_ne!(view_object_id("orders"), view_object_id("items"));
    }

    #[test]
    fn index_entry_roundtrips_serde() {
        let entry = IndexEntry {
            name: "idx_customer".to_string(),
            table: "orders".to_string(),
            index_cols: vec!["customer_id".to_string()],
            pk_cols: vec!["order_id".to_string()],
            where_pred: None,
            state: IndexState::Ready,
        };
        let bytes = serde_json::to_vec(&entry).unwrap();
        let decoded: IndexEntry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, entry);
    }

    #[test]
    fn index_state_building_roundtrips_serde() {
        let entry = IndexEntry {
            name: "idx_partial".to_string(),
            table: "events".to_string(),
            index_cols: vec!["event_type".to_string()],
            pk_cols: vec!["event_id".to_string()],
            where_pred: Some("event_type = 'click'".to_string()),
            state: IndexState::Building,
        };
        let bytes = serde_json::to_vec(&entry).unwrap();
        let decoded: IndexEntry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, entry);
        assert_eq!(decoded.state, IndexState::Building);
    }

    #[test]
    fn index_object_id_is_stable() {
        assert_eq!(index_object_id("idx_foo"), index_object_id("idx_foo"));
        assert_ne!(index_object_id("idx_foo"), index_object_id("idx_bar"));
        // The two functions are separate (independent function bodies), even though
        // the algorithm is the same — keys are differentiated by CatalogType byte.
        assert_ne!(
            index_key("orders"),
            view_key("orders"),
            "index_key and view_key must differ (different CatalogType byte)"
        );
    }

    #[test]
    fn view_entry_serde_roundtrip() {
        let entry = ViewEntry {
            name: "orders_sum".to_string(),
            sql: "SELECT k, SUM(v) FROM t GROUP BY k".to_string(),
            plan: PlanNode::Source {
                name: "t".to_string(),
            },
            schema_version: 1,
            columns: vec![col("k", "Int64"), col("s", "Int64")],
        };
        let bytes = serde_json::to_vec(&entry).unwrap();
        let decoded: ViewEntry = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, entry);
    }
}

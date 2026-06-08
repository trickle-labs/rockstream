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

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use rockstream_plan::PlanNode;
use rockstream_storage::{keys::CatalogType, CatalogKeyEncoder, ShardDb, WriteBatch};
use rockstream_types::schema_evolution::SchemaChangeKind;

use crate::error::SqlError;

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

const DEFAULT_NAMESPACE: u128 = 0;

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
    pub async fn list_view_names(&self) -> Result<Vec<String>, SqlError> {
        let prefix = CatalogKeyEncoder::namespace_prefix(CatalogType::View, DEFAULT_NAMESPACE);
        let entries = self.db.scan_prefix(&prefix).await?;
        let mut names = Vec::new();
        for (_key, value) in entries {
            let entry: ViewEntry = serde_json::from_slice(&value)?;
            names.push(entry.name);
        }
        Ok(names)
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

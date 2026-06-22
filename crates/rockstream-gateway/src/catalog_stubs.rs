//! Catalog stubs for `pg_catalog` and `information_schema`.
//!
//! Handles ORM and psql reflection queries against standard Postgres system
//! catalogs. All responses are synthesised from the in-memory view catalog.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// Postgres type OIDs used in RowDescription fields.
pub const PG_OID_INT4: i32 = 23;
pub const PG_OID_INT8: i32 = 20;
pub const PG_OID_FLOAT8: i32 = 701;
pub const PG_OID_TEXT: i32 = 25;
pub const PG_OID_BOOL: i32 = 16;
pub const PG_OID_BYTEA: i32 = 17;
pub const PG_OID_TIMESTAMP: i32 = 1114;

/// Map an Arrow data type name to a Postgres type OID.
pub fn arrow_type_to_pg_oid(arrow_type: &str) -> i32 {
    match arrow_type {
        "Int32" => PG_OID_INT4,
        "Int64" => PG_OID_INT8,
        "Float64" => PG_OID_FLOAT8,
        "Utf8" | "LargeUtf8" => PG_OID_TEXT,
        "Boolean" => PG_OID_BOOL,
        "Binary" | "LargeBinary" => PG_OID_BYTEA,
        "Timestamp" | "TimestampMicrosecond" | "TimestampNanosecond" => PG_OID_TIMESTAMP,
        _ => PG_OID_TEXT,
    }
}

/// Map an Arrow data type name to a Postgres data type string (for
/// `information_schema.columns.data_type`).
pub fn arrow_type_to_pg_data_type(arrow_type: &str) -> &'static str {
    match arrow_type {
        "Int32" => "integer",
        "Int64" => "bigint",
        "Float64" => "double precision",
        "Utf8" | "LargeUtf8" => "text",
        "Boolean" => "boolean",
        "Binary" | "LargeBinary" => "bytea",
        "Timestamp" | "TimestampMicrosecond" | "TimestampNanosecond" => {
            "timestamp without time zone"
        }
        _ => "text",
    }
}

/// Stable OID derived from a view name (FNV-1a, truncated to positive i32).
pub fn view_oid(view_name: &str) -> i32 {
    let mut h: u32 = 0x811c9dc5;
    for byte in view_name.as_bytes() {
        h ^= *byte as u32;
        h = h.wrapping_mul(0x01000193);
    }
    // Ensure positive and non-zero
    ((h & 0x7FFF_FFFF) | 1) as i32
}

/// A view entry as seen by the catalog stubs.
#[derive(Debug, Clone)]
pub struct CatalogView {
    pub name: String,
    pub sql: String,
    pub columns: Vec<CatalogColumn>,
    /// Namespace this view belongs to (v0.26). Default: "public".
    pub namespace: String,
}

/// A table entry registered by `CREATE TABLE` commands.
#[derive(Debug, Clone)]
pub struct CatalogTable {
    pub name: String,
    pub columns: Vec<CatalogColumn>,
}

/// A column in a catalog view entry.
#[derive(Debug, Clone)]
pub struct CatalogColumn {
    pub name: String,
    /// Arrow data type name (e.g. "Int64", "Utf8").
    pub data_type: String,
}

/// State of a secondary index as tracked by the gateway catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogIndexState {
    Building,
    Ready,
}

/// A secondary index entry registered via `CREATE INDEX` through the pgwire layer.
#[derive(Debug, Clone)]
pub struct CatalogIndexEntry {
    pub name: String,
    pub table: String,
    pub index_cols: Vec<String>,
    pub state: CatalogIndexState,
}

/// Interior of `CatalogStubs` — held behind an `RwLock` for runtime mutation.
#[derive(Debug, Default)]
struct CatalogStubsInner {
    /// Keyed by view name.
    views: HashMap<String, CatalogView>,
    /// Keyed by table name (from CREATE TABLE).
    tables: HashMap<String, CatalogTable>,
    /// Dependency map: view_name → list of view names it depends on (parsed
    /// from `FROM`/`JOIN` clauses in CREATE VIEW SQL).
    deps: HashMap<String, Vec<String>>,
    /// Keyed by index name (from CREATE INDEX). v0.32.
    indexes: HashMap<String, CatalogIndexEntry>,
}

/// In-memory catalog of views exposed to Postgres clients.
///
/// Thread-safe via interior `RwLock` so that `Arc<CatalogStubs>` can be
/// mutated at runtime (e.g. by CREATE VIEW commands on the gateway).
#[derive(Debug, Default)]
pub struct CatalogStubs {
    inner: RwLock<CatalogStubsInner>,
}

impl CatalogStubs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a view in the catalog without dependency tracking.
    pub fn add_view(&self, view: CatalogView) {
        let mut inner = self.inner.write().unwrap();
        inner.views.insert(view.name.clone(), view);
    }

    /// Register a view with an explicit dependency list.
    ///
    /// Does **not** perform cycle detection — use
    /// [`detect_cycle_with_new_view`] first if needed.
    pub fn add_view_with_deps(&self, view: CatalogView, deps: Vec<String>) {
        let mut inner = self.inner.write().unwrap();
        inner.deps.insert(view.name.clone(), deps);
        inner.views.insert(view.name.clone(), view);
    }

    /// Register a view that already has its namespace set.
    pub fn add_view_in_namespace(&self, view: CatalogView) {
        let mut inner = self.inner.write().unwrap();
        inner.views.insert(view.name.clone(), view);
    }

    /// List all registered views.
    pub fn list_views(&self) -> Vec<CatalogView> {
        let inner = self.inner.read().unwrap();
        let mut v: Vec<CatalogView> = inner.views.values().cloned().collect();
        v.sort_by_key(|v| v.name.clone());
        v
    }

    /// Look up a view by name, cloning the entry.
    pub fn get_view(&self, name: &str) -> Option<CatalogView> {
        let inner = self.inner.read().unwrap();
        inner.views.get(name).cloned()
    }

    /// Register a table in the catalog.
    ///
    /// If the table already exists, the call is idempotent only when the caller
    /// handles `IF NOT EXISTS` logic. Returns `true` if a new table was inserted,
    /// `false` if a table with the same name already existed.
    pub fn add_table(&self, table: CatalogTable) -> bool {
        let mut inner = self.inner.write().unwrap();
        if inner.tables.contains_key(&table.name) {
            return false;
        }
        inner.tables.insert(table.name.clone(), table);
        true
    }

    /// Update the column list for an existing view (called after materialisation).
    pub fn update_view_columns(&self, view_name: &str, columns: Vec<CatalogColumn>) {
        let mut inner = self.inner.write().unwrap();
        if let Some(v) = inner.views.get_mut(view_name) {
            v.columns = columns;
        }
    }

    /// Return the dependency list for a specific view.
    pub fn get_view_deps(&self, view_name: &str) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner.deps.get(view_name).cloned().unwrap_or_default()
    }

    /// Look up a table by name, cloning the entry.
    pub fn get_table(&self, name: &str) -> Option<CatalogTable> {
        let inner = self.inner.read().unwrap();
        inner.tables.get(name).cloned()
    }

    /// List all registered tables.
    pub fn list_tables(&self) -> Vec<CatalogTable> {
        let inner = self.inner.read().unwrap();
        let mut v: Vec<CatalogTable> = inner.tables.values().cloned().collect();
        v.sort_by_key(|t| t.name.clone());
        v
    }

    /// Check whether adding a new view with the given dependencies would
    /// introduce a cycle.
    ///
    /// Returns `Some((view_name, cycle_path))` if a cycle is detected,
    /// `None` if the new view is acyclic.
    pub fn detect_cycle_with_new_view(
        &self,
        new_name: &str,
        new_deps: &[String],
    ) -> Option<(String, Vec<String>)> {
        let inner = self.inner.read().unwrap();
        let mut all_deps = inner.deps.clone();
        all_deps.insert(new_name.to_string(), new_deps.to_vec());

        // DFS: can we reach `new_name` starting from any of its deps?
        fn can_reach(
            target: &str,
            current: &str,
            all_deps: &HashMap<String, Vec<String>>,
            visited: &mut HashSet<String>,
            path: &mut Vec<String>,
        ) -> bool {
            if current == target {
                return true;
            }
            if visited.contains(current) {
                return false;
            }
            visited.insert(current.to_string());
            path.push(current.to_string());
            if let Some(dep_list) = all_deps.get(current) {
                for dep in dep_list {
                    if can_reach(target, dep, all_deps, visited, path) {
                        return true;
                    }
                }
            }
            path.pop();
            false
        }

        for dep in new_deps {
            let mut visited = HashSet::new();
            let mut path = vec![new_name.to_string()];
            if can_reach(new_name, dep, &all_deps, &mut visited, &mut path) {
                path.push(dep.to_string());
                return Some((new_name.to_string(), path));
            }
        }
        None
    }

    // ── Index catalog (v0.32 pgwire DDL wiring) ───────────────────────────────

    /// Register a new index in Building state. Returns `false` if an index
    /// with the same name and a different table already exists (RS-2016).
    pub fn add_index(&self, entry: CatalogIndexEntry) -> bool {
        let mut inner = self.inner.write().unwrap();
        if let Some(existing) = inner.indexes.get(&entry.name) {
            if existing.table != entry.table {
                return false;
            }
        }
        inner.indexes.insert(entry.name.clone(), entry);
        true
    }

    /// Look up an index by name.
    pub fn get_index(&self, name: &str) -> Option<CatalogIndexEntry> {
        let inner = self.inner.read().unwrap();
        inner.indexes.get(name).cloned()
    }

    /// Remove an index entry (DROP INDEX).
    pub fn remove_index(&self, name: &str) {
        let mut inner = self.inner.write().unwrap();
        inner.indexes.remove(name);
    }

    /// Transition an existing index back to Building state (REBUILD INDEX).
    /// Returns `false` if the index does not exist.
    pub fn rebuild_index(&self, name: &str) -> bool {
        let mut inner = self.inner.write().unwrap();
        if let Some(entry) = inner.indexes.get_mut(name) {
            entry.state = CatalogIndexState::Building;
            return true;
        }
        false
    }

    /// List all registered index names.
    pub fn list_index_names(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let mut names: Vec<String> = inner.indexes.keys().cloned().collect();
        names.sort();
        names
    }

    /// Dispatch a query string to a catalog handler. Returns `Some(rows)` if
    /// this is a recognized catalog query, `None` if the query should be
    /// forwarded to the normal query path.
    ///
    /// Rows are returned as `Vec<Vec<Option<String>>>`, one inner Vec per row.
    pub fn handle_query(&self, query: &str) -> Option<CatalogResponse> {
        let q = query.trim();

        // SHOW commands
        if q.eq_ignore_ascii_case("show server_version")
            || q.eq_ignore_ascii_case("show server_version;")
        {
            return Some(CatalogResponse::rows(
                vec!["server_version".to_string()],
                vec![vec![Some("14.0".to_string())]],
            ));
        }

        if q.eq_ignore_ascii_case("show transaction_isolation")
            || q.eq_ignore_ascii_case("show transaction_isolation;")
        {
            return Some(CatalogResponse::rows(
                vec!["transaction_isolation".to_string()],
                vec![vec![Some("read committed".to_string())]],
            ));
        }

        let ql = q.to_lowercase();

        // SET commands → CommandComplete("SET")
        if ql.starts_with("set ") || ql.starts_with("set\t") {
            return Some(CatalogResponse::CommandComplete("SET".to_string()));
        }

        // pg_catalog queries
        if ql.contains("pg_catalog.pg_tables")
            || ql.contains("pg_tables") && ql.contains("pg_catalog")
        {
            return Some(self.pg_tables());
        }
        if ql.contains("pg_catalog.pg_views")
            || (ql.contains("pg_views") && ql.contains("pg_catalog"))
        {
            return Some(self.pg_views());
        }
        if ql.contains("pg_catalog.pg_class")
            || (ql.contains("pg_class") && ql.contains("pg_catalog"))
        {
            return Some(self.pg_class());
        }
        if ql.contains("pg_catalog.pg_attribute")
            || (ql.contains("pg_attribute") && ql.contains("pg_catalog"))
        {
            return Some(self.pg_attribute());
        }
        if ql.contains("pg_catalog.pg_namespace")
            || (ql.contains("pg_namespace") && ql.contains("pg_catalog"))
        {
            return Some(self.pg_namespace());
        }
        if ql.contains("pg_catalog.pg_type")
            || (ql.contains("pg_type") && ql.contains("pg_catalog"))
        {
            return Some(self.pg_type());
        }

        // information_schema
        if ql.contains("information_schema.tables") {
            return Some(self.information_schema_tables());
        }
        if ql.contains("information_schema.columns") {
            return Some(self.information_schema_columns());
        }

        None
    }

    // ── pg_catalog.pg_tables ──────────────────────────────────────────────────

    fn pg_tables(&self) -> CatalogResponse {
        let cols = vec![
            "schemaname".to_string(),
            "tablename".to_string(),
            "tableowner".to_string(),
            "tablespace".to_string(),
            "hasindexes".to_string(),
            "hasrules".to_string(),
            "hastriggers".to_string(),
        ];
        let rows: Vec<Vec<Option<String>>> = self
            .list_views()
            .into_iter()
            .map(|v| {
                vec![
                    Some("public".to_string()),
                    Some(v.name.clone()),
                    Some("rockstream".to_string()),
                    None,
                    Some("f".to_string()),
                    Some("f".to_string()),
                    Some("f".to_string()),
                ]
            })
            .collect();
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_views ──────────────────────────────────────────────────

    fn pg_views(&self) -> CatalogResponse {
        let cols = vec![
            "schemaname".to_string(),
            "viewname".to_string(),
            "viewowner".to_string(),
            "definition".to_string(),
        ];
        let rows: Vec<Vec<Option<String>>> = self
            .list_views()
            .into_iter()
            .map(|v| {
                vec![
                    Some("public".to_string()),
                    Some(v.name.clone()),
                    Some("rockstream".to_string()),
                    Some(v.sql.clone()),
                ]
            })
            .collect();
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_class ──────────────────────────────────────────────────

    fn pg_class(&self) -> CatalogResponse {
        let cols = vec![
            "oid".to_string(),
            "relname".to_string(),
            "relnamespace".to_string(),
            "relkind".to_string(),
        ];
        let rows: Vec<Vec<Option<String>>> = self
            .list_views()
            .into_iter()
            .map(|v| {
                vec![
                    Some(view_oid(&v.name).to_string()),
                    Some(v.name.clone()),
                    Some("2200".to_string()), // public namespace OID
                    Some("v".to_string()),    // 'v' = view
                ]
            })
            .collect();
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_attribute ──────────────────────────────────────────────

    fn pg_attribute(&self) -> CatalogResponse {
        let cols = vec![
            "attrelid".to_string(),
            "attname".to_string(),
            "atttypid".to_string(),
            "attnum".to_string(),
            "attnotnull".to_string(),
        ];
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        for v in self.list_views() {
            let oid = view_oid(&v.name);
            for (i, col) in v.columns.iter().enumerate() {
                rows.push(vec![
                    Some(oid.to_string()),
                    Some(col.name.clone()),
                    Some(arrow_type_to_pg_oid(&col.data_type).to_string()),
                    Some((i as i16 + 1).to_string()),
                    Some("f".to_string()),
                ]);
            }
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_namespace ──────────────────────────────────────────────

    fn pg_namespace(&self) -> CatalogResponse {
        let cols = vec!["oid".to_string(), "nspname".to_string()];
        let rows = vec![vec![Some("2200".to_string()), Some("public".to_string())]];
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_type ───────────────────────────────────────────────────

    fn pg_type(&self) -> CatalogResponse {
        let cols = vec![
            "oid".to_string(),
            "typname".to_string(),
            "typlen".to_string(),
        ];
        let type_rows = vec![
            (PG_OID_INT4, "int4", 4i32),
            (PG_OID_INT8, "int8", 8),
            (PG_OID_FLOAT8, "float8", 8),
            (PG_OID_TEXT, "text", -1),
            (PG_OID_BOOL, "bool", 1),
            (PG_OID_BYTEA, "bytea", -1),
            (PG_OID_TIMESTAMP, "timestamp", 8),
        ];
        let rows = type_rows
            .into_iter()
            .map(|(oid, name, len)| {
                vec![
                    Some(oid.to_string()),
                    Some(name.to_string()),
                    Some(len.to_string()),
                ]
            })
            .collect();
        CatalogResponse::rows(cols, rows)
    }

    // ── information_schema.tables ────────────────────────────────────────────

    fn information_schema_tables(&self) -> CatalogResponse {
        let cols = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "table_type".to_string(),
        ];
        let rows: Vec<Vec<Option<String>>> = self
            .list_views()
            .into_iter()
            .map(|v| {
                vec![
                    Some("rockstream".to_string()),
                    Some("public".to_string()),
                    Some(v.name.clone()),
                    Some("VIEW".to_string()),
                ]
            })
            .collect();
        CatalogResponse::rows(cols, rows)
    }

    // ── information_schema.columns ───────────────────────────────────────────

    fn information_schema_columns(&self) -> CatalogResponse {
        let cols = vec![
            "table_catalog".to_string(),
            "table_schema".to_string(),
            "table_name".to_string(),
            "column_name".to_string(),
            "ordinal_position".to_string(),
            "data_type".to_string(),
        ];
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        for v in self.list_views() {
            for (i, col) in v.columns.iter().enumerate() {
                rows.push(vec![
                    Some("rockstream".to_string()),
                    Some("public".to_string()),
                    Some(v.name.clone()),
                    Some(col.name.clone()),
                    Some((i + 1).to_string()),
                    Some(arrow_type_to_pg_data_type(&col.data_type).to_string()),
                ]);
            }
        }
        CatalogResponse::rows(cols, rows)
    }
}

/// A response from the catalog stub handler.
#[derive(Debug)]
pub enum CatalogResponse {
    /// A result set with column names and rows.
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    },
    /// A command completion (no rows).
    CommandComplete(String),
}

impl CatalogResponse {
    fn rows(columns: Vec<String>, rows: Vec<Vec<Option<String>>>) -> Self {
        CatalogResponse::Rows { columns, rows }
    }
}

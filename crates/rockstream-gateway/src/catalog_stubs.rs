//! Catalog stubs for `pg_catalog` and `information_schema`.
//!
//! Handles ORM and psql reflection queries against standard Postgres system
//! catalogs. All responses are synthesised from the in-memory view catalog.

use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock};
use std::time::SystemTime;

/// Session context passed to catalog query handlers.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub backend_pid: u32,
    pub search_path: String,
    pub principal_name: String,
}

impl Default for SessionInfo {
    fn default() -> Self {
        SessionInfo {
            backend_pid: 0,
            search_path: "public".to_string(),
            principal_name: "postgres".to_string(),
        }
    }
}

static SERVER_START_TIME: OnceLock<SystemTime> = OnceLock::new();

/// Postgres type OIDs used in RowDescription fields.
pub const PG_OID_INT2: i32 = 21;
pub const PG_OID_INT4: i32 = 23;
pub const PG_OID_INT8: i32 = 20;
pub const PG_OID_FLOAT4: i32 = 700;
pub const PG_OID_FLOAT8: i32 = 701;
pub const PG_OID_TEXT: i32 = 25;
pub const PG_OID_BOOL: i32 = 16;
pub const PG_OID_BYTEA: i32 = 17;
pub const PG_OID_TIMESTAMP: i32 = 1114;
pub const PG_OID_TIMESTAMPTZ: i32 = 1184;
pub const PG_OID_DATE: i32 = 1082;
pub const PG_OID_TIME: i32 = 1083;
pub const PG_OID_UUID: i32 = 2950;
pub const PG_OID_NUMERIC: i32 = 1700;
pub const PG_OID_JSON: i32 = 114;
pub const PG_OID_JSONB: i32 = 3802;
pub const PG_OID_VARCHAR: i32 = 1043;
pub const PG_OID_CHAR: i32 = 1042;
pub const PG_OID_INTERVAL: i32 = 1186;

pub const PG_OID_ARRAY_INT4: i32 = 1007;
pub const PG_OID_ARRAY_INT8: i32 = 1016;
pub const PG_OID_ARRAY_TEXT: i32 = 1009;
pub const PG_OID_ARRAY_FLOAT8: i32 = 1022;
pub const PG_OID_ARRAY_BOOL: i32 = 1000;
pub const PG_OID_ARRAY_UUID: i32 = 2951;

/// Map an Arrow data type name to a Postgres type OID.
pub fn arrow_type_to_pg_oid(arrow_type: &str) -> i32 {
    match arrow_type {
        "Int16" => PG_OID_INT2,
        "Int32" => PG_OID_INT4,
        "Int64" => PG_OID_INT8,
        "Float32" => PG_OID_FLOAT4,
        "Float64" => PG_OID_FLOAT8,
        "Utf8" | "LargeUtf8" => PG_OID_TEXT,
        "Boolean" => PG_OID_BOOL,
        "Binary" | "LargeBinary" => PG_OID_BYTEA,
        "Timestamp" | "TimestampMicrosecond" | "TimestampNanosecond" => PG_OID_TIMESTAMP,
        "TimestampTz" => PG_OID_TIMESTAMPTZ,
        "Date32" | "Date64" => PG_OID_DATE,
        "Time32" | "Time64" => PG_OID_TIME,
        "Uuid" | "UUID" => PG_OID_UUID,
        "Decimal" | "Decimal128" | "Decimal256" => PG_OID_NUMERIC,
        "Json" | "JSON" => PG_OID_JSON,
        "Jsonb" | "JSONB" => PG_OID_JSONB,
        "Varchar" | "VARCHAR" => PG_OID_VARCHAR,
        "Char" | "CHAR" => PG_OID_CHAR,
        "Interval" => PG_OID_INTERVAL,
        "_int4" | "List(Int32)" => PG_OID_ARRAY_INT4,
        "_int8" | "List(Int64)" => PG_OID_ARRAY_INT8,
        "_text" | "List(Utf8)" => PG_OID_ARRAY_TEXT,
        "_float8" | "List(Float64)" => PG_OID_ARRAY_FLOAT8,
        "_bool" | "List(Boolean)" => PG_OID_ARRAY_BOOL,
        "_uuid" | "List(Uuid)" => PG_OID_ARRAY_UUID,
        _ => PG_OID_TEXT,
    }
}

/// Map an Arrow data type name to a Postgres data type string (for
/// `information_schema.columns.data_type`).
pub fn arrow_type_to_pg_data_type(arrow_type: &str) -> &'static str {
    match arrow_type {
        "Int16" => "smallint",
        "Int32" => "integer",
        "Int64" => "bigint",
        "Float32" => "real",
        "Float64" => "double precision",
        "Utf8" | "LargeUtf8" => "text",
        "Boolean" => "boolean",
        "Binary" | "LargeBinary" => "bytea",
        "Timestamp" | "TimestampMicrosecond" | "TimestampNanosecond" => {
            "timestamp without time zone"
        }
        "TimestampTz" => "timestamp with time zone",
        "Date32" | "Date64" => "date",
        "Time32" | "Time64" => "time without time zone",
        "Uuid" | "UUID" => "uuid",
        "Decimal" | "Decimal128" | "Decimal256" => "numeric",
        "Json" | "JSON" => "json",
        "Jsonb" | "JSONB" => "jsonb",
        "Varchar" | "VARCHAR" => "character varying",
        "Char" | "CHAR" => "character",
        "Interval" => "interval",
        "_int4" | "List(Int32)" => "integer[]",
        "_int8" | "List(Int64)" => "bigint[]",
        "_text" | "List(Utf8)" => "text[]",
        "_float8" | "List(Float64)" => "double precision[]",
        "_bool" | "List(Boolean)" => "boolean[]",
        "_uuid" | "List(Uuid)" => "uuid[]",
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
    /// OperatorId (as u64) of the `IndexArrangeOp` backing this index.
    /// Set via `mark_index_ready` once backfill completes. `None` means the
    /// index is still Building and cannot serve point lookups through the gateway.
    pub op_id: Option<u64>,
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

    /// Transition an existing index to Ready state and record its `op_id`.
    /// Returns `false` if the index does not exist.
    pub fn mark_index_ready(&self, name: &str, op_id: u64) -> bool {
        let mut inner = self.inner.write().unwrap();
        if let Some(entry) = inner.indexes.get_mut(name) {
            entry.state = CatalogIndexState::Ready;
            entry.op_id = Some(op_id);
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

    /// List all registered indexes.
    pub fn list_indexes(&self) -> Vec<CatalogIndexEntry> {
        let inner = self.inner.read().unwrap();
        let mut idxs: Vec<CatalogIndexEntry> = inner.indexes.values().cloned().collect();
        idxs.sort_by(|a, b| a.name.cmp(&b.name));
        idxs
    }

    /// Dispatch a query string to a catalog handler. Returns `Some(rows)` if
    /// this is a recognized catalog query, `None` if the query should be
    /// forwarded to the normal query path.
    ///
    /// Rows are returned as `Vec<Vec<Option<String>>>`, one inner Vec per row.
    pub fn handle_query(&self, query: &str, session_info: &SessionInfo) -> Option<CatalogResponse> {
        let q = query.trim();
        let ql = q.to_lowercase();

        // SHOW commands
        if ql.contains("server_version") {
            return Some(CatalogResponse::rows(
                vec!["server_version".to_string()],
                vec![vec![Some("14.0".to_string())]],
            ));
        }

        if ql.contains("transaction_isolation") || ql.contains("transaction isolation level") {
            return Some(CatalogResponse::rows(
                vec!["transaction_isolation".to_string()],
                vec![vec![Some("read committed".to_string())]],
            ));
        }

        if ql.contains("standard_conforming_strings") {
            return Some(CatalogResponse::rows(
                vec!["standard_conforming_strings".to_string()],
                vec![vec![Some("on".to_string())]],
            ));
        }

        // functions
        if ql.contains("version()") {
            return Some(self.version());
        }
        if ql.contains("current_schema()") {
            return Some(self.current_schema());
        }
        if ql.contains("current_database()") {
            return Some(self.current_database());
        }
        if ql.contains("current_setting") {
            return Some(self.current_setting(&ql));
        }

        let requested_cols = parse_select_columns(query);

        // pg_catalog queries
        if ql.contains("pg_attribute") {
            return Some(self.pg_attribute(&requested_cols));
        }
        if ql.contains("pg_type") {
            return Some(self.pg_type(&requested_cols));
        }
        if ql.contains("pg_class") {
            return Some(self.pg_class(&requested_cols));
        }
        if ql.contains("pg_proc") {
            return Some(self.pg_proc(&requested_cols));
        }
        if ql.contains("pg_index") {
            return Some(self.pg_index(&requested_cols));
        }
        if ql.contains("pg_constraint") {
            return Some(self.pg_constraint(&requested_cols));
        }
        if ql.contains("pg_description") {
            return Some(self.pg_description(&requested_cols));
        }
        if ql.contains("pg_enum") {
            return Some(self.pg_enum(&requested_cols));
        }
        if ql.contains("pg_roles") {
            return Some(self.pg_roles(&session_info.principal_name, &requested_cols));
        }
        if ql.contains("pg_user") {
            return Some(self.pg_user(&session_info.principal_name, &requested_cols));
        }
        if ql.contains("pg_namespace") {
            return Some(self.pg_namespace(&requested_cols));
        }
        if ql.contains("pg_tables") {
            return Some(self.pg_tables(&requested_cols));
        }
        if ql.contains("pg_views") {
            return Some(self.pg_views(&requested_cols));
        }

        // information_schema
        if ql.contains("information_schema.tables") {
            return Some(self.information_schema_tables(&requested_cols));
        }
        if ql.contains("information_schema.columns") {
            return Some(self.information_schema_columns(&requested_cols));
        }
        if ql.contains("information_schema.key_column_usage") || ql.contains("key_column_usage") {
            return Some(self.key_column_usage(&requested_cols));
        }
        if ql.contains("information_schema.referential_constraints")
            || ql.contains("referential_constraints")
        {
            return Some(self.referential_constraints(&requested_cols));
        }

        // S7: bootstrap functions and identity keywords (SELECT without FROM)
        if ql.starts_with("select ") && !ql.contains(" from ") {
            if ql.contains("current_user") {
                return Some(CatalogResponse::rows(
                    vec!["current_user".to_string()],
                    vec![vec![Some(session_info.principal_name.clone())]],
                ));
            }
            if ql.contains("session_user") {
                return Some(CatalogResponse::rows(
                    vec!["session_user".to_string()],
                    vec![vec![Some(session_info.principal_name.clone())]],
                ));
            }
            if ql.contains("current_schemas(") {
                let include_implicit = ql.contains("true");
                let mut parts: Vec<String> = if include_implicit {
                    vec!["pg_catalog".to_string()]
                } else {
                    vec![]
                };
                for s in session_info.search_path.split(',') {
                    let trimmed = s.trim().to_string();
                    if !trimmed.is_empty() {
                        parts.push(trimmed);
                    }
                }
                let arr = format!("{{{}}}", parts.join(","));
                return Some(CatalogResponse::rows(
                    vec!["current_schemas".to_string()],
                    vec![vec![Some(arr)]],
                ));
            }
            if ql.contains("pg_backend_pid(") {
                return Some(CatalogResponse::rows(
                    vec!["pg_backend_pid".to_string()],
                    vec![vec![Some(session_info.backend_pid.to_string())]],
                ));
            }
            if ql.contains("pg_is_in_recovery(") {
                return Some(CatalogResponse::rows(
                    vec!["pg_is_in_recovery".to_string()],
                    vec![vec![Some("f".to_string())]],
                ));
            }
            if ql.contains("set_config(") {
                let val = parse_set_config_value(q).unwrap_or_default();
                return Some(CatalogResponse::rows(
                    vec!["set_config".to_string()],
                    vec![vec![Some(val)]],
                ));
            }
            if ql.contains("pg_postmaster_start_time(") {
                let t = *SERVER_START_TIME.get_or_init(SystemTime::now);
                let ts = format_system_time(t);
                return Some(CatalogResponse::rows(
                    vec!["pg_postmaster_start_time".to_string()],
                    vec![vec![Some(ts)]],
                ));
            }
            if ql.contains("txid_current(") {
                return Some(CatalogResponse::rows(
                    vec!["txid_current".to_string()],
                    vec![vec![Some("0".to_string())]],
                ));
            }
        }

        // S7: SHOW ALL
        if ql.trim_end_matches(';') == "show all" {
            let cols = vec![
                "name".to_string(),
                "setting".to_string(),
                "description".to_string(),
            ];
            let params: &[(&str, &str, &str)] = &[
                (
                    "search_path",
                    session_info.search_path.as_str(),
                    "Schema search path",
                ),
                ("client_encoding", "UTF8", "Client-side character encoding"),
                ("server_encoding", "UTF8", "Server-side character encoding"),
                (
                    "transaction_isolation",
                    "read committed",
                    "Transaction isolation level",
                ),
                (
                    "standard_conforming_strings",
                    "on",
                    "Standard conforming strings",
                ),
                ("DateStyle", "ISO, YMD", "Date/time format"),
                ("integer_datetimes", "on", "Integer timestamps"),
                ("server_version", "14.9", "Server version"),
                ("timezone", "UTC", "Timezone"),
                ("application_name", "", "Application name"),
            ];
            let rows: Vec<Vec<Option<String>>> = params
                .iter()
                .map(|(name, val, desc)| {
                    vec![
                        Some(name.to_string()),
                        Some(val.to_string()),
                        Some(desc.to_string()),
                    ]
                })
                .collect();
            return Some(CatalogResponse::rows(cols, rows));
        }

        // S7: SHOW client_encoding / SHOW server_encoding
        if ql.starts_with("show ") {
            let key = ql["show ".len()..].trim().trim_end_matches(';');
            if key == "client_encoding" || key == "server_encoding" {
                return Some(CatalogResponse::rows(
                    vec![key.to_string()],
                    vec![vec![Some("UTF8".to_string())]],
                ));
            }
        }

        None
    }

    /// Resolve a view by name within a given search_path (S9 search-path-aware lookup).
    pub fn resolve_view(&self, name: &str, search_path: &[&str]) -> Option<CatalogView> {
        let inner = self.inner.read().unwrap();
        if let Some(view) = inner.views.get(name) {
            if search_path
                .iter()
                .any(|s| s.trim() == view.namespace.as_str())
            {
                return Some(view.clone());
            }
        }
        None
    }

    // ── pg_catalog.pg_tables ──────────────────────────────────────────────────

    fn pg_tables(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "schemaname".to_string(),
                "tablename".to_string(),
                "tableowner".to_string(),
                "hasindexes".to_string(),
                "hasrules".to_string(),
                "hastriggers".to_string(),
                "rowsecurity".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let mut rows = Vec::new();
        for t in self.list_tables() {
            let mut row = Vec::new();
            for c in &cols {
                let val = match c.as_str() {
                    "schemaname" => Some("public".to_string()),
                    "tablename" => Some(t.name.clone()),
                    "tableowner" => Some("rockstream".to_string()),
                    "hasindexes" => Some("f".to_string()),
                    "hasrules" => Some("f".to_string()),
                    "hastriggers" => Some("f".to_string()),
                    "rowsecurity" => Some("f".to_string()),
                    _ => Some("".to_string()),
                };
                row.push(val);
            }
            rows.push(row);
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_views ───────────────────────────────────────────────────

    fn pg_views(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "schemaname".to_string(),
                "viewname".to_string(),
                "viewowner".to_string(),
                "definition".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let mut rows = Vec::new();
        for v in self.list_views() {
            let mut row = Vec::new();
            for c in &cols {
                let val = match c.as_str() {
                    "schemaname" => Some("public".to_string()),
                    "viewname" => Some(v.name.clone()),
                    "viewowner" => Some("rockstream".to_string()),
                    "definition" => Some(v.sql.clone()),
                    _ => Some("".to_string()),
                };
                row.push(val);
            }
            rows.push(row);
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_class ───────────────────────────────────────────────────

    fn pg_class(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "oid".to_string(),
                "relname".to_string(),
                "relnamespace".to_string(),
                "relkind".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut items = Vec::new();
        for t in self.list_tables() {
            items.push((view_oid(&t.name), t.name.clone(), "r"));
        }
        for v in self.list_views() {
            items.push((view_oid(&v.name), v.name.clone(), "v"));
        }
        for idx in self.list_indexes() {
            items.push((view_oid(&idx.name), idx.name.clone(), "i"));
        }
        for (oid, name, kind) in items {
            let mut row = Vec::new();
            for c in &cols {
                let val = match c.as_str() {
                    "oid" => Some(oid.to_string()),
                    "relname" => Some(name.clone()),
                    "relnamespace" => Some("2200".to_string()),
                    "relkind" => Some(kind.to_string()),
                    "relhasrules" => Some("f".to_string()),
                    "relhastriggers" => Some("f".to_string()),
                    "relispartition" => Some("f".to_string()),
                    _ => {
                        if c.contains("id") {
                            Some("0".to_string())
                        } else if c.contains("has") || c.contains("is") {
                            Some("f".to_string())
                        } else {
                            Some("".to_string())
                        }
                    }
                };
                row.push(val);
            }
            rows.push(row);
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_attribute ──────────────────────────────────────────────

    fn pg_attribute(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() || requested_cols.contains(&"*".to_string()) {
            vec![
                "nspname".to_string(),
                "relname".to_string(),
                "attname".to_string(),
                "atttypid".to_string(),
                "attnotnull".to_string(),
                "atttypmod".to_string(),
                "attlen".to_string(),
                "typtypmod".to_string(),
                "attnum".to_string(),
                "attidentity".to_string(),
                "attgenerated".to_string(),
                "adsrc".to_string(),
                "description".to_string(),
                "typbasetype".to_string(),
                "typtype".to_string(),
                "format_type".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut items = Vec::new();
        for t in self.list_tables() {
            let oid = view_oid(&t.name);
            for (i, col) in t.columns.iter().enumerate() {
                items.push((
                    oid,
                    t.name.clone(),
                    col.name.clone(),
                    arrow_type_to_pg_oid(&col.data_type),
                    arrow_type_to_pg_data_type(&col.data_type),
                    i as i16 + 1,
                ));
            }
        }
        for v in self.list_views() {
            let oid = view_oid(&v.name);
            for (i, col) in v.columns.iter().enumerate() {
                items.push((
                    oid,
                    v.name.clone(),
                    col.name.clone(),
                    arrow_type_to_pg_oid(&col.data_type),
                    arrow_type_to_pg_data_type(&col.data_type),
                    i as i16 + 1,
                ));
            }
        }
        for (attrelid, relname, attname, atttypid, format_type, attnum) in items {
            let mut row = Vec::new();
            for c in &cols {
                let val = match c.as_str() {
                    "attrelid" => Some(attrelid.to_string()),
                    "relname" | "table_name" => Some(relname.clone()),
                    "attname" | "name" => Some(attname.clone()),
                    "atttypid" => Some(atttypid.to_string()),
                    "format_type" => Some(format_type.to_string()),
                    "attnum" => Some(attnum.to_string()),
                    "attnotnull" | "not_null" => Some("f".to_string()),
                    "atthasdef" | "default" => None,
                    "attidentity" | "identity_options" => None,
                    "attgenerated" | "generated" => Some("".to_string()),
                    "comment" => None,
                    "collation" => None,
                    "is_dropped" | "attisdropped" => Some("f".to_string()),
                    "nspname" => Some("public".to_string()),
                    "atttypmod" => Some("-1".to_string()),
                    "attlen" => Some("-1".to_string()),
                    "typtypmod" => Some("-1".to_string()),
                    "adsrc" => None,
                    "description" => None,
                    "typbasetype" => Some("0".to_string()),
                    "typtype" => Some("b".to_string()),
                    _ => {
                        if c.contains("id") {
                            Some("0".to_string())
                        } else if c.contains("has") || c.contains("is") {
                            Some("f".to_string())
                        } else {
                            Some("".to_string())
                        }
                    }
                };
                row.push(val);
            }
            rows.push(row);
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_namespace ──────────────────────────────────────────────

    fn pg_namespace(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec!["oid".to_string(), "nspname".to_string()]
        } else {
            requested_cols.to_vec()
        };
        let mut row = Vec::new();
        for c in &cols {
            let val = match c.as_str() {
                "oid" => Some("2200".to_string()),
                "nspname" => Some("public".to_string()),
                _ => Some("".to_string()),
            };
            row.push(val);
        }
        CatalogResponse::rows(cols, vec![row])
    }

    // ── pg_catalog.pg_type ───────────────────────────────────────────────────

    fn pg_type(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "oid".to_string(),
                "typname".to_string(),
                "typlen".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let type_rows = vec![
            (PG_OID_INT2, "int2", 2i32, 0, 11, "b"),
            (PG_OID_INT4, "int4", 4i32, 1007, 11, "b"),
            (PG_OID_INT8, "int8", 8, 1016, 11, "b"),
            (PG_OID_FLOAT4, "float4", 4, 0, 11, "b"),
            (PG_OID_FLOAT8, "float8", 8, 1022, 11, "b"),
            (PG_OID_TEXT, "text", -1, 1009, 11, "b"),
            (PG_OID_BOOL, "bool", 1, 1000, 11, "b"),
            (PG_OID_BYTEA, "bytea", -1, 0, 11, "b"),
            (PG_OID_TIMESTAMP, "timestamp", 8, 0, 11, "b"),
            (PG_OID_TIMESTAMPTZ, "timestamptz", 8, 0, 11, "b"),
            (PG_OID_DATE, "date", 4, 0, 11, "b"),
            (PG_OID_TIME, "time", 8, 0, 11, "b"),
            (PG_OID_UUID, "uuid", 16, 2951, 11, "b"),
            (PG_OID_NUMERIC, "numeric", -1, 0, 11, "b"),
            (PG_OID_JSON, "json", -1, 0, 11, "b"),
            (PG_OID_JSONB, "jsonb", -1, 0, 11, "b"),
            (PG_OID_VARCHAR, "varchar", -1, 0, 11, "b"),
            (PG_OID_CHAR, "bpchar", -1, 0, 11, "b"),
            (PG_OID_INTERVAL, "interval", 16, 0, 11, "b"),
            (PG_OID_ARRAY_INT4, "_int4", -1, 0, 11, "b"),
            (PG_OID_ARRAY_INT8, "_int8", -1, 0, 11, "b"),
            (PG_OID_ARRAY_TEXT, "_text", -1, 0, 11, "b"),
            (PG_OID_ARRAY_FLOAT8, "_float8", -1, 0, 11, "b"),
            (PG_OID_ARRAY_BOOL, "_bool", -1, 0, 11, "b"),
            (PG_OID_ARRAY_UUID, "_uuid", -1, 0, 11, "b"),
        ];
        let mut rows = Vec::new();
        for (oid, name, len, typarray, typnamespace, typtype) in type_rows {
            let mut row = Vec::new();
            for c in &cols {
                let val = match c.as_str() {
                    "oid" => Some(oid.to_string()),
                    "typname" | "domname" => Some(name.to_string()),
                    "typlen" => Some(len.to_string()),
                    "typarray" => Some(typarray.to_string()),
                    "typnamespace" => Some(typnamespace.to_string()),
                    "typtype" => Some(typtype.to_string()),
                    "attype" => Some(name.to_string()),
                    "typbasetype" => Some("0".to_string()),
                    "typtypmod" => Some("-1".to_string()),
                    _ => {
                        if c.contains("id") {
                            Some("0".to_string())
                        } else {
                            Some("".to_string())
                        }
                    }
                };
                row.push(val);
            }
            rows.push(row);
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── information_schema.tables ────────────────────────────────────────────

    fn information_schema_tables(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "table_catalog".to_string(),
                "table_schema".to_string(),
                "table_name".to_string(),
                "table_type".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let mut rows = Vec::new();
        let mut items = Vec::new();
        for t in self.list_tables() {
            items.push((t.name.clone(), "BASE TABLE"));
        }
        for v in self.list_views() {
            items.push((v.name.clone(), "VIEW"));
        }
        for (name, table_type) in items {
            let mut row = Vec::new();
            for c in &cols {
                let val = match c.as_str() {
                    "table_catalog" => Some("rockstream".to_string()),
                    "table_schema" => Some("public".to_string()),
                    "table_name" => Some(name.clone()),
                    "table_type" => Some(table_type.to_string()),
                    _ => Some("".to_string()),
                };
                row.push(val);
            }
            rows.push(row);
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── information_schema.columns ───────────────────────────────────────────

    fn information_schema_columns(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "table_catalog".to_string(),
                "table_schema".to_string(),
                "table_name".to_string(),
                "column_name".to_string(),
                "ordinal_position".to_string(),
                "data_type".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let mut rows = Vec::new();
        let mut items = Vec::new();
        for t in self.list_tables() {
            for (i, col) in t.columns.iter().enumerate() {
                items.push((
                    t.name.clone(),
                    col.name.clone(),
                    i + 1,
                    arrow_type_to_pg_data_type(&col.data_type),
                ));
            }
        }
        for v in self.list_views() {
            for (i, col) in v.columns.iter().enumerate() {
                items.push((
                    v.name.clone(),
                    col.name.clone(),
                    i + 1,
                    arrow_type_to_pg_data_type(&col.data_type),
                ));
            }
        }
        for (table_name, column_name, pos, data_type) in items {
            let mut row = Vec::new();
            for c in &cols {
                let val = match c.as_str() {
                    "table_catalog" => Some("rockstream".to_string()),
                    "table_schema" => Some("public".to_string()),
                    "table_name" => Some(table_name.clone()),
                    "column_name" => Some(column_name.clone()),
                    "ordinal_position" => Some(pos.to_string()),
                    "data_type" => Some(data_type.to_string()),
                    _ => Some("".to_string()),
                };
                row.push(val);
            }
            rows.push(row);
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_proc ────────────────────────────────────────────────────

    fn pg_proc(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "oid".to_string(),
                "proname".to_string(),
                "pronamespace".to_string(),
                "prorettype".to_string(),
                "proargtypes".to_string(),
                "prokind".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let raw_rows = vec![
            (2147, "count", 11, 20, "", "a"),
            (2108, "sum", 11, 20, "", "a"),
            (2101, "avg", 11, 1700, "", "a"),
            (2130, "min", 11, 25, "", "a"),
            (2115, "max", 11, 25, "", "a"),
        ];
        let mut rows = Vec::new();
        for (oid, proname, pronamespace, prorettype, proargtypes, prokind) in raw_rows {
            let mut row = Vec::new();
            for c in &cols {
                let val = match c.as_str() {
                    "oid" => Some(oid.to_string()),
                    "proname" => Some(proname.to_string()),
                    "pronamespace" => Some(pronamespace.to_string()),
                    "prorettype" => Some(prorettype.to_string()),
                    "proargtypes" => Some(proargtypes.to_string()),
                    "prokind" => Some(prokind.to_string()),
                    _ => Some("".to_string()),
                };
                row.push(val);
            }
            rows.push(row);
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_constraint ──────────────────────────────────────────────

    fn pg_constraint(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "oid".to_string(),
                "conname".to_string(),
                "connamespace".to_string(),
                "contype".to_string(),
                "conrelid".to_string(),
                "contypid".to_string(),
                "conindid".to_string(),
                "confrelid".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        CatalogResponse::rows(cols, Vec::new())
    }

    // ── pg_catalog.pg_index ───────────────────────────────────────────────────

    fn pg_index(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "indexrelid".to_string(),
                "indrelid".to_string(),
                "indisunique".to_string(),
                "indisprimary".to_string(),
                "indkey".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let mut rows = Vec::new();
        for idx in self.list_indexes() {
            let indexrelid = view_oid(&idx.name).to_string();
            let indrelid = view_oid(&idx.table).to_string();
            let mut indkey_vals = Vec::new();
            if let Some(t) = self.get_table(&idx.table) {
                for col_name in &idx.index_cols {
                    if let Some(pos) = t.columns.iter().position(|c| &c.name == col_name) {
                        indkey_vals.push((pos + 1).to_string());
                    }
                }
            } else if let Some(v) = self.get_view(&idx.table) {
                for col_name in &idx.index_cols {
                    if let Some(pos) = v.columns.iter().position(|c| &c.name == col_name) {
                        indkey_vals.push((pos + 1).to_string());
                    }
                }
            }
            if indkey_vals.is_empty() {
                indkey_vals.push("1".to_string());
            }
            let mut row = Vec::new();
            for c in &cols {
                let val = match c.as_str() {
                    "indexrelid" => Some(indexrelid.clone()),
                    "indrelid" => Some(indrelid.clone()),
                    "indisunique" => Some("f".to_string()),
                    "indisprimary" => Some("f".to_string()),
                    "indkey" => Some(indkey_vals.join(" ")),
                    _ => Some("".to_string()),
                };
                row.push(val);
            }
            rows.push(row);
        }
        CatalogResponse::rows(cols, rows)
    }

    // ── pg_catalog.pg_description ─────────────────────────────────────────────

    fn pg_description(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "objoid".to_string(),
                "classoid".to_string(),
                "objsubid".to_string(),
                "description".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        CatalogResponse::rows(cols, Vec::new())
    }

    // ── pg_catalog.pg_enum ────────────────────────────────────────────────────

    fn pg_enum(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "oid".to_string(),
                "enumtypid".to_string(),
                "enumsortorder".to_string(),
                "enumlabel".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        CatalogResponse::rows(cols, Vec::new())
    }

    // ── pg_catalog.pg_roles ───────────────────────────────────────────────────

    fn pg_roles(&self, principal_name: &str, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "oid".to_string(),
                "rolname".to_string(),
                "rolsuper".to_string(),
                "rolinherit".to_string(),
                "rolcreaterole".to_string(),
                "rolcreatedb".to_string(),
                "rolcanlogin".to_string(),
                "rolconnlimit".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let mut row = Vec::new();
        for c in &cols {
            let val = match c.as_str() {
                "oid" => Some(view_oid(principal_name).to_string()),
                "rolname" => Some(principal_name.to_string()),
                "rolsuper" => Some("t".to_string()),
                "rolinherit" => Some("t".to_string()),
                "rolcreaterole" => Some("t".to_string()),
                "rolcreatedb" => Some("t".to_string()),
                "rolcanlogin" => Some("t".to_string()),
                "rolconnlimit" => Some("-1".to_string()),
                _ => Some("".to_string()),
            };
            row.push(val);
        }
        CatalogResponse::rows(cols, vec![row])
    }

    // ── pg_catalog.pg_user ────────────────────────────────────────────────────

    fn pg_user(&self, principal_name: &str, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "usename".to_string(),
                "usesysid".to_string(),
                "usecreatedb".to_string(),
                "usesuper".to_string(),
                "usecatupd".to_string(),
                "valuntil".to_string(),
                "useconfig".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        let mut row = Vec::new();
        for c in &cols {
            let val = match c.as_str() {
                "usename" => Some(principal_name.to_string()),
                "usesysid" => Some(view_oid(principal_name).to_string()),
                "usecreatedb" => Some("t".to_string()),
                "usesuper" => Some("t".to_string()),
                "usecatupd" => Some("t".to_string()),
                "valuntil" => None,
                "useconfig" => None,
                _ => Some("".to_string()),
            };
            row.push(val);
        }
        CatalogResponse::rows(cols, vec![row])
    }

    // ── information_schema.key_column_usage ──────────────────────────────────

    fn key_column_usage(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "constraint_catalog".to_string(),
                "constraint_schema".to_string(),
                "constraint_name".to_string(),
                "table_catalog".to_string(),
                "table_schema".to_string(),
                "table_name".to_string(),
                "column_name".to_string(),
                "ordinal_position".to_string(),
                "position_in_unique_constraint".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        CatalogResponse::rows(cols, Vec::new())
    }

    // ── information_schema.referential_constraints ───────────────────────────

    fn referential_constraints(&self, requested_cols: &[String]) -> CatalogResponse {
        let cols = if requested_cols.is_empty() {
            vec![
                "constraint_catalog".to_string(),
                "constraint_schema".to_string(),
                "constraint_name".to_string(),
                "unique_constraint_catalog".to_string(),
                "unique_constraint_schema".to_string(),
                "unique_constraint_name".to_string(),
                "match_option".to_string(),
                "update_rule".to_string(),
                "delete_rule".to_string(),
            ]
        } else {
            requested_cols.to_vec()
        };
        CatalogResponse::rows(cols, Vec::new())
    }

    // ── pg_catalog functions ─────────────────────────────────────────────────

    fn version(&self) -> CatalogResponse {
        let cols = vec!["version".to_string()];
        let rows = vec![vec![Some(
            "PostgreSQL 14.9 (RockStream) on x86_64-unknown-linux-gnu, compiled by rustc, 64-bit"
                .to_string(),
        )]];
        CatalogResponse::rows(cols, rows)
    }

    fn current_schema(&self) -> CatalogResponse {
        let cols = vec!["current_schema".to_string()];
        let rows = vec![vec![Some("public".to_string())]];
        CatalogResponse::rows(cols, rows)
    }

    fn current_database(&self) -> CatalogResponse {
        let cols = vec!["current_database".to_string()];
        let rows = vec![vec![Some("rockstream".to_string())]];
        CatalogResponse::rows(cols, rows)
    }

    fn current_setting(&self, query: &str) -> CatalogResponse {
        let cols = vec!["current_setting".to_string()];
        let val = if query.contains("transaction_isolation") {
            "read committed"
        } else if query.contains("server_version_num") {
            "140000"
        } else if query.contains("standard_conforming_strings") {
            "on"
        } else {
            ""
        };
        let rows = vec![vec![Some(val.to_string())]];
        CatalogResponse::rows(cols, rows)
    }
}

/// Parse the second quoted string argument from `set_config('key', 'value', is_local)`.
fn parse_set_config_value(query: &str) -> Option<String> {
    let mut quote_count = 0u32;
    let mut in_quote = false;
    let mut collecting = false;
    let mut val = String::new();
    for c in query.chars() {
        if c == '\'' {
            if in_quote {
                if collecting {
                    return Some(val);
                }
                in_quote = false;
                quote_count += 1;
            } else {
                in_quote = true;
                if quote_count == 1 {
                    collecting = true;
                }
            }
        } else if collecting && in_quote {
            val.push(c);
        }
    }
    None
}

/// Format a SystemTime as ISO 8601 without chrono (YYYY-MM-DD HH:MM:SS+00).
fn format_system_time(t: SystemTime) -> String {
    let duration = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs();
    let secs_of_day = secs % 86400;
    let days = secs / 86400;
    let h = secs_of_day / 3600;
    let m = (secs_of_day % 3600) / 60;
    let s = secs_of_day % 60;
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}+00", y, mo, d, h, m, s)
}

/// Rata Die civil_from_days algorithm (no-dep calendar math).
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    (y as i32, mo as u32, d as u32)
}

/// Helper to parse SELECT column names/aliases from query.
fn parse_select_columns(query: &str) -> Vec<String> {
    let ql = query.to_lowercase();
    let select_idx = match ql.find("select") {
        Some(idx) => idx + 6,
        None => return Vec::new(),
    };
    // Find "from" at paren level 0
    let mut from_idx = None;
    let mut paren_level = 0;
    let chars: Vec<char> = query.chars().collect();
    let mut i = select_idx;
    while i < chars.len() {
        let c = chars[i];
        if c == '(' {
            paren_level += 1;
        } else if c == ')' {
            if paren_level > 0 {
                paren_level -= 1;
            }
        } else if c.is_alphabetic() && paren_level == 0 {
            if i + 4 <= chars.len() {
                let word: String = chars[i..i + 4].iter().collect();
                if word.eq_ignore_ascii_case("from") {
                    let prev_char = if i > 0 { chars[i - 1] } else { ' ' };
                    let next_char = if i + 4 < chars.len() {
                        chars[i + 4]
                    } else {
                        ' '
                    };
                    if !prev_char.is_alphanumeric() && !next_char.is_alphanumeric() {
                        from_idx = Some(i);
                        break;
                    }
                }
            }
        }
        i += 1;
    }

    let from_idx = match from_idx {
        Some(idx) => idx,
        None => return Vec::new(),
    };
    if from_idx <= select_idx {
        return Vec::new();
    }
    let select_part = &query[select_idx..from_idx];

    let mut parts = Vec::new();
    let mut current = String::new();
    let mut paren_level = 0;
    for c in select_part.chars() {
        if c == '(' {
            paren_level += 1;
            current.push(c);
        } else if c == ')' {
            if paren_level > 0 {
                paren_level -= 1;
            }
            current.push(c);
        } else if c == ',' && paren_level == 0 {
            parts.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(c);
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }

    let mut cols = Vec::new();
    for part in parts {
        // Normalize whitespace (newlines, tabs, extra spaces)
        let part_normalized = part.split_whitespace().collect::<Vec<&str>>().join(" ");
        let part_lower = part_normalized.to_lowercase();

        if let Some(as_idx) = part_lower.rfind(" as ") {
            let alias = part_normalized[as_idx + 4..]
                .trim()
                .trim_matches('"')
                .trim();
            cols.push(alias.to_string());
        } else {
            let words: Vec<&str> = part_normalized.split_whitespace().collect();
            if let Some(last_word) = words.last() {
                let last_word = last_word.trim_matches('"').trim();
                if let Some(dot_idx) = last_word.rfind('.') {
                    cols.push(last_word[dot_idx + 1..].to_string());
                } else {
                    cols.push(last_word.to_string());
                }
            }
        }
    }
    cols
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

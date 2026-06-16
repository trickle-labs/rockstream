//! Catalog stubs for `pg_catalog` and `information_schema`.
//!
//! Handles ORM and psql reflection queries against standard Postgres system
//! catalogs. All responses are synthesised from the in-memory view catalog.

use std::collections::HashMap;

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
        "Timestamp" | "TimestampMicrosecond" | "TimestampNanosecond" => "timestamp without time zone",
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
}

/// A column in a catalog view entry.
#[derive(Debug, Clone)]
pub struct CatalogColumn {
    pub name: String,
    /// Arrow data type name (e.g. "Int64", "Utf8").
    pub data_type: String,
}

/// In-memory catalog of views exposed to Postgres clients.
#[derive(Debug, Clone, Default)]
pub struct CatalogStubs {
    /// Keyed by view name.
    views: HashMap<String, CatalogView>,
}

impl CatalogStubs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a view in the catalog.
    pub fn add_view(&mut self, view: CatalogView) {
        self.views.insert(view.name.clone(), view);
    }

    /// List all registered views.
    pub fn list_views(&self) -> Vec<&CatalogView> {
        let mut v: Vec<&CatalogView> = self.views.values().collect();
        v.sort_by_key(|v| &v.name);
        v
    }

    /// Look up a view by name.
    pub fn get_view(&self, name: &str) -> Option<&CatalogView> {
        self.views.get(name)
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
        if ql.contains("pg_catalog.pg_tables") || ql.contains("pg_tables") && ql.contains("pg_catalog") {
            return Some(self.pg_tables());
        }
        if ql.contains("pg_catalog.pg_views") || (ql.contains("pg_views") && ql.contains("pg_catalog")) {
            return Some(self.pg_views());
        }
        if ql.contains("pg_catalog.pg_class") || (ql.contains("pg_class") && ql.contains("pg_catalog")) {
            return Some(self.pg_class());
        }
        if ql.contains("pg_catalog.pg_attribute") || (ql.contains("pg_attribute") && ql.contains("pg_catalog")) {
            return Some(self.pg_attribute());
        }
        if ql.contains("pg_catalog.pg_namespace") || (ql.contains("pg_namespace") && ql.contains("pg_catalog")) {
            return Some(self.pg_namespace());
        }
        if ql.contains("pg_catalog.pg_type") || (ql.contains("pg_type") && ql.contains("pg_catalog")) {
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
        let cols = vec!["oid".to_string(), "typname".to_string(), "typlen".to_string()];
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

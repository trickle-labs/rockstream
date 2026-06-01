//! Postgres system catalog stubs for the RockStream gateway (v0.40).
//!
//! Implements virtual table stubs for:
//!
//! - `pg_catalog.pg_type` — built-in type metadata (used by ORMs for type inference).
//! - `pg_catalog.pg_namespace` — schema/namespace metadata.
//! - `pg_catalog.pg_class` — relation (table/view) metadata.
//! - `information_schema.tables` — ANSI standard view listing.
//! - `information_schema.columns` — ANSI standard column listing.
//!
//! ## Why this matters
//!
//! When SQLAlchemy, JDBC, or `psql \d` introspects the database it runs
//! queries against `pg_catalog` and `information_schema`.  Without stubs
//! those queries return no rows, causing ORM connection errors ("table not
//! found") or missing schema warnings.
//!
//! By returning well-formed rows with correct OIDs and data types, clients
//! can reflect view schemas without errors.
//!
//! ## v0.40 proof criterion
//!
//! `proof_view_schema_reflects_without_orm_errors` verifies that:
//! - `information_schema.columns` returns a row for each registered view column.
//! - Column OIDs match the expected Postgres types.
//! - `information_schema.tables` lists the registered view.

use serde::{Deserialize, Serialize};

use crate::pgwire::{map_to_postgres_oid, PostgresOid};

// ─── pg_catalog.pg_type rows ──────────────────────────────────────────────────

/// A row from `pg_catalog.pg_type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgType {
    /// Type OID.
    pub oid: PostgresOid,
    /// Type name (e.g. "int4", "text").
    pub typname: String,
    /// Length in bytes; -1 for variable-length types.
    pub typlen: i16,
    /// Type category: 'b' = base, 'c' = composite, 'd' = domain, etc.
    pub typtype: char,
    /// Namespace OID (11 = pg_catalog).
    pub typnamespace: u32,
}

/// Return the built-in `pg_type` rows for types exposed by the gateway.
///
/// This is the minimal set needed to satisfy ORM type reflection.
pub fn pg_types() -> Vec<PgType> {
    vec![
        PgType {
            oid: 16,
            typname: "bool".into(),
            typlen: 1,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 21,
            typname: "int2".into(),
            typlen: 2,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 23,
            typname: "int4".into(),
            typlen: 4,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 20,
            typname: "int8".into(),
            typlen: 8,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 700,
            typname: "float4".into(),
            typlen: 4,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 701,
            typname: "float8".into(),
            typlen: 8,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 25,
            typname: "text".into(),
            typlen: -1,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 1043,
            typname: "varchar".into(),
            typlen: -1,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 17,
            typname: "bytea".into(),
            typlen: -1,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 1082,
            typname: "date".into(),
            typlen: 4,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 1114,
            typname: "timestamp".into(),
            typlen: 8,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 1184,
            typname: "timestamptz".into(),
            typlen: 8,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 2950,
            typname: "uuid".into(),
            typlen: 16,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 3802,
            typname: "jsonb".into(),
            typlen: -1,
            typtype: 'b',
            typnamespace: 11,
        },
        PgType {
            oid: 1700,
            typname: "numeric".into(),
            typlen: -1,
            typtype: 'b',
            typnamespace: 11,
        },
    ]
}

// ─── pg_catalog.pg_namespace rows ────────────────────────────────────────────

/// A row from `pg_catalog.pg_namespace`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgNamespace {
    /// Namespace OID.
    pub oid: u32,
    /// Schema name.
    pub nspname: String,
}

/// Return the built-in namespace rows exposed by the gateway.
pub fn pg_namespaces() -> Vec<PgNamespace> {
    vec![
        PgNamespace {
            oid: 11,
            nspname: "pg_catalog".into(),
        },
        PgNamespace {
            oid: 2200,
            nspname: "public".into(),
        },
        PgNamespace {
            oid: 99,
            nspname: "information_schema".into(),
        },
    ]
}

// ─── pg_catalog.pg_class rows ─────────────────────────────────────────────────

/// The relation kind character (matches `pg_class.relkind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelKind {
    /// Regular table ('r').
    Table,
    /// View ('v').
    View,
    /// Materialized view ('m').
    MaterializedView,
}

impl RelKind {
    pub fn as_char(self) -> char {
        match self {
            Self::Table => 'r',
            Self::View => 'v',
            Self::MaterializedView => 'm',
        }
    }
}

/// A row from `pg_catalog.pg_class`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgClass {
    /// Relation OID.
    pub oid: u32,
    /// Relation name.
    pub relname: String,
    /// Namespace OID.
    pub relnamespace: u32,
    /// Relation kind.
    pub relkind: RelKind,
}

// ─── information_schema.tables rows ──────────────────────────────────────────

/// A row from `information_schema.tables`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InformationSchemaTable {
    /// Catalog name (database name).
    pub table_catalog: String,
    /// Schema name.
    pub table_schema: String,
    /// Table/view name.
    pub table_name: String,
    /// "VIEW" or "BASE TABLE".
    pub table_type: String,
}

/// Generate `information_schema.tables` rows for the given views.
pub fn information_schema_tables(
    catalog: &str,
    schema: &str,
    view_names: &[&str],
) -> Vec<InformationSchemaTable> {
    view_names
        .iter()
        .map(|name| InformationSchemaTable {
            table_catalog: catalog.to_string(),
            table_schema: schema.to_string(),
            table_name: name.to_string(),
            table_type: "VIEW".to_string(),
        })
        .collect()
}

// ─── information_schema.columns rows ─────────────────────────────────────────

/// A row from `information_schema.columns`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InformationSchemaColumn {
    /// Catalog name.
    pub table_catalog: String,
    /// Schema name.
    pub table_schema: String,
    /// Table/view name.
    pub table_name: String,
    /// Column name.
    pub column_name: String,
    /// 1-based ordinal position.
    pub ordinal_position: u32,
    /// ANSI data type string (e.g. "integer", "text", "boolean").
    pub data_type: String,
    /// Postgres OID for the column type (extension beyond ANSI standard).
    pub udt_oid: PostgresOid,
    /// Whether the column is nullable ("YES" or "NO").
    pub is_nullable: String,
}

/// Map a RockStream type tag to an ANSI `data_type` string.
pub fn ansi_type_name(type_tag: u8) -> &'static str {
    match type_tag {
        1 => "boolean",
        2 => "integer",
        3 => "bigint",
        4 => "double precision",
        5 => "text",
        6 => "character varying",
        7 => "bytea",
        8 => "date",
        9 => "timestamp without time zone",
        10 => "uuid",
        11 => "jsonb",
        12 => "numeric",
        13 => "counter",
        14 => "max_register",
        15 => "min_register",
        16 => "lww",
        _ => "unknown",
    }
}

/// A column descriptor for building information_schema rows.
#[derive(Debug, Clone)]
pub struct ColumnSpec<'a> {
    pub name: &'a str,
    pub type_tag: u8,
    pub nullable: bool,
}

/// Generate `information_schema.columns` rows for a single view.
pub fn information_schema_columns<'a>(
    catalog: &str,
    schema: &str,
    view_name: &str,
    columns: &[ColumnSpec<'a>],
) -> Vec<InformationSchemaColumn> {
    columns
        .iter()
        .enumerate()
        .map(|(i, col)| InformationSchemaColumn {
            table_catalog: catalog.to_string(),
            table_schema: schema.to_string(),
            table_name: view_name.to_string(),
            column_name: col.name.to_string(),
            ordinal_position: (i + 1) as u32,
            data_type: ansi_type_name(col.type_tag).to_string(),
            udt_oid: map_to_postgres_oid(col.type_tag),
            is_nullable: if col.nullable { "YES" } else { "NO" }.to_string(),
        })
        .collect()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgwire::oid;

    #[test]
    fn pg_types_contains_common_types() {
        let types = pg_types();
        let oids: Vec<u32> = types.iter().map(|t| t.oid).collect();
        assert!(oids.contains(&16), "bool must be present");
        assert!(oids.contains(&23), "int4 must be present");
        assert!(oids.contains(&25), "text must be present");
        assert!(oids.contains(&1114), "timestamp must be present");
    }

    #[test]
    fn pg_namespaces_contains_pg_catalog_and_public() {
        let ns = pg_namespaces();
        let names: Vec<&str> = ns.iter().map(|n| n.nspname.as_str()).collect();
        assert!(names.contains(&"pg_catalog"));
        assert!(names.contains(&"public"));
        assert!(names.contains(&"information_schema"));
    }

    #[test]
    fn information_schema_tables_returns_view_entries() {
        let rows = information_schema_tables("rockstream", "public", &["orders_v", "items_v"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].table_name, "orders_v");
        assert_eq!(rows[0].table_type, "VIEW");
        assert_eq!(rows[1].table_name, "items_v");
    }

    /// Proof: view schema reflects without ORM errors.
    ///
    /// SQLAlchemy and psql run `information_schema.columns` queries to reflect
    /// view schemas.  We prove that the returned rows have:
    /// - Correct column names in ordinal order.
    /// - Correct ANSI data type strings.
    /// - Correct Postgres OIDs (used by the psql client for display).
    /// - No "unknown" types (which would cause ORM errors).
    #[test]
    fn proof_view_schema_reflects_without_orm_errors() {
        let columns = vec![
            ColumnSpec {
                name: "order_id",
                type_tag: 3,
                nullable: false,
            }, // INT8
            ColumnSpec {
                name: "customer",
                type_tag: 5,
                nullable: false,
            }, // TEXT
            ColumnSpec {
                name: "amount",
                type_tag: 4,
                nullable: true,
            }, // FLOAT8
            ColumnSpec {
                name: "created_at",
                type_tag: 9,
                nullable: false,
            }, // TIMESTAMP
        ];

        let rows = information_schema_columns("rockstream", "public", "orders_v", &columns);

        assert_eq!(rows.len(), 4, "must have 4 column rows");

        // Verify ordinal positions are 1-based and sequential.
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.ordinal_position, (i + 1) as u32);
            assert_eq!(row.table_name, "orders_v");
            // No column must have "unknown" type — that would cause ORM errors.
            assert_ne!(
                row.data_type, "unknown",
                "column '{}' must not have unknown type",
                row.column_name
            );
            assert_ne!(
                row.udt_oid,
                oid::UNKNOWN,
                "column '{}' must not have UNKNOWN OID",
                row.column_name
            );
        }

        assert_eq!(rows[0].column_name, "order_id");
        assert_eq!(rows[0].data_type, "bigint");
        assert_eq!(rows[0].udt_oid, oid::INT8);
        assert_eq!(rows[0].is_nullable, "NO");

        assert_eq!(rows[1].column_name, "customer");
        assert_eq!(rows[1].data_type, "text");
        assert_eq!(rows[1].udt_oid, oid::TEXT);

        assert_eq!(rows[2].column_name, "amount");
        assert_eq!(rows[2].data_type, "double precision");
        assert_eq!(rows[2].udt_oid, oid::FLOAT8);
        assert_eq!(rows[2].is_nullable, "YES");

        assert_eq!(rows[3].column_name, "created_at");
        assert_eq!(rows[3].data_type, "timestamp without time zone");
        assert_eq!(rows[3].udt_oid, oid::TIMESTAMP);
    }

    #[test]
    fn ansi_type_name_covers_all_common_tags() {
        for tag in 1u8..=12 {
            assert_ne!(
                ansi_type_name(tag),
                "unknown",
                "type tag {tag} must have a known ANSI name"
            );
        }
    }
}

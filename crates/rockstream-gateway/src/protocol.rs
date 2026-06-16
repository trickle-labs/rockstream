//! Protocol constants and helpers for the PostgreSQL wire layer.

/// Postgres type OID → `pgwire::api::Type` mapping.
///
/// Mirrors the OIDs defined in `catalog_stubs` so both sides stay in sync.
pub fn pg_type_from_name(arrow_type: &str) -> pgwire::api::Type {
    use crate::catalog_stubs::{
        PG_OID_BOOL, PG_OID_BYTEA, PG_OID_FLOAT8, PG_OID_INT4, PG_OID_INT8,
        PG_OID_TIMESTAMP,
    };
    use pgwire::api::Type;
    let oid = crate::catalog_stubs::arrow_type_to_pg_oid(arrow_type);
    match oid {
        x if x == PG_OID_INT4 => Type::INT4,
        x if x == PG_OID_INT8 => Type::INT8,
        x if x == PG_OID_FLOAT8 => Type::FLOAT8,
        x if x == PG_OID_BOOL => Type::BOOL,
        x if x == PG_OID_BYTEA => Type::BYTEA,
        x if x == PG_OID_TIMESTAMP => Type::TIMESTAMP,
        _ => Type::TEXT,
    }
}

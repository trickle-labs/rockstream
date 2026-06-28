//! Protocol constants and helpers for the PostgreSQL wire layer.

/// Postgres type OID → `pgwire::api::Type` mapping.
///
/// Mirrors the OIDs defined in `catalog_stubs` so both sides stay in sync.
pub fn pg_type_from_name(arrow_type: &str) -> pgwire::api::Type {
    use crate::catalog_stubs::{
        PG_OID_ARRAY_BOOL, PG_OID_ARRAY_FLOAT8, PG_OID_ARRAY_INT4, PG_OID_ARRAY_INT8,
        PG_OID_ARRAY_TEXT, PG_OID_ARRAY_UUID, PG_OID_BOOL, PG_OID_BYTEA, PG_OID_CHAR, PG_OID_DATE,
        PG_OID_FLOAT4, PG_OID_FLOAT8, PG_OID_INT2, PG_OID_INT4, PG_OID_INT8, PG_OID_INTERVAL,
        PG_OID_JSON, PG_OID_JSONB, PG_OID_NUMERIC, PG_OID_TIME, PG_OID_TIMESTAMP,
        PG_OID_TIMESTAMPTZ, PG_OID_UUID, PG_OID_VARCHAR,
    };
    use pgwire::api::Type;
    let oid = crate::catalog_stubs::arrow_type_to_pg_oid(arrow_type);
    match oid {
        x if x == PG_OID_INT2 => Type::INT2,
        x if x == PG_OID_INT4 => Type::INT4,
        x if x == PG_OID_INT8 => Type::INT8,
        x if x == PG_OID_FLOAT4 => Type::FLOAT4,
        x if x == PG_OID_FLOAT8 => Type::FLOAT8,
        x if x == PG_OID_BOOL => Type::BOOL,
        x if x == PG_OID_BYTEA => Type::BYTEA,
        x if x == PG_OID_TIMESTAMP => Type::TIMESTAMP,
        x if x == PG_OID_TIMESTAMPTZ => Type::TIMESTAMPTZ,
        x if x == PG_OID_DATE => Type::DATE,
        x if x == PG_OID_TIME => Type::TIME,
        x if x == PG_OID_UUID => Type::UUID,
        x if x == PG_OID_NUMERIC => Type::NUMERIC,
        x if x == PG_OID_JSON => Type::JSON,
        x if x == PG_OID_JSONB => Type::JSONB,
        x if x == PG_OID_VARCHAR => Type::VARCHAR,
        x if x == PG_OID_CHAR => Type::CHAR,
        x if x == PG_OID_INTERVAL => Type::INTERVAL,
        x if x == PG_OID_ARRAY_INT4 => Type::INT4_ARRAY,
        x if x == PG_OID_ARRAY_INT8 => Type::INT8_ARRAY,
        x if x == PG_OID_ARRAY_TEXT => Type::TEXT_ARRAY,
        x if x == PG_OID_ARRAY_FLOAT8 => Type::FLOAT8_ARRAY,
        x if x == PG_OID_ARRAY_BOOL => Type::BOOL_ARRAY,
        x if x == PG_OID_ARRAY_UUID => Type::UUID_ARRAY,
        _ => Type::TEXT,
    }
}

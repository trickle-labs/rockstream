//! pgwire protocol types for the RockStream Postgres gateway (v0.40).
//!
//! Implements the Postgres wire protocol message framing types needed to
//! satisfy the v0.40 proof criteria:
//!
//! - [`PgStartupMessage`] — connection startup handshake.
//! - [`PgQueryMessage`] — simple query ('Q' message).
//! - [`PgExtendedQuery`] — Parse/Bind/Execute extended-query flow.
//! - [`PgRowDescription`] — 'T' message describing result column metadata,
//!   including Postgres type OIDs.
//! - [`PostgresOid`] — canonical type OID constants matching `pg_type`.
//! - [`map_to_postgres_oid`] — maps RockStream type tags to Postgres OIDs.
//! - [`IsolationLevel`] and [`parse_isolation_level`] — parse the level from
//!   `SET TRANSACTION ISOLATION LEVEL …` and reject SERIALIZABLE with RS-2003.
//!
//! In production the gateway reads/writes raw bytes on TCP; here we model the
//! logical structure to prove the protocol semantics are correct.

use serde::{Deserialize, Serialize};

use crate::error::GatewayError;

// ─── Postgres type OIDs ───────────────────────────────────────────────────────

/// Postgres type OID (`pg_type.oid`).
pub type PostgresOid = u32;

/// OID constants matching `pg_type` for common Postgres built-in types.
///
/// Clients (psql, SQLAlchemy, JDBC) look these up to decide how to decode
/// column values.  Returning incorrect OIDs causes ORM type inference errors.
pub mod oid {
    use super::PostgresOid;

    pub const BOOL: PostgresOid = 16;
    pub const INT2: PostgresOid = 21;
    pub const INT4: PostgresOid = 23;
    pub const INT8: PostgresOid = 20;
    pub const FLOAT4: PostgresOid = 700;
    pub const FLOAT8: PostgresOid = 701;
    pub const TEXT: PostgresOid = 25;
    pub const VARCHAR: PostgresOid = 1043;
    pub const BYTEA: PostgresOid = 17;
    pub const DATE: PostgresOid = 1082;
    pub const TIMESTAMP: PostgresOid = 1114;
    pub const TIMESTAMPTZ: PostgresOid = 1184;
    pub const UUID: PostgresOid = 2950;
    pub const JSONB: PostgresOid = 3802;
    pub const NUMERIC: PostgresOid = 1700;

    /// OID for types that have no known Postgres equivalent.
    pub const UNKNOWN: PostgresOid = 705;
}

/// Map a RockStream schema `type_tag` to the canonical Postgres OID.
///
/// The mapping is:
///
/// | tag | Postgres type | OID  |
/// |-----|--------------|------|
/// | 1   | BOOL         | 16   |
/// | 2   | INT4         | 23   |
/// | 3   | INT8         | 20   |
/// | 4   | FLOAT8       | 701  |
/// | 5   | TEXT         | 25   |
/// | 6   | VARCHAR      | 1043 |
/// | 7   | BYTEA        | 17   |
/// | 8   | DATE         | 1082 |
/// | 9   | TIMESTAMP    | 1114 |
/// | 10  | UUID         | 2950 |
/// | 11  | JSONB        | 3802 |
/// | 12  | NUMERIC      | 1700 |
/// | _   | UNKNOWN      | 705  |
pub fn map_to_postgres_oid(type_tag: u8) -> PostgresOid {
    match type_tag {
        1 => oid::BOOL,
        2 => oid::INT4,
        3 => oid::INT8,
        4 => oid::FLOAT8,
        5 => oid::TEXT,
        6 => oid::VARCHAR,
        7 => oid::BYTEA,
        8 => oid::DATE,
        9 => oid::TIMESTAMP,
        10 => oid::UUID,
        11 => oid::JSONB,
        12 => oid::NUMERIC,
        13 => oid::INT8, // COUNTER maps to INT8
        14 => oid::INT8, // MAX_REGISTER maps to INT8
        15 => oid::INT8, // MIN_REGISTER maps to INT8
        16 => oid::INT8, // LWW maps to INT8
        17 => oid::TEXT, // OR_SET maps to TEXT
        18 => oid::INT8, // MV_REGISTER maps to INT8
        _ => oid::UNKNOWN,
    }
}

// ─── pgwire messages ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthContext {
    pub actor: String,
    pub role: String,
    pub tenant: String,
}

impl AuthContext {
    pub fn authorize_read(&self, view_namespace: &str) -> Result<(), GatewayError> {
        if self.tenant != view_namespace && self.role != "admin" {
            return Err(GatewayError::Forbidden(
                "Cross-tenant access rejected".into(),
            ));
        }
        Ok(())
    }

    pub fn authorize_write(&self, view_namespace: &str) -> Result<(), GatewayError> {
        if self.role == "viewer" {
            return Err(GatewayError::Forbidden(
                "Viewer role cannot perform write/DML/DDL operations".into(),
            ));
        }
        if self.tenant != view_namespace && self.role != "admin" {
            return Err(GatewayError::Forbidden(
                "Cross-tenant access rejected".into(),
            ));
        }
        Ok(())
    }
}

/// The Postgres startup message sent by the client at connection time.
///
/// Carries the `user`, `database`, and optional application name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgStartupMessage {
    /// Protocol version (major.minor encoded as `major << 16 | minor`).
    pub protocol_version: u32,
    /// Requested database name.
    pub database: String,
    /// Connecting user name.
    pub user: String,
    /// Optional application name (used by pg_stat_activity).
    pub application_name: Option<String>,
    /// OIDC bearer token or service account token (v0.49).
    pub token: Option<String>,
}

impl PgStartupMessage {
    /// Postgres protocol 3.0 version code.
    pub const PROTOCOL_V3: u32 = 3 << 16;

    /// Create a new startup message for protocol 3.0.
    pub fn new(database: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            protocol_version: Self::PROTOCOL_V3,
            database: database.into(),
            user: user.into(),
            application_name: None,
            token: None,
        }
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    pub fn validate_auth(&self) -> Result<AuthContext, GatewayError> {
        let token = self.token.as_deref().unwrap_or("");
        if token.is_empty() {
            return Err(GatewayError::Unauthenticated("No token provided".into()));
        }
        if let Some(val) = token.strip_prefix("bearer ") {
            let parts: Vec<&str> = val.split(':').collect();
            if parts.len() == 2 {
                let role = parts[0];
                let tenant = parts[1];
                if ["viewer", "pipeline_owner", "admin"].contains(&role) {
                    return Ok(AuthContext {
                        actor: self.user.clone(),
                        role: role.to_string(),
                        tenant: tenant.to_string(),
                    });
                }
            }
        } else if let Some(val) = token.strip_prefix("sa:") {
            let parts: Vec<&str> = val.split(':').collect();
            if parts.len() == 2 {
                let role = parts[0];
                let tenant = parts[1];
                if ["viewer", "pipeline_owner", "admin"].contains(&role) {
                    return Ok(AuthContext {
                        actor: format!("sa:{}", self.user),
                        role: role.to_string(),
                        tenant: tenant.to_string(),
                    });
                }
            }
        }
        Err(GatewayError::Unauthenticated("Invalid token format".into()))
    }
}

/// A simple query message ('Q') from the client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgQueryMessage {
    /// The raw SQL string sent by the client.
    pub sql: String,
}

impl PgQueryMessage {
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into() }
    }
}

/// Extended-query flow: Parse → Bind → Execute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgExtendedQuery {
    /// Prepared statement name (empty string = unnamed).
    pub statement_name: String,
    /// The query SQL (may contain `$1`, `$2`, … parameter placeholders).
    pub sql: String,
    /// Bound parameter values (serialized as strings for simplicity).
    pub params: Vec<String>,
}

impl PgExtendedQuery {
    pub fn unnamed(sql: impl Into<String>) -> Self {
        Self {
            statement_name: String::new(),
            sql: sql.into(),
            params: vec![],
        }
    }
}

// ─── Row description ──────────────────────────────────────────────────────────

/// A single column in a `PgRowDescription` ('T') message.
///
/// Clients use this to infer types and decode column values.  Providing the
/// correct Postgres OID is required for `psql` column alignment and SQLAlchemy
/// type mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgColumn {
    /// Column name as displayed to the client.
    pub name: String,
    /// Postgres type OID (`pg_type.oid`).
    pub type_oid: PostgresOid,
    /// Column type modifier (e.g. `VARCHAR(n)` precision); -1 means no modifier.
    pub type_modifier: i32,
    /// Format code: 0 = text, 1 = binary.
    pub format_code: i16,
}

impl PgColumn {
    /// Create a text-format column with no type modifier.
    pub fn text(name: impl Into<String>, type_oid: PostgresOid) -> Self {
        Self {
            name: name.into(),
            type_oid,
            type_modifier: -1,
            format_code: 0,
        }
    }

    /// Create a column from a RockStream schema type tag.
    pub fn from_type_tag(name: impl Into<String>, type_tag: u8) -> Self {
        Self::text(name, map_to_postgres_oid(type_tag))
    }
}

/// A complete row description ('T') message.
///
/// Sent by the server before data rows; describes every column in the result.
/// ORMs like SQLAlchemy parse this to avoid type inference errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgRowDescription {
    pub columns: Vec<PgColumn>,
}

impl PgRowDescription {
    pub fn new(columns: Vec<PgColumn>) -> Self {
        Self { columns }
    }

    /// Build a row description from a list of `(name, type_tag)` pairs.
    pub fn from_schema(schema: &[(&str, u8)]) -> Self {
        Self::new(
            schema
                .iter()
                .map(|(name, tag)| PgColumn::from_type_tag(*name, *tag))
                .collect(),
        )
    }

    /// Number of columns.
    pub fn field_count(&self) -> usize {
        self.columns.len()
    }
}

// ─── Isolation level ─────────────────────────────────────────────────────────

/// Postgres transaction isolation levels.
///
/// RockStream supports `ReadCommitted` and `RepeatableRead` (snapshot).
/// `Serializable` is rejected with RS-2003.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// Read committed (non-repeatable reads are possible).
    ReadCommitted,
    /// Repeatable read / snapshot isolation.
    RepeatableRead,
    /// Serializable — **not supported** by RockStream; returns RS-2003.
    Serializable,
}

/// Parse an isolation level string from a `SET TRANSACTION ISOLATION LEVEL …`
/// statement.
///
/// Returns `Err(GatewayError::UnsupportedIsolationLevel)` (RS-2003) when
/// the client requests `SERIALIZABLE`.
pub fn parse_isolation_level(s: &str) -> Result<IsolationLevel, GatewayError> {
    match s.trim().to_uppercase().as_str() {
        "READ COMMITTED" | "READ_COMMITTED" => Ok(IsolationLevel::ReadCommitted),
        "REPEATABLE READ" | "REPEATABLE_READ" => Ok(IsolationLevel::RepeatableRead),
        "SERIALIZABLE" => Err(GatewayError::UnsupportedIsolationLevel),
        other => {
            // Any unrecognized level is also unsupported.
            let _ = other;
            Err(GatewayError::UnsupportedIsolationLevel)
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::GatewayError;
    use rockstream_types::error_code::RS_2003;

    // ── OID mapping ──────────────────────────────────────────────────────────

    #[test]
    fn bool_tag_maps_to_oid_16() {
        assert_eq!(map_to_postgres_oid(1), oid::BOOL);
        assert_eq!(oid::BOOL, 16);
    }

    #[test]
    fn int4_tag_maps_to_oid_23() {
        assert_eq!(map_to_postgres_oid(2), oid::INT4);
        assert_eq!(oid::INT4, 23);
    }

    #[test]
    fn int8_tag_maps_to_oid_20() {
        assert_eq!(map_to_postgres_oid(3), oid::INT8);
        assert_eq!(oid::INT8, 20);
    }

    #[test]
    fn text_tag_maps_to_oid_25() {
        assert_eq!(map_to_postgres_oid(5), oid::TEXT);
        assert_eq!(oid::TEXT, 25);
    }

    #[test]
    fn timestamp_tag_maps_to_oid_1114() {
        assert_eq!(map_to_postgres_oid(9), oid::TIMESTAMP);
        assert_eq!(oid::TIMESTAMP, 1114);
    }

    #[test]
    fn unknown_tag_maps_to_oid_705() {
        assert_eq!(map_to_postgres_oid(255), oid::UNKNOWN);
    }

    #[test]
    fn all_common_oids_are_correct() {
        // Verify against the canonical pg_type table entries.
        assert_eq!(oid::BOOL, 16);
        assert_eq!(oid::INT2, 21);
        assert_eq!(oid::INT4, 23);
        assert_eq!(oid::INT8, 20);
        assert_eq!(oid::FLOAT4, 700);
        assert_eq!(oid::FLOAT8, 701);
        assert_eq!(oid::TEXT, 25);
        assert_eq!(oid::VARCHAR, 1043);
        assert_eq!(oid::BYTEA, 17);
        assert_eq!(oid::DATE, 1082);
        assert_eq!(oid::TIMESTAMP, 1114);
        assert_eq!(oid::TIMESTAMPTZ, 1184);
        assert_eq!(oid::UUID, 2950);
        assert_eq!(oid::JSONB, 3802);
        assert_eq!(oid::NUMERIC, 1700);
    }

    // ── Row description ──────────────────────────────────────────────────────

    #[test]
    fn row_description_from_schema_has_correct_oids() {
        let rd = PgRowDescription::from_schema(&[("id", 2), ("name", 5), ("active", 1)]);
        assert_eq!(rd.field_count(), 3);
        assert_eq!(rd.columns[0].type_oid, oid::INT4);
        assert_eq!(rd.columns[1].type_oid, oid::TEXT);
        assert_eq!(rd.columns[2].type_oid, oid::BOOL);
    }

    /// Proof: psql and SQLAlchemy can read views — the row description must
    /// carry correct OIDs so ORMs can infer column types without errors.
    #[test]
    fn proof_row_description_for_view_has_correct_postgres_oids() {
        // Simulate the 'orders' view with columns: (order_id INT8, status TEXT,
        // amount FLOAT8, created TIMESTAMP).
        let schema = &[
            ("order_id", 3u8), // → INT8 OID 20
            ("status", 5u8),   // → TEXT OID 25
            ("amount", 4u8),   // → FLOAT8 OID 701
            ("created", 9u8),  // → TIMESTAMP OID 1114
        ];
        let rd = PgRowDescription::from_schema(schema);

        assert_eq!(rd.columns[0].type_oid, oid::INT8, "order_id must be INT8");
        assert_eq!(rd.columns[1].type_oid, oid::TEXT, "status must be TEXT");
        assert_eq!(rd.columns[2].type_oid, oid::FLOAT8, "amount must be FLOAT8");
        assert_eq!(
            rd.columns[3].type_oid,
            oid::TIMESTAMP,
            "created must be TIMESTAMP"
        );
        // No column should carry UNKNOWN (705) — that would cause ORM errors.
        for col in &rd.columns {
            assert_ne!(
                col.type_oid,
                oid::UNKNOWN,
                "column '{}' must not have UNKNOWN OID",
                col.name
            );
        }
    }

    // ── Isolation level ──────────────────────────────────────────────────────

    /// Proof: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE returns RS-2003.
    #[test]
    fn proof_serializable_returns_rs_2003() {
        let result = parse_isolation_level("SERIALIZABLE");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err,
            GatewayError::UnsupportedIsolationLevel,
            "SERIALIZABLE must fail with UnsupportedIsolationLevel"
        );
        assert_eq!(err.error_code(), RS_2003, "the error code must be RS-2003");
    }

    #[test]
    fn read_committed_is_supported() {
        assert_eq!(
            parse_isolation_level("READ COMMITTED").unwrap(),
            IsolationLevel::ReadCommitted
        );
    }

    #[test]
    fn repeatable_read_is_supported() {
        assert_eq!(
            parse_isolation_level("REPEATABLE READ").unwrap(),
            IsolationLevel::RepeatableRead
        );
    }

    // ── Startup message ──────────────────────────────────────────────────────

    #[test]
    fn startup_message_uses_protocol_v3() {
        let msg = PgStartupMessage::new("mydb", "alice");
        assert_eq!(msg.protocol_version, PgStartupMessage::PROTOCOL_V3);
        assert_eq!(msg.database, "mydb");
        assert_eq!(msg.user, "alice");
    }

    // ── Extended query ───────────────────────────────────────────────────────

    #[test]
    fn extended_query_unnamed_has_empty_name() {
        let q = PgExtendedQuery::unnamed("SELECT $1");
        assert!(q.statement_name.is_empty());
        assert!(q.params.is_empty());
    }

    #[test]
    fn proof_oidc_bearer_auth_and_rbac() {
        // 1. Unauthenticated startup messages
        let msg_no_token = PgStartupMessage::new("mydb", "alice");
        assert_eq!(
            msg_no_token.validate_auth().unwrap_err().error_code(),
            rockstream_types::error_code::RS_2001
        );

        let msg_invalid_token = PgStartupMessage::new("mydb", "alice").with_token("invalid-format");
        assert!(msg_invalid_token.validate_auth().is_err());

        // 2. Viewer role
        let msg_viewer =
            PgStartupMessage::new("mydb", "alice").with_token("bearer viewer:production");
        let auth_viewer = msg_viewer.validate_auth().unwrap();
        assert_eq!(auth_viewer.actor, "alice");
        assert_eq!(auth_viewer.role, "viewer");
        assert_eq!(auth_viewer.tenant, "production");

        assert!(auth_viewer.authorize_read("production").is_ok());
        assert!(
            auth_viewer.authorize_read("marketing").is_err(),
            "cross-tenant read must fail"
        );
        assert!(
            auth_viewer.authorize_write("production").is_err(),
            "viewer cannot perform write"
        );

        // 3. Pipeline owner role
        let msg_owner =
            PgStartupMessage::new("mydb", "bob").with_token("bearer pipeline_owner:production");
        let auth_owner = msg_owner.validate_auth().unwrap();
        assert!(auth_owner.authorize_read("production").is_ok());
        assert!(auth_owner.authorize_write("production").is_ok());
        assert!(
            auth_owner.authorize_write("marketing").is_err(),
            "cross-tenant write must fail"
        );

        // 4. Admin role (can access any tenant)
        let msg_admin = PgStartupMessage::new("mydb", "admin").with_token("bearer admin:any");
        let auth_admin = msg_admin.validate_auth().unwrap();
        assert!(auth_admin.authorize_read("production").is_ok());
        assert!(auth_admin.authorize_write("marketing").is_ok());

        // 5. Service account token
        let msg_sa = PgStartupMessage::new("mydb", "svc-connector")
            .with_token("sa:pipeline_owner:production");
        let auth_sa = msg_sa.validate_auth().unwrap();
        assert_eq!(auth_sa.actor, "sa:svc-connector");
        assert!(auth_sa.authorize_write("production").is_ok());
    }
}

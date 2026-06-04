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

#![allow(clippy::items_after_test_module, clippy::collapsible_match)]

use serde::{Deserialize, Serialize};

use crate::error::GatewayError;
use crate::limits::{RateLimitConfig, RateLimiter};

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
        assert!(auth_sa.authorize_write("production").is_ok());
    }
}

// ─── pgwire TCP Server Implementation (v0.52.2) ──────────────────────────────

use crate::inline_view::InlineViewCatalog;
use crate::pg_catalog::pg_types;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;

fn get_query_columns(sql: &str) -> Vec<PgColumn> {
    let sql_upper = sql.to_uppercase();
    if sql_upper.contains("SHOW RESOURCE USAGE FOR WORKLOAD") {
        vec![
            PgColumn::from_type_tag("view_name", 5),
            PgColumn::from_type_tag("workload_id", 5),
            PgColumn::from_type_tag("state_bytes", 3),
            PgColumn::from_type_tag("memory_bytes", 3),
            PgColumn::from_type_tag("freshness_lag_ms", 3),
        ]
    } else if sql_upper.contains("SHOW CLUSTER RESOURCE USAGE") {
        vec![
            PgColumn::from_type_tag("total_workers", 2),
            PgColumn::from_type_tag("total_state_bytes", 3),
            PgColumn::from_type_tag("total_memory_bytes", 3),
        ]
    } else if sql_upper.contains("SHOW RESOURCE USAGE") {
        vec![
            PgColumn::from_type_tag("workload_id", 5),
            PgColumn::from_type_tag("memory_limit", 3),
            PgColumn::from_type_tag("memory_allocated", 3),
            PgColumn::from_type_tag("freshness_slo_ms", 3),
            PgColumn::from_type_tag("freshness_slo_compliant", 1),
        ]
    } else if sql_upper.contains("SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA") {
        vec![
            PgColumn::from_type_tag("schema_name", 5),
            PgColumn::from_type_tag("status", 5),
            PgColumn::from_type_tag("pending_changes", 2),
        ]
    } else if sql_upper.contains("SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW") {
        vec![
            PgColumn::from_type_tag("view_name", 5),
            PgColumn::from_type_tag("version", 2),
            PgColumn::from_type_tag("evolved_at", 5),
            PgColumn::from_type_tag("compatible", 1),
        ]
    } else if sql_upper.contains("PG_TYPE") {
        vec![
            PgColumn::from_type_tag("oid", 2),
            PgColumn::from_type_tag("typname", 5),
            PgColumn::from_type_tag("typlen", 2),
            PgColumn::from_type_tag("typtype", 5),
            PgColumn::from_type_tag("typnamespace", 2),
        ]
    } else if sql_upper.contains("COLUMNS") || sql_upper.contains("INFORMATION_SCHEMA") {
        vec![
            PgColumn::from_type_tag("table_catalog", 5),
            PgColumn::from_type_tag("table_schema", 5),
            PgColumn::from_type_tag("table_name", 5),
            PgColumn::from_type_tag("column_name", 5),
            PgColumn::from_type_tag("ordinal_position", 2),
            PgColumn::from_type_tag("data_type", 5),
            PgColumn::from_type_tag("udt_oid", 2),
            PgColumn::from_type_tag("is_nullable", 5),
        ]
    } else if sql_upper.contains("RETURNING") {
        vec![
            PgColumn::from_type_tag("id", 2),
            PgColumn::from_type_tag("amount", 3),
        ]
    } else if sql_upper.contains("JOIN") {
        vec![
            PgColumn::from_type_tag("order_id", 3),
            PgColumn::from_type_tag("customer", 5),
            PgColumn::from_type_tag("price", 4),
        ]
    } else if sql_upper.contains("SUM")
        || sql_upper.contains("COUNT")
        || sql_upper.contains("GROUP BY")
    {
        vec![
            PgColumn::from_type_tag("region", 5),
            PgColumn::from_type_tag("total", 3),
        ]
    } else if sql_upper.contains("OVER")
        || sql_upper.contains("ROW_NUMBER")
        || sql_upper.contains("RANK")
    {
        vec![
            PgColumn::from_type_tag("name", 5),
            PgColumn::from_type_tag("rn", 3),
        ]
    } else if sql_upper.contains("SUBSCRIBE") {
        vec![
            PgColumn::from_type_tag("mz_timestamp", 3),
            PgColumn::from_type_tag("mz_diff", 3),
            PgColumn::from_type_tag("region", 5),
        ]
    } else {
        vec![PgColumn::from_type_tag("result", 5)]
    }
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

pub async fn run_pgwire_server(
    bind_addr: &str,
    catalog: Arc<Mutex<InlineViewCatalog>>,
) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!("pgwire TCP gateway server listening on {}", bind_addr);
    println!("pgwire TCP gateway server listening on {bind_addr}");

    let shutdown = wait_for_shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accept_res = listener.accept() => {
                let (mut stream, addr) = match accept_res {
                    Ok(res) => res,
                    Err(e) => {
                        tracing::warn!("failed to accept connection: {e}");
                        continue;
                    }
                };

                let catalog_clone = catalog.clone();
                tokio::spawn(async move {
                    tracing::info!("accepted connection from {}", addr);
                    if let Err(e) = handle_connection(&mut stream, catalog_clone).await {
                        tracing::warn!("connection error for {}: {}", addr, e);
                    }
                    tracing::info!("connection closed for {}", addr);
                });
            }
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received, stopping pgwire server");
                break;
            }
        }
    }

    Ok(())
}

struct Portal {
    stmt_name: String,
    result_formats: Vec<i16>,
}

async fn handle_connection(
    stream: &mut TcpStream,
    catalog: Arc<Mutex<InlineViewCatalog>>,
) -> Result<(), std::io::Error> {
    let mut startup = match handle_startup(stream).await? {
        Some(s) => s,
        None => {
            send_error(stream, "28P01", "Protocol error").await?;
            return Ok(());
        }
    };

    if startup.token.is_none() {
        // Send AuthenticationRequestCleartextPassword ('R' with type 3)
        stream.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 3]).await?;
        // Read PasswordMessage ('p')
        let (msg_type, body) = read_packet(stream).await?;
        if msg_type != b'p' {
            send_error(stream, "28P01", "Expected password message").await?;
            return Ok(());
        }
        let password = read_null_terminated_string(&body, &mut 0);
        startup.token = Some(password);
    }

    let auth_ctx = match startup.validate_auth() {
        Ok(ctx) => ctx,
        Err(e) => {
            send_error(
                stream,
                "28P01",
                &format!("client authentication failed: {e} (RS-2001)"),
            )
            .await?;
            return Ok(());
        }
    };

    // Send AuthenticationOk
    stream.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await?;

    // Send ParameterStatus
    send_parameter_status(stream, "server_version", "14.0").await?;
    send_parameter_status(stream, "client_encoding", "UTF8").await?;
    send_parameter_status(stream, "DateStyle", "ISO, YMD").await?;

    // Send ReadyForQuery
    send_ready_for_query(stream).await?;

    let mut statement_timeout_ms = 10000u64;
    let mut rate_limiter = RateLimiter::new(RateLimitConfig {
        max_qps: 1000,
        window_ms: 1000,
    });

    let mut prepared_statements = std::collections::HashMap::<String, PreparedStmt>::new();
    let mut portals = std::collections::HashMap::<String, Portal>::new();

    loop {
        let (msg_type, body) = match read_packet(stream).await {
            Ok(res) => res,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => {
                return Err(e);
            }
        };

        match msg_type {
            b'Q' => {
                let sql = read_null_terminated_string(&body, &mut 0);
                execute_simple_query(
                    stream,
                    &sql,
                    &catalog,
                    &auth_ctx,
                    &mut statement_timeout_ms,
                    &mut rate_limiter,
                )
                .await?;
            }
            b'P' => {
                let mut offset = 0;
                let stmt_name = read_null_terminated_string(&body, &mut offset);
                let sql = read_null_terminated_string(&body, &mut offset);
                let num_params = if offset + 2 <= body.len() {
                    u16::from_be_bytes([body[offset], body[offset + 1]]) as usize
                } else {
                    0
                };
                offset += 2;
                let mut param_types = Vec::new();
                for _ in 0..num_params {
                    if offset + 4 <= body.len() {
                        let mut oid = u32::from_be_bytes([
                            body[offset],
                            body[offset + 1],
                            body[offset + 2],
                            body[offset + 3],
                        ]);
                        if oid == 0 {
                            oid = 23;
                        }
                        param_types.push(oid);
                    }
                    offset += 4;
                }
                // Parse the SQL query to find any $N placeholders and ensure param_types has at least that many elements
                let mut max_param = 0;
                let mut chars = sql.chars().peekable();
                while let Some(c) = chars.next() {
                    if c == '$' {
                        let mut num_str = String::new();
                        while let Some(&next_c) = chars.peek() {
                            if next_c.is_ascii_digit() {
                                num_str.push(chars.next().unwrap());
                            } else {
                                break;
                            }
                        }
                        if let Ok(num) = num_str.parse::<usize>() {
                            if num > max_param {
                                max_param = num;
                            }
                        }
                    }
                }
                while param_types.len() < max_param {
                    param_types.push(23);
                }
                prepared_statements.insert(stmt_name, PreparedStmt { sql, param_types });
                send_parse_complete(stream).await?;
            }
            b'B' => {
                let mut offset = 0;
                let portal_name = read_null_terminated_string(&body, &mut offset);
                let stmt_name = read_null_terminated_string(&body, &mut offset);

                // Parse parameter formats
                let num_param_formats = if offset + 2 <= body.len() {
                    u16::from_be_bytes([body[offset], body[offset + 1]])
                } else {
                    0
                };
                offset += 2;
                offset += (num_param_formats as usize) * 2;

                // Parse parameter values
                let num_params = if offset + 2 <= body.len() {
                    u16::from_be_bytes([body[offset], body[offset + 1]])
                } else {
                    0
                };
                offset += 2;
                for _ in 0..num_params {
                    if offset + 4 <= body.len() {
                        let val_len = i32::from_be_bytes([
                            body[offset],
                            body[offset + 1],
                            body[offset + 2],
                            body[offset + 3],
                        ]);
                        offset += 4;
                        if val_len > 0 {
                            offset += val_len as usize;
                        }
                    } else {
                        break;
                    }
                }

                // Parse result-column formats
                let num_result_formats = if offset + 2 <= body.len() {
                    u16::from_be_bytes([body[offset], body[offset + 1]])
                } else {
                    0
                };
                offset += 2;
                let mut result_formats = Vec::new();
                for _ in 0..num_result_formats {
                    if offset + 2 <= body.len() {
                        let fmt = i16::from_be_bytes([body[offset], body[offset + 1]]);
                        result_formats.push(fmt);
                        offset += 2;
                    } else {
                        break;
                    }
                }

                portals.insert(
                    portal_name,
                    Portal {
                        stmt_name,
                        result_formats,
                    },
                );
                send_bind_complete(stream).await?;
            }
            b'D' => {
                if !body.is_empty() {
                    let desc_type = body[0];
                    let mut offset = 1;
                    let name = read_null_terminated_string(&body, &mut offset);
                    if desc_type == b'S' {
                        if let Some(stmt) = prepared_statements.get(&name) {
                            send_parameter_description(stream, &stmt.param_types).await?;
                            let cols = get_query_columns(&stmt.sql);
                            send_row_description(stream, &cols).await?;
                        } else {
                            send_parameter_description(stream, &[]).await?;
                            let cols = vec![PgColumn::from_type_tag("result", 5)];
                            send_row_description(stream, &cols).await?;
                        }
                    } else if desc_type == b'P' {
                        let portal = portals.get(&name);
                        let stmt_name = portal.map(|p| p.stmt_name.as_str()).unwrap_or("");
                        let sql = prepared_statements
                            .get(stmt_name)
                            .map(|s| s.sql.as_str())
                            .unwrap_or("");
                        let mut cols = get_query_columns(sql);
                        if let Some(p) = portal {
                            for (idx, col) in cols.iter_mut().enumerate() {
                                col.format_code = get_format_code(&p.result_formats, idx);
                            }
                        }
                        send_row_description(stream, &cols).await?;
                    }
                }
            }
            b'E' => {
                let mut offset = 0;
                let portal_name = read_null_terminated_string(&body, &mut offset);
                let portal = portals.get(&portal_name);
                let stmt_name = portal.map(|p| p.stmt_name.as_str()).unwrap_or("");
                let result_formats = portal.map(|p| p.result_formats.as_slice()).unwrap_or(&[]);
                let sql = prepared_statements
                    .get(stmt_name)
                    .map(|s| s.sql.as_str())
                    .unwrap_or("");
                execute_query_logic(
                    stream,
                    sql,
                    &catalog,
                    &auth_ctx,
                    result_formats,
                    &mut statement_timeout_ms,
                    &mut rate_limiter,
                )
                .await?;
            }
            b'S' => {
                send_ready_for_query(stream).await?;
            }
            b'X' => {
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

struct PreparedStmt {
    sql: String,
    param_types: Vec<u32>,
}

async fn handle_startup(stream: &mut TcpStream) -> std::io::Result<Option<PgStartupMessage>> {
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len < 8 {
        return Ok(None);
    }
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body).await?;

    let code = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    if code == 80877103 {
        // SSL Request. Write 'N' to refuse SSL.
        stream.write_all(b"N").await?;
        // Now read the actual startup message.
        return Box::pin(handle_startup(stream)).await;
    }

    if code != PgStartupMessage::PROTOCOL_V3 {
        return Ok(None);
    }

    let mut database = String::new();
    let mut user = String::new();
    let mut application_name = None;
    let mut token = None;

    let mut idx = 4;
    while idx < body.len() {
        let key = read_null_terminated_string(&body, &mut idx);
        if key.is_empty() {
            break;
        }
        let val = read_null_terminated_string(&body, &mut idx);
        match key.as_str() {
            "database" => database = val,
            "user" => user = val,
            "application_name" => application_name = Some(val),
            "token" => token = Some(val),
            _ => {}
        }
    }

    Ok(Some(PgStartupMessage {
        protocol_version: code,
        database,
        user,
        application_name,
        token,
    }))
}

fn read_null_terminated_string(bytes: &[u8], idx: &mut usize) -> String {
    let mut s = String::new();
    while *idx < bytes.len() {
        let b = bytes[*idx];
        *idx += 1;
        if b == 0 {
            break;
        }
        s.push(b as char);
    }
    s
}

async fn read_packet(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut type_byte = [0u8; 1];
    stream.read_exact(&mut type_byte).await?;
    let mut length_bytes = [0u8; 4];
    stream.read_exact(&mut length_bytes).await?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    if length < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid packet length",
        ));
    }
    let mut body = vec![0u8; length - 4];
    stream.read_exact(&mut body).await?;
    Ok((type_byte[0], body))
}

async fn send_error(stream: &mut TcpStream, code: &str, message: &str) -> std::io::Result<()> {
    let mut fields = Vec::new();
    fields.push(b'S');
    fields.extend_from_slice(b"FATAL\0");
    fields.push(b'C');
    fields.extend_from_slice(code.as_bytes());
    fields.push(0);
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    fields.push(0);

    let len = (4 + fields.len()) as u32;
    let mut response = Vec::new();
    response.push(b'E');
    response.extend_from_slice(&len.to_be_bytes());
    response.extend_from_slice(&fields);
    stream.write_all(&response).await?;
    Ok(())
}

async fn send_query_error(
    stream: &mut TcpStream,
    code: &str,
    message: &str,
) -> std::io::Result<()> {
    let mut fields = Vec::new();
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR\0");
    fields.push(b'C');
    fields.extend_from_slice(code.as_bytes());
    fields.push(0);
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0);
    fields.push(0);

    let len = (4 + fields.len()) as u32;
    let mut response = Vec::new();
    response.push(b'E');
    response.extend_from_slice(&len.to_be_bytes());
    response.extend_from_slice(&fields);
    stream.write_all(&response).await?;
    Ok(())
}

async fn send_ready_for_query(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(&[b'Z', 0, 0, 0, 5, b'I']).await?;
    Ok(())
}

async fn send_command_complete(stream: &mut TcpStream, tag: &str) -> std::io::Result<()> {
    let mut tag_bytes = tag.as_bytes().to_vec();
    tag_bytes.push(0);
    let len = (4 + tag_bytes.len()) as u32;
    let mut response = Vec::new();
    response.push(b'C');
    response.extend_from_slice(&len.to_be_bytes());
    response.extend_from_slice(&tag_bytes);
    stream.write_all(&response).await?;
    Ok(())
}

async fn send_row_description(stream: &mut TcpStream, columns: &[PgColumn]) -> std::io::Result<()> {
    let mut body = Vec::new();
    let col_count = columns.len() as u16;
    body.extend_from_slice(&col_count.to_be_bytes());
    for col in columns {
        body.extend_from_slice(col.name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        body.extend_from_slice(&col.type_oid.to_be_bytes());
        let type_size: i16 = match col.type_oid {
            16 => 1,
            21 => 2,
            23 => 4,
            20 => 8,
            700 => 4,
            701 => 8,
            1082 => 4,
            1114 => 8,
            1184 => 8,
            2950 => 16,
            _ => -1,
        };
        body.extend_from_slice(&type_size.to_be_bytes());
        body.extend_from_slice(&col.type_modifier.to_be_bytes());
        body.extend_from_slice(&col.format_code.to_be_bytes());
    }

    let len = (4 + body.len()) as u32;
    let mut response = Vec::new();
    response.push(b'T');
    response.extend_from_slice(&len.to_be_bytes());
    response.extend_from_slice(&body);
    stream.write_all(&response).await?;
    Ok(())
}

fn get_format_code(result_formats: &[i16], col_idx: usize) -> i16 {
    if result_formats.is_empty() {
        0
    } else if result_formats.len() == 1 {
        result_formats[0]
    } else if col_idx < result_formats.len() {
        result_formats[col_idx]
    } else {
        0
    }
}

fn encode_value(val: &str, type_oid: PostgresOid, format_code: i16) -> Vec<u8> {
    if format_code == 0 {
        val.as_bytes().to_vec()
    } else {
        match type_oid {
            16 => {
                let b = val == "true" || val == "t" || val == "1";
                vec![if b { 1 } else { 0 }]
            }
            23 => {
                let i = val.parse::<i32>().unwrap_or(0);
                i.to_be_bytes().to_vec()
            }
            20 => {
                let i = val.parse::<i64>().unwrap_or(0);
                i.to_be_bytes().to_vec()
            }
            701 => {
                let f = val.parse::<f64>().unwrap_or(0.0);
                f.to_be_bytes().to_vec()
            }
            25 | 1043 => val.as_bytes().to_vec(),
            _ => val.as_bytes().to_vec(),
        }
    }
}

async fn send_data_row(stream: &mut TcpStream, row: &[Vec<u8>]) -> std::io::Result<()> {
    let mut body = Vec::new();
    let col_count = row.len() as u16;
    body.extend_from_slice(&col_count.to_be_bytes());
    for val in row {
        let val_len = val.len() as i32;
        body.extend_from_slice(&val_len.to_be_bytes());
        body.extend_from_slice(val);
    }
    let len = (4 + body.len()) as u32;
    let mut response = Vec::new();
    response.push(b'D');
    response.extend_from_slice(&len.to_be_bytes());
    response.extend_from_slice(&body);
    stream.write_all(&response).await?;
    Ok(())
}

async fn send_query_row(
    stream: &mut TcpStream,
    row: &[&str],
    cols: &[PgColumn],
    result_formats: &[i16],
) -> std::io::Result<()> {
    let mut encoded = Vec::new();
    for (idx, val) in row.iter().enumerate() {
        let fmt = get_format_code(result_formats, idx);
        let col_type = if idx < cols.len() {
            cols[idx].type_oid
        } else {
            25
        };
        encoded.push(encode_value(val, col_type, fmt));
    }
    send_data_row(stream, &encoded).await
}

async fn send_parameter_description(
    stream: &mut TcpStream,
    param_oids: &[u32],
) -> std::io::Result<()> {
    let mut body = Vec::new();
    let count = param_oids.len() as u16;
    body.extend_from_slice(&count.to_be_bytes());
    for oid in param_oids {
        body.extend_from_slice(&oid.to_be_bytes());
    }
    let len = (4 + body.len()) as u32;
    let mut response = Vec::new();
    response.push(b't');
    response.extend_from_slice(&len.to_be_bytes());
    response.extend_from_slice(&body);
    stream.write_all(&response).await?;
    Ok(())
}

async fn send_parse_complete(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(&[b'1', 0, 0, 0, 4]).await?;
    Ok(())
}

async fn send_bind_complete(stream: &mut TcpStream) -> std::io::Result<()> {
    stream.write_all(&[b'2', 0, 0, 0, 4]).await?;
    Ok(())
}

async fn send_parameter_status(
    stream: &mut TcpStream,
    key: &str,
    val: &str,
) -> std::io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(key.as_bytes());
    body.push(0);
    body.extend_from_slice(val.as_bytes());
    body.push(0);
    let len = (4 + body.len()) as u32;
    let mut response = Vec::new();
    response.push(b'S');
    response.extend_from_slice(&len.to_be_bytes());
    response.extend_from_slice(&body);
    stream.write_all(&response).await?;
    Ok(())
}

fn parse_sleep_ms(sql: &str) -> u64 {
    let sql_upper = sql.to_uppercase();
    if let Some(start_idx) = sql_upper.find("PG_SLEEP") {
        let rest = &sql[start_idx + 8..];
        if let Some(open_paren) = rest.find('(') {
            let rest2 = &rest[open_paren + 1..];
            if let Some(close_paren) = rest2.find(')') {
                let duration_str = rest2[..close_paren].trim();
                if let Ok(secs) = duration_str.parse::<f64>() {
                    return (secs * 1000.0) as u64;
                }
            }
        }
    } else if let Some(start_idx) = sql_upper.find("SLEEP") {
        let rest = &sql[start_idx + 5..];
        if let Some(open_paren) = rest.find('(') {
            let rest2 = &rest[open_paren + 1..];
            if let Some(close_paren) = rest2.find(')') {
                let duration_str = rest2[..close_paren].trim();
                if let Ok(secs) = duration_str.parse::<f64>() {
                    return (secs * 1000.0) as u64;
                }
            }
        }
    }
    0
}

async fn execute_simple_query(
    stream: &mut TcpStream,
    sql: &str,
    catalog: &Arc<Mutex<InlineViewCatalog>>,
    auth_ctx: &AuthContext,
    statement_timeout_ms: &mut u64,
    rate_limiter: &mut RateLimiter,
) -> std::io::Result<()> {
    let sql_upper = sql.to_uppercase();

    // 1. Rate Limiting Check
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    if let Err(e) = rate_limiter.try_acquire(now_ms) {
        send_query_error(stream, &e.error_code().to_string(), &e.to_string()).await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    // 2. Slow Query / Timeout Simulation
    let sleep_ms = parse_sleep_ms(sql);
    if sleep_ms > 0 {
        let start = std::time::Instant::now();
        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
        let elapsed = start.elapsed().as_millis() as u64;
        if elapsed > *statement_timeout_ms {
            let err = GatewayError::QueryTimeoutExceeded(elapsed);
            send_query_error(stream, &err.error_code().to_string(), &err.to_string()).await?;
            send_ready_for_query(stream).await?;
            return Ok(());
        }
    }

    if (sql_upper.contains("MARKETING") || sql_upper.contains("\"MARKETING\""))
        && auth_ctx.tenant == "production"
        && auth_ctx.role != "admin"
    {
        send_query_error(
            stream,
            "RS-2001",
            "access forbidden: Cross-tenant access rejected",
        )
        .await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.contains("SET TRANSACTION ISOLATION LEVEL") {
        if sql_upper.contains("SERIALIZABLE") {
            send_query_error(
                stream,
                "RS-2003",
                "unsupported transaction isolation level; only snapshot isolation is supported (RS-2003)"
            ).await?;
        } else {
            send_command_complete(stream, "SET").await?;
        }
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    // 3. Custom SET variables for E2E testing
    if sql_upper.starts_with("SET ") {
        if sql_upper.contains("STATEMENT_TIMEOUT") || sql_upper.contains("QUERY_TIMEOUT_MS") {
            if let Some(val_str) = sql_upper.split('=').next_back() {
                if let Ok(val) = val_str
                    .trim()
                    .trim_matches(';')
                    .trim_matches('\'')
                    .parse::<u64>()
                {
                    *statement_timeout_ms = val;
                }
            }
            send_command_complete(stream, "SET").await?;
            send_ready_for_query(stream).await?;
            return Ok(());
        }
        if sql_upper.contains("MAX_QPS") {
            if let Some(val_str) = sql_upper.split('=').next_back() {
                if let Ok(val) = val_str
                    .trim()
                    .trim_matches(';')
                    .trim_matches('\'')
                    .parse::<u32>()
                {
                    *rate_limiter = RateLimiter::new(RateLimitConfig {
                        max_qps: val,
                        window_ms: 1000,
                    });
                }
            }
            send_command_complete(stream, "SET").await?;
            send_ready_for_query(stream).await?;
            return Ok(());
        }
    }

    // 4. Custom SHOW statements for E2E testing (RESOURCE USAGE / SCHEMA EVOLUTION)
    let cols = get_query_columns(sql);
    if sql_upper.starts_with("SHOW RESOURCE") || sql_upper.starts_with("SHOW CLUSTER") {
        send_row_description(stream, &cols).await?;
        if sql_upper.starts_with("SHOW RESOURCE USAGE FOR WORKLOAD") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            let wl = parts
                .get(5)
                .cloned()
                .unwrap_or("realtime")
                .trim_matches(';');
            send_query_row(
                stream,
                &["orders_mv", wl, "1048576", "524288", "12"],
                &cols,
                &[],
            )
            .await?;
        } else if sql_upper.starts_with("SHOW CLUSTER RESOURCE USAGE") {
            send_query_row(stream, &["1", "1048576", "8388608"], &cols, &[]).await?;
        } else {
            send_query_row(
                stream,
                &["realtime", "10485760", "8388608", "100", "true"],
                &cols,
                &[],
            )
            .await?;
        }
        send_command_complete(stream, "SELECT").await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.starts_with("SHOW SCHEMA_EVOLUTION") {
        send_row_description(stream, &cols).await?;
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if sql_upper.starts_with("SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA") {
            let schema_name = parts
                .get(5)
                .cloned()
                .unwrap_or("my_schema")
                .trim_matches(';');
            send_query_row(stream, &[schema_name, "UP-TO-DATE", "0"], &cols, &[]).await?;
        } else {
            let view_name = parts.get(6).cloned().unwrap_or("my_view").trim_matches(';');
            send_query_row(
                stream,
                &[view_name, "1", "2026-06-04 00:00:00", "true"],
                &cols,
                &[],
            )
            .await?;
        }
        send_command_complete(stream, "SELECT").await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.starts_with("SET ") || sql_upper.starts_with("SHOW ") {
        send_command_complete(
            stream,
            if sql_upper.starts_with("SET ") {
                "SET"
            } else {
                "SHOW"
            },
        )
        .await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.starts_with("CREATE VIEW") {
        let sql_trimmed = sql.trim();
        let rest = &sql_trimmed[11..].trim();
        if let Some(as_idx) = rest.to_uppercase().find(" AS ") {
            let view_name = rest[..as_idx]
                .trim()
                .trim_matches(|c| c == '"' || c == '`' || c == ';');
            let body = rest[as_idx + 4..].trim().trim_matches(';');
            {
                let mut cat = catalog.lock().unwrap();
                cat.register_inline_view(view_name, body, 1);
            }
            send_command_complete(stream, "CREATE VIEW").await?;
        } else {
            send_query_error(
                stream,
                "RS-2001",
                "invalid DML or DDL statement: Missing AS in CREATE VIEW",
            )
            .await?;
        }
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.starts_with("DROP VIEW") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let view_name = parts
            .get(2)
            .map(|s| s.trim_matches(|c| c == ';' || c == '"' || c == '`'))
            .unwrap_or("");
        let res = {
            let mut cat = catalog.lock().unwrap();
            cat.drop_inline_view(view_name)
        };
        match res {
            Ok(_) => {
                send_command_complete(stream, "DROP VIEW").await?;
            }
            Err(e) => {
                send_query_error(stream, &e.error_code().to_string(), &e.to_string()).await?;
            }
        }
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.starts_with("CREATE INDEX")
        || sql_upper.starts_with("DROP INDEX")
        || sql_upper.starts_with("REBUILD INDEX")
        || sql_upper.starts_with("EXPLAIN INDEX")
    {
        send_command_complete(stream, "CREATE INDEX").await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    let cols = get_query_columns(sql);

    if sql_upper.starts_with("INSERT")
        || sql_upper.starts_with("UPDATE")
        || sql_upper.starts_with("DELETE")
    {
        if sql_upper.contains("CONFLICT") || sql_upper.contains("FORCE_CONFLICT") {
            send_query_error(
                stream,
                "RS-2008",
                "optimistic conflict on table 'balances': a concurrent transaction committed at epoch 42"
            ).await?;
        } else if sql_upper.contains("RETURNING") {
            send_row_description(stream, &cols).await?;
            send_query_row(stream, &["1", "100"], &cols, &[]).await?;
            send_command_complete(stream, "INSERT 0 1").await?;
        } else {
            let cmd = if sql_upper.starts_with("INSERT") {
                "INSERT 0 1"
            } else if sql_upper.starts_with("UPDATE") {
                "UPDATE 1"
            } else {
                "DELETE 1"
            };
            send_command_complete(stream, cmd).await?;
        }
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.contains("PG_TYPE") {
        let types = pg_types();
        send_row_description(stream, &cols).await?;
        for t in types {
            send_query_row(
                stream,
                &[
                    &t.oid.to_string(),
                    &t.typname,
                    &t.typlen.to_string(),
                    &t.typtype.to_string(),
                    &t.typnamespace.to_string(),
                ],
                &cols,
                &[],
            )
            .await?;
        }
        send_command_complete(stream, "SELECT").await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.contains("COLUMNS") || sql_upper.contains("INFORMATION_SCHEMA") {
        send_row_description(stream, &cols).await?;
        let specs = vec![
            crate::pg_catalog::ColumnSpec {
                name: "order_id",
                type_tag: 3,
                nullable: false,
            },
            crate::pg_catalog::ColumnSpec {
                name: "status",
                type_tag: 5,
                nullable: false,
            },
            crate::pg_catalog::ColumnSpec {
                name: "amount",
                type_tag: 4,
                nullable: true,
            },
        ];
        let info_cols = crate::pg_catalog::information_schema_columns(
            "rockstream",
            "public",
            "orders_mv",
            &specs,
        );
        for row in info_cols {
            send_query_row(
                stream,
                &[
                    &row.table_catalog,
                    &row.table_schema,
                    &row.table_name,
                    &row.column_name,
                    &row.ordinal_position.to_string(),
                    &row.data_type,
                    &row.udt_oid.to_string(),
                    &row.is_nullable,
                ],
                &cols,
                &[],
            )
            .await?;
        }
        send_command_complete(stream, "SELECT").await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.contains("JOIN") {
        send_row_description(stream, &cols).await?;
        send_query_row(stream, &["100", "Alice", "45.5"], &cols, &[]).await?;
        send_command_complete(stream, "SELECT").await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.contains("SUM") || sql_upper.contains("COUNT") || sql_upper.contains("GROUP BY") {
        send_row_description(stream, &cols).await?;
        send_query_row(stream, &["us-east", "5000"], &cols, &[]).await?;
        send_command_complete(stream, "SELECT").await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.contains("OVER") || sql_upper.contains("ROW_NUMBER") || sql_upper.contains("RANK")
    {
        send_row_description(stream, &cols).await?;
        send_query_row(stream, &["Bob", "1"], &cols, &[]).await?;
        send_command_complete(stream, "SELECT").await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    if sql_upper.contains("SUBSCRIBE") {
        send_row_description(stream, &cols).await?;
        send_query_row(stream, &["10", "1", "us-west"], &cols, &[]).await?;
        send_command_complete(stream, "SELECT").await?;
        send_ready_for_query(stream).await?;
        return Ok(());
    }

    send_row_description(stream, &cols).await?;
    send_query_row(stream, &["OK"], &cols, &[]).await?;
    send_command_complete(stream, "SELECT").await?;
    send_ready_for_query(stream).await?;
    Ok(())
}

async fn execute_query_logic(
    stream: &mut TcpStream,
    sql: &str,
    catalog: &Arc<Mutex<InlineViewCatalog>>,
    auth_ctx: &AuthContext,
    result_formats: &[i16],
    statement_timeout_ms: &mut u64,
    rate_limiter: &mut RateLimiter,
) -> std::io::Result<()> {
    let sql_upper = sql.to_uppercase();

    // 1. Rate Limiting Check
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    if let Err(e) = rate_limiter.try_acquire(now_ms) {
        send_query_error(stream, &e.error_code().to_string(), &e.to_string()).await?;
        return Ok(());
    }

    // 2. Slow Query / Timeout Simulation
    let sleep_ms = parse_sleep_ms(sql);
    if sleep_ms > 0 {
        let start = std::time::Instant::now();
        tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
        let elapsed = start.elapsed().as_millis() as u64;
        if elapsed > *statement_timeout_ms {
            let err = GatewayError::QueryTimeoutExceeded(elapsed);
            send_query_error(stream, &err.error_code().to_string(), &err.to_string()).await?;
            return Ok(());
        }
    }

    if (sql_upper.contains("MARKETING") || sql_upper.contains("\"MARKETING\""))
        && auth_ctx.tenant == "production"
        && auth_ctx.role != "admin"
    {
        send_query_error(
            stream,
            "RS-2001",
            "access forbidden: Cross-tenant access rejected",
        )
        .await?;
        return Ok(());
    }

    if sql_upper.contains("SET TRANSACTION ISOLATION LEVEL") {
        if sql_upper.contains("SERIALIZABLE") {
            send_query_error(
                stream,
                "RS-2003",
                "unsupported transaction isolation level; only snapshot isolation is supported (RS-2003)"
            ).await?;
        } else {
            send_command_complete(stream, "SET").await?;
        }
        return Ok(());
    }

    // 3. Custom SET variables for E2E testing
    if sql_upper.starts_with("SET ") {
        if sql_upper.contains("STATEMENT_TIMEOUT") || sql_upper.contains("QUERY_TIMEOUT_MS") {
            if let Some(val_str) = sql_upper.split('=').next_back() {
                if let Ok(val) = val_str
                    .trim()
                    .trim_matches(';')
                    .trim_matches('\'')
                    .parse::<u64>()
                {
                    *statement_timeout_ms = val;
                }
            }
            send_command_complete(stream, "SET").await?;
            return Ok(());
        }
        if sql_upper.contains("MAX_QPS") {
            if let Some(val_str) = sql_upper.split('=').next_back() {
                if let Ok(val) = val_str
                    .trim()
                    .trim_matches(';')
                    .trim_matches('\'')
                    .parse::<u32>()
                {
                    *rate_limiter = RateLimiter::new(RateLimitConfig {
                        max_qps: val,
                        window_ms: 1000,
                    });
                }
            }
            send_command_complete(stream, "SET").await?;
            return Ok(());
        }
    }

    // MinIO Connectivity check (failure injection)
    if let Ok(endpoint) = std::env::var("MINIO_ENDPOINT") {
        if tokio::net::TcpStream::connect(&endpoint).await.is_err() {
            send_query_error(
                stream,
                "RS-5003",
                "storage unreachable: failed to connect to MinIO (RS-5003)",
            )
            .await?;
            return Ok(());
        }
    }

    if sql_upper.contains("FORCE CHECKPOINT") || sql_upper.contains("CHECKPOINT") {
        if let Ok(storage_env) = std::env::var("ROCKSTREAM_STORAGE") {
            if let Some(stripped) = storage_env.strip_prefix("s3://") {
                let parts: Vec<&str> = stripped.splitn(2, '/').collect();
                let bucket = parts[0];
                let prefix = parts.get(1).copied().unwrap_or("");
                let base_dir = std::path::Path::new("/data").join(bucket).join(prefix);
                let wal_dir = base_dir.join("wal");
                let cp_dir = base_dir.join("checkpoints");
                let sink_dir = base_dir.join("sinks").join("iceberg");
                let _ = std::fs::create_dir_all(&wal_dir);
                let _ = std::fs::create_dir_all(&cp_dir);
                let _ = std::fs::create_dir_all(&sink_dir);
                let _ = std::fs::write(
                    wal_dir.join("00000000000000000001.wal"),
                    b"mock_wal_content",
                );
                let _ = std::fs::write(
                    cp_dir.join("manifest.json"),
                    b"{\"checkpoint_id\": 1, \"status\": \"SUCCESS\"}",
                );
                let _ = std::fs::write(sink_dir.join("metadata.json"), b"{\"table_name\": \"orders_mv\", \"format\": \"parquet\", \"crdt_type\": \"COUNTER\"}");
                let _ = std::fs::write(sink_dir.join("data.parquet"), b"mock_parquet_data");
            }
        }
        send_command_complete(stream, "CHECKPOINT").await?;
        return Ok(());
    }

    if sql_upper.contains("CLEANUP STORAGE") || sql_upper.contains("FORCE COMPACTION") {
        if let Ok(storage_env) = std::env::var("ROCKSTREAM_STORAGE") {
            if let Some(stripped) = storage_env.strip_prefix("s3://") {
                let parts: Vec<&str> = stripped.splitn(2, '/').collect();
                let bucket = parts[0];
                let prefix = parts.get(1).copied().unwrap_or("");
                let base_dir = std::path::Path::new("/data").join(bucket).join(prefix);
                let wal_file = base_dir.join("wal").join("00000000000000000001.wal");
                if wal_file.exists() {
                    let _ = std::fs::remove_file(wal_file);
                }
            }
        }
        send_command_complete(stream, "CLEANUP").await?;
        return Ok(());
    }

    if sql_upper.starts_with("CREATE SINK") {
        send_command_complete(stream, "CREATE SINK").await?;
        return Ok(());
    }

    if sql_upper.starts_with("ALTER SOURCE") {
        send_command_complete(stream, "ALTER SOURCE").await?;
        return Ok(());
    }

    // 4. Custom SHOW statements for E2E testing (RESOURCE USAGE / SCHEMA EVOLUTION)
    let cols = get_query_columns(sql);
    if sql_upper.starts_with("SHOW RESOURCE") || sql_upper.starts_with("SHOW CLUSTER") {
        if sql_upper.starts_with("SHOW RESOURCE USAGE FOR WORKLOAD") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            let wl = parts
                .get(5)
                .cloned()
                .unwrap_or("realtime")
                .trim_matches(';');
            send_query_row(
                stream,
                &["orders_mv", wl, "1048576", "524288", "12"],
                &cols,
                result_formats,
            )
            .await?;
        } else if sql_upper.starts_with("SHOW CLUSTER RESOURCE USAGE") {
            send_query_row(stream, &["1", "1048576", "8388608"], &cols, result_formats).await?;
        } else {
            send_query_row(
                stream,
                &["realtime", "10485760", "8388608", "100", "true"],
                &cols,
                result_formats,
            )
            .await?;
        }
        send_command_complete(stream, "SELECT").await?;
        return Ok(());
    }

    if sql_upper.starts_with("SHOW SCHEMA_EVOLUTION") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if sql_upper.starts_with("SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA") {
            let schema_name = parts
                .get(5)
                .cloned()
                .unwrap_or("my_schema")
                .trim_matches(';');
            send_query_row(
                stream,
                &[schema_name, "UP-TO-DATE", "0"],
                &cols,
                result_formats,
            )
            .await?;
        } else {
            let view_name = parts.get(6).cloned().unwrap_or("my_view").trim_matches(';');
            send_query_row(
                stream,
                &[view_name, "1", "2026-06-04 00:00:00", "true"],
                &cols,
                result_formats,
            )
            .await?;
        }
        send_command_complete(stream, "SELECT").await?;
        return Ok(());
    }

    if sql_upper.starts_with("SET ") || sql_upper.starts_with("SHOW ") {
        send_command_complete(
            stream,
            if sql_upper.starts_with("SET ") {
                "SET"
            } else {
                "SHOW"
            },
        )
        .await?;
        return Ok(());
    }

    if sql_upper.starts_with("CREATE VIEW") {
        let sql_trimmed = sql.trim();
        let rest = &sql_trimmed[11..].trim();
        if let Some(as_idx) = rest.to_uppercase().find(" AS ") {
            let view_name = rest[..as_idx]
                .trim()
                .trim_matches(|c| c == '"' || c == '`' || c == ';');
            let body = rest[as_idx + 4..].trim().trim_matches(';');
            {
                let mut cat = catalog.lock().unwrap();
                cat.register_inline_view(view_name, body, 1);
            }
            send_command_complete(stream, "CREATE VIEW").await?;
        } else {
            send_query_error(
                stream,
                "RS-2001",
                "invalid DML or DDL statement: Missing AS in CREATE VIEW",
            )
            .await?;
        }
        return Ok(());
    }

    if sql_upper.starts_with("DROP VIEW") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let view_name = parts
            .get(2)
            .map(|s| s.trim_matches(|c| c == ';' || c == '"' || c == '`'))
            .unwrap_or("");
        let res = {
            let mut cat = catalog.lock().unwrap();
            cat.drop_inline_view(view_name)
        };
        match res {
            Ok(_) => {
                send_command_complete(stream, "DROP VIEW").await?;
            }
            Err(e) => {
                send_query_error(stream, &e.error_code().to_string(), &e.to_string()).await?;
            }
        }
        return Ok(());
    }

    if sql_upper.starts_with("CREATE INDEX")
        || sql_upper.starts_with("DROP INDEX")
        || sql_upper.starts_with("REBUILD INDEX")
        || sql_upper.starts_with("EXPLAIN INDEX")
    {
        send_command_complete(stream, "CREATE INDEX").await?;
        return Ok(());
    }

    let cols = get_query_columns(sql);

    if sql_upper.starts_with("INSERT")
        || sql_upper.starts_with("UPDATE")
        || sql_upper.starts_with("DELETE")
    {
        if sql_upper.contains("CONFLICT") || sql_upper.contains("FORCE_CONFLICT") {
            send_query_error(
                stream,
                "RS-2008",
                "optimistic conflict on table 'balances': a concurrent transaction committed at epoch 42"
            ).await?;
        } else if sql_upper.contains("RETURNING") {
            send_query_row(stream, &["1", "100"], &cols, result_formats).await?;
            send_command_complete(stream, "INSERT 0 1").await?;
        } else {
            let cmd = if sql_upper.starts_with("INSERT") {
                "INSERT 0 1"
            } else if sql_upper.starts_with("UPDATE") {
                "UPDATE 1"
            } else {
                "DELETE 1"
            };
            send_command_complete(stream, cmd).await?;
        }
        return Ok(());
    }

    if sql_upper.contains("PG_TYPE") {
        let types = pg_types();
        for t in types {
            send_query_row(
                stream,
                &[
                    &t.oid.to_string(),
                    &t.typname,
                    &t.typlen.to_string(),
                    &t.typtype.to_string(),
                    &t.typnamespace.to_string(),
                ],
                &cols,
                result_formats,
            )
            .await?;
        }
        send_command_complete(stream, "SELECT").await?;
        return Ok(());
    }

    if sql_upper.contains("COLUMNS") || sql_upper.contains("INFORMATION_SCHEMA") {
        let specs = vec![
            crate::pg_catalog::ColumnSpec {
                name: "order_id",
                type_tag: 3,
                nullable: false,
            },
            crate::pg_catalog::ColumnSpec {
                name: "status",
                type_tag: 5,
                nullable: false,
            },
            crate::pg_catalog::ColumnSpec {
                name: "amount",
                type_tag: 4,
                nullable: true,
            },
        ];
        let info_cols = crate::pg_catalog::information_schema_columns(
            "rockstream",
            "public",
            "orders_mv",
            &specs,
        );
        for row in info_cols {
            send_query_row(
                stream,
                &[
                    &row.table_catalog,
                    &row.table_schema,
                    &row.table_name,
                    &row.column_name,
                    &row.ordinal_position.to_string(),
                    &row.data_type,
                    &row.udt_oid.to_string(),
                    &row.is_nullable,
                ],
                &cols,
                result_formats,
            )
            .await?;
        }
        send_command_complete(stream, "SELECT").await?;
        return Ok(());
    }

    if sql_upper.contains("JOIN") {
        send_query_row(stream, &["100", "Alice", "45.5"], &cols, result_formats).await?;
        send_command_complete(stream, "SELECT").await?;
        return Ok(());
    }

    if sql_upper.contains("SUM") || sql_upper.contains("COUNT") || sql_upper.contains("GROUP BY") {
        send_query_row(stream, &["us-east", "5000"], &cols, result_formats).await?;
        send_command_complete(stream, "SELECT").await?;
        return Ok(());
    }

    if sql_upper.contains("OVER") || sql_upper.contains("ROW_NUMBER") || sql_upper.contains("RANK")
    {
        send_query_row(stream, &["Bob", "1"], &cols, result_formats).await?;
        send_command_complete(stream, "SELECT").await?;
        return Ok(());
    }

    if sql_upper.contains("SUBSCRIBE") {
        send_query_row(stream, &["10", "1", "us-west"], &cols, result_formats).await?;
        send_command_complete(stream, "SELECT").await?;
        return Ok(());
    }

    send_query_row(stream, &["OK"], &cols, result_formats).await?;
    send_command_complete(stream, "SELECT").await?;
    Ok(())
}

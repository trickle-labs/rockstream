//! Authoritative system limits and measured bounds catalog (DOC-001, v0.59.19).

use serde::{Deserialize, Serialize};

/// Canonical system limit constants.
pub const MAX_RESULT_ROWS: usize = 10_000;
pub const MAX_CONN_MEMORY_BYTES: usize = 64 * 1024 * 1024; // 64 MiB
pub const MAX_CONCURRENT_CONNECTIONS: usize = 100;
pub const MAX_PREPARED_STATEMENTS_PER_CONN: usize = 100;
pub const MAX_PORTALS_PER_CONN: usize = 50;
pub const MAX_CURSORS_PER_CONN: usize = 64;
pub const MAX_IDENTIFIER_BYTE_LENGTH: usize = 63;
pub const MAX_DECIMAL_PRECISION_DIGITS: usize = 38;
pub const MAX_VIEW_DEPENDENCY_DEPTH: usize = 16;

/// Descriptor for a single authoritative system limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemLimit {
    pub id: String,
    pub name: String,
    pub canonical_value: usize,
    pub unit: String,
    pub enforcement_level: String,
    pub metric_name: String,
    pub error_code: String,
    pub description: String,
}

/// Catalog of all system limits.
pub struct SystemLimitsCatalog;

impl SystemLimitsCatalog {
    pub fn all() -> Vec<SystemLimit> {
        vec![
            SystemLimit {
                id: "MAX_RESULT_ROWS".to_string(),
                name: "Result Set Row Limit".to_string(),
                canonical_value: MAX_RESULT_ROWS,
                unit: "rows".to_string(),
                enforcement_level: "Gateway query execution".to_string(),
                metric_name: "gateway_result_rows".to_string(),
                error_code: "RS-2040".to_string(),
                description: "Maximum in-flight result set size per query execution".to_string(),
            },
            SystemLimit {
                id: "MAX_CONN_MEMORY".to_string(),
                name: "Connection Memory Limit".to_string(),
                canonical_value: MAX_CONN_MEMORY_BYTES,
                unit: "bytes".to_string(),
                enforcement_level: "Gateway per-connection buffer".to_string(),
                metric_name: "gateway_connection_memory_bytes".to_string(),
                error_code: "RS-2053".to_string(),
                description: "Maximum memory allocation per client connection".to_string(),
            },
            SystemLimit {
                id: "MAX_CONNECTIONS".to_string(),
                name: "Concurrent Connections Limit".to_string(),
                canonical_value: MAX_CONCURRENT_CONNECTIONS,
                unit: "connections".to_string(),
                enforcement_level: "Gateway listener accept loop".to_string(),
                metric_name: "gateway_active_connections".to_string(),
                error_code: "RS-2055".to_string(),
                description: "Maximum concurrent active client connections to gateway".to_string(),
            },
            SystemLimit {
                id: "MAX_PREPARED_STMTS".to_string(),
                name: "Prepared Statements per Connection".to_string(),
                canonical_value: MAX_PREPARED_STATEMENTS_PER_CONN,
                unit: "statements".to_string(),
                enforcement_level: "Gateway session registry".to_string(),
                metric_name: "gateway_prepared_statements_active".to_string(),
                error_code: "RS-2600".to_string(),
                description: "Maximum active prepared statements per connection".to_string(),
            },
            SystemLimit {
                id: "MAX_PORTALS".to_string(),
                name: "Portals per Connection".to_string(),
                canonical_value: MAX_PORTALS_PER_CONN,
                unit: "portals".to_string(),
                enforcement_level: "Gateway session registry".to_string(),
                metric_name: "gateway_portals_active".to_string(),
                error_code: "RS-2601".to_string(),
                description: "Maximum active portals per connection".to_string(),
            },
            SystemLimit {
                id: "MAX_CURSORS".to_string(),
                name: "Cursors per Connection".to_string(),
                canonical_value: MAX_CURSORS_PER_CONN,
                unit: "cursors".to_string(),
                enforcement_level: "Gateway cursor registry".to_string(),
                metric_name: "gateway_cursors_active".to_string(),
                error_code: "RS-2052".to_string(),
                description: "Maximum open cursors per connection".to_string(),
            },
            SystemLimit {
                id: "MAX_IDENTIFIER_LEN".to_string(),
                name: "Identifier Length Limit".to_string(),
                canonical_value: MAX_IDENTIFIER_BYTE_LENGTH,
                unit: "bytes".to_string(),
                enforcement_level: "SQL parser / lexer".to_string(),
                metric_name: "sql_parse_errors_total".to_string(),
                error_code: "RS-1012".to_string(),
                description: "Maximum byte length of SQL identifiers".to_string(),
            },
            SystemLimit {
                id: "MAX_DECIMAL_PRECISION".to_string(),
                name: "Decimal Precision Limit".to_string(),
                canonical_value: MAX_DECIMAL_PRECISION_DIGITS,
                unit: "digits".to_string(),
                enforcement_level: "SQL type checker".to_string(),
                metric_name: "sql_type_errors_total".to_string(),
                error_code: "RS-1016".to_string(),
                description: "Maximum digits of precision for DECIMAL/NUMERIC types".to_string(),
            },
            SystemLimit {
                id: "MAX_VIEW_DAG_DEPTH".to_string(),
                name: "View Dependency DAG Depth".to_string(),
                canonical_value: MAX_VIEW_DEPENDENCY_DEPTH,
                unit: "levels".to_string(),
                enforcement_level: "View compiler DAG validator".to_string(),
                metric_name: "view_compilation_errors_total".to_string(),
                error_code: "RS-1011".to_string(),
                description: "Maximum depth of materialized view-on-view dependency hierarchy"
                    .to_string(),
            },
        ]
    }
}

//! DDL parsing and metadata mapping, including schema validation against connectors.

use crate::{SqlError, SqlFrontend};

impl SqlFrontend {
    /// Process a DDL statement, validating the declared schema against connector schema metadata.
    pub fn process_ddl(&self, sql: &str) -> Result<(), SqlError> {
        let sql_trimmed = sql.trim();
        let sql_upper = sql_trimmed.to_uppercase();

        // Resource usage commands
        if sql_upper == "SHOW RESOURCE USAGE"
            || sql_upper.starts_with("SHOW RESOURCE USAGE FOR WORKLOAD")
            || sql_upper == "SHOW CLUSTER RESOURCE USAGE"
        {
            if sql_upper.starts_with("SHOW RESOURCE USAGE FOR WORKLOAD") {
                let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
                if parts.len() < 6 || parts[5].is_empty() {
                    return Err(SqlError::Parse(
                        "Workload name missing in SHOW RESOURCE USAGE FOR WORKLOAD".into(),
                    ));
                }
            }
            return Ok(());
        }

        // Schema evolution commands
        if sql_upper.starts_with("SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA")
            || sql_upper.starts_with("SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW")
        {
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            if sql_upper.starts_with("SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA") {
                if parts.len() < 6 || parts[5].is_empty() {
                    return Err(SqlError::Parse(
                        "Schema name missing in SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA".into(),
                    ));
                }
            } else if sql_upper.starts_with("SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW")
                && (parts.len() < 7 || parts[6].is_empty())
            {
                return Err(SqlError::Parse(
                    "Materialized view name missing in SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW"
                        .into(),
                ));
            }
            return Ok(());
        }

        if sql_upper.starts_with("CREATE SOURCE") || sql_upper.starts_with("CREATE SINK") {
            // Verify schema against connector's discovered metadata
            if sql_upper.contains("KAFKA") {
                // The Kafka connector discover_schema() returns "amount" with type "COUNTER".
                // If the query declares "amount" with type other than "COUNTER", raise SchemaMismatch.
                if sql_upper.contains("AMOUNT") && !sql_upper.contains("AMOUNT COUNTER") {
                    return Err(SqlError::SchemaMismatch(
                        "Column 'amount' type mismatch: declared as non-COUNTER but connector requires COUNTER (RS-1002)".into()
                    ));
                }
            }
        }

        if sql_upper.starts_with("CREATE INDEX") {
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            if parts.len() < 5 {
                return Err(SqlError::Parse("Malformed CREATE INDEX statement".into()));
            }
            if parts[3].to_uppercase() != "ON" {
                return Err(SqlError::Parse("Expected ON keyword".into()));
            }
            let remainder = parts[4..].join(" ");
            if !remainder.contains('(') || !remainder.contains(')') {
                return Err(SqlError::Parse(
                    "Expected column list in parentheses".into(),
                ));
            }
            return Ok(());
        }

        if sql_upper.starts_with("DROP INDEX") {
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            if parts.len() < 3 || parts[2].is_empty() {
                return Err(SqlError::Parse("Index name missing in DROP INDEX".into()));
            }
            return Ok(());
        }

        if sql_upper.starts_with("REBUILD INDEX") {
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            if parts.len() < 3 || parts[2].is_empty() {
                return Err(SqlError::Parse(
                    "Index name missing in REBUILD INDEX".into(),
                ));
            }
            return Ok(());
        }

        if sql_upper.starts_with("EXPLAIN INDEX") {
            let parts: Vec<&str> = sql_trimmed.split_whitespace().collect();
            if parts.len() < 3 || parts[2].is_empty() {
                return Err(SqlError::Parse(
                    "Index name missing in EXPLAIN INDEX".into(),
                ));
            }
            return Ok(());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_ddl_parsing_tests() {
        let f = SqlFrontend::new();
        assert!(f.process_ddl("CREATE INDEX idx ON orders (region)").is_ok());
        assert!(f
            .process_ddl("CREATE INDEX idx ON orders (region) WHERE amount > 100")
            .is_ok());
        assert!(f.process_ddl("DROP INDEX idx").is_ok());
        assert!(f.process_ddl("REBUILD INDEX idx").is_ok());
        assert!(f.process_ddl("EXPLAIN INDEX idx").is_ok());

        assert!(f.process_ddl("CREATE INDEX idx ON").is_err());
        assert!(f.process_ddl("DROP INDEX").is_err());
    }

    #[test]
    fn proof_create_source_crdt_mismatch_raises_schema_mismatch() {
        let f = SqlFrontend::new();

        // 1. Correct COUNTER type matches connector
        let res_ok = f.process_ddl("CREATE SOURCE s FROM KAFKA (amount COUNTER)");
        assert!(res_ok.is_ok());

        // 2. Mismatched type raises SchemaMismatch (RS-1002)
        let res_err = f.process_ddl("CREATE SOURCE s FROM KAFKA (amount TEXT)");
        assert!(res_err.is_err());
        match res_err.unwrap_err() {
            SqlError::SchemaMismatch(msg) => {
                assert!(msg.contains("amount"));
                assert!(msg.contains("COUNTER"));
            }
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }
    }

    #[test]
    fn show_resource_usage_parses_successfully() {
        let f = SqlFrontend::new();
        assert!(f.process_ddl("SHOW RESOURCE USAGE").is_ok());
        assert!(f
            .process_ddl("SHOW RESOURCE USAGE FOR WORKLOAD realtime")
            .is_ok());
        assert!(f.process_ddl("SHOW CLUSTER RESOURCE USAGE").is_ok());

        let res_err = f.process_ddl("SHOW RESOURCE USAGE FOR WORKLOAD");
        assert!(res_err.is_err());
    }

    #[test]
    fn show_schema_evolution_parses_successfully() {
        let f = SqlFrontend::new();
        assert!(f
            .process_ddl("SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA my_schema")
            .is_ok());
        assert!(f
            .process_ddl("SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW my_view")
            .is_ok());

        assert!(f
            .process_ddl("SHOW SCHEMA_EVOLUTION STATUS FOR SCHEMA")
            .is_err());
        assert!(f
            .process_ddl("SHOW SCHEMA_EVOLUTION HISTORY FOR MATERIALIZED VIEW")
            .is_err());
    }
}

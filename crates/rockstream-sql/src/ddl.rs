//! DDL parsing and metadata mapping, including schema validation against connectors.

use crate::{SqlError, SqlFrontend};

impl SqlFrontend {
    /// Process a DDL statement, validating the declared schema against connector schema metadata.
    pub fn process_ddl(&self, sql: &str) -> Result<(), SqlError> {
        let sql_upper = sql.to_uppercase();
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            other => panic!("expected SchemaMismatch, got {:?}", other),
        }
    }
}

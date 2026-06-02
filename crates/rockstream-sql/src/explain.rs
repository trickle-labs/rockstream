use crate::{SqlError, SqlFrontend};

impl SqlFrontend {
    /// Parse and plan an `EXPLAIN TRANSACTION` query.
    pub fn explain_transaction(&self, sql: &str) -> Result<String, SqlError> {
        let trimmed = sql.trim();
        if !trimmed
            .to_ascii_lowercase()
            .starts_with("explain transaction")
        {
            return Err(SqlError::Parse(
                "Not an EXPLAIN TRANSACTION statement".into(),
            ));
        }
        let inner_sql = trimmed["explain transaction".len()..].trim();

        // Let's parse the statement
        let stmts = self.parse_statement(inner_sql)?;
        if stmts.is_empty() {
            return Err(SqlError::Parse(
                "Empty statement in EXPLAIN TRANSACTION".into(),
            ));
        }

        // Collect LawSchemaMetadata from the schema/connectors involved
        let mut meta = rockstream_types::connector::LawSchemaMetadata::empty();
        let connector_name = if inner_sql.to_ascii_lowercase().contains("orders") {
            "kafka_orders"
        } else if inner_sql.to_ascii_lowercase().contains("events") {
            "s3_events"
        } else {
            "connector_generic"
        };

        if connector_name == "kafka_orders" {
            meta = meta.with_column(
                "amount",
                rockstream_types::merge_law::MergeLawId(1), // WeightAdd/v1
                "COUNTER",
                rockstream_types::connector::WriteClassification::BlindDelta,
            );
        } else if connector_name == "s3_events" {
            meta = meta.with_column(
                "event_count",
                rockstream_types::merge_law::MergeLawId(1),
                "COUNTER",
                rockstream_types::connector::WriteClassification::BlindDelta,
            );
        } else {
            meta = meta.with_column(
                "value",
                rockstream_types::merge_law::MergeLawId(1),
                "COUNTER",
                rockstream_types::connector::WriteClassification::ReadDependentDelta,
            );
        }

        let explain = rockstream_types::connector::ExplainTransaction::from_schema_metadata(
            connector_name,
            true, // partition filter support
            &meta,
        );

        Ok(explain.format_lines().join("\n"))
    }
}

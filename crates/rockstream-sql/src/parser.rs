use crate::{SqlError, SqlFrontend};
use datafusion::sql::parser::{DFParser, Statement};

impl SqlFrontend {
    /// Parse a SQL string into a list of DataFusion `Statement`s.
    pub fn parse_statement(&self, sql: &str) -> Result<Vec<Statement>, SqlError> {
        DFParser::parse_sql_with_dialect(sql, &self.dialect)
            .map(|stmts| stmts.into_iter().collect())
            .map_err(|e| SqlError::Parse(e.to_string()))
    }
}

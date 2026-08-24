//! SQL Contract matrix surface contributor (DOC-001).

use crate::manifest::SqlContractSurface;
use crate::sql_matrix::{SqlMatrixDocument, SqlMatrixError};

pub struct SqlContractContributor;

impl SqlContractContributor {
    /// Extract SQL contract compatibility surface from `contracts/sql-type-matrix.toml`.
    pub fn extract() -> Result<SqlContractSurface, SqlMatrixError> {
        let doc = SqlMatrixDocument::load_canonical()?;
        Ok(doc.to_surface())
    }
}

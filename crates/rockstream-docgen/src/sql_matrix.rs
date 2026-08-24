//! Parser, Validator, and Conformance Checker for `contracts/sql-type-matrix.toml` (DOC-001).

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use thiserror::Error;

use crate::manifest::{SqlContractSurface, SqlTypeContract};

/// Errors encountered when parsing or validating `contracts/sql-type-matrix.toml`.
#[derive(Debug, Error)]
pub enum SqlMatrixError {
    #[error("Failed to parse TOML: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Unknown error code referenced: {0}")]
    UnknownErrorCode(String),
}

/// Metadata header for `contracts/sql-type-matrix.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlMatrixContractHeader {
    pub version: String,
    pub roadmap: String,
    pub description: String,
}

/// Root document representation for `contracts/sql-type-matrix.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlMatrixDocument {
    pub contract: SqlMatrixContractHeader,
    #[serde(rename = "type", default)]
    pub types: Vec<SqlTypeContract>,
}

impl SqlMatrixDocument {
    /// Parse from a raw TOML string.
    pub fn parse(toml_str: &str) -> Result<Self, SqlMatrixError> {
        let doc: SqlMatrixDocument = toml::from_str(toml_str)?;
        doc.validate()?;
        Ok(doc)
    }

    /// Load from the canonical embedded or referenced file content.
    pub fn load_canonical() -> Result<Self, SqlMatrixError> {
        const DEFAULT_TOML: &str = include_str!("../../../contracts/sql-type-matrix.toml");
        Self::parse(DEFAULT_TOML)
    }

    /// Validate document invariants:
    /// 1. Type names are unique.
    /// 2. All operations have valid status tiers ("Core", "Supported", "Experimental", "Unsupported").
    /// 3. Unsupported operations declare a valid `rejection_code` (e.g. `RS-1012`, `RS-1013`, `RS-1016`, `RS-1019`, `RS-1020`, `RS-1021`).
    /// 4. Rejection codes exist in the authoritative error catalog.
    pub fn validate(&self) -> Result<(), SqlMatrixError> {
        let mut seen_types = HashSet::new();
        let valid_statuses = ["Core", "Supported", "Experimental", "Unsupported"];

        for ty in &self.types {
            if !seen_types.insert(ty.name.to_uppercase()) {
                return Err(SqlMatrixError::Validation(format!(
                    "Duplicate SQL type declaration: {}",
                    ty.name
                )));
            }

            if ty.operations.is_empty() {
                return Err(SqlMatrixError::Validation(format!(
                    "SQL type '{}' has no operation declarations",
                    ty.name
                )));
            }

            let mut seen_ops = HashSet::new();
            for op in &ty.operations {
                if !seen_ops.insert(op.operation.to_lowercase()) {
                    return Err(SqlMatrixError::Validation(format!(
                        "SQL type '{}' has duplicate operation: {}",
                        ty.name, op.operation
                    )));
                }

                if !valid_statuses.contains(&op.status.as_str()) {
                    return Err(SqlMatrixError::Validation(format!(
                        "SQL type '{}' operation '{}' has invalid status: '{}'",
                        ty.name, op.operation, op.status
                    )));
                }

                if op.status == "Unsupported" {
                    let code = op.rejection_code.as_ref().ok_or_else(|| {
                        SqlMatrixError::Validation(format!(
                            "SQL type '{}' operation '{}' is marked Unsupported but declares no rejection_code",
                            ty.name, op.operation
                        ))
                    })?;

                    if !code.starts_with("RS-") {
                        return Err(SqlMatrixError::Validation(format!(
                            "SQL type '{}' operation '{}' rejection_code '{}' does not match RS-XXXX format",
                            ty.name, op.operation, code
                        )));
                    }

                    // Verify error code exists in ErrorCatalog
                    if !rockstream_types::error_code::ErrorCatalog::current()
                        .errors()
                        .iter()
                        .any(|e| e.code.to_string() == *code)
                    {
                        return Err(SqlMatrixError::UnknownErrorCode(format!(
                            "SQL type '{}' operation '{}' references unknown error code '{}'",
                            ty.name, op.operation, code
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Convert to normalized `SqlContractSurface`.
    pub fn to_surface(&self) -> SqlContractSurface {
        let mut surface = SqlContractSurface {
            types: self.types.clone(),
        };
        surface.types.sort_by(|a, b| a.name.cmp(&b.name));
        for t in &mut surface.types {
            t.operations.sort_by(|a, b| a.operation.cmp(&b.operation));
        }
        surface
    }
}

//! Normalized Product Surface Manifest Domain Models (DOC-001).
//!
//! Strongly typed models representing the unified product surface across CLI, configuration,
//! SQL functions, system catalog tables, Prometheus metrics, error codes, and the SQL type contract.

use serde::{Deserialize, Serialize};

/// Top-level unified product surface manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProductSurfaceManifest {
    pub manifest_metadata: ManifestMetadata,
    pub cli_surface: CliSurface,
    pub config_surface: ConfigSurface,
    pub function_surface: FunctionSurface,
    pub catalog_surface: CatalogSurface,
    pub metric_surface: MetricSurface,
    pub error_surface: ErrorSurface,
    pub sql_contract_surface: SqlContractSurface,
}

impl ProductSurfaceManifest {
    /// Sort all sub-surfaces and array collections into canonical, deterministic order.
    pub fn sort_canonical(&mut self) {
        self.cli_surface
            .commands
            .sort_by(|a, b| a.name.cmp(&b.name));
        for cmd in &mut self.cli_surface.commands {
            cmd.sort_canonical();
        }

        self.config_surface
            .options
            .sort_by(|a, b| a.key.cmp(&b.key));

        self.function_surface.functions.sort_by(|a, b| {
            (a.name.as_str(), a.signature.as_str()).cmp(&(b.name.as_str(), b.signature.as_str()))
        });

        self.catalog_surface
            .schemas
            .sort_by(|a, b| a.name.cmp(&b.name));
        for schema in &mut self.catalog_surface.schemas {
            schema.tables.sort_by(|a, b| a.name.cmp(&b.name));
            for table in &mut schema.tables {
                table.columns.sort_by_key(|c| c.ordinal_position);
            }
        }

        self.metric_surface
            .metrics
            .sort_by(|a, b| a.name.cmp(&b.name));
        self.error_surface
            .errors
            .sort_by(|a, b| a.code.cmp(&b.code));

        self.sql_contract_surface
            .types
            .sort_by(|a, b| a.name.cmp(&b.name));
        for ty in &mut self.sql_contract_surface.types {
            ty.operations.sort_by(|a, b| a.operation.cmp(&b.operation));
        }
    }

    /// Serialize manifest to deterministic canonical JSON string.
    pub fn to_canonical_json(&self) -> Result<String, serde_json::Error> {
        let mut cloned = self.clone();
        cloned.sort_canonical();
        serde_json::to_string_pretty(&cloned)
    }
}

/// Metadata header for the product surface manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestMetadata {
    pub schema_version: String,
    pub engine_version: String,
    pub candidate_identity_digest: String,
    pub generator_version: String,
    pub generated_at: String,
}

/// CLI surface descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CliSurface {
    pub commands: Vec<CliCommandDescriptor>,
}

/// CLI command descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliCommandDescriptor {
    pub name: String,
    pub about: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subcommands: Vec<CliCommandDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<CliOptionDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exit_codes: Vec<CliExitCodeDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub error_codes: Vec<String>,
}

impl CliCommandDescriptor {
    pub fn sort_canonical(&mut self) {
        self.subcommands.sort_by(|a, b| a.name.cmp(&b.name));
        for sub in &mut self.subcommands {
            sub.sort_canonical();
        }
        self.options.sort_by(|a, b| a.name.cmp(&b.name));
        self.exit_codes.sort_by_key(|e| e.code);
        self.error_codes.sort();
    }
}

/// CLI option / argument descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliOptionDescriptor {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub short: Option<char>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long: Option<String>,
    pub help: String,
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub possible_values: Vec<String>,
}

/// CLI exit code descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliExitCodeDescriptor {
    pub code: i32,
    pub title: String,
    pub description: String,
}

/// Configuration surface descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConfigSurface {
    pub options: Vec<ConfigOptionDescriptor>,
}

/// Single configuration option descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigOptionDescriptor {
    pub key: String,
    pub data_type: String,
    pub default_value: String,
    pub description: String,
    pub deprecated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env_var: Option<String>,
    pub source_origin: String,
}

/// SQL and UDF Function surface descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FunctionSurface {
    pub functions: Vec<FunctionDescriptor>,
}

/// Function descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionDescriptor {
    pub name: String,
    pub category: String,
    pub signature: String,
    pub argument_types: Vec<String>,
    pub return_type: String,
    pub null_handling: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<String>,
}

/// System Catalog surface descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CatalogSurface {
    pub schemas: Vec<CatalogSchemaDescriptor>,
}

/// System Catalog schema descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSchemaDescriptor {
    pub name: String,
    pub description: String,
    pub tables: Vec<CatalogTableDescriptor>,
}

/// System Catalog table descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogTableDescriptor {
    pub name: String,
    pub description: String,
    pub columns: Vec<CatalogColumnDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cardinality_bound: Option<String>,
}

/// System Catalog column descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogColumnDescriptor {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub ordinal_position: u32,
    pub description: String,
}

/// Prometheus Metric surface descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MetricSurface {
    pub metrics: Vec<MetricDescriptor>,
}

/// Single metric descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricDescriptor {
    pub name: String,
    pub metric_type: String,
    pub unit: String,
    pub labels: Vec<String>,
    pub stability: String,
    pub description: String,
}

/// Error catalog surface descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ErrorSurface {
    pub errors: Vec<ErrorSurfaceEntry>,
}

/// Single error surface entry matching contracts/errors.toml.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorSurfaceEntry {
    pub code: String,
    pub key: String,
    pub title: String,
    pub severity: String,
    pub sqlstate: String,
    pub retry_class: String,
    pub default_next_steps: String,
    pub doc_anchor: String,
}

/// SQL contract compatibility surface descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SqlContractSurface {
    pub types: Vec<SqlTypeContract>,
}

/// SQL type contract definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlTypeContract {
    pub name: String,
    pub family: String,
    pub aliases: Vec<String>,
    pub description: String,
    pub operations: Vec<SqlOperationContract>,
}

/// SQL operation contract definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlOperationContract {
    pub operation: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

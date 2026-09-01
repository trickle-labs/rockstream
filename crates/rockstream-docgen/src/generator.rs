//! Deterministic Product Surface Manifest Generator (DOC-001, DOC-004).

use sha2::{Digest, Sha256};
use std::error::Error;

use crate::contributors::{
    CatalogContributor, CliContributor, ConfigContributor, ErrorContributor, FunctionContributor,
    MetricContributor, SqlContractContributor,
};
use crate::manifest::{ManifestMetadata, ProductSurfaceManifest};

/// Generate the full, unified `ProductSurfaceManifest` by running all surface contributors.
pub fn generate_manifest() -> Result<ProductSurfaceManifest, Box<dyn Error + Send + Sync>> {
    let cli_surface = CliContributor::extract();
    let config_surface = ConfigContributor::extract();
    let function_surface = FunctionContributor::extract();
    let catalog_surface = CatalogContributor::extract();
    let metric_surface = MetricContributor::extract();
    let error_surface = ErrorContributor::extract();
    let sql_contract_surface = SqlContractContributor::extract()?;

    // Compute deterministic candidate digest based on contracts
    let mut hasher = Sha256::new();
    hasher.update(b"rockstream-0.59.19-product-surface");
    for err in &error_surface.errors {
        hasher.update(err.code.as_bytes());
        hasher.update(err.key.as_bytes());
    }
    for ty in &sql_contract_surface.types {
        hasher.update(ty.name.as_bytes());
        for op in &ty.operations {
            hasher.update(op.operation.as_bytes());
            hasher.update(op.status.as_bytes());
        }
    }
    let candidate_identity_digest = format!("{:x}", hasher.finalize());

    let manifest_metadata = ManifestMetadata {
        schema_version: "1.0.0".to_string(),
        engine_version: "0.59.19".to_string(),
        candidate_identity_digest,
        generator_version: "1.0.0".to_string(),
        generated_at: "2026-08-24T00:00:00Z".to_string(),
    };

    let mut manifest = ProductSurfaceManifest {
        manifest_metadata,
        cli_surface,
        config_surface,
        function_surface,
        catalog_surface,
        metric_surface,
        error_surface,
        sql_contract_surface,
    };

    manifest.sort_canonical();
    Ok(manifest)
}

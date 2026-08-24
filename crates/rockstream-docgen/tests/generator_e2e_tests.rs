//! Generator E2E tests for RockStream Product Surface (DOC-001, DOC-004).

use rockstream_docgen::generate_manifest;
use std::fs;
use std::path::Path;

#[test]
fn test_generated_manifest_matches_docs_json() {
    let manifest = generate_manifest().expect("manifest generation must succeed");
    let generated_json = manifest
        .to_canonical_json()
        .expect("canonical JSON serialization must succeed");

    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let docs_json_path = repo_root.join("docs").join("product-surface.json");

    assert!(
        docs_json_path.exists(),
        "docs/product-surface.json must exist at {}",
        docs_json_path.display()
    );

    let existing_json =
        fs::read_to_string(&docs_json_path).expect("failed to read docs/product-surface.json");

    assert_eq!(
        generated_json.trim(),
        existing_json.trim(),
        "Generated product surface manifest must match docs/product-surface.json byte-for-byte"
    );

    // Verify metadata invariants
    assert_eq!(manifest.manifest_metadata.schema_version, "1.0.0");
    assert_eq!(manifest.manifest_metadata.engine_version, "0.59.13");
    assert_eq!(manifest.manifest_metadata.generator_version, "1.0.0");
    assert!(!manifest
        .manifest_metadata
        .candidate_identity_digest
        .is_empty());

    // Verify all surface components are populated
    assert!(
        !manifest.cli_surface.commands.is_empty(),
        "CLI surface must not be empty"
    );
    assert!(
        !manifest.config_surface.options.is_empty(),
        "Config surface must not be empty"
    );
    assert!(
        !manifest.function_surface.functions.is_empty(),
        "Function surface must not be empty"
    );
    assert!(
        !manifest.catalog_surface.schemas.is_empty(),
        "Catalog surface must not be empty"
    );
    assert!(
        !manifest.catalog_surface.schemas[0].tables.is_empty(),
        "Catalog schemas must have tables"
    );
    assert!(
        !manifest.metric_surface.metrics.is_empty(),
        "Metric surface must not be empty"
    );
    assert!(
        !manifest.error_surface.errors.is_empty(),
        "Error surface must not be empty"
    );
    assert!(
        !manifest.sql_contract_surface.types.is_empty(),
        "SQL contract surface must not be empty"
    );
}

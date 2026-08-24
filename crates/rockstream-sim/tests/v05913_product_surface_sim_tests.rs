//! Deterministic simulation tests for Single-Source Product Surface (v0.59.13 / DOC-001, DOC-004).

use rockstream_docgen::{generate_manifest, SqlMatrixDocument};
use rockstream_sim::buggify;
use rockstream_sim::buggify::buggify_init;
use rockstream_types::error_code::ErrorCatalog;

#[tokio::test]
async fn test_manifest_invariant_under_fault_injection() {
    let catalog = ErrorCatalog::current();
    assert!(!catalog.errors().is_empty());

    let matrix_doc = SqlMatrixDocument::load_canonical().expect("SQL type matrix must load");
    assert!(!matrix_doc.types.is_empty());

    for seed in 59130..59180 {
        buggify_init(seed);

        // 1. Serialization determinism stress under simulated fault injection
        let stress_serialization = buggify!("v05913.docgen.manifest_serialization", 0.5);
        let m1 = generate_manifest().expect("manifest generation must succeed");
        let j1 = m1.to_canonical_json().expect("canonical json must succeed");

        if stress_serialization {
            let m2 = generate_manifest().expect("second manifest generation must succeed");
            let j2 = m2
                .to_canonical_json()
                .expect("second canonical json must succeed");
            assert_eq!(
                j1, j2,
                "Deterministic serialization must produce bit-identical output under seed {seed}"
            );
            assert!(
                !j1.contains("\r\n"),
                "Normalized LF line endings required under seed {seed}"
            );
        }

        // 2. High-concurrency / randomized ID resolution stress
        let stress_ids = buggify!("v05913.docgen.id_resolution_stress", 0.5);
        if stress_ids {
            // Verify all CLI command error codes exist in catalog
            for cmd in &m1.cli_surface.commands {
                for code in &cmd.error_codes {
                    let exists = catalog.errors().iter().any(|e| e.code.to_string() == *code);
                    assert!(
                        exists,
                        "Seed {seed}: CLI command {} references unknown error code {code}",
                        cmd.name
                    );
                }
            }

            // Verify all SQL contract rejection codes exist in catalog
            for ty in &m1.sql_contract_surface.types {
                for op in &ty.operations {
                    if let Some(code) = &op.rejection_code {
                        let exists = catalog.errors().iter().any(|e| e.code.to_string() == *code);
                        assert!(
                            exists,
                            "Seed {seed}: Type {} op {} references unknown rejection code {code}",
                            ty.name, op.operation
                        );
                    }
                }
            }

            // Verify all error surface entries exist in catalog
            for err in &m1.error_surface.errors {
                let exists = catalog
                    .errors()
                    .iter()
                    .any(|e| e.code.to_string() == err.code);
                assert!(
                    exists,
                    "Seed {seed}: Error surface entry {} missing from catalog",
                    err.code
                );
            }
        }

        // 3. Invariants on manifest metadata
        assert_eq!(m1.manifest_metadata.schema_version, "1.0.0");
        assert_eq!(m1.manifest_metadata.engine_version, "0.59.13");
        assert_eq!(m1.manifest_metadata.generator_version, "1.0.0");
        assert!(!m1.manifest_metadata.candidate_identity_digest.is_empty());
    }
}

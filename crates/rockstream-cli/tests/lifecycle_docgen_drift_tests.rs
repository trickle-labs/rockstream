//! Lifecycle Documentation & Product Surface Conformance Tests (v0.59.21 Slice 7 / Phase 3b).

use std::fs;
use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn test_lifecycle_documentation_and_gate_conformance() {
    let root = workspace_root();

    // 1. Verify docs/lifecycle.md existence and core sections
    let lifecycle_doc_path = root.join("docs/lifecycle.md");
    let content = fs::read_to_string(&lifecycle_doc_path).unwrap_or_else(|_| {
        panic!(
            "docs/lifecycle.md must exist at {}",
            lifecycle_doc_path.display()
        )
    });

    // Verify all lifecycle states are documented
    let required_states = [
        "Starting",
        "Ready",
        "Degraded",
        "DependencyLoss",
        "Draining",
        "ShuttingDown",
        "Terminated",
        "Fatal",
    ];
    for state in required_states {
        assert!(
            content.contains(state),
            "docs/lifecycle.md must document state '{state}'"
        );
    }

    // Verify all management HTTP probe endpoints are documented
    let required_probes = ["/live", "/ready", "/health"];
    for probe in required_probes {
        assert!(
            content.contains(probe),
            "docs/lifecycle.md must document probe '{probe}'"
        );
    }

    // Verify lifecycle error codes are documented
    let required_codes = ["RS-3023", "RS-3010", "RS-2056"];
    for code in required_codes {
        assert!(
            content.contains(code),
            "docs/lifecycle.md must document error code '{code}'"
        );
    }

    // 2. Verify product surface manifest drift
    let manifest_path = root.join("docs/product-surface.json");
    let existing_json = fs::read_to_string(&manifest_path).unwrap_or_else(|_| {
        panic!(
            "docs/product-surface.json must exist at {}",
            manifest_path.display()
        )
    });

    let generated_manifest = rockstream_docgen::generate_manifest()
        .expect("rockstream_docgen::generate_manifest must succeed");
    let generated_json = generated_manifest
        .to_canonical_json()
        .expect("generate canonical json must succeed");

    assert_eq!(
        existing_json.trim(),
        generated_json.trim(),
        "docs/product-surface.json must match live code without drift"
    );
}

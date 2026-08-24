//! Tests for ProductSurfaceManifest determinism, key sorting, array ordering, and ID resolution (DOC-001, DOC-004).

use rockstream_docgen::generate_manifest;
use rockstream_types::error_code::ErrorCatalog;

#[test]
fn test_manifest_key_sorting() {
    let manifest = generate_manifest().expect("manifest generation must succeed");
    let json_str = manifest
        .to_canonical_json()
        .expect("canonical JSON serialization must succeed");

    // Parse back to Value to check top-level keys
    let val: serde_json::Value = serde_json::from_str(&json_str).unwrap();
    let obj = val.as_object().unwrap();

    let keys: Vec<&String> = obj.keys().collect();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(
        keys, sorted_keys,
        "Top level JSON keys must be alphabetically sorted"
    );
}

#[test]
fn test_manifest_array_ordering() {
    let manifest = generate_manifest().expect("manifest generation must succeed");

    // Check CLI commands sorted by name
    let cmd_names: Vec<&str> = manifest
        .cli_surface
        .commands
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    let mut sorted_cmds = cmd_names.clone();
    sorted_cmds.sort();
    assert_eq!(
        cmd_names, sorted_cmds,
        "CLI commands must be sorted by name"
    );

    // Check Config options sorted by key
    let config_keys: Vec<&str> = manifest
        .config_surface
        .options
        .iter()
        .map(|c| c.key.as_str())
        .collect();
    let mut sorted_configs = config_keys.clone();
    sorted_configs.sort();
    assert_eq!(
        config_keys, sorted_configs,
        "Config options must be sorted by key"
    );

    // Check Errors sorted by code
    let error_codes: Vec<&str> = manifest
        .error_surface
        .errors
        .iter()
        .map(|e| e.code.as_str())
        .collect();
    let mut sorted_errors = error_codes.clone();
    sorted_errors.sort();
    assert_eq!(
        error_codes, sorted_errors,
        "Error surface entries must be sorted by code"
    );

    // Check SQL types sorted by name
    let type_names: Vec<&str> = manifest
        .sql_contract_surface
        .types
        .iter()
        .map(|t| t.name.as_str())
        .collect();
    let mut sorted_types = type_names.clone();
    sorted_types.sort();
    assert_eq!(type_names, sorted_types, "SQL types must be sorted by name");
}

#[test]
fn test_cross_platform_byte_equality() {
    let manifest1 = generate_manifest().unwrap();
    let json1 = manifest1.to_canonical_json().unwrap();

    let manifest2 = generate_manifest().unwrap();
    let json2 = manifest2.to_canonical_json().unwrap();

    assert_eq!(
        json1, json2,
        "Multiple runs must produce bit-for-bit identical outputs"
    );
    assert!(
        !json1.contains("\r\n"),
        "Output must contain normalized LF newlines"
    );
}

#[test]
fn test_all_public_ids_resolve() {
    let manifest = generate_manifest().unwrap();
    let catalog = ErrorCatalog::current();

    // Verify all error codes in ErrorSurface exist in ErrorCatalog
    for err in &manifest.error_surface.errors {
        let found = catalog
            .errors()
            .iter()
            .any(|e| e.code.to_string() == err.code);
        assert!(
            found,
            "Error code {} in manifest must exist in ErrorCatalog",
            err.code
        );
    }

    // Verify all rejection codes in SQL type matrix exist in ErrorCatalog
    for ty in &manifest.sql_contract_surface.types {
        for op in &ty.operations {
            if let Some(code) = &op.rejection_code {
                let found = catalog.errors().iter().any(|e| e.code.to_string() == *code);
                assert!(
                    found,
                    "Rejection code {} in type {} op {} must exist in ErrorCatalog",
                    code, ty.name, op.operation
                );
            }
        }
    }

    // Verify all error codes referenced in CLI commands exist in ErrorCatalog
    for cmd in &manifest.cli_surface.commands {
        for code in &cmd.error_codes {
            let found = catalog.errors().iter().any(|e| e.code.to_string() == *code);
            assert!(
                found,
                "CLI command {} error code reference {} must exist in ErrorCatalog",
                cmd.name, code
            );
        }
    }
}

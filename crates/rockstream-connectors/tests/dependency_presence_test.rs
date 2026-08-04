//! Test asserting that Cargo.toml carries real rdkafka and aws-sdk/object_store dependencies.

use std::fs;
use std::path::Path;

#[test]
fn test_dependency_presence() {
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let content = fs::read_to_string(&manifest_path).unwrap();

    assert!(
        content.contains("rdkafka"),
        "crates/rockstream-connectors/Cargo.toml must specify rdkafka dependency"
    );
    assert!(
        content.contains("aws-sdk-s3") || content.contains("object_store"),
        "crates/rockstream-connectors/Cargo.toml must specify aws-sdk-s3 or object_store dependency"
    );
}

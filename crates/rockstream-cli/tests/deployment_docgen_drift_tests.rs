//! Platform Documentation & Contract Drift Tests (v0.59.22 Slice 6 / Phase 3a).

use std::fs;
use std::path::Path;

#[test]
fn test_platform_documentation_and_contract_drift() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();

    let contract_path = root.join("contracts/platform-matrix.toml");
    let contract_str =
        fs::read_to_string(&contract_path).expect("contracts/platform-matrix.toml must exist");

    let platforms_doc_path = root.join("docs/platforms.md");
    let platforms_doc_str =
        fs::read_to_string(&platforms_doc_path).expect("docs/platforms.md must exist");

    let deployment_doc_path = root.join("docs/deployment-profiles.md");
    let deployment_doc_str =
        fs::read_to_string(&deployment_doc_path).expect("docs/deployment-profiles.md must exist");

    let upgrades_doc_path = root.join("docs/rolling-upgrades.md");
    let upgrades_doc_str =
        fs::read_to_string(&upgrades_doc_path).expect("docs/rolling-upgrades.md must exist");

    // 1. Verify contract version and key architectures in documentation
    assert!(contract_str.contains("version = \"0.59.22\""));
    assert!(platforms_doc_str.contains("x86_64"));
    assert!(platforms_doc_str.contains("aarch64"));
    assert!(platforms_doc_str.contains("Supported"));
    assert!(platforms_doc_str.contains("Compatible, unverified"));
    assert!(platforms_doc_str.contains("Unsupported"));

    // 2. Verify deployment guides cover all 4 profiles
    assert!(deployment_doc_str.contains("OCI Container (`docker run`)"));
    assert!(deployment_doc_str.contains("Docker Compose"));
    assert!(deployment_doc_str.contains("Systemd Service Profiles"));
    assert!(deployment_doc_str.contains("Kubernetes & Minimal Helm Chart"));

    // 3. Verify rolling upgrade steps
    assert!(upgrades_doc_str.contains("Step 1: Control Plane Nodes"));
    assert!(upgrades_doc_str.contains("Step 2: Worker Nodes"));
    assert!(upgrades_doc_str.contains("Step 3: Gateway Nodes"));
    assert!(upgrades_doc_str.contains("ReleaseLease"));
}

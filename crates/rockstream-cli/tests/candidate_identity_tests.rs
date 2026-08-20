use rockstream_cli::transport::{ClientIdentity, StorageClient};
use rockstream_types::acl::Role;
use rockstream_types::candidate_identity::CandidateIdentity;
use rockstream_types::metrics::generate_prometheus_metrics;
use std::path::Path;

#[test]
fn test_workspace_version_matches_candidate_identity() {
    let id = CandidateIdentity::current();
    assert_eq!(id.semantic_version, "0.59.6");

    let cargo_toml = std::fs::read_to_string("../../Cargo.toml")
        .or_else(|_| std::fs::read_to_string("../Cargo.toml"))
        .or_else(|_| std::fs::read_to_string("Cargo.toml"))
        .expect("Cargo.toml must exist");

    assert!(
        cargo_toml.contains("version = \"0.59.6\""),
        "Workspace Cargo.toml must declare version = \"0.59.6\""
    );
}

#[test]
fn test_cli_version_structured_identity() {
    let id = CandidateIdentity::current();
    let text = id.display_text();
    assert!(text.contains("rockstream 0.59.6"));
    assert!(text.contains(&format!("commit: {}", id.commit_sha)));
    assert!(text.contains(&format!("build_timestamp: {}", id.build_timestamp_rfc3339)));
    assert!(text.contains(&format!("compiler: {}", id.compiler_version)));
    assert!(text.contains(&format!("lockfile_digest: {}", id.lockfile_digest)));

    let json_str = id.to_json().expect("to_json must succeed");
    let v: serde_json::Value = serde_json::from_str(&json_str).expect("must parse JSON");
    assert_eq!(v["semantic_version"], "0.59.6");
    assert_eq!(v["commit_sha"], id.commit_sha);
    assert_eq!(v["build_timestamp_rfc3339"], id.build_timestamp_rfc3339);
    assert_eq!(v["compiler_version"], id.compiler_version);
    assert_eq!(v["lockfile_digest"], id.lockfile_digest);
    assert!(v["enabled_features"].is_array());
}

#[test]
fn test_prometheus_build_info_metric_matches_identity() {
    let id = CandidateIdentity::current();
    let metrics = generate_prometheus_metrics();
    assert!(
        metrics.contains("# HELP rockstream_build_info"),
        "Prometheus metrics must export HELP for rockstream_build_info"
    );
    assert!(
        metrics.contains("# TYPE rockstream_build_info gauge"),
        "Prometheus metrics must export TYPE gauge for rockstream_build_info"
    );

    let expected_pattern = format!(
        "rockstream_build_info{{version=\"{}\",commit_sha=\"{}\",build_timestamp=\"{}\",compiler_version=\"{}\",lockfile_digest=\"{}\",features=\"{}\"}} 1",
        id.semantic_version,
        id.commit_sha,
        id.build_timestamp_rfc3339,
        id.compiler_version,
        id.lockfile_digest,
        id.enabled_features.join(",")
    );
    assert!(
        metrics.contains(&expected_pattern),
        "Prometheus metrics must contain exact metric line:\n{expected_pattern}\nActual metrics:\n{metrics}"
    );
}

#[test]
fn test_support_bundle_contains_candidate_identity() {
    let id = CandidateIdentity::current();
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = StorageClient::with_identity(ClientIdentity::new("admin").with_role(Role::Admin));

    let bundle_info = storage
        .generate_support_bundle(tmp.path(), None, None, None)
        .expect("support bundle generation must succeed");

    let bundle_file = Path::new(&bundle_info.bundle_path);
    assert!(
        bundle_file.exists(),
        "Support bundle file must exist on disk"
    );

    let content = std::fs::read_to_string(bundle_file).expect("read support bundle");
    let v: serde_json::Value = serde_json::from_str(&content).expect("parse support bundle json");

    assert!(
        v.get("candidate_identity").is_some(),
        "Support bundle must contain candidate_identity block"
    );
    let bundle_id = &v["candidate_identity"];
    assert_eq!(bundle_id["semantic_version"], id.semantic_version);
    assert_eq!(bundle_id["commit_sha"], id.commit_sha);
    assert_eq!(bundle_id["lockfile_digest"], id.lockfile_digest);
    assert_eq!(bundle_id["compiler_version"], id.compiler_version);
}

#[test]
fn test_dockerfile_image_labels_match_identity() {
    let dockerfile = std::fs::read_to_string("../../Dockerfile")
        .or_else(|_| std::fs::read_to_string("../Dockerfile"))
        .or_else(|_| std::fs::read_to_string("Dockerfile"))
        .expect("Dockerfile must exist");

    assert!(
        dockerfile.contains("org.opencontainers.image.version=\"0.59.6\""),
        "Dockerfile must declare OCI version label matching candidate identity"
    );
    assert!(
        dockerfile.contains("org.opencontainers.image.revision="),
        "Dockerfile must declare OCI revision label"
    );
    assert!(
        dockerfile.contains("org.opencontainers.image.created="),
        "Dockerfile must declare OCI created label"
    );
    assert!(
        dockerfile.contains("rockstream.lockfile_digest="),
        "Dockerfile must declare lockfile_digest label"
    );
}

#[test]
fn test_manifest_docs_conformance() {
    let cap_toml = std::fs::read_to_string("../../capabilities.toml")
        .or_else(|_| std::fs::read_to_string("../capabilities.toml"))
        .or_else(|_| std::fs::read_to_string("capabilities.toml"))
        .expect("capabilities.toml must exist");

    assert!(
        cap_toml.contains("version = \"v0.59.6\""),
        "capabilities.toml contract version must be v0.59.6"
    );
}

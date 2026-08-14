//! CLI documentation coverage and conformance tests (v0.53 Slice 8).

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
fn test_docs_cli_conformance_and_coverage() {
    let root = workspace_root();
    let doc_path = root.join("docs/cli.md");
    let doc_content = fs::read_to_string(&doc_path)
        .unwrap_or_else(|_| panic!("docs/cli.md must exist at {}", doc_path.display()));

    // Required subcommands that must be documented
    let required_subcommands = [
        "rockstream start",
        "rockstream view",
        "rockstream view list",
        "rockstream view show",
        "rockstream view status",
        "rockstream source",
        "rockstream source list",
        "rockstream source show",
        "rockstream schema",
        "rockstream schema list",
        "rockstream schema show",
        "rockstream workload",
        "rockstream workload list",
        "rockstream workload show",
        "rockstream cluster",
        "rockstream cluster status",
        "rockstream cluster quotas",
        "rockstream cluster workers",
        "rockstream shard",
        "rockstream shard list",
        "rockstream checkpoint",
        "rockstream checkpoint list",
        "rockstream resource",
        "rockstream resource usage",
        "rockstream resource cluster",
        "rockstream schema-evolution",
        "rockstream schema-evolution status",
        "rockstream schema-evolution history",
        "rockstream audit",
        "rockstream audit tail",
        "rockstream audit query",
        "rockstream explain",
        "rockstream sql",
    ];

    for subcmd in &required_subcommands {
        assert!(
            doc_content.contains(subcmd),
            "docs/cli.md must document `{subcmd}`"
        );
    }

    // Required error codes documented
    let required_codes = [
        "RS-0002", "RS-0003", "RS-0004", "RS-1001", "RS-1005", "RS-1012", "RS-1731", "RS-4009",
    ];

    for code in &required_codes {
        assert!(
            doc_content.contains(code),
            "docs/cli.md must document error code `{code}`"
        );
    }

    // Global --json flag documented
    assert!(
        doc_content.contains("--json"),
        "docs/cli.md must document `--json` flag"
    );
}

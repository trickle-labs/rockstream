use std::fs;
use std::path::Path;

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
}

fn documented_commands() -> Vec<String> {
    fs::read_to_string(repo_root().join("docs/test-commands.md"))
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("$ "))
        .map(|line| line[2..].to_string())
        .collect()
}

#[test]
fn contributor_workflows_name_executable_commands() {
    assert_eq!(
        documented_commands(),
        vec![
            "make build",
            "make fmt",
            "make clippy",
            "make test",
            "make error-codes",
            "make exit-criteria",
            "make documentation",
            "make check",
            "cargo deny check",
            "cargo audit",
            "bash scripts/check-documentation.test.sh",
            "cargo test -p rockstream-cli --test documentation_transcript_tests",
        ]
    );
}

#[test]
fn maintainer_taxonomy_matches_repo_commands() {
    let makefile = fs::read_to_string(repo_root().join("Makefile")).unwrap();
    let policy = fs::read_to_string(repo_root().join("DEPENDENCY_POLICY.md")).unwrap();
    let ci = fs::read_to_string(repo_root().join(".github/workflows/ci.yml")).unwrap();
    let scheduled =
        fs::read_to_string(repo_root().join(".github/workflows/dependency-audit.yml")).unwrap();

    for command in [
        "cargo build --workspace",
        "cargo fmt --all --check",
        "cargo clippy --workspace --all-targets -- -D warnings",
        "cargo test --workspace",
    ] {
        assert!(
            makefile.contains(command),
            "missing Makefile command: {command}"
        );
    }
    for command in ["cargo deny check", "cargo audit"] {
        assert!(
            policy.contains(command),
            "missing dependency-policy command: {command}"
        );
    }
    assert!(scheduled.contains("cargo deny"));
    assert!(scheduled.contains("cargo audit"));
    assert!(
        fs::read_to_string(repo_root().join("docs/test-commands.md"))
            .unwrap()
            .contains("$ make check")
    );
    assert!(ci.contains("bash scripts/check-documentation.sh"));
    assert!(scheduled.contains("cron: \"0 3 * * *\""));
}

#[test]
fn operator_navigation_has_only_existing_targets() {
    let root = repo_root();
    for target in [
        "docs/configuration.md",
        "docs/connectors.md",
        "docs/sre-operations.md",
        "docs/disaster-recovery.md",
        "docs/rolling-upgrades.md",
        "docs/known-limitations.md",
        "docs/README.md",
    ] {
        assert!(
            root.join(target).exists(),
            "missing operator target: {target}"
        );
    }
}

#[test]
fn history_and_adr_indexes_resolve() {
    let root = repo_root();
    for target in [
        ".claude/v0.59.15-plan.md",
        ".claude/v0.59.14-plan.md",
        ".claude/v0.59.14-evidence.md",
        "sign-offs",
        "docs/adr/0001-documentation-navigation.md",
        "docs/adr/0002-reference-compatibility.md",
        "docs/adr/0003-transcript-ownership.md",
    ] {
        assert!(
            root.join(target).exists(),
            "missing history target: {target}"
        );
    }
}

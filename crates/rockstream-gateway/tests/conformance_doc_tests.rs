//! v0.42 Slice 1 + Slice 3 — conformance doc link-checker and CI coverage gate tests.

use std::collections::HashSet;

/// Reads `docs/pgwire-conformance.md`, extracts every `file::function` link,
/// and asserts the named function exists in the gateway test corpus.
///
/// Link format: `some_file.rs::some_function_name`
/// The check greps for `fn some_function_name` in the tests/ directory.
#[test]
fn test_conformance_doc_has_linked_tests() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // doc is two levels above the crate: crates/rockstream-gateway/../../docs/
    let doc_path = manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("docs/pgwire-conformance.md");

    assert!(
        doc_path.exists(),
        "docs/pgwire-conformance.md not found at {:?}",
        doc_path
    );

    let content = std::fs::read_to_string(&doc_path)
        .expect("failed to read docs/pgwire-conformance.md");

    // Extract all `filename.rs::function_name` references from the doc.
    let mut links: Vec<(String, String)> = Vec::new();
    for token in content.split_whitespace() {
        // Strip Markdown table separators and backticks
        let token = token
            .trim_matches('`')
            .trim_matches('|')
            .trim_matches('`');
        if let Some(sep) = token.find("::") {
            let file = &token[..sep];
            let func = &token[sep + 2..];
            if file.ends_with(".rs") && !func.is_empty() && func.chars().all(|c| c.is_alphanumeric() || c == '_') {
                links.push((file.to_string(), func.to_string()));
            }
        }
    }

    assert!(
        !links.is_empty(),
        "no file::function links found in docs/pgwire-conformance.md"
    );

    let tests_dir = manifest_dir.join("tests");
    assert!(tests_dir.exists(), "tests/ directory not found at {:?}", tests_dir);

    // Collect all test function names from the test corpus.
    let mut known_functions: HashSet<String> = HashSet::new();
    for entry in std::fs::read_dir(&tests_dir).expect("read tests/") {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().map(|e| e == "rs").unwrap_or(false) {
            let src = std::fs::read_to_string(&path).unwrap_or_default();
            for line in src.lines() {
                let trimmed = line.trim();
                // Match `async fn name` or `fn name`
                for prefix in &["async fn ", "fn "] {
                    if let Some(rest) = trimmed.strip_prefix(prefix) {
                        let name: String = rest
                            .chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect();
                        if !name.is_empty() {
                            known_functions.insert(name);
                        }
                    }
                }
            }
        }
    }

    let mut missing: Vec<String> = Vec::new();
    for (file, func) in &links {
        if !known_functions.contains(func.as_str()) {
            missing.push(format!("{}::{}", file, func));
        }
    }

    assert!(
        missing.is_empty(),
        "docs/pgwire-conformance.md references test functions that do not exist:\n{}",
        missing.join("\n")
    );
}

/// Parses `.github/workflows/ci.yml` and asserts that both coverage gate flags
/// are present: `--fail-under-lines 90` and `--fail-under-branches 85`.
///
/// Slice 3 green gate: CI must enforce these thresholds.
#[test]
fn test_coverage_gate_config_is_present() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let ci_path = manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(".github/workflows/ci.yml");

    assert!(
        ci_path.exists(),
        ".github/workflows/ci.yml not found at {:?}",
        ci_path
    );

    let content = std::fs::read_to_string(&ci_path)
        .expect("failed to read .github/workflows/ci.yml");

    assert!(
        content.contains("--fail-under-lines 90"),
        "ci.yml must contain `--fail-under-lines 90` in the coverage job"
    );

    assert!(
        content.contains("--fail-under-branches 85"),
        "ci.yml must contain `--fail-under-branches 85` in the coverage job"
    );
}

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

    let content =
        std::fs::read_to_string(&doc_path).expect("failed to read docs/pgwire-conformance.md");

    // Extract all `filename.rs::function_name` references from the doc.
    let mut links: Vec<(String, String)> = Vec::new();
    for token in content.split_whitespace() {
        // Strip Markdown table separators and backticks
        let token = token.trim_matches('`').trim_matches('|').trim_matches('`');
        if let Some(sep) = token.find("::") {
            let file = &token[..sep];
            let func = &token[sep + 2..];
            if file.ends_with(".rs")
                && !func.is_empty()
                && func.chars().all(|c| c.is_alphanumeric() || c == '_')
            {
                links.push((file.to_string(), func.to_string()));
            }
        }
    }

    assert!(
        !links.is_empty(),
        "no file::function links found in docs/pgwire-conformance.md"
    );

    let tests_dir = manifest_dir.join("tests");
    assert!(
        tests_dir.exists(),
        "tests/ directory not found at {:?}",
        tests_dir
    );

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
/// are present: `--fail-under-lines 70` and `--fail-under-regions 70`.
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

    let content =
        std::fs::read_to_string(&ci_path).expect("failed to read .github/workflows/ci.yml");

    // v0.45.3: the gateway's floor ratchets up from 70 to its actual
    // measured baseline (77) as part of expanding the coverage gate to all
    // 13 workspace crates (see `test_all_gated_crates_present_in_ci_coverage_job`
    // and `.claude/v0.45.3-plan.md` S1/S4). The floor only ever goes up.
    assert!(
        content.contains("--package rockstream-gateway --fail-under-lines 77"),
        "ci.yml must contain a `--fail-under-lines 77` step for rockstream-gateway in the coverage job"
    );

    assert!(
        content.contains("--package rockstream-gateway --fail-under-regions 77"),
        "ci.yml must contain a `--fail-under-regions 77` step for rockstream-gateway in the coverage job"
    );
}

/// The 13 workspace crate names that must each have a coverage-gate floor
/// (added incrementally in v0.45.3: `rockstream-diff`, `-ops`, `-storage`,
/// `-runtime`, `-sql`, `-control`, `-connectors`, `-types`, `-plan`, `-sim`,
/// `-cli`, `-oracle`, and `-gateway`, gated since v0.42).
const GATED_CRATE_NAMES: &[&str] = &[
    "rockstream-gateway",
    "rockstream-diff",
    "rockstream-ops",
    "rockstream-storage",
    "rockstream-runtime",
    "rockstream-sql",
    "rockstream-control",
    "rockstream-connectors",
    "rockstream-types",
    "rockstream-plan",
    "rockstream-sim",
    "rockstream-cli",
    "rockstream-oracle",
];

/// Extracts `(crate_name, fail_under_lines, fail_under_regions)` triples
/// from a `cargo llvm-cov --package <crate> --fail-under-lines <N>` /
/// `--fail-under-regions <N>` pair of lines anywhere in `content`.
fn extract_coverage_floors(content: &str) -> std::collections::HashMap<String, (u32, u32)> {
    let mut lines: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    let mut regions: std::collections::HashMap<String, u32> = std::collections::HashMap::new();

    for cap_line in content.lines() {
        let Some(pkg_idx) = cap_line.find("--package ") else {
            continue;
        };
        let rest = &cap_line[pkg_idx + "--package ".len()..];
        let crate_name = rest.split_whitespace().next().unwrap_or("").to_string();
        if crate_name.is_empty() {
            continue;
        }
        if let Some(idx) = cap_line.find("--fail-under-lines ") {
            let rest = &cap_line[idx + "--fail-under-lines ".len()..];
            if let Some(num) = rest.split_whitespace().next() {
                if let Ok(n) = num.parse::<u32>() {
                    lines.insert(crate_name.clone(), n);
                }
            }
        }
        if let Some(idx) = cap_line.find("--fail-under-regions ") {
            let rest = &cap_line[idx + "--fail-under-regions ".len()..];
            if let Some(num) = rest.split_whitespace().next() {
                if let Ok(n) = num.parse::<u32>() {
                    regions.insert(crate_name.clone(), n);
                }
            }
        }
    }

    let mut merged = std::collections::HashMap::new();
    for name in lines.keys().chain(regions.keys()) {
        let l = *lines.get(name).unwrap_or(&0);
        let r = *regions.get(name).unwrap_or(&0);
        merged.insert(name.clone(), (l, r));
    }
    merged
}

/// P1: `cargo llvm-cov` must be configured in CI to fail if any of the 13
/// workspace crates' coverage drops below its floor — not just the
/// hot-path crates or the historically-gated gateway.
#[test]
fn test_all_gated_crates_present_in_ci_coverage_job() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let ci_path = manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(".github/workflows/ci.yml");
    let content = std::fs::read_to_string(&ci_path).expect("failed to read ci.yml");

    let floors = extract_coverage_floors(&content);

    for crate_name in GATED_CRATE_NAMES {
        let (lines, regions) = floors
            .get(*crate_name)
            .unwrap_or_else(|| panic!("ci.yml coverage job is missing a gate for `{crate_name}`"));
        assert!(
            *lines >= 70,
            "ci.yml `{crate_name}` --fail-under-lines must be >= 70, got {lines}"
        );
        assert!(
            *regions >= 70,
            "ci.yml `{crate_name}` --fail-under-regions must be >= 70, got {regions}"
        );
    }
    assert_eq!(
        floors.len(),
        GATED_CRATE_NAMES.len(),
        "ci.yml coverage job must gate exactly the 13 workspace crates, found: {:?}",
        floors.keys().collect::<Vec<_>>()
    );
}

/// P2: `make coverage-gate`'s enforced thresholds must match `ci.yml`'s
/// actual enforced thresholds exactly, crate-for-crate.
#[test]
fn test_makefile_coverage_gate_matches_ci_yml() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().unwrap().parent().unwrap();
    let ci_path = repo_root.join(".github/workflows/ci.yml");
    let makefile_path = repo_root.join("Makefile");

    let ci_content = std::fs::read_to_string(&ci_path).expect("failed to read ci.yml");
    let makefile_content =
        std::fs::read_to_string(&makefile_path).expect("failed to read Makefile");

    let ci_floors = extract_coverage_floors(&ci_content);
    let make_floors = extract_coverage_floors(&makefile_content);

    let ci_names: HashSet<&String> = ci_floors.keys().collect();
    let make_names: HashSet<&String> = make_floors.keys().collect();
    assert_eq!(
        ci_names, make_names,
        "ci.yml and Makefile must gate the exact same set of crate names"
    );

    for (name, (ci_lines, ci_regions)) in &ci_floors {
        let (make_lines, make_regions) = make_floors
            .get(name)
            .unwrap_or_else(|| panic!("Makefile coverage-gate is missing `{name}`"));
        assert_eq!(
            ci_lines, make_lines,
            "`{name}` --fail-under-lines differs between ci.yml ({ci_lines}) and Makefile ({make_lines})"
        );
        assert_eq!(
            ci_regions, make_regions,
            "`{name}` --fail-under-regions differs between ci.yml ({ci_regions}) and Makefile ({make_regions})"
        );
    }
}

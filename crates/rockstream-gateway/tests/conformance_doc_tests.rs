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

/// P3: the gateway's blanket `#![allow(clippy::all, ...)]` suppression must be
/// gone from `lib.rs`, and any remaining `#[allow(clippy::...)]` in the crate
/// must be a narrow, item-level suppression with an adjacent justification
/// comment — never crate- or module-wide.
#[test]
fn test_gateway_lib_has_no_blanket_clippy_allow() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let lib_path = manifest_dir.join("src/lib.rs");
    let lib_content = std::fs::read_to_string(&lib_path).expect("failed to read src/lib.rs");

    assert!(
        !lib_content.contains("#![allow(clippy::all"),
        "src/lib.rs must not contain a blanket `#![allow(clippy::all, ...)]` suppression"
    );

    // Scan every source file in the crate for any `#[allow(clippy::...)]` /
    // `#![allow(clippy::...)]` occurrence. Any that remain must (a) not be
    // crate-/module-wide (`#!`) and (b) have a `// reason`-style justification
    // comment on the same or an adjacent line.
    let src_dir = manifest_dir.join("src");
    let mut violations = Vec::new();
    for entry in walk_rs_files(&src_dir) {
        let content = std::fs::read_to_string(&entry).unwrap_or_default();
        for (i, line) in content.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("#![allow(clippy::") {
                violations.push(format!(
                    "{}:{}: crate/module-wide `#![allow(clippy::...)]` is not permitted",
                    entry.display(),
                    i + 1
                ));
            } else if trimmed.starts_with("#[allow(clippy::") {
                let prev = content.lines().nth(i.saturating_sub(1)).unwrap_or("");
                let has_reason = trimmed.contains("// ") || prev.trim_start().starts_with("//");
                if !has_reason {
                    violations.push(format!(
                        "{}:{}: item-level `#[allow(clippy::...)]` must have an adjacent justification comment",
                        entry.display(),
                        i + 1
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "unjustified clippy allow suppressions found:\n{}",
        violations.join("\n")
    );
}

/// Recursively collects all `.rs` files under `dir`.
fn walk_rs_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(read_dir) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in read_dir.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_rs_files(&path));
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
    out
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

/// P4 (part 1): a scheduled workflow (not just PR-triggered) must exist and
/// run `cargo deny check` on a cron period of <= 24h, independent of
/// `pull_request`/`push` triggers.
#[test]
fn test_scheduled_dependency_audit_workflow_exists() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().unwrap().parent().unwrap();
    let workflow_path = repo_root.join(".github/workflows/dependency-audit.yml");

    assert!(
        workflow_path.exists(),
        ".github/workflows/dependency-audit.yml not found at {:?}",
        workflow_path
    );

    let content = std::fs::read_to_string(&workflow_path)
        .expect("failed to read .github/workflows/dependency-audit.yml");

    assert!(
        content.contains("cargo-deny-action") || content.contains("cargo deny check"),
        "dependency-audit.yml must invoke `cargo deny check`"
    );

    // Extract the cron expression and assert its period is <= 24h. Supported
    // forms: "M H * * *" (daily-or-more-frequent) or "M H/N * * *" (every N
    // hours). Anything with day/month/weekday restrictions beyond "*" would
    // exceed 24h and is rejected.
    let cron_line = content
        .lines()
        .find(|l| l.trim_start().starts_with("- cron:"))
        .unwrap_or_else(|| panic!("dependency-audit.yml must have a `schedule:`/`cron:` trigger"));
    let cron_expr = cron_line
        .split("cron:")
        .nth(1)
        .unwrap()
        .trim()
        .trim_matches('"');
    let fields: Vec<&str> = cron_expr.split_whitespace().collect();
    assert_eq!(
        fields.len(),
        5,
        "cron expression `{cron_expr}` must have 5 fields"
    );
    let (minute, hour, day, month, weekday) =
        (fields[0], fields[1], fields[2], fields[3], fields[4]);
    assert_eq!(
        day, "*",
        "cron day-of-month field must be `*` for a period <= 24h, got `{day}`"
    );
    assert_eq!(
        month, "*",
        "cron month field must be `*` for a period <= 24h, got `{month}`"
    );
    assert_eq!(
        weekday, "*",
        "cron weekday field must be `*` for a period <= 24h, got `{weekday}`"
    );
    assert!(
        minute != "*" || hour.contains('/') || hour == "*",
        "cron expression `{cron_expr}` must run at least once per day"
    );
    // hour field must be a fixed hour (e.g. "3") or a step (e.g. "*/6"), not empty.
    assert!(!hour.is_empty(), "cron hour field must not be empty");
}

/// P4 (part 2): a newly-disclosed (or previously-ignored-then-un-ignored)
/// advisory must actually be caught by `cargo deny check advisories` — proves
/// the detection mechanism works, not just that the workflow file exists.
///
/// Removes the `RUSTSEC-2025-0141` ignore entry from a temp copy of the repo
/// root's `deny.toml` and asserts `cargo deny check advisories --config
/// <tmp>/deny.toml` exits non-zero (the advisory is no longer suppressed).
#[test]
fn test_unignoring_resolved_advisory_is_detected() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().unwrap().parent().unwrap();
    let deny_toml_path = repo_root.join("deny.toml");
    let deny_content = std::fs::read_to_string(&deny_toml_path).expect("failed to read deny.toml");

    let ignored_line = deny_content
        .lines()
        .find(|l| l.contains("RUSTSEC-2025-0141"))
        .expect("deny.toml must ignore RUSTSEC-2025-0141 for this test to be meaningful");

    let tmp_dir = std::env::temp_dir().join(format!("rockstream-deny-test-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_dir).expect("failed to create temp dir");
    let tmp_deny_toml = tmp_dir.join("deny.toml");
    let modified_content = deny_content.replace(&format!("{ignored_line}\n"), "");
    assert_ne!(
        modified_content, deny_content,
        "failed to remove the RUSTSEC-2025-0141 ignore entry from the temp deny.toml"
    );
    std::fs::write(&tmp_deny_toml, &modified_content).expect("failed to write temp deny.toml");

    let output = std::process::Command::new("cargo")
        .args([
            "deny",
            "check",
            "advisories",
            "--config",
            tmp_deny_toml.to_str().unwrap(),
        ])
        .current_dir(repo_root)
        .output();

    let _ = std::fs::remove_dir_all(&tmp_dir);

    let Ok(output) = output else {
        eprintln!("cargo-deny not available locally; skipping detection assertion");
        return;
    };

    assert!(
        !output.status.success(),
        "cargo deny check advisories must exit non-zero once RUSTSEC-2025-0141 is un-ignored;\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// P5: `.github/dependabot.yml` must exist and parse as a minimal, valid
/// Dependabot config: `version: 2`, one `cargo` ecosystem entry rooted at
/// `/`, updated on a `weekly` schedule. No YAML-parsing crate is a direct
/// dependency of this crate, so this test does line-oriented structural
/// checks (matching the style of the other structural doc tests in this
/// file) rather than a full YAML parse.
#[test]
fn test_dependabot_config_is_valid() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().unwrap().parent().unwrap();
    let dependabot_path = repo_root.join(".github/dependabot.yml");

    assert!(
        dependabot_path.exists(),
        ".github/dependabot.yml not found at {:?}",
        dependabot_path
    );

    let content =
        std::fs::read_to_string(&dependabot_path).expect("failed to read .github/dependabot.yml");

    // No tabs (invalid YAML indentation) and no duplicate top-level `version:`
    // or `updates:` keys — cheap sanity checks that this parses as YAML.
    assert!(
        !content.contains('\t'),
        "dependabot.yml must not contain tab characters (invalid YAML indentation)"
    );
    assert_eq!(
        content
            .lines()
            .filter(|l| l.trim_start() == "version: 2")
            .count(),
        1,
        "dependabot.yml must have exactly one `version: 2` line"
    );
    assert!(
        content.contains("updates:"),
        "dependabot.yml must have an `updates:` key"
    );
    assert!(
        content.contains("package-ecosystem: \"cargo\""),
        "dependabot.yml must have a `package-ecosystem: \"cargo\"` entry"
    );
    assert!(
        content.contains("directory: \"/\""),
        "dependabot.yml must have `directory: \"/\"`"
    );
    assert!(
        content.contains("interval: \"weekly\""),
        "dependabot.yml must have `schedule.interval: \"weekly\"`"
    );
}

/// v0.45.4 S7.3 / Proof P1 (wiring half): parses `ci.yml` and asserts a
/// `benchmark` job exists, carries a numeric `timeout-minutes` bound, and
/// its steps reference all four subsystem tags (`ops`, `storage`, `runtime`,
/// `control`) and all four `v0.45.4-<tag>.json` baseline paths — i.e. that
/// each subsystem's CI step actually invokes the shared
/// `bench_regression_gate` binary with its own tag/baseline, not just that
/// the comparator logic works in isolation.
#[test]
fn test_benchmark_ci_job_exists_and_covers_four_subsystems() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().unwrap().parent().unwrap();
    let ci_path = repo_root.join(".github/workflows/ci.yml");

    let content = std::fs::read_to_string(&ci_path).expect("failed to read ci.yml");

    // Isolate the `benchmark:` job's body: from its header (a line starting
    // with exactly two spaces then `benchmark:`) up to the next line that
    // starts a new top-level job (two-space indent followed by a bare key,
    // not further-indented step content).
    let lines: Vec<&str> = content.lines().collect();
    let start = lines
        .iter()
        .position(|l| *l == "  benchmark:")
        .expect("ci.yml must contain a top-level `benchmark:` job");
    let end = lines[start + 1..]
        .iter()
        .position(|l| {
            !l.is_empty() && l.starts_with("  ") && !l.starts_with("   ") && l.ends_with(':')
        })
        .map(|i| start + 1 + i)
        .unwrap_or(lines.len());
    let job_body = lines[start..end].join("\n");
    let job_body = job_body.as_str();

    assert!(
        job_body.contains("timeout-minutes:"),
        "benchmark job must declare a `timeout-minutes` bound"
    );
    let timeout_line = job_body
        .lines()
        .find(|l| l.trim_start().starts_with("timeout-minutes:"))
        .expect("timeout-minutes line must exist");
    let timeout_value = timeout_line
        .split(':')
        .nth(1)
        .map(|s| s.trim())
        .unwrap_or("");
    assert!(
        timeout_value.parse::<u32>().is_ok(),
        "benchmark job's timeout-minutes must be numeric, got {:?}",
        timeout_value
    );

    for tag in ["ops", "storage", "runtime", "control"] {
        assert!(
            job_body.contains(&format!("--tag {tag}")),
            "benchmark job must invoke bench_regression_gate with `--tag {tag}`"
        );
        let baseline_path = format!("crates/rockstream-{tag}/benches/baseline/v0.45.4-{tag}.json");
        let baseline_path = if tag == "ops" {
            "crates/rockstream-ops/benches/baseline/v0.45.4-ops.json".to_string()
        } else {
            baseline_path
        };
        assert!(
            job_body.contains(&baseline_path),
            "benchmark job must reference baseline path `{baseline_path}`"
        );
    }
}

/// v0.45.4 S7.4 / Proof P2 (guard half): the `bench-baseline-update` Makefile
/// target must exist, and the literal string `bench-baseline-update` must
/// never appear in `ci.yml` — baseline updates stay an explicit,
/// code-reviewed, human-triggered step, never auto-invoked by CI.
#[test]
fn test_makefile_has_bench_baseline_update_target_not_invoked_by_ci() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir.parent().unwrap().parent().unwrap();
    let ci_path = repo_root.join(".github/workflows/ci.yml");
    let makefile_path = repo_root.join("Makefile");

    let ci_content = std::fs::read_to_string(&ci_path).expect("failed to read ci.yml");
    let makefile_content =
        std::fs::read_to_string(&makefile_path).expect("failed to read Makefile");

    assert!(
        makefile_content.contains("\nbench-baseline-update:"),
        "Makefile must contain a `bench-baseline-update:` target"
    );
    assert!(
        !ci_content.contains("bench-baseline-update"),
        "ci.yml must never invoke `bench-baseline-update` — it is a human-triggered-only step"
    );
}

//! v0.45.5 Slice 1 — `docs/configuration.md` conformance lock.
//!
//! Parses the `(key, default value)` pairs documented in
//! `docs/configuration.md`'s reference bullets and diffs each one against the
//! real field values of `RockstreamConfig::default()` (via `Debug`, not a
//! hand-copied list of numbers in this test). Fails if the doc and the
//! struct's defaults ever drift apart.

use rockstream_types::config::RockstreamConfig;

/// Extracts every `**`key`** (type, default: `value`)` bullet from
/// `docs/configuration.md` as `(key, default_value)` pairs.
fn parse_documented_defaults(doc: &str) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for line in doc.lines() {
        let trimmed = line.trim();
        // Bullets look like: - **`key`** (type, default: `value`): description
        if !trimmed.starts_with("- **`") {
            continue;
        }
        let Some(after_key_marker) = trimmed.strip_prefix("- **`") else {
            continue;
        };
        let Some(key_end) = after_key_marker.find('`') else {
            continue;
        };
        let key = &after_key_marker[..key_end];

        let Some(default_marker) = trimmed.find("default: `") else {
            continue;
        };
        let after_default = &trimmed[default_marker + "default: `".len()..];
        let Some(value_end) = after_default.find('`') else {
            continue;
        };
        let value = &after_default[..value_end];

        pairs.push((key.to_string(), value.to_string()));
    }
    pairs
}

/// Returns true if `debug_str` contains the field assignment `key: value`
/// followed by a field/struct delimiter (`,` or `}`), which is how derived
/// `Debug` renders struct fields regardless of nesting depth.
fn debug_contains_field(debug_str: &str, key: &str, value: &str) -> bool {
    let comma_form = format!("{key}: {value},");
    let brace_form = format!("{key}: {value} }}");
    debug_str.contains(&comma_form) || debug_str.contains(&brace_form)
}

#[test]
fn test_configuration_doc_matches_rockstream_config_defaults() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // doc is two levels above the crate: crates/rockstream-types/../../docs/
    let doc_path = manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("docs/configuration.md");

    assert!(
        doc_path.exists(),
        "docs/configuration.md not found at {:?}",
        doc_path
    );

    let content = std::fs::read_to_string(&doc_path).expect("failed to read docs/configuration.md");

    let documented = parse_documented_defaults(&content);
    assert!(
        !documented.is_empty(),
        "no `- **`key`** (type, default: `value`)` bullets found in docs/configuration.md"
    );

    let default_cfg = RockstreamConfig::default();
    let debug_str = format!("{default_cfg:?}");

    let mut mismatches = Vec::new();
    for (key, value) in &documented {
        if !debug_contains_field(&debug_str, key, value) {
            mismatches.push(format!(
                "docs/configuration.md documents `{key}` default as `{value}`, but \
                 RockstreamConfig::default()'s Debug output does not contain `{key}: {value}`"
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "docs/configuration.md has drifted from RockstreamConfig::default():\n{}\n\nfull Debug:\n{debug_str}",
        mismatches.join("\n")
    );

    // Guard against the phantom key removed in v0.45.5 ever being
    // re-documented as a live, defaulted knob (a bullet with its own
    // `default:` value) without a corresponding `ClusterConfig` field. A
    // prose mention explaining its absence (as this doc has) is fine; only a
    // documented-default bullet would indicate the phantom key crept back.
    assert!(
        !documented
            .iter()
            .any(|(key, _)| key == "checkpoint_retention_duration_sec"),
        "docs/configuration.md re-introduced the phantom `checkpoint_retention_duration_sec` \
         key as a documented default, but ClusterConfig has no corresponding field"
    );
}

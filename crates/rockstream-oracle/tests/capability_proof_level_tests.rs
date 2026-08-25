//! Verifies that every Core capability's achieved proof levels cover its
//! declared minimum, per `capabilities.toml`'s `[[capability]]` schema.

use std::collections::HashSet;
use std::path::Path;

fn load_capabilities() -> toml::Value {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../capabilities.toml");
    let text = std::fs::read_to_string(&root)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", root.display()));
    toml::from_str(&text).expect("capabilities.toml must be valid TOML")
}

fn string_set(value: Option<&toml::Value>, cap_id: &str, field: &str) -> HashSet<String> {
    value
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("{cap_id} is missing {field}"))
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .unwrap_or_else(|| panic!("{cap_id} has a non-string {field} entry"))
                .to_string()
        })
        .collect()
}

#[test]
fn every_core_capability_meets_min_proof_level() {
    let document = load_capabilities();
    let capabilities = document
        .get("capability")
        .and_then(|v| v.as_array())
        .expect("capabilities.toml must define [[capability]] records");

    let core_capabilities: Vec<&toml::Value> = capabilities
        .iter()
        .filter(|item| item.get("tier").and_then(|t| t.as_str()) == Some("Core"))
        .collect();

    assert_eq!(
        core_capabilities.len(),
        11,
        "expected exactly 11 Core capabilities, found {}",
        core_capabilities.len()
    );

    for item in core_capabilities {
        let cap_id = item
            .get("id")
            .and_then(|v| v.as_str())
            .expect("Core capability must have an id");

        let achieved = string_set(
            item.get("proof_levels_achieved"),
            cap_id,
            "proof_levels_achieved",
        );
        let minimum = string_set(item.get("min_proof_level"), cap_id, "min_proof_level");

        assert!(
            achieved.is_superset(&minimum),
            "{cap_id}: proof_levels_achieved {achieved:?} is not a superset of min_proof_level {minimum:?}"
        );
    }
}

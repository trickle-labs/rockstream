//! Unit tests for CapabilityRegistry (OBS-01).

use rockstream_types::capability::CapabilityRegistry;

#[test]
fn test_capability_registry_loads_embedded_toml() {
    let registry = CapabilityRegistry::current();
    assert!(
        !registry.capabilities().is_empty(),
        "capabilities must not be empty"
    );
    assert!(
        !registry.dispatches().is_empty(),
        "dispatches must not be empty"
    );
    assert_eq!(registry.contract().roadmap, "NEW_ROADMAP.md");

    // Check language.query-read
    let query_read = registry
        .get_by_id("language.query-read")
        .expect("query-read must exist");
    assert_eq!(query_read.kind, "language");
    assert_eq!(query_read.tier, "Core");
    assert_eq!(query_read.reachability, "SQL over pgwire");
    assert_eq!(query_read.dispatch_count(), 3);
    assert!(!query_read.proof_ref().is_empty());
    assert!(!query_read.doc_anchor().is_empty());
}

#[test]
fn test_capability_registry_filters() {
    let registry = CapabilityRegistry::current();
    let language_caps = registry.filter_by_kind("language");
    assert!(!language_caps.is_empty());
    for c in &language_caps {
        assert_eq!(c.kind, "language");
    }

    let core_caps = registry.filter_by_tier("Core");
    assert!(!core_caps.is_empty());
    for c in &core_caps {
        assert_eq!(c.tier, "Core");
    }
}

//! Namespace catalog for v0.26.
//!
//! In-memory implementation for unit tests.

use std::collections::HashSet;

use parking_lot::RwLock;

/// In-memory namespace catalog.
pub struct NamespaceCatalog {
    // Audit: each synchronous mutation is atomic to this set and remains valid if
    // a holder panics after inserting a namespace; no guard crosses an await.
    namespaces: RwLock<HashSet<String>>,
}

impl NamespaceCatalog {
    pub fn new() -> Self {
        let mut ns = HashSet::new();
        ns.insert("public".to_string()); // "public" always exists
        Self {
            namespaces: RwLock::new(ns),
        }
    }

    pub fn create_namespace(&self, name: &str) {
        self.namespaces.write().insert(name.to_string());
    }

    pub fn namespace_exists(&self, name: &str) -> bool {
        self.namespaces.read().contains(name)
    }

    pub fn list_namespaces(&self) -> Vec<String> {
        let mut v: Vec<String> = self.namespaces.read().iter().cloned().collect();
        v.sort();
        v
    }
}

impl Default for NamespaceCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// S4 green gate: create_namespace_persists_and_roundtrips
    #[test]
    fn create_namespace_persists_and_roundtrips() {
        let catalog = NamespaceCatalog::new();
        assert!(catalog.namespace_exists("public"));
        assert!(!catalog.namespace_exists("analytics"));
        catalog.create_namespace("analytics");
        assert!(catalog.namespace_exists("analytics"));
        let list = catalog.list_namespaces();
        assert!(list.contains(&"analytics".to_string()));
        assert!(list.contains(&"public".to_string()));
    }

    #[test]
    fn namespace_catalog_peer_operations_survive_writer_panic() {
        let catalog = NamespaceCatalog::new();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut namespaces = catalog.namespaces.write();
            namespaces.insert("abandoned".to_string());
            panic!("injected namespace writer panic");
        }));
        assert!(panic.is_err());

        catalog.create_namespace("analytics");
        assert!(catalog.namespace_exists("analytics"));
        assert_eq!(
            catalog.list_namespaces(),
            vec![
                "abandoned".to_string(),
                "analytics".to_string(),
                "public".to_string(),
            ]
        );
    }
}

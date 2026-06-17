//! Namespace catalog for v0.26.
//!
//! In-memory implementation for unit tests.

use std::collections::HashSet;
use std::sync::RwLock;

/// In-memory namespace catalog.
pub struct NamespaceCatalog {
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
        self.namespaces.write().unwrap().insert(name.to_string());
    }

    pub fn namespace_exists(&self, name: &str) -> bool {
        self.namespaces.read().unwrap().contains(name)
    }

    pub fn list_namespaces(&self) -> Vec<String> {
        let mut v: Vec<String> = self.namespaces.read().unwrap().iter().cloned().collect();
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
}

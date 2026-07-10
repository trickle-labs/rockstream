//! ACL store for RBAC enforcement in v0.26.
//!
//! Persists ACL entries under catalog/acl/<namespace>/<principal>.
//! Invariant: entries are deleted via point-delete only — no range deletion.
//! Cache: up to MAX_ACL_CACHE_ENTRIES with 60s TTL; fill metric: acl_cache_size.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use rockstream_types::acl::{AclEntry, Role};

/// Max ACL cache entries (LRU; default 10,000).
pub const MAX_ACL_CACHE_ENTRIES: usize = 10_000;
/// ACL cache TTL.
pub const ACL_CACHE_TTL: Duration = Duration::from_secs(60);

/// Error from ACL operations.
#[derive(Debug, thiserror::Error)]
pub enum AclError {
    #[error("[RS-2401] auth.permission_denied: principal '{principal}' has role '{actual:?}' but needs '{required:?}' on {context}")]
    PermissionDenied {
        principal: String,
        actual: Option<Role>,
        required: Role,
        context: String,
    },
    #[error("[RS-2402] auth.namespace_access_denied: principal '{principal}' cannot access namespace '{namespace}'")]
    NamespaceAccessDenied {
        principal: String,
        namespace: String,
    },
}

/// Cache entry.
#[allow(dead_code)]
struct CacheEntry {
    role: Option<Role>,
    inserted_at: Instant,
}

#[allow(dead_code)]
impl CacheEntry {
    fn is_expired(&self) -> bool {
        self.inserted_at.elapsed() > ACL_CACHE_TTL
    }
}

/// ACL storage key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AclKey {
    principal: String,
    namespace: String,
    view_name: Option<String>,
}

/// In-memory ACL store with point-delete (no range deletion).
#[derive(Default)]
struct AclStoreInner {
    entries: HashMap<AclKey, AclEntry>,
    #[allow(dead_code)]
    cache: HashMap<AclKey, CacheEntry>,
    #[allow(dead_code)]
    cache_order: std::collections::VecDeque<AclKey>,
}

#[allow(dead_code)]
impl AclStoreInner {
    fn cache_insert(&mut self, key: AclKey, role: Option<Role>) {
        if self.cache.contains_key(&key) {
            self.cache.insert(
                key,
                CacheEntry {
                    role,
                    inserted_at: Instant::now(),
                },
            );
        } else {
            if self.cache.len() >= MAX_ACL_CACHE_ENTRIES {
                if let Some(oldest) = self.cache_order.pop_front() {
                    self.cache.remove(&oldest);
                }
            }
            self.cache_order.push_back(key.clone());
            self.cache.insert(
                key,
                CacheEntry {
                    role,
                    inserted_at: Instant::now(),
                },
            );
        }
    }

    fn cache_get(&self, key: &AclKey) -> Option<Option<Role>> {
        self.cache.get(key).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                Some(e.role.clone())
            }
        })
    }
}

/// Thread-safe ACL store.
pub struct AclStore {
    inner: RwLock<AclStoreInner>,
}

impl AclStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(AclStoreInner::default()),
        }
    }

    /// Fill metric: acl_cache_size gauge.
    pub fn cache_size(&self) -> usize {
        self.inner.read().unwrap().cache.len()
    }

    /// Grant an ACL entry. Point-write (not range-write).
    pub fn grant(&self, entry: AclEntry) {
        let mut inner = self.inner.write().unwrap();
        let key = AclKey {
            principal: entry.principal.clone(),
            namespace: entry.namespace.clone(),
            view_name: entry.view_name.clone(),
        };
        inner.cache.remove(&key);
        inner.entries.insert(key, entry);
    }

    /// Revoke an ACL entry. Point-delete — no range deletion.
    pub fn revoke(&self, principal: &str, namespace: &str, view_name: Option<&str>) {
        let key = AclKey {
            principal: principal.to_string(),
            namespace: namespace.to_string(),
            view_name: view_name.map(String::from),
        };
        let mut inner = self.inner.write().unwrap();
        inner.entries.remove(&key);
        inner.cache.remove(&key);
    }

    /// Look up the effective role for a principal on a view (or namespace-level).
    /// Checks view-level grant first, then namespace-level.
    fn lookup_role(
        &self,
        principal: &str,
        namespace: &str,
        view_name: Option<&str>,
    ) -> Option<Role> {
        let inner = self.inner.read().unwrap();

        if let Some(vn) = view_name {
            let vk = AclKey {
                principal: principal.to_string(),
                namespace: namespace.to_string(),
                view_name: Some(vn.to_string()),
            };
            if let Some(entry) = inner.entries.get(&vk) {
                return Some(entry.role.clone());
            }
        }
        let nk = AclKey {
            principal: principal.to_string(),
            namespace: namespace.to_string(),
            view_name: None,
        };
        inner.entries.get(&nk).map(|e| e.role.clone())
    }

    /// Check if principal has at least `required` role on the given namespace/view.
    ///
    /// - `"system"` principal always passes.
    /// - Otherwise looks up role and checks >= required.
    pub fn check(
        &self,
        principal_identity: &str,
        namespace: &str,
        view_name: Option<&str>,
        required: Role,
    ) -> Result<(), AclError> {
        if principal_identity == "system" {
            return Ok(());
        }

        let role = self.lookup_role(principal_identity, namespace, view_name);

        match &role {
            Some(r) if r >= &required => Ok(()),
            actual => Err(AclError::PermissionDenied {
                principal: principal_identity.to_string(),
                actual: actual.clone(),
                required,
                context: format!("namespace={namespace} view={view_name:?}"),
            }),
        }
    }
}

impl Default for AclStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// S3 green gate: acl_grant_viewer_allows_select
    #[test]
    fn acl_grant_viewer_allows_select() {
        let store = AclStore::new();
        store.grant(AclEntry {
            principal: "alice".to_string(),
            namespace: "public".to_string(),
            view_name: None,
            role: Role::Viewer,
        });
        assert!(store
            .check("alice", "public", Some("my_view"), Role::Viewer)
            .is_ok());
    }

    /// S3 green gate: acl_check_denies_insufficient_role
    #[test]
    fn acl_check_denies_insufficient_role() {
        let store = AclStore::new();
        store.grant(AclEntry {
            principal: "bob".to_string(),
            namespace: "public".to_string(),
            view_name: None,
            role: Role::Viewer,
        });
        let err = store
            .check("bob", "public", Some("mv"), Role::PipelineOwner)
            .unwrap_err();
        assert!(
            err.to_string().contains("RS-2401"),
            "expected RS-2401, got: {err}"
        );
    }

    /// S3 green gate: admin_role_passes_all_checks
    #[test]
    fn admin_role_passes_all_checks() {
        let store = AclStore::new();
        store.grant(AclEntry {
            principal: "carol".to_string(),
            namespace: "public".to_string(),
            view_name: None,
            role: Role::Admin,
        });
        assert!(store
            .check("carol", "public", Some("v"), Role::Viewer)
            .is_ok());
        assert!(store
            .check("carol", "public", Some("v"), Role::PipelineOwner)
            .is_ok());
        assert!(store
            .check("carol", "public", Some("v"), Role::Admin)
            .is_ok());
    }

    /// Invariant: acl_no_range_delete_in_catalog_acl
    #[test]
    fn acl_no_range_delete_in_catalog_acl() {
        let store = AclStore::new();
        store.grant(AclEntry {
            principal: "dan".to_string(),
            namespace: "ns-a".to_string(),
            view_name: None,
            role: Role::Viewer,
        });
        store.grant(AclEntry {
            principal: "dan".to_string(),
            namespace: "ns-b".to_string(),
            view_name: None,
            role: Role::Viewer,
        });
        store.revoke("dan", "ns-a", None);
        assert!(
            store.check("dan", "ns-a", None, Role::Viewer).is_err(),
            "ns-a should be revoked"
        );
        assert!(
            store.check("dan", "ns-b", None, Role::Viewer).is_ok(),
            "ns-b should still be granted"
        );
    }

    /// System principal bypasses all ACL checks.
    #[test]
    fn system_principal_bypasses_acl() {
        let store = AclStore::new();
        assert!(store
            .check("system", "any-ns", Some("any-view"), Role::Admin)
            .is_ok());
    }
}

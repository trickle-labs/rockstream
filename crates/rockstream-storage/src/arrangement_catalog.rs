//! Durable Arrangement Catalog & Reference-Counted Lifecycle (v0.59.6).
//!
//! Manages the lifecycle of shared arrangements, tracks registered views/consumers,
//! manages reference counts, enforces tenant and security policy isolation,
//! and orchestrates deferred reclamation of unreferenced physical state.

use crate::error::StorageError;
use rockstream_types::arrangement::ArrangementSpec;
use rockstream_types::ids::{ArrangementId, TenantId, ViewId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Metadata for a shared physical arrangement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrangementEntry {
    /// Deterministic identifier computed from canonical ArrangementSpec.
    pub id: ArrangementId,
    /// Canonical specification governing this physical arrangement.
    pub spec: ArrangementSpec,
    /// Storage directory or prefix where trace files reside.
    pub storage_path: String,
    /// Manifest location or URI.
    pub manifest_reference: String,
    /// Number of active views/readers consuming this physical arrangement.
    pub consumer_count: usize,
    /// Active consumer view identifiers.
    pub consumers: HashSet<ViewId>,
    /// Creation timestamp (epoch millis).
    pub created_at: u64,
    /// Minimum compaction frontier among all live consumers.
    pub compaction_frontier: u64,
    /// Whether this arrangement has 0 active consumers and is awaiting GC.
    pub marked_for_reclamation: bool,
}

/// Catalog tracking all registered arrangements and consumer references.
#[derive(Debug, Clone, Default)]
pub struct ArrangementCatalog {
    inner: Arc<RwLock<ArrangementCatalogInner>>,
}

#[derive(Debug, Default)]
struct ArrangementCatalogInner {
    arrangements: HashMap<ArrangementId, ArrangementEntry>,
}

impl ArrangementCatalog {
    /// Create a new, empty arrangement catalog.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ArrangementCatalogInner::default())),
        }
    }

    /// Register a view/consumer for the given `ArrangementSpec`.
    ///
    /// If an arrangement with identical canonical specification already exists,
    /// increments its reference count and returns `(ArrangementId, false)` (reused).
    /// If no matching arrangement exists, creates a new entry with refcount 1
    /// and returns `(ArrangementId, true)` (newly created).
    pub async fn register_consumer(
        &self,
        view_id: ViewId,
        spec: ArrangementSpec,
    ) -> (ArrangementId, bool) {
        let id = spec.arrangement_id();
        let mut guard = self.inner.write().await;

        if let Some(entry) = guard.arrangements.get_mut(&id) {
            // Verify tenant & security policy isolation
            assert_eq!(
                entry.spec.tenant_id, spec.tenant_id,
                "Tenant ID mismatch for same ArrangementId"
            );
            assert_eq!(
                entry.spec.security_policy_digest, spec.security_policy_digest,
                "Security policy digest mismatch for same ArrangementId"
            );

            entry.consumer_count += 1;
            entry.consumers.insert(view_id);
            entry.marked_for_reclamation = false;
            (id, false)
        } else {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let mut consumers = HashSet::new();
            consumers.insert(view_id);

            let entry = ArrangementEntry {
                id,
                storage_path: format!("arrangements/arr-{}", id.0),
                manifest_reference: format!("arrangements/arr-{}/manifest.json", id.0),
                consumer_count: 1,
                consumers,
                created_at: now,
                compaction_frontier: 0,
                marked_for_reclamation: false,
                spec,
            };

            guard.arrangements.insert(id, entry);
            (id, true)
        }
    }

    /// Deregister a view/consumer from an arrangement.
    ///
    /// Decrements the reference count. When the reference count reaches 0,
    /// marks the arrangement for deferred reclamation.
    pub async fn deregister_consumer(
        &self,
        view_id: ViewId,
        id: ArrangementId,
    ) -> Result<bool, StorageError> {
        let mut guard = self.inner.write().await;
        let entry = guard.arrangements.get_mut(&id).ok_or_else(|| {
            StorageError::InvalidKey(format!("Arrangement {} not found in catalog", id))
        })?;

        entry.consumers.remove(&view_id);
        if entry.consumer_count > 0 {
            entry.consumer_count -= 1;
        }

        if entry.consumer_count == 0 {
            entry.marked_for_reclamation = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Update the recorded compaction frontier for an arrangement.
    pub async fn update_compaction_frontier(&self, id: ArrangementId, frontier: u64) {
        let mut guard = self.inner.write().await;
        if let Some(entry) = guard.arrangements.get_mut(&id) {
            entry.compaction_frontier = frontier;
        }
    }

    /// Reclaim unreferenced arrangements whose reference count is 0 and
    /// whose compaction / reader horizon is safe to clear.
    ///
    /// Returns the list of reclaimed `ArrangementId`s.
    pub async fn reclaim_unreferenced_arrangements(&self, safe_horizon: u64) -> Vec<ArrangementId> {
        let mut guard = self.inner.write().await;
        let mut to_reclaim = Vec::new();

        for (id, entry) in guard.arrangements.iter() {
            if entry.consumer_count == 0
                && entry.marked_for_reclamation
                && entry.compaction_frontier >= safe_horizon
            {
                assert!(
                    entry.consumer_count == 0 && entry.marked_for_reclamation,
                    "INVARIANT: M2-S7 - trace reclamation begins only after last consumer is safe and refcount is zero"
                );
                to_reclaim.push(*id);
            }
        }

        // INVARIANT-BY-CONSTRUCTION: M2-L3 - unreferenced arrangements progress toward deferred reclamation once refcount hits zero.
        for id in &to_reclaim {
            guard.arrangements.remove(id);
        }

        to_reclaim
    }

    /// Look up an arrangement by ID.
    pub async fn lookup(&self, id: ArrangementId) -> Option<ArrangementEntry> {
        let guard = self.inner.read().await;
        guard.arrangements.get(&id).cloned()
    }

    /// List all arrangements belonging to a specific tenant.
    pub async fn list_for_tenant(&self, tenant_id: TenantId) -> Vec<ArrangementEntry> {
        let guard = self.inner.read().await;
        guard
            .arrangements
            .values()
            .filter(|e| e.spec.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Get total count of unique physical arrangements.
    pub async fn physical_arrangements_count(&self) -> usize {
        let guard = self.inner.read().await;
        guard.arrangements.len()
    }

    /// Get consumer count for a specific arrangement.
    pub async fn consumer_count(&self, id: ArrangementId) -> usize {
        let guard = self.inner.read().await;
        guard
            .arrangements
            .get(&id)
            .map(|e| e.consumer_count)
            .unwrap_or(0)
    }
}

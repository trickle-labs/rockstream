//! Capacity threshold manifest and raw chunk durable storage (v0.59.23 Slice 5 / Phase 3b).

use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use rockstream_types::capacity::CapacityThresholdManifest;

/// Durable store for capacity threshold manifest and raw measurement chunks.
#[derive(Debug, Clone)]
pub struct CapacityThresholdStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl CapacityThresholdStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            prefix: Path::from("control/capacity"),
        }
    }

    pub fn with_prefix(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: Path::from(prefix.into()),
        }
    }

    fn manifest_path(&self) -> Path {
        self.prefix.child("capacity-manifest.json")
    }

    fn chunk_path(&self, chunk_id: &str) -> Path {
        self.prefix.child("chunks").child(chunk_id)
    }

    /// Persist the sealed threshold manifest with create-only semantics.
    ///
    /// Fails with an error if the manifest already exists (mutations prohibited).
    pub async fn save_manifest(&self, manifest: &CapacityThresholdManifest) -> Result<(), String> {
        let manifest_path = self.manifest_path();

        // Enforce create-only storage: verify it does not already exist
        if self.store.head(&manifest_path).await.is_ok() {
            return Err(format!(
                "RS-3030: Capacity threshold manifest already exists at '{}'; mutation prohibited",
                manifest_path
            ));
        }

        let payload = serde_json::to_vec_pretty(manifest)
            .map_err(|e| format!("RS-3030: serialize manifest: {e}"))?;

        self.store
            .put(&manifest_path, payload.into())
            .await
            .map(|_| ())
            .map_err(|e| format!("RS-3030: persist capacity manifest: {e}"))
    }

    /// Load the sealed threshold manifest if present.
    pub async fn load_manifest(&self) -> Result<Option<CapacityThresholdManifest>, String> {
        let manifest_path = self.manifest_path();
        let get_result = match self.store.get(&manifest_path).await {
            Ok(res) => res,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(format!("RS-3030: get capacity manifest: {e}")),
        };

        let bytes = get_result
            .bytes()
            .await
            .map_err(|e| format!("RS-3030: read capacity manifest bytes: {e}"))?;

        let manifest: CapacityThresholdManifest = serde_json::from_slice(&bytes)
            .map_err(|e| format!("RS-3030: deserialize capacity manifest: {e}"))?;

        // Verify cryptographic seal integrity upon load
        manifest
            .verify_seal()
            .map_err(|e| format!("RS-3030: manifest integrity verification failed: {e}"))?;

        Ok(Some(manifest))
    }

    /// Save an immutable raw capacity record chunk.
    pub async fn save_raw_chunk(&self, chunk_id: &str, chunk_bytes: &[u8]) -> Result<(), String> {
        let path = self.chunk_path(chunk_id);
        self.store
            .put(&path, chunk_bytes.to_vec().into())
            .await
            .map(|_| ())
            .map_err(|e| format!("RS-3030: persist raw chunk '{chunk_id}': {e}"))
    }

    /// Load all raw capacity chunks under `chunks/`.
    pub async fn load_raw_chunks(&self) -> Result<Vec<Vec<u8>>, String> {
        let chunks_prefix = self.prefix.child("chunks");
        let mut list_stream = self.store.list(Some(&chunks_prefix));
        let mut chunks = Vec::new();

        while let Some(meta_res) = list_stream.next().await {
            let meta = meta_res.map_err(|e| format!("RS-3030: list raw chunks: {e}"))?;
            let get_res = self
                .store
                .get(&meta.location)
                .await
                .map_err(|e| format!("RS-3030: get raw chunk '{}': {e}", meta.location))?;
            let bytes = get_res
                .bytes()
                .await
                .map_err(|e| format!("RS-3030: read raw chunk bytes: {e}"))?;
            chunks.push(bytes.to_vec());
        }

        Ok(chunks)
    }

    /// Clean up storage using explicit scan-and-delete.
    ///
    /// Iterates individual objects and deletes each key one by one, never calling SlateDB range delete.
    pub async fn cleanup_scan_and_delete(&self, sub_prefix: &str) -> Result<usize, String> {
        let target = self.prefix.child(sub_prefix);
        let mut list_stream = self.store.list(Some(&target));
        let mut deleted_count = 0;

        while let Some(meta_res) = list_stream.next().await {
            let meta = meta_res.map_err(|e| format!("RS-3030: scan objects for cleanup: {e}"))?;
            self.store
                .delete(&meta.location)
                .await
                .map_err(|e| format!("RS-3030: delete object '{}': {e}", meta.location))?;
            deleted_count += 1;
        }

        Ok(deleted_count)
    }
}

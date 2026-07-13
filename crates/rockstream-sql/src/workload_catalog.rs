use std::collections::HashMap;
use std::sync::Arc;

use rockstream_control::raft::RaftHandle;
use rockstream_storage::keys::{CatalogKeyEncoder, CatalogType};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::workload::WorkloadDef;

use crate::catalog::DEFAULT_NAMESPACE;
use crate::SqlError;

/// Durable workload catalog backed by `ShardDb`.
///
/// When `raft` is attached ([`WorkloadCatalog::with_raft`]), every write
/// (`register_workload`/`update_workload`/`remove_workload` — the
/// `CREATE WORKLOAD`/update/drop DDL paths) is gated on this node currently
/// being the Raft-elected control-plane leader (v0.45.2, M7-S2). Without a
/// `raft` handle attached (the default), no gating is performed — this
/// preserves exact pre-v0.45.2 single-node behavior for every existing
/// caller/test.
pub struct WorkloadCatalog {
    db: Arc<ShardDb>,
    raft: Option<RaftHandle>,
}

impl std::fmt::Debug for WorkloadCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkloadCatalog").finish_non_exhaustive()
    }
}

impl WorkloadCatalog {
    pub fn new(db: Arc<ShardDb>) -> Self {
        Self { db, raft: None }
    }

    /// Attach a [`RaftHandle`] so every write is gated on current
    /// control-plane leadership (M7-S2). Builder-style, mirrors
    /// `ControlService::with_shard_manager`.
    pub fn with_raft(mut self, raft: RaftHandle) -> Self {
        self.raft = Some(raft);
        self
    }

    /// M7-S2 leader-only write gate: returns `Err(SqlError::NotLeader)` if
    /// a Raft handle is attached and this node is not currently leader.
    fn require_leader(&self) -> Result<(), SqlError> {
        if let Some(raft) = &self.raft {
            raft.require_leader()
                .map_err(|_: rockstream_control::raft::NotLeader| SqlError::NotLeader)?;
        }
        Ok(())
    }

    pub async fn register_workload(&self, workload: &WorkloadDef) -> Result<(), SqlError> {
        self.require_leader()?;
        self.put_workload(workload).await
    }

    pub async fn load_all_workloads(&self) -> Result<HashMap<String, WorkloadDef>, SqlError> {
        let prefix = CatalogKeyEncoder::namespace_prefix(CatalogType::Workload, DEFAULT_NAMESPACE);
        let entries = self.db.scan_prefix(&prefix).await?;
        let mut workloads = HashMap::new();
        for (_key, value) in entries {
            let workload: WorkloadDef = serde_json::from_slice(&value)?;
            workloads.insert(workload.name.clone(), workload);
        }
        Ok(workloads)
    }

    pub async fn update_workload(&self, workload: &WorkloadDef) -> Result<(), SqlError> {
        self.require_leader()?;
        self.put_workload(workload).await
    }

    pub async fn remove_workload(&self, name: &str) -> Result<(), SqlError> {
        self.require_leader()?;
        let key = workload_key(name);
        let mut batch = WriteBatch::new();
        batch.delete(&key);
        self.db.write_batch(batch).await?;
        Ok(())
    }

    async fn put_workload(&self, workload: &WorkloadDef) -> Result<(), SqlError> {
        let key = workload_key(&workload.name);
        let value = serde_json::to_vec(workload)?;
        let mut batch = WriteBatch::new();
        batch.put(&key, &value);
        self.db.write_batch(batch).await?;
        Ok(())
    }
}

fn workload_object_id(name: &str) -> u128 {
    let mut hash: u128 = 0x6c62272e07bb0142_62b821756295c58d_u128;
    for byte in name.as_bytes() {
        hash ^= *byte as u128;
        hash = hash.wrapping_mul(0x0000_0000_0001_0000_0000_0000_0000_013B_u128);
    }
    hash
}

fn workload_key(name: &str) -> Vec<u8> {
    CatalogKeyEncoder::encode(
        CatalogType::Workload,
        DEFAULT_NAMESPACE,
        workload_object_id(name),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use rockstream_control::raft::{spawn_raft_node, RaftConfig};
    use rockstream_types::workload::{MemoryLimit, WorkloadDef, WorkloadPriority};
    use std::time::Duration;

    #[test]
    fn workload_object_id_is_stable() {
        assert_eq!(workload_object_id("fast"), workload_object_id("fast"));
        assert_ne!(workload_object_id("fast"), workload_object_id("slow"));
    }

    async fn mem_shard_db() -> Arc<ShardDb> {
        let store = Arc::new(InMemory::new());
        Arc::new(ShardDb::builder("catalog", store).build().await.unwrap())
    }

    fn sample_workload() -> WorkloadDef {
        WorkloadDef::new("fast")
            .with_memory_limit(MemoryLimit::new(1_048_576))
            .with_priority(WorkloadPriority::HIGH)
    }

    #[tokio::test]
    async fn writes_succeed_without_a_raft_handle_attached() {
        // Default (pre-v0.45.2) behavior: no gating at all.
        let db = mem_shard_db().await;
        let catalog = WorkloadCatalog::new(db);
        catalog.register_workload(&sample_workload()).await.unwrap();
    }

    #[tokio::test]
    async fn writes_rejected_with_rs_1731_when_not_leader() {
        // A non-bootstrap node with no reachable peers cannot win an
        // election before its own randomized timeout floor
        // (DEFAULT_ELECTION_TIMEOUT_MIN = 150ms) elapses, so checking
        // immediately after spawn deterministically observes it as a
        // Follower (M7-S2 gating test).
        let config = RaftConfig::new(0, Vec::new(), false);
        let node = spawn_raft_node("127.0.0.1:0", config, Arc::new(InMemory::new()))
            .await
            .unwrap();
        assert!(!node.handle.is_leader());

        let db = mem_shard_db().await;
        let catalog = WorkloadCatalog::new(db).with_raft(node.handle.clone());

        let err = catalog
            .register_workload(&sample_workload())
            .await
            .unwrap_err();
        assert!(matches!(err, SqlError::NotLeader));
        assert_eq!(err.error_code().value(), 1731);

        // update_workload and remove_workload are gated identically.
        let err = catalog
            .update_workload(&sample_workload())
            .await
            .unwrap_err();
        assert!(matches!(err, SqlError::NotLeader));
        let err = catalog.remove_workload("fast").await.unwrap_err();
        assert!(matches!(err, SqlError::NotLeader));

        node.shutdown();
    }

    #[tokio::test]
    async fn writes_succeed_once_this_node_becomes_leader() {
        // A single-node "bootstrap" group has a majority of one — it
        // becomes leader almost immediately.
        let config = RaftConfig::new(0, Vec::new(), true);
        let node = spawn_raft_node("127.0.0.1:0", config, Arc::new(InMemory::new()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(node.handle.is_leader());

        let db = mem_shard_db().await;
        let catalog = WorkloadCatalog::new(db).with_raft(node.handle.clone());
        catalog.register_workload(&sample_workload()).await.unwrap();

        node.shutdown();
    }
}

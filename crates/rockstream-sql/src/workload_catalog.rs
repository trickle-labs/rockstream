use std::collections::HashMap;
use std::sync::Arc;

use rockstream_storage::keys::{CatalogKeyEncoder, CatalogType};
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::workload::WorkloadDef;

use crate::catalog::DEFAULT_NAMESPACE;
use crate::SqlError;

/// Durable workload catalog backed by `ShardDb`.
pub struct WorkloadCatalog {
    db: Arc<ShardDb>,
}

impl std::fmt::Debug for WorkloadCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkloadCatalog").finish_non_exhaustive()
    }
}

impl WorkloadCatalog {
    pub fn new(db: Arc<ShardDb>) -> Self {
        Self { db }
    }

    pub async fn register_workload(&self, workload: &WorkloadDef) -> Result<(), SqlError> {
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
        self.put_workload(workload).await
    }

    pub async fn remove_workload(&self, name: &str) -> Result<(), SqlError> {
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

    #[test]
    fn workload_object_id_is_stable() {
        assert_eq!(workload_object_id("fast"), workload_object_id("fast"));
        assert_ne!(workload_object_id("fast"), workload_object_id("slow"));
    }
}

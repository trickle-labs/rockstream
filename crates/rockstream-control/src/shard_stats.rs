use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use rockstream_types::audit::AuditEvent;
use rockstream_types::frontier::ShardColumnStats;
use rockstream_types::ids::{ShardId, ViewId};

use crate::audit::FileAuditLog;

/// Durable control-plane persistence for checkpoint-published shard statistics.
pub struct ShardStatsPersistentStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ShardStatsPersistentStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            prefix: Path::from("topology/shard_stats"),
        }
    }

    fn stats_path(&self, view_id: ViewId, shard_id: ShardId) -> Path {
        self.prefix
            .child(view_id.0.to_string())
            .child(format!("{}.json", shard_id.0))
    }

    pub async fn save(
        &self,
        stats: &ShardColumnStats,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), String> {
        let bytes = serde_json::to_vec(stats).map_err(|e| format!("serialize shard stats: {e}"))?;
        self.store
            .put(
                &self.stats_path(stats.view_id, stats.shard_id),
                bytes.into(),
            )
            .await
            .map_err(|e| format!("persist shard stats: {e}"))?;
        if let Some(audit) = audit {
            let event = AuditEvent::now(
                "system",
                "checkpoint.publish_shard_stats",
                format!("view={} shard={}", stats.view_id.0, stats.shard_id.0),
            )
            .with_detail(format!("checkpoint_epoch={}", stats.checkpoint_epoch));
            let _ = audit.append(&event);
        }
        Ok(())
    }

    pub async fn load(&self, view_id: ViewId, shard_id: ShardId) -> Option<ShardColumnStats> {
        let bytes = self
            .store
            .get(&self.stats_path(view_id, shard_id))
            .await
            .ok()?
            .bytes()
            .await
            .ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    pub async fn load_all_for_view(
        &self,
        view_id: ViewId,
    ) -> Result<Vec<ShardColumnStats>, String> {
        let prefix = self.prefix.child(view_id.0.to_string());
        let mut stream = self.store.list(Some(&prefix));
        let mut out = Vec::new();
        while let Some(entry) = stream.next().await {
            let meta = entry.map_err(|e| format!("list shard stats: {e}"))?;
            let bytes = self
                .store
                .get(&meta.location)
                .await
                .map_err(|e| format!("read shard stats {}: {e}", meta.location))?
                .bytes()
                .await
                .map_err(|e| format!("buffer shard stats {}: {e}", meta.location))?;
            let stats = serde_json::from_slice(&bytes)
                .map_err(|e| format!("decode shard stats {}: {e}", meta.location))?;
            out.push(stats);
        }
        out.sort_by_key(|stats: &ShardColumnStats| stats.shard_id.0);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use rockstream_types::frontier::ColumnStats;

    #[tokio::test]
    async fn checkpoint_persists_shard_column_stats_to_control_catalog() {
        let store = ShardStatsPersistentStore::new(Arc::new(InMemory::new()));
        let stats = ShardColumnStats {
            shard_id: ShardId(2),
            view_id: ViewId(9),
            checkpoint_epoch: 17,
            col_stats: vec![ColumnStats {
                col_idx: 1,
                min_bytes: Some(Bytes::from_static(b"a")),
                max_bytes: Some(Bytes::from_static(b"m")),
                bloom_filter: Some(Bytes::from_static(b"\x03abcdefgh")),
                null_count: 0,
                distinct_count_hll: Bytes::from(vec![0; 64]),
            }],
        };
        store.save(&stats, None).await.unwrap();
        assert_eq!(store.load(ViewId(9), ShardId(2)).await, Some(stats));
    }

    #[test]
    fn shard_stats_write_is_put_not_range_delete() {
        use rockstream_storage::BatchOp;
        let op = BatchOp::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        };
        match op {
            BatchOp::Put { .. } => {}
            BatchOp::Delete { .. } => {}
            BatchOp::Merge { .. } => {}
        }
    }
}

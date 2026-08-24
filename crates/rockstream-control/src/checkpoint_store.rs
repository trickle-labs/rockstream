use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use rockstream_types::checkpoint::{CheckpointId, ClusterCheckpoint};
use rockstream_types::error_code::RS_3022;
use rockstream_types::ids::ShardId;

use crate::audit::{AuditEvent, FileAuditLog};
use crate::checkpoint::ChangelogCheckpointContribution;

const MANIFEST_MAGIC: &[u8; 4] = b"RSC1";
const MANIFEST_HEADER_LEN: usize = 4 + 1 + 1 + 8;

#[derive(Debug, Clone)]
pub struct CheckpointManifestStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl CheckpointManifestStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            prefix: Path::from("control/checkpoints"),
        }
    }

    fn manifest_path(&self, checkpoint_id: CheckpointId) -> Path {
        self.prefix.child(checkpoint_id.0.to_string())
    }

    fn changelog_path(&self, checkpoint_id: CheckpointId, shard_id: ShardId) -> Path {
        Path::from("control/changelog-checkpoints")
            .child(format!("{}-{}", checkpoint_id.0, shard_id.0))
    }

    /// Persist one shard's delta-native checkpoint contribution before that
    /// shard confirms the cluster manifest.
    pub async fn save_changelog_contribution(
        &self,
        shard_id: ShardId,
        contribution: &ChangelogCheckpointContribution,
    ) -> Result<(), String> {
        let payload = serde_json::to_vec(contribution)
            .map_err(|error| format!("serialize changelog contribution: {error}"))?;
        self.store
            .put(
                &self.changelog_path(contribution.checkpoint_id, shard_id),
                payload.into(),
            )
            .await
            .map(|_| ())
            .map_err(|error| format!("persist changelog contribution: {error}"))
    }

    /// Load a single durable shard contribution without scanning live state.
    pub async fn load_changelog_contribution(
        &self,
        checkpoint_id: CheckpointId,
        shard_id: ShardId,
    ) -> Result<Option<ChangelogCheckpointContribution>, String> {
        let path = self.changelog_path(checkpoint_id, shard_id);
        let result = self.store.get(&path).await;
        let bytes = match result {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|error| format!("read changelog contribution: {error}"))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(format!("load changelog contribution: {error}")),
        };
        let contribution: ChangelogCheckpointContribution = serde_json::from_slice(&bytes)
            .map_err(|error| format!("decode changelog contribution: {error}"))?;
        if contribution.checkpoint_id != checkpoint_id {
            return Err(format!(
                "changelog contribution checkpoint mismatch: requested {}, found {}",
                checkpoint_id.0, contribution.checkpoint_id.0
            ));
        }
        Ok(Some(contribution))
    }

    /// Delete expired changelog objects one at a time. This deliberately uses
    /// object listing plus point deletion, never SlateDB range deletion.
    pub async fn gc_old_changelog_contributions(
        &self,
        latest_checkpoint_id: CheckpointId,
        retention_horizon: u64,
    ) -> Result<Vec<(CheckpointId, ShardId)>, String> {
        if latest_checkpoint_id.0 <= retention_horizon {
            return Ok(Vec::new());
        }
        let cutoff = latest_checkpoint_id.0 - retention_horizon;
        let prefix = Path::from("control/changelog-checkpoints");
        let mut listing = self.store.list(Some(&prefix));
        let mut deleted = Vec::new();
        while let Some(entry) = listing.next().await {
            let entry = entry.map_err(|error| format!("list changelog contributions: {error}"))?;
            let Some(name) = entry.location.filename() else {
                continue;
            };
            let Some((checkpoint, shard)) = name.split_once('-') else {
                continue;
            };
            let (Ok(checkpoint_id), Ok(shard_id)) = (checkpoint.parse(), shard.parse()) else {
                continue;
            };
            if checkpoint_id < cutoff {
                self.store
                    .delete(&entry.location)
                    .await
                    .map_err(|error| format!("delete changelog contribution: {error}"))?;
                deleted.push((CheckpointId(checkpoint_id), ShardId(shard_id)));
            }
        }
        deleted.sort_unstable();
        Ok(deleted)
    }

    pub async fn save_manifest(
        &self,
        manifest: &ClusterCheckpoint,
        codec_capability_floor: bool,
        audit: Option<&FileAuditLog>,
    ) -> Result<(), String> {
        let payload = encode_manifest(manifest, codec_capability_floor)?;
        self.store
            .put(&self.manifest_path(manifest.checkpoint_id), payload.into())
            .await
            .map_err(|e| format!("persist checkpoint manifest: {e}"))?;
        if let Some(audit) = audit {
            let event = AuditEvent::now(
                "system",
                "checkpoint.publish_manifest",
                manifest.checkpoint_id.to_string(),
            )
            .with_detail(format!(
                "codec={}",
                if codec_capability_floor {
                    "zstd"
                } else {
                    "json"
                }
            ));
            let _ = audit.append(&event);
        }
        Ok(())
    }

    pub async fn load_manifest(&self, checkpoint_id: CheckpointId) -> Option<ClusterCheckpoint> {
        self.load_manifest_checked(checkpoint_id)
            .await
            .ok()
            .flatten()
    }

    async fn load_manifest_checked(
        &self,
        checkpoint_id: CheckpointId,
    ) -> Result<Option<ClusterCheckpoint>, String> {
        let bytes = self
            .store
            .get(&self.manifest_path(checkpoint_id))
            .await
            .map_err(|error| format!("load checkpoint manifest {}: {error}", checkpoint_id.0))?
            .bytes()
            .await
            .map_err(|error| format!("read checkpoint manifest {}: {error}", checkpoint_id.0))?;
        decode_manifest(&bytes).map(Some).map_err(|error| {
            format!(
                "checkpoint manifest {} is malformed: {error}",
                checkpoint_id.0
            )
        })
    }

    pub async fn load_latest_manifest(&self) -> Result<Option<ClusterCheckpoint>, String> {
        let ids = self.list_manifest_ids().await?;
        let Some(checkpoint_id) = ids.into_iter().max() else {
            return Ok(None);
        };
        self.load_manifest_checked(checkpoint_id).await
    }

    pub async fn gc_old_manifests(
        &self,
        latest_checkpoint_id: CheckpointId,
        retention_horizon: u64,
        audit: Option<&FileAuditLog>,
    ) -> Result<Vec<CheckpointId>, String> {
        if latest_checkpoint_id.0 <= retention_horizon {
            return Ok(Vec::new());
        }
        let cutoff = latest_checkpoint_id.0 - retention_horizon;
        let mut deleted = Vec::new();
        for checkpoint_id in self.list_manifest_ids().await? {
            if checkpoint_id.0 < cutoff {
                self.store
                    .delete(&self.manifest_path(checkpoint_id))
                    .await
                    .map_err(|e| format!("delete checkpoint manifest {}: {e}", checkpoint_id.0))?;
                deleted.push(checkpoint_id);
            }
        }
        if let Some(audit) = audit {
            for checkpoint_id in &deleted {
                let event = AuditEvent::now(
                    "system",
                    "checkpoint.gc_manifest",
                    checkpoint_id.to_string(),
                )
                .with_detail(format!("retained_after={}", latest_checkpoint_id.0));
                let _ = audit.append(&event);
            }
        }
        Ok(deleted)
    }

    async fn list_manifest_ids(&self) -> Result<Vec<CheckpointId>, String> {
        let mut listing = self.store.list(Some(&self.prefix));
        let mut ids = Vec::new();
        while let Some(entry) = listing.next().await {
            let meta = entry.map_err(|e| format!("list checkpoint manifests: {e}"))?;
            if let Some(name) = meta.location.filename() {
                if let Ok(id) = name.parse::<u64>() {
                    ids.push(CheckpointId(id));
                }
            }
        }
        Ok(ids)
    }
}

fn encode_manifest(
    manifest: &ClusterCheckpoint,
    codec_capability_floor: bool,
) -> Result<Vec<u8>, String> {
    let json =
        serde_json::to_vec(manifest).map_err(|e| format!("serialize checkpoint manifest: {e}"))?;
    if !codec_capability_floor {
        return Ok(json);
    }
    let compressed = zstd::bulk::compress(&json, 1)
        .map_err(|e| manifest_codec_error(format!("zstd compression failed: {e}")))?;
    let mut framed = Vec::with_capacity(MANIFEST_HEADER_LEN + compressed.len());
    framed.extend_from_slice(MANIFEST_MAGIC);
    framed.push(1);
    framed.push(2);
    framed.extend_from_slice(&(json.len() as u64).to_be_bytes());
    framed.extend_from_slice(&compressed);
    Ok(framed)
}

fn decode_manifest(payload: &[u8]) -> Result<ClusterCheckpoint, String> {
    if payload.len() < MANIFEST_HEADER_LEN || &payload[..4] != MANIFEST_MAGIC {
        return serde_json::from_slice(payload)
            .map_err(|e| manifest_codec_error(format!("legacy json decode failed: {e}")));
    }
    let wire_version = payload[4];
    if wire_version != 1 {
        return Err(manifest_codec_error(format!(
            "unsupported checkpoint manifest wire_version {wire_version}"
        )));
    }
    if payload[5] != 2 {
        return Err(manifest_codec_error(format!(
            "unknown checkpoint manifest codec {}",
            payload[5]
        )));
    }
    let uncompressed_len = u64::from_be_bytes(
        payload[6..MANIFEST_HEADER_LEN]
            .try_into()
            .expect("manifest header length is fixed"),
    ) as usize;
    let json = zstd::bulk::decompress(&payload[MANIFEST_HEADER_LEN..], uncompressed_len)
        .map_err(|e| manifest_codec_error(format!("zstd decompression failed: {e}")))?;
    serde_json::from_slice(&json)
        .map_err(|e| manifest_codec_error(format!("checkpoint manifest decode failed: {e}")))
}

fn manifest_codec_error(detail: String) -> String {
    format!(
        "[{RS_3022}] {detail}. Next steps: verify the control-plane capability floor, inspect control: checkpoints/ payloads for corruption, and complete the rolling upgrade before enabling manifest compression."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use rockstream_types::checkpoint::PerShardCheckpoint;
    use rockstream_types::ids::ShardId;
    use rockstream_types::state_mutation::{EpochStateDelta, StateMutation};

    fn manifest() -> ClusterCheckpoint {
        let mut manifest = ClusterCheckpoint::new(CheckpointId(7));
        manifest.record_shard(ShardId(1), PerShardCheckpoint::new(CheckpointId(7), 11));
        manifest.record_shard(ShardId(2), PerShardCheckpoint::new(CheckpointId(7), 22));
        manifest
    }

    #[tokio::test]
    async fn changelog_contribution_roundtrip_preserves_exact_delta() {
        let store = Arc::new(InMemory::new());
        let checkpoints = CheckpointManifestStore::new(store);
        let contribution = ChangelogCheckpointContribution::new(
            CheckpointId(7),
            EpochStateDelta::from_mutations(vec![
                StateMutation::Put {
                    key: b"orders/7".to_vec(),
                    value: bytes::Bytes::from_static(b"paid"),
                },
                StateMutation::Delete {
                    key: b"orders/4".to_vec(),
                },
            ]),
        );

        checkpoints
            .save_changelog_contribution(ShardId(2), &contribution)
            .await
            .unwrap();
        assert_eq!(
            checkpoints
                .load_changelog_contribution(CheckpointId(7), ShardId(2))
                .await
                .unwrap(),
            Some(contribution)
        );
    }

    #[tokio::test]
    async fn changelog_gc_uses_point_deletes_and_keeps_the_retention_window() {
        let store = Arc::new(InMemory::new());
        let checkpoints = CheckpointManifestStore::new(store);
        for checkpoint_id in [CheckpointId(4), CheckpointId(5), CheckpointId(6)] {
            checkpoints
                .save_changelog_contribution(
                    ShardId(2),
                    &ChangelogCheckpointContribution::new(checkpoint_id, EpochStateDelta::new()),
                )
                .await
                .unwrap();
        }

        assert_eq!(
            checkpoints
                .gc_old_changelog_contributions(CheckpointId(6), 1)
                .await
                .unwrap(),
            vec![(CheckpointId(4), ShardId(2))]
        );
        assert_eq!(
            checkpoints
                .load_changelog_contribution(CheckpointId(4), ShardId(2))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            checkpoints
                .load_changelog_contribution(CheckpointId(5), ShardId(2))
                .await
                .unwrap(),
            Some(ChangelogCheckpointContribution::new(
                CheckpointId(5),
                EpochStateDelta::new(),
            ))
        );
        assert_eq!(
            checkpoints
                .load_changelog_contribution(CheckpointId(6), ShardId(2))
                .await
                .unwrap(),
            Some(ChangelogCheckpointContribution::new(
                CheckpointId(6),
                EpochStateDelta::new(),
            ))
        );
    }

    #[tokio::test]
    async fn checkpoint_store_roundtrip_zstd_manifest() {
        let store = Arc::new(InMemory::new());
        let manifests = CheckpointManifestStore::new(store.clone());
        let manifest = manifest();
        manifests
            .save_manifest(&manifest, true, None)
            .await
            .unwrap();
        let raw = store
            .get(&Path::from("control/checkpoints/7"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&raw[..4], MANIFEST_MAGIC);
        assert_eq!(raw[5], 2);
        assert_eq!(
            manifests.load_manifest(CheckpointId(7)).await,
            Some(manifest)
        );
    }

    #[tokio::test]
    async fn legacy_json_checkpoint_manifest_still_loads() {
        let store = Arc::new(InMemory::new());
        let manifests = CheckpointManifestStore::new(store.clone());
        let manifest = manifest();
        store
            .put(
                &Path::from("control/checkpoints/7"),
                serde_json::to_vec(&manifest).unwrap().into(),
            )
            .await
            .unwrap();
        assert_eq!(
            manifests.load_manifest(CheckpointId(7)).await,
            Some(manifest)
        );
    }

    #[tokio::test]
    async fn latest_manifest_does_not_fall_back_after_corruption() {
        let store = Arc::new(InMemory::new());
        let manifests = CheckpointManifestStore::new(store.clone());
        manifests
            .save_manifest(&manifest(), false, None)
            .await
            .unwrap();
        store
            .put(
                &Path::from("control/checkpoints/8"),
                bytes::Bytes::from_static(b"truncated").into(),
            )
            .await
            .unwrap();

        let error = manifests.load_latest_manifest().await.unwrap_err();
        assert!(error.contains("checkpoint manifest 8 is malformed"));
    }

    #[test]
    fn checkpoint_store_uses_put_and_scan_delete_not_range_delete() {
        let source = std::fs::read_to_string(format!(
            "{}/src/checkpoint_store.rs",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
        assert!(production.contains(".put("));
        assert!(production.contains(".list("));
        assert!(production.contains(".delete("));
        assert!(!production.contains(".range_delete("));
        assert!(!production.contains("delete_range("));
    }
}

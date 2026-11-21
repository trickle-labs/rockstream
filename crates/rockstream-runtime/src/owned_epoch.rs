use rockstream_storage::{keys::ShardKeyEncoder, ShardDb};
use rockstream_types::data_plane::OwnedEpochIntent;
use std::sync::Arc;
use tokio::sync::Mutex;

fn record_key() -> Vec<u8> {
    ShardKeyEncoder::meta_key(b"owned-source-epoch")
}

async fn load_record(db: &ShardDb) -> Result<Option<OwnedEpochIntent>, String> {
    let Some(bytes) = db
        .get(&record_key())
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| "invalid owned source epoch record".to_owned())
}

pub(crate) struct OwnedEpochJournal {
    db: Arc<ShardDb>,
    operation: Mutex<()>,
}

impl OwnedEpochJournal {
    pub(crate) fn new(db: Arc<ShardDb>) -> Self {
        Self {
            db,
            operation: Mutex::new(()),
        }
    }

    pub(crate) async fn load(&self) -> Result<Option<OwnedEpochIntent>, String> {
        let _operation = self.operation.lock().await;
        load_record(&self.db).await
    }

    pub(crate) async fn prepare(&self, intent: &OwnedEpochIntent) -> Result<(), String> {
        let _operation = self.operation.lock().await;
        if load_record(&self.db).await?.is_some() {
            return Err("owned source epoch requires reconciliation".to_owned());
        }
        let bytes = serde_json::to_vec(intent).map_err(|error| error.to_string())?;
        self.db
            .put(&record_key(), &bytes)
            .await
            .map_err(|error| error.to_string())
    }

    pub(crate) async fn finish(&self, intent: &OwnedEpochIntent) -> Result<(), String> {
        let _operation = self.operation.lock().await;
        if load_record(&self.db).await?.as_ref() != Some(intent) {
            return Err("owned source epoch does not match the pending record".to_owned());
        }
        self.db
            .delete(&record_key())
            .await
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::{ShardId, WorkloadId};

    #[tokio::test]
    async fn pending_epoch_survives_reopen_until_matching_finish() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let db = Arc::new(
            ShardDb::builder("owned-epoch", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let journal = OwnedEpochJournal::new(db.clone());
        let intent = OwnedEpochIntent {
            attempt_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            version: 2,
            request_id: "request-1".to_owned(),
            workload_id: WorkloadId(7),
            epoch: 9,
            source: "items".to_owned(),
            anchor_shard: ShardId(1),
            target_shards: vec![ShardId(1), ShardId(2)],
        };
        journal.prepare(&intent).await.unwrap();
        assert_eq!(journal.load().await.unwrap(), Some(intent.clone()));
        (*db).clone().close().await.unwrap();
        drop((journal, db));

        let db = Arc::new(
            ShardDb::builder("owned-epoch", store.clone())
                .build()
                .await
                .unwrap(),
        );
        let journal = OwnedEpochJournal::new(db.clone());
        assert_eq!(journal.load().await.unwrap(), Some(intent.clone()));
        assert_eq!(
            journal.prepare(&intent).await,
            Err("owned source epoch requires reconciliation".to_owned())
        );
        let different = OwnedEpochIntent {
            epoch: 10,
            ..intent.clone()
        };
        assert_eq!(
            journal.finish(&different).await,
            Err("owned source epoch does not match the pending record".to_owned())
        );
        assert_eq!(journal.load().await.unwrap(), Some(intent.clone()));
        journal.finish(&intent).await.unwrap();
        (*db).clone().close().await.unwrap();
        drop((journal, db));
        let db = Arc::new(
            ShardDb::builder("owned-epoch", store)
                .build()
                .await
                .unwrap(),
        );
        assert_eq!(OwnedEpochJournal::new(db).load().await.unwrap(), None);
    }

    #[tokio::test]
    async fn concurrent_prepare_preserves_one_exact_intent() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            ShardDb::builder("owned-epoch-concurrent", store)
                .build()
                .await
                .unwrap(),
        );
        let journal = OwnedEpochJournal::new(db);
        let first = OwnedEpochIntent {
            attempt_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            version: 2,
            request_id: "first".to_owned(),
            workload_id: WorkloadId(7),
            epoch: 9,
            source: "items".to_owned(),
            anchor_shard: ShardId(1),
            target_shards: vec![ShardId(1), ShardId(2)],
        };
        let second = OwnedEpochIntent {
            request_id: "second".to_owned(),
            epoch: 10,
            ..first.clone()
        };
        let (a, b) = tokio::join!(journal.prepare(&first), journal.prepare(&second));
        let expected = match (a, b) {
            (Ok(()), Err(error)) => {
                assert_eq!(error, "owned source epoch requires reconciliation");
                first
            }
            (Err(error), Ok(())) => {
                assert_eq!(error, "owned source epoch requires reconciliation");
                second
            }
            other => panic!("unexpected prepare outcomes: {other:?}"),
        };
        assert_eq!(journal.load().await.unwrap(), Some(expected));
    }

    #[tokio::test]
    async fn malformed_record_is_not_overwritten() {
        let store = Arc::new(object_store::memory::InMemory::new());
        let db = Arc::new(
            ShardDb::builder("owned-epoch-invalid", store)
                .build()
                .await
                .unwrap(),
        );
        db.put(&record_key(), b"bad").await.unwrap();
        let intent = OwnedEpochIntent {
            attempt_id: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
            version: 2,
            request_id: "request-1".to_owned(),
            workload_id: WorkloadId(7),
            epoch: 9,
            source: "items".to_owned(),
            anchor_shard: ShardId(1),
            target_shards: vec![ShardId(1)],
        };
        assert_eq!(
            OwnedEpochJournal::new(db.clone()).prepare(&intent).await,
            Err("invalid owned source epoch record".to_owned())
        );
        assert_eq!(
            db.get(&record_key()).await.unwrap().unwrap().as_ref(),
            b"bad"
        );
    }
}

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
};
use rockstream_types::authorization::PolicyScope;
use rockstream_types::ids::PrincipalId;
use serde::{Deserialize, Serialize};

pub(crate) const MAX_ARTIFACT_BYTES: usize = 10 * 1024 * 1024;
pub(crate) const MAX_ARTIFACT_STORAGE_BYTES: usize = 100 * 1024 * 1024;
pub(crate) const ARTIFACT_TTL: Duration = Duration::from_secs(10 * 60);
const LEDGER_CAS_RETRIES: usize = 32;
const LEDGER_FORMAT_VERSION: u16 = 1;

#[derive(Clone)]
pub(crate) struct SupportArtifactStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    available: Arc<AtomicBool>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ArtifactLedger {
    format_version: u16,
    artifacts: BTreeMap<String, LedgerArtifact>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
struct LedgerArtifact {
    owner: PrincipalId,
    scope: PolicyScope,
    size: usize,
    expires_at_ms: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct ArtifactExpiry {
    pub unix_ms: u64,
}

pub(crate) struct ArtifactMetadata {
    pub owner: PrincipalId,
    pub scope: PolicyScope,
    pub expires_at_ms: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ArtifactStoreError {
    TooLarge,
    StorageFull,
    Exists,
    NotFound,
    WrongOwner,
    Unsupported,
    StorageUnavailable,
}

impl SupportArtifactStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            prefix: Path::from("control/support-artifacts"),
            available: Arc::new(AtomicBool::new(true)),
        }
    }

    pub async fn initialize(&self) -> Result<(), ArtifactStoreError> {
        match async {
            self.mutate_ledger(|_| Ok(true)).await?;
            self.mutate_ledger(|_| Ok(true)).await
        }
        .await
        {
            Ok(()) => Ok(()),
            Err(ArtifactStoreError::Unsupported) => {
                self.available.store(false, Ordering::Release);
                Err(ArtifactStoreError::Unsupported)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn insert(
        &self,
        id: String,
        owner: PrincipalId,
        scope: PolicyScope,
        content: Arc<[u8]>,
        expiry: ArtifactExpiry,
        now_ms: u64,
    ) -> Result<(), ArtifactStoreError> {
        self.ensure_available()?;
        if content.len() > MAX_ARTIFACT_BYTES {
            return Err(ArtifactStoreError::TooLarge);
        }
        if expiry.unix_ms <= now_ms {
            return Err(ArtifactStoreError::NotFound);
        }
        self.cleanup_expired(now_ms).await?;

        let entry = LedgerArtifact {
            owner,
            scope,
            size: content.len(),
            expires_at_ms: expiry.unix_ms,
        };
        self.mutate_ledger(|ledger| {
            if let Some(existing) = ledger.artifacts.get(&id) {
                return if existing == &entry {
                    Ok(false)
                } else {
                    Err(ArtifactStoreError::Exists)
                };
            }
            let used = ledger
                .artifacts
                .values()
                .try_fold(0usize, |total, item| total.checked_add(item.size));
            let Some(next_size) = used.and_then(|used| used.checked_add(entry.size)) else {
                return Err(ArtifactStoreError::StorageFull);
            };
            if next_size > MAX_ARTIFACT_STORAGE_BYTES {
                return Err(ArtifactStoreError::StorageFull);
            }
            ledger.artifacts.insert(id.clone(), entry.clone());
            Ok(true)
        })
        .await?;

        let path = self.artifact_path(&id);
        match self
            .store
            .put_opts(
                &path,
                content.as_ref().to_vec().into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                let result = self
                    .store
                    .get(&path)
                    .await
                    .map_err(|_| ArtifactStoreError::StorageUnavailable)?;
                let existing = result
                    .bytes()
                    .await
                    .map_err(|_| ArtifactStoreError::StorageUnavailable)?;
                if existing.as_ref() == content.as_ref() {
                    Ok(())
                } else {
                    Err(ArtifactStoreError::Exists)
                }
            }
            Err(_) => Err(ArtifactStoreError::StorageUnavailable),
        }
    }

    pub async fn metadata(
        &self,
        id: &str,
        now_ms: u64,
    ) -> Result<ArtifactMetadata, ArtifactStoreError> {
        self.ensure_available()?;
        let (ledger, _) = self.read_ledger().await?;
        let artifact = ledger
            .artifacts
            .get(id)
            .filter(|artifact| artifact.expires_at_ms > now_ms)
            .ok_or(ArtifactStoreError::NotFound)?;
        let meta = self
            .store
            .head(&self.artifact_path(id))
            .await
            .map_err(|error| match error {
                ObjectStoreError::NotFound { .. } => ArtifactStoreError::NotFound,
                _ => ArtifactStoreError::StorageUnavailable,
            })?;
        if meta.size != artifact.size as u64 {
            return Err(ArtifactStoreError::StorageUnavailable);
        }
        Ok(ArtifactMetadata {
            owner: artifact.owner,
            scope: artifact.scope.clone(),
            expires_at_ms: artifact.expires_at_ms,
        })
    }

    pub async fn download(
        &self,
        id: &str,
        owner: PrincipalId,
        now_ms: u64,
    ) -> Result<(Arc<[u8]>, u64, Instant), ArtifactStoreError> {
        self.ensure_available()?;
        let (ledger, _) = self.read_ledger().await?;
        let artifact = ledger
            .artifacts
            .get(id)
            .filter(|artifact| artifact.expires_at_ms > now_ms)
            .ok_or(ArtifactStoreError::NotFound)?;
        if artifact.owner != owner {
            return Err(ArtifactStoreError::WrongOwner);
        }
        let result =
            self.store
                .get(&self.artifact_path(id))
                .await
                .map_err(|error| match error {
                    ObjectStoreError::NotFound { .. } => ArtifactStoreError::NotFound,
                    _ => ArtifactStoreError::StorageUnavailable,
                })?;
        let content = result
            .bytes()
            .await
            .map_err(|_| ArtifactStoreError::StorageUnavailable)?;
        if content.len() != artifact.size {
            return Err(ArtifactStoreError::StorageUnavailable);
        }
        let remaining = artifact.expires_at_ms.saturating_sub(now_ms);
        Ok((
            Arc::from(content.to_vec()),
            artifact.expires_at_ms,
            Instant::now() + Duration::from_millis(remaining),
        ))
    }

    pub async fn remove(&self, id: &str, owner: PrincipalId) -> Result<(), ArtifactStoreError> {
        self.ensure_available()?;
        let (ledger, _) = self.read_ledger().await?;
        let Some(artifact) = ledger.artifacts.get(id) else {
            return Ok(());
        };
        if artifact.owner != owner {
            return Ok(());
        }
        self.delete_artifact(id).await?;
        self.mutate_ledger(|ledger| {
            if ledger
                .artifacts
                .get(id)
                .is_some_and(|current| current.owner == owner)
            {
                ledger.artifacts.remove(id);
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .await
    }

    pub async fn cleanup_expired(&self, now_ms: u64) -> Result<(), ArtifactStoreError> {
        self.ensure_available()?;
        let (ledger, _) = self.read_ledger().await?;
        let expired: Vec<_> = ledger
            .artifacts
            .iter()
            .filter(|(_, artifact)| artifact.expires_at_ms <= now_ms)
            .map(|(id, _)| id.clone())
            .collect();
        if expired.is_empty() {
            return Ok(());
        }
        for id in &expired {
            self.delete_artifact(id).await?;
        }
        self.mutate_ledger(|ledger| {
            let before = ledger.artifacts.len();
            ledger
                .artifacts
                .retain(|_, artifact| artifact.expires_at_ms > now_ms);
            Ok(ledger.artifacts.len() != before)
        })
        .await
    }

    async fn delete_artifact(&self, id: &str) -> Result<(), ArtifactStoreError> {
        match self.store.delete(&self.artifact_path(id)).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(_) => Err(ArtifactStoreError::StorageUnavailable),
        }
    }

    pub fn disable(&self) {
        self.available.store(false, Ordering::Release);
    }

    pub fn is_available(&self) -> bool {
        self.available.load(Ordering::Acquire)
    }

    fn ensure_available(&self) -> Result<(), ArtifactStoreError> {
        if self.available.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(ArtifactStoreError::Unsupported)
        }
    }

    async fn read_ledger(
        &self,
    ) -> Result<(ArtifactLedger, Option<UpdateVersion>), ArtifactStoreError> {
        match self.store.get(&self.ledger_path()).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|_| ArtifactStoreError::StorageUnavailable)?;
                let ledger: ArtifactLedger = serde_json::from_slice(&bytes)
                    .map_err(|_| ArtifactStoreError::StorageUnavailable)?;
                if ledger.format_version != LEDGER_FORMAT_VERSION {
                    return Err(ArtifactStoreError::StorageUnavailable);
                }
                Ok((ledger, Some(version)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok((
                ArtifactLedger {
                    format_version: LEDGER_FORMAT_VERSION,
                    artifacts: BTreeMap::new(),
                },
                None,
            )),
            Err(_) => Err(ArtifactStoreError::StorageUnavailable),
        }
    }

    async fn mutate_ledger<F>(&self, mutation: F) -> Result<(), ArtifactStoreError>
    where
        F: Fn(&mut ArtifactLedger) -> Result<bool, ArtifactStoreError>,
    {
        for _ in 0..LEDGER_CAS_RETRIES {
            let (mut ledger, version) = self.read_ledger().await?;
            let changed = mutation(&mut ledger)?;
            if !changed {
                return Ok(());
            }
            let bytes =
                serde_json::to_vec(&ledger).map_err(|_| ArtifactStoreError::StorageUnavailable)?;
            let mode = version.map_or(PutMode::Create, PutMode::Update);
            match self
                .store
                .put_opts(
                    &self.ledger_path(),
                    bytes.into(),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(ObjectStoreError::AlreadyExists { .. })
                | Err(ObjectStoreError::Precondition { .. })
                | Err(ObjectStoreError::NotFound { .. }) => continue,
                Err(ObjectStoreError::NotImplemented { .. }) => {
                    return Err(ArtifactStoreError::Unsupported)
                }
                Err(_) => return Err(ArtifactStoreError::StorageUnavailable),
            }
        }
        Err(ArtifactStoreError::StorageUnavailable)
    }

    fn ledger_path(&self) -> Path {
        self.prefix.clone().join("ledger.json")
    }

    fn artifact_path(&self, id: &str) -> Path {
        self.prefix
            .clone()
            .join(format!("artifacts/{}.json", hex::encode(id.as_bytes())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn artifacts_are_shared_actor_bound_and_expire_at_the_deadline() {
        let object_store = Arc::new(InMemory::new());
        let store = SupportArtifactStore::new(object_store.clone());
        let other_service = SupportArtifactStore::new(object_store);
        store.initialize().await.unwrap();
        let now_ms = 1_000_000;
        let expires_at_ms = now_ms + ARTIFACT_TTL.as_millis() as u64;
        let content: Arc<[u8]> = Arc::from(b"{\"safe\":\"complete artifact\"}".to_vec());
        store
            .insert(
                "opaque-id".to_owned(),
                PrincipalId(7),
                PolicyScope::Cluster,
                content.clone(),
                ArtifactExpiry {
                    unix_ms: expires_at_ms,
                },
                now_ms,
            )
            .await
            .unwrap();
        let retained_content: Arc<[u8]> =
            Arc::from(b"{\"safe\":\"other actor artifact\"}".to_vec());
        let retained_expiry_ms = expires_at_ms + 60_000;
        store
            .insert(
                "retained-id".to_owned(),
                PrincipalId(8),
                PolicyScope::Cluster,
                retained_content.clone(),
                ArtifactExpiry {
                    unix_ms: retained_expiry_ms,
                },
                now_ms,
            )
            .await
            .unwrap();

        assert_eq!(
            other_service
                .download("opaque-id", PrincipalId(7), now_ms)
                .await
                .unwrap()
                .0,
            content
        );
        assert_eq!(
            other_service
                .download("opaque-id", PrincipalId(8), now_ms)
                .await,
            Err(ArtifactStoreError::WrongOwner)
        );
        let restarted_service = SupportArtifactStore::new(Arc::clone(&store.store));
        restarted_service.initialize().await.unwrap();
        restarted_service
            .cleanup_expired(expires_at_ms)
            .await
            .unwrap();
        assert_eq!(
            restarted_service
                .download("opaque-id", PrincipalId(7), expires_at_ms)
                .await,
            Err(ArtifactStoreError::NotFound)
        );
        assert!(matches!(
            restarted_service
                .store
                .get(&restarted_service.artifact_path("opaque-id"))
                .await,
            Err(ObjectStoreError::NotFound { .. })
        ));
        assert_eq!(
            restarted_service
                .download("retained-id", PrincipalId(8), expires_at_ms)
                .await
                .unwrap()
                .0,
            retained_content
        );
        assert_eq!(
            restarted_service
                .download("retained-id", PrincipalId(7), expires_at_ms)
                .await,
            Err(ArtifactStoreError::WrongOwner)
        );
        assert_eq!(
            restarted_service.read_ledger().await.unwrap().0.artifacts,
            BTreeMap::from([(
                "retained-id".to_owned(),
                LedgerArtifact {
                    owner: PrincipalId(8),
                    scope: PolicyScope::Cluster,
                    size: retained_content.len(),
                    expires_at_ms: retained_expiry_ms,
                },
            )])
        );
    }

    #[tokio::test]
    async fn concurrent_services_share_the_deployment_storage_budget() {
        let object_store = Arc::new(InMemory::new());
        let first = SupportArtifactStore::new(object_store.clone());
        let second = SupportArtifactStore::new(object_store);
        let now_ms = 1_000_000;
        let expires_at_ms = now_ms + ARTIFACT_TTL.as_millis() as u64;
        let content: Arc<[u8]> = Arc::from(vec![0; MAX_ARTIFACT_BYTES]);

        for index in 0..9 {
            let store = if index % 2 == 0 { &first } else { &second };
            store
                .insert(
                    format!("seed-{index}"),
                    PrincipalId(1),
                    PolicyScope::Cluster,
                    content.clone(),
                    ArtifactExpiry {
                        unix_ms: expires_at_ms,
                    },
                    now_ms,
                )
                .await
                .unwrap();
        }
        let expiry = ArtifactExpiry {
            unix_ms: expires_at_ms,
        };
        let (left, right) = tokio::join!(
            first.insert(
                "race-left".to_owned(),
                PrincipalId(1),
                PolicyScope::Cluster,
                content.clone(),
                expiry,
                now_ms,
            ),
            second.insert(
                "race-right".to_owned(),
                PrincipalId(1),
                PolicyScope::Cluster,
                content,
                expiry,
                now_ms,
            ),
        );

        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert!(matches!(
            left.err().or_else(|| right.err()),
            Some(ArtifactStoreError::StorageFull)
        ));
    }

    #[tokio::test]
    async fn concurrent_processes_share_the_minio_storage_budget() {
        assert!(rockstream_test_support::docker_available());
        let bucket = "rockstream-support-artifact-process-race";
        let (_minio, port) = rockstream_test_support::minio::start_minio(bucket)
            .await
            .expect("MinIO is required for cross-process artifact budget coverage");
        let directory = tempfile::tempdir().unwrap();
        let first_id = uuid::Uuid::new_v4().to_string();
        let second_id = uuid::Uuid::new_v4().to_string();
        let first_result_path = directory.path().join("first-result");
        let second_result_path = directory.path().join("second-result");
        let first_ready_path = directory.path().join("first-ready");
        let second_ready_path = directory.path().join("second-ready");
        let start_path = directory.path().join("start");
        let now_ms = chrono::Utc::now().timestamp_millis().max(1) as u64;
        let expires_at_ms = now_ms + ARTIFACT_TTL.as_millis() as u64;
        let filler_id = uuid::Uuid::new_v4().to_string();
        let mut artifacts = BTreeMap::new();
        artifacts.insert(
            filler_id.clone(),
            LedgerArtifact {
                owner: PrincipalId(1),
                scope: PolicyScope::Cluster,
                size: MAX_ARTIFACT_STORAGE_BYTES - 1,
                expires_at_ms,
            },
        );
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
            rockstream_test_support::minio::minio_object_store(port, bucket),
            "management",
        ));
        let ledger_path = Path::from("control/support-artifacts/ledger.json");
        store
            .put(
                &ledger_path,
                serde_json::to_vec(&ArtifactLedger {
                    format_version: LEDGER_FORMAT_VERSION,
                    artifacts,
                })
                .unwrap()
                .into(),
            )
            .await
            .unwrap();

        let child_test = "support_artifact::tests::support_artifact_budget_insert_child";
        let spawn_child =
            |id: &str, result_path: &std::path::Path, ready_path: &std::path::Path| {
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", child_test])
                    .env("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_PORT", port.to_string())
                    .env("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_BUCKET", bucket)
                    .env("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_ID", id)
                    .env(
                        "ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_EXPIRY",
                        expires_at_ms.to_string(),
                    )
                    .env("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_RESULT", result_path)
                    .env("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_READY", ready_path)
                    .env("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_START", &start_path)
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                    .unwrap()
            };
        let mut first = spawn_child(&first_id, &first_result_path, &first_ready_path);
        let mut second = spawn_child(&second_id, &second_result_path, &second_ready_path);
        let ready = tokio::time::timeout(Duration::from_secs(10), async {
            while !first_ready_path.exists() || !second_ready_path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        if ready.is_err() {
            let _ = first.kill();
            let _ = second.kill();
            panic!("artifact child processes did not reach the start barrier");
        }
        std::fs::write(&start_path, "start").unwrap();
        let output = first.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "first artifact child failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let output = second.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "second artifact child failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let first_result = std::fs::read_to_string(first_result_path).unwrap();
        let second_result = std::fs::read_to_string(second_result_path).unwrap();
        assert_eq!(
            usize::from(first_result == "inserted") + usize::from(second_result == "inserted"),
            1
        );
        assert_eq!(
            usize::from(first_result == "storage_full")
                + usize::from(second_result == "storage_full"),
            1
        );
        let (winner_id, loser_id) = if first_result == "inserted" {
            (&first_id, &second_id)
        } else {
            (&second_id, &first_id)
        };
        let recovered = store
            .get(&ledger_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let ledger: ArtifactLedger = serde_json::from_slice(&recovered).unwrap();
        let mut actual_ids = ledger.artifacts.keys().cloned().collect::<Vec<_>>();
        actual_ids.sort();
        let mut expected_ids = vec![filler_id, winner_id.clone()];
        expected_ids.sort();
        assert_eq!(actual_ids, expected_ids);
        assert_eq!(
            ledger
                .artifacts
                .values()
                .map(|artifact| artifact.size)
                .sum::<usize>(),
            MAX_ARTIFACT_STORAGE_BYTES
        );
        assert_eq!(
            store
                .get(&SupportArtifactStore::new(store.clone()).artifact_path(winner_id))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"x"
        );
        assert!(matches!(
            store
                .get(&SupportArtifactStore::new(store.clone()).artifact_path(loser_id))
                .await,
            Err(ObjectStoreError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn support_artifact_budget_insert_child() {
        let (
            Ok(port),
            Ok(bucket),
            Ok(id),
            Ok(expiry),
            Ok(result_path),
            Ok(ready_path),
            Ok(start_path),
        ) = (
            std::env::var("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_PORT"),
            std::env::var("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_BUCKET"),
            std::env::var("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_ID"),
            std::env::var("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_EXPIRY"),
            std::env::var("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_RESULT"),
            std::env::var("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_READY"),
            std::env::var("ROCKSTREAM_SUPPORT_ARTIFACT_CHILD_START"),
        )
        else {
            return;
        };
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
            rockstream_test_support::minio::minio_object_store(port.parse().unwrap(), &bucket),
            "management",
        ));
        let artifact_store = SupportArtifactStore::new(store);
        artifact_store.initialize().await.unwrap();
        std::fs::write(ready_path, "ready").unwrap();
        while !std::path::Path::new(&start_path).exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let now_ms = chrono::Utc::now().timestamp_millis().max(1) as u64;
        let result = artifact_store
            .insert(
                id,
                PrincipalId(1),
                PolicyScope::Cluster,
                Arc::from(b"x".to_vec()),
                ArtifactExpiry {
                    unix_ms: expiry.parse().unwrap(),
                },
                now_ms,
            )
            .await;
        let result = match result {
            Ok(()) => "inserted",
            Err(ArtifactStoreError::StorageFull) => "storage_full",
            Err(error) => panic!("unexpected cross-process insert result: {error:?}"),
        };
        std::fs::write(result_path, result).unwrap();
    }

    #[tokio::test]
    async fn local_filesystem_without_compare_and_swap_disables_artifacts() {
        let directory = tempfile::tempdir().unwrap();
        let object_store = Arc::new(LocalFileSystem::new_with_prefix(directory.path()).unwrap());
        let store = SupportArtifactStore::new(object_store);

        assert_eq!(
            store.initialize().await,
            Err(ArtifactStoreError::Unsupported)
        );
        assert!(!store.is_available());
        assert_eq!(
            store
                .insert(
                    "opaque-id".to_owned(),
                    PrincipalId(1),
                    PolicyScope::Cluster,
                    Arc::from(b"safe".to_vec()),
                    ArtifactExpiry { unix_ms: 2_000_000 },
                    1_000_000,
                )
                .await,
            Err(ArtifactStoreError::Unsupported)
        );
    }
}

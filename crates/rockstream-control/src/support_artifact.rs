use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion,
};
use rockstream_types::authorization::PolicyScope;
use rockstream_types::ids::PrincipalId;
use serde::{Deserialize, Serialize};

pub(crate) const MAX_ARTIFACT_BYTES: usize = 10 * 1024 * 1024;
pub(crate) const MAX_ARTIFACT_STORAGE_BYTES: usize = 100 * 1024 * 1024;
pub(crate) const MAX_ARTIFACTS_PER_DEPLOYMENT: usize = 2_048;
pub(crate) const ARTIFACT_TTL: Duration = Duration::from_secs(10 * 60);
pub(crate) const ARTIFACT_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
// Two passes plus one cleanup tick must fit inside the 60-second retention bound.
const ARTIFACT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(29);
const LEDGER_CAS_RETRIES: usize = 32;
const LEGACY_LEDGER_FORMAT_VERSION: u16 = 1;
const LEDGER_FORMAT_VERSION: u16 = 2;

pub(crate) fn maybe_crash_after_debug_test_boundary(boundary: &str) {
    #[cfg(any(debug_assertions, feature = "test-support-artifact-crash-injection"))]
    if std::env::var("ROCKSTREAM_TEST_SUPPORT_ARTIFACT_CRASH_AFTER").as_deref() == Ok(boundary) {
        std::process::exit(86);
    }
    #[cfg(not(any(debug_assertions, feature = "test-support-artifact-crash-injection")))]
    let _ = boundary;
}

#[derive(Clone)]
pub(crate) struct SupportArtifactStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    available: Arc<AtomicBool>,
    #[cfg(test)]
    pub(super) artifact_put_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    #[cfg(test)]
    cleanup_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ArtifactLedger {
    format_version: u16,
    artifacts: BTreeMap<String, LedgerArtifact>,
    #[serde(default)]
    object_generations: BTreeMap<String, String>,
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
            #[cfg(test)]
            artifact_put_gate: None,
            #[cfg(test)]
            cleanup_gate: None,
        }
    }

    pub async fn initialize(&self) -> Result<(), ArtifactStoreError> {
        match async {
            self.mutate_ledger(|_| Ok(true)).await?;
            self.mutate_ledger(|_| Ok(true)).await.map(|_| ())
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
        // A retry gets a fresh object path so stale cleanup snapshots cannot delete it.
        let new_generation = uuid::Uuid::new_v4().simple().to_string();
        let storage_generation = std::sync::Mutex::new(None);
        let reserved = self
            .mutate_ledger(|ledger| {
                if let Some(existing) = ledger.artifacts.get(&id) {
                    if existing == &entry {
                        *storage_generation
                            .lock()
                            .expect("artifact generation mutex poisoned") =
                            ledger.object_generations.get(&id).cloned();
                        return Ok(false);
                    }
                    return Err(ArtifactStoreError::Exists);
                }
                if ledger.artifacts.len() >= MAX_ARTIFACTS_PER_DEPLOYMENT {
                    return Err(ArtifactStoreError::StorageFull);
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
                ledger
                    .object_generations
                    .insert(id.clone(), new_generation.clone());
                *storage_generation
                    .lock()
                    .expect("artifact generation mutex poisoned") = Some(new_generation.clone());
                Ok(true)
            })
            .await?;

        if reserved {
            maybe_crash_after_debug_test_boundary("ledger-reserved");
        }

        #[cfg(test)]
        if let Some((started, resume)) = &self.artifact_put_gate {
            started.notify_one();
            resume.notified().await;
        }

        let storage_generation = storage_generation
            .into_inner()
            .expect("artifact generation mutex poisoned");
        let path = self.artifact_path_with_generation(&id, storage_generation.as_deref());
        match self
            .store
            .put_opts(
                &path,
                content.as_ref().to_vec().into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => {
                maybe_crash_after_debug_test_boundary("artifact-written");
                Ok(())
            }
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
                } else if existing.len() == content.len() {
                    // The operation key identifies one artifact; the first complete object wins.
                    Ok(())
                } else {
                    if reserved {
                        self.mutate_ledger(|ledger| {
                            if ledger.artifacts.get(&id) == Some(&entry) {
                                ledger.artifacts.remove(&id);
                                ledger.object_generations.remove(&id);
                                Ok(true)
                            } else {
                                Ok(false)
                            }
                        })
                        .await?;
                    }
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
            .head(&self.artifact_path_with_generation(
                id,
                ledger.object_generations.get(id).map(String::as_str),
            ))
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
        let result = self
            .store
            .get(&self.artifact_path_with_generation(
                id,
                ledger.object_generations.get(id).map(String::as_str),
            ))
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
        let Some(artifact) = ledger.artifacts.get(id).cloned() else {
            return self.delete_artifact(id, None).await;
        };
        if artifact.owner != owner {
            return Ok(());
        }
        let storage_generation = ledger.object_generations.get(id).cloned();
        self.mutate_ledger(|ledger| {
            if ledger.artifacts.get(id) == Some(&artifact)
                && ledger.object_generations.get(id) == storage_generation.as_ref()
            {
                ledger.artifacts.remove(id);
                ledger.object_generations.remove(id);
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .await?;
        self.delete_artifact(id, storage_generation.as_deref())
            .await
    }

    pub async fn cleanup_expired(&self, now_ms: u64) -> Result<(), ArtifactStoreError> {
        self.cleanup_expired_with_timeout(now_ms, ARTIFACT_CLEANUP_TIMEOUT)
            .await
    }

    async fn cleanup_expired_with_timeout(
        &self,
        now_ms: u64,
        timeout: Duration,
    ) -> Result<(), ArtifactStoreError> {
        tokio::time::timeout(timeout, self.cleanup_expired_inner(now_ms))
            .await
            .map_err(|_| ArtifactStoreError::StorageUnavailable)?
    }

    async fn cleanup_expired_inner(&self, now_ms: u64) -> Result<(), ArtifactStoreError> {
        self.ensure_available()?;
        let (ledger, _) = self.read_ledger().await?;
        let expired: Vec<_> = ledger
            .artifacts
            .iter()
            .filter(|(_, artifact)| artifact.expires_at_ms <= now_ms)
            .map(|(id, _)| (id.clone(), ledger.object_generations.get(id).cloned()))
            .collect();
        if expired.is_empty() {
            return self.cleanup_orphaned_artifacts().await;
        }
        let paths = expired
            .iter()
            .map(|(id, generation)| {
                Ok::<_, ObjectStoreError>(
                    self.artifact_path_with_generation(id, generation.as_deref()),
                )
            })
            .collect::<Vec<_>>();
        let _: Vec<()> = self
            .store
            .delete_stream(futures::stream::iter(paths).boxed())
            .map(|result| match result {
                Ok(_) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
                Err(_) => Err(ArtifactStoreError::StorageUnavailable),
            })
            .try_collect()
            .await?;
        maybe_crash_after_debug_test_boundary("cleanup-after-delete");
        self.mutate_ledger(|ledger| {
            let mut changed = false;
            for (id, _) in &expired {
                if ledger
                    .artifacts
                    .get(id)
                    .is_some_and(|artifact| artifact.expires_at_ms <= now_ms)
                {
                    ledger.artifacts.remove(id);
                    ledger.object_generations.remove(id);
                    changed = true;
                }
            }
            Ok(changed)
        })
        .await?;
        self.cleanup_orphaned_artifacts().await
    }

    async fn cleanup_orphaned_artifacts(&self) -> Result<(), ArtifactStoreError> {
        let artifact_prefix = self.prefix.clone().join("artifacts/").to_string();
        let paths = self
            .store
            .list(Some(&self.prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|_| ArtifactStoreError::StorageUnavailable)?;
        let (ledger, _) = self.read_ledger().await?;
        let retained = ledger
            .artifacts
            .keys()
            .map(|id| {
                self.artifact_path_with_generation(
                    id,
                    ledger.object_generations.get(id).map(String::as_str),
                )
            })
            .collect::<BTreeSet<_>>();
        let orphaned = paths
            .into_iter()
            .map(|meta| meta.location)
            .filter(|path| path.as_ref().starts_with(&artifact_prefix))
            .filter(|path| !retained.contains(path))
            .map(Ok::<_, ObjectStoreError>)
            .collect::<Vec<_>>();
        #[cfg(test)]
        if let Some((started, resume)) = &self.cleanup_gate {
            started.notify_one();
            resume.notified().await;
        }
        let _: Vec<()> = self
            .store
            .delete_stream(futures::stream::iter(orphaned).boxed())
            .map(|result| match result {
                Ok(_) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
                Err(_) => Err(ArtifactStoreError::StorageUnavailable),
            })
            .try_collect()
            .await?;
        Ok(())
    }

    async fn delete_artifact(
        &self,
        id: &str,
        storage_generation: Option<&str>,
    ) -> Result<(), ArtifactStoreError> {
        match self
            .store
            .delete(&self.artifact_path_with_generation(id, storage_generation))
            .await
        {
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
                let mut ledger: ArtifactLedger = serde_json::from_slice(&bytes)
                    .map_err(|_| ArtifactStoreError::StorageUnavailable)?;
                match ledger.format_version {
                    LEGACY_LEDGER_FORMAT_VERSION => {
                        ledger.format_version = LEDGER_FORMAT_VERSION;
                    }
                    LEDGER_FORMAT_VERSION => {}
                    _ => return Err(ArtifactStoreError::StorageUnavailable),
                }
                Ok((ledger, Some(version)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok((
                ArtifactLedger {
                    format_version: LEDGER_FORMAT_VERSION,
                    artifacts: BTreeMap::new(),
                    object_generations: BTreeMap::new(),
                },
                None,
            )),
            Err(_) => Err(ArtifactStoreError::StorageUnavailable),
        }
    }

    async fn mutate_ledger<F>(&self, mutation: F) -> Result<bool, ArtifactStoreError>
    where
        F: Fn(&mut ArtifactLedger) -> Result<bool, ArtifactStoreError>,
    {
        for _ in 0..LEDGER_CAS_RETRIES {
            let (mut ledger, version) = self.read_ledger().await?;
            let changed = mutation(&mut ledger)?;
            if !changed {
                return Ok(false);
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
                Ok(_) => return Ok(true),
                Err(ObjectStoreError::AlreadyExists { .. })
                | Err(ObjectStoreError::Precondition { .. })
                | Err(ObjectStoreError::NotFound { .. }) => continue,
                Err(ObjectStoreError::NotImplemented { .. }) => {
                    return Err(ArtifactStoreError::Unsupported);
                }
                Err(_) => return Err(ArtifactStoreError::StorageUnavailable),
            }
        }
        Err(ArtifactStoreError::StorageUnavailable)
    }

    fn ledger_path(&self) -> Path {
        self.prefix.clone().join("ledger.json")
    }

    #[cfg(test)]
    fn artifact_path(&self, id: &str) -> Path {
        self.artifact_path_with_generation(id, None)
    }

    fn artifact_path_with_generation(&self, id: &str, storage_generation: Option<&str>) -> Path {
        let generation = storage_generation
            .map(|generation| format!("-{generation}"))
            .unwrap_or_default();
        self.prefix.clone().join(format!(
            "artifacts/{}{generation}.json",
            hex::encode(id.as_bytes())
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tokio::sync::Notify;

    #[derive(Debug)]
    struct FailingListStore {
        inner: Arc<InMemory>,
    }

    impl std::fmt::Display for FailingListStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingListStore")
        }
    }

    fn listing_failure() -> ObjectStoreError {
        ObjectStoreError::Generic {
            store: "support artifact test",
            source: Box::new(std::io::Error::other("injected list failure")),
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingListStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            options: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            paths: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(paths)
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            Box::pin(futures::stream::iter(vec![Err(listing_failure())]))
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn artifact_cleanup_fails_closed_when_prefix_listing_is_unavailable() {
        let store = SupportArtifactStore::new(Arc::new(FailingListStore {
            inner: Arc::new(InMemory::new()),
        }));
        store.initialize().await.unwrap();

        assert_eq!(
            store.cleanup_expired(1_000).await,
            Err(ArtifactStoreError::StorageUnavailable)
        );
    }

    #[tokio::test]
    async fn reserved_artifact_is_written_when_retried_after_store_restart() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = SupportArtifactStore::new(Arc::clone(&object_store));
        store.initialize().await.unwrap();
        let id = "reserved-artifact".to_owned();
        let now_ms = 1_000_000;
        let expires_at_ms = now_ms + ARTIFACT_TTL.as_millis() as u64;
        let content: Arc<[u8]> = Arc::from(b"{\"safe\":\"resumed\"}".to_vec());
        let expiry = ArtifactExpiry {
            unix_ms: expires_at_ms,
        };
        let reservation = LedgerArtifact {
            owner: PrincipalId(7),
            scope: PolicyScope::Cluster,
            size: content.len(),
            expires_at_ms,
        };
        store
            .mutate_ledger(|ledger| {
                ledger.artifacts.insert(id.clone(), reservation.clone());
                Ok(true)
            })
            .await
            .unwrap();

        let restarted = SupportArtifactStore::new(object_store);
        restarted.initialize().await.unwrap();
        restarted
            .insert(
                id,
                PrincipalId(7),
                PolicyScope::Cluster,
                content.clone(),
                expiry,
                now_ms,
            )
            .await
            .unwrap();
        let metadata = restarted
            .metadata("reserved-artifact", now_ms)
            .await
            .unwrap();
        assert_eq!(metadata.owner, reservation.owner);
        assert_eq!(metadata.scope, reservation.scope);
        assert_eq!(metadata.expires_at_ms, reservation.expires_at_ms);
        let (downloaded, downloaded_expiry, _) = restarted
            .download("reserved-artifact", PrincipalId(7), now_ms)
            .await
            .unwrap();
        assert_eq!(downloaded.as_ref(), content.as_ref());
        assert_eq!(downloaded_expiry, expires_at_ms);
    }

    #[tokio::test]
    async fn concurrent_same_id_writers_preserve_the_first_object() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut first = SupportArtifactStore::new(Arc::clone(&object_store));
        let second = SupportArtifactStore::new(object_store);
        first.initialize().await.unwrap();
        let started = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        first.artifact_put_gate = Some((Arc::clone(&started), Arc::clone(&resume)));

        let id = "concurrent-same-id".to_owned();
        let owner = PrincipalId(7);
        let scope = PolicyScope::Cluster;
        let now_ms = 1_000_000;
        let expiry = ArtifactExpiry {
            unix_ms: now_ms + ARTIFACT_TTL.as_millis() as u64,
        };
        let first_scope = scope.clone();
        let reserved_content: Arc<[u8]> = Arc::from(b"first".to_vec());
        let committed_content: Arc<[u8]> = Arc::from(b"other".to_vec());
        let first_task = tokio::spawn(async move {
            first
                .insert(
                    id.clone(),
                    owner,
                    first_scope,
                    reserved_content,
                    expiry,
                    now_ms,
                )
                .await
        });
        started.notified().await;
        second
            .insert(
                "concurrent-same-id".to_owned(),
                owner,
                scope.clone(),
                committed_content.clone(),
                expiry,
                now_ms,
            )
            .await
            .unwrap();
        resume.notify_one();
        first_task.await.unwrap().unwrap();

        let (downloaded, _, _) = second
            .download("concurrent-same-id", owner, now_ms)
            .await
            .unwrap();
        assert_eq!(downloaded.as_ref(), committed_content.as_ref());
        assert_eq!(
            second
                .read_ledger()
                .await
                .unwrap()
                .0
                .artifacts
                .get("concurrent-same-id")
                .unwrap()
                .size,
            committed_content.len()
        );
    }

    #[tokio::test]
    async fn concurrent_orphan_cleanup_cannot_delete_a_retry_object() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut cleaner = SupportArtifactStore::new(Arc::clone(&object_store));
        let writer = SupportArtifactStore::new(Arc::clone(&object_store));
        cleaner.initialize().await.unwrap();
        writer.initialize().await.unwrap();
        let id = "orphan-retry";
        let now_ms = 1_000;
        let expiry = ArtifactExpiry { unix_ms: 2_000 };
        let requested: Arc<[u8]> = Arc::from(b"retry result".to_vec());
        object_store
            .put(&cleaner.artifact_path(id), b"late orphan".to_vec().into())
            .await
            .unwrap();

        let started = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        cleaner.cleanup_gate = Some((Arc::clone(&started), Arc::clone(&resume)));
        let cleanup = tokio::spawn(async move { cleaner.cleanup_expired(now_ms).await });
        started.notified().await;

        writer
            .insert(
                id.to_owned(),
                PrincipalId(7),
                PolicyScope::Cluster,
                requested.clone(),
                expiry,
                now_ms,
            )
            .await
            .unwrap();
        resume.notify_one();
        cleanup.await.unwrap().unwrap();

        let (downloaded, _, _) = writer.download(id, PrincipalId(7), now_ms).await.unwrap();
        assert_eq!(downloaded.as_ref(), requested.as_ref());
    }

    #[tokio::test]
    async fn orphaned_late_write_is_removed_during_restart_cleanup() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut writer = SupportArtifactStore::new(Arc::clone(&object_store));
        let cleaner = SupportArtifactStore::new(Arc::clone(&object_store));
        writer.initialize().await.unwrap();
        let now_ms = 1_000_000;
        let expiry = ArtifactExpiry {
            unix_ms: now_ms + ARTIFACT_TTL.as_millis() as u64,
        };
        let retained_content: Arc<[u8]> = Arc::from(b"other actor's artifact".to_vec());
        cleaner
            .insert(
                "retained-artifact".to_owned(),
                PrincipalId(8),
                PolicyScope::Cluster,
                retained_content.clone(),
                expiry,
                now_ms,
            )
            .await
            .unwrap();

        let started = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        writer.artifact_put_gate = Some((Arc::clone(&started), Arc::clone(&resume)));
        let writer_task_store = writer.clone();
        let writer_task = tokio::spawn(async move {
            writer_task_store
                .insert(
                    "late-artifact".to_owned(),
                    PrincipalId(7),
                    PolicyScope::Cluster,
                    Arc::from(b"late write".to_vec()),
                    expiry,
                    now_ms,
                )
                .await
        });
        started.notified().await;
        let ledger = writer.read_ledger().await.unwrap().0;
        let late_path = writer.artifact_path_with_generation(
            "late-artifact",
            ledger
                .object_generations
                .get("late-artifact")
                .map(String::as_str),
        );

        cleaner
            .remove("late-artifact", PrincipalId(7))
            .await
            .unwrap();
        resume.notify_one();
        writer_task.await.unwrap().unwrap();
        assert_eq!(
            object_store
                .get(&late_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"late write"
        );
        let listed = object_store
            .list(Some(&cleaner.prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(
            listed.iter().any(|meta| meta.location == late_path),
            "late object was not returned by artifact-prefix listing: {listed:?}"
        );
        assert_eq!(
            cleaner
                .read_ledger()
                .await
                .unwrap()
                .0
                .artifacts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["retained-artifact"]
        );

        let restarted = SupportArtifactStore::new(Arc::clone(&object_store));
        restarted.initialize().await.unwrap();
        restarted.cleanup_expired(now_ms).await.unwrap();
        assert!(matches!(
            object_store.get(&late_path).await,
            Err(ObjectStoreError::NotFound { .. })
        ));
        let (retained, _, _) = restarted
            .download("retained-artifact", PrincipalId(8), now_ms)
            .await
            .unwrap();
        assert_eq!(retained.as_ref(), retained_content.as_ref());
        assert_eq!(
            restarted
                .read_ledger()
                .await
                .unwrap()
                .0
                .artifacts
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["retained-artifact"]
        );
    }

    #[tokio::test]
    async fn legacy_ledger_keeps_using_the_original_object_path() {
        let object_store = Arc::new(InMemory::new());
        let store = SupportArtifactStore::new(object_store.clone());
        let id = "legacy-artifact";
        let content: Arc<[u8]> = Arc::from(b"legacy object".to_vec());
        let mut legacy_ledger = serde_json::to_value(ArtifactLedger {
            format_version: LEGACY_LEDGER_FORMAT_VERSION,
            artifacts: BTreeMap::from([(
                id.to_owned(),
                LedgerArtifact {
                    owner: PrincipalId(7),
                    scope: PolicyScope::Cluster,
                    size: content.len(),
                    expires_at_ms: 2_000,
                },
            )]),
            object_generations: BTreeMap::new(),
        })
        .unwrap();
        legacy_ledger
            .as_object_mut()
            .unwrap()
            .remove("object_generations")
            .unwrap();
        object_store
            .put(
                &store.ledger_path(),
                serde_json::to_vec(&legacy_ledger).unwrap().into(),
            )
            .await
            .unwrap();
        object_store
            .put(&store.artifact_path(id), content.to_vec().into())
            .await
            .unwrap();

        store.initialize().await.unwrap();
        let (downloaded, _, _) = store.download(id, PrincipalId(7), 1_000).await.unwrap();
        assert_eq!(downloaded.as_ref(), content.as_ref());
        assert_eq!(
            store.read_ledger().await.unwrap().0.format_version,
            LEDGER_FORMAT_VERSION
        );
        assert_eq!(
            store
                .read_ledger()
                .await
                .unwrap()
                .0
                .object_generations
                .get(id),
            None
        );
    }

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
        let ledger = store.read_ledger().await.unwrap().0;
        let expired_path = store.artifact_path_with_generation(
            "opaque-id",
            ledger
                .object_generations
                .get("opaque-id")
                .map(String::as_str),
        );

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
            restarted_service.store.get(&expired_path).await,
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
    async fn artifact_over_limit_is_rejected_without_writing_data() {
        let object_store = Arc::new(InMemory::new());
        let store = SupportArtifactStore::new(object_store.clone());
        store.initialize().await.unwrap();
        let id = "over-limit";

        assert_eq!(
            store
                .insert(
                    id.to_owned(),
                    PrincipalId(1),
                    PolicyScope::Cluster,
                    Arc::from(vec![0; MAX_ARTIFACT_BYTES + 1]),
                    ArtifactExpiry { unix_ms: 2_000 },
                    1_000,
                )
                .await,
            Err(ArtifactStoreError::TooLarge)
        );
        assert!(store.read_ledger().await.unwrap().0.artifacts.is_empty());
        assert!(matches!(
            object_store.get(&store.artifact_path(id)).await,
            Err(ObjectStoreError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn artifact_count_over_limit_preserves_the_complete_ledger() {
        let object_store = Arc::new(InMemory::new());
        let store = SupportArtifactStore::new(object_store.clone());
        store.initialize().await.unwrap();
        let expected = (0..MAX_ARTIFACTS_PER_DEPLOYMENT)
            .map(|index| {
                (
                    format!("retained-{index:04}"),
                    LedgerArtifact {
                        owner: PrincipalId(1),
                        scope: PolicyScope::Cluster,
                        size: 0,
                        expires_at_ms: 2_000,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        store
            .mutate_ledger(|ledger| {
                ledger.artifacts = expected.clone();
                Ok(true)
            })
            .await
            .unwrap();

        let id = "over-count";
        assert_eq!(
            store
                .insert(
                    id.to_owned(),
                    PrincipalId(1),
                    PolicyScope::Cluster,
                    Arc::from(b"{\"safe\":\"small\"}".to_vec()),
                    ArtifactExpiry { unix_ms: 2_000 },
                    1_000,
                )
                .await,
            Err(ArtifactStoreError::StorageFull)
        );
        assert_eq!(store.read_ledger().await.unwrap().0.artifacts, expected);
        assert!(matches!(
            object_store.get(&store.artifact_path(id)).await,
            Err(ObjectStoreError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn concurrent_services_at_artifact_count_limit_reject_without_eviction() {
        let object_store = Arc::new(InMemory::new());
        let first = SupportArtifactStore::new(object_store.clone());
        let second = SupportArtifactStore::new(object_store.clone());
        first.initialize().await.unwrap();
        let now_ms = 1_000;
        let expiry = ArtifactExpiry { unix_ms: 2_000 };
        let mut expected = (0..MAX_ARTIFACTS_PER_DEPLOYMENT - 1)
            .map(|index| {
                (
                    format!("retained-{index:04}"),
                    LedgerArtifact {
                        owner: PrincipalId(1),
                        scope: PolicyScope::Cluster,
                        size: 0,
                        expires_at_ms: expiry.unix_ms,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        first
            .mutate_ledger(|ledger| {
                ledger.artifacts = expected.clone();
                Ok(true)
            })
            .await
            .unwrap();

        let content: Arc<[u8]> = Arc::from(Vec::new());
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
        let (accepted_id, rejected_id, rejected_result) = if left.is_ok() {
            ("race-left", "race-right", right)
        } else {
            ("race-right", "race-left", left)
        };
        assert!(matches!(
            rejected_result,
            Err(ArtifactStoreError::StorageFull)
        ));
        expected.insert(
            accepted_id.to_owned(),
            LedgerArtifact {
                owner: PrincipalId(1),
                scope: PolicyScope::Cluster,
                size: 0,
                expires_at_ms: expiry.unix_ms,
            },
        );
        assert_eq!(first.read_ledger().await.unwrap().0.artifacts, expected);
        let ledger = first.read_ledger().await.unwrap().0;
        let accepted_path = first.artifact_path_with_generation(
            accepted_id,
            ledger
                .object_generations
                .get(accepted_id)
                .map(String::as_str),
        );
        assert_eq!(
            object_store
                .get(&accepted_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b""
        );
        assert!(matches!(
            object_store.get(&first.artifact_path(rejected_id)).await,
            Err(ObjectStoreError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn concurrent_artifact_writers_fill_remaining_slots_without_eviction() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = SupportArtifactStore::new(Arc::clone(&object_store));
        store.initialize().await.unwrap();
        let now_ms = 1_000;
        let expiry = ArtifactExpiry { unix_ms: 2_000 };
        let entry = || LedgerArtifact {
            owner: PrincipalId(1),
            scope: PolicyScope::Cluster,
            size: 0,
            expires_at_ms: expiry.unix_ms,
        };
        let mut expected = (0..MAX_ARTIFACTS_PER_DEPLOYMENT - 8)
            .map(|index| (format!("retained-{index:04}"), entry()))
            .collect::<BTreeMap<_, _>>();
        store
            .mutate_ledger(|ledger| {
                ledger.artifacts = expected.clone();
                Ok(true)
            })
            .await
            .unwrap();

        let services = (0..16)
            .map(|_| SupportArtifactStore::new(Arc::clone(&object_store)))
            .collect::<Vec<_>>();
        let attempts = (0..16).map(|index| {
            let service = services[index % services.len()].clone();
            async move {
                let id = format!("pressure-{index:02}");
                let result = service
                    .insert(
                        id.clone(),
                        PrincipalId(1),
                        PolicyScope::Cluster,
                        Arc::from(Vec::new()),
                        expiry,
                        now_ms,
                    )
                    .await;
                (id, result)
            }
        });
        let results = futures::future::join_all(attempts).await;
        let (accepted, rejected): (Vec<_>, Vec<_>) =
            results.into_iter().partition(|(_, result)| result.is_ok());
        assert_eq!(accepted.len(), 8);
        for (id, result) in accepted {
            assert_eq!(result, Ok(()));
            expected.insert(id.clone(), entry());
            let ledger = store.read_ledger().await.unwrap().0;
            let path = store.artifact_path_with_generation(
                &id,
                ledger.object_generations.get(&id).map(String::as_str),
            );
            assert_eq!(
                object_store
                    .get(&path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .as_ref(),
                b""
            );
        }
        for (id, result) in rejected {
            assert_eq!(result, Err(ArtifactStoreError::StorageFull));
            assert!(matches!(
                object_store.get(&store.artifact_path(&id)).await,
                Err(ObjectStoreError::NotFound { .. })
            ));
        }

        assert_eq!(expected.len(), MAX_ARTIFACTS_PER_DEPLOYMENT);
        assert_eq!(store.read_ledger().await.unwrap().0.artifacts, expected);
    }

    #[tokio::test]
    async fn cleanup_at_the_artifact_count_limit_removes_every_expired_object() {
        let object_store = Arc::new(InMemory::new());
        let store = SupportArtifactStore::new(object_store.clone());
        store.initialize().await.unwrap();
        let expected = (0..MAX_ARTIFACTS_PER_DEPLOYMENT)
            .map(|index| {
                (
                    format!("expired-{index:04}"),
                    LedgerArtifact {
                        owner: PrincipalId(1),
                        scope: PolicyScope::Cluster,
                        size: 0,
                        expires_at_ms: 2_000,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        store
            .mutate_ledger(|ledger| {
                ledger.artifacts = expected.clone();
                Ok(true)
            })
            .await
            .unwrap();
        for id in expected.keys() {
            object_store
                .put(&store.artifact_path(id), Vec::new().into())
                .await
                .unwrap();
        }

        store.cleanup_expired(2_000).await.unwrap();

        assert!(store.read_ledger().await.unwrap().0.artifacts.is_empty());
        for id in expected.keys() {
            assert!(matches!(
                object_store.get(&store.artifact_path(id)).await,
                Err(ObjectStoreError::NotFound { .. })
            ));
        }
    }

    #[tokio::test]
    async fn minio_cleanup_at_the_artifact_count_limit_finishes_within_the_bound() {
        assert!(rockstream_test_support::docker_available());
        let bucket = "rockstream-support-artifact-cleanup-pressure";
        let (_minio, port) = rockstream_test_support::minio::start_minio(bucket)
            .await
            .expect("MinIO is required for maximum-count cleanup coverage");
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
            rockstream_test_support::minio::minio_object_store(port, bucket),
            "management",
        ));
        let store = SupportArtifactStore::new(Arc::clone(&object_store));
        store.initialize().await.unwrap();
        let now_ms = chrono::Utc::now().timestamp_millis().max(1) as u64;
        let artifacts = (0..MAX_ARTIFACTS_PER_DEPLOYMENT)
            .map(|index| {
                (
                    format!("expired-{index:04}"),
                    LedgerArtifact {
                        owner: PrincipalId(1),
                        scope: PolicyScope::Cluster,
                        size: 0,
                        expires_at_ms: now_ms,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        store
            .mutate_ledger(|ledger| {
                ledger.artifacts = artifacts.clone();
                Ok(true)
            })
            .await
            .unwrap();
        let paths = artifacts
            .keys()
            .map(|id| store.artifact_path(id))
            .collect::<Vec<_>>();
        futures::stream::iter(paths)
            .map(|path| {
                let object_store = Arc::clone(&object_store);
                async move { object_store.put(&path, Vec::new().into()).await }
            })
            .buffer_unordered(32)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let started = Instant::now();
        store.cleanup_expired(now_ms).await.unwrap();
        assert!(started.elapsed() <= ARTIFACT_CLEANUP_TIMEOUT);
        assert!(store.read_ledger().await.unwrap().0.artifacts.is_empty());
        for id in artifacts.keys() {
            assert!(matches!(
                object_store.get(&store.artifact_path(id)).await,
                Err(ObjectStoreError::NotFound { .. })
            ));
        }
    }

    #[tokio::test]
    async fn cleanup_rescans_artifacts_that_expire_during_a_sweep() {
        let object_store = Arc::new(InMemory::new());
        let delete_delay = Duration::from_millis(15);
        let throttled_store = Arc::new(object_store::throttle::ThrottledStore::new(
            object_store.clone(),
            object_store::throttle::ThrottleConfig {
                wait_delete_per_call: delete_delay,
                ..Default::default()
            },
        ));
        let store = SupportArtifactStore::new(throttled_store);
        store.initialize().await.unwrap();

        let already_expired = LedgerArtifact {
            owner: PrincipalId(1),
            scope: PolicyScope::Cluster,
            size: 7,
            expires_at_ms: 1_000,
        };
        let expires_during_sweep = LedgerArtifact {
            owner: PrincipalId(1),
            scope: PolicyScope::Cluster,
            size: 7,
            expires_at_ms: 1_001,
        };
        store
            .mutate_ledger(|ledger| {
                ledger
                    .artifacts
                    .insert("already-expired".to_owned(), already_expired.clone());
                ledger.artifacts.insert(
                    "expires-during-sweep".to_owned(),
                    expires_during_sweep.clone(),
                );
                Ok(true)
            })
            .await
            .unwrap();
        for (id, content) in [
            ("already-expired", b"expired".as_slice()),
            ("expires-during-sweep", b"pending".as_slice()),
        ] {
            object_store
                .put(&store.artifact_path(id), content.to_vec().into())
                .await
                .unwrap();
        }

        let started = Instant::now();
        store.cleanup_expired(1_000).await.unwrap();
        assert!(started.elapsed() >= delete_delay);
        assert_eq!(
            store.read_ledger().await.unwrap().0.artifacts,
            BTreeMap::from([("expires-during-sweep".to_owned(), expires_during_sweep)])
        );
        assert!(matches!(
            object_store
                .get(&store.artifact_path("already-expired"))
                .await,
            Err(ObjectStoreError::NotFound { .. })
        ));
        assert_eq!(
            object_store
                .get(&store.artifact_path("expires-during-sweep"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"pending"
        );

        store.cleanup_expired(1_002).await.unwrap();
        assert!(store.read_ledger().await.unwrap().0.artifacts.is_empty());
        for id in ["already-expired", "expires-during-sweep"] {
            assert!(matches!(
                object_store.get(&store.artifact_path(id)).await,
                Err(ObjectStoreError::NotFound { .. })
            ));
        }
    }

    #[test]
    fn cleanup_timeout_accounts_for_an_artifact_expiring_mid_sweep() {
        assert!(
            ARTIFACT_CLEANUP_TIMEOUT * 2 + ARTIFACT_CLEANUP_INTERVAL <= Duration::from_secs(60)
        );
    }

    #[tokio::test]
    async fn cleanup_timeout_keeps_the_ledger_entry_for_retry() {
        let object_store = Arc::new(InMemory::new());
        let throttled_store = Arc::new(object_store::throttle::ThrottledStore::new(
            object_store.clone(),
            object_store::throttle::ThrottleConfig {
                wait_delete_per_call: Duration::from_secs(1),
                ..Default::default()
            },
        ));
        let store = SupportArtifactStore::new(throttled_store.clone());
        store.initialize().await.unwrap();
        let entry = LedgerArtifact {
            owner: PrincipalId(1),
            scope: PolicyScope::Cluster,
            size: 7,
            expires_at_ms: 2_000,
        };
        store
            .mutate_ledger(|ledger| {
                ledger.artifacts.insert("expired".to_owned(), entry.clone());
                Ok(true)
            })
            .await
            .unwrap();
        object_store
            .put(&store.artifact_path("expired"), b"expired".to_vec().into())
            .await
            .unwrap();

        assert_eq!(
            store
                .cleanup_expired_with_timeout(2_000, Duration::from_millis(1))
                .await,
            Err(ArtifactStoreError::StorageUnavailable)
        );
        assert_eq!(
            store.read_ledger().await.unwrap().0.artifacts,
            BTreeMap::from([("expired".to_owned(), entry)])
        );
        assert_eq!(
            object_store
                .get(&store.artifact_path("expired"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            b"expired"[..]
        );

        throttled_store.config_mut(|config| config.wait_delete_per_call = Duration::ZERO);
        store.cleanup_expired(2_000).await.unwrap();
        assert!(store.read_ledger().await.unwrap().0.artifacts.is_empty());
        assert!(matches!(
            object_store.get(&store.artifact_path("expired")).await,
            Err(ObjectStoreError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn untracked_object_is_reclaimed_before_insert() {
        let object_store = Arc::new(InMemory::new());
        let store = SupportArtifactStore::new(object_store.clone());
        store.initialize().await.unwrap();
        let id = "existing-collision";
        let requested_content: Arc<[u8]> = Arc::from(b"requested object".to_vec());
        object_store
            .put(&store.artifact_path(id), b"orphaned object".to_vec().into())
            .await
            .unwrap();

        store
            .insert(
                id.to_owned(),
                PrincipalId(1),
                PolicyScope::Cluster,
                requested_content.clone(),
                ArtifactExpiry { unix_ms: 2_000 },
                1_000,
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .download(id, PrincipalId(1), 1_000)
                .await
                .unwrap()
                .0
                .as_ref(),
            requested_content.as_ref()
        );
        assert_eq!(
            store.read_ledger().await.unwrap().0.artifacts,
            BTreeMap::from([(
                id.to_owned(),
                LedgerArtifact {
                    owner: PrincipalId(1),
                    scope: PolicyScope::Cluster,
                    size: requested_content.len(),
                    expires_at_ms: 2_000,
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
                    object_generations: BTreeMap::new(),
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
        let winner_path = SupportArtifactStore::new(store.clone()).artifact_path_with_generation(
            winner_id,
            ledger.object_generations.get(winner_id).map(String::as_str),
        );
        assert_eq!(
            store
                .get(&winner_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"x"
        );
        let loser_prefix = format!("artifacts/{}-", hex::encode(loser_id.as_bytes()));
        let paths = store
            .list(Some(&Path::from("control/support-artifacts")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!paths
            .iter()
            .any(|meta| { meta.location.as_ref().starts_with(&loser_prefix) }));
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

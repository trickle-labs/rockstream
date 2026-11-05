use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rockstream_types::authorization::PolicyScope;
use rockstream_types::ids::PrincipalId;

pub(crate) const MAX_ARTIFACT_BYTES: usize = 10 * 1024 * 1024;
pub(crate) const MAX_ARTIFACT_STORAGE_BYTES: usize = 100 * 1024 * 1024;
pub(crate) const ARTIFACT_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Copy)]
pub(crate) struct ArtifactExpiry {
    pub monotonic: Instant,
    pub unix_ms: u64,
}

#[derive(Clone, Default)]
pub(crate) struct SupportArtifactStore {
    // shortcut: the aggregate cap is per service instance; use deployment-shared accounting before qualification.
    inner: Arc<Mutex<Inner>>,
}

#[derive(Default)]
struct Inner {
    artifacts: HashMap<String, StoredArtifact>,
    bytes_used: usize,
}

struct StoredArtifact {
    owner: PrincipalId,
    scope: PolicyScope,
    content: Arc<[u8]>,
    expires_at: Instant,
    expires_at_ms: u64,
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
}

impl SupportArtifactStore {
    pub fn insert(
        &self,
        id: String,
        owner: PrincipalId,
        scope: PolicyScope,
        content: Arc<[u8]>,
        expiry: ArtifactExpiry,
        now: Instant,
    ) -> Result<(), ArtifactStoreError> {
        if content.len() > MAX_ARTIFACT_BYTES {
            return Err(ArtifactStoreError::TooLarge);
        }

        let mut inner = self.inner.lock();
        Self::remove_expired(&mut inner, now);
        if inner.artifacts.contains_key(&id) {
            return Err(ArtifactStoreError::Exists);
        }
        let Some(next_size) = inner.bytes_used.checked_add(content.len()) else {
            return Err(ArtifactStoreError::StorageFull);
        };
        if next_size > MAX_ARTIFACT_STORAGE_BYTES {
            return Err(ArtifactStoreError::StorageFull);
        }
        inner.bytes_used = next_size;
        inner.artifacts.insert(
            id,
            StoredArtifact {
                owner,
                scope,
                content,
                expires_at: expiry.monotonic,
                expires_at_ms: expiry.unix_ms,
            },
        );
        Ok(())
    }

    pub fn metadata(&self, id: &str, now: Instant) -> Result<ArtifactMetadata, ArtifactStoreError> {
        let mut inner = self.inner.lock();
        Self::remove_expired(&mut inner, now);
        inner
            .artifacts
            .get(id)
            .filter(|artifact| artifact.expires_at > now)
            .map(|artifact| ArtifactMetadata {
                owner: artifact.owner,
                scope: artifact.scope.clone(),
                expires_at_ms: artifact.expires_at_ms,
            })
            .ok_or(ArtifactStoreError::NotFound)
    }

    pub fn download(
        &self,
        id: &str,
        owner: PrincipalId,
        now: Instant,
    ) -> Result<(Arc<[u8]>, u64, Instant), ArtifactStoreError> {
        let mut inner = self.inner.lock();
        Self::remove_expired(&mut inner, now);
        let artifact = inner
            .artifacts
            .get(id)
            .ok_or(ArtifactStoreError::NotFound)?;
        if artifact.expires_at <= now {
            return Err(ArtifactStoreError::NotFound);
        }
        if artifact.owner != owner {
            return Err(ArtifactStoreError::WrongOwner);
        }
        Ok((
            artifact.content.clone(),
            artifact.expires_at_ms,
            artifact.expires_at,
        ))
    }

    pub fn remove(&self, id: &str, owner: PrincipalId) {
        let mut inner = self.inner.lock();
        if inner
            .artifacts
            .get(id)
            .is_some_and(|artifact| artifact.owner == owner)
        {
            if let Some(artifact) = inner.artifacts.remove(id) {
                inner.bytes_used = inner.bytes_used.saturating_sub(artifact.content.len());
            }
        }
    }

    pub fn cleanup_expired(&self, now: Instant) {
        Self::remove_expired(&mut self.inner.lock(), now);
    }

    pub fn clear(&self) {
        *self.inner.lock() = Inner::default();
    }

    fn remove_expired(inner: &mut Inner, now: Instant) {
        inner.artifacts.retain(|_, artifact| {
            artifact.expires_at > now || Arc::strong_count(&artifact.content) > 1
        });
        inner.bytes_used = inner
            .artifacts
            .values()
            .map(|artifact| artifact.content.len())
            .sum();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact_expiry(monotonic: Instant, unix_ms: u64) -> ArtifactExpiry {
        ArtifactExpiry { monotonic, unix_ms }
    }

    #[test]
    fn artifact_bytes_are_actor_bound_and_expire_at_the_deadline() {
        let store = SupportArtifactStore::default();
        let now = Instant::now();
        let expires_at = now + ARTIFACT_TTL;
        let content: Arc<[u8]> = Arc::from(b"{\"safe\":\"complete artifact\"}".to_vec());
        store
            .insert(
                "opaque-id".to_owned(),
                PrincipalId(7),
                PolicyScope::Cluster,
                content.clone(),
                artifact_expiry(expires_at, 600_000),
                now,
            )
            .unwrap();

        assert_eq!(
            store.download("opaque-id", PrincipalId(7), now).unwrap(),
            (content.clone(), 600_000, expires_at)
        );
        assert_eq!(
            store.download("opaque-id", PrincipalId(8), now),
            Err(ArtifactStoreError::WrongOwner)
        );
        drop(content);
        assert_eq!(
            store.download("opaque-id", PrincipalId(7), expires_at),
            Err(ArtifactStoreError::NotFound)
        );
        assert!(store.metadata("opaque-id", expires_at).is_err());
    }

    #[test]
    fn retained_artifacts_obey_per_item_and_aggregate_limits() {
        let store = SupportArtifactStore::default();
        let now = Instant::now();
        let expiry = now + ARTIFACT_TTL;

        for index in 0..MAX_ARTIFACT_STORAGE_BYTES / MAX_ARTIFACT_BYTES {
            store
                .insert(
                    index.to_string(),
                    PrincipalId(1),
                    PolicyScope::Cluster,
                    Arc::from(vec![index as u8; MAX_ARTIFACT_BYTES]),
                    artifact_expiry(expiry, 1),
                    now,
                )
                .unwrap();
        }
        assert_eq!(
            store.insert(
                "overflow".to_owned(),
                PrincipalId(1),
                PolicyScope::Cluster,
                Arc::from(vec![0; 1]),
                artifact_expiry(expiry, 1),
                now,
            ),
            Err(ArtifactStoreError::StorageFull)
        );
        assert_eq!(
            store.insert(
                "oversized".to_owned(),
                PrincipalId(1),
                PolicyScope::Cluster,
                Arc::from(vec![0; MAX_ARTIFACT_BYTES + 1]),
                artifact_expiry(expiry, 1),
                now,
            ),
            Err(ArtifactStoreError::TooLarge)
        );

        let (active_content, _, _) = store.download("0", PrincipalId(1), now).unwrap();
        store.cleanup_expired(expiry);
        for index in 0..MAX_ARTIFACT_STORAGE_BYTES / MAX_ARTIFACT_BYTES - 1 {
            store
                .insert(
                    format!("replacement-{index}"),
                    PrincipalId(1),
                    PolicyScope::Cluster,
                    Arc::from(vec![0; MAX_ARTIFACT_BYTES]),
                    artifact_expiry(expiry + ARTIFACT_TTL, 2),
                    expiry,
                )
                .unwrap();
        }
        assert_eq!(
            store.insert(
                "over-budget-with-active-download".to_owned(),
                PrincipalId(1),
                PolicyScope::Cluster,
                Arc::from(vec![0; 1]),
                artifact_expiry(expiry + ARTIFACT_TTL, 2),
                expiry,
            ),
            Err(ArtifactStoreError::StorageFull)
        );
        drop(active_content);
        store.cleanup_expired(expiry);
        store
            .insert(
                "after-cleanup".to_owned(),
                PrincipalId(1),
                PolicyScope::Cluster,
                Arc::from(b"released capacity".to_vec()),
                artifact_expiry(expiry + ARTIFACT_TTL, 2),
                expiry,
            )
            .unwrap();
    }
}

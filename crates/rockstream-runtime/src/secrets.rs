//! Memory-only worker secret-token resolution.

use parking_lot::{Mutex, RwLock};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use rockstream_types::{
    ids::WorkloadId,
    secret::{SecretToken, SecretType, MAX_WORKER_SECRET_TOKEN_PAYLOAD_BYTES},
};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;

/// Bound for tokens retained by one worker process.
pub const MAX_WORKER_SECRET_TOKENS: usize = 1024;
/// Bound for serialized credential payloads retained by one worker process.
pub const MAX_WORKER_SECRET_CACHE_BYTES: usize = 64 * 1024 * 1024;
const AES_256_GCM_TAG_BYTES: usize = 16;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SecretManagerError {
    #[error("[RS-2423] secret.token_invalid: {message}. Next steps: request a fresh secret token using valid mTLS node credentials.")]
    Invalid { message: String },
    #[error("[RS-3601] secret token cache is full. Next steps: remove unused secret bindings before retrying.")]
    Capacity,
}

/// A decrypted secret retained only in the worker process heap.
#[derive(Clone, PartialEq, Eq)]
pub struct ResolvedSecret {
    pub workload_id: WorkloadId,
    pub secret_name: String,
    pub secret_type: SecretType,
    pub payload: HashMap<String, String>,
    pub token_id: String,
    pub expires_at: u64,
}

/// Bounded, memory-only cache of worker secret credentials.
pub struct WorkerSecretManager {
    worker_identity: String,
    resolve_lock: Mutex<()>,
    cache: RwLock<WorkerSecretCache>,
    refresh_targets: RwLock<HashSet<(WorkloadId, String)>>,
    fill_level: AtomicUsize,
    fill_bytes: AtomicUsize,
}

#[derive(Default)]
struct WorkerSecretCache {
    entries: HashMap<(WorkloadId, String), ResolvedSecret>,
    pending: HashMap<(WorkloadId, String), ResolvedSecret>,
    entry_bytes: HashMap<(WorkloadId, String), usize>,
    pending_bytes: HashMap<(WorkloadId, String), usize>,
    workload_epochs: HashMap<WorkloadId, u64>,
}

impl WorkerSecretCache {
    fn len(&self) -> usize {
        self.entries.len()
            + self
                .pending
                .keys()
                .filter(|key| !self.entries.contains_key(*key))
                .count()
    }

    fn bytes_used(&self) -> usize {
        self.entry_bytes
            .values()
            .chain(self.pending_bytes.values())
            .copied()
            .fold(0usize, usize::saturating_add)
    }

    fn bytes_after_insert(&self, key: &(WorkloadId, String), bytes: usize) -> usize {
        self.bytes_used()
            .saturating_sub(self.pending_bytes.get(key).copied().unwrap_or_default())
            .saturating_add(bytes)
    }
}

impl WorkerSecretManager {
    pub fn new(worker_identity: impl Into<String>) -> Self {
        Self {
            worker_identity: worker_identity.into(),
            resolve_lock: Mutex::new(()),
            cache: RwLock::new(WorkerSecretCache::default()),
            refresh_targets: RwLock::new(HashSet::new()),
            fill_level: AtomicUsize::new(0),
            fill_bytes: AtomicUsize::new(0),
        }
    }

    pub fn worker_identity(&self) -> &str {
        &self.worker_identity
    }

    pub fn track_refresh(
        &self,
        workload_id: WorkloadId,
        secret_name: impl Into<String>,
    ) -> Result<(), SecretManagerError> {
        let secret_name = secret_name.into();
        if workload_id.0 == 0 || secret_name.trim().is_empty() {
            return Err(SecretManagerError::Invalid {
                message: "workload or secret name is invalid".to_string(),
            });
        }
        let mut targets = self.refresh_targets.write();
        let target = (workload_id, secret_name);
        if !targets.contains(&target) && targets.len() >= MAX_WORKER_SECRET_TOKENS {
            return Err(SecretManagerError::Capacity);
        }
        targets.insert(target);
        Ok(())
    }

    pub fn refresh_targets(
        &self,
        active_workloads: &HashSet<WorkloadId>,
    ) -> Vec<(WorkloadId, String)> {
        let mut targets = self.refresh_targets.write();
        targets.retain(|(workload_id, _)| active_workloads.contains(workload_id));
        let mut targets: Vec<_> = targets.iter().cloned().collect();
        targets.sort_by(|(left_id, left_name), (right_id, right_name)| {
            left_id
                .0
                .cmp(&right_id.0)
                .then_with(|| left_name.cmp(right_name))
        });
        targets
    }

    pub fn reject_refresh(&self, workload_id: WorkloadId, secret_name: &str, retry: bool) {
        let mut cache = self.cache.write();
        let key = (workload_id, secret_name.to_owned());
        cache.entries.remove(&key);
        cache.pending.remove(&key);
        cache.entry_bytes.remove(&key);
        cache.pending_bytes.remove(&key);
        self.fill_level.store(cache.len(), Ordering::Relaxed);
        self.fill_bytes.store(cache.bytes_used(), Ordering::Relaxed);
        drop(cache);
        if !retry {
            self.refresh_targets.write().remove(&key);
        }
    }

    pub fn initialize_workload_epoch(&self, workload_id: WorkloadId, epoch: u64) {
        self.cache
            .write()
            .workload_epochs
            .entry(workload_id)
            .and_modify(|current| *current = (*current).max(epoch))
            .or_insert(epoch);
    }

    /// Activate replacements only after the workload enters a later epoch.
    pub fn advance_workload_epoch(&self, workload_id: WorkloadId, epoch: u64) -> usize {
        let mut cache = self.cache.write();
        let current = cache.workload_epochs.entry(workload_id).or_insert(epoch);
        if epoch <= *current {
            return 0;
        }
        *current = epoch;
        let keys: Vec<_> = cache
            .pending
            .keys()
            .filter(|(pending_workload_id, _)| *pending_workload_id == workload_id)
            .cloned()
            .collect();
        for key in &keys {
            if let Some(secret) = cache.pending.remove(key) {
                cache.entry_bytes.remove(key);
                let bytes = cache.pending_bytes.remove(key).unwrap_or_default();
                cache.entries.insert(key.clone(), secret);
                cache.entry_bytes.insert(key.clone(), bytes);
            }
        }
        self.fill_level.store(cache.len(), Ordering::Relaxed);
        self.fill_bytes.store(cache.bytes_used(), Ordering::Relaxed);
        keys.len()
    }

    /// Decrypt and install a token without writing any credential bytes to disk.
    pub fn resolve_token(
        &self,
        token: &SecretToken,
        now_secs: u64,
    ) -> Result<ResolvedSecret, SecretManagerError> {
        let _resolve_guard = self.resolve_lock.lock();
        if token.format_version != SecretToken::FORMAT_VERSION {
            return Err(SecretManagerError::Invalid {
                message: "token format version is unsupported".to_string(),
            });
        }
        if token.worker_identity != self.worker_identity {
            return Err(SecretManagerError::Invalid {
                message: "token identity does not match this worker".to_string(),
            });
        }
        if token.workload_id.0 == 0 {
            return Err(SecretManagerError::Invalid {
                message: "token workload is invalid".to_string(),
            });
        }
        if token.is_expired(now_secs) {
            return Err(SecretManagerError::Invalid {
                message: "token is expired".to_string(),
            });
        }
        if token.nonce.len() != 12 {
            return Err(SecretManagerError::Invalid {
                message: "token nonce is invalid".to_string(),
            });
        }
        if token.encrypted_payload.len()
            > MAX_WORKER_SECRET_TOKEN_PAYLOAD_BYTES + AES_256_GCM_TAG_BYTES
        {
            return Err(SecretManagerError::Invalid {
                message: "token payload exceeds the maximum supported size".to_string(),
            });
        }
        let payload_bytes = token
            .encrypted_payload
            .len()
            .saturating_sub(AES_256_GCM_TAG_BYTES);

        let key = UnboundKey::new(
            &AES_256_GCM,
            &SecretToken::workload_key(&self.worker_identity, token.workload_id),
        )
        .map_err(|_| SecretManagerError::Invalid {
            message: "token key is invalid".to_string(),
        })?;
        let nonce = Nonce::try_assume_unique_for_key(&token.nonce).map_err(|_| {
            SecretManagerError::Invalid {
                message: "token nonce is invalid".to_string(),
            }
        })?;
        let mut ciphertext = token.encrypted_payload.clone();
        let plaintext = LessSafeKey::new(key)
            .open_in_place(nonce, Aad::empty(), &mut ciphertext)
            .map_err(|_| SecretManagerError::Invalid {
                message: "token authentication failed".to_string(),
            })?;
        let payload =
            serde_json::from_slice(plaintext).map_err(|_| SecretManagerError::Invalid {
                message: "token payload is malformed".to_string(),
            })?;

        let resolved = ResolvedSecret {
            workload_id: token.workload_id,
            secret_name: token.secret_name.clone(),
            secret_type: token.secret_type.clone(),
            payload,
            token_id: token.token_id.clone(),
            expires_at: token.expires_at,
        };
        self.track_refresh(resolved.workload_id, resolved.secret_name.clone())?;
        let cache_key = (resolved.workload_id, resolved.secret_name.clone());
        let mut cache = self.cache.write();
        let active = cache.entries.contains_key(&cache_key);
        let pending = cache.pending.contains_key(&cache_key);
        if (!active && !pending && cache.len() >= MAX_WORKER_SECRET_TOKENS)
            || cache.bytes_after_insert(&cache_key, payload_bytes) > MAX_WORKER_SECRET_CACHE_BYTES
        {
            return Err(SecretManagerError::Capacity);
        }
        if active || pending {
            cache.pending.insert(cache_key.clone(), resolved.clone());
            cache.pending_bytes.insert(cache_key, payload_bytes);
        } else {
            cache.entries.insert(cache_key.clone(), resolved.clone());
            cache.entry_bytes.insert(cache_key, payload_bytes);
        }
        self.fill_level.store(cache.len(), Ordering::Relaxed);
        self.fill_bytes.store(cache.bytes_used(), Ordering::Relaxed);
        Ok(resolved)
    }

    /// Return a current secret and remove expired entries as a single bounded operation.
    pub fn get(
        &self,
        workload_id: WorkloadId,
        secret_name: &str,
        now_secs: u64,
    ) -> Option<ResolvedSecret> {
        let mut cache = self.cache.write();
        cache
            .entries
            .retain(|_, value| value.expires_at != 0 && now_secs < value.expires_at);
        cache
            .pending
            .retain(|_, value| value.expires_at != 0 && now_secs < value.expires_at);
        let WorkerSecretCache {
            entries,
            entry_bytes,
            pending,
            pending_bytes,
            ..
        } = &mut *cache;
        entry_bytes.retain(|key, _| entries.contains_key(key));
        pending_bytes.retain(|key, _| pending.contains_key(key));
        self.fill_level.store(cache.len(), Ordering::Relaxed);
        self.fill_bytes.store(cache.bytes_used(), Ordering::Relaxed);
        cache
            .entries
            .get(&(workload_id, secret_name.to_owned()))
            .cloned()
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }

    /// Return the serialized credential payload bytes retained by this worker.
    pub fn fill_bytes(&self) -> usize {
        self.fill_bytes.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_control::kek::EnvKekProvider;
    use rockstream_control::SecretStore;
    use std::sync::Arc;

    #[test]
    fn cache_byte_admission_counts_active_and_pending_generations() {
        let active = (WorkloadId(1), "active".to_owned());
        let other = (WorkloadId(2), "other".to_owned());
        let mut cache = WorkerSecretCache::default();
        cache
            .entry_bytes
            .insert(active.clone(), MAX_WORKER_SECRET_CACHE_BYTES - 10);

        assert_eq!(cache.bytes_used(), MAX_WORKER_SECRET_CACHE_BYTES - 10);
        assert_eq!(
            cache.bytes_after_insert(&other, 10),
            MAX_WORKER_SECRET_CACHE_BYTES
        );
        assert_eq!(
            cache.bytes_after_insert(&other, 11),
            MAX_WORKER_SECRET_CACHE_BYTES + 1
        );

        cache.pending_bytes.insert(active.clone(), 5);
        assert_eq!(cache.bytes_used(), MAX_WORKER_SECRET_CACHE_BYTES - 5);
        assert_eq!(
            cache.bytes_after_insert(&active, 5),
            MAX_WORKER_SECRET_CACHE_BYTES - 5
        );
        assert_eq!(
            cache.bytes_after_insert(&active, 11),
            MAX_WORKER_SECRET_CACHE_BYTES + 1
        );
    }

    #[tokio::test]
    async fn aggregate_cache_limit_accepts_exact_fill_and_preserves_active_on_overflow() {
        let store = SecretStore::new(
            None,
            Arc::new(EnvKekProvider::from_passphrase("worker-cache-budget-kek")),
        );
        let old_payload = HashMap::from([("credential".to_owned(), "old".to_owned())]);
        let old_payload_bytes = serde_json::to_vec(&old_payload).unwrap().len();
        store
            .create_secret(0, "bounded", SecretType::BearerToken, old_payload, "test")
            .await
            .unwrap();
        let workload_id = WorkloadId(17);
        let old_token = store
            .issue_worker_token(0, workload_id, "bounded", "worker-7", 60, "test")
            .await
            .unwrap();

        let payload_for_size = |size: usize| {
            let empty = HashMap::from([("credential".to_owned(), String::new())]);
            let overhead = serde_json::to_vec(&empty).unwrap().len();
            HashMap::from([("credential".to_owned(), "x".repeat(size - overhead))])
        };
        let exact_payload_bytes = MAX_WORKER_SECRET_TOKEN_PAYLOAD_BYTES - old_payload_bytes;
        let exact_payload = payload_for_size(exact_payload_bytes);
        assert_eq!(
            serde_json::to_vec(&exact_payload).unwrap().len(),
            exact_payload_bytes
        );
        store
            .alter_secret(0, "bounded", exact_payload, "test")
            .await
            .unwrap();
        let exact_token = store
            .issue_worker_token(0, workload_id, "bounded", "worker-7", 60, "test")
            .await
            .unwrap();

        let over_payload_bytes = exact_payload_bytes + 1;
        let over_payload = payload_for_size(over_payload_bytes);
        assert_eq!(
            serde_json::to_vec(&over_payload).unwrap().len(),
            over_payload_bytes
        );
        store
            .alter_secret(0, "bounded", over_payload, "test")
            .await
            .unwrap();
        let over_token = store
            .issue_worker_token(0, workload_id, "bounded", "worker-7", 60, "test")
            .await
            .unwrap();
        assert_eq!(
            exact_token.encrypted_payload.len() - AES_256_GCM_TAG_BYTES,
            exact_payload_bytes
        );
        assert_eq!(
            over_token.encrypted_payload.len() - AES_256_GCM_TAG_BYTES,
            over_payload_bytes
        );

        let manager = WorkerSecretManager::new("worker-7");
        manager.initialize_workload_epoch(workload_id, 5);
        manager
            .resolve_token(&old_token, old_token.issued_at)
            .unwrap();
        let mut cache = manager.cache.write();
        // Model 63 admitted 1 MiB secrets without allocating their payloads.
        for index in 0..63 {
            let filler_workload = WorkloadId(100 + index);
            let filler_name = format!("filler-{index}");
            let key = (filler_workload, filler_name.clone());
            cache.entries.insert(
                key.clone(),
                ResolvedSecret {
                    workload_id: filler_workload,
                    secret_name: filler_name.clone(),
                    secret_type: SecretType::BearerToken,
                    payload: HashMap::new(),
                    token_id: filler_name,
                    expires_at: old_token.expires_at,
                },
            );
            cache
                .entry_bytes
                .insert(key, MAX_WORKER_SECRET_TOKEN_PAYLOAD_BYTES);
        }
        manager.fill_level.store(cache.len(), Ordering::Relaxed);
        manager
            .fill_bytes
            .store(cache.bytes_used(), Ordering::Relaxed);
        assert_eq!(
            cache.bytes_used(),
            63 * MAX_WORKER_SECRET_TOKEN_PAYLOAD_BYTES + old_payload_bytes
        );
        drop(cache);

        manager
            .resolve_token(&exact_token, exact_token.issued_at)
            .unwrap();
        assert_eq!(manager.fill_bytes(), MAX_WORKER_SECRET_CACHE_BYTES);
        assert_eq!(
            manager
                .get(workload_id, "bounded", exact_token.issued_at)
                .unwrap()
                .token_id,
            old_token.token_id
        );

        let error = manager
            .resolve_token(&over_token, over_token.issued_at)
            .err()
            .expect("overflow must be rejected");
        assert_eq!(
            error.to_string(),
            "[RS-3601] secret token cache is full. Next steps: remove unused secret bindings before retrying."
        );
        assert_eq!(manager.fill_bytes(), MAX_WORKER_SECRET_CACHE_BYTES);
        assert_eq!(
            manager
                .get(workload_id, "bounded", over_token.issued_at)
                .unwrap()
                .token_id,
            old_token.token_id
        );

        assert_eq!(manager.advance_workload_epoch(workload_id, 6), 1);
        assert_eq!(
            manager
                .get(workload_id, "bounded", exact_token.issued_at)
                .unwrap()
                .token_id,
            exact_token.token_id
        );
        assert_eq!(
            manager.fill_bytes(),
            63 * MAX_WORKER_SECRET_TOKEN_PAYLOAD_BYTES + exact_payload_bytes
        );
    }
}

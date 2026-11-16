//! Memory-only worker secret-token resolution.

use parking_lot::RwLock;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use rockstream_types::{
    ids::WorkloadId,
    secret::{SecretToken, SecretType},
};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;

/// Bound for tokens retained by one worker process.
pub const MAX_WORKER_SECRET_TOKENS: usize = 1024;

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
    cache: RwLock<WorkerSecretCache>,
    refresh_targets: RwLock<HashSet<(WorkloadId, String)>>,
    fill_level: AtomicUsize,
}

#[derive(Default)]
struct WorkerSecretCache {
    entries: HashMap<(WorkloadId, String), ResolvedSecret>,
    pending: HashMap<(WorkloadId, String), ResolvedSecret>,
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
}

impl WorkerSecretManager {
    pub fn new(worker_identity: impl Into<String>) -> Self {
        Self {
            worker_identity: worker_identity.into(),
            cache: RwLock::new(WorkerSecretCache::default()),
            refresh_targets: RwLock::new(HashSet::new()),
            fill_level: AtomicUsize::new(0),
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
        self.fill_level.store(cache.len(), Ordering::Relaxed);
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
                cache.entries.insert(key.clone(), secret);
            }
        }
        self.fill_level.store(cache.len(), Ordering::Relaxed);
        keys.len()
    }

    /// Decrypt and install a token without writing any credential bytes to disk.
    pub fn resolve_token(
        &self,
        token: &SecretToken,
        now_secs: u64,
    ) -> Result<ResolvedSecret, SecretManagerError> {
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
        if !active && !pending && cache.len() >= MAX_WORKER_SECRET_TOKENS {
            return Err(SecretManagerError::Capacity);
        }
        if active || pending {
            cache.pending.insert(cache_key, resolved.clone());
        } else {
            cache.entries.insert(cache_key, resolved.clone());
        }
        self.fill_level.store(cache.len(), Ordering::Relaxed);
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
        self.fill_level.store(cache.len(), Ordering::Relaxed);
        cache
            .entries
            .get(&(workload_id, secret_name.to_owned()))
            .cloned()
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }
}

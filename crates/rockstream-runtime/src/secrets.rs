//! Memory-only worker secret-token resolution.

use parking_lot::RwLock;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use rockstream_types::secret::{SecretToken, SecretType};
use serde_json;
use std::collections::HashMap;
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
    pub secret_name: String,
    pub secret_type: SecretType,
    pub payload: HashMap<String, String>,
    pub token_id: String,
    pub expires_at: u64,
}

/// Bounded, memory-only cache of worker secret credentials.
pub struct WorkerSecretManager {
    worker_identity: String,
    entries: RwLock<HashMap<String, ResolvedSecret>>,
    fill_level: AtomicUsize,
}

impl WorkerSecretManager {
    pub fn new(worker_identity: impl Into<String>) -> Self {
        Self {
            worker_identity: worker_identity.into(),
            entries: RwLock::new(HashMap::new()),
            fill_level: AtomicUsize::new(0),
        }
    }

    pub fn worker_identity(&self) -> &str {
        &self.worker_identity
    }

    /// Decrypt and install a token without writing any credential bytes to disk.
    pub fn resolve_token(
        &self,
        token: &SecretToken,
        now_secs: u64,
    ) -> Result<ResolvedSecret, SecretManagerError> {
        if token.worker_identity != self.worker_identity {
            return Err(SecretManagerError::Invalid {
                message: "token identity does not match this worker".to_string(),
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
            &SecretToken::worker_key(&self.worker_identity),
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
            secret_name: token.secret_name.clone(),
            secret_type: token.secret_type.clone(),
            payload,
            token_id: token.token_id.clone(),
            expires_at: token.expires_at,
        };
        let mut entries = self.entries.write();
        if !entries.contains_key(&resolved.secret_name) && entries.len() >= MAX_WORKER_SECRET_TOKENS
        {
            return Err(SecretManagerError::Capacity);
        }
        entries.insert(resolved.secret_name.clone(), resolved.clone());
        self.fill_level.store(entries.len(), Ordering::Relaxed);
        Ok(resolved)
    }

    /// Return a current secret and remove expired entries as a single bounded operation.
    pub fn get(&self, secret_name: &str, now_secs: u64) -> Option<ResolvedSecret> {
        let mut entries = self.entries.write();
        entries.retain(|_, value| !value.expires_at.eq(&0) && now_secs < value.expires_at);
        self.fill_level.store(entries.len(), Ordering::Relaxed);
        entries.get(secret_name).cloned()
    }

    pub fn fill_level(&self) -> usize {
        self.fill_level.load(Ordering::Relaxed)
    }
}

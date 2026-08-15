//! SlateDB-backed secret storage and lifecycle management (v0.55.1).

use crate::audit::{AuditEvent, FileAuditLog};
use crate::kek::{
    envelope_decrypt_secret, envelope_encrypt_secret, rotate_secret_kek, KekError, KekProvider,
};
use parking_lot::RwLock;
use rockstream_types::error_code::{
    ErrorCode, RS_2420, RS_2421, RS_2422, RS_2423, RS_2424, RS_2425, RS_2426, RS_3601,
};
use rockstream_types::secret::{
    EncryptedSecret, Secret, SecretMetadata, SecretRotation, SecretToken, SecretType,
};
use slatedb::Db;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

type SecretReferenceKey = (u128, String);
type SecretReferences = HashMap<SecretReferenceKey, HashSet<String>>;

/// Prefix for secrets stored in the SlateDB catalog: `0x01 0x0B`.
pub const CATALOG_SECRETS_PREFIX: [u8; 2] = [0x01, 0x0B];

/// Maximum number of secrets that can be stored per namespace.
pub const MAX_SECRETS_PER_NAMESPACE: usize = 100_000;
/// Maximum number of in-memory secrets retained by a process without SlateDB.
pub const MAX_IN_MEMORY_SECRETS: usize = 1_000_000;
/// Maximum number of source/sink references retained for one secret.
pub const MAX_SECRET_REFERENCES: usize = 10_000;
/// Maximum number of source/sink references retained by one process.
pub const MAX_TOTAL_SECRET_REFERENCES: usize = 1_000_000;
/// One latest rotation notification is retained per store.
pub const SECRET_ROTATION_CHANNEL_CAPACITY: usize = 1;

/// Fill levels for bounded secret-store scans and in-memory state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretStoreMetricsSnapshot {
    pub secret_scan_fill_level: usize,
    pub reference_fill_level: usize,
    pub in_memory_fill_level: usize,
    pub rotation_fill_level: usize,
}

#[derive(Debug, Default)]
struct SecretStoreMetrics {
    secret_scan_fill_level: AtomicUsize,
    reference_fill_level: AtomicUsize,
    in_memory_fill_level: AtomicUsize,
    rotation_fill_level: AtomicUsize,
}

impl SecretStoreMetrics {
    fn snapshot(&self) -> SecretStoreMetricsSnapshot {
        SecretStoreMetricsSnapshot {
            secret_scan_fill_level: self.secret_scan_fill_level.load(Ordering::Relaxed),
            reference_fill_level: self.reference_fill_level.load(Ordering::Relaxed),
            in_memory_fill_level: self.in_memory_fill_level.load(Ordering::Relaxed),
            rotation_fill_level: self.rotation_fill_level.load(Ordering::Relaxed),
        }
    }
}

/// Errors produced by the secret store.
#[derive(Debug, Error)]
pub enum SecretStoreError {
    #[error(
        "[{code}] secret.not_found: secret '{name}' does not exist. Next steps: verify the secret name or run CREATE SECRET to define it."
    )]
    NotFound { code: ErrorCode, name: String },

    #[error(
        "[{code}] secret.already_exists: secret '{name}' already exists. Next steps: choose a distinct secret name or run ALTER SECRET to modify the existing secret."
    )]
    AlreadyExists { code: ErrorCode, name: String },

    #[error("[{code}] secret.encryption_failed: {message}")]
    EncryptionFailed { code: ErrorCode, message: String },

    #[error("[{code}] secret.token_invalid: {message}")]
    TokenInvalid { code: ErrorCode, message: String },

    #[error("[{code}] secret.ddl_invalid: {message}")]
    DdlInvalid { code: ErrorCode, message: String },

    #[error("[{code}] secret.rotation_failed: {message}")]
    RotationFailed { code: ErrorCode, message: String },

    #[error(
        "[{code}] secret.in_use_by_source_or_sink: secret '{name}' is in use by sources/sinks [{references}]. Next steps: drop or alter referencing sources and sinks before dropping the secret."
    )]
    InUse {
        code: ErrorCode,
        name: String,
        references: String,
    },

    #[error(
        "[{code}] secret.capacity_exceeded: {message}. Next steps: remove unused secrets or references, then retry."
    )]
    CapacityExceeded { code: ErrorCode, message: String },

    #[error(
        "[{code}] secret.storage_unavailable: {message}. Next steps: verify catalog storage health and retry."
    )]
    Storage { code: ErrorCode, message: String },
}

impl SecretStoreError {
    pub fn not_found(name: impl Into<String>) -> Self {
        Self::NotFound {
            code: RS_2420,
            name: name.into(),
        }
    }

    pub fn already_exists(name: impl Into<String>) -> Self {
        Self::AlreadyExists {
            code: RS_2421,
            name: name.into(),
        }
    }

    pub fn encryption_failed(msg: impl Into<String>) -> Self {
        Self::EncryptionFailed {
            code: RS_2422,
            message: msg.into(),
        }
    }

    pub fn token_invalid(msg: impl Into<String>) -> Self {
        Self::TokenInvalid {
            code: RS_2423,
            message: msg.into(),
        }
    }

    pub fn ddl_invalid(msg: impl Into<String>) -> Self {
        Self::DdlInvalid {
            code: RS_2424,
            message: msg.into(),
        }
    }

    pub fn rotation_failed(msg: impl Into<String>) -> Self {
        Self::RotationFailed {
            code: RS_2425,
            message: msg.into(),
        }
    }

    pub fn in_use(name: impl Into<String>, references: impl Into<String>) -> Self {
        Self::InUse {
            code: RS_2426,
            name: name.into(),
            references: references.into(),
        }
    }

    fn capacity_exceeded(message: impl Into<String>) -> Self {
        Self::CapacityExceeded {
            code: RS_3601,
            message: message.into(),
        }
    }

    fn storage(message: impl Into<String>) -> Self {
        Self::Storage {
            code: rockstream_types::error_code::RS_0003,
            message: message.into(),
        }
    }
}

impl From<KekError> for SecretStoreError {
    fn from(err: KekError) -> Self {
        match err {
            KekError::EncryptionFailed { message, .. } => Self::encryption_failed(message),
            KekError::DecryptionFailed { message, .. } => Self::encryption_failed(message),
            KekError::ProviderError { message, .. } => Self::encryption_failed(message),
            KekError::InvalidKey { message, .. } => Self::encryption_failed(message),
        }
    }
}

/// Metadata item returned when listing secrets (never contains secret values).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretListing {
    pub name: String,
    pub secret_type: SecretType,
    pub created_at: u64,
    pub updated_at: u64,
    pub version: u64,
}

/// Construct the SlateDB binary key for a secret:
/// `0x01 0x0B namespace_id(16) secret_name(var)`
pub fn secret_key(namespace_id: u128, name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 16 + name.len());
    key.extend_from_slice(&CATALOG_SECRETS_PREFIX);
    key.extend_from_slice(&namespace_id.to_be_bytes());
    key.extend_from_slice(name.as_bytes());
    key
}

/// Construct the SlateDB prefix for all secrets in a namespace.
pub fn secrets_namespace_prefix(namespace_id: u128) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(2 + 16);
    prefix.extend_from_slice(&CATALOG_SECRETS_PREFIX);
    prefix.extend_from_slice(&namespace_id.to_be_bytes());
    prefix
}

/// SlateDB-backed secret storage manager.
pub struct SecretStore {
    db: Option<Arc<Db>>,
    kek_provider: Arc<RwLock<Arc<dyn KekProvider>>>,
    audit_log: Option<Arc<FileAuditLog>>,
    in_memory: Arc<RwLock<HashMap<(u128, String), EncryptedSecret>>>,
    references: Arc<RwLock<SecretReferences>>,
    metrics: Arc<SecretStoreMetrics>,
    rotation_tx: watch::Sender<Option<SecretRotation>>,
}

impl SecretStore {
    /// Create a new `SecretStore`.
    pub fn new(db: Option<Arc<Db>>, kek_provider: Arc<dyn KekProvider>) -> Self {
        let (rotation_tx, _) = watch::channel(None);
        Self {
            db,
            kek_provider: Arc::new(RwLock::new(kek_provider)),
            audit_log: None,
            in_memory: Arc::new(RwLock::new(HashMap::new())),
            references: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(SecretStoreMetrics::default()),
            rotation_tx,
        }
    }

    /// Set an optional audit log.
    pub fn with_audit_log(mut self, audit_log: Arc<FileAuditLog>) -> Self {
        self.audit_log = Some(audit_log);
        self
    }

    fn record_audit(&self, actor: &str, action: &str, resource: &str, err_code: Option<ErrorCode>) {
        let mut event = AuditEvent::now(actor, action, resource);
        if let Some(ec) = err_code {
            event = event.with_error_code(ec.to_string());
        }
        if let Some(log) = &self.audit_log {
            let _ = log.append(&event);
        }
    }

    /// Get the current KEK provider.
    pub fn kek_provider(&self) -> Arc<dyn KekProvider> {
        self.kek_provider.read().clone()
    }

    /// Return fill levels for bounded secret-store state and scans.
    pub fn metrics(&self) -> SecretStoreMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Subscribe to the latest secret rotation notification.
    pub fn subscribe_rotation(&self) -> watch::Receiver<Option<SecretRotation>> {
        self.rotation_tx.subscribe()
    }

    /// Add a reference to a secret (e.g. from a source or sink).
    pub fn add_reference(
        &self,
        namespace_id: u128,
        secret_name: &str,
        ref_name: &str,
    ) -> Result<(), SecretStoreError> {
        let mut refs = self.references.write();
        let key = (namespace_id, secret_name.to_string());
        if refs.get(&key).is_some_and(|set| set.contains(ref_name)) {
            return Ok(());
        }
        if refs.get(&key).map_or(0, HashSet::len) >= MAX_SECRET_REFERENCES {
            return Err(SecretStoreError::capacity_exceeded(
                "secret reference limit reached",
            ));
        }
        let total_references: usize = refs.values().map(HashSet::len).sum();
        if total_references >= MAX_TOTAL_SECRET_REFERENCES {
            return Err(SecretStoreError::capacity_exceeded(
                "in-memory secret reference limit reached",
            ));
        }
        let set = refs.entry(key).or_default();
        set.insert(ref_name.to_string());
        self.metrics
            .reference_fill_level
            .store(total_references + 1, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a reference to a secret.
    pub fn remove_reference(&self, namespace_id: u128, secret_name: &str, ref_name: &str) {
        let mut refs = self.references.write();
        let key = (namespace_id, secret_name.to_string());
        let remove_key = if let Some(set) = refs.get_mut(&key) {
            set.remove(ref_name);
            set.is_empty()
        } else {
            false
        };
        if remove_key {
            refs.remove(&key);
        }
        let total_references: usize = refs.values().map(HashSet::len).sum();
        self.metrics
            .reference_fill_level
            .store(total_references, Ordering::Relaxed);
    }

    /// Get all references for a secret.
    pub fn get_references(&self, namespace_id: u128, secret_name: &str) -> Vec<String> {
        let refs = self.references.read();
        refs.get(&(namespace_id, secret_name.to_string()))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect()
    }

    /// Synchronously check if a secret exists in memory.
    pub fn has_secret(&self, namespace_id: u128, secret_name: &str) -> bool {
        let map = self.in_memory.read();
        map.contains_key(&(namespace_id, secret_name.trim().to_string()))
    }

    /// Check whether a secret exists in memory or persistent catalog storage.
    pub async fn contains_secret(
        &self,
        namespace_id: u128,
        secret_name: &str,
    ) -> Result<bool, SecretStoreError> {
        Ok(self
            .get_encrypted_raw(namespace_id, secret_name.trim())
            .await?
            .is_some())
    }

    /// Synchronously get secret metadata if present in memory.
    pub fn get_secret_metadata_sync(
        &self,
        namespace_id: u128,
        secret_name: &str,
    ) -> Option<SecretMetadata> {
        let map = self.in_memory.read();
        map.get(&(namespace_id, secret_name.trim().to_string()))
            .map(|e| e.metadata.clone())
    }

    async fn get_encrypted_raw(
        &self,
        namespace_id: u128,
        name: &str,
    ) -> Result<Option<EncryptedSecret>, SecretStoreError> {
        let key = secret_key(namespace_id, name);
        if let Some(db) = &self.db {
            let bytes_opt = db
                .get(&key)
                .await
                .map_err(|e| SecretStoreError::storage(e.to_string()))?;
            if let Some(bytes) = bytes_opt {
                let encrypted: EncryptedSecret = serde_json::from_slice(&bytes)
                    .map_err(|e| SecretStoreError::storage(format!("deserialize failed: {e}")))?;
                return Ok(Some(encrypted));
            }
            Ok(None)
        } else {
            let map = self.in_memory.read();
            Ok(map.get(&(namespace_id, name.to_string())).cloned())
        }
    }

    async fn put_encrypted_raw(
        &self,
        namespace_id: u128,
        name: &str,
        encrypted: &EncryptedSecret,
    ) -> Result<(), SecretStoreError> {
        let key = secret_key(namespace_id, name);
        let serialized = serde_json::to_vec(encrypted)
            .map_err(|e| SecretStoreError::storage(format!("serialize failed: {e}")))?;

        if let Some(db) = &self.db {
            db.put(&key, &serialized)
                .await
                .map_err(|e| SecretStoreError::storage(e.to_string()))?;
        } else {
            let mut map = self.in_memory.write();
            map.insert((namespace_id, name.to_string()), encrypted.clone());
            self.metrics
                .in_memory_fill_level
                .store(map.len(), Ordering::Relaxed);
        }
        Ok(())
    }

    async fn delete_raw(&self, namespace_id: u128, name: &str) -> Result<(), SecretStoreError> {
        let key = secret_key(namespace_id, name);
        if let Some(db) = &self.db {
            // Assert point delete (zero range deletions)
            db.delete(&key)
                .await
                .map_err(|e| SecretStoreError::storage(e.to_string()))?;
        }
        let mut map = self.in_memory.write();
        map.remove(&(namespace_id, name.to_string()));
        self.metrics
            .in_memory_fill_level
            .store(map.len(), Ordering::Relaxed);
        Ok(())
    }

    async fn count_namespace_secrets(&self, namespace_id: u128) -> Result<usize, SecretStoreError> {
        if let Some(db) = &self.db {
            let prefix = secrets_namespace_prefix(namespace_id);
            let mut iter = db
                .scan_prefix(&prefix)
                .await
                .map_err(|e| SecretStoreError::storage(e.to_string()))?;
            let mut count = 0;
            while iter
                .next()
                .await
                .map_err(|e| SecretStoreError::storage(e.to_string()))?
                .is_some()
            {
                count += 1;
                self.metrics
                    .secret_scan_fill_level
                    .store(count, Ordering::Relaxed);
                if count >= MAX_SECRETS_PER_NAMESPACE {
                    break;
                }
            }
            Ok(count)
        } else {
            let map = self.in_memory.read();
            let count = map.keys().filter(|(ns, _)| *ns == namespace_id).count();
            self.metrics
                .secret_scan_fill_level
                .store(count, Ordering::Relaxed);
            Ok(count)
        }
    }

    /// Create a new secret with envelope encryption.
    pub async fn create_secret(
        &self,
        namespace_id: u128,
        name: &str,
        secret_type: SecretType,
        payload: HashMap<String, String>,
        actor: &str,
    ) -> Result<SecretMetadata, SecretStoreError> {
        let trimmed_name = name.trim();
        if trimmed_name.is_empty() {
            return Err(SecretStoreError::ddl_invalid("secret name cannot be empty"));
        }

        if self
            .get_encrypted_raw(namespace_id, trimmed_name)
            .await?
            .is_some()
        {
            self.record_audit(actor, "secret.create_failed", trimmed_name, Some(RS_2421));
            return Err(SecretStoreError::already_exists(trimmed_name));
        }

        if self.count_namespace_secrets(namespace_id).await? >= MAX_SECRETS_PER_NAMESPACE {
            return Err(SecretStoreError::capacity_exceeded(
                "secret namespace limit reached",
            ));
        }
        if self.db.is_none() && self.in_memory.read().len() >= MAX_IN_MEMORY_SECRETS {
            return Err(SecretStoreError::capacity_exceeded(
                "in-memory secret limit reached",
            ));
        }

        let now = chrono::Utc::now().timestamp() as u64;
        let metadata = SecretMetadata {
            created_at: now,
            updated_at: now,
            version: 1,
            source_refs: Vec::new(),
        };

        let kek_provider = self.kek_provider();
        let encrypted = envelope_encrypt_secret(
            trimmed_name,
            secret_type,
            &payload,
            metadata.clone(),
            kek_provider.as_ref(),
        )
        .await?;

        self.put_encrypted_raw(namespace_id, trimmed_name, &encrypted)
            .await?;
        self.record_audit(actor, "secret.created", trimmed_name, None);

        Ok(metadata)
    }

    /// Alter an existing secret's payload, updating its version and timestamp.
    pub async fn alter_secret(
        &self,
        namespace_id: u128,
        name: &str,
        payload: HashMap<String, String>,
        actor: &str,
    ) -> Result<SecretMetadata, SecretStoreError> {
        let trimmed_name = name.trim();
        let existing = match self.get_encrypted_raw(namespace_id, trimmed_name).await? {
            Some(e) => e,
            None => {
                self.record_audit(actor, "secret.alter_failed", trimmed_name, Some(RS_2420));
                return Err(SecretStoreError::not_found(trimmed_name));
            }
        };

        let now = chrono::Utc::now().timestamp() as u64;
        let metadata = SecretMetadata {
            created_at: existing.metadata.created_at,
            updated_at: now,
            version: existing.metadata.version + 1,
            source_refs: existing.metadata.source_refs,
        };

        let kek_provider = self.kek_provider();
        let encrypted = envelope_encrypt_secret(
            trimmed_name,
            existing.secret_type,
            &payload,
            metadata.clone(),
            kek_provider.as_ref(),
        )
        .await?;

        self.put_encrypted_raw(namespace_id, trimmed_name, &encrypted)
            .await?;
        self.record_audit(actor, "secret.altered", trimmed_name, None);
        let _ = self.rotation_tx.send(Some(SecretRotation {
            secret_name: trimmed_name.to_string(),
            version: metadata.version,
        }));
        self.metrics.rotation_fill_level.store(1, Ordering::Relaxed);
        self.record_audit(actor, "secret.rotated", trimmed_name, None);

        Ok(metadata)
    }

    /// Drop a secret from the store using point delete (zero range deletions).
    pub async fn drop_secret(
        &self,
        namespace_id: u128,
        name: &str,
        actor: &str,
    ) -> Result<(), SecretStoreError> {
        let trimmed_name = name.trim();
        let _ = match self.get_encrypted_raw(namespace_id, trimmed_name).await? {
            Some(e) => e,
            None => {
                self.record_audit(actor, "secret.drop_failed", trimmed_name, Some(RS_2420));
                return Err(SecretStoreError::not_found(trimmed_name));
            }
        };

        let refs = self.get_references(namespace_id, trimmed_name);
        if !refs.is_empty() {
            let ref_str = refs.join(", ");
            self.record_audit(actor, "secret.drop_failed", trimmed_name, Some(RS_2426));
            return Err(SecretStoreError::in_use(trimmed_name, ref_str));
        }

        self.delete_raw(namespace_id, trimmed_name).await?;
        self.record_audit(actor, "secret.dropped", trimmed_name, None);

        Ok(())
    }

    /// Get decrypted secret by name.
    pub async fn get_secret(
        &self,
        namespace_id: u128,
        name: &str,
    ) -> Result<Secret, SecretStoreError> {
        let trimmed_name = name.trim();
        let encrypted = match self.get_encrypted_raw(namespace_id, trimmed_name).await? {
            Some(e) => e,
            None => return Err(SecretStoreError::not_found(trimmed_name)),
        };

        let kek_provider = self.kek_provider();
        let secret = envelope_decrypt_secret(&encrypted, kek_provider.as_ref()).await?;
        Ok(secret)
    }

    /// Get metadata for a secret without decrypting the payload.
    pub async fn get_secret_metadata(
        &self,
        namespace_id: u128,
        name: &str,
    ) -> Result<SecretMetadata, SecretStoreError> {
        let trimmed_name = name.trim();
        let encrypted = match self.get_encrypted_raw(namespace_id, trimmed_name).await? {
            Some(e) => e,
            None => return Err(SecretStoreError::not_found(trimmed_name)),
        };
        Ok(encrypted.metadata)
    }

    /// List all secrets in a namespace (metadata only, values strictly masked/omitted).
    pub async fn list_secrets(
        &self,
        namespace_id: u128,
    ) -> Result<Vec<SecretListing>, SecretStoreError> {
        let mut listings = Vec::new();
        if let Some(db) = &self.db {
            let prefix = secrets_namespace_prefix(namespace_id);
            let mut iter = db
                .scan_prefix(&prefix)
                .await
                .map_err(|e| SecretStoreError::storage(e.to_string()))?;
            while let Some(entry) = iter
                .next()
                .await
                .map_err(|e| SecretStoreError::storage(e.to_string()))?
            {
                if listings.len() >= MAX_SECRETS_PER_NAMESPACE {
                    return Err(SecretStoreError::capacity_exceeded(
                        "secret list scan limit reached",
                    ));
                }
                let encrypted: EncryptedSecret = serde_json::from_slice(&entry.value)
                    .map_err(|e| SecretStoreError::storage(format!("deserialize failed: {e}")))?;
                listings.push(SecretListing {
                    name: encrypted.name,
                    secret_type: encrypted.secret_type,
                    created_at: encrypted.metadata.created_at,
                    updated_at: encrypted.metadata.updated_at,
                    version: encrypted.metadata.version,
                });
                self.metrics
                    .secret_scan_fill_level
                    .store(listings.len(), Ordering::Relaxed);
            }
        } else {
            let map = self.in_memory.read();
            for ((ns, _), enc) in map.iter() {
                if *ns == namespace_id {
                    if listings.len() >= MAX_SECRETS_PER_NAMESPACE {
                        return Err(SecretStoreError::capacity_exceeded(
                            "secret list scan limit reached",
                        ));
                    }
                    listings.push(SecretListing {
                        name: enc.name.clone(),
                        secret_type: enc.secret_type.clone(),
                        created_at: enc.metadata.created_at,
                        updated_at: enc.metadata.updated_at,
                        version: enc.metadata.version,
                    });
                }
            }
            self.metrics
                .secret_scan_fill_level
                .store(listings.len(), Ordering::Relaxed);
        }

        listings.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(listings)
    }

    /// Re-wrap all secrets with a new KEK provider (dynamic rotation).
    pub async fn rotate_kek(
        &self,
        new_kek_provider: Arc<dyn KekProvider>,
        actor: &str,
    ) -> Result<usize, SecretStoreError> {
        let old_kek = self.kek_provider();
        let mut rotated_count = 0;

        if let Some(db) = &self.db {
            let prefix = CATALOG_SECRETS_PREFIX;
            let mut iter = db
                .scan_prefix(&prefix)
                .await
                .map_err(|e| SecretStoreError::storage(e.to_string()))?;
            while let Some(entry) = iter
                .next()
                .await
                .map_err(|e| SecretStoreError::storage(e.to_string()))?
            {
                let encrypted: EncryptedSecret = serde_json::from_slice(&entry.value)
                    .map_err(|e| SecretStoreError::storage(format!("deserialize failed: {e}")))?;
                let rotated =
                    rotate_secret_kek(&encrypted, old_kek.as_ref(), new_kek_provider.as_ref())
                        .await?;
                let serialized = serde_json::to_vec(&rotated)
                    .map_err(|e| SecretStoreError::storage(format!("serialize failed: {e}")))?;
                db.put(&entry.key, &serialized)
                    .await
                    .map_err(|e| SecretStoreError::storage(e.to_string()))?;
                rotated_count += 1;
                self.metrics
                    .secret_scan_fill_level
                    .store(rotated_count, Ordering::Relaxed);
            }
        } else {
            let entries: Vec<_> = {
                let map = self.in_memory.read();
                map.iter()
                    .map(|(key, encrypted)| (key.clone(), encrypted.clone()))
                    .collect()
            };
            if entries.len() > MAX_IN_MEMORY_SECRETS {
                return Err(SecretStoreError::capacity_exceeded(
                    "in-memory secret rotation window limit reached",
                ));
            }
            for (key, enc) in entries {
                let rotated =
                    rotate_secret_kek(&enc, old_kek.as_ref(), new_kek_provider.as_ref()).await?;
                self.in_memory.write().insert(key, rotated);
                rotated_count += 1;
                self.metrics
                    .secret_scan_fill_level
                    .store(rotated_count, Ordering::Relaxed);
            }
        }

        *self.kek_provider.write() = new_kek_provider;
        self.record_audit(actor, "secret.rotated", "cluster_kek", None);

        Ok(rotated_count)
    }

    /// Issue a short-lived secret token encrypted for a worker node identity.
    pub async fn issue_worker_token(
        &self,
        namespace_id: u128,
        secret_name: &str,
        worker_identity: &str,
        ttl_secs: u64,
        actor: &str,
    ) -> Result<SecretToken, SecretStoreError> {
        let secret = self.get_secret(namespace_id, secret_name).await?;

        let node_key = SecretToken::worker_key(worker_identity);

        let serialized_payload = serde_json::to_vec(&secret.payload)
            .map_err(|e| SecretStoreError::encryption_failed(format!("serialize failed: {e}")))?;

        let (encrypted_payload, nonce) =
            crate::kek::encrypt_aes_256_gcm(&node_key, &serialized_payload)?;

        let now = chrono::Utc::now().timestamp() as u64;
        let token_id = uuid::Uuid::new_v4().to_string();

        let token = SecretToken {
            token_id,
            secret_name: secret_name.to_string(),
            secret_type: secret.secret_type,
            worker_identity: worker_identity.to_string(),
            encrypted_payload,
            nonce,
            issued_at: now,
            expires_at: now + ttl_secs,
        };

        self.record_audit(
            actor,
            "secret.token_issued",
            &format!("{secret_name}:{worker_identity}"),
            None,
        );

        Ok(token)
    }
}

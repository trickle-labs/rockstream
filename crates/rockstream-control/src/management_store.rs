use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;

pub const MANAGEMENT_OPERATION_RECORD_VERSION: u16 = 2;
pub const IDEMPOTENCY_KEY_RETENTION_MS: i64 = 24 * 60 * 60 * 1_000;
pub const TERMINAL_OPERATION_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
pub const MAX_ACTIVE_OPERATIONS: usize = 1_000;
pub const MAX_RETAINED_OPERATIONS: usize = 10_000;
pub const MAX_OPERATION_PAGE_SIZE: usize = 100;
const MAX_OPERATION_ID_BYTES: usize = 128;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
const IDEMPOTENCY_RECORD_VERSION: u16 = 1;
const LEGACY_OPERATION_RECORD_VERSION: u16 = 1;
const MAX_OPERATION_TRANSITIONS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    DrainWorker,
    MigrateShard,
    CreateBackup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Pending,
    Running,
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
}

impl OperationStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Pending,
                Self::Running | Self::Waiting | Self::Failed | Self::Cancelled
            ) | (
                Self::Running,
                Self::Waiting | Self::Succeeded | Self::Failed | Self::Cancelled
            ) | (
                Self::Waiting,
                Self::Running | Self::Failed | Self::Cancelled
            )
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRecord {
    record_version: u16,
    operation_id: String,
    kind: OperationKind,
    status: OperationStatus,
    started_at: i64,
    updated_at: i64,
    progress: Option<u8>,
    phase: Option<String>,
    error_code: Option<String>,
    next_steps: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request: Option<serde_json::Value>,
}

/// Durable binding of one client idempotency key to one accepted operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    record_version: u16,
    request_digest: String,
    operation_id: String,
    accepted_at: i64,
}

impl IdempotencyRecord {
    pub fn request_digest(&self) -> &str {
        &self.request_digest
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub fn accepted_at_ms(&self) -> i64 {
        self.accepted_at
    }
}

impl OperationRecord {
    pub fn accepted(
        operation_id: impl Into<String>,
        kind: OperationKind,
        accepted_at_ms: i64,
    ) -> Self {
        Self::accepted_with_request(operation_id, kind, accepted_at_ms, None)
    }

    fn accepted_with_request(
        operation_id: impl Into<String>,
        kind: OperationKind,
        accepted_at_ms: i64,
        request: Option<serde_json::Value>,
    ) -> Self {
        Self {
            record_version: MANAGEMENT_OPERATION_RECORD_VERSION,
            operation_id: operation_id.into(),
            kind,
            status: OperationStatus::Pending,
            started_at: accepted_at_ms,
            updated_at: accepted_at_ms,
            progress: None,
            phase: None,
            error_code: None,
            next_steps: Vec::new(),
            request,
        }
    }

    pub fn record_version(&self) -> u16 {
        self.record_version
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub fn kind(&self) -> OperationKind {
        self.kind
    }

    pub fn status(&self) -> OperationStatus {
        self.status
    }

    pub fn started_at_ms(&self) -> i64 {
        self.started_at
    }

    pub fn updated_at_ms(&self) -> i64 {
        self.updated_at
    }

    pub fn progress(&self) -> Option<u8> {
        self.progress
    }

    pub fn phase(&self) -> Option<&str> {
        self.phase.as_deref()
    }

    pub fn error_code(&self) -> Option<&str> {
        self.error_code.as_deref()
    }

    pub fn next_steps(&self) -> &[String] {
        &self.next_steps
    }

    pub fn request(&self) -> Option<&serde_json::Value> {
        self.request.as_ref()
    }

    pub fn apply_update(&mut self, update: OperationUpdate) -> Result<(), OperationLifecycleError> {
        if self.status.is_terminal() {
            return Err(OperationLifecycleError::TerminalStateIsImmutable(
                self.status,
            ));
        }
        if update.updated_at_ms < self.updated_at {
            return Err(OperationLifecycleError::TimestampRegression {
                current: self.updated_at,
                proposed: update.updated_at_ms,
            });
        }
        if let Some(progress) = update.progress.filter(|progress| *progress > 100) {
            return Err(OperationLifecycleError::InvalidProgress(progress));
        }
        let metadata_update = update.status == self.status
            && matches!(
                self.status,
                OperationStatus::Running | OperationStatus::Waiting
            );
        if !metadata_update && !self.status.can_transition_to(update.status) {
            return Err(OperationLifecycleError::IllegalTransition {
                from: self.status,
                to: update.status,
            });
        }

        self.status = update.status;
        self.updated_at = update.updated_at_ms;
        self.progress = update.progress;
        self.phase = update.phase;
        self.error_code = update.error_code;
        self.next_steps = update.next_steps;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationUpdate {
    pub status: OperationStatus,
    pub updated_at_ms: i64,
    pub progress: Option<u8>,
    pub phase: Option<String>,
    pub error_code: Option<String>,
    pub next_steps: Vec<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OperationLifecycleError {
    #[error("illegal operation transition {from:?} -> {to:?}")]
    IllegalTransition {
        from: OperationStatus,
        to: OperationStatus,
    },
    #[error("terminal operation state {0:?} is immutable")]
    TerminalStateIsImmutable(OperationStatus),
    #[error("operation timestamp regressed from {current} to {proposed}")]
    TimestampRegression { current: i64, proposed: i64 },
    #[error("operation progress {0} exceeds 100")]
    InvalidProgress(u8),
}

#[derive(Debug, Error)]
pub enum OperationStoreError {
    #[error("operation identifier must contain 1 to {MAX_OPERATION_ID_BYTES} bytes")]
    InvalidOperationId,
    #[error("idempotency key must contain 1 to {MAX_IDEMPOTENCY_KEY_BYTES} bytes")]
    InvalidIdempotencyKey,
    #[error("operation {0} already exists")]
    AlreadyExists(String),
    #[error("operation record version {actual} is unsupported; expected {expected}")]
    UnsupportedRecordVersion { actual: u16, expected: u16 },
    #[error("operation record is corrupt: {0}")]
    CorruptRecord(String),
    #[error("operation page token is invalid")]
    InvalidPageToken,
    #[error("operation page size must be between 1 and {MAX_OPERATION_PAGE_SIZE}")]
    InvalidPageSize,
    #[error("operation history exceeds the retained limit of {MAX_RETAINED_OPERATIONS}")]
    HistoryLimit,
    #[error("active operation limit of {MAX_ACTIVE_OPERATIONS} reached")]
    ActiveLimit,
    #[error("retained operation limit of {MAX_RETAINED_OPERATIONS} reached")]
    RetainedLimit,
    #[error("operation {0} was not found")]
    NotFound(String),
    #[error("operation {0} changed while the request was processed")]
    TransitionConflict(String),
    #[error("operation {0} exceeded the transition limit of {MAX_OPERATION_TRANSITIONS}")]
    TransitionLimit(String),
    #[error(
        "idempotency key is already bound to operation {operation_id} with a different request"
    )]
    IdempotencyConflict { operation_id: String },
    #[error("idempotency key for operation {operation_id} expired and cannot be reused")]
    IdempotencyExpired { operation_id: String },
    #[error("idempotency record is corrupt: {0}")]
    CorruptIdempotencyRecord(String),
    #[error("operation record serialization failed: {0}")]
    Serialization(String),
    #[error("operation store failed: {0}")]
    Storage(String),
    #[error(transparent)]
    Lifecycle(#[from] OperationLifecycleError),
}

fn decode_operation_record(bytes: &[u8]) -> Result<OperationRecord, OperationStoreError> {
    let mut record: OperationRecord = serde_json::from_slice(bytes)
        .map_err(|error| OperationStoreError::CorruptRecord(error.to_string()))?;
    match record.record_version {
        LEGACY_OPERATION_RECORD_VERSION => {
            record.record_version = MANAGEMENT_OPERATION_RECORD_VERSION;
        }
        MANAGEMENT_OPERATION_RECORD_VERSION => {}
        actual => {
            return Err(OperationStoreError::UnsupportedRecordVersion {
                actual,
                expected: MANAGEMENT_OPERATION_RECORD_VERSION,
            });
        }
    }
    Ok(record)
}

/// Durable, versioned operation records stored alongside control metadata.
#[derive(Clone)]
pub struct ManagementOperationStore {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    // ponytail: one process-local lock serializes scans; create-only transition records fence shared-store writers.
    write_lock: Arc<Mutex<()>>,
}

impl ManagementOperationStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            prefix: Path::from("control/management-operations"),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    fn operation_path(&self, operation_id: &str) -> Result<Path, OperationStoreError> {
        if operation_id.is_empty() || operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(OperationStoreError::InvalidOperationId);
        }
        Ok(self
            .prefix
            .clone()
            .join(format!("{}.json", hex::encode(operation_id.as_bytes()))))
    }

    fn transition_path(
        &self,
        operation_id: &str,
        previous_record: &[u8],
    ) -> Result<Path, OperationStoreError> {
        if operation_id.is_empty() || operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(OperationStoreError::InvalidOperationId);
        }
        Ok(self
            .prefix
            .clone()
            .join("transitions")
            .join(hex::encode(operation_id.as_bytes()))
            .join(format!(
                "{}.json",
                hex::encode(Sha256::digest(previous_record))
            )))
    }

    fn idempotency_path(&self, key: &str) -> Result<Path, OperationStoreError> {
        if key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(OperationStoreError::InvalidIdempotencyKey);
        }
        Ok(self.prefix.clone().join("idempotency").join(format!(
            "{}.json",
            hex::encode(Sha256::digest(key.as_bytes()))
        )))
    }

    fn legacy_idempotency_path(&self, key: &str) -> Result<Path, OperationStoreError> {
        if key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(OperationStoreError::InvalidIdempotencyKey);
        }
        Ok(self.prefix.clone().join(format!(
            "idempotency/{}.json",
            hex::encode(Sha256::digest(key.as_bytes()))
        )))
    }

    /// Hashes a protocol version and a canonical JSON request representation.
    pub fn canonical_request_digest<T: Serialize>(
        protocol_version: u16,
        request: &T,
    ) -> Result<String, OperationStoreError> {
        fn canonicalize(value: serde_json::Value) -> serde_json::Value {
            match value {
                serde_json::Value::Array(values) => {
                    serde_json::Value::Array(values.into_iter().map(canonicalize).collect())
                }
                serde_json::Value::Object(values) => serde_json::Value::Object(
                    values
                        .into_iter()
                        .map(|(key, value)| (key, canonicalize(value)))
                        .collect::<BTreeMap<_, _>>()
                        .into_iter()
                        .collect(),
                ),
                value => value,
            }
        }

        let request = serde_json::to_value(request)
            .map_err(|error| OperationStoreError::Serialization(error.to_string()))?;
        let canonical = serde_json::to_vec(&canonicalize(request))
            .map_err(|error| OperationStoreError::Serialization(error.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(protocol_version.to_be_bytes());
        hasher.update(canonical);
        Ok(hex::encode(hasher.finalize()))
    }

    /// Persist Pending before returning acceptance to the caller.
    pub async fn accept(
        &self,
        operation_id: impl Into<String>,
        kind: OperationKind,
        // Stored as both started_at and updated_at in UTC Unix epoch milliseconds.
        accepted_at_ms: i64,
    ) -> Result<OperationRecord, OperationStoreError> {
        let _guard = self.write_lock.lock().await;
        self.accept_locked(operation_id, kind, accepted_at_ms, None)
            .await
    }

    /// Persist an idempotency binding and its Pending operation before dispatching an effect.
    pub async fn accept_idempotent<T: Serialize>(
        &self,
        idempotency_key: &str,
        protocol_version: u16,
        request: &T,
        operation_id: impl Into<String>,
        kind: OperationKind,
        accepted_at_ms: i64,
    ) -> Result<OperationRecord, OperationStoreError> {
        let canonical_request = (kind, request);
        let request_digest = Self::canonical_request_digest(protocol_version, &canonical_request)?;
        let request = serde_json::to_value(request)
            .map_err(|error| OperationStoreError::Serialization(error.to_string()))?;
        let _guard = self.write_lock.lock().await;
        let path = self.idempotency_path(idempotency_key)?;
        let operation_id = operation_id.into();
        self.operation_path(&operation_id)?;
        let binding = IdempotencyRecord {
            record_version: IDEMPOTENCY_RECORD_VERSION,
            request_digest: request_digest.clone(),
            operation_id,
            accepted_at: accepted_at_ms,
        };
        let payload = serde_json::to_vec(&binding)
            .map_err(|error| OperationStoreError::Serialization(error.to_string()))?;
        match self
            .store
            .put_opts(
                &path,
                payload.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(error) => return Err(OperationStoreError::Storage(error.to_string())),
        }
        let binding = self
            .read_idempotency(idempotency_key)
            .await?
            .ok_or_else(|| {
                OperationStoreError::CorruptIdempotencyRecord(
                    "idempotency binding disappeared after create".to_owned(),
                )
            })?;
        self.resolve_idempotency(binding, &request_digest, kind, &request, accepted_at_ms)
            .await
    }

    async fn accept_locked(
        &self,
        operation_id: impl Into<String>,
        kind: OperationKind,
        accepted_at_ms: i64,
        request: Option<serde_json::Value>,
    ) -> Result<OperationRecord, OperationStoreError> {
        let records = self.load_all().await?;
        let expired = records
            .iter()
            .filter(|record| {
                record.status().is_terminal()
                    && record.updated_at_ms()
                        < accepted_at_ms.saturating_sub(TERMINAL_OPERATION_RETENTION_MS)
            })
            .map(|record| record.operation_id().to_owned())
            .collect::<Vec<_>>();
        for operation_id in expired {
            self.store
                .delete(&self.operation_path(&operation_id)?)
                .await
                .map_err(|error| OperationStoreError::Storage(error.to_string()))?;
            let transition_prefix = self
                .prefix
                .clone()
                .join("transitions")
                .join(hex::encode(operation_id.as_bytes()));
            let mut listing = self.store.list(Some(&transition_prefix));
            while let Some(entry) = listing.next().await {
                let meta =
                    entry.map_err(|error| OperationStoreError::Storage(error.to_string()))?;
                self.store
                    .delete(&meta.location)
                    .await
                    .map_err(|error| OperationStoreError::Storage(error.to_string()))?;
            }
        }
        let records = self.load_all().await?;
        if records.len() >= MAX_RETAINED_OPERATIONS {
            return Err(OperationStoreError::RetainedLimit);
        }
        if records
            .iter()
            .filter(|record| !record.status().is_terminal())
            .count()
            >= MAX_ACTIVE_OPERATIONS
        {
            return Err(OperationStoreError::ActiveLimit);
        }
        let record =
            OperationRecord::accepted_with_request(operation_id, kind, accepted_at_ms, request);
        let path = self.operation_path(record.operation_id())?;
        let payload = serde_json::to_vec(&record)
            .map_err(|error| OperationStoreError::Serialization(error.to_string()))?;
        match self
            .store
            .put_opts(
                &path,
                payload.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(record),
            Err(object_store::Error::AlreadyExists { .. }) => Err(
                OperationStoreError::AlreadyExists(record.operation_id().to_owned()),
            ),
            Err(error) => Err(OperationStoreError::Storage(error.to_string())),
        }
    }

    async fn resolve_idempotency(
        &self,
        binding: IdempotencyRecord,
        request_digest: &str,
        kind: OperationKind,
        request: &serde_json::Value,
        now_ms: i64,
    ) -> Result<OperationRecord, OperationStoreError> {
        if binding.accepted_at < now_ms.saturating_sub(IDEMPOTENCY_KEY_RETENTION_MS) {
            return Err(OperationStoreError::IdempotencyExpired {
                operation_id: binding.operation_id,
            });
        }
        if binding.request_digest != request_digest {
            return Err(OperationStoreError::IdempotencyConflict {
                operation_id: binding.operation_id,
            });
        }
        if let Some(record) = self.get(&binding.operation_id).await? {
            return Ok(record);
        }
        match self
            .accept_locked(
                binding.operation_id.clone(),
                kind,
                binding.accepted_at,
                Some(request.clone()),
            )
            .await
        {
            Ok(record) => Ok(record),
            Err(OperationStoreError::AlreadyExists(_)) => {
                self.get(&binding.operation_id).await?.ok_or_else(|| {
                    OperationStoreError::CorruptIdempotencyRecord(
                        "binding references a missing operation".to_owned(),
                    )
                })
            }
            Err(error) => Err(error),
        }
    }

    pub async fn get(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationRecord>, OperationStoreError> {
        self.read_entry(operation_id).await
    }

    pub async fn list(
        &self,
        page_size: usize,
        page_token: &str,
    ) -> Result<(Vec<OperationRecord>, String), OperationStoreError> {
        if !(1..=MAX_OPERATION_PAGE_SIZE).contains(&page_size) {
            return Err(OperationStoreError::InvalidPageSize);
        }
        let offset = if page_token.is_empty() {
            0
        } else {
            page_token
                .parse::<usize>()
                .map_err(|_| OperationStoreError::InvalidPageToken)?
        };
        let mut records = self.load_all().await?;
        records.sort_by(|left, right| {
            left.updated_at_ms()
                .cmp(&right.updated_at_ms())
                .then_with(|| left.operation_id().cmp(right.operation_id()))
        });
        if offset > records.len() {
            return Err(OperationStoreError::InvalidPageToken);
        }
        let end = offset.saturating_add(page_size).min(records.len());
        let next = if end < records.len() {
            end.to_string()
        } else {
            String::new()
        };
        Ok((
            records.into_iter().skip(offset).take(page_size).collect(),
            next,
        ))
    }

    pub async fn counts(&self) -> Result<(usize, usize), OperationStoreError> {
        let records = self.load_all().await?;
        let active = records
            .iter()
            .filter(|record| !record.status().is_terminal())
            .count();
        Ok((active, records.len()))
    }

    pub async fn nonterminal(&self) -> Result<Vec<OperationRecord>, OperationStoreError> {
        let bound = self.bound_operation_ids().await?;
        let mut records = self
            .load_all()
            .await?
            .into_iter()
            .filter(|record| {
                !record.status().is_terminal()
                    && (record.request().is_none() || bound.contains(record.operation_id()))
            })
            .collect::<Vec<_>>();
        records.sort_by(|left, right| left.operation_id().cmp(right.operation_id()));
        Ok(records)
    }

    async fn bound_operation_ids(
        &self,
    ) -> Result<std::collections::HashSet<String>, OperationStoreError> {
        let mut listing = self
            .store
            .list(Some(&self.prefix.clone().join("idempotency")));
        let mut operation_ids = std::collections::HashSet::new();
        while let Some(entry) = listing.next().await {
            let meta = entry.map_err(|error| OperationStoreError::Storage(error.to_string()))?;
            let bytes = self
                .store
                .get(&meta.location)
                .await
                .map_err(|error| OperationStoreError::Storage(error.to_string()))?
                .bytes()
                .await
                .map_err(|error| OperationStoreError::Storage(error.to_string()))?;
            let binding: IdempotencyRecord = serde_json::from_slice(&bytes).map_err(|error| {
                OperationStoreError::CorruptIdempotencyRecord(error.to_string())
            })?;
            if binding.record_version != IDEMPOTENCY_RECORD_VERSION {
                return Err(OperationStoreError::CorruptIdempotencyRecord(format!(
                    "record version {} is unsupported; expected {IDEMPOTENCY_RECORD_VERSION}",
                    binding.record_version
                )));
            }
            operation_ids.insert(binding.operation_id);
        }
        Ok(operation_ids)
    }

    pub async fn transition(
        &self,
        operation_id: &str,
        update: OperationUpdate,
    ) -> Result<OperationRecord, OperationStoreError> {
        self.write_transition(operation_id, None, None, update)
            .await
    }

    pub async fn transition_if(
        &self,
        operation_id: &str,
        expected_status: OperationStatus,
        expected_phase: Option<&str>,
        update: OperationUpdate,
    ) -> Result<OperationRecord, OperationStoreError> {
        self.write_transition(operation_id, Some(expected_status), expected_phase, update)
            .await
    }

    async fn write_transition(
        &self,
        operation_id: &str,
        expected_status: Option<OperationStatus>,
        expected_phase: Option<&str>,
        update: OperationUpdate,
    ) -> Result<OperationRecord, OperationStoreError> {
        let _guard = self.write_lock.lock().await;
        let (mut record, previous_bytes) = self
            .read_state(operation_id)
            .await?
            .ok_or_else(|| OperationStoreError::NotFound(operation_id.to_owned()))?;
        if let Some(expected_status) = expected_status {
            if record.status() != expected_status || record.phase() != expected_phase {
                return Err(OperationStoreError::TransitionConflict(
                    operation_id.to_owned(),
                ));
            }
        }
        record.apply_update(update)?;
        let payload = serde_json::to_vec(&record)
            .map_err(|error| OperationStoreError::Serialization(error.to_string()))?;
        if payload == previous_bytes {
            return Ok(record);
        }
        self.store
            .put_opts(
                &self.transition_path(operation_id, &previous_bytes)?,
                payload.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|error| match error {
                object_store::Error::AlreadyExists { .. } => {
                    OperationStoreError::TransitionConflict(operation_id.to_owned())
                }
                error => OperationStoreError::Storage(error.to_string()),
            })?;
        Ok(record)
    }

    async fn read_state(
        &self,
        operation_id: &str,
    ) -> Result<Option<(OperationRecord, Vec<u8>)>, OperationStoreError> {
        let path = self.operation_path(operation_id)?;
        let object = match self.store.get(&path).await {
            Ok(object) => object,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(OperationStoreError::Storage(error.to_string())),
        };
        let mut bytes = object
            .bytes()
            .await
            .map_err(|error| OperationStoreError::Storage(error.to_string()))?
            .to_vec();
        let mut record = decode_operation_record(&bytes)?;
        if record.operation_id() != operation_id {
            return Err(OperationStoreError::CorruptRecord(
                "record identifier does not match its storage key".to_owned(),
            ));
        }
        for transition in 0..=MAX_OPERATION_TRANSITIONS {
            let path = self.transition_path(operation_id, &bytes)?;
            let next = match self.store.get(&path).await {
                Ok(next) if transition < MAX_OPERATION_TRANSITIONS => next,
                Ok(_) => {
                    return Err(OperationStoreError::TransitionLimit(
                        operation_id.to_owned(),
                    ))
                }
                Err(object_store::Error::NotFound { .. }) => return Ok(Some((record, bytes))),
                Err(error) => return Err(OperationStoreError::Storage(error.to_string())),
            };
            bytes = next
                .bytes()
                .await
                .map_err(|error| OperationStoreError::Storage(error.to_string()))?
                .to_vec();
            record = decode_operation_record(&bytes)?;
            if record.operation_id() != operation_id {
                return Err(OperationStoreError::CorruptRecord(
                    "transition record identifier does not match its storage key".to_owned(),
                ));
            }
        }
        unreachable!("the transition limit returns an error before this point")
    }

    async fn read_entry(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationRecord>, OperationStoreError> {
        Ok(self
            .read_state(operation_id)
            .await?
            .map(|(record, _)| record))
    }

    async fn load_all(&self) -> Result<Vec<OperationRecord>, OperationStoreError> {
        let mut listing = self.store.list(Some(&self.prefix));
        let mut records = Vec::new();
        while let Some(entry) = listing.next().await {
            let meta = entry.map_err(|error| OperationStoreError::Storage(error.to_string()))?;
            let location = meta.location.to_string();
            if location.contains("idempotency") || location.contains("transitions") {
                continue;
            }
            let bytes = self
                .store
                .get(&meta.location)
                .await
                .map_err(|error| OperationStoreError::Storage(error.to_string()))?
                .bytes()
                .await
                .map_err(|error| OperationStoreError::Storage(error.to_string()))?;
            let base_record = decode_operation_record(&bytes)?;
            let record = self
                .read_entry(base_record.operation_id())
                .await?
                .unwrap_or(base_record);
            records.push(record);
            if records.len() > MAX_RETAINED_OPERATIONS {
                return Err(OperationStoreError::HistoryLimit);
            }
        }
        Ok(records)
    }

    async fn read_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<IdempotencyRecord>, OperationStoreError> {
        let path = self.idempotency_path(idempotency_key)?;
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                let legacy_path = self.legacy_idempotency_path(idempotency_key)?;
                match self.store.get(&legacy_path).await {
                    Ok(result) => result,
                    Err(object_store::Error::NotFound { .. }) => return Ok(None),
                    Err(error) => return Err(OperationStoreError::Storage(error.to_string())),
                }
            }
            Err(error) => return Err(OperationStoreError::Storage(error.to_string())),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| OperationStoreError::Storage(error.to_string()))?;
        let record: IdempotencyRecord = serde_json::from_slice(&bytes)
            .map_err(|error| OperationStoreError::CorruptIdempotencyRecord(error.to_string()))?;
        if record.record_version != IDEMPOTENCY_RECORD_VERSION {
            return Err(OperationStoreError::CorruptIdempotencyRecord(format!(
                "record version {} is unsupported; expected {IDEMPOTENCY_RECORD_VERSION}",
                record.record_version
            )));
        }
        Ok(Some(record))
    }
}

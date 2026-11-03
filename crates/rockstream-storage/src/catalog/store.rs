//! Durable Catalog Store Implementation (Slices 1–4).
//!
//! Authoritative store for all metadata categories backed by an `ObjectStore`
//! (`file://` or `s3://`).
//!
//! Provides:
//! - Atomic logical transactions with `commit_txn`
//! - Monotonic revision tracking and deduplication
//! - Snapshot-plus-log recovery and compaction without range deletion
//! - Strict pre-commit validation (cycles, missing references, drop cascade)

use async_trait::async_trait;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::identity::IdAllocator;
use super::log::MAX_REPLAY_BUFFER_BYTES;
use super::snapshot::CatalogSnapshot;
use super::txn::{CatalogMutation, CatalogTxn};
use super::{
    CatalogDatabase, CatalogError, CatalogIndexEntry, CatalogInlineView, CatalogNamespace,
    CatalogRoleEntry, CatalogSinkEntry, CatalogSourceEntry, CatalogTable, CatalogView,
    CatalogWorkloadEntry, CompiledPlanRecord, DependencyKind, ViewDependency,
};
use rockstream_types::authorization::{
    AuditCorrelation, AuditField, AuditIdentity, PermissionPreset, PolicyScope, SecurityAuditClass,
    SecurityAuditOutcome, SecurityAuditRecord, SecurityPrincipal, SECURITY_AUDIT_FORMAT_VERSION,
};
use rockstream_types::ids::{
    CompiledPlanId, DatabaseId, IndexId, NamespaceId, PrincipalId, SinkId, SourceId, TableId,
    ViewId, WorkloadId,
};
use rockstream_types::workload::WorkloadDef;

const SECURITY_AUDIT_RETENTION_MS: u64 = 90 * 24 * 60 * 60 * 1000;
const MAX_SECURITY_AUDIT_BYTES: u64 = 1024 * 1024 * 1024;

/// Abstract interface for catalog operations.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait CatalogStore: Send + Sync {
    async fn commit_txn(&self, txn: CatalogTxn) -> Result<u64, CatalogError>;
    async fn get_revision(&self) -> u64;
    async fn allocate_id(&self) -> Result<u64, CatalogError>;

    async fn get_database(&self, id: DatabaseId) -> Result<Option<CatalogDatabase>, CatalogError>;
    async fn get_namespace(
        &self,
        id: NamespaceId,
    ) -> Result<Option<CatalogNamespace>, CatalogError>;
    async fn get_table(&self, id: TableId) -> Result<Option<CatalogTable>, CatalogError>;
    async fn get_table_by_name(&self, name: &str) -> Result<Option<CatalogTable>, CatalogError>;
    async fn list_tables(&self) -> Result<Vec<CatalogTable>, CatalogError>;

    async fn get_view(&self, id: ViewId) -> Result<Option<CatalogView>, CatalogError>;
    async fn get_view_by_name(&self, name: &str) -> Result<Option<CatalogView>, CatalogError>;
    async fn list_views(&self) -> Result<Vec<CatalogView>, CatalogError>;

    async fn get_inline_view(&self, id: ViewId) -> Result<Option<CatalogInlineView>, CatalogError>;
    async fn list_inline_views(&self) -> Result<Vec<CatalogInlineView>, CatalogError>;

    async fn list_view_dependencies(&self) -> Result<Vec<ViewDependency>, CatalogError>;

    async fn get_index(&self, id: IndexId) -> Result<Option<CatalogIndexEntry>, CatalogError>;
    async fn list_indexes(&self) -> Result<Vec<CatalogIndexEntry>, CatalogError>;

    async fn get_workload(&self, id: WorkloadId) -> Result<Option<WorkloadDef>, CatalogError>;
    async fn list_workloads(&self) -> Result<Vec<WorkloadDef>, CatalogError>;

    async fn get_source(&self, id: SourceId) -> Result<Option<CatalogSourceEntry>, CatalogError>;
    async fn list_sources(&self) -> Result<Vec<CatalogSourceEntry>, CatalogError>;

    async fn get_sink(&self, id: SinkId) -> Result<Option<CatalogSinkEntry>, CatalogError>;
    async fn list_sinks(&self) -> Result<Vec<CatalogSinkEntry>, CatalogError>;

    async fn get_role(&self, id: PrincipalId) -> Result<Option<CatalogRoleEntry>, CatalogError>;
    async fn list_roles(&self) -> Result<Vec<CatalogRoleEntry>, CatalogError>;

    async fn get_security_principal(
        &self,
        id: PrincipalId,
    ) -> Result<Option<SecurityPrincipal>, CatalogError>;
    async fn list_security_principals(&self) -> Result<Vec<SecurityPrincipal>, CatalogError>;
    async fn list_security_audit_records(&self) -> Result<Vec<SecurityAuditRecord>, CatalogError>;

    async fn get_compiled_plan(
        &self,
        id: CompiledPlanId,
    ) -> Result<Option<CompiledPlanRecord>, CatalogError>;
    async fn list_compiled_plans(&self) -> Result<Vec<CompiledPlanRecord>, CatalogError>;
}

/// In-memory catalog state snapshot.
#[derive(Default, Clone)]
struct CatalogInnerState {
    revision: u64,
    committed_operations: HashSet<u64>,
    databases: HashMap<DatabaseId, CatalogDatabase>,
    namespaces: HashMap<NamespaceId, CatalogNamespace>,
    tables: HashMap<TableId, CatalogTable>,
    tables_by_name: HashMap<String, TableId>,
    views: HashMap<ViewId, CatalogView>,
    views_by_name: HashMap<String, ViewId>,
    inline_views: HashMap<ViewId, CatalogInlineView>,
    view_dependencies: Vec<ViewDependency>,
    indexes: HashMap<IndexId, CatalogIndexEntry>,
    workloads: HashMap<WorkloadId, WorkloadDef>,
    sources: HashMap<SourceId, CatalogSourceEntry>,
    sinks: HashMap<SinkId, CatalogSinkEntry>,
    roles: HashMap<PrincipalId, CatalogRoleEntry>,
    security_principals: HashMap<PrincipalId, SecurityPrincipal>,
    security_audit_records: Vec<SecurityAuditRecord>,
    security_audit_bytes: u64,
    security_audit_oldest_timestamp_ms: Option<u64>,
    compiled_plans: HashMap<CompiledPlanId, CompiledPlanRecord>,
}

/// Durable catalog store backed by an `ObjectStore`.
pub struct DurableCatalogStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    state: RwLock<CatalogInnerState>,
    id_allocator: IdAllocator,
}

impl std::fmt::Debug for DurableCatalogStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableCatalogStore")
            .field("prefix", &self.prefix)
            .field("id_allocator", &self.id_allocator)
            .finish()
    }
}

impl DurableCatalogStore {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
            state: RwLock::new(CatalogInnerState::default()),
            id_allocator: IdAllocator::new(0),
        }
    }

    pub fn id_allocator(&self) -> &IdAllocator {
        &self.id_allocator
    }

    fn snapshot_path(&self, revision: u64) -> ObjectPath {
        let p = format!("{}/catalog/snapshots/{:020}.snap", self.prefix, revision);
        ObjectPath::from(p.trim_start_matches('/'))
    }

    fn snapshot_tmp_path(&self, revision: u64) -> ObjectPath {
        let p = format!(
            "{}/catalog/snapshots/{:020}.snap.tmp",
            self.prefix, revision
        );
        ObjectPath::from(p.trim_start_matches('/'))
    }

    fn log_path(&self, revision: u64, operation_id: u64) -> ObjectPath {
        let p = format!(
            "{}/catalog/log/{:020}_{:020}.log",
            self.prefix, revision, operation_id
        );
        ObjectPath::from(p.trim_start_matches('/'))
    }

    /// Persist a snapshot of the current state at the given revision.
    pub async fn create_snapshot(&self) -> Result<CatalogSnapshot, CatalogError> {
        let state = self.state.read().await;
        let audit_cutoff_ms = security_audit_cutoff_ms(current_time_ms()?);
        let retained_audit_records = state
            .security_audit_records
            .iter()
            .filter(|record| record.timestamp_ms >= audit_cutoff_ms)
            .cloned()
            .collect();
        let snapshot = CatalogSnapshot::new(
            state.revision,
            self.id_allocator.high_water_mark(),
            state.databases.values().cloned().collect(),
            state.namespaces.values().cloned().collect(),
            state.tables.values().cloned().collect(),
            state.views.values().cloned().collect(),
            state.inline_views.values().cloned().collect(),
            state.view_dependencies.clone(),
            state.indexes.values().cloned().collect(),
            state
                .workloads
                .iter()
                .map(|(&id, definition)| CatalogWorkloadEntry {
                    id,
                    definition: definition.clone(),
                })
                .collect(),
            state.sources.values().cloned().collect(),
            state.sinks.values().cloned().collect(),
            state.roles.values().cloned().collect(),
            state.compiled_plans.values().cloned().collect(),
            state.committed_operations.iter().copied().collect(),
        )?
        .with_security_records(
            state.security_principals.values().cloned().collect(),
            retained_audit_records,
        )?;
        drop(state);

        let bytes = snapshot.encode()?;
        let tmp_path = self.snapshot_tmp_path(snapshot.revision);
        let final_path = self.snapshot_path(snapshot.revision);

        // Write to tmp path then finalize
        self.store
            .put(&tmp_path, bytes.clone().into())
            .await
            .map_err(|e| {
                CatalogError::Storage(format!("[RS-0003] failed to write snapshot tmp file: {e}"))
            })?;

        self.store
            .put(&final_path, bytes.into())
            .await
            .map_err(|e| {
                CatalogError::Storage(format!("[RS-0003] failed to finalize snapshot: {e}"))
            })?;

        // Clean up tmp file
        let _ = self.store.delete(&tmp_path).await;

        Ok(snapshot)
    }

    /// Compact old transaction log files covered by a durable snapshot.
    ///
    /// Invariant: old log records are compacted ONLY after the durable snapshot
    /// is confirmed persisted. Never uses SlateDB range deletion!
    pub async fn compact_logs(&self, snapshot_revision: u64) -> Result<usize, CatalogError> {
        // Verify snapshot exists and is valid
        let snap_path = self.snapshot_path(snapshot_revision);
        let get_res = self.store.get(&snap_path).await.map_err(|_| {
            CatalogError::Storage(format!(
                "[RS-0003] snapshot at revision {} does not exist; cannot delete logs",
                snapshot_revision
            ))
        })?;

        let bytes = get_res.bytes().await.map_err(|e| {
            CatalogError::Storage(format!("[RS-0003] failed to read snapshot bytes: {e}"))
        })?;

        CatalogSnapshot::decode(&bytes)?;

        // Scan catalog/log/ and delete entries with revision <= snapshot_revision
        let log_prefix = format!("{}/catalog/log/", self.prefix);
        let log_prefix_path = ObjectPath::from(log_prefix.trim_start_matches('/'));

        let mut entries = self.store.list(Some(&log_prefix_path));
        let mut to_delete = Vec::new();

        while let Some(item) = entries.next().await {
            let meta =
                item.map_err(|e| CatalogError::Storage(format!("[RS-0003] log list error: {e}")))?;
            let location_str = meta.location.as_ref();
            if let Some(filename) = location_str.split('/').next_back() {
                if filename.ends_with(".log") {
                    let parts: Vec<&str> = filename.trim_end_matches(".log").split('_').collect();
                    if let Ok(rev) = parts[0].parse::<u64>() {
                        if rev <= snapshot_revision {
                            to_delete.push(meta.location);
                        }
                    }
                }
            }
        }

        let count = to_delete.len();
        for loc in to_delete {
            self.store.delete(&loc).await.map_err(|e| {
                CatalogError::Storage(format!("[RS-0003] failed to delete log file: {e}"))
            })?;
        }

        Ok(count)
    }

    /// Recover catalog state from durable storage.
    ///
    /// A failed listing is never an empty listing. An unreadable snapshot can
    /// only be bypassed when an earlier snapshot and contiguous logs reconstruct
    /// at least the revision that the unreadable snapshot advertised.
    pub async fn recover(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
    ) -> Result<Self, CatalogError> {
        let prefix = prefix.into();
        let catalog = Self::new(Arc::clone(&store), prefix.clone());
        let audit_cutoff_ms = security_audit_cutoff_ms(current_time_ms()?);

        // Find all finalized snapshots before selecting a recovery base.
        let snap_prefix = format!("{}/catalog/snapshots/", prefix);
        let snap_prefix_path = ObjectPath::from(snap_prefix.trim_start_matches('/'));

        let mut snapshots: Vec<(u64, ObjectPath)> = Vec::new();
        let mut list_stream = store.list(Some(&snap_prefix_path));
        while let Some(item) = list_stream.next().await {
            let meta = item.map_err(|e| {
                CatalogError::Storage(format!("[RS-0003] snapshot list error: {e}"))
            })?;
            let location = meta.location;
            let filename = location.as_ref().split('/').next_back().unwrap_or_default();
            if filename.ends_with(".snap") {
                if let Ok(rev) = filename.trim_end_matches(".snap").parse::<u64>() {
                    snapshots.push((rev, location));
                }
            }
        }

        snapshots.sort_by_key(|(rev, _)| *rev);
        let latest_snapshot_revision = snapshots.last().map(|(rev, _)| *rev);
        let mut snapshot_error = None;
        let mut recovered_state = CatalogInnerState::default();
        let mut start_revision = 0;

        // Try snapshots from newest to oldest. Do not publish an older state
        // unless log replay below can fill every missing revision.
        while let Some((listed_revision, path)) = snapshots.pop() {
            let loaded = async {
                let result = store.get(&path).await.map_err(|e| {
                    CatalogError::Storage(format!(
                        "[RS-0003] failed to get catalog snapshot {}: {e}",
                        path.as_ref()
                    ))
                })?;
                let bytes = result.bytes().await.map_err(|e| {
                    CatalogError::Storage(format!(
                        "[RS-0003] failed to read catalog snapshot bytes {}: {e}",
                        path.as_ref()
                    ))
                })?;
                let snap = CatalogSnapshot::decode(&bytes)?;
                if snap.revision != listed_revision {
                    return Err(CatalogError::DecodeError(format!(
                        "[RS-1003] catalog snapshot {} claims revision {} instead of {}; next_steps: inspect snapshot file",
                        path.as_ref(),
                        snap.revision,
                        listed_revision
                    )));
                }
                // Legacy snapshots omitted workload IDs and cannot restore
                // those identities without their original transaction logs.
                if !snap.workloads.is_empty() && snap.workload_entries.is_empty() {
                    return Err(CatalogError::DecodeError(
                        "[RS-1003] snapshot contains workloads without durable IDs; cannot recover identities safely".into(),
                    ));
                }
                Ok::<CatalogSnapshot, CatalogError>(snap)
            }
            .await;

            let snap = match loaded {
                Ok(snap) => snap,
                Err(error) => {
                    // Keep the newest failure to report if fallback is incomplete.
                    if snapshot_error.is_none() {
                        snapshot_error = Some(error);
                    }
                    continue;
                }
            };

            start_revision = snap.revision;
            recovered_state.revision = snap.revision;
            recovered_state.committed_operations = snap.committed_operations.into_iter().collect();
            catalog.id_allocator.observe(snap.high_water_mark);

            for db in snap.databases {
                catalog.id_allocator.observe(db.id.0);
                recovered_state.databases.insert(db.id, db);
            }
            for ns in snap.namespaces {
                catalog.id_allocator.observe(ns.id.0);
                recovered_state.namespaces.insert(ns.id, ns);
            }
            for tbl in snap.tables {
                catalog.id_allocator.observe(tbl.id.0);
                recovered_state
                    .tables_by_name
                    .insert(tbl.name.clone(), tbl.id);
                recovered_state.tables.insert(tbl.id, tbl);
            }
            for view in snap.views {
                catalog.id_allocator.observe(view.id.0);
                recovered_state
                    .views_by_name
                    .insert(view.name.clone(), view.id);
                recovered_state.views.insert(view.id, view);
            }
            for iv in snap.inline_views {
                catalog.id_allocator.observe(iv.id.0);
                recovered_state.inline_views.insert(iv.id, iv);
            }
            recovered_state.view_dependencies = snap.view_dependencies;
            for idx in snap.indexes {
                catalog.id_allocator.observe(idx.id.0);
                recovered_state.indexes.insert(idx.id, idx);
            }
            for entry in snap.workload_entries {
                catalog.id_allocator.observe(entry.id.0);
                recovered_state.workloads.insert(entry.id, entry.definition);
            }
            for src in snap.sources {
                catalog.id_allocator.observe(src.id.0);
                recovered_state.sources.insert(src.id, src);
            }
            for sink in snap.sinks {
                catalog.id_allocator.observe(sink.id.0);
                recovered_state.sinks.insert(sink.id, sink);
            }
            for role in snap.roles {
                catalog.id_allocator.observe(role.id.0);
                recovered_state.roles.insert(role.id, role);
            }
            for principal in snap.security_principals {
                catalog.id_allocator.observe(principal.id.0);
                recovered_state
                    .security_principals
                    .insert(principal.id, principal);
            }
            recovered_state.security_audit_records = snap.security_audit_records;
            for plan in snap.compiled_plans {
                catalog.id_allocator.observe(plan.id.0);
                recovered_state.compiled_plans.insert(plan.id, plan);
            }
            break;
        }

        recovered_state
            .security_audit_records
            .retain(|record| record.timestamp_ms >= audit_cutoff_ms);
        rebuild_security_audit_index(&mut recovered_state)?;

        // The log listing must finish successfully before replay can publish
        // anything. A failed page is not proof that no later files exist.
        let log_prefix = format!("{}/catalog/log/", prefix);
        let log_prefix_path = ObjectPath::from(log_prefix.trim_start_matches('/'));

        let mut log_files: Vec<(u64, u64, ObjectPath)> = Vec::new();
        let mut log_stream = store.list(Some(&log_prefix_path));
        while let Some(item) = log_stream.next().await {
            let meta =
                item.map_err(|e| CatalogError::Storage(format!("[RS-0003] log list error: {e}")))?;
            let location = meta.location;
            let filename = location.as_ref().split('/').next_back().unwrap_or_default();
            if filename.ends_with(".log") {
                let parts: Vec<&str> = filename.trim_end_matches(".log").split('_').collect();
                if parts.len() == 2 {
                    if let (Ok(rev), Ok(op_id)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                    {
                        if rev > start_revision {
                            log_files.push((rev, op_id, location));
                        }
                    }
                }
            }
        }

        log_files.sort_by_key(|(rev, op_id, _)| (*rev, *op_id));

        let mut total_buffer_bytes = 0;
        let mut expected_revision = start_revision;
        for (rev, op_id, path) in log_files {
            if rev != expected_revision.saturating_add(1) {
                return Err(CatalogError::DecodeError(format!(
                    "[RS-1003] incomplete catalog log history: expected revision {}, found {}; next_steps: inspect catalog log files",
                    expected_revision.saturating_add(1),
                    rev
                )));
            }

            let res = store.get(&path).await.map_err(|e| {
                CatalogError::Storage(format!(
                    "[RS-0003] failed to read log file {}: {e}",
                    path.as_ref()
                ))
            })?;
            let bytes = res.bytes().await.map_err(|e| {
                CatalogError::Storage(format!("[RS-0003] failed to load log bytes: {e}"))
            })?;

            total_buffer_bytes += bytes.len();
            if total_buffer_bytes > MAX_REPLAY_BUFFER_BYTES {
                return Err(CatalogError::ReplayBufferExceeded(format!(
                    "[RS-1003] uncommitted txn replay buffer exceeded maximum size of {} bytes; next_steps: compact catalog log files",
                    MAX_REPLAY_BUFFER_BYTES
                )));
            }

            // A bad record must not turn a complete listing into a partial
            // catalog. Preserve the original error and every stored object.
            let txn = CatalogTxn::decode(&bytes)?;
            if txn.revision != rev || txn.operation_id != op_id {
                return Err(CatalogError::DecodeError(format!(
                    "[RS-1003] catalog log {} has mismatched revision or operation ID; next_steps: inspect catalog log files",
                    path.as_ref()
                )));
            }
            expected_revision = rev;

            if recovered_state
                .committed_operations
                .contains(&txn.operation_id)
            {
                continue;
            }

            validate_security_mutations(&recovered_state, &txn)?;

            for mutation in &txn.mutations {
                apply_mutation_to_state(&mut recovered_state, &catalog.id_allocator, mutation)?;
            }
            prune_expired_security_audit_records(&mut recovered_state, audit_cutoff_ms)?;
            recovered_state
                .committed_operations
                .insert(txn.operation_id);
            recovered_state.revision = txn.revision;
        }

        // An unreadable newer snapshot cannot be ignored if the remaining
        // history stops short of its revision (including a corrupt revision 0).
        if let Some(error) = snapshot_error {
            if latest_snapshot_revision.is_some_and(|rev| recovered_state.revision < rev)
                || (start_revision == 0 && recovered_state.revision == 0)
            {
                return Err(error);
            }
        }

        *catalog.state.write().await = recovered_state;
        {
            let mut state = catalog.state.write().await;
            prune_expired_security_audit_records(&mut state, audit_cutoff_ms)?;
            ensure_security_audit_budget_bytes(
                state.security_audit_bytes,
                MAX_SECURITY_AUDIT_BYTES,
            )?;
        }
        Ok(catalog)
    }

    /// Preprovision the first administrator from trusted offline deployment tooling.
    ///
    /// Call this before opening application listeners. The operation ID must be
    /// unique for the catalog and stable across retries.
    pub async fn preprovision_first_administrator(
        &self,
        principal: SecurityPrincipal,
        operation_id: u64,
    ) -> Result<u64, CatalogError> {
        let revision = self.get_revision().await.checked_add(1).ok_or_else(|| {
            invalid_security_policy("catalog revision is exhausted during administrator bootstrap")
        })?;
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| CatalogError::Internal(format!("system clock error: {error}")))?
            .as_millis()
            .try_into()
            .map_err(|_| {
                CatalogError::Internal("system clock exceeds audit timestamp range".into())
            })?;
        let audit = SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: format!("first-admin-bootstrap:{operation_id}"),
            timestamp_ms,
            actor: AuditIdentity::Unknown,
            target: AuditField::Known(format!("principal:{}", principal.id.0)),
            scope: AuditField::Known("cluster".into()),
            action: SecurityAuditClass::GrantChange,
            outcome: SecurityAuditOutcome::Success,
            correlation: AuditCorrelation::NotApplicable,
            operation_id: AuditField::Known(operation_id.to_string()),
            policy_revision: AuditField::Known(principal.revision),
            before_state: AuditField::NotApplicable,
            after_state: AuditField::Known(principal_audit_state(&principal)),
        };
        let txn = CatalogTxn::new(
            revision,
            operation_id,
            vec![CatalogMutation::PutSecurityPrincipal {
                principal: principal.clone(),
                expected_revision: None,
                grantor_revision: None,
                audit: audit.clone(),
            }],
        )?;
        self.commit_txn(txn).await?;

        let persisted_principal = self.get_security_principal(principal.id).await?;
        let audit_committed =
            self.list_security_audit_records()
                .await?
                .into_iter()
                .any(|mut record| {
                    record.timestamp_ms = audit.timestamp_ms;
                    record == audit
                });
        if persisted_principal.as_ref() != Some(&principal) || !audit_committed {
            return Err(invalid_security_policy(
                "bootstrap operation ID is already committed to a different catalog change",
            ));
        }
        Ok(self.get_revision().await)
    }
}

fn current_time_ms() -> Result<u64, CatalogError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| CatalogError::Internal(format!("system clock error: {error}")))?
        .as_millis()
        .try_into()
        .map_err(|_| CatalogError::Internal("system clock exceeds audit timestamp range".into()))
}

fn security_audit_cutoff_ms(now_ms: u64) -> u64 {
    now_ms.saturating_sub(SECURITY_AUDIT_RETENTION_MS)
}

#[derive(Default)]
struct JsonByteCounter(u64);

impl Write for JsonByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len() as u64);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encoded_security_audit_bytes<'a>(
    records: impl IntoIterator<Item = &'a SecurityAuditRecord>,
) -> Result<u64, CatalogError> {
    let mut total = 2u64; // JSON array delimiters.
    let mut has_record = false;
    for record in records {
        if has_record {
            total = total.saturating_add(1); // JSON array separator.
        }
        total = total.saturating_add(encoded_security_audit_record_bytes(record)?);
        has_record = true;
    }
    Ok(total)
}

fn encoded_security_audit_record_bytes(record: &SecurityAuditRecord) -> Result<u64, CatalogError> {
    let mut counter = JsonByteCounter::default();
    serde_json::to_writer(&mut counter, record).map_err(|error| {
        CatalogError::Internal(format!("failed to size security audit record: {error}"))
    })?;
    Ok(counter.0)
}

fn ensure_security_audit_budget_bytes(bytes: u64, max_bytes: u64) -> Result<(), CatalogError> {
    if bytes > max_bytes {
        return Err(CatalogError::Storage(
            "security audit retention budget is full (1 GiB); required security operation was not committed".into(),
        ));
    }
    Ok(())
}

fn rebuild_security_audit_index(state: &mut CatalogInnerState) -> Result<(), CatalogError> {
    state.security_audit_bytes = encoded_security_audit_bytes(&state.security_audit_records)?;
    state.security_audit_oldest_timestamp_ms = state
        .security_audit_records
        .iter()
        .map(|record| record.timestamp_ms)
        .min();
    Ok(())
}

fn prune_expired_security_audit_records(
    state: &mut CatalogInnerState,
    cutoff_ms: u64,
) -> Result<(), CatalogError> {
    if state
        .security_audit_oldest_timestamp_ms
        .is_some_and(|timestamp_ms| timestamp_ms < cutoff_ms)
    {
        state
            .security_audit_records
            .retain(|record| record.timestamp_ms >= cutoff_ms);
        rebuild_security_audit_index(state)?;
    }
    Ok(())
}

fn append_security_audit_record_to_state(
    state: &mut CatalogInnerState,
    record: &SecurityAuditRecord,
) -> Result<(), CatalogError> {
    let separator_bytes = if state.security_audit_records.is_empty() {
        2
    } else {
        1
    };
    state.security_audit_bytes = state
        .security_audit_bytes
        .saturating_add(separator_bytes)
        .saturating_add(encoded_security_audit_record_bytes(record)?);
    state.security_audit_oldest_timestamp_ms = Some(
        state
            .security_audit_oldest_timestamp_ms
            .map_or(record.timestamp_ms, |oldest| {
                oldest.min(record.timestamp_ms)
            }),
    );
    state.security_audit_records.push(record.clone());
    Ok(())
}

fn security_audit_record(mutation: &CatalogMutation) -> Option<&SecurityAuditRecord> {
    match mutation {
        CatalogMutation::PutSecurityPrincipal { audit, .. }
        | CatalogMutation::DeleteSecurityPrincipal { audit, .. } => Some(audit),
        CatalogMutation::AppendSecurityAudit(record) => Some(record),
        _ => None,
    }
}

fn ensure_txn_security_audit_budget(
    state: &CatalogInnerState,
    txn: &CatalogTxn,
    cutoff_ms: u64,
    max_bytes: u64,
) -> Result<(), CatalogError> {
    let new_records = txn
        .mutations
        .iter()
        .filter_map(security_audit_record)
        .collect::<Vec<_>>();
    if new_records
        .iter()
        .any(|record| record.timestamp_ms < cutoff_ms)
    {
        return Err(invalid_security_policy(
            "new security audit record is outside the 90-day retention window",
        ));
    }

    let has_expired_records = state
        .security_audit_oldest_timestamp_ms
        .is_some_and(|timestamp_ms| timestamp_ms < cutoff_ms);
    let (mut bytes, mut count) = if has_expired_records {
        let records = state
            .security_audit_records
            .iter()
            .filter(|record| record.timestamp_ms >= cutoff_ms);
        (
            encoded_security_audit_bytes(records.clone())?.saturating_sub(2),
            records.count() as u64,
        )
    } else {
        let count = state.security_audit_records.len() as u64;
        (
            if count == 0 {
                0
            } else {
                state.security_audit_bytes.saturating_sub(2)
            },
            count,
        )
    };
    for record in new_records {
        if count == 0 {
            bytes = bytes.saturating_add(2);
        } else {
            bytes = bytes.saturating_add(1);
        }
        bytes = bytes.saturating_add(encoded_security_audit_record_bytes(record)?);
        count = count.saturating_add(1);
    }
    ensure_security_audit_budget_bytes(bytes, max_bytes)
}

fn validate_security_mutations(
    state: &CatalogInnerState,
    txn: &CatalogTxn,
) -> Result<(), CatalogError> {
    let mut principals = state.security_principals.clone();
    let mut changed_principals = HashSet::new();
    let mut audit_ids: HashSet<&str> = state
        .security_audit_records
        .iter()
        .map(|record| record.event_id.as_str())
        .collect();
    let mut has_principal_change = false;
    let mut has_bootstrap = false;

    for mutation in &txn.mutations {
        match mutation {
            CatalogMutation::PutSecurityPrincipal {
                principal,
                expected_revision,
                grantor_revision,
                audit,
            } => {
                has_principal_change = true;
                let is_first_administrator =
                    state.security_principals.is_empty() && expected_revision.is_none();
                principal.validate().map_err(|error| {
                    CatalogError::InvalidSecurityPolicy(format!(
                        "invalid security principal: {error}"
                    ))
                })?;
                if !changed_principals.insert(principal.id) {
                    return Err(invalid_security_policy(
                        "transaction changes the same principal more than once",
                    ));
                }

                let old = state.security_principals.get(&principal.id);
                match (old, expected_revision) {
                    (None, None) => {
                        if state.security_principals.is_empty() {
                            if has_bootstrap
                                || !is_active_administrator(principal)
                                || !principal.has_interactive_login()
                                || grantor_revision.is_some()
                                || principal.revision != 1
                            {
                                return Err(invalid_security_policy(
                                    "first-administrator preprovisioning requires an empty security catalog and one enabled interactive administrator at revision 1",
                                ));
                            }
                            has_bootstrap = true;
                        } else {
                            if principal.revision != 1 {
                                return Err(invalid_security_policy(
                                    "new principal revision must start at one",
                                ));
                            }
                            validate_grantor(state, *grantor_revision, audit)?;
                        }
                    }
                    (Some(old), Some(expected)) => {
                        if old.revision != *expected {
                            return Err(security_revision_conflict(
                                principal.id,
                                *expected,
                                old.revision,
                            ));
                        }
                        if principal.revision != expected.saturating_add(1) {
                            return Err(invalid_security_policy(
                                "updated principal revision must increment by exactly one",
                            ));
                        }
                        validate_grantor(state, *grantor_revision, audit)?;
                    }
                    (Some(old), None) => {
                        return Err(security_revision_conflict(principal.id, 0, old.revision));
                    }
                    (None, Some(expected)) => {
                        return Err(security_revision_conflict(principal.id, *expected, 0));
                    }
                }

                validate_grant_audit(
                    audit,
                    txn.operation_id,
                    principal.id,
                    principal.revision,
                    old.map(principal_audit_state),
                    Some(principal_audit_state(principal)),
                    is_first_administrator,
                )?;
                if !audit_ids.insert(audit.event_id.as_str()) {
                    return Err(invalid_security_policy(
                        "audit event identity is already present",
                    ));
                }
                principals.insert(principal.id, principal.clone());
            }
            CatalogMutation::DeleteSecurityPrincipal {
                principal_id,
                expected_revision,
                grantor_revision,
                audit,
            } => {
                has_principal_change = true;
                if !changed_principals.insert(*principal_id) {
                    return Err(invalid_security_policy(
                        "transaction changes the same principal more than once",
                    ));
                }
                let old = state.security_principals.get(principal_id).ok_or_else(|| {
                    security_revision_conflict(*principal_id, *expected_revision, 0)
                })?;
                if old.revision != *expected_revision {
                    return Err(security_revision_conflict(
                        *principal_id,
                        *expected_revision,
                        old.revision,
                    ));
                }
                validate_grantor(state, *grantor_revision, audit)?;
                validate_grant_audit(
                    audit,
                    txn.operation_id,
                    *principal_id,
                    expected_revision.saturating_add(1),
                    Some(principal_audit_state(old)),
                    None,
                    false,
                )?;
                if !audit_ids.insert(audit.event_id.as_str()) {
                    return Err(invalid_security_policy(
                        "audit event identity is already present",
                    ));
                }
                principals.remove(principal_id);
            }
            CatalogMutation::AppendSecurityAudit(record) => {
                record.validate().map_err(|error| {
                    CatalogError::InvalidSecurityPolicy(format!(
                        "invalid security audit record: {error}"
                    ))
                })?;
                if !audit_ids.insert(record.event_id.as_str()) {
                    return Err(invalid_security_policy(
                        "audit event identity is already present",
                    ));
                }
            }
            _ => {}
        }
    }

    SecurityPrincipal::validate_unique_login_identities(principals.values()).map_err(|error| {
        invalid_security_policy(&format!("invalid security login mapping: {error}"))
    })?;

    if has_principal_change && !principals.values().any(is_active_administrator) {
        return Err(invalid_security_policy(
            "principal mutation would remove or disable the last active administrator",
        ));
    }
    Ok(())
}

fn invalid_security_policy(message: &str) -> CatalogError {
    CatalogError::InvalidSecurityPolicy(message.to_string())
}

fn security_revision_conflict(
    principal_id: PrincipalId,
    expected: u64,
    actual: u64,
) -> CatalogError {
    CatalogError::OptimisticConflict(format!(
        "[RS-2008] transaction.optimistic_conflict: principal {} revision was {}, expected {}; retry the transaction against current policy",
        principal_id.0, actual, expected
    ))
}

fn validate_grantor(
    state: &CatalogInnerState,
    grantor_revision: Option<(PrincipalId, u64)>,
    audit: &SecurityAuditRecord,
) -> Result<(), CatalogError> {
    let (grantor_id, expected_revision) = grantor_revision.ok_or_else(|| {
        invalid_security_policy("principal changes require an authorized grantor")
    })?;
    let grantor = state
        .security_principals
        .get(&grantor_id)
        .ok_or_else(|| invalid_security_policy("grantor principal does not exist"))?;
    if grantor.revision != expected_revision {
        return Err(security_revision_conflict(
            grantor_id,
            expected_revision,
            grantor.revision,
        ));
    }
    if !is_active_administrator(grantor) {
        return Err(invalid_security_policy(
            "grantor lacks current principal and grant management authority",
        ));
    }
    if audit.actor != AuditIdentity::Verified(format!("principal:{}", grantor_id.0)) {
        return Err(invalid_security_policy(
            "grant audit actor does not match the current grantor",
        ));
    }
    Ok(())
}

fn is_active_administrator(principal: &SecurityPrincipal) -> bool {
    principal.enabled
        && principal.validate().is_ok()
        && principal.has_interactive_login()
        && principal.preset_grants.iter().any(|grant| {
            grant.preset == PermissionPreset::Administrator && grant.scope == PolicyScope::Cluster
        })
}

fn principal_audit_state(principal: &SecurityPrincipal) -> String {
    let policy = serde_json::to_string(&(
        principal.enabled,
        &principal.preset_grants,
        &principal.scoped_grants,
    ))
    .expect("serializing policy fields cannot fail");
    format!("revision={};policy={policy}", principal.revision)
}

fn validate_grant_audit(
    audit: &SecurityAuditRecord,
    operation_id: u64,
    principal_id: PrincipalId,
    policy_revision: u64,
    before_state: Option<String>,
    after_state: Option<String>,
    allow_unknown_actor: bool,
) -> Result<(), CatalogError> {
    audit.validate().map_err(|error| {
        CatalogError::InvalidSecurityPolicy(format!("invalid grant audit record: {error}"))
    })?;
    let target = format!("principal:{}", principal_id.0);
    let actor_is_valid = if allow_unknown_actor {
        matches!(&audit.actor, AuditIdentity::Unknown)
    } else {
        matches!(&audit.actor, AuditIdentity::Verified(_))
    };
    if audit.action != SecurityAuditClass::GrantChange
        || audit.outcome != SecurityAuditOutcome::Success
        || !matches!(&audit.target, AuditField::Known(value) if value == &target)
        || audit.scope != AuditField::Known("cluster".to_string())
        || audit.operation_id != AuditField::Known(operation_id.to_string())
        || audit.policy_revision != AuditField::Known(policy_revision)
        || audit.before_state != before_state.map_or(AuditField::NotApplicable, AuditField::Known)
        || audit.after_state != after_state.map_or(AuditField::NotApplicable, AuditField::Known)
        || !actor_is_valid
    {
        return Err(invalid_security_policy(
            "grant audit must record the permitted actor identity, target, operation, policy revision, and exact before/after state",
        ));
    }
    Ok(())
}

fn apply_mutation_to_state(
    state: &mut CatalogInnerState,
    allocator: &IdAllocator,
    mutation: &CatalogMutation,
) -> Result<(), CatalogError> {
    match mutation {
        CatalogMutation::PutDatabase(db) => {
            allocator.observe(db.id.0);
            state.databases.insert(db.id, db.clone());
        }
        CatalogMutation::PutNamespace(ns) => {
            allocator.observe(ns.id.0);
            state.namespaces.insert(ns.id, ns.clone());
        }
        CatalogMutation::PutTable(tbl) => {
            allocator.observe(tbl.id.0);
            state.tables_by_name.insert(tbl.name.clone(), tbl.id);
            state.tables.insert(tbl.id, tbl.clone());
        }
        CatalogMutation::DeleteTable(id) => {
            if let Some(tbl) = state.tables.remove(id) {
                state.tables_by_name.remove(&tbl.name);
            }
        }
        CatalogMutation::PutView(view) => {
            allocator.observe(view.id.0);
            state.views_by_name.insert(view.name.clone(), view.id);
            state.views.insert(view.id, view.clone());
        }
        CatalogMutation::DeleteView(id) => {
            if let Some(v) = state.views.remove(id) {
                state.views_by_name.remove(&v.name);
            }
        }
        CatalogMutation::PutInlineView(iv) => {
            allocator.observe(iv.id.0);
            state.inline_views.insert(iv.id, iv.clone());
        }
        CatalogMutation::DeleteInlineView(id) => {
            state.inline_views.remove(id);
        }
        CatalogMutation::PutViewDependency(dep) => {
            state
                .view_dependencies
                .retain(|d| !(d.parent_id == dep.parent_id && d.child_id == dep.child_id));
            state.view_dependencies.push(dep.clone());
        }
        CatalogMutation::DeleteViewDependency {
            parent_id,
            child_id,
        } => {
            state
                .view_dependencies
                .retain(|d| !(d.parent_id == *parent_id && d.child_id == *child_id));
        }
        CatalogMutation::PutIndex(idx) => {
            allocator.observe(idx.id.0);
            state.indexes.insert(idx.id, idx.clone());
        }
        CatalogMutation::DeleteIndex(id) => {
            state.indexes.remove(id);
        }
        CatalogMutation::PutWorkload(wl) => {
            let existing_id = state
                .workloads
                .iter()
                .find(|(_, w)| w.name == wl.name)
                .map(|(id, _)| *id);
            let wid = existing_id.unwrap_or_else(|| WorkloadId(allocator.allocate()));
            state.workloads.insert(wid, wl.clone());
        }
        CatalogMutation::PutWorkloadWithId(entry) => {
            allocator.observe(entry.id.0);
            state.workloads.insert(entry.id, entry.definition.clone());
        }
        CatalogMutation::DeleteWorkload(id) => {
            state.workloads.remove(id);
        }
        CatalogMutation::PutSource(src) => {
            allocator.observe(src.id.0);
            state.sources.insert(src.id, src.clone());
        }
        CatalogMutation::DeleteSource(id) => {
            state.sources.remove(id);
        }
        CatalogMutation::PutSink(sink) => {
            allocator.observe(sink.id.0);
            state.sinks.insert(sink.id, sink.clone());
        }
        CatalogMutation::DeleteSink(id) => {
            state.sinks.remove(id);
        }
        CatalogMutation::PutRole(r) => {
            allocator.observe(r.id.0);
            state.roles.insert(r.id, r.clone());
        }
        CatalogMutation::DeleteRole(id) => {
            state.roles.remove(id);
        }
        CatalogMutation::PutSecurityPrincipal {
            principal, audit, ..
        } => {
            allocator.observe(principal.id.0);
            state
                .security_principals
                .insert(principal.id, principal.clone());
            append_security_audit_record_to_state(state, audit)?;
        }
        CatalogMutation::DeleteSecurityPrincipal {
            principal_id,
            audit,
            ..
        } => {
            state.security_principals.remove(principal_id);
            append_security_audit_record_to_state(state, audit)?;
        }
        CatalogMutation::AppendSecurityAudit(record) => {
            append_security_audit_record_to_state(state, record)?;
        }
        CatalogMutation::PutCompiledPlan(p) => {
            allocator.observe(p.id.0);
            state.compiled_plans.insert(p.id, p.clone());
        }
        CatalogMutation::DeleteCompiledPlan(id) => {
            state.compiled_plans.remove(id);
        }
    }
    Ok(())
}

#[async_trait]
impl CatalogStore for DurableCatalogStore {
    async fn commit_txn(&self, txn: CatalogTxn) -> Result<u64, CatalogError> {
        let mut state = self.state.write().await;

        // 1. Replay deduplication: if operation_id already committed, return current revision as idempotent no-op
        if state.committed_operations.contains(&txn.operation_id) {
            return Ok(state.revision);
        }

        validate_security_mutations(&state, &txn)?;
        let audit_cutoff_ms = if txn
            .mutations
            .iter()
            .any(|mutation| security_audit_record(mutation).is_some())
        {
            let cutoff_ms = security_audit_cutoff_ms(current_time_ms()?);
            ensure_txn_security_audit_budget(&state, &txn, cutoff_ms, MAX_SECURITY_AUDIT_BYTES)?;
            cutoff_ms
        } else {
            0
        };

        // 2. Pre-commit validation
        // 2a. Cycle detection in view dependencies
        let mut sim_deps = state.view_dependencies.clone();
        for m in &txn.mutations {
            if let CatalogMutation::PutViewDependency(dep) = m {
                sim_deps.retain(|d| !(d.parent_id == dep.parent_id && d.child_id == dep.child_id));
                sim_deps.push(dep.clone());
            }
        }
        if check_dependency_cycle(&sim_deps) {
            return Err(CatalogError::DependencyCycle(
                "[RS-1002] CycleDetected: view dependency cycle detected; next_steps: remove circular view dependencies".to_string(),
            ));
        }

        // 2b. Drop integrity: reject drop if dependent views exist
        for m in &txn.mutations {
            match m {
                CatalogMutation::DeleteTable(tbl_id) => {
                    let has_dependent_view = sim_deps.iter().any(|d| {
                        d.child_id == tbl_id.0 && matches!(d.dependency_kind, DependencyKind::Table)
                    });
                    if has_dependent_view {
                        return Err(CatalogError::ReferencedObjectExists(format!(
                            "[RS-1002] cannot drop table {}: dependent views exist; next_steps: drop dependent views first",
                            tbl_id
                        )));
                    }
                }
                CatalogMutation::DeleteView(view_id) => {
                    let has_dependent_view = sim_deps.iter().any(|d| {
                        d.child_id == view_id.0 && matches!(d.dependency_kind, DependencyKind::View)
                    });
                    if has_dependent_view {
                        return Err(CatalogError::ReferencedObjectExists(format!(
                            "[RS-1002] cannot drop view {}: dependent views exist; next_steps: drop dependent views first",
                            view_id
                        )));
                    }
                }
                _ => {}
            }
        }

        // 2c. Referenced object existence: referenced parent/child objects must exist
        for m in &txn.mutations {
            if let CatalogMutation::PutViewDependency(dep) = m {
                let parent_exists = state.views.contains_key(&ViewId(dep.parent_id))
                    || txn.mutations.iter().any(
                        |m| matches!(m, CatalogMutation::PutView(v) if v.id.0 == dep.parent_id),
                    );
                if !parent_exists {
                    return Err(CatalogError::ObjectNotFound(format!(
                        "[RS-1004] referenced parent view {} not found; next_steps: create parent view first",
                        dep.parent_id
                    )));
                }
                let child_exists = match dep.dependency_kind {
                    DependencyKind::Table => state.tables.contains_key(&TableId(dep.child_id))
                        || txn.mutations.iter().any(
                            |m| matches!(m, CatalogMutation::PutTable(t) if t.id.0 == dep.child_id),
                        ),
                    DependencyKind::View => state.views.contains_key(&ViewId(dep.child_id))
                        || txn.mutations.iter().any(
                            |m| matches!(m, CatalogMutation::PutView(v) if v.id.0 == dep.child_id),
                        ),
                };
                if !child_exists {
                    return Err(CatalogError::ObjectNotFound(format!(
                        "[RS-1004] referenced dependency object {} not found; next_steps: create prerequisite table or view first",
                        dep.child_id
                    )));
                }
            }
        }

        // Resolve implicit workload identities before writing the transaction log.
        // Replay must use recorded IDs, never allocate according to replay order.
        let mut resolved = txn.mutations.clone();
        let mut workload_ids: HashMap<String, WorkloadId> = state
            .workloads
            .iter()
            .map(|(&id, def)| (def.name.clone(), id))
            .collect();
        for mutation in &mut resolved {
            match mutation {
                CatalogMutation::PutWorkload(definition) => {
                    let id = *workload_ids
                        .entry(definition.name.clone())
                        .or_insert_with(|| WorkloadId(self.id_allocator.allocate()));
                    *mutation = CatalogMutation::PutWorkloadWithId(CatalogWorkloadEntry {
                        id,
                        definition: definition.clone(),
                    });
                }
                CatalogMutation::PutWorkloadWithId(entry) => {
                    workload_ids.insert(entry.definition.name.clone(), entry.id);
                }
                CatalogMutation::DeleteWorkload(id) => {
                    workload_ids.retain(|_, existing| existing != id);
                }
                _ => {}
            }
        }
        let txn = CatalogTxn::new(txn.revision, txn.operation_id, resolved)?;

        // 3. Durably persist transaction log
        let bytes = txn.encode()?;
        let path = self.log_path(txn.revision, txn.operation_id);
        self.store.put(&path, bytes.into()).await.map_err(|e| {
            CatalogError::Storage(format!("[RS-0003] failed to write catalog log: {e}"))
        })?;

        // 4. Apply all mutations to in-memory state
        if audit_cutoff_ms > 0 {
            prune_expired_security_audit_records(&mut state, audit_cutoff_ms)?;
        }
        for m in &txn.mutations {
            apply_mutation_to_state(&mut state, &self.id_allocator, m)?;
        }

        state.committed_operations.insert(txn.operation_id);
        state.revision = txn.revision;

        Ok(state.revision)
    }

    async fn get_revision(&self) -> u64 {
        self.state.read().await.revision
    }

    async fn allocate_id(&self) -> Result<u64, CatalogError> {
        Ok(self.id_allocator.allocate())
    }

    async fn get_database(&self, id: DatabaseId) -> Result<Option<CatalogDatabase>, CatalogError> {
        Ok(self.state.read().await.databases.get(&id).cloned())
    }

    async fn get_namespace(
        &self,
        id: NamespaceId,
    ) -> Result<Option<CatalogNamespace>, CatalogError> {
        Ok(self.state.read().await.namespaces.get(&id).cloned())
    }

    async fn get_table(&self, id: TableId) -> Result<Option<CatalogTable>, CatalogError> {
        Ok(self.state.read().await.tables.get(&id).cloned())
    }

    async fn get_table_by_name(&self, name: &str) -> Result<Option<CatalogTable>, CatalogError> {
        let state = self.state.read().await;
        if let Some(id) = state.tables_by_name.get(name) {
            Ok(state.tables.get(id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn list_tables(&self) -> Result<Vec<CatalogTable>, CatalogError> {
        Ok(self.state.read().await.tables.values().cloned().collect())
    }

    async fn get_view(&self, id: ViewId) -> Result<Option<CatalogView>, CatalogError> {
        Ok(self.state.read().await.views.get(&id).cloned())
    }

    async fn get_view_by_name(&self, name: &str) -> Result<Option<CatalogView>, CatalogError> {
        let state = self.state.read().await;
        if let Some(id) = state.views_by_name.get(name) {
            Ok(state.views.get(id).cloned())
        } else {
            Ok(None)
        }
    }

    async fn list_views(&self) -> Result<Vec<CatalogView>, CatalogError> {
        Ok(self.state.read().await.views.values().cloned().collect())
    }

    async fn get_inline_view(&self, id: ViewId) -> Result<Option<CatalogInlineView>, CatalogError> {
        Ok(self.state.read().await.inline_views.get(&id).cloned())
    }

    async fn list_inline_views(&self) -> Result<Vec<CatalogInlineView>, CatalogError> {
        Ok(self
            .state
            .read()
            .await
            .inline_views
            .values()
            .cloned()
            .collect())
    }

    async fn list_view_dependencies(&self) -> Result<Vec<ViewDependency>, CatalogError> {
        Ok(self.state.read().await.view_dependencies.clone())
    }

    async fn get_index(&self, id: IndexId) -> Result<Option<CatalogIndexEntry>, CatalogError> {
        Ok(self.state.read().await.indexes.get(&id).cloned())
    }

    async fn list_indexes(&self) -> Result<Vec<CatalogIndexEntry>, CatalogError> {
        Ok(self.state.read().await.indexes.values().cloned().collect())
    }

    async fn get_workload(&self, id: WorkloadId) -> Result<Option<WorkloadDef>, CatalogError> {
        Ok(self.state.read().await.workloads.get(&id).cloned())
    }

    async fn list_workloads(&self) -> Result<Vec<WorkloadDef>, CatalogError> {
        Ok(self
            .state
            .read()
            .await
            .workloads
            .values()
            .cloned()
            .collect())
    }

    async fn get_source(&self, id: SourceId) -> Result<Option<CatalogSourceEntry>, CatalogError> {
        Ok(self.state.read().await.sources.get(&id).cloned())
    }

    async fn list_sources(&self) -> Result<Vec<CatalogSourceEntry>, CatalogError> {
        Ok(self.state.read().await.sources.values().cloned().collect())
    }

    async fn get_sink(&self, id: SinkId) -> Result<Option<CatalogSinkEntry>, CatalogError> {
        Ok(self.state.read().await.sinks.get(&id).cloned())
    }

    async fn list_sinks(&self) -> Result<Vec<CatalogSinkEntry>, CatalogError> {
        Ok(self.state.read().await.sinks.values().cloned().collect())
    }

    async fn get_role(&self, id: PrincipalId) -> Result<Option<CatalogRoleEntry>, CatalogError> {
        Ok(self.state.read().await.roles.get(&id).cloned())
    }

    async fn list_roles(&self) -> Result<Vec<CatalogRoleEntry>, CatalogError> {
        Ok(self.state.read().await.roles.values().cloned().collect())
    }

    async fn get_security_principal(
        &self,
        id: PrincipalId,
    ) -> Result<Option<SecurityPrincipal>, CatalogError> {
        Ok(self
            .state
            .read()
            .await
            .security_principals
            .get(&id)
            .cloned())
    }

    async fn list_security_principals(&self) -> Result<Vec<SecurityPrincipal>, CatalogError> {
        Ok(self
            .state
            .read()
            .await
            .security_principals
            .values()
            .cloned()
            .collect())
    }

    async fn list_security_audit_records(&self) -> Result<Vec<SecurityAuditRecord>, CatalogError> {
        let cutoff_ms = security_audit_cutoff_ms(current_time_ms()?);
        Ok(self
            .state
            .read()
            .await
            .security_audit_records
            .iter()
            .filter(|record| record.timestamp_ms >= cutoff_ms)
            .cloned()
            .collect())
    }

    async fn get_compiled_plan(
        &self,
        id: CompiledPlanId,
    ) -> Result<Option<CompiledPlanRecord>, CatalogError> {
        Ok(self.state.read().await.compiled_plans.get(&id).cloned())
    }

    async fn list_compiled_plans(&self) -> Result<Vec<CompiledPlanRecord>, CatalogError> {
        Ok(self
            .state
            .read()
            .await
            .compiled_plans
            .values()
            .cloned()
            .collect())
    }
}

/// Check if dependencies graph contains any cycle using DFS.
fn check_dependency_cycle(deps: &[ViewDependency]) -> bool {
    let mut adj: HashMap<u64, Vec<u64>> = HashMap::new();
    for d in deps {
        adj.entry(d.parent_id).or_default().push(d.child_id);
    }

    let mut visited: HashMap<u64, bool> = HashMap::new();
    let mut on_stack: HashMap<u64, bool> = HashMap::new();

    fn dfs(
        node: u64,
        adj: &HashMap<u64, Vec<u64>>,
        visited: &mut HashMap<u64, bool>,
        on_stack: &mut HashMap<u64, bool>,
    ) -> bool {
        visited.insert(node, true);
        on_stack.insert(node, true);

        if let Some(neighbors) = adj.get(&node) {
            for &next in neighbors {
                if !visited.get(&next).copied().unwrap_or(false) {
                    if dfs(next, adj, visited, on_stack) {
                        return true;
                    }
                } else if on_stack.get(&next).copied().unwrap_or(false) {
                    return true;
                }
            }
        }

        on_stack.insert(node, false);
        false
    }

    for &node in adj.keys() {
        if !visited.get(&node).copied().unwrap_or(false)
            && dfs(node, &adj, &mut visited, &mut on_stack)
        {
            return true;
        }
    }

    false
}

#[cfg(test)]
mod security_audit_budget_tests {
    use super::*;

    fn audit_record(event_id: &str, timestamp_ms: u64) -> SecurityAuditRecord {
        SecurityAuditRecord {
            format_version: SECURITY_AUDIT_FORMAT_VERSION,
            event_id: event_id.to_string(),
            timestamp_ms,
            actor: AuditIdentity::Unknown,
            target: AuditField::Unknown,
            scope: AuditField::NotApplicable,
            action: SecurityAuditClass::AuthenticationFailure,
            outcome: SecurityAuditOutcome::Failure,
            correlation: AuditCorrelation::NotApplicable,
            operation_id: AuditField::NotApplicable,
            policy_revision: AuditField::NotApplicable,
            before_state: AuditField::NotApplicable,
            after_state: AuditField::NotApplicable,
        }
    }

    #[test]
    fn retained_audit_budget_uses_exact_json_array_size_and_rejects_overflow() {
        let event = audit_record("event-1", 1);
        let encoded = encoded_security_audit_bytes([&event]).unwrap();
        assert_eq!(
            encoded,
            serde_json::to_vec(&[event.clone()]).unwrap().len() as u64
        );
        assert!(matches!(
            ensure_security_audit_budget_bytes(encoded, encoded),
            Ok(())
        ));
        assert!(matches!(
            ensure_security_audit_budget_bytes(encoded, encoded - 1),
            Err(CatalogError::Storage(message))
                if message == "security audit retention budget is full (1 GiB); required security operation was not committed"
        ));
    }

    #[test]
    fn audit_retention_excludes_expired_records_but_rejects_expired_new_events() {
        let now_ms = SECURITY_AUDIT_RETENTION_MS + 1_000;
        let cutoff_ms = security_audit_cutoff_ms(now_ms);
        let expired = audit_record("expired", cutoff_ms - 1);
        let current = audit_record("current", now_ms);
        let txn = CatalogTxn::new(
            1,
            1,
            vec![CatalogMutation::AppendSecurityAudit(current.clone())],
        )
        .unwrap();
        let current_size = encoded_security_audit_bytes([&current]).unwrap();
        let mut state = CatalogInnerState {
            security_audit_records: vec![expired.clone()],
            ..CatalogInnerState::default()
        };
        rebuild_security_audit_index(&mut state).unwrap();

        assert!(matches!(
            ensure_txn_security_audit_budget(&state, &txn, cutoff_ms, current_size),
            Ok(())
        ));

        let expired_txn = CatalogTxn::new(
            2,
            2,
            vec![CatalogMutation::AppendSecurityAudit(expired.clone())],
        )
        .unwrap();
        assert!(matches!(
            ensure_txn_security_audit_budget(
                &CatalogInnerState::default(),
                &expired_txn,
                cutoff_ms,
                MAX_SECURITY_AUDIT_BYTES
            ),
            Err(CatalogError::InvalidSecurityPolicy(message))
                if message == "new security audit record is outside the 90-day retention window"
        ));
    }

    #[tokio::test]
    async fn expired_audit_records_are_hidden_and_omitted_from_snapshots() {
        let now_ms = current_time_ms().unwrap();
        let cutoff_ms = security_audit_cutoff_ms(now_ms);
        let expired = audit_record("expired", cutoff_ms.saturating_sub(1).max(1));
        let current = audit_record("current", now_ms);
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let catalog = DurableCatalogStore::new(object_store.clone(), "audit-retention");
        {
            let mut state = catalog.state.write().await;
            state.security_audit_records = vec![expired, current.clone()];
            rebuild_security_audit_index(&mut state).unwrap();
        }

        assert_eq!(
            catalog.list_security_audit_records().await.unwrap(),
            vec![current.clone()]
        );
        let snapshot = catalog.create_snapshot().await.unwrap();
        assert_eq!(snapshot.security_audit_records, vec![current.clone()]);
        let recovered = DurableCatalogStore::recover(object_store, "audit-retention")
            .await
            .unwrap();
        assert_eq!(
            recovered.list_security_audit_records().await.unwrap(),
            vec![current]
        );
    }
}

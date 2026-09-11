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
use object_store::ObjectStore;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::identity::IdAllocator;
use super::log::MAX_REPLAY_BUFFER_BYTES;
use super::snapshot::CatalogSnapshot;
use super::txn::{CatalogMutation, CatalogTxn};
use super::{
    CatalogDatabase, CatalogError, CatalogIndexEntry, CatalogInlineView, CatalogNamespace,
    CatalogRoleEntry, CatalogSinkEntry, CatalogSourceEntry, CatalogTable, CatalogView,
    CompiledPlanRecord, DependencyKind, ViewDependency,
};
use rockstream_types::ids::{
    CompiledPlanId, DatabaseId, IndexId, NamespaceId, PrincipalId, SinkId, SourceId, TableId,
    ViewId, WorkloadId,
};
use rockstream_types::workload::WorkloadDef;

/// Abstract interface for catalog operations.
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
            state.workloads.values().cloned().collect(),
            state.sources.values().cloned().collect(),
            state.sinks.values().cloned().collect(),
            state.roles.values().cloned().collect(),
            state.compiled_plans.values().cloned().collect(),
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
    pub async fn recover(
        store: Arc<dyn ObjectStore>,
        prefix: impl Into<String>,
    ) -> Result<Self, CatalogError> {
        let prefix = prefix.into();
        let catalog = Self::new(Arc::clone(&store), prefix.clone());

        // 1. Find latest valid snapshot
        let snap_prefix = format!("{}/catalog/snapshots/", prefix);
        let snap_prefix_path = ObjectPath::from(snap_prefix.trim_start_matches('/'));

        let mut snapshots: Vec<(u64, ObjectPath)> = Vec::new();
        let mut list_stream = store.list(Some(&snap_prefix_path));
        while let Some(item) = list_stream.next().await {
            if let Ok(meta) = item {
                let location = meta.location;
                let filename = location.as_ref().split('/').next_back().unwrap_or_default();
                if filename.ends_with(".snap") && !filename.ends_with(".tmp") {
                    if let Ok(rev) = filename.trim_end_matches(".snap").parse::<u64>() {
                        snapshots.push((rev, location));
                    }
                }
            }
        }

        snapshots.sort_by_key(|(rev, _)| *rev);

        let mut recovered_state = CatalogInnerState::default();
        let mut start_revision = 0;

        // Try from latest snapshot backward until a valid one is found
        while let Some((_rev, path)) = snapshots.pop() {
            if let Ok(res) = store.get(&path).await {
                if let Ok(bytes) = res.bytes().await {
                    if let Ok(snap) = CatalogSnapshot::decode(&bytes) {
                        start_revision = snap.revision;
                        recovered_state.revision = snap.revision;
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
                        for (idx, wl) in snap.workloads.into_iter().enumerate() {
                            let wid = WorkloadId((idx + 1) as u64);
                            catalog.id_allocator.observe(wid.0);
                            recovered_state.workloads.insert(wid, wl);
                        }
                        for src in snap.sources {
                            catalog.id_allocator.observe(src.id.0);
                            recovered_state.sources.insert(src.id, src);
                        }
                        for sink in snap.sinks {
                            catalog.id_allocator.observe(sink.id.0);
                            recovered_state.sinks.insert(sink.id, sink);
                        }
                        for r in snap.roles {
                            catalog.id_allocator.observe(r.id.0);
                            recovered_state.roles.insert(r.id, r);
                        }
                        for plan in snap.compiled_plans {
                            catalog.id_allocator.observe(plan.id.0);
                            recovered_state.compiled_plans.insert(plan.id, plan);
                        }
                        break;
                    }
                }
            }
        }

        // 2. Scan and replay log entries with revision > start_revision
        let log_prefix = format!("{}/catalog/log/", prefix);
        let log_prefix_path = ObjectPath::from(log_prefix.trim_start_matches('/'));

        let mut log_files: Vec<(u64, u64, ObjectPath)> = Vec::new();
        let mut log_stream = store.list(Some(&log_prefix_path));
        while let Some(item) = log_stream.next().await {
            if let Ok(meta) = item {
                let location = meta.location;
                let filename = location.as_ref().split('/').next_back().unwrap_or_default();
                if filename.ends_with(".log") {
                    let parts: Vec<&str> = filename.trim_end_matches(".log").split('_').collect();
                    if parts.len() == 2 {
                        if let (Ok(rev), Ok(op_id)) =
                            (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                        {
                            if rev > start_revision {
                                log_files.push((rev, op_id, location));
                            }
                        }
                    }
                }
            }
        }

        log_files.sort_by_key(|(rev, op_id, _)| (*rev, *op_id));

        let mut total_buffer_bytes = 0;
        for (rev, _op_id, path) in log_files {
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

            let txn = match CatalogTxn::decode(&bytes) {
                Ok(t) => t,
                Err(e) => {
                    // Halt log replay cleanly on corruption
                    tracing::warn!("Catalog log corruption detected at revision {rev}: {e}");
                    break;
                }
            };

            // Deduplication
            if recovered_state
                .committed_operations
                .contains(&txn.operation_id)
            {
                continue;
            }

            // Apply mutations
            for mutation in &txn.mutations {
                apply_mutation_to_state(&mut recovered_state, &catalog.id_allocator, mutation);
            }
            recovered_state
                .committed_operations
                .insert(txn.operation_id);
            recovered_state.revision = txn.revision;
        }

        *catalog.state.write().await = recovered_state;
        Ok(catalog)
    }
}

fn apply_mutation_to_state(
    state: &mut CatalogInnerState,
    allocator: &IdAllocator,
    mutation: &CatalogMutation,
) {
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
        CatalogMutation::PutCompiledPlan(p) => {
            allocator.observe(p.id.0);
            state.compiled_plans.insert(p.id, p.clone());
        }
        CatalogMutation::DeleteCompiledPlan(id) => {
            state.compiled_plans.remove(id);
        }
    }
}

#[async_trait]
impl CatalogStore for DurableCatalogStore {
    async fn commit_txn(&self, txn: CatalogTxn) -> Result<u64, CatalogError> {
        let mut state = self.state.write().await;

        // 1. Replay deduplication: if operation_id already committed, return current revision as idempotent no-op
        if state.committed_operations.contains(&txn.operation_id) {
            return Ok(state.revision);
        }

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

        // 3. Durably persist transaction log
        let bytes = txn.encode()?;
        let path = self.log_path(txn.revision, txn.operation_id);
        self.store.put(&path, bytes.into()).await.map_err(|e| {
            CatalogError::Storage(format!("[RS-0003] failed to write catalog log: {e}"))
        })?;

        // 4. Apply all mutations to in-memory state
        for m in &txn.mutations {
            apply_mutation_to_state(&mut state, &self.id_allocator, m);
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

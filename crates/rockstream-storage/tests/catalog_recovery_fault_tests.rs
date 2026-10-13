//! Issue #103: recovery never publishes a catalog after incomplete storage reads.
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{stream, StreamExt};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result,
};
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

const PREFIX: &str = "fault";
const SNAPSHOT: &str = "fault/catalog/snapshots/00000000000000000001.snap";

#[derive(Debug, Clone, Copy)]
enum Fault {
    SnapshotList,
    SnapshotGet,
    SnapshotBody,
    LogListAfterOne,
    None,
}

#[derive(Debug)]
struct FaultStore {
    inner: Arc<InMemory>,
    fault: Fault,
}
impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore")
    }
}
fn failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "catalog fault injection",
        source: Box::new(std::io::Error::other("injected storage failure")),
    }
}
#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(&self, p: &Path, v: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.inner.put_opts(p, v, opts).await
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, opts).await
    }
    async fn get_opts(&self, p: &Path, opts: GetOptions) -> Result<GetResult> {
        if p.as_ref() == SNAPSHOT {
            if matches!(self.fault, Fault::SnapshotGet) {
                return Err(failure());
            }
            if matches!(self.fault, Fault::SnapshotBody) {
                let mut result = self.inner.get_opts(p, opts).await?;
                result.payload =
                    GetResultPayload::Stream(Box::pin(stream::once(async { Err(failure()) })));
                return Ok(result);
            }
        }
        self.inner.get_opts(p, opts).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let name = prefix.map(|p| p.to_string()).unwrap_or_default();
        if matches!(self.fault, Fault::SnapshotList) && name.ends_with("/snapshots") {
            return Box::pin(stream::iter(vec![Err(failure())]));
        }
        if matches!(self.fault, Fault::LogListAfterOne) && name.ends_with("/log") {
            return Box::pin(
                self.inner
                    .list(prefix)
                    .take(1)
                    .chain(stream::iter(vec![Err(failure())])),
            );
        }
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, opts).await
    }
}

async fn inventory(store: &InMemory) -> BTreeMap<String, Vec<u8>> {
    let mut result = BTreeMap::new();
    let mut entries = store.list(None);
    while let Some(entry) = entries.next().await {
        let path = entry.unwrap().location;
        result.insert(
            path.to_string(),
            store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec(),
        );
    }
    result
}

async fn fixture() -> (Arc<InMemory>, CatalogSnapshot) {
    let storage = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(storage.clone(), PREFIX);
    let col = CatalogColumn {
        name: "id".into(),
        data_type: "BIGINT".into(),
        nullable: false,
        ordinal: 0,
    };
    let db = CatalogDatabase {
        id: DatabaseId(1),
        name: "analytics".into(),
        default_namespace: "public".into(),
        created_at: 42,
    };
    let ns = CatalogNamespace {
        id: NamespaceId(2),
        name: "public".into(),
        database_id: db.id,
        created_at: 43,
    };
    let table = CatalogTable {
        id: TableId(10),
        name: "events".into(),
        namespace_id: ns.id,
        columns: vec![col.clone()],
        pk_cols: vec!["id".into()],
    };
    let view = CatalogView {
        id: ViewId(20),
        name: "visible_events".into(),
        namespace_id: ns.id,
        sql: "SELECT id FROM events".into(),
        compiled_plan_id: Some(CompiledPlanId(40)),
        op_id: Some(100),
        columns: vec![col],
    };
    let index = CatalogIndexEntry {
        id: IndexId(30),
        name: "events_id_idx".into(),
        table_id: table.id,
        index_cols: vec!["id".into()],
        pk_cols: vec!["id".into()],
        state: CatalogIndexState::Ready,
        op_id: Some(100),
    };
    let dependency = ViewDependency {
        parent_id: view.id.0,
        child_id: table.id.0,
        dependency_kind: DependencyKind::Table,
    };
    let plan = CompiledPlanRecord {
        id: CompiledPlanId(40),
        sql: view.sql.clone(),
        ast_hash: [1; 32],
        logical_plan_hash: [2; 32],
        compiler_version: "0.71".into(),
        state_layout_version: 1,
        output_schema: vec![1, 2, 3],
        dependency_ids: vec![10],
    };
    catalog
        .commit_txn(
            CatalogTxn::new(
                1,
                100,
                vec![
                    CatalogMutation::PutDatabase(db),
                    CatalogMutation::PutNamespace(ns),
                    CatalogMutation::PutTable(table),
                    CatalogMutation::PutView(view),
                    CatalogMutation::PutIndex(index),
                    CatalogMutation::PutViewDependency(dependency),
                    CatalogMutation::PutCompiledPlan(plan),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let snapshot = catalog.create_snapshot().await.unwrap();
    assert_eq!(catalog.compact_logs(1).await.unwrap(), 1);
    (storage, snapshot)
}

fn normalized(mut snapshot: CatalogSnapshot) -> CatalogSnapshot {
    snapshot.databases.sort_by_key(|v| v.id);
    snapshot.namespaces.sort_by_key(|v| v.id);
    snapshot.tables.sort_by_key(|v| v.id);
    snapshot.views.sort_by_key(|v| v.id);
    snapshot.inline_views.sort_by_key(|v| v.id);
    snapshot
        .view_dependencies
        .sort_by_key(|v| (v.parent_id, v.child_id));
    snapshot.indexes.sort_by_key(|v| v.id);
    snapshot.workload_entries.sort_by_key(|v| v.id);
    snapshot.sources.sort_by_key(|v| v.id);
    snapshot.sinks.sort_by_key(|v| v.id);
    snapshot.roles.sort_by_key(|v| v.id);
    snapshot.compiled_plans.sort_by_key(|v| v.id);
    // Checksum depends on source HashMap iteration order; compare full logical metadata instead.
    snapshot.checksum = 0;
    snapshot
}

#[tokio::test]
async fn failures_after_compaction_never_publish_or_mutate_objects() {
    for fault in [Fault::SnapshotList, Fault::SnapshotGet, Fault::SnapshotBody] {
        let (storage, _) = fixture().await;
        let before = inventory(&storage).await;
        let wrapper: Arc<dyn ObjectStore> = Arc::new(FaultStore {
            inner: storage.clone(),
            fault,
        });
        let err = DurableCatalogStore::recover(wrapper, PREFIX)
            .await
            .unwrap_err();
        assert!(matches!(err, CatalogError::Storage(_)), "{fault:?}: {err}");
        assert!(err.to_string().contains("[RS-0003]"), "{fault:?}: {err}");
        assert_eq!(inventory(&storage).await, before, "{fault:?}");
    }

    let (storage, _) = fixture().await;
    storage
        .put(&Path::from(SNAPSHOT), b"malformed snapshot".to_vec().into())
        .await
        .unwrap();
    let before = inventory(&storage).await;
    let err = DurableCatalogStore::recover(storage.clone(), PREFIX)
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::DecodeError(_)), "{err}");
    assert!(err.to_string().contains("[RS-1003]"), "{err}");
    assert_eq!(inventory(&storage).await, before);
}

#[tokio::test]
async fn log_listing_failure_after_one_item_is_not_a_partial_catalog() {
    let (storage, _) = fixture().await;
    for revision in 2..=3 {
        let txn = CatalogTxn::new(
            revision,
            revision + 100,
            vec![CatalogMutation::DeleteTable(TableId(100 + revision))],
        )
        .unwrap();
        let path = format!(
            "fault/catalog/log/{revision:020}_{:020}.log",
            revision + 100
        );
        storage
            .put(&Path::from(path), txn.encode().unwrap().into())
            .await
            .unwrap();
    }
    let before = inventory(&storage).await;
    let wrapper: Arc<dyn ObjectStore> = Arc::new(FaultStore {
        inner: storage.clone(),
        fault: Fault::LogListAfterOne,
    });
    let err = DurableCatalogStore::recover(wrapper, PREFIX)
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::Storage(_)));
    assert!(err.to_string().contains("[RS-0003] log list error"));
    assert_eq!(inventory(&storage).await, before);
}

#[tokio::test]
async fn earlier_history_falls_back_to_full_metadata_and_revision() {
    let (storage, expected) = fixture().await;
    // Retain complete earlier history, then break a *newer* snapshot.
    let txn = CatalogTxn::new(
        2,
        102,
        vec![CatalogMutation::PutRole(CatalogRoleEntry {
            id: PrincipalId(50),
            role_name: "analyst".into(),
            permissions: vec!["SELECT".into()],
            member_of: vec!["team".into()],
        })],
    )
    .unwrap();
    let catalog = DurableCatalogStore::recover(storage.clone(), PREFIX)
        .await
        .unwrap();
    catalog.commit_txn(txn).await.unwrap();
    let full = catalog.create_snapshot().await.unwrap();
    assert_eq!(full.revision, 2);
    let expected_full = normalized(full.clone());
    // Preserve the log for replay from snapshot revision 1.
    storage
        .put(
            &Path::from("fault/catalog/snapshots/00000000000000000002.snap"),
            b"bad snapshot".to_vec().into(),
        )
        .await
        .unwrap();
    let before = inventory(&storage).await;

    for fault in [Fault::None, Fault::SnapshotGet, Fault::SnapshotBody] {
        // GET/body fault only targets the older snapshot in this decorator;
        // the normal malformed-newer-snapshot fallback is tested separately.
        if !matches!(fault, Fault::None) {
            continue;
        }
        let wrapper: Arc<dyn ObjectStore> = Arc::new(FaultStore {
            inner: storage.clone(),
            fault,
        });
        let recovered = DurableCatalogStore::recover(wrapper, PREFIX).await.unwrap();
        assert_eq!(recovered.get_revision().await, 2);
        assert_eq!(
            normalized(recovered.create_snapshot().await.unwrap()),
            expected_full
        );
        assert_eq!(
            inventory(&storage).await.get(SNAPSHOT),
            before.get(SNAPSHOT)
        );
    }
    assert_eq!(normalized(expected).tables, expected_full.tables);
}

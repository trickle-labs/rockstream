//! Audit reproductions: copy into rockstream-storage/tests to run.
//! These assert the intended contract and fail at revision 5d9c3b9.
use object_store::memory::InMemory;
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use rockstream_types::workload::WorkloadDef;
use std::sync::Arc;

#[tokio::test]
async fn audit_snapshot_preserves_operation_idempotency() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "audit");
    let table = CatalogTable {
        id: TableId(10),
        name: "deleted_table".into(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    let create = CatalogTxn::new(1, 555, vec![CatalogMutation::PutTable(table)]).unwrap();
    catalog.commit_txn(create.clone()).await.unwrap();
    catalog
        .commit_txn(
            CatalogTxn::new(2, 556, vec![CatalogMutation::DeleteTable(TableId(10))]).unwrap(),
        )
        .await
        .unwrap();
    catalog.create_snapshot().await.unwrap();
    catalog.compact_logs(2).await.unwrap();
    let recovered = DurableCatalogStore::recover(store, "audit").await.unwrap();
    assert_eq!(
        (
            recovered.get_revision().await,
            recovered.list_tables().await.unwrap()
        ),
        (2, vec![])
    );
    let revision = recovered.commit_txn(create).await.unwrap();
    assert_eq!(
        (
            revision,
            recovered.get_revision().await,
            recovered.list_tables().await.unwrap()
        ),
        (2, 2, vec![])
    );
}

async fn assert_workload_identity(snapshot: bool) {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "audit");
    catalog.allocate_id().await.unwrap();
    let workload = WorkloadDef::new("interactive");
    catalog
        .commit_txn(
            CatalogTxn::new(1, 1, vec![CatalogMutation::PutWorkload(workload.clone())]).unwrap(),
        )
        .await
        .unwrap();
    let before = (
        catalog.get_workload(WorkloadId(1)).await.unwrap(),
        catalog.get_workload(WorkloadId(2)).await.unwrap(),
    );
    assert_eq!(before, (None, Some(workload)));
    if snapshot {
        catalog.create_snapshot().await.unwrap();
        catalog.compact_logs(1).await.unwrap();
    }
    let recovered = DurableCatalogStore::recover(store, "audit").await.unwrap();
    let after = (
        recovered.get_workload(WorkloadId(1)).await.unwrap(),
        recovered.get_workload(WorkloadId(2)).await.unwrap(),
    );
    assert_eq!(after, before);
}

#[tokio::test]
async fn audit_log_replay_preserves_workload_identity() {
    assert_workload_identity(false).await;
}

#[tokio::test]
async fn audit_snapshot_preserves_workload_identity() {
    assert_workload_identity(true).await;
}

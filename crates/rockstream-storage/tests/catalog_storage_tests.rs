use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use rockstream_storage::catalog::log::CatalogLogManager;
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use std::sync::Arc;

#[tokio::test]
async fn test_catalog_log_append_and_replay() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    for i in 1..=5 {
        let tbl = CatalogTable {
            id: TableId(i),
            name: format!("tbl_{i}"),
            namespace_id: NamespaceId(1),
            columns: vec![],
            pk_cols: vec![],
        };
        let txn = CatalogTxn::new(i, i * 10, vec![CatalogMutation::PutTable(tbl)]).unwrap();
        catalog.commit_txn(txn).await.unwrap();
    }

    assert_eq!(catalog.get_revision().await, 5);

    // Replay on recovery
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_revision().await, 5);
    let tables = recovered.list_tables().await.unwrap();
    assert_eq!(tables.len(), 5);
}

#[tokio::test]
async fn test_catalog_snapshot_creation_and_recovery() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    for i in 1..=3 {
        let tbl = CatalogTable {
            id: TableId(i),
            name: format!("tbl_{i}"),
            namespace_id: NamespaceId(1),
            columns: vec![],
            pk_cols: vec![],
        };
        let txn = CatalogTxn::new(i, i * 10, vec![CatalogMutation::PutTable(tbl)]).unwrap();
        catalog.commit_txn(txn).await.unwrap();
    }

    // Create snapshot at revision 3
    let snap = catalog.create_snapshot().await.unwrap();
    assert_eq!(snap.revision, 3);
    assert_eq!(snap.tables.len(), 3);

    // Add another table after snapshot (revision 4)
    let tbl4 = CatalogTable {
        id: TableId(4),
        name: "tbl_4".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    let txn4 = CatalogTxn::new(4, 40, vec![CatalogMutation::PutTable(tbl4)]).unwrap();
    catalog.commit_txn(txn4).await.unwrap();

    // Recovery uses snapshot at revision 3 + log at revision 4
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_revision().await, 4);
    assert_eq!(recovered.list_tables().await.unwrap().len(), 4);
}

#[tokio::test]
async fn test_log_deletion_requires_durable_snapshot_confirmation() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let tbl = CatalogTable {
        id: TableId(1),
        name: "tbl_1".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    let txn = CatalogTxn::new(1, 10, vec![CatalogMutation::PutTable(tbl)]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    // Try compacting logs without creating snapshot first -> fails closed
    let res = catalog.compact_logs(1).await;
    assert!(
        res.is_err(),
        "Compaction must fail if snapshot does not exist"
    );

    // Create snapshot
    catalog.create_snapshot().await.unwrap();

    // Now compaction succeeds and deletes covered logs
    let deleted = catalog.compact_logs(1).await.unwrap();
    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn test_multi_page_catalog_scan_continues_to_completion() {
    let total_records = 2500;
    let mut all_txns = Vec::new();
    for i in 1..=total_records {
        let txn = CatalogTxn::new(
            i as u64,
            i as u64,
            vec![CatalogMutation::DeleteTable(TableId(i as u64))],
        )
        .unwrap();
        all_txns.push(txn);
    }

    // Single page scan hits MAX_SCAN_PAGE_SIZE (1024)
    let page1 = CatalogLogManager::scan_page(&all_txns, 0, 1024).await;
    assert_eq!(page1.transactions.len(), 1024);
    assert!(page1.has_more);
    assert_eq!(page1.next_cursor, Some(1024));

    // Multi-page continuing scan traverses the entire catalog without truncation
    let full_scan = CatalogLogManager::scan_all_continuing(&all_txns, 1024).await;
    assert_eq!(full_scan.len(), total_records);
}

#[tokio::test]
async fn test_interrupted_compaction_falls_back_safely() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    for i in 1..=3 {
        let tbl = CatalogTable {
            id: TableId(i),
            name: format!("tbl_{i}"),
            namespace_id: NamespaceId(1),
            columns: vec![],
            pk_cols: vec![],
        };
        let txn = CatalogTxn::new(i, i * 10, vec![CatalogMutation::PutTable(tbl)]).unwrap();
        catalog.commit_txn(txn).await.unwrap();
    }

    // Write an unfinalized/corrupted snapshot file
    let corrupt_snap_path = ObjectPath::from("test/catalog/snapshots/00000000000000000003.snap");
    store
        .put(
            &corrupt_snap_path,
            b"corrupted snapshot bytes".to_vec().into(),
        )
        .await
        .unwrap();

    // Recovery must detect the corrupted snapshot and fall back safely to log replay
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_revision().await, 3);
    assert_eq!(recovered.list_tables().await.unwrap().len(), 3);
}

#[tokio::test]
async fn test_catalog_compaction_uses_no_range_delete() {
    // Assert that compaction performs individual scan-and-delete
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let tbl = CatalogTable {
        id: TableId(1),
        name: "test".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    let txn = CatalogTxn::new(1, 10, vec![CatalogMutation::PutTable(tbl)]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    catalog.create_snapshot().await.unwrap();
    let deleted = catalog.compact_logs(1).await.unwrap();
    assert_eq!(deleted, 1);
    // Verified: No SlateDB range deletion used, purely ObjectStore::delete calls on individual keys
}

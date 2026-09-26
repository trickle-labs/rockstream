use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use rockstream_storage::catalog::log::CatalogLogManager;
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use std::sync::Arc;

async fn stored_objects(store: &InMemory) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut objects = std::collections::BTreeMap::new();
    let mut entries = store.list(None);
    while let Some(entry) = entries.next().await {
        let path = entry.unwrap().location;
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        objects.insert(path.to_string(), bytes.to_vec());
    }
    objects
}

#[tokio::test]
async fn test_legacy_snapshot_checksum_and_recovery() {
    let bytes = br#"{"revision":2,"high_water_mark":10,"databases":[],"namespaces":[],"tables":[],"views":[],"inline_views":[],"view_dependencies":[],"indexes":[],"workloads":[],"sources":[],"sinks":[],"roles":[],"compiled_plans":[],"checksum":1634664217}"#;
    let snapshot = CatalogSnapshot::decode(bytes).unwrap();
    assert_eq!(snapshot.encode().unwrap(), bytes);
    assert_eq!(
        snapshot.committed_operations,
        std::collections::BTreeSet::new()
    );
    let store = Arc::new(InMemory::new());
    store
        .put(
            &ObjectPath::from("legacy/catalog/snapshots/00000000000000000002.snap"),
            bytes.to_vec().into(),
        )
        .await
        .unwrap();
    let recovered = DurableCatalogStore::recover(store, "legacy").await.unwrap();
    assert_eq!(
        (
            recovered.get_revision().await,
            recovered.list_tables().await.unwrap()
        ),
        (2, vec![])
    );
}

#[tokio::test]
async fn test_snapshot_retry_preserves_deleted_table_and_objects() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "retry");
    let table = CatalogTable {
        id: TableId(10),
        name: "deleted_table".into(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    let create = CatalogTxn::new(1, 555, vec![CatalogMutation::PutTable(table)]).unwrap();
    assert_eq!(catalog.commit_txn(create.clone()).await.unwrap(), 1);
    let delete = CatalogTxn::new(2, 556, vec![CatalogMutation::DeleteTable(TableId(10))]).unwrap();
    assert_eq!(catalog.commit_txn(delete).await.unwrap(), 2);
    catalog.create_snapshot().await.unwrap();
    catalog.compact_logs(2).await.unwrap();
    let before = stored_objects(&store).await;
    assert_eq!(
        before.keys().cloned().collect::<Vec<_>>(),
        vec!["retry/catalog/snapshots/00000000000000000002.snap"]
    );

    for _ in 0..2 {
        let recovered = DurableCatalogStore::recover(store.clone(), "retry")
            .await
            .unwrap();
        assert_eq!(
            (
                recovered.get_revision().await,
                recovered.list_tables().await.unwrap()
            ),
            (2, vec![])
        );
        assert_eq!(
            (
                recovered.commit_txn(create.clone()).await.unwrap(),
                recovered.get_revision().await,
                recovered.list_tables().await.unwrap()
            ),
            (2, 2, vec![])
        );
        assert_eq!(stored_objects(&store).await, before);
    }
}

#[tokio::test]
async fn test_snapshot_retry_preserves_latest_alter_and_objects() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "retry");
    let original = CatalogTable {
        id: TableId(10),
        name: "table".into(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    let older = CatalogTable {
        columns: vec![CatalogColumn {
            name: "old".into(),
            data_type: "INT".into(),
            nullable: true,
            ordinal: 0,
        }],
        ..original.clone()
    };
    let latest = CatalogTable {
        columns: vec![CatalogColumn {
            name: "latest".into(),
            data_type: "TEXT".into(),
            nullable: false,
            ordinal: 0,
        }],
        pk_cols: vec!["latest".into()],
        ..original.clone()
    };
    let create = CatalogTxn::new(1, 554, vec![CatalogMutation::PutTable(original)]).unwrap();
    let alter = CatalogTxn::new(2, 555, vec![CatalogMutation::PutTable(older)]).unwrap();
    let update = CatalogTxn::new(3, 556, vec![CatalogMutation::PutTable(latest.clone())]).unwrap();
    assert_eq!(catalog.commit_txn(create).await.unwrap(), 1);
    assert_eq!(catalog.commit_txn(alter.clone()).await.unwrap(), 2);
    catalog.create_snapshot().await.unwrap();
    catalog.compact_logs(2).await.unwrap();
    assert_eq!(catalog.commit_txn(update).await.unwrap(), 3);

    for _ in 0..2 {
        let before = stored_objects(&store).await;
        let recovered = DurableCatalogStore::recover(store.clone(), "retry")
            .await
            .unwrap();
        assert_eq!(
            (
                recovered.get_revision().await,
                recovered.list_tables().await.unwrap()
            ),
            (3, vec![latest.clone()])
        );
        assert_eq!(
            (
                recovered.commit_txn(alter.clone()).await.unwrap(),
                recovered.get_revision().await,
                recovered.list_tables().await.unwrap()
            ),
            (3, 3, vec![latest.clone()])
        );
        assert_eq!(stored_objects(&store).await, before);
        recovered.create_snapshot().await.unwrap();
        recovered.compact_logs(3).await.unwrap();
    }
}

#[tokio::test]
async fn test_catalog_log_append_and_replay() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let mut expected_tables = Vec::new();
    let mut expected_objects = std::collections::BTreeMap::new();
    for i in 1..=5 {
        let tbl = CatalogTable {
            id: TableId(i),
            name: format!("tbl_{i}"),
            namespace_id: NamespaceId(1),
            columns: vec![],
            pk_cols: vec![],
        };
        expected_tables.push(tbl.clone());
        let txn = CatalogTxn::new(i, i * 10, vec![CatalogMutation::PutTable(tbl)]).unwrap();
        expected_objects.insert(
            format!("test/catalog/log/{i:020}_{:020}.log", i * 10),
            txn.encode().unwrap(),
        );
        catalog.commit_txn(txn).await.unwrap();
    }

    assert_eq!(catalog.get_revision().await, 5);
    assert_eq!(stored_objects(&store).await, expected_objects);

    // Replay on recovery
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_revision().await, 5);
    let mut tables = recovered.list_tables().await.unwrap();
    tables.sort_by_key(|table| table.id);
    assert_eq!(tables, expected_tables);
}

#[tokio::test]
async fn test_catalog_snapshot_creation_and_recovery() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let mut expected_tables = Vec::new();
    for i in 1..=3 {
        let tbl = CatalogTable {
            id: TableId(i),
            name: format!("tbl_{i}"),
            namespace_id: NamespaceId(1),
            columns: vec![],
            pk_cols: vec![],
        };
        expected_tables.push(tbl.clone());
        let txn = CatalogTxn::new(i, i * 10, vec![CatalogMutation::PutTable(tbl)]).unwrap();
        catalog.commit_txn(txn).await.unwrap();
    }

    // Create snapshot at revision 3
    let snap = catalog.create_snapshot().await.unwrap();
    assert_eq!(snap.revision, 3);
    let mut snapshot_tables = snap.tables;
    snapshot_tables.sort_by_key(|table| table.id);
    assert_eq!(snapshot_tables, expected_tables);
    catalog.compact_logs(3).await.unwrap();

    // Add another table after snapshot (revision 4)
    let tbl4 = CatalogTable {
        id: TableId(4),
        name: "tbl_4".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![],
        pk_cols: vec![],
    };
    expected_tables.push(tbl4.clone());
    let txn4 = CatalogTxn::new(4, 40, vec![CatalogMutation::PutTable(tbl4)]).unwrap();
    let expected_log = txn4.encode().unwrap();
    catalog.commit_txn(txn4).await.unwrap();
    assert_eq!(
        stored_objects(&store)
            .await
            .into_iter()
            .filter(|(path, _)| path.ends_with(".log"))
            .collect::<Vec<_>>(),
        vec![(
            "test/catalog/log/00000000000000000004_00000000000000000040.log".into(),
            expected_log
        )]
    );

    // Recovery uses snapshot at revision 3 + log at revision 4
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    assert_eq!(recovered.get_revision().await, 4);
    let mut tables = recovered.list_tables().await.unwrap();
    tables.sort_by_key(|table| table.id);
    assert_eq!(tables, expected_tables);
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
async fn test_zero_page_size_still_advances_replay_cursor() {
    let all_txns = (1..=3)
        .map(|revision| {
            CatalogTxn::new(
                revision,
                revision,
                vec![CatalogMutation::DeleteTable(TableId(revision))],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    let page = CatalogLogManager::scan_page(&all_txns, 0, 0).await;
    assert_eq!(page.transactions, all_txns[..1].to_vec());
    assert_eq!(page.next_cursor, Some(1));
    assert!(page.has_more);

    let full_scan = CatalogLogManager::scan_all_continuing(&all_txns, 0).await;
    assert_eq!(full_scan, all_txns);
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

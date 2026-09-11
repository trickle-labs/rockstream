//! Catalog Negative Tests and Corrupt Record Containment (v0.63 Slice 8).
//!
//! Asserts that:
//! 1. Corrupted transaction log records halt log replay cleanly, preserving prior valid
//!    revision without publishing partial corrupted metadata.
//! 2. Missing referenced objects prevent transaction commit with RS-1004.

use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use rockstream_storage::catalog::{
    CatalogColumn, CatalogMutation, CatalogStore, CatalogTable, CatalogTxn, DependencyKind,
    DurableCatalogStore, ViewDependency,
};
use rockstream_types::ids::{NamespaceId, TableId};
use std::sync::Arc;

#[tokio::test]
async fn test_corrupt_record_fails_closed_without_partial_metadata() {
    let store = Arc::new(InMemory::new());
    let prefix = "catalog_negative_test";
    let catalog = DurableCatalogStore::new(store.clone(), prefix);

    // 1. Commit a valid table at revision 1
    let table = CatalogTable {
        id: TableId::from(1),
        name: "valid_table".to_string(),
        namespace_id: NamespaceId::from(1),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int32".to_string(),
            nullable: false,
            ordinal: 1,
        }],
        pk_cols: vec!["id".to_string()],
    };

    let txn1 = CatalogTxn::new(1, 100, vec![CatalogMutation::PutTable(table)]).expect("valid txn");
    catalog.commit_txn(txn1).await.expect("commit txn1 failed");

    assert_eq!(catalog.get_revision().await, 1);
    assert!(catalog
        .get_table_by_name("valid_table")
        .await
        .unwrap()
        .is_some());

    // 2. Inject a corrupted log file for revision 2 with an invalid checksum / corrupted payload
    let corrupt_log_path =
        ObjectPath::from(format!("{}/catalog/log/{:020}_{:020}.log", prefix, 2, 101));
    // Corrupted bytes that fail checksum / envelope decoding
    let corrupted_payload = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
    store
        .put(&corrupt_log_path, corrupted_payload.into())
        .await
        .expect("put corrupted log failed");

    // 3. Recover catalog from durable storage:
    // Replay should halt cleanly at the corruption boundary without crashing,
    // preserving the last known valid state (revision 1) and never creating partial metadata.
    let recovered = DurableCatalogStore::recover(store.clone(), prefix)
        .await
        .expect("recovery should halt cleanly on corrupted log entry");

    assert_eq!(
        recovered.get_revision().await,
        1,
        "Recovery must preserve prior valid revision"
    );
    assert!(
        recovered
            .get_table_by_name("valid_table")
            .await
            .unwrap()
            .is_some(),
        "Valid table must be intact"
    );
}

#[tokio::test]
async fn test_missing_referenced_object_prevents_transaction_commit() {
    let store = Arc::new(InMemory::new());
    let prefix = "catalog_missing_ref_test";
    let catalog = DurableCatalogStore::new(store.clone(), prefix);

    // Attempt to add a ViewDependency referencing nonexistent table ID 999
    let dep = ViewDependency {
        parent_id: 1,  // nonexistent view
        child_id: 999, // nonexistent table
        dependency_kind: DependencyKind::Table,
    };

    let txn =
        CatalogTxn::new(1, 201, vec![CatalogMutation::PutViewDependency(dep)]).expect("valid txn");

    let res = catalog.commit_txn(txn).await;
    assert!(
        res.is_err(),
        "commit_txn must fail when referenced object is missing"
    );

    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("[RS-1004]") || err.to_string().contains("not found"),
        "Error must contain RS-1004 or object not found: {err}"
    );

    // Revision must not advance
    assert_eq!(catalog.get_revision().await, 0);
}

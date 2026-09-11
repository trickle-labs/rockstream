//! System Catalog Projections and Invalidation Tests (v0.63 Slice 6).
//!
//! Asserts that:
//! 1. pg_catalog.pg_tables projection matches exact row multiset before and after restart.
//! 2. pg_catalog.pg_views projection matches exact row multiset and definitions post-restart.
//! 3. pg_catalog.pg_class and pg_attribute projections match tables, views, and indexes post-restart.
//! 4. information_schema.tables projection matches exact table definitions post-restart.
//! 5. Projection caches are keyed by revision and invalidated atomically on revision change.

use object_store::memory::InMemory;
use rockstream_gateway::catalog_stubs::{
    CatalogColumn as GatewayColumn, CatalogResponse, CatalogStubs, CatalogTable as GatewayTable,
    SessionInfo,
};
use rockstream_storage::catalog::{
    CatalogColumn, CatalogIndexEntry, CatalogIndexState, CatalogMutation, CatalogStore,
    CatalogTable, CatalogTxn, CatalogView, DurableCatalogStore,
};
use rockstream_types::ids::{IndexId, NamespaceId, TableId, ViewId};
use std::sync::Arc;

fn get_rows(response: Option<CatalogResponse>) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    match response.expect("expected response") {
        CatalogResponse::Rows { columns, rows } => (columns, rows),
        _ => panic!("expected rows response"),
    }
}

#[tokio::test]
async fn test_pg_tables_projection_post_restart() {
    let store = Arc::new(InMemory::new());
    let prefix = "catalog_tables_test";
    let durable = DurableCatalogStore::new(store.clone(), prefix);

    let table = CatalogTable {
        id: TableId::from(1),
        name: "customers".to_string(),
        namespace_id: NamespaceId::from(1),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int32".to_string(),
                nullable: false,
                ordinal: 1,
            },
            CatalogColumn {
                name: "email".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
                ordinal: 2,
            },
        ],
        pk_cols: vec!["id".to_string()],
    };

    let txn = CatalogTxn::new(1, 100, vec![CatalogMutation::PutTable(table)]).expect("valid txn");
    durable.commit_txn(txn).await.expect("commit failed");

    // Gateway before restart
    let gw1 = CatalogStubs::new();
    gw1.set_durable_store(Arc::new(durable));
    gw1.sync_from_durable_store().await.expect("sync failed");

    let session = SessionInfo::default();
    let (_, rows_before) =
        get_rows(gw1.handle_query("SELECT * FROM pg_catalog.pg_tables", &session));
    assert!(
        rows_before
            .iter()
            .any(|r| r.contains(&Some("customers".to_string()))),
        "customers table must exist before restart"
    );

    // Fresh gateway instance reconstructed post-restart
    let recovered_durable = DurableCatalogStore::recover(store.clone(), prefix)
        .await
        .expect("recover failed");
    let gw2 = CatalogStubs::new();
    gw2.set_durable_store(Arc::new(recovered_durable));
    gw2.sync_from_durable_store().await.expect("sync failed");

    let (_, rows_after) =
        get_rows(gw2.handle_query("SELECT * FROM pg_catalog.pg_tables", &session));
    assert_eq!(
        rows_before, rows_after,
        "pg_tables projection must be bit-identical post-restart"
    );
}

#[tokio::test]
async fn test_pg_views_projection_post_restart() {
    let store = Arc::new(InMemory::new());
    let prefix = "catalog_views_test";
    let durable = DurableCatalogStore::new(store.clone(), prefix);

    let view = CatalogView {
        id: ViewId::from(2),
        name: "active_customers".to_string(),
        namespace_id: NamespaceId::from(1),
        sql: "SELECT id, email FROM customers WHERE active = true".to_string(),
        compiled_plan_id: None,
        op_id: Some(10),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int32".to_string(),
                nullable: false,
                ordinal: 1,
            },
            CatalogColumn {
                name: "email".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
                ordinal: 2,
            },
        ],
    };

    let txn = CatalogTxn::new(1, 101, vec![CatalogMutation::PutView(view)]).expect("valid txn");
    durable.commit_txn(txn).await.expect("commit failed");

    // Gateway before restart
    let gw1 = CatalogStubs::new();
    gw1.set_durable_store(Arc::new(durable));
    gw1.sync_from_durable_store().await.expect("sync failed");

    let session = SessionInfo::default();
    let (_, rows_before) =
        get_rows(gw1.handle_query("SELECT * FROM pg_catalog.pg_views", &session));
    assert!(
        rows_before
            .iter()
            .any(|r| r.contains(&Some("active_customers".to_string()))),
        "active_customers view must exist before restart"
    );

    // Fresh gateway instance post-restart
    let recovered_durable = DurableCatalogStore::recover(store.clone(), prefix)
        .await
        .expect("recover failed");
    let gw2 = CatalogStubs::new();
    gw2.set_durable_store(Arc::new(recovered_durable));
    gw2.sync_from_durable_store().await.expect("sync failed");

    let (_, rows_after) = get_rows(gw2.handle_query("SELECT * FROM pg_catalog.pg_views", &session));
    assert_eq!(
        rows_before, rows_after,
        "pg_views projection must be bit-identical post-restart"
    );
}

#[tokio::test]
async fn test_pg_class_and_attribute_projection_post_restart() {
    let store = Arc::new(InMemory::new());
    let prefix = "catalog_class_test";
    let durable = DurableCatalogStore::new(store.clone(), prefix);

    let table = CatalogTable {
        id: TableId::from(1),
        name: "orders".to_string(),
        namespace_id: NamespaceId::from(1),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int32".to_string(),
            nullable: false,
            ordinal: 1,
        }],
        pk_cols: vec!["id".to_string()],
    };

    let view = CatalogView {
        id: ViewId::from(2),
        name: "order_summary".to_string(),
        namespace_id: NamespaceId::from(1),
        sql: "SELECT count(*) FROM orders".to_string(),
        compiled_plan_id: None,
        op_id: Some(100),
        columns: vec![CatalogColumn {
            name: "count".to_string(),
            data_type: "Int64".to_string(),
            nullable: false,
            ordinal: 1,
        }],
    };

    let index = CatalogIndexEntry {
        id: IndexId::from(3),
        name: "idx_orders_id".to_string(),
        table_id: TableId::from(1),
        index_cols: vec!["id".to_string()],
        pk_cols: vec!["id".to_string()],
        state: CatalogIndexState::Ready,
        op_id: Some(200),
    };

    let txn = CatalogTxn::new(
        1,
        102,
        vec![
            CatalogMutation::PutTable(table),
            CatalogMutation::PutView(view),
            CatalogMutation::PutIndex(index),
        ],
    )
    .expect("valid txn");
    durable.commit_txn(txn).await.expect("commit failed");

    let gw1 = CatalogStubs::new();
    gw1.set_durable_store(Arc::new(durable));
    gw1.sync_from_durable_store().await.expect("sync failed");

    let session = SessionInfo::default();
    let (_, class_before) =
        get_rows(gw1.handle_query("SELECT * FROM pg_catalog.pg_class", &session));
    let (_, attr_before) =
        get_rows(gw1.handle_query("SELECT * FROM pg_catalog.pg_attribute", &session));

    // Assert kinds in pg_class: r (table), v (view), i (index)
    assert!(class_before
        .iter()
        .any(|r| r.contains(&Some("orders".to_string())) && r.contains(&Some("r".to_string()))));
    assert!(class_before
        .iter()
        .any(|r| r.contains(&Some("order_summary".to_string()))
            && r.contains(&Some("v".to_string()))));
    assert!(class_before
        .iter()
        .any(|r| r.contains(&Some("idx_orders_id".to_string()))
            && r.contains(&Some("i".to_string()))));

    // Restart
    let recovered_durable = DurableCatalogStore::recover(store.clone(), prefix)
        .await
        .expect("recover failed");
    let gw2 = CatalogStubs::new();
    gw2.set_durable_store(Arc::new(recovered_durable));
    gw2.sync_from_durable_store().await.expect("sync failed");

    let (_, class_after) =
        get_rows(gw2.handle_query("SELECT * FROM pg_catalog.pg_class", &session));
    let (_, attr_after) =
        get_rows(gw2.handle_query("SELECT * FROM pg_catalog.pg_attribute", &session));

    assert_eq!(
        class_before, class_after,
        "pg_class must match post-restart"
    );
    assert_eq!(
        attr_before, attr_after,
        "pg_attribute must match post-restart"
    );
}

#[tokio::test]
async fn test_information_schema_tables_post_restart() {
    let store = Arc::new(InMemory::new());
    let prefix = "catalog_info_schema_test";
    let durable = DurableCatalogStore::new(store.clone(), prefix);

    let table = CatalogTable {
        id: TableId::from(1),
        name: "events".to_string(),
        namespace_id: NamespaceId::from(1),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int32".to_string(),
            nullable: false,
            ordinal: 1,
        }],
        pk_cols: vec![],
    };

    let txn = CatalogTxn::new(1, 103, vec![CatalogMutation::PutTable(table)]).expect("valid txn");
    durable.commit_txn(txn).await.expect("commit failed");

    let gw1 = CatalogStubs::new();
    gw1.set_durable_store(Arc::new(durable));
    gw1.sync_from_durable_store().await.expect("sync failed");

    let session = SessionInfo::default();
    let (_, info_tables_before) =
        get_rows(gw1.handle_query("SELECT * FROM information_schema.tables", &session));

    assert!(info_tables_before.iter().any(|r| {
        r.contains(&Some("events".to_string())) && r.contains(&Some("BASE TABLE".to_string()))
    }));

    // Restart
    let recovered_durable = DurableCatalogStore::recover(store.clone(), prefix)
        .await
        .expect("recover failed");
    let gw2 = CatalogStubs::new();
    gw2.set_durable_store(Arc::new(recovered_durable));
    gw2.sync_from_durable_store().await.expect("sync failed");

    let (_, info_tables_after) =
        get_rows(gw2.handle_query("SELECT * FROM information_schema.tables", &session));
    assert_eq!(
        info_tables_before, info_tables_after,
        "information_schema.tables must match post-restart"
    );
}

#[tokio::test]
async fn test_projection_cache_invalidation_on_revision_change() {
    let gw = CatalogStubs::new();
    let initial_rev = gw.get_revision();

    gw.add_table(GatewayTable {
        name: "items".to_string(),
        columns: vec![GatewayColumn {
            name: "id".to_string(),
            data_type: "Int32".to_string(),
        }],
    });
    let rev1 = gw.get_revision();
    assert!(rev1 > initial_rev, "add_table must advance revision");

    let session = SessionInfo::default();
    let (_, rows1) = get_rows(gw.handle_query("SELECT * FROM pg_catalog.pg_tables", &session));
    assert_eq!(rows1.len(), 1);

    // Call again - hits projection cache
    let (_, rows1_cached) =
        get_rows(gw.handle_query("SELECT * FROM pg_catalog.pg_tables", &session));
    assert_eq!(rows1, rows1_cached);

    // Add another table - invalidates cache and advances revision
    gw.add_table(GatewayTable {
        name: "users".to_string(),
        columns: vec![GatewayColumn {
            name: "id".to_string(),
            data_type: "Int32".to_string(),
        }],
    });
    let rev2 = gw.get_revision();
    assert!(rev2 > rev1, "second add_table must advance revision");

    let (_, rows2) = get_rows(gw.handle_query("SELECT * FROM pg_catalog.pg_tables", &session));
    assert_eq!(
        rows2.len(),
        2,
        "cache must be invalidated and reflect both tables"
    );

    // Remove a table - invalidates cache and advances revision
    gw.remove_table("items");
    let rev3 = gw.get_revision();
    assert!(rev3 > rev2, "remove_table must advance revision");

    let (_, rows3) = get_rows(gw.handle_query("SELECT * FROM pg_catalog.pg_tables", &session));
    assert_eq!(
        rows3.len(),
        1,
        "cache must be invalidated and reflect only users"
    );
    assert_eq!(rows3[0][1], Some("users".to_string()));
}

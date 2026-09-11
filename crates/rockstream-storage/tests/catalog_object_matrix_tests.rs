use object_store::memory::InMemory;
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use rockstream_types::workload::WorkloadDef;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn test_domain_id_types_and_display() {
    assert_eq!(TableId(42).to_string(), "table-42");
    assert_eq!(IndexId(101).to_string(), "idx-101");
    assert_eq!(DatabaseId(1).to_string(), "db-1");
    assert_eq!(SinkId(99).to_string(), "sink-99");
    assert_eq!(CompiledPlanId(77).to_string(), "plan-77");
    assert_eq!(PrincipalId(12).to_string(), "principal-12");
    assert_eq!(NamespaceId(5).to_string(), "ns-5");
    assert_eq!(ViewId(88).to_string(), "view-88");
}

#[test]
fn test_catalog_domain_entities_serialization() {
    let db = CatalogDatabase {
        id: DatabaseId(1),
        name: "test_db".to_string(),
        default_namespace: "public".to_string(),
        created_at: 1000,
    };
    let json = serde_json::to_string(&db).unwrap();
    let db_back: CatalogDatabase = serde_json::from_str(&json).unwrap();
    assert_eq!(db, db_back);

    let ns = CatalogNamespace {
        id: NamespaceId(1),
        name: "public".to_string(),
        database_id: DatabaseId(1),
        created_at: 1000,
    };
    let json = serde_json::to_string(&ns).unwrap();
    let ns_back: CatalogNamespace = serde_json::from_str(&json).unwrap();
    assert_eq!(ns, ns_back);

    let col = CatalogColumn {
        name: "id".to_string(),
        data_type: "Int64".to_string(),
        nullable: false,
        ordinal: 0,
    };
    let json = serde_json::to_string(&col).unwrap();
    let col_back: CatalogColumn = serde_json::from_str(&json).unwrap();
    assert_eq!(col, col_back);

    let tbl = CatalogTable {
        id: TableId(10),
        name: "users".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![col],
        pk_cols: vec!["id".to_string()],
    };
    let json = serde_json::to_string(&tbl).unwrap();
    let tbl_back: CatalogTable = serde_json::from_str(&json).unwrap();
    assert_eq!(tbl, tbl_back);

    let view = CatalogView {
        id: ViewId(20),
        name: "active_users".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT id FROM users".to_string(),
        compiled_plan_id: Some(CompiledPlanId(30)),
        op_id: Some(5),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
            nullable: false,
            ordinal: 0,
        }],
    };
    let json = serde_json::to_string(&view).unwrap();
    let view_back: CatalogView = serde_json::from_str(&json).unwrap();
    assert_eq!(view, view_back);

    let iv = CatalogInlineView {
        id: ViewId(21),
        name: "inline_users".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT id FROM users".to_string(),
        ast_json: "{}".to_string(),
        referenced_objects: vec!["users".to_string()],
    };
    let json = serde_json::to_string(&iv).unwrap();
    let iv_back: CatalogInlineView = serde_json::from_str(&json).unwrap();
    assert_eq!(iv, iv_back);

    let dep = ViewDependency {
        parent_id: 20,
        child_id: 10,
        dependency_kind: DependencyKind::Table,
    };
    let json = serde_json::to_string(&dep).unwrap();
    let dep_back: ViewDependency = serde_json::from_str(&json).unwrap();
    assert_eq!(dep, dep_back);

    let idx = CatalogIndexEntry {
        id: IndexId(1),
        name: "users_idx".to_string(),
        table_id: TableId(10),
        index_cols: vec!["id".to_string()],
        pk_cols: vec!["id".to_string()],
        state: CatalogIndexState::Ready,
        op_id: Some(7),
    };
    let json = serde_json::to_string(&idx).unwrap();
    let idx_back: CatalogIndexEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(idx, idx_back);

    let mut options = HashMap::new();
    options.insert("topic".to_string(), "events".to_string());
    let src = CatalogSourceEntry {
        id: SourceId(1),
        name: "kafka_source".to_string(),
        connector_type: "kafka".to_string(),
        table_name: Some("events_tbl".to_string()),
        options: options.clone(),
    };
    let json = serde_json::to_string(&src).unwrap();
    let src_back: CatalogSourceEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(src, src_back);

    let sink = CatalogSinkEntry {
        id: SinkId(1),
        name: "iceberg_sink".to_string(),
        sink_type: "iceberg".to_string(),
        target: "s3://bucket/iceberg".to_string(),
        options,
        status: "RUNNING".to_string(),
    };
    let json = serde_json::to_string(&sink).unwrap();
    let sink_back: CatalogSinkEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(sink, sink_back);

    let role = CatalogRoleEntry {
        id: PrincipalId(1),
        role_name: "analyst".to_string(),
        permissions: vec!["SELECT".to_string()],
        member_of: vec!["staff".to_string()],
    };
    let json = serde_json::to_string(&role).unwrap();
    let role_back: CatalogRoleEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(role, role_back);

    let plan = CompiledPlanRecord {
        id: CompiledPlanId(1),
        sql: "SELECT id FROM users".to_string(),
        ast_hash: [1u8; 32],
        logical_plan_hash: [2u8; 32],
        compiler_version: "0.63.0".to_string(),
        state_layout_version: 1,
        output_schema: vec![0x10, 0x20],
        dependency_ids: vec![10],
    };
    let json = serde_json::to_string(&plan).unwrap();
    let plan_back: CompiledPlanRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(plan, plan_back);
}

#[test]
fn test_unsupported_ddl_rejection() {
    let cases = [
        "CREATE TRIGGER check_update AFTER UPDATE ON accounts FOR EACH ROW EXECUTE PROCEDURE check_update()",
        "create or replace trigger my_trig before insert on t for each row execute function f()",
        "CREATE PROCEDURE insert_data(a integer, b integer) LANGUAGE SQL AS $$ INSERT INTO tbl VALUES (a, b); $$",
        "CREATE FOREIGN TABLE ft (id integer) SERVER srv",
        "CREATE DOMAIN us_postal_code AS TEXT CHECK(VALUE ~ '^\\d{5}$')",
        "CREATE SEQUENCE seq_items START 101",
    ];

    for sql in cases {
        let res = validate_unsupported_ddl(sql);
        assert!(res.is_err(), "Expected rejection for: {sql}");
        let err = res.unwrap_err();
        assert!(
            err.to_string().contains("[RS-1001]"),
            "Expected RS-1001 in error: {err}"
        );
    }

    // Supported DDL should pass
    assert!(validate_unsupported_ddl("CREATE TABLE test (id INT)").is_ok());
    assert!(validate_unsupported_ddl("CREATE MATERIALIZED VIEW v AS SELECT * FROM test").is_ok());
    assert!(validate_unsupported_ddl("CREATE INDEX idx ON test (id)").is_ok());
}

#[tokio::test]
async fn test_database_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let db = CatalogDatabase {
        id: DatabaseId(1),
        name: "test_db".to_string(),
        default_namespace: "public".to_string(),
        created_at: 500,
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutDatabase(db.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_database(DatabaseId(1)).await.unwrap();
    assert_eq!(retrieved, Some(db));
}

#[tokio::test]
async fn test_namespace_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let ns = CatalogNamespace {
        id: NamespaceId(2),
        name: "analytics".to_string(),
        database_id: DatabaseId(1),
        created_at: 600,
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutNamespace(ns.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_namespace(NamespaceId(2)).await.unwrap();
    assert_eq!(retrieved, Some(ns));
}

#[tokio::test]
async fn test_table_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let tbl = CatalogTable {
        id: TableId(10),
        name: "orders".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![
            CatalogColumn {
                name: "order_id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
                ordinal: 0,
            },
            CatalogColumn {
                name: "customer_id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
                ordinal: 1,
            },
        ],
        pk_cols: vec!["order_id".to_string()],
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutTable(tbl.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_table(TableId(10)).await.unwrap();
    assert_eq!(retrieved, Some(tbl));
}

#[tokio::test]
async fn test_column_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let tbl = CatalogTable {
        id: TableId(11),
        name: "items".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![
            CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
                ordinal: 0,
            },
            CatalogColumn {
                name: "price".to_string(),
                data_type: "Float64".to_string(),
                nullable: true,
                ordinal: 1,
            },
        ],
        pk_cols: vec!["id".to_string()],
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutTable(tbl.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_table(TableId(11)).await.unwrap().unwrap();
    assert_eq!(retrieved.columns.len(), 2);
    assert_eq!(retrieved.columns[1].name, "price");
    assert_eq!(retrieved.columns[1].data_type, "Float64");
    assert!(retrieved.columns[1].nullable);
}

#[tokio::test]
async fn test_materialized_view_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let view = CatalogView {
        id: ViewId(50),
        name: "customer_orders".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT customer_id, count(*) FROM orders GROUP BY customer_id".to_string(),
        compiled_plan_id: Some(CompiledPlanId(1)),
        op_id: Some(10),
        columns: vec![CatalogColumn {
            name: "customer_id".to_string(),
            data_type: "Int64".to_string(),
            nullable: false,
            ordinal: 0,
        }],
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutView(view.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_view(ViewId(50)).await.unwrap();
    assert_eq!(retrieved, Some(view));
}

#[tokio::test]
async fn test_inline_view_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let iv = CatalogInlineView {
        id: ViewId(51),
        name: "orders_inline".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT order_id FROM orders".to_string(),
        ast_json: "{\"ast\": true}".to_string(),
        referenced_objects: vec!["orders".to_string()],
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutInlineView(iv.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_inline_view(ViewId(51)).await.unwrap();
    assert_eq!(retrieved, Some(iv));
}

#[tokio::test]
async fn test_view_dependency_cycle_rejection() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store, "test");

    // Create prerequisite views A (id=1) and B (id=2)
    let v1 = CatalogView {
        id: ViewId(1),
        name: "view_a".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT 1".to_string(),
        compiled_plan_id: Some(CompiledPlanId(1)),
        op_id: Some(1),
        columns: vec![],
    };
    let v2 = CatalogView {
        id: ViewId(2),
        name: "view_b".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT 1".to_string(),
        compiled_plan_id: Some(CompiledPlanId(2)),
        op_id: Some(2),
        columns: vec![],
    };
    let setup_txn = CatalogTxn::new(
        1,
        1,
        vec![CatalogMutation::PutView(v1), CatalogMutation::PutView(v2)],
    )
    .unwrap();
    catalog.commit_txn(setup_txn).await.unwrap();

    // Commit A -> B
    let dep1 = ViewDependency {
        parent_id: 1,
        child_id: 2,
        dependency_kind: DependencyKind::View,
    };
    let txn1 = CatalogTxn::new(2, 2, vec![CatalogMutation::PutViewDependency(dep1)]).unwrap();
    catalog.commit_txn(txn1).await.unwrap();

    // Try committing B -> A (introduces cycle A -> B -> A)
    let dep2 = ViewDependency {
        parent_id: 2,
        child_id: 1,
        dependency_kind: DependencyKind::View,
    };
    let txn2 = CatalogTxn::new(3, 3, vec![CatalogMutation::PutViewDependency(dep2)]).unwrap();
    let res = catalog.commit_txn(txn2).await;
    assert!(res.is_err());
    let err = res.unwrap_err();
    assert!(
        err.to_string().contains("[RS-1002]") && err.to_string().contains("CycleDetected"),
        "Expected RS-1002 CycleDetected error, got: {err}"
    );
}

#[tokio::test]
async fn test_index_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let idx = CatalogIndexEntry {
        id: IndexId(7),
        name: "idx_cust".to_string(),
        table_id: TableId(10),
        index_cols: vec!["customer_id".to_string()],
        pk_cols: vec!["order_id".to_string()],
        state: CatalogIndexState::Building,
        op_id: None,
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutIndex(idx.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_index(IndexId(7)).await.unwrap();
    assert_eq!(retrieved, Some(idx));
}

#[tokio::test]
async fn test_workload_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let wl = WorkloadDef::new("analytics_wl");
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutWorkload(wl.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_workload(WorkloadId(1)).await.unwrap();
    assert_eq!(retrieved, Some(wl));
}

#[tokio::test]
async fn test_source_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let mut opts = HashMap::new();
    opts.insert(
        "bootstrap.servers".to_string(),
        "localhost:9092".to_string(),
    );
    let src = CatalogSourceEntry {
        id: SourceId(3),
        name: "kafka_in".to_string(),
        connector_type: "kafka".to_string(),
        table_name: Some("raw_events".to_string()),
        options: opts,
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutSource(src.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_source(SourceId(3)).await.unwrap();
    assert_eq!(retrieved, Some(src));
}

#[tokio::test]
async fn test_sink_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let mut opts = HashMap::new();
    opts.insert("format".to_string(), "parquet".to_string());
    let sink = CatalogSinkEntry {
        id: SinkId(4),
        name: "export_sink".to_string(),
        sink_type: "file".to_string(),
        target: "/data/export".to_string(),
        options: opts,
        status: "ACTIVE".to_string(),
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutSink(sink.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_sink(SinkId(4)).await.unwrap();
    assert_eq!(retrieved, Some(sink));
}

#[tokio::test]
async fn test_role_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let role = CatalogRoleEntry {
        id: PrincipalId(8),
        role_name: "admin".to_string(),
        permissions: vec!["ALL".to_string()],
        member_of: vec![],
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutRole(role.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_role(PrincipalId(8)).await.unwrap();
    assert_eq!(retrieved, Some(role));
}

#[tokio::test]
async fn test_compiled_plan_metadata_durability() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let plan = CompiledPlanRecord {
        id: CompiledPlanId(9),
        sql: "SELECT * FROM t".to_string(),
        ast_hash: [3u8; 32],
        logical_plan_hash: [4u8; 32],
        compiler_version: "0.63.0".to_string(),
        state_layout_version: 1,
        output_schema: vec![1, 2, 3],
        dependency_ids: vec![100],
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutCompiledPlan(plan.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered
        .get_compiled_plan(CompiledPlanId(9))
        .await
        .unwrap();
    assert_eq!(retrieved, Some(plan));
}

use object_store::memory::InMemory;
use rockstream_storage::catalog::identity::IdAllocator;
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use rockstream_types::workload::{MemoryLimit, WorkloadDef};
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn test_id_allocator_avoids_collisions_with_restored_ids() {
    let allocator = IdAllocator::new(0);

    // Initial allocations
    assert_eq!(allocator.allocate(), 1);
    assert_eq!(allocator.allocate(), 2);

    // Observe a restored ID from remote storage with high value
    allocator.observe(100);
    assert_eq!(allocator.high_water_mark(), 100);

    // Future allocations strictly exceed observed high-water mark
    assert_eq!(allocator.allocate(), 101);
    assert_eq!(allocator.allocate(), 102);

    // Observing a lower ID does not regress high-water mark
    allocator.observe(50);
    assert_eq!(allocator.high_water_mark(), 102);
    assert_eq!(allocator.allocate(), 103);
}

#[tokio::test]
async fn test_table_id_invariance_across_rename_and_restart() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let table_id = TableId(catalog.allocate_id().await.unwrap());
    let tbl = CatalogTable {
        id: table_id,
        name: "original_table".to_string(),
        namespace_id: NamespaceId(1),
        columns: vec![CatalogColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
            nullable: false,
            ordinal: 0,
        }],
        pk_cols: vec!["id".to_string()],
    };
    let txn1 = CatalogTxn::new(1, 1, vec![CatalogMutation::PutTable(tbl.clone())]).unwrap();
    catalog.commit_txn(txn1).await.unwrap();

    // Rename table
    let mut renamed_tbl = tbl.clone();
    renamed_tbl.name = "renamed_table".to_string();
    let txn2 = CatalogTxn::new(2, 2, vec![CatalogMutation::PutTable(renamed_tbl)]).unwrap();
    catalog.commit_txn(txn2).await.unwrap();

    // Restart process from storage
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_table(table_id).await.unwrap().unwrap();
    assert_eq!(retrieved.id, table_id);
    assert_eq!(retrieved.name, "renamed_table");
}

#[tokio::test]
async fn test_view_id_invariance_across_restart_and_compaction() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let view_id = ViewId(catalog.allocate_id().await.unwrap());
    let view = CatalogView {
        id: view_id,
        name: "my_view".to_string(),
        namespace_id: NamespaceId(1),
        sql: "SELECT 1".to_string(),
        compiled_plan_id: None,
        op_id: None,
        columns: vec![],
    };
    let txn = CatalogTxn::new(1, 1, vec![CatalogMutation::PutView(view.clone())]).unwrap();
    catalog.commit_txn(txn).await.unwrap();

    // Create snapshot and compact logs
    catalog.create_snapshot().await.unwrap();
    catalog.compact_logs(1).await.unwrap();

    // Restart process
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_view(view_id).await.unwrap().unwrap();
    assert_eq!(retrieved.id, view_id);
    assert_eq!(retrieved.name, "my_view");
}

#[tokio::test]
async fn test_index_id_invariance_across_rebuild_and_restart() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let index_id = IndexId(catalog.allocate_id().await.unwrap());
    let idx = CatalogIndexEntry {
        id: index_id,
        name: "idx_test".to_string(),
        table_id: TableId(10),
        index_cols: vec!["col1".to_string()],
        pk_cols: vec!["id".to_string()],
        state: CatalogIndexState::Building,
        op_id: None,
    };
    let txn1 = CatalogTxn::new(1, 1, vec![CatalogMutation::PutIndex(idx.clone())]).unwrap();
    catalog.commit_txn(txn1).await.unwrap();

    // Rebuild index: state transitions to Ready
    let mut ready_idx = idx.clone();
    ready_idx.state = CatalogIndexState::Ready;
    ready_idx.op_id = Some(42);
    let txn2 = CatalogTxn::new(2, 2, vec![CatalogMutation::PutIndex(ready_idx)]).unwrap();
    catalog.commit_txn(txn2).await.unwrap();

    // Restart process
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_index(index_id).await.unwrap().unwrap();
    assert_eq!(retrieved.id, index_id);
    assert_eq!(retrieved.state, CatalogIndexState::Ready);
    assert_eq!(retrieved.op_id, Some(42));
}

#[tokio::test]
async fn test_source_id_invariance_across_status_changes() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let source_id = SourceId(catalog.allocate_id().await.unwrap());
    let mut opts = HashMap::new();
    opts.insert("status".to_string(), "ACTIVE".to_string());
    let src = CatalogSourceEntry {
        id: source_id,
        name: "kafka_stream".to_string(),
        connector_type: "kafka".to_string(),
        table_name: None,
        options: opts,
    };
    let txn1 = CatalogTxn::new(1, 1, vec![CatalogMutation::PutSource(src.clone())]).unwrap();
    catalog.commit_txn(txn1).await.unwrap();

    // Update status to PAUSED
    let mut paused_src = src.clone();
    paused_src
        .options
        .insert("status".to_string(), "PAUSED".to_string());
    let txn2 = CatalogTxn::new(2, 2, vec![CatalogMutation::PutSource(paused_src)]).unwrap();
    catalog.commit_txn(txn2).await.unwrap();

    // Restart
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_source(source_id).await.unwrap().unwrap();
    assert_eq!(retrieved.id, source_id);
    assert_eq!(retrieved.options.get("status").unwrap(), "PAUSED");
}

#[tokio::test]
async fn test_workload_id_invariance_across_alter_and_restart() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "test");

    let workload_id = WorkloadId(catalog.allocate_id().await.unwrap());
    let wl = WorkloadDef::new("interactive");
    let txn1 = CatalogTxn::new(1, 1, vec![CatalogMutation::PutWorkload(wl.clone())]).unwrap();
    catalog.commit_txn(txn1).await.unwrap();

    // Alter workload budget
    let altered = wl.with_memory_limit(MemoryLimit::new(1024 * 1024 * 100));
    let txn2 = CatalogTxn::new(2, 2, vec![CatalogMutation::PutWorkload(altered.clone())]).unwrap();
    catalog.commit_txn(txn2).await.unwrap();

    // Restart
    let recovered = DurableCatalogStore::recover(store, "test").await.unwrap();
    let retrieved = recovered.get_workload(workload_id).await.unwrap().unwrap();
    assert_eq!(retrieved.name, "interactive");
    assert_eq!(
        retrieved.memory_limit,
        Some(MemoryLimit::new(1024 * 1024 * 100))
    );
}

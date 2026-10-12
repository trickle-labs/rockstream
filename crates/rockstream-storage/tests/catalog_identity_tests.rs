use object_store::memory::InMemory;
use rockstream_storage::catalog::identity::IdAllocator;
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use rockstream_types::workload::{FreshnessSlo, MemoryLimit, WorkloadDef, WorkloadPriority};
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

// Exact ID-to-definition oracle, including every missing ID.
async fn mapping(catalog: &DurableCatalogStore, max_id: u64) -> Vec<Option<WorkloadDef>> {
    let mut result = Vec::new();
    for id in 1..=max_id {
        result.push(catalog.get_workload(WorkloadId(id)).await.unwrap());
    }
    result
}

fn workload(name: &str, memory: u64, parallelism: u32, slo: u64, priority: u8) -> WorkloadDef {
    WorkloadDef::new(name)
        .with_memory_limit(MemoryLimit::new(memory))
        .with_max_parallelism(parallelism)
        .with_freshness_slo(FreshnessSlo::new(slo))
        .with_priority(WorkloadPriority(priority))
}

async fn assert_single_workload_identity(snapshot: bool) {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "identity");
    assert_eq!(catalog.allocate_id().await.unwrap(), 1);

    let original = workload("interactive", 100 * 1024 * 1024, 8, 250, 17);
    catalog
        .commit_txn(
            CatalogTxn::new(1, 100, vec![CatalogMutation::PutWorkload(original.clone())]).unwrap(),
        )
        .await
        .unwrap();

    let expected = vec![None, Some(original.clone())];
    assert_eq!(mapping(&catalog, 2).await, expected);

    if snapshot {
        let saved = catalog.create_snapshot().await.unwrap();
        assert_eq!(
            saved.workload_entries,
            vec![CatalogWorkloadEntry {
                id: WorkloadId(2),
                definition: original.clone(),
            }]
        );
        assert_eq!(catalog.compact_logs(1).await.unwrap(), 1);
    }

    let recovered = DurableCatalogStore::recover(store, "identity")
        .await
        .unwrap();
    assert_eq!(mapping(&recovered, 2).await, expected);
    assert_eq!(recovered.allocate_id().await.unwrap(), 3);
}

#[tokio::test]
async fn workload_identity_survives_log_only_recovery() {
    assert_single_workload_identity(false).await;
}

#[tokio::test]
async fn workload_identity_survives_snapshot_compaction() {
    assert_single_workload_identity(true).await;
}

async fn assert_gaps_deletion_and_second_recovery(snapshot: bool) {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "gaps");
    assert_eq!(catalog.allocate_id().await.unwrap(), 1);
    let first = workload("first", 1024, 2, 100, 3);
    let deleted = workload("deleted", 2048, 4, 200, 7);
    let third = workload("third", 4096, 8, 300, 11);
    let updated = workload("first", 8192, 16, 400, 13);

    catalog
        .commit_txn(CatalogTxn::new(1, 101, vec![CatalogMutation::PutWorkload(first)]).unwrap())
        .await
        .unwrap();
    assert_eq!(catalog.allocate_id().await.unwrap(), 3);
    catalog
        .commit_txn(CatalogTxn::new(2, 102, vec![CatalogMutation::PutWorkload(deleted)]).unwrap())
        .await
        .unwrap();
    assert_eq!(catalog.allocate_id().await.unwrap(), 5);
    catalog
        .commit_txn(
            CatalogTxn::new(3, 103, vec![CatalogMutation::PutWorkload(third.clone())]).unwrap(),
        )
        .await
        .unwrap();
    catalog
        .commit_txn(
            CatalogTxn::new(4, 104, vec![CatalogMutation::PutWorkload(updated.clone())]).unwrap(),
        )
        .await
        .unwrap();
    catalog
        .commit_txn(
            CatalogTxn::new(5, 105, vec![CatalogMutation::DeleteWorkload(WorkloadId(4))]).unwrap(),
        )
        .await
        .unwrap();

    let expected = vec![
        None,
        Some(updated.clone()),
        None,
        None,
        None,
        Some(third.clone()),
    ];
    assert_eq!(mapping(&catalog, 6).await, expected);

    if snapshot {
        catalog.create_snapshot().await.unwrap();
        assert_eq!(catalog.compact_logs(5).await.unwrap(), 5);
    }

    let recovered = DurableCatalogStore::recover(store.clone(), "gaps")
        .await
        .unwrap();
    assert_eq!(mapping(&recovered, 6).await, expected);
    assert_eq!(recovered.allocate_id().await.unwrap(), 7);

    // Deleting by the ORIGINAL ID must not remove a neighboring workload.
    recovered
        .commit_txn(
            CatalogTxn::new(6, 106, vec![CatalogMutation::DeleteWorkload(WorkloadId(2))]).unwrap(),
        )
        .await
        .unwrap();
    let surviving = vec![None, None, None, None, None, Some(third)];
    assert_eq!(mapping(&recovered, 6).await, surviving);

    let second = DurableCatalogStore::recover(store.clone(), "gaps")
        .await
        .unwrap();
    assert_eq!(mapping(&second, 6).await, surviving);
    if snapshot {
        second.create_snapshot().await.unwrap();
        second.compact_logs(6).await.unwrap();
        let third_recovery = DurableCatalogStore::recover(store, "gaps").await.unwrap();
        assert_eq!(mapping(&third_recovery, 6).await, surviving);
    }
}

#[tokio::test]
async fn workload_gaps_and_deletes_survive_log_recovery() {
    assert_gaps_deletion_and_second_recovery(false).await;
}

#[tokio::test]
async fn workload_gaps_and_deletes_survive_snapshot_recovery() {
    assert_gaps_deletion_and_second_recovery(true).await;
}

#[tokio::test]
async fn workload_id_invariance_across_alter_and_restart() {
    let store = Arc::new(InMemory::new());
    let catalog = DurableCatalogStore::new(store.clone(), "alter");
    let original = workload("interactive", 2048, 2, 500, 9);
    let altered = workload("interactive", 4096, 4, 250, 5);
    catalog
        .commit_txn(CatalogTxn::new(1, 1, vec![CatalogMutation::PutWorkload(original)]).unwrap())
        .await
        .unwrap();
    catalog
        .commit_txn(
            CatalogTxn::new(2, 2, vec![CatalogMutation::PutWorkload(altered.clone())]).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(mapping(&catalog, 1).await, vec![Some(altered.clone())]);
    let recovered = DurableCatalogStore::recover(store, "alter").await.unwrap();
    assert_eq!(mapping(&recovered, 1).await, vec![Some(altered)]);
}

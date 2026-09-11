use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_catalog_store_lfs_lifecycle() {
    let tmp = tempdir().unwrap();
    let lfs_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());

    // Phase 1: Initialize store and commit multiple entities
    {
        let catalog = DurableCatalogStore::new(lfs_store.clone(), "");

        let db = CatalogDatabase {
            id: DatabaseId(1),
            name: "lfs_db".to_string(),
            default_namespace: "public".to_string(),
            created_at: 100,
        };
        let ns = CatalogNamespace {
            id: NamespaceId(1),
            name: "public".to_string(),
            database_id: DatabaseId(1),
            created_at: 100,
        };
        let tbl = CatalogTable {
            id: TableId(10),
            name: "lfs_table".to_string(),
            namespace_id: NamespaceId(1),
            columns: vec![CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
                ordinal: 0,
            }],
            pk_cols: vec!["id".to_string()],
        };
        let view = CatalogView {
            id: ViewId(20),
            name: "lfs_view".to_string(),
            namespace_id: NamespaceId(1),
            sql: "SELECT id FROM lfs_table".to_string(),
            compiled_plan_id: None,
            op_id: Some(1),
            columns: vec![],
        };

        let txn1 = CatalogTxn::new(
            1,
            1,
            vec![
                CatalogMutation::PutDatabase(db),
                CatalogMutation::PutNamespace(ns),
                CatalogMutation::PutTable(tbl),
                CatalogMutation::PutView(view),
            ],
        )
        .unwrap();

        catalog.commit_txn(txn1).await.unwrap();
        assert_eq!(catalog.get_revision().await, 1);

        // Create snapshot at revision 1
        catalog.create_snapshot().await.unwrap();

        // Mutate again (revision 2)
        let idx = CatalogIndexEntry {
            id: IndexId(30),
            name: "lfs_idx".to_string(),
            table_id: TableId(10),
            index_cols: vec!["id".to_string()],
            pk_cols: vec!["id".to_string()],
            state: CatalogIndexState::Ready,
            op_id: Some(2),
        };
        let txn2 = CatalogTxn::new(2, 2, vec![CatalogMutation::PutIndex(idx)]).unwrap();
        catalog.commit_txn(txn2).await.unwrap();
        assert_eq!(catalog.get_revision().await, 2);
    }

    // Phase 2: Simulate complete process termination and restart with a fresh store pointing to same directory
    {
        let new_lfs_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
        let recovered = DurableCatalogStore::recover(new_lfs_store, "")
            .await
            .unwrap();

        assert_eq!(recovered.get_revision().await, 2);

        let tbl = recovered.get_table(TableId(10)).await.unwrap().unwrap();
        assert_eq!(tbl.name, "lfs_table");

        let view = recovered.get_view(ViewId(20)).await.unwrap().unwrap();
        assert_eq!(view.name, "lfs_view");

        let idx = recovered.get_index(IndexId(30)).await.unwrap().unwrap();
        assert_eq!(idx.name, "lfs_idx");
        assert_eq!(idx.state, CatalogIndexState::Ready);
    }
}

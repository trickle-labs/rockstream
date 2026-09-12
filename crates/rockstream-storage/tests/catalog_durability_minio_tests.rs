//! MinIO catalog durability integration test (v0.63 durability commitment).
//!
//! Asserts that `DurableCatalogStore` correctly persists transactions, creates
//! snapshots, and recovers exact catalog metadata across node restarts using a real
//! S3-compatible MinIO backend via TestContainers.

use std::sync::Arc;

use rockstream_storage::catalog::*;
use rockstream_types::ids::*;

const MINIO_BUCKET: &str = "rockstream-catalog-minio-test";

#[tokio::test]
async fn test_catalog_store_minio_lifecycle() {
    let (_container, port) = match rockstream_test_support::minio::start_minio(MINIO_BUCKET).await {
        Some(m) => m,
        None => {
            eprintln!("SKIP test_catalog_store_minio_lifecycle: Docker not available");
            return;
        }
    };
    let minio_store = Arc::new(rockstream_test_support::minio::minio_object_store(
        port,
        MINIO_BUCKET,
    ));
    let prefix = "catalog_minio_lifecycle";

    // Phase 1: Initialize store and commit multiple entities
    {
        let catalog = DurableCatalogStore::new(minio_store.clone(), prefix);

        let db = CatalogDatabase {
            id: DatabaseId(1),
            name: "minio_db".to_string(),
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
            name: "minio_table".to_string(),
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
            name: "minio_view".to_string(),
            namespace_id: NamespaceId(1),
            sql: "SELECT id FROM minio_table".to_string(),
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
            name: "minio_idx".to_string(),
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

    // Phase 2: Simulate restart with a fresh store pointing to same prefix
    {
        let new_minio_store = Arc::new(rockstream_test_support::minio::minio_object_store(
            port,
            MINIO_BUCKET,
        ));
        let recovered = DurableCatalogStore::recover(new_minio_store, prefix)
            .await
            .unwrap();

        assert_eq!(recovered.get_revision().await, 2);

        let tbl = recovered.get_table(TableId(10)).await.unwrap().unwrap();
        assert_eq!(tbl.name, "minio_table");

        let view = recovered.get_view(ViewId(20)).await.unwrap().unwrap();
        assert_eq!(view.name, "minio_view");

        let idx = recovered.get_index(IndexId(30)).await.unwrap().unwrap();
        assert_eq!(idx.name, "minio_idx");
        assert_eq!(idx.state, CatalogIndexState::Ready);
    }
}

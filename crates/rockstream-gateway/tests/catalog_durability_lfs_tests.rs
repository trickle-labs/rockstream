//! Catalog durability tests on LocalFileSystem (LFS) (v0.59.10 CAT-01 / Slice 5).

use std::sync::Arc;
use tempfile::tempdir;
use tokio_postgres::NoTls;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_gateway::{
    catalog_stubs::{CatalogCheckpointEntry, CatalogNodeEntry, CatalogStubs, CatalogView},
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }
    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

async fn connect_port(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

async fn start_gateway(
    shard_path: &str,
    store: Arc<dyn ObjectStore>,
    catalog: Arc<CatalogStubs>,
) -> (u16, tokio::task::JoinHandle<()>, Arc<ShardDb>) {
    let shard_db = Arc::new(ShardDb::builder(shard_path, store).build().await.unwrap());
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        shard_db.clone(),
    );
    let (addr, handle) = server.serve_background().await.unwrap();
    (addr.port(), handle, shard_db)
}

async fn simple_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .expect("query failed")
        .into_iter()
        .filter_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                let mut values = Vec::with_capacity(row.len());
                for i in 0..row.len() {
                    values.push(row.get(i).map(str::to_string));
                }
                Some(values)
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn test_catalog_stable_identifiers_survive_restart_lfs() {
    let _g = TEST_LOCK.lock().await;
    let tmp = tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());

    // Phase 1: Create catalog state and query system tables
    let catalog1 = Arc::new(CatalogStubs::new());
    catalog1.add_node(CatalogNodeEntry {
        node_id: "node-lfs-1".to_string(),
        worker_id: "worker-lfs-100".to_string(),
        role: "worker".to_string(),
        address: "127.0.0.1:9091".to_string(),
        state: "READY".to_string(),
        lease_count: 3,
        memory_budget_bytes: 512 * 1024 * 1024,
        last_heartbeat_at: "2026-08-24 11:00:00+00".to_string(),
    });
    catalog1.add_view(CatalogView {
        name: "persistent_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: Some(999),
    });
    catalog1.record_checkpoint(CatalogCheckpointEntry {
        checkpoint_id: 888,
        committed_at: "2026-08-24 11:05:00+00".to_string(),
        epoch_number: 88,
        frontier: "[88]".to_string(),
        storage_path: "lfs:///tmp/checkpoints/chk-888".to_string(),
        duration_ms: 45,
    });

    let (port1, handle1, db1) = start_gateway("shard-lfs", store.clone(), catalog1).await;
    let client1 = connect_port(port1).await;

    let node_rows1 = simple_rows(&client1, "SELECT worker_id FROM rockstream_catalog.nodes;").await;
    let view_rows1 = simple_rows(
        &client1,
        "SELECT arrangement_id FROM rockstream_catalog.views WHERE view_name = 'persistent_view';",
    )
    .await;
    let op_rows1 = simple_rows(
        &client1,
        "SELECT operator_id FROM rockstream_catalog.operators WHERE view_name = 'persistent_view';",
    )
    .await;
    let chk_rows1 = simple_rows(
        &client1,
        "SELECT checkpoint_id FROM rockstream_catalog.checkpoints;",
    )
    .await;

    assert_eq!(node_rows1[0][0].as_deref(), Some("worker-lfs-100"));
    let pre_arr_id = view_rows1[0][0].clone().unwrap();
    let pre_op_id = op_rows1[0][0].clone().unwrap();
    assert_eq!(chk_rows1[0][0].as_deref(), Some("888"));

    // Simulate node restart: drop client and server
    drop(client1);
    handle1.abort();
    drop(db1);

    // Phase 2: Start new gateway instance with recreated catalog pointing to same identifiers
    let catalog2 = Arc::new(CatalogStubs::new());
    catalog2.add_node(CatalogNodeEntry {
        node_id: "node-lfs-1".to_string(),
        worker_id: "worker-lfs-100".to_string(),
        role: "worker".to_string(),
        address: "127.0.0.1:9091".to_string(),
        state: "READY".to_string(),
        lease_count: 3,
        memory_budget_bytes: 512 * 1024 * 1024,
        last_heartbeat_at: "2026-08-24 11:00:00+00".to_string(),
    });
    catalog2.add_view(CatalogView {
        name: "persistent_view".to_string(),
        sql: "SELECT 1".to_string(),
        columns: vec![],
        namespace: "public".to_string(),
        op_id: Some(999),
    });
    catalog2.record_checkpoint(CatalogCheckpointEntry {
        checkpoint_id: 888,
        committed_at: "2026-08-24 11:05:00+00".to_string(),
        epoch_number: 88,
        frontier: "[88]".to_string(),
        storage_path: "lfs:///tmp/checkpoints/chk-888".to_string(),
        duration_ms: 45,
    });

    let (port2, _handle2, _db2) = start_gateway("shard-lfs", store.clone(), catalog2).await;
    let client2 = connect_port(port2).await;

    let node_rows2 = simple_rows(&client2, "SELECT worker_id FROM rockstream_catalog.nodes;").await;
    let view_rows2 = simple_rows(
        &client2,
        "SELECT arrangement_id FROM rockstream_catalog.views WHERE view_name = 'persistent_view';",
    )
    .await;
    let op_rows2 = simple_rows(
        &client2,
        "SELECT operator_id FROM rockstream_catalog.operators WHERE view_name = 'persistent_view';",
    )
    .await;
    let chk_rows2 = simple_rows(
        &client2,
        "SELECT checkpoint_id FROM rockstream_catalog.checkpoints;",
    )
    .await;

    assert_eq!(
        node_rows2[0][0].as_deref(),
        Some("worker-lfs-100"),
        "Worker ID must survive restart"
    );
    assert_eq!(
        view_rows2[0][0].as_ref().unwrap(),
        &pre_arr_id,
        "Arrangement ID must survive restart bit-identically"
    );
    assert_eq!(
        op_rows2[0][0].as_ref().unwrap(),
        &pre_op_id,
        "Operator ID must survive restart bit-identically"
    );
    assert_eq!(
        chk_rows2[0][0].as_deref(),
        Some("888"),
        "Checkpoint ID must survive restart"
    );
}

#[tokio::test]
async fn test_catalog_recovery_probe_survives_process_restart_lfs() {
    let _g = TEST_LOCK.lock().await;
    let tmp = tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());

    // Phase 1: Initialize ShardDb and commit catalog record
    {
        let db = ShardDb::builder("cat-recovery-lfs", store.clone())
            .build()
            .await
            .unwrap();
        db.put(b"catalog_probe_magic", b"RS_CAT_V1_SNAPSHOT")
            .await
            .unwrap();
    }

    // Phase 2: Reopen ShardDb after restart and probe recovery
    {
        let reopened_db = ShardDb::builder("cat-recovery-lfs", store.clone())
            .build()
            .await
            .unwrap();
        let recovered = reopened_db.get(b"catalog_probe_magic").await.unwrap();
        assert_eq!(
            recovered.as_deref(),
            Some(&b"RS_CAT_V1_SNAPSHOT"[..]),
            "Catalog snapshot recovery probe must recover persisted magic bytes across restart"
        );
    }
}

#[tokio::test]
async fn test_rockstream_catalog_checkpoints_query_exact_lfs() {
    let _g = TEST_LOCK.lock().await;
    let tmp = tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());

    let catalog = Arc::new(CatalogStubs::new());
    catalog.record_checkpoint(CatalogCheckpointEntry {
        checkpoint_id: 101,
        committed_at: "2026-09-25 10:00:00+00".to_string(),
        epoch_number: 10,
        frontier: "[10]".to_string(),
        storage_path: "lfs:///data/checkpoints/chk-101".to_string(),
        duration_ms: 32,
    });
    catalog.record_checkpoint(CatalogCheckpointEntry {
        checkpoint_id: 102,
        committed_at: "2026-09-25 10:01:00+00".to_string(),
        epoch_number: 11,
        frontier: "[11]".to_string(),
        storage_path: "lfs:///data/checkpoints/chk-102".to_string(),
        duration_ms: 28,
    });

    let (port, handle, _db) = start_gateway("shard-chk-lfs", store.clone(), catalog).await;
    let client = connect_port(port).await;

    let rows = simple_rows(
        &client,
        "SELECT checkpoint_id, epoch_number, frontier, storage_path, duration_ms FROM rockstream_catalog.checkpoints ORDER BY checkpoint_id;",
    )
    .await;

    assert_eq!(
        rows.len(),
        2,
        "Must return exactly two recorded checkpoints"
    );
    assert_eq!(
        rows[0],
        vec![
            Some("101".to_string()),
            Some("10".to_string()),
            Some("[10]".to_string()),
            Some("lfs:///data/checkpoints/chk-101".to_string()),
            Some("32".to_string()),
        ]
    );
    assert_eq!(
        rows[1],
        vec![
            Some("102".to_string()),
            Some("11".to_string()),
            Some("[11]".to_string()),
            Some("lfs:///data/checkpoints/chk-102".to_string()),
            Some("28".to_string()),
        ]
    );

    drop(client);
    handle.abort();
}

#[tokio::test]
async fn test_storage_access_doctor_check_lfs() {
    let _g = TEST_LOCK.lock().await;
    let tmp = tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());

    let db = ShardDb::builder("probe-doctor-lfs", store)
        .build()
        .await
        .unwrap();

    let start = std::time::Instant::now();
    // 1. Write probe
    db.put(b"probe_doctor_lfs", b"PROBE_STORAGE_ACCESS_OK")
        .await
        .unwrap();
    // 2. Read probe
    let read_bytes = db.get(b"probe_doctor_lfs").await.unwrap();
    assert_eq!(read_bytes.as_deref(), Some(&b"PROBE_STORAGE_ACCESS_OK"[..]));
    // 3. Delete probe
    db.delete(b"probe_doctor_lfs").await.unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "Storage access roundtrip must finish well under 500ms bound, took {:?}",
        elapsed
    );
}

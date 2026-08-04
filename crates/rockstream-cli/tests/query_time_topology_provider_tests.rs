use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_cli::{start_gateway_with_shard, StartOptions};
use rockstream_storage::{ShardDb, ShardKeyEncoder};
use rockstream_types::config::RockstreamConfig;
use rockstream_types::topology::{WorkerCapabilities, WorkerLocation};
use tokio_postgres::NoTls;

fn options(storage: &std::path::Path, extra_shards: Vec<std::path::PathBuf>) -> StartOptions {
    StartOptions {
        storage: storage.to_path_buf(),
        role: "gateway".to_string(),
        control: None,
        auth_mode: "off".to_string(),
        worker_location: WorkerLocation::default(),
        worker_capabilities: WorkerCapabilities::default(),
        config: RockstreamConfig::default(),
        metrics_addr: None,
        listen_addr: Some("127.0.0.1:0".to_string()),
        raft_peers: None,
        raft_node_id: None,
        raft_bind: None,
        raft_bootstrap: false,
        daemon: false,
        control_bind: None,
        control_shared_storage: None,
        query_time_shard_dirs: extra_shards,
    }
}

async fn seed_shard(
    root: &std::path::Path,
    id: i64,
    frontier: u64,
) -> (Arc<ShardDb>, Arc<dyn ObjectStore>) {
    std::fs::create_dir_all(root).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
    let db = Arc::new(ShardDb::builder("db", store.clone()).build().await.unwrap());
    db.put(
        format!("view_output/sales/{id:02}").as_bytes(),
        format!("{id}\tregion-{id}").as_bytes(),
    )
    .await
    .unwrap();
    db.put(&ShardKeyEncoder::frontier_key(), &frontier.to_be_bytes())
        .await
        .unwrap();
    db.flush().await.unwrap();
    (db, store)
}

async fn connect(address: std::net::SocketAddr) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn cli_topology_provider_refreshes_all_owning_shards_at_one_frontier() {
    let root = tempfile::tempdir().unwrap();
    let local_root = root.path().join("shard-0");
    let shard_one_root = root.path().join("shard-1");
    let shard_two_root = root.path().join("shard-2");
    let (local_db, local_store) = seed_shard(&local_root, 1, 17).await;
    let _shard_one = seed_shard(&shard_one_root, 2, 17).await;
    let _shard_two = seed_shard(&shard_two_root, 3, 17).await;
    let (address, _handle) = start_gateway_with_shard(
        &options(root.path(), vec![shard_one_root, shard_two_root]),
        local_db,
        local_store,
        "db",
    )
    .await
    .unwrap();
    let client = connect(address).await;
    client
        .simple_query("CREATE TABLE sales (id BIGINT, region TEXT)")
        .await
        .unwrap();
    let rows: Vec<Vec<String>> = client
        .simple_query("SELECT id, region FROM sales WHERE id >= 1 ORDER BY id")
        .await
        .unwrap()
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(vec![
                row.get("id").unwrap().to_string(),
                row.get("region").unwrap().to_string(),
            ]),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            vec![String::from("1"), String::from("region-1")],
            vec![String::from("2"), String::from("region-2")],
            vec![String::from("3"), String::from("region-3")],
        ],
    );
}

#[tokio::test]
async fn cli_topology_provider_rejects_mismatched_frontiers() {
    let root = tempfile::tempdir().unwrap();
    let local_root = root.path().join("shard-0");
    let stale_root = root.path().join("shard-1");
    let (local_db, local_store) = seed_shard(&local_root, 1, 17).await;
    let _stale = seed_shard(&stale_root, 2, 16).await;
    let (address, _handle) = start_gateway_with_shard(
        &options(root.path(), vec![stale_root]),
        local_db,
        local_store,
        "db",
    )
    .await
    .unwrap();
    let client = connect(address).await;
    client
        .simple_query("CREATE TABLE sales (id BIGINT, region TEXT)")
        .await
        .unwrap();
    let error = client
        .simple_query("SELECT id FROM sales WHERE id >= 1")
        .await
        .unwrap_err();
    let message = error.as_db_error().expect("pgwire ErrorResponse").message();
    assert!(message.contains("RS-2030"), "unexpected error: {message}");
    assert!(
        message.contains("next_steps"),
        "unexpected error: {message}"
    );
}

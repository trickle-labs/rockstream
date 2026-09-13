use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use rockstream_cli::output::{
    ManagementClusterStatusInfo, ManagementNodeInfo, ManagementOperationInfo,
};
use rockstream_control::{CheckpointExportOutcome, CheckpointExportService};
use rockstream_management_proto::v1::{
    management_service_client::ManagementServiceClient, CreateBackupRequest,
    Operation as WireOperation,
};
use rockstream_storage::build_migration_object_store;
use rockstream_types::checkpoint::ClusterCheckpoint;
use rockstream_types::ids::ShardId;
use std::sync::Arc;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

struct RunningNode(Child);

impl Drop for RunningNode {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("read ephemeral port")
}

fn start_embedded(
    binary: &Path,
    storage: &Path,
    listen_addr: SocketAddr,
    management_addr: Option<SocketAddr>,
) -> RunningNode {
    let mut args = vec![
        "start".to_owned(),
        "--storage".to_owned(),
        storage.display().to_string(),
        "--role".to_owned(),
        "all".to_owned(),
        "--auth".to_owned(),
        "off".to_owned(),
        "--listen".to_owned(),
        listen_addr.to_string(),
    ];
    if let Some(addr) = management_addr {
        args.extend(["--management-addr".to_owned(), addr.to_string()]);
    }
    RunningNode(
        Command::new(binary)
            .env("RUST_LOG", "off")
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start embedded role-all node"),
    )
}

async fn connect_gateway(addr: SocketAddr) -> Client {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match tokio_postgres::connect(
            &format!(
                "host={} port={} user=rockstream dbname=rockstream",
                addr.ip(),
                addr.port()
            ),
            NoTls,
        )
        .await
        {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return client;
            }
            Err(error) => assert!(
                Instant::now() < deadline,
                "embedded gateway did not accept PostgreSQL connections: {error}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn seed_backup_value(storage: &Path) {
    std::fs::create_dir_all(storage).expect("create source storage");
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(storage)
            .expect("open source local object store"),
    );
    let shard_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::prefix::PrefixStore::new(store, "shards/0"));
    let db = rockstream_storage::ShardDb::builder("db", shard_store)
        .build()
        .await
        .expect("open shard 0 before starting worker");
    db.put(b"backup-test-key", b"durable-value")
        .await
        .expect("seed exact shard value");
    db.flush().await.expect("flush exact shard value");
    db.close().await.expect("close seed shard database");
}

fn run_backup_create(binary: &Path, management: SocketAddr, destination: &Path) -> Output {
    Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "json",
            "--management",
            &management.to_string(),
            "admin",
            "backup",
            "create",
            destination.to_str().expect("backup path is UTF-8"),
        ])
        .output()
        .expect("run management backup create")
}

fn run_operation_show(binary: &Path, management: SocketAddr, operation_id: &str) -> Output {
    Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "json",
            "--management",
            &management.to_string(),
            "admin",
            "operation",
            "show",
            operation_id,
        ])
        .output()
        .expect("run management operation show")
}

fn run_json_status(binary: &Path, management: SocketAddr) -> Output {
    Command::new(binary)
        .env("RUST_LOG", "off")
        .args([
            "--output",
            "json",
            "--management",
            &management.to_string(),
            "status",
        ])
        .output()
        .expect("run management cluster status")
}

fn management_operation(operation: WireOperation) -> ManagementOperationInfo {
    ManagementOperationInfo {
        operation_id: operation.operation_id,
        kind: operation.kind,
        state: operation.state,
        started_at: operation.started_at,
        updated_at: operation.updated_at,
        progress: operation.progress,
        phase: operation.phase,
        error_code: operation.error_code,
        next_steps: operation.next_steps,
        source_version: operation.source_version,
    }
}

async fn submit_backup_request(
    management: SocketAddr,
    destination: &Path,
    idempotency_key: &str,
) -> ManagementOperationInfo {
    let mut client = ManagementServiceClient::connect(format!("http://{management}"))
        .await
        .expect("connect to management RPC");
    let response = client
        .create_backup(CreateBackupRequest {
            protocol_version: 1,
            destination: destination.display().to_string(),
            idempotency_key: idempotency_key.to_owned(),
        })
        .await
        .expect("submit management backup request")
        .into_inner();
    assert_eq!(response.protocol_version, 1);
    management_operation(response.operation.expect("backup operation response"))
}

fn expected_operation(state: &str, progress: &str, phase: &str) -> ManagementOperationInfo {
    ManagementOperationInfo {
        operation_id: "<uuid>".to_owned(),
        kind: "create_backup".to_owned(),
        state: state.to_owned(),
        started_at: "<timestamp>".to_owned(),
        updated_at: "<timestamp>".to_owned(),
        progress: progress.to_owned(),
        phase: phase.to_owned(),
        error_code: String::new(),
        next_steps: Vec::new(),
        source_version: "operation-record:2".to_owned(),
    }
}

fn timestamp_is_utc_millis(value: &str) -> bool {
    value.len() == 24
        && value.as_bytes()[4] == b'-'
        && value.as_bytes()[7] == b'-'
        && value.as_bytes()[10] == b'T'
        && value.as_bytes()[13] == b':'
        && value.as_bytes()[16] == b':'
        && value.as_bytes()[19] == b'.'
        && value.ends_with('Z')
        && value.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 4 | 7 | 10 | 13 | 16 | 19 | 23) || byte.is_ascii_digit()
        })
}

fn normalize_operation(mut operation: ManagementOperationInfo) -> ManagementOperationInfo {
    assert!(uuid::Uuid::parse_str(&operation.operation_id).is_ok());
    assert!(timestamp_is_utc_millis(&operation.started_at));
    assert!(timestamp_is_utc_millis(&operation.updated_at));
    operation.operation_id = "<uuid>".to_owned();
    operation.started_at = "<timestamp>".to_owned();
    operation.updated_at = "<timestamp>".to_owned();
    operation
}

fn assert_operation_transcript(
    output: &Output,
    expected: &ManagementOperationInfo,
) -> ManagementOperationInfo {
    assert!(
        output.status.success(),
        "management CLI failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stderr, b"");
    let transcript = String::from_utf8(output.stdout.clone()).expect("CLI JSON is UTF-8");
    let operation: ManagementOperationInfo =
        serde_json::from_slice(&output.stdout).expect("management operation JSON");
    assert_eq!(
        transcript,
        format!("{}\n", serde_json::to_string_pretty(&operation).unwrap()),
        "stdout must contain only the complete pretty-printed JSON transcript"
    );
    let operation_id = operation.operation_id.clone();
    let started_at = operation.started_at.clone();
    let updated_at = operation.updated_at.clone();
    let normalized = normalize_operation(operation.clone());
    assert_eq!(
        transcript
            .replace(&operation_id, "<uuid>")
            .replace(&started_at, "<timestamp>")
            .replace(&updated_at, "<timestamp>"),
        format!("{}\n", serde_json::to_string_pretty(&normalized).unwrap()),
        "only UUID and timestamp values may vary in the full JSON transcript"
    );
    assert_eq!(&normalized, expected);
    operation
}

fn assert_status_transcript(output: &Output, retained_operations: u32) {
    assert!(
        output.status.success(),
        "management status failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stderr, b"");
    let transcript = String::from_utf8(output.stdout.clone()).expect("status JSON is UTF-8");
    let mut status: ManagementClusterStatusInfo =
        serde_json::from_slice(&output.stdout).expect("management cluster status JSON");
    assert_eq!(
        transcript,
        format!("{}\n", serde_json::to_string_pretty(&status).unwrap())
    );
    assert!(timestamp_is_utc_millis(&status.observed_at));
    assert!(status.source_version.starts_with("topology:"));
    assert_eq!(status.nodes.len(), 1);
    assert!(status.nodes[0].registered_at_ms > 0);
    assert_eq!(status.nodes[0].source_version, status.source_version);

    status.observed_at = "<timestamp>".to_owned();
    status.source_version = "<source-revision>".to_owned();
    for node in &mut status.nodes {
        node.registered_at_ms = 0;
        node.source_version = "<source-revision>".to_owned();
    }
    assert_eq!(
        status,
        ManagementClusterStatusInfo {
            observed_at: "<timestamp>".to_owned(),
            source_version: "<source-revision>".to_owned(),
            state: "healthy".to_owned(),
            nodes: vec![ManagementNodeInfo {
                node_id: 1,
                role: "worker".to_owned(),
                address: "127.0.0.1:0".to_owned(),
                state: "active".to_owned(),
                capacity_headroom: 1.0,
                host_id: String::new(),
                availability_zone: String::new(),
                healthy: true,
                lifecycle_state: "active".to_owned(),
                registered_at_ms: 0,
                source_version: "<source-revision>".to_owned(),
            }],
            active_operations: 0,
            retained_operations,
            request_fill: 1,
        }
    );
}

async fn wait_for_backup_success(
    binary: &Path,
    management: SocketAddr,
    operation_id: &str,
) -> ManagementOperationInfo {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let output = run_operation_show(binary, management, operation_id);
        assert!(
            output.status.success(),
            "operation show failed: stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let operation: ManagementOperationInfo =
            serde_json::from_slice(&output.stdout).expect("operation show JSON");
        assert_eq!(operation.operation_id, operation_id);
        assert_eq!(operation.kind, "create_backup");
        assert_eq!(operation.source_version, "operation-record:2");
        let expected = match operation.state.as_str() {
            "pending" => expected_operation("pending", "", ""),
            "running" if operation.phase.starts_with("backup_checkpoint:") => {
                expected_operation("running", "5%", &operation.phase)
            }
            "running" if operation.phase.starts_with("backup_exporting:") => {
                expected_operation("running", "70%", &operation.phase)
            }
            "succeeded" => expected_operation("succeeded", "100%", "completed"),
            "failed" | "cancelled" | "waiting" => {
                let transcript = String::from_utf8_lossy(&output.stdout);
                panic!("backup operation ended unexpectedly: {transcript}")
            }
            other => panic!("unexpected backup operation state {other:?}"),
        };
        let verified = assert_operation_transcript(&output, &expected);
        if verified.state == "succeeded" {
            return verified;
        }
        assert!(Instant::now() < deadline, "backup operation did not finish");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_registered_worker(
    binary: &Path,
    management: SocketAddr,
    retained_operations: u32,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let output = run_json_status(binary, management);
        if output.status.success()
            && serde_json::from_slice::<ManagementClusterStatusInfo>(&output.stdout)
                .is_ok_and(|status| status.nodes.len() == 1)
        {
            assert_status_transcript(&output, retained_operations);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "worker did not register after restart: stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn release_management_backup_commits_and_restores_real_shard_data() {
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let root = TempDir::new().expect("create backup fixture");
    let storage = root.path().join("source-storage");
    let destination = root.path().join("backup");
    let restored_storage = root.path().join("restored-storage");
    let listen_addr = free_addr();
    let management_addr = free_addr();
    seed_backup_value(&storage).await;
    let node = start_embedded(binary, &storage, listen_addr, Some(management_addr));

    let client = connect_gateway(listen_addr).await;
    client
        .simple_query(
            "CREATE TABLE v066_backup_rows (id BIGINT PRIMARY KEY, payload BIGINT NOT NULL)",
        )
        .await
        .expect("create backup source table");
    client
        .simple_query("INSERT INTO v066_backup_rows VALUES (1, 662066)")
        .await
        .expect("insert backup source row");
    let rows = client
        .simple_query("SELECT id, payload FROM v066_backup_rows")
        .await
        .expect("read backup source row");
    let values: Vec<_> = rows
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some((row.get(0).unwrap(), row.get(1).unwrap())),
            _ => None,
        })
        .collect();
    assert_eq!(values, [("1", "662066")]);

    assert_status_transcript(&run_json_status(binary, management_addr), 0);

    let created = run_backup_create(binary, management_addr, &destination);
    let create_response =
        assert_operation_transcript(&created, &expected_operation("pending", "", ""));
    let operation_id = create_response.operation_id.clone();
    assert!(uuid::Uuid::parse_str(&operation_id).is_ok());

    let terminal = wait_for_backup_success(binary, management_addr, &operation_id).await;
    assert_eq!(terminal.state, "succeeded");

    let generation = format!("management-{operation_id}");
    let destination_store = build_migration_object_store(destination.to_str().unwrap())
        .expect("open local backup object store");
    let export = CheckpointExportService::new();
    let outcome = export
        .validate_generation(destination_store.clone(), &generation)
        .await
        .expect("validate committed backup generation and every payload");
    assert_eq!(outcome.status, "SUCCESS");
    assert_eq!(outcome.generation, generation);
    assert!(outcome.object_count > 0);

    let generation_dir = destination.join("checkpoint-exports").join(&generation);
    let generation_record: serde_json::Value = serde_json::from_slice(
        &std::fs::read(generation_dir.join("generation")).expect("committed generation record"),
    )
    .expect("generation record JSON");
    assert_eq!(generation_record["generation"], generation);
    assert_eq!(
        generation_record["object_count"].as_u64(),
        Some(outcome.object_count)
    );
    let commit: serde_json::Value = serde_json::from_slice(
        &std::fs::read(generation_dir.join("commit")).expect("terminal commit marker"),
    )
    .expect("commit marker JSON");
    let committed_outcome: CheckpointExportOutcome =
        serde_json::from_value(commit["outcome"].clone()).expect("committed outcome");
    assert_eq!(committed_outcome, outcome);

    let shard_inventory = (0..outcome.object_count).any(|index| {
        let record_path = generation_dir
            .join("inventory")
            .join(format!("{index:020}"));
        let Ok(bytes) = std::fs::read(record_path) else {
            return false;
        };
        let Ok(record) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            return false;
        };
        record["source"]
            .as_str()
            .is_some_and(|source| source.starts_with("shards/0/db/"))
            && record["byte_len"].as_u64().is_some_and(|length| length > 0)
    });
    assert!(
        shard_inventory,
        "backup must include non-empty shard 0 data"
    );

    assert_status_transcript(&run_json_status(binary, management_addr), 1);

    let restored_store = build_migration_object_store(restored_storage.to_str().unwrap())
        .expect("open restored local object store");
    let restored = export
        .restore_generation(destination_store, restored_store.clone(), &generation)
        .await
        .expect("restore the committed backup generation");
    assert_eq!(restored.checkpoint_id, outcome.checkpoint_id);
    assert_eq!(restored.generation, generation);
    assert_eq!(restored.restored_shards, 1);
    drop(client);
    drop(node);
    let checkpoint: ClusterCheckpoint =
        serde_json::from_value(generation_record["checkpoint"].clone())
            .expect("committed checkpoint record");
    let snapshot_id = checkpoint.shards[&ShardId(0)]
        .snapshot_id
        .as_deref()
        .expect("shard checkpoint pins an exact SlateDB snapshot");
    let restored_shard_store: Arc<dyn object_store::ObjectStore> = Arc::new(
        object_store::prefix::PrefixStore::new(restored_store, "shards/0"),
    );
    let snapshot_reader = rockstream_storage::ShardReader::open_with_snapshot_id(
        "db",
        restored_shard_store,
        snapshot_id,
    )
    .await
    .expect("open restored exact shard snapshot through production reader");
    assert_eq!(
        snapshot_reader
            .get(b"backup-test-key")
            .await
            .expect("read restored key")
            .expect("restored key exists")
            .as_ref(),
        b"durable-value"
    );
}

#[tokio::test]
async fn release_management_backup_idempotency_survives_role_all_restart() {
    let binary = Path::new(env!("CARGO_BIN_EXE_rockstream"));
    let root = TempDir::new().expect("create restart fixture");
    let storage = root.path().join("source-storage");
    let destination = root.path().join("backup");
    let idempotency_key = "v066-backup-restart-idempotency-key";
    let first_listen = free_addr();
    let first_management = free_addr();
    seed_backup_value(&storage).await;

    let first_node = start_embedded(binary, &storage, first_listen, Some(first_management));
    let first_client = connect_gateway(first_listen).await;
    let accepted = submit_backup_request(first_management, &destination, idempotency_key).await;
    assert_eq!(
        normalize_operation(accepted.clone()),
        expected_operation("pending", "", "")
    );
    let operation_id = accepted.operation_id.clone();
    let completed = wait_for_backup_success(binary, first_management, &operation_id).await;
    assert_eq!(completed.state, "succeeded");
    drop(first_client);
    drop(first_node);

    let second_listen = free_addr();
    let second_management = free_addr();
    let second_node = start_embedded(binary, &storage, second_listen, Some(second_management));
    let second_client = connect_gateway(second_listen).await;
    wait_for_registered_worker(binary, second_management, 1).await;

    let retried = submit_backup_request(second_management, &destination, idempotency_key).await;
    assert_eq!(
        retried, completed,
        "same idempotency key must return the exact durable record"
    );
    let shown = run_operation_show(binary, second_management, &operation_id);
    let after_restart = assert_operation_transcript(
        &shown,
        &expected_operation("succeeded", "100%", "completed"),
    );
    assert_eq!(after_restart, completed);

    let generation = format!("management-{operation_id}");
    let generation_root = destination.join("checkpoint-exports");
    let mut generations: Vec<_> = std::fs::read_dir(&generation_root)
        .expect("list committed backup generations")
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    generations.sort();
    assert_eq!(generations.as_slice(), std::slice::from_ref(&generation));
    assert!(generation_root.join(&generation).join("commit").is_file());
    let destination_store = build_migration_object_store(destination.to_str().unwrap())
        .expect("open local backup object store");
    let outcome = CheckpointExportService::new()
        .validate_generation(destination_store, &generation)
        .await
        .expect("validate the single committed generation after retry");
    assert_eq!(outcome.generation, generation);
    assert_eq!(outcome.status, "SUCCESS");
    drop(second_client);
    drop(second_node);
}

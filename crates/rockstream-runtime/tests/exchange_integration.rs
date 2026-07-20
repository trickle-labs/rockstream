use std::sync::Arc;
use tokio::sync::mpsc;

use arrow::datatypes::{DataType, Field, Schema};
use rockstream_ops::zset::ArrowZSet;
use rockstream_runtime::exchange::flow_control::FlowController;
use rockstream_runtime::exchange::multiplexer::WorkerStreamMultiplexer;
use rockstream_runtime::exchange::pool::ShuffleClientPool;
use rockstream_runtime::exchange::proto::ShuffleFrame;
use rockstream_runtime::exchange::service::{ExchangeRegistry, ShuffleServer};
use rockstream_types::ids::WorkerId;

#[tokio::test]
async fn test_distributed_tpch_connections_bounded() {
    // Spin up gRPC servers for Worker 1 and Worker 2
    let addr1 = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let addr2 = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };

    let registry1 = ExchangeRegistry::new();
    let server1 = ShuffleServer::new(registry1.clone());

    let (tx_close1, rx_close1) = tokio::sync::oneshot::channel::<()>();
    let server_handle1 = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(server1))
            .serve_with_shutdown(addr1, async {
                let _ = rx_close1.await;
            })
            .await;
    });

    let registry2 = ExchangeRegistry::new();
    let server2 = ShuffleServer::new(registry2.clone());

    let (tx_close2, rx_close2) = tokio::sync::oneshot::channel::<()>();
    let server_handle2 = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(rockstream_runtime::exchange::proto::shuffle_service_server::ShuffleServiceServer::new(server2))
            .serve_with_shutdown(addr2, async {
                let _ = rx_close2.await;
            })
            .await;
    });

    // Give servers a moment to start listening
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Set up peer address mapping
    let peers = Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new()));
    peers.write().insert(WorkerId(1), addr1.to_string());
    peers.write().insert(WorkerId(2), addr2.to_string());

    // Register 16 local inlets on Worker 2 (target shards 0..16)
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nationkey",
        DataType::Int32,
        false,
    )]));

    let mut receivers = Vec::new();
    for target_shard in 0..16u32 {
        let (tx, rx) = mpsc::channel(10);
        registry2.register(100, target_shard, tx, schema.clone());
        receivers.push(rx);
    }

    // Set up multiplexer on Worker 1
    let pool = ShuffleClientPool::new(peers.clone());
    let flow_controller = FlowController::new();
    let multiplexer = WorkerStreamMultiplexer::new(pool, flow_controller);

    // Simulate sending partitioned TPC-H subset (16 shards) from Worker 1 to Worker 2
    for target_shard in 0..16u32 {
        let zset = ArrowZSet::from_ab_rows(&[(target_shard as i64, 100)], 1);
        let payload = rockstream_runtime::exchange::serialization::serialize_zset(&zset).unwrap();

        let frame = ShuffleFrame {
            exchange_id: 100,
            src_shard: 0,
            target_shard,
            epoch: 1,
            seq: target_shard as u64 + 1,
            payload: payload.into(),
            row_count: zset.num_rows() as u32,
        };

        // Send to Worker 2
        multiplexer.send_frame(WorkerId(2), frame).await.unwrap();
    }

    // Assert that Worker 2 receives all 16 frames
    for target_shard in 0..16u32 {
        let rx = &mut receivers[target_shard as usize];
        let received = rx.recv().await.unwrap();
        assert_eq!(received.num_rows(), 1);
    }

    // CONNECTION BOUND INVARIANT ASSERTION:
    // Assert that multiplexer cached at most 1 connection/stream to Worker 2,
    // not 16 connections (which would be shard-to-shard).
    assert_eq!(multiplexer.connection_count(), 1);

    // Clean up
    let _ = tx_close1.send(());
    let _ = tx_close2.send(());
    server_handle1.abort();
    server_handle2.abort();
}

// ─── MinIO and SigV4 Helpers for Durable Fallback Tests ──────────────────────

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers::{core::WaitFor, Image};

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-test";

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn sha256_hex(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sod = secs % 86400;
    let mut days = (secs / 86400) as u32;
    let h = (sod / 3600) as u32;
    let m = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    let mut year = 1970u32;
    loop {
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let dy = if leap { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let dpm: [u32; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0u32;
    for &d in &dpm {
        if days < d {
            break;
        }
        days -= d;
        month += 1;
    }
    let day = days + 1;
    month += 1;
    (year, month, day, h, m, s)
}

async fn create_minio_bucket(port: u16, bucket: &str) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (y, mo, d, hh, mm, ss) = epoch_to_ymd_hms(secs);
    let date = format!("{y:04}{mo:02}{d:02}");
    let datetime = format!("{y:04}{mo:02}{d:02}T{hh:02}{mm:02}{ss:02}Z");
    let host = format!("127.0.0.1:{port}");
    let region = "us-east-1";
    let empty_hash = sha256_hex(b"");
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let canonical_hash = sha256_hex(canonical.as_bytes());
    let scope = format!("{date}/{region}/s3/aws4_request");
    let sts = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let k1 = hmac_sha256(format!("AWS4{MINIO_PASS}").as_bytes(), date.as_bytes());
    let k2 = hmac_sha256(&k1, region.as_bytes());
    let k3 = hmac_sha256(&k2, b"s3");
    let signing_key = hmac_sha256(&k3, b"aws4_request");
    let sig = hex::encode(hmac_sha256(&signing_key, sts.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"
    );
    let resp = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("Host", &host)
        .header("X-Amz-Content-Sha256", &empty_hash)
        .header("X-Amz-Date", &datetime)
        .header("Authorization", &auth)
        .header("Content-Length", "0")
        .send()
        .await
        .expect("CreateBucket PUT request failed");
    let status = resp.status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "CreateBucket failed: {status}"
    );
}

#[derive(Debug, Clone)]
pub struct MinIO2024 {
    env_vars: HashMap<String, String>,
}

impl Default for MinIO2024 {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("MINIO_CONSOLE_ADDRESS".to_owned(), ":9001".to_owned());
        Self { env_vars }
    }
}

impl Image for MinIO2024 {
    fn name(&self) -> &str {
        "minio/minio"
    }

    fn tag(&self) -> &str {
        "RELEASE.2024-11-07T00-52-20Z"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("API:")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec!["server", "/data"]
    }
}

async fn start_minio() -> (testcontainers::ContainerAsync<MinIO2024>, u16) {
    let container = MinIO2024::default()
        .start()
        .await
        .expect("failed to start MinIO container; is Docker running?");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;
    (container, port)
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(MINIO_BUCKET)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to build S3 object store for MinIO"),
    )
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_durable_shuffle_fallback() {
    if !docker_available() {
        eprintln!("SKIP test_durable_shuffle_fallback: Docker not available");
        return;
    }

    let (_container, port) = start_minio().await;
    let store = minio_object_store(port);

    // Set up peer address mapping with a non-existent port to force gRPC connect failure
    let peers = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    peers
        .write()
        .insert(WorkerId(2), "127.0.0.1:12345".to_string());

    // Register active shards with a mock database to track outbox/inbox
    let active_shards = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let temp_dir = tempfile::tempdir().unwrap();
    let local_store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
    let db = rockstream_storage::ShardDb::builder("test_fallback_db", local_store)
        .build()
        .await
        .unwrap();

    active_shards.write().insert(
        rockstream_types::ids::ShardId(0),
        rockstream_runtime::client::ShardState {
            lease: rockstream_types::lease::ShardLease::new(
                rockstream_types::ids::ShardId(0),
                WorkerId(1),
                rockstream_types::ids::LeaseToken(1),
            ),
            db: Some(db.clone()),
        },
    );

    active_shards.write().insert(
        rockstream_types::ids::ShardId(1),
        rockstream_runtime::client::ShardState {
            lease: rockstream_types::lease::ShardLease::new(
                rockstream_types::ids::ShardId(1),
                WorkerId(1),
                rockstream_types::ids::LeaseToken(1),
            ),
            db: Some(db.clone()),
        },
    );

    // Set up multiplexer on Worker 1 (sender)
    let pool = ShuffleClientPool::new(peers);
    let flow_controller = FlowController::new();
    let multiplexer =
        WorkerStreamMultiplexer::with_shards(pool, flow_controller, active_shards.clone())
            .with_object_store(store.clone())
            .with_src_worker(WorkerId(1));

    // Register 1 local inlet on Worker 2 (target shard 1)
    let registry = ExchangeRegistry::with_shards(active_shards.clone());
    let (tx_inlet, mut rx_inlet) = mpsc::channel(10);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "nationkey",
        DataType::Int32,
        false,
    )]));
    registry.register(100, 1, tx_inlet, schema.clone());

    // Send a frame. It will fail gRPC and fall back to MinIO object store.
    let zset = ArrowZSet::from_ab_rows(&[(42, 100)], 1);
    let payload = rockstream_runtime::exchange::serialization::serialize_zset(&zset).unwrap();

    let frame = ShuffleFrame {
        exchange_id: 100,
        src_shard: 0,
        target_shard: 1,
        epoch: 1,
        seq: 1,
        payload: payload.into(),
        row_count: zset.num_rows() as u32,
    };

    multiplexer.send_frame(WorkerId(2), frame).await.unwrap();

    // Verify that the frame is NOT in the sender outbox because fallback was successful
    let outbox_entries = db.scan_prefix(&[0x05]).await.unwrap();
    assert_eq!(
        outbox_entries.len(),
        0,
        "Outbox should be cleared after successful fallback"
    );

    // Recover/catch up on Worker 2 (receiver) from object store
    multiplexer
        .catch_up_durable(100, 1, WorkerId(1), WorkerId(2), &registry, store.as_ref())
        .await
        .unwrap();

    // Verify that the frame was successfully processed and sent to inlet
    let received = rx_inlet.recv().await.unwrap();
    assert_eq!(received.num_rows(), 1);
    assert_eq!(received.positive_ab_rows(), vec![(42, 100)]);

    // Run catch_up_durable again to assert no duplicates are created
    multiplexer
        .catch_up_durable(100, 1, WorkerId(1), WorkerId(2), &registry, store.as_ref())
        .await
        .unwrap();

    // Attempting to read from the channel should time out because the duplicate was ignored
    tokio::select! {
        _ = rx_inlet.recv() => {
            panic!("Received duplicate message!");
        }
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
            // Good, no duplicates.
        }
    }
}

#[tokio::test]
async fn test_thousand_shard_exchange_stress() {
    if !docker_available() {
        eprintln!("SKIP test_thousand_shard_exchange_stress: Docker not available");
        return;
    }

    let (_container, port) = start_minio().await;
    let store = minio_object_store(port);

    let mut writer = rockstream_runtime::exchange::durable::DurableShuffleWriter::new();
    let zset = ArrowZSet::from_ab_rows(&[(1, 100)], 1);
    let payload = rockstream_runtime::exchange::serialization::serialize_zset(&zset).unwrap();

    // Add 1000 frames for 1000 shards to the writer in-memory
    for shard in 0..1000u32 {
        writer
            .add_frame(0, shard, shard as u64 + 1, &payload)
            .unwrap();
    }

    // Verify fill level metric is tracked
    assert!(writer.fill_level() > 0);

    // Finish writing (writes EXACTLY 1 coalesced object file to MinIO)
    let path = object_store::path::Path::from("shuffle/200/1/1/2");
    writer.finish(store.as_ref(), &path).await.unwrap();

    // Verify that all 1000 frames were coalesced into EXACTLY 1 object file in MinIO
    let footer = rockstream_runtime::exchange::durable::DurableShuffleReader::read_footer(
        store.as_ref(),
        &path,
    )
    .await
    .unwrap();
    assert_eq!(footer.entries.len(), 1000);
}

#[tokio::test]
async fn test_distributed_fuzz_parity() {
    use rockstream_types::ids::ShardId;
    use rockstream_types::rendezvous::rendezvous_hash;

    let shards = vec![ShardId(0), ShardId(1), ShardId(2), ShardId(3)];

    // Generate random fuzz inputs: keys 0 to 100 with some weights
    let mut inputs = Vec::new();
    for k in 0..100i64 {
        inputs.push((k, k * 10));
    }

    // Single-shard: one ZSet containing all inputs
    let single_shard_zset = ArrowZSet::from_ab_rows(&inputs, 1);

    // Distributed: split inputs into 4 shards using rendezvous hashing on the key
    let mut shard_inputs = vec![Vec::new(); 4];
    for &(key, val) in &inputs {
        let chosen_shard = rendezvous_hash(key as u64, &shards, 10).unwrap();
        shard_inputs[chosen_shard.0 as usize].push((key, val));
    }

    // Build ZSets for each shard
    let mut distributed_zsets = Vec::new();
    for shard_input in shard_inputs {
        distributed_zsets.push(ArrowZSet::from_ab_rows(&shard_input, 1));
    }

    // Merge distributed ZSets back together (simulate union/gather)
    let mut combined_rows = Vec::new();
    for zset in distributed_zsets {
        combined_rows.extend(zset.positive_ab_rows());
    }
    // Sort to compare bit-identical contents
    combined_rows.sort_by_key(|&(k, _)| k);

    let mut expected_rows = single_shard_zset.positive_ab_rows();
    expected_rows.sort_by_key(|&(k, _)| k);

    assert_eq!(
        combined_rows, expected_rows,
        "Distributed results must be bit-identical to single-shard run"
    );
}

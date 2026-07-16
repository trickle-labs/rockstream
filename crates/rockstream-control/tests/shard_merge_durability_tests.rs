use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::{
    CheckpointCoordinator, MigrationPersistentStore, MigrationShard, ProactiveSplitConfig,
    ProactiveSplitter,
};
use rockstream_storage::{ShardDb, ShardKeyEncoder, ShardPrefix};
use rockstream_types::ids::ShardId;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-cold-merge-test";

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
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
    (year, month + 1, days + 1, h, m, s)
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
        .unwrap();
    assert!(resp.status().is_success() || resp.status().as_u16() == 409);
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    use object_store::aws::AmazonS3Builder;
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
            .unwrap(),
    )
}

async fn make_shard(
    shard_id: u64,
    path: &str,
    store: Arc<dyn ObjectStore>,
    frontier: u64,
) -> MigrationShard {
    let db = ShardDb::builder(path.to_string(), store.clone())
        .build()
        .await
        .unwrap();
    MigrationShard {
        shard_id: ShardId(shard_id),
        path: path.to_string(),
        object_store: store,
        db,
        frontier,
    }
}

async fn seed_shard(db: &ShardDb, rows: usize) {
    for idx in 0..rows {
        let op_key = ShardKeyEncoder::encode(
            ShardPrefix::OpState,
            1,
            format!("group-{idx:04}").as_bytes(),
        );
        let view_key = ShardKeyEncoder::encode(
            ShardPrefix::ViewOutput,
            1,
            format!("row-{idx:04}").as_bytes(),
        );
        db.put(&op_key, &[7u8; 64]).await.unwrap();
        db.put(&view_key, &[9u8; 64]).await.unwrap();
    }
    db.flush().await.unwrap();
}

async fn run_merge(store: Arc<dyn ObjectStore>) -> String {
    let donor = make_shard(1, "durability-merge/donor", store.clone(), 88).await;
    let recipient = make_shard(2, "durability-merge/recipient", store.clone(), 88).await;
    seed_shard(&donor.db, 2).await;
    seed_shard(&recipient.db, 2).await;
    let checkpoints = CheckpointCoordinator::new(vec![donor.shard_id]);
    let persistent = MigrationPersistentStore::new(store.clone());
    let mut splitter = ProactiveSplitter::new(ProactiveSplitConfig {
        target_shard_state_bytes: 512,
        min_shard_state_bytes: 1024,
        split_trigger_fraction: 1.5,
        alert_threshold_fraction: 1.75,
    });
    splitter
        .maybe_merge(
            &donor,
            &recipient,
            &checkpoints,
            Some(&persistent),
            None,
            60_000,
        )
        .await
        .unwrap()
        .unwrap()
        .migration_id
}

#[tokio::test]
async fn cold_merge_survives_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let migration_id = run_merge(store.clone()).await;

    let persistent = MigrationPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(
        persistent.load_history(&migration_id).await.unwrap().state,
        rockstream_types::migration::MigrationState::Done
    );
}

#[tokio::test]
async fn cold_merge_survives_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP cold_merge_survives_restart_minio_tc: Docker not available");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;

    let store = minio_object_store(port);
    let migration_id = run_merge(store.clone()).await;
    let persistent = MigrationPersistentStore::new(store);
    assert_eq!(
        persistent.load_history(&migration_id).await.unwrap().state,
        rockstream_types::migration::MigrationState::Done
    );
}

#[tokio::test]
async fn cold_merge_is_scan_and_delete_never_range_delete() {
    let source =
        std::fs::read_to_string(format!("{}/src/skew.rs", env!("CARGO_MANIFEST_DIR"))).unwrap();
    assert!(source.contains("scan_prefix_bounded"));
    assert!(source.contains("batch.delete"));
    assert!(!source.contains("range_delete"));
}

#[tokio::test]
async fn cold_merge_is_bounded_with_fill_level_metric() {
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let donor = make_shard(1, "bounded-merge/donor", store.clone(), 99).await;
    let recipient = make_shard(2, "bounded-merge/recipient", store.clone(), 99).await;
    seed_shard(&donor.db, 2).await;
    seed_shard(&recipient.db, 2).await;
    let checkpoints = CheckpointCoordinator::new(vec![donor.shard_id]);
    let mut splitter = ProactiveSplitter::new(ProactiveSplitConfig {
        target_shard_state_bytes: 512,
        min_shard_state_bytes: 1024,
        split_trigger_fraction: 1.5,
        alert_threshold_fraction: 1.75,
    });

    let outcome = splitter
        .maybe_merge(&donor, &recipient, &checkpoints, None, None, 60_000)
        .await
        .unwrap()
        .unwrap();
    assert!(outcome.fill_level.used > 0);
    assert!(outcome.fill_level.used <= outcome.fill_level.capacity);
}

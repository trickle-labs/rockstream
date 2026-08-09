use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use rockstream_control::MigrationPersistentStore;
use rockstream_types::ids::ShardId;
use rockstream_types::migration::{BucketSet, MigrationRecord, MigrationState};

fn make_record() -> MigrationRecord {
    let mut record = MigrationRecord::new(
        "durable-migration",
        vec![ShardId(1)],
        ShardId(2),
        BucketSet::new([7]),
        42,
        9,
    );
    record
        .apply_transition(MigrationState::Snapshotting)
        .unwrap();
    record.apply_transition(MigrationState::Copying).unwrap();
    record
        .apply_transition(MigrationState::DualWriting)
        .unwrap();
    record
}

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-migration-durability-test";

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
        .expect("CreateBucket PUT request failed");
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
            .expect("failed to build MinIO object store"),
    )
}

#[tokio::test]
async fn migration_record_survives_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let persistent_a = MigrationPersistentStore::new(store.clone());
    let state = make_record();
    persistent_a.save(&state).await.unwrap();

    let persistent_b = MigrationPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(persistent_b.load(&state.migration_id).await.unwrap(), state);
}

#[tokio::test]
async fn migration_record_survives_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP migration_record_survives_restart_minio_tc: Docker not available");
        return;
    }
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;

    let persistent_a = MigrationPersistentStore::new(minio_object_store(port));
    let state = make_record();
    persistent_a.save(&state).await.unwrap();
    let persistent_b = MigrationPersistentStore::new(minio_object_store(port));
    assert_eq!(persistent_b.load(&state.migration_id).await.unwrap(), state);
}

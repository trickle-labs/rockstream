//! MinIO (S3) backend integration tests for `SnapshotOp` (v0.13 — Slice 2).

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rockstream_ops::op::Operator;
use rockstream_ops::sink::ViewSinkOp;
use rockstream_ops::snapshot::SnapshotOp;
use rockstream_ops::zset::ArrowZSet;
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use sha2::{Digest, Sha256};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-test-snapshot";

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
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
    let status = resp.status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "CreateBucket failed: {status}"
    );
}

async fn start_minio() -> (testcontainers::ContainerAsync<MinIO>, u16) {
    let container = MinIO::default()
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
            .build()
            .expect("failed to build S3 object store for MinIO"),
    )
}

async fn open_shard_db_minio(port: u16, path: &str) -> Arc<ShardDb> {
    let store = minio_object_store(port);
    Arc::new(
        ShardDb::builder(path, store)
            .build()
            .await
            .expect("failed to open ShardDb on MinIO"),
    )
}

fn schema_kv() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

#[tokio::test]
async fn minio_snapshot_bootstrap_restart_resilience() {
    if !docker_available() {
        eprintln!("SKIP minio_snapshot_bootstrap_restart_resilience: Docker not available");
        return;
    }
    let (_container, port) = start_minio().await;
    let op_id = OperatorId(101);
    let schema = schema_kv();
    let db_path = "snap-resilience";

    // ── Phase 1: Write initial epochs, verify first chunk, and close ─────
    {
        let db = open_shard_db_minio(port, db_path).await;
        let sink = ViewSinkOp::new(db.clone(), op_id);

        // Epoch 0: 6 rows
        let batch0 =
            ArrowZSet::from_ab_rows(&[(1, 10), (2, 20), (2, 20), (3, 30), (4, 40), (5, 50)], 1);
        sink.write_epoch(&batch0, 0).await.unwrap();

        // Epoch 1: retract (2, 20) and (3, 30), and add new (6, 60)
        let batch1_retract_2 = ArrowZSet::from_ab_rows(&[(2, 20)], -1);
        let batch1_retract_3 = ArrowZSet::from_ab_rows(&[(3, 30)], -1);
        let batch1_add_6 = ArrowZSet::from_ab_rows(&[(6, 60)], 1);

        sink.write_epoch(&batch1_retract_2, 1).await.unwrap();
        sink.write_epoch(&batch1_retract_3, 2).await.unwrap();
        sink.write_epoch(&batch1_add_6, 3).await.unwrap();

        db.flush().await.unwrap();

        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 2, schema.clone(), 0)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        // Emit first chunk: (1,10), (2,20)
        let out1 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out1.num_rows(), 2);
        let k0 = out1
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let k1 = out1
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1);
        assert_eq!(k0, 1);
        assert_eq!(k1, 2);
        assert!(!snap_op.is_complete());

        drop(sink);
        drop(snap_op);

        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }

    // ── Phase 2: Reopen, resume from offset 2, process next chunk, and close ──
    {
        let db = open_shard_db_minio(port, db_path).await;

        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 2, schema.clone(), 2)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        let out2 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out2.num_rows(), 2);
        let k0 = out2
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let k1 = out2
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(1);
        assert_eq!(k0, 4);
        assert_eq!(k1, 5);
        assert!(!snap_op.is_complete());

        drop(snap_op);

        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }

    // ── Phase 3: Reopen, resume from offset 4, process last chunk, and complete ──
    {
        let db = open_shard_db_minio(port, db_path).await;

        let snap_op = SnapshotOp::load_and_initialize(db.clone(), op_id, 2, schema.clone(), 4)
            .await
            .unwrap();

        assert!(!snap_op.is_complete());

        let out3 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert_eq!(out3.num_rows(), 1);
        let k0 = out3
            .data
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(k0, 6);
        assert!(snap_op.is_complete());

        let out4 = snap_op
            .process_delta(ArrowZSet::empty(schema.clone()))
            .unwrap();
        assert!(out4.is_empty());
        assert!(snap_op.is_complete());

        drop(snap_op);

        Arc::try_unwrap(db)
            .ok()
            .expect("single owner")
            .close()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn minio_snapshot_bootstrap_large_scale() {
    if !docker_available() {
        eprintln!("SKIP minio_snapshot_bootstrap_large_scale: Docker not available");
        return;
    }
    let (_container, port) = start_minio().await;
    let op_id = OperatorId(102);
    let schema = schema_kv();
    let db = open_shard_db_minio(port, "snap-large").await;

    let mut wb = rockstream_storage::WriteBatch::new();
    let limit = rockstream_ops::snapshot::SNAPSHOT_BUFFER_LIMIT;

    for i in 0..=limit {
        let mut key = Vec::with_capacity(1 + 8 + 8 + 8);
        key.push(rockstream_storage::ShardPrefix::ViewOutput.as_byte());
        key.extend_from_slice(&op_id.0.to_be_bytes());
        key.extend_from_slice(&0u64.to_be_bytes());
        key.extend_from_slice(&(i as u64).to_be_bytes());

        let mut value = Vec::with_capacity(26);
        value.push(0u8); // TAG_INT64
        value.extend_from_slice(&(i as i64).to_be_bytes());
        value.push(0u8); // TAG_INT64
        value.extend_from_slice(&(i as i64).to_be_bytes());
        value.extend_from_slice(&1i64.to_be_bytes());
        wb.put(&key, &value);

        if i > 0 && i % 100_000 == 0 {
            db.write_batch(wb).await.unwrap();
            wb = rockstream_storage::WriteBatch::new();
        }
    }
    if !wb.is_empty() {
        db.write_batch(wb).await.unwrap();
    }

    db.flush().await.unwrap();

    let res = SnapshotOp::load_and_initialize(db.clone(), op_id, 100, schema.clone(), 0).await;
    assert!(
        res.is_err(),
        "Expected error due to SNAPSHOT_BUFFER_LIMIT violation"
    );

    match res {
        Err(rockstream_ops::error::OpError::Storage { source, .. }) => match source {
            rockstream_storage::StorageError::Unsupported(msg) => {
                assert!(msg.contains("exceeds SNAPSHOT_BUFFER_LIMIT"));
            }
            other => panic!("Expected StorageError::Unsupported, got {:?}", other),
        },
        Err(other) => panic!(
            "Expected StorageError::Unsupported, got Err variant: {:?}",
            other
        ),
        Ok(_) => panic!("Expected StorageError::Unsupported, got Ok"),
    }

    Arc::try_unwrap(db)
        .ok()
        .expect("single owner")
        .close()
        .await
        .unwrap();
}

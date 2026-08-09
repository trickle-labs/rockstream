use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rockstream_ops::recursion::{load_recursion_state, persist_recursion_state, RecursionOp};
use rockstream_ops::zset::ArrowZSet;
use rockstream_plan::{Expr, JoinSemantics, PlanNode};
use rockstream_storage::ShardDb;
use rockstream_types::ids::OperatorId;
use sha2::{Digest, Sha256};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-test-recursion";

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

fn schema_edges() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("src", DataType::Int64, false),
        Field::new("dst", DataType::Int64, false),
    ]))
}

fn make_input(rows: &[(i64, i64, i64)]) -> ArrowZSet {
    let src: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let dst: Vec<i64> = rows.iter().map(|row| row.1).collect();
    let weights: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let data = RecordBatch::try_new(
        schema_edges(),
        vec![
            Arc::new(Int64Array::from(src)) as ArrayRef,
            Arc::new(Int64Array::from(dst)) as ArrayRef,
        ],
    )
    .unwrap();
    ArrowZSet::new(data, weights)
}

fn base_plan() -> PlanNode {
    PlanNode::Source {
        name: "edges".to_string(),
    }
}

fn step_plan() -> PlanNode {
    PlanNode::Project {
        input: Box::new(PlanNode::InnerJoin {
            left: Box::new(PlanNode::Source {
                name: "reach".to_string(),
            }),
            right: Box::new(PlanNode::Source {
                name: "edges".to_string(),
            }),
            left_keys: vec![1],
            right_keys: vec![0],
            left_arr_id: OperatorId(1),
            right_arr_id: OperatorId(2),
            semantics: JoinSemantics::default(),
        }),
        columns: vec![Expr::Column(0), Expr::Column(3)],
    }
}

fn accumulate(state: &mut BTreeMap<(i64, i64), i64>, batch: &ArrowZSet) {
    if batch.is_empty() {
        return;
    }
    let src = batch
        .data
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let dst = batch
        .data
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for row_idx in 0..batch.num_rows() {
        *state
            .entry((src.value(row_idx), dst.value(row_idx)))
            .or_insert(0) += batch.weights[row_idx];
    }
    state.retain(|_, weight| *weight > 0);
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
        .unwrap();
    assert!(resp.status().is_success() || resp.status().as_u16() == 409);
}

async fn start_minio() -> (testcontainers::ContainerAsync<MinIO>, u16) {
    let container = MinIO::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;
    (container, port)
}

fn minio_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(MINIO_BUCKET)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

async fn open_shard_minio(port: u16, path: &str) -> Arc<ShardDb> {
    Arc::new(
        ShardDb::builder(path, minio_store(port))
            .build()
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn recursion_state_persists_and_replays_on_minio() {
    if !docker_available() {
        eprintln!("Docker not available — skipping recursion_state_persists_and_replays_on_minio");
        return;
    }

    let (_container, port) = start_minio().await;
    let op_id = OperatorId(60);
    let mut net = BTreeMap::new();

    {
        let db = open_shard_minio(port, "recursion-state").await;
        let op = RecursionOp::new(schema_edges(), base_plan(), step_plan(), 16, true);
        let out = op
            .process_epoch(make_input(&[(1, 2, 1), (2, 3, 1), (3, 4, 1)]), 1)
            .unwrap();
        accumulate(&mut net, &out);
        persist_recursion_state(&db, &op, op_id).await.unwrap();
        db.flush().await.unwrap();
        Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
    }

    {
        let db = open_shard_minio(port, "recursion-state").await;
        let op = load_recursion_state(
            &db,
            schema_edges(),
            base_plan(),
            step_plan(),
            16,
            true,
            op_id,
        )
        .await
        .unwrap();
        let out = op.process_epoch(make_input(&[(4, 5, 1)]), 2).unwrap();
        accumulate(&mut net, &out);
        assert!(net.contains_key(&(1, 5)));
        Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
    }
}

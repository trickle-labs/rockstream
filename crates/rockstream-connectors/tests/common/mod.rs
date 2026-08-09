// Shared test helper module included via `mod common;` by several separate
// test binaries (LFS, MinIO, TC variants). Each binary only uses a subset of
// these helpers, so per-binary dead-code lints are false positives here.
#![allow(dead_code)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};

pub const MINIO_USER: &str = "minioadmin";
pub const MINIO_PASS: &str = "minioadmin";
pub const RNG_SEED: u64 = 0x4400_0044;

pub fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

pub fn make_cumulative_batch(last_id: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from((1..=last_id).collect::<Vec<_>>()));
    let names: ArrayRef = Arc::new(StringArray::from(
        (1..=last_id)
            .map(|id| format!("row-{id}"))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, vec![ids, names]).unwrap()
}

pub fn render_batches(batches: &[RecordBatch]) -> String {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row_idx in 0..batch.num_rows() {
            rows.push(format!("{}|{}", ids.value(row_idx), names.value(row_idx)));
        }
    }
    rows.join("\n")
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
    for &days_in_month in &dpm {
        if days < days_in_month {
            break;
        }
        days -= days_in_month;
        month += 1;
    }
    let day = days + 1;
    (year, month + 1, day, h, m, s)
}

pub async fn create_minio_bucket(port: u16, bucket: &str) {
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
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let k1 = hmac_sha256(format!("AWS4{MINIO_PASS}").as_bytes(), date.as_bytes());
    let k2 = hmac_sha256(&k1, region.as_bytes());
    let k3 = hmac_sha256(&k2, b"s3");
    let signing_key = hmac_sha256(&k3, b"aws4_request");
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );
    let url = format!("http://{host}/{bucket}");
    let response = reqwest::Client::new()
        .put(url)
        .header("host", host)
        .header("x-amz-date", datetime)
        .header("x-amz-content-sha256", empty_hash)
        .header("authorization", auth)
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success() || response.status().as_u16() == 409,
        "create bucket failed: {}",
        response.status()
    );
}

pub fn build_minio_store(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(bucket)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, Image};

pub const MINIO_USER: &str = "minioadmin";
pub const MINIO_PASS: &str = "minioadmin";

type HmacSha256 = Hmac<Sha256>;

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let s = (secs % 60) as u32;
    let total_m = secs / 60;
    let m = (total_m % 60) as u32;
    let total_h = total_m / 60;
    let h = (total_h % 24) as u32;
    let mut days = (total_h / 24) as u32;
    let mut year = 1970u32;
    loop {
        let is_leap =
            (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400);
        let diy = if is_leap { 366 } else { 365 };
        if days < diy {
            break;
        }
        days -= diy;
        year += 1;
    }
    let is_leap = (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400);
    let dpm = [
        31,
        if is_leap { 29 } else { 28 },
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

pub fn minio_object_store(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(bucket)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to build MinIO object store"),
    )
}

pub fn build_minio_store(port: u16, bucket: &str) -> Arc<dyn ObjectStore> {
    minio_object_store(port, bucket)
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

pub async fn start_minio(bucket: &str) -> Option<(ContainerAsync<MinIO2024>, u16)> {
    let container = match MinIO2024::default().start().await {
        Ok(c) => c,
        Err(err) => {
            eprintln!("SKIP start_minio: cannot start MinIO container ({err:?})");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(9000).await {
        Ok(p) => p,
        Err(err) => {
            eprintln!("SKIP start_minio: cannot get MinIO port ({err:?})");
            return None;
        }
    };
    create_minio_bucket(port, bucket).await;
    Some((container, port))
}

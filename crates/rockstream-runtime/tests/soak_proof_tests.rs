use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

use rockstream_runtime::exchange::durable::{DurableShuffleReader, DurableShuffleWriter};
use rockstream_sim::object_store::ObjectStoreError;
use rockstream_sim::Runtime;
use rockstream_sim::SimRuntime;

// ─── Throttled Store Wrapper for testing ───

#[derive(Debug)]
struct ThrottledStoreWrapper<T> {
    inner: T,
    fail_counter: Arc<AtomicU32>,
    max_fails: Option<u32>,
    fail_probability: Option<f64>,
}

impl<T> ThrottledStoreWrapper<T> {
    fn with_max_fails(inner: T, max_fails: u32) -> Self {
        Self {
            inner,
            fail_counter: Arc::new(AtomicU32::new(0)),
            max_fails: Some(max_fails),
            fail_probability: None,
        }
    }

    fn with_probability(inner: T, prob: f64) -> Self {
        Self {
            inner,
            fail_counter: Arc::new(AtomicU32::new(0)),
            max_fails: None,
            fail_probability: Some(prob),
        }
    }

    fn maybe_fail(&self) -> Result<()> {
        if let Some(max) = self.max_fails {
            let count = self.fail_counter.fetch_add(1, Ordering::Relaxed);
            if count < max {
                return Err(object_store::Error::Generic {
                    store: "ThrottledStoreWrapper",
                    source: Box::new(std::io::Error::other(
                        "HTTP 429 Too Many Requests (throttling)",
                    )),
                });
            }
        }
        if let Some(prob) = self.fail_probability {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            if rng.gen_bool(prob) {
                return Err(object_store::Error::Generic {
                    store: "ThrottledStoreWrapper",
                    source: Box::new(std::io::Error::other(
                        "HTTP 429 Too Many Requests (throttling)",
                    )),
                });
            }
        }
        Ok(())
    }
}

impl<T> std::fmt::Display for ThrottledStoreWrapper<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ThrottledStoreWrapper")
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for ThrottledStoreWrapper<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.maybe_fail()?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.maybe_fail()?;
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.maybe_fail()?;
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.maybe_fail()?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.maybe_fail()?;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.maybe_fail()?;
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.maybe_fail()?;
        self.inner.put_multipart_opts(location, opts).await
    }
}

// ─── Wrapper for SimObjectStore to implement ObjectStore ───

#[derive(Debug)]
struct SimObjectStoreWrapper {
    inner: rockstream_sim::SimObjectStoreHandle,
}

impl SimObjectStoreWrapper {
    fn new(inner: rockstream_sim::SimObjectStoreHandle) -> Self {
        Self { inner }
    }

    fn convert_err(e: ObjectStoreError) -> object_store::Error {
        object_store::Error::Generic {
            store: "SimObjectStoreWrapper",
            source: Box::new(std::io::Error::other(e.to_string())),
        }
    }
}

impl std::fmt::Display for SimObjectStoreWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SimObjectStoreWrapper")
    }
}

#[async_trait]
#[allow(unused_variables)]
impl ObjectStore for SimObjectStoreWrapper {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let bytes = payload.clone().into();
        self.inner
            .put(location.as_ref(), bytes)
            .map_err(Self::convert_err)?;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let bytes = self
            .inner
            .get(location.as_ref())
            .map_err(Self::convert_err)?;
        let (range, data) = match options.range {
            Some(r) => {
                let resolved =
                    r.as_range(bytes.len() as u64)
                        .map_err(|e| object_store::Error::Generic {
                            store: "SimObjectStoreWrapper",
                            source: Box::new(e),
                        })?;
                let start = resolved.start as usize;
                let end = resolved.end as usize;
                (resolved, bytes.slice(start..end))
            }
            None => (0..bytes.len() as u64, bytes.clone()),
        };
        Ok(GetResult {
            payload: object_store::GetResultPayload::Stream(
                futures::stream::once(futures::future::ready(Ok(data))).boxed(),
            ),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: std::time::SystemTime::now().into(),
                size: bytes.len() as u64,
                e_tag: None,
                version: None,
            },
            range,
            attributes: Default::default(),
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner
            .delete(location.as_ref())
            .map_err(Self::convert_err)?;
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.as_ref()).unwrap_or("");
        let keys = self.inner.list(prefix_str);
        let metas: Vec<Result<ObjectMeta>> = keys
            .into_iter()
            .map(|k| {
                Ok(ObjectMeta {
                    location: Path::from(k),
                    last_modified: std::time::SystemTime::now().into(),
                    size: 0,
                    e_tag: None,
                    version: None,
                })
            })
            .collect();
        futures::stream::iter(metas).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        Err(object_store::Error::NotSupported {
            source: Box::new(std::io::Error::other("list_with_delimiter not supported")),
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(std::io::Error::other("copy not supported")),
        })
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(std::io::Error::other("copy_if_not_exists not supported")),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: Box::new(std::io::Error::other("put_multipart_opts not supported")),
        })
    }
}

// ─── Test 1: metrics server exposition ───

#[tokio::test]
async fn test_metrics_server_exposition() {
    rockstream_types::metrics::reset_all();

    // Start server
    let server = rockstream_cli::metrics_server::start_metrics_server("127.0.0.1:0")
        .await
        .unwrap();
    let addr = server.local_addr;

    // Record mock durations
    rockstream_types::metrics::record_flush_duration(Duration::from_millis(1500));
    rockstream_types::metrics::record_flush_duration(Duration::from_millis(500));

    let client = reqwest::Client::new();
    let res = client
        .get(format!("http://{}/metrics", addr))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), reqwest::StatusCode::OK);
    let body = res.text().await.unwrap();

    assert!(body.contains("# HELP flush_duration_seconds_sum"));
    assert!(body.contains("flush_duration_seconds_sum 2.0000"));
    assert!(body.contains("flush_duration_seconds_count 2"));
    assert!(body.contains("flush_duration_seconds_last 0.5000"));

    server.shutdown();
}

// ─── Test 2: coalesced durable fallback retry ───

#[tokio::test]
async fn test_coalesced_durable_fallback_retry() {
    let raw_store = object_store::memory::InMemory::new();
    let store = ThrottledStoreWrapper::with_max_fails(raw_store, 3);
    let path = Path::from("test_coalesced_durable_fallback_retry");

    let mut writer = DurableShuffleWriter::new();
    let payload = Bytes::from("shuffle_payload_data");
    writer.add_frame(0, 1, 1, &payload).unwrap();

    writer.finish(&store, &path).await.unwrap();

    // Reset fail counter
    store.fail_counter.store(0, Ordering::Relaxed);

    let footer = DurableShuffleReader::read_footer(&store, &path)
        .await
        .unwrap();
    assert_eq!(footer.entries.len(), 1);

    // Reset fail counter
    store.fail_counter.store(0, Ordering::Relaxed);

    let frame = DurableShuffleReader::read_frame(&store, &path, &footer.entries[0])
        .await
        .unwrap();
    assert_eq!(frame, payload);
}

// ─── Test 3: object store limit soak sim ───

#[tokio::test]
async fn test_object_store_limit_soak_sim() {
    rockstream_sim::buggify::buggify_init(424242);

    let rt = SimRuntime::new(424242);
    rt.object_store().set_rate_limit(Some(20.0));

    let wrapper = SimObjectStoreWrapper::new(rt.object_store().clone());

    let simulated_duration = Duration::from_secs(72 * 3600);
    let step = simulated_duration / 500; // run 500 iterations

    for i in 0..500 {
        rt.advance_time(step);

        let path = Path::from(format!("soak/obj_{}", i));
        let mut writer = DurableShuffleWriter::new();
        let payload = Bytes::from(format!("payload_data_{}", i));
        writer.add_frame(0, 1, 1, &payload).unwrap();

        writer.finish(&wrapper, &path).await.unwrap();

        let footer = DurableShuffleReader::read_footer(&wrapper, &path)
            .await
            .unwrap();
        assert_eq!(footer.entries.len(), 1);

        let frame = DurableShuffleReader::read_frame(&wrapper, &path, &footer.entries[0])
            .await
            .unwrap();
        assert_eq!(frame, payload);
    }

    rockstream_sim::buggify::buggify_disable();
}

// ─── Test 4: MinIO Rate Limiting integration test ───

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-soak-test";

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
        let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
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
            .build()
            .expect("failed to build MinIO object store"),
    )
}

#[tokio::test]
async fn test_minio_rate_limit_soak_tc() {
    if !docker_available() {
        eprintln!("SKIP test_minio_rate_limit_soak_tc: Docker not available");
        return;
    }

    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .expect("failed to start MinIO; is Docker running?");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;

    let real_store = minio_object_store(port);
    let store = ThrottledStoreWrapper::with_probability(real_store, 0.1); // 10% failure rate

    let path = Path::from("test_minio_rate_limit_soak_tc");

    let mut writer = DurableShuffleWriter::new();
    let payload = Bytes::from("minio_shuffle_payload_data");
    writer.add_frame(0, 1, 1, &payload).unwrap();

    writer.finish(&store, &path).await.unwrap();

    let footer = DurableShuffleReader::read_footer(&store, &path)
        .await
        .unwrap();
    assert_eq!(footer.entries.len(), 1);

    let frame = DurableShuffleReader::read_frame(&store, &path, &footer.entries[0])
        .await
        .unwrap();
    assert_eq!(frame, payload);
}

//! MinIO catalog durability integration test (v0.63 durability commitment).
//!
//! Asserts that `DurableCatalogStore` correctly persists transactions, creates
//! snapshots, and recovers exact catalog metadata across node restarts using a real
//! S3-compatible MinIO backend via TestContainers.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use rockstream_storage::catalog::*;
use rockstream_types::ids::*;
use sha2::{Digest, Sha256};
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::Image;

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-catalog-minio-test";

#[derive(Debug)]
struct MinIO2024 {
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
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (yr, mo, dy, hr, mn, sc) = epoch_to_ymd_hms(now);
    let amz_date = format!("{yr:04}{mo:02}{dy:02}T{hr:02}{mn:02}{sc:02}Z");
    let date_stamp = format!("{yr:04}{mo:02}{dy:02}");

    let region = "us-east-1";
    let service = "s3";
    let host = format!("127.0.0.1:{port}");
    let url = format!("http://{host}/{bucket}");

    let payload_hash = sha256_hex(b"");
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_req =
        format!("PUT\n/{bucket}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_req.as_bytes())
    );

    let k_date = hmac_sha256(
        format!("AWS4{MINIO_PASS}").as_bytes(),
        date_stamp.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let sig = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders={signed_headers}, Signature={sig}"
    );

    let client = reqwest::Client::new();
    let resp = client
        .put(&url)
        .header("host", &host)
        .header("x-amz-content-sha256", &payload_hash)
        .header("x-amz-date", &amz_date)
        .header("authorization", &auth_header)
        .send()
        .await
        .expect("failed to send CreateBucket request to MinIO");

    let status = resp.status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "unexpected status {status} creating bucket {bucket}"
    );
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

#[tokio::test]
async fn test_catalog_store_minio_lifecycle() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP test_catalog_store_minio_lifecycle: Docker not available");
        return;
    }

    let container = match MinIO2024::default().start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "SKIP test_catalog_store_minio_lifecycle: could not start MinIO container: {e}"
            );
            return;
        }
    };
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;
    let minio_store = minio_object_store(port);
    let prefix = "catalog_minio_lifecycle";

    // Phase 1: Initialize store and commit multiple entities
    {
        let catalog = DurableCatalogStore::new(minio_store.clone(), prefix);

        let db = CatalogDatabase {
            id: DatabaseId(1),
            name: "minio_db".to_string(),
            default_namespace: "public".to_string(),
            created_at: 100,
        };
        let ns = CatalogNamespace {
            id: NamespaceId(1),
            name: "public".to_string(),
            database_id: DatabaseId(1),
            created_at: 100,
        };
        let tbl = CatalogTable {
            id: TableId(10),
            name: "minio_table".to_string(),
            namespace_id: NamespaceId(1),
            columns: vec![CatalogColumn {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
                ordinal: 0,
            }],
            pk_cols: vec!["id".to_string()],
        };
        let view = CatalogView {
            id: ViewId(20),
            name: "minio_view".to_string(),
            namespace_id: NamespaceId(1),
            sql: "SELECT id FROM minio_table".to_string(),
            compiled_plan_id: None,
            op_id: Some(1),
            columns: vec![],
        };

        let txn1 = CatalogTxn::new(
            1,
            1,
            vec![
                CatalogMutation::PutDatabase(db),
                CatalogMutation::PutNamespace(ns),
                CatalogMutation::PutTable(tbl),
                CatalogMutation::PutView(view),
            ],
        )
        .unwrap();

        catalog.commit_txn(txn1).await.unwrap();
        assert_eq!(catalog.get_revision().await, 1);

        // Create snapshot at revision 1
        catalog.create_snapshot().await.unwrap();

        // Mutate again (revision 2)
        let idx = CatalogIndexEntry {
            id: IndexId(30),
            name: "minio_idx".to_string(),
            table_id: TableId(10),
            index_cols: vec!["id".to_string()],
            pk_cols: vec!["id".to_string()],
            state: CatalogIndexState::Ready,
            op_id: Some(2),
        };
        let txn2 = CatalogTxn::new(2, 2, vec![CatalogMutation::PutIndex(idx)]).unwrap();
        catalog.commit_txn(txn2).await.unwrap();
        assert_eq!(catalog.get_revision().await, 2);
    }

    // Phase 2: Simulate restart with a fresh store pointing to same prefix
    {
        let new_minio_store = minio_object_store(port);
        let recovered = DurableCatalogStore::recover(new_minio_store, prefix)
            .await
            .unwrap();

        assert_eq!(recovered.get_revision().await, 2);

        let tbl = recovered.get_table(TableId(10)).await.unwrap().unwrap();
        assert_eq!(tbl.name, "minio_table");

        let view = recovered.get_view(ViewId(20)).await.unwrap().unwrap();
        assert_eq!(view.name, "minio_view");

        let idx = recovered.get_index(IndexId(30)).await.unwrap().unwrap();
        assert_eq!(idx.name, "minio_idx");
        assert_eq!(idx.state, CatalogIndexState::Ready);
    }
}

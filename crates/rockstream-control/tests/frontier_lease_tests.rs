//! v0.45.6 S7 — durability tests for the *new* `frontier/leader` fencing-
//! token CAS record and synchronously-flushed published-frontier value
//! introduced by S3–S6 (`FrontierLeaseStore`, `rockstream_control::frontier`).
//!
//! `.claude/v0.45.6-plan.md` §4 "Durability Slices": this is new durable
//! state that did not exist before this version (the pre-v0.45.6
//! `FrontierAggregator` was purely in-memory, per-process, with no
//! persisted lease) and therefore needs both required tests per the Phase 2
//! rule — a LocalFileSystem (embedded) test and a MinIO (S3, TestContainers)
//! test — plus a focused sync-flush regression pair, per the plan.
//!
//! This is also the runtime witness for **M2-L1** (`PublicationProgress`:
//! the store's `published_frontier` reaches its target value across a
//! restart) and **M2-L2** (`FailoverProgress`: a second aggregator recovers
//! and continues publishing after the first crashes) from
//! `formal/m2_frontier_agg.fizz`.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use rockstream_control::frontier::{FrontierLeaseError, FrontierLeaseStore};
use rockstream_types::ids::AggregatorId;

fn docker_available() -> bool {
    rockstream_test_support::docker_available()
}

/// **S7 durability slice** — LocalFileSystem (embedded) backend: a
/// publisher acquires the lease, publishes, "crashes" (the `FrontierLeaseStore`
/// handle is dropped without closing anything explicitly — SlateDB durability
/// is not process-lifetime-dependent); a second aggregator recovers against
/// the same on-disk directory, acquires with a strictly higher token, and
/// its first read observes the crashed publisher's last (synchronously-
/// flushed) write — never a stale value.
///
/// M2-L1/M2-L2 witness: `published_frontier` survives the restart
/// (`PublicationProgress`) and a fresh aggregator continues publishing after
/// the first's "crash" (`FailoverProgress`).
#[tokio::test]
async fn frontier_leader_lease_cas_survives_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    // First "boot": aggregator 1 acquires the lease and publishes frontier 42.
    let lease_a = FrontierLeaseStore::open("frontier-lease", store.clone())
        .await
        .unwrap();
    assert_eq!(lease_a.current_fence_token().await, 0);
    let token_a = lease_a
        .acquire_publisher_lease(AggregatorId(1), 0)
        .await
        .unwrap();
    lease_a.publish_frontier(token_a, 42).await.unwrap();
    assert_eq!(
        lease_a.read_published_frontier_after_handoff().await,
        Some(42)
    );

    // "Crash": drop the first handle without any explicit close/flush call
    // (SlateDB's own durability, not process cleanup, is what's under test).
    drop(lease_a);

    // "Restart": a brand-new `FrontierLeaseStore` (as a fresh aggregator
    // process would construct), backed by the SAME on-disk directory.
    let lease_b = FrontierLeaseStore::open(
        "frontier-lease",
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
    )
    .await
    .unwrap();

    // M2-L1: published_frontier survived the restart, and the recovered
    // handoff read observes the prior synchronously-flushed write — never
    // a stale value (S5's assert_flush_before_lease_handoff_read must not
    // panic here).
    assert!(
        lease_b.read_published_frontier_after_handoff().await == Some(42),
        "M2-L1: published_frontier must survive the restart"
    );

    // M2-L2: the recovering aggregator (2) acquires with a strictly higher
    // token than the crashed publisher's last token, and can continue
    // publishing — failover progress.
    assert_eq!(lease_b.current_fence_token().await, token_a.0);
    let token_b = lease_b
        .acquire_publisher_lease(AggregatorId(2), token_a.0)
        .await
        .unwrap();
    assert!(token_b.0 > token_a.0);
    lease_b.publish_frontier(token_b, 99).await.unwrap();
    assert_eq!(
        lease_b.read_published_frontier_after_handoff().await,
        Some(99)
    );

    // The superseded aggregator's stale token must now be rejected — never
    // resurrecting a lower/older frontier value.
    let err = lease_b
        .acquire_publisher_lease(AggregatorId(1), token_a.0)
        .await
        .unwrap_err();
    assert!(matches!(err, FrontierLeaseError::StaleFenceToken { .. }));
}

/// **S7 focused regression**: directly exercises S5's
/// `assert_flush_before_lease_handoff_read` panic path by forcing an
/// unflushed write ahead of a lease handoff, mirroring the existing
/// `commit_panics_via_assert_commit_pointer_atomic_on_truncated_write`
/// pattern in `object_store_sink.rs` (`crates/rockstream-connectors/src/`).
///
/// `FrontierLeaseStore` itself always writes with `await_durable: true`
/// (there is no code path that produces an unflushed publish), so this
/// drives the paired assertion function directly, the same way
/// `assert_valid_publisher`'s panic path is unit-tested in `frontier.rs`.
#[tokio::test]
async fn sync_flush_before_lease_handoff_read_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let lease = FrontierLeaseStore::open("frontier-lease-sync", store)
        .await
        .unwrap();

    // The real store never produces an unflushed published value — confirm
    // that directly (S5's panic path is unreachable via the public API).
    let token = lease
        .acquire_publisher_lease(AggregatorId(1), 0)
        .await
        .unwrap();
    lease.publish_frontier(token, 3).await.unwrap();
    assert_eq!(lease.read_published_frontier_after_handoff().await, Some(3));

    // Directly drive the paired assertion with a forced "unflushed write"
    // scenario (`has_published_value=true`, `last_write_synced=false`) —
    // this is the RS-8003 panic path itself.
    let result = std::panic::catch_unwind(|| {
        rockstream_control::frontier::assert_flush_before_lease_handoff_read(true, false);
    });
    assert!(
        result.is_err(),
        "RS-8003: expected panic on unflushed publish ahead of lease-handoff read"
    );
}

// ─── MinIO (S3, TestContainers) durability test ────────────────────────────

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-frontier-lease-durability-test";

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
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to build MinIO object store"),
    )
}

/// **S7 durability slice** — MinIO (S3, TestContainers) backend: same
/// acquire → publish → "crash" → recover → re-acquire-with-higher-token
/// round trip as the LFS test above, but against a real S3-compatible
/// object store.
///
/// Skips gracefully (rather than failing) when Docker is unavailable,
/// following this repo's existing MinIO TestContainers convention.
#[tokio::test]
async fn frontier_leader_lease_cas_survives_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP frontier_leader_lease_cas_survives_restart_minio_tc: Docker not available");
        return;
    }

    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .expect("failed to start MinIO; is Docker running?");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;

    let store_a = minio_object_store(port);
    let lease_a = FrontierLeaseStore::open("frontier-lease-minio", store_a)
        .await
        .unwrap();
    let token_a = lease_a
        .acquire_publisher_lease(AggregatorId(1), 0)
        .await
        .unwrap();
    lease_a.publish_frontier(token_a, 42).await.unwrap();
    assert_eq!(
        lease_a.read_published_frontier_after_handoff().await,
        Some(42)
    );
    drop(lease_a);

    let store_b = minio_object_store(port);
    let lease_b = FrontierLeaseStore::open("frontier-lease-minio", store_b)
        .await
        .unwrap();
    assert_eq!(
        lease_b.read_published_frontier_after_handoff().await,
        Some(42)
    );

    let token_b = lease_b
        .acquire_publisher_lease(AggregatorId(2), token_a.0)
        .await
        .unwrap();
    assert!(token_b.0 > token_a.0);
    lease_b.publish_frontier(token_b, 99).await.unwrap();
    assert_eq!(
        lease_b.read_published_frontier_after_handoff().await,
        Some(99)
    );

    let err = lease_b
        .acquire_publisher_lease(AggregatorId(1), token_a.0)
        .await
        .unwrap_err();
    assert!(matches!(err, FrontierLeaseError::StaleFenceToken { .. }));
}

/// **S7 focused regression** — MinIO backend: same
/// `assert_flush_before_lease_handoff_read` direct-drive as the LFS variant
/// above, confirming the real S3-backed store also never produces an
/// unflushed publish via its public API.
#[tokio::test]
async fn sync_flush_before_lease_handoff_read_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP sync_flush_before_lease_handoff_read_minio_tc: Docker not available");
        return;
    }

    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .expect("failed to start MinIO; is Docker running?");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;

    let store = minio_object_store(port);
    let lease = FrontierLeaseStore::open("frontier-lease-minio-sync", store)
        .await
        .unwrap();
    let token = lease
        .acquire_publisher_lease(AggregatorId(1), 0)
        .await
        .unwrap();
    lease.publish_frontier(token, 3).await.unwrap();
    assert_eq!(lease.read_published_frontier_after_handoff().await, Some(3));

    let result = std::panic::catch_unwind(|| {
        rockstream_control::frontier::assert_flush_before_lease_handoff_read(true, false);
    });
    assert!(
        result.is_err(),
        "RS-8003: expected panic on unflushed publish ahead of lease-handoff read"
    );
}

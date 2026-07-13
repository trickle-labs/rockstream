//! v0.45.2 M7 S5 — durability tests for the *new* Raft term/vote persistence
//! path introduced by S1 (`RaftPersistentStore`, `control/raft/state.json`).
//!
//! `.claude/v0.45.2-plan.md` §4 "Durability Slices": this is new durable
//! state that did not exist before v0.45.2 and therefore needs both required
//! tests per the Phase 2 rule — a LocalFileSystem (embedded) test and a
//! MinIO (S3, TestContainers) test.
//!
//! Scope note: the persisted state is `current_term`/`voted_for` only
//! (`RaftPersistentState`, see `rockstream_control::raft`) — this
//! implementation's Raft group does not replicate an application log
//! through this store (only leader-election state), so there is no
//! separate "log entries" round trip to test here; the plan's prose
//! mentions log entries as an aspirational superset, but the actual S1
//! implementation (already complete, not modified by this test) only ever
//! writes term/vote. Both tests below assert what is actually persisted.

use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use rockstream_control::raft::{RaftPersistentState, RaftPersistentStore};

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// **S5 durability slice** — LocalFileSystem (embedded) backend: a control
/// node persists `current_term`/`voted_for`, "restarts" (a fresh
/// `RaftPersistentStore` pointed at the same on-disk directory, modeling a
/// process restart against the same `--storage` path), and recovers the
/// identical state.
#[tokio::test]
async fn raft_term_vote_log_survive_restart_lfs() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    // First "boot": no state persisted yet, node votes for itself at term 3.
    let persistent_a = RaftPersistentStore::new(store.clone());
    let before_boot = persistent_a.load().await;
    assert_eq!(
        before_boot,
        RaftPersistentState::default(),
        "a fresh control node with no prior state must see the zero-value default"
    );
    let state = RaftPersistentState {
        current_term: 3,
        voted_for: Some(7),
    };
    persistent_a.save(&state).await;

    // "Restart": a brand-new `RaftPersistentStore` instance (as a fresh
    // process boot would construct), backed by the SAME on-disk directory.
    let persistent_b = RaftPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    let recovered = persistent_b.load().await;
    assert_eq!(
        recovered, state,
        "current_term/voted_for must survive an LFS-backed control node restart identically"
    );

    // A second term/vote update after "restart" also persists correctly —
    // proves the recovered store is fully live, not just readable once.
    let state2 = RaftPersistentState {
        current_term: 4,
        voted_for: Some(9),
    };
    persistent_b.save(&state2).await;
    let persistent_c = RaftPersistentStore::new(Arc::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    assert_eq!(persistent_c.load().await, state2);
}

// ─── MinIO (S3, TestContainers) durability test ────────────────────────────

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-raft-durability-test";

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

/// **S5 durability slice** — MinIO (S3, TestContainers) backend: same
/// persist → "restart" → recover round trip as the LFS test above, but
/// against a real S3-compatible object store, proving the Tier-3 deployment
/// profile's durability path for the new Raft term/vote state.
///
/// Skips gracefully (rather than failing) when Docker is unavailable,
/// following this repo's existing MinIO TestContainers convention (see
/// `rockstream-storage/tests/minio_backend.rs`).
#[tokio::test]
async fn raft_term_vote_log_survive_restart_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP raft_term_vote_log_survive_restart_minio_tc: Docker not available");
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
    let persistent_a = RaftPersistentStore::new(store_a);
    assert_eq!(
        persistent_a.load().await,
        RaftPersistentState::default(),
        "a fresh bucket must see the zero-value default"
    );
    let state = RaftPersistentState {
        current_term: 5,
        voted_for: Some(2),
    };
    persistent_a.save(&state).await;

    // "Restart": brand-new `RaftPersistentStore` + brand-new S3 client
    // pointed at the same MinIO bucket (as a fresh process boot would
    // construct against the same durable backing store).
    let store_b = minio_object_store(port);
    let persistent_b = RaftPersistentStore::new(store_b);
    let recovered = persistent_b.load().await;
    assert_eq!(
        recovered, state,
        "current_term/voted_for must survive a MinIO-backed control node restart identically"
    );
}

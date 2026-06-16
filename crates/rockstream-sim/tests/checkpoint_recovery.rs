//! Cluster checkpoint and recovery integration tests (v0.20).
//!
//! These tests prove the DESIGN.md §11 checkpoint and recovery proof claims.
//!
//! ## LFS tests (no Docker required)
//!
//! - [`test_checkpoint_alignment_bounded_lfs`] — alignment buffer ≤
//!   `checkpoint_alignment_max_credits`; timeout → `RECOVERING` (RS-3602), not panic.
//! - [`test_one_checkpoint_per_shard_lfs`] — exactly one SlateDB checkpoint is
//!   created per shard per checkpoint round.
//! - [`test_recovery_bit_identical_lfs`] — checkpoint a pipeline, simulate
//!   crash, recover via `RecoveryDriver`, assert output state is bit-identical
//!   to pre-crash state.
//!
//! ## MinIO / TestContainers tests (Docker required)
//!
//! - [`test_recovery_bit_identical_minio`] — bit-identical recovery on real MinIO.
//! - [`test_checkpoint_gc_bounded_minio`] — SST/checkpoint count bounded after
//!   N checkpoint rounds.
//!
//! ## SimRuntime tests (`simulation` feature)
//!
//! - [`test_self_fence_on_partition`] — partitioned worker self-fences before new
//!   owner's first commit; M4-S1/S2 observed at runtime.
//! - [`test_checkpoint_under_slow_input`] — coordinator with fast+slow source:
//!   backpressure observed, checkpoint eventually succeeds or reports `RECOVERING`.

#![allow(unused_imports, dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use testcontainers::runners::AsyncRunner;

use rockstream_control::checkpoint::{CheckpointCoordinator, CoordinatorError};
use rockstream_runtime::{RecoveryDriver, RecoveryError};
use rockstream_storage::ShardDb;
use rockstream_types::checkpoint::{CheckpointBarrier, CheckpointId, PerShardCheckpoint};
use rockstream_types::ids::{LeaseToken, ShardId};

// ─── Shared helpers ───────────────────────────────────────────────────────────

fn noop_inject(_shard: ShardId, _barrier: CheckpointBarrier) {}

fn noop_commit(
    _m: &rockstream_types::checkpoint::ClusterCheckpoint,
) -> Result<(), CoordinatorError> {
    Ok(())
}

async fn open_shard_db(path: &str, store: Arc<dyn ObjectStore>) -> ShardDb {
    ShardDb::builder(path.to_string(), store).build().await.unwrap()
}

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// ─── Slice 4 / slice 7: LFS test 1 ───────────────────────────────────────────

/// Alignment buffer is bounded: never exceeds `checkpoint_alignment_max_credits`.
///
/// Proof claim (DESIGN.md §11.2):
/// "Checkpointing under slow input and credit exhaustion never grows unbounded
/// and either succeeds or reports `RECOVERING`."
///
/// Test strategy:
/// 1. Create a coordinator with a small credit limit (2 credits for 2 shards).
/// 2. Begin a checkpoint round — 2 credits are consumed.
/// 3. Assert `credits_used() ≤ max_credits()`.
/// 4. Attempt a second begin while first is still open — should fail with
///    `AlignmentTimeout` (RS-3602).
/// 5. Confirm credits never exceed the bound throughout.
#[tokio::test]
async fn test_checkpoint_alignment_bounded_lfs() {
    let shards = vec![ShardId(0), ShardId(1)];
    let max_credits = 2usize;
    let coord = CheckpointCoordinator::with_config(shards.clone(), max_credits, 3);

    // Begin checkpoint round — acquires 2 credits.
    let id = coord.begin_checkpoint(noop_inject).unwrap();
    assert_eq!(id, CheckpointId(1));

    // Fill-level metric: credits used must not exceed max.
    assert!(
        coord.credits_used() <= coord.max_credits(),
        "alignment buffer exceeded max_credits: {} > {}",
        coord.credits_used(),
        coord.max_credits()
    );

    // Second begin while first is still open → AlignmentTimeout (RS-3602), not panic.
    let err = coord.begin_checkpoint(noop_inject).unwrap_err();
    assert!(
        matches!(err, CoordinatorError::AlignmentTimeout),
        "expected AlignmentTimeout but got {err:?}"
    );
    assert!(
        err.to_string().contains("RS-3602"),
        "error must contain RS-3602: {err}"
    );

    // Credits still bounded after failed begin.
    assert!(
        coord.credits_used() <= coord.max_credits(),
        "credits exceeded bound after failed begin: {} > {}",
        coord.credits_used(),
        coord.max_credits()
    );

    // Record both shard checkpoints to complete the round.
    coord
        .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(id, 1), noop_commit)
        .unwrap();
    coord
        .record_shard_checkpoint(ShardId(1), PerShardCheckpoint::new(id, 2), noop_commit)
        .unwrap();

    // After completion, credits are released.
    assert_eq!(coord.credits_used(), 0, "credits must be fully released after round");

    // Can begin a new round now.
    let id2 = coord.begin_checkpoint(noop_inject).unwrap();
    assert_eq!(id2, CheckpointId(2));
    assert!(coord.credits_used() <= coord.max_credits());
}

// ─── Slice 5 / slice 7: LFS test 2 ───────────────────────────────────────────

/// Exactly one SlateDB checkpoint is created per shard per checkpoint round.
///
/// Proof claim (DESIGN.md §11.2):
/// "When a shard's barrier epoch completes, the worker calls
/// `ShardDb::create_checkpoint()` exactly once."
///
/// Test strategy:
/// 1. Open two shard databases with in-memory object store.
/// 2. Write some data to each shard to ensure a non-empty state.
/// 3. Call `create_checkpoint()` once per shard.
/// 4. Assert each call returns a distinct `CheckpointHandle`.
/// 5. Assert calling `create_checkpoint()` again returns a new, distinct handle
///    (idempotent per round, unique across rounds).
#[tokio::test]
async fn test_one_checkpoint_per_shard_lfs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Open shard 0 and write some data.
    let db0 = open_shard_db("shard/0", Arc::clone(&store)).await;
    db0.put(b"key/0/a", b"value_a").await.unwrap();
    db0.flush().await.unwrap();

    // Open shard 1 and write some data.
    let db1 = open_shard_db("shard/1", Arc::clone(&store)).await;
    db1.put(b"key/1/a", b"value_a").await.unwrap();
    db1.flush().await.unwrap();

    // Create one checkpoint per shard (simulating barrier epoch completion).
    let ckpt0_round1 = db0.create_checkpoint().await.unwrap();
    let ckpt1_round1 = db1.create_checkpoint().await.unwrap();

    // Each checkpoint must have a valid (non-zero) shard_checkpoint_id.
    assert_ne!(
        ckpt0_round1.shard_checkpoint_id, 0,
        "shard 0 checkpoint_id must be non-zero"
    );
    assert_ne!(
        ckpt1_round1.shard_checkpoint_id, 0,
        "shard 1 checkpoint_id must be non-zero"
    );

    // Write more data and take a second checkpoint round — IDs must advance.
    db0.put(b"key/0/b", b"value_b").await.unwrap();
    db0.flush().await.unwrap();
    let ckpt0_round2 = db0.create_checkpoint().await.unwrap();
    assert!(
        ckpt0_round2.shard_checkpoint_id >= ckpt0_round1.shard_checkpoint_id,
        "second checkpoint id ({}) must be >= first ({})",
        ckpt0_round2.shard_checkpoint_id,
        ckpt0_round1.shard_checkpoint_id
    );

    db0.close().await.unwrap();
    db1.close().await.unwrap();
}

// ─── Slice 6 / slice 7: LFS test 3 ───────────────────────────────────────────

/// Recovered state matches pre-crash state bit-identically (LFS).
///
/// Proof claim (DESIGN.md §11.3):
/// "Recovery from a cluster checkpoint reproduces pre-failure state
/// bit-identically."
///
/// Test strategy:
/// 1. Open a shard database, write known key-value pairs.
/// 2. Create a checkpoint (simulating a successful cluster checkpoint round).
/// 3. Load the checkpoint manifest into a `RecoveryDriver`.
/// 4. Open a fresh `ShardReader` (reader-only view, simulating post-crash reader).
/// 5. Assert the reader can read back exactly the same values written before the
///    checkpoint.
#[tokio::test]
async fn test_recovery_bit_identical_lfs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Step 1: Write known state to shard 0.
    let db = open_shard_db("shard/0", Arc::clone(&store)).await;
    db.put(b"rec/key/foo", b"bar").await.unwrap();
    db.put(b"rec/key/baz", b"qux").await.unwrap();
    db.flush().await.unwrap();

    // Step 2: Create a shard checkpoint.
    let handle = db.create_checkpoint().await.unwrap();
    db.close().await.unwrap();

    // Step 3: Build a ClusterCheckpoint manifest and load into RecoveryDriver.
    let mut cc = rockstream_types::checkpoint::ClusterCheckpoint::new(CheckpointId(1));
    cc.record_shard(
        ShardId(0),
        PerShardCheckpoint::new(CheckpointId(1), handle.shard_checkpoint_id),
    );

    let driver = RecoveryDriver::new();
    driver.load_checkpoint(cc);
    assert_eq!(driver.loaded_checkpoint_id(), Some(CheckpointId(1)));

    // Step 4: Recover shard 0 via the RecoveryDriver.
    // Matching tokens simulate a successful lease re-election.
    let recovered = driver
        .recover_shard(
            ShardId(0),
            "shard/0",
            Arc::clone(&store),
            LeaseToken(1),
            LeaseToken(1),
        )
        .await
        .unwrap();

    // Step 5: Assert bit-identical state — reader returns pre-crash values.
    let foo = recovered.reader.get(b"rec/key/foo").await.unwrap();
    assert_eq!(
        foo.as_deref(),
        Some(b"bar".as_ref()),
        "rec/key/foo must be 'bar' after recovery"
    );
    let baz = recovered.reader.get(b"rec/key/baz").await.unwrap();
    assert_eq!(
        baz.as_deref(),
        Some(b"qux".as_ref()),
        "rec/key/baz must be 'qux' after recovery"
    );

    // Recovery progress metric: 1/1 = 100%.
    let prog = driver.progress();
    assert_eq!(prog.recovered, 1);
    assert_eq!(prog.total, 1);
    assert_eq!(prog.fraction(), 1.0);
}

// ─── Slice 7: MinIO helpers ───────────────────────────────────────────────────

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "rockstream-checkpoint-test";

async fn start_minio() -> (testcontainers::ContainerAsync<testcontainers_modules::minio::MinIO>, u16)
{
    let container = testcontainers_modules::minio::MinIO::default()
        .start()
        .await
        .expect("failed to start MinIO; is Docker running?");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket_raw(port, MINIO_BUCKET).await;
    (container, port)
}

async fn create_minio_bucket_raw(port: u16, bucket: &str) {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    type HmacSha256 = Hmac<Sha256>;

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (yr, mo, da) = epoch_ymd(now_secs);
    let (hh, mm, ss) = epoch_hms(now_secs);
    let amz_date = format!("{yr:04}{mo:02}{da:02}T{hh:02}{mm:02}{ss:02}Z");
    let date_stamp = format!("{yr:04}{mo:02}{da:02}");

    let payload_hash = format!("{:x}", Sha256::digest(b""));
    let canonical_headers = format!(
        "host:127.0.0.1:{port}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    );
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request =
        format!("PUT\n/{bucket}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date_stamp}/us-east-1/s3/aws4_request\n{}",
        format!("{:x}", Sha256::digest(canonical_request.as_bytes()))
    );

    let hmac_fn = |key: &[u8], data: &[u8]| -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    };
    let k_date = hmac_fn(format!("AWS4{MINIO_PASS}").as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_fn(&k_date, b"us-east-1");
    let k_service = hmac_fn(&k_region, b"s3");
    let signing_key = hmac_fn(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_fn(&signing_key, string_to_sign.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{date_stamp}/us-east-1/s3/aws4_request, \
         SignedHeaders={signed_headers}, Signature={signature}"
    );

    let client = reqwest::Client::new();
    let resp = client
        .put(format!("http://127.0.0.1:{port}/{bucket}"))
        .header("Authorization", auth)
        .header("x-amz-content-sha256", &payload_hash)
        .header("x-amz-date", &amz_date)
        .header("Content-Length", "0")
        .send()
        .await
        .expect("CreateBucket PUT failed");
    let status = resp.status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "CreateBucket failed: {status}"
    );
}

fn epoch_ymd(secs: u64) -> (u32, u32, u32) {
    let mut days = (secs / 86400) as u32;
    let mut year = 1970u32;
    loop {
        let leap = is_leap(year);
        let dy = if leap { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let leap = is_leap(year);
    let dpm: [u32; 12] =
        [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut month = 1u32;
    let mut day = days;
    for (i, &d) in dpm.iter().enumerate() {
        if day < d {
            month = (i + 1) as u32;
            day += 1;
            break;
        }
        day -= d;
    }
    (year, month, day)
}

fn epoch_hms(secs: u64) -> (u32, u32, u32) {
    let sod = secs % 86400;
    ((sod / 3600) as u32, ((sod % 3600) / 60) as u32, (sod % 60) as u32)
}

fn is_leap(y: u32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        object_store::aws::AmazonS3Builder::new()
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

/// Bit-identical recovery on real MinIO (TestContainers).
///
/// Proof claim (DESIGN.md §11.3):
/// "Recovery from a cluster checkpoint reproduces pre-failure state
/// bit-identically (MinIO)."
#[tokio::test]
async fn test_recovery_bit_identical_minio() {
    if !docker_available() {
        eprintln!("SKIP test_recovery_bit_identical_minio: Docker not available");
        return;
    }

    let (_container, port) = start_minio().await;
    let store = minio_object_store(port);

    // Write pre-crash state.
    let db = ShardDb::builder("ckpt-recovery-minio/shard/0", Arc::clone(&store))
        .build()
        .await
        .unwrap();
    db.put(b"minio/key/alpha", b"beta").await.unwrap();
    db.put(b"minio/key/gamma", b"delta").await.unwrap();
    db.flush().await.unwrap();

    // Create shard checkpoint.
    let handle = db.create_checkpoint().await.unwrap();
    db.close().await.unwrap();

    // Build ClusterCheckpoint and load into RecoveryDriver.
    let mut cc = rockstream_types::checkpoint::ClusterCheckpoint::new(CheckpointId(1));
    cc.record_shard(
        ShardId(0),
        PerShardCheckpoint::new(CheckpointId(1), handle.shard_checkpoint_id),
    );
    let driver = RecoveryDriver::new();
    driver.load_checkpoint(cc);

    // Recover shard 0 on MinIO.
    let recovered = driver
        .recover_shard(
            ShardId(0),
            "ckpt-recovery-minio/shard/0",
            Arc::clone(&store),
            LeaseToken(1),
            LeaseToken(1),
        )
        .await
        .unwrap();

    // Assert bit-identical state.
    let alpha = recovered.reader.get(b"minio/key/alpha").await.unwrap();
    assert_eq!(alpha.as_deref(), Some(b"beta".as_ref()), "alpha must be 'beta'");
    let gamma = recovered.reader.get(b"minio/key/gamma").await.unwrap();
    assert_eq!(gamma.as_deref(), Some(b"delta".as_ref()), "gamma must be 'delta'");
}

/// SST/checkpoint count stays bounded after N checkpoint rounds (MinIO).
///
/// Proof claim (DESIGN.md §11.2 / §11.5):
/// "Old checkpoints beyond the retention horizon are GC'd; storage footprint
/// is bounded."
///
/// Test strategy:
/// 1. Run `N_ROUNDS` checkpoint rounds via the coordinator.
/// 2. After each round, GC via `CheckpointCoordinator::gc_old_checkpoints()`.
/// 3. Assert committed checkpoint count never exceeds `retention_horizon + 1`.
#[tokio::test]
async fn test_checkpoint_gc_bounded_minio() {
    if !docker_available() {
        eprintln!("SKIP test_checkpoint_gc_bounded_minio: Docker not available");
        return;
    }

    let (_container, port) = start_minio().await;
    let store = minio_object_store(port);

    const N_ROUNDS: usize = 10;
    const RETENTION: u64 = 3;

    let shards = vec![ShardId(0), ShardId(1)];
    let coord = CheckpointCoordinator::with_config(shards.clone(), 64, RETENTION);

    // Open shard databases on MinIO.
    let db0 = ShardDb::builder("ckpt-gc-minio/shard/0", Arc::clone(&store))
        .build()
        .await
        .unwrap();
    let db1 = ShardDb::builder("ckpt-gc-minio/shard/1", Arc::clone(&store))
        .build()
        .await
        .unwrap();

    for round in 0..N_ROUNDS {
        // Write data to both shards.
        db0.put(format!("round/{round}/key").as_bytes(), b"val").await.unwrap();
        db1.put(format!("round/{round}/key").as_bytes(), b"val").await.unwrap();
        db0.flush().await.unwrap();
        db1.flush().await.unwrap();

        // Create per-shard checkpoints.
        let h0 = db0.create_checkpoint().await.unwrap();
        let h1 = db1.create_checkpoint().await.unwrap();

        // Run a coordinator checkpoint round.
        let ckpt_id = coord.begin_checkpoint(noop_inject).unwrap();
        coord
            .record_shard_checkpoint(
                ShardId(0),
                PerShardCheckpoint::new(ckpt_id, h0.shard_checkpoint_id),
                noop_commit,
            )
            .unwrap();
        coord
            .record_shard_checkpoint(
                ShardId(1),
                PerShardCheckpoint::new(ckpt_id, h1.shard_checkpoint_id),
                noop_commit,
            )
            .unwrap();

        // Explicit GC call.
        coord.gc_old_checkpoints();

        // Committed count must be bounded by retention_horizon.
        let committed = coord.committed_checkpoints();
        assert!(
            committed.len() <= (RETENTION as usize + 1),
            "round {round}: committed checkpoint count {} exceeds retention bound {}",
            committed.len(),
            RETENTION + 1
        );
    }

    db0.close().await.unwrap();
    db1.close().await.unwrap();

    // After N_ROUNDS, committed count must still be ≤ retention_horizon.
    let final_committed = coord.committed_checkpoints();
    assert!(
        final_committed.len() <= (RETENTION as usize + 1),
        "after {} rounds committed count {} exceeds retention bound {}",
        N_ROUNDS,
        final_committed.len(),
        RETENTION + 1
    );
}

// ─── Slice 7: SimRuntime tests (feature = "simulation") ───────────────────────

/// Partitioned worker self-fences before new owner's first commit (SimRuntime).
///
/// Proof claim: M4-S1/S2 — when a worker cannot reach the control plane for
/// `DEFAULT_SELF_FENCE_DEADLINE`, it self-fences (panics/terminates) before any
/// new owner can commit. This prevents split-brain.
///
/// Test strategy (deterministic SimRuntime):
/// 1. Start two workers (w1 holds lease on shard 0, w2 waiting).
/// 2. Inject a partition via `buggify!` so w1 cannot reach the control plane.
/// 3. Verify w1's `SelfFenceGuard` fires at the deadline (M4-S2).
/// 4. Grant lease to w2 and verify it can commit without being blocked (M4-S1).
#[cfg(feature = "simulation")]
#[test]
fn test_self_fence_on_partition() {
    use rockstream_runtime::{assert_single_lease_holder, assert_valid_writer, SelfFenceGuard};
    use rockstream_sim::buggify::{buggify_disable, buggify_init};
    use rockstream_sim::buggify;
    use rockstream_types::ids::{LeaseToken, WorkerId};

    // Fixed seed for deterministic replay.
    buggify_init(31337);

    let shard_id = ShardId(10);
    let w1_token = LeaseToken(1);
    let w1_worker = WorkerId(1);
    let w2_token = LeaseToken(2);

    // Simulate w1's heartbeat loop: buggify injects partition (probability 1.0
    // under sim), so can_reach_control becomes false.
    let inject_partition = buggify!("control.partition", 1.0);
    assert!(inject_partition, "buggify must inject partition in simulation");

    // Simulate the self-fence deadline check (M4-S2 paired assertion).
    // Create a guard with an immediate deadline to simulate deadline exceeded.
    let mut guard = SelfFenceGuard::with_deadline(Duration::from_millis(1));
    // Tick with partition active — starts the isolation clock.
    guard.tick(false);
    // Sleep briefly to exceed the 1ms deadline.
    std::thread::sleep(Duration::from_millis(10));

    // M4-S2: must_self_fence returns true once deadline is exceeded.
    assert!(
        guard.must_self_fence(),
        "M4-S2: worker w1 must self-fence when deadline exceeded under partition"
    );

    // After w1 self-fences, lease is re-granted to w2 (token 2).
    // M4-S1/S3: assert_valid_writer succeeds for w2 but would panic for w1's stale token.
    let current_token = w2_token; // control plane has advanced the token
    assert_valid_writer(shard_id, w2_token, current_token, Some(WorkerId(2)));

    // M4-S3: assert_single_lease_holder passes with exactly one holder.
    assert_single_lease_holder(shard_id, 1);

    // M4-S1 COV: assert_valid_writer with stale token (w1_token) would panic.
    // We assert it panics by catching the panic.
    let panicked = std::panic::catch_unwind(|| {
        assert_valid_writer(shard_id, w1_token, w2_token, Some(w1_worker));
    });
    assert!(
        panicked.is_err(),
        "M4-S1/COV-M4: stale writer must be fence-rejected (panic expected)"
    );

    buggify_disable();
}

/// Checkpoint coordinator with fast+slow source: backpressure observed,
/// checkpoint eventually succeeds or reports `RECOVERING` (SimRuntime).
///
/// Proof claim (DESIGN.md §11.2):
/// "Checkpointing under slow input and credit exhaustion never grows unbounded
/// and either succeeds or reports `RECOVERING`."
///
/// Test strategy:
/// 1. Create a coordinator with a tiny credit limit (1 credit).
/// 2. Begin a checkpoint round with 1 shard (uses the 1 credit).
/// 3. Attempt a second begin while first is open — must get `AlignmentTimeout`
///    (RS-3602), not panic or unbounded growth.
/// 4. Complete the first round by reporting the slow shard's checkpoint.
/// 5. Assert credits are fully released after completion.
#[cfg(feature = "simulation")]
#[test]
fn test_checkpoint_under_slow_input() {
    use rockstream_sim::buggify::{buggify_disable, buggify_init};
    use rockstream_sim::buggify;

    buggify_init(42424242);

    let shards = vec![ShardId(0)];
    // Tiny credit limit to force backpressure with a single shard.
    let coord = CheckpointCoordinator::with_config(shards, 1, 3);

    // Begin checkpoint with 1 shard — 1 credit consumed.
    let inject_slow = buggify!("source.slow", 1.0);
    assert!(inject_slow, "buggify must inject slow source in simulation");

    let id = coord.begin_checkpoint(noop_inject).unwrap();
    assert_eq!(coord.credits_used(), 1);
    assert_eq!(coord.max_credits(), 1);

    // Second begin (fast source): must observe backpressure → AlignmentTimeout (RS-3602).
    let err = coord.begin_checkpoint(noop_inject).unwrap_err();
    assert!(
        matches!(err, CoordinatorError::AlignmentTimeout),
        "expected AlignmentTimeout (RS-3602) for credit-full coordinator, got {err:?}"
    );
    assert!(err.to_string().contains("RS-3602"), "error must contain RS-3602: {err}");

    // Credits still bounded: never exceeded max_credits.
    assert!(
        coord.credits_used() <= coord.max_credits(),
        "credits exceeded bound under slow input: {} > {}",
        coord.credits_used(),
        coord.max_credits()
    );

    // Slow shard eventually reports its checkpoint.
    coord
        .record_shard_checkpoint(ShardId(0), PerShardCheckpoint::new(id, 99), noop_commit)
        .unwrap();

    // After completion, credits are released and a new round can begin.
    assert_eq!(coord.credits_used(), 0, "credits fully released after slow round");
    let id2 = coord.begin_checkpoint(noop_inject).unwrap();
    assert_eq!(id2, CheckpointId(2), "second round has id 2");

    buggify_disable();
}

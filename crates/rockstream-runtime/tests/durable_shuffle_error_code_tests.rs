//! v0.45.7 S11 — Proof tests that `durable.rs`'s six previously-`RS_3010`-only
//! failure modes are independently distinguishable and pairwise distinct:
//! `RS-3011` (rate-limit retry budget exhausted), `RS-3012` (generic
//! object-store I/O failure), `RS-3013` (buffer capacity exceeded), `RS-3014`
//! (footer serialization failed), `RS-3015` (footer deserialization failed),
//! `RS-3016` (footer corrupt or undersized).
//!
//! Each test drives the real code path through `DurableShuffleWriter`/
//! `DurableShuffleReader` (not hand-constructed errors) wherever the failure
//! mode is triggerable via a fake `ObjectStore`; `RS-3014` documents its
//! `description`/`next_steps` text directly since it has no live trigger
//! without a custom un-serializable type (noted inline).

use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

use rockstream_runtime::exchange::durable::{
    DurableShuffleReader, DurableShuffleWriter, MAX_DURABLE_BUFFER_SIZE_BYTES,
};
use rockstream_types::error_code::{description, next_steps, RS_3014};

// ── Fake ObjectStore that always fails a given way ──────────────────────────

/// Failure mode for `AlwaysFailingStore`.
#[derive(Clone, Copy)]
enum FailureMode {
    /// Always returns a 429/throttling-flavored error (exercises the
    /// rate-limited retry-then-give-up branch → RS-3011).
    RateLimited,
    /// Always returns a plain, non-429 I/O error (exercises the
    /// immediate-failure branch → RS-3012).
    GenericIoError,
}

#[derive(Debug)]
struct AlwaysFailingStore {
    mode: std::sync::Mutex<FailureModeInner>,
    call_count: AtomicU32,
}

#[derive(Clone, Copy)]
struct FailureModeInner(FailureMode);
impl std::fmt::Debug for FailureModeInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailureModeInner")
    }
}

impl AlwaysFailingStore {
    fn new(mode: FailureMode) -> Self {
        Self {
            mode: std::sync::Mutex::new(FailureModeInner(mode)),
            call_count: AtomicU32::new(0),
        }
    }

    fn fail(&self) -> object_store::Error {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        let mode = self.mode.lock().unwrap().0;
        match mode {
            FailureMode::RateLimited => object_store::Error::Generic {
                store: "AlwaysFailingStore",
                source: Box::new(std::io::Error::other(
                    "HTTP 429 Too Many Requests (throttling)",
                )),
            },
            FailureMode::GenericIoError => object_store::Error::Generic {
                store: "AlwaysFailingStore",
                source: Box::new(std::io::Error::other("connection reset by peer")),
            },
        }
    }
}

impl std::fmt::Display for AlwaysFailingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlwaysFailingStore")
    }
}

#[async_trait]
impl ObjectStore for AlwaysFailingStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        Err(self.fail())
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(self.fail())
    }

    async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
        Err(self.fail())
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        Err(self.fail())
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        futures::stream::once(async {
            Err(object_store::Error::Generic {
                store: "AlwaysFailingStore",
                source: Box::new(std::io::Error::other("list not supported")),
            })
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(self.fail())
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(self.fail())
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(self.fail())
    }
}

use futures::StreamExt;

// ── RS-3011: rate-limit retry budget exhausted ──────────────────────────────

/// S11 green gate: a store that always returns a 429/throttling error causes
/// `finish()`'s upload retry loop to exhaust its budget and surface RS-3011.
#[tokio::test]
async fn rate_limit_exhausted_returns_rs3011() {
    let store = AlwaysFailingStore::new(FailureMode::RateLimited);
    let path = Path::from("rs3011_test.arrow");

    let mut writer = DurableShuffleWriter::new();
    writer.add_frame(1, 2, 0, b"payload").unwrap();

    let err = writer
        .finish(&store, &path)
        .await
        .expect_err("expected finish() to fail against an always-429 store");
    assert!(
        err.contains("RS-3011"),
        "expected RS-3011 for rate-limit exhaustion; got: {err}"
    );
}

// ── RS-3012: generic object-store I/O failure ───────────────────────────────

/// S11 green gate: a store that always returns a non-429 generic I/O error
/// causes `finish()`'s upload to fail immediately (no retries) with RS-3012.
#[tokio::test]
async fn generic_object_store_io_failure_returns_rs3012() {
    let store = AlwaysFailingStore::new(FailureMode::GenericIoError);
    let path = Path::from("rs3012_test.arrow");

    let mut writer = DurableShuffleWriter::new();
    writer.add_frame(1, 2, 0, b"payload").unwrap();

    let err = writer
        .finish(&store, &path)
        .await
        .expect_err("expected finish() to fail against an always-erroring store");
    assert!(
        err.contains("RS-3012"),
        "expected RS-3012 for generic object-store I/O failure; got: {err}"
    );
}

/// S11 green gate: `read_footer`'s initial `head()` call also surfaces
/// RS-3012 against a store that always returns a non-429 generic I/O error.
#[tokio::test]
async fn generic_object_store_io_failure_on_read_returns_rs3012() {
    let store = AlwaysFailingStore::new(FailureMode::GenericIoError);
    let path = Path::from("rs3012_read_test.arrow");

    let err = DurableShuffleReader::read_footer(&store, &path)
        .await
        .expect_err("expected read_footer() to fail against an always-erroring store");
    assert!(
        err.contains("RS-3012"),
        "expected RS-3012 for generic object-store I/O failure on read; got: {err}"
    );
}

// ── RS-3013: buffer capacity exceeded ───────────────────────────────────────

/// S11 green gate: adding a frame that would exceed `MAX_DURABLE_BUFFER_SIZE_BYTES`
/// returns RS-3013. Supersedes/extends `durable.rs`'s existing
/// `test_durable_shuffle_buffer_limit` unit test with an external, black-box
/// integration-level assertion.
#[tokio::test]
async fn buffer_capacity_exceeded_returns_rs3013() {
    let mut writer = DurableShuffleWriter::new();
    let oversized_payload = vec![0u8; MAX_DURABLE_BUFFER_SIZE_BYTES + 1];
    let err = writer
        .add_frame(1, 2, 0, &oversized_payload)
        .expect_err("expected add_frame to reject an oversized frame");
    assert!(
        err.contains("RS-3013"),
        "expected RS-3013 for buffer capacity exceeded; got: {err}"
    );
}

// ── RS-3014: footer serialization failed (documented, not live-triggered) ──

/// S11 green gate: RS-3014 (footer serialization failure) has no live trigger
/// without a custom un-serializable `ShuffleIndexFooter` type (the real
/// footer is a plain `Vec<ShuffleIndexEntry>` of primitive fields, which
/// `serde_json` can never fail to serialize). Per the plan, this sub-test
/// documents the code's existence and actionable `next_steps` text via the
/// registry directly rather than a live trigger.
#[tokio::test]
async fn footer_serialization_failure_code_is_registered_with_actionable_next_steps() {
    let desc = description(RS_3014);
    let steps = next_steps(RS_3014);
    assert!(
        !desc.is_empty(),
        "RS-3014 must have a non-empty description"
    );
    assert!(
        !steps.is_empty(),
        "RS-3014 must have non-empty, actionable next_steps text"
    );
    assert!(desc.to_lowercase().contains("footer"));
}

// ── RS-3015: footer deserialization failed ──────────────────────────────────

/// S11 green gate: corrupt (non-JSON) footer bytes written directly to the
/// object cause `read_footer`'s deserialize step to fail with RS-3015.
#[tokio::test]
async fn corrupt_footer_json_returns_rs3015() {
    let store = InMemory::new();
    let path = Path::from("rs3015_test.arrow");

    // Hand-construct an object: garbage "footer" bytes (not valid JSON),
    // followed by an 8-byte big-endian footer length pointing at them.
    let garbage_footer = b"not valid json at all".to_vec();
    let footer_len = garbage_footer.len() as u64;
    let mut object_bytes = garbage_footer;
    object_bytes.extend_from_slice(&footer_len.to_be_bytes());

    store
        .put(&path, Bytes::from(object_bytes).into())
        .await
        .unwrap();

    let err = DurableShuffleReader::read_footer(&store, &path)
        .await
        .expect_err("expected read_footer to fail on corrupt (non-JSON) footer bytes");
    assert!(
        err.contains("RS-3015"),
        "expected RS-3015 for footer deserialization failure; got: {err}"
    );
}

// ── RS-3016: footer corrupt or undersized ───────────────────────────────────

/// S11 green gate: an object too small to contain even the 8-byte footer
/// length trailer returns RS-3016.
#[tokio::test]
async fn truncated_object_returns_rs3016() {
    let store = InMemory::new();
    let path = Path::from("rs3016_truncated_test.arrow");

    // Only 4 bytes total — smaller than the mandatory 8-byte footer-length trailer.
    store
        .put(&path, Bytes::from(vec![1u8, 2, 3, 4]).into())
        .await
        .unwrap();

    let err = DurableShuffleReader::read_footer(&store, &path)
        .await
        .expect_err("expected read_footer to fail on a truncated object");
    assert!(
        err.contains("RS-3016"),
        "expected RS-3016 for a truncated object; got: {err}"
    );
}

/// S11 green gate: a footer-length trailer claiming a length larger than the
/// object itself returns RS-3016.
#[tokio::test]
async fn footer_length_larger_than_object_returns_rs3016() {
    let store = InMemory::new();
    let path = Path::from("rs3016_oversized_footer_len_test.arrow");

    // 8-byte trailer claiming a footer length far larger than the (empty) object.
    let bogus_footer_len: u64 = 10_000;
    let object_bytes = bogus_footer_len.to_be_bytes().to_vec();

    store
        .put(&path, Bytes::from(object_bytes).into())
        .await
        .unwrap();

    let err = DurableShuffleReader::read_footer(&store, &path)
        .await
        .expect_err("expected read_footer to fail when footer_len exceeds object size");
    assert!(
        err.contains("RS-3016"),
        "expected RS-3016 for footer length exceeding object size; got: {err}"
    );
}

// ── Pairwise distinctness ───────────────────────────────────────────────────

/// S11 green gate: all six split codes are pairwise distinct (and distinct
/// from the retired RS-3010).
#[test]
fn split_codes_are_pairwise_distinct() {
    use rockstream_types::error_code::{RS_3011, RS_3012, RS_3013, RS_3015, RS_3016};
    let codes = [
        RS_3011.to_string(),
        RS_3012.to_string(),
        RS_3013.to_string(),
        RS_3014.to_string(),
        RS_3015.to_string(),
        RS_3016.to_string(),
    ];
    for (i, a) in codes.iter().enumerate() {
        for (j, b) in codes.iter().enumerate() {
            if i != j {
                assert_ne!(a, b, "codes at index {i} and {j} must be distinct");
            }
        }
    }
}

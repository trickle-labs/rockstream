//! Issue #276: a bounded ENOSPC at the object-store boundary must withhold the
//! write acknowledgement and recover only a whole epoch after capacity returns.

use std::{
    error::Error as StdError,
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use rockstream_storage::{ShardDb, ShardKeyEncoder, WriteBatch};
use tempfile::TempDir;
use tokio::sync::Notify;

struct EnospcStore {
    inner: Arc<LocalFileSystem>,
    fail_puts: AtomicBool,
    failures: AtomicUsize,
    fault_path: Mutex<Option<String>>,
    fault_reached: Notify,
}

impl fmt::Debug for EnospcStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnospcStore").finish_non_exhaustive()
    }
}

impl fmt::Display for EnospcStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EnospcStore")
    }
}

impl EnospcStore {
    fn fail_if_requested(&self, location: &ObjectPath) -> object_store::Result<()> {
        // shortcut: fail every put only during the single staged write, until capacity is restored.
        if self.fail_puts.load(Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            *self.fault_path.lock().unwrap() = Some(location.to_string());
            self.fault_reached.notify_one();
            return Err(object_store::Error::Generic {
                store: "bounded ENOSPC adapter",
                source: Box::new(std::io::Error::from_raw_os_error(libc::ENOSPC)),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for EnospcStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        bytes: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.fail_if_requested(location)?;
        self.inner.put_opts(location, bytes, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.fail_if_requested(location)?;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[derive(Debug, PartialEq, Eq)]
struct EpochState {
    row_1: Option<Vec<u8>>,
    row_2: Option<Vec<u8>>,
    row_3: Option<Vec<u8>>,
    source_offset: Option<Vec<u8>>,
    frontier: Option<Vec<u8>>,
    operation_id: Option<Vec<u8>>,
}

async fn open_shard(store: Arc<EnospcStore>) -> ShardDb {
    ShardDb::builder("shard", store)
        .with_flush_interval(Duration::from_millis(10))
        .build()
        .await
        .unwrap()
}

async fn open_shard_for_crash_test(store: Arc<EnospcStore>) -> ShardDb {
    ShardDb::builder("shard", store)
        .with_flush_interval(Duration::from_secs(60))
        .build()
        .await
        .unwrap()
}

async fn value(db: &ShardDb, key: &[u8]) -> Option<Vec<u8>> {
    db.get(key).await.unwrap().map(|value| value.to_vec())
}

async fn state(db: &ShardDb) -> EpochState {
    EpochState {
        row_1: value(db, b"row/1").await,
        row_2: value(db, b"row/2").await,
        row_3: value(db, b"row/3").await,
        source_offset: value(db, b"source/orders/offset").await,
        frontier: value(db, &ShardKeyEncoder::frontier_key()).await,
        operation_id: value(db, b"operation/last").await,
    }
}

fn has_enospc(error: &(dyn StdError + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| io_error.raw_os_error() == Some(libc::ENOSPC))
        {
            return true;
        }
        cause = error.source();
    }
    false
}

#[tokio::test]
async fn enospc_withholds_ack_and_recovers_a_complete_epoch() {
    let dir = TempDir::new().unwrap();
    let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let store = Arc::new(EnospcStore {
        inner,
        fail_puts: AtomicBool::new(false),
        failures: AtomicUsize::new(0),
        fault_path: Mutex::new(None),
        fault_reached: Notify::new(),
    });
    let frontier_key = ShardKeyEncoder::frontier_key();
    let expected_epoch_1 = EpochState {
        row_1: Some(b"10:100".to_vec()),
        row_2: Some(b"20:50".to_vec()),
        row_3: None,
        source_offset: Some(b"12".to_vec()),
        frontier: Some(1u64.to_be_bytes().to_vec()),
        operation_id: Some(b"epoch-1".to_vec()),
    };
    let expected_epoch_2 = EpochState {
        row_1: Some(b"10:200".to_vec()),
        row_2: None,
        row_3: Some(b"30:75".to_vec()),
        source_offset: Some(b"15".to_vec()),
        frontier: Some(2u64.to_be_bytes().to_vec()),
        operation_id: Some(b"epoch-2".to_vec()),
    };
    if let Ok(root) = std::env::var("ROCKSTREAM_ENOSPC_RECOVERY_DIR") {
        let store = Arc::new(EnospcStore {
            inner: Arc::new(LocalFileSystem::new_with_prefix(root).unwrap()),
            fail_puts: AtomicBool::new(false),
            failures: AtomicUsize::new(0),
            fault_path: Mutex::new(None),
            fault_reached: Notify::new(),
        });
        let recovered = open_shard(store).await;
        let actual = state(&recovered).await;
        assert_eq!(actual, expected_epoch_2, "fresh-process recovered state");
        std::fs::write(
            std::env::var("ROCKSTREAM_ENOSPC_RECOVERY_RESULT").unwrap(),
            format!("{actual:?}"),
        )
        .unwrap();
        recovered.close().await.unwrap();
        return;
    }

    let db = open_shard(store.clone()).await;
    let mut epoch_1 = WriteBatch::new();
    epoch_1.put(b"row/1", b"10:100");
    epoch_1.put(b"row/2", b"20:50");
    epoch_1.put(b"source/orders/offset", b"12");
    epoch_1.put(&frontier_key, &1u64.to_be_bytes());
    epoch_1.put(b"operation/last", b"epoch-1");
    db.write_batch(epoch_1).await.unwrap();
    db.flush().await.unwrap();
    assert_eq!(state(&db).await, expected_epoch_1);
    db.close().await.unwrap();

    let db = open_shard(store.clone()).await;
    let mut epoch_2 = WriteBatch::new();
    epoch_2.put(b"row/1", b"10:200");
    epoch_2.delete(b"row/2");
    epoch_2.put(b"row/3", b"30:75");
    epoch_2.put(b"source/orders/offset", b"15");
    epoch_2.put(&frontier_key, &2u64.to_be_bytes());
    epoch_2.put(b"operation/last", b"epoch-2");
    let epoch_2_retry = epoch_2.clone();
    store.fail_puts.store(true, Ordering::SeqCst);
    let (fault_response, ack_after_recovery) = {
        let fault_reached = store.fault_reached.notified();
        tokio::pin!(fault_reached);
        let write = db.write_batch(epoch_2);
        tokio::pin!(write);
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::select! {
                _ = &mut fault_reached => {},
                result = &mut write => panic!("write completed before the fault was observed: {result:?}"),
            }
        })
        .await
        .expect("fault adapter must intercept a production storage put");
        let response_at_fault = tokio::time::timeout(Duration::from_millis(50), &mut write).await;
        let fault_response = match response_at_fault {
            Err(_) => "pending",
            Ok(Err(error)) => {
                assert!(has_enospc(&error), "expected OS ENOSPC, got {error:?}");
                "error"
            }
            Ok(Ok(())) => panic!("storage acknowledged the epoch while ENOSPC was active"),
        };
        store.fail_puts.store(false, Ordering::SeqCst);
        let ack_after_recovery = if fault_response == "pending" {
            match tokio::time::timeout(Duration::from_secs(10), &mut write)
                .await
                .expect("write must resume after capacity is restored")
            {
                Ok(()) => "completed after capacity was restored",
                Err(error) => {
                    assert!(has_enospc(&error), "expected OS ENOSPC, got {error:?}");
                    db.write_batch(epoch_2_retry).await.unwrap();
                    "retried after capacity was restored"
                }
            }
        } else {
            db.write_batch(epoch_2_retry).await.unwrap();
            "retried after capacity was restored"
        };
        (fault_response, ack_after_recovery)
    };
    assert!(store.failures.load(Ordering::SeqCst) > 0);
    db.flush().await.expect("flush after capacity is restored");
    db.close()
        .await
        .expect("close after restoring fixture capacity");

    let recovered = open_shard(store.clone()).await;
    let actual = state(&recovered).await;
    assert!(
        actual == expected_epoch_2,
        "recovery after restored capacity must expose the complete later epoch: {actual:?}"
    );
    recovered.close().await.unwrap();

    let result_dir = tempfile::tempdir().unwrap();
    let result_path = result_dir.path().join("fresh-process-state.txt");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "enospc_withholds_ack_and_recovers_a_complete_epoch",
            "--exact",
            "--nocapture",
        ])
        .env("ROCKSTREAM_ENOSPC_RECOVERY_DIR", dir.path())
        .env("ROCKSTREAM_ENOSPC_RECOVERY_RESULT", &result_path)
        .spawn()
        .unwrap();
    let child_pid = child.id();
    assert_ne!(child_pid, std::process::id());
    let child = child.wait_with_output().unwrap();
    assert!(
        child.status.success(),
        "fresh-process ENOSPC recovery failed: {}",
        String::from_utf8_lossy(&child.stderr)
    );
    let fresh_process_state = std::fs::read_to_string(&result_path).unwrap();
    assert_eq!(fresh_process_state, format!("{expected_epoch_2:?}"));
    let revision = std::env::var("GITHUB_SHA").unwrap_or_else(|_| "local".to_owned());
    eprintln!(
        "post_ack_enospc_v1 source_sha={revision} backend=LocalFileSystem fault=object-store-put path={:?} errno={} failures={} ack_at_fault=withheld fault_response={fault_response} ack_after_recovery={ack_after_recovery} before={expected_epoch_1:?} recovered={actual:?} fresh_process_pid={child_pid} fresh_process={fresh_process_state:?}",
        store.fault_path.lock().unwrap(),
        libc::ENOSPC,
        store.failures.load(Ordering::SeqCst)
    );
}

#[tokio::test]
#[ignore = "requires an isolated Linux dm-log-writes device"]
async fn dm_log_writes_replays_the_exact_post_ack_epoch() {
    let root = std::env::var("ROCKSTREAM_POST_ACK_ROOT").unwrap();
    let mode = std::env::var("ROCKSTREAM_POST_ACK_MODE").unwrap();
    let store = Arc::new(EnospcStore {
        inner: Arc::new(LocalFileSystem::new_with_prefix(root).unwrap()),
        fail_puts: AtomicBool::new(false),
        failures: AtomicUsize::new(0),
        fault_path: Mutex::new(None),
        fault_reached: Notify::new(),
    });
    let expected_epoch_1 = EpochState {
        row_1: Some(b"10:100".to_vec()),
        row_2: Some(b"20:50".to_vec()),
        row_3: None,
        source_offset: Some(b"12".to_vec()),
        frontier: Some(1u64.to_be_bytes().to_vec()),
        operation_id: Some(b"epoch-1".to_vec()),
    };
    let expected_epoch_2 = EpochState {
        row_1: Some(b"10:200".to_vec()),
        row_2: None,
        row_3: Some(b"30:75".to_vec()),
        source_offset: Some(b"15".to_vec()),
        frontier: Some(2u64.to_be_bytes().to_vec()),
        operation_id: Some(b"epoch-2".to_vec()),
    };
    let revision = std::env::var("GITHUB_SHA").unwrap_or_else(|_| "local".to_owned());

    if mode == "recover" {
        let recovered = open_shard_for_crash_test(store).await;
        let actual = state(&recovered).await;
        assert_eq!(actual, expected_epoch_2, "replayed post-ACK state");
        eprintln!(
            "post_ack_dm_log_writes_v1 source_sha={revision} backend=LocalFileSystem mode=recovered state={actual:?}"
        );
        return;
    }

    assert_eq!(mode, "write", "mode must be write or recover");
    let db = open_shard_for_crash_test(store).await;
    let mut epoch_1 = WriteBatch::new();
    epoch_1.put(b"row/1", b"10:100");
    epoch_1.put(b"row/2", b"20:50");
    epoch_1.put(b"source/orders/offset", b"12");
    epoch_1.put(&ShardKeyEncoder::frontier_key(), &1u64.to_be_bytes());
    epoch_1.put(b"operation/last", b"epoch-1");
    db.write_batch(epoch_1).await.unwrap();
    db.flush().await.unwrap();
    assert_eq!(state(&db).await, expected_epoch_1);

    let mut epoch_2 = WriteBatch::new();
    epoch_2.put(b"row/1", b"10:200");
    epoch_2.delete(b"row/2");
    epoch_2.put(b"row/3", b"30:75");
    epoch_2.put(b"source/orders/offset", b"15");
    epoch_2.put(&ShardKeyEncoder::frontier_key(), &2u64.to_be_bytes());
    epoch_2.put(b"operation/last", b"epoch-2");
    db.write_batch(epoch_2).await.unwrap();
    let before_ack = state(&db).await;
    assert_eq!(before_ack, expected_epoch_2, "state before durable flush");
    db.flush().await.unwrap();

    let pid = std::process::id();
    std::fs::write(
        std::env::var("ROCKSTREAM_POST_ACK_SIGNAL").unwrap(),
        format!(
            "post_ack_dm_log_writes_v1 source_sha={revision} backend=LocalFileSystem ack=ShardDb::flush-returned pid={pid} state={before_ack:?}"
        ),
    )
    .unwrap();
    std::future::pending::<()>().await;
}

use async_trait::async_trait;
use object_store::{path::Path as ObjectPath, ObjectStore};
use rockstream_control::audit::FileAuditLog;
use rockstream_control::kek::{EnvKekProvider, KekError, KekProvider, RetainedKekProvider};
use rockstream_control::secret_store::{SecretStore, CATALOG_SECRETS_PREFIX};
use rockstream_types::secret::{Secret, SecretType};
use slatedb::Db;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Notify;

async fn open_db(dir: &TempDir) -> Arc<Db> {
    Arc::new(
        Db::builder(
            "catalog_db",
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    )
}

struct FailSecondWrap {
    inner: EnvKekProvider,
    calls: AtomicUsize,
}

#[async_trait]
impl KekProvider for FailSecondWrap {
    fn provider_name(&self) -> &str {
        self.inner.provider_name()
    }

    async fn wrap_dek(&self, dek: &[u8]) -> Result<Vec<u8>, KekError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            return Err(KekError::provider_error("injected second-wrap failure"));
        }
        self.inner.wrap_dek(dek).await
    }

    async fn unwrap_dek(&self, wrapped_dek: &[u8]) -> Result<Vec<u8>, KekError> {
        self.inner.unwrap_dek(wrapped_dek).await
    }
}

async fn seed(store: &SecretStore) -> BTreeMap<u128, Secret> {
    let mut expected = BTreeMap::new();
    for (namespace, user, password) in [(11, "alpha", "alpha-secret"), (22, "beta", "beta-secret")]
    {
        let payload: HashMap<String, String> = [
            ("username".to_string(), user.to_string()),
            ("password".to_string(), password.to_string()),
        ]
        .into();
        let metadata = store
            .create_secret(
                namespace,
                "credentials",
                SecretType::SaslPlain,
                payload.clone(),
                "test",
            )
            .await
            .unwrap();
        expected.insert(
            namespace,
            Secret {
                name: "credentials".to_string(),
                secret_type: SecretType::SaslPlain,
                payload,
                metadata,
            },
        );
    }
    expected
}

async fn assert_secrets(store: &SecretStore, expected: &BTreeMap<u128, Secret>) {
    let mut actual = BTreeMap::new();
    for namespace in expected.keys() {
        let identities: Vec<_> = store
            .list_secrets(*namespace)
            .await
            .unwrap()
            .into_iter()
            .map(|secret| (secret.name, secret.secret_type))
            .collect();
        assert_eq!(
            identities,
            vec![(
                expected[namespace].name.clone(),
                expected[namespace].secret_type.clone()
            )]
        );
        actual.insert(
            *namespace,
            store.get_secret(*namespace, "credentials").await,
        );
    }
    let actual: BTreeMap<_, _> = actual
        .into_iter()
        .map(|(namespace, result)| (namespace, result.expect("all secrets must remain readable")))
        .collect();
    assert_eq!(&actual, expected);
}

async fn catalog(db: &Db) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut entries = BTreeMap::new();
    let mut scan = db.scan_prefix(CATALOG_SECRETS_PREFIX, ..).await.unwrap();
    while let Some(entry) = scan.next().await.unwrap() {
        entries.insert(entry.key.to_vec(), entry.value.to_vec());
    }
    entries
}

#[tokio::test]
async fn failed_second_wrap_preserves_every_secret_and_allows_retry() {
    for durable in [false, true] {
        let dir = TempDir::new().unwrap();
        let db = if durable {
            Some(open_db(&dir).await)
        } else {
            None
        };
        let audit_path = dir.path().join("audit.jsonl");
        let store = SecretStore::new(db.clone(), Arc::new(EnvKekProvider::new([1; 32])))
            .with_audit_log(Arc::new(FileAuditLog::open(&audit_path).unwrap()));
        let expected = seed(&store).await;
        let before = if let Some(db) = &db {
            Some(catalog(db).await)
        } else {
            None
        };
        let replacement = Arc::new(FailSecondWrap {
            inner: EnvKekProvider::new([2; 32]),
            calls: AtomicUsize::new(0),
        });
        let error = store
            .rotate_kek(replacement.clone(), "test")
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "[RS-2422] secret.encryption_failed: injected second-wrap failure"
        );
        assert_secrets(&store, &expected).await;
        if let Some(db) = &db {
            assert_eq!(Some(catalog(db).await), before);
        }
        assert_eq!(store.rotate_kek(replacement, "test").await.unwrap(), 2);
        assert_secrets(&store, &expected).await;
        let mut artifacts = std::fs::read(audit_path).unwrap();
        if let Some(db) = &db {
            for bytes in catalog(db).await.values() {
                let encrypted: rockstream_types::secret::EncryptedSecret =
                    serde_json::from_slice(bytes).unwrap();
                assert!(!encrypted.ciphertext.is_empty());
                assert!(!encrypted.wrapped_dek.is_empty());
                artifacts.extend(bytes);
            }
        }
        for literal in ["alpha-secret", "beta-secret"] {
            assert!(!artifacts
                .windows(literal.len())
                .any(|bytes| bytes == literal.as_bytes()));
        }
        if let Some(db) = db {
            drop(store);
            db.close().await.unwrap();
            let db = open_db(&dir).await;
            let store = SecretStore::new(Some(db.clone()), Arc::new(EnvKekProvider::new([2; 32])));
            assert_secrets(&store, &expected).await;
            db.close().await.unwrap();
        }
    }
}

struct PauseSecondWrap {
    inner: EnvKekProvider,
    calls: AtomicUsize,
    paused: Notify,
    resume: Notify,
}

impl PauseSecondWrap {
    fn new() -> Self {
        Self {
            inner: EnvKekProvider::new([2; 32]),
            calls: AtomicUsize::new(0),
            paused: Notify::new(),
            resume: Notify::new(),
        }
    }
}

#[async_trait]
impl KekProvider for PauseSecondWrap {
    fn provider_name(&self) -> &str {
        self.inner.provider_name()
    }

    async fn wrap_dek(&self, dek: &[u8]) -> Result<Vec<u8>, KekError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        self.inner.wrap_dek(dek).await
    }

    async fn unwrap_dek(&self, wrapped_dek: &[u8]) -> Result<Vec<u8>, KekError> {
        self.inner.unwrap_dek(wrapped_dek).await
    }
}

#[tokio::test]
async fn cancelled_preparation_and_rejected_commit_survive_reopen_and_retry() {
    for cancel in [true, false] {
        let dir = TempDir::new().unwrap();
        let db = open_db(&dir).await;
        let store = Arc::new(SecretStore::new(
            Some(db.clone()),
            Arc::new(EnvKekProvider::new([1; 32])),
        ));
        let expected = seed(&store).await;
        let replacement = Arc::new(PauseSecondWrap::new());
        let rotation = tokio::spawn({
            let store = store.clone();
            let replacement = replacement.clone();
            async move { store.rotate_kek(replacement, "test").await }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            replacement.paused.notified(),
        )
        .await
        .unwrap();
        db.flush().await.unwrap();
        if cancel {
            rotation.abort();
            assert!(rotation.await.unwrap_err().is_cancelled());
            assert_secrets(&store, &expected).await;
            db.close().await.unwrap();
        } else {
            db.close().await.unwrap();
            replacement.resume.notify_one();
            assert!(matches!(
                rotation.await.unwrap(),
                Err(rockstream_control::secret_store::SecretStoreError::Storage { .. })
            ));
        }
        drop(store);
        let db = open_db(&dir).await;
        let store = SecretStore::new(Some(db.clone()), Arc::new(EnvKekProvider::new([1; 32])));
        assert_secrets(&store, &expected).await;
        assert_eq!(
            store
                .rotate_kek(Arc::new(EnvKekProvider::new([2; 32])), "test")
                .await
                .unwrap(),
            2
        );
        assert_secrets(&store, &expected).await;
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn rotation_serializes_reads_writes_and_other_rotations() {
    for operation in ["read", "create", "alter", "drop", "rotate"] {
        let store = Arc::new(SecretStore::new(
            None,
            Arc::new(EnvKekProvider::new([1; 32])),
        ));
        let mut expected = seed(&store).await;
        let replacement = Arc::new(PauseSecondWrap::new());
        let rotation = tokio::spawn({
            let store = store.clone();
            let replacement = replacement.clone();
            async move { store.rotate_kek(replacement, "test").await }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            replacement.paused.notified(),
        )
        .await
        .unwrap();
        {
            let concurrent = async {
                match operation {
                    "read" => assert_eq!(
                        store.get_secret(11, "credentials").await.unwrap(),
                        expected[&11]
                    ),
                    "create" => {
                        let payload: HashMap<String, String> =
                            [("token".to_string(), "created-secret".to_string())].into();
                        let metadata = store
                            .create_secret(
                                33,
                                "credentials",
                                SecretType::BearerToken,
                                payload.clone(),
                                "test",
                            )
                            .await
                            .unwrap();
                        expected.insert(
                            33,
                            Secret {
                                name: "credentials".into(),
                                secret_type: SecretType::BearerToken,
                                payload,
                                metadata,
                            },
                        );
                    }
                    "alter" => {
                        let payload: HashMap<String, String> =
                            [("password".to_string(), "altered-secret".to_string())].into();
                        let metadata = store
                            .alter_secret(11, "credentials", payload.clone(), "test")
                            .await
                            .unwrap();
                        expected.get_mut(&11).unwrap().payload = payload;
                        expected.get_mut(&11).unwrap().metadata = metadata;
                    }
                    "drop" => {
                        store.drop_secret(22, "credentials", "test").await.unwrap();
                        expected.remove(&22);
                    }
                    "rotate" => assert_eq!(
                        store
                            .rotate_kek(Arc::new(EnvKekProvider::new([3; 32])), "test")
                            .await
                            .unwrap(),
                        2
                    ),
                    _ => unreachable!(),
                }
            };
            tokio::pin!(concurrent);
            assert!(
                futures::poll!(concurrent.as_mut()).is_pending(),
                "{operation} must wait for rotation"
            );
            replacement.resume.notify_one();
            assert_eq!(rotation.await.unwrap().unwrap(), 2);
            concurrent.await;
        }
        assert_secrets(&store, &expected).await;
        if operation == "drop" {
            assert_eq!(store.list_secrets(22).await.unwrap(), vec![]);
        }
    }
}

#[derive(Debug)]
struct WalFaultStore {
    inner: Arc<dyn ObjectStore>,
    action: AtomicUsize,
    paused: Notify,
    resume: Notify,
}

impl std::fmt::Display for WalFaultStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "rotation WAL fault store")
    }
}

#[async_trait]
impl ObjectStore for WalFaultStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let action = if location.as_ref().contains("/wal/") {
            self.action.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if action != 0 {
            self.paused.notify_one();
            self.resume.notified().await;
        }
        if action == 2 {
            return Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "injected WAL fencing error".into(),
            });
        }
        if action == 4 {
            return Err(object_store::Error::Generic {
                store: "rotation WAL fault store",
                source: "injected transient WAL write failure".into(),
            });
        }
        let result = self.inner.put_opts(location, payload, options).await?;
        if action == 3 {
            return Err(object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "injected lost WAL acknowledgement".into(),
            });
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn submitted_rotation_survives_cancellation_and_wal_errors() {
    // Cancel, fence, lose a durable acknowledgement, or fail one retryable write.
    for action in [1, 2, 3, 4] {
        let dir = TempDir::new().unwrap();
        let objects = Arc::new(WalFaultStore {
            inner: Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
            ),
            action: AtomicUsize::new(0),
            paused: Notify::new(),
            resume: Notify::new(),
        });
        let db = Arc::new(
            Db::builder("catalog_db", objects.clone())
                .build()
                .await
                .unwrap(),
        );
        let previous = Arc::new(EnvKekProvider::new([1; 32]));
        let replacement = Arc::new(EnvKekProvider::new([2; 32]));
        let store = Arc::new(SecretStore::new(Some(db.clone()), previous.clone()));
        let expected = seed(&store).await;
        db.flush().await.unwrap();
        let before = catalog(&db).await;
        objects.action.store(action, Ordering::SeqCst);
        let rotation = tokio::spawn({
            let store = store.clone();
            let replacement = replacement.clone();
            async move { store.rotate_kek(replacement, "test").await }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            objects.paused.notified(),
        )
        .await
        .unwrap();
        let submitted = catalog(&db).await;
        if action == 1 {
            rotation.abort();
            assert!(rotation.await.unwrap_err().is_cancelled());
            objects.resume.notify_one();
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                assert_secrets(&store, &expected),
            )
            .await
            .unwrap();
            db.close().await.unwrap();
        } else {
            objects.resume.notify_one();
            let result = tokio::time::timeout(std::time::Duration::from_secs(10), rotation)
                .await
                .unwrap()
                .unwrap();
            if action == 3 || action == 4 {
                assert_eq!(result.unwrap(), 2);
            } else {
                assert!(
                    matches!(
                        result,
                        Err(rockstream_control::secret_store::SecretStoreError::Storage { .. })
                    ),
                    "expected storage error for action {action}: {result:?}"
                );
            }
            let _ = db.close().await;
        }
        drop(store);
        let db = Arc::new(Db::builder("catalog_db", objects).build().await.unwrap());
        assert_eq!(
            catalog(&db).await,
            if action == 2 { before } else { submitted }
        );
        let store = SecretStore::new(
            Some(db.clone()),
            Arc::new(RetainedKekProvider::new(replacement.clone(), previous)),
        );
        assert_secrets(&store, &expected).await;
        assert_eq!(
            store.rotate_kek(replacement.clone(), "test").await.unwrap(),
            2
        );
        assert_secrets(&store, &expected).await;
        drop(store);
        db.close().await.unwrap();
        let db = open_db(&dir).await;
        let store = SecretStore::new(Some(db.clone()), replacement);
        assert_secrets(&store, &expected).await;
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn retained_keys_recover_a_legacy_partial_rotation() {
    let dir = TempDir::new().unwrap();
    let db = open_db(&dir).await;
    let previous = Arc::new(EnvKekProvider::new([1; 32]));
    let replacement = Arc::new(EnvKekProvider::new([2; 32]));
    let store = SecretStore::new(Some(db.clone()), previous.clone());
    let expected = seed(&store).await;
    // Build the on-disk state produced by older releases after their first replacement.
    let (key, bytes) = catalog(&db).await.into_iter().next().unwrap();
    let encrypted = serde_json::from_slice(&bytes).unwrap();
    let rotated = rockstream_control::kek::rotate_secret_kek(
        &encrypted,
        previous.as_ref(),
        replacement.as_ref(),
    )
    .await
    .unwrap();
    db.put(key, serde_json::to_vec(&rotated).unwrap())
        .await
        .unwrap();
    drop(store);
    db.close().await.unwrap();
    let db = open_db(&dir).await;
    let store = SecretStore::new(
        Some(db.clone()),
        Arc::new(RetainedKekProvider::new(replacement.clone(), previous)),
    );
    assert_secrets(&store, &expected).await;
    assert_eq!(
        store.rotate_kek(replacement.clone(), "test").await.unwrap(),
        2
    );
    drop(store);
    db.close().await.unwrap();
    let db = open_db(&dir).await;
    let store = SecretStore::new(Some(db.clone()), replacement);
    assert_secrets(&store, &expected).await;
    db.close().await.unwrap();
}

#[tokio::test]
async fn empty_and_successful_rotation_preserve_all_records() {
    for durable in [false, true] {
        let dir = TempDir::new().unwrap();
        let db = if durable {
            Some(open_db(&dir).await)
        } else {
            None
        };
        let store = SecretStore::new(db.clone(), Arc::new(EnvKekProvider::new([1; 32])));
        assert_eq!(
            store
                .rotate_kek(Arc::new(EnvKekProvider::new([2; 32])), "test")
                .await
                .unwrap(),
            0
        );
        let expected = seed(&store).await;
        assert_eq!(
            store
                .rotate_kek(Arc::new(EnvKekProvider::new([3; 32])), "test")
                .await
                .unwrap(),
            2
        );
        assert_secrets(&store, &expected).await;
        if let Some(db) = db {
            drop(store);
            db.close().await.unwrap();
            let db = open_db(&dir).await;
            let store = SecretStore::new(Some(db.clone()), Arc::new(EnvKekProvider::new([3; 32])));
            assert_secrets(&store, &expected).await;
            db.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn retained_provider_wraps_with_current_and_unwraps_both_keys() {
    let previous = Arc::new(EnvKekProvider::new([1; 32]));
    let current = Arc::new(EnvKekProvider::new([2; 32]));
    let provider = RetainedKekProvider::new(current.clone(), previous.clone());
    let dek = [9; 32];
    let wrapped = provider.wrap_dek(&dek).await.unwrap();
    assert_eq!(current.unwrap_dek(&wrapped).await.unwrap(), dek);
    assert_eq!(provider.unwrap_dek(&wrapped).await.unwrap(), dek);
    let wrapped = previous.wrap_dek(&dek).await.unwrap();
    assert_eq!(provider.unwrap_dek(&wrapped).await.unwrap(), dek);
    let unrelated = EnvKekProvider::new([3; 32]).wrap_dek(&dek).await.unwrap();
    assert_eq!(
        provider
            .unwrap_dek(&unrelated)
            .await
            .unwrap_err()
            .to_string(),
        "[RS-2422] decryption failed: AEAD open failed: ring::error::Unspecified"
    );
}

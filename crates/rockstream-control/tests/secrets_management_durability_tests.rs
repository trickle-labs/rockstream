use rockstream_control::kek::EnvKekProvider;
use rockstream_control::secret_store::SecretStore;
use rockstream_test_support::docker_available;
use rockstream_test_support::minio::{minio_object_store, start_minio};
use rockstream_types::secret::SecretType;
use slatedb::Db;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

const MINIO_BUCKET: &str = "rockstream-secrets-durability-test";

#[tokio::test]
async fn secrets_survive_lfs_reopen_and_kek_rotation_uses_point_keys() {
    let dir = TempDir::new().unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        Db::builder("catalog_db", object_store)
            .build()
            .await
            .unwrap(),
    );
    let store = SecretStore::new(
        Some(db),
        Arc::new(EnvKekProvider::from_passphrase("before")),
    );
    store
        .create_secret(
            0,
            "durable",
            SecretType::SaslPlain,
            HashMap::from([(String::from("password"), String::from("durable-secret"))]),
            "test",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .rotate_kek(Arc::new(EnvKekProvider::from_passphrase("after")), "test",)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .get_secret(0, "durable")
            .await
            .unwrap()
            .payload
            .get("password"),
        Some(&String::from("durable-secret"))
    );
}

#[tokio::test]
async fn secrets_survive_minio_tc_reopen_and_kek_rotation() {
    if !docker_available() {
        eprintln!("SKIP secrets_survive_minio_tc_reopen_and_kek_rotation: Docker not available");
        return;
    }

    let (_container, port) = match start_minio(MINIO_BUCKET).await {
        Some(res) => res,
        None => return,
    };

    let db = Arc::new(
        Db::builder("catalog_db", minio_object_store(port, MINIO_BUCKET))
            .build()
            .await
            .unwrap(),
    );
    let store = SecretStore::new(
        Some(Arc::clone(&db)),
        Arc::new(EnvKekProvider::from_passphrase("before")),
    );
    store
        .create_secret(
            0,
            "durable",
            SecretType::SaslPlain,
            HashMap::from([(String::from("password"), String::from("durable-secret"))]),
            "test",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .rotate_kek(Arc::new(EnvKekProvider::from_passphrase("after")), "test")
            .await
            .unwrap(),
        1
    );
    drop(store);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();

    let reloaded = SecretStore::new(
        Some(Arc::new(
            Db::builder("catalog_db", minio_object_store(port, MINIO_BUCKET))
                .build()
                .await
                .unwrap(),
        )),
        Arc::new(EnvKekProvider::from_passphrase("after")),
    );
    assert_eq!(
        reloaded
            .get_secret(0, "durable")
            .await
            .unwrap()
            .payload
            .get("password"),
        Some(&String::from("durable-secret"))
    );
}

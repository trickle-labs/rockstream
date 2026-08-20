use std::sync::Arc;

use object_store::local::LocalFileSystem;
use rockstream_cli::{output::OutputFormat, run_format_migrate};
use rockstream_storage::keys::{ShardKeyEncoder, ShardPrefix};
use rockstream_storage::ShardDb;
use tempfile::TempDir;

#[tokio::test(flavor = "current_thread")]
async fn root_migrate_dispatches_and_reports_exact_results() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = ShardDb::builder("shards/1/db", store.clone())
        .build()
        .await
        .unwrap();
    let key = ShardKeyEncoder::encode(ShardPrefix::OpState, 7, b"a");
    db.put(&key, b"value").await.unwrap();
    db.flush().await.unwrap();
    db.close().await.unwrap();

    let output =
        run_format_migrate(OutputFormat::Text, 1, 2, dir.path().to_str().unwrap()).unwrap();
    assert_eq!(
        output,
        "format migration 1 -> 2\nshards/1/db: objects_migrated=1 already_complete=false"
    );

    let reopened = ShardDb::builder("shards/1/db", store)
        .build()
        .await
        .unwrap();
    assert_eq!(reopened.format_version(), 2);
    assert_eq!(
        reopened.get(&key).await.unwrap().unwrap(),
        b"value".as_slice()
    );
}

#[test]
fn root_migrate_rejects_unsupported_transition_with_rs0002() {
    let error = run_format_migrate(OutputFormat::Text, 3, 4, "/tmp/does-not-matter").unwrap_err();
    assert_eq!(error.code.to_string(), "RS-0002");
    assert!(error.message.contains("storage format migration 3→4 is not supported"));
}

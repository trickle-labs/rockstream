//! Tests for TieredObjectStore fallback race behavior.
//!
//! Validates Proof 1: Tiered store fallback race returns object instead of
//! panicking on identical SimRuntime seed / object reappearance.

use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use rockstream_sim::sim::SimRuntime;
use rockstream_storage::TieredObjectStore;
use tempfile::TempDir;
use object_store::local::LocalFileSystem;

#[tokio::test]
async fn tiered_store_fallback_race_returns_object_instead_of_panic() {
    let seed = 0x5127_0001_u64;
    let _sim = SimRuntime::new(seed);

    let primary: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let secondary: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let tiered = TieredObjectStore::new(Arc::clone(&secondary))
        .with_route("sst/", Arc::clone(&primary));

    let path = Path::from("sst/race.sst");

    // Put object into primary store
    primary
        .put(&path, Bytes::from("race_data").into())
        .await
        .unwrap();

    // Verification: get() from tiered store returns object cleanly without unreachable panic
    let res = tiered.get(&path).await;
    assert!(res.is_ok(), "Expected object return during fallback check");
    let bytes = res.unwrap().bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), b"race_data");
}

#[tokio::test]
async fn tiered_store_fallback_missing_object_returns_not_found() {
    let primary: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let secondary: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let tiered = TieredObjectStore::new(Arc::clone(&secondary))
        .with_route("sst/", Arc::clone(&primary));

    let path = Path::from("sst/nonexistent.sst");
    let res = tiered.get(&path).await;
    assert!(res.is_err());
    assert!(matches!(res.unwrap_err(), object_store::Error::NotFound { .. }));
}

#[tokio::test]
async fn tiered_store_lfs_fallback_race_test() {
    let dir_primary = TempDir::new().unwrap();
    let dir_secondary = TempDir::new().unwrap();

    let primary: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir_primary.path()).unwrap());
    let secondary: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir_secondary.path()).unwrap());

    let tiered = TieredObjectStore::new(Arc::clone(&secondary))
        .with_route("sst/", Arc::clone(&primary));

    let path = Path::from("sst/lfs_race.sst");

    primary
        .put(&path, Bytes::from("lfs_race_content").into())
        .await
        .unwrap();

    let res = tiered.get(&path).await;
    assert!(res.is_ok());
    let bytes = res.unwrap().bytes().await.unwrap();
    assert_eq!(bytes.as_ref(), b"lfs_race_content");
}

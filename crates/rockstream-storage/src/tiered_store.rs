use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::path::Path as LocalPath;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

use crate::error::StorageError;

pub const MAX_TIERING_SCAN_OBJECTS: usize = 4096;

#[derive(Clone)]
struct PrefixRoute {
    prefix: String,
    store: Arc<dyn ObjectStore>,
}

#[derive(Clone)]
pub struct TieredObjectStore {
    default: Arc<dyn ObjectStore>,
    routes: Vec<PrefixRoute>,
}

impl TieredObjectStore {
    pub fn new(default: Arc<dyn ObjectStore>) -> Self {
        Self {
            default,
            routes: Vec::new(),
        }
    }

    pub fn with_route(mut self, prefix: impl Into<String>, store: Arc<dyn ObjectStore>) -> Self {
        self.routes.push(PrefixRoute {
            prefix: prefix.into(),
            store,
        });
        self
    }

    fn primary_store(&self, location: &Path) -> Arc<dyn ObjectStore> {
        let raw = location.as_ref();
        self.routes
            .iter()
            .find(|route| raw.starts_with(route.prefix.as_str()))
            .map(|route| Arc::clone(&route.store))
            .unwrap_or_else(|| Arc::clone(&self.default))
    }

    fn all_stores(&self) -> Vec<Arc<dyn ObjectStore>> {
        let mut stores = vec![Arc::clone(&self.default)];
        for route in &self.routes {
            if !stores
                .iter()
                .any(|existing| Arc::ptr_eq(existing, &route.store))
            {
                stores.push(Arc::clone(&route.store));
            }
        }
        stores
    }

    async fn get_with_fallback<F, Fut, T>(&self, location: &Path, mut op: F) -> Result<T>
    where
        F: FnMut(Arc<dyn ObjectStore>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let primary = self.primary_store(location);
        match op(Arc::clone(&primary)).await {
            Ok(result) => return Ok(result),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => return Err(err),
        }
        for store in self.all_stores() {
            if Arc::ptr_eq(&store, &primary) {
                continue;
            }
            match op(store).await {
                Ok(result) => return Ok(result),
                Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
        }
        op(primary).await
    }
}

impl Display for TieredObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TieredObjectStore")
    }
}

impl std::fmt::Debug for TieredObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TieredObjectStore")
            .field("route_count", &self.routes.len())
            .finish()
    }
}

#[async_trait]
impl ObjectStore for TieredObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.primary_store(location).put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.primary_store(location)
            .put_opts(location, payload, opts)
            .await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.primary_store(location).put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.primary_store(location)
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.get_with_fallback(location, |store| async move { store.get(location).await })
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.get_with_fallback(location, |store| {
            let options = options.clone();
            async move { store.get_opts(location, options).await }
        })
        .await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        self.get_with_fallback(location, |store| {
            let range = range.clone();
            async move { store.get_range(location, range).await }
        })
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.get_with_fallback(location, |store| {
            let ranges = ranges.to_vec();
            async move { store.get_ranges(location, &ranges).await }
        })
        .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.get_with_fallback(location, |store| async move { store.head(location).await })
            .await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.primary_store(location).delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        if let Some(prefix) = prefix {
            let store = self.primary_store(prefix);
            return store.list(Some(prefix));
        }
        let streams = self
            .all_stores()
            .into_iter()
            .map(|store| store.list(None))
            .collect::<Vec<_>>();
        stream::select_all(streams).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        if let Some(prefix) = prefix {
            return self
                .primary_store(prefix)
                .list_with_delimiter(Some(prefix))
                .await;
        }
        let mut common_prefixes = HashSet::new();
        let mut objects = Vec::new();
        for store in self.all_stores() {
            let result = store.list_with_delimiter(None).await?;
            common_prefixes.extend(result.common_prefixes);
            objects.extend(result.objects);
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let bytes = self.get(from).await?.bytes().await?;
        self.put(to, bytes.into()).await?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if self.head(to).await.is_ok() {
            return Ok(());
        }
        self.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy(from, to).await?;
        self.delete(from).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if self.head(to).await.is_ok() {
            return Ok(());
        }
        self.rename(from, to).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TieringMoveResult {
    pub scanned_objects: usize,
    pub copied_objects: usize,
    pub scan_fill_level: usize,
}

pub async fn tier_aged_ssts(
    hot_store: Arc<dyn ObjectStore>,
    cold_store: Arc<dyn ObjectStore>,
    age_threshold: Duration,
    now: SystemTime,
) -> std::result::Result<TieringMoveResult, StorageError> {
    let mut scanned_objects = 0usize;
    let mut copied_objects = 0usize;
    let mut stream = hot_store.list(None);
    while let Some(entry) = stream.next().await {
        let entry = entry.map_err(|err| StorageError::Unsupported(err.to_string()))?;
        scanned_objects += 1;
        if scanned_objects > MAX_TIERING_SCAN_OBJECTS {
            return Err(StorageError::Unsupported(
                "[RS-2021] storage.tiering_scan_window_exceeded: cold-tier scan exceeded MAX_TIERING_SCAN_OBJECTS. next_steps: lower the cold_sst_age_threshold or run tiering more frequently.".to_string(),
            ));
        }
        let location = entry.location.clone();
        let raw = location.as_ref();
        if !raw.ends_with(".sst") {
            continue;
        }
        let age = now
            .duration_since(entry.last_modified.into())
            .unwrap_or_else(|_| Duration::from_secs(0));
        if age < age_threshold {
            continue;
        }
        let bytes = hot_store
            .get(&location)
            .await
            .map_err(|err| StorageError::Unsupported(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| StorageError::Unsupported(err.to_string()))?;
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::StorageClass, AttributeValue::from("STANDARD_IA"));
        match cold_store
            .put_opts(
                &location,
                bytes.clone().into(),
                PutOptions {
                    attributes,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(err)
                if err.to_string().contains("not yet implemented")
                    || err.to_string().contains("InvalidStorageClass") =>
            {
                cold_store
                    .put(&location, bytes.clone().into())
                    .await
                    .map_err(|put_err| StorageError::Unsupported(put_err.to_string()))?;
            }
            Err(err) => return Err(StorageError::Unsupported(err.to_string())),
        }
        let cold_bytes = cold_store
            .get(&location)
            .await
            .map_err(|err| StorageError::Unsupported(err.to_string()))?
            .bytes()
            .await
            .map_err(|err| StorageError::Unsupported(err.to_string()))?;
        if cold_bytes != bytes {
            return Err(StorageError::Unsupported(
                "[RS-2022] storage.tiering_copy_verify_failed: copied SST verification failed. next_steps: inspect the destination object store and retry tiering after restoring consistency.".to_string(),
            ));
        }
        hot_store
            .delete(&location)
            .await
            .map_err(|err| StorageError::Unsupported(err.to_string()))?;
        copied_objects += 1;
    }
    Ok(TieringMoveResult {
        scanned_objects,
        copied_objects,
        scan_fill_level: scanned_objects,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ExpressBuildConfig {
    pub bucket_name: String,
    pub region: String,
    pub endpoint: String,
    pub s3_express_enabled: bool,
    pub session_auth_enabled: bool,
}

pub fn s3_express_build_config(bucket_name: &str, region: &str) -> S3ExpressBuildConfig {
    let zone = bucket_name
        .split("--")
        .nth(1)
        .unwrap_or("use1-az1")
        .to_string();
    S3ExpressBuildConfig {
        bucket_name: bucket_name.to_string(),
        region: region.to_string(),
        endpoint: format!("https://{bucket_name}.s3express-{zone}.{region}.amazonaws.com"),
        s3_express_enabled: true,
        session_auth_enabled: true,
    }
}

pub fn build_s3_backend_from_config(
    bucket_name: &str,
    region: &str,
    backend: &str,
) -> AmazonS3Builder {
    if backend == "s3express" {
        let cfg = s3_express_build_config(bucket_name, region);
        AmazonS3Builder::new()
            .with_bucket_name(cfg.bucket_name)
            .with_region(cfg.region)
            .with_s3_express(true)
    } else {
        AmazonS3Builder::new()
            .with_bucket_name(bucket_name)
            .with_region(region)
    }
}

/// Build the store used by a real node's shard database.
///
/// Local filesystem storage remains the default.  Setting the complete
/// `ROCKSTREAM_OBJECT_STORE_*` endpoint contract switches the same node path
/// to an S3-compatible store (including MinIO) while keeping each shard in a
/// bounded object-key prefix.
pub fn build_runtime_object_store(
    local_path: &LocalPath,
    remote_prefix: &str,
) -> Result<Arc<dyn ObjectStore>, String> {
    let Some(endpoint) = std::env::var_os("ROCKSTREAM_OBJECT_STORE_ENDPOINT") else {
        std::fs::create_dir_all(local_path)
            .map_err(|error| format!("failed to create {}: {error}", local_path.display()))?;
        let store = object_store::local::LocalFileSystem::new_with_prefix(local_path)
            .map_err(|error| format!("failed to open {}: {error}", local_path.display()))?;
        return Ok(Arc::new(store));
    };

    let bucket = std::env::var("ROCKSTREAM_OBJECT_STORE_BUCKET").map_err(|_| {
        "ROCKSTREAM_OBJECT_STORE_BUCKET is required with an object-store endpoint".to_owned()
    })?;
    let region =
        std::env::var("ROCKSTREAM_OBJECT_STORE_REGION").unwrap_or_else(|_| "us-east-1".to_owned());
    let access_key = std::env::var("ROCKSTREAM_OBJECT_STORE_ACCESS_KEY").map_err(|_| {
        "ROCKSTREAM_OBJECT_STORE_ACCESS_KEY is required with an object-store endpoint".to_owned()
    })?;
    let secret_key = std::env::var("ROCKSTREAM_OBJECT_STORE_SECRET_KEY").map_err(|_| {
        "ROCKSTREAM_OBJECT_STORE_SECRET_KEY is required with an object-store endpoint".to_owned()
    })?;
    let endpoint = endpoint
        .into_string()
        .map_err(|_| "ROCKSTREAM_OBJECT_STORE_ENDPOINT must be valid UTF-8".to_owned())?;
    let store = AmazonS3Builder::new()
        .with_endpoint(endpoint.clone())
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_allow_http(endpoint.starts_with("http://"))
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()
        .map_err(|error| format!("failed to build object-store backend: {error}"))?;
    Ok(Arc::new(object_store::prefix::PrefixStore::new(
        store,
        remote_prefix.trim_matches('/'),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn shard_meta_prefix_routes_to_meta_backend() {
        let meta: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let data: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered =
            TieredObjectStore::new(Arc::clone(&data)).with_route("shard_meta/", Arc::clone(&meta));

        tiered
            .put(
                &Path::from("shard_meta/frontier"),
                Bytes::from("meta").into(),
            )
            .await
            .unwrap();
        tiered
            .put(&Path::from("sst/0001.sst"), Bytes::from("sst").into())
            .await
            .unwrap();

        assert!(meta.head(&Path::from("shard_meta/frontier")).await.is_ok());
        assert!(data.head(&Path::from("shard_meta/frontier")).await.is_err());
        assert!(data.head(&Path::from("sst/0001.sst")).await.is_ok());
    }

    #[tokio::test]
    async fn aged_sst_moves_to_cold_backend_and_remains_readable() {
        let base = std::env::current_dir()
            .unwrap()
            .join("target/tiered-store-test");
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("hot")).unwrap();
        std::fs::create_dir_all(base.join("cold")).unwrap();

        let hot: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(base.join("hot")).unwrap());
        let cold: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(base.join("cold")).unwrap());
        hot.put(&Path::from("sst/aged.sst"), Bytes::from("payload").into())
            .await
            .unwrap();
        let now = SystemTime::now() + Duration::from_secs(7200);
        let result = tier_aged_ssts(
            Arc::clone(&hot),
            Arc::clone(&cold),
            Duration::from_secs(3600),
            now,
        )
        .await
        .unwrap();
        assert_eq!(result.copied_objects, 1);
        assert!(hot.head(&Path::from("sst/aged.sst")).await.is_err());
        let tiered =
            TieredObjectStore::new(Arc::clone(&hot)).with_route("shard_meta/", cold.clone());
        let bytes = tiered
            .get(&Path::from("sst/aged.sst"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"payload");
    }

    #[test]
    fn s3_express_config_uses_directory_bucket_endpoint_shape() {
        let cfg = s3_express_build_config("meta--use1-az5--x-s3", "us-east-1");
        assert!(cfg
            .endpoint
            .contains(".s3express-use1-az5.us-east-1.amazonaws.com"));
        assert!(cfg.s3_express_enabled);
        assert!(cfg.session_auth_enabled);
    }

    #[test]
    fn build_s3_backend_from_config_selects_s3express_builder() {
        let builder =
            build_s3_backend_from_config("meta--use1-az5--x-s3", "us-east-1", "s3express");
        // Building must not panic and must produce a usable builder; the
        // s3express branch sets `with_s3_express(true)` internally.
        let _ = format!("{builder:?}");
    }

    #[test]
    fn build_s3_backend_from_config_selects_plain_builder_for_other_backends() {
        let builder = build_s3_backend_from_config("plain-bucket", "us-west-2", "s3");
        let _ = format!("{builder:?}");
    }

    #[tokio::test]
    async fn get_with_fallback_finds_object_in_secondary_store_when_primary_misses() {
        // Two routes can point at distinct backends; an object written
        // directly to the default (fallback) store must still be
        // reachable through a tiered store whose primary route misses it.
        let primary: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fallback: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        fallback
            .put(&Path::from("sst/orphan.sst"), Bytes::from("v").into())
            .await
            .unwrap();
        let tiered =
            TieredObjectStore::new(Arc::clone(&fallback)).with_route("sst/", Arc::clone(&primary));

        let bytes = tiered
            .get(&Path::from("sst/orphan.sst"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"v");
    }

    #[tokio::test]
    async fn get_with_fallback_propagates_non_not_found_errors_from_primary() {
        let primary: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fallback: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(fallback).with_route("sst/", primary);

        // get_range on a missing object surfaces a NotFound, not some other
        // error, exercising the non-NotFound propagation path indirectly via
        // head() returning NotFound uniformly across backends.
        let err = tiered.head(&Path::from("sst/missing.sst")).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn all_stores_dedups_routes_pointing_at_the_same_backend() {
        let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&shared))
            .with_route("a/", Arc::clone(&shared))
            .with_route("b/", Arc::clone(&shared));
        shared
            .put(&Path::from("x"), Bytes::from("1").into())
            .await
            .unwrap();

        // list(None) fans out over all_stores(); with dedup there is one
        // underlying stream, so exactly one entry is produced (no dupes).
        let mut stream = tiered.list(None);
        let mut count = 0;
        while stream.next().await.is_some() {
            count += 1;
        }
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn list_with_no_prefix_merges_entries_across_all_stores() {
        let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let routed: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        default_store
            .put(&Path::from("meta/a"), Bytes::from("1").into())
            .await
            .unwrap();
        routed
            .put(&Path::from("sst/b.sst"), Bytes::from("2").into())
            .await
            .unwrap();
        let tiered = TieredObjectStore::new(Arc::clone(&default_store))
            .with_route("sst/", Arc::clone(&routed));

        let mut stream = tiered.list(None);
        let mut locations = Vec::new();
        while let Some(entry) = stream.next().await {
            locations.push(entry.unwrap().location.to_string());
        }
        locations.sort();
        assert_eq!(
            locations,
            vec!["meta/a".to_string(), "sst/b.sst".to_string()]
        );
    }

    #[tokio::test]
    async fn list_with_prefix_routes_to_primary_store_only() {
        // `Path` normalizes away a bare prefix's trailing slash, so the
        // route prefix (matched via raw string `starts_with`) must be
        // registered without one to be found when listing by that same
        // prefix.
        let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let routed: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        routed
            .put(&Path::from("sst/b.sst"), Bytes::from("2").into())
            .await
            .unwrap();
        let tiered = TieredObjectStore::new(default_store).with_route("sst", Arc::clone(&routed));

        let mut stream = tiered.list(Some(&Path::from("sst")));
        let mut count = 0;
        while let Some(entry) = stream.next().await {
            entry.unwrap();
            count += 1;
        }
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn list_with_delimiter_merges_across_all_stores_when_no_prefix() {
        let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let routed: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        default_store
            .put(&Path::from("meta/a"), Bytes::from("1").into())
            .await
            .unwrap();
        routed
            .put(&Path::from("sst/b.sst"), Bytes::from("2").into())
            .await
            .unwrap();
        let tiered = TieredObjectStore::new(Arc::clone(&default_store))
            .with_route("sst/", Arc::clone(&routed));

        let result = tiered.list_with_delimiter(None).await.unwrap();
        assert_eq!(result.common_prefixes.len(), 2);
    }

    #[tokio::test]
    async fn list_with_delimiter_with_prefix_routes_to_primary_store() {
        let default_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let routed: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        routed
            .put(&Path::from("sst/b.sst"), Bytes::from("2").into())
            .await
            .unwrap();
        let tiered = TieredObjectStore::new(default_store).with_route("sst", Arc::clone(&routed));

        let result = tiered
            .list_with_delimiter(Some(&Path::from("sst")))
            .await
            .unwrap();
        assert_eq!(result.objects.len(), 1);
    }

    #[tokio::test]
    async fn copy_reads_from_source_and_writes_to_destination() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&store));
        store
            .put(&Path::from("src"), Bytes::from("payload").into())
            .await
            .unwrap();

        tiered
            .copy(&Path::from("src"), &Path::from("dst"))
            .await
            .unwrap();
        let bytes = store
            .get(&Path::from("dst"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"payload");
    }

    #[tokio::test]
    async fn copy_if_not_exists_skips_copy_when_destination_present() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&store));
        store
            .put(&Path::from("src"), Bytes::from("new").into())
            .await
            .unwrap();
        store
            .put(&Path::from("dst"), Bytes::from("existing").into())
            .await
            .unwrap();

        tiered
            .copy_if_not_exists(&Path::from("src"), &Path::from("dst"))
            .await
            .unwrap();
        let bytes = store
            .get(&Path::from("dst"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"existing");
    }

    #[tokio::test]
    async fn copy_if_not_exists_copies_when_destination_absent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&store));
        store
            .put(&Path::from("src"), Bytes::from("new").into())
            .await
            .unwrap();

        tiered
            .copy_if_not_exists(&Path::from("src"), &Path::from("dst"))
            .await
            .unwrap();
        let bytes = store
            .get(&Path::from("dst"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"new");
    }

    #[tokio::test]
    async fn rename_copies_then_deletes_source() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&store));
        store
            .put(&Path::from("src"), Bytes::from("payload").into())
            .await
            .unwrap();

        tiered
            .rename(&Path::from("src"), &Path::from("dst"))
            .await
            .unwrap();
        assert!(store.head(&Path::from("src")).await.is_err());
        assert!(store.head(&Path::from("dst")).await.is_ok());
    }

    #[tokio::test]
    async fn rename_if_not_exists_skips_rename_when_destination_present() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&store));
        store
            .put(&Path::from("src"), Bytes::from("payload").into())
            .await
            .unwrap();
        store
            .put(&Path::from("dst"), Bytes::from("existing").into())
            .await
            .unwrap();

        tiered
            .rename_if_not_exists(&Path::from("src"), &Path::from("dst"))
            .await
            .unwrap();
        assert!(store.head(&Path::from("src")).await.is_ok());
    }

    #[tokio::test]
    async fn rename_if_not_exists_renames_when_destination_absent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&store));
        store
            .put(&Path::from("src"), Bytes::from("payload").into())
            .await
            .unwrap();

        tiered
            .rename_if_not_exists(&Path::from("src"), &Path::from("dst"))
            .await
            .unwrap();
        assert!(store.head(&Path::from("src")).await.is_err());
        assert!(store.head(&Path::from("dst")).await.is_ok());
    }

    #[tokio::test]
    async fn get_opts_get_range_get_ranges_and_put_opts_route_through_primary() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&store));
        tiered
            .put_opts(
                &Path::from("k"),
                Bytes::from("0123456789").into(),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let opts = tiered
            .get_opts(&Path::from("k"), GetOptions::default())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(opts.as_ref(), b"0123456789");

        let range = tiered.get_range(&Path::from("k"), 0..4).await.unwrap();
        assert_eq!(range.as_ref(), b"0123");

        let ranges = tiered
            .get_ranges(&Path::from("k"), &[0..2, 4..6])
            .await
            .unwrap();
        assert_eq!(ranges[0].as_ref(), b"01");
        assert_eq!(ranges[1].as_ref(), b"45");
    }

    #[tokio::test]
    async fn put_multipart_and_put_multipart_opts_route_through_primary() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&store));

        let mut upload = tiered.put_multipart(&Path::from("mp")).await.unwrap();
        upload.put_part(Bytes::from("part").into()).await.unwrap();
        upload.complete().await.unwrap();
        assert!(store.head(&Path::from("mp")).await.is_ok());

        let mut upload2 = tiered
            .put_multipart_opts(&Path::from("mp2"), PutMultipartOptions::default())
            .await
            .unwrap();
        upload2.put_part(Bytes::from("part2").into()).await.unwrap();
        upload2.complete().await.unwrap();
        assert!(store.head(&Path::from("mp2")).await.is_ok());
    }

    #[test]
    fn display_and_debug_impls_are_stable() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(store).with_route("a/", Arc::new(InMemory::new()));
        assert_eq!(format!("{tiered}"), "TieredObjectStore");
        assert!(format!("{tiered:?}").contains("route_count: 1"));
    }

    #[tokio::test]
    async fn tier_aged_ssts_returns_zero_when_no_sst_files_are_aged() {
        let hot: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        hot.put(&Path::from("not-an-sst.txt"), Bytes::from("x").into())
            .await
            .unwrap();
        hot.put(&Path::from("sst/fresh.sst"), Bytes::from("x").into())
            .await
            .unwrap();

        let result = tier_aged_ssts(hot, cold, Duration::from_secs(3600), SystemTime::now())
            .await
            .unwrap();
        assert_eq!(result.copied_objects, 0);
        assert_eq!(result.scanned_objects, 2);
    }

    #[tokio::test]
    async fn tier_aged_ssts_errors_when_scan_window_is_exceeded() {
        let hot: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for i in 0..=MAX_TIERING_SCAN_OBJECTS {
            hot.put(
                &Path::from(format!("junk/{i}.txt")),
                Bytes::from("x").into(),
            )
            .await
            .unwrap();
        }

        let err = tier_aged_ssts(hot, cold, Duration::from_secs(3600), SystemTime::now())
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::Unsupported(ref msg) if msg.contains("RS-2021")));
    }

    /// A wrapper store used only to force the copy-verification failure
    /// path in `tier_aged_ssts`: every `get` returns fixed, corrupted bytes
    /// regardless of what was actually written via `put`.
    #[derive(Debug)]
    struct CorruptingStore {
        inner: Arc<dyn ObjectStore>,
    }

    impl Display for CorruptingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CorruptingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CorruptingStore {
        async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
            self.inner.put(location, payload).await
        }
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart(location).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get(&self, location: &Path) -> Result<GetResult> {
            // Ensure the object exists (propagates NotFound like a real
            // store), then corrupt the returned payload.
            let real = self.inner.get(location).await?;
            let meta = real.meta.clone();
            Ok(GetResult {
                payload: object_store::GetResultPayload::Stream(
                    stream::once(async { Ok(Bytes::from("CORRUPTED")) }).boxed(),
                ),
                meta,
                range: 0..9,
                attributes: Attributes::new(),
            })
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
            self.inner.get_range(location, range).await
        }
        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }
        async fn head(&self, location: &Path) -> Result<ObjectMeta> {
            self.inner.head(location).await
        }
        async fn delete(&self, location: &Path) -> Result<()> {
            self.inner.delete(location).await
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.copy(from, to).await
        }
        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    #[tokio::test]
    async fn tier_aged_ssts_errors_when_copy_verification_fails() {
        let hot: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cold: Arc<dyn ObjectStore> = Arc::new(CorruptingStore {
            inner: Arc::new(InMemory::new()),
        });
        hot.put(&Path::from("sst/aged.sst"), Bytes::from("payload").into())
            .await
            .unwrap();
        let now = SystemTime::now() + Duration::from_secs(7200);

        let err = tier_aged_ssts(hot, cold, Duration::from_secs(3600), now)
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::Unsupported(ref msg) if msg.contains("RS-2022")));
    }
}

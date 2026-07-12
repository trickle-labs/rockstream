use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::ops::Range;
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
        primary.get(location).await.map(|_| unreachable!())
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
}

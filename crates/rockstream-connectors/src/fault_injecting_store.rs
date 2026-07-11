//! Object-store decorator that injects deterministic partial-write faults.
//!
//! This wrapper preserves the underlying store behavior for all operations except
//! `put`/`put_opts`, where it can truncate the uploaded payload when the
//! `object_store.partial_write` fault is armed.

use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rockstream_sim::buggify;

#[derive(Debug)]
pub struct FaultInjectingObjectStore {
    inner: Arc<dyn ObjectStore>,
    partial_write_probability: f64,
    deterministic_fault_rng: Mutex<Option<SmallRng>>,
}

impl FaultInjectingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            partial_write_probability: 0.0,
            deterministic_fault_rng: Mutex::new(None),
        }
    }

    pub fn inner(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.inner)
    }

    pub fn set_partial_write_probability(&mut self, probability: f64) {
        self.partial_write_probability = probability.clamp(0.0, 1.0);
    }

    pub fn set_deterministic_fault_seed(&self, seed: u64) {
        *self.deterministic_fault_rng.lock().unwrap() = Some(SmallRng::seed_from_u64(seed));
    }

    fn maybe_truncate_payload(&self, payload: PutPayload) -> PutPayload {
        if !self.should_truncate_payload() {
            return payload;
        }
        let bytes: Bytes = payload.into();
        let truncated_len = bytes.len() / 2;
        bytes.slice(0..truncated_len).into()
    }

    fn should_truncate_payload(&self) -> bool {
        if self.partial_write_probability <= 0.0 {
            return false;
        }

        if let Some(rng) = self.deterministic_fault_rng.lock().unwrap().as_mut() {
            return rng.gen_bool(self.partial_write_probability);
        }

        buggify!("object_store.partial_write", self.partial_write_probability)
    }
}

impl Display for FaultInjectingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultInjectingObjectStore")
    }
}

#[async_trait]
impl ObjectStore for FaultInjectingObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.inner
            .put(location, self.maybe_truncate_payload(payload))
            .await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner
            .put_opts(location, self.maybe_truncate_payload(payload), opts)
            .await
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
        self.inner.get(location).await
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

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    const PARTIAL_WRITE_SEED: u64 = 0;

    #[tokio::test]
    async fn put_is_byte_identical_when_probability_zero() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut store = FaultInjectingObjectStore::new(inner);
        store.set_partial_write_probability(0.0);
        store.set_deterministic_fault_seed(PARTIAL_WRITE_SEED);

        let path = Path::from("zero-probability.bin");
        let payload = b"abcdefghi".to_vec();
        store.put(&path, payload.clone().into()).await.unwrap();

        let observed = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(observed.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn put_truncates_when_seeded_fault_fires() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut store = FaultInjectingObjectStore::new(inner);
        store.set_partial_write_probability(0.5);
        store.set_deterministic_fault_seed(PARTIAL_WRITE_SEED);

        let path = Path::from("truncated.bin");
        let payload = b"abcdefgh".to_vec();
        store.put(&path, payload.clone().into()).await.unwrap();

        let observed = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(observed.as_ref(), &payload[..payload.len() / 2]);
    }

    #[tokio::test]
    async fn lfs_put_is_byte_identical_when_probability_zero() {
        let dir = TempDir::new().unwrap();
        let inner: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let mut store = FaultInjectingObjectStore::new(inner);
        store.set_partial_write_probability(0.0);
        store.set_deterministic_fault_seed(PARTIAL_WRITE_SEED);

        let path = Path::from("lfs.bin");
        let payload = b"lfs-payload".to_vec();
        store.put(&path, payload.clone().into()).await.unwrap();

        let observed = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(observed.as_ref(), payload.as_slice());
    }
}

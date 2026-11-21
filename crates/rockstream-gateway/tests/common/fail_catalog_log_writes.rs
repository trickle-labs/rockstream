use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutOptions,
    PutPayload, PutResult,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Debug)]
pub(crate) struct FailCatalogLogWrites {
    pub(crate) inner: InMemory,
    pub(crate) fail: AtomicBool,
    pub(crate) failed_appends: AtomicUsize,
}

impl std::fmt::Display for FailCatalogLogWrites {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FailCatalogLogWrites")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailCatalogLogWrites {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        bytes: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail.load(Ordering::SeqCst) && location.as_ref().contains("catalog/log/") {
            self.failed_appends.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "FailCatalogLogWrites",
                source: "injected catalog log append failure".into(),
            });
        }
        self.inner.put_opts(location, bytes, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<GetResult> {
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
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
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
}

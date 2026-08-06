#![no_main]
use libfuzzer_sys::fuzz_target;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs, view_reader::ViewReadStrategy, GatewayError, GatewayServer,
    ViewReader,
};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

struct FuzzViewReader;

#[async_trait::async_trait]
impl ViewReader for FuzzViewReader {
    async fn read_view(
        &self,
        _view_name: &str,
        _limit: Option<usize>,
        _strategy: ViewReadStrategy,
    ) -> Result<Vec<Vec<u8>>, GatewayError> {
        Ok(vec![])
    }

    fn published_frontier(&self) -> Option<u64> {
        None
    }
}

fuzz_target!(|data: &[u8]| {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();

    if let Ok(rt) = rt {
        rt.block_on(async {
            let catalog = Arc::new(CatalogStubs::new());
            let view_reader = Arc::new(FuzzViewReader);
            let server =
                GatewayServer::with_catalog("127.0.0.1:0".parse().unwrap(), catalog, view_reader);

            if let Ok((addr, _handle)) = server.serve_background().await {
                if let Ok(mut socket) = TcpStream::connect(addr).await {
                    let _ = socket.write_all(data).await;
                    let _ = socket.shutdown().await;
                }
            }
        });
    }
});

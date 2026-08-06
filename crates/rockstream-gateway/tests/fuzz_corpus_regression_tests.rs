//! Permanent regression suite replaying all checked-in fuzz corpus inputs.
//!
//! Replays all inputs in `fuzz/corpus/` against their respective decoder boundaries
//! to assert zero panics across code updates.

use arrow::datatypes::Schema;
use rockstream_connectors::postgres_cdc::{CdcWireFormat, PostgresCdcSource};
use rockstream_gateway::{
    catalog_stubs::CatalogStubs, view_reader::ViewReadStrategy, GatewayError, GatewayServer,
    HttpWebhookSource, ViewReader, WebhookFormat,
};
use rockstream_sql::frontend::SqlFrontend;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

struct TestViewReader;

#[async_trait::async_trait]
impl ViewReader for TestViewReader {
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

fn get_corpus_dir(target_name: &str) -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let root = Path::new(&manifest_dir).parent().unwrap().parent().unwrap();
    root.join("fuzz").join("corpus").join(target_name)
}

fn read_corpus_files(target_name: &str) -> Vec<(PathBuf, Vec<u8>)> {
    let dir = get_corpus_dir(target_name);
    if !dir.exists() {
        return vec![];
    }
    let mut files = vec![];
    if let Ok(entries) = fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Ok(data) = fs::read(&path) {
                    files.push((path, data));
                }
            }
        }
    }
    files
}

async fn run_fuzz_sql_parser_corpus() {
    let corpus = read_corpus_files("fuzz_sql_parser");
    let frontend = SqlFrontend::new();
    for (_path, data) in corpus {
        if let Ok(sql) = std::str::from_utf8(&data) {
            let _ = frontend.parse_ddl(sql);
            let _ = frontend.sql_to_plan_node(sql).await;
        }
    }
}

async fn run_fuzz_pgwire_decoder_corpus() {
    let corpus = read_corpus_files("fuzz_pgwire_decoder");
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(TestViewReader);
    let server = GatewayServer::with_catalog("127.0.0.1:0".parse().unwrap(), catalog, view_reader);
    let (addr, _handle) = server.serve_background().await.unwrap();

    for (_path, data) in corpus {
        if let Ok(mut socket) = tokio::net::TcpStream::connect(addr).await {
            let _ = socket.write_all(&data).await;
            let _ = socket.shutdown().await;
        }
    }
}

fn run_fuzz_postgres_cdc_corpus() {
    let corpus = read_corpus_files("fuzz_postgres_cdc");
    let schema = Arc::new(Schema::empty());
    for (_path, data) in corpus {
        let mut pg_source = PostgresCdcSource::new(
            rockstream_types::ids::ConnectorId(99),
            schema.clone(),
            CdcWireFormat::PgOutput,
        );
        let _ = pg_source.decode_and_enqueue(&data);

        let mut wal_source = PostgresCdcSource::new(
            rockstream_types::ids::ConnectorId(100),
            schema.clone(),
            CdcWireFormat::Wal2Json,
        );
        let _ = wal_source.decode_and_enqueue(&data);
    }
}

fn run_fuzz_webhook_body_corpus() {
    let corpus = read_corpus_files("fuzz_webhook_body");
    for (_path, data) in corpus {
        let mut json_source = HttpWebhookSource::new("secret", WebhookFormat::Json);
        let _ = json_source.accept(b"secret", Some("delivery-1"), &data);

        let mut csv_source = HttpWebhookSource::new("secret", WebhookFormat::Csv);
        let _ = csv_source.accept(b"secret", Some("delivery-2"), &data);
    }
}

#[tokio::test]
async fn replay_fuzz_sql_parser_corpus() {
    run_fuzz_sql_parser_corpus().await;
}

#[tokio::test]
async fn replay_fuzz_pgwire_decoder_corpus() {
    run_fuzz_pgwire_decoder_corpus().await;
}

#[test]
fn replay_fuzz_postgres_cdc_corpus() {
    run_fuzz_postgres_cdc_corpus();
}

#[test]
fn replay_fuzz_webhook_body_corpus() {
    run_fuzz_webhook_body_corpus();
}

#[tokio::test]
async fn replay_all_fuzz_corpora() {
    run_fuzz_sql_parser_corpus().await;
    run_fuzz_pgwire_decoder_corpus().await;
    run_fuzz_postgres_cdc_corpus();
    run_fuzz_webhook_body_corpus();
}

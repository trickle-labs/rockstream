//! Permanent regression suite replaying all checked-in fuzz corpus inputs.
//!
//! Replays all inputs in `fuzz/corpus/` against their respective decoder boundaries
//! to assert zero panics across code updates.

use arrow::datatypes::Schema;
use rockstream_connectors::kafka_source::decode_kafka_payload;
use rockstream_connectors::postgres_cdc::{CdcWireFormat, PostgresCdcSource};
use rockstream_gateway::auth::JwtVerifier;
use rockstream_gateway::{
    catalog_stubs::CatalogStubs, view_reader::ViewReadStrategy, GatewayError, GatewayServer,
    ViewReader,
};
use rockstream_sql::frontend::SqlFrontend;
use rockstream_types::raft::RaftRpcRequest;
use rockstream_types::topology::WorkerMessage;
use std::collections::BTreeSet;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct TestViewReader;

const MAX_CORPUS_FILES_PER_TARGET: usize = 1024;
const MAX_CORPUS_FILE_BYTES: u64 = 1024 * 1024;
const MAX_CORPUS_BYTES_PER_TARGET: u64 = 16 * 1024 * 1024;
const FUZZ_TARGETS: [&str; 7] = [
    "fuzz_sql_parser",
    "fuzz_pgwire_decoder",
    "fuzz_postgres_cdc",
    "fuzz_control_worker_decoder",
    "fuzz_raft_rpc_decoder",
    "fuzz_kafka_payload_decoder",
    "fuzz_oidc_jwt_decoder",
];

#[derive(Default)]
struct CorpusStats {
    files: usize,
    bytes: u64,
}

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

fn assert_corpus_directories() {
    let corpus_root = get_corpus_dir("");
    let manifest_targets = fs::read_to_string(corpus_root.parent().unwrap().join("Cargo.toml"))
        .expect("fuzz manifest must be readable")
        .lines()
        .filter_map(|line| {
            if !line.trim_start().starts_with("name =") {
                return None;
            }
            let name = line.split('"').nth(1)?;
            name.starts_with("fuzz_").then_some(name.to_string())
        })
        .collect::<BTreeSet<_>>();
    let configured_targets = FUZZ_TARGETS
        .iter()
        .map(|target| (*target).to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        manifest_targets, configured_targets,
        "fuzz manifest targets and replay targets must agree"
    );
    let mut directory_count = 0;
    for entry in fs::read_dir(&corpus_root).expect("fuzz corpus root must be readable") {
        let entry = entry.expect("fuzz corpus directory entry must be readable");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        assert!(
            FUZZ_TARGETS.contains(&name.as_ref()),
            "unexpected fuzz corpus directory: {name}"
        );
        assert!(
            entry.path().is_dir(),
            "fuzz corpus target must be a directory: {name}"
        );
        directory_count += 1;
    }
    assert_eq!(
        directory_count,
        FUZZ_TARGETS.len(),
        "fuzz target and corpus directory counts must agree"
    );
    for target in FUZZ_TARGETS {
        assert!(
            get_corpus_dir(target).is_dir(),
            "missing fuzz corpus directory for registered target {target}"
        );
    }
}

fn replay_corpus(target_name: &str, mut decoder: impl FnMut(&[u8])) -> CorpusStats {
    let dir = get_corpus_dir(target_name);
    assert!(
        dir.is_dir(),
        "missing fuzz corpus directory for {target_name}"
    );
    let mut stats = CorpusStats::default();
    for entry in fs::read_dir(&dir).expect("fuzz corpus target must be readable") {
        let entry = entry.expect("fuzz corpus entry must be readable");
        let path = entry.path();
        assert!(
            path.is_file(),
            "fuzz corpus entry must be a file: {}",
            path.display()
        );
        stats.files += 1;
        assert!(
            stats.files <= MAX_CORPUS_FILES_PER_TARGET,
            "fuzz corpus file cap exceeded for {target_name}: max {}",
            MAX_CORPUS_FILES_PER_TARGET
        );
        let file_bytes = fs::metadata(&path)
            .expect("fuzz corpus metadata must be readable")
            .len();
        assert!(
            file_bytes <= MAX_CORPUS_FILE_BYTES,
            "fuzz corpus file cap exceeded for {}: {} bytes > {}",
            path.display(),
            file_bytes,
            MAX_CORPUS_FILE_BYTES
        );
        assert!(
            stats.bytes.saturating_add(file_bytes) <= MAX_CORPUS_BYTES_PER_TARGET,
            "fuzz corpus byte cap exceeded for {target_name}: max {}",
            MAX_CORPUS_BYTES_PER_TARGET
        );
        let mut data = Vec::new();
        let mut file = fs::File::open(&path)
            .expect("fuzz corpus file must be openable")
            .take(MAX_CORPUS_FILE_BYTES + 1);
        file.read_to_end(&mut data)
            .expect("fuzz corpus file must be readable");
        assert!(
            data.len() as u64 <= MAX_CORPUS_FILE_BYTES,
            "fuzz corpus file grew beyond the configured cap: {}",
            path.display()
        );
        stats.bytes += data.len() as u64;
        decoder(&data);
    }
    eprintln!(
        "fuzz corpus target={target_name} corpus-files={} corpus-bytes={}",
        stats.files, stats.bytes
    );
    stats
}

async fn run_fuzz_sql_parser_corpus() {
    let frontend = SqlFrontend::new();
    replay_corpus("fuzz_sql_parser", |data| {
        if let Ok(sql) = std::str::from_utf8(data) {
            let _ = frontend.parse_ddl(sql);
            let _ = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(frontend.sql_to_plan_node(sql))
            });
        }
    });
}

async fn run_fuzz_pgwire_decoder_corpus() {
    let catalog = Arc::new(CatalogStubs::new());
    let view_reader = Arc::new(TestViewReader);
    let server = GatewayServer::with_catalog("127.0.0.1:0".parse().unwrap(), catalog, view_reader);
    let (addr, _handle) = server.serve_background().await.unwrap();

    replay_corpus("fuzz_pgwire_decoder", |data| {
        if let Ok(mut socket) = std::net::TcpStream::connect(addr) {
            let _ = socket.write_all(data);
            let _ = socket.shutdown(std::net::Shutdown::Both);
        }
    });
}

async fn run_fuzz_postgres_cdc_corpus() {
    let schema = Arc::new(Schema::empty());
    replay_corpus("fuzz_postgres_cdc", |data| {
        let mut pg_source = PostgresCdcSource::new(
            rockstream_types::ids::ConnectorId(99),
            schema.clone(),
            CdcWireFormat::PgOutput,
        );
        let _ = pg_source.decode_and_enqueue(data);

        let mut wal_source = PostgresCdcSource::new(
            rockstream_types::ids::ConnectorId(100),
            schema.clone(),
            CdcWireFormat::Wal2Json,
        );
        let _ = wal_source.decode_and_enqueue(data);
    });
}

async fn run_fuzz_control_worker_decoder_corpus() {
    replay_corpus("fuzz_control_worker_decoder", |data| {
        let _ = serde_json::from_slice::<WorkerMessage>(data);
    });
}

async fn run_fuzz_raft_rpc_decoder_corpus() {
    replay_corpus("fuzz_raft_rpc_decoder", |data| {
        let _ = serde_json::from_slice::<RaftRpcRequest>(data);
    });
}

async fn run_fuzz_kafka_payload_decoder_corpus() {
    replay_corpus("fuzz_kafka_payload_decoder", |data| {
        let _ = decode_kafka_payload(data);
    });
}

async fn run_fuzz_oidc_jwt_decoder_corpus() {
    replay_corpus("fuzz_oidc_jwt_decoder", |data| {
        if let Ok(token) = std::str::from_utf8(data) {
            let verifier = JwtVerifier::with_hs256_key(b"fuzz-corpus-key".to_vec());
            let _ = verifier.verify(token);
        }
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_fuzz_sql_parser_corpus() {
    run_fuzz_sql_parser_corpus().await;
}

#[tokio::test]
async fn replay_fuzz_pgwire_decoder_corpus() {
    run_fuzz_pgwire_decoder_corpus().await;
}

#[tokio::test]
async fn replay_fuzz_postgres_cdc_corpus() {
    run_fuzz_postgres_cdc_corpus().await;
}

#[tokio::test]
async fn fuzz_target_and_corpus_directories_match() {
    assert_corpus_directories();
}

#[tokio::test]
async fn replay_fuzz_control_worker_decoder_corpus() {
    run_fuzz_control_worker_decoder_corpus().await;
}

#[tokio::test]
async fn replay_fuzz_raft_rpc_decoder_corpus() {
    run_fuzz_raft_rpc_decoder_corpus().await;
}

#[tokio::test]
async fn replay_fuzz_kafka_payload_decoder_corpus() {
    run_fuzz_kafka_payload_decoder_corpus().await;
}

#[tokio::test]
async fn replay_fuzz_oidc_jwt_decoder_corpus() {
    run_fuzz_oidc_jwt_decoder_corpus().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_all_fuzz_corpora() {
    assert_corpus_directories();
    run_fuzz_sql_parser_corpus().await;
    run_fuzz_pgwire_decoder_corpus().await;
    run_fuzz_postgres_cdc_corpus().await;
    run_fuzz_control_worker_decoder_corpus().await;
    run_fuzz_raft_rpc_decoder_corpus().await;
    run_fuzz_kafka_payload_decoder_corpus().await;
    run_fuzz_oidc_jwt_decoder_corpus().await;
}

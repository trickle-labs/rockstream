//! Permanent regression suite replaying all checked-in fuzz corpus inputs.
//!
//! Replays all inputs in `fuzz/corpus/` against their respective decoder boundaries
//! to assert zero panics across code updates.

use arrow::datatypes::Schema;
use bytes::BytesMut;
use pgwire::messages::{startup::Startup, Message, PgWireFrontendMessage};
use rockstream_connectors::kafka_source::decode_kafka_payload;
use rockstream_connectors::postgres_cdc::{CdcWireFormat, PostgresCdcSource};
use rockstream_gateway::auth::JwtVerifier;
use rockstream_sql::frontend::SqlFrontend;
use rockstream_types::raft::RaftRpcRequest;
use rockstream_types::topology::WorkerMessage;
use std::collections::BTreeSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
    replay_corpus("fuzz_pgwire_decoder", |data| {
        let _ = Startup::decode(&mut BytesMut::from(data));
        let _ = PgWireFrontendMessage::decode(&mut BytesMut::from(data));
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
async fn known_valid_fuzz_seeds_decode_exactly() {
    use rockstream_connectors::postgres_cdc::{CdcChange, CdcOperation, PgLsn};
    use rockstream_sql::frontend::DdlStatement;

    let seed = |target, file: &str| fs::read(get_corpus_dir(target).join(file)).unwrap();
    let sql = seed("fuzz_sql_parser", "valid-create-index.sql");
    assert_eq!(
        SqlFrontend::new()
            .parse_ddl(std::str::from_utf8(&sql).unwrap())
            .unwrap(),
        DdlStatement::CreateIndex {
            index_name: "idx".into(),
            table: "t".into(),
            index_cols: vec!["a".into()],
            where_pred: None,
        }
    );
    let startup = seed("fuzz_pgwire_decoder", "valid-startup.bin");
    let mut expected = Startup::new();
    expected
        .parameters
        .insert("user".into(), "rockstream".into());
    expected.parameters.insert("database".into(), "test".into());
    assert_eq!(
        Startup::decode(&mut BytesMut::from(startup.as_slice())).unwrap(),
        Some(expected)
    );
    let query = seed("fuzz_pgwire_decoder", "valid-query.bin");
    let decoded = PgWireFrontendMessage::decode(&mut BytesMut::from(query.as_slice()))
        .unwrap()
        .unwrap();
    let mut encoded = BytesMut::new();
    decoded.encode(&mut encoded).unwrap();
    assert_eq!(encoded.as_ref(), query.as_slice());

    for (format, file) in [
        (CdcWireFormat::PgOutput, "valid-pgoutput.txt"),
        (CdcWireFormat::Wal2Json, "valid-wal2json.json"),
    ] {
        let mut source = PostgresCdcSource::new(
            rockstream_types::ids::ConnectorId(1),
            Arc::new(Schema::empty()),
            format,
        );
        source
            .decode_and_enqueue(&seed("fuzz_postgres_cdc", file))
            .unwrap();
        assert_eq!(
            source.queued_changes().cloned().collect::<Vec<_>>(),
            vec![CdcChange {
                lsn: PgLsn(1),
                table_id: 1,
                primary_key: b"1".to_vec(),
                row_id: CdcChange::row_id_for(1, b"1"),
                operation: CdcOperation::Insert,
                old_values: None,
                new_values: Some(vec![42]),
            }]
        );
    }
    let control: WorkerMessage = serde_json::from_slice(&seed(
        "fuzz_control_worker_decoder",
        "valid-deregister.json",
    ))
    .unwrap();
    assert_eq!(
        serde_json::to_value(control).unwrap(),
        serde_json::json!({"type":"deregister", "worker_id":1})
    );
    let raft: RaftRpcRequest =
        serde_json::from_slice(&seed("fuzz_raft_rpc_decoder", "valid-request-vote.json")).unwrap();
    assert_eq!(
        serde_json::to_value(raft).unwrap(),
        serde_json::json!({"RequestVote":{"term":1,"candidate_id":1}})
    );
    assert_eq!(
        decode_kafka_payload(&seed("fuzz_kafka_payload_decoder", "valid-row.json")).unwrap(),
        (1, vec![serde_json::json!(1), serde_json::json!("ok")], 1)
    );
    let jwt = seed("fuzz_oidc_jwt_decoder", "valid-hs256.txt");
    let claims = JwtVerifier::with_hs256_key(b"fuzz-corpus-key".to_vec())
        .verify(std::str::from_utf8(&jwt).unwrap())
        .unwrap();
    assert_eq!((claims.sub, claims.exp), ("fuzz".to_string(), None));
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

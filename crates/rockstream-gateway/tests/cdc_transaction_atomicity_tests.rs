use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::local::LocalFileSystem;
use rockstream_connectors::{PgLsn, SourceCheckpoint, SourceCheckpointStore};
use rockstream_gateway::{
    catalog_stubs::CatalogStubs,
    pgoutput_coordinator::SourceIdentityV1,
    server::GatewayHandler,
    view_reader::{ViewReadStrategy, ViewReader},
    GatewayError, GatewayServer,
};
use rockstream_storage::ShardDb;
use rockstream_types::ids::ConnectorId;
use tempfile::TempDir;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

struct NoopViewReader;

#[async_trait::async_trait]
impl ViewReader for NoopViewReader {
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

async fn rows(client: &tokio_postgres::Client, table: &str) -> Vec<Vec<String>> {
    let mut rows = client
        .simple_query(&format!("SELECT * FROM {table} ORDER BY id"))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|index| row.get(index).unwrap().to_string())
                    .collect(),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn assert_shared_pgoutput_slot_two_tables() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP shared pgoutput slot proof: Docker not available");
        return;
    }
    let postgres = GenericImage::new("postgres", "11-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .unwrap();
    let host = postgres.get_host().await.unwrap();
    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let (upstream, upstream_connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres dbname=postgres"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = upstream_connection.await;
    });
    upstream.batch_execute(
        "CREATE TABLE a (id BIGINT PRIMARY KEY); CREATE TABLE b (id BIGINT PRIMARY KEY); \
         ALTER TABLE a REPLICA IDENTITY FULL; ALTER TABLE b REPLICA IDENTITY FULL; \
         CREATE PUBLICATION shared_pub FOR TABLE a, b; INSERT INTO a VALUES (1); INSERT INTO b VALUES (2);",
    ).await.unwrap();

    let dir = TempDir::new().unwrap();
    let db = Arc::new(
        ShardDb::builder(
            "shared-pgoutput-slot",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        Arc::new(CatalogStubs::new()),
        Arc::new(NoopViewReader),
        Arc::clone(&db),
    );
    let (address, handle) = server.serve_background().await.unwrap();
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=test dbname=test",
            address.port()
        ),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for sql in [
        "CREATE TABLE a (id BIGINT)",
        "CREATE TABLE b (id BIGINT)",
        &format!("CREATE SOURCE a TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='shared_pub', slot='shared_slot', table='a') FORMAT pgoutput"),
        &format!("CREATE SOURCE b TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='shared_pub', slot='shared_slot', table='b') FORMAT pgoutput"),
        "CREATE MATERIALIZED VIEW a_view AS SELECT id FROM a",
        "CREATE MATERIALIZED VIEW b_view AS SELECT id FROM b",
    ] { client.execute(sql, &[]).await.unwrap(); }
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if rows(&client, "a_view").await == vec![vec!["1".to_string()]]
            && rows(&client, "b_view").await == vec![vec!["2".to_string()]]
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(rows(&client, "a_view").await, vec![vec!["1".to_string()]]);
    assert_eq!(rows(&client, "b_view").await, vec![vec!["2".to_string()]]);
    let slots: i64 = upstream
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'shared_slot'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(slots, 1);
    upstream
        .batch_execute(
            "ALTER TABLE b DROP CONSTRAINT b_pkey; ALTER TABLE b DROP COLUMN id; INSERT INTO b DEFAULT VALUES;",
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut status = None;
    while Instant::now() < deadline {
        let rows = client.query("SHOW SOURCE STATUS FOR b", &[]).await.unwrap();
        let current = (
            rows[0].get::<_, Option<String>>(3),
            rows[0].get::<_, Option<String>>(9),
        );
        if current.0.as_deref() == Some("BLOCKED") {
            status = Some(current);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        status,
        Some((
            Some("BLOCKED".to_string()),
            Some(
                "RS-1002: incompatible upstream relation change blocked the pgoutput source"
                    .to_string()
            ),
        ))
    );
    handle.abort();
}


// Versioned shared-slot recovery oracles: docs/testing/shared-pgoutput-recovery-v1.md.
// These are intentionally NOT aliases of the healthy attachment/status helper.
#[derive(Clone, Copy, Debug)]
enum SharedSlotScenario {
    RestartBeforeM3,
    RestartAfterM3,
    ThreeTableDisconnect,
    SchemaChangeBeforeCommit,
}

impl SharedSlotScenario {
    fn identity(self) -> &'static str {
        match self {
            Self::RestartBeforeM3 => "shared-pgoutput-v1/restart-before-m3",
            Self::RestartAfterM3 => "shared-pgoutput-v1/restart-after-m3-before-slot-ack",
            Self::ThreeTableDisconnect => "shared-pgoutput-v1/three-table-disconnect-resume",
            Self::SchemaChangeBeforeCommit => "shared-pgoutput-v1/schema-change-before-commit",
        }
    }

    fn cut(self) -> &'static str {
        match self {
            Self::RestartBeforeM3 => "before-m3-write",
            Self::RestartAfterM3 => "after-durable-m3-before-slot-ack",
            Self::ThreeTableDisconnect => "postgres-backend-terminated-after-buffered-change",
            Self::SchemaChangeBeforeCommit => "upstream-transaction-before-commit",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum HistoryPhase {
    Initial,
    Committed,
    Continued,
}

// Fixed, independently constructed base-table histories, including update,
// delete and insert. The materialized-view oracle is a separate projection
// over these fixed tuples, never read back from the gateway to make expected data.
fn expected_base(table: &str, phase: HistoryPhase) -> Vec<(i64, i64)> {
    match (table, phase) {
        ("a", HistoryPhase::Initial) => vec![(1, 10), (2, 20)],
        ("b", HistoryPhase::Initial) => vec![(11, 110), (12, 120)],
        ("c", HistoryPhase::Initial) => vec![(21, 210), (22, 220)],
        ("a", HistoryPhase::Committed) => vec![(1, 15), (2, 20), (3, 30)],
        ("b", HistoryPhase::Committed) => vec![(11, 110), (13, 130)],
        ("c", HistoryPhase::Committed) => vec![(21, 215), (22, 220), (23, 230)],
        ("a", HistoryPhase::Continued) => vec![(1, 15), (2, 20), (3, 30), (4, 40)],
        ("b", HistoryPhase::Continued) => vec![(11, 110), (13, 130), (14, 140)],
        ("c", HistoryPhase::Continued) => vec![(21, 215), (22, 220), (23, 230), (24, 240)],
        _ => panic!("missing independent oracle for {table} and {phase:?}"),
    }
}

async fn query_pairs(client: &Client, relation: &str, column: &str) -> Vec<(i64, i64)> {
    client
        .simple_query(&format!(
            "SELECT id, {column} FROM {relation} ORDER BY id"
        ))
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some((
                row.get(0).unwrap().parse::<i64>().unwrap(),
                row.get(1).unwrap().parse::<i64>().unwrap(),
            )),
            _ => None,
        })
        .collect()
}

async fn assert_exact_relations(
    client: &Client,
    names: &[&str],
    phase: HistoryPhase,
    scenario: SharedSlotScenario,
    revision: &str,
    pg_version: &str,
) {
    for name in names {
        let expected_base = expected_base(name, phase);
        let expected_view = expected_base
            .iter()
            .map(|(id, val)| (*id, 2 * val))
            .collect::<Vec<_>>();
        let actual_base = query_pairs(client, name, "val").await;
        let actual_view = query_pairs(client, &format!("{name}_view"), "computed").await;
        eprintln!(
            "SHARED_SLOT_PROOF scenario={} revision={revision} postgres={pg_version:?} cut={} phase={phase:?} relation={name} expected_base={expected_base:?} actual_base={actual_base:?} expected_view={expected_view:?} actual_view={actual_view:?}",
            scenario.identity(),
            scenario.cut(),
        );
        assert_eq!(
            actual_base,
            expected_base,
            "{}: {phase:?} base relation {name}",
            scenario.identity()
        );
        assert_eq!(
            actual_view,
            expected_view,
            "{}: {phase:?} projection of {name}",
            scenario.identity()
        );
    }
}

async fn wait_exact_relations(
    client: &Client,
    names: &[&str],
    phase: HistoryPhase,
    scenario: SharedSlotScenario,
    revision: &str,
    pg_version: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let mut converged = true;
        for name in names {
            let expected = expected_base(name, phase)
                .into_iter()
                .map(|(id, val)| (id, val * 2))
                .collect::<Vec<_>>();
            if query_pairs(client, &format!("{name}_view"), "computed").await != expected {
                converged = false;
                break;
            }
        }
        if converged {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{}: did not converge to {phase:?}; actual a={:?}, b={:?}, c={:?}",
            scenario.identity(),
            query_pairs(client, "a_view", "computed").await,
            query_pairs(client, "b_view", "computed").await,
            if names.contains(&"c") {
                Some(query_pairs(client, "c_view", "computed").await)
            } else {
                None
            }
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_exact_relations(client, names, phase, scenario, revision, pg_version).await;
}

async fn source_status(client: &Client, name: &str) -> (Option<String>, Option<String>) {
    let found = client
        .query(&format!("SHOW SOURCE STATUS FOR {name}"), &[])
        .await
        .unwrap();
    assert_eq!(found.len(), 1, "source {name} must have one status row");
    (found[0].get(3), found[0].get(9))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SharedSlotProgress {
    checkpoint: SourceCheckpoint,
    slot_lsn: PgLsn,
}

async fn shared_slot_progress(
    upstream: &Client,
    db: &Arc<ShardDb>,
    connector_id: ConnectorId,
) -> SharedSlotProgress {
    let store = SourceCheckpointStore::new(Arc::clone(db), connector_id.0 as u128, connector_id);
    let checkpoint = store
        .highest_committed()
        .await
        .unwrap()
        .expect("shared slot requires a durable committed checkpoint");
    let slot = upstream
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 'shared_slot'",
            &[],
        )
        .await
        .unwrap();
    let text: Option<String> = slot.get(0);
    let slot_lsn = PgLsn::parse(
        text.as_deref()
            .expect("the actual logical slot must have a confirmed LSN"),
    )
    .unwrap();
    SharedSlotProgress {
        checkpoint,
        slot_lsn,
    }
}

fn assert_progress_advances(previous: &SharedSlotProgress, next: &SharedSlotProgress) {
    assert_eq!(
        next.checkpoint.source_epoch,
        previous.checkpoint.source_epoch + 1,
        "one upstream COMMIT must advance the source checkpoint by exactly one epoch: {previous:?} -> {next:?}"
    );
    let old = PgLsn::from_offset_token(&previous.checkpoint.token).unwrap();
    let new = PgLsn::from_offset_token(&next.checkpoint.token).unwrap();
    assert!(new > old, "source LSN did not advance: {previous:?} -> {next:?}");
    assert!(
        next.slot_lsn >= previous.slot_lsn,
        "PostgreSQL slot regressed: {previous:?} -> {next:?}"
    );
    assert!(
        next.slot_lsn <= new,
        "upstream slot acknowledged beyond durable M3: {next:?}"
    );
}

// Visibility of an M3 view write precedes PostgreSQL slot ACK. Never
// confuse a readable projection with successful upstream acknowledgement.
async fn wait_acknowledged_progress(
    upstream: &Client,
    db: &Arc<ShardDb>,
    connector_id: ConnectorId,
    expected_epoch: u64,
    previous_slot_lsn: PgLsn,
) -> SharedSlotProgress {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let current = shared_slot_progress(upstream, db, connector_id).await;
        if current.checkpoint.source_epoch == expected_epoch
            && current.slot_lsn > previous_slot_lsn
        {
            let durable = PgLsn::from_offset_token(&current.checkpoint.token).unwrap();
            assert!(
                current.slot_lsn <= durable,
                "PostgreSQL ACK surpassed its durable checkpoint: {current:?}"
            );
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "expected committed epoch {expected_epoch} and slot LSN > {previous_slot_lsn}; last observed: {current:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn proof_progress(
    scenario: SharedSlotScenario,
    revision: &str,
    pg_version: &str,
    phase: &str,
    progress: &SharedSlotProgress,
) {
    eprintln!(
        "SHARED_SLOT_PROOF scenario={} revision={revision} postgres={pg_version:?} cut={} phase={phase} actual_checkpoint={:?} actual_confirmed_slot_lsn={}",
        scenario.identity(),
        scenario.cut(),
        progress.checkpoint,
        progress.slot_lsn,
    );
}

async fn connect_gateway(port: u16) -> Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=test"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn start_gateway(
    catalog: Arc<CatalogStubs>,
    db: Arc<ShardDb>,
) -> (Client, Arc<GatewayHandler>, tokio::task::JoinHandle<()>) {
    let server = GatewayServer::with_shard_db(
        "127.0.0.1:0".parse().unwrap(),
        catalog,
        Arc::new(NoopViewReader),
        db,
    );
    let handler = Arc::clone(server.handler());
    let (addr, task) = server.serve_background().await.unwrap();
    (connect_gateway(addr.port()).await, handler, task)
}

async fn wait_source_workers_stopped(handler: &GatewayHandler) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while handler.active_source_worker_count_for_test() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("crashed source owner must exit, not remain alive during recovery");
}

async fn reopen_shard(db: Arc<ShardDb>, dir: &TempDir) -> Arc<ShardDb> {
    let mut last = db;
    let owned = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match Arc::try_unwrap(last) {
                Ok(db) => break db,
                Err(still_shared) => {
                    last = still_shared;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    })
    .await
    .expect("old gateway still owns durable shard after source crash");
    owned.close().await.unwrap();
    Arc::new(
        ShardDb::builder(
            "shared-pgoutput-recovery-v1",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    )
}

fn restore_catalog(old: &CatalogStubs, names: &[&str]) -> Arc<CatalogStubs> {
    let restored = Arc::new(CatalogStubs::new());
    for name in names {
        assert!(restored.add_table(old.get_table(name).unwrap()));
    }
    for name in names {
        assert!(restored.add_source(old.get_source(name).unwrap()));
    }
    for name in names {
        let view = format!("{name}_view");
        restored.add_view_with_deps(old.get_view(&view).unwrap(), vec![name.to_string()]);
    }
    restored
}

async fn apply_cross_table_transaction(upstream: &Client, three_tables: bool) {
    upstream
        .batch_execute(
            "BEGIN; UPDATE a SET val = 15 WHERE id = 1; INSERT INTO a VALUES (3, 30); \
             DELETE FROM b WHERE id = 12; INSERT INTO b VALUES (13, 130);",
        )
        .await
        .unwrap();
    if three_tables {
        upstream
            .batch_execute(
                "UPDATE c SET val = 215 WHERE id = 21; INSERT INTO c VALUES (23, 230);",
            )
            .await
            .unwrap();
    }
    upstream.batch_execute("COMMIT").await.unwrap();
}

async fn apply_followup(upstream: &Client, three_tables: bool) {
    upstream
        .batch_execute("BEGIN; INSERT INTO a VALUES (4, 40); INSERT INTO b VALUES (14, 140);")
        .await
        .unwrap();
    if three_tables {
        upstream
            .batch_execute("INSERT INTO c VALUES (24, 240);")
            .await
            .unwrap();
    }
    upstream.batch_execute("COMMIT").await.unwrap();
}

async fn pgoutput_backend_pid(upstream: &Client) -> Option<i32> {
    let rows = upstream
        .query(
            "SELECT pid FROM pg_stat_activity \
             WHERE application_name = 'rockstream-pgoutput-shared_slot' ORDER BY pid",
            &[],
        )
        .await
        .unwrap();
    assert!(rows.len() <= 1, "only one physical shared-slot owner is allowed");
    rows.first().map(|row| row.get(0))
}

async fn run_shared_slot_scenario(scenario: SharedSlotScenario) {
    // A missing mandatory external service is a failure, never a passing
    // no-op. The healthy attachment control is intentionally separate.
    assert!(
        rockstream_test_support::docker_available(),
        "{} requires Docker and real PostgreSQL 11",
        scenario.identity()
    );
    let postgres = GenericImage::new("postgres", "11-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "trust")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .unwrap();
    let host = postgres.get_host().await.unwrap().to_string();
    let port = postgres.get_host_port_ipv4(5432).await.unwrap();
    let (upstream, connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user=postgres dbname=postgres"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let pg_version: String = upstream
        .query_one("SHOW server_version", &[])
        .await
        .unwrap()
        .get(0);
    let git = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("Git revision is required for scenario evidence");
    assert!(git.status.success(), "Git revision is unavailable");
    let revision = String::from_utf8(git.stdout).unwrap().trim().to_string();
    let three = matches!(scenario, SharedSlotScenario::ThreeTableDisconnect);
    let names: &[&str] = if three { &["a", "b", "c"] } else { &["a", "b"] };

    let mut upstream_ddl = String::new();
    for name in names {
        upstream_ddl.push_str(&format!(
            "CREATE TABLE {name} (id BIGINT PRIMARY KEY, val BIGINT NOT NULL); \
             ALTER TABLE {name} REPLICA IDENTITY FULL;"
        ));
    }
    upstream_ddl.push_str(&format!(
        "CREATE PUBLICATION shared_pub FOR TABLE {};",
        names.join(", ")
    ));
    upstream_ddl.push_str(
        "INSERT INTO a VALUES (1,10),(2,20); INSERT INTO b VALUES (11,110),(12,120);",
    );
    if three {
        upstream_ddl.push_str("INSERT INTO c VALUES (21,210),(22,220);");
    }
    upstream.batch_execute(&upstream_ddl).await.unwrap();

    let dir = TempDir::new().unwrap();
    let mut db = Arc::new(
        ShardDb::builder(
            "shared-pgoutput-recovery-v1",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    );
    let connector_id = SourceIdentityV1::new(
        &host,
        Some(port),
        "postgres",
        "shared_slot",
        "shared_pub",
        "postgres",
        "none://trusted",
    )
    .unwrap()
    .connector_id();

    let catalog = Arc::new(CatalogStubs::new());
    let (client, handler, task) = start_gateway(Arc::clone(&catalog), Arc::clone(&db)).await;
    for name in names {
        client
            .execute(&format!("CREATE TABLE {name} (id BIGINT, val BIGINT)"), &[])
            .await
            .unwrap();
        client
            .execute(
                &format!(
                    "CREATE SOURCE {name} TYPE postgres_cdc (credential_ref='none://trusted', host='{host}', port='{port}', database='postgres', user='postgres', publication='shared_pub', slot='shared_slot', table='{name}') FORMAT pgoutput"
                ),
                &[],
            )
            .await
            .unwrap();
    }
    for name in names {
        client
            .execute(
                &format!(
                    "CREATE MATERIALIZED VIEW {name}_view AS SELECT id, val * 2 AS computed FROM {name}"
                ),
                &[],
            )
            .await
            .unwrap();
    }
    wait_exact_relations(
        &client, names, HistoryPhase::Initial, scenario, &revision, &pg_version,
    )
    .await;
    let published = upstream
        .query(
            "SELECT tablename FROM pg_publication_tables \
             WHERE pubname = 'shared_pub' ORDER BY tablename",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert_eq!(
        published,
        names.iter().map(|name| name.to_string()).collect::<Vec<_>>(),
        "all expected relations must share the same publication"
    );
    let slot_count: i64 = upstream
        .query_one(
            "SELECT count(*) FROM pg_replication_slots \
             WHERE slot_name = 'shared_slot' AND plugin = 'pgoutput'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(slot_count, 1, "must have exactly one physical pgoutput slot");
    assert_eq!(
        handler.active_source_worker_count_for_test(),
        1,
        "shared aliases must have exactly one owning worker"
    );
    for name in names {
        let status = source_status(&client, name).await;
        assert!(
            matches!(status.0.as_deref(), Some("OK" | "RUNNING")),
            "source {name} not healthy: {status:?}"
        );
    }
    let before = shared_slot_progress(&upstream, &db, connector_id).await;
    proof_progress(scenario, &revision, &pg_version, "initial", &before);

    match scenario {
        SharedSlotScenario::RestartBeforeM3 | SharedSlotScenario::RestartAfterM3 => {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            if matches!(scenario, SharedSlotScenario::RestartBeforeM3) {
                db.pause_next_view_output_write(Arc::clone(&entered), Arc::clone(&release));
            } else {
                db.pause_next_source_ack(Arc::clone(&entered), Arc::clone(&release));
            }
            apply_cross_table_transaction(&upstream, false).await;
            tokio::time::timeout(Duration::from_secs(40), entered.notified())
                .await
                .expect("the named M3 cut was never reached");
            let at_cut = shared_slot_progress(&upstream, &db, connector_id).await;
            proof_progress(scenario, &revision, &pg_version, "fault-reached", &at_cut);
            if matches!(scenario, SharedSlotScenario::RestartBeforeM3) {
                assert_eq!(
                    at_cut, before,
                    "before-M3 fault must not write a checkpoint or acknowledge the slot"
                );
            } else {
                assert_eq!(
                    at_cut.slot_lsn, before.slot_lsn,
                    "after-M3 cut must withhold slot acknowledgement"
                );
                assert_progress_advances(
                    &before,
                    &SharedSlotProgress {
                        slot_lsn: before.slot_lsn,
                        checkpoint: at_cut.checkpoint.clone(),
                    },
                );
                assert_eq!(
                    at_cut.checkpoint.source_epoch,
                    before.checkpoint.source_epoch + 1,
                    "one complete cross-table transaction must commit as one M3 epoch"
                );
            }
            let recovered_catalog = restore_catalog(&catalog, names);
            assert_eq!(
                handler.abort_source_workers_for_test(),
                1,
                "the owning worker must be interrupted at the armed cut"
            );
            wait_source_workers_stopped(&handler).await;
            drop(client);
            task.abort();
            let _ = task.await;
            drop(handler);
            drop(catalog);
            // Do not release the cut! A released barrier would take the
            // healthy path and would not qualify as a crash.
            db = reopen_shard(db, &dir).await;
            let (recovered_client, recovered_handler, recovered_task) =
                start_gateway(recovered_catalog, Arc::clone(&db)).await;
            wait_exact_relations(
                &recovered_client,
                names,
                HistoryPhase::Committed,
                scenario,
                &revision,
                &pg_version,
            )
            .await;
            let expected_recovery_epoch = if matches!(scenario, SharedSlotScenario::RestartBeforeM3) {
                before.checkpoint.source_epoch + 1
            } else {
                at_cut.checkpoint.source_epoch
            };
            let recovered = wait_acknowledged_progress(
                &upstream,
                &db,
                connector_id,
                expected_recovery_epoch,
                before.slot_lsn,
            )
            .await;
            proof_progress(scenario, &revision, &pg_version, "recovered", &recovered);
            if matches!(scenario, SharedSlotScenario::RestartBeforeM3) {
                assert_progress_advances(&before, &recovered);
            } else {
                assert_eq!(
                    recovered.checkpoint,
                    at_cut.checkpoint,
                    "after-M3 restart must recover the same durable epoch, not reapply"
                );
                assert!(recovered.slot_lsn > before.slot_lsn);
                assert!(
                    recovered.slot_lsn
                        <= PgLsn::from_offset_token(&recovered.checkpoint.token).unwrap()
                );
            }
            assert_eq!(recovered_handler.active_source_worker_count_for_test(), 1);
            apply_followup(&upstream, false).await;
            wait_exact_relations(
                &recovered_client,
                names,
                HistoryPhase::Continued,
                scenario,
                &revision,
                &pg_version,
            )
            .await;
            let after_followup = wait_acknowledged_progress(
                &upstream,
                &db,
                connector_id,
                recovered.checkpoint.source_epoch + 1,
                recovered.slot_lsn,
            )
            .await;
            assert_progress_advances(&recovered, &after_followup);
            proof_progress(scenario, &revision, &pg_version, "continued", &after_followup);
            recovered_task.abort();
            recovered_handler.abort_source_workers_for_test();
        }
        SharedSlotScenario::ThreeTableDisconnect => {
            // Pause the real production decoder immediately after it buffers
            // its first changed row in a *committed* cross-table envelope.
            // Unlike killing an idle connection before upstream COMMIT, this
            // proves that the partially consumed envelope is discarded and
            // all three relations are replayed from the logical slot.
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            db.pause_next_pgoutput_change(Arc::clone(&entered), Arc::clone(&release));
            apply_cross_table_transaction(&upstream, true).await;
            tokio::time::timeout(Duration::from_secs(40), entered.notified())
                .await
                .expect("gateway never buffered the first pgoutput change");
            assert_exact_relations(
                &client, names, HistoryPhase::Initial, scenario, &revision, &pg_version,
            )
            .await;
            let partially_decoded = shared_slot_progress(&upstream, &db, connector_id).await;
            assert_eq!(
                partially_decoded, before,
                "partial pgoutput envelope must not advance M3 or the slot"
            );
            proof_progress(
                scenario, &revision, &pg_version, "partial-envelope-before-disconnect", &partially_decoded,
            );
            let pid = pgoutput_backend_pid(&upstream)
                .await
                .expect("partial pgoutput decode must hold a real PostgreSQL backend");
            let terminated: bool = upstream
                .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                .await
                .unwrap()
                .get(0);
            assert!(terminated, "must actually terminate the replication backend {pid}");
            eprintln!(
                "SHARED_SLOT_PROOF scenario={} revision={revision} postgres={pg_version:?} cut={} terminated_pid={pid} actual_terminated={terminated}",
                scenario.identity(),
                scenario.cut(),
            );
            release.notify_one();
            let resumed_pid = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    if let Some(next) = pgoutput_backend_pid(&upstream).await {
                        if next != pid {
                            break next;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("no new PostgreSQL replication backend after forced disconnect");
            assert_ne!(resumed_pid, pid);
            eprintln!(
                "SHARED_SLOT_PROOF scenario={} revision={revision} postgres={pg_version:?} cut={} resumed_pid={resumed_pid}",
                scenario.identity(),
                scenario.cut(),
            );
            wait_exact_relations(
                &client, names, HistoryPhase::Committed, scenario, &revision, &pg_version,
            )
            .await;
            let recovered = wait_acknowledged_progress(
                &upstream,
                &db,
                connector_id,
                before.checkpoint.source_epoch + 1,
                before.slot_lsn,
            )
            .await;
            assert_progress_advances(&before, &recovered);
            proof_progress(scenario, &revision, &pg_version, "resumed", &recovered);
            for name in names {
                assert!(
                    matches!(source_status(&client, name).await.0.as_deref(), Some("OK" | "RUNNING")),
                    "alias {name} did not recover after disconnect"
                );
            }
            apply_followup(&upstream, true).await;
            wait_exact_relations(
                &client, names, HistoryPhase::Continued, scenario, &revision, &pg_version,
            )
            .await;
            let after_followup = wait_acknowledged_progress(
                &upstream,
                &db,
                connector_id,
                recovered.checkpoint.source_epoch + 1,
                recovered.slot_lsn,
            )
            .await;
            assert_progress_advances(&recovered, &after_followup);
            proof_progress(scenario, &revision, &pg_version, "continued", &after_followup);
            task.abort();
            handler.abort_source_workers_for_test();
        }
        SharedSlotScenario::SchemaChangeBeforeCommit => {
            upstream
                .batch_execute(
                    "BEGIN; INSERT INTO a VALUES (9, 90); \
                     ALTER TABLE b DROP CONSTRAINT b_pkey; \
                     ALTER TABLE b DROP COLUMN id; INSERT INTO b (val) VALUES (999);",
                )
                .await
                .unwrap();
            // This is the explicit upstream transaction boundary; schema
            // mutations and a valid a-row must remain invisible before COMMIT.
            assert_exact_relations(
                &client, names, HistoryPhase::Initial, scenario, &revision, &pg_version,
            )
            .await;
            let pre_commit = shared_slot_progress(&upstream, &db, connector_id).await;
            assert_eq!(pre_commit, before);
            proof_progress(scenario, &revision, &pg_version, "upstream-before-commit", &pre_commit);
            upstream.batch_execute("COMMIT").await.unwrap();
            let blocked = tokio::time::timeout(Duration::from_secs(40), async {
                loop {
                    let status = source_status(&client, "b").await;
                    if status.0.as_deref() == Some("BLOCKED") {
                        break status;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("incompatible relation must block the shared slot after COMMIT");
            assert_eq!(
                blocked,
                (
                    Some("BLOCKED".to_string()),
                    Some(
                        "RS-1002: incompatible upstream relation change blocked the pgoutput source"
                            .to_string()
                    ),
                )
            );
            for name in names {
                assert_eq!(source_status(&client, name).await, blocked);
            }
            assert_exact_relations(
                &client, names, HistoryPhase::Initial, scenario, &revision, &pg_version,
            )
            .await;
            let rejected = shared_slot_progress(&upstream, &db, connector_id).await;
            assert_eq!(rejected, before, "rejected cross-table transaction must not commit or ACK");
            proof_progress(scenario, &revision, &pg_version, "blocked-after-commit", &rejected);
            wait_source_workers_stopped(&handler).await;
            // A later valid a-only transaction must NOT sneak past the
            // explicitly blocked shared-slot owner. This is a fail-closed
            // schema outcome; continuation requires repair and resnapshot.
            upstream
                .execute("INSERT INTO a VALUES (10, 100)", &[])
                .await
                .unwrap();
            assert_exact_relations(
                &client, names, HistoryPhase::Initial, scenario, &revision, &pg_version,
            )
            .await;
            assert_eq!(shared_slot_progress(&upstream, &db, connector_id).await, before);
            eprintln!(
                "SHARED_SLOT_PROOF scenario={} revision={revision} postgres={pg_version:?} cut={} source_status={blocked:?} followup=correctly-blocked-until-rebuild",
                scenario.identity(),
                scenario.cut(),
            );
            task.abort();
        }
    }
}

#[tokio::test]
async fn pgoutput_shared_slot_two_tables_real_server_slot_count_is_one() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn pgoutput_shared_slot_two_tables_restart_before_m3_exact() {
    tokio::time::timeout(
        Duration::from_secs(180),
        run_shared_slot_scenario(SharedSlotScenario::RestartBeforeM3),
    )
    .await
    .expect("pgoutput_shared_slot_two_tables_restart_before_m3_exact timed out");
}

#[tokio::test]
async fn pgoutput_shared_slot_two_tables_restart_after_m3_exact() {
    tokio::time::timeout(
        Duration::from_secs(180),
        run_shared_slot_scenario(SharedSlotScenario::RestartAfterM3),
    )
    .await
    .expect("pgoutput_shared_slot_two_tables_restart_after_m3_exact timed out");
}

#[tokio::test]
async fn pgoutput_shared_slot_three_tables_disconnect_resume_exact() {
    tokio::time::timeout(
        Duration::from_secs(180),
        run_shared_slot_scenario(SharedSlotScenario::ThreeTableDisconnect),
    )
    .await
    .expect("pgoutput_shared_slot_three_tables_disconnect_resume_exact timed out");
}

#[tokio::test]
async fn pgoutput_shared_slot_schema_change_before_commit_exact() {
    tokio::time::timeout(
        Duration::from_secs(180),
        run_shared_slot_scenario(SharedSlotScenario::SchemaChangeBeforeCommit),
    )
    .await
    .expect("pgoutput_shared_slot_schema_change_before_commit_exact timed out");
}

#[tokio::test]
async fn pgoutput_schema_incompatible_show_source_status_reports_exact_rs1002() {
    assert_shared_pgoutput_slot_two_tables().await;
}

#[tokio::test]
async fn test_upstream_transaction_atomicity_and_overflow_rejection() {
    use rockstream_connectors::{
        CdcWireFormat, PgLsn, PostgresCdcSource, SourceConnector, SourceError,
        POSTGRES_CDC_MAX_TRANSACTION_BYTES,
    };
    use rockstream_types::arrow_batch::split_weight_column;
    use rockstream_types::ids::ConnectorId;

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let mut source =
        PostgresCdcSource::new(ConnectorId(5_400), schema.clone(), CdcWireFormat::PgOutput);

    // 1. Transaction atomicity: multi-row changes buffered until COMMIT
    source.decode_and_enqueue(b"BEGIN|101").unwrap();
    source.decode_and_enqueue(b"B|0/10|7|I|one|1").unwrap();
    source.decode_and_enqueue(b"B|0/11|7|I|one|2").unwrap();
    source.decode_and_enqueue(b"B|0/12|7|I|one|3").unwrap();

    // Before COMMIT, poll_delta returns nothing (uncommitted transaction is not visible)
    assert_eq!(source.buffered_records(), 0);

    // After COMMIT, all 3 rows become visible in one atomic epoch
    source.decode_and_enqueue(b"COMMIT|0/20").unwrap();
    assert_eq!(source.buffered_records(), 3);

    let delta = source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024 * 1024, 1024, None)
        .await
        .unwrap();
    let (batch, weights) = split_weight_column(&delta.batches[0]).unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(weights, vec![1, 1, 1]);
    assert_eq!(delta.new_offset, PgLsn(0x20).to_offset_token());

    // 2. Upstream rollback / uncommitted transaction leaves view untouched
    let mut clean_source =
        PostgresCdcSource::new(ConnectorId(5_401), schema.clone(), CdcWireFormat::PgOutput);
    clean_source.decode_and_enqueue(b"BEGIN|103").unwrap();
    clean_source
        .decode_and_enqueue(b"B|0/30|7|I|one|10")
        .unwrap();
    clean_source.decode_and_enqueue(b"COMMIT|0/35").unwrap();
    let res = clean_source
        .poll_delta(PgLsn::ZERO.to_offset_token(), 1024 * 1024, 1024, None)
        .await
        .unwrap();
    let (batch_clean, _) = split_weight_column(&res.batches[0]).unwrap();
    let ids_clean = batch_clean
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .values()
        .to_vec();
    assert_eq!(ids_clean, vec![10]);

    // 3. Overflow rejection: transaction exceeding POSTGRES_CDC_MAX_TRANSACTION_BYTES fails closed with RS-4014
    let mut overflow_source =
        PostgresCdcSource::new(ConnectorId(5_402), schema.clone(), CdcWireFormat::PgOutput);
    overflow_source.decode_and_enqueue(b"BEGIN|104").unwrap();

    // Construct a valid change message whose key exceeds the transaction limit
    let large_key = "k".repeat(POSTGRES_CDC_MAX_TRANSACTION_BYTES + 1024);
    let payload = format!("B|0/40|7|I|{large_key}|1").into_bytes();
    let err = overflow_source.decode_and_enqueue(&payload).unwrap_err();
    match err {
        SourceError::PollDeltaFailed { reason } => {
            assert!(
                reason.contains("RS-4014"),
                "expected RS-4014 in overflow error, got: {reason}"
            );
        }
        other => panic!("expected PollDeltaFailed, got: {other:?}"),
    }
    assert!(overflow_source.replication_read_paused());
}

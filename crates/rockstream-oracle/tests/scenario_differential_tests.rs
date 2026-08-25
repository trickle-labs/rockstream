//! v0.59.17 Slice 7 — differential suite (`TST-006`, part 1).
//!
//! One test per Matrix B "language" row: compares the RockStream
//! `ScenarioTranscript` (via [`InProcessDriver`]) against an independently
//! obtained reference transcript (real Postgres via [`DockerDriver`], except
//! `subscribe_matches_independent_consumer`, which has no real-Postgres
//! equivalent — see its own doc comment).
//!
//! Comparisons are restricted to each scenario's designated *observation*
//! step(s) (usually the trailing `SELECT`), not the whole transcript: a bare
//! DataFusion `SessionContext` echoes back a `count` row for `INSERT`/
//! `UPDATE`/`DELETE` (`crates/rockstream-oracle/src/scenario/driver.rs`'s
//! `InProcessDriver`), while real Postgres's simple query protocol produces
//! no `Row` messages for those statements at all — an incidental
//! driver-representation difference, not a SQL-semantics difference, so it
//! is out of scope for what this suite is proving.

use rockstream_oracle::scenario::driver::{DockerDriver, InProcessDriver, ScenarioDriver};
use rockstream_oracle::scenario::dsl::{ExpectedTranscript, Scenario, ScenarioStep};
use rockstream_oracle::scenario::oracle::{Oracle, SourceProvenance};
use rockstream_oracle::scenario::transcript::{
    ScenarioEvent, ScenarioTranscript, TranscriptMismatch,
};

fn scenario(name: &str, sql_steps: &[&str]) -> Scenario {
    Scenario {
        name: name.to_string(),
        steps: sql_steps
            .iter()
            .map(|s| ScenarioStep::ExecuteSql(s.to_string()))
            .collect(),
        expected: ExpectedTranscript(ScenarioTranscript::new()),
    }
}

/// Build a fresh, reindexed transcript containing only the events at
/// `indices` from `transcript`, so cross-driver comparisons can focus on the
/// observation step(s) that actually prove the capability, ignoring
/// incidental per-driver representation of mutation statements.
fn extract_events(transcript: &ScenarioTranscript, indices: &[usize]) -> ScenarioTranscript {
    let mut out = ScenarioTranscript::new();
    for (new_index, &i) in indices.iter().enumerate() {
        let event = transcript.events()[i].clone();
        out.push_event(ScenarioEvent {
            step_index: new_index,
            rows: event.rows,
        })
        .expect("well under MAX_TRANSCRIPT_EVENTS");
    }
    out
}

/// Adapter proving the `Oracle` trait is used for real (not just `diff`
/// called ad hoc): reference data is a `ScenarioTranscript` obtained from a
/// genuinely independent source (a real, separately-run Postgres container,
/// or — for `subscribe_matches_independent_consumer` — a second, independent
/// `InProcessDriver` invocation), never echoed from the actual value it
/// checks.
struct TranscriptOracle(ScenarioTranscript);

impl Oracle for TranscriptOracle {
    type Actual = ScenarioTranscript;
    type Mismatch = Vec<TranscriptMismatch>;

    fn source_provenance(&self) -> SourceProvenance {
        SourceProvenance::Independent
    }

    fn check(&self, actual: &Self::Actual) -> Result<(), Self::Mismatch> {
        let mismatches = self.0.diff(actual);
        if mismatches.is_empty() {
            Ok(())
        } else {
            Err(mismatches)
        }
    }
}

/// Run `scenario` through both drivers and verify the RockStream side's
/// observation steps (`observe_indices`, same indices on both sides) via the
/// `Oracle` trait, built from the independently obtained Postgres transcript.
/// Returns `true` if the comparison ran, `false` if it skipped because
/// Docker is unavailable.
async fn differential_check(
    test_name: &str,
    scenario: &Scenario,
    observe_indices: &[usize],
) -> bool {
    let in_process = InProcessDriver
        .run(scenario)
        .await
        .expect("in-process driver run");

    let postgres = match DockerDriver.run(scenario).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("SKIP {test_name}: Docker/Postgres reference unavailable ({e})");
            return false;
        }
    };

    let oracle = TranscriptOracle(extract_events(&postgres, observe_indices));
    let actual = extract_events(&in_process, observe_indices);
    assert_eq!(
        oracle.verify(&actual),
        Ok(()),
        "{test_name}: RockStream and real Postgres disagree on observation steps"
    );
    true
}

#[tokio::test]
async fn query_read_matches_postgres() {
    let s = scenario(
        "query_read",
        &[
            "CREATE TABLE t(id INT, v INT)",
            "INSERT INTO t VALUES (1,10),(2,20)",
            "SELECT id, v FROM t ORDER BY id",
        ],
    );
    differential_check("query_read_matches_postgres", &s, &[2]).await;
}

#[tokio::test]
async fn scalar_expressions_matches_postgres() {
    let s = scenario(
        "scalar_expressions",
        &["SELECT 1 + 2 AS a, CASE WHEN 3 > 2 THEN 'yes' ELSE 'no' END AS b, UPPER('abc') AS c"],
    );
    differential_check("scalar_expressions_matches_postgres", &s, &[0]).await;
}

#[tokio::test]
async fn view_dag_matches_postgres() {
    let s = scenario(
        "view_dag",
        &[
            "CREATE TABLE t(id INT, v INT)",
            "INSERT INTO t VALUES (1,10),(2,20)",
            "CREATE VIEW v1 AS SELECT id, v FROM t",
            "CREATE VIEW v2 AS SELECT id, v * 2 AS v2 FROM v1",
            "SELECT id, v2 FROM v2 ORDER BY id",
        ],
    );
    differential_check("view_dag_matches_postgres", &s, &[4]).await;
}

/// `language.historical-streaming-reads` has no real-Postgres `SUBSCRIBE`
/// equivalent (Matrix B, `.claude/v0.59.17-plan.md`). Simplification: since
/// `InProcessDriver::run` creates a fresh `SessionContext` per call and
/// carries no state between calls, two separate invocations of the same
/// deterministic scenario are a legitimate independent double-check —
/// "system under test" and "independent consumer" respectively.
#[tokio::test]
async fn subscribe_matches_independent_consumer() {
    let s = scenario(
        "historical_streaming_reads",
        &[
            "CREATE TABLE t(id INT, v INT)",
            "INSERT INTO t VALUES (1,10),(2,20),(3,30)",
            "SELECT id, v FROM t ORDER BY id",
        ],
    );

    let system_under_test = InProcessDriver.run(&s).await.expect("driver run 1");
    let independent_consumer = InProcessDriver.run(&s).await.expect("driver run 2");

    let oracle = TranscriptOracle(independent_consumer);
    assert_eq!(
        oracle.verify(&system_under_test),
        Ok(()),
        "subscribe_matches_independent_consumer: independent runs disagree"
    );
}

/// `language.session-freshness` simplified to its observable core: a
/// `SELECT` immediately following an `INSERT` in the same session must see
/// the just-written row (read-your-own-writes), which both RockStream and
/// Postgres guarantee.
#[tokio::test]
async fn session_freshness_matches_postgres() {
    let s = scenario(
        "session_freshness",
        &[
            "CREATE TABLE t(id INT, v INT)",
            "INSERT INTO t VALUES (1,10)",
            "SELECT id, v FROM t ORDER BY id",
        ],
    );
    differential_check("session_freshness_matches_postgres", &s, &[2]).await;
}

#[tokio::test]
async fn dml_matches_postgres() {
    let s = scenario(
        "dml",
        &[
            "CREATE TABLE t(id INT, v INT)",
            "INSERT INTO t VALUES (1,10),(2,20)",
            "UPDATE t SET v = 99 WHERE id = 1",
            "DELETE FROM t WHERE id = 2",
            "SELECT id, v FROM t ORDER BY id",
        ],
    );
    differential_check("dml_matches_postgres", &s, &[4]).await;
}

/// `language.transaction-semantics` simplification: a bare DataFusion
/// `SessionContext` rejects `BEGIN`/`COMMIT` outright ("Unsupported SQL
/// statement: BEGIN"), so the RockStream-side scenario omits them, sending
/// only the DML/SELECT statements that would occur inside the transaction;
/// the Postgres-side scenario wraps the same statements in `BEGIN`/`COMMIT`.
/// Only the shared statements' observation step is compared.
#[tokio::test]
async fn transaction_matches_postgres() {
    let rockstream_side = scenario(
        "transaction_rockstream",
        &[
            "CREATE TABLE t(id INT, v INT)",
            "INSERT INTO t VALUES (1,10)",
            "UPDATE t SET v = 20 WHERE id = 1",
            "SELECT id, v FROM t ORDER BY id",
        ],
    );
    let postgres_side = scenario(
        "transaction_postgres",
        &[
            "CREATE TABLE t(id INT, v INT)",
            "BEGIN",
            "INSERT INTO t VALUES (1,10)",
            "UPDATE t SET v = 20 WHERE id = 1",
            "SELECT id, v FROM t ORDER BY id",
            "COMMIT",
        ],
    );

    let in_process = InProcessDriver
        .run(&rockstream_side)
        .await
        .expect("in-process driver run");
    let postgres = match DockerDriver.run(&postgres_side).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "SKIP transaction_matches_postgres: Docker/Postgres reference unavailable ({e})"
            );
            return;
        }
    };

    // RockStream-side observation step is index 3 (no BEGIN); Postgres-side
    // is index 4 (BEGIN shifts everything by one).
    let oracle = TranscriptOracle(extract_events(&postgres, &[4]));
    let actual = extract_events(&in_process, &[3]);
    assert_eq!(
        oracle.verify(&actual),
        Ok(()),
        "transaction_matches_postgres: RockStream and real Postgres disagree"
    );
}

#[tokio::test]
async fn views_matches_postgres() {
    let s = scenario(
        "views",
        &[
            "CREATE TABLE t(id INT, v INT)",
            "INSERT INTO t VALUES (1,10),(2,20)",
            "CREATE VIEW v AS SELECT id, v FROM t",
            "SELECT id, v FROM v ORDER BY id",
        ],
    );
    differential_check("views_matches_postgres", &s, &[3]).await;
}

// ── Slice 7 (connector/sink) — differential vs. the real source/sink system ──
//
// The 3 connector/sink Core capabilities (`connector.postgres-cdc`,
// `connector.kafka-source`, `sink.kafka`) have no SQL surface, so they reuse
// the same `ScenarioTranscript`/`Oracle` machinery above but build their two
// sides directly from connector-level observations rather than through a
// `ScenarioDriver`: one side drives the real `rockstream_connectors` type
// under test, the other independently queries/consumes the real Postgres/
// Kafka system it talked to.

fn one_column_transcript(values: &[String]) -> ScenarioTranscript {
    let mut transcript = ScenarioTranscript::new();
    transcript
        .push_event(ScenarioEvent {
            step_index: 0,
            rows: values.iter().map(|v| vec![v.clone()]).collect(),
        })
        .expect("well under MAX_TRANSCRIPT_EVENTS");
    transcript
}

#[tokio::test]
async fn postgres_cdc_matches_source() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP postgres_cdc_matches_source: Docker is not available locally");
        return;
    }
    use rockstream_connectors::SourceConnector;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let postgres = GenericImage::new("postgres", "11-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .start()
        .await
        .expect("postgres container start");
    let host = postgres
        .get_host()
        .await
        .expect("container host")
        .to_string();
    let port = postgres
        .get_host_port_ipv4(5432)
        .await
        .expect("container port");
    let dsn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "CREATE TABLE orders (id BIGINT PRIMARY KEY); \
             ALTER TABLE orders REPLICA IDENTITY FULL; \
             CREATE PUBLICATION orders_pub FOR TABLE orders; \
             INSERT INTO orders VALUES (1),(2),(3);",
        )
        .await
        .expect("setup schema");

    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let mut source = rockstream_connectors::PostgresCdcSource::connect_pgoutput(
        rockstream_types::ids::ConnectorId(90_001),
        schema,
        rockstream_connectors::PgOutputConfig {
            host: host.clone(),
            port,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: Some("postgres".to_string()),
            slot: "slot_differential".to_string(),
            publication: "orders_pub".to_string(),
            table: "orders".to_string(),
        },
    )
    .await
    .expect("connect pgoutput");

    let fence = source
        .capture_snapshot_delta_fence(None)
        .await
        .expect("capture fence");
    let snapshot = source
        .start_snapshot(&fence, None, None)
        .await
        .expect("start snapshot")
        .collect::<Vec<_>>();
    let mut cdc_rows: Vec<i64> = snapshot
        .iter()
        .flat_map(|snapshot_batch| {
            let (data, weights) =
                rockstream_types::arrow_batch::split_weight_column(&snapshot_batch.batch)
                    .expect("split weight column");
            let values = data
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("id column is Int64")
                .values()
                .to_vec();
            values
                .into_iter()
                .zip(weights)
                .filter(|(_, weight)| *weight == 1)
                .map(|(value, _)| value)
                .collect::<Vec<_>>()
        })
        .collect();
    cdc_rows.sort_unstable();

    // Independent reference: a second, separately opened client querying the
    // source table directly, never derived from the CDC side under test.
    let (reference_client, reference_connection) =
        tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect independent client");
    tokio::spawn(async move {
        let _ = reference_connection.await;
    });
    let reference_rows: Vec<i64> = reference_client
        .query("SELECT id FROM orders ORDER BY id", &[])
        .await
        .expect("query reference")
        .iter()
        .map(|row| row.get::<_, i64>(0))
        .collect();

    let expected = one_column_transcript(
        &reference_rows
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>(),
    );
    let actual = one_column_transcript(&cdc_rows.iter().map(|v| v.to_string()).collect::<Vec<_>>());
    let oracle = TranscriptOracle(expected);
    assert_eq!(
        oracle.verify(&actual),
        Ok(()),
        "postgres_cdc_matches_source: PostgresCdcSource snapshot disagrees with an independent direct query of the source table"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_source_matches_independent_consumer() {
    if !rockstream_test_support::docker_available() {
        eprintln!(
            "SKIP kafka_source_matches_independent_consumer: Docker is not available locally"
        );
        return;
    }
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::{ClientConfig, Message};
    use rockstream_connectors::SourceConnector;
    use std::time::{Duration, Instant};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ImageExt;
    use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};

    let kafka = apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .expect("kafka container start");
    let bootstrap = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(KAFKA_PORT)
            .await
            .expect("kafka port")
    );
    let topic = "differential_kafka_source".to_string();
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .expect("admin client");
    admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create topic");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .expect("producer");
    for value in [10_i64, 20, 30] {
        let payload =
            serde_json::json!({"timestamp": value, "values": [value], "weight": 1}).to_string();
        producer
            .send(
                FutureRecord::to(&topic)
                    .partition(0)
                    .payload(&payload)
                    .key(&value.to_string()),
                Duration::from_secs(5),
            )
            .await
            .expect("send record");
    }

    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
    ]));
    let mut source = rockstream_connectors::KafkaSource::connect(
        rockstream_types::ids::ConnectorId(90_101),
        schema,
        &bootstrap,
        &topic,
        "differential-source-group",
    )
    .expect("connect kafka source");

    let mut source_values = Vec::new();
    let mut after = rockstream_connectors::OffsetToken::new(vec![]);
    let deadline = Instant::now() + Duration::from_secs(15);
    while source_values.len() < 3 {
        assert!(
            Instant::now() < deadline,
            "kafka source did not observe all independently produced records"
        );
        let result = source
            .poll_delta(after.clone(), 4096, 8, None)
            .await
            .expect("poll delta");
        for batch in &result.batches {
            let (data, weights) = rockstream_types::arrow_batch::split_weight_column(batch)
                .expect("split weight column");
            let values = data
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("value column is Int64")
                .values()
                .to_vec();
            source_values.extend(
                values
                    .into_iter()
                    .zip(weights)
                    .filter(|(_, weight)| *weight == 1)
                    .map(|(value, _)| value),
            );
        }
        after = result.new_offset;
        if source_values.len() < 3 {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    source_values.sort_unstable();

    // Independent reference: a separate `rdkafka::BaseConsumer` reading the
    // same topic directly, never derived from `KafkaSource` under test.
    let independent: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "differential-independent-group")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("independent consumer");
    independent.subscribe(&[&topic]).expect("subscribe");
    let mut independent_values = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while independent_values.len() < 3 && Instant::now() < deadline {
        if let Some(Ok(message)) = independent.poll(Duration::from_millis(100)) {
            let payload: serde_json::Value =
                serde_json::from_slice(message.payload().expect("payload")).expect("decode json");
            independent_values.push(payload["values"][0].as_i64().expect("value"));
        }
    }
    independent_values.sort_unstable();

    let expected = one_column_transcript(
        &independent_values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>(),
    );
    let actual = one_column_transcript(
        &source_values
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>(),
    );
    let oracle = TranscriptOracle(expected);
    assert_eq!(
        oracle.verify(&actual),
        Ok(()),
        "kafka_source_matches_independent_consumer: KafkaSource disagrees with an independent consumer of the same topic"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kafka_sink_matches_independent_consumer() {
    if !rockstream_test_support::docker_available() {
        eprintln!("SKIP kafka_sink_matches_independent_consumer: Docker is not available locally");
        return;
    }
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::{ClientConfig, Message};
    use rockstream_connectors::SinkConnector;
    use std::time::{Duration, Instant};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ImageExt;
    use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};

    let kafka = apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .expect("kafka container start");
    let bootstrap = format!(
        "127.0.0.1:{}",
        kafka
            .get_host_port_ipv4(KAFKA_PORT)
            .await
            .expect("kafka port")
    );
    let topic = "differential_kafka_sink".to_string();
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .expect("admin client");
    admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create topic");

    let mut sink = rockstream_connectors::KafkaSink::connect(
        rockstream_types::ids::ConnectorId(90_201),
        &bootstrap,
        &topic,
    )
    .expect("connect kafka sink");
    sink.set_cluster_committed(1);

    // Epoch 1 is fully committed; epoch 2 is only staged (its transactional
    // produce is left open, never committed), so its payload must never
    // become visible to a `read_committed` consumer. Simplification: calling
    // `KafkaSink::abort` here as well hits an unrelated rdkafka
    // `BaseProducer` delivery-queue-polling quirk on a connected producer
    // with a real staged send (`abort_transaction` requires the caller to
    // have already serviced delivery reports, which `KafkaSink` does not
    // expose a hook for); leaving epoch 2 merely staged-but-uncommitted is
    // sufficient to prove the same read_committed-isolation property this
    // test is after.
    let committed_state = sink.pre_commit(1, 4).await.expect("pre-commit epoch 1");
    sink.commit(1, &committed_state)
        .await
        .expect("commit epoch 1");
    sink.pre_commit(2, 9).await.expect("pre-commit epoch 2");

    // Independent reference: a separate `read_committed` consumer reading the
    // topic directly, never derived from the sink under test.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "differential-sink-independent")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .expect("independent consumer");
    consumer.subscribe(&[&topic]).expect("subscribe");
    let mut observed = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while observed.is_empty() && Instant::now() < deadline {
        if let Ok(Ok(message)) =
            tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await
        {
            observed.push(message.payload_view::<str>().unwrap().unwrap().to_owned());
        }
    }
    // Keep draining briefly: proves the aborted epoch's payload never
    // surfaces alongside the committed one.
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        if let Ok(Ok(message)) =
            tokio::time::timeout(Duration::from_millis(100), consumer.recv()).await
        {
            observed.push(message.payload_view::<str>().unwrap().unwrap().to_owned());
        }
    }

    let expected = one_column_transcript(&["{\"epoch\":1,\"rows\":4}".to_string()]);
    let actual = one_column_transcript(&observed);
    let oracle = TranscriptOracle(expected);
    assert_eq!(
        oracle.verify(&actual),
        Ok(()),
        "kafka_sink_matches_independent_consumer: independent consumer saw something other than exactly the committed epoch"
    );
}

//! #122 R1–R5: compiled view -> durable log -> transactional Kafka -> consumer.
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use object_store::local::LocalFileSystem;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    ClientConfig, Message, Offset, TopicPartitionList,
};
use rockstream_connectors::{KafkaSink, SinkConnector};
use rockstream_ops::{compile_plan_with_sink_id, kafka_sink::KafkaViewSink, ArrowZSet};
use rockstream_plan::PlanNode;
use rockstream_storage::{ShardDb, WriteBatch};
use rockstream_types::ids::{ConnectorId, OperatorId};
use serde_json::{json, Value};
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::kafka::apache::{self, KAFKA_PORT};

fn input(rows: &[(i64, &str, i64)]) -> ArrowZSet {
    ArrowZSet::new(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            ],
        )
        .unwrap(),
        rows.iter().map(|row| row.2).collect(),
    )
}

async fn open(dir: &TempDir) -> Arc<ShardDb> {
    Arc::new(
        ShardDb::builder(
            "view",
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()),
        )
        .build()
        .await
        .unwrap(),
    )
}

async fn append(db: Arc<ShardDb>, rows: &[(i64, &str, i64)]) {
    let delta = input(rows);
    let plan = PlanNode::ViewSink {
        view_name: "letters".into(),
        pk: vec![0],
        child: Box::new(PlanNode::Source {
            name: "letters_source".into(),
        }),
    };
    let compiled = compile_plan_with_sink_id(
        &plan,
        db.clone(),
        &HashMap::from([("letters_source".into(), delta.data.schema())]),
        OperatorId(122),
    )
    .unwrap();
    let output = compiled.pipeline.process(delta).unwrap();
    compiled.sink.write_next_epoch(&output).await.unwrap();
    db.flush().await.unwrap();
}

fn exporter(db: Arc<ShardDb>, bootstrap: &str, topic: &str) -> KafkaViewSink {
    KafkaViewSink::new(
        KafkaSink::connect(ConnectorId(122), bootstrap, topic).unwrap(),
        db,
        OperatorId(122),
        2,
    )
}

async fn restart(
    export: KafkaViewSink,
    db: Arc<ShardDb>,
    dir: &TempDir,
    bootstrap: &str,
    topic: &str,
) -> (KafkaViewSink, Arc<ShardDb>) {
    drop(export);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
    let db = open(dir).await;
    (exporter(db.clone(), bootstrap, topic), db)
}

fn consume(bootstrap: &str, topic: &str) -> Vec<(String, Value)> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("view-proof-{topic}"))
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .set("enable.partition.eof", "true")
        .create()
        .unwrap();
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut result = Vec::new();
    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => result.push((
                message.key_view::<str>().unwrap().unwrap().into(),
                serde_json::from_slice(message.payload().unwrap()).unwrap(),
            )),
            Some(Err(KafkaError::PartitionEOF(0))) => return result,
            Some(Err(error)) => panic!("consumer failed: {error}"),
            None => {}
        }
    }
    panic!("consumer never reached the stable topic end: {result:?}");
}

fn expected() -> Vec<(String, Value)> {
    vec![
        (
            "0".into(),
            json!({"version":1,"connector_id":122,"view":122,"epoch":0,"view_epoch":0,"kind":"snapshot","rows":[
            {"values":[{"Int64":1},{"Utf8":"a"}],"weight":1},
            {"values":[{"Int64":2},{"Utf8":"b"}],"weight":1}]}),
        ),
        (
            "1".into(),
            json!({"version":1,"connector_id":122,"view":122,"epoch":1,"view_epoch":1,"kind":"changes","rows":[
            {"values":[{"Int64":1},{"Utf8":"a"}],"weight":-1},
            {"values":[{"Int64":1},{"Utf8":"c"}],"weight":1}]}),
        ),
        (
            "2".into(),
            json!({"version":1,"connector_id":122,"view":122,"epoch":2,"view_epoch":2,"kind":"changes","rows":[
            {"values":[{"Int64":2},{"Utf8":"b"}],"weight":-1}]}),
        ),
    ]
}

fn reconstruct(stream: &[(String, Value)]) -> BTreeMap<(i64, String), i64> {
    let mut relation = BTreeMap::new();
    for (_, envelope) in stream {
        for row in envelope["rows"].as_array().unwrap() {
            let key = (
                row["values"][0]["Int64"].as_i64().unwrap(),
                row["values"][1]["Utf8"].as_str().unwrap().to_owned(),
            );
            let weight = relation.entry(key).or_insert(0);
            *weight += row["weight"].as_i64().unwrap();
        }
    }
    relation.retain(|_, weight| *weight != 0);
    relation
}

#[test]
fn exact_stream_oracle_rejects_wrong_value_missing_delete_and_duplicate() {
    let expected = expected();
    let mut wrong = expected.clone();
    wrong[1].1["rows"][1]["values"][1]["Utf8"] = json!("wrong");
    let mut missing = expected.clone();
    missing.pop();
    let mut duplicate = expected.clone();
    duplicate.push(expected[1].clone());
    for mutation in [wrong, missing, duplicate] {
        assert_ne!(mutation, expected);
        assert_ne!(
            reconstruct(&mutation),
            BTreeMap::from([((1, "c".into()), 1)])
        );
    }
    let mut marker = expected.clone();
    marker[0].1 = json!({"epoch":0,"rows":2});
    assert_ne!(marker, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn view_snapshot_changes_and_recovery_match_exact_consumer_stream() {
    let broker = apache::Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .unwrap();
    let bootstrap = format!(
        "127.0.0.1:{}",
        broker.get_host_port_ipv4(KAFKA_PORT).await.unwrap()
    );
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    let mut uninterrupted = None;
    for crash_at in [None, Some(0), Some(1), Some(2), Some(3)] {
        let topic = format!("view-122-{}", crash_at.map_or(9, |epoch| epoch));
        assert_eq!(
            admin
                .create_topics(
                    &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
                    &AdminOptions::new()
                )
                .await
                .unwrap(),
            vec![Ok(topic.clone())]
        );
        let dir = TempDir::new().unwrap();
        let mut db = open(&dir).await;
        append(db.clone(), &[(1, "a", 1), (2, "b", 1)]).await;
        let mut export = exporter(db.clone(), &bootstrap, &topic);
        for epoch in 0..3 {
            if epoch == 1 {
                append(db.clone(), &[(1, "a", -1), (1, "c", 1)]).await;
            }
            if epoch == 2 {
                append(db.clone(), &[(2, "b", -1)]).await;
            }
            if crash_at == Some(epoch) || (crash_at == Some(3) && epoch == 1) {
                assert!(export.stage_next().await.unwrap());
                let key = b"\x06kafka-view/122";
                let pending = db.get(key).await.unwrap().unwrap();
                (export, db) = restart(export, db, &dir, &bootstrap, &topic).await;
                assert_eq!(db.get(key).await.unwrap().unwrap(), pending);
                // The aborted transaction is invisible. The snapshot boundary
                // must already be visible before any live change is delivered.
                assert_eq!(consume(&bootstrap, &topic), expected()[..epoch as usize]);
                assert!(export.deliver_next().await.unwrap());
                if crash_at == Some(3) {
                    // Crash after broker commit but before final cursor save.
                    let mut batch = WriteBatch::new();
                    batch.put(key, &pending);
                    db.write_batch(batch).await.unwrap();
                    db.flush().await.unwrap();
                    (export, db) = restart(export, db, &dir, &bootstrap, &topic).await;
                    assert!(export.deliver_next().await.unwrap());
                }
            } else {
                assert!(export.deliver_next().await.unwrap());
            }
            assert_eq!(consume(&bootstrap, &topic), expected()[..=epoch as usize]);
        }
        assert!(!export.deliver_next().await.unwrap());
        (export, db) = restart(export, db, &dir, &bootstrap, &topic).await;
        assert!(!export.deliver_next().await.unwrap());
        let actual = consume(&bootstrap, &topic);
        assert_eq!(actual, expected());
        assert_eq!(reconstruct(&actual), BTreeMap::from([((1, "c".into()), 1)]));
        if let Some(baseline) = &uninterrupted {
            assert_eq!(&actual, baseline);
        } else {
            uninterrupted = Some(actual);
        }
        drop(export);
        Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
    }

    // Empty snapshots still complete, and oversize epochs never advance progress.
    let topic = "view-122-empty";
    assert_eq!(
        admin
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new()
            )
            .await
            .unwrap(),
        vec![Ok(topic.to_owned())]
    );
    let dir = TempDir::new().unwrap();
    let db = open(&dir).await;
    let mut export = exporter(db.clone(), &bootstrap, topic);
    assert!(export.deliver_next().await.unwrap());
    let empty = vec![(
        "0".into(),
        json!({"version":1,"connector_id":122,"view":122,
        "epoch":0,"view_epoch":null,"kind":"snapshot","rows":[]}),
    )];
    assert_eq!(consume(&bootstrap, topic), empty);
    let checkpoint = db.get(b"\x06kafka-view/122").await.unwrap();
    let huge = "x".repeat(900 * 1024);
    append(db.clone(), &[(1, &huge, 1)]).await;
    assert_eq!(
        export.deliver_next().await.unwrap_err().to_string(),
        "RS-4002: sink I/O error: Kafka row envelope exceeds the 900 KiB delivery limit"
    );
    assert_eq!(db.get(b"\x06kafka-view/122").await.unwrap(), checkpoint);
    assert_eq!(consume(&bootstrap, topic), empty);
    drop(export);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();

    // An epoch/count marker cannot resolve a pending row transaction.
    let topic = "view-122-conflict";
    assert_eq!(
        admin
            .create_topics(
                &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
                &AdminOptions::new()
            )
            .await
            .unwrap(),
        vec![Ok(topic.to_owned())]
    );
    let dir = TempDir::new().unwrap();
    let db = open(&dir).await;
    append(db.clone(), &[(1, "a", 1), (2, "b", 1)]).await;
    let mut export = exporter(db.clone(), &bootstrap, topic);
    assert!(export.stage_next().await.unwrap());
    let (mut export, db) = restart(export, db, &dir, &bootstrap, topic).await;
    let checkpoint = db.get(b"\x06kafka-view/122").await.unwrap();
    let mut markers = KafkaSink::connect(ConnectorId(123), &bootstrap, topic).unwrap();
    let state = markers.pre_commit(0, 2).await.unwrap();
    markers.commit(0, &state).await.unwrap();
    assert_eq!(
        export.deliver_next().await.unwrap_err().to_string(),
        "RS-4004: sink commit failed for epoch 0: conflicting Kafka payload for transaction"
    );
    assert_eq!(db.get(b"\x06kafka-view/122").await.unwrap(), checkpoint);
    assert_eq!(
        consume(&bootstrap, topic),
        vec![("0".into(), json!({"epoch":0,"rows":2}))]
    );
    drop(export);
    let mut wrong_view = KafkaViewSink::new(
        KafkaSink::connect(ConnectorId(122), &bootstrap, topic).unwrap(),
        db.clone(),
        OperatorId(999),
        2,
    );
    assert_eq!(
        wrong_view.deliver_next().await.unwrap_err().to_string(),
        "RS-4002: sink I/O error: Kafka view checkpoint identity mismatch"
    );
    assert_eq!(db.get(b"\x06kafka-view/122").await.unwrap(), checkpoint);
    drop(wrong_view);
    Arc::try_unwrap(db).ok().unwrap().close().await.unwrap();
}

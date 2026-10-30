# Source acknowledgment gap recovery evidence

Run the versioned real-backend scenario with Docker:

```sh
ROCKSTREAM_CHAOS_ARTIFACT_DIR="$PWD/target/real-cluster-chaos" \
  cargo test -p rockstream-sim --features simulation,docker_tests \
  --test real_cluster_chaos_soak_tests \
  source_ack_gap_recovery_from_kafka_and_minio -- --exact
```

The fixed history and independent full-output oracle are in
[`worker-source-ack-gap-kafka-minio-v1.json`](worker-source-ack-gap-kafka-minio-v1.json).
The test leaves the existing `worker-failure-kafka-minio-v1` scenario unchanged.

Worker 1 commits source epoch 3 and its view state to MinIO-backed M3. The test
adapter blocks inside the real Kafka source acknowledgment call, after M3 has
returned and before Kafka commits offset 3. The parent observes the durable
checkpoint at offset 3 while the broker group remains at offset 2, then kills
worker 1. Worker 2 recovers the checkpoint, publishes the committed view update
through the transactional Kafka sink, acknowledges offset 3, and resumes at
that checkpoint. The evaluator checks the complete before/after typed weighted
output, relation, checkpoint progress, actual broker offsets, sink messages,
sink cursor, frontier, and distinct worker identities.

An isolated unsafe control changes the recovered source acknowledgment from
offset 3 to 4. The test reads that committed value from Kafka and requires the
same evaluator to reject it as `SourceAcknowledgement` before the worker polls
again. It does not qualify the unsafe run.

Evidence is written under `target/real-cluster-chaos/` with the source revision,
manifest and evidence digests, Kafka and MinIO image versions, process IDs,
fault and acknowledgment observations, exact oracle, worker logs, and actual
Kafka offsets. The qualified path is `KafkaSource` to a transactional
`KafkaViewSink` on Apache Kafka 3.8.0 with MinIO
`RELEASE.2025-09-07T16-13-09Z`. This does not qualify PostgreSQL CDC, other
source or sink types, or source broker redelivery/idempotency behavior.

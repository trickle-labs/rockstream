# PostgreSQL CDC to Kafka recovery qualification

`postgres-cdc-kafka-minio-v1.json` fixes the transaction history and exact weighted-view and sink-record oracles for this scenario. The real-cluster test starts PostgreSQL, Kafka, and MinIO; kills the worker after M3 commits epoch 3 but before the logical slot advances; then verifies recovered slot progress and the final read-committed sink history. A test-only unsafe future-slot-ACK mutation must show the slot ahead of the durable source checkpoint.

Scope: one PostgreSQL table and logical slot, one shard, and `INT64`/`TEXT` rows. It does not qualify other PostgreSQL types, failover, physical power loss, or exactly-once behavior for unrelated connectors.

Run it with:

```sh
cargo test --locked -p rockstream-sim --features simulation,docker_tests --test real_postgres_cdc_chaos_tests postgres_cdc_real_process_ack_gap_recovers_to_kafka -- --exact --test-threads=1
```

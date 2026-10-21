# Files

- [Experimental Kafka view delivery and recovery](kafka-view-delivery.md) - How KafkaViewSink exports typed view snapshots and signed changes through bounded transactions, persists recovery state, and resolves uncertain Kafka delivery. Explains checkpoint coupling and the limits of tested exactly-once behavior.
- [Kafka and PostgreSQL CDC ingestion](source-ingestion.md) - How Kafka and PostgreSQL CDC share fenced source checkpoints while retaining different snapshot, offset, transaction, acknowledgment, and recovery contracts.

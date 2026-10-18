# Connector migration

The Iceberg, Delta, object-store, S3, and HTTP webhook connector frontends
were removed in v0.52.4. RockStream rejects their DDL and webhook ingress with
`RS-4017 connector.removed`.

| Removed surface | Replacement |
| --- | --- |
| S3 source | Use an external loader through pgwire or Kafka. |
| HTTP webhook source | Use an external HTTP-to-Kafka or HTTP-to-PostgreSQL adapter. |
| Iceberg, Delta, and object-store sink | Experimental bounded Kafka runtime view delivery; no automatic lakehouse export replacement. |
| Cold-tier configuration | No automatic Kafka-based lakehouse export; the runtime view adapter has explicit size limits. |

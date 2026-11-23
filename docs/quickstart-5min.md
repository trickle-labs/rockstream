# 5-Minute RockStream Quickstart

Connect RockStream to live PostgreSQL CDC (or Kafka), create a live
materialized view, mutate source data, restart RockStream, and verify fresh
durable results in under 5 minutes.

## Prerequisites

- Docker (`docker compose`)
- `psql` (PostgreSQL client) and `curl`

## One-Command Quickstart

```bash
./deploy/quickstart/quickstart.sh
```

To run against a locally compiled `rockstream` binary instead of pulling a
container image:

```bash
./deploy/quickstart/quickstart.sh --local-binary
```

To also include a local Kafka-compatible broker (Redpanda):

```bash
./deploy/quickstart/quickstart.sh --kafka
```

## What the Quickstart Verifies

1. Starts PostgreSQL (`wal_level=logical`) with seeded `customers` and `orders`
   tables and publication `rockstream_pub`, plus MinIO (S3-compatible storage)
   and RockStream (`--role=all`).
2. Waits for `http://127.0.0.1:9090/ready`.
3. Creates a live materialized view (`sales_by_region`).
4. Applies live `INSERT`, `UPDATE`, and `DELETE` mutations.
5. Restarts RockStream to verify durable state recovery.
6. Compares the exact sorted result rows of `sales_by_region` against the
   authoritative PostgreSQL query and exits non-zero on any discrepancy.

## Tear Down

```bash
./deploy/quickstart/quickstart.sh --down
```

## Next Steps: Kubernetes + S3 Deployment

For multi-node `control`, `worker`, stateless `gateway`, and optional
`compactor` deployments on S3-compatible object storage with HPA and
Prometheus/Grafana dashboards, see `deploy/helm/rockstream/` and run:

```bash
./scripts/smoke-test-k8s-helm.sh
```

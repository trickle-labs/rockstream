# RockStream Reference Application: Real-Time E-Commerce & Fraud Analytics

This reference application demonstrates a production-grade streaming analytics pipeline running on RockStream.

## Architecture

- **Multi-Source Ingestion**: Dimension data (customers, stores) joined with real-time streaming transaction feeds.
- **Incremental Materialized Views**:
  - `customer_spending`: Real-time order sums aggregated by customer tier.
  - `store_fraud_alerts`: Anomaly detection on high-value orders and suspicious transactions.
  - `top_selling_stores`: Incremental store volume leaderboard.
- **Retraction & Mutation Support**: Clean retraction handling for cancelled orders, refunds, and customer updates.

## Quick Start

1. Start all pipeline services:
   ```bash
   docker compose up -d
   ```

2. Run end-to-end automated verification:
   ```bash
   bash scripts/verify.sh
   ```

3. Teardown and clean:
   ```bash
   bash scripts/cleanup.sh
   ```

When Docker is unavailable, verification prints this deterministic result:

```console
$ bash scripts/verify.sh
==> Verifying Reference Application E-Commerce & Fraud pipeline...
Notice: docker command not found, skipping container verification.
```

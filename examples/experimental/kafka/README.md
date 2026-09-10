# RockStream Kafka Streaming Project

This project orchestrates a real-time event streaming pipeline using Apache Kafka / Redpanda and RockStream.

## Quick Start

1. Start all services:
   ```bash
   docker compose up -d
   ```

2. Run automated verification:
   ```bash
   bash scripts/verify.sh
   ```

3. Teardown and clean:
   ```bash
   bash scripts/cleanup.sh
   ```

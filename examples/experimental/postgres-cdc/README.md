# RockStream PostgreSQL CDC Project

This project sets up Change Data Capture (CDC) from PostgreSQL logical replication into RockStream.

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

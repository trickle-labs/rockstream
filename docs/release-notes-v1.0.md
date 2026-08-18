# RockStream v1.0 Release Notes

## Overview

RockStream v1.0 is a distributed incremental view maintenance (IVM) and stream-table storage system that provides real-time materialized views over high-throughput append logs and CDC streams, served directly to PostgreSQL clients over pgwire.

---

## Key Features

1. **Incremental View Maintenance (IVM)**:
   - Differential dataflow operator engine supporting joins, aggregates, window functions, and set operations.
   - Exact algebraic retraction and delta propagation with zero silent wrong answers.

2. **Durable Stream-Table Storage**:
   - SlateDB cloud object store integration (WAL replay, checkpointing, and compaction filters).
   - Bounded memory usage with explicit spilling and backpressure.

3. **PostgreSQL Wire Protocol Compatibility**:
   - Native PGWire frontend supporting standard PostgreSQL drivers, ORMs, and BI tools.
   - Built-in `SUBSCRIBE` CDC streaming extensions.

4. **Fault-Tolerant Distributed Architecture**:
   - Raft-based control plane, dynamic shard lease rebalancing, and automatic worker failure recovery.
   - Zero epoch loss during in-flight mutual TLS certificate rollover.

5. **Security & Supply Chain Integrity**:
   - Complete assessor readiness review with 0 open P0/P1 vulnerabilities.
   - SLSA v1.0 build provenance and SPDX 2.3 / CycloneDX 1.5 SBOM attestations.

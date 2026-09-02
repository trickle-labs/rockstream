# Zero-Downtime Rolling Upgrade & Migration Guide

This document outlines the authoritative sequencing, shard mobility semantics, and state compatibility rules for executing zero-downtime rolling upgrades across RockStream clusters.

---

## 1. Rolling Upgrade Sequencing

To prevent control split-brain and ensure continuous query availability, cluster components must be upgraded in the following strict order:

```
Step 1: Control Plane Nodes (1 by 1)
        └── Raft consensus maintains quorum while rolling leader/followers.
Step 2: Worker Nodes (1 by 1 with graceful drain)
        └── Worker sends SIGTERM -> flushes dirty epoch -> releases shard leases -> peer takes over -> old worker exits -> new worker starts.
Step 3: Gateway Nodes (Rolling Deployment)
        └── Gateway stops accepting new connections -> finishes in-flight queries -> closes -> new gateway starts behind load balancer.
```

---

## 2. Worker Graceful Drain & Lease Handover

When a worker receives `SIGTERM` during a Kubernetes `RollingUpdate` or systemd service restart:

1. **Readiness Drop**: The worker immediately transitions `/ready` to HTTP 503 `not_ready (draining)`.
2. **Epoch Flush**: The worker completes its active micro-batch epoch and flushes memtables and dirty write buffers to SlateDB storage.
3. **2PC Sink Commit**: In-flight two-phase commit sink transactions are finalized.
4. **Lease Release**: The worker sends an explicit `ReleaseLease` RPC to the Control Plane for all owned shards.
5. **Zero-Delay Takeover**: The Control Plane immediately reassigns the released shard leases to surviving or upgraded peer workers without waiting for the 5-second heartbeat timeout.
6. **Clean Termination**: The worker tears down network listeners and exits cleanly with code 0 within `shutdown_timeout_secs` (default 30s).

---

## 3. Gateway Connection Draining

1. **Traffic Shift**: Gateway readiness probe `/ready` drops to 503, prompting Kubernetes Ingress or load balancer to stop routing new TCP connections.
2. **Client Notice**: The Gateway broadcasts PostgreSQL Notice `57P01` (`admin_shutdown` / `RS-2056`) over existing PGWire sessions to advise clients to reconnect.
3. **In-Flight Query Drain**: Active SQL queries and streaming subscriptions complete within the graceful deadline before sockets are closed.

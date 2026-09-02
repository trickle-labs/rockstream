# RockStream Deployment Profiles Guide

RockStream supports four production-grade deployment profiles tailored to different environments and orchestration systems:

1. **OCI Container (`docker run`)**
2. **Docker Compose (Standalone & Distributed Topologies)**
3. **Linux Systemd Service Units**
4. **Kubernetes Manifests & Minimal Helm Chart**

---

## 1. OCI Container (`docker run`)

RockStream publishes multi-architecture images (`linux/amd64`, `linux/arm64`) to `ghcr.io/trickle-labs/rockstream`.

### Running Standalone Unprivileged Container

```bash
docker run -d \
  --name rockstream \
  --restart unless-stopped \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  -u 10001:10001 \
  -v rockstream_data:/data:rw \
  -p 5432:5432 \
  -p 9090:9090 \
  ghcr.io/trickle-labs/rockstream:0.59.22 \
  start \
  --role=all \
  --listen=0.0.0.0:5432 \
  --metrics-addr=0.0.0.0:9090 \
  --storage=/data
```

---

## 2. Docker Compose Profiles

Reference compose manifests are located in `deploy/compose/`:

- `deploy/compose/docker-compose.standalone.yaml`: Single-node RockStream instance with volume persistence and `/ready` healthcheck probe.
- `deploy/compose/docker-compose.distributed.yaml`: Multi-container topology with 1 Control Plane, 2 Workers, 1 Gateway, MinIO object store, and Redpanda broker.
- `deploy/compose/docker-compose.cdc.yaml`: PostgreSQL CDC source paired with RockStream CDC streaming engine.

### Starting Distributed Stack

```bash
docker compose -f deploy/compose/docker-compose.distributed.yaml up -d
```

---

## 3. Systemd Service Profiles

Systemd unit files are located in `deploy/systemd/`:

- `rockstream.service`: Standalone single-node daemon.
- `rockstream-gateway.service`: Stateless query gateway daemon.
- `rockstream-worker.service`: Stateful stream processing worker daemon.
- `rockstream-control.service`: Raft-backed control plane node.

### Systemd Security Directives

All unit files implement defense-in-depth Linux sandboxing:
- `User=rockstream`, `Group=rockstream` (unprivileged service account)
- `ProtectSystem=strict` (read-only OS filesystem)
- `ProtectHome=yes` (hides `/home` and `/root`)
- `PrivateTmp=yes` (isolated per-service `/tmp`)
- `NoNewPrivileges=yes` (prevents privilege escalation)
- `MemoryDenyWriteExecute=yes` (prevents executable memory mappings)
- `LimitNOFILE=65536` (high file descriptor limit for networking and SlateDB SST files)
- `WatchdogSec=30` (heartbeat integration)

---

## 4. Kubernetes & Minimal Helm Chart

Production Kubernetes manifests and a minimal Helm chart are provided in `deploy/kubernetes/` and `deploy/helm/rockstream/`.

### Architecture on Kubernetes

```
                    ┌────────────────────────┐
                    │    PGWire Client       │
                    │   (psql / BI / App)    │
                    └───────────┬────────────┘
                                │ Port 5432
                                ▼
                    ┌────────────────────────┐
                    │   Gateway Deployment   │
                    │  (Stateless, HPA 2..N) │
                    └───────────┬────────────┘
                                │ Query & Catalog RPC
                    ┌───────────┴────────────┐
                    ▼                        ▼
        ┌───────────────────────┐  ┌───────────────────────┐
        │  Control StatefulSet  │  │   Worker StatefulSet  │
        │  (3 Replicas / Raft)  │  │  (Stateful / PVCs)    │
        └───────────────────────┘  └───────────────────────┘
```

### Deploying via Helm

```bash
helm install rockstream deploy/helm/rockstream/ \
  --set cluster.id="prod-cluster" \
  --set worker.replicas=3 \
  --set gateway.replicas=2
```

### Probes & Lifecycle Integration

- **Startup Probe**: `GET http://<pod>:9090/ready` (polling until initial startup and state recovery complete).
- **Liveness Probe**: `GET http://<pod>:9090/live` (ensures process event loop is responsive).
- **Readiness Probe**: `GET http://<pod>:9090/ready` (dynamically drops traffic during starting, draining, or shard migrations).

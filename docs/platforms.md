# Platform & Environment Support Matrix

RockStream categorizes host architectures, operating systems, C libraries, filesystems, and external services into three authoritative classification tiers defined in `contracts/platform-matrix.toml`:

- **Supported**: Formally qualified in continuous integration and release qualification gates. Operates with zero diagnostic warnings.
- **Compatible, unverified**: Protocol-compatible or POSIX-compliant environments not continuously qualified in release gates. `rockstream doctor` and startup emit informational warnings (`RS-3025`) explaining the unverified status, but allow execution.
- **Unsupported**: Known incompatible, unsafe, or deprecated environments. Node startup and `rockstream doctor` fail fast and reject execution with fatal code `RS-3028` or `RS-3029`.

---

## 1. CPU Architecture Support

| Architecture | Tier | Notes & Status | Rejection Code |
|---|---|---|---|
| `x86_64` (AMD64) | **Supported** | x86-64-v2 and x86-64-v3 microarchitectures with SSE4.2 / AVX2. Primary qualification target. | — |
| `aarch64` (ARM64) | **Supported** | ARMv8.2-A+ (including Apple Silicon M-series and AWS Graviton 2/3/4). Primary qualification target. | — |
| `riscv64` | **Compatible, unverified** | 64-bit RISC-V with standard extensions. Protocol-compatible; unverified in release gates. | `RS-3025` (Warn) |
| `ppc64le` | **Compatible, unverified** | POWER 64-bit Little Endian. Protocol-compatible; unverified in release gates. | `RS-3025` (Warn) |
| `s390x` | **Compatible, unverified** | IBM System/390x 64-bit. Protocol-compatible; unverified in release gates. | `RS-3025` (Warn) |
| `x86` (i386/i686) | **Unsupported** | 32-bit architecture lacking 64-bit memory addressing and atomic CAS primitives. | `RS-3028` (Fatal) |
| `arm` (ARMv7/armhf) | **Unsupported** | 32-bit ARM architecture unsupported. | `RS-3028` (Fatal) |

---

## 2. Operating System & Linux Distribution Matrix

| OS / Distribution | Version / Kernel | Libc Runtime | Tier | Notes |
|---|---|---|---|---|
| **Debian** | 12 (Bookworm) | glibc 2.36 | **Supported** | Official OCI base image; primary release target. |
| **Ubuntu** | 22.04 / 24.04 LTS | glibc 2.35 / 2.39 | **Supported** | Continuous integration and qualification runner. |
| **RHEL / Rocky / Alma** | 9.x | glibc 2.34 | **Supported** | Enterprise Linux reference deployment. |
| **Alpine Linux** | 3.19+ | musl 1.2.4+ | **Supported** | Static and lightweight musl-based container distribution. |
| **Amazon Linux** | 2023 (AL2023) | glibc 2.34 | **Supported** | AWS cloud reference distribution. |
| **macOS** | 13+ (Ventura, Sonoma, Sequoia) | Apple Libc | **Supported** | Qualified for local development, testing, and evaluation. |
| **WSL2** | Ubuntu on Windows | glibc >= 2.31 | **Compatible, unverified** | Windows Subsystem for Linux 2 development evaluation. |
| **Other Linux** | Kernel >= 5.4 | glibc >= 2.31 / musl | **Compatible, unverified** | Any POSIX-compliant 64-bit Linux distribution. |
| **Legacy Linux** | Kernel < 5.4 | glibc < 2.31 | **Unsupported** | Lacks modern asynchronous I/O and futex features (`RS-3028`). |
| **Native Windows** | Windows 10/11, Server | MSVCRT | **Unsupported** | Lacks POSIX file locking and async io_uring execution (`RS-3028`). |

---

## 3. Storage & Object Store Compatibility

| Storage Backend | Protocol / Filesystem | Tier | Notes & Tested Scope |
|---|---|---|---|
| **Local Filesystem (LFS)** | ext4, xfs, btrfs, apfs, tmpfs | **Supported** | Primary local NVMe/SSD storage for SlateDB WAL and SST caching. |
| **AWS S3** | S3 Standard, Express One Zone | **Supported** | Fully qualified S3 object store backend with multi-part and range read support. |
| **MinIO** | MinIO (RELEASE.2023+) | **Supported** | Qualified S3-compatible test and private cloud harness. |
| **Cloudflare R2** | S3 API compatible | **Compatible, unverified** | Protocol-compatible S3 API; doctor emits informational notice. |
| **Google Cloud Storage (GCS)** | S3 Interop API | **Compatible, unverified** | Protocol-compatible S3 API; doctor emits informational notice. |
| **Azure Blob Storage** | S3 API Proxy | **Compatible, unverified** | Protocol-compatible S3 API; doctor emits informational notice. |
| **Ceph RADOS Gateway** | S3 API compatible | **Compatible, unverified** | Protocol-compatible S3 API; doctor emits informational notice. |
| **Network File System (NFS/SMB)** | NFSv3/v4, SMB | **Unsupported** | Lacks strict POSIX flock / O_DIRECT durability required for SlateDB WAL (`RS-3028`). |

---

## 4. Database & Event Broker Compatibility

| Backend Service | Qualified Versions | Tier | Status / Behavior |
|---|---|---|---|
| **PostgreSQL** | 14, 15, 16, 17, 18 (`postgres:18.0` pinned reference) | **Supported** | Standard `pgoutput` logical replication CDC source. |
| **PostgreSQL** | 12, 13 | **Compatible, unverified** | Supports logical decoding; older than primary release target (`RS-3025`). |
| **PostgreSQL Cloud** | Neon, Supabase, AWS Aurora Postgres | **Compatible, unverified** | Compatible PostgreSQL wire and replication features. |
| **PostgreSQL Legacy** | < 12 | **Unsupported** | Lacks required CDC streaming primitives (`RS-3029`). |
| **Apache Kafka** | 3.4 - 3.9 (KRaft & ZooKeeper) | **Supported** | Primary event streaming broker. |
| **Redpanda** | 23.x / 24.x | **Supported** | Qualified Kafka API compatibility. |
| **Apache Kafka** | 2.8 - 3.3 | **Compatible, unverified** | Early KRaft; compatible wire protocol (`RS-3025`). |
| **Kafka Legacy** | < 2.8 / non-Kafka brokers | **Unsupported** | Incompatible partition and metadata semantics (`RS-3029`). |

---

## 5. Security & Container Standards

All container packaging and deployment profiles enforce:
- **Non-Root Execution**: Default unprivileged service user `rockstream` (UID 10001, GID 10001).
- **Read-Only Root Filesystem**: Container root filesystem is mounted read-only (`readOnlyRootFilesystem: true` / `--read-only`). All writable state is restricted to dedicated volume mount `/data` and ephemeral `/tmp`.
- **Pod Security Standards**: Manifests conform to Kubernetes `restricted` Pod Security Standards with all capabilities dropped (`drop: ["ALL"]`) and `RuntimeDefault` seccomp profile.
- **Port Standardization**:
  - `5432`: PostgreSQL Wire Protocol SQL Gateway
  - `9090`: HTTP Management (`/live`, `/ready`, `/health`) & Prometheus Metrics (`/metrics`)
  - `9100`: Worker DataPlane & Exchange RPC
  - `9200`: Control Plane Raft Consensus RPC

# Management API reference

The management API exposes typed status reads and long-running administrative operations. The protocol source is [`management.proto`](../../crates/rockstream-management-proto/proto/management/v1/management.proto).

**Qualification status:** v0.66 is signed off. The v0.73 mTLS principal, scoped-grant, and support-artifact paths are implemented in part and still require release qualification. `CreateBackup` runs only when the server has an attached shard store. Health remains `unknown` until authoritative process-health telemetry is available.

## Endpoint and protocol

`control.management_addr` selects the server address. Its default is `127.0.0.1:9201`. The CLI uses that address unless you pass `--management`.

The service uses tonic over HTTP/2 with mutual TLS. The verified CLI client certificate fingerprint must map to an enabled durable principal. Each RPC checks that principal's current persisted grants; a CLI role flag does not grant authority. Management calls fail closed if the security catalog or required audit storage is unavailable. See [security.md](../security.md) for certificate setup and policy.

The protocol version is `1`. Every request carries `protocol_version`. The server returns `FAILED_PRECONDITION` for an unsupported version.

## Methods

| Method | Behavior |
|---|---|
| `GetClusterStatus` | Returns the observed cluster state, node snapshot, operation counts, management request fill, and ACK waiter fill. |
| `ListNodes` | Returns a page of registered nodes. |
| `GetNode` | Returns one registered node or `NOT_FOUND`. |
| `ListShards` | Returns a page of current shard leases. |
| `GetShard` | Returns one current shard lease or `NOT_FOUND`. |
| `ListOperations` | Returns a page of retained operation records. |
| `GetOperation` | Returns one operation or `NOT_FOUND`. |
| `ListSecurityPrincipals` | Returns safe principal, fixed-preset, scoped-grant, and effective-permission data to a principal authorized for grant management. Login credentials are omitted. |
| `SetSecurityPrincipalGrants` | Replaces an existing principal's grants after checking the reviewed target and grantor revisions. Persists the operation before applying the change and commits the grant record with its audit event. |
| `GetConfigSummary` | Returns effective configuration values and redaction flags. |
| `GetCapabilities` | Returns the methods attached to this server. |
| `GetHealth` | Returns `unknown` until the process registers authoritative health telemetry. |
| `DrainWorker` | Persists an idempotent operation request, then drains a worker asynchronously. |
| `MigrateShard` | Persists an idempotent request, flushes and closes the donor shard, transfers its lease, and waits for the recipient to open it. Before donor handoff, the v0.66 executor rejects a shard with a registered active workload deployment. |
| `CreateBackup` | Persists an idempotent operation, requests a durable checkpoint from every shard owner, exports checkpoint-pinned shard data and control state while excluding the audit and management-operation logs and live worker topology, and reports success only after validating its terminal marker. Workers rebuild their topology records when they register. The method appears in capabilities only when a backup source store is attached. |
| `CancelOperation` | Cancels a pending operation or a migration before lease transfer. During donor handoff, the executor waits for the worker acknowledgement and reopens the donor before returning to its caller. It returns `FAILED_PRECONDITION` after lease transfer starts. |
| `GenerateSupportArtifact` | Creates a sanitized cluster- or node-scoped support artifact and binds its opaque UUID to the authenticated principal, scope, and client idempotency key. |
| `DownloadSupportArtifact` | Streams an artifact to its creating principal when current download and source-scope grants still allow access. The UUID is opaque and never names a server filesystem path. |

The control service attaches drain and migration executors. It attaches the backup executor only when it can read the authoritative shard store. Embedded `--role all` supports one local worker. Multi-worker backups require every shard owner to use the same configured shared object store.

The CLI sends `admin backup create` to `CreateBackup` only when you pass `--management`. Without that flag, it writes the legacy local manifest used by `admin backup inspect` and `admin backup verify`.

## CLI examples

These commands target a local management endpoint. Replace `<operation-id>` with an ID returned by an operation command.

```sh
rockstream --output json --management 127.0.0.1:9201 status
rockstream --output json --management 127.0.0.1:9201 health
rockstream --output json --management 127.0.0.1:9201 capabilities
rockstream --output json --management 127.0.0.1:9201 cluster workers list
rockstream --output json --management 127.0.0.1:9201 cluster workers status 1
rockstream --output json --management 127.0.0.1:9201 shard list
rockstream --output json --management 127.0.0.1:9201 shard show 0
rockstream --output json --management 127.0.0.1:9201 config summary
rockstream --output json --management 127.0.0.1:9201 admin operation list
rockstream --output json --management 127.0.0.1:9201 admin operation show <operation-id>
rockstream --management 127.0.0.1:9201 support artifact create --idempotency-key <client-generated-uuid>
rockstream --management 127.0.0.1:9201 support artifact download <opaque-id> --out support.json
```

Keep the client-generated UUID before sending the create request and reuse it for retries. If it is omitted, the CLI prints a generated UUID to stderr before the request. The response returns the same opaque ID. Artifacts contain only sanitized operational metadata: no business rows, SQL literals, connector payloads, secret values, keys, or tokens. Each artifact is at most 10 MiB, remains downloadable for at most 10 minutes, and is bound to its creator and source scope. A durable conditional-write ledger limits shared artifact storage to 100 MiB per deployment and does not evict unexpired artifacts. Artifacts survive service restart; startup removes expired data before listeners open, and background cleanup removes newly expired data within 60 seconds. A backend without atomic conditional writes disables support-artifact generation and download while leaving other management operations available. After expiry or deletion, replaying the same key reports that the artifact is unavailable; use a new key to request a new artifact.

If the create response is lost, retry `support artifact create` with the same UUID or use the authorized `admin operation show <uuid>` lookup. The operation UUID is known before the mutation starts, and an unavailable or expired artifact is never silently regenerated under the old key.

`CreateBackup` accepts a local path, a `file://` path, or an `s3://bucket/prefix` destination. It requires an idle cluster with no active workload deployments or source writes. The operation record stores the destination and checkpoint phase. The server stores its checkpoint manifest in the source store and writes the export under `checkpoint-exports/management-<operation_id>`. A missing worker or temporary storage error leaves the operation waiting. An invalid destination or checkpoint integrity error leaves it failed.

The management backup format is a committed checkpoint generation with inventory records and a terminal `commit` marker. It is separate from the local `manifest.json` format used by the legacy `admin backup inspect` and `admin backup verify` commands.

A successful `DrainWorker` result requires a matching recipient-open acknowledgement for every moved shard. A drain remains active while a recipient is unavailable or has not acknowledged its lease.

## Response fields

`observed_at` uses RFC 3339 UTC. `source_version` identifies the state source or record version. Compound status fields do not share a transactional snapshot across the catalog and lease store.

`request_fill` and `request_capacity` report the management request semaphore. `ack_waiter_fill` and `ack_waiter_capacity` report registered migration and backup worker acknowledgements. Each executor admits at most 64 ACK waiters. The reported ACK waiter capacity is 64 for each attached executor.

`Node` includes its registered address, role, capacity headroom, host and zone, health, registration time, and lifecycle state. Missing process-health telemetry remains `unknown`.

`Shard` reports its owner and lease token. `key_range_known` is false and `key_range` is empty because the current lease store does not publish key ranges.

`Operation` reports an identifier, kind, state, start and update times, progress, phase, error code, next steps, source record version, and (when authorized) a complete `result_json`. Operation listings omit result contents. For grant changes, the grantor or a current grant manager can use `GetOperation` to recover the safe committed grant result. The states are `Pending`, `Running`, `Waiting`, `Succeeded`, `Failed`, and `Cancelled`. The server persists times as Unix milliseconds and renders them as RFC 3339 UTC.

`GetConfigSummary` marks each redacted value with `redacted=true`. It does not return the original secret.

## Pagination and limits

`page_size=0` selects the default of 50. The maximum page size is 100. Page tokens are decimal offsets; malformed or out-of-range tokens return `INVALID_ARGUMENT`.

The server admits at most 64 concurrent management requests and 64 ACK waiters for each attached migration or backup executor. It limits gRPC messages to 1 MiB and support-artifact chunks to 64 KiB. The operation store retains at most 1,000 active records and 10,000 total records. Terminal records and idempotency bindings remain for 90 days. Each operation accepts at most 64 state transitions. Operation record version 4 adds durable mutation results and audit outbox state while migrating versions 1 through 3.

## Idempotency and cancellation

`DrainWorker`, `MigrateShard`, and `CreateBackup` accept an `idempotency_key`. `SetSecurityPrincipalGrants` requires a canonical client-generated UUID; the same UUID is its operation ID. The server binds it to the protocol version, authenticated grantor, deployment context, target, reviewed revisions, and requested grants before applying the change. An exact retry returns the original operation and result. A changed request under the same key returns `ALREADY_EXISTS` without changing policy. An expired key returns `FAILED_PRECONDITION`. The authorized `GetOperation` RPC reconciles a lost response; submit the original UUID and never resubmit the mutation with a new key.

The valid status transitions are:

| Current state | Next states |
|---|---|
| `Pending` | `Running`, `Waiting`, `Failed`, `Cancelled` |
| `Running` | `Waiting`, `Succeeded`, `Failed`, `Cancelled` |
| `Waiting` | `Running`, `Failed`, `Cancelled` |
| `Succeeded` | None |
| `Failed` | None |
| `Cancelled` | None |

`Running` and `Waiting` can also receive progress metadata updates without changing state. Terminal states cannot change.

The server durably records `Pending` before it returns an operation ID. A transition uses a create-only record keyed by the previous record hash. This prevents two store instances from advancing the same operation state at once. A transition conflict returns `FAILED_PRECONDITION`.

Cancellation is allowed before lease transfer. A migration cancelled during donor handoff waits for the acknowledgement and reopens the donor under the unchanged lease before stopping. The operation record reports `cancelled_before_lease_transfer`. The server rejects cancellation after lease transfer starts. A rejected cancellation leaves the operation record unchanged.

## Current limitations

Management RPC errors retain their gRPC status. The CLI currently maps management status errors to `RS-0003`, so automation cannot distinguish every gRPC failure through the CLI output.

`SetSecurityPrincipalGrants` updates grants on an existing principal. Principal creation, deletion, login-method changes, password changes, and their bootstrap workflows have separate interfaces and are not implemented by this RPC. The CLI's local identity flags do not authenticate a caller to the management service.

The server has no authoritative process-health telemetry, so `GetHealth` returns `unknown`. This is the qualified behavior for v0.66; the limitation remains explicit until health telemetry is implemented.

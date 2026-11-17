# RockStream Security Architecture & Internal mTLS (v0.55)

## Secrets and envelope encryption (v0.55.1)

`CREATE SECRET`, `ALTER SECRET`, `DROP SECRET`, and `SHOW SECRETS` store only
AES-256-GCM envelopes under `catalog/secrets/`. The control plane uses
`RS_SECRET_KEK` for wrapping keys; if it is unset or empty, secret operations
fail closed without a built-in local key. `SHOW SECRETS` returns metadata only.

Secret SQL is authorized through the durable principal policy. `CREATE SECRET`,
`ALTER SECRET`, and `DROP SECRET` require `secret.manage` for the selected
namespace and secret; `SHOW SECRETS` requires `secret.metadata.read` for the
namespace. The Administrator preset grants those secret administration actions
within its scope. The Operator preset grants none of them, and neither preset
grants `secret.use`. A gateway running without a durable authenticated policy
denies secret administration. Secret input remains write-only, and no SQL GET
returns an existing secret value.

Applications can inject an `AwsKmsKekProvider::new(key_arn, region)` into a
`SecretStore` and supply that store through `ControlService::with_secret_store`.
Supplying an ARN alone does not select this provider in the default services.
The provider sends SigV4-signed KMS `Encrypt` and `Decrypt` requests with the
configured `KeyId` on both operations. The control-plane identity needs
`kms:Encrypt` and `kms:Decrypt` permission on that symmetric KMS key. DEKs are
generated locally; only their wrapping and unwrapping use KMS.

AWS credential discovery reuses the storage layer's refreshable credential
chain: environment credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
and optional `AWS_SESSION_TOKEN`), web identity, ECS task credentials, or EC2
instance credentials. Shared AWS profile files are not loaded. Applications
can supply another credential provider with `with_credentials`.
`with_endpoint` supports HTTPS KMS-compatible endpoints; plaintext HTTP is
restricted to numeric loopback addresses for local fixtures. Redirects are
disabled, requests time out after ten seconds, and failures return `RS-2422`
without request/response bodies or local encryption fallback.

**Compatibility:** envelopes created by the former ARN/region-derived local
placeholder are not KMS ciphertext and will fail to decrypt with this provider.
Any such data needs a controlled offline migration before upgrading. That
placeholder provided no KMS authorization boundary; this implementation never
falls back to decrypting its envelopes.

Local KMS protocol fixtures verify signed requests, denial, unavailability,
wrong-key handling, and env/KMS rewrapping. They do not qualify AWS enforcement.
The ignored `real_kms_enforces_key_and_principal_permissions` test requires a
scoped AWS fixture: `ROCKSTREAM_TEST_KMS_KEY_ARN`,
`ROCKSTREAM_TEST_KMS_WRONG_KEY_ARN` (a different usable key),
`ROCKSTREAM_TEST_KMS_REGION`, and a denied principal's
`ROCKSTREAM_TEST_KMS_DENIED_ACCESS_KEY_ID`,
`ROCKSTREAM_TEST_KMS_DENIED_SECRET_ACCESS_KEY`, and optional
`ROCKSTREAM_TEST_KMS_DENIED_SESSION_TOKEN`. The ordinary AWS credential chain
must identify an authorized principal. Run explicitly with
`cargo test -p rockstream-control --test aws_kms_tests real_kms_enforces_key_and_principal_permissions -- --ignored --exact`.

`SecretStore::rotate_kek` stages all rewrapped envelopes before publishing one
atomic SlateDB batch (or replacing the in-memory catalog). Secret reads and
mutations wait for rotation; once the batch is submitted, cancelling the caller
does not cancel its commit. Success waits for durable storage before retiring
the store's old provider. A provider or decode failure during staging leaves
every stored envelope unchanged.

With durable storage, rotation also requires the durable security audit
catalog. The rewrap batch carries a structured audit outbox record, and success
waits for that record to reach the catalog. The method's actor label is recorded
as unverified because this internal API does not establish authentication; it
has no client request identity, and its operation identity is recorded as
unknown. Pending outbox records are flushed before gateway listeners open.

Keep both KEKs available until rotation has returned success and the replacement
provider has been configured for restart. After an interruption or storage error,
the durable batch may contain either generation. Reopen the catalog using
`Arc::new(RetainedKekProvider::new(replacement.clone(), previous.clone()))` from
`rockstream_control::kek` as the `SecretStore::new` provider, then call
`rotate_kek(replacement, actor)` again. This also recovers mixed generations left
by older releases. The retained provider encrypts with its first provider and
tries both providers for decryption; it never writes key material to the catalog.
Only retire the previous key after successful recovery and durable rotation.
Use one shared `SecretStore` for a catalog; independent instances must not mutate
that catalog during rotation. Rotation stages memory proportional to the catalog.

Workers receive short-lived tokens over authenticated mTLS. The worker token
request path still lacks the required signed actor assertion and current actor
policy check, so delegated worker secret resolution remains unqualified.
Decrypted payloads remain in worker process memory and are refreshed at the
next epoch after an `ALTER SECRET`; they are never written to worker storage.

Connector SQL continues to accept `secret = '<name>'`; newly persisted source
and sink metadata uses the canonical `secret://<name>` reference. A role preset
does not imply secret use: authorization requires an explicit grant scoped to
the secret and consumer principal, and missing identity or durable policy
fails closed. Gateway source resolution reads the consumer's current durable
policy before decrypting the value. Legacy source records without a persisted
consumer identity are denied at resolution; an authorized migration route
remains to be qualified.

This document describes the security model, mutual TLS (mTLS) transport encryption, identity verification, and dynamic certificate rotation in RockStream.

For full assessor findings, verification records, and machine-readable audit metadata, see [Security Report](file:///Users/grove/projects/rockstream/docs/security-report.md) and [Security Assessment Metadata](file:///Users/grove/projects/rockstream/docs/security-assessment.json).


---

## Overview

### Gateway client authentication status

The gateway supports unauthenticated `off`, RS256 OIDC bearer authentication,
and client-certificate mTLS. OIDC requires `[auth].oidc_issuer`,
`[auth].oidc_audience`, `[auth].oidc_jwks_url`, and gateway TLS certificate
and key paths. Clients may supply the bearer token in the PostgreSQL password
response or the `Authorization` startup parameter; both require an encrypted
TLS connection. The gateway fetches and validates JWKS before opening the
listener and rejects requests when token verification fails. The verified `(issuer, subject)`
must map to an enabled durable principal before the gateway creates an
application session. Gateway mTLS requires `gateway.tls.cert_path`, `key_path`,
and `ca_cert_path`; a CA-verified client certificate must map by SHA-256
fingerprint to an enabled durable principal. Its common name alone grants no
authority, and an unmapped certificate is rejected before application session
creation. Internal node mTLS described below remains a separate transport
boundary. SCRAM-SHA-256 requires a recovered durable principal store with an
enabled SCRAM login; startup rejects an unavailable or empty store. MD5 is not
a release authentication mode. Enabling these startup paths does not satisfy
the v0.73 ten-case release qualification gate by itself.

Starting in **v0.55**, RockStream enforces mutual TLS (mTLS) with cryptographically verified node identity across all internal network transport boundaries:

The control-worker TCP listener used for registration, heartbeats, and shard leases requires internal mTLS. Control, worker, and embedded `all` roles need a certificate, matching private key, trusted CA roots, and `client_auth_required = true`; invalid or incomplete material fails startup before the listener opens.

For local development only, `--allow-insecure-internal-dev` explicitly permits plaintext control traffic when the control listener or worker target uses a literal loopback socket address. The option rejects non-loopback addresses and must not be used for production or release qualification.

Worker shuffle and native management APIs are separate transport boundaries. A configured worker shuffle endpoint requires certificate, key, CA roots, and mutual TLS; plaintext servers and clients are limited to literal loopback addresses. Shuffle clients verify the advertised worker ID against the peer certificate's `worker-<id>` DNS name, and the server rejects RPCs from CA-verified certificates whose parsed node role is not `Worker` before reading frames. This is partial enforcement: the server does not yet bind the peer certificate to a registered worker ID or shard assignment, role denials are only transiently logged, and the rejected RPC does not close its underlying HTTP/2 connection. Keep worker shuffle qualification blocked until durable denial audit, connection rejection, and assignment binding are demonstrated. Native management remains a separate boundary and must be qualified under its owning security work before production use.

---

## Identity Verification & Certificate Schema

Every certificate in the cluster embeds a verified `NodeIdentity` encoded in the Common Name (CN) and Subject Alternative Name (SAN):

| Node Role | CN / SAN Schema | Description |
|---|---|---|
| **Control** | `control:<node-id>` | Control-plane leader/follower nodes |
| **Worker** | `worker:<worker-id>` | Stateful/stateless data execution nodes |
| **CLI** | `cli:<username>` | Administrative CLI operator sessions |

### Verification Invariants
- **Identity Enforcement**: During handshake, the receiver extracts the leaf certificate's CN/SAN. If a connecting node presents a valid certificate whose embedded role or ID does not match its advertised wire identity, the handshake is rejected immediately with error `RS-2412` (`IdentityMismatch`).
- **CA Chain Validation**: Every peer must chain to a configured CA root. Untrusted, malformed, or expired certificates are rejected with `RS-2411` (`UntrustedCa` / `ExpiredCert`).
- **Mandatory Authentication**: Unauthenticated/plaintext connections are refused with `RS-2410` (`InternalMtlsRequired`), except when the explicit loopback-only development option is enabled.
- **Role Permissions**: Node roles identify transport peers; they do not grant user or administrator permissions. Authenticated worker-ID claims are bound to the registered worker, and execution progress and fence checks are bound to the assigned shard. User-facing service operations still require the separate current actor policy.
- **Worker Connection Binding**: On authenticated mTLS connections, messages carrying a worker ID must match both the certificate and the registered connection. A mismatch returns `RS-2401`, is durably audited, and has no protected effect; the authenticated connection remains available after the denial. Execution progress and write-fence checks also require the connection's worker to own the assigned shard or lease.

---

## Dynamic Certificate Rotation Under Load

RockStream features **zero-downtime in-flight certificate rotation** via `TlsCertificateReloader`.

### Key Capabilities
- **Zero Epoch Loss**: Dynamic reload does not drop existing active TCP connections or gRPC streaming channels.
- **Dual-Generation CA Trust**: During CA rollover windows, nodes accept certificates signed by both Generation N and Generation N+1 CAs.
- **Atomic Credential Swapping**: Active certificates and private keys are updated in memory without restarting any cluster daemon.

### Rotation Procedure

1. **Generate Generation 2 CA & Certificates**:
   ```bash
   # Generate new CA and leaf certificates for control, workers, and CLI
   openssl req -new -x509 -days 365 -key ca-v2.key -out ca-v2.crt
   ```

2. **Deploy Dual-CA Bundle to All Nodes**:
   Concatenate old and new CA certificates into `cluster-ca-bundle.pem` and distribute across the cluster.

3. **Reload Certificates In-Flight**:
   Control nodes and workers detect updated certificate files on disk or reload credentials via the control API / `ControlServiceHandle::reload_tls`.

4. **Retire Old CA (Post-Grace Window)**:
   Once all nodes have transitioned to Generation 2 leaf certificates, remove Generation 1 CA from the trust bundle.

---

## Error Codes Reference

| Error Code | Identifier | Description | Resolution |
|---|---|---|---|
| `RS-2410` | `InternalMtlsRequired` | Connection attempted over plaintext or missing client certificate | Configure `--tls-cert-path`, `--tls-key-path`, and `--tls-ca-cert-path` |
| `RS-2411` | `InternalMtlsUntrusted` | Peer certificate expired, self-signed, or untrusted CA | Verify certificate validity and ensure CA root matches cluster CA |
| `RS-2412` | `InternalMtlsIdentityMismatch` | Peer advertised node ID does not match certificate CN | Ensure certificate CN matches the node's assigned identity |
| `RS-2413` | `InternalMtlsRoleUnauthorized` | Node role in certificate lacks permission for requested action | Verify node role in certificate (e.g. worker vs control vs cli) |

---

## Audit Trail

When configured, the control service writes mTLS and CLI identity events to a structured JSONL audit log:
- `security.internal_mtls_denied`: Handshake refusal with peer address, error code (`RS-2410`/`RS-2411`), and reason.
- `cli.authenticated`: Successful administrative connection with operator CN identity.
- `worker.registered`: Authenticated worker join with verified node identity.

Pre-authentication handshake and certificate-extraction failures record actor
`unknown`; the listener records only the observed peer address. A registration
whose requested worker ID conflicts with a CA-verified certificate records the
certificate identity as the actor and rejects the connection before topology
registration. If the required audit log is unavailable or an append fails, the
connection is rejected and the server reports the audit failure.

`SHOW SECURITY AUDIT` reads the authoritative durable catalog history, separate
from the JSONL operational audit log above. Retain records for 90 days under the
1 GiB deployment-wide cap; do not evict unexpired records to make room. If a
required append is full or unavailable, reject the related mutation and expose
degraded health. Stop admitting new authenticated sessions when their
authentication event cannot be stored, and close an existing session when its
required denial event cannot be stored.

Authenticated PGWire clients with the current `security.audit.read` permission
can read retained security records with `SHOW SECURITY AUDIT [FROM
<timestamp_ms>] [TO <timestamp_ms>] [AFTER <cursor>] [LIMIT <rows>]`. Time bounds
are inclusive epoch milliseconds. Rows are ordered by timestamp and event ID;
the cursor returned on each row is `<timestamp_ms>:<unpadded base64url UTF-8 event_id>`
and `AFTER` excludes that row. Pages default to 100 rows and are limited to 500.
The gateway checks the requested time-range grant on every page and durably
audits denials while preserving an authenticated session. The result includes
explicit verified, unverified, unknown, and not-applicable field states.
Pages use live keyset ordering rather than a multi-request snapshot. Records
committed during pagination may appear when their key sorts after the cursor;
restart from the first page to include records that sort at or before it. If
the policy revision changes between authorization and page read, return a
retryable conflict without disclosing records.

## First administrator preprovisioning

Trusted offline deployment tooling must create the first interactive administrator before application listeners open. Use `DurableCatalogStore::preprovision_first_administrator` against the deployment's durable catalog with a unique operation ID that remains stable across retries. The principal and its grant-change audit event commit together; the audit actor is `unknown` because bootstrap has no authenticated application session. The operation rejects a nonempty security catalog, and a retry verifies the committed principal and audit record. It is not a public network operation. Catalog mutations also reject removal, disablement, or downgrade of the last active administrator.

# RockStream Security Architecture & Internal mTLS (v0.55)

## Secrets and envelope encryption (v0.55.1)

`CREATE SECRET`, `ALTER SECRET`, `DROP SECRET`, and `SHOW SECRETS` store only
AES-256-GCM envelopes under `catalog/secrets/`. The control plane uses
`RS_SECRET_KEK` by default; AWS KMS-compatible provider wiring is available
for deployments that supply a KMS ARN. `SHOW SECRETS` returns metadata only.

Workers receive short-lived tokens over authenticated mTLS. Decrypted payloads
remain in the worker process memory and are refreshed at the next epoch after
an `ALTER SECRET`; they are never written to worker storage.

This document describes the security model, mutual TLS (mTLS) transport encryption, identity verification, and dynamic certificate rotation in RockStream.

For full assessor findings, verification records, and machine-readable audit metadata, see [Security Report](file:///Users/grove/projects/rockstream/docs/security-report.md) and [Security Assessment Metadata](file:///Users/grove/projects/rockstream/docs/security-assessment.json).


---

## Overview

Starting in **v0.55**, RockStream enforces mutual TLS (mTLS) with cryptographically verified node identity across all internal network transport boundaries:

1. **Worker ↔ Control-Plane**: TCP control channel for worker registration, heartbeats, and shard lease acquisition.
2. **Worker ↔ Worker Shuffle Exchange**: gRPC Flight data transport for high-throughput shuffle exchanges.
3. **CLI ↔ Control-Plane**: Administrative inspection and cluster control commands.

All internal TLS handshakes require mutual authentication (`client_auth_required = true`) using X.509 certificates issued by a trusted cluster Certificate Authority (CA).

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
- **Mandatory Authentication**: Unauthenticated/plaintext connections to an mTLS-enabled node are refused with `RS-2410` (`InternalMtlsRequired`).
- **Role Permissions**: Only authenticated nodes with `NodeRole::Worker` can receive shard leases; only `NodeRole::Cli` or `NodeRole::Control` can execute administrative operations.

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

Every mTLS rejection and successful CLI session writes a structured JSONL audit entry:
- `security.internal_mtls_denied`: Handshake refusal with peer address, error code (`RS-2410`/`RS-2411`), and reason.
- `cli.authenticated`: Successful administrative connection with operator CN identity.
- `worker.registered`: Authenticated worker join with verified node identity.

# RockStream threat model

This is the machine-checked trust-boundary model for v0.55.2. Each row names
the protected assets, the trust transition, the enforcing control, the exact
refusal behavior where one exists, and one executable proof.

| Boundary | Assets | Trust transition | Control | RS behavior | Proof |
|---|---|---|---|---|---|
| Client to pgwire gateway | Credentials, SQL session, catalog data | Untrusted client to authenticated gateway session | SCRAM, MD5, OIDC, or mTLS authenticates the session before SQL dispatch | Authentication failures return RS-2400; TLS configuration failures return RS-2403 | `crates/rockstream-gateway/tests/auth_scram_tests.rs::test_scram_wrong_password` |
| Pgwire DDL/DML authorization | Catalog, pipeline state, write buffer, audit log | Authenticated session to state-changing SQL executor | AclStore role checks and audited denial before mutation | SQLSTATE 42501 with RS-2401; state is unchanged | `crates/rockstream-gateway/tests/auth_proof_tests.rs::proof_rbac_denies_cross_namespace_access` |
| Operator CLI to control plane | Cluster state, checkpoints, support artifacts | CLI transport identity to control-plane mutator | Authenticated transport identity and the shared mutating-command policy | Unauthorized mutation returns RS-2401 and an audit denial | `crates/rockstream-cli/tests/cli_mutating_commands_tests.rs::test_cli_unauthorized_identity_refused_and_audited_exhaustive` |
| Worker to control plane | Worker admission, leases, secrets, topology | Worker connection to control-plane worker protocol | Internal mTLS certificate validation and certificate-to-worker identity match | Handshake or identity refusal is audited with RS-2410, RS-2411, or RS-2412 | `crates/rockstream-control/tests/control_worker_mtls_tests.rs::test_control_worker_mtls_missing_cert_rejected` |
| Worker to worker shuffle | Shuffle data, peer identity, transport integrity | Worker peer to worker peer | gRPC mTLS requires a trusted peer certificate | Unauthenticated or untrusted peers are refused before data exchange | `crates/rockstream-runtime/tests/shuffle_mtls_tests.rs::test_worker_shuffle_grpc_mtls_unauthenticated_rejected` |
| TLS rollover | In-flight sessions, trust roots, worker availability | Existing trust generation to dual-generation trust during rotation | Dual-generation trust and in-flight reload without restart | Invalid peers remain refused; valid traffic has no loss or restart | `crates/rockstream-control/tests/internal_mtls_tests.rs::test_certificate_rotation_3_worker_cluster_under_load_zero_loss_zero_restart` |
| Secret storage and connector credential resolution | Secret plaintext, encrypted metadata, short-lived tokens | Secret store to worker connector memory and audit artifacts | Envelope encryption, identity-bound token resolution, expiry removal, and literal redaction | Invalid or expired tokens are rejected and plaintext is absent from artifacts | `crates/rockstream-control/tests/secrets_redaction_tests.rs::secret_literal_is_absent_from_audit_and_metadata_artifacts` |
| SQL injection and malformed ingress | SQL parser state, gateway process, decoder memory | Arbitrary network bytes to parser and protocol decoder | Per-statement dispatch plus fuzz and property replay of live decoders | Invalid input returns an error or closes safely; it cannot bypass policy or panic | `crates/rockstream-gateway/tests/protocol_fuzzer_tests.rs::protocol_fuzzer_no_panics_no_hangs` |
| Dependency supply chain | Build inputs, shipped binaries, advisory status | External crate registry to CI build graph | cargo audit, cargo deny, reviewed exception records, and Dependabot updates | Vulnerable or expired-exception inputs fail the dependency gate | `scripts/check-dependency-audit.test.sh` |

The commissioned independent review has no report artifact yet; any received
P0/P1 finding is added here with its issue identifier, fix, and regression
proof before sign-off.

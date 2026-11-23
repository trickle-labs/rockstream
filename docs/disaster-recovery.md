# Disaster recovery

## Current availability

The v0.73 CLI does not currently expose authenticated checkpoint inspection,
export, or restore operations. Its management protocol has no checkpoint RPC;
the old local CLI path trusted a caller-selected `--identity-role`, which does
not establish a durable principal or grant. The binary now fails closed with
`RS-2400` and does not access checkpoint storage. Do not use the historical
local command path from the v0.56.1 drill as a current recovery procedure.
An authenticated management operation is required before this runbook can be used for v0.73.

## Historical v0.56.1 drill

The measured local-filesystem drill exported checkpoint 56 and restored it with
the then-current local CLI.

- Measured RPO: 0 committed checkpoints.
- Measured RTO: 0.42 seconds from restore invocation to published bootstrap pointer.

These historical measurements do not qualify the current authenticated CLI.
Refresh this runbook after an authenticated checkpoint endpoint and its release-
process recovery matrix are implemented.

# Disaster recovery

Use a checkpoint export stored in a separate bucket, account, or region. Do
not copy the live RockStream object-store prefix.

## Export schedule

Run after each required recovery point:

```console
rockstream --storage-dir "$ROCKSTREAM_STORAGE" --identity-role admin checkpoint export --destination "$DR_EXPORT_URL"
```

Retain exports according to the workload's recovery policy and verify the
reported checkpoint, object count, byte count, and `SUCCESS` status.

## Full-region-loss restore

Provision an empty target bucket or directory, then run:

```console
rockstream --storage-dir "$ROCKSTREAM_AUDIT_DIR" --identity-role admin checkpoint restore --source "$DR_EXPORT_URL" --storage "$FRESH_STORAGE_URL" --yes
```

Start the single `rockstream` binary against `$FRESH_STORAGE_URL`. Verify every
materialized view with complete sorted-row comparisons and confirm the restored
checkpoint epoch, catalog, topology, leases, connector offsets, and source
resume positions.

Never reuse a target containing another active generation. `RS-5035` means the
export is incomplete, malformed, truncated, or inconsistent; leave the target
offline, correct object-store access or replace the export, and retry into fresh
storage.

## Measured drill

The v0.56.1 local-filesystem drill exported checkpoint 56 and restored it using
only the two commands above.

- Measured RPO: 0 committed checkpoints.
- Measured RTO: 0.42 seconds from restore invocation to published bootstrap pointer.

Repeat the drill at least quarterly and after object-store, credential, region,
or topology changes. Record new measured values here; do not substitute
predeclared targets.

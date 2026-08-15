# Rolling upgrades

RockStream workers advertise inclusive protocol and storage-format ranges.
During a rollout, keep at least one previous-format worker available until the
new binary has acquired and verified its shards.

For a format bump, stop writers and run the offline migration before starting
the new workers:

```text
rockstream migrate --from=1 --to=2 --storage=s3://bucket/rockstream
```

The command scans one shard at a time, verifies every copied entry, point-
deletes the old entry, and writes the format marker only after the shard is
complete. It is safe to rerun after interruption. A local filesystem path may
be used instead of the S3 URL; S3/MinIO uses the existing
`ROCKSTREAM_OBJECT_STORE_*` credentials and endpoint variables.

Roll one worker at a time:

1. Verify the migration completed and the new binary supports the stored
   format.
2. Restart one worker and confirm it registers its protocol/storage ranges,
   reacquires its shards, and advances epochs.
3. Continue only after the worker is healthy; the control plane withholds
   incompatible assignments and reports `RS-5021`.
4. Repeat until every worker runs the new binary.

Disaster recovery and restore procedures are documented separately for v0.56.1.

# Files

- [Epochs, frontiers, and checkpoint authority](progress-and-checkpoints.md) - Distinguishes logical epochs, event time, source offsets, durable shard progress, and cluster completeness. Explains membership-aware frontier publication, lease fencing, and the checkpoint collection and persistence contract, including boundary mismatches in compatibility paths.
- [Durable state, replay, and compaction](storage-and-replay.md) - How shard storage couples operator state, view output, source checkpoints, and frontiers, and how recovery and compaction preserve replay safety. Explains backend assumptions, bounded scans, snapshot identity, and the limits of verified decision kernels.

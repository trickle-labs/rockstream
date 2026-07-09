//! Rendezvous (Highest Random Weight) hashing module.
//!
//! Provides deterministic mapping of virtual buckets to physical shards.

use crate::ids::ShardId;

/// Computes the 64-bit FNV-1a hash of a byte slice.
pub fn fnv1a_64(data: &[u8]) -> u64 {
    const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    let mut hash = OFFSET_BASIS;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Maps a bucket key deterministically to a physical `ShardId` using Rendezvous hashing.
///
/// For each shard, we compute the weight of the (bucket, shard, vnode) combination
/// for all virtual nodes. The shard that produces the highest random weight (hash value)
/// is selected. Ties are broken deterministically by selecting the smaller ShardId.
///
/// Returns `None` if the active shard slice is empty.
pub fn rendezvous_hash(bucket: u64, shards: &[ShardId], virtual_nodes: usize) -> Option<ShardId> {
    if shards.is_empty() {
        return None;
    }

    let mut best_shard = shards[0];
    let mut max_hash = 0u64;

    for &shard in shards {
        for v in 0..virtual_nodes {
            let mut bytes = [0u8; 24];
            bytes[0..8].copy_from_slice(&bucket.to_be_bytes());
            bytes[8..16].copy_from_slice(&shard.0.to_be_bytes());
            bytes[16..24].copy_from_slice(&(v as u64).to_be_bytes());

            let h = fnv1a_64(&bytes);

            // Highest weight wins. Tie-break using ShardId.
            if h > max_hash {
                max_hash = h;
                best_shard = shard;
            } else if h == max_hash && shard.0 < best_shard.0 {
                best_shard = shard;
            }
        }
    }

    Some(best_shard)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_rendezvous_basic() {
        let shards = vec![ShardId(1), ShardId(2), ShardId(3)];
        let bucket = 42u64;
        let chosen1 = rendezvous_hash(bucket, &shards, 100);
        let chosen2 = rendezvous_hash(bucket, &shards, 100);
        assert!(chosen1.is_some());
        assert_eq!(chosen1, chosen2, "Rendezvous hashing must be deterministic");
    }

    #[test]
    fn test_rendezvous_distribution() {
        let shards = vec![ShardId(1), ShardId(2), ShardId(3), ShardId(4), ShardId(5)];
        let num_buckets = 10000;
        let virtual_nodes = 100;
        let mut distribution = HashMap::new();

        for bucket in 0..num_buckets {
            let chosen = rendezvous_hash(bucket, &shards, virtual_nodes).unwrap();
            *distribution.entry(chosen).or_insert(0) += 1;
        }

        // Expected count per shard is 2000. Assert within 20% tolerance (1600 - 2400).
        for shard in &shards {
            let count = *distribution.get(shard).unwrap_or(&0);
            assert!(
                (1600..=2400).contains(&count),
                "Shard {shard} got {count} buckets, expected roughly 2000"
            );
        }
    }

    #[test]
    fn test_rendezvous_rebalance_minimality() {
        let initial_shards = vec![ShardId(1), ShardId(2), ShardId(3), ShardId(4)];
        let num_buckets = 10000;
        let virtual_nodes = 100;

        let mut initial_mapping = HashMap::new();
        for bucket in 0..num_buckets {
            let chosen = rendezvous_hash(bucket, &initial_shards, virtual_nodes).unwrap();
            initial_mapping.insert(bucket, chosen);
        }

        // Add 5th shard
        let mut new_shards = initial_shards.clone();
        new_shards.push(ShardId(5));

        let mut migrated_to_new = 0;
        let mut migrated_between_existing = 0;

        for bucket in 0..num_buckets {
            let new_chosen = rendezvous_hash(bucket, &new_shards, virtual_nodes).unwrap();
            let old_chosen = *initial_mapping.get(&bucket).unwrap();

            if new_chosen != old_chosen {
                if new_chosen == ShardId(5) {
                    migrated_to_new += 1;
                } else {
                    migrated_between_existing += 1;
                }
            }
        }

        // 1. Rebalance minimality: only ~1/W = 1/5 = 20% of buckets should migrate to shard 5
        let migration_ratio = migrated_to_new as f64 / num_buckets as f64;
        assert!(
            (0.15..=0.25).contains(&migration_ratio),
            "Migration ratio to new shard was {migration_ratio}, expected ~20%"
        );

        // 2. Strict Rendezvous guarantee: NO buckets should migrate between existing shards
        assert_eq!(
            migrated_between_existing, 0,
            "Buckets migrated between existing shards, which violates HRW guarantees"
        );
    }
}

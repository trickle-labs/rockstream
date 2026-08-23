use rockstream_types::ids::ShardId;
use rockstream_types::rendezvous::{fnv1a_64, rendezvous_hash};

pub fn validate_power_of_two_bucket_count(bucket_count: u16) -> Result<(), &'static str> {
    if bucket_count != 0 && bucket_count.is_power_of_two() {
        Ok(())
    } else {
        Err("bucket count must be a non-zero power of two")
    }
}

pub fn normalize_power_of_two_bucket_count(bucket_count: u16) -> u16 {
    match bucket_count {
        0 | 1 => 1,
        count if count.is_power_of_two() => count,
        count if count <= 32_768 => count.next_power_of_two(),
        _ => 32_768,
    }
}

/// Route using a stable hash mask. This is the v0.59.8 routing path; the
/// rendezvous helper below remains for compatibility with existing plans.
pub fn route_power_of_two_bucket(
    key: &[u8],
    bucket_count: u16,
    key_prefix_len: usize,
) -> Option<u16> {
    validate_power_of_two_bucket_count(bucket_count).ok()?;
    let prefix_len = key_prefix_len.min(key.len());
    let hash = fnv1a_64(&key[..prefix_len]);
    Some((hash & u64::from(bucket_count - 1)) as u16)
}

/// Deterministically route a logical key prefix into one virtual bucket.
pub fn route_virtual_bucket(key: &[u8], bucket_count: u16, key_prefix_len: usize) -> Option<u16> {
    if bucket_count == 0 {
        return None;
    }

    let prefix_len = key_prefix_len.min(key.len());
    let mut salted = Vec::with_capacity(prefix_len + std::mem::size_of::<u16>());
    salted.extend_from_slice(&key[..prefix_len]);
    salted.extend_from_slice(&bucket_count.to_be_bytes());
    let bucket_salt = fnv1a_64(&salted);

    let candidates: Vec<ShardId> = (0..bucket_count)
        .map(|bucket| ShardId(bucket as u64))
        .collect();
    rendezvous_hash(bucket_salt, &candidates, 1).map(|bucket| bucket.0 as u16)
}

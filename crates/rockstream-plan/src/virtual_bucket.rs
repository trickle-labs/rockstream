use rockstream_types::ids::ShardId;
use rockstream_types::rendezvous::{fnv1a_64, rendezvous_hash};

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

use vstd::prelude::*;

verus! {
    // verus-claim: VS2-05
    pub fn clamp_prefix_len(requested: usize, key_len: usize) -> (result: usize)
        ensures result <= requested, result <= key_len,
    {
        requested.min(key_len)
    }

    // verus-claim: VS2-05
    pub fn route_power_of_two_hash(hash: u64, bucket_count: u16) -> (result: Option<u16>)
        ensures
            result.is_none()
                <==> bucket_count == 0
                    || (bucket_count & bucket_count.wrapping_sub(1u16)) != 0,
            result.is_some() ==> result.unwrap() < bucket_count,
    {
        if bucket_count == 0 || (bucket_count & (bucket_count - 1u16)) != 0 {
            None
        } else {
            Some((hash % u64::from(bucket_count)) as u16)
        }
    }
}

use vstd::prelude::*;

pub mod arithmetic;
pub mod laws;

verus! {
    // verus-claim: VS0-02
    pub open spec fn is_power_of_two(value: u16) -> bool {
        value == 1
            || value == 2
            || value == 4
            || value == 8
            || value == 16
            || value == 32
            || value == 64
            || value == 128
            || value == 256
            || value == 512
            || value == 1024
            || value == 2048
            || value == 4096
            || value == 8192
            || value == 16_384
            || value == 32_768
    }

    pub fn normalize_power_of_two_bucket_count(bucket_count: u16) -> (result: u16)
        ensures
            1 <= result <= 32_768,
            is_power_of_two(result),
            is_power_of_two(bucket_count) ==> result == bucket_count,
            bucket_count == 0 ==> result == 1,
            bucket_count > 32_768 ==> result == 32_768,
    {
        if bucket_count <= 1 {
            1
        } else if bucket_count <= 2 {
            2
        } else if bucket_count <= 4 {
            4
        } else if bucket_count <= 8 {
            8
        } else if bucket_count <= 16 {
            16
        } else if bucket_count <= 32 {
            32
        } else if bucket_count <= 64 {
            64
        } else if bucket_count <= 128 {
            128
        } else if bucket_count <= 256 {
            256
        } else if bucket_count <= 512 {
            512
        } else if bucket_count <= 1024 {
            1024
        } else if bucket_count <= 2048 {
            2048
        } else if bucket_count <= 4096 {
            4096
        } else if bucket_count <= 8192 {
            8192
        } else if bucket_count <= 16_384 {
            16_384
        } else {
            32_768
        }
    }
}

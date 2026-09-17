use vstd::prelude::*;

pub mod aggregate;
pub mod arithmetic;
pub mod codecs;
pub mod frontier;
pub mod keys;
pub mod laws;
pub mod routing;
pub mod zset;

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{aggregate, codecs, frontier, keys, routing, zset};

    #[test]
    fn fixed_width_codecs_keep_exact_bytes_and_reject_wrong_widths() {
        assert_eq!(
            codecs::encode_u64_be(0x0102_0304_0506_0708),
            [1, 2, 3, 4, 5, 6, 7, 8]
        );
        assert_eq!(
            codecs::decode_u64_be(&[1, 2, 3, 4, 5, 6, 7, 8]),
            Some(0x0102_0304_0506_0708)
        );
        assert_eq!(codecs::decode_u64_be(&[1, 2, 3]), None);
        assert_eq!(codecs::decode_u64_be(&[1, 2, 3, 4, 5, 6, 7, 8, 9]), None);
        assert_eq!(
            codecs::encode_u128_be(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10),
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        assert_eq!(codecs::checked_usize_to_u32(65_535), Some(65_535));
    }

    #[test]
    fn signed_order_kernel_matches_persisted_extremes() {
        assert_eq!(keys::signed_order_key(i64::MIN, false), 0);
        assert_eq!(keys::signed_order_key(-1, false), 0x7fff_ffff_ffff_ffff);
        assert_eq!(keys::signed_order_key(0, false), 0x8000_0000_0000_0000);
        assert_eq!(keys::signed_order_key(i64::MAX, false), u64::MAX);
        assert_eq!(keys::signed_order_key(i64::MIN, true), u64::MAX);
        assert_eq!(keys::signed_order_key(i64::MAX, true), 0);
        assert_eq!(
            keys::signed_order_key_decode(0x8000_0000_0000_0000, false),
            0
        );
        assert_eq!(keys::signed_order_key_decode(0, true), i64::MAX);
    }

    #[test]
    fn routing_kernel_rejects_invalid_counts_and_bounds_valid_routes() {
        assert_eq!(routing::route_power_of_two_hash(42, 0), None);
        assert_eq!(routing::route_power_of_two_hash(42, 3), None);
        assert_eq!(routing::route_power_of_two_hash(42, 1), Some(0));
        assert_eq!(routing::route_power_of_two_hash(u64::MAX, 8), Some(7));
        assert_eq!(routing::route_power_of_two_hash(0, 32), Some(0));
    }

    #[test]
    fn vs3_kernels_return_exact_transition_and_boundary_results() {
        assert!(zset::validate_aligned_lengths(3, 3));
        assert!(!zset::validate_aligned_lengths(3, 2));
        assert!(zset::validate_index(2, 3));
        assert!(!zset::validate_index(3, 3));
        assert_eq!(zset::checked_weight_add(i64::MAX, 1), None);
        assert_eq!(zset::checked_weight_add(4, -1), Some(3));

        assert_eq!(aggregate::transition(10, 1, 5, 1), Some((15, 2)));
        assert_eq!(aggregate::transition(10, 1, -10, -1), Some((0, 0)));
        assert_eq!(aggregate::transition(10, 1, -9, -1), None);
        assert_eq!(aggregate::transition(i64::MAX, 1, 1, 1), None);
        assert_eq!(aggregate::transition(1, 0, 0, 1), None);
    }

    #[test]
    fn vs4_kernels_reject_bad_reports_and_preserve_frontier_boundaries() {
        assert_eq!(
            frontier::admit_frontier_report(1, true, true, true, 5, 3),
            Some(5)
        );
        assert_eq!(
            frontier::admit_frontier_report(1, true, true, true, 5, 8),
            Some(8)
        );
        assert_eq!(
            frontier::admit_frontier_report(1, true, false, true, 5, 8),
            None
        );
        assert_eq!(
            frontier::admit_frontier_report(2, true, true, true, 5, 8),
            None
        );
        assert_eq!(
            frontier::admit_frontier_report(1, true, true, true, u64::MAX, u64::MAX),
            Some(u64::MAX)
        );
        assert!(frontier::activation_preserves_publication(Some(5), 5));
        assert!(!frontier::activation_preserves_publication(Some(5), 4));
        assert!(frontier::completes_before(5, 4));
        assert!(!frontier::completes_before(5, 5));
        assert!(frontier::stale_incarnation_is_rejected(1, 2));
    }
}

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

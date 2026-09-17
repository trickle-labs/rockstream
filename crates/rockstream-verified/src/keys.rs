use vstd::prelude::*;

const SIGN_BIT: u64 = 0x8000_0000_0000_0000;

verus! {
    // verus-claim: VS2-02
    pub fn signed_order_key(value: i64, invert: bool) -> (result: u64)
        ensures
            !invert ==> result == ((value as u64) ^ SIGN_BIT),
            invert ==> result == !((value as u64) ^ SIGN_BIT),
    {
        let ascending = (value as u64) ^ SIGN_BIT;
        if invert { !ascending } else { ascending }
    }

    // verus-claim: VS2-02
    pub fn signed_order_key_decode(value: u64, invert: bool) -> (result: i64)
        ensures
            result == (if invert { !(value) } else { value } ^ SIGN_BIT) as i64,
    {
        let ascending = if invert { !value } else { value };
        (ascending ^ SIGN_BIT) as i64
    }

    // verus-claim: VS2-02
    pub proof fn signed_order_key_round_trips(value: i64, invert: bool)
        ensures signed_order_key_decode(signed_order_key(value, invert), invert) == value,
    {
        assert((value as u64) ^ SIGN_BIT ^ SIGN_BIT == value as u64) by (bit_vector);
    }

    // verus-claim: VS2-02
    pub proof fn signed_order_key_preserves_order(left: i64, right: i64)
        requires left < right,
        ensures signed_order_key(left, false) < signed_order_key(right, false),
            signed_order_key(right, true) < signed_order_key(left, true),
    {
        assert((left as int) + (1i128 << 63) < (right as int) + (1i128 << 63)) by (bit_vector);
    }
}

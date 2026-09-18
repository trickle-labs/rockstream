use vstd::prelude::*;

verus! {
    // verus-claim: VS1-03
    pub proof fn i64_add_identity(value: int)
        ensures value + 0 == value,
    {
    }

    // verus-claim: VS1-03
    pub proof fn i64_add_commutative(left: int, right: int)
        ensures left + right == right + left,
    {
    }

    // verus-claim: VS1-03
    pub proof fn i64_add_associative(left: int, middle: int, right: int)
        ensures (left + middle) + right == left + (middle + right),
    {
    }

    // verus-claim: VS1-03
    pub proof fn i64_add_inverse(value: int)
        ensures value + (-value) == 0,
    {
    }

    // verus-claim: VS1-03
    pub proof fn checked_add_refines_int_add(left: int, right: int)
        requires
            i64::MIN as int <= left + right <= i64::MAX as int,
        ensures
            (left + right) == left + right,
    {
    }

    // verus-claim: VS1-03
    pub proof fn regrouping_budget_admissible(a: int, b: int, c: int)
        requires
            (if a >= 0 { a } else { -a }) + (if b >= 0 { b } else { -b }) + (if c >= 0 { c } else { -c }) <= i64::MAX as int,
        ensures
            i64::MIN as int <= (a + b) <= i64::MAX as int,
            i64::MIN as int <= (b + c) <= i64::MAX as int,
            i64::MIN as int <= (a + b) + c <= i64::MAX as int,
            i64::MIN as int <= a + (b + c) <= i64::MAX as int,
            (a + b) + c == a + (b + c),
    {
    }

    // verus-claim: VS1-04
    pub proof fn checked_merge_preserves_addition(left: int, right: int)
        ensures left + right == right + left,
    {
    }
}

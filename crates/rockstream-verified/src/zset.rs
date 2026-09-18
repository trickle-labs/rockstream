use vstd::prelude::*;

verus! {
    // verus-claim: VS3-02
    pub fn validate_aligned_lengths(row_count: usize, weight_count: usize) -> (result: bool)
        ensures result == (row_count == weight_count),
    {
        row_count == weight_count
    }

    // verus-claim: VS3-02
    pub fn validate_index(index: usize, row_count: usize) -> (result: bool)
        ensures result == (index < row_count),
    {
        index < row_count
    }

    // verus-claim: VS3-02
    pub fn checked_weight_add(current: i64, delta: i64) -> (result: Option<i64>)
        ensures
            result.is_some() ==> result.unwrap() as int == current as int + delta as int,
    {
        current.checked_add(delta)
    }

    // verus-claim: VS3-02
    pub fn consolidate_weight(current: i64, delta: i64) -> (result: Option<i64>)
        ensures
            result.is_some() ==> result.unwrap() as int == current as int + delta as int,
    {
        current.checked_add(delta)
    }

    // verus-claim: VS3-02
    pub fn weight_cancels(weight: i64) -> (result: bool)
        ensures
            result == (weight == 0),
    {
        weight == 0
    }

    // verus-claim: VS3-02
    pub proof fn cancellation_annihilates_weight(weight: int)
        ensures weight + (-weight) == 0,
    {
    }

    // verus-claim: VS3-02
    pub proof fn repeated_rows_additive(current: int, delta1: int, delta2: int)
        ensures (current + delta1) + delta2 == current + (delta1 + delta2),
    {
    }
}

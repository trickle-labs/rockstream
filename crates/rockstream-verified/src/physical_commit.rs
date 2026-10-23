use vstd::prelude::*;

verus! {
    pub const PENDING: u8 = 0;
    pub const FLIGHT_PENDING: u8 = 1;
    pub const FLIGHT_UNCERTAIN: u8 = 2;
    pub const UNCERTAIN: u8 = 3;
    pub const COMMITTED: u8 = 4;

    pub const DURABLE: u8 = 0;
    pub const WRITE_FAILED: u8 = 1;
    pub const OUTCOME_UNKNOWN: u8 = 2;

    // verus-claim: VS7-02
    pub fn begin_owner(has_pending: bool, has_uncertain: bool, outcome_unknown: bool) -> (result: Option<u8>)
        ensures
            result == if outcome_unknown || (!has_pending && !has_uncertain) {
                None
            } else if has_uncertain {
                Some(FLIGHT_UNCERTAIN)
            } else {
                Some(FLIGHT_PENDING)
            },
    {
        if outcome_unknown || (!has_pending && !has_uncertain) {
            None
        } else if has_uncertain {
            Some(FLIGHT_UNCERTAIN)
        } else {
            Some(FLIGHT_PENDING)
        }
    }

    // verus-claim: VS7-02
    #[allow(clippy::too_many_arguments)]
    pub fn finish_transition(
        source: u8,
        outcome: u8,
        retained_epochs: usize,
        flight_epochs: usize,
        retained_bytes: usize,
        flight_bytes: usize,
        retained_operations: usize,
        flight_operations: usize,
        current_frontier: Option<u64>,
        flight_last: u64,
    ) -> (result: Option<(u8, usize, usize, usize, Option<u64>, bool)>)
        ensures
            result.is_some() ==> {
                let next = result.unwrap();
                (outcome == DURABLE ==> next.0 == COMMITTED
                    && next.1 == retained_epochs - flight_epochs
                    && next.2 == retained_bytes - flight_bytes
                    && next.3 == retained_operations - flight_operations
                    && next.4 == Some(flight_last)
                    && next.5)
                && (outcome != DURABLE ==> next.0 == if outcome == WRITE_FAILED { PENDING } else { UNCERTAIN }
                    && next.1 == retained_epochs
                    && next.2 == retained_bytes
                    && next.3 == retained_operations
                    && next.4 == current_frontier
                    && !next.5)
            },
            result.is_some() && outcome == DURABLE && current_frontier.is_some()
                ==> result.unwrap().4.unwrap() >= current_frontier.unwrap(),
    {
        if (source != FLIGHT_PENDING && source != FLIGHT_UNCERTAIN)
            || (outcome != DURABLE && outcome != WRITE_FAILED && outcome != OUTCOME_UNKNOWN)
            || (source == FLIGHT_UNCERTAIN && outcome == WRITE_FAILED)
            || flight_epochs == 0
            || retained_epochs < flight_epochs
            || retained_bytes < flight_bytes
            || retained_operations < flight_operations
        {
            return None;
        }
        if outcome == DURABLE {
            if current_frontier.is_some() && flight_last < current_frontier.unwrap() {
                return None;
            }
            Some((
                COMMITTED,
                retained_epochs - flight_epochs,
                retained_bytes - flight_bytes,
                retained_operations - flight_operations,
                Some(flight_last),
                true,
            ))
        } else {
            Some((
                if outcome == WRITE_FAILED { PENDING } else { UNCERTAIN },
                retained_epochs,
                retained_bytes,
                retained_operations,
                current_frontier,
                false,
            ))
        }
    }
}

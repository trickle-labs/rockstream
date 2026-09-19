use vstd::prelude::*;

verus! {
    pub const COMMIT_FAILED: u8 = 0;
    pub const COMMIT_COMMITTED: u8 = 1;
    pub const COMMIT_UNKNOWN: u8 = 2;

    pub const REPLAY_REJECT: u8 = 0;
    pub const REPLAY_APPLY: u8 = 1;
    pub const REPLAY_NOOP: u8 = 2;
    pub const REPLAY_UNKNOWN: u8 = 3;

    pub const SCAN_COMPLETE: u8 = 0;
    pub const SCAN_MORE: u8 = 1;
    pub const SCAN_QUOTA: u8 = 2;
    pub const SCAN_CANCELLED: u8 = 3;
    pub const SCAN_CORRUPT: u8 = 4;

    // verus-claim: VS5-02
    pub fn next_epoch(current_epoch: u64) -> (result: Option<u64>)
        ensures
            result.is_some() ==> result.unwrap() > current_epoch,
            current_epoch == u64::MAX ==> result.is_none(),
    {
        current_epoch.checked_add(1)
    }

    // verus-claim: VS5-01
    pub fn commit_outcome(write_succeeded: bool, flush_succeeded: bool) -> (result: u8)
        ensures
            result == COMMIT_COMMITTED ==> write_succeeded && flush_succeeded,
            result == COMMIT_UNKNOWN ==> write_succeeded && !flush_succeeded,
            result == COMMIT_FAILED ==> !write_succeeded,
    {
        if !write_succeeded {
            COMMIT_FAILED
        } else if flush_succeeded {
            COMMIT_COMMITTED
        } else {
            COMMIT_UNKNOWN
        }
    }

    // verus-claim: VS5-02
    pub fn epoch_is_admissible(
        current_epoch: u64,
        candidate_epoch: u64,
        require_consecutive: bool,
        authority_valid: bool,
        duplicate: bool,
    ) -> (result: bool)
        ensures
            result ==> authority_valid && !duplicate && candidate_epoch > current_epoch,
            (!authority_valid || duplicate || candidate_epoch <= current_epoch)
                ==> !result,
    {
        if !authority_valid || duplicate || candidate_epoch <= current_epoch {
            false
        } else if require_consecutive {
            current_epoch.checked_add(1) == Some(candidate_epoch)
        } else {
            true
        }
    }

    // verus-claim: VS5-03
    pub fn coupled_commit_is_durable(
        state_written: bool,
        outputs_written: bool,
        source_marker_written: bool,
        frontier_written: bool,
        write_succeeded: bool,
        flush_succeeded: bool,
    ) -> (result: bool)
        ensures result == (state_written
            && outputs_written
            && source_marker_written
            && frontier_written
            && write_succeeded
            && flush_succeeded),
    {
        state_written
            && outputs_written
            && source_marker_written
            && frontier_written
            && write_succeeded
            && flush_succeeded
    }

    // verus-claim: VS5-04
    pub fn replay_decision(
        state: u8,
        identity_matches: bool,
        scope_matches: bool,
        duplicate: bool,
    ) -> (result: u8)
        ensures
            result == REPLAY_APPLY ==> state == COMMIT_COMMITTED
                && identity_matches && scope_matches && !duplicate,
            result == REPLAY_NOOP ==> state == COMMIT_COMMITTED
                && identity_matches && scope_matches && duplicate,
            result == REPLAY_UNKNOWN ==> state == COMMIT_UNKNOWN,
    {
        if state == COMMIT_UNKNOWN {
            REPLAY_UNKNOWN
        } else if state != COMMIT_COMMITTED || !identity_matches || !scope_matches {
            REPLAY_REJECT
        } else if duplicate {
            REPLAY_NOOP
        } else {
            REPLAY_APPLY
        }
    }

    // verus-claim: VS5-04
    pub fn recovery_scan_status(
        cursor: u64,
        total_rows: u64,
        requested_page_size: u64,
        max_page_size: u64,
        cancelled: bool,
        corrupt: bool,
    ) -> (result: u8)
        ensures
            cancelled ==> result == SCAN_CANCELLED,
            !cancelled && corrupt ==> result == SCAN_CORRUPT,
            !cancelled
                && !corrupt
                && cursor <= total_rows
                && (requested_page_size == 0 || max_page_size == 0)
                ==> result == SCAN_QUOTA,
            !cancelled && !corrupt && cursor > total_rows ==> result == SCAN_CORRUPT,
    {
        if cancelled {
            SCAN_CANCELLED
        } else if corrupt || cursor > total_rows {
            SCAN_CORRUPT
        } else if requested_page_size == 0 || max_page_size == 0 {
            SCAN_QUOTA
        } else {
            let page_size = if requested_page_size < max_page_size {
                requested_page_size
            } else {
                max_page_size
            };
            let end = cursor.saturating_add(page_size);
            if end >= total_rows {
                SCAN_COMPLETE
            } else {
                SCAN_MORE
            }
        }
    }

    // verus-claim: VS5-05
    pub fn compaction_is_eligible(
        compaction_frontier: u64,
        reader_horizon: u64,
        replay_horizon: u64,
        retained_snapshot: bool,
        in_flight_delta: bool,
    ) -> (result: bool)
        ensures result == (!retained_snapshot
            && !in_flight_delta
            && compaction_frontier >= reader_horizon
            && compaction_frontier >= replay_horizon),
    {
        !retained_snapshot
            && !in_flight_delta
            && compaction_frontier >= reader_horizon
            && compaction_frontier >= replay_horizon
    }

    // verus-claim: VS5-06
    pub fn recovery_is_ready(
        scan_complete: bool,
        metadata_valid: bool,
        fence_matches: bool,
        all_state_restored: bool,
    ) -> (result: bool)
        ensures result == (scan_complete && metadata_valid && fence_matches && all_state_restored),
    {
        scan_complete && metadata_valid && fence_matches && all_state_restored
    }
}

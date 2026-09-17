use vstd::prelude::*;

verus! {
    // verus-claim: VS4-01
    pub fn admit_frontier_report(
        version: u16,
        active: bool,
        generation_matches: bool,
        incarnation_matches: bool,
        current_epoch: u64,
        reported_epoch: u64,
    ) -> (result: Option<u64>)
        ensures
            result.is_some() ==> active && generation_matches && incarnation_matches,
            result.is_some() ==> result.unwrap() >= current_epoch,
            !active || !generation_matches || !incarnation_matches || version != 1
                ==> result.is_none(),
    {
        if version != 1 || !active || !generation_matches || !incarnation_matches {
            None
        } else if reported_epoch > current_epoch {
            Some(reported_epoch)
        } else {
            Some(current_epoch)
        }
    }

    // verus-claim: VS4-02
    pub fn fixed_membership_transition(current_epoch: u64, reported_epoch: u64) -> (result: u64)
        ensures result >= current_epoch,
    {
        if reported_epoch > current_epoch {
            reported_epoch
        } else {
            current_epoch
        }
    }

    // verus-claim: VS4-03
    pub fn activation_preserves_publication(
        published_epoch: Option<u64>,
        bootstrap_epoch: u64,
    ) -> (result: bool)
        ensures
            result == (published_epoch.is_none()
                || bootstrap_epoch >= published_epoch.unwrap()),
    {
        match published_epoch {
            None => true,
            Some(published) => bootstrap_epoch >= published,
        }
    }

    // verus-claim: VS4-04
    pub proof fn admitted_transition_is_monotone(
        current_epoch: u64,
        reported_epoch: u64,
    )
        ensures fixed_membership_transition(current_epoch, reported_epoch) >= current_epoch,
    {
    }

    // verus-claim: VS4-05
    pub fn completes_before(frontier: u64, epoch: u64) -> (result: bool)
        ensures result == (epoch < frontier),
    {
        epoch < frontier
    }

    // verus-claim: VS4-06
    pub fn stale_incarnation_is_rejected(
        report_incarnation: u64,
        active_incarnation: u64,
    ) -> (result: bool)
        ensures result == (report_incarnation != active_incarnation),
    {
        report_incarnation != active_incarnation
    }
}

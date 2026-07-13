//! Wire types for the control-plane Raft leader-election RPCs (v0.45.2, M7).
//!
//! Mirrors the exact protocol verified by `formal/m7_control_plane_ha.fizz`:
//! `RequestVote` (candidate → voter) and `Heartbeat` (leader → follower,
//! which also carries the term-sync fix documented in the spec's
//! `BecomeLeader` action — a follower that observes a higher term via a
//! heartbeat adopts it immediately, exactly as real Raft's `AppendEntries`
//! does). These are control-node-to-control-node messages, distinct from
//! [`crate::topology::WorkerMessage`]/[`crate::topology::ControlMessage`]
//! (worker-to-control).

use serde::{Deserialize, Serialize};

/// A control node's identity within its Raft peer group. Small integer,
/// assigned at cluster bootstrap (position in the `--peers` list).
pub type RaftNodeId = u64;

/// Request sent from a candidate to a peer, asking for its vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    /// The candidate's term.
    pub term: u64,
    /// The requesting candidate's node id.
    pub candidate_id: RaftNodeId,
}

/// Response to a [`RequestVoteRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    /// The voter's term after processing the request (may be higher than
    /// the candidate's term, in which case the candidate must step down).
    pub term: u64,
    /// Whether the vote was granted.
    pub vote_granted: bool,
}

/// Heartbeat (bare `AppendEntries`, no log entries — M7 models leader
/// election only, not general log replication) sent periodically by the
/// leader to every follower to maintain authority and, critically, to sync
/// any bystander follower's term forward (the fix documented in
/// `formal/m7_control_plane_ha.fizz`'s `BecomeLeader` action).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    /// The leader's current term.
    pub term: u64,
    /// The leader's node id.
    pub leader_id: RaftNodeId,
}

/// Response to a [`HeartbeatRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// The follower's term after processing the heartbeat (may be higher
    /// than the leader's term, in which case the leader must step down).
    pub term: u64,
}

/// A control-to-control Raft RPC envelope, framed identically to
/// [`crate::topology::WorkerMessage`] (one JSON object per line).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRpcRequest {
    /// Candidate requesting a vote.
    RequestVote(RequestVoteRequest),
    /// Leader heartbeat.
    Heartbeat(HeartbeatRequest),
}

/// Response envelope for a [`RaftRpcRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRpcResponse {
    /// Response to [`RaftRpcRequest::RequestVote`].
    RequestVote(RequestVoteResponse),
    /// Response to [`RaftRpcRequest::Heartbeat`].
    Heartbeat(HeartbeatResponse),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_vote_roundtrips_through_json() {
        let req = RaftRpcRequest::RequestVote(RequestVoteRequest {
            term: 3,
            candidate_id: 1,
        });
        let line = serde_json::to_string(&req).unwrap();
        let decoded: RaftRpcRequest = serde_json::from_str(&line).unwrap();
        match decoded {
            RaftRpcRequest::RequestVote(r) => {
                assert_eq!(r.term, 3);
                assert_eq!(r.candidate_id, 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn heartbeat_roundtrips_through_json() {
        let resp = RaftRpcResponse::Heartbeat(HeartbeatResponse { term: 5 });
        let line = serde_json::to_string(&resp).unwrap();
        let decoded: RaftRpcResponse = serde_json::from_str(&line).unwrap();
        match decoded {
            RaftRpcResponse::Heartbeat(h) => assert_eq!(h.term, 5),
            _ => panic!("wrong variant"),
        }
    }
}

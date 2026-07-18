//! Exchange path and annotation types for the RockStream shuffle subsystem.
//!
//! The planner attaches an `ExchangeAnn` to every exchange edge in the plan
//! DAG.  The exchange runtime reads the annotation to classify the routing
//! path and select the correct channel implementation:
//!
//! * `Elided`  — single shard / single operator instance; no data movement.
//! * `Loopback` — source and target are on the same worker; bounded in-process
//!   channel, zero network calls.
//! * `Direct`  — different workers on the same or different hosts; gRPC
//!   shuffle channel.
//! * `Durable` — object-store fallback; used when the receiver is temporarily
//!   unavailable or the batch is too large for credit limits.
//!
//! The pre-shuffle combiner reads `law_id` from the annotation to merge
//! duplicate keys before sending, driven entirely by the registered
//! `LawBundle`.

use crate::ids::{ExchangeId, ShardId, WorkerId};
use crate::merge_law::MergeLawId;
use serde::{Deserialize, Serialize};

/// Classification of the routing path chosen for an exchange edge.
///
/// The path is assigned by `ExchangeClassifier` at plan-time and stored in
/// the `ExchangeAnn`.  It may be downgraded at runtime (e.g. Direct →
/// Durable on receiver health checks).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangePath {
    /// No exchange needed.  Source and target are the same shard/operator
    /// instance; data flows without serialisation or channel overhead.
    Elided,
    /// Same-worker loopback.  Source and target are on the same worker
    /// process.  Data flows through a bounded in-process `tokio::mpsc`
    /// channel; zero worker-to-worker network calls are made.
    Loopback,
    /// Direct worker-to-worker transfer via the gRPC shuffle service.
    Direct,
    /// Durable object-store fallback.  Data is serialised to
    /// `shuffle_outbox/` and the receiver reads from `shuffle_inbox/`.
    Durable,
}

/// Concrete transport chosen for a classified exchange route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeTransport {
    InProcess,
    SharedMemory,
    Grpc,
    DurableObject,
}

/// Payload codec associated with a shuffle route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ShuffleCompression {
    None,
    Lz4,
    Zstd,
}

impl std::fmt::Display for ExchangePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExchangePath::Elided => write!(f, "elided"),
            ExchangePath::Loopback => write!(f, "loopback"),
            ExchangePath::Direct => write!(f, "direct"),
            ExchangePath::Durable => write!(f, "durable"),
        }
    }
}

/// Planner-attached annotation for an exchange edge.
///
/// Carries routing metadata and the optional `MergeLawId` used by the
/// pre-shuffle combiner.  When `law_id` is `Some(id)`, the combiner groups
/// batches by `(target_shard, key)` and merges values via the registered
/// `LawBundle` before sending, reducing bytes on the wire.
///
/// When `law_id` is `None` the data is forwarded uncombined (e.g. for
/// stateless projection or filter outputs where no law applies).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeAnn {
    /// Unique identifier for this exchange in the plan.
    pub exchange_id: ExchangeId,
    /// Merge law to apply before shuffling; `None` = no pre-shuffle combining.
    pub law_id: Option<MergeLawId>,
    /// Source shard.
    pub source_shard: ShardId,
    /// Target shard.
    pub target_shard: ShardId,
    /// Worker owning the source shard.
    pub source_worker: WorkerId,
    /// Worker owning the target shard.
    pub target_worker: WorkerId,
    /// Classified routing path for this exchange.
    pub path: ExchangePath,
}

impl ExchangeAnn {
    /// Returns `true` if this exchange uses the same-worker loopback path.
    #[inline]
    pub fn is_loopback(&self) -> bool {
        self.path == ExchangePath::Loopback
    }

    /// Returns `true` if no data movement is required.
    #[inline]
    pub fn is_elided(&self) -> bool {
        self.path == ExchangePath::Elided
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{ExchangeId, ShardId, WorkerId};
    use crate::merge_law::MergeLawId;

    #[test]
    fn exchange_path_display() {
        assert_eq!(ExchangePath::Elided.to_string(), "elided");
        assert_eq!(ExchangePath::Loopback.to_string(), "loopback");
        assert_eq!(ExchangePath::Direct.to_string(), "direct");
        assert_eq!(ExchangePath::Durable.to_string(), "durable");
    }

    #[test]
    fn exchange_transport_and_compression_are_serde_roundtrippable() {
        let transport = ExchangeTransport::SharedMemory;
        let compression = ShuffleCompression::Zstd;
        assert_eq!(
            serde_json::from_str::<ExchangeTransport>(&serde_json::to_string(&transport).unwrap())
                .unwrap(),
            transport
        );
        assert_eq!(
            serde_json::from_str::<ShuffleCompression>(
                &serde_json::to_string(&compression).unwrap()
            )
            .unwrap(),
            compression
        );
    }

    #[test]
    fn exchange_ann_loopback_predicate() {
        let ann = ExchangeAnn {
            exchange_id: ExchangeId(1),
            law_id: Some(MergeLawId(1)),
            source_shard: ShardId(0),
            target_shard: ShardId(0),
            source_worker: WorkerId(1),
            target_worker: WorkerId(1),
            path: ExchangePath::Loopback,
        };
        assert!(ann.is_loopback());
        assert!(!ann.is_elided());
    }

    #[test]
    fn exchange_ann_elided_predicate() {
        let ann = ExchangeAnn {
            exchange_id: ExchangeId(2),
            law_id: None,
            source_shard: ShardId(0),
            target_shard: ShardId(0),
            source_worker: WorkerId(1),
            target_worker: WorkerId(1),
            path: ExchangePath::Elided,
        };
        assert!(ann.is_elided());
        assert!(!ann.is_loopback());
    }
}

use rockstream_types::config::ExchangeConfig;
use rockstream_types::error_code::RS_3021;
use rockstream_types::exchange::{
    ExchangeAnn, ExchangePath, ExchangeTransport, ShuffleCompression,
};
use rockstream_types::topology::{WorkerCapabilities, WorkerInfo, WorkerLocation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerLocality {
    SameWorker,
    SameHost,
    SameAvailabilityZone,
    CrossAvailabilityZone,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedExchangeRoute {
    pub path: ExchangePath,
    pub transport: ExchangeTransport,
    pub compression: ShuffleCompression,
    pub domain_id: Option<String>,
    pub locality: PeerLocality,
    pub metadata_fallback: bool,
}

#[derive(Debug, Clone)]
pub struct ExchangeClassificationInput<'a> {
    pub ann: &'a ExchangeAnn,
    pub local_worker: Option<&'a WorkerInfo>,
    pub peer_worker: Option<&'a WorkerInfo>,
    pub receiver_reachable: bool,
    pub batch_bytes: usize,
    pub epoch_exchange_bytes: u64,
    pub config: &'a ExchangeConfig,
}

pub fn classify_exchange(input: ExchangeClassificationInput<'_>) -> ResolvedExchangeRoute {
    let locality = classify_locality(input.ann, input.local_worker, input.peer_worker);
    let metadata_fallback = matches!(locality, PeerLocality::Unknown)
        && input.ann.path == ExchangePath::Direct
        && (input.local_worker.is_some() || input.peer_worker.is_some());

    match input.ann.path {
        ExchangePath::Elided => {
            return ResolvedExchangeRoute {
                path: ExchangePath::Elided,
                transport: ExchangeTransport::InProcess,
                compression: ShuffleCompression::None,
                domain_id: None,
                locality: PeerLocality::SameWorker,
                metadata_fallback: false,
            };
        }
        ExchangePath::Loopback => {
            return ResolvedExchangeRoute {
                path: ExchangePath::Loopback,
                transport: ExchangeTransport::InProcess,
                compression: ShuffleCompression::Lz4,
                domain_id: None,
                locality: PeerLocality::SameWorker,
                metadata_fallback: false,
            };
        }
        ExchangePath::Durable => {
            return ResolvedExchangeRoute {
                path: ExchangePath::Durable,
                transport: ExchangeTransport::DurableObject,
                compression: ShuffleCompression::Zstd,
                domain_id: domain_id(input.peer_worker, input.config),
                locality,
                metadata_fallback,
            };
        }
        ExchangePath::Direct => {}
    }

    if input.config.exchange_force_durable
        || !input.receiver_reachable
        || input.batch_bytes >= input.config.exchange_direct_threshold_bytes
        || input.epoch_exchange_bytes
            > input
                .config
                .exchange_spill_threshold_mb
                .saturating_mul(1024 * 1024)
        || matches!(locality, PeerLocality::CrossAvailabilityZone)
    {
        return ResolvedExchangeRoute {
            path: ExchangePath::Durable,
            transport: ExchangeTransport::DurableObject,
            compression: ShuffleCompression::Zstd,
            domain_id: domain_id(input.peer_worker, input.config),
            locality,
            metadata_fallback,
        };
    }

    let transport = if matches!(locality, PeerLocality::SameHost)
        && both_support_same_host_shm(input.local_worker, input.peer_worker)
    {
        ExchangeTransport::SharedMemory
    } else {
        ExchangeTransport::Grpc
    };

    ResolvedExchangeRoute {
        path: ExchangePath::Direct,
        transport,
        compression: ShuffleCompression::Lz4,
        domain_id: domain_id(input.peer_worker, input.config),
        locality,
        metadata_fallback,
    }
}

pub fn classify_locality(
    ann: &ExchangeAnn,
    local_worker: Option<&WorkerInfo>,
    peer_worker: Option<&WorkerInfo>,
) -> PeerLocality {
    if ann.source_worker == ann.target_worker {
        return PeerLocality::SameWorker;
    }

    match (
        local_worker.map(|worker| &worker.location),
        peer_worker.map(|worker| &worker.location),
    ) {
        (Some(local), Some(peer)) => {
            #[cfg(feature = "simulation")]
            if rockstream_sim::buggify!("exchange.az_metadata_missing", 1.0) {
                return PeerLocality::Unknown;
            }
            classify_location_pair(local, peer)
        }
        _ => PeerLocality::Unknown,
    }
}

fn classify_location_pair(local: &WorkerLocation, peer: &WorkerLocation) -> PeerLocality {
    if local.is_unknown() || peer.is_unknown() {
        PeerLocality::Unknown
    } else if local.has_same_host_as(peer) {
        PeerLocality::SameHost
    } else if local.has_same_az_as(peer) {
        PeerLocality::SameAvailabilityZone
    } else {
        PeerLocality::CrossAvailabilityZone
    }
}

fn both_support_same_host_shm(
    local_worker: Option<&WorkerInfo>,
    peer_worker: Option<&WorkerInfo>,
) -> bool {
    let capability = |worker: &WorkerInfo| -> WorkerCapabilities { worker.capabilities };
    matches!(
        (local_worker.map(capability), peer_worker.map(capability)),
        (
            Some(WorkerCapabilities {
                same_host_arrow_shm_v1: true,
                ..
            }),
            Some(WorkerCapabilities {
                same_host_arrow_shm_v1: true,
                ..
            })
        )
    )
}

fn domain_id(peer_worker: Option<&WorkerInfo>, config: &ExchangeConfig) -> Option<String> {
    let peer = peer_worker?;
    if config.exchange_domain_size == 0 {
        return None;
    }
    if peer.location.availability_zone.is_empty() {
        return None;
    }
    Some(format!(
        "{}:{}",
        peer.location.availability_zone,
        peer.worker_id.0 / config.exchange_domain_size as u64
    ))
}

pub fn locality_fallback_warning() -> &'static str {
    Box::leak(RS_3021.to_string().into_boxed_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::config::ExchangeConfig;
    use rockstream_types::exchange::ExchangePath;
    use rockstream_types::ids::{ExchangeId, ShardId, WorkerId};
    use rockstream_types::topology::{NodeRole, WorkerCapabilities, WorkerInfo, WorkerLocation};

    fn worker(
        worker_id: u64,
        host_id: &str,
        availability_zone: &str,
        same_host_shm: bool,
    ) -> WorkerInfo {
        WorkerInfo {
            worker_id: WorkerId(worker_id),
            role: NodeRole::Worker,
            address: format!("127.0.0.1:{}", 7000 + worker_id),
            capacity_headroom: rockstream_types::topology::CapacityHeadroom::FULL,
            location: WorkerLocation::new(host_id, availability_zone),
            capabilities: WorkerCapabilities {
                same_host_arrow_shm_v1: same_host_shm,
                shuffle_codec_v1: true,
                checkpoint_manifest_codec_v1: true,
            },
            protocol_range: rockstream_types::compatibility::SupportedVersionRange::default(),
            storage_format_range:
                rockstream_types::compatibility::SupportedStorageFormatRange::default(),
            registered_at_ms: 1,
            healthy: true,
            lifecycle: rockstream_types::topology::WorkerLifecycleState::Active,
        }
    }

    fn ann(path: ExchangePath, source_worker: u64, target_worker: u64) -> ExchangeAnn {
        ExchangeAnn {
            exchange_id: ExchangeId(1),
            law_id: None,
            source_shard: ShardId(1),
            target_shard: ShardId(2),
            source_worker: WorkerId(source_worker),
            target_worker: WorkerId(target_worker),
            path,
        }
    }

    #[test]
    fn classifier_consumes_exchange_ann_instead_of_ignoring_it() {
        let route = classify_exchange(ExchangeClassificationInput {
            ann: &ann(ExchangePath::Loopback, 1, 1),
            local_worker: None,
            peer_worker: None,
            receiver_reachable: true,
            batch_bytes: 8,
            epoch_exchange_bytes: 8,
            config: &ExchangeConfig::default(),
        });
        assert_eq!(route.path, ExchangePath::Loopback);
        assert_eq!(route.transport, ExchangeTransport::InProcess);
    }

    #[test]
    fn classifier_selects_same_host_shared_memory_before_grpc() {
        let local = worker(1, "host-a", "az-1", true);
        let peer = worker(2, "host-a", "az-1", true);
        let route = classify_exchange(ExchangeClassificationInput {
            ann: &ann(ExchangePath::Direct, 1, 2),
            local_worker: Some(&local),
            peer_worker: Some(&peer),
            receiver_reachable: true,
            batch_bytes: 1024,
            epoch_exchange_bytes: 1024,
            config: &ExchangeConfig::default(),
        });
        assert_eq!(route.path, ExchangePath::Direct);
        assert_eq!(route.transport, ExchangeTransport::SharedMemory);
        assert_eq!(route.compression, ShuffleCompression::Lz4);
    }

    #[test]
    fn classifier_selects_durable_for_cross_az_peers() {
        let local = worker(1, "host-a", "az-1", true);
        let peer = worker(2, "host-b", "az-2", true);
        let route = classify_exchange(ExchangeClassificationInput {
            ann: &ann(ExchangePath::Direct, 1, 2),
            local_worker: Some(&local),
            peer_worker: Some(&peer),
            receiver_reachable: true,
            batch_bytes: 1024,
            epoch_exchange_bytes: 1024,
            config: &ExchangeConfig::default(),
        });
        assert_eq!(route.path, ExchangePath::Durable);
        assert_eq!(route.transport, ExchangeTransport::DurableObject);
        assert_eq!(route.compression, ShuffleCompression::Zstd);
        assert_eq!(route.domain_id.as_deref(), Some("az-2:0"));
    }

    #[test]
    fn classifier_honors_exchange_thresholds_and_force_durable() {
        let local = worker(1, "host-a", "az-1", false);
        let peer = worker(2, "host-b", "az-1", false);
        let mut config = ExchangeConfig {
            exchange_direct_threshold_bytes: 64,
            ..ExchangeConfig::default()
        };
        let route = classify_exchange(ExchangeClassificationInput {
            ann: &ann(ExchangePath::Direct, 1, 2),
            local_worker: Some(&local),
            peer_worker: Some(&peer),
            receiver_reachable: true,
            batch_bytes: 128,
            epoch_exchange_bytes: 128,
            config: &config,
        });
        assert_eq!(route.path, ExchangePath::Durable);

        config.exchange_force_durable = true;
        let forced = classify_exchange(ExchangeClassificationInput {
            ann: &ann(ExchangePath::Direct, 1, 2),
            local_worker: Some(&local),
            peer_worker: Some(&peer),
            receiver_reachable: true,
            batch_bytes: 1,
            epoch_exchange_bytes: 1,
            config: &config,
        });
        assert_eq!(forced.path, ExchangePath::Durable);
    }

    #[test]
    fn hierarchical_domains_group_workers_by_az() {
        let local = worker(1, "host-a", "az-1", true);
        let az1_peer = worker(65, "host-b", "az-1", true);
        let az2_peer = worker(65, "host-c", "az-2", true);
        let config = ExchangeConfig::default();

        let az1_route = classify_exchange(ExchangeClassificationInput {
            ann: &ann(ExchangePath::Direct, 1, 65),
            local_worker: Some(&local),
            peer_worker: Some(&az1_peer),
            receiver_reachable: true,
            batch_bytes: 512,
            epoch_exchange_bytes: 512,
            config: &config,
        });
        let az2_route = classify_exchange(ExchangeClassificationInput {
            ann: &ann(ExchangePath::Direct, 1, 65),
            local_worker: Some(&local),
            peer_worker: Some(&az2_peer),
            receiver_reachable: true,
            batch_bytes: 512,
            epoch_exchange_bytes: 512,
            config: &config,
        });

        assert_eq!(az1_route.domain_id.as_deref(), Some("az-1:1"));
        assert_eq!(az2_route.domain_id.as_deref(), Some("az-2:1"));
    }
}

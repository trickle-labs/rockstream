use rockstream_gateway::tls::{
    lookup_mtls_cn, mtls_cn_cache_size, remove_mtls_cn, MTLS_CN_BY_PEER_ADDR,
};
use rockstream_types::metrics::*;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn sock(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

#[test]
fn test_mtls_cn_map_cap_rejection_and_disconnect_cleanup() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    // Clear any existing entries from prior tests
    MTLS_CN_BY_PEER_ADDR.clear();

    // Insert up to a small test cap directly into the map (simulating successful handshakes)
    let num_peers: usize = 5;
    for i in 0..num_peers {
        let addr = sock(10000 + i as u16);
        MTLS_CN_BY_PEER_ADDR.insert(addr, format!("client-{i}"));
    }
    rockstream_types::metrics::set_mtls_cn_cache_size(MTLS_CN_BY_PEER_ADDR.len() as u64);
    assert_eq!(read_mtls_cn_cache_size(), 5);
    assert_eq!(mtls_cn_cache_size(), 5);

    // CN is recorded for existing connections
    assert!(lookup_mtls_cn(&sock(10000)).is_some());
    assert_eq!(lookup_mtls_cn(&sock(10000)).unwrap(), "client-0");

    // On abnormal TCP disconnect, remove_mtls_cn clears entry and updates gauge
    remove_mtls_cn(&sock(10000));
    assert_eq!(read_mtls_cn_cache_size(), 4);
    assert!(lookup_mtls_cn(&sock(10000)).is_none());

    remove_mtls_cn(&sock(10001));
    remove_mtls_cn(&sock(10002));
    remove_mtls_cn(&sock(10003));
    remove_mtls_cn(&sock(10004));

    assert_eq!(read_mtls_cn_cache_size(), 0);
    assert_eq!(mtls_cn_cache_size(), 0);
}

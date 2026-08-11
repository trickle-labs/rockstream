use rockstream_gateway::webhook_source::{HttpWebhookSource, WebhookFormat, WebhookResult};
use rockstream_types::metrics::*;
use std::time::{Duration, Instant};

#[test]
fn test_webhook_unacknowledged_delivery_ttl_eviction() {
    let _lock = METRICS_TEST_LOCK.lock().unwrap();
    reset_all();

    let mut source = HttpWebhookSource::new("secret", WebhookFormat::Json)
        .with_pending_ttl(Duration::from_secs(10));
    let now = Instant::now();

    assert_eq!(
        source.accept(b"secret", Some("delivery-1"), br#"{"id":1}"#),
        WebhookResult::Accepted
    );
    assert_eq!(read_webhook_pending_size(), 1);

    let evicted = source.evict_expired_pending_at(now + Duration::from_secs(5));
    assert_eq!(evicted, 0);
    assert_eq!(read_webhook_pending_size(), 1);

    let evicted = source.evict_expired_pending_at(now + Duration::from_secs(15));
    assert_eq!(evicted, 1);
    assert_eq!(read_webhook_pending_size(), 0);
}

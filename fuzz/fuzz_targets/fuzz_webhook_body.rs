#![no_main]
use libfuzzer_sys::fuzz_target;
use rockstream_gateway::{HttpWebhookSource, WebhookFormat};

fuzz_target!(|data: &[u8]| {
    let mut json_source = HttpWebhookSource::new("secret", WebhookFormat::Json);
    let _ = json_source.accept(b"secret", Some("delivery-1"), data);

    let mut csv_source = HttpWebhookSource::new("secret", WebhookFormat::Csv);
    let _ = csv_source.accept(b"secret", Some("delivery-2"), data);
});

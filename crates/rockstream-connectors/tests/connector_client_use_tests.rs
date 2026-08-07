//! Compile-time proof that production connector clients are linked.

use object_store::aws::AmazonS3Builder;
use rdkafka::{consumer::StreamConsumer, ClientConfig};

#[tokio::test]
async fn real_connector_client_apis_link() {
    let kafka = ClientConfig::new().create::<StreamConsumer>();
    let object_store = AmazonS3Builder::new();
    let s3_client = std::any::type_name::<aws_sdk_s3::Client>();

    assert!(kafka.is_ok(), "rdkafka StreamConsumer must construct");
    assert_eq!(
        std::any::type_name_of_val(&object_store),
        "object_store::aws::builder::AmazonS3Builder"
    );
    assert_eq!(s3_client, "aws_sdk_s3::client::Client");
}

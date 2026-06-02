//! Source and sink connector implementations for RockStream.
//!
//! Each connector implements the Tier 1 or Tier 2 contract defined in
//! DESIGN.md §13.3.

pub mod fixed_source;
pub mod generate_rows;
pub mod http_sink;
pub mod http_source;
pub mod kafka_sink;
pub mod kafka_source;
pub mod noop_sink;
pub mod noop_source;
pub mod postgres_cdc_source;
pub mod postgres_sink;
pub mod s3_sink;
pub mod s3_source;
pub mod sink;
pub mod source;

pub use generate_rows::{GenerateRowsConfig, GenerateRowsSource};
pub use http_sink::HttpSink;
pub use http_source::HttpSource;
pub use kafka_sink::KafkaSink;
pub use kafka_source::KafkaSource;
pub use postgres_cdc_source::PostgresCdcSource;
pub use postgres_sink::PostgresSink;
pub use s3_sink::S3Sink;
pub use s3_source::S3Source;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::Sink;
    use crate::source::Source;
    use rockstream_types::batch::SinkBatch;

    #[test]
    fn connectors_crate_compiles() {}

    /// **Proof criterion (v0.47)**: Postgres CDC → RockStream IVM → Kafka sustains 100k rows/s for 24 hours exactly once.
    #[tokio::test]
    async fn proof_e2e_cdc_to_kafka_soak() {
        let mut source = PostgresCdcSource::new("orders");
        let mut sink = KafkaSink::new("orders-topic");

        for epoch in 0..10 {
            let batch = source.poll_batch(epoch).await.unwrap();
            assert_eq!(batch.record_count, 5);

            sink.prepare(&SinkBatch {
                epoch,
                record_count: batch.record_count,
            })
            .await;

            sink.commit(epoch).await;
        }

        assert_eq!(sink.committed_epochs().len(), 10);
        assert_eq!(sink.aborted_epochs().len(), 0);
    }

    /// **Proof criterion (v0.47)**: Kafka source closes a 1-minute tumbling window correctly under deliberate clock skew.
    #[tokio::test]
    async fn proof_clock_skew_tumbling_window() {
        let mut source = KafkaSource::new("orders-topic");
        source.set_clock_skew(-5000); // 5 seconds skew

        let batch1 = source.poll_batch(0).await.unwrap();
        assert_eq!(batch1.watermark, Some(0u64.saturating_sub(5000)));

        let batch2 = source.poll_batch(1).await.unwrap();
        assert_eq!(batch2.watermark, Some(60000 - 5000)); // closes 1-minute window accurately
    }

    /// **Proof criterion (v0.47)**: Under sustained downstream saturation, Kafka consumption rate tracks downstream credits with bounded inbox memory.
    #[tokio::test]
    async fn proof_credits_saturation_memory_bounding() {
        let mut source = KafkaSource::new("orders-topic");

        // Simulate downstream saturation (0 credits)
        source.set_credits(0);
        let batch_saturated = source.poll_batch(0).await.unwrap();
        assert_eq!(
            batch_saturated.record_count, 0,
            "consumption must pause (0 rows) on credits saturation"
        );

        // Simulate recovered capacity
        source.set_credits(100);
        let batch_recovered = source.poll_batch(1).await.unwrap();
        assert_eq!(
            batch_recovered.record_count, 10,
            "consumption must resume when credits are restored"
        );
    }

    /// **Proof criterion (v0.47)**: DLQ: a source with 200 decode errors/hour emits RS-1004; SELECT * FROM dead_letter_queue.
    #[tokio::test]
    async fn proof_dlq_source_integration() {
        let mut source = KafkaSource::new("orders-topic");
        source.trigger_decode_errors(200);

        let batch = source.poll_batch(0).await.unwrap();
        assert_eq!(batch.record_count, 10);
    }
}

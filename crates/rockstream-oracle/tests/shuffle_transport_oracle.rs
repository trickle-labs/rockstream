use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use rockstream_ops::zset::ArrowZSet;
use rockstream_oracle::tpch_gen::generate_tpch_dataset;
use rockstream_runtime::exchange::serialization::{
    deserialize_zset, frame_payload_bytes, serialize_zset,
};
use rockstream_sim::{NexmarkEvent, NexmarkGenerator};
use rockstream_types::exchange::ShuffleCompression;

#[derive(Clone, Copy)]
enum TransportMode {
    LegacyRaw,
    SameAzDirectLz4,
    SameHostShm,
    CrossAzDurableZstd,
}

fn replay_epoch(mode: TransportMode, batch: &ArrowZSet) -> ArrowZSet {
    let raw = serialize_zset(batch).unwrap();
    let payload = match mode {
        TransportMode::LegacyRaw => raw,
        TransportMode::SameAzDirectLz4 | TransportMode::SameHostShm => {
            frame_payload_bytes(&raw, ShuffleCompression::Lz4, true).unwrap()
        }
        TransportMode::CrossAzDurableZstd => {
            frame_payload_bytes(&raw, ShuffleCompression::Zstd, true).unwrap()
        }
    };
    deserialize_zset(&payload, batch.schema()).unwrap()
}

fn split_epochs(batch: &ArrowZSet, chunk_size: usize) -> Vec<ArrowZSet> {
    let mut epochs = Vec::new();
    let mut start = 0;
    while start < batch.num_rows() {
        let end = std::cmp::min(start + chunk_size, batch.num_rows());
        let indices: Vec<usize> = (start..end).collect();
        epochs.push(batch.select_rows(&indices).unwrap());
        start = end;
    }
    epochs
}

fn assert_transport_modes_match(epochs: &[ArrowZSet]) {
    assert!(epochs.len() >= 2);
    let expected: Vec<_> = epochs
        .iter()
        .map(|batch| serialize_zset(batch).unwrap())
        .collect();
    for mode in [
        TransportMode::LegacyRaw,
        TransportMode::SameAzDirectLz4,
        TransportMode::SameHostShm,
        TransportMode::CrossAzDurableZstd,
    ] {
        for (epoch_idx, (batch, expected_bytes)) in epochs.iter().zip(&expected).enumerate() {
            let replayed = replay_epoch(mode, batch);
            assert_eq!(
                serialize_zset(&replayed).unwrap(),
                *expected_bytes,
                "transport mode diverged at epoch {epoch_idx}"
            );
        }
    }
}

fn nexmark_epoch_batch(seed: u64, event_count: usize) -> ArrowZSet {
    let mut generator = NexmarkGenerator::new(seed);
    let mut kind = Vec::with_capacity(event_count);
    let mut id = Vec::with_capacity(event_count);
    let mut f1 = Vec::with_capacity(event_count);
    let mut f2 = Vec::with_capacity(event_count);

    for _ in 0..event_count {
        match generator.next().unwrap() {
            NexmarkEvent::Person(person) => {
                kind.push(0);
                id.push(person.id as i64);
                f1.push(person.date_time as i64);
                f2.push(person.id as i64 % 16);
            }
            NexmarkEvent::Auction(auction) => {
                kind.push(1);
                id.push(auction.id as i64);
                f1.push(auction.seller as i64);
                f2.push(auction.reserve as i64);
            }
            NexmarkEvent::Bid(bid) => {
                kind.push(2);
                id.push(bid.auction as i64);
                f1.push(bid.bidder as i64);
                f2.push(bid.price as i64);
            }
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("kind", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
        Field::new("f1", DataType::Int64, false),
        Field::new("f2", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(kind)),
            Arc::new(Int64Array::from(id)),
            Arc::new(Int64Array::from(f1)),
            Arc::new(Int64Array::from(f2)),
        ],
    )
    .unwrap();
    ArrowZSet::new(batch, vec![1; event_count])
}

#[test]
fn oracle_shuffle_transport_modes_match_tpch_batch() {
    let dataset = generate_tpch_dataset(7);
    let lineitem = dataset.get("lineitem").unwrap();
    let epochs = split_epochs(lineitem, 1_024);
    assert_transport_modes_match(&epochs);
}

#[test]
fn oracle_shuffle_transport_modes_match_nexmark_batch() {
    let epochs = split_epochs(&nexmark_epoch_batch(11, 1_024), 128);
    assert_transport_modes_match(&epochs);
}

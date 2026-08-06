#![no_main]
use arrow::datatypes::Schema;
use libfuzzer_sys::fuzz_target;
use rockstream_connectors::postgres_cdc::{CdcWireFormat, PostgresCdcSource};
use rockstream_types::ids::ConnectorId;
use std::sync::Arc;

fuzz_target!(|data: &[u8]| {
    let schema = Arc::new(Schema::empty());
    let mut pg_source =
        PostgresCdcSource::new(ConnectorId(1), schema.clone(), CdcWireFormat::PgOutput);
    let _ = pg_source.decode_and_enqueue(data);

    let mut wal_source = PostgresCdcSource::new(ConnectorId(2), schema, CdcWireFormat::Wal2Json);
    let _ = wal_source.decode_and_enqueue(data);
});

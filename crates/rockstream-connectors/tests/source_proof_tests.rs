//! Integration and proof tests for source connectors (v0.28).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::datatypes::{DataType, Field, Schema};
use rockstream_connectors::{
    OffsetToken, PollDeltaResult, S3Source, SnapshotStream, SourceConnector, SourceEpochRegistry,
    SourceError,
};
use rockstream_ops::time_window::TumbleWindowOp;
use rockstream_plan::LateDataPolicy;
use rockstream_types::arrow_batch::split_weight_column;
use rockstream_types::connector::PartitionFilter;
use rockstream_types::frontier::{FreshnessToken, SourceProgress};
use rockstream_types::ids::{ConnectorId, SourceId};
use rockstream_types::timestamp::Epoch;

use object_store::local::LocalFileSystem;
use rockstream_storage::{keys::CatalogType, CatalogKeyEncoder, ShardDb, WriteBatch};
use tempfile::TempDir;

use hmac::{Hmac, Mac};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers::{core::WaitFor, Image};

struct RecordedSource {
    schema: SchemaRef,
    partitions: BTreeMap<u64, Vec<(i64, Vec<i64>)>>,
    watermarks: BTreeMap<u64, i64>,
    paused: bool,
    last_committed: Option<(Epoch, OffsetToken)>,
}

impl RecordedSource {
    fn new(schema: SchemaRef, partitions: &[u64]) -> Self {
        Self {
            schema,
            partitions: partitions
                .iter()
                .map(|partition| (*partition, vec![]))
                .collect(),
            watermarks: partitions
                .iter()
                .map(|partition| (*partition, i64::MIN))
                .collect(),
            paused: false,
            last_committed: None,
        }
    }

    fn add_record(&mut self, partition: u64, timestamp: i64, values: Vec<i64>) {
        self.partitions
            .entry(partition)
            .or_default()
            .push((timestamp, values));
        self.watermarks.entry(partition).or_insert(i64::MIN);
    }

    fn get_partition_offset(&self, token: &OffsetToken, partition: u64) -> Option<u64> {
        if token.as_bytes().is_empty() {
            return Some(0);
        }
        serde_json::from_slice::<BTreeMap<u64, u64>>(token.as_bytes())
            .ok()
            .map(|offsets| offsets.get(&partition).copied().unwrap_or(0))
    }

    fn last_committed(&self) -> Option<(Epoch, OffsetToken)> {
        self.last_committed.clone()
    }
}

impl SourceConnector for RecordedSource {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        Ok(self.schema.clone())
    }

    fn start_snapshot(
        &mut self,
        _frontier: Epoch,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        Ok(SnapshotStream::new(vec![]))
    }

    fn poll_delta(
        &mut self,
        after: OffsetToken,
        _max_bytes: usize,
        credits: usize,
        _partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        if self.paused || credits == 0 {
            return Ok(PollDeltaResult {
                batches: vec![],
                new_offset: after,
                watermark: None,
            });
        }
        let mut offsets = if after.as_bytes().is_empty() {
            BTreeMap::new()
        } else {
            serde_json::from_slice(after.as_bytes()).unwrap()
        };
        let mut rows = vec![];
        for (partition, records) in &self.partitions {
            for (index, (timestamp, values)) in records
                .iter()
                .enumerate()
                .skip(offsets.get(partition).copied().unwrap_or(0) as usize)
            {
                if rows.len() == credits {
                    break;
                }
                rows.push((*timestamp, values.clone()));
                offsets.insert(*partition, (index + 1) as u64);
                self.watermarks
                    .entry(*partition)
                    .and_modify(|watermark| *watermark = (*watermark).max(*timestamp));
            }
            if rows.len() == credits {
                break;
            }
        }
        let batches = if rows.is_empty() {
            vec![]
        } else {
            use arrow::array::Int64Array;
            use rockstream_types::arrow_batch::append_weight_column;
            let columns = (0..self.schema.fields().len())
                .map(|column| {
                    Arc::new(Int64Array::from(
                        rows.iter()
                            .map(|(_, values)| values[column])
                            .collect::<Vec<_>>(),
                    )) as arrow::array::ArrayRef
                })
                .collect();
            let batch =
                arrow::record_batch::RecordBatch::try_new(self.schema.clone(), columns).unwrap();
            vec![append_weight_column(batch, &vec![1; rows.len()]).unwrap()]
        };
        Ok(PollDeltaResult {
            batches,
            new_offset: OffsetToken::new(serde_json::to_vec(&offsets).unwrap()),
            watermark: self
                .watermarks
                .values()
                .copied()
                .min()
                .filter(|watermark| *watermark != i64::MIN)
                .map(|watermark| watermark as u64),
        })
    }

    fn commit_offset(&mut self, epoch: Epoch, offset: OffsetToken) -> Result<(), SourceError> {
        self.last_committed = Some((epoch, offset));
        Ok(())
    }

    fn pause(&mut self, _reason: String) -> Result<(), SourceError> {
        self.paused = true;
        Ok(())
    }

    fn resume(&mut self) -> Result<(), SourceError> {
        self.paused = false;
        Ok(())
    }
}

#[test]
fn test_kafka_clock_skew_window_closure() {
    // Schema: [t: Int64, v: Int64]
    let schema = Arc::new(Schema::new(vec![
        Field::new("t", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));

    let mut source = RecordedSource::new(schema.clone(), &[0, 1]);

    // Partition 0 is skewed (slower), starting with a record at t=500
    source.add_record(0, 500, vec![500, 10]);
    // Partition 1 is faster, starting with a record at t=1500
    source.add_record(1, 1500, vec![1500, 20]);

    // Create a TumbleWindowOp with window size 1000
    // Time column is at index 0 (the "t" field)
    let op = TumbleWindowOp::new(schema, 0, 1000, LateDataPolicy::Drop);

    // ─── Poll and Process first batch (respecting credit limit of 2) ───
    let res1 = source
        .poll_delta(OffsetToken::new(vec![]), 1024, 2, None)
        .unwrap();
    assert_eq!(res1.batches.len(), 1);

    // Under partition skew:
    // partition 0 watermark = 500
    // partition 1 watermark = 1500
    // min(500, 1500) = 500. So global watermark is Some(500)
    assert_eq!(res1.watermark, Some(500));

    // Construct a FreshnessToken representing this source's progress in causal frontier
    let mut source_progress = BTreeMap::new();
    source_progress.insert(
        SourceId(1),
        SourceProgress::new(1, res1.watermark.map(|w| w as i64)),
    );
    let token1 = FreshnessToken::new(source_progress, 42);

    // Attach frontier token to the polled batch and process through TumbleWindowOp
    let input1 = rockstream_ops::zset::ArrowZSet::new(
        res1.batches[0].clone(),
        split_weight_column(&res1.batches[0]).unwrap().1,
    )
    .with_frontier(token1);

    let out1 = op.process_epoch(input1, 1).unwrap();
    // Both records (t=500, t=1500) are processed and output
    assert_eq!(out1.num_rows(), 2);

    // Check window_ids are 0 and 1000
    let col0 = out1
        .data
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let window_ids: Vec<i64> = (0..out1.num_rows()).map(|i| col0.value(i)).collect();
    assert!(window_ids.contains(&0));
    assert!(window_ids.contains(&1000));

    // The operator's watermark is 500 < 1000, so window [0, 1000) is NOT closed/finalized.
    assert_eq!(op.watermark_ms(), 500);

    // ─── Partition 0 advances past t=1000 ───
    source.add_record(0, 1100, vec![1100, 11]);

    let res2 = source
        .poll_delta(res1.new_offset.clone(), 1024, 1, None)
        .unwrap();
    assert_eq!(res2.batches.len(), 1);

    // Now partition 0 watermark is 1100, partition 1 watermark remains 1500.
    // min(1100, 1500) = 1100. Global watermark is Some(1100)
    assert_eq!(res2.watermark, Some(1100));

    let mut source_progress2 = BTreeMap::new();
    source_progress2.insert(
        SourceId(1),
        SourceProgress::new(2, res2.watermark.map(|w| w as i64)),
    );
    let token2 = FreshnessToken::new(source_progress2, 43);

    let input2 = rockstream_ops::zset::ArrowZSet::new(
        res2.batches[0].clone(),
        split_weight_column(&res2.batches[0]).unwrap().1,
    )
    .with_frontier(token2);

    let _out2 = op.process_epoch(input2, 2).unwrap();
    // Watermark is now 1100 >= 1000, so window [0, 1000) closes
    assert_eq!(op.watermark_ms(), 1100);
}

#[test]
fn test_kafka_offset_tracking_causal_frontier() {
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));

    let mut source = RecordedSource::new(schema, &[0, 1]);
    source.add_record(0, 100, vec![10]);
    source.add_record(1, 200, vec![20]);

    // Poll first epoch: 1 record from partition 0
    let start_token = OffsetToken::new(vec![]);
    let res1 = source.poll_delta(start_token, 1024, 1, None).unwrap();
    assert_eq!(res1.batches.len(), 1);

    // Check offsets in token
    let p0_off = source.get_partition_offset(&res1.new_offset, 0).unwrap();
    let p1_off = source.get_partition_offset(&res1.new_offset, 1).unwrap();
    assert_eq!(p0_off, 1);
    assert_eq!(p1_off, 0);

    // Commit offset for epoch 1
    source.commit_offset(1, res1.new_offset.clone()).unwrap();

    // Poll second epoch: 1 record from partition 1
    let res2 = source
        .poll_delta(res1.new_offset.clone(), 1024, 1, None)
        .unwrap();
    assert_eq!(res2.batches.len(), 1);

    let p0_off2 = source.get_partition_offset(&res2.new_offset, 0).unwrap();
    let p1_off2 = source.get_partition_offset(&res2.new_offset, 1).unwrap();
    assert_eq!(p0_off2, 1);
    assert_eq!(p1_off2, 1);

    // Commit offset for epoch 2
    source.commit_offset(2, res2.new_offset.clone()).unwrap();

    // Verify last committed matches
    let (epoch, commit_token) = source.last_committed().unwrap();
    assert_eq!(epoch, 2);

    let comm_p0 = source.get_partition_offset(&commit_token, 0).unwrap();
    let comm_p1 = source.get_partition_offset(&commit_token, 1).unwrap();
    assert_eq!(comm_p0, 1);
    assert_eq!(comm_p1, 1);
}

#[tokio::test]
async fn test_source_offset_replay_lfs() {
    let dir = TempDir::new().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Arc::new(
        ShardDb::builder("shard", store.clone())
            .build()
            .await
            .unwrap(),
    );

    let connector_id = ConnectorId(123);
    let mut registry = SourceEpochRegistry::new(connector_id);

    // Schema: [v: Int64]
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let mut source = RecordedSource::new(schema, &[0, 1]);

    // Add records to partitions
    source.add_record(0, 100, vec![10]);
    source.add_record(1, 200, vec![20]);

    // Poll first epoch: 1 record from partition 0
    let res1 = source
        .poll_delta(OffsetToken::new(vec![]), 1024, 1, None)
        .unwrap();
    assert_eq!(res1.batches.len(), 1);

    // Save offsets partition-by-partition to registry
    let mut part_offsets = BTreeMap::new();
    for &pid in &[0u64, 1u64] {
        let off = source
            .get_partition_offset(&res1.new_offset, pid)
            .unwrap_or(0);
        part_offsets.insert(pid, OffsetToken::new(serde_json::to_vec(&off).unwrap()));
    }

    let entry = registry.prepare_commit(part_offsets).unwrap();

    // Save entry to ShardDb
    let mut suffix = b"epoch_map/".to_vec();
    suffix.extend_from_slice(&entry.source_epoch.to_be_bytes());
    let key = CatalogKeyEncoder::encode_with_suffix(
        CatalogType::Connector,
        0,
        connector_id.0 as u128,
        &suffix,
    );
    let val = serde_json::to_vec(&entry.partition_offsets).unwrap();

    let mut batch = WriteBatch::new();
    batch.put(&key, &val);
    db.write_batch(batch).await.unwrap();
    db.flush().await.unwrap();

    // Commit epoch in registry
    registry.commit_epoch(entry).unwrap();

    // Simulate crash by closing the DB and recreating everything
    Arc::try_unwrap(db)
        .ok()
        .expect("db has single owner")
        .close()
        .await
        .unwrap();

    // Reopen ShardDb
    let db2 = Arc::new(ShardDb::builder("shard", store).build().await.unwrap());

    // Recover by scanning prefix
    let prefix = CatalogKeyEncoder::encode_with_suffix(
        CatalogType::Connector,
        0,
        connector_id.0 as u128,
        b"epoch_map/",
    );

    let entries = db2.scan_prefix(&prefix).await.unwrap();
    assert!(!entries.is_empty());

    let mut highest_epoch = 0;
    let mut recovered_offsets = BTreeMap::new();
    for (key, val) in entries {
        if key.len() >= 8 {
            let start = key.len() - 8;
            let ep = u64::from_be_bytes(key[start..].try_into().unwrap());
            if ep > highest_epoch {
                highest_epoch = ep;
                recovered_offsets = serde_json::from_slice(&val).unwrap();
            }
        }
    }

    assert_eq!(highest_epoch, 1);

    // Restore registry
    let recovered_registry =
        SourceEpochRegistry::restore(connector_id, highest_epoch, recovered_offsets);

    // Build single resume poll token from recovered partition offsets
    let mut resume_map = BTreeMap::new();
    for (&pid, tok) in recovered_registry.recovery_offsets().unwrap() {
        let off: u64 = serde_json::from_slice(tok.as_bytes()).unwrap();
        resume_map.insert(pid, off);
    }
    let resume_token = OffsetToken::new(serde_json::to_vec(&resume_map).unwrap());

    // Verify it resumes from correct position (polled from partition 1 next)
    let res2 = source.poll_delta(resume_token, 1024, 1, None).unwrap();
    assert_eq!(res2.batches.len(), 1);
    let (data2, _) = split_weight_column(&res2.batches[0]).unwrap();
    let val_col = data2
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(val_col.value(0), 20); // partition 1 value (20) instead of partition 0 (10)
}

// ─── MinIO test helpers ──────────────────────────────────────────────────────

const MINIO_USER: &str = "minioadmin";
const MINIO_PASS: &str = "minioadmin";
const MINIO_BUCKET: &str = "source-test-bucket";

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .args(["info"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn sha256_hex(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn epoch_to_ymd_hms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sod = secs % 86400;
    let mut days = (secs / 86400) as u32;
    let h = (sod / 3600) as u32;
    let m = ((sod % 3600) / 60) as u32;
    let s = (sod % 60) as u32;
    let mut year = 1970u32;
    loop {
        let leap =
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
        let dy = if leap { 366 } else { 365 };
        if days < dy {
            break;
        }
        days -= dy;
        year += 1;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let dpm: [u32; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0u32;
    for &d in &dpm {
        if days < d {
            break;
        }
        days -= d;
        month += 1;
    }
    let day = days + 1;
    month += 1;
    (year, month, day, h, m, s)
}

async fn create_minio_bucket(port: u16, bucket: &str) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let (y, mo, d, hh, mm, ss) = epoch_to_ymd_hms(secs);
    let date = format!("{y:04}{mo:02}{d:02}");
    let datetime = format!("{y:04}{mo:02}{d:02}T{hh:02}{mm:02}{ss:02}Z");
    let host = format!("127.0.0.1:{port}");
    let region = "us-east-1";
    let empty_hash = sha256_hex(b"");
    let canonical = format!(
        "PUT\n/{bucket}\n\nhost:{host}\nx-amz-content-sha256:{empty_hash}\nx-amz-date:{datetime}\n\nhost;x-amz-content-sha256;x-amz-date\n{empty_hash}"
    );
    let canonical_hash = sha256_hex(canonical.as_bytes());
    let scope = format!("{date}/{region}/s3/aws4_request");
    let sts = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{canonical_hash}");
    let k1 = hmac_sha256(format!("AWS4{MINIO_PASS}").as_bytes(), date.as_bytes());
    let k2 = hmac_sha256(&k1, region.as_bytes());
    let k3 = hmac_sha256(&k2, b"s3");
    let signing_key = hmac_sha256(&k3, b"aws4_request");
    let sig = hex::encode(hmac_sha256(&signing_key, sts.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_USER}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}"
    );
    let resp = reqwest::Client::new()
        .put(format!("http://{host}/{bucket}"))
        .header("Host", &host)
        .header("X-Amz-Content-Sha256", &empty_hash)
        .header("X-Amz-Date", &datetime)
        .header("Authorization", &auth)
        .header("Content-Length", "0")
        .send()
        .await
        .expect("CreateBucket PUT request failed");
    let status = resp.status();
    assert!(
        status.is_success() || status.as_u16() == 409,
        "CreateBucket failed: {status}"
    );
}

#[derive(Debug, Clone)]
pub struct MinIO2024 {
    env_vars: HashMap<String, String>,
}

impl Default for MinIO2024 {
    fn default() -> Self {
        let mut env_vars = HashMap::new();
        env_vars.insert("MINIO_CONSOLE_ADDRESS".to_owned(), ":9001".to_owned());
        Self { env_vars }
    }
}

impl Image for MinIO2024 {
    fn name(&self) -> &str {
        "minio/minio"
    }

    fn tag(&self) -> &str {
        "RELEASE.2024-11-07T00-52-20Z"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("API:")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        &self.env_vars
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        vec!["server", "/data"]
    }
}

async fn start_minio() -> (testcontainers::ContainerAsync<MinIO2024>, u16) {
    let container = MinIO2024::default()
        .start()
        .await
        .expect("failed to start MinIO container; is Docker running?");
    let port = container.get_host_port_ipv4(9000).await.unwrap();
    create_minio_bucket(port, MINIO_BUCKET).await;
    (container, port)
}

fn minio_object_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://127.0.0.1:{port}"))
            .with_bucket_name(MINIO_BUCKET)
            .with_access_key_id(MINIO_USER)
            .with_secret_access_key(MINIO_PASS)
            .with_region("us-east-1")
            .with_allow_http(true)
            .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
            .build()
            .expect("failed to build S3 object store for MinIO"),
    )
}

#[tokio::test]
async fn test_s3_source_minio_tc() {
    if !docker_available() {
        eprintln!("SKIP test_s3_source_minio_tc: Docker not available");
        return;
    }

    let (_container, port) = start_minio().await;
    let store = minio_object_store(port);

    // Upload files to MinIO bucket: JSON array formatted records
    let file1_data = b"[[1, 10], [2, 20]]";
    let file2_data = b"[[3, 30]]";

    store
        .put(
            &object_store::path::Path::from("file1.json"),
            file1_data.to_vec().into(),
        )
        .await
        .unwrap();
    store
        .put(
            &object_store::path::Path::from("file2.json"),
            file2_data.to_vec().into(),
        )
        .await
        .unwrap();

    // Create S3Source
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let mut source = S3Source::new(ConnectorId(202), schema).with_object_store(store, None);

    // Poll first batch (1 record limit)
    let res1 = source
        .poll_delta(OffsetToken::new(vec![]), 1024, 1, None)
        .unwrap();
    assert_eq!(res1.batches.len(), 1);
    let (data1, _) = split_weight_column(&res1.batches[0]).unwrap();
    assert_eq!(data1.num_rows(), 1);
    let col_a = data1
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(col_a.value(0), 1);

    let pos1 = source.get_file_position(&res1.new_offset).unwrap();
    assert_eq!(pos1, (0, 1)); // file 0, line 1

    // Poll next batch (10 records limit)
    let res2 = source
        .poll_delta(res1.new_offset.clone(), 1024, 10, None)
        .unwrap();
    assert_eq!(res2.batches.len(), 1);
    let (data2, _) = split_weight_column(&res2.batches[0]).unwrap();
    assert_eq!(data2.num_rows(), 2); // remaining [2, 20] from file1 and [3, 30] from file2
    let col_a2 = data2
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(col_a2.value(0), 2);
    assert_eq!(col_a2.value(1), 3);

    let pos2 = source.get_file_position(&res2.new_offset).unwrap();
    assert_eq!(pos2, (2, 0)); // both files completed

    // Commit offsets
    source.commit_offset(10, res2.new_offset.clone()).unwrap();
    let (epoch, commit_tok) = source.last_committed().unwrap();
    assert_eq!(epoch, 10);
    assert_eq!(commit_tok, res2.new_offset);
}

// ─── ChaosSource for SimRuntime Chaos Test ───────────────────────────────────

struct ChaosSource<'a, S: SourceConnector> {
    inner: S,
    sim_rt: &'a rockstream_sim::SimRuntime,
}

impl<'a, S: SourceConnector> SourceConnector for ChaosSource<'a, S> {
    fn discover_schema(&self) -> Result<SchemaRef, SourceError> {
        self.inner.discover_schema()
    }

    fn start_snapshot(
        &mut self,
        frontier: Epoch,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<SnapshotStream, SourceError> {
        self.inner.start_snapshot(frontier, partition_filter)
    }

    fn poll_delta(
        &mut self,
        after: OffsetToken,
        max_bytes: usize,
        credits_available: usize,
        partition_filter: Option<PartitionFilter>,
    ) -> Result<PollDeltaResult, SourceError> {
        // Inject pause fault with 10% probability
        if self.sim_rt.random_bool(0.1) {
            self.inner.pause("chaos pause".to_string())?;
        }

        // Inject connection drop / error with 10% probability
        if self.sim_rt.random_bool(0.1) {
            return Err(SourceError::Io(
                "RS-4001: source connection failed (chaos)".to_string(),
            ));
        }

        // Resume with 20% probability
        if self.sim_rt.random_bool(0.2) {
            let _ = self.inner.resume();
        }

        self.inner
            .poll_delta(after, max_bytes, credits_available, partition_filter)
    }

    fn commit_offset(&mut self, epoch: Epoch, offset: OffsetToken) -> Result<(), SourceError> {
        self.inner.commit_offset(epoch, offset)
    }

    fn pause(&mut self, reason: String) -> Result<(), SourceError> {
        self.inner.pause(reason)
    }

    fn resume(&mut self) -> Result<(), SourceError> {
        self.inner.resume()
    }
}

#[test]
fn test_source_coordination_sim() {
    use rockstream_sim::{SeedOutcome, SoakRunner};

    let mut runner = SoakRunner::new();
    const SEEDS: u64 = 20;

    for seed in 0..SEEDS {
        runner.run_seed(seed, |sim_rt| {
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));

            // Set up a KafkaSource and populate it with deterministic data based on seed
            let mut base_source = RecordedSource::new(schema.clone(), &[0, 1]);
            for i in 0..10 {
                base_source.add_record(0, i * 100, vec![i]);
                base_source.add_record(1, i * 100 + 50, vec![100 + i]);
            }

            let mut source = ChaosSource {
                inner: base_source,
                sim_rt,
            };

            // Process all records by polling under chaos
            let mut current_offset = OffsetToken::new(vec![]);
            let mut processed_values = Vec::new();
            let mut retries = 0;

            while retries < 500 && processed_values.len() < 20 {
                // Poll delta (respecting credits_available = 5)
                match source.poll_delta(current_offset.clone(), 1024, 5, None) {
                    Ok(res) => {
                        current_offset = res.new_offset;
                        for batch in res.batches {
                            let (data, _) = split_weight_column(&batch).unwrap();
                            let col = data
                                .column(0)
                                .as_any()
                                .downcast_ref::<arrow::array::Int64Array>()
                                .unwrap();
                            for i in 0..col.len() {
                                processed_values.push(col.value(i));
                            }
                        }
                    }
                    Err(SourceError::Io(ref msg)) if msg.contains("RS-4001") => {
                        // Handle simulated connection drop by retrying (zero-loss retry path)
                        retries += 1;
                    }
                    Err(e) => panic!("unexpected error: {e:?}"),
                }
                sim_rt.advance_time(std::time::Duration::from_millis(1));
            }

            // Verify exactly-once ingestion of all 20 records (values 0..10 and 100..110)
            assert_eq!(
                processed_values.len(),
                20,
                "Should successfully ingest all 20 records despite chaos"
            );

            let mut expected: Vec<i64> = (0..10).chain(100..110).collect();
            processed_values.sort();
            expected.sort();
            assert_eq!(
                processed_values, expected,
                "Ingested values must exactly match reference set"
            );

            SeedOutcome::Pass
        });
    }

    assert_eq!(runner.seeds_run(), SEEDS);
    assert!(
        runner.all_passed(),
        "All simulation seeds should pass successfully"
    );
}

#[test]
fn test_s3_file_pointer_tracking() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let mut source = S3Source::new(ConnectorId(202), schema.clone());

    // Add files to the mock bucket
    source.add_file("file1.json".to_string(), vec![vec![1, 10], vec![2, 20]]);
    source.add_file("file2.json".to_string(), vec![vec![3, 30]]);

    // Poll with 1 credit limit (first row of file1)
    let token_start = OffsetToken::new(vec![]);
    let res1 = source
        .poll_delta(token_start.clone(), 1024, 1, None)
        .unwrap();
    assert_eq!(res1.batches.len(), 1);
    let (data1, _) = split_weight_column(&res1.batches[0]).unwrap();
    assert_eq!(data1.num_rows(), 1);

    let pos1 = source.get_file_position(&res1.new_offset).unwrap();
    assert_eq!(pos1, (0, 1)); // file 0, line 1

    // Commit offset
    source.commit_offset(1, res1.new_offset.clone()).unwrap();

    // Recreate source to test resuming from last committed offset token
    let mut source2 = S3Source::new(ConnectorId(202), schema);
    source2.add_file("file1.json".to_string(), vec![vec![1, 10], vec![2, 20]]);
    source2.add_file("file2.json".to_string(), vec![vec![3, 30]]);

    // Resume from the committed token
    let res2 = source2
        .poll_delta(res1.new_offset.clone(), 1024, 10, None)
        .unwrap();
    assert_eq!(res2.batches.len(), 1);
    let (data2, _) = split_weight_column(&res2.batches[0]).unwrap();
    assert_eq!(data2.num_rows(), 2); // 1 remaining from file1, 1 from file2
    let col_a = data2
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(col_a.value(0), 2);
    assert_eq!(col_a.value(1), 3);

    let pos2 = source2.get_file_position(&res2.new_offset).unwrap();
    assert_eq!(pos2, (2, 0)); // finished both files
}

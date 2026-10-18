//! Exact, single-sample worker recovery evidence shared with qualification.
use std::collections::BTreeMap;

use rockstream_types::{audit::AuditEvent, lease::ShardLease};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const MANIFEST: &str =
    include_str!("../../../../docs/testing/worker-failure-kafka-minio-v1.json");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerObservation {
    pub observed_at_ms: u64,
    pub process_id: u32,
    pub lease: ShardLease,
    pub source_epoch: u64,
    pub source_offsets: BTreeMap<u64, u64>,
    pub view_frontier: u64,
    pub weighted_output: Value,
    pub relation: Value,
    pub in_flight_epochs: usize,
    pub checkpoint_history_entries: usize,
    pub sink_progress: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRecoveryEvidence {
    pub scenario_id: String,
    pub manifest_sha256: String,
    pub revision: String,
    pub sample_count: u64,
    pub fault_at_ms: u64,
    pub detection: AuditEvent,
    pub ownership_observed_at_ms: u64,
    pub first_correct_query_at_ms: u64,
    pub query_frontier: u64,
    pub query_rows: Vec<String>,
    pub docker_command_ms: u64,
    pub before: WorkerObservation,
    pub after: WorkerObservation,
    pub control_lease: ShardLease,
    pub sink_records_before: Value,
    pub sink_records: Value,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RecoveryFailure {
    MissingObservation,
    Candidate,
    Timings,
    Identity,
    Ownership,
    Progress,
    ViewOutput,
    SinkOutput,
    ApplicationState,
    DetectionDeadline,
    OwnershipDeadline,
    QueryDeadline,
}

/// Durations are derived from captured application events, never supplied SLO samples.
pub fn recovery_durations(e: &WorkerRecoveryEvidence) -> Value {
    json!({
        "detection": e.detection.timestamp_ms.checked_sub(e.fault_at_ms),
        "ownership": e.ownership_observed_at_ms.checked_sub(e.fault_at_ms),
        "query": e.first_correct_query_at_ms.checked_sub(e.fault_at_ms),
    })
}

pub fn qualification_record(observations: Value, candidate: &Value, durations: Value) -> Value {
    let result = serde_json::from_value::<WorkerRecoveryEvidence>(observations.clone())
        .map_err(|_| RecoveryFailure::MissingObservation)
        .and_then(|e| {
            if candidate["commit_sha"] != e.revision {
                return Err(RecoveryFailure::Candidate);
            }
            validate(&e)?;
            if durations != recovery_durations(&e) {
                return Err(RecoveryFailure::Timings);
            }
            Ok(())
        });
    json!({
        "candidate": candidate,
        "observations": observations,
        "recovery_durations_ms": durations,
        "status": if result.is_ok() { "PASSED" } else { "FAILED" },
        "error": result.err().map(|error| format!("{error:?}")),
    })
}

/// Apply qualification controls to the same complete observations used for proof.
pub fn qualification_records(e: &WorkerRecoveryEvidence, candidate: &Value) -> Value {
    let observations = serde_json::to_value(e).unwrap();
    let durations = recovery_durations(e);
    let application = qualification_record(observations.clone(), candidate, durations.clone());
    let mut controls = serde_json::Map::new();
    for name in [
        "missing_event",
        "constant_timings",
        "stale_query",
        "changed_value",
        "duplicate_output",
        "helper_only_status",
    ] {
        let mut corrupted = observations.clone();
        let mut timings = durations.clone();
        match name {
            "missing_event" => {
                corrupted.as_object_mut().unwrap().remove("detection");
            }
            "constant_timings" => {
                timings = json!({"detection":0,"ownership":0,"query":0});
            }
            "stale_query" => {
                corrupted["query_frontier"] = json!(3);
            }
            "changed_value" => {
                corrupted["query_rows"] = json!(["1\twrong"]);
            }
            "duplicate_output" => {
                corrupted["sink_records"]
                    .as_array_mut()
                    .unwrap()
                    .push(e.sink_records[2].clone());
            }
            "helper_only_status" => {
                corrupted["after"]["process_id"] = json!(e.before.process_id);
            }
            _ => unreachable!(),
        }
        controls.insert(
            name.into(),
            qualification_record(corrupted, candidate, timings),
        );
    }
    json!({"application": application, "negative_controls": controls})
}

/// Reconstruct independently of the production view materializer.
fn relation(records: &Value) -> Option<Value> {
    let mut weights = BTreeMap::<String, (Value, i64)>::new();
    for record in records.as_array()? {
        for row in record.get(1)?.get("rows")?.as_array()? {
            let values = row.get("values")?.clone();
            let entry = weights.entry(values.to_string()).or_insert((values, 0));
            entry.1 = entry.1.checked_add(row.get("weight")?.as_i64()?)?;
        }
    }
    Some(json!(weights
        .into_values()
        .filter(|(_, w)| *w != 0)
        .collect::<Vec<_>>()))
}

fn staging(progress: &Value) -> Option<Value> {
    let mut normalized = progress.clone();
    let pending = &progress["pending"]["PreCommitted"]["pending_handle"];
    let bytes = pending
        .as_array()?
        .iter()
        .map(|v| u8::try_from(v.as_u64()?).ok())
        .collect::<Option<Vec<_>>>()?;
    let mut handle: Value = serde_json::from_slice(&bytes).ok()?;
    handle["payload"] = serde_json::from_str(handle["payload"].as_str()?).ok()?;
    normalized["pending"]["PreCommitted"]["pending_handle"] = handle;
    Some(normalized)
}

/// Validate complete observations, never counts or a digest in place of rows.
pub fn validate(e: &WorkerRecoveryEvidence) -> Result<(), RecoveryFailure> {
    use RecoveryFailure::*;
    let m: Value = serde_json::from_str(MANIFEST).expect("checked-in manifest");
    if e.scenario_id != m["scenario_id"].as_str().unwrap()
        || e.manifest_sha256 != format!("{:x}", Sha256::digest(MANIFEST))
        || e.sample_count != 1
        || e.revision.len() != 40
        || !e.revision.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(Identity);
    }
    if e.before.lease.worker_id.0 != 1
        || e.after.lease.worker_id.0 != 2
        || e.before.lease.shard_id.0 != 126
        || e.after.lease.shard_id != e.before.lease.shard_id
        || e.before.lease.lease_token.0 != 1
        || e.after.lease.lease_token.0 != 2
        || e.control_lease != e.after.lease
        || e.before.process_id == e.after.process_id
        || e.before.process_id == 0
        || e.after.process_id == 0
    {
        return Err(Ownership);
    }
    if e.before.source_epoch != 3
        || e.before.source_offsets != BTreeMap::from([(0, 3)])
        || e.before.view_frontier != 3
        || e.after.source_epoch != 5
        || e.after.source_offsets != BTreeMap::from([(0, 5)])
        || e.after.view_frontier != 5
        || e.query_frontier != 5
    {
        return Err(Progress);
    }
    if e.before.weighted_output != json!(&m["expected_weighted_output"].as_array().unwrap()[..3])
        || e.after.weighted_output != m["expected_weighted_output"]
        || e.before.relation != json!([[[{"Int64":2},{"Utf8":"b"}],1]])
        || e.after.relation != m["expected_relation"]
        || e.query_rows != ["1\tc"]
    {
        return Err(ViewOutput);
    }
    if e.sink_records_before != json!(&m["expected_sink"].as_array().unwrap()[..2])
        || e.sink_records != m["expected_sink"]
        || relation(&e.sink_records).as_ref() != Some(&m["expected_relation"])
    {
        return Err(SinkOutput);
    }
    if e.before.in_flight_epochs != 0
        || e.after.in_flight_epochs != 0
        || e.before.checkpoint_history_entries != 3
        || e.after.checkpoint_history_entries != 5
        || staging(&e.before.sink_progress)
            != Some(
                json!({"view":126,"columns":2,"topic":"worker-failure-output","snapshot_complete":true,"view_epoch":3,"transaction":3,
            "pending":{"PreCommitted":{"staged_rows":1,"pending_handle":{"connector_id":126,"topic":"worker-failure-output","epoch":2,"payload":m["expected_sink"][2][1]}}}}),
            )
        || e.after.sink_progress
            != json!({"view":126,"columns":2,"topic":"worker-failure-output","snapshot_complete":true,"view_epoch":5,"transaction":5,"pending":null})
    {
        return Err(ApplicationState);
    }
    if e.detection.action != "worker.disconnected"
        || e.detection.resource != "worker-1"
        || e.detection.actor != "control"
        || e.detection.error_code.is_some()
        || e.detection.detail.is_some()
        || e.before.observed_at_ms > e.fault_at_ms
        || e.detection.timestamp_ms < e.fault_at_ms
        || e.ownership_observed_at_ms < e.detection.timestamp_ms
        || e.first_correct_query_at_ms < e.ownership_observed_at_ms
        || e.after.observed_at_ms > e.first_correct_query_at_ms
    {
        return Err(Progress);
    }
    for (at, limit, error) in [
        (e.detection.timestamp_ms, 5_000, DetectionDeadline),
        (e.ownership_observed_at_ms, 30_000, OwnershipDeadline),
        (e.first_correct_query_at_ms, 60_000, QueryDeadline),
    ] {
        if at - e.fault_at_ms > limit {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rockstream_types::ids::{LeaseToken, ShardId, WorkerId};

    // Synthetic validator fixture only; the Docker case constructs evidence
    // exclusively from running source, view, control, and sink observations.
    fn fixture() -> WorkerRecoveryEvidence {
        let m: Value = serde_json::from_str(MANIFEST).unwrap();
        let pending_handle = serde_json::to_vec(&json!({"connector_id":126,"topic":"worker-failure-output","epoch":2,"payload":m["expected_sink"][2][1].to_string()})).unwrap();
        let before = WorkerObservation {
            observed_at_ms: 990,
            process_id: 11,
            lease: ShardLease::new(ShardId(126), WorkerId(1), LeaseToken(1)),
            source_epoch: 3,
            source_offsets: BTreeMap::from([(0, 3)]),
            view_frontier: 3,
            weighted_output: json!(&m["expected_weighted_output"].as_array().unwrap()[..3]),
            relation: json!([[[{"Int64":2},{"Utf8":"b"}],1]]),
            in_flight_epochs: 0,
            checkpoint_history_entries: 3,
            sink_progress: json!({"view":126,"columns":2,"topic":"worker-failure-output","snapshot_complete":true,"view_epoch":3,"transaction":3,"pending":{"PreCommitted":{"staged_rows":1,"pending_handle":pending_handle}}}),
        };
        let after = WorkerObservation {
            observed_at_ms: 1400,
            process_id: 22,
            lease: ShardLease::new(ShardId(126), WorkerId(2), LeaseToken(2)),
            source_epoch: 5,
            source_offsets: BTreeMap::from([(0, 5)]),
            view_frontier: 5,
            weighted_output: m["expected_weighted_output"].clone(),
            relation: m["expected_relation"].clone(),
            in_flight_epochs: 0,
            checkpoint_history_entries: 5,
            sink_progress: json!({"view":126,"columns":2,"topic":"worker-failure-output","snapshot_complete":true,"view_epoch":5,"transaction":5,"pending":null}),
        };
        let mut detection = AuditEvent::now("control", "worker.disconnected", "worker-1");
        detection.timestamp_ms = 1100;
        WorkerRecoveryEvidence {
            scenario_id: m["scenario_id"].as_str().unwrap().into(),
            manifest_sha256: format!("{:x}", Sha256::digest(MANIFEST)),
            revision: "a".repeat(40),
            sample_count: 1,
            fault_at_ms: 1000,
            detection,
            ownership_observed_at_ms: 1200,
            first_correct_query_at_ms: 1500,
            query_frontier: 5,
            query_rows: vec!["1\tc".into()],
            docker_command_ms: 99,
            control_lease: after.lease.clone(),
            before,
            after,
            sink_records_before: json!(&m["expected_sink"].as_array().unwrap()[..2]),
            sink_records: m["expected_sink"].clone(),
        }
    }

    #[test]
    fn exact_evidence_round_trips_and_validates() {
        let e = fixture();
        let encoded = serde_json::to_string(&e).unwrap();
        let decoded = serde_json::from_str(&encoded).unwrap();
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            serde_json::to_value(&decoded).unwrap()
        );
        assert_eq!(validate(&decoded), Ok(()));
    }

    #[test]
    fn complete_qualification_records_reject_substituted_observations() {
        let e = fixture();
        let candidate = json!({"commit_sha":"a".repeat(40),"tracked_diff_sha256":"fixture"});
        let observations = serde_json::to_value(&e).unwrap();
        let expected = json!({
            "candidate": candidate, "observations": observations,
            "recovery_durations_ms": {"detection":100,"ownership":200,"query":500},
            "status":"PASSED", "error":null,
        });
        assert_eq!(
            qualification_record(observations.clone(), &candidate, recovery_durations(&e)),
            expected
        );
        let mut missing = observations.clone();
        missing.as_object_mut().unwrap().remove("detection");
        let mut stale = observations.clone();
        stale["query_frontier"] = json!(3);
        let mut changed = observations.clone();
        changed["query_rows"] = json!(["1\twrong"]);
        let mut duplicate = observations.clone();
        duplicate["sink_records"]
            .as_array_mut()
            .unwrap()
            .push(e.sink_records[2].clone());
        let mut helper = observations.clone();
        helper["after"]["process_id"] = json!(11);
        let mut controls = serde_json::Map::new();
        for (name, data, timings, reason) in [
            (
                "missing_event",
                missing,
                recovery_durations(&e),
                "MissingObservation",
            ),
            (
                "constant_timings",
                observations.clone(),
                json!({"detection":0,"ownership":0,"query":0}),
                "Timings",
            ),
            ("stale_query", stale, recovery_durations(&e), "Progress"),
            (
                "changed_value",
                changed,
                recovery_durations(&e),
                "ViewOutput",
            ),
            (
                "duplicate_output",
                duplicate,
                recovery_durations(&e),
                "SinkOutput",
            ),
            (
                "helper_only_status",
                helper,
                recovery_durations(&e),
                "Ownership",
            ),
        ] {
            let record = json!({"candidate":candidate,"observations":data,
                "recovery_durations_ms":timings,"status":"FAILED","error":reason});
            assert_eq!(qualification_record(data, &candidate, timings), record);
            controls.insert(name.into(), record);
        }
        assert_eq!(
            qualification_records(&e, &candidate),
            json!({"application":expected,"negative_controls":controls})
        );
        let wrong_candidate = json!({"commit_sha":"b".repeat(40)});
        let mut rejected = expected;
        rejected["candidate"] = wrong_candidate.clone();
        rejected["status"] = json!("FAILED");
        rejected["error"] = json!("Candidate");
        assert_eq!(
            qualification_record(observations, &wrong_candidate, recovery_durations(&e)),
            rejected
        );
    }

    #[test]
    fn corrupt_returned_value_fails_view_output() {
        let mut e = fixture();
        e.after.weighted_output[3][2][1]["Utf8"] = json!("wrong");
        assert_eq!(validate(&e), Err(RecoveryFailure::ViewOutput));
    }

    #[test]
    fn duplicate_consumed_record_fails_sink_output() {
        let mut e = fixture();
        let duplicate = e.sink_records[2].clone();
        e.sink_records.as_array_mut().unwrap().push(duplicate);
        assert_eq!(validate(&e), Err(RecoveryFailure::SinkOutput));
    }

    #[test]
    fn suppressed_progress_fails_required_frontier() {
        let mut e = fixture();
        e.after.view_frontier = 4;
        assert_eq!(validate(&e), Err(RecoveryFailure::Progress));
    }

    #[test]
    fn missed_deadline_fails_query_deadline() {
        let mut e = fixture();
        e.first_correct_query_at_ms = 61_001;
        assert_eq!(validate(&e), Err(RecoveryFailure::QueryDeadline));
    }

    #[test]
    fn manifest_identity_and_application_state_are_checked() {
        let mut e = fixture();
        e.manifest_sha256 = "wrong".into();
        assert_eq!(validate(&e), Err(RecoveryFailure::Identity));
        let mut e = fixture();
        e.after.sink_progress["pending"] = json!({"uncommitted":true});
        assert_eq!(validate(&e), Err(RecoveryFailure::ApplicationState));
        let mut e = fixture();
        e.control_lease = e.before.lease.clone();
        assert_eq!(validate(&e), Err(RecoveryFailure::Ownership));
    }

    #[test]
    fn malformed_staging_and_each_event_deadline_fail_closed() {
        let mut e = fixture();
        e.before.sink_progress["pending"]["PreCommitted"]["pending_handle"] = json!([256]);
        assert_eq!(validate(&e), Err(RecoveryFailure::ApplicationState));
        let mut e = fixture();
        e.detection.timestamp_ms = 6_001;
        e.ownership_observed_at_ms = 6_002;
        e.first_correct_query_at_ms = 6_003;
        assert_eq!(validate(&e), Err(RecoveryFailure::DetectionDeadline));
        let mut e = fixture();
        e.ownership_observed_at_ms = 31_001;
        e.first_correct_query_at_ms = 31_002;
        assert_eq!(validate(&e), Err(RecoveryFailure::OwnershipDeadline));
    }
}

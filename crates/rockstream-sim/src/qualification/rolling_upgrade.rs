//! Exact observations for the three-worker release qualification.
use super::worker_failure::{relation, MANIFEST as RECOVERY_MANIFEST};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub const MANIFEST: &str =
    include_str!("../../../../docs/testing/rolling-upgrade-kafka-minio-v1.json");

pub fn manifest() -> Value {
    serde_json::from_str(MANIFEST).expect("checked-in upgrade manifest")
}

pub fn manifest_sha256() -> String {
    format!("{:x}", Sha256::digest(MANIFEST))
}

pub fn recovery_sha256() -> String {
    format!("{:x}", Sha256::digest(RECOVERY_MANIFEST))
}

fn hex(s: &str, length: usize) -> bool {
    s.len() == length && s.bytes().all(|b| b.is_ascii_hexdigit())
}

pub fn digest(reference: &str) -> Result<&str, &'static str> {
    let digest = reference
        .strip_prefix("sha256:")
        .or_else(|| reference.rsplit_once("@sha256:").map(|(_, digest)| digest))
        .ok_or("image must be pinned by SHA-256")?;
    if !hex(digest, 64) {
        return Err("image must be pinned by SHA-256");
    }
    Ok(digest)
}

pub fn preflight(old: &str, new: &str) -> Result<(), &'static str> {
    if digest(old)?.eq_ignore_ascii_case(digest(new)?) {
        return Err("old and candidate image digests must differ");
    }
    Ok(())
}

pub fn reject_downgrade(
    current: &str,
    target: &str,
    old: &str,
    new: &str,
) -> Result<(), &'static str> {
    if current != old || target != new {
        return Err("downgrade is not qualified; retry the candidate forward");
    }
    preflight(old, new)
}

/// Fixed #126 inserts/update/delete followed by a reset before each repetition.
pub fn operation(offset: u64) -> Value {
    let recovery: Value = serde_json::from_str(RECOVERY_MANIFEST).unwrap();
    let mut row = if offset % 6 == 5 {
        manifest()["reset"].clone()
    } else {
        recovery["history"][(offset % 6) as usize].clone()
    };
    row["timestamp"] = json!(offset + 1);
    row
}

/// Expected payloads come from #126's independent oracle, never from source input copies.
pub fn expected(epoch: u64) -> (Value, Value) {
    let recovery: Value = serde_json::from_str(RECOVERY_MANIFEST).unwrap();
    let mut output = Vec::new();
    let mut sink = Vec::new();
    for offset in 0..epoch {
        let mut row = if offset % 6 == 5 {
            manifest()["reset_output"].clone()
        } else {
            recovery["expected_weighted_output"][(offset % 6) as usize].clone()
        };
        row[0] = json!(offset + 1);
        sink.push(json!([offset.to_string(), {
            "version":1, "connector_id":126, "view":126, "epoch":offset,
            "view_epoch":offset+1, "kind":if offset==0 {"snapshot"} else {"changes"},
            "rows":[{"values":row[2], "weight":row[3]}]
        }]));
        output.push(row);
    }
    (json!(output), json!(sink))
}

/// Validate the full transcript. A missing observation is a failure, including locally.
pub fn validate(e: &Value) -> Result<(), &'static str> {
    let m = manifest();
    if e["scenario_id"] != m["scenario_id"]
        || e["manifest_sha256"] != manifest_sha256()
        || e["recovery_manifest_sha256"] != recovery_sha256()
        || !hex(e["revision"].as_str().unwrap_or(""), 40)
    {
        return Err("scenario identity");
    }
    let images = e["images"].as_array().ok_or("image identity")?;
    if images.len() != 2 {
        return Err("image identity");
    }
    preflight(
        images[0]["reference"].as_str().ok_or("image identity")?,
        images[1]["reference"].as_str().ok_or("image identity")?,
    )?;
    for image in images {
        digest(image["id"].as_str().ok_or("image identity")?)?;
        if !hex(image["revision"].as_str().unwrap_or(""), 40)
            || image["version"].as_str().unwrap_or("").is_empty()
            || image["protocol"] != m["scenario_id"]
        {
            return Err("image identity");
        }
    }
    if images[0]["id"] == images[1]["id"] || images[0]["revision"] == images[1]["revision"] {
        return Err("image identity");
    }
    let workers = e["workers"].as_array().ok_or("serving process identity")?;
    if workers.len() != 7 {
        return Err("serving process identity");
    }
    for (index, worker) in workers.iter().enumerate() {
        let id = index + 1;
        let image = &images[usize::from(id > 3)];
        let identity = &worker["identity"];
        if worker["worker_id"] != id
            || identity["worker_id"] != id
            || worker["image_id"] != image["id"]
            || identity["revision"] != image["revision"]
            || identity["version"] != image["version"]
            || identity["protocol"] != m["scenario_id"]
            || identity["registered_at_ms"].as_u64().unwrap_or(0) == 0
            || !hex(worker["container_id"].as_str().unwrap_or(""), 64)
            || workers[..index]
                .iter()
                .any(|other| other["container_id"] == worker["container_id"])
        {
            return Err("serving process identity");
        }
    }
    let acks = e["acknowledged_operations"]
        .as_array()
        .ok_or("acknowledged history")?;
    if acks.len() != 35 {
        return Err("acknowledged history");
    }
    let mut last_ack = 0;
    for (offset, ack) in acks.iter().enumerate() {
        let at = ack["ack_at_ms"].as_u64().ok_or("acknowledged history")?;
        if ack["operation"] != operation(offset as u64)
            || ack["partition"] != 0
            || ack["offset"] != offset
            || at == 0
            || at < last_ack
        {
            return Err("acknowledged history");
        }
        last_ack = at;
    }
    let replacements = e["replacements"].as_array().ok_or("replacement sequence")?;
    if replacements.len() != 3 {
        return Err("replacement sequence");
    }
    let mut last_registration = 0;
    for (index, replacement) in replacements.iter().enumerate() {
        let stopped = replacement["stopped_at_ms"]
            .as_u64()
            .ok_or("replacement sequence")?;
        let registered = workers[index + 3]["identity"]["registered_at_ms"]
            .as_u64()
            .unwrap();
        let start = 5 + index * 6;
        if replacement["old_worker"] != index + 1
            || replacement["new_worker"] != index + 4
            || stopped < last_registration
            || registered < stopped
            || acks[start + 1]["ack_at_ms"].as_u64().unwrap() > stopped
            || acks[start + 2]["ack_at_ms"].as_u64().unwrap() < stopped
            || acks[start + 5]["ack_at_ms"].as_u64().unwrap() > registered
        {
            return Err("replacement sequence");
        }
        last_registration = registered;
    }
    if e["failed_replacement"]
        != json!({"worker_id":5,"exit_code":42,
        "registered":false,"created_state_directory":false})
        || e["downgrade_rejection"] != "downgrade is not qualified; retry the candidate forward"
    {
        return Err("failed replacement safety");
    }
    if e["reopen_before_launch"] != json!({"workers":[],"leases":[],"local_state_exists":false}) {
        return Err("fresh reopen");
    }
    let checkpoints = e["checkpoints"].as_array().ok_or("checkpoint sequence")?;
    if checkpoints.len() != 11 {
        return Err("checkpoint sequence");
    }
    let active = [
        json!([1, 2, 3]),
        json!([2, 3]),
        json!([2, 3, 4]),
        json!([3, 4]),
        json!([3, 4]),
        json!([3, 4, 5]),
        json!([4, 5]),
        json!([4, 5, 6]),
        json!([4, 5, 6]),
        json!([7]),
        json!([7]),
    ];
    let mut previous_owner = 0;
    let mut previous_token = 0;
    for (index, checkpoint) in checkpoints.iter().enumerate() {
        let epoch = m["checkpoints"][index][1].as_u64().unwrap();
        let observation = &checkpoint["observation"];
        let lease = &observation["lease"];
        let owner = lease["worker_id"].as_u64().ok_or("ownership")?;
        let token = lease["lease_token"].as_u64().ok_or("ownership")?;
        if checkpoint["name"] != m["checkpoints"][index][0]
            || checkpoint["active_workers"] != active[index]
        {
            return Err("checkpoint sequence");
        }
        if lease != &checkpoint["control_lease"]
            || lease["shard_id"] != 126
            || observation["process_id"].as_u64().unwrap_or(0) == 0
            || !active[index].as_array().unwrap().contains(&json!(owner))
            || token == 0
            || token < previous_token
            || (previous_owner != owner && token <= previous_token)
        {
            return Err("ownership");
        }
        previous_owner = owner;
        previous_token = token;
        if observation["source_epoch"] != epoch
            || observation["source_offsets"] != json!({"0":epoch})
            || observation["view_frontier"] != epoch
            || checkpoint["query_frontier"] != epoch
        {
            return Err("source/view progress");
        }
        let (output, sink) = expected(epoch);
        let expected_relation = if epoch % 6 == 1 {
            manifest()["during_relation"].clone()
        } else {
            serde_json::from_str::<Value>(RECOVERY_MANIFEST).unwrap()["expected_relation"].clone()
        };
        let query_rows = if epoch % 6 == 1 {
            manifest()["during_query_rows"].clone()
        } else {
            manifest()["query_rows"].clone()
        };
        if observation["weighted_output"] != output
            || observation["relation"] != expected_relation
            || checkpoint["query_rows"] != query_rows
        {
            return Err("view output");
        }
        if checkpoint["sink_records"] != sink
            || relation(&checkpoint["sink_records"]).as_ref() != Some(&expected_relation)
        {
            return Err("sink output");
        }
        if checkpoint["schema"] != m["schema"] {
            return Err("schema compatibility");
        }
        if observation["in_flight_epochs"] != 0
            || observation["checkpoint_history_entries"] != epoch
            || observation["sink_progress"]
                != json!({"view":126,"columns":2,"topic":"worker-failure-output",
                "snapshot_complete":true,"view_epoch":epoch,"transaction":epoch,"pending":null})
        {
            return Err("application state");
        }
        let observed = observation["observed_at_ms"]
            .as_u64()
            .ok_or("checkpoint deadline")?;
        let query_at = checkpoint["query_at_ms"]
            .as_u64()
            .ok_or("checkpoint deadline")?;
        let ack_at = acks[(epoch - 1) as usize]["ack_at_ms"].as_u64().unwrap();
        if query_at < ack_at || query_at < observed || query_at - ack_at > 60_000 {
            return Err("checkpoint deadline");
        }
        if let Some(step) = [1, 3, 6].iter().position(|i| *i == index) {
            let stopped = replacements[step]["stopped_at_ms"].as_u64().unwrap();
            let registered = workers[step + 3]["identity"]["registered_at_ms"]
                .as_u64()
                .unwrap();
            if query_at < stopped || query_at > registered {
                return Err("replacement query boundary");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Validator fixture only. Release evidence is assembled from running processes.
    fn fixture() -> Value {
        let images = json!([
            {"reference":format!("old@sha256:{}","a".repeat(64)),"id":format!("sha256:{}","c".repeat(64)),
                "revision":"a".repeat(40),"version":"old","protocol":manifest()["scenario_id"]},
            {"reference":format!("new@sha256:{}","b".repeat(64)),"id":format!("sha256:{}","d".repeat(64)),
                "revision":"b".repeat(40),"version":"candidate","protocol":manifest()["scenario_id"]}
        ]);
        let acks = (0..35)
            .map(|offset| {
                json!({"operation":operation(offset),"offset":offset,
            "partition":0,"ack_at_ms":1000+offset*100})
            })
            .collect::<Vec<_>>();
        let workers = (1..=7).map(|id| {
            let image = &images[usize::from(id>3)];
            json!({"worker_id":id,"container_id":format!("{id:064x}"),"image_id":image["id"],
                "identity":{"worker_id":id,"registered_at_ms":if id<=3 {100} else {id*600-350},
                    "revision":image["revision"],"version":image["version"],"protocol":image["protocol"]}})
        }).collect::<Vec<_>>();
        let active = [
            json!([1, 2, 3]),
            json!([2, 3]),
            json!([2, 3, 4]),
            json!([3, 4]),
            json!([3, 4]),
            json!([3, 4, 5]),
            json!([4, 5]),
            json!([4, 5, 6]),
            json!([4, 5, 6]),
            json!([7]),
            json!([7]),
        ];
        let owners = [1, 2, 2, 3, 3, 3, 4, 4, 4, 7, 7];
        let tokens = [1, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5];
        let checkpoints = manifest()["checkpoints"].as_array().unwrap().iter().enumerate().map(|(i,c)| {
            let epoch = c[1].as_u64().unwrap();
            let (output,sink) = expected(epoch);
            let expected_relation = if epoch % 6 == 1 { manifest()["during_relation"].clone() } else { serde_json::from_str::<Value>(RECOVERY_MANIFEST).unwrap()["expected_relation"].clone() };
            let query_rows = if epoch % 6 == 1 { manifest()["during_query_rows"].clone() } else { manifest()["query_rows"].clone() };
            let lease = json!({"worker_id":owners[i],"shard_id":126,"lease_token":tokens[i]});
            json!({"name":c[0],"active_workers":active[i],"schema":manifest()["schema"],
                "control_lease":lease,"query_frontier":epoch,"query_rows":query_rows,"query_at_ms":1000+epoch*100,
                "sink_records":sink,"observation":{"lease":lease,"process_id":1,"observed_at_ms":999+epoch*100,
                    "source_epoch":epoch,"source_offsets":{"0":epoch},"view_frontier":epoch,
                    "weighted_output":output,"relation":expected_relation,
                    "in_flight_epochs":0,"checkpoint_history_entries":epoch,
                    "sink_progress":{"view":126,"columns":2,"topic":"worker-failure-output",
                        "snapshot_complete":true,"view_epoch":epoch,"transaction":epoch,"pending":null}}})
        }).collect::<Vec<_>>();
        json!({"scenario_id":manifest()["scenario_id"],"manifest_sha256":manifest_sha256(),
            "recovery_manifest_sha256":recovery_sha256(),"revision":"e".repeat(40),"images":images,
            "workers":workers,"acknowledged_operations":acks,"checkpoints":checkpoints,
            "replacements":[{"old_worker":1,"new_worker":4,"stopped_at_ms":1650},
                {"old_worker":2,"new_worker":5,"stopped_at_ms":2250},
                {"old_worker":3,"new_worker":6,"stopped_at_ms":2850}],
            "failed_replacement":{"worker_id":5,"exit_code":42,"registered":false,"created_state_directory":false},
            "reopen_before_launch":{"workers":[],"leases":[],"local_state_exists":false},
            "downgrade_rejection":"downgrade is not qualified; retry the candidate forward"})
    }

    #[test]
    fn oracle_preserves_the_complete_recovery_contract() {
        let recovery: Value = serde_json::from_str(RECOVERY_MANIFEST).unwrap();
        assert_eq!(
            expected(5),
            (
                recovery["expected_weighted_output"].clone(),
                recovery["expected_sink"].clone()
            )
        );
        assert_eq!(
            (0..5).map(operation).collect::<Vec<_>>(),
            recovery["history"].as_array().unwrap().clone()
        );
        assert_eq!(
            operation(5),
            json!({"timestamp":6,"values":[1,"c"],"weight":-1})
        );
        assert_eq!(
            operation(6),
            json!({"timestamp":7,"values":[1,"a"],"weight":1})
        );
        assert_eq!(relation(&expected(6).1), Some(json!([])));
        assert_eq!(
            relation(&expected(35).1),
            Some(recovery["expected_relation"].clone())
        );
    }

    #[test]
    fn rejects_mutable_equal_and_downgrade_images() {
        let old = format!("old@sha256:{}", "a".repeat(64));
        let same = format!("other@sha256:{}", "a".repeat(64));
        let new = format!("sha256:{}", "b".repeat(64));
        assert_eq!(
            preflight("old:latest", &new),
            Err("image must be pinned by SHA-256")
        );
        assert_eq!(
            preflight(&old, &same),
            Err("old and candidate image digests must differ")
        );
        assert_eq!(
            preflight("sha256:abc", &new),
            Err("image must be pinned by SHA-256")
        );
        assert_eq!(reject_downgrade(&old, &new, &old, &new), Ok(()));
        assert_eq!(
            reject_downgrade(&new, &old, &old, &new),
            Err("downgrade is not qualified; retry the candidate forward")
        );
    }

    #[test]
    fn complete_transcript_round_trips_and_qualifies() {
        let evidence = fixture();
        assert_eq!(
            serde_json::from_slice::<Value>(&serde_json::to_vec(&evidence).unwrap()).unwrap(),
            evidence
        );
        assert_eq!(validate(&evidence), Ok(()));
    }

    #[test]
    fn corruptions_fail_at_their_specific_boundary() {
        let evidence = fixture();
        assert_eq!(validate(&evidence), Ok(()));
        for (pointer, value, error) in [
            ("/manifest_sha256", json!("wrong"), "scenario identity"),
            (
                "/images/1/revision",
                evidence["images"][0]["revision"].clone(),
                "image identity",
            ),
            (
                "/workers/3/identity/revision",
                json!("wrong"),
                "serving process identity",
            ),
            (
                "/acknowledged_operations/4/operation/weight",
                json!(1),
                "acknowledged history",
            ),
            (
                "/replacements/1/stopped_at_ms",
                json!(1500),
                "replacement sequence",
            ),
            (
                "/failed_replacement/registered",
                json!(true),
                "failed replacement safety",
            ),
            (
                "/reopen_before_launch/local_state_exists",
                json!(true),
                "fresh reopen",
            ),
            (
                "/images/1/id",
                evidence["images"][0]["id"].clone(),
                "image identity",
            ),
            (
                "/workers/3/container_id",
                evidence["workers"][0]["container_id"].clone(),
                "serving process identity",
            ),
            (
                "/checkpoints/1/observation/process_id",
                json!(0),
                "ownership",
            ),
            (
                "/checkpoints/1/query_at_ms",
                json!(1600),
                "checkpoint deadline",
            ),
            (
                "/checkpoints/1/active_workers",
                json!([1, 2, 3]),
                "checkpoint sequence",
            ),
            (
                "/checkpoints/1/control_lease/lease_token",
                json!(1),
                "ownership",
            ),
            (
                "/checkpoints/1/observation/source_epoch",
                json!(5),
                "source/view progress",
            ),
            (
                "/checkpoints/1/query_frontier",
                json!(5),
                "source/view progress",
            ),
            (
                "/checkpoints/1/observation/weighted_output/3/2/1/Utf8",
                json!("wrong"),
                "view output",
            ),
            (
                "/checkpoints/1/query_rows/0",
                json!("1\twrong"),
                "view output",
            ),
            (
                "/checkpoints/1/schema/0/1",
                json!("Utf8"),
                "schema compatibility",
            ),
            (
                "/checkpoints/1/observation/sink_progress/pending",
                json!({}),
                "application state",
            ),
            (
                "/checkpoints/1/query_at_ms",
                json!(999999),
                "checkpoint deadline",
            ),
            (
                "/checkpoints/9/query_rows/0",
                json!("1\twrong"),
                "view output",
            ),
            (
                "/checkpoints/10/observation/source_offsets/0",
                json!(29),
                "source/view progress",
            ),
        ] {
            let mut mutated = evidence.clone();
            *mutated.pointer_mut(pointer).unwrap() = value;
            assert_eq!(validate(&mutated), Err(error), "{pointer}");
        }
        for duplicate in [false, true] {
            let mut mutated = evidence.clone();
            let records = mutated["checkpoints"][1]["sink_records"]
                .as_array_mut()
                .unwrap();
            if duplicate {
                records.push(records[0].clone());
            } else {
                records.remove(0);
            }
            assert_eq!(validate(&mutated), Err("sink output"));
        }
    }
}

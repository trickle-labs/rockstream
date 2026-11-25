use rockstream_control::{
    plan_hot_key_mitigation, AdaptiveSkewSplitter, HotKeyMitigationPlan, SkewSplitDecision,
    SKEW_SPLIT_TRIGGER_WINDOW,
};
use rockstream_types::config::{SkewSplitConfig, TunerOverrides};
use rockstream_types::ids::{OperatorId, ShardId};
use rockstream_types::laws::{SumCountV1, WeightAddV1};
use rockstream_types::merge_law::LawDescriptor;
use rockstream_types::topology::{KeyLoadSample, ShardLoadSample};

fn make_shard_sample(
    shard_id: u64,
    hot_key: &[u8],
    hot_cpu: u64,
    cold_cpu: u64,
) -> ShardLoadSample {
    let mut key_loads = vec![KeyLoadSample {
        key_prefix: hot_key.to_vec(),
        cpu_nanos: hot_cpu,
        bytes_per_epoch: hot_cpu / 2,
        state_writes_per_epoch: hot_cpu / 4,
    }];
    for idx in 0..4 {
        key_loads.push(KeyLoadSample {
            key_prefix: format!("cold-{idx}").into_bytes(),
            cpu_nanos: cold_cpu,
            bytes_per_epoch: cold_cpu / 2,
            state_writes_per_epoch: cold_cpu / 4,
        });
    }
    ShardLoadSample {
        shard_id: ShardId(shard_id),
        state_bytes: 1024,
        rows_per_epoch: 2048,
        cpu_nanos: key_loads.iter().map(|load| load.cpu_nanos).sum(),
        bytes_per_epoch: key_loads.iter().map(|load| load.bytes_per_epoch).sum(),
        state_writes_per_epoch: key_loads
            .iter()
            .map(|load| load.state_writes_per_epoch)
            .sum(),
        key_loads,
    }
}

fn make_cluster_samples() -> Vec<ShardLoadSample> {
    vec![
        make_shard_sample(1, b"hot", 60_000, 1_000),
        make_shard_sample(2, b"warm", 1_500, 1_000),
        make_shard_sample(3, b"cool", 1_200, 900),
    ]
}

#[test]
fn skew_control_loop_waits_thirty_seconds_then_spills_without_domain_admission() {
    let mut controller = AdaptiveSkewSplitter::new(SkewSplitConfig {
        enabled: true,
        hot_key_factor: 10.0,
        max_skew_buckets: 16,
    });
    let operator = OperatorId(17);
    let samples = make_cluster_samples();

    assert!(controller
        .observe(
            operator,
            &LawDescriptor::from_bundle(&SumCountV1),
            &samples,
            None,
            ShardId(99),
            0,
            None,
        )
        .unwrap()
        .is_none());

    let decision = controller
        .observe(
            operator,
            &LawDescriptor::from_bundle(&SumCountV1),
            &samples,
            Some(&TunerOverrides {
                skew_buckets: Some(8),
                ..TunerOverrides::default()
            }),
            ShardId(99),
            SKEW_SPLIT_TRIGGER_WINDOW.as_millis() as u64 + 1,
            None,
        )
        .unwrap()
        .expect("expected skew split after 30s");

    assert_eq!(decision.operator_id, operator);
    assert_eq!(decision.shard_id, ShardId(1));
    assert_eq!(decision.bucket_count, 8);
    assert!(decision.load_factor > 10.0);
    assert!(matches!(
        decision.plan,
        HotKeyMitigationPlan::Spill { shard_id, code, .. }
            if shard_id == ShardId(99) && code == rockstream_types::error_code::RS_5036
    ));
}

#[test]
fn skew_control_loop_audits_non_composable_spill_decision() {
    let dir = tempfile::tempdir().unwrap();
    let log =
        rockstream_control::audit::FileAuditLog::open(dir.path().join("audit.jsonl")).unwrap();
    let mut controller = AdaptiveSkewSplitter::new(SkewSplitConfig {
        enabled: true,
        hot_key_factor: 10.0,
        max_skew_buckets: 16,
    });
    assert!(controller
        .observe(
            OperatorId(23),
            &LawDescriptor::from_bundle(&WeightAddV1),
            &make_cluster_samples(),
            None,
            ShardId(77),
            0,
            None,
        )
        .unwrap()
        .is_none());

    let decision = controller
        .observe(
            OperatorId(23),
            &LawDescriptor::from_bundle(&WeightAddV1),
            &make_cluster_samples(),
            None,
            ShardId(77),
            SKEW_SPLIT_TRIGGER_WINDOW.as_millis() as u64 + 1,
            Some(&log),
        )
        .unwrap()
        .expect("expected spill routing decision");

    assert!(matches!(
        decision,
        SkewSplitDecision {
            plan: HotKeyMitigationPlan::Spill {
                shard_id,
                code,
                ..
            },
            ..
        } if shard_id == ShardId(77) && code == rockstream_types::error_code::RS_5036
    ));

    let events = log.read_all().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "skew_splitting.adjusted");
    let detail = events[0].detail.as_deref().unwrap();
    assert!(detail.contains("load_factor="));
    assert!(detail.contains("decision=single_owner_fallback"));
    assert!(detail.contains("reassociation_admitted=false"));
}

#[test]
fn routing_decision_requires_executable_domain_admission() {
    let composable = plan_hot_key_mitigation(
        &LawDescriptor::from_bundle(&SumCountV1),
        OperatorId(5),
        4,
        ShardId(8),
    );
    let non_composable = plan_hot_key_mitigation(
        &LawDescriptor::from_bundle(&WeightAddV1),
        OperatorId(5),
        4,
        ShardId(8),
    );

    assert!(matches!(
        composable,
        HotKeyMitigationPlan::Spill { code, .. } if code == rockstream_types::error_code::RS_5036
    ));
    assert!(matches!(
        non_composable,
        HotKeyMitigationPlan::Spill { code, .. } if code == rockstream_types::error_code::RS_5036
    ));
}

#[test]
fn disabled_skew_controller_ignores_an_overloaded_cluster() {
    let mut controller = AdaptiveSkewSplitter::new(SkewSplitConfig {
        enabled: false,
        hot_key_factor: 1.0,
        max_skew_buckets: 16,
    });

    assert_eq!(
        controller
            .observe(
                OperatorId(17),
                &LawDescriptor::from_bundle(&SumCountV1),
                &make_cluster_samples(),
                None,
                ShardId(99),
                SKEW_SPLIT_TRIGGER_WINDOW.as_millis() as u64 + 1,
                None,
            )
            .unwrap(),
        None
    );
}

#[test]
fn changing_hot_keys_must_restart_the_sustained_window() {
    let mut controller = AdaptiveSkewSplitter::new(SkewSplitConfig {
        enabled: true,
        hot_key_factor: 10.0,
        max_skew_buckets: 15,
    });
    let operator = OperatorId(31);
    let law = LawDescriptor::from_bundle(&SumCountV1);
    let mut samples = make_cluster_samples();
    let window = SKEW_SPLIT_TRIGGER_WINDOW.as_millis() as u64;
    assert_eq!(
        controller
            .observe(operator, &law, &samples, None, ShardId(99), 0, None)
            .unwrap(),
        None
    );
    samples[0] = make_shard_sample(1, b"new-hot", 60_000, 1_000);
    assert_eq!(
        controller
            .observe(operator, &law, &samples, None, ShardId(99), window, None)
            .unwrap(),
        None
    );
    let decision = controller
        .observe(
            operator,
            &law,
            &samples,
            None,
            ShardId(99),
            window * 2,
            None,
        )
        .unwrap()
        .unwrap();
    assert_eq!(decision.hot_key.key_prefix, b"new-hot".to_vec());
    assert_eq!(decision.bucket_count, 8);
    assert!(matches!(
        decision.plan,
        HotKeyMitigationPlan::Spill { shard_id, .. } if shard_id == ShardId(99)
    ));
}

#[test]
fn equal_score_keys_and_shards_do_not_depend_on_report_order() {
    let mut samples = make_cluster_samples();
    samples[0] = make_shard_sample(1, b"alpha", 60_000, 1_000);
    samples[0].key_loads.push(KeyLoadSample {
        key_prefix: b"zeta".to_vec(),
        cpu_nanos: 60_000,
        bytes_per_epoch: 30_000,
        state_writes_per_epoch: 15_000,
    });
    let mut tied_shard = samples[0].clone();
    tied_shard.shard_id = ShardId(4);
    samples.push(tied_shard);
    let mut reversed = samples.clone();
    reversed.reverse();
    for sample in &mut reversed {
        sample.key_loads.reverse();
    }
    let decide = |samples: &[ShardLoadSample]| {
        let mut controller = AdaptiveSkewSplitter::new(SkewSplitConfig {
            enabled: true,
            hot_key_factor: 10.0,
            max_skew_buckets: 16,
        });
        let law = LawDescriptor::from_bundle(&SumCountV1);
        let operator = OperatorId(31);
        assert_eq!(
            controller
                .observe(operator, &law, samples, None, ShardId(99), 0, None)
                .unwrap(),
            None
        );
        controller
            .observe(
                operator,
                &law,
                samples,
                None,
                ShardId(99),
                SKEW_SPLIT_TRIGGER_WINDOW.as_millis() as u64,
                None,
            )
            .unwrap()
            .unwrap()
    };
    let decision = decide(&samples);
    assert_eq!(decision.shard_id, ShardId(1));
    assert_eq!(decision.hot_key.key_prefix, b"alpha".to_vec());
    assert_eq!(decide(&reversed), decision);
}

#[test]
fn invalid_skew_configuration_cannot_trigger_a_split() {
    for (factor, max_buckets) in [(f64::NAN, 16), (10.0, 0)] {
        let mut controller = AdaptiveSkewSplitter::new(SkewSplitConfig {
            enabled: true,
            hot_key_factor: factor,
            max_skew_buckets: max_buckets,
        });
        for now in [0, SKEW_SPLIT_TRIGGER_WINDOW.as_millis() as u64] {
            assert_eq!(
                controller
                    .observe(
                        OperatorId(17),
                        &LawDescriptor::from_bundle(&SumCountV1),
                        &make_cluster_samples(),
                        None,
                        ShardId(99),
                        now,
                        None,
                    )
                    .unwrap(),
                None
            );
        }
    }
}

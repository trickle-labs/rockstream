use rockstream_control::skew::{plan_hot_key_mitigation, HotKeyMitigationPlan};
use rockstream_types::error_code::RS_5036;
use rockstream_types::ids::{OperatorId, ShardId};
use rockstream_types::laws::{SumCountV1, WeightAddV1};
use rockstream_types::merge_law::LawDescriptor;

#[test]
fn non_composable_laws_route_hot_keys_to_single_spill_shard() {
    let plan = plan_hot_key_mitigation(
        &LawDescriptor::from_bundle(&WeightAddV1),
        OperatorId(11),
        8,
        ShardId(99),
    );

    assert!(matches!(
        plan,
        HotKeyMitigationPlan::Spill {
            shard_id,
            code,
            ..
        } if shard_id == ShardId(99) && code == RS_5036
    ));
}

#[test]
fn composable_laws_without_operand_admission_use_spill_fallback() {
    let plan = plan_hot_key_mitigation(
        &LawDescriptor::from_bundle(&SumCountV1),
        OperatorId(11),
        8,
        ShardId(99),
    );

    assert!(matches!(
        plan,
        HotKeyMitigationPlan::Spill { shard_id, code, .. }
            if shard_id == ShardId(99) && code == RS_5036
    ));
}

#[test]
fn regrouping_fallback_does_not_normalize_unadmitted_bucket_count() {
    let plan = plan_hot_key_mitigation(
        &LawDescriptor::from_bundle(&SumCountV1),
        OperatorId(11),
        6,
        ShardId(99),
    );

    assert!(matches!(
        plan,
        HotKeyMitigationPlan::Spill { shard_id, code, .. }
            if shard_id == ShardId(99) && code == RS_5036
    ));
}

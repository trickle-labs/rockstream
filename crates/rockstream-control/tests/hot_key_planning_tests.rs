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
fn composable_laws_use_virtual_bucket_split_and_combine() {
    let plan = plan_hot_key_mitigation(
        &LawDescriptor::from_bundle(&SumCountV1),
        OperatorId(11),
        8,
        ShardId(99),
    );

    assert!(matches!(
        plan,
        HotKeyMitigationPlan::Split { bucket_count, source, .. } if bucket_count == 8 && source == OperatorId(11)
    ));
}

#[test]
fn composable_laws_use_power_of_two_virtual_bucket_split() {
    let plan = plan_hot_key_mitigation(
        &LawDescriptor::from_bundle(&SumCountV1),
        OperatorId(11),
        6,
        ShardId(99),
    );

    assert!(matches!(
        plan,
        HotKeyMitigationPlan::Split { bucket_count, .. }
            if bucket_count.is_power_of_two() && bucket_count >= 6
    ));
}

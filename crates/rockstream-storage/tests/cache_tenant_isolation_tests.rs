//! Cache Tenant Isolation Tests (v0.59.6).
//!
//! Asserts that cross-tenant and row-security policy data are strictly partitioned
//! and never observable across tenant or security digest boundaries in the shared cache.

use rockstream_storage::storage_context::{BlockCacheKey, WorkerStorageContext};
use rockstream_types::ids::{ArrangementId, TenantId};

#[test]
fn test_storage_context_tenant_isolation() {
    let ctx = WorkerStorageContext::new(1024 * 1024);
    let tenant1 = TenantId(1);
    let tenant2 = TenantId(2);
    let policy = [0u8; 32];
    let arr_id = ArrangementId(42);
    let block_id = 100;

    let key_tenant1 = BlockCacheKey::new(tenant1, policy, arr_id, block_id);
    let key_tenant2 = BlockCacheKey::new(tenant2, policy, arr_id, block_id);

    // Tenant 1 puts sensitive data into cache
    ctx.put_block(key_tenant1.clone(), b"tenant_1_secret_data".to_vec());

    // Tenant 1 can read it
    assert_eq!(
        ctx.get_block(&key_tenant1),
        Some(b"tenant_1_secret_data".to_vec())
    );

    // Tenant 2 reading same arrangement & block ID must get a cache miss!
    assert_eq!(ctx.get_block(&key_tenant2), None);

    // Security policy digest isolation
    let mut different_policy = policy;
    different_policy[0] = 0xAA;
    let key_diff_policy = BlockCacheKey::new(tenant1, different_policy, arr_id, block_id);
    assert_eq!(ctx.get_block(&key_diff_policy), None);
}

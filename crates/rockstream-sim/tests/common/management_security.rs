use std::{io::BufReader, path::Path, sync::Arc};

use rockstream_storage::catalog::DurableCatalogStore;
use rockstream_test_support::pki::TestPki;
use rockstream_types::authorization::{
    LoginMethod, PermissionPreset, PolicyScope, ScopedPresetGrant, SecurityPrincipal,
    SECURITY_PRINCIPAL_FORMAT_VERSION,
};
use rockstream_types::ids::PrincipalId;
use sha2::{Digest, Sha256};

pub async fn preprovision_management_catalog(storage_dir: &Path, pki: &TestPki) {
    let catalog_dir = storage_dir.join("gateway-shard");
    std::fs::create_dir_all(&catalog_dir).unwrap();
    let store =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(catalog_dir).unwrap());
    let catalog = DurableCatalogStore::new(store, "gateway");
    let certs = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(&pki.cli_cert_path).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let certificate_sha256 = Sha256::digest(certs[0].as_ref()).into();
    let principal = SecurityPrincipal {
        format_version: SECURITY_PRINCIPAL_FORMAT_VERSION,
        id: PrincipalId(1),
        display_name: "management test administrator".to_owned(),
        enabled: true,
        revision: 1,
        login_methods: vec![LoginMethod::Mtls { certificate_sha256 }],
        preset_grants: [PermissionPreset::Administrator, PermissionPreset::Operator]
            .into_iter()
            .map(|preset| ScopedPresetGrant {
                preset,
                scope: PolicyScope::Cluster,
            })
            .collect(),
        scoped_grants: Vec::new(),
    };
    catalog
        .preprovision_first_administrator(principal, 1)
        .await
        .unwrap();
}

use super::*;

fn record(id: &str) -> Record {
    Record {
        format_version: FORMAT_VERSION,
        id: id.into(),
        tenant: "customer-123".into(),
        title: "store-A".into(),
        home_region: "eu".into(),
        storage: storage(id),
        storage_format: 2,
        storage_bytes: Some(100),
        permissions: vec!["read".into()],
        cache_ttl_seconds: CACHE_TTL_SECONDS,
        state: Lifecycle::Active as i32,
        created_at: 1,
        deleting_at: 0,
    }
}

/// Fixed IDs, so assertions do not depend on the RNG.
fn id(n: u8) -> String {
    format!("{n:032x}")
}

#[tokio::test]
async fn generated_ids_are_valid_identities() {
    let generated = bedrock_lib::uuid();
    assert!(valid_id(&generated), "{generated}");
    for bad in [
        "",
        "ABCDEF0123456789abcdef0123456789",
        "abc",
        "customer-123/store-A",
        &format!("{generated}0"),
    ] {
        assert!(!valid_id(bad), "{bad}");
    }
}

#[test]
fn every_region_decodes_the_same_identity_from_the_same_bytes() {
    let id = id(0);
    let bytes = record(&id).encode_to_vec();
    let (eu, us) = (decode(&id, &bytes).unwrap(), decode(&id, &bytes).unwrap());
    assert_eq!(eu, us);
    assert_eq!(eu.storage, format!("kv/{id}"));
    assert_eq!(eu.home_region, "eu");
}

#[test]
fn records_that_do_not_match_their_key_or_contract_are_corrupt() {
    // Not id(0): one mutation swaps in the all-zero ID.
    let (id, other) = (id(1), id(2));
    let mutations: [fn(&mut Record); 5] = [
        |r| r.format_version = 2,
        |r| r.storage = "kv/elsewhere".into(),
        |r| r.cache_ttl_seconds = 300,
        |r| r.state = 7,
        |r| r.id = "0".repeat(32),
    ];
    for mutate in mutations {
        let mut bad = record(&id);
        mutate(&mut bad);
        assert!(matches!(
            decode(&id, &bad.encode_to_vec()),
            Err(Error::Corrupt(_))
        ));
    }
    assert!(matches!(
        decode(&other, &record(&id).encode_to_vec()),
        Err(Error::Corrupt(_))
    ));
    assert!(matches!(decode(&id, b"\xff\xff"), Err(Error::Corrupt(_))));
}

#[test]
fn a_namespaces_key_and_storage_never_cover_a_siblings() {
    let a = id(0);
    let b = id(1);
    assert!(!key(&b).starts_with(&key(&a)) && !key(&a).starts_with(&key(&b)));
    // The writer stores namespace `a` under `kv/{a}/`; with fixed-length IDs no other namespace shares that prefix.
    assert!(!format!("{}/", storage(&b)).starts_with(&format!("{}/", storage(&a))));
    assert_eq!(key(&a), format!("kv/namespaces/{a}").into_bytes());
}

#[test]
fn deletion_and_quota_are_checked_on_every_admission() {
    let id = id(0);
    let mut namespace = record(&id);
    assert_eq!(admit_open(&namespace), Ok(()));
    assert_eq!(admit_write(&namespace, 60, 40), Ok(()));
    assert_eq!(admit_write(&namespace, 60, 41), Err(Rejected::StorageQuota));
    assert_eq!(
        admit_write(&namespace, u64::MAX, 1),
        Err(Rejected::StorageQuota)
    );
    namespace.storage_bytes = None;
    assert_eq!(admit_write(&namespace, u64::MAX, u64::MAX), Ok(()));
    namespace.set_state(Lifecycle::Deleting);
    assert_eq!(admit_open(&namespace), Err(Rejected::Deleted));
    assert_eq!(admit_write(&namespace, 0, 0), Err(Rejected::Deleted));
}

#[test]
fn create_requests_need_bounded_printable_labels_within_cloudflares_limits() {
    let ok = Create {
        tenant: "customer-123".into(),
        title: "store-A".into(),
        home_region: "eu".into(),
        ..Create::default()
    };
    assert_eq!(ok.validate(), Ok(()));
    for bad in [
        Create {
            tenant: String::new(),
            ..ok.clone()
        },
        Create {
            title: "a\nb".into(),
            ..ok.clone()
        },
        Create {
            home_region: "x".repeat(MAX_REGION + 1),
            ..ok.clone()
        },
    ] {
        assert!(matches!(bad.validate(), Err(Error::Invalid(_))));
    }
}

#[test]
fn a_tenant_cannot_reuse_an_active_title() {
    let mut records = vec![record(&id(0))];
    assert!(taken(&records, "customer-123", "store-A", ""));
    // Renaming a namespace to its own title, another tenant and other titles are free.
    assert!(!taken(&records, "customer-123", "store-A", &id(0)));
    assert!(!taken(&records, "customer-456", "store-A", ""));
    assert!(!taken(&records, "customer-123", "store-B", ""));
    // A deleted namespace frees its title.
    records[0].set_state(Lifecycle::Deleting);
    assert!(!taken(&records, "customer-123", "store-A", ""));
}

#[test]
fn storage_is_purgeable_only_after_the_grace_since_deletion() {
    let grace = PURGE_GRACE.as_millis() as u64;
    let mut namespace = record(&id(1));
    assert!(
        !purgeable(&namespace, u64::MAX),
        "an active namespace keeps its storage"
    );
    namespace.set_state(Lifecycle::Deleting);
    namespace.deleting_at = 1_000;
    assert!(!purgeable(&namespace, 1_000 + grace - 1));
    assert!(purgeable(&namespace, 1_000 + grace));
    // No recorded deletion time, e.g. from a binary that predates it during an upgrade: never purged until deleting it
    // again records one.
    namespace.deleting_at = 0;
    assert!(!purgeable(&namespace, u64::MAX));
}

use super::*;

fn record(id: &str) -> Record {
    Record {
        format_version: FORMAT_VERSION,
        id: id.into(),
        account: "customer-123".into(),
        title: "store-A".into(),
        home_region: "eu".into(),
        storage: storage(id),
        storage_format: 2,
        storage_bytes: Some(100),
        permissions: vec!["read".into()],
        cache_ttl_seconds: CACHE_TTL_SECONDS,
        state: Lifecycle::Active as i32,
        created_at: 1,
    }
}

#[test]
fn ids_are_valid_identities_and_spread_by_time_and_counter() {
    let ids = [
        new_id("kv-1", 1, 0),
        new_id("kv-1", 1, 1),
        new_id("kv-1", 2, 0),
        new_id("kv-2", 1, 0),
    ];
    for id in &ids {
        assert!(valid_id(id), "{id}");
    }
    let distinct: std::collections::BTreeSet<_> = ids.iter().collect();
    assert_eq!(distinct.len(), ids.len());
    assert_eq!(
        new_id("kv-1", 1, 0),
        ids[0],
        "deterministic, so a seed replays"
    );
    for bad in [
        "",
        "ABCDEF0123456789abcdef0123456789",
        "abc",
        "customer-123/store-A",
        &format!("{}0", ids[0]),
    ] {
        assert!(!valid_id(bad), "{bad}");
    }
}

#[test]
fn every_region_decodes_the_same_identity_from_the_same_bytes() {
    let id = new_id("kv-1", 1, 0);
    let bytes = record(&id).encode_to_vec();
    let (eu, us) = (decode(&id, &bytes).unwrap(), decode(&id, &bytes).unwrap());
    assert_eq!(eu, us);
    assert_eq!(eu.storage, format!("kv/{id}"));
    assert_eq!(eu.home_region, "eu");
}

#[test]
fn records_that_do_not_match_their_key_or_contract_are_corrupt() {
    let id = new_id("kv-1", 1, 0);
    let other = new_id("kv-1", 1, 1);
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
    let a = new_id("kv-1", 1, 0);
    let b = new_id("kv-1", 1, 1);
    assert!(!key(&b).starts_with(&key(&a)) && !key(&a).starts_with(&key(&b)));
    // The writer stores namespace `a` under `kv/{a}/`; with fixed-length IDs no other namespace shares that prefix.
    assert!(!format!("{}/", storage(&b)).starts_with(&format!("{}/", storage(&a))));
    assert_eq!(key(&a), format!("kv/namespaces/{a}").into_bytes());
}

#[test]
fn deletion_and_quota_are_checked_on_every_admission() {
    let id = new_id("kv-1", 1, 0);
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
fn create_requests_need_bounded_printable_labels() {
    let ok = Create {
        account: "customer-123".into(),
        title: "store-A".into(),
        home_region: "eu".into(),
        ..Create::default()
    };
    assert_eq!(ok.validate(), Ok(()));
    for bad in [
        Create {
            account: String::new(),
            ..ok.clone()
        },
        Create {
            title: "a\nb".into(),
            ..ok.clone()
        },
        Create {
            home_region: "x".repeat(MAX_LABEL + 1),
            ..ok.clone()
        },
    ] {
        assert!(matches!(bad.validate(), Err(Error::Invalid(_))));
    }
}

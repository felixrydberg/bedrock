use super::*;
use bedrock_lib::kv::Mutation;

fn value(bytes: usize, expiration: u64) -> Stored {
    Stored {
        value: vec![7; bytes],
        metadata: Some("{\"kind\":\"test\"}".into()),
        expiration,
    }
}

fn write(changes: Vec<(&[u8], Option<Stored>)>) -> Write {
    Write {
        changes: changes
            .into_iter()
            .map(|(key, value)| Mutation {
                key: key.to_vec(),
                value,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

#[test]
fn bulk_validates_every_mutation_before_resolving_duplicate_keys() {
    let valid = write(vec![
        (b"key", Some(value(8, 0))),
        (b"key", None),
        (b"other", Some(value(4, 0))),
    ]);
    let resolved = validate_write(&valid, 1000).unwrap();
    assert_eq!(resolved.len(), 2);
    assert_eq!(resolved[b"key".as_slice()], None);
    assert_eq!(resolved[b"other".as_slice()], Some(value(4, 0)));

    let invalid = write(vec![(b"key", Some(value(1, 2))), (b"key", None)]);
    assert_eq!(
        validate_write(&invalid, 1000).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    for key in [b"".as_slice(), b".", b"..", &[0xff]] {
        assert!(validate_write(&write(vec![(key, None)]), 0).is_err());
    }
    assert!(validate_write(&write(Vec::new()), 0).is_err());
}

#[test]
fn relative_ttl_is_resolved_at_holder_admission_after_routing_delay() {
    let mut request = write(vec![(b"key", Some(value(3, 0)))]);
    request.changes[0].expiration_ttl = Some(60);
    let resolved = validate_write(&request, 25_000).unwrap();
    assert_eq!(resolved[b"key".as_slice()].as_ref().unwrap().expiration, 85);
    assert_eq!(request.changes[0].value.as_ref().unwrap().expiration, 0);
    request.changes[0].expiration_ttl = Some(59);
    assert!(validate_write(&request, 25_000).is_err());
    request.changes[0].expiration_ttl = Some(u64::MAX);
    assert!(validate_write(&request, 25_000).is_err());
    request.changes[0].expiration_ttl = Some(60);
    request.changes[0].value.as_mut().unwrap().expiration = 90;
    assert!(validate_write(&request, 25_000).is_err());
}

#[test]
fn quota_replaces_old_bytes_and_deletes_charge_even_if_value_expired() {
    let old = value(30, 1);
    let smaller = value(3, 0);
    let other = value(12, 0);
    let initial = replace_bytes(0, b"key", None, Some(&old)).unwrap();
    let initial = replace_bytes(initial, b"other", None, Some(&other)).unwrap();
    assert!(old.expired(2));
    let replaced = replace_bytes(initial, b"key", Some(&old), Some(&smaller)).unwrap();
    assert_eq!(
        replaced,
        (b"key".len() + smaller.encoded_len() + b"other".len() + other.encoded_len()) as u64
    );
    let deleted = replace_bytes(replaced, b"key", Some(&smaller), None).unwrap();
    assert_eq!(deleted, (b"other".len() + other.encoded_len()) as u64);
    assert!(replace_bytes(0, b"key", Some(&old), None).is_err());
    assert!(replace_bytes(u64::MAX, b"key", None, Some(&old)).is_err());
}

#[test]
fn stored_value_validation_checks_metadata_and_expiration_at_admission_only() {
    let mut valid = value(0, 61);
    assert!(validate_value(&valid, Some(1000)).is_ok());
    assert!(validate_value(&valid, Some(2000)).is_err());
    assert!(
        validate_value(&valid, None).is_ok(),
        "expired physical rows remain decodable"
    );
    valid.metadata = Some("{".into());
    assert!(validate_value(&valid, None).is_err());
    valid.metadata = Some(format!("\"{}\"", "a".repeat(kv::MAX_METADATA)));
    assert!(validate_value(&valid, None).is_err());
    valid.metadata = Some("null".into());
    valid.expiration = 0;
    assert!(validate_value(&valid, Some(u64::MAX)).is_ok());
}

#[test]
fn corrupt_and_future_value_envelopes_are_data_loss() {
    let meta = Meta {
        version: 1,
        revision: 4,
        ..Default::default()
    };
    let mut row = Row {
        version: 1,
        revision: 3,
        value: Some(value(2, 10)),
    };
    assert_eq!(
        decode_row(&row.encode_to_vec(), &meta).unwrap(),
        value(2, 10)
    );
    for bytes in [vec![0xff], Vec::new()] {
        assert_eq!(
            decode_row(&bytes, &meta).unwrap_err().code(),
            tonic::Code::DataLoss
        );
    }
    row.revision = 5;
    assert_eq!(
        decode_row(&row.encode_to_vec(), &meta).unwrap_err().code(),
        tonic::Code::DataLoss
    );
    row.revision = 4;
    row.value = None;
    assert_eq!(
        decode_row(&row.encode_to_vec(), &meta).unwrap_err().code(),
        tonic::Code::DataLoss
    );
}

#[test]
fn retry_decodes_original_revision_and_commit_time_without_renewal() {
    let meta = Meta {
        version: 1,
        revision: 7,
        committed_at: 500,
        ..Default::default()
    };
    let original = Retained {
        version: 1,
        revision: 2,
        committed_at: 100,
        expires_at: 100 + kv::RETRY_MS,
        write: None,
        written_keys: 3,
    };
    let recovered = decode_retained(&original.encode_to_vec(), &meta, kv::RETRY_MS, false).unwrap();
    assert_eq!(recovered.revision, 2);
    assert_eq!(recovered.committed_at, 100);
    assert_eq!(recovered.expires_at, 100 + kv::RETRY_MS);
    assert_eq!(recovered.written_keys, 3);
    let mut renewed = original;
    renewed.expires_at += 1;
    assert_eq!(
        decode_retained(&renewed.encode_to_vec(), &meta, kv::RETRY_MS, false)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
}

#[test]
fn retained_event_keeps_original_bulk_with_duplicates_and_deletes() {
    let meta = Meta {
        version: 1,
        revision: 3,
        ..Default::default()
    };
    let original = write(vec![
        (b"one", Some(value(4, 0))),
        (b"one", None),
        (b"two", Some(value(7, 0))),
    ]);
    let row = Retained {
        version: 1,
        revision: 3,
        committed_at: 55,
        expires_at: 55 + kv::EVENT_MS,
        write: Some(original.clone()),
        written_keys: 3,
    };
    let recovered = decode_retained(&row.encode_to_vec(), &meta, kv::EVENT_MS, true).unwrap();
    assert_eq!(recovered.write, Some(original));
    assert!(decode_retained(&row.encode_to_vec(), &meta, kv::RETRY_MS, false).is_err());
}

#[test]
fn absent_values_use_namespace_progress_and_key_prefixes_cannot_alias_metadata() {
    let meta = Meta {
        version: 1,
        revision: 8,
        committed_at: 9,
        bytes: 0,
    };
    let reply = answer(&meta);
    assert_eq!(reply.revision, 8);
    assert_eq!(reply.committed_at, 9);
    assert!(minimum(&meta, 8).is_ok());
    assert_eq!(
        minimum(&meta, 9).unwrap_err().code(),
        tonic::Code::Unavailable
    );
    for customer in [META, DATA, RETRY, EVENT, b"\0m"] {
        assert_ne!(prefixed(DATA, customer), META);
        assert!(!prefixed(DATA, customer).starts_with(RETRY));
        assert!(!prefixed(DATA, customer).starts_with(EVENT));
    }
    let after = exclusive(prefixed(DATA, b"a"));
    assert!(prefixed(DATA, b"a") < after);
    assert!(after <= prefixed(DATA, b"a\0"));
    assert!(after < prefixed(DATA, b"aa"));
}

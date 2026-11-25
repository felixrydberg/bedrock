use super::*;

#[test]
fn cloudflare_key_metadata_and_expiration_limits_are_validated_before_routing() {
    for key in ["", ".", "..", &"x".repeat(kv::MAX_KEY + 1)] {
        assert!(valid_key(key).is_err());
    }
    assert_eq!(
        valid_key(&"x".repeat(kv::MAX_KEY)).unwrap().len(),
        kv::MAX_KEY
    );
    assert!(stored(vec![], Some("not JSON".into()), 0).is_err());
    assert!(
        stored(
            vec![],
            Some(format!("\"{}\"", "x".repeat(kv::MAX_METADATA))),
            0
        )
        .is_err()
    );
    assert!(expiration(None, Some(59)).is_err());
    assert!(expiration(Some(u64::MAX), Some(60)).is_err());
    assert!(expiration(None, Some(u64::MAX)).is_err());
    assert!(expiration(None, Some(60)).is_ok());
}

#[tokio::test]
async fn multipart_keeps_binary_value_and_json_metadata_and_rejects_duplicate_fields() {
    let headers = HeaderMap::from_iter([(
        header::CONTENT_TYPE,
        "multipart/form-data; boundary=bedrock".parse().unwrap(),
    )]);
    let body = Bytes::from_static(b"--bedrock\r\nContent-Disposition: form-data; name=\"value\"\r\n\r\n\x00\xff\r\n--bedrock\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n{\"n\":1}\r\n--bedrock--\r\n");
    let (value, metadata) = value_and_metadata(&headers, body).await.unwrap();
    assert_eq!(value, [0, 255]);
    assert_eq!(metadata.as_deref(), Some("{\"n\":1}"));
    for media in [
        "Multipart/Form-Data",
        "MULTIPART/FORM-DATA",
        "multipart/form-data-extra",
    ] {
        let headers = HeaderMap::from_iter([(
            header::CONTENT_TYPE,
            format!("{media}; boundary=bedrock").parse().unwrap(),
        )]);
        let body = Bytes::from_static(b"--bedrock\r\nContent-Disposition: form-data; name=\"value\"\r\n\r\nbinary\r\n--bedrock--\r\n");
        let parsed = value_and_metadata(&headers, body.clone()).await.unwrap();
        assert_eq!(
            parsed.0,
            if media.ends_with("extra") {
                body.to_vec()
            } else {
                b"binary".to_vec()
            }
        );
    }
    // The reconstructed multipart request must keep the REST body's 100 MiB limit, not axum's 2 MiB default.
    let mut large = b"--bedrock\r\nContent-Disposition: form-data; name=\"value\"\r\n\r\n".to_vec();
    large.extend(vec![0xff; (2 << 20) + 1]);
    large.extend_from_slice(b"\r\n--bedrock--\r\n");
    assert_eq!(
        value_and_metadata(&headers, Bytes::from(large))
            .await
            .unwrap()
            .0
            .len(),
        (2 << 20) + 1
    );
    let duplicate = Bytes::from_static(b"--bedrock\r\nContent-Disposition: form-data; name=\"value\"\r\n\r\na\r\n--bedrock\r\nContent-Disposition: form-data; name=\"value\"\r\n\r\nb\r\n--bedrock--\r\n");
    assert_eq!(
        value_and_metadata(&headers, duplicate)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn malformed_cache_envelopes_are_errors_instead_of_key_absence() {
    assert_eq!(decode(b"\xff").unwrap_err().code(), tonic::Code::DataLoss);
    let invalid = Stored {
        value: vec![],
        metadata: Some("bad JSON".into()),
        expiration: 0,
    };
    assert_eq!(
        decode(&invalid.encode_to_vec()).unwrap_err().code(),
        tonic::Code::DataLoss
    );
}

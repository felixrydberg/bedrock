use super::*;

#[test]
fn keys_follow_cloudflare_limits() {
    assert_eq!(valid_key("a/b c").unwrap(), b"a/b c");
    for key in ["", ".", ".."] {
        assert!(matches!(valid_key(key), Err(Error::Invalid(_))), "{key:?}");
    }
    assert!(valid_key(&"k".repeat(MAX_KEY)).is_ok());
    assert!(matches!(
        valid_key(&"k".repeat(MAX_KEY + 1)),
        Err(Error::TooLarge(_))
    ));
}

#[test]
fn values_and_metadata_follow_cloudflare_limits() {
    let stored = stored(b"v".to_vec(), Some(r#"{"a":1}"#.into()), 0).unwrap();
    assert_eq!(stored.metadata.as_deref(), Some(r#"{"a":1}"#));
    assert!(matches!(
        super::stored(vec![0; MAX_VALUE + 1], None, 0),
        Err(Error::TooLarge(_))
    ));
    let big = format!(r#""{}""#, "m".repeat(MAX_METADATA));
    assert!(matches!(
        super::stored(Vec::new(), Some(big), 0),
        Err(Error::TooLarge(_))
    ));
    assert!(matches!(
        super::stored(Vec::new(), Some("not json".into()), 0),
        Err(Error::Invalid(_))
    ));
}

#[test]
fn expirations_are_absolute_and_at_least_a_minute_away() {
    let now = namespaces::wall_ms() / 1000;
    assert_eq!(expiration(None, None).unwrap(), 0);
    let ttl = expiration(None, Some(MIN_TTL_SECONDS)).unwrap();
    assert!((now + MIN_TTL_SECONDS..=now + MIN_TTL_SECONDS + 1).contains(&ttl));
    assert!(expiration(None, Some(MIN_TTL_SECONDS - 1)).is_err());
    assert!(expiration(Some(now + 3600), None).unwrap() == now + 3600);
    assert!(expiration(Some(now + 30), None).is_err());
    assert!(expiration(Some(now + 3600), Some(3600)).is_err());
}

#[cfg_attr(not(madsim), tokio::test)]
#[cfg_attr(madsim, tokio::test)]
async fn a_multipart_put_carries_value_and_metadata() {
    let boundary = "XBOUNDARY";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"value\"\r\n\r\nhello\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n{{\"m\":true}}\r\n--{boundary}--\r\n"
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        format!("multipart/form-data; boundary={boundary}")
            .parse()
            .unwrap(),
    );
    let (value, metadata) = value_and_metadata(&headers, Bytes::from(body))
        .await
        .unwrap();
    assert_eq!(value, b"hello");
    assert_eq!(metadata.as_deref(), Some(r#"{"m":true}"#));

    let (raw, none) = value_and_metadata(&HeaderMap::new(), Bytes::from_static(b"raw"))
        .await
        .unwrap();
    assert_eq!((raw.as_slice(), none), (b"raw".as_slice(), None));
}

use super::*;

fn row() -> PathRow {
    PathRow {
        sha256: Hash::of(b"<html>"),
        content_type: "text/html; charset=utf-8".into(),
        size: 6,
    }
}

#[test]
fn a_path_row_round_trips_and_every_malformed_value_is_an_error() {
    let encoded = row().encode().unwrap();
    assert_eq!(PathRow::decode(&encoded), Ok(row()));
    let empty = PathRow {
        content_type: String::new(),
        ..row()
    };
    assert_eq!(PathRow::decode(&empty.encode().unwrap()), Ok(empty));

    let mut version = encoded.clone();
    version[0] = 2;
    let mut control = encoded.clone();
    control.push(b'\n');
    let mut utf8 = encoded.clone();
    utf8.push(0xff);
    for value in [&[][..], &encoded[..HEADER - 1], &version, &control, &utf8] {
        assert!(PathRow::decode(value).is_err(), "{value:?}");
    }
    let header = PathRow {
        content_type: "text/html\r\nx: y".into(),
        ..row()
    };
    assert!(header.encode().is_err());
}

#[test]
fn keys_have_one_parse_and_refuse_ambiguous_deployment_ids() {
    let hash = row().sha256;
    assert_eq!(
        path_key("d1", "/assets/a.css").unwrap(),
        b"path/d1//assets/a.css"
    );
    assert_eq!(
        ref_key(&hash, "d1").unwrap(),
        format!("ref/{hash}/d1").into_bytes()
    );
    assert!(
        ref_key(&hash, "d1")
            .unwrap()
            .starts_with(ref_prefix(&hash).as_bytes())
    );
    for id in ["", "a/b"] {
        assert!(path_key(id, "/").is_err());
        assert!(ref_key(&hash, id).is_err());
    }
}

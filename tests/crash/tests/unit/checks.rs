use super::*;

#[test]
fn fixture_copy_replaces_old_contents_and_reports_failures() {
    let root = std::env::temp_dir().join(format!(
        "bedrock-crash-copy-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let source = root.join("source");
    let copied = root.join("copied");
    fs::create_dir_all(source.join("dataset")).unwrap();
    fs::create_dir_all(&copied).unwrap();
    fs::write(source.join("dataset/data"), b"complete fixture").unwrap();
    fs::write(copied.join("old"), b"old fixture").unwrap();
    copy_dir(&source, &copied).unwrap();
    assert_eq!(
        fs::read(copied.join("dataset/data")).unwrap(),
        b"complete fixture"
    );
    assert!(!copied.join("old").exists());

    assert_eq!(
        copy_dir(&root.join("missing"), &copied).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    let blocked = root.join("blocked");
    fs::write(&blocked, b"cannot remove this as a directory").unwrap();
    assert!(copy_dir(&source, &blocked).is_err());
    assert_eq!(
        fs::read(&blocked).unwrap(),
        b"cannot remove this as a directory"
    );
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn unvalidated_observer_cannot_supply_a_retained_view() {
    let (quicksilver, target) = View::new("quicksilver", CELL);
    let (cdn, _) = View::new("cdn", CELL);
    let peers = Arc::new(Peers::new(&Admin::new()));
    let observer =
        Arc::new(Session::start(peers.clone(), vec!["127.0.0.1:1".into()], vec![target]).unwrap());
    let views = Views {
        quicksilver,
        cdn,
        observer,
        unavailable: Mutex::default(),
    };
    // An empty published directory is readable, but it is not a validated observation.
    assert!(views.quicksilver.directory("quicksilver").is_ok());
    views.refresh(&peers);
    let error = views
        .read("quicksilver", |view| view.directory("quicksilver"))
        .unwrap_err();
    assert!(error.contains("no validated membership revision"));
    assert!(views.read("cdn", View::members).is_err());
}

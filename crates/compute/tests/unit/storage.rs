use super::*;

fn query(db: &mut Db, sql: &str) -> rusqlite::Result<i64> {
    db.connection()?.query_row(sql, [], |row| row.get(0))
}

/// Pages written through the VFS, synced and loaded on another holder read back the same database, and a page that
/// is not cached fails the read and names itself instead of reading zeros.
#[test]
fn synced_pages_reopen_and_misses_name_their_page() {
    let mut first = Db::new(0);
    run(first.connection().unwrap(), &|tx: &Transaction<'_>| {
        tx.execute_batch(
            "CREATE TABLE t (n INTEGER); WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE \
             n < 2000) INSERT INTO t SELECT n FROM c;",
        )
    })
    .unwrap();
    let changes = lock(&first.pages).sync();
    let size = changes
        .iter()
        .find(|c| c.key == SIZE_KEY)
        .and_then(|c| c.value.as_deref())
        .map(|v| u64::from_be_bytes(v.try_into().unwrap()))
        .unwrap();
    assert!(size > PAGE as u64, "the table spans several pages");
    assert!(size.is_multiple_of(PAGE as u64));
    assert!(
        lock(&first.pages).sync().is_empty(),
        "a second sync has nothing to commit"
    );

    let mut second = Db::new(size);
    let pages: BTreeMap<u32, Vec<u8>> = changes
        .iter()
        .filter_map(|c| {
            Some((
                u32::from_be_bytes(c.key.as_slice().try_into().ok()?),
                c.value.clone()?,
            ))
        })
        .collect();
    assert_eq!(pages.len() as u64, size / PAGE as u64);
    let last = *pages.keys().last().unwrap();
    {
        let mut cached = lock(&second.pages);
        cached.cached = pages.clone();
        cached.cached.remove(&last);
    }
    assert!(query(&mut second, "SELECT sum(n) FROM t").is_err());
    assert_eq!(lock(&second.pages).missing, Some(last));

    // A new connection, as after a miss, over the complete pages.
    second.connection = None;
    lock(&second.pages).cached = pages;
    assert_eq!(
        query(&mut second, "SELECT sum(n) FROM t").unwrap(),
        2000 * 2001 / 2
    );
}

/// A transaction that fails leaves nothing to commit and the file size where the last sync left it.
#[test]
fn a_failed_transaction_discards_its_pages() {
    let mut db = Db::new(0);
    run(db.connection().unwrap(), &|tx: &Transaction<'_>| {
        tx.execute_batch("CREATE TABLE t (n INTEGER PRIMARY KEY)")
    })
    .unwrap();
    lock(&db.pages).sync();
    let synced = lock(&db.pages).size;
    let failed = run(db.connection().unwrap(), &|tx: &Transaction<'_>| {
        tx.execute_batch(
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 5000) INSERT INTO t SELECT n \
             FROM c; INSERT INTO t VALUES (1);",
        )
    });
    assert!(failed.is_err(), "the duplicate key fails the transaction");
    let mut pages = lock(&db.pages);
    pages.discard();
    assert_eq!(pages.size, synced);
    assert!(pages.sync().is_empty());
    drop(pages);
    assert_eq!(query(&mut db, "SELECT count(*) FROM t").unwrap(), 0);
}

#[test]
fn vfs_rejects_partial_unaligned_and_unaddressable_writes() {
    let pages = Arc::new(Mutex::new(Pages::default()));
    let mut file = File(pages.clone());
    let full_page = vec![7; PAGE];
    for (offset, bytes) in [
        (0, &full_page[..PAGE - 1]),
        (1, full_page.as_slice()),
        (PAGE - 1, full_page.as_slice()),
        (0, &full_page[..0]),
    ] {
        assert_eq!(
            PagesVfs.write(&mut file, offset, bytes),
            Err(vars::SQLITE_IOERR_WRITE)
        );
    }
    // The on-volume key is a u32 page number, even on 64-bit hosts.
    if let Some(offset) = (u64::from(u32::MAX) + 1)
        .checked_mul(PAGE as u64)
        .and_then(|n| usize::try_from(n).ok())
    {
        assert_eq!(
            PagesVfs.write(&mut file, offset, &full_page),
            Err(vars::SQLITE_IOERR_WRITE)
        );
    }
    assert_eq!(PagesVfs.file_size(&mut file).unwrap(), 0);
    assert!(lock(&pages).sync().is_empty());
}

#[test]
fn vfs_reads_pending_writes_then_restores_synced_bytes_on_discard() {
    let pages = Arc::new(Mutex::new(Pages::default()));
    let mut file = File(pages.clone());
    PagesVfs.write(&mut file, 0, &vec![3; PAGE]).unwrap();
    lock(&pages).sync();
    PagesVfs.write(&mut file, 0, &vec![9; PAGE]).unwrap();
    PagesVfs.write(&mut file, PAGE, &vec![8; PAGE]).unwrap();

    let mut bytes = [0; 8];
    assert_eq!(PagesVfs.read(&mut file, PAGE - 8, &mut bytes), Ok(8));
    assert_eq!(bytes, [9; 8]);
    assert_eq!(PagesVfs.file_size(&mut file).unwrap(), PAGE * 2);

    lock(&pages).discard();
    assert_eq!(PagesVfs.read(&mut file, PAGE - 8, &mut bytes), Ok(8));
    assert_eq!(bytes, [3; 8]);
    assert_eq!(PagesVfs.read(&mut file, PAGE, &mut bytes), Ok(0));
    assert_eq!(PagesVfs.file_size(&mut file).unwrap(), PAGE);
    assert!(lock(&pages).sync().is_empty());
}

#[test]
fn vfs_distinguishes_eof_cache_misses_and_cross_page_reads() {
    let pages = Arc::new(Mutex::new(Pages {
        size: (PAGE * 2) as u64,
        synced_size: (PAGE * 2) as u64,
        ..Pages::default()
    }));
    let mut file = File(pages.clone());
    let mut bytes = [42; 8];
    assert_eq!(PagesVfs.read(&mut file, PAGE * 2, &mut bytes), Ok(0));
    assert_eq!(lock(&pages).missing, None);
    assert_eq!(
        PagesVfs.read(&mut file, PAGE - 4, &mut bytes),
        Err(vars::SQLITE_IOERR_READ)
    );
    assert_eq!(lock(&pages).missing, None);
    assert_eq!(
        PagesVfs.read(&mut file, PAGE + 4, &mut bytes),
        Err(vars::SQLITE_IOERR_READ)
    );
    assert_eq!(lock(&pages).missing, Some(1));
    assert_eq!(bytes, [42; 8], "a failed read must not invent data");
}

#[test]
fn truncation_emits_page_deletes_and_size_and_can_be_rolled_back() {
    let pages = Arc::new(Mutex::new(Pages::default()));
    let mut file = File(pages.clone());
    for page in 0..3 {
        PagesVfs
            .write(&mut file, page * PAGE, &vec![page as u8; PAGE])
            .unwrap();
    }
    lock(&pages).sync();

    PagesVfs.truncate(&mut file, PAGE).unwrap();
    lock(&pages).discard();
    assert_eq!(PagesVfs.file_size(&mut file).unwrap(), PAGE * 3);
    let mut bytes = [0; 4];
    assert_eq!(PagesVfs.read(&mut file, PAGE * 2, &mut bytes), Ok(4));
    assert_eq!(bytes, [2; 4]);

    PagesVfs.truncate(&mut file, PAGE).unwrap();
    let changes = lock(&pages).sync();
    assert_eq!(changes.len(), 3);
    for page in 1_u32..3 {
        assert!(
            changes
                .iter()
                .any(|change| { change.key == page.to_be_bytes() && change.value.is_none() })
        );
    }
    assert!(changes.iter().any(|change| {
        change.key == SIZE_KEY && change.value == Some((PAGE as u64).to_be_bytes().to_vec())
    }));
    assert_eq!(lock(&pages).cached.len(), 1);
    assert_eq!(PagesVfs.read(&mut file, PAGE, &mut bytes), Ok(0));
    assert!(lock(&pages).sync().is_empty());
}

#[test]
fn databases_are_isolated_and_unregister_on_drop() {
    let mut first = Db::new(0);
    let mut second = Db::new(0);
    assert_ne!(first.name, second.name);
    for (db, value) in [(&mut first, 11), (&mut second, 22)] {
        run(db.connection().unwrap(), &|tx: &Transaction<'_>| {
            tx.execute_batch("CREATE TABLE t (n INTEGER)")?;
            tx.execute("INSERT INTO t VALUES (?)", [value])?;
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(query(&mut first, "SELECT n FROM t").unwrap(), 11);
    assert_eq!(query(&mut second, "SELECT n FROM t").unwrap(), 22);
    let first_name = first.name.clone();
    drop(first);
    assert!(!lock(&FILES).contains_key(&first_name));
    assert!(lock(&FILES).contains_key(&second.name));
    assert_eq!(query(&mut second, "SELECT n FROM t").unwrap(), 22);
}

#[test]
fn read_only_sql_produces_no_changes_and_failed_updates_preserve_existing_rows() {
    let mut db = Db::new(0);
    run(db.connection().unwrap(), &|tx: &Transaction<'_>| {
        tx.execute_batch("CREATE TABLE t (n INTEGER UNIQUE); INSERT INTO t VALUES (5), (9)")
    })
    .unwrap();
    lock(&db.pages).sync();
    let sum = run(db.connection().unwrap(), &|tx: &Transaction<'_>| {
        tx.query_row("SELECT sum(n) FROM t", [], |row| row.get::<_, i64>(0))
    })
    .unwrap();
    assert_eq!(sum, 14);
    assert!(lock(&db.pages).sync().is_empty());

    let result = run(db.connection().unwrap(), &|tx: &Transaction<'_>| {
        tx.execute_batch("UPDATE t SET n = 7 WHERE n = 5; INSERT INTO t VALUES (9)")
    });
    assert!(result.is_err());
    lock(&db.pages).discard();
    assert_eq!(query(&mut db, "SELECT sum(n) FROM t").unwrap(), 14);
    assert!(lock(&db.pages).sync().is_empty());
}

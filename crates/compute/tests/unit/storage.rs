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
